// Readers-Writers Problem (reader-preference) using C++20 semaphores.
// Build: g++ -std=c++20 -O2 -pthread main.cpp -o rw
// Run:   ./rw [output.html] [seed]
//
// Layout of this file:
//   1. EventLogger        - thread-safe trace recorder (visualization support)
//   2. ReaderPreferenceRW - the synchronization algorithm (the part to explain)
//   3. validateTrace      - offline checker that replays the recorded trace
//   4. writeHtml          - turns the recorded trace into a standalone HTML replay
//   5. main               - workload definition, thread creation, reporting
#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <random>
#include <semaphore>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;

static void sleepMs(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

// ---------------------------------------------------------------- 1. Logger
struct Event {
    int seq;               // position in the global order (assigned under the logger lock)
    long long micros;      // time since program start
    std::string thread;    // "R1", "W2", ...
    std::string type;      // R_REQUEST, W_ACQUIRED, ...
    int readCount;         // value of readCount when logged under readCountMutex, else -1
    std::string op;        // semaphore operation this event refers to
    std::string detail;    // human-readable explanation
};

// One mutex makes "take sequence number + timestamp + store" a single atomic step,
// so seq order is exactly the order in which events were recorded.
// This mutex is only for the trace; it is NOT part of the readers-writers algorithm.
class EventLogger {
public:
    void record(const std::string& thread, const std::string& type, int readCount,
                const std::string& op, const std::string& detail) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start_).count();
        events_.push_back({static_cast<int>(events_.size()), us, thread, type, readCount, op, detail});
    }
    std::vector<Event> snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return events_;
    }
private:
    mutable std::mutex mutex_;
    std::vector<Event> events_;
    Clock::time_point start_ = Clock::now();
};

// ------------------------------------------------- 2. Synchronization logic
// SHARED RESOURCE: sharedData (a plain int, deliberately NOT atomic, so a missing lock
//                  would be a real data race).
// readCount      : number of readers currently in the reading group (shared variable).
class ReaderPreferenceRW {
public:
    explicit ReaderPreferenceRW(EventLogger& logger) : log_(logger) {}

    void reader(const std::string& name, int readMs) {
        log_.record(name, "R_REQUEST", -1, "-", "Reader wants to read.");

        readCountMutex_.acquire();                 // wait(readCountMutex)  -- entry section
        ++readCount_;                              // protected: critical section on readCount
        if (readCount_ == 1) {
            // First reader locks the resource on behalf of the whole group.
            // May block here if a writer holds it; later readers then queue on readCountMutex.
            log_.record(name, "R_FIRST_ACQUIRING", -1, "wait(resourceSem)",
                        "First reader calls wait(resourceSem); blocks if a writer is inside.");
            resourceSem_.acquire();                // wait(resourceSem)
            log_.record(name, "R_FIRST_ACQUIRED", readCount_, "wait(resourceSem) succeeded",
                        "First reader got resourceSem (1->0). Reading group is open.");
        } else {
            log_.record(name, "R_JOIN", readCount_, "(resourceSem already held by group)",
                        "Reader joins the existing group without touching resourceSem.");
        }
        readCountMutex_.release();                 // signal(readCountMutex)

        // ---- CRITICAL SECTION (shared mode): many readers may be here together ----
        if (activeWriters_.load() != 0) ++violations_;   // a writer must never be here
        activeReaders_.fetch_add(1);
        int valueSeen = sharedData_;               // read only
        sleepMs(readMs);                           // simulate reading work
        activeReaders_.fetch_sub(1);
        // ---------------------------------------------------------------------------

        readCountMutex_.acquire();                 // wait(readCountMutex)  -- exit section
        --readCount_;
        log_.record(name, "R_LEAVE", readCount_, "-",
                    "Reader leaves the group after reading sharedData=" + std::to_string(valueSeen) + ".");
        if (readCount_ == 0) {
            // Log BEFORE releasing: once released, another thread may acquire immediately,
            // and its event must appear after this one in the trace.
            log_.record(name, "R_LAST_RELEASE", readCount_, "signal(resourceSem)",
                        "Last reader releases resourceSem (0->1). Resource is free.");
            resourceSem_.release();                // signal(resourceSem)
        }
        readCountMutex_.release();                 // signal(readCountMutex)
        log_.record(name, "R_DONE", -1, "-", "Reader finished.");
    }

    void writer(const std::string& name, int writeMs) {
        log_.record(name, "W_REQUEST", -1, "wait(resourceSem)",
                    "Writer requests exclusive access and calls wait(resourceSem).");
        resourceSem_.acquire();                    // wait(resourceSem) -- blocks while any reader/writer is inside
        log_.record(name, "W_ACQUIRED", -1, "wait(resourceSem) succeeded",
                    "Writer got resourceSem (1->0). Exclusive access.");

        // ---- CRITICAL SECTION (exclusive mode) ----
        if (activeReaders_.load() != 0 || activeWriters_.load() != 0) ++violations_;
        activeWriters_.fetch_add(1);
        ++sharedData_;                             // write
        int written = sharedData_;
        sleepMs(writeMs);                          // simulate writing work
        activeWriters_.fetch_sub(1);
        // -------------------------------------------

        log_.record(name, "W_RELEASE", -1, "signal(resourceSem)",
                    "Writer finished writing sharedData=" + std::to_string(written) +
                    " and releases resourceSem (0->1).");
        resourceSem_.release();                    // signal(resourceSem)
        log_.record(name, "W_DONE", -1, "-", "Writer finished.");
    }

    int violations() const { return violations_.load(); }
    int finalData() const { return sharedData_; }

private:
    EventLogger& log_;
    std::binary_semaphore readCountMutex_{1};  // initial 1: protects readCount_ (mutual exclusion)
    std::binary_semaphore resourceSem_{1};     // initial 1: 1 = resource free, 0 = locked
    int readCount_ = 0;                        // guarded by readCountMutex_
    int sharedData_ = 0;                       // guarded by resourceSem_ (reader group / one writer)
    // Self-checks, independent of the semaphores:
    std::atomic<int> activeReaders_{0}, activeWriters_{0}, violations_{0};
};

// ------------------------------------------------------- 3. Trace validator
struct Verdict {
    bool ok = true;
    std::vector<std::string> problems;
    int maxReaders = 0, resourceAcquires = 0, resourceReleases = 0;
};

Verdict validateTrace(const std::vector<Event>& ev) {
    Verdict v;
    bool locked = false, writerIn = false;
    std::string owner;
    int readers = 0;
    long long lastTime = -1;
    auto fail = [&](int seq, const std::string& msg) {
        v.ok = false;
        v.problems.push_back("seq " + std::to_string(seq) + ": " + msg);
    };
    for (size_t i = 0; i < ev.size(); ++i) {
        const Event& e = ev[i];
        if (e.seq != static_cast<int>(i)) fail(e.seq, "sequence number not contiguous");
        if (e.micros < lastTime) fail(e.seq, "timestamp went backwards");
        lastTime = e.micros;

        if (e.type == "R_FIRST_ACQUIRED") {
            if (locked || readers || writerIn) fail(e.seq, "first reader acquired a locked resource");
            locked = true; owner = "readers"; ++readers; ++v.resourceAcquires;
        } else if (e.type == "R_JOIN") {
            if (!locked || owner != "readers" || writerIn) fail(e.seq, "reader joined without a reader-held lock");
            ++readers;
        } else if (e.type == "R_LEAVE") {
            if (readers <= 0) fail(e.seq, "reader left but none were reading"); else --readers;
        } else if (e.type == "R_LAST_RELEASE") {
            if (!locked || owner != "readers" || readers != 0) fail(e.seq, "bad last-reader release");
            locked = false; owner.clear(); ++v.resourceReleases;
        } else if (e.type == "W_ACQUIRED") {
            if (locked || readers || writerIn) fail(e.seq, "writer overlaps a reader or writer");
            locked = true; owner = "writer"; writerIn = true; ++v.resourceAcquires;
        } else if (e.type == "W_RELEASE") {
            if (!writerIn || !locked) fail(e.seq, "writer released without owning");
            writerIn = false; locked = false; owner.clear(); ++v.resourceReleases;
        }
        if (e.readCount >= 0 && e.readCount != readers)
            fail(e.seq, "recorded readCount " + std::to_string(e.readCount) +
                        " != replayed " + std::to_string(readers));
        v.maxReaders = std::max(v.maxReaders, readers);
    }
    if (locked || readers || writerIn) fail(static_cast<int>(ev.size()), "resource still held at end of trace");
    if (v.resourceAcquires != v.resourceReleases) fail(static_cast<int>(ev.size()), "acquire/release count mismatch");
    return v;
}

// --------------------------------------------------------- 4. HTML generation
static std::string jsonEscape(const std::string& s) {
    std::string r;
    for (char c : s) { if (c == '"' || c == '\\') r += '\\'; r += c; }
    return r;
}

static const char* kHtmlHead = R"HTML(<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Resource Control Room: Who Gets Access?</title>
<style>
:root{--bg:#0b1020;--panel:#131a30;--line:#26314f;--tx:#e6ebf7;--mut:#8a94b0;--wait:#8a94a6;--acq:#f5b942;--read:#38d9c0;--write:#ff4d6d;--rel:#b197fc}
@media (prefers-color-scheme:light){:root{--bg:#eef1f8;--panel:#fff;--line:#c9d1e6;--tx:#161c30;--mut:#5b6580}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--tx);font:14px system-ui,sans-serif}
header{padding:14px 20px;border-bottom:1px solid var(--line);display:flex;gap:12px;align-items:baseline;flex-wrap:wrap}
h1{margin:0;font-size:20px;letter-spacing:.5px}header span{color:var(--mut)}
main{display:grid;grid-template-columns:minmax(0,1fr) 320px;gap:14px;padding:14px}
@media(max-width:1000px){main{grid-template-columns:1fr}}
.card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:12px}
.wrap{overflow-x:auto}
#stage{position:relative;width:880px;height:410px;margin:auto}
.zone{position:absolute;color:var(--mut);font:11px ui-monospace,monospace;text-transform:uppercase;letter-spacing:1px}
#chamber{position:absolute;left:330px;top:30px;width:430px;height:370px;border:2px dashed var(--line);border-radius:18px;transition:.4s}
#chamber.readers{border:2px solid var(--read);box-shadow:0 0 28px #38d9c055 inset}
#chamber.writer{border:2px solid var(--write);box-shadow:0 0 28px #ff4d6d66 inset}
#gate{position:absolute;left:236px;top:30px;width:84px;height:370px;border:1px dotted var(--line);border-radius:10px}
#lamp{position:absolute;left:350px;top:40px;display:flex;gap:8px;align-items:center;font:12px ui-monospace,monospace}
#lamp i{width:18px;height:18px;border-radius:50%;background:#3ddc84;box-shadow:0 0 12px #3ddc84;transition:.3s}
#lamp.locked i{background:var(--acq);box-shadow:0 0 12px var(--acq)}#lamp.wlocked i{background:var(--write);box-shadow:0 0 12px var(--write)}
.chip{position:absolute;width:84px;height:38px;display:flex;flex-direction:column;justify-content:center;align-items:center;border:2px solid var(--wait);background:var(--panel);transition:left .6s,top .6s,opacity .4s,border-color .3s,box-shadow .3s;line-height:1.1}
.chip.reader{border-radius:19px}.chip.writer{border-radius:6px}.chip i{font:10px ui-monospace,monospace;font-style:normal;color:var(--mut)}
.chip.idle{opacity:0}.chip.waiting{border-color:var(--wait)}.chip.acquiring{border-color:var(--acq);box-shadow:0 0 12px var(--acq);animation:pulse .8s infinite alternate}
.chip.reading{border-color:var(--read);box-shadow:0 0 10px var(--read)}.chip.writing{border-color:var(--write);box-shadow:0 0 14px var(--write)}
.chip.releasing{border-color:var(--rel);box-shadow:0 0 10px var(--rel)}.chip.done{opacity:.35}.chip.sel{outline:2px solid var(--tx)}
@keyframes pulse{to{transform:scale(1.08)}}
.stats{display:grid;grid-template-columns:1fr 1fr;gap:8px}.stat{background:var(--bg);border-radius:8px;padding:8px}
.stat b{display:block;font-size:22px}.stat span{color:var(--mut);font-size:11px}
#controls{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-top:10px}
button,select{background:var(--bg);color:var(--tx);border:1px solid var(--line);border-radius:8px;padding:7px 12px;font:inherit;cursor:pointer}
button:hover{border-color:var(--read)}
#tl{list-style:none;margin:8px 0 0;padding:0;max-height:210px;overflow:auto;font:12px ui-monospace,monospace}
#tl li{padding:3px 8px;border-left:4px solid var(--line);cursor:pointer;opacity:.45}#tl li.reader{border-color:var(--read)}#tl li.writer{border-color:var(--write)}
#tl li.past{opacity:1}#tl li.cur{background:var(--bg);outline:1px solid var(--tx);opacity:1}
#details p{margin:4px 0}.k{color:var(--mut)}.legend span{display:inline-block;margin:2px 8px 2px 0;font-size:12px}
.legend i{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:4px}
h3{margin:0 0 8px;font-size:13px;text-transform:uppercase;letter-spacing:1px;color:var(--mut)}
</style></head><body>
<header><h1>Resource Control Room</h1><span>Who Gets Access? Replay of a real C++ semaphore run (reader-preference)</span></header>
<main>
<div>
 <div class="card wrap"><div id="stage">
  <div class="zone" style="left:24px;top:8px">Lobby: readers | writers</div>
  <div class="zone" style="left:236px;top:8px">Gate</div>
  <div class="zone" style="left:340px;top:8px">Shared resource: sharedData</div>
  <div class="zone" style="left:790px;top:8px">Exited</div>
  <div id="gate"></div><div id="chamber"></div>
  <div id="lamp"><i></i><span id="lamptxt"></span></div>
 </div>
 <div id="controls">
  <button id="play">&#9654; Play</button><button id="pause">&#10074;&#10074; Pause</button>
  <button id="step">Step &#9654;|</button><button id="reset">&#8634; Reset</button>
  <label>Speed <select id="speed"><option value="0.5">0.5x</option><option value="1" selected>1x</option><option value="2">2x</option><option value="4">4x</option></select></label>
  <span class="k" id="pos"></span>
 </div>
 <div class="legend" style="margin-top:8px"><span><i style="background:var(--wait)"></i>Waiting</span><span><i style="background:var(--acq)"></i>Acquiring</span><span><i style="background:var(--read)"></i>Reading</span><span><i style="background:var(--write)"></i>Writing</span><span><i style="background:var(--rel)"></i>Releasing</span><span class="k">round = reader, square = writer</span></div>
 </div>
 <div class="card" style="margin-top:14px"><h3>Live event timeline (click to jump)</h3><ul id="tl"></ul></div>
</div>
<aside>
 <div class="card"><h3>Control panel</h3><div class="stats">
  <div class="stat"><b id="sReaders">0</b><span>Active readers</span></div>
  <div class="stat"><b id="sSem">1</b><span>resourceSem value</span></div>
  <div class="stat"><b id="sWW">0</b><span>Writers waiting</span></div>
  <div class="stat"><b id="sMax">0</b><span>Max concurrent readers (whole run)</span></div></div>
  <p><span class="k">Resource status:</span> <b id="sStatus"></b></p><p class="k" id="verdict"></p></div>
 <div class="card" id="details" style="margin-top:14px"><h3>Selected event</h3><div id="dbody"></div></div>
</aside></main>
<script>
)HTML";

static const char* kHtmlTail = R"HTML(
const E=DATA.events,SUM=DATA.summary,$=id=>document.getElementById(id);
const names=[...new Set(E.map(e=>e.thread))];
const role=n=>n[0]==='R'?'reader':'writer';
const MAP={R_REQUEST:'waiting',R_FIRST_ACQUIRING:'acquiring',R_FIRST_ACQUIRED:'reading',R_JOIN:'reading',R_LEAVE:'releasing',R_LAST_RELEASE:'releasing',R_DONE:'done',W_REQUEST:'waiting',W_ACQUIRED:'writing',W_RELEASE:'releasing',W_DONE:'done'};
const LABEL={idle:'',waiting:'waiting',acquiring:'acquiring',reading:'reading',writing:'writing',releasing:'releasing',done:'done'};
let idx=-1,timer=null;
names.forEach(n=>{const d=document.createElement('div');d.className='chip idle '+role(n);d.id='c_'+n;d.innerHTML='<b>'+n+'</b><i></i>';$('stage').appendChild(d);});
E.forEach((e,i)=>{const li=document.createElement('li');li.id='e'+i;li.className=role(e.thread);li.innerHTML='#'+e.seq+' <b>'+e.thread+'</b> '+e.type;li.onclick=()=>{pause();go(i);};$('tl').appendChild(li);});
$('verdict').textContent='C++ trace check: '+(SUM.traceOk?'PASS':'FAIL')+' | runtime violations: '+SUM.runtimeViolations+' | events: '+E.length+' | seed '+SUM.seed;
$('sMax').textContent=SUM.maxReaders;
function compute(n){const st={};names.forEach(x=>st[x]='idle');let owner=null;
 for(let i=0;i<=n;i++){const e=E[i];if(MAP[e.type])st[e.thread]=MAP[e.type];
  if(e.type==='R_FIRST_ACQUIRED')owner='readers';if(e.type==='W_ACQUIRED')owner='writer';
  if(e.type==='R_LAST_RELEASE'||e.type==='W_RELEASE')owner=null;}
 return{st,owner};}
function render(){
 const{st,owner}=compute(idx);const L={rw:[],ww:[],gate:[],rd:[],wr:[],done:[]};
 names.forEach(n=>{const s=st[n],r=role(n);
  if(s==='waiting')(r==='reader'?L.rw:L.ww).push(n);else if(s==='acquiring'||s==='releasing')L.gate.push(n);
  else if(s==='reading')L.rd.push(n);else if(s==='writing')L.wr.push(n);else if(s==='done')L.done.push(n);});
 const pos={};
 L.rw.forEach((n,i)=>pos[n]=[24,40+i*50]);L.ww.forEach((n,i)=>pos[n]=[130,40+i*50]);
 L.gate.forEach((n,i)=>pos[n]=[238,90+i*50]);
 L.rd.forEach((n,i)=>pos[n]=[350+(i%3)*132,120+Math.floor(i/3)*56]);
 L.wr.forEach(n=>pos[n]=[480,200]);L.done.forEach((n,i)=>pos[n]=[782,40+i*48]);
 names.forEach(n=>{const d=$('c_'+n),p=pos[n]||[24,-60];d.style.left=p[0]+'px';d.style.top=p[1]+'px';
  d.className='chip '+st[n]+' '+role(n)+(idx>=0&&E[idx].thread===n?' sel':'');d.lastChild.textContent=LABEL[st[n]];});
 $('chamber').className=owner==='readers'?'readers':owner==='writer'?'writer':'';
 $('lamp').className=owner==='readers'?'locked':owner==='writer'?'wlocked':'';
 const txt=owner==='readers'?'LOCKED (0): shared by reading group':owner==='writer'?'LOCKED (0): exclusive writer':'AVAILABLE (1)';
 $('lamptxt').textContent='resourceSem = '+txt;$('sStatus').textContent=txt.split(':')[0]+(owner?' by '+owner:'');
 $('sReaders').textContent=L.rd.length;$('sSem').textContent=owner?0:1;$('sWW').textContent=L.ww.length;
 $('pos').textContent='event '+(idx+1)+' / '+E.length;
 document.querySelectorAll('#tl li').forEach((li,i)=>{li.classList.toggle('past',i<=idx);li.classList.toggle('cur',i===idx);});
 if(idx>=0)$('e'+idx).scrollIntoView({block:'nearest'});
 const e=E[idx];$('dbody').innerHTML=idx<0?'<p class="k">Press Play or Step to replay the recorded execution.</p>':
  '<p><b>#'+e.seq+' '+e.type+'</b> by <b>'+e.thread+'</b></p><p>'+e.detail+'</p><p><span class="k">Semaphore operation:</span> '+e.op+
  '</p><p><span class="k">Real time:</span> '+(e.micros/1000).toFixed(2)+' ms after start</p><p><span class="k">readCount at this event:</span> '+(e.readCount<0?'not read here':e.readCount)+'</p>';
}
function go(n){idx=Math.max(-1,Math.min(E.length-1,n));render();}
function pause(){clearInterval(timer);timer=null;}
function play(){pause();if(idx>=E.length-1)go(-1);timer=setInterval(()=>{if(idx>=E.length-1){pause();return;}go(idx+1);},1000/parseFloat($('speed').value));}
$('play').onclick=play;$('pause').onclick=pause;$('step').onclick=()=>{pause();go(idx+1);};$('reset').onclick=()=>{pause();go(-1);};
$('speed').onchange=()=>{if(timer)play();};
render();
</script></body></html>
)HTML";

void writeHtml(const std::string& path, const std::vector<Event>& ev, const Verdict& v,
               int runtimeViolations, unsigned seed) {
    std::ostringstream js;
    js << "const DATA={summary:{maxReaders:" << v.maxReaders << ",traceOk:" << (v.ok ? "true" : "false")
       << ",runtimeViolations:" << runtimeViolations << ",seed:" << seed << "},events:[";
    for (size_t i = 0; i < ev.size(); ++i) {
        const Event& e = ev[i];
        js << (i ? "," : "") << "{seq:" << e.seq << ",micros:" << e.micros << ",thread:\"" << e.thread
           << "\",type:\"" << e.type << "\",readCount:" << e.readCount << ",op:\"" << jsonEscape(e.op)
           << "\",detail:\"" << jsonEscape(e.detail) << "\"}";
    }
    js << "]};";
    std::ofstream out(path);
    out << kHtmlHead << js.str() << kHtmlTail;
}

// ------------------------------------------------------------------ 5. main
struct Job { std::string name; bool isReader; int startDelayMs; int holdMs; };

int main(int argc, char** argv) {
    std::string outPath = argc > 1 ? argv[1] : "readers_writers_semaphore.html";
    unsigned seed = argc > 2 ? static_cast<unsigned>(std::stoul(argv[2])) : 0;
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int> jitter(0, 15);   // small timing variation per seed

    // Workload chosen to expose reader preference: R3 arrives while W1 is already
    // waiting, yet R3 joins the group and W1 is delayed.
    std::vector<Job> jobs = {
        {"R1", true, 0, 150},   {"R2", true, 30, 150},  {"W1", false, 50, 120},
        {"R3", true, 90, 150},  {"W2", false, 140, 120}, {"R4", true, 200, 100},
        {"R5", true, 230, 100},
    };

    EventLogger logger;
    ReaderPreferenceRW rw(logger);
    {
        std::vector<std::jthread> threads;   // jthread joins automatically at scope end
        for (const Job& j : jobs) {
            int delay = j.startDelayMs + (seed ? jitter(rng) : 0);
            threads.emplace_back([&rw, j, delay] {
                sleepMs(delay);
                if (j.isReader) rw.reader(j.name, j.holdMs); else rw.writer(j.name, j.holdMs);
            });
        }
    }   // all threads joined here

    std::vector<Event> events = logger.snapshot();
    Verdict v = validateTrace(events);
    writeHtml(outPath, events, v, rw.violations(), seed);

    std::cout << "events recorded      : " << events.size() << "\n"
              << "max concurrent readers: " << v.maxReaders << "\n"
              << "resource acquires/releases: " << v.resourceAcquires << "/" << v.resourceReleases << "\n"
              << "runtime violations   : " << rw.violations() << "\n"
              << "final sharedData     : " << rw.finalData() << " (expected 2 writers)\n"
              << "trace validation     : " << (v.ok ? "PASS" : "FAIL") << "\n";
    for (const auto& p : v.problems) std::cout << "  problem: " << p << "\n";
    std::cout << "HTML written to " << outPath << "\n";
    return (v.ok && rw.violations() == 0 && rw.finalData() == 2) ? 0 : 1;
}
