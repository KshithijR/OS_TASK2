g++ --version// Readers-Writers problem solved with a MONITOR (mutex + condition variables).
// Writer preference. Events are recorded from the REAL threads, then written
// into readers_writers_monitor.html as an embedded JSON trace.
// Build: g++ -std=c++20 -O2 -pthread main.cpp -o rw_monitor && ./rw_monitor
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
using Clock = std::chrono::steady_clock;
static void sleepMs(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

// ---------- Thread-safe event recorder ----------
struct Event {
    int seq; long long t_us;
    std::string type, role, who, cv, note;
    int readers, waitingWriters; bool writerActive;   // monitor state snapshot
};
class Recorder {
public:
    // The sequence number and timestamp are taken under the same lock, so
    // seq order == time order, even with many threads logging.
    void log(const std::string& type, const std::string& role, const std::string& who,
             const std::string& cv, const std::string& note, int r, int w, bool a) {
        std::lock_guard<std::mutex> g(m_);
        long long t = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - t0_).count();
        events_.push_back({(int)events_.size(), t, type, role, who, cv, note, r, w, a});
    }
    const std::vector<Event>& events() const { return events_; }  // read only after join
private:
    std::mutex m_; std::vector<Event> events_; Clock::time_point t0_ = Clock::now();
};

// ---------- The monitor ----------
// Shared state: activeReaders, waitingWriters, writerActive. Guarded by mutex_.
// Writer preference: a reader may enter only if NO writer is active AND NO writer is waiting.
class RWMonitor {
public:
    explicit RWMonitor(Recorder& rec) : rec_(rec) {}

    void startRead(int id) {
        std::unique_lock<std::mutex> lk(mutex_);
        const std::string who = "R" + std::to_string(id);
        note("REQUEST", "reader", who, "", "wants to read");
        // while-loop + wait == wait(lk, predicate): re-checks after every wake-up
        // (guards against spurious wake-ups and stolen wake-ups).
        while (writerActive_ || waitingWriters_ > 0) {
            note("CV_WAIT", "reader", who, "can_read", "blocked: writer active or queued");
            canRead_.wait(lk);                    // atomically unlocks mutex_ and sleeps
            note("CV_WAKE", "reader", who, "can_read", "woke up, re-checking predicate");
        }
        ++activeReaders_;
        note("ADMIT", "reader", who, "", "admitted: now reading");
    }
    void endRead(int id) {
        std::unique_lock<std::mutex> lk(mutex_);
        const std::string who = "R" + std::to_string(id);
        --activeReaders_;
        note("EXIT", "reader", who, "", "finished reading, left database");
        if (activeReaders_ == 0 && waitingWriters_ > 0) {   // last reader out wakes ONE writer
            note("NOTIFY", "reader", who, "can_write", "last reader out: notify_one writer");
            canWrite_.notify_one();
        }
    }
    void startWrite(int id) {
        std::unique_lock<std::mutex> lk(mutex_);
        const std::string who = "W" + std::to_string(id);
        note("REQUEST", "writer", who, "", "wants to write");
        ++waitingWriters_;                        // from now on NEW readers must wait
        note("QUEUE", "writer", who, "", "registered as waiting writer");
        while (writerActive_ || activeReaders_ > 0) {
            note("CV_WAIT", "writer", who, "can_write", "blocked: readers active or writer active");
            canWrite_.wait(lk);
            note("CV_WAKE", "writer", who, "can_write", "woke up, re-checking predicate");
        }
        --waitingWriters_;
        writerActive_ = true;                     // only now is the writer really in the CS
        note("ADMIT", "writer", who, "", "exclusive access granted: now writing");
    }
    void endWrite(int id) {
        std::unique_lock<std::mutex> lk(mutex_);
        const std::string who = "W" + std::to_string(id);
        writerActive_ = false;
        note("EXIT", "writer", who, "", "finished writing, left database");
        if (waitingWriters_ > 0) {                // writer preference: next writer first
            note("NOTIFY", "writer", who, "can_write", "notify_one: another writer is queued");
            canWrite_.notify_one();
        } else {
            note("NOTIFY", "writer", who, "can_read", "notify_all: wake every blocked reader");
            canRead_.notify_all();
        }
    }
private:
    void note(const char* type, const char* role, const std::string& who, const char* cv, const char* txt) {
        rec_.log(type, role, who, cv, txt, activeReaders_, waitingWriters_, writerActive_);  // call with mutex_ held
    }
    std::mutex mutex_;
    std::condition_variable canRead_, canWrite_;
    int activeReaders_ = 0, waitingWriters_ = 0; bool writerActive_ = false;
    Recorder& rec_;
};

// ---------- Shared resource + independent overlap detector ----------
struct Shared { int a = 0, b = 0; };   // invariant: a == b whenever no writer is mid-update
struct Probe { std::atomic<int> readers{0}, writers{0}, maxReaders{0}, violations{0}; };

static void readerTask(RWMonitor& m, Shared& d, Probe& p, int id, int delay, int work) {
    sleepMs(delay); m.startRead(id);
    int now = ++p.readers;
    if (p.writers.load() > 0) ++p.violations;               // reader overlapping a writer?
    int prev = p.maxReaders.load();
    while (now > prev && !p.maxReaders.compare_exchange_weak(prev, now)) {}
    if (d.a != d.b) ++p.violations;                          // torn read?
    sleepMs(work);
    --p.readers; m.endRead(id);
}
static void writerTask(RWMonitor& m, Shared& d, Probe& p, int id, int delay, int work) {
    sleepMs(delay); m.startWrite(id);
    if (p.writers.fetch_add(1) > 0 || p.readers.load() > 0) ++p.violations;  // exclusivity check
    ++d.a; sleepMs(10); ++d.b;                               // two-step update: breaks if anyone overlaps
    sleepMs(work);
    --p.writers; m.endWrite(id);
}

// ---------- Trace self-check: replay events and verify every invariant ----------
static bool verifyTrace(const std::vector<Event>& ev, int& maxOverlap) {
    int r = 0, w = 0; bool a = false; bool ok = true; maxOverlap = 0;
    auto bad = [&](const Event& e, const char* why) { std::cerr << "TRACE ERROR seq " << e.seq << ": " << why << "\n"; ok = false; };
    long long lastT = -1;
    for (size_t i = 0; i < ev.size(); ++i) {
        const Event& e = ev[i];
        if (e.seq != (int)i) bad(e, "sequence gap");
        if (e.t_us < lastT) bad(e, "time went backwards");
        lastT = e.t_us;
        bool rd = e.role == "reader";
        if (e.type == "QUEUE") ++w;
        else if (e.type == "ADMIT" && rd) { if (a || w > 0) bad(e, "reader admitted while writer active/waiting"); ++r; }
        else if (e.type == "ADMIT") { if (a || r > 0) bad(e, "writer admitted while not exclusive"); --w; a = true; }
        else if (e.type == "EXIT" && rd) { if (--r < 0) bad(e, "negative readers"); }
        else if (e.type == "EXIT") { if (!a) bad(e, "writer exit but none active"); a = false; }
        if (r != e.readers || w != e.waitingWriters || a != e.writerActive) bad(e, "snapshot != replayed state");
        if (a && r > 0) bad(e, "writer and reader active together");
        if (r > maxOverlap) maxOverlap = r;
    }
    return ok && r == 0 && w == 0 && !a;
}

static std::string toJson(const std::vector<Event>& ev) {
    std::ostringstream o; o << "[";
    for (size_t i = 0; i < ev.size(); ++i) {
        const Event& e = ev[i];
        o << (i ? ",\n" : "\n") << "{\"seq\":" << e.seq << ",\"t\":" << e.t_us << ",\"type\":\"" << e.type
          << "\",\"role\":\"" << e.role << "\",\"who\":\"" << e.who << "\",\"cv\":\"" << e.cv << "\",\"note\":\""
          << e.note << "\",\"r\":" << e.readers << ",\"w\":" << e.waitingWriters << ",\"a\":" << (e.writerActive ? "true" : "false") << "}";
    }
    o << "\n]"; return o.str();
}

// HTML template: __TRACE__ is replaced with the recorded events.
static const char* HTML_A = R"HTML(<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Shared Database: Inside the Monitor</title>
<style>
:root{--bg:#f6f3ee;--ink:#2b2b2b;--mut:#74706a;--line:#d9d3c9;--blue:#4a6fa5;--grn:#5b8a5f;--amb:#b8862f;--red:#a8514a}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif}
header{padding:16px 24px;border-bottom:1px solid var(--line)}h1{font-size:18px;margin:0;font-weight:600}header p{margin:2px 0 0;color:var(--mut);font-size:13px}
main{display:grid;grid-template-columns:1fr 310px;gap:20px;padding:20px 24px;max-width:1150px;margin:auto}
@media(max-width:860px){main{grid-template-columns:1fr}}
#stage{position:relative;height:330px;border:1px solid var(--line);border-radius:6px;background:#fbf9f5;overflow:hidden}
.zone{position:absolute;top:10px;bottom:10px;border:1px dashed var(--line);border-radius:6px}
.zone b,.sub{position:absolute;left:8px;font-size:11px;font-weight:500;color:var(--mut);letter-spacing:.04em;text-transform:uppercase}
#zdb{border:2px solid var(--blue);transition:border-color .3s,background .3s}#zdb.shared{background:#eef2f8}#zdb.excl{border-color:var(--red);background:#f6e9e7}
.chip{position:absolute;width:50px;height:30px;border-radius:4px;display:flex;align-items:center;justify-content:center;font:600 12px ui-monospace,Menlo,monospace;border:1.5px solid;background:#fff;transition:left .5s,top .5s,opacity .4s,background .3s,box-shadow .3s}
.chip.reader{color:var(--blue);border-color:var(--blue)}.chip.writer{color:var(--ink);border-color:var(--ink)}
.chip.req,.chip.wait{border-style:dashed}.chip.wait{background:#f3e9d3}
.chip.in.reader{background:#dfe9dd;border-color:var(--grn);color:#2f5a33}.chip.in.writer{background:#f1dcda;border-color:var(--red);color:#7a2f29}
.chip.done{opacity:.35}.chip.hl{box-shadow:0 0 0 3px rgba(74,111,165,.4)}.chip.hl2{box-shadow:0 0 0 2px rgba(184,134,47,.55)}
.panel{border-top:1px solid var(--line);padding:12px 0}.panel h2{font-size:11px;letter-spacing:.05em;text-transform:uppercase;color:var(--mut);margin:0 0 8px;font-weight:600}
dl{display:grid;grid-template-columns:auto 1fr;gap:4px 12px;margin:0}dt{color:var(--mut)}dd{margin:0;font:13px ui-monospace,Menlo,monospace;text-align:right}
.ctl{display:flex;gap:8px;align-items:center;margin-top:14px;flex-wrap:wrap}
button,select{font:inherit;padding:5px 12px;border:1px solid var(--line);background:#fff;color:var(--ink);border-radius:4px;cursor:pointer}button:hover{border-color:var(--blue)}
#tl{margin-top:12px;max-height:210px;overflow:auto;border:1px solid var(--line);border-radius:6px;background:#fbf9f5}
.row{display:grid;grid-template-columns:38px 62px 44px 1fr;padding:3px 10px;cursor:pointer;font:12px ui-monospace,Menlo,monospace;border-bottom:1px solid #eee9e0}
.row:hover{background:#f1ede5}.row.cur{background:#e3eaf3}.row.fut{color:#aaa49a}
.ok{color:var(--grn)}.no{color:var(--red)}#det{font-size:13px;min-height:96px}#det .t{font-weight:600}.mut{color:var(--mut)}
</style></head><body>
<header><h1>Shared Database: Inside the Monitor</h1><p>Replay of an actual C++ execution: <span id="cnt"></span> recorded events. Steps are evenly spaced; real timestamps appear in the details panel.</p></header>
<main><section>
<div id="stage"><div class="zone" id="zw" style="left:1%;width:31%"><b>Blocked / queued</b><span class="sub" style="top:34%">can_read &uarr; &nbsp; can_write &darr;</span></div>
<div class="zone" id="zdb" style="left:34%;width:44%"><b id="dbl">Database &middot; idle</b></div>
<div class="zone" id="zd" style="left:81%;width:18%"><b>Departed</b></div></div>
<div class="ctl"><button id="bPlay">Play</button><button id="bPause">Pause</button><button id="bBack">&#9664; Step</button><button id="bFwd">Step &#9654;</button><button id="bReset">Reset</button>
<label class="mut">Speed <select id="sp"><option value="0.5">0.5x</option><option value="1" selected>1x</option><option value="2">2x</option><option value="4">4x</option></select></label></div>
<div id="tl"></div></section>
<aside><div class="panel" style="border:0;padding-top:0"><h2>Monitor state (recorded)</h2><dl>
<dt>activeReaders</dt><dd id="sr">0</dd><dt>waitingWriters</dt><dd id="sw">0</dd><dt>writerActive</dt><dd id="sa">false</dd>
<dt>reader may enter</dt><dd id="pr"></dd><dt>writer may enter</dt><dd id="pw"></dd></dl></div>
<div class="panel"><h2>Event details</h2><div id="det" class="mut">Press Play or Step. Click any timeline row to jump to that state.</div></div>
<div class="panel"><h2>Legend</h2><div class="mut" style="font-size:12px">Dashed = waiting. Blue ring = acting thread. Amber ring = threads a notification can wake. A notification is <b>not</b> admission: a thread is shown inside only after its ADMIT event.</div></div></aside></main>
<script>
const EV=__TRACE__;
)HTML";
static const char* HTML_B = R"HTML(
const $=i=>document.getElementById(i),stage=$('stage');
const HUM={REQUEST:'requests access',QUEUE:'queued as waiting writer',CV_WAIT:'blocks on condition variable',CV_WAKE:'wakes, re-checks predicate',NOTIFY:'notifies (not an admission)',ADMIT:'admitted',EXIT:'leaves'};
const labels=[...new Set(EV.map(e=>e.who))],role=l=>EV.find(e=>e.who===l).role,chips={};
labels.forEach(l=>{const d=document.createElement('div');d.className='chip '+role(l);d.textContent=l;d.style.opacity=0;stage.appendChild(d);chips[l]=d});
$('cnt').textContent=EV.length;
let idx=-1,timer=null;
const rows=EV.map((e,k)=>{const r=document.createElement('div');r.className='row';r.innerHTML=`<span>#${e.seq}</span><span>${(e.t/1000).toFixed(1)}ms</span><span>${e.who}</span><span>${e.type}${e.cv?' '+e.cv:''}</span>`;r.onclick=()=>{go(k)};$('tl').appendChild(r);return r});
function render(){
 const st={},ord={};
 for(let k=0;k<=idx;k++){const e=EV[k];
  if(e.type==='REQUEST'){st[e.who]='req';ord[e.who]=k}
  else if(e.type==='QUEUE'||e.type==='CV_WAIT')st[e.who]='wait';
  else if(e.type==='ADMIT')st[e.who]='in';else if(e.type==='EXIT')st[e.who]='done'}
 const W=stage.clientWidth,zx=.01*W,dx=.34*W,dw=.44*W,ex=.81*W,n={wr:0,ww:0,in:0,dn:0};
 labels.forEach(l=>{chips[l].style.opacity=st[l]?(st[l]==='done'?.35:1):0;chips[l].className='chip '+role(l)+' '+(st[l]||'')});
 labels.filter(l=>st[l]).sort((a,b)=>ord[a]-ord[b]).forEach(l=>{const s=st[l],w=role(l)==='writer';let x,y,i;
  if(s==='done'){i=n.dn++;x=ex+10+(i%2)*56;y=36+Math.floor(i/2)*38}
  else if(s==='in'){if(w){x=dx+dw/2-25;y=150}else{i=n.in++;x=dx+14+(i%4)*58;y=50+Math.floor(i/4)*40}}
  else if(w){i=n.ww++;x=zx+10+(i%3)*58;y=205+Math.floor(i/3)*38}
  else{i=n.wr++;x=zx+10+(i%3)*58;y=40+Math.floor(i/3)*38}
  chips[l].style.left=x+'px';chips[l].style.top=y+'px'});
 const e=EV[idx]||{r:0,w:0,a:false};
 if(EV[idx]){chips[e.who].classList.add('hl');
  if(e.type==='NOTIFY')labels.forEach(l=>{if(st[l]==='wait'&&((e.cv==='can_write')===(role(l)==='writer')))chips[l].classList.add('hl2')})}
 $('sr').textContent=e.r;$('sw').textContent=e.w;$('sa').textContent=e.a;
 const rp=!e.a&&e.w===0,wp=!e.a&&e.r===0;
 $('pr').textContent=rp?'yes':'no';$('pr').className=rp?'ok':'no';$('pw').textContent=wp?'yes':'no';$('pw').className=wp?'ok':'no';
 $('zdb').className='zone '+(e.a?'excl':e.r>0?'shared':'');
 $('dbl').textContent='Database \u00b7 '+(e.a?'exclusive write':e.r>0?e.r+' reading (shared)':'idle');
 $('det').innerHTML=EV[idx]?`<div class="t">#${e.seq} ${e.who} ${HUM[e.type]}</div><div>${e.note}</div><div class="mut">${e.cv?'condition variable: '+e.cv+'<br>':''}recorded at ${(e.t/1000).toFixed(2)} ms<br>state after event: readers=${e.r}, waitingWriters=${e.w}, writerActive=${e.a}</div>`:'Initial state: monitor idle.';
 rows.forEach((r,k)=>{r.classList.toggle('cur',k===idx);r.classList.toggle('fut',k>idx)});
 if(rows[idx])rows[idx].scrollIntoView({block:'nearest'});
}
function go(k){idx=Math.max(-1,Math.min(EV.length-1,k));render()}
function stop(){clearInterval(timer);timer=null}
function play(){stop();if(idx>=EV.length-1)idx=-1;timer=setInterval(()=>{if(idx>=EV.length-1)return stop();go(idx+1)},900/parseFloat($('sp').value))}
$('bPlay').onclick=play;$('bPause').onclick=stop;$('bFwd').onclick=()=>{stop();go(idx+1)};$('bBack').onclick=()=>{stop();go(idx-1)};
$('bReset').onclick=()=>{stop();go(-1)};$('sp').onchange=()=>{if(timer)play()};addEventListener('resize',render);render();
</script></body></html>
)HTML";

int main(int argc, char** argv) {
    const std::string outFile = argc > 1 ? argv[1] : "readers_writers_monitor.html";
    Recorder rec; RWMonitor mon(rec); Shared data; Probe probe;
    struct Job { char kind; int id, delayMs, workMs; };
    // Start delays only stagger the arrivals; the ORDER of events is decided by the real scheduler/monitor.
    const std::vector<Job> jobs = {{'R',1,0,400},{'R',2,0,400},{'R',3,0,400},{'W',1,100,300},
                                   {'R',4,200,300},{'W',2,250,250},{'R',5,500,300},{'R',6,900,300}};
    std::vector<std::thread> ts;
    for (const Job& j : jobs) {
        if (j.kind == 'R') ts.emplace_back(readerTask, std::ref(mon), std::ref(data), std::ref(probe), j.id, j.delayMs, j.workMs);
        else               ts.emplace_back(writerTask, std::ref(mon), std::ref(data), std::ref(probe), j.id, j.delayMs, j.workMs);
    }
    for (auto& t : ts) t.join();

    int maxOverlap = 0;
    const bool traceOk = verifyTrace(rec.events(), maxOverlap);
    std::ofstream f(outFile);
    if (!f) { std::cerr << "cannot write " << outFile << "\n"; return 2; }
    std::string a = HTML_A, key = "__TRACE__";
    a.replace(a.find(key), key.size(), toJson(rec.events()));
    f << a << HTML_B; f.close();

    std::cout << "events recorded        : " << rec.events().size() << "\n"
              << "max overlapping readers: " << probe.maxReaders << " (observed by threads)\n"
              << "max readers in trace   : " << maxOverlap << "\n"
              << "overlap/torn violations: " << probe.violations << "\n"
              << "final a,b              : " << data.a << "," << data.b << "\n"
              << "trace consistent       : " << (traceOk ? "yes" : "NO") << "\n"
              << "wrote " << outFile << "\n";
    return (traceOk && probe.violations == 0 && data.a == data.b && data.a == 2) ? 0 : 1;
}
