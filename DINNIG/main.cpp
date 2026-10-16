// =============================================================================
//  Dining Philosophers with semaphores  --  C++20
//  Four philosopher threads, four binary fork semaphores, one room semaphore (3).
//  The program records a real execution trace, validates it, and writes a
//  self-contained HTML replay ("The Living Table").
//
//  Build : g++ -std=c++20 -O2 -pthread -Wall -Wextra main.cpp -o dining
//  Run   : ./dining [rounds=3] [output.html] [seed]      |   ./dining --selftest
//
//  SEMAPHORES (what they are for, and why that initial value)
//  ---------------------------------------------------------------------------
//  fork[0..3]  std::binary_semaphore, initial 1
//      One per fork. 1 = fork lying on the table, 0 = somebody holds it.
//      acquire() is the "pick up" operation (blocks if taken), release() is
//      "put down". Because the value can only be 0 or 1, at most one
//      philosopher can own a fork  ->  MUTUAL EXCLUSION.
//  room        std::counting_semaphore<3>, initial 3
//      Counts how many philosophers may compete for forks at once. With 4
//      philosophers and 4 forks, deadlock needs all four to hold exactly one
//      fork and wait for the next (circular wait). Admitting only 3 means at
//      least one philosopher is outside, so at most 3 forks can be "one-in-
//      hand" while a 4th fork stays free -> someone can always finish.
//      This is DEADLOCK PREVENTION (it breaks the circular-wait condition).
//
//  STARVATION: the room semaphore guarantees progress of the system, not
//  fairness to each thread. std::counting_semaphore does not promise FIFO
//  wake-up, so an unlucky philosopher could in theory lose every race.
//  Deadlock-free != starvation-free. (A FIFO/ticket queue would fix that.)
//
//  FORK NUMBERING (matches the animation): Fork i lies between philosopher i
//  and i+1.  So philosopher i uses left fork (i+3)%4 and right fork i.
// =============================================================================
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <random>
#include <semaphore>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

constexpr int N = 4;            // philosophers == forks
constexpr int ROOM_PERMITS = 3; // N - 1

// ----------------------------------------------------------------------------
// 1. Event schema
// ----------------------------------------------------------------------------
enum class Ev { Think, Hungry, RoomTry, RoomOk, ForkReq, ForkOk,
                EatStart, EatEnd, ForkRel, RoomRel, Done };

static const char* evName(Ev e) {
    static const char* n[] = {"THINK", "HUNGRY", "ROOM_TRY", "ROOM_OK", "FORK_REQ", "FORK_OK",
                              "EAT_START", "EAT_END", "FORK_REL", "ROOM_REL", "DONE"};
    return n[static_cast<int>(e)];
}

struct Event {
    long seq;        // global order, assigned under the recorder's mutex
    long long us;    // microseconds since program start
    int phil;        // which philosopher (thread)
    Ev type;
    int fork;        // fork index for FORK_* events, otherwise -1
};

// ----------------------------------------------------------------------------
// 2. Thread-safe recorder. seq and timestamp are taken INSIDE the lock, so the
//    log order is a single total order that all threads agree on.
// ----------------------------------------------------------------------------
class Recorder {
    std::mutex m_;
    std::vector<Event> ev_;
    std::chrono::steady_clock::time_point t0_ = std::chrono::steady_clock::now();
public:
    void log(int phil, Ev type, int fork = -1) {
        std::lock_guard<std::mutex> lk(m_);
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t0_).count();
        ev_.push_back({static_cast<long>(ev_.size()), us, phil, type, fork});
    }
    std::vector<Event> take() { std::lock_guard<std::mutex> lk(m_); return ev_; }
};

// ----------------------------------------------------------------------------
// 3. The shared table: the semaphores
// ----------------------------------------------------------------------------
struct Table {
    std::counting_semaphore<ROOM_PERMITS> room{ROOM_PERMITS};
    std::array<std::binary_semaphore, N> fork{{std::binary_semaphore{1}, std::binary_semaphore{1},
                                               std::binary_semaphore{1}, std::binary_semaphore{1}}};
};

// ----------------------------------------------------------------------------
// 4. Philosopher thread
//    Logging rule that keeps the trace truthful:
//      * ACQUIRE: log the request BEFORE acquire(), log "OK" AFTER it returns
//        (ownership is recorded only once the semaphore really granted it).
//      * RELEASE: log BEFORE release(), so no other thread can acquire the
//        resource (and log OK) ahead of our release entry.
//    Hence the logged ownership intervals are never longer than the real ones,
//    and an invariant that holds in reality can never look violated.
// ----------------------------------------------------------------------------
static void philosopher(int id, int rounds, unsigned seed, Table& t, Recorder& rec) {
    std::mt19937 rng(seed + 7919u * static_cast<unsigned>(id));
    auto nap = [&](int lo, int hi) {
        std::this_thread::sleep_for(std::chrono::milliseconds(std::uniform_int_distribution<int>(lo, hi)(rng)));
    };
    const int left = (id + N - 1) % N, right = id;

    for (int r = 0; r < rounds; ++r) {
        rec.log(id, Ev::Think);   nap(10, 40);
        rec.log(id, Ev::Hungry);

        rec.log(id, Ev::RoomTry);                  // attempt, may block
        t.room.acquire();                          // enter the room (permit 1 of 3)
        rec.log(id, Ev::RoomOk);                   // success

        rec.log(id, Ev::ForkReq, left);
        t.fork[left].acquire();
        rec.log(id, Ev::ForkOk, left);
        nap(2, 8);                                 // holding one fork: widens the race window

        rec.log(id, Ev::ForkReq, right);
        t.fork[right].acquire();
        rec.log(id, Ev::ForkOk, right);

        rec.log(id, Ev::EatStart);  nap(20, 50);   // critical section: both forks held
        rec.log(id, Ev::EatEnd);

        rec.log(id, Ev::ForkRel, left);   t.fork[left].release();
        rec.log(id, Ev::ForkRel, right);  t.fork[right].release();
        rec.log(id, Ev::RoomRel);         t.room.release();
    }
    rec.log(id, Ev::Done);
}

// ----------------------------------------------------------------------------
// 5. Trace validator -- replays the log and checks every invariant.
//    It only READS the trace; it never edits or hides events.
// ----------------------------------------------------------------------------
struct Check { std::string name; std::vector<std::string> bad; };

static unsigned allowedNext(Ev t) {   // legal successor events for one philosopher
    auto b = [](Ev x) { return 1u << static_cast<int>(x); };
    switch (t) {
        case Ev::Think:    return b(Ev::Hungry);
        case Ev::Hungry:   return b(Ev::RoomTry);
        case Ev::RoomTry:  return b(Ev::RoomOk);
        case Ev::RoomOk:   return b(Ev::ForkReq);
        case Ev::ForkReq:  return b(Ev::ForkOk);
        case Ev::ForkOk:   return b(Ev::ForkReq) | b(Ev::EatStart);
        case Ev::EatStart: return b(Ev::EatEnd);
        case Ev::EatEnd:   return b(Ev::ForkRel);
        case Ev::ForkRel:  return b(Ev::ForkRel) | b(Ev::RoomRel);
        case Ev::RoomRel:  return b(Ev::Think) | b(Ev::Done);
        case Ev::Done:     return 0;
    }
    return 0;
}

static std::vector<Check> validate(const std::vector<Event>& ev) {
    std::vector<Check> c = {
        {"Exactly 4 philosophers and 4 forks exist (all ids in range)", {}},
        {"A fork is never owned by two philosophers at once", {}},
        {"Adjacent philosophers never eat simultaneously", {}},
        {"Eating only after acquiring both adjacent forks", {}},
        {"Every acquisition has a matching release", {}},
        {"Room never admits more than 3 contenders", {}},
        {"All 4 threads terminate (DONE recorded)", {}},
        {"Each philosopher follows the legal state order", {}}};
    auto bad = [&](int i, const Event& e, const std::string& msg) {
        c[i].bad.push_back("#" + std::to_string(e.seq) + " P" + std::to_string(e.phil) + ": " + msg);
    };
    static_assert(N == 4 && ROOM_PERMITS == 3, "assignment fixes 4 philosophers and 3 room permits");

    int owner[N]; std::fill(owner, owner + N, -1);
    bool eating[N] = {}, inRoom[N] = {}, done[N] = {};
    int last[N]; std::fill(last, last + N, -1);
    int inside = 0;

    for (const Event& e : ev) {
        if (e.phil < 0 || e.phil >= N) { bad(0, e, "philosopher id out of range"); continue; }
        const int p = e.phil, f = e.fork, left = (p + N - 1) % N, right = p;
        const bool needsFork = e.type == Ev::ForkReq || e.type == Ev::ForkOk || e.type == Ev::ForkRel;
        if (needsFork && (f < 0 || f >= N)) { bad(0, e, "fork id out of range"); continue; }

        // legal order per philosopher
        bool legal = last[p] < 0 ? e.type == Ev::Think
                                 : (allowedNext(static_cast<Ev>(last[p])) & (1u << static_cast<int>(e.type))) != 0;
        if (!legal) bad(7, e, std::string("unexpected ") + evName(e.type));
        last[p] = static_cast<int>(e.type);

        switch (e.type) {
            case Ev::RoomOk:
                inRoom[p] = true;
                if (++inside > ROOM_PERMITS) bad(5, e, std::to_string(inside) + " philosophers inside the room");
                break;
            case Ev::RoomRel:
                if (!inRoom[p]) bad(4, e, "released a room permit it never acquired");
                else { inRoom[p] = false; --inside; }
                break;
            case Ev::ForkReq:
                if (!inRoom[p]) bad(5, e, "requested a fork without a room permit");
                break;
            case Ev::ForkOk:
                if (owner[f] != -1) bad(1, e, "Fork " + std::to_string(f) + " already owned by P" + std::to_string(owner[f]));
                if (f != left && f != right) bad(3, e, "acquired non-adjacent Fork " + std::to_string(f));
                owner[f] = p;
                break;
            case Ev::EatStart:
                if (owner[left] != p || owner[right] != p) bad(3, e, "started eating without both adjacent forks");
                if (eating[(p + 1) % N] || eating[(p + N - 1) % N]) bad(2, e, "a neighbour is already eating");
                eating[p] = true;
                break;
            case Ev::EatEnd:
                eating[p] = false;
                break;
            case Ev::ForkRel:
                if (owner[f] != p) bad(4, e, "released Fork " + std::to_string(f) + " it does not own");
                else owner[f] = -1;
                break;
            case Ev::Done:
                done[p] = true;
                break;
            default: break;
        }
    }
    Event end{static_cast<long>(ev.size()), 0, 0, Ev::Done, -1};
    for (int f = 0; f < N; ++f)
        if (owner[f] != -1) { end.phil = owner[f]; bad(4, end, "Fork " + std::to_string(f) + " never released"); }
    if (inside != 0) bad(4, end, std::to_string(inside) + " room permit(s) never released");
    for (int p = 0; p < N; ++p)
        if (!done[p]) { end.phil = p; bad(6, end, "thread did not finish"); }
    return c;
}

// --selftest: feeds the validator a deliberately corrupted trace and proves it notices.
static int selfTest() {
    std::vector<Event> ev;
    auto add = [&](int p, Ev t, int f = -1) { ev.push_back({static_cast<long>(ev.size()), 0, p, t, f}); };
    for (int p = 0; p < 4; ++p) { add(p, Ev::Think); add(p, Ev::Hungry); add(p, Ev::RoomTry); add(p, Ev::RoomOk); }
    add(0, Ev::ForkReq, 3); add(0, Ev::ForkOk, 3); add(0, Ev::ForkReq, 0); add(0, Ev::ForkOk, 0);
    add(1, Ev::ForkReq, 0); add(1, Ev::ForkOk, 0);          // fork 0 given to a second owner
    add(1, Ev::ForkReq, 1); add(1, Ev::ForkOk, 1);
    add(0, Ev::EatStart); add(1, Ev::EatStart);             // adjacent philosophers both eat
    add(3, Ev::EatStart);                                   // illegal order / no forks
    int flagged = 0;
    for (const Check& k : validate(ev)) {
        std::cout << (k.bad.empty() ? "  not triggered : " : "  DETECTED      : ") << k.name << "\n";
        flagged += !k.bad.empty();
    }
    std::cout << "selftest: validator flagged " << flagged << " of 8 checks on the corrupted trace\n";
    return flagged >= 7 ? 0 : 1;
}

// ----------------------------------------------------------------------------
// 6. JSON + HTML generation
// ----------------------------------------------------------------------------
static std::string jsonEsc(const std::string& s) {
    std::string o;
    for (char ch : s) { if (ch == '"' || ch == '\\') o += '\\'; o += ch; }
    return o;
}

static std::string buildJson(const std::vector<Event>& ev, const std::vector<Check>& checks, int rounds, unsigned seed) {
    std::ostringstream j;
    j << "{\"n\":" << N << ",\"room\":" << ROOM_PERMITS << ",\"rounds\":" << rounds << ",\"seed\":" << seed << ",\"events\":[";
    for (size_t i = 0; i < ev.size(); ++i) {
        const Event& e = ev[i];
        j << (i ? "," : "") << "{\"s\":" << e.seq << ",\"t\":" << e.us << ",\"p\":" << e.phil
          << ",\"k\":\"" << evName(e.type) << "\",\"f\":" << e.fork << "}";
    }
    j << "],\"checks\":[";
    for (size_t i = 0; i < checks.size(); ++i) {
        j << (i ? "," : "") << "{\"name\":\"" << jsonEsc(checks[i].name) << "\",\"bad\":[";
        for (size_t k = 0; k < checks[i].bad.size(); ++k)
            j << (k ? "," : "") << "\"" << jsonEsc(checks[i].bad[k]) << "\"";
        j << "]}";
    }
    j << "]}";
    return j.str();
}

// The page is a fixed template; the only dynamic part is the recorded trace
// that replaces the token __TRACE_JSON__ below.
static const char* kTemplate = R"HTML(<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>The Living Table - Dining Philosophers with Semaphores</title>
<style>
:root{--ivory:#F6F1E7;--navy:#18283D;--teal:#4E9B8A;--coral:#D97767;--gold:#E7BD65;--ink:#263238;--paper:#FBF8F1;--line:#DDD3BF;--mute:#6B746F}
*{box-sizing:border-box}
body{margin:0;background:var(--ivory);color:var(--ink);font:15px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
.wrap{max-width:1120px;margin:0 auto;padding:22px 16px 44px}
h1{font:600 28px/1.2 Georgia,"Times New Roman",serif;margin:0;letter-spacing:.2px}
.sub{color:var(--mute);margin:6px 0 20px;max-width:720px}
h2{font:600 11px/1 system-ui,sans-serif;letter-spacing:1.4px;text-transform:uppercase;color:var(--mute);margin:0 0 10px}
.grid{display:grid;grid-template-columns:minmax(0,1.3fr) minmax(0,1fr);gap:20px;align-items:start}
@media(max-width:840px){.grid{grid-template-columns:1fr}}
.card{background:var(--paper);border:1px solid var(--line);border-radius:10px;padding:16px}
.stage{padding:6px}
svg{display:block;width:100%;height:auto}
.fork{transition:transform .55s cubic-bezier(.4,0,.2,1)}
.phil circle.body{transition:fill .35s}
.ring,.link{transition:opacity .3s}
.controls{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:14px 0 8px}
button,select{font:inherit;font-size:14px;color:var(--navy);background:var(--paper);border:1px solid var(--navy);border-radius:6px;padding:7px 13px;cursor:pointer}
button:hover{background:#EFE8D8}
button.primary{background:var(--navy);color:var(--ivory);min-width:76px}
button:focus-visible,select:focus-visible,input:focus-visible{outline:2px solid var(--gold);outline-offset:2px}
.spacer{flex:1}
.prog{display:flex;align-items:center;gap:12px;font-size:13px;color:var(--mute)}
input[type=range]{flex:1;accent-color:var(--navy)}
.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:16px}
.stat{border:1px solid var(--line);border-radius:8px;padding:8px 6px;text-align:center;background:var(--ivory)}
.stat b{display:block;font:600 22px/1.1 Georgia,serif}
.stat span{font-size:11px;color:var(--mute)}
.sec{margin-bottom:18px}
.kv{font-size:14px;margin:2px 0}.kv b{font-weight:600}
.detail{border-left:4px solid var(--gold);padding:2px 0 2px 12px}
.detail .t{font:600 17px/1.3 Georgia,serif;margin:0 0 2px}
.detail .m{font-size:12px;color:var(--mute);margin-bottom:6px}
.banner{border-radius:8px;padding:9px 12px;font-size:14px;margin-bottom:8px}
.banner.ok{background:#E3EFEA;border:1px solid var(--teal)}
.banner.bad{background:#F8E3DE;border:1px solid var(--coral)}
.chk{font-size:13px;margin:3px 0;display:flex;gap:8px}
.chip{font-size:10px;font-weight:700;letter-spacing:.6px;padding:1px 6px;border-radius:4px;height:18px;margin-top:2px;color:var(--navy)}
.chip.ok{background:var(--teal)}.chip.bad{background:var(--coral)}
.viol{font-size:12px;color:#8A3B2E;margin:2px 0 4px 40px}
details summary{cursor:pointer;font-size:13px;color:var(--mute)}
#tl{max-height:270px;overflow:auto;border:1px solid var(--line);border-radius:8px;background:var(--ivory);position:relative}
.row{display:flex;gap:10px;padding:5px 12px;border-left:4px solid transparent;font-size:13px;cursor:pointer;opacity:.55}
.row:hover{background:#EFE8D8;opacity:1}
.row.past{opacity:.9}
.row.now{background:#F5E4B8;opacity:1;font-weight:600}
.row .n{width:34px;color:var(--mute);text-align:right}.row .pp{width:24px;font-weight:600}
.c-wait{border-left-color:var(--coral)}.c-got{border-left-color:var(--teal)}.c-eat{border-left-color:var(--gold)}.c-other{border-left-color:#B8AD93}
footer{margin-top:22px;color:var(--mute);font-size:12px}
@media(prefers-reduced-motion:reduce){.fork,.ring,.link,.phil circle.body{transition:none}}
</style></head><body><div class="wrap">
<h1>The Living Table</h1>
<p class="sub">Four philosophers, four fork semaphores, one room semaphore (3 permits). This is a replay of an actual multithreaded C++ run: every step below was recorded while the threads executed.</p>
<div class="grid">
 <div>
  <div class="card stage"><svg id="svg" viewBox="0 0 600 600" role="img" aria-label="Dining table with four philosophers and four forks"></svg></div>
  <div class="controls">
   <button id="bReset">Reset</button><button id="bBack">&larr; Step</button>
   <button id="bPlay" class="primary">Play</button><button id="bFwd">Step &rarr;</button>
   <span class="spacer"></span>
   <label for="sp" style="font-size:13px;color:var(--mute)">Speed</label>
   <select id="sp"><option value="0.5">0.5x</option><option value="1" selected>1x</option><option value="2">2x</option><option value="4">4x</option></select>
  </div>
  <div class="prog"><input id="sl" type="range" min="0" value="0" aria-label="Event position"><span id="pl" style="min-width:120px;text-align:right"></span></div>
 </div>
 <div class="card">
  <div class="stats">
   <div class="stat"><b id="s0">0</b><span>Thinking</span></div>
   <div class="stat"><b id="s1">0</b><span>Hungry / waiting</span></div>
   <div class="stat"><b id="s2">0</b><span>Eating</span></div>
   <div class="stat"><b id="s3">0</b><span>Forks held</span></div>
  </div>
  <div class="sec"><h2>Selected event</h2><div class="detail" id="det"></div></div>
  <div class="sec"><h2>Room semaphore &amp; meals</h2><div id="room"></div></div>
  <div class="sec"><h2>Trace validation</h2><div id="val"></div></div>
 </div>
</div>
<div class="card" style="margin-top:20px"><h2>Event timeline (click an event to jump)</h2><div id="tl"></div></div>
<footer>Generated by main.cpp from a recorded trace. Space = play/pause, arrow keys = step. No network, no libraries.</footer>
</div>
<script>
const TRACE = __TRACE_JSON__;
(function(){
const T=TRACE,EV=T.events,N=T.n,ROOM=T.room,NS="http://www.w3.org/2000/svg";
const $=id=>document.getElementById(id);
const C={ivory:"#F6F1E7",navy:"#18283D",teal:"#4E9B8A",coral:"#D97767",gold:"#E7BD65",ink:"#263238"};
const ST={idle:["Idle","#E4DCCB"],thinking:["Thinking","#E9E1CF"],hungry:["Hungry",C.coral],waiting:["Waiting",C.coral],
 acquiring:["Acquiring",C.coral],eating:["Eating",C.gold],releasing:["Releasing","#F1DDA8"],done:["Done","#D5D0C4"]};
const NAME={THINK:"Starts thinking",HUNGRY:"Becomes hungry",ROOM_TRY:"Requests a room permit",ROOM_OK:"Room permit acquired",
 FORK_REQ:"Requests a fork",FORK_OK:"Fork acquired",EAT_START:"Starts eating",EAT_END:"Finishes eating",
 FORK_REL:"Releases a fork",ROOM_REL:"Releases the room permit",DONE:"Thread finished"};
const CX=300,CY=300,PR=232,TR=165,RR=128,HELD=66;
const rad=d=>d*Math.PI/180;
const PP=[0,1,2,3].map(i=>({x:CX+PR*Math.cos(rad(-90+90*i)),y:CY+PR*Math.sin(rad(-90+90*i))})); // P0 top, P1 right, P2 bottom, P3 left
const PL=[0,1,2,3].map(i=>({x:CX+108*Math.cos(rad(-90+90*i)),y:CY+108*Math.sin(rad(-90+90*i))}));
const FA=[0,1,2,3].map(f=>-45+90*f); // fork f sits between philosophers f and f+1
const FR=FA.map(a=>({x:CX+RR*Math.cos(rad(a)),y:CY+RR*Math.sin(rad(a))}));
const svg=$("svg");
function el(tag,at,par,txt){const e=document.createElementNS(NS,tag);for(const k in at)e.setAttribute(k,at[k]);if(txt!==undefined)e.textContent=txt;(par||svg).appendChild(e);return e}

// ---- static scene ----
el("circle",{cx:CX,cy:CY,r:TR,fill:C.navy});
el("circle",{cx:CX,cy:CY,r:TR-8,fill:"none",stroke:"#2C4160","stroke-width":1});
const plate=PL.map(p=>el("circle",{cx:p.x,cy:p.y,r:20,fill:"none",stroke:"#3B5173","stroke-width":2}));
const link=[0,1,2,3].map(()=>el("line",{stroke:C.coral,"stroke-width":4,"stroke-linecap":"round",opacity:0,class:"link"}));
const pips=[0,1,2].map(i=>el("circle",{cx:CX-30+30*i,cy:292,r:9,fill:"none",stroke:C.ivory,"stroke-width":1.5}));
el("text",{x:CX,y:262,"text-anchor":"middle",fill:C.ivory,"font-size":11,"letter-spacing":2},svg,"ROOM SEMAPHORE");
const tPer=el("text",{x:CX,y:326,"text-anchor":"middle",fill:C.ivory,"font-size":13});
const tWait=el("text",{x:CX,y:346,"text-anchor":"middle",fill:C.ivory,"font-size":12,opacity:.8});
const ph=PP.map((p,i)=>{
 const g=el("g",{class:"phil"});
 const ring=el("circle",{cx:p.x,cy:p.y,r:53,fill:"none",stroke:C.gold,"stroke-width":3.5,opacity:0,class:"ring"},g);
 const body=el("circle",{cx:p.x,cy:p.y,r:44,fill:ST.idle[1],stroke:C.ink,"stroke-width":1.5,class:"body"},g);
 el("text",{x:p.x,y:p.y-5,"text-anchor":"middle","font-size":19,"font-weight":600,fill:C.ink,"font-family":"Georgia,serif"},g,"P"+i);
 const lab=el("text",{x:p.x,y:p.y+15,"text-anchor":"middle","font-size":12,fill:C.ink},g,"Idle");
 return {ring,body,lab};
});
const fk=[0,1,2,3].map(f=>{
 const g=el("g",{class:"fork"});g.style.transform="translate("+FR[f].x+"px,"+FR[f].y+"px)";
 const th=FA[f]-90;
 const inner=el("g",{transform:"rotate("+th+")"},g);
 const tines=el("path",{d:"M-6,-21v-9M0,-21v-9M6,-21v-9",stroke:C.teal,"stroke-width":2.5,"stroke-linecap":"round",fill:"none"},inner);
 const body=el("rect",{x:-11,y:-21,width:22,height:42,rx:11,fill:C.teal,stroke:C.ivory,"stroke-width":1.5},inner);
 el("text",{"text-anchor":"middle",y:4,"font-size":12,"font-weight":700,fill:C.navy},g,"F"+f);
 const ring=el("circle",{r:28,fill:"none",stroke:C.gold,"stroke-width":3,opacity:0,class:"ring"},g);
 return {g,body,tines,ring};
});

// ---- replay: rebuild the complete state from events 0..k ----
function stateAt(k){
 const s={ph:[],own:[-1,-1,-1,-1]};
 for(let i=0;i<N;i++)s.ph.push({st:"idle",queued:false,inside:false,req:-1,meals:0});
 for(let i=0;i<=k;i++){
  const e=EV[i],p=s.ph[e.p];
  switch(e.k){
   case "THINK":p.st="thinking";break;
   case "HUNGRY":p.st="hungry";break;
   case "ROOM_TRY":p.st="hungry";p.queued=true;break;
   case "ROOM_OK":p.queued=false;p.inside=true;p.st="waiting";break;
   case "FORK_REQ":p.req=e.f;p.st="waiting";break;
   case "FORK_OK":s.own[e.f]=e.p;p.req=-1;p.st="acquiring";break;
   case "EAT_START":p.st="eating";break;
   case "EAT_END":p.st="releasing";p.meals++;break;
   case "FORK_REL":s.own[e.f]=-1;break;
   case "ROOM_REL":p.inside=false;break;
   case "DONE":p.st="done";break;
  }
 }
 return s;
}
const side=(p,f)=>f===(p+N-1)%N?"left":"right";
function explain(e,s){
 const p="P"+e.p,inside=s.ph.filter(x=>x.inside).length;
 switch(e.k){
  case "THINK":return p+" is thinking and holds no shared resource.";
  case "HUNGRY":return p+" wants to eat. Next it must pass the room semaphore.";
  case "ROOM_TRY":return p+" calls room.acquire(). It blocks if all "+ROOM+" permits are in use. This is an attempt only; nothing is owned yet.";
  case "ROOM_OK":return "room.acquire() returned. "+p+" is inside ("+inside+" of "+ROOM+" permits used). Only now may it compete for forks.";
  case "FORK_REQ":return p+" calls fork["+e.f+"].acquire() on its "+side(e.p,e.f)+" fork. Attempt only: it waits if Fork "+e.f+" is taken.";
  case "FORK_OK":return "fork["+e.f+"].acquire() returned. "+p+" now owns Fork "+e.f+" ("+side(e.p,e.f)+"). Its semaphore is 0, so neighbours must wait.";
  case "EAT_START":return p+" holds both adjacent forks and eats. Neither neighbour can eat: each would need one of these forks.";
  case "EAT_END":return p+" has finished eating and starts putting the forks down.";
  case "FORK_REL":return p+" puts down Fork "+e.f+" (logged just before fork["+e.f+"].release(), so it becomes available again).";
  case "ROOM_REL":return p+" leaves the room and releases its permit. A waiting philosopher may now enter.";
  case "DONE":return p+" has completed all rounds; its thread ends and can be joined.";
 }
 return "";
}

// ---- timeline ----
const tl=$("tl");
const cat=k=>["HUNGRY","ROOM_TRY","FORK_REQ"].includes(k)?"c-wait":["ROOM_OK","FORK_OK"].includes(k)?"c-got":["EAT_START","EAT_END"].includes(k)?"c-eat":"c-other";
const rows=EV.map((e,i)=>{
 const r=document.createElement("div");r.className="row "+cat(e.k);
 r.innerHTML='<span class="n">'+(i+1)+'</span><span class="pp">P'+e.p+'</span><span>'+NAME[e.k]+(e.f>=0?" (Fork "+e.f+")":"")+'</span>';
 r.onclick=()=>go(i);tl.appendChild(r);return r;
});

// ---- render ----
let cur=-1,timer=null;
const sl=$("sl");sl.max=EV.length;
function render(){
 const s=stateAt(cur),e=cur>=0?EV[cur]:null;
 let held=0;
 s.own.forEach(o=>{if(o>=0)held++});
 // philosophers
 ph.forEach((x,i)=>{
  const q=s.ph[i];x.body.setAttribute("fill",ST[q.st][1]);x.lab.textContent=ST[q.st][0];
  x.ring.setAttribute("opacity",e&&e.p===i?1:0);
  plate[i].setAttribute("fill",q.st==="eating"?C.gold:"none");
 });
 // forks: on the table, or moved next to the owner
 for(let f=0;f<N;f++){
  const o=s.own[f];let x=FR[f].x,y=FR[f].y,col=C.teal;
  const L=link[f];L.setAttribute("opacity",0);
  if(o>=0){
   const b=PP[o],dx=FR[f].x-b.x,dy=FR[f].y-b.y,d=Math.hypot(dx,dy);
   x=b.x+dx/d*HELD;y=b.y+dy/d*HELD;col=ST[s.ph[o].st][1];
   L.setAttribute("x1",b.x);L.setAttribute("y1",b.y);L.setAttribute("x2",x);L.setAttribute("y2",y);
   L.setAttribute("stroke",s.ph[o].st==="eating"?C.gold:C.coral);L.removeAttribute("stroke-dasharray");L.setAttribute("opacity",1);
  }else{
   const w=s.ph.findIndex(q=>q.req===f);
   if(w>=0){const b=PP[w];L.setAttribute("x1",b.x);L.setAttribute("y1",b.y);L.setAttribute("x2",x);L.setAttribute("y2",y);
    L.setAttribute("stroke",C.coral);L.setAttribute("stroke-dasharray","3 8");L.setAttribute("opacity",.9);}
  }
  fk[f].g.style.transform="translate("+x+"px,"+y+"px)";
  fk[f].body.setAttribute("fill",col);fk[f].tines.setAttribute("stroke",col);
  fk[f].ring.setAttribute("opacity",e&&e.f===f?1:0);
 }
 // room semaphore
 const inside=s.ph.filter(q=>q.inside).length,queue=[];
 s.ph.forEach((q,i)=>{if(q.queued)queue.push("P"+i)});
 pips.forEach((p,i)=>p.setAttribute("fill",i<inside?C.gold:"none"));
 tPer.textContent="permits free: "+(ROOM-inside)+" / "+ROOM;
 tWait.textContent=queue.length?"waiting: "+queue.join(", "):"no one waiting";
 const inList=s.ph.map((q,i)=>q.inside?"P"+i:null).filter(Boolean);
 $("room").innerHTML='<div class="kv"><b>Permits free:</b> '+(ROOM-inside)+' of '+ROOM+'</div>'+
  '<div class="kv"><b>Inside (competing):</b> '+(inList.join(", ")||"none")+'</div>'+
  '<div class="kv"><b>Blocked outside:</b> '+(queue.join(", ")||"none")+'</div>'+
  '<div class="kv"><b>Meals so far:</b> '+s.ph.map((q,i)=>"P"+i+"="+q.meals).join("&nbsp; ")+'</div>';
 // summary
 const cnt=f=>s.ph.filter(q=>f.includes(q.st)).length;
 $("s0").textContent=cnt(["thinking"]);$("s1").textContent=cnt(["hungry","waiting","acquiring"]);
 $("s2").textContent=cnt(["eating"]);$("s3").textContent=held+" / "+N;
 // details
 if(e){
  $("det").innerHTML='<p class="t">'+NAME[e.k]+(e.f>=0?" &middot; Fork "+e.f:"")+'</p><div class="m">Event '+(cur+1)+' of '+EV.length+
   ' &middot; P'+e.p+' &middot; t = '+(e.t/1000).toFixed(2)+' ms</div><div>'+explain(e,s)+'</div>';
 }else{
  $("det").innerHTML='<p class="t">Initial state</p><div>All forks lie on the table and the room semaphore is '+ROOM+
   '. Press Play or Step to replay the recorded run.</div>';
 }
 // timeline + progress
 rows.forEach((r,i)=>{r.classList.toggle("now",i===cur);r.classList.toggle("past",i<cur)});
 if(cur>=0){const r=rows[cur];tl.scrollTop=Math.max(0,r.offsetTop-tl.clientHeight/2+r.clientHeight/2)}
 else tl.scrollTop=0;
 sl.value=cur+1;$("pl").textContent="Event "+(cur+1)+" / "+EV.length;
 $("bPlay").textContent=timer?"Pause":"Play";
}
function go(k){cur=Math.max(-1,Math.min(EV.length-1,k));render()}
function stop(){if(timer){clearInterval(timer);timer=null}}
function play(){
 if(timer){stop();render();return}
 if(cur>=EV.length-1)cur=-1;
 const delay=650/parseFloat($("sp").value);
 timer=setInterval(()=>{if(cur>=EV.length-1){stop();render();return}go(cur+1)},delay);
 go(cur+1);
}
$("bPlay").onclick=play;
$("bReset").onclick=()=>{stop();go(-1)};
$("bFwd").onclick=()=>{stop();go(cur+1)};
$("bBack").onclick=()=>{stop();go(cur-1)};
$("sp").onchange=()=>{if(timer){stop();play()}};
sl.oninput=()=>{stop();go(parseInt(sl.value,10)-1)};
document.addEventListener("keydown",ev=>{
 if(ev.target.tagName==="SELECT")return;
 if(ev.key===" "){ev.preventDefault();play()}
 else if(ev.key==="ArrowRight"){stop();go(cur+1)}
 else if(ev.key==="ArrowLeft"){stop();go(cur-1)}
});

// ---- validation panel (produced by the C++ validator, shown unmodified) ----
let nBad=0;T.checks.forEach(c=>nBad+=c.bad.length);
let h='<div class="banner '+(nBad?"bad":"ok")+'">'+(nBad?nBad+" invariant violation(s) detected in this trace.":"All "+T.checks.length+" invariants held for this trace ("+EV.length+" events, "+T.rounds+" rounds per philosopher).")+'</div><details'+(nBad?" open":"")+'><summary>Show the individual checks</summary>';
T.checks.forEach(c=>{
 h+='<div class="chk"><span class="chip '+(c.bad.length?"bad":"ok")+'">'+(c.bad.length?"FAIL":"OK")+'</span><span>'+c.name+'</span></div>';
 c.bad.forEach(b=>{h+='<div class="viol">'+b+'</div>'});
});
$("val").innerHTML=h+'</details>';
render();
})();
</script></body></html>
)HTML";

// ----------------------------------------------------------------------------
// 7. main: run threads -> join -> validate -> write HTML
// ----------------------------------------------------------------------------
int main(int argc, char** argv) {
    if (argc > 1 && std::string(argv[1]) == "--selftest") return selfTest();

    const int rounds = argc > 1 ? std::max(1, std::atoi(argv[1])) : 3;
    const std::string outPath = argc > 2 ? argv[2] : "dining_philosophers_semaphore.html";
    const unsigned seed = argc > 3 ? static_cast<unsigned>(std::strtoul(argv[3], nullptr, 10)) : std::random_device{}();

    Table table;
    Recorder rec;
    std::vector<std::thread> threads;
    for (int i = 0; i < N; ++i)
        threads.emplace_back(philosopher, i, rounds, seed, std::ref(table), std::ref(rec));
    for (auto& t : threads) t.join();   // all four must terminate

    const std::vector<Event> ev = rec.take();
    const std::vector<Check> checks = validate(ev);

    int violations = 0;
    std::cout << "Recorded " << ev.size() << " events (" << rounds << " rounds, seed " << seed << ")\n";
    for (const Check& c : checks) {
        std::cout << (c.bad.empty() ? "  [ OK ] " : "  [FAIL] ") << c.name << "\n";
        for (const auto& b : c.bad) std::cout << "         " << b << "\n";
        violations += static_cast<int>(c.bad.size());
    }
    int meals[N] = {};
    for (const Event& e : ev) if (e.type == Ev::EatStart) ++meals[e.phil];
    std::cout << "Meals: P0=" << meals[0] << " P1=" << meals[1] << " P2=" << meals[2] << " P3=" << meals[3] << "\n";

    std::string html = kTemplate;
    const std::string token = "__TRACE_JSON__";
    html.replace(html.find(token), token.size(), buildJson(ev, checks, rounds, seed));
    std::ofstream out(outPath, std::ios::binary);
    if (!out) { std::cerr << "cannot write " << outPath << "\n"; return 2; }
    out << html;
    std::cout << "Wrote " << outPath << (violations ? "  (with violations!)" : "") << "\n";
    return violations ? 1 : 0;
}
