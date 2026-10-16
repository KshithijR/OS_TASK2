// Dining Philosophers with a monitor (C++20). Records a real concurrent trace and
// generates dining_philosophers_monitor.html from it. Build: g++ -std=c++20 -O2 -pthread main.cpp -o dp
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <fstream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>
using namespace std;

constexpr int N = 4;      // philosophers = forks = 4
constexpr int MEALS = 5;  // meals per philosopher
enum State { THINKING, HUNGRY, EATING };
const char* NAMES[] = {"THINKING", "HUNGRY", "EATING"};
int L(int i) { return (i + N - 1) % N; }  // left neighbour
int R(int i) { return (i + 1) % N; }      // right neighbour

// One recorded event = what happened + a full snapshot of the monitor right after it.
struct Event {
  long seq = 0, us = 0;
  int p = -1;
  string kind, msg;
  array<int, N> st{}, waiting{}, fork{};  // fork[j] = owner of fork j, or -1 if available
};

// Thread-safe logger: its own mutex assigns the sequence number and timestamp.
class Logger {
  mutex m_;
  vector<Event> ev_;
  chrono::steady_clock::time_point t0_ = chrono::steady_clock::now();
 public:
  void add(Event e) {
    lock_guard<mutex> g(m_);
    e.seq = (long)ev_.size();
    e.us = chrono::duration_cast<chrono::microseconds>(chrono::steady_clock::now() - t0_).count();
    ev_.push_back(std::move(e));
  }
  vector<Event> events() { lock_guard<mutex> g(m_); return ev_; }
};

// The monitor: all shared state is private and only touched while holding mtx_.
class Monitor {
  mutex mtx_;
  array<condition_variable, N> cv_;  // one condition variable per philosopher
  array<State, N> st_{};             // all THINKING initially
  array<int, N> waiting_{};          // 1 while the philosopher is blocked on its cv
  Logger& log_;
  int violations_ = 0;

  // Entry condition. Caller must hold mtx_.
  bool canEat(int i) const { return st_[L(i)] != EATING && st_[R(i)] != EATING; }

  // Record an event with a snapshot of the current state. Caller must hold mtx_,
  // so the snapshot is consistent and the log order equals the monitor order.
  void rec(int p, const char* kind, string msg) {
    Event e; e.p = p; e.kind = kind;
    for (int j = 0; j < N; j++) {
      e.st[j] = st_[j]; e.waiting[j] = waiting_[j];
      e.fork[j] = st_[j] == EATING ? j : (st_[R(j)] == EATING ? R(j) : -1);  // fork j sits between j and j+1
    }
    for (int j = 0; j < N; j++)  // runtime invariant check: report, never hide or rewrite
      if (st_[j] == EATING && st_[R(j)] == EATING) {
        violations_++; msg += " [INVARIANT VIOLATION]";
        fprintf(stderr, "VIOLATION: P%d and P%d eat together\n", j, R(j));
      }
    e.msg = std::move(msg);
    log_.add(std::move(e));
  }
  static string P(int i) { return "P" + to_string(i); }

 public:
  explicit Monitor(Logger& l) : log_(l) {}
  int violations() { lock_guard<mutex> g(mtx_); return violations_; }
  void init() { lock_guard<mutex> g(mtx_); rec(-1, "INIT", "All philosophers THINKING, all forks available"); }
  void startThinking(int i) { lock_guard<mutex> g(mtx_); rec(i, "THINK", P(i) + " starts thinking"); }

  void pickup(int i) {
    unique_lock<mutex> lk(mtx_);
    st_[i] = HUNGRY;
    rec(i, "HUNGRY", P(i) + " is hungry and asks the monitor for permission");
    rec(i, "CHECK", "Monitor checks " + P(L(i)) + " (" + NAMES[st_[L(i)]] + ") and " + P(R(i)) + " (" +
        NAMES[st_[R(i)]] + "): " + (canEat(i) ? "neither is eating" : "a neighbour is eating"));
    // Wait in a loop: after every wake-up (real notify, stale notify or spurious wake-up)
    // the predicate is checked again. This is what cv.wait(lk, pred) does internally.
    while (!canEat(i)) {
      waiting_[i] = 1;
      rec(i, "WAIT", P(i) + " blocks on cv[" + to_string(i) + "] and releases the mutex");
      cv_[i].wait(lk);  // mutex is released while blocked, re-acquired on wake-up
      rec(i, "WAKE", P(i) + " woke up and re-acquired the mutex; " +
          (canEat(i) ? "predicate now true" : "predicate still false (spurious or stale), waits again"));
    }
    waiting_[i] = 0;
    st_[i] = EATING;  // admission: check and state change happen under one lock hold
    rec(i, "ADMIT", "Monitor admits " + P(i) + ": both neighbours not eating, state HUNGRY -> EATING, takes forks " +
        to_string(L(i)) + " and " + to_string(i));
  }

  void putdown(int i) {
    unique_lock<mutex> lk(mtx_);
    st_[i] = THINKING;
    rec(i, "DONE", P(i) + " finishes eating, state EATING -> THINKING, forks " + to_string(L(i)) + " and " +
        to_string(i) + " available");
    for (int n : {L(i), R(i)})  // notify only hungry neighbours that can now eat
      if (st_[n] == HUNGRY && canEat(n)) {
        rec(n, "NOTIFY", "notify_one on cv[" + to_string(n) + "]: " + P(n) + " may proceed (not yet admitted)");
        cv_[n].notify_one();
      }
  }
};

static string esc(const string& s) {
  string o; for (char c : s) { if (c == '"' || c == '\\') o += '\\'; o += c; } return o;
}
static string arr(const array<int, N>& a) {
  string s = "["; for (int j = 0; j < N; j++) s += (j ? "," : "") + to_string(a[j]); return s + "]";
}

extern const char* HTML_PRE;
extern const char* HTML_POST;

int main() {
  Logger logger; Monitor mon(logger);
  mon.init();
  vector<thread> th;
  for (int i = 0; i < N; i++)
    th.emplace_back([&, i] {
      mt19937 rng(random_device{}() + i);
      uniform_int_distribution<int> ms(15, 60);
      for (int m = 0; m < MEALS; m++) {
        mon.startThinking(i);
        this_thread::sleep_for(chrono::milliseconds(ms(rng)));  // thinking
        mon.pickup(i);
        this_thread::sleep_for(chrono::milliseconds(ms(rng)));  // eating (outside the mutex)
        mon.putdown(i);
      }
    });
  for (auto& t : th) t.join();

  // Independent post-run verification of the recorded trace.
  auto ev = logger.events();
  int bad = 0; array<int, N> meals{};
  for (auto& e : ev) {
    for (int j = 0; j < N; j++) if (e.st[j] == EATING && e.st[R(j)] == EATING) bad++;
    if (e.kind == "ADMIT") { meals[e.p]++; if (e.st[L(e.p)] == EATING || e.st[R(e.p)] == EATING) bad++; }
  }
  string j = "{\"n\":" + to_string(N) + ",\"events\":[";
  for (size_t k = 0; k < ev.size(); k++) {
    auto& e = ev[k];
    j += string(k ? "," : "") + "{\"s\":" + to_string(e.seq) + ",\"t\":" + to_string(e.us) + ",\"p\":" + to_string(e.p) +
         ",\"k\":\"" + e.kind + "\",\"m\":\"" + esc(e.msg) + "\",\"st\":" + arr(e.st) + ",\"w\":" + arr(e.waiting) +
         ",\"f\":" + arr(e.fork) + "}";
  }
  j += "]}";
  ofstream("dining_philosophers_monitor.html") << HTML_PRE << j << HTML_POST;

  printf("events=%zu runtime_violations=%d trace_violations=%d meals=[%d,%d,%d,%d]\n", ev.size(), mon.violations(),
         bad, meals[0], meals[1], meals[2], meals[3]);
  printf("Wrote dining_philosophers_monitor.html\n");
  return (bad || mon.violations()) ? 1 : 0;
}

// ---- Self-contained HTML replay page (data is injected between PRE and POST) ----
const char* HTML_PRE = R"HTML(<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Dining Philosophers - Monitor trace</title>
<style>
:root{--bg:#F6F1E7;--table:#18283D;--ok:#4E9B8A;--wait:#D97767;--eat:#E7BD65;--ink:#263238;--line:#d8cfbb}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 Georgia,serif}
header{padding:18px 24px 0}h1{font-size:22px;margin:0 0 2px}header p{margin:0;font-size:13px;opacity:.75}
main{display:grid;grid-template-columns:minmax(300px,1.1fr) minmax(300px,1fr);gap:22px;padding:14px 24px 24px}
@media(max-width:820px){main{grid-template-columns:1fr;padding:12px}header{padding:14px 12px 0}}
svg{width:100%;max-width:560px;display:block;margin:0 auto}
svg *{transition:fill .35s,stroke .35s,transform .5s,opacity .3s}
.bar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:10px 0}
button,select{font:inherit;font-size:14px;color:var(--ink);background:#fff8;border:1px solid var(--ink);border-radius:4px;padding:5px 11px;cursor:pointer}
button:focus-visible,select:focus-visible,input:focus-visible{outline:3px solid var(--eat);outline-offset:1px}
input[type=range]{flex:1;min-width:140px;accent-color:var(--ok)}
.stats{display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin:8px 0}
.stats div{border-top:3px solid var(--ink);padding-top:4px;font-size:12px}.stats b{display:block;font-size:20px}
.msg{border-left:5px solid var(--eat);padding:6px 12px;background:#fff6;min-height:64px;margin:8px 0}
#tl{height:200px;overflow:auto;position:relative;border:1px solid var(--line);background:#fff5}
#tl div{padding:3px 10px;cursor:pointer;font-size:13px;border-bottom:1px solid var(--line);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#tl div.on{background:var(--eat)}
.WAIT{color:#b4503f}.ADMIT{font-weight:bold}.NOTIFY{color:#2f7566}
table{border-collapse:collapse;width:100%;font-size:13px;margin-top:6px}td,th{border-bottom:1px solid var(--line);padding:2px 6px;text-align:left}
h2{font-size:15px;margin:14px 0 4px}
@media(prefers-reduced-motion:reduce){svg *{transition:none}}
</style></head><body>
<header><h1>The Living Table</h1><p>Replay of one recorded run: 4 philosopher threads, one monitor mutex, one condition variable each.</p></header>
<main><section><svg id="sv" viewBox="0 0 520 520" role="img" aria-label="Dining table with four philosophers and four forks"></svg>
<div class="stats"><div>Thinking<b id="s0"></b></div><div>Hungry / waiting<b id="s1"></b></div><div>Eating<b id="s2"></b></div><div>Forks free<b id="s3"></b></div><div>Event<b id="s4" style="font-size:14px;padding-top:5px"></b></div></div></section>
<section><div class="bar"><button id="pp">Play</button><button id="bk">Step back</button><button id="fw">Step forward</button><button id="rs">Reset</button>
<label>Speed <select id="sp"><option>0.5</option><option selected>1</option><option>2</option><option>4</option><option>8</option></select></label></div>
<div class="bar"><input id="rg" type="range" min="0" value="0" aria-label="Progress"><span id="ct"></span></div>
<div class="msg" id="msg"></div><h2>Event timeline</h2><div id="tl"></div>
<h2>Selected event and monitor state</h2><div id="dt"></div></section></main>
<script>const DATA=)HTML";

const char* HTML_POST = R"HTML(;
const E=DATA.events,C=260,NS='http://www.w3.org/2000/svg',NM=['THINKING','HUNGRY','EATING'];
let i=0,timer=null;const $=id=>document.getElementById(id);
function el(t,a,p){const x=document.createElementNS(NS,t);for(const k in a)x.setAttribute(k,a[k]);(p||$('sv')).appendChild(x);return x}
const ang=k=>(-90+90*k)*Math.PI/180,pt=(a,r)=>[C+r*Math.cos(a),C+r*Math.sin(a)];
el('circle',{cx:C,cy:C,r:150,fill:'#18283D'});
const ph=[],pl=[],fk=[];
for(let k=0;k<4;k++){
 const [px,py]=pt(ang(k),92);pl.push(el('circle',{cx:px,cy:py,r:20,fill:'none',stroke:'#F6F1E7','stroke-opacity':.45}));
 const [lx,ly]=pt(ang(k-.5),138);el('text',{x:lx,y:ly+4,'text-anchor':'middle','font-size':11,fill:'#F6F1E7','fill-opacity':.7},0).textContent='F'+k;
}
for(let k=0;k<4;k++){const g=el('g',{});el('line',{x1:-17,y1:0,x2:17,y2:0,'stroke-width':5,'stroke-linecap':'round'},g);
 el('line',{x1:9,y1:-5,x2:9,y2:5,'stroke-width':3,'stroke-linecap':'round'},g);fk.push(g)}
for(let k=0;k<4;k++){const [x,y]=pt(ang(k),205),g=el('g',{});
 const ring=el('circle',{cx:x,cy:y,r:46,fill:'none',stroke:'#E7BD65','stroke-width':3,'stroke-dasharray':'5 4'},g);
 const c=el('circle',{cx:x,cy:y,r:38,'stroke-width':3},g);
 const a=el('text',{x:x,y:y-3,'text-anchor':'middle','font-size':17,'font-weight':'bold',fill:'#263238'},g),b=el('text',{x:x,y:y+15,'text-anchor':'middle','font-size':10.5,fill:'#263238'},g);
 ph.push({ring,c,a,b})}
function place(k,e){const fa=ang(k-.5),o=e.f[k];let a=fa,r=118;
 if(o>=0){a=ang(o)+(fa-ang(o))*.3;r=112}const [x,y]=pt(a,r);
 fk[k].style.transform=`translate(${x}px,${y}px) rotate(${a*180/Math.PI}deg)`;
 fk[k].querySelectorAll('line').forEach(l=>l.setAttribute('stroke',o>=0?'#E7BD65':'#4E9B8A'))}
E.forEach((e,n)=>{const d=document.createElement('div');d.className=e.k;d.textContent=`#${e.s} ${e.k}${e.p>=0?' P'+e.p:''}`;d.onclick=()=>go(n);$('tl').appendChild(d)});
$('rg').max=E.length-1;
function go(n){i=Math.max(0,Math.min(E.length-1,n));const e=E[i];let c=[0,0,0],free=0;
 for(let k=0;k<4;k++){const s=e.st[k],w=e.w[k],p=ph[k];c[s]++;
  p.c.setAttribute('fill',s==2?'#E7BD65':w?'#D97767':'#F6F1E7');
  p.c.setAttribute('stroke',s==1?'#D97767':'#263238');p.c.setAttribute('stroke-width',s==1?5:3);
  p.a.textContent='P'+k;p.b.textContent=w?'WAITING':NM[s];p.ring.style.opacity=e.p==k?1:0;
  pl[k].setAttribute('fill',s==2?'#E7BD65':'none');place(k,e);if(e.f[k]<0)free++}
 $('s0').textContent=c[0];$('s1').textContent=c[1];$('s2').textContent=c[2];$('s3').textContent=free+' / 4';$('s4').textContent=e.k;
 $('msg').innerHTML=`<b>${e.k}</b> &nbsp;${e.m}`;$('rg').value=i;$('ct').textContent=`Event ${i+1} / ${E.length}`;
 [...$('tl').children].forEach((d,n)=>d.classList.toggle('on',n==i));const r=$('tl').children[i];$('tl').scrollTop=r.offsetTop-90;
 let h=`<p style="margin:0">Seq #${e.s}, t = ${(e.t/1000).toFixed(2)} ms, event ${e.k}${e.p>=0?', philosopher P'+e.p:''}</p><table><tr><th>Philosopher</th><th>State</th><th>Blocked on cv</th><th>Fork</th><th>Owner</th></tr>`;
 for(let k=0;k<4;k++)h+=`<tr><td>P${k}</td><td>${NM[e.st[k]]}</td><td>${e.w[k]?'yes':'no'}</td><td>F${k}</td><td>${e.f[k]<0?'available':'P'+e.f[k]}</td></tr>`;
 $('dt').innerHTML=h+'</table>'}
function stop(){clearInterval(timer);timer=null;$('pp').textContent='Play'}
function start(){clearInterval(timer);if(i>=E.length-1)go(0);$('pp').textContent='Pause';
 timer=setInterval(()=>{if(i>=E.length-1)stop();else go(i+1)},700/parseFloat($('sp').value))}
$('pp').onclick=()=>timer?stop():start();$('fw').onclick=()=>{stop();go(i+1)};$('bk').onclick=()=>{stop();go(i-1)};
$('rs').onclick=()=>{stop();go(0)};$('sp').onchange=()=>{if(timer)start()};$('rg').oninput=e=>{stop();go(+e.target.value)};
go(0);
</script></body></html>)HTML";
