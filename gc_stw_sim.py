# -*- coding: utf-8 -*-
"""
gc_stw_sim.py
极简自研虚拟机 —— 并发标记-清除 GC 的 Stop-The-World 仿真内核（纯标准库单文件）。

运行:  python gc_stw_sim.py [端口]
默认端口 8000，浏览器自动打开 http://127.0.0.1:8000/

并发模型（无死锁设计）:
  * N 个应用线程(Mutator): 随机分配对象 / 修改引用图 / 摘除根(制造循环垃圾)
  * 1 个 GC 线程:  并发标记(Dijkstra 写屏障, 三色不变式) -> STW 重标记 -> 分块清除
  * 锁: heap.lock 仅保护堆状态(临界区均为微秒级); stw_cond 仅保护挂起协议。
        两把锁从不嵌套持有, 因而不可能形成锁环。
  * STW: GC 置 stw_flag, 所有 Mutator 在每个“安全点”轮询该标志并自行 park;
        park 时不持有任何堆锁。GC 等齐全部线程后才开始清除, 最后 broadcast 放行。
"""

import sys
import time
import json
import random
import threading
import webbrowser
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ---------------------------------------------------------------- 参数
GRID_W, GRID_H = 100, 72          # 堆 = 固定大小“方格”, 每格一个对象
MUTATOR_NUM = 4                  # 应用线程数
ROOTS_PER_THREAD_MAX = 16        # 每个线程持有的根上限(超出会随机摘根 -> 产生垃圾)
REFS_PER_OBJ_MAX = 3             # 每个对象最大出度
GC_INTERVAL = 3.0                # GC 周期(秒)
SWEEP_CHUNK = 420                # STW 中每块清除的对象数(分块以便前端看到扫线移动)
SWEEP_CHUNK_SLEEP = 0.035        # 块间休眠 -> 拉长 STW, 强化“全场静止”观感
MARK_BATCH_SLEEP = 0.0015        # 并发标记每处理一批让出 GIL
MARK_TRAIL_MAX = 900             # 扫描光标轨迹长度
HTTP_PORT_DEFAULT = 8000

# 对象标记位
FREE, WHITE, GRAY, BLACK, DOOMED = 0, 1, 2, 3, 4
# GC 阶段
IDLE_PHASE, MARK_PHASE, REMARK_PHASE, SWEEP_PHASE = "idle", "mark", "remark", "sweep"


class Heap:
    def __init__(self):
        n = GRID_W * GRID_H
        self.lock = threading.Lock()
        self.cells = [FREE] * n            # 标记位
        self.refs = [[] for _ in range(n)] # 出边(引用图)
        self.free = deque(range(n - 1, -1, -1))
        self.live = []                     # 已分配对象(供随机选边, 顺序即地址)
        self.roots = {}                    # thread_id -> set(obj)
        for t in range(MUTATOR_NUM):
            self.roots[t] = set()

        self.gc_phase = IDLE_PHASE
        self.grayq = deque()                 # 并发标记的灰色工作队列
        self.mark_trail = deque(maxlen=MARK_TRAIL_MAX)  # 并发扫描光标轨迹
        self.sweep_cursor = -1            # STW 清除扫线所在行
        self.alloc_since_gc = 0
        self.gc_count = 0
        self.last_pause_ms = 0.0
        self.cycle_freed = 0
        self.cycle_doomed = 0
        self.mark_scanned = 0

        # STW 挂起协议(独立条件变量, 与 heap.lock 不嵌套)
        self.stw_cond = threading.Condition()
        self.stw_flag = False
        self.parked = set()
        self.stw_start = 0.0

    # -------------------------------------------------- 安全点
    def safepoint(self, tid):
        """Mutator 在每次操作间隙调用; 若 STW 则自行挂起, 挂起时不持有任何锁。"""
        with self.stw_cond:
            if not self.stw_flag:
                return
            self.parked.add(tid)
            self.stw_cond.notify_all()
            while self.stw_flag:
                self.stw_cond.wait()
            self.parked.discard(tid)

    def request_pause(self):
        with self.stw_cond:
            self.stw_flag = True
            self.stw_start = time.perf_counter()

    def wait_safepoints(self):
        deadline = time.perf_counter() + 10.0
        with self.stw_cond:
            while len(self.parked) < MUTATOR_NUM:
                remaining = deadline - time.perf_counter()
                if remaining <= 0:
                    return
                self.stw_cond.wait(remaining)

    def resume(self):
        with self.stw_cond:
            self.stw_flag = False
            self.parked.clear()
            self.stw_cond.notify_all()

    # -------------------------------------------------- 堆操作
    def _shade(self, idx):
        """白 -> 灰, 并入队(调用方持锁)。"""
        if self.cells[idx] == WHITE:
            self.cells[idx] = GRAY
            self.grayq.append(idx)

    def _new_cell(self, tid):
        """分配一个对象, 返回下标; 堆满返回 None。调用方持锁。"""
        while self.free:
            idx = self.free.pop()
            if self.cells[idx] == FREE:
                self.cells[idx] = GRAY if self.gc_phase == MARK_PHASE else WHITE
                self.refs[idx] = []
                self.live.append(idx)
                self.alloc_since_gc += 1
                if self.gc_phase == MARK_PHASE:
                    # 标记中新对象直接视为灰并入队, 保证不被误杀
                    self.grayq.append(idx)
                    self.mark_trail.append(idx)
                return idx
        return None

    def _random_live(self, tid):
        for _ in range(6):
            if not self.live:
                return None
            cand = self.live[random.randrange(len(self.live))]
            if self.cells[cand] != FREE:
                return cand
        return None

    def allocate(self, tid):
        """Mutator 分配: 可能是小对象簇(含环), 也可能是单对象。"""
        with self.lock:
            if self.gc_phase == IDLE_PHASE and not self.free:
                return
            head = self._new_cell(tid)
            if head is None:
                return
            size = random.randint(2, 4)
            chain = [head]
            for _ in range(size - 1):
                idx = self._new_cell(tid)
                if idx is None:
                    break
                chain.append(idx)
            # 链成一串
            for a, b in zip(chain, chain[1:]):
                if len(self.refs[a]) < REFS_PER_OBJ_MAX:
                    self.refs[a].append(b)
                    if self.gc_phase == MARK_PHASE:
                        self._shade(b)
            # 制造环引用: 尾 -> 头(循环垃圾的主要来源)
            if len(chain) >= 3 and random.random() < 0.5:
                tail = chain[-1]
                if len(self.refs[tail]) < REFS_PER_OBJ_MAX:
                    self.refs[tail].append(head)
                    if self.gc_phase == MARK_PHASE:
                        self._shade(head)
            # 挂到根集合 / 已有存活对象
            roots = self.roots[tid]
            if not roots or random.random() < 0.6:
                roots.add(head)
                if len(roots) > ROOTS_PER_THREAD_MAX:
                    roots.discard(random.choice(tuple(roots)))
            else:
                anchor = self._random_live(tid)
                if anchor is not None and len(self.refs[anchor]) < REFS_PER_OBJ_MAX:
                    self.refs[anchor].append(head)
                    if self.gc_phase == MARK_PHASE:
                        self._shade(head)
                else:
                    roots.add(head)

    def mutate_refs(self, tid):
        """Mutator 修改引用图: 随机增边, 写屏障保证三色不变式。"""
        with self.lock:
            src = self._random_live(tid)
            if src is None:
                return
            if len(self.refs[src]) >= REFS_PER_OBJ_MAX and random.random() < 0.5:
                # 删边
                del self.refs[src][random.randrange(len(self.refs[src]))]
                return
            dst = self._random_live(tid)
            if dst is None or dst == src:
                return
            if dst in self.refs[src] or len(self.refs[src]) >= REFS_PER_OBJ_MAX:
                return
            self.refs[src].append(dst)
            # Dijkstra 写屏障: 标记阶段, 黑/灰对象指向白对象时, 立即把目标染灰
            if self.gc_phase == MARK_PHASE:
                self._shade(dst)

    def churn_roots(self, tid):
        """Mutator 摘除旧根 -> 失去根的孤岛(含环)成为垃圾。"""
        roots = self.roots[tid]
        if len(roots) > 8 and random.random() < 0.35:
            roots.discard(random.choice(tuple(roots)))

    # -------------------------------------------------- GC
    def gc_cycle(self, log):
        # 阶段 1: 并发标记(应用线程继续跑, 靠写屏障兜底)
        with self.lock:
            if self.gc_phase != IDLE_PHASE:
                return
            self.gc_phase = MARK_PHASE
            self.mark_scanned = 0
            self.mark_trail.clear()
            self.grayq.clear()
            roots = {t: tuple(rs) for t, rs in self.roots.items()}
            for objs in roots.values():
                for obj in objs:
                    self._shade(obj)

        while True:
            with self.lock:
                batch = 0
                # 灰色队列: 光标沿真实引用边 BFS 游走
                while self.grayq and batch < 400:
                    cur = self.grayq.popleft()
                    if self.cells[cur] != GRAY:
                        continue
                    self.cells[cur] = BLACK
                    self.mark_scanned += 1
                    self.mark_trail.append(cur)
                    for dst in self.refs[cur]:
                        self._shade(dst)
                    batch += 1
                done = not self.grayq
            # 队列短暂排空不代表永久终止(mutator 还在写), 没关系:
            # STW 重标记会做一次从根出发的完整可达性重扫兜底。
            if done:
                break
            time.sleep(MARK_BATCH_SLEEP)

        # 阶段 2: STW —— 挂起全部应用线程
        self.request_pause()
        self.wait_safepoints()
        t0 = time.perf_counter()
        try:
            with self.lock:
                self.gc_phase = REMARK_PHASE
                self.sweep_cursor = -1
                # 2a. 重标记(STW): 从全部根出发做完整可达性重扫。
                # 必须重新检查黑对象的边 —— 并发期黑对象扫描过后可能新增引用,
                # 这是写屏障方案标准的终止兜底。
                stack = []
                for objs in self.roots.values():
                    stack.extend(objs)
                seen = set()
                while stack:
                    cur = stack.pop()
                    if cur in seen:
                        continue
                    seen.add(cur)
                    self.cells[cur] = BLACK
                    for dst in self.refs[cur]:
                        if self.cells[dst] == WHITE:
                            self.cells[dst] = GRAY
                        if dst not in seen:
                            stack.append(dst)
                # 2b. 仍为白 = 不可达垃圾, 宣判
                doomed = [o for o in self.live if self.cells[o] == WHITE]
                for o in doomed:
                    self.cells[o] = DOOMED
                self.cycle_doomed = len(doomed)
                self.cycle_freed = 0
                self.gc_phase = SWEEP_PHASE

            # 阶段 3: STW 内分块清除(线程依旧全部挂起, 前端可看扫线推进)
            i = 0
            total = len(doomed)
            while i < total:
                chunk_end = min(i + SWEEP_CHUNK, total)
                with self.lock:
                    for k in range(i, chunk_end):
                        o = doomed[k]
                        self.cells[o] = FREE
                        self.refs[o] = []
                        self.free.append(o)
                        self.sweep_cursor = o // GRID_W
                    self.cycle_freed += chunk_end - i
                    live = self.live
                    # 原地压缩存活表
                    wpos = 0
                    for o in live:
                        if self.cells[o] != FREE:
                            live[wpos] = o
                            wpos += 1
                    del live[wpos:]
                i = chunk_end
                time.sleep(SWEEP_CHUNK_SLEEP)

            with self.lock:
                self.gc_phase = IDLE_PHASE
                self.sweep_cursor = -1
                self.mark_trail.clear()
                self.alloc_since_gc = 0
                self.gc_count += 1
                self.last_pause_ms = (time.perf_counter() - t0) * 1000.0
                pause = self.last_pause_ms
                freed = self.cycle_freed
            log("GC#%d 完成: 清除 %d 个垃圾对象, STW 暂停 %.1f ms",
                self.gc_count, freed, pause)
        finally:
            # 无论如何必须放行, 杜绝挂死
            self.resume()

    # -------------------------------------------------- 快照
    def snapshot(self):
        # 两把锁各读一次但不嵌套: stw 状态仅在进入/退出 STW 的微秒级窗口可能错位,
        # 该窗口内以 heap 锁内读到的 phase 为准做一次交叉校正。
        with self.stw_cond:
            stw_flag = self.stw_flag
            parked = len(self.parked)
        with self.lock:
            trans = {FREE: 48, WHITE: 49, GRAY: 50, BLACK: 51, DOOMED: 52}
            grid = bytes(trans[c] for c in self.cells).decode("ascii")
            live_count = len(self.live)
            snap = {
                "w": GRID_W,
                "h": GRID_H,
                "grid": grid,
                "phase": self.gc_phase,
                "trail": list(self.mark_trail),
                "sweep_row": self.sweep_cursor,
                "_stw_flag": stw_flag,
                "_parked": parked,
                "live": live_count,
                "capacity": GRID_W * GRID_H,
                "freed": self.cycle_freed,
                "doomed": self.cycle_doomed,
                "scanned": self.mark_scanned,
                "gc_count": self.gc_count,
                "pause_ms": round(self.last_pause_ms, 1),
                "alloc_since": self.alloc_since_gc,
            }
        # STW 语义只覆盖重标记/清除阶段; 与 phase 交叉校正, 杜绝前端瞬时错帧
        stw = stw_flag and snap["phase"] in (REMARK_PHASE, SWEEP_PHASE)
        with self.stw_cond:
            if stw:
                parked_now = len(self.parked)
                stw_elapsed = (time.perf_counter() - self.stw_start) * 1000.0
            else:
                parked_now = 0
                stw_elapsed = 0.0
        snap["stw"] = stw
        snap["stw_ms"] = round(stw_elapsed, 1)
        snap["parked"] = parked_now
        snap["threads"] = MUTATOR_NUM
        return snap

# ---------------------------------------------------------------- 线程
def mutator_loop(heap, tid, stats):
    rng = random.Random(tid * 7919 + 13)
    bursts = 40  # 启动预热
    while True:
        heap.safepoint(tid)
        action = rng.random()
        if action < 0.26 or bursts > 0:
            heap.allocate(tid)
            if bursts > 0:
                bursts -= 1
        elif action < 0.85:
            heap.mutate_refs(tid)
        else:
            heap.churn_roots(tid)
        stats[tid] = stats.get(tid, 0) + 1
        time.sleep(rng.uniform(0.0025, 0.007))


def gc_loop(heap, manual_event, log):
    last = time.perf_counter()
    while True:
        if manual_event.wait(0.05):
            manual_event.clear()
        if time.perf_counter() - last >= GC_INTERVAL or manual_event.is_set():
            heap.gc_cycle(log)
            last = time.perf_counter()


# ---------------------------------------------------------------- 前端
PAGE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>GC Stop-The-World 仿真内核</title>
<style>
  html,body{margin:0;background:#0d1117;color:#c9d1d9;
    font-family:Consolas,"Courier New",monospace;font-size:13px}
  .wrap{display:flex;gap:16px;padding:14px}
  canvas{background:#010409;border:1px solid #30363d;border-radius:6px}
  #panel{width:300px}
  h1{font-size:16px;margin:0 0 10px;color:#58a6ff}
  .card{background:#161b22;border:1px solid #30363d;border-radius:6px;
    padding:10px 12px;margin-bottom:10px}
  .row{display:flex;justify-content:space-between;margin:3px 0}
  .k{color:#8b949e}.v{color:#e6edf3;font-weight:bold}
  button{width:100%;padding:8px;margin-top:6px;background:#238636;color:#fff;
    border:0;border-radius:6px;font-family:inherit;font-size:13px;cursor:pointer}
  button:hover{background:#2ea043}
  .legend i{display:inline-block;width:10px;height:10px;border-radius:2px;
    margin-right:6px;vertical-align:middle}
  .phase{font-size:15px;font-weight:bold}
  .dots span{display:inline-block;width:14px;height:14px;border-radius:50%;
    margin-right:6px;background:#3fb950;border:1px solid #1a7f37}
  .dots span.park{background:#f85149;border-color:#8b1a1a;animation:blink .5s infinite alternate}
  @keyframes blink{to{opacity:.35}}
</style>
</head>
<body>
<div class="wrap">
  <canvas id="cv" width="800" height="576" title="对象堆网格"></canvas>
  <div id="panel">
    <h1>GC STW 仿真内核</h1>
    <div class="card">
      <div class="row"><span class="k">GC 阶段</span><span id="phase" class="phase">—</span></div>
      <div class="row"><span class="k">本轮已扫描</span><span id="scanned" class="v">0</span></div>
      <div class="row"><span class="k">判定垃圾</span><span id="doomed" class="v">0</span></div>
      <div class="row"><span class="k">本轮已清除</span><span id="freed" class="v">0</span></div>
    </div>
    <div class="card">
      <div class="row"><span class="k">存活对象</span><span id="live" class="v">0</span></div>
      <div class="row"><span class="k">堆占用率</span><span id="occ" class="v">0%</span></div>
      <div class="row"><span class="k">累计分配(轮内)</span><span id="alloc" class="v">0</span></div>
      <div class="row"><span class="k">分配速率</span><span id="arate" class="v">0/s</span></div>
      <div class="row"><span class="k">GC 次数</span><span id="gcc" class="v">0</span></div>
      <div class="row"><span class="k">上次 STW 暂停</span><span id="pause" class="v">0 ms</span></div>
      <div class="row"><span class="k">当前 STW 已持续</span><span id="stwt" class="v">—</span></div>
      <div class="row" style="margin-top:6px"><span class="k">应用线程</span>
        <span class="dots" id="dots"></span></div>
    </div>
    <div class="card legend">
      <div><i style="background:#1f6f3f"></i>存活对象(白/黑标记)</div>
      <div><i style="background:#d29922"></i>灰色待扫描</div>
      <div><i style="background:#58a6ff"></i>已标记存活(黑)</div>
      <div><i style="background:#f85149"></i>垃圾 / 清除扫线</div>
      <div><i style="background:#8957e5"></i>扫描光标轨迹</div>
    </div>
    <button id="btngc">手动触发一次 GC</button>
  </div>
</div>
<script>
const cv = document.getElementById('cv'), ctx = cv.getContext('2d');
const SC = 8;
const COLORS = {
  48: [13,17,23],       // 空
  49: [31,111,63],      // 白(尚未扫到的存活)
  50: [210,153,34],     // 灰
  51: [88,166,255],     // 黑
  52: [248,81,73]       // 垃圾(待清除)
};
let img = ctx.createImageData(100,72), lastAlloc = 0, lastT = 0, emaRate = 0;

const $ = id => document.getElementById(id);
const dotsEl = $('dots');

function drawDots(n, parked, stw){
  let html = '';
  for(let i=0;i<n;i++){
    const p = stw && i < parked ? ' park' : '';
    html += '<span class="'+p+'" title="线程'+i+'"></span>';
  }
  dotsEl.innerHTML = html;
}

function poll(){
  fetch('/state?_=' + Date.now()).then(r => r.json()).then(s => {
    const W = s.w, H = s.h, d = img.data;
    for(let i=0;i<s.grid.length;i++){
      const c = COLORS[s.grid.charCodeAt(i)] || COLORS[48];
      const p = i*4;
      d[p]=c[0]; d[p+1]=c[1]; d[p+2]=c[2]; d[p+3]=255;
    }
    // 扫描光标轨迹: 紫色叠加(新到旧衰减)
    const tr = s.trail, seen = new Int8Array(W*H);
    for(let k=0;k<tr.length;k++){
      const idx = tr[k];
      if(seen[idx]) continue; seen[idx]=1;
      const a = 0.15 + 0.55*(k/tr.length);
      const p = idx*4;
      d[p]   = d[p]  *(1-a) + 137*a;
      d[p+1] = d[p+1]*(1-a) + 87*a;
      d[p+2] = d[p+2]*(1-a) + 229*a;
    }
    // 先把小位图放大到一个离屏画布
    const off = drawDots._off || (drawDots._off = document.createElement('canvas'));
    off.width = W; off.height = H;
    off.getContext('2d').putImageData(img,0,0);
    ctx.imageSmoothingEnabled = false;
    ctx.globalAlpha = 1;
    ctx.clearRect(0,0,cv.width,cv.height);
    ctx.drawImage(off,0,0,cv.width,cv.height);

    const stw = s.stw;
    if(stw){ // STW: 网格压暗 + 红色扫线
      ctx.fillStyle = 'rgba(0,0,0,0.45)';
      ctx.fillRect(0,0,cv.width,cv.height);
      if(s.sweep_row >= 0){
        ctx.fillStyle = 'rgba(248,81,73,0.9)';
        ctx.fillRect(0, s.sweep_row*SC, cv.width, SC);
      }
      ctx.fillStyle = '#f85149';
      ctx.font = 'bold 30px Consolas';
      ctx.textAlign = 'center';
      ctx.fillText('STW — 全部应用线程已挂起', cv.width/2, cv.height/2 - 8);
      ctx.font = '16px Consolas';
      ctx.fillStyle = '#ffa198';
      ctx.fillText('暂停 ' + s.stw_ms.toFixed(0) + ' ms · 已清除 ' + s.freed,
                   cv.width/2, cv.height/2 + 22);
      ctx.textAlign = 'left';
    }

    const names = {idle:'空闲(并发分配中)', mark:'并发标记中',
                   remark:'STW 重标记', sweep:'STW 清除中'};
    const phEl = $('phase');
    phEl.textContent = names[s.phase];
    phEl.style.color = stw ? '#f85149' : (s.phase==='mark' ? '#d29922' : '#3fb950');
    $('scanned').textContent = s.scanned;
    $('doomed').textContent = s.doomed;
    $('freed').textContent = s.freed;
    $('live').textContent = s.live;
    $('occ').textContent = (100*s.live/s.capacity).toFixed(1) + '%';
    $('alloc').textContent = s.alloc_since;
    const now = performance.now();
    if(lastT){
      const dt = (now-lastT)/1000, inst = (s.alloc_since-lastAlloc)/dt;
      emaRate = emaRate ? emaRate*0.8 + inst*0.2 : inst;
    }
    lastT = now; lastAlloc = s.alloc_since;
    $('arate').textContent = Math.max(0,Math.round(emaRate)) + '/s';
    $('gcc').textContent = s.gc_count;
    $('pause').textContent = s.pause_ms + ' ms';
    $('stwt').textContent = stw ? s.stw_ms.toFixed(1) + ' ms' : '—';
    drawDots(s.threads, s.parked, stw);
  }).catch(()=>{}).finally(()=>setTimeout(poll, 50));
}
$('btngc').onclick = () => fetch('/trigger',{method:'POST'}).catch(()=>{});
poll();
</script>
</body></html>
"""


# ---------------------------------------------------------------- HTTP
def make_handler(heap, manual_event):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _send(self, body, ctype):
            data = body.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            path = self.path.split("?", 1)[0]
            if path == "/":
                self._send(PAGE, "text/html; charset=utf-8")
            elif path == "/state":
                self._send(json.dumps(heap.snapshot()),
                           "application/json; charset=utf-8")
            else:
                self.send_error(404)

        def do_POST(self):
            if self.path.split("?", 1)[0] == "/trigger":
                manual_event.set()
                self._send(json.dumps({"ok": True}),
                           "application/json; charset=utf-8")
            else:
                self.send_error(404)

    return Handler


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    port = HTTP_PORT_DEFAULT
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            pass

    heap = Heap()
    manual_event = threading.Event()
    stats = {}

    def log(fmt, *args):
        print("[GC] " + fmt % args, flush=True)

    for tid in range(MUTATOR_NUM):
        threading.Thread(target=mutator_loop, args=(heap, tid, stats),
                         daemon=True, name="mutator-%d" % tid).start()
    threading.Thread(target=gc_loop, args=(heap, manual_event, log),
                     daemon=True, name="gc").start()

    server = ThreadingHTTPServer(("127.0.0.1", port),
                                 make_handler(heap, manual_event))
    url = "http://127.0.0.1:%d/" % port
    print("=" * 56)
    print(" GC STW 仿真内核已启动: %s" % url)
    print(" 堆网格 %dx%d · 应用线程 %d · GC 周期 %.1fs" %
          (GRID_W, GRID_H, MUTATOR_NUM, GC_INTERVAL))
    print(" 按 Ctrl+C 退出")
    print("=" * 56)
    if "--no-browser" not in sys.argv:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已退出。")


if __name__ == "__main__":
    main()