#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gc_stw_sim.py —— 自研 VM 的 GC / Stop-The-World 并发仿真内核（单文件闭环）

仅依赖 Python 标准库：
  - threading        应用线程(mutator) / GC 线程 / 内置 HTTP 服务线程
  - http.server      渲染原生 HTML + Canvas 页面并推送仿真快照
  - json / base64    堆网格快照编码
  - webbrowser       一键打开演示页面

一、并发模型
  * N 个 mutator 线程：随机分配对象、改写引用边、丢弃 root，持续制造垃圾。
  * 1 个 GC 线程：定期执行「并发标记 -> STW 重标记 -> STW 清除」。
  * 任意数量的 HTTP 线程：只读拷贝快照，绝不阻塞后台线程。

二、并发图遍历的读写竞态处理（严格保守，保证不漏标）
  * heap_lock 是唯一的堆锁，保护对象表/引用边/mark 位/颜色/统计量。
  * SATB（Snapshot-At-The-Beginning）旧值写屏障：
        改写或删除出边、从 root 集合摘除对象之前，先把旧目标记入 satb 日志。
    于是并发标记开始那一刻「逻辑存活」的对象，即使引用随后被 mutator
    删光，也会被日志兜住并继续扫描。
  * 新增量屏障（incremental update）：标记期间新加入的出边，若目标未标记
    则立即入栈——这是比 SATB 更强的一层保守保护。
  * 标记期间新分配的对象一律立即标记（视为 mutator 的新 root）。
  * 最终标记收敛(remark)阶段所有 mutator 已在安全点挂起，把残留 SATB
    日志和标记栈一次性排空，杜绝「漏标 -> 活对象被当垃圾清除」。

三、STW 安全点协议（无死锁）
  * mutator 在每次操作的循环边界进入协作式安全点 safepoint()。
  * GC 通过 epoch + Condition + acks 集合挂起全部 mutator：
    epoch 推进并广播 -> mutator 在安全点登记 ack -> GC 收齐 ack 才开始清扫。
  * 锁序全局唯一：stop_cond 锁 -> heap_lock，从不反向获取；
    等待条件时不持有 heap_lock；所有等待均带超时并以 running 标志兜底，
    因此即使线程被异常中断也不会永久挂死。

四、前端（/）
  原生 HTML + Canvas，20Hz 轮询 /state，独立 rAF 渲染循环：
  存活对象 / 未抵达对象 / 垃圾对象 / 空槽，并发标记光标青色余晖，
  STW 清扫红色扫描线逐行压屏，分配与回收闪光，STW 红屏与停顿计时。
"""

import argparse
import base64
import json
import random
import threading
import time
import webbrowser
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse


# ----------------------------------------------------------------------------
# 网格 / 颜色 / GC 阶段常量
# ----------------------------------------------------------------------------
GRID_W = 80
GRID_H = 44
CELLS_N = GRID_W * GRID_H

# color 缓冲区取值（前端按同一约定上色）
C_EMPTY = 0      # 空槽
C_ALIVE = 1      # 已确认存活（已标记 / 空闲期对象）
C_UNREACHED = 2  # 已占用但本轮标记尚未抵达
C_GARBAGE = 3    # STW 重标记后判定为垃圾

# GC 扫描光标类型
CUR_MARK = 1     # 并发标记 / 重标记光标：青色
CUR_SWEEP = 2    # STW 清除扫描线：红色

PHASE_IDLE = "idle"
PHASE_MARK = "concurrent_mark"
PHASE_STW_REMARK = "stw_remark"
PHASE_STW_SWEEP = "stw_sweep"
PHASE_NAMES = {
    PHASE_IDLE: "运行中（无 GC）",
    PHASE_MARK: "并发标记",
    PHASE_STW_REMARK: "STW 重标记",
    PHASE_STW_SWEEP: "STW 清除",
}

ROOT_CAP = 6                 # 每个 mutator 保留的 root 上限
GC_FILL_TRIGGER = 0.92       # 占用率超过该值时不等周期，提前触发 GC
MAX_EDGES = 3                # 单个对象的出边上限
TRAIL_TTL = 1.2              # 光标轨迹保留时长（秒）


class Simulation(object):
    """堆 + GC + mutator 的并发仿真内核。"""

    def __init__(self, n_mutators=4, rate_total=500.0,
                 gc_interval=3.5, sweep_step=2, sweep_sleep=0.012,
                 prewarm=True):
        self.w = GRID_W
        self.h = GRID_H
        self.n = CELLS_N

        # 堆：cells[i] 为对象 i 的出边集合；None 表示空槽
        self.cells = [None] * self.n
        self.mark = bytearray(self.n)          # 标记位
        self.color = bytearray(self.n)         # 面向前端的显示状态
        self.pos_of = [-1] * self.n            # occupied 列表中的位置（O(1) 删除）
        self.occupied = []                     # 已占用槽位
        self.free = list(range(self.n))        # 空闲槽位
        random.shuffle(self.free)

        # 并发标记相关
        self.phase = PHASE_IDLE
        self.marking = False
        self.mark_stack = []
        self.satb = []                         # SATB 旧值日志
        self.trail = deque(maxlen=512)         # GC 光标轨迹 (idx, kind, t)

        # 锁
        self.heap_lock = threading.RLock()
        self.stop_lock = threading.Lock()
        self.stop_cond = threading.Condition(self.stop_lock)
        # STW 代际号：两者初始同为 -1，begin_stw 推进 stw_epoch，
        # mutator 发现 stw_epoch > epoch 即挂起；end_stw 让两者追平。
        self.epoch = -1
        self.stw_epoch = -1
        self.acks = set()

        # 运行控制
        self.running = True
        self.paused = False
        self.manual_gc = threading.Event()
        # STW 已生效（所有 mutator 已在安全点挂起）。
        # phase 只表示 GC 想进入的阶段；stw_armed 才是「世界已停止」。
        self.stw_armed = threading.Event()

        # 参数
        self.n_mutators = int(n_mutators)
        self.rate_total = float(rate_total)
        self.gc_interval = float(gc_interval)
        self.sweep_step = int(sweep_step)
        self.sweep_sleep = float(sweep_sleep)

        # 统计
        self.total_alloc = 0
        self.total_freed = 0
        self.total_cycles = 0
        self.last_freed = 0
        self.last_pause_ms = 0.0
        self.last_mark_ms = 0.0
        self.stw_count = 0
        self.stw_timeouts = 0
        self.gen = 1                          # 配置重置代际号（前端使用）
        self._last_cursor = None
        self._alloc_base = 0
        self._freed_base = 0
        self._stat_t = time.time()
        self.stw_begin_t = 0.0

        self.mutators = [Mutator(i, self) for i in range(self.n_mutators)]
        self.gc = GcThread(self)
        if prewarm:
            self.prewarm()

    # ------------------------------------------------------------------
    # 堆内部操作（调用方必须持有 heap_lock）
    # ------------------------------------------------------------------
    def _occupied_sample_locked(self, rng):
        if not self.occupied:
            return -1
        return self.occupied[rng.randrange(len(self.occupied))]

    def _alloc_locked(self, rng):
        """从空闲链表取一个槽，初始化为新对象，返回下标；堆满返回 -1。"""
        if not self.free:
            return -1
        idx = self.free.pop()
        edges = set()
        for _ in range(rng.randint(0, 2)):
            target = self._occupied_sample_locked(rng)
            if target >= 0 and target != idx and len(edges) < MAX_EDGES:
                edges.add(target)
        self.cells[idx] = edges
        self.pos_of[idx] = len(self.occupied)
        self.occupied.append(idx)
        # 标记期间分配：作为 mutator 新 root 立即标记
        if self.marking:
            self._mark_push_locked(idx)
            self.color[idx] = C_ALIVE
        else:
            self.color[idx] = C_ALIVE if self.phase == PHASE_IDLE else C_UNREACHED
        self.total_alloc += 1
        return idx

    def _free_locked(self, idx):
        self.cells[idx] = None
        self.mark[idx] = 0
        self.color[idx] = C_EMPTY
        self.free.append(idx)
        last = self.occupied.pop()
        if last != idx:
            pos = self.pos_of[idx]
            self.occupied[pos] = last
            self.pos_of[last] = pos
        self.pos_of[idx] = -1
        self.total_freed += 1

    def _mark_push_locked(self, idx):
        if idx < 0 or idx >= self.n or self.mark[idx]:
            return
        obj = self.cells[idx]
        if obj is None:
            return
        self.mark[idx] = 1
        self.mark_stack.append(idx)
        if self.phase == PHASE_STW_REMARK:
            self.color[idx] = C_ALIVE

    def _cursor_locked(self, idx, kind):
        now = time.time()
        key = (idx, kind)
        if self._last_cursor == key:
            return
        self._last_cursor = key
        self.trail.append((idx, kind, now))

    def _root_push(self, mutator, idx):
        """给某 mutator 挂 root，超出上限时丢弃最旧 root（走 SATB 屏障）。"""
        roots = mutator.roots
        roots.append(idx)
        if len(roots) > ROOT_CAP:
            old = roots.pop(0)
            if self.marking:
                self.satb.append(old)


    # ------------------------------------------------------------------
    # STW 安全点协议
    #
    # begin_stw()  仅由 GC 线程调用：推进 stw_epoch 并广播；
    #              然后等待「所有存活 mutator」在同一个 epoch 上登记 ack。
    # end_stw()    由 GC 线程调用：把当前 epoch 作废并广播放行。
    # safepoint()  仅由 mutator 在操作循环边界调用：发现自己落后于当前
    #              STW epoch 就登记 ack 并阻塞，直到 GC 放行。
    #
    # 等待全部基于 Condition + 超时 + running 标志，任何路径都不会
    # 永久等待；stop_cond 锁内绝不申请 heap_lock（GC 在 heap 工作完成
    # 释放 heap_lock 后才调用 end_stw），锁序恒定，不可能成环死锁。
    # ------------------------------------------------------------------
    def begin_stw(self, phase):
        """挂起全部 mutator。成功返回 True；关停或超时返回 False。

        关键约束：本方法必须在未持有 heap_lock 时调用——否则所有
        mutator 都堵在 heap_lock 上无法抵达安全点，GC 将自死锁。
        这里只做一次极短的加锁设置阶段，随即释放。
        """
        with self.heap_lock:
            self.phase = phase
            self.stw_begin_t = time.time()
        self.stw_armed.clear()
        with self.stop_cond:
            self.stw_epoch += 1
            epoch = self.stw_epoch
            # acks 必须从空集开始：只有 mutator 真正抵达安全点并自行登记后
            # 才算挂起。预先把线程放进集合会让 GC 在线程仍在改写堆时就
            # 开始清扫，是致命的协议错误。
            self.acks = set()
            self.stop_cond.notify_all()
            deadline = time.time() + 5.0
            while self.running:
                need = {(m.tid, epoch) for m in self.mutators
                        if m.alive and m is not threading.current_thread()}
                if need <= self.acks:
                    self.stw_count += 1
                    self.stw_armed.set()
                    return True
                left = deadline - time.time()
                if left <= 0:
                    # 兜底：真实 VM 里此处会进入安全点强制接管；
                    # 仿真里记一次超时并继续，保证系统永远可恢复。
                    self.stw_timeouts += 1
                    return False
                self.stop_cond.wait(timeout=min(0.05, left))
        return False

    def end_stw(self):
        self.stw_armed.clear()
        with self.stop_cond:
            self.epoch = self.stw_epoch
            self.acks = set()
            self.stop_cond.notify_all()

    def safepoint(self, mutator):
        """mutator 协作式安全点：暂停 / STW 时在此挂起。返回 False 表示应退出。"""
        parked = False
        with self.stop_cond:
            while self.running:
                epoch = self.stw_epoch
                want_park = self.paused or epoch > self.epoch
                if not want_park:
                    break
                if not parked:
                    self.acks.add((mutator.tid, epoch))
                    self.stop_cond.notify_all()
                    parked = True
                    mutator.parked = True
                self.stop_cond.wait(timeout=0.25)
        mutator.parked = False
        return self.running

    def start_threads(self):
        self.gc.start()
        for m in self.mutators:
            m.start()

    def shutdown(self):
        self.running = False
        self.manual_gc.set()
        with self.stop_cond:
            self.stop_cond.notify_all()
        for m in self.mutators:
            m.join(timeout=2.0)
        self.gc.join(timeout=2.0)

    # ------------------------------------------------------------------
    # 预热：开演前构造一张引用图——约 32% 存活（root 可达链），
    # 约 26% 无根垃圾（含环），第一次 GC 就能看到明显回收。
    # ------------------------------------------------------------------
    def prewarm(self):
        rng = random.Random(20260921)
        with self.heap_lock:
            live_target = int(self.n * 0.32)
            garbage_target = int(self.n * 0.26)
            live = []
            garbage = []
            for _ in range(live_target):
                idx = self._alloc_locked(rng)
                if idx < 0:
                    break
                live.append(idx)
            for _ in range(garbage_target):
                idx = self._alloc_locked(rng)
                if idx < 0:
                    break
                garbage.append(idx)
            # 存活对象串成链并少量交织引用
            for pos, idx in enumerate(live):
                edges = self.cells[idx]
                if pos + 1 < len(live):
                    edges.add(live[pos + 1])
                if pos + 3 < len(live) and rng.random() < 0.3:
                    edges.add(live[pos + 3])
            # 垃圾对象彼此引用并成环，保证「引用计数无法回收」的演示点
            for pos, idx in enumerate(garbage):
                edges = self.cells[idx]
                edges.add(garbage[(pos + 1) % len(garbage)])
                if rng.random() < 0.25 and len(garbage) > 2:
                    edges.add(garbage[rng.randrange(len(garbage))])
            # 存活链头挂到第一个 mutator 的 root（每 160 个挂一个）
            if self.mutators:
                anchor = self.mutators[0]
                for pos in range(0, len(live), 160):
                    anchor.roots.append(live[pos])

    # ------------------------------------------------------------------
    # 只读快照：HTTP 线程高频调用。
    # 只做一次短时加锁的内存拷贝，绝不与后台生命周期操作交叉，
    # 前端渲染完全基于返回的副本，后台继续高频并发不受影响。
    # ------------------------------------------------------------------
    def snapshot(self):
        now = time.time()
        with self.heap_lock:
            occupied = len(self.occupied)
            free_n = len(self.free)
            marked = sum(self.mark)
            phase = self.phase
            marking = self.marking
            colors = base64.b64encode(bytes(self.color)).decode("ascii")
            cutoff = now - TRAIL_TTL
            trail = [[idx, kind, round(ts, 3)]
                     for idx, kind, ts in self.trail if ts >= cutoff]
            total_alloc = self.total_alloc
            total_freed = self.total_freed
            last_freed = self.last_freed
            last_pause = self.last_pause_ms
            last_mark = self.last_mark_ms
            cycles = self.total_cycles
            timeouts = self.stw_timeouts
            stw_begin = self.stw_begin_t if phase in (
                PHASE_STW_REMARK, PHASE_STW_SWEEP) else 0.0
            dt = max(1e-6, now - self._stat_t)
            alloc_rate = (total_alloc - self._alloc_base) / dt
            freed_rate = (total_freed - self._freed_base) / dt
            self._alloc_base = total_alloc
            self._freed_base = total_freed
            self._stat_t = now
            threads = [{"name": m.name, "parked": m.parked,
                        "ops": m.ops} for m in self.mutators]
            paused = self.paused
            gen = self.gen
            armed = self.stw_armed.is_set()
        if phase == PHASE_IDLE:
            alive = occupied
            garbage_n = 0
        else:
            alive = marked
            garbage_n = occupied - marked
        return {
            "gen": gen,
            "t": round(now, 3),
            "w": self.w,
            "h": self.h,
            "colors": colors,
            "trail": trail,
            "phase": phase,
            "phase_name": PHASE_NAMES[phase],
            "stw": armed,
            "stw_begin": round(stw_begin, 3),
            "marking": marking,
            "paused": paused,
            "occupied": occupied,
            "free": free_n,
            "alive": alive,
            "garbage": garbage_n,
            "fill": occupied / float(self.n),
            "alloc_rate": round(alloc_rate, 1),
            "freed_rate": round(freed_rate, 1),
            "total_alloc": total_alloc,
            "total_freed": total_freed,
            "last_freed": last_freed,
            "last_pause_ms": round(last_pause, 1),
            "last_mark_ms": round(last_mark, 1),
            "cycles": cycles,
            "timeouts": timeouts,
            "threads": threads,
        }



class Mutator(threading.Thread):
    """应用线程：按目标速率分配对象 / 改引用图 / 丢 root。"""

    def __init__(self, tid, sim):
        super(Mutator, self).__init__(name="mutator-%d" % tid, daemon=True)
        self.tid = tid
        self.sim = sim
        self.roots = []
        self.rng = random.Random()
        self.parked = False
        self.alive = True
        self.ops = 0

    # 可被 STW / 暂停立即打断的操作间隔睡眠
    def _sleep_interruptible(self, seconds):
        end = time.time() + seconds
        while self.sim.running:
            left = end - time.time()
            if left <= 0:
                return
            # 热路径：绝大多数时候既没暂停也没 STW，只读一次布尔/整数
            # （CPython GIL 下原子可见）即可直接睡，避免每个操作都去
            # 争抢 stop_cond 锁——那会让所有 mutator 与 GC 互相拖慢。
            if not self.sim.paused and self.sim.stw_epoch <= self.sim.epoch:
                time.sleep(min(0.02, left))
                continue
            with self.sim.stop_cond:
                if self.sim.paused or self.sim.stw_epoch > self.sim.epoch:
                    return
                self.sim.stop_cond.wait(timeout=min(0.02, left))

    def run(self):
        sim = self.sim
        per_thread = max(1.0, sim.rate_total / max(1, sim.n_mutators))
        interval = 1.0 / per_thread
        while sim.running:
            if not sim.safepoint(self):
                break
            if sim.phase != PHASE_IDLE and sim.phase != PHASE_MARK:
                # STW 期间所有应用操作都被安全点挡住，这里只是双保险
                self._sleep_interruptible(0.01)
                continue
            roll = self.rng.random()
            if roll < 0.60:
                self.op_alloc()
            elif roll < 0.82:
                self.op_ref()
            else:
                self.op_drop()
            self.ops += 1
            self._sleep_interruptible(interval)
        self.alive = False

    def op_alloc(self):
        sim = self.sim
        with sim.heap_lock:
            idx = sim._alloc_locked(self.rng)
            if idx >= 0:
                sim._root_push(self, idx)

    def op_ref(self):
        """随机给一个已存活对象改写出边（SATB 记录旧值 + 新值即时标记）。"""
        sim = self.sim
        with sim.heap_lock:
            src = sim._occupied_sample_locked(self.rng)
            if src < 0:
                return
            edges = sim.cells[src]
            if edges:
                old = next(iter(edges))
                if sim.marking:
                    sim.satb.append(old)      # SATB：旧目标兜底
                edges.discard(old)
            dst = sim._occupied_sample_locked(self.rng)
            if dst >= 0 and dst != src and len(edges) < MAX_EDGES:
                edges.add(dst)
                if sim.marking and not sim.mark[dst]:
                    sim._mark_push_locked(dst)  # 增量更新屏障

    def op_drop(self):
        """丢弃一个本线程 root（旧值进 SATB 日志），是垃圾的主要来源。"""
        sim = self.sim
        with sim.heap_lock:
            if self.roots:
                pos = self.rng.randrange(len(self.roots))
                old = self.roots.pop(pos)
                if sim.marking:
                    sim.satb.append(old)


class GcThread(threading.Thread):
    """GC 线程：定期触发标记-清除周期。"""

    def __init__(self, sim):
        super(GcThread, self).__init__(name="gc", daemon=True)
        self.sim = sim
        self.rng = random.Random()

    def run(self):
        sim = self.sim
        while sim.running:
            # 等待「周期到 / 堆满 / 手动触发」三者最早发生
            wait_end = time.time() + sim.gc_interval
            while sim.running:
                if sim.manual_gc.is_set():
                    sim.manual_gc.clear()
                    break
                with sim.heap_lock:
                    fill = len(sim.occupied) / float(sim.n)
                if fill >= GC_FILL_TRIGGER:
                    break
                left = wait_end - time.time()
                if left <= 0:
                    break
                if sim.manual_gc.wait(timeout=min(0.1, max(0.0, left))):
                    sim.manual_gc.clear()
                    break
            if not sim.running:
                break
            with sim.heap_lock:
                if not sim.occupied:
                    continue
            self._cycle()

    # ------------------------------------------------------------------
    # 阶段 1：并发标记（mutator 照常运行，写屏障保证不漏标）
    # ------------------------------------------------------------------
    def _concurrent_mark(self):
        sim = self.sim
        t0 = time.time()
        with sim.heap_lock:
            sim.phase = PHASE_MARK
            sim.marking = True
            sim.mark_stack = []
            sim.satb = []
            sim.mark = bytearray(sim.n)
            # 标记开始瞬间，所有已占用对象先渲染为「未抵达」，
            # 随着光标的游走逐个翻成存活，形成波纹感
            for idx in sim.occupied:
                sim.color[idx] = C_UNREACHED
            # 初始 root 集合打标（所有 mutator 的 root）
            for m in sim.mutators:
                for r in m.roots:
                    sim._mark_push_locked(r)

        while sim.running:
            with sim.heap_lock:
                budget = 96
                while budget > 0 and sim.mark_stack:
                    idx = sim.mark_stack.pop()
                    obj = sim.cells[idx]
                    if obj is None or not sim.mark[idx]:
                        continue
                    sim.color[idx] = C_ALIVE
                    sim._cursor_locked(idx, CUR_MARK)
                    for ref in list(obj):
                        sim._mark_push_locked(ref)
                    budget -= 1
                # 并发期间积压的 SATB 旧值同样入栈
                while sim.satb:
                    sim._mark_push_locked(sim.satb.pop())
                drained = not sim.mark_stack and not sim.satb
            if drained:
                # 释放 heap_lock 后再观察一次日志：
                # 若恰好在退出瞬间有 mutator 追加旧值，下一轮再排空
                with sim.heap_lock:
                    if not sim.satb and not sim.mark_stack:
                        break
            time.sleep(0.005)
        sim.last_mark_ms = (time.time() - t0) * 1000.0

    # ------------------------------------------------------------------
    # 阶段 2 + 3：STW 重标记（收敛）与 STW 清除（逐行扫描线）
    # ------------------------------------------------------------------
    def _stw_remark_and_sweep(self):
        sim = self.sim
        t0 = time.time()
        ok = sim.begin_stw(PHASE_STW_REMARK)
        if not ok:
            # 关停中：复位标记状态，直接返回（end_stw 不调用，
            # 因为 begin_stw 失败时尚未生效）
            with sim.heap_lock:
                sim.marking = False
                sim.phase = PHASE_IDLE
            return
        try:
            # --- 重标记：mutator 全部挂起，图不再变化，一次排空 ---
            with sim.heap_lock:
                while sim.satb:
                    sim._mark_push_locked(sim.satb.pop())
                while sim.mark_stack:
                    idx = sim.mark_stack.pop()
                    obj = sim.cells[idx]
                    if obj is None or not sim.mark[idx]:
                        continue
                    sim.color[idx] = C_ALIVE
                    sim._cursor_locked(idx, CUR_MARK)
                    for ref in list(obj):
                        sim._mark_push_locked(ref)
                # 未标记的占用对象即垃圾：翻成琥珀红，给肉眼一个停顿
                for idx in sim.occupied:
                    if not sim.mark[idx]:
                        sim.color[idx] = C_GARBAGE
            time.sleep(0.12)  # 展示性停顿：垃圾全部翻红、分配完全静止

            # --- 清除：红色扫描线逐行压过整屏 ---
            with sim.heap_lock:
                sim.phase = PHASE_STW_SWEEP
                freed = 0
                to_free = [i for i in sim.occupied if not sim.mark[i]]
                garbage = set(to_free)
            row_start = 0
            step = max(1, sim.sweep_step)
            while row_start < sim.h and sim.running:
                row_end = min(row_start + step, sim.h)
                with sim.heap_lock:
                    lo = row_start * sim.w
                    hi = row_end * sim.w
                    for idx in to_free:
                        if lo <= idx < hi:
                            sim._free_locked(idx)
                            freed += 1
                    sim._cursor_locked(lo, CUR_SWEEP)
                row_start = row_end
                time.sleep(sim.sweep_sleep)

            # 清除悬空出边 / 悬空 root（对象已物理回收）
            with sim.heap_lock:
                for idx in sim.occupied:
                    edges = sim.cells[idx]
                    if edges:
                        dead = [e for e in edges if e in garbage]
                        for e in dead:
                            edges.discard(e)
                for m in sim.mutators:
                    if m.roots:
                        m.roots = [r for r in m.roots if r not in garbage]
                # 存活对象恢复常亮绿色
                for idx in sim.occupied:
                    sim.color[idx] = C_ALIVE
                sim.marking = False
                sim.mark_stack = []
                sim.satb = []
                sim.mark = bytearray(sim.n)
                sim.phase = PHASE_IDLE
                sim.total_cycles += 1
                sim.last_freed = freed
        finally:
            sim.end_stw()
        sim.last_pause_ms = (time.time() - t0) * 1000.0

    def _cycle(self):
        self._concurrent_mark()
        if not self.sim.running:
            with self.sim.heap_lock:
                self.sim.marking = False
                self.sim.phase = PHASE_IDLE
            return
        self._stw_remark_and_sweep()


    # ------------------------------------------------------------------
    # 预热：开演前构造一张引用图——约 32% 存活（root 可达链），
    # 约 26% 无根垃圾（含环），第一次 GC 就能看到明显回收。
HTML_PAGE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>GC Stop-The-World 仿真内核</title>
<style>
  :root {
    --bg: #0a0e14;
    --panel: #111722;
    --panel2: #0d131c;
    --line: #1e293b;
    --txt: #dbe6f5;
    --dim: #7d8ca3;
    --green: #34d399;
    --dim-green: #1d5c48;
    --amber: #f59e0b;
    --red: #ef4444;
    --cyan: #22d3ee;
  }
  * { box-sizing: border-box; }
  html, body {
    margin: 0; padding: 0; background: var(--bg); color: var(--txt);
    font-family: "Cascadia Code", Consolas, "Microsoft YaHei", sans-serif;
  }
  header {
    padding: 10px 18px; border-bottom: 1px solid var(--line);
    display: flex; align-items: baseline; gap: 14px; flex-wrap: wrap;
    background: linear-gradient(180deg, #0e1520, #0a0e14);
  }
  header h1 { font-size: 16px; margin: 0; letter-spacing: 1px; }
  header .sub { color: var(--dim); font-size: 12px; }
  #layout { display: flex; gap: 14px; padding: 14px 18px; align-items: flex-start; }
  .card {
    background: var(--panel); border: 1px solid var(--line);
    border-radius: 10px; padding: 12px 14px;
  }
  #stage-card { position: relative; flex: 1 1 auto; min-width: 0; }
  #side { width: 300px; flex: 0 0 300px; display: flex; flex-direction: column; gap: 12px; }
  #canvas-wrap { position: relative; border-radius: 8px; overflow: hidden;
    border: 1px solid #0a0f17; background: #070a0f; }
  canvas#grid { display: block; width: 100%; height: auto; }
  #stw-banner {
    position: absolute; inset: 0; pointer-events: none; opacity: 0;
    transition: opacity 90ms linear;
    box-shadow: inset 0 0 120px 18px rgba(239, 68, 68, 0.55);
    display: flex; align-items: flex-start; justify-content: center;
  }
  #stw-banner .badge {
    margin-top: 14px; padding: 8px 18px; border-radius: 999px;
    background: rgba(120, 12, 12, 0.82); border: 1px solid #ff6b6b;
    font-weight: 700; letter-spacing: 2px; font-size: 14px;
    box-shadow: 0 0 24px rgba(239, 68, 68, 0.7);
  }
  #phase-bar {
    display: flex; align-items: center; gap: 10px; margin-top: 10px;
    font-size: 12px; color: var(--dim);
  }
  #phase-dot { width: 9px; height: 9px; border-radius: 50%; background: var(--green);
    box-shadow: 0 0 8px var(--green); }
  .metric { display: flex; justify-content: space-between; font-size: 12px;
    padding: 3px 0; border-bottom: 1px dashed #1a2433; }
  .metric:last-child { border-bottom: 0; }
  .metric .v { color: #e8f1ff; font-variant-numeric: tabular-nums; }
  .section-title { font-size: 11px; color: var(--dim); letter-spacing: 2px;
    margin: 0 0 6px 2px; }
  canvas.spark { width: 100%; height: 56px; display: block;
    background: var(--panel2); border-radius: 6px; }
  .legend { display: grid; grid-template-columns: 1fr 1fr; gap: 4px 10px;
    font-size: 11.5px; color: var(--dim); }
  .legend span.dot { display: inline-block; width: 9px; height: 9px;
    border-radius: 2px; margin-right: 6px; vertical-align: -1px; }
  #threads { display: flex; flex-direction: column; gap: 5px; }
  .thr { display: flex; align-items: center; gap: 8px; font-size: 11.5px;
    color: var(--dim); }
  .thr .tdot { width: 8px; height: 8px; border-radius: 50%;
    background: var(--green); box-shadow: 0 0 6px var(--green); }
  .thr.parked .tdot { background: var(--red); box-shadow: 0 0 6px var(--red); }
  .thr.parked { color: #f0a8a8; }
  .thr .ops { margin-left: auto; font-variant-numeric: tabular-nums; }
  .btns { display: flex; gap: 8px; }
  button {
    flex: 1; padding: 8px 0; border-radius: 7px; cursor: pointer;
    border: 1px solid #2a3a52; background: #16202e; color: var(--txt);
    font-size: 12.5px; font-family: inherit;
  }
  button:hover { background: #1c2a3d; }
  button.primary { background: #0f2c26; border-color: #1f6b55; color: #7ef0c4; }
  button.warn { background: #331414; border-color: #7a2a2a; color: #ff9c9c; }
  .slider-row { display: flex; align-items: center; gap: 8px; font-size: 11.5px;
    color: var(--dim); margin: 5px 0; }
  .slider-row input[type=range] { flex: 1; }
  .slider-row .sv { width: 52px; text-align: right; color: #cfe0f5;
    font-variant-numeric: tabular-nums; }
  footer { padding: 4px 18px 18px; color: var(--dim); font-size: 11.5px;
    line-height: 1.7; }
  footer code { color: #9fd6c2; }
</style>
</head>
<body>
<header>
  <h1>GC / STOP-THE-WORLD 并发标记-清除仿真</h1>
  <span class="sub">纯 Python 标准库 · 单堆锁 + SATB 写屏障 + epoch 安全点 ·
  网格 80×44 = 3520 个对象槽</span>
</header>
<div id="layout">
  <div class="card" id="stage-card">
    <div id="canvas-wrap">
      <canvas id="grid" width="800" height="440"></canvas>
      <div id="stw-banner"><div class="badge" id="stw-badge">STW · ALL THREADS PARKED</div></div>
    </div>
    <div id="phase-bar">
      <span id="phase-dot"></span>
      <span id="phase-name">启动中…</span>
      <span style="margin-left:auto">STW 已停顿：<b id="stw-timer"
        style="color:var(--red);font-variant-numeric:tabular-nums">0 ms</b></span>
      <span>· 上次暂停 <b id="last-pause" style="color:#ffb4b4">0 ms</b></span>
    </div>
  </div>
  <div id="side">
    <div class="card">
      <p class="section-title">实时指标</p>
      <div class="metric"><span>占用 / 空闲</span><span class="v" id="m-fill">-</span></div>
      <div class="metric"><span>存活对象</span><span class="v" id="m-alive"
        style="color:var(--green)">-</span></div>
      <div class="metric"><span>垃圾对象</span><span class="v" id="m-garbage"
        style="color:var(--amber)">-</span></div>
      <div class="metric"><span>分配速率</span><span class="v" id="m-alloc">-</span></div>
      <div class="metric"><span>本轮回收</span><span class="v" id="m-freed">-</span></div>
      <div class="metric"><span>GC 周期 / 安全点超时</span><span class="v" id="m-cycles">-</span></div>
      <canvas class="spark" id="spark-alloc"></canvas>
    </div>
    <div class="card">
      <p class="section-title">应用线程</p>
      <div id="threads"></div>
    </div>
    <div class="card">
      <p class="section-title">控制</p>
      <div class="btns" style="margin-bottom:8px">
        <button class="primary" id="btn-pause">暂停应用</button>
        <button id="btn-gc">立即触发 GC</button>
      </div>
      <div class="slider-row">线程数
        <input type="range" id="s-threads" min="1" max="8" step="1" value="4">
        <span class="sv" id="v-threads">4</span></div>
      <div class="slider-row">分配速率
        <input type="range" id="s-rate" min="50" max="2000" step="50" value="500">
        <span class="sv" id="v-rate">500</span></div>
      <div class="slider-row">GC 周期(s)
        <input type="range" id="s-gc" min="1" max="10" step="0.5" value="3.5">
        <span class="sv" id="v-gc">3.5</span></div>
      <div class="slider-row">STW 扫描速度
        <input type="range" id="s-sweep" min="1" max="6" step="1" value="2">
        <span class="sv" id="v-sweep">2</span></div>
      <div class="btns"><button class="warn" id="btn-reset">应用参数并重置堆</button></div>
    </div>
    <div class="card">
      <p class="section-title">图例</p>
      <div class="legend">
        <span><span class="dot" style="background:var(--green)"></span>存活对象</span>
        <span><span class="dot" style="background:var(--dim-green)"></span>未抵达对象</span>
        <span><span class="dot" style="background:var(--amber)"></span>垃圾(STW)</span>
        <span><span class="dot" style="background:#1b2433"></span>空闲槽</span>
        <span><span class="dot" style="background:var(--cyan)"></span>标记光标</span>
        <span><span class="dot" style="background:var(--red)"></span>清除扫描线</span>
      </div>
    </div>
  </div>
</div>
<footer>
  并发正确性：所有堆结构由单把 <code>heap_lock</code> 保护；删边/丢 root 先走
  <code>SATB</code> 旧值屏障，新增边走增量标记，标记期分配即标记，最终在
  STW 安全点收敛——因此任何图遍历读写竞态都不会漏标存活对象。
  STW 协议：<code>epoch + Condition + acks</code>，mutator 在操作边界的协作安全点挂起，
  锁序恒定（stop_cond → heap_lock）且所有等待带超时，系统无死锁。
  观察点：青色光标游走时分配仍在继续；红屏出现后整屏闪光完全静止，红色扫描线逐行压过，
  结束瞬间分配闪光同时恢复——那一下「冻结→释放」就是 Stop-The-World 卡顿。
</footer>
<script>
"use strict";

var CELL = 10;
var gridC = document.getElementById("grid");
var gx = gridC.getContext("2d");
var sparkC = document.getElementById("spark-alloc");
var sx = sparkC.getContext("2d");

var state = null;
var prevColors = null;
var flashes = [];        // 新分配闪光 {idx,t}
var freedFx = [];        // 回收扩散环 {idx,t}
var stwActive = false;
var stwBegin = 0;
var rateHist = [];
var stwHist = [];
var paused = false;

var COLORS = {
  0: null,
  1: "#34d399",
  2: "#1d5c48",
  3: "#f59e0b"
};

var B64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
var B64_LOOKUP = (function () {
  var o = {};
  for (var i = 0; i < B64.length; i++) o[B64.charAt(i)] = i;
  return o;
})();

function decodeColors(str) {
  var out = new Uint8Array(Math.floor(str.length / 4) * 3);
  var p = 0;
  for (var i = 0; i < str.length; i += 4) {
    var a = B64_LOOKUP[str.charAt(i)];
    var b = B64_LOOKUP[str.charAt(i + 1)];
    var c = B64_LOOKUP[str.charAt(i + 2)];
    var d = B64_LOOKUP[str.charAt(i + 3)];
    var n = (a << 18) | (b << 12) | (c << 6) | d;
    out[p++] = (n >> 16) & 255;
    out[p++] = (n >> 8) & 255;
    out[p++] = n & 255;
  }
  return out;
}

function postAction(body) {
  fetch("/control", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify(body)
  }).catch(function () {});
}

function poll() {
  fetch("/state", {cache: "no-store"})
    .then(function (r) { return r.json(); })
    .then(function (s) {
      if (state && s.gen !== state.gen) {
        prevColors = null;
        flashes = [];
        freedFx = [];
      }
      var colors = decodeColors(s.colors);
      var now = s.t;
      if (prevColors && prevColors.length === colors.length) {
        for (var i = 0; i < colors.length; i++) {
          var o = prevColors[i], n = colors[i];
          if (o === n) continue;
          if (n === 1 || (n === 2 && (o === 0))) {
            if (o === 0 && n === 1) flashes.push({idx: i, t: now});
          }
          if (n === 0 && o !== 0) freedFx.push({idx: i, t: now});
        }
      }
      prevColors = colors;
      s._colors = colors;
      state = s;
      frame._clientAt = (typeof performance !== "undefined" ? performance.now() : Date.now()) / 1000;
      if (s.stw && !stwActive) { stwActive = true; stwBegin = s.stw_begin || s.t; }
      if (!s.stw) stwActive = false;
      rateHist.push({t: now, v: s.alloc_rate, stw: s.stw});
      while (rateHist.length > 160) rateHist.shift();
    })
    .catch(function () {})
    .then(function () { setTimeout(poll, 50); });
}

var bgCanvas = document.createElement("canvas");
bgCanvas.width = gridC.width;
bgCanvas.height = gridC.height;
(function paintBase() {
  var b = bgCanvas.getContext("2d");
  b.fillStyle = "#070a0f";
  b.fillRect(0, 0, gridC.width, gridC.height);
  b.strokeStyle = "rgba(42,58,82,0.18)";
  b.lineWidth = 1;
  for (var x = 0; x <= gridC.width; x += CELL) {
    b.beginPath(); b.moveTo(x + 0.5, 0); b.lineTo(x + 0.5, gridC.height); b.stroke();
  }
  for (var y = 0; y <= gridC.height; y += CELL) {
    b.beginPath(); b.moveTo(0, y + 0.5); b.lineTo(gridC.width, y + 0.5); b.stroke();
  }
})();

function drawGrid(nowSec) {
  if (!state) return;
  var s = state;
  var w = s.w, h = s.h;
  gx.drawImage(bgCanvas, 0, 0);

  var colors = s._colors;
  var i, x, y;
  for (i = 0; i < colors.length; i++) {
    var c = COLORS[colors[i]];
    if (!c) continue;
    x = (i % w) * CELL;
    y = Math.floor(i / w) * CELL;
    gx.fillStyle = c;
    gx.fillRect(x + 1, y + 1, CELL - 2, CELL - 2);
  }

  // GC 光标轨迹（按年龄衰减）
  var trail = s.trail;
  var newestMark = null, newestSweep = null;
  for (var k = 0; k < trail.length; k++) {
    var e = trail[k];
    var idx = e[0], kind = e[1], ts = e[2];
    var age = nowSec - ts;
    if (age < 0 || age > 1.2) continue;
    var alpha = 1.0 - age / 1.2;
    x = (idx % w) * CELL + CELL / 2;
    y = Math.floor(idx / w) * CELL + CELL / 2;
    if (kind === 2) {
      // 清除扫描线：整条横线
      gx.strokeStyle = "rgba(239,68,68," + (0.25 + 0.65 * alpha) + ")";
      gx.lineWidth = 2;
      gx.beginPath();
      gx.moveTo(0, y);
      gx.lineTo(gridC.width, y);
      gx.stroke();
      if (!newestSweep || ts > newestSweep[2]) newestSweep = e;
    } else {
      var rad = 1 + (1 - alpha) * 5;
      gx.fillStyle = "rgba(34,211,238," + (0.12 + 0.5 * alpha) + ")";
      gx.beginPath();
      gx.arc(x, y, rad, 0, Math.PI * 2);
      gx.fill();
      if (!newestMark || ts > newestMark[2]) newestMark = e;
    }
  }
  if (newestMark) {
    i = newestMark[0];
    gx.strokeStyle = "rgba(165,243,252,0.95)";
    gx.lineWidth = 1.5;
    gx.strokeRect((i % w) * CELL + 0.5, Math.floor(i / w) * CELL + 0.5,
                  CELL - 1, CELL - 1);
  }

  // 新分配：淡绿色呼吸闪光
  flashes = flashes.filter(function (f) { return nowSec - f.t < 0.55; });
  for (var fi = 0; fi < flashes.length; fi++) {
    var f = flashes[fi];
    var fa = 1 - (nowSec - f.t) / 0.55;
    x = (f.idx % w) * CELL;
    y = Math.floor(f.idx / w) * CELL;
    gx.fillStyle = "rgba(110,231,183," + (0.55 * fa) + ")";
    gx.fillRect(x, y, CELL, CELL);
  }

  // 回收：橙色扩散环
  freedFx = freedFx.filter(function (f) { return nowSec - f.t < 0.6; });
  for (var ri = 0; ri < freedFx.length; ri++) {
    var f2 = freedFx[ri];
    var age2 = nowSec - f2.t;
    var rr = 2 + age2 * 12;
    x = (f2.idx % w) * CELL + CELL / 2;
    y = Math.floor(f2.idx / w) * CELL + CELL / 2;
    gx.strokeStyle = "rgba(251,146,60," + (0.8 * (1 - age2 / 0.6)) + ")";
    gx.lineWidth = 1.2;
    gx.beginPath();
    gx.arc(x, y, rr, 0, Math.PI * 2);
    gx.stroke();
  }
}

function drawSpark() {
  var W = sparkC.width = sparkC.clientWidth * (window.devicePixelRatio || 1);
  var H = sparkC.height = 56 * (window.devicePixelRatio || 1);
  sx.clearRect(0, 0, W, H);
  if (rateHist.length < 2) return;
  var maxV = 60;
  for (var i = 0; i < rateHist.length; i++) {
    if (rateHist[i].v > maxV) maxV = rateHist[i].v;
  }
  var n = rateHist.length;
  // STW 区间红底
  sx.fillStyle = "rgba(239,68,68,0.16)";
  var segStart = -1;
  for (i = 0; i <= n; i++) {
    var isStw = i < n && rateHist[i].stw;
    if (isStw && segStart < 0) segStart = i;
    if (!isStw && segStart >= 0) {
      sx.fillRect(segStart / n * W, 0, (i - segStart) / n * W, H);
      segStart = -1;
    }
  }
  sx.strokeStyle = "#34d399";
  sx.lineWidth = 1.5 * (window.devicePixelRatio || 1);
  sx.beginPath();
  for (i = 0; i < n; i++) {
    var px = i / (n - 1) * W;
    var py = H - (rateHist[i].v / maxV) * (H - 6) - 3;
    if (i === 0) sx.moveTo(px, py); else sx.lineTo(px, py);
  }
  sx.stroke();
}

var el = function (id) { return document.getElementById(id); };

function updateHud(nowSec) {
  if (!state) return;
  var s = state;
  el("phase-name").textContent = s.phase_name +
    (s.paused ? "（已手动暂停）" : "");
  var dot = el("phase-dot");
  if (s.stw) { dot.style.background = "#ef4444"; dot.style.boxShadow = "0 0 10px #ef4444"; }
  else if (s.phase === "concurrent_mark") {
    dot.style.background = "#22d3ee"; dot.style.boxShadow = "0 0 10px #22d3ee";
  } else { dot.style.background = "#34d399"; dot.style.boxShadow = "0 0 8px #34d399"; }

  el("m-fill").textContent = s.occupied + " / " + s.free +
    "  (" + (s.fill * 100).toFixed(1) + "%)";
  el("m-alive").textContent = s.alive;
  el("m-garbage").textContent = s.garbage;
  el("m-alloc").textContent = s.alloc_rate.toFixed(0) + " obj/s";
  el("m-freed").textContent = s.last_freed +
    "（累计 " + s.total_freed + "）";
  el("m-cycles").textContent = s.cycles + " / " + s.timeouts;
  el("last-pause").textContent = s.last_pause_ms.toFixed(0) + " ms";

  var timer = el("stw-timer");
  if (s.stw) {
    var ms = Math.max(0, (nowSec - (s.stw_begin || nowSec)) * 1000);
    timer.textContent = ms.toFixed(0) + " ms";
  } else {
    timer.textContent = "0 ms";
  }
  el("stw-banner").style.opacity = s.stw ? "1" : "0";
  el("stw-badge").textContent = s.phase === "stw_sweep"
    ? "STW · SWEEPING（分配完全静止）"
    : "STW · REMARK（所有应用线程已挂起）";

  var html = "";
  for (var i = 0; i < s.threads.length; i++) {
    var t = s.threads[i];
    html += "<div class='thr" + (t.parked ? " parked" : "") + "'>" +
      "<span class='tdot'></span><span>" + t.name + "</span>" +
      "<span style='color:#5a6b82'>" + (t.parked ? "parked @ safepoint" : "running") +
      "</span><span class='ops'>" + t.ops + " ops</span></div>";
  }
  el("threads").innerHTML = html;
}

function frame() {
  var clientNow = (typeof performance !== "undefined" ? performance.now() : Date.now()) / 1000;
  // 后端时戳与客户端时钟存在固定偏移，这里换算成「服务器时基」，
  // 保证轨迹衰减、闪光和 STW 计时与后端快照同一时基。
  var t = state ? (state.t + (clientNow - frame._clientAt)) : clientNow;
  drawGrid(t);
  updateHud(t);
  drawSpark();
  requestAnimationFrame(frame);
}

el("btn-pause").addEventListener("click", function () {
  paused = !paused;
  el("btn-pause").textContent = paused ? "恢复应用" : "暂停应用";
  postAction({action: "pause", value: paused});
});
el("btn-gc").addEventListener("click", function () {
  postAction({action: "gc"});
});

function bindSlider(id, vid, suffix) {
  var input = el(id), label = el(vid);
  input.addEventListener("input", function () {
    label.textContent = input.value + (suffix || "");
  });
}
bindSlider("s-threads", "v-threads");
bindSlider("s-rate", "v-rate");
bindSlider("s-gc", "v-gc");
bindSlider("s-sweep", "v-sweep");

el("btn-reset").addEventListener("click", function () {
  var gcSec = parseFloat(el("s-gc").value);
  postAction({
    action: "reset",
    n_mutators: parseInt(el("s-threads").value, 10),
    rate_total: parseFloat(el("s-rate").value),
    gc_interval: gcSec,
    sweep_step: parseInt(el("s-sweep").value, 10),
    // 扫描行数越快（步长大），行间 sleep 越短；统一换算给后端
    sweep_sleep: +(0.014 - (parseInt(el("s-sweep").value, 10) - 1) * 0.002).toFixed(3)
  });
});

poll();
requestAnimationFrame(frame);
</script>
</body>
</html>
"""


class SimServer(object):
    """内置 HTTP 服务：/ 返回原生页面，/state 返回快照，/control 接收控制。"""

    def __init__(self, host, port, params, open_browser=True):
        self.host = host
        self.port = port
        self.params = dict(params)
        self.open_browser = open_browser
        self.swap_lock = threading.Lock()
        self.sim = Simulation(**self.params)
        self.sim.start_threads()

        handler = self._make_handler()
        self.httpd = ThreadingHTTPServer((host, port), handler)
        self.port = self.httpd.server_address[1]
        self.httpd.daemon_threads = True
        self.thread = threading.Thread(target=self.httpd.serve_forever,
                                       name="http", daemon=True)

    def _make_handler(self):
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, code, body, ctype="application/json; charset=utf-8"):
                data = body if isinstance(body, bytes) else body.encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                path = urlparse(self.path).path
                if path in ("/", "/index.html"):
                    self._send(200, HTML_PAGE, "text/html; charset=utf-8")
                elif path == "/state":
                    with outer.swap_lock:
                        sim = outer.sim
                    try:
                        payload = sim.snapshot()
                    except Exception as exc:  # 快照绝不能打挂服务
                        payload = {"error": str(exc)}
                    self._send(200, json.dumps(payload, ensure_ascii=False))
                elif path == "/favicon.ico":
                    self._send(204, b"")
                else:
                    self._send(404, json.dumps({"error": "not found"}))

            def do_POST(self):
                path = urlparse(self.path).path
                if path != "/control":
                    self._send(404, json.dumps({"error": "not found"}))
                    return
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b"{}"
                try:
                    data = json.loads(raw.decode("utf-8") or "{}")
                except Exception:
                    data = {}
                action = str(data.get("action", ""))
                with outer.swap_lock:
                    sim = outer.sim
                    if action == "pause":
                        sim.paused = bool(data.get("value", True))
                        with sim.stop_cond:
                            sim.stop_cond.notify_all()
                    elif action == "gc":
                        sim.manual_gc.set()
                    elif action == "reset":
                        params = dict(outer.params)
                        for key in ("n_mutators", "rate_total",
                                    "gc_interval", "sweep_step", "sweep_sleep"):
                            if key in data:
                                params[key] = data[key]
                        params["n_mutators"] = int(params["n_mutators"])
                        params["rate_total"] = float(params["rate_total"])
                        params["gc_interval"] = float(params["gc_interval"])
                        params["sweep_step"] = int(params["sweep_step"])
                        params["sweep_sleep"] = float(params["sweep_sleep"])
                        outer.params = params
                        old = outer.sim
                        new_sim = Simulation(**params)
                        outer.sim = new_sim
                        new_sim.start_threads()
                        old.shutdown()
                    result = {"ok": True}
                self._send(200, json.dumps(result, ensure_ascii=False))

        return Handler

    def serve(self):
        url = "http://%s:%d/" % ("127.0.0.1" if self.host in ("0.0.0.0", "")
                                 else self.host, self.port)
        print("=" * 64)
        print(" GC STW simulation kernel is running")
        print(" open: %s" % url)
        print(" press Ctrl+C to stop")
        print("=" * 64)
        self.thread.start()
        if self.open_browser:
            threading.Timer(0.8, lambda: webbrowser.open(url)).start()
        try:
            while True:
                time.sleep(0.5)
        except KeyboardInterrupt:
            print("\nshutting down ...")
        finally:
            with self.swap_lock:
                self.sim.shutdown()
            self.httpd.shutdown()


def main():
    parser = argparse.ArgumentParser(
        description="GC Stop-The-World concurrent mark-sweep simulation "
                    "(stdlib only).")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--threads", type=int, default=4,
                        help="number of mutator threads")
    parser.add_argument("--rate", type=float, default=500.0,
                        help="total mutator operations per second")
    parser.add_argument("--gc-interval", type=float, default=3.5,
                        help="GC cycle interval in seconds")
    parser.add_argument("--sweep-step", type=int, default=2,
                        help="grid rows swept per STW batch")
    parser.add_argument("--sweep-sleep", type=float, default=0.012,
                        help="sleep seconds between STW sweep batches")
    parser.add_argument("--no-browser", action="store_true",
                        help="do not open the browser automatically")
    args = parser.parse_args()

    params = {
        "n_mutators": args.threads,
        "rate_total": args.rate,
        "gc_interval": args.gc_interval,
        "sweep_step": args.sweep_step,
        "sweep_sleep": args.sweep_sleep,
    }
    server = SimServer(args.host, args.port, params,
                       open_browser=not args.no_browser)
    server.serve()


if __name__ == "__main__":
    main()
