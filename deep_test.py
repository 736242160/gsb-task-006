import importlib.util
import threading
import time

spec = importlib.util.spec_from_file_location("gcst", "gc_stw_sim.py")
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

sim = m.Simulation(n_mutators=4, rate_total=800, gc_interval=1000.0)
sim.start_threads()
time.sleep(0.4)

# 直接驱动一个 GC 周期，并在 STW 窗口内高频检查 parked
results = {"saw_stw": False, "all_parked": False, "max_running": 0,
           "phases": set()}
stop = threading.Event()


def watcher():
    while not stop.is_set():
        phase = sim.phase
        results["phases"].add(phase)
        if phase in (m.PHASE_STW_REMARK, m.PHASE_STW_SWEEP):
            results["saw_stw"] = True
            running = [x for x in sim.mutators if not x.parked]
            results["max_running"] = max(results["max_running"], len(running))
            if not running:
                results["all_parked"] = True
        time.sleep(0.001)


t = threading.Thread(target=watcher)
t.start()
sim.gc._cycle()
stop.set()
t.join()

snap = sim.snapshot()
ok = (results["saw_stw"] and results["all_parked"]
      and results["max_running"] == 0
      and m.PHASE_STW_SWEEP in results["phases"]
      and snap["timeouts"] == 0 and snap["last_freed"] > 0)
print("phases seen:", sorted(results["phases"]))
print("saw_stw=%s all_parked_at_some_point=%s max_mutators_not_parked=%d"
      % (results["saw_stw"], results["all_parked"], results["max_running"]))
print("last_freed=%d last_pause=%.1fms timeouts=%d"
      % (snap["last_freed"], snap["last_pause_ms"], snap["timeouts"]))

# 连续 5 个周期：检查无死锁、活对象绝不被误回收（root 链保持可达）
frees = []
for _ in range(5):
    sim.gc._cycle()
    with sim.heap_lock:
        for r in list(sim.mutators[0].roots):
            assert sim.cells[r] is not None, "live root got freed!"
    frees.append(sim.last_freed)
print("5 more cycles freed:", frees)
print("RESULT:", "PASS" if ok else "FAIL")
sim.shutdown()
raise SystemExit(0 if ok else 1)