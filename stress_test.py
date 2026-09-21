import importlib.util
import time

spec = importlib.util.spec_from_file_location("gcst", "gc_stw_sim.py")
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

sim = m.Simulation(n_mutators=8, rate_total=2000.0, gc_interval=0.8,
                   sweep_step=1, sweep_sleep=0.004)
sim.start_threads()

deadline = time.time() + 30
violations = 0
samples = 0
busy_ops_0 = sum(x.ops for x in sim.mutators)
busy_t = 0.0
busy_ops = 0
last_t = time.time()
last_armed = False

while time.time() < deadline:
    with sim.heap_lock:
        armed = sim.stw_armed.is_set()
    now = time.time()
    dt = now - last_t
    last_t = now
    if not armed and not sim.paused:
        busy_t += dt
    if armed:
        with sim.heap_lock:
            running = [x for x in sim.mutators if not x.parked]
        if running:
            violations += 1
    samples += 1
    time.sleep(0.002)

busy_ops = sum(x.ops for x in sim.mutators) - busy_ops_0
snap = sim.snapshot()
with sim.heap_lock:
    conserv = len(sim.occupied) + len(sim.free) == sim.n
    ok_occ = all(sim.cells[i] is not None for i in sim.occupied)
    ok_free = all(sim.cells[i] is None for i in sim.free)
    dangling = sum(1 for i in sim.occupied for e in sim.cells[i]
                   if sim.cells[e] is None)
    droot = sum(1 for mu in sim.mutators for r in mu.roots
                if sim.cells[r] is None)
effective = busy_ops / busy_t if busy_t > 0 else 0

print("cycles=%d alloc=%d freed=%d violations=%d/%d"
      % (snap["cycles"], snap["total_alloc"], snap["total_freed"],
         violations, samples))
print("conservation=%s occ_ok=%s free_ok=%s dangling=%d droot=%d"
      % (conserv, ok_occ, ok_free, dangling, droot))
print("effective non-STW throughput=%.0f ops/s over %.1fs busy, timeouts=%d"
      % (effective, busy_t, snap["timeouts"]))

ok = (snap["cycles"] >= 20 and violations == 0 and conserv and ok_occ
      and ok_free and dangling == 0 and droot == 0
      and snap["timeouts"] == 0 and effective > 800)
print("RESULT:", "PASS" if ok else "FAIL")
sim.shutdown()
raise SystemExit(0 if ok else 1)