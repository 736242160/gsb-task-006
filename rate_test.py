import importlib.util, time
spec = importlib.util.spec_from_file_location("gcst", "gc_stw_sim.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)

# 极端长 GC 周期 -> 实际不会进入 GC
for n in (4, 8):
    sim = m.Simulation(n_mutators=n, rate_total=2000.0, gc_interval=1e9)
    sim.start_threads()
    t0 = time.time(); ops0 = sum(x.ops for x in sim.mutators)
    time.sleep(5)
    ops1 = sum(x.ops for x in sim.mutators)
    dt = time.time() - t0
    print("threads=%d observed=%.0f ops/s phase=%s"
          % (n, (ops1-ops0)/dt, sim.phase))
    sim.shutdown()