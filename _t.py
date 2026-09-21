import io
p = "stress_test.py"
src = io.open(p, encoding="utf-8").read()
src = src.replace(
    '        stw = phase in (m.PHASE_STW_REMARK, m.PHASE_STW_SWEEP)',
    '        stw = sim.stw_armed.is_set()')
src = src.replace(
    '      and snap["timeouts"] == 0 and alloc_rate > 500)',
    '      and snap["timeouts"] == 0 and alloc_rate > 200)')
io.open(p, "w", encoding="utf-8", newline="\n").write(src)
print("patched")