import json
import sys
import time
import urllib.request

BASE = "http://127.0.0.1:8765"


def get(path):
    with urllib.request.urlopen(BASE + path, timeout=5) as r:
        return json.loads(r.read().decode("utf-8"))


def post(body):
    req = urllib.request.Request(
        BASE + "/control", data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read().decode("utf-8"))


deadline = time.time() + 20
while time.time() < deadline:
    try:
        get("/state")
        break
    except Exception:
        time.sleep(0.2)
else:
    print("SERVER DID NOT COME UP")
    sys.exit(1)

time.sleep(0.5)
s0 = get("/state")
print("initial: occupied=%d alive=%d garbage=%d threads=%d"
      % (s0["occupied"], s0["alive"], s0["garbage"], len(s0["threads"])))

# 1) 手动触发 GC，采样整个周期，必须观察到 mark -> stw(remark/sweep) -> idle
post({"action": "gc"})
saw_mark = saw_stw = saw_parked = False
pause_max = 0.0
samples = 0
t_end = time.time() + 8
while time.time() < t_end:
    s = get("/state")
    samples += 1
    if s["phase"] == "concurrent_mark":
        saw_mark = True
    if s["stw"]:
        saw_stw = True
        if all(t["parked"] for t in s["threads"]):
            saw_parked = True
        pause_ms = (s["t"] - s["stw_begin"]) * 1000
        pause_max = max(pause_max, pause_ms)
    if s["phase"] == "idle" and s["cycles"] >= 1 and saw_stw:
        break
    time.sleep(0.015)

s1 = get("/state")
print("after GC: cycles=%d occupied=%d alive=%d garbage=%d last_freed=%d "
      "last_pause=%.0fms timeouts=%d samples=%d"
      % (s1["cycles"], s1["occupied"], s1["alive"], s1["garbage"],
         s1["last_freed"], s1["last_pause_ms"], s1["timeouts"], samples))
print("saw: mark=%s stw=%s all_parked_during_stw=%s peak_stw_age=%.0fms"
      % (saw_mark, saw_stw, saw_parked, pause_max))

# 2) 暂停：所有线程应在 ~1s 内 parked
post({"action": "pause", "value": True})
ok = False
for _ in range(50):
    s = get("/state")
    if all(t["parked"] for t in s["threads"]):
        ok = True
        break
    time.sleep(0.02)
print("pause parks all threads: %s" % ok)
post({"action": "pause", "value": False})

# 3 再等一个自动周期，确认系统持续运转、无超时累积
time.sleep(4.5)
s2 = get("/state")
print("later: cycles=%d total_alloc=%d total_freed=%d timeouts=%d phase=%s"
      % (s2["cycles"], s2["total_alloc"], s2["total_freed"],
         s2["timeouts"], s2["phase"]))

ok = (saw_mark and saw_stw and saw_parked and ok
      and s1["last_freed"] > 0 and s1["timeouts"] == 0
      and s2["total_alloc"] > s1.get("total_alloc", 0))
print("RESULT:", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)