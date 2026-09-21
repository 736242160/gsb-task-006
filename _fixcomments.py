import io
path = "gc_stw_sim.py"
with io.open(path, "r", encoding="utf-8") as f:
    src = f.read()

old = '''        self.gc.join(timeout=2.0)



    # ------------------------------------------------------------------
    def prewarm(self):'''

new = '''        self.gc.join(timeout=2.0)

    # ------------------------------------------------------------------
    # 预热：开演前构造一张引用图——约 32% 存活（root 可达链），
    # 约 26% 无根垃圾（含环），第一次 GC 就能看到明显回收。
    # ------------------------------------------------------------------
    def prewarm(self):'''

assert old in src, "prewarm anchor not found"
src = src.replace(old, new, 1)

old2 = '''    # ------------------------------------------------------------------
    # 只读快照：HTTP 线程高频调用。
    # 只做一次短时加锁的内存拷贝，绝不与后台生命周期操作交叉，
    # 前端渲染完全基于返回的副本，后台继续高频并发不受影响。
    # ------------------------------------------------------------------
    def snapshot(self):'''
if old2 not in src:
    marker = "    def snapshot(self):"
    idx = src.index(marker)
    # 找到 snapshot 前面被压坏的注释行（含 "鍙" 或一行多注释）
    line_start = src.rfind("\n", 0, idx)
    prev_block_start = src.rfind("    # ---", 0, line_start)
    block_line_start = src.rfind("\n", 0, prev_block_start) + 1
    src = (src[:block_line_start] +
           "    # ------------------------------------------------------------------\n"
           "    # 只读快照：HTTP 线程高频调用。只做一次短时加锁的内存拷贝，\n"
           "    # 绝不与后台生命周期操作交叉，前端渲染基于返回的副本，\n"
           "    # 后台继续高频并发不受影响。\n"
           "    # ------------------------------------------------------------------\n"
           + src[line_start + 1:])

with io.open(path, "w", encoding="utf-8", newline="\n") as f:
    f.write(src)
print("comments restored")