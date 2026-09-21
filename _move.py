import io

path = "gc_stw_sim.py"
with io.open(path, "r", encoding="utf-8") as f:
    lines = f.readlines()

# 1) 定位 prewarm 注释块起点（"    # ---..." 紧跟 "# 预热" 注释）
start = None
for i, line in enumerate(lines):
    if line.startswith("    def prewarm(self):"):
        # 向上找到其注释块开头
        j = i - 1
        while j >= 0 and (lines[j].startswith("    #") or lines[j].strip() == ""):
            if lines[j].startswith("    # ---"):
                break
            j -= 1
        start = j
        def_line = i
        break
assert start is not None, "prewarm not found"

# 2) snapshot 结束于 HTML_PAGE 赋值之前的空行区
end = None
for i in range(def_line, len(lines)):
    if lines[i].startswith("HTML_PAGE = "):
        end = i
        break
assert end is not None, "HTML_PAGE not found"

block = lines[start:end]
# 去掉块前导/尾随空行
while block and block[0].strip() == "":
    block.pop(0)
while block and block[-1].strip() == "":
    block.pop()
block_text = "".join(block)

# 3) 删除原块
del lines[start:end]

# 4) 插入到 Simulation.shutdown 之后、class Mutator 之前
anchor = None
for i, line in enumerate(lines):
    if line.startswith("class Mutator("):
        anchor = i
        break
assert anchor is not None, "Mutator anchor not found"
insert = ["\n", block_text, "\n\n\n"]
lines[anchor:anchor] = insert

with io.open(path, "w", encoding="utf-8", newline="\n") as f:
    f.writelines(lines)
print("moved block, %d chars" % len(block_text))