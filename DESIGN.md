# 计量区间质检覆盖分析设计（DESIGN）

本轮只交付设计与可重跑观察：不实现覆盖维护器，不修改 `intervaljoin/` 关联业务源码与 `checks.py` 原回归。文中所有标注"实测"的内容来自对现有基线的真实调用（证据见第 6 节围栏，可用 `python3 coverage_observe.py replay` 逐值重现）；标注"预测"的内容描述尚未实现的方案行为，不能当作实测。

## 1. 基线调用链、事务边界与信息缺口（实测源码定位）

### 1.1 调用链

`Cluster.apply`（intervaljoin/cluster.py:15-16）按 `owner(key)` 的 SHA-256 固定哈希路由（intervaljoin/cluster.py:12-13）到所属 `Worker.apply`（intervaljoin/worker.py:40）。每个 Worker 是独立 SQLite 连接、独立数据库（intervaljoin/worker.py:8-9，`isolation_level=None` 即自动提交、手工 BEGIN），表结构在 intervaljoin/worker.py:11-20。`Worker.apply` 内顺序如下：

1. 格式校验：身份/seq（intervaljoin/worker.py:41-44），`normalize(changes)` 逐字段校验并拒绝同批重复 (side,id)（intervaljoin/worker.py:45 → intervaljoin/model.py:9-30，重复判定在 intervaljoin/model.py:25-28）。
2. 计算回执绑定 `binding = canonical({"seq","changes"})`（intervaljoin/worker.py:46 → intervaljoin/model.py:5-6）。
3. `BEGIN IMMEDIATE`（intervaljoin/worker.py:47）——**事务边界起点**。
4. 回执查询（intervaljoin/worker.py:49）：同绑定重放直接 `COMMIT` 返回旧响应（intervaljoin/worker.py:50-54，注意第 53 行的 COMMIT）；异绑定 `REQUEST_CONFLICT`。
5. head 连续性检查，断档 `SEQ_GAP`（intervaljoin/worker.py:55-57）。
6. 读批前关联结果 `before`（intervaljoin/worker.py:58）；`revise` 做最高修订裁决（intervaljoin/worker.py:59 → intervaljoin/model.py:33-47）：旧修订跳过（intervaljoin/model.py:39-40）、同版同内容跳过、同版异内容 `REVISION_CONFLICT`（intervaljoin/model.py:41-44）、接受进 `accepted`（intervaljoin/model.py:45-46）。
7. 全量关联物化 `materialize`（intervaljoin/worker.py:60 → intervaljoin/model.py:50-68：配对循环 54-60，无匹配行 61-67，canonical 排序 68）与集合净差 `delta`（intervaljoin/worker.py:61 → intervaljoin/model.py:71-75）。
8. 写源行（intervaljoin/worker.py:62-63）、整键删除并重写 outputs（intervaljoin/worker.py:64-65）。
9. `fail_at="after_state"` 注入 `INJECTED`（intervaljoin/worker.py:66-67）。
10. 写 heads/journal/receipts（intervaljoin/worker.py:69-71；digest 列存的是 binding 规范字符串本身）。
11. `fail_at="after_receipt"` 注入（intervaljoin/worker.py:72-73）。
12. `COMMIT`（intervaljoin/worker.py:74）——**事务边界终点**；任何异常整批 `ROLLBACK`（intervaljoin/worker.py:76-78）。

`Worker.read`（intervaljoin/worker.py:29-38）与 `export_key`（intervaljoin/worker.py:80-91）各自开读事务。覆盖参照 `coverage_ref.coverage`（coverage_ref.py:5-21）对 `export_key` 的单个快照做全量端点分段（分段点 coverage_ref.py:13-14，并集求和 coverage_ref.py:15-16，约分 coverage_ref.py:17-20），无持久状态、无事务接入。观察脚本 coverage_observe.py 的 replay 真实调用 apply/read/export_key（coverage_observe.py:25-27）并核对样例期望（coverage_observe.py:28-31）、denominator 空净差（coverage_observe.py:32-33）、首请求重放（coverage_observe.py:36-39）与两处故障注入（coverage_observe.py:45-55）。

### 1.2 当前信息缺口（覆盖维护器需要什么、链路上没有什么）

- **缺口 G1（左区间自身信息）**：第 7 步的 `diff` 是关联输出行（交集段与无匹配行）的集合净差，不含左记录自身区间。左修订若保持所有交集与匹配身份不变，净差为空，左区间变化在 diff 中完全不可见（实测见第 2 节 denominator seq=2）。
- **缺口 G2（幸存重叠）**：撤销一条右记录时，remove 里只有消失的交集段；仍覆盖同一左记录的其他右记录不在 diff 中，无法由 diff 求撤销后的并集（实测 overlap seq=2）。
- **缺口 G3（批前源像）**：`revise` 内部读过旧行（intervaljoin/model.py:38）但不返回；批前源行映射其实已在 intervaljoin/worker.py:59 的 `self._rows(key)` 结果中，只是没有传给任何分析组件。补齐此缺口不需要额外 SQL。
- **缺口 G4（定位索引）**：源行 body 是 JSON 文本（intervaljoin/worker.py:13-14），无法按区间做索引查询；逐批解析全部源行又被 CONTRACT 禁止。需要独立的左/右区间定位表。
- **缺口 G5（分析状态）**：没有任何每左记录的覆盖结果表；`coverage()` 不能查询时遍历右记录重做并集。
- **事务边界现状**：apply 自管 `BEGIN IMMEDIATE … COMMIT`（intervaljoin/worker.py:47,74），连接为自动提交模式（intervaljoin/worker.py:9）。调用者若先在 `Worker.db` 上自行 BEGIN，intervaljoin/worker.py:47 的 `BEGIN IMMEDIATE` 会以"事务中不能再开事务"报错；且重放路径的 COMMIT（intervaljoin/worker.py:53）会把调用者的外层事务一并提交。当前结构不支持外层事务组合，所需调整见第 4.6、5 节（本轮只定位、不实现）。

## 2. 基线实测观察（真实运行，非方案预测）

以下均为对现有基线的实测，完整逐值证据在第 6 节围栏（与 `python3 coverage_observe.py replay` 输出逐值相等，`python3 reviewcheck.py verify` 核对）。样例 probes 为左记录 `l` 与无关左记录 `b`（[100,104)，全程零覆盖）。

**overlap 键**（重叠撤销、端点相接、复活、旧修订、左墓碑与复活）：

- seq=1 seed：L l=[0,10)、b=[100,104)，R r1=[0,7)、r2=[3,10)。l 的 covered=10、duration=10、ratio=[1,1]——r1、r2 重叠区 [3,7) 只计一次；b 为 [0,4,[0,1]]。
- seq=2 drop-one（r1 墓碑）：关联响应 remove=[l,r1,0,7]、add=[]；l 变为 [7,10,[7,10]]。撤销 r1 没有抹掉 r2 的贡献 [3,10)，幸存重叠必须重新求并集，不能从 diff 减法得到。
- seq=3 drop-last（r2 墓碑）：l 的最后覆盖消失，关联输出出现 l 的无匹配行 [l,None,0,10]，covered=0、ratio=[0,1]。无匹配行横跨整个左区间但表示"没有覆盖"，不是覆盖贡献。
- seq=4 touch（r1 复活到 [10,12)）：端点相接不算重叠，l 仍为 [0,10,[0,1]]。
- seq=5 revive（r2 复活 [3,10)）：l 恢复 [7,10,[7,10]]。
- seq=6 stale（r1 旧 rev=2 重投）：被修订裁决忽略，响应 remove/add 皆空，覆盖不变，seq 仍推进到 6。
- seq=7 left-drop（l 墓碑）：l 的 covered/duration/ratio 全为 null，seq=7；b 不变。
- seq=8 left-back（l 复活 [0,10)）：l 恢复 [7,10,[7,10]]。

**denominator 键**（净差为空仍改分母）：seq=1 后 l=[0,10) 被 r=[0,10) 全覆盖 [10,10,[1,1]]。seq=2 仅把 l 扩为 [-5,10)，**实测关联响应 remove=[]、add=[]**（coverage_observe.py:32-33 断言），但覆盖参照给出 [10,15,[2,3]]：交集 [0,10) 与匹配身份都没变，关联净差为空，分母却从 10 变 15。

**joint 键**（同批两侧移动）：seq=2 同批把 l 移到 [20,30)、r 移到 [25,35)，实测只呈现批后最终配对 [l,r,25,30]（remove 旧交集、add 新交集各一条），覆盖 [5,10,[1,2]]，不出现两次单边更新的临时未匹配行。

**重放与故障（三个场景相同）**：首请求原样重放返回与首次完全相同的响应，export 逐值不变、head 不推进（coverage_observe.py:36-39；各场景 head_after_replay 等于最终 seq）；随后用一条会被接受的右记录分别以 fail_at=after_state / after_receipt 注入，均抛 INJECTED，export 与覆盖参照逐值保持注入前状态（coverage_observe.py:45-55）。无关左记录 b 的覆盖值全程 [0,4,[0,1]]，但其 seq 随键推进——seq 来自键级 head，与单左记录状态无关。

## 3. 信息接口比较：关联净差+必要源信息 vs 已接受源记录前后像

### 方案 A：关联净差 + 必要源信息

把 intervaljoin/worker.py:61 的 `diff`（remove/add 关联行）交给分析组件，不足处另查补充。

- **何时足够**：仅右记录变化时，diff 基本够用——凡与变化右记录旧/新区间相交的左记录，其配对行必变（交集段不变则覆盖贡献也不变），故受影响左记录 = diff 行中出现的左 id。但撤销后要求并集，diff 中没有幸存配对，须再按左 id 回查 outputs 中现存匹配行；交集段相同的左右同时移动不产生 diff，也无需维护。
- **何时不足**：左记录自身区间/删除状态变化而关联输出不变时，diff 为空，左 id 根本不出现——denominator seq=2 实测 remove/add 皆空而分母 10→15。要补上，必须另行比对左记录的批前批后区间，而这正是方案 B 的信息。此外 diff 中右记录只有逐左交集段，没有右记录自身的完整旧/新区间。
- **受影响左记录如何确定**：diff 行中的左 id ∪ 额外源比对发现的左变化；幸存配对与左区间都要补查询。
- **结论**：A 的核心信息在分母场景观测上就不充分，两个补丁（左前后像、幸存配对回查）叠加后，"净差"部分变为冗余，且正确性依赖无匹配行发射规则等关联输出语义（intervaljoin/model.py:61-67），与关联物化耦合过紧。

### 方案 B：已接受源记录前后像（推荐）

在修订裁决之后，把每条**被接受**变更的 (side, id, 批前像或 null, 批后像) 交给分析组件。批前像取自 intervaljoin/worker.py:59 已加载的批前源行映射（零额外 I/O），批后像即 `accepted` 项（intervaljoin/model.py:45-46）。

- **何时足够**：总是足够。左记录覆盖只依赖其自身当前区间与当前有效右记录集合；前后像精确给出变化左记录的新区间（分母）与变化右记录的触及范围（旧∪新有效区间），被忽略的旧修订与同版同内容根本不进 `accepted`（intervaljoin/model.py:39-44），天然不触发维护。
- **受影响左记录如何确定**：(i) 有效区间（未删除时的 [start,end)）或删除状态发生变化的左记录自身；(ii) 当前区间与某变化右记录"触及范围"（旧有效区间 ∪ 新有效区间）严格相交（max(start)<min(end)）的左记录。健全性：未变化左记录若与所有触及范围不相交，则每个变化右记录对它的贡献前后皆为空，未变化右记录贡献不变，故覆盖必不变。允许保守多选（覆盖数值未变也可入选），仅载荷（value/rev）变化按契约过滤掉。
- **结论**：B 用一处信息源同时覆盖分母变化、重叠撤销、同批移动、墓碑复活与忽略修订，判定在关联物化之前独立完成，与全量 materialize 解耦。**推荐方案 B**；A 只在"纯右记录变化且愿回查幸存配对"时局部够用，其必需补丁收敛于 B。

## 4. 推荐方案设计（未实现，以下为预测）

### 4.1 表与索引（与现有表同库，随 intervaljoin/worker.py:11-20 的建表块一并创建）

```sql
CREATE TABLE IF NOT EXISTS cov_meta(k TEXT PRIMARY KEY);                 -- 该键分析已构建
CREATE TABLE IF NOT EXISTS cov_lindex(k TEXT, lid TEXT, start INTEGER, end INTEGER,
                                      PRIMARY KEY(k,lid));               -- 有效左记录定位
CREATE INDEX IF NOT EXISTS cov_lindex_span ON cov_lindex(k, start, end);
CREATE TABLE IF NOT EXISTS cov_rindex(k TEXT, rid TEXT, start INTEGER, end INTEGER,
                                      PRIMARY KEY(k,rid));               -- 有效右记录定位
CREATE INDEX IF NOT EXISTS cov_rindex_span ON cov_rindex(k, start, end);
CREATE TABLE IF NOT EXISTS cov_state(k TEXT, lid TEXT, covered INTEGER, duration INTEGER,
                                     num INTEGER, den INTEGER, PRIMARY KEY(k,lid)); -- 已维护结果
```

### 4.2 状态不变量

- I1：`cov_lindex(k,lid)` 存在 ⟺ 键 k 有有效（未删除）L 记录 lid，(start,end) 为其当前区间。
- I2：`cov_rindex` 同理对应有效 R 记录。
- I3：`cov_state(k,lid)` 存在 ⟺ `cov_lindex(k,lid)` 存在；covered = 所有有效右记录与 [start,end) 交集的**并集**长度（重叠只计一次）；duration=end-start；num/den 为 covered/duration 的约分（covered=0 时为 0/1，与 coverage_ref.py:17-20 的 Fraction 行为一致）。
- I4：`cov_meta(k)` 存在 ⟺ 该键已完成首次全量构建；此后每个提交的 apply 保持 I1–I3。
- I5：cov_* 与 rows/outputs/heads/journal/receipts 同事务生效、同事务回滚；任何失败（含 INJECTED）不留半边分析状态。
- I6：`coverage()` 返回的 seq 恒等于 heads（无 heads 行视为 0），与 cov_state 行是否存在无关；左记录缺失或已删除时三值为 null。

### 4.3 批前批后信息契约

每个被接受变更一条 image：`{side, id, old: null|{rev,start,end,deleted}, new: {rev,start,end,deleted}}`。同批 (side,id) 唯一（intervaljoin/model.py:25-28），故 image 无歧义；同批两侧变更一次性整体交给维护器，只按批后状态计算，不呈现中间态。过滤规则：`effective(old) != effective(new)`（effective = 未删除时的 [start,end)，否则无）才进入维护——纯载荷/rev 变化、新建即墓碑（无→无）都被滤除；旧修订、同版同内容在裁决阶段已消失（intervaljoin/model.py:39-44），不产生 image。

### 4.4 受影响左记录与覆盖边界维护

每批（键已构建且过滤后 image 非空时）：

1. **更新定位索引**：按 L 类 image 改 cov_lindex（新像无有效区间则删行，否则 upsert）；按 R 类 image 改 cov_rindex。
2. **受影响左记录**：`aff = {L 类 image 的 id}` ∪ 对每个 R 类 image 的旧、新有效区间分别查 `SELECT lid FROM cov_lindex WHERE k=? AND start<e AND end>s`（严格相交，端点相接不入选）的并集。查询发生在索引更新之后，用的是批后左区间——未变化左记录前后区间相同，已变化左记录本就在 aff 中，判定健全（第 3 节）。
3. **逐左重建覆盖边界**：对 aff 中每个 lid，查 cov_lindex 当前行；不存在（左墓碑/不存在）则删 cov_state 行（coverage 三值变 null）。存在则查 `SELECT start,end FROM cov_rindex WHERE k=? AND start<cur.end AND end>cur.start`，在内存中做端点分段求并集长度（与 coverage_ref.py:13-16 同法），duration=end-start，Fraction 约分，upsert cov_state。允许按左整体重建，不要求通用动态区间树；边界分段是临时的，持久化的只有 cov_state 标量。
4. 过滤后 image 为空则**什么都不做**（没有受影响左记录不得重算）；空批、全旧修订批即属此类，seq 随 heads 照常推进。

**首次全量构建**：apply 内发现 cov_meta 无该键时，用批后源状态一次性构建三张表并写 cov_meta。仅此一次允许扫描全部源行（直接复用 intervaljoin/worker.py:59 已在内存的批后映射，不再查库）；此后任何批次不得全量扫描或借原全扫描复制全量状态给分析组件。升级前已存在的旧键在升级后首次 apply 时构建；若某旧键在首次 apply 前就被 coverage() 查询，按 I6 返回当前 seq 与三值 null（记录在案的边界行为，本题样例键均从空键经 apply 建立，不触发）。

### 4.5 coverage 读取与公开入口

`Cluster.coverage(key, left_id)`（拟议公开入口，路由方式同 intervaljoin/cluster.py:18-19）→ `Worker.coverage`：非空字符串校验，非法抛 `ValueError("IDENTITY")`；读 heads 得 seq（无则 0）；查 cov_state(k,left_id)，有行返回 `{seq, covered, duration, ratio:[num,den]}`，无行返回 `{seq, null, null, null}`。查询只读已维护标量，不遍历右记录、不重做并集、不重建整键。

### 4.6 调用链接入点与关键事务伪代码

接入点：维护调用插入 intervaljoin/worker.py:65（outputs 重写完成）与 intervaljoin/worker.py:66（after_state 注入）之间——"state"由此涵盖源行、关联结果与覆盖分析，两处故障注入点（intervaljoin/worker.py:66-67、72-73）原语义不变且都落在分析写入之后，注入即整批回滚。所有校验（intervaljoin/worker.py:41-45）、回执判定（49-54）、SEQ_GAP（55-57）、REVISION_CONFLICT（59）都在维护之前，错误顺序不变；回执绑定计算（intervaljoin/worker.py:46）与存储（71）不变。

```
def apply(key, seq, request, changes, fail_at=None):
    # 校验与绑定：worker.py:41-46 原样（外层事务下也在任何保存点之前失败，不碰调用者事务）
    own = not db.in_transaction                 # 新增：外层事务检测
    if own: db.execute("BEGIN IMMEDIATE")       # 自管路径，同 worker.py:47
    else:   db.execute("SAVEPOINT ij_apply")    # 外层路径：不 BEGIN、绝不 COMMIT 调用者事务
    try:
        # 回执重放/冲突：worker.py:49-54 原逻辑；重放返回前
        #   自管路径 COMMIT（现状），外层路径仅 RELEASE ij_apply（不提交外层）
        # SEQ_GAP：worker.py:55-57 原样
        before_rows = self._rows(key)           # worker.py:59 已有
        updated, accepted = revise(before_rows, changes)
        images = [(c, before_rows.get((c["side"], c["id"]))) for c in accepted]  # G3 补齐
        # 关联物化与写回：worker.py:60-65 原样（全量 materialize 暂不优化）
        maintain_coverage(key, images, updated) # 新增：4.4 节；首建用 updated 全量
        # after_state 注入：worker.py:66-67 原位
        # heads/journal/receipts：worker.py:69-71 原样
        # after_receipt 注入：worker.py:72-73 原位
        db.execute("COMMIT") if own else db.execute("RELEASE ij_apply")
        return response
    except BaseException:
        if own: db.execute("ROLLBACK")                                   # 同 worker.py:77
        else:   db.execute("ROLLBACK TO ij_apply"); db.execute("RELEASE ij_apply")
        raise

def coverage(key, left_id):
    # 身份校验非法抛 ValueError("IDENTITY")
    own = not db.in_transaction
    if own: db.execute("BEGIN")                 # 读事务模式同 worker.py:30
    seq = heads.get(key, 0)
    row = cov_state.get((key, left_id))
    if own: db.execute("COMMIT")
    return {"seq": seq, **(row or {"covered": None, "duration": None, "ratio": None})}
```

`Worker.read`/`export_key`（intervaljoin/worker.py:29-38、80-91）同样加 `in_transaction` 守卫：外层事务内跳过自身 BEGIN/COMMIT，直接在同一连接上读（可见未提交的本键新值）。外层事务中不得跨键或跨工作者调用（记录在案的限制，单进程串行范围内不做强制）。

### 4.7 关键语义解释（预测，均与第 2 节实测口径一致）

- **重叠贡献撤销**：r1 墓碑的 image 触及 [0,7)，命中 l；重建时从 cov_rindex 取到幸存的 r2=[3,10)，并集为 [3,10) → covered=7。重叠区 [3,7) 在分段求并时只计一次，撤销 r1 不抹掉 r2 的贡献。
- **完整左区间变化**：l 扩为 [-5,10) 的 image 使 l 入选，duration 用新区间得 15，covered 重算为 10 → [2,3]。关联净差为空也照常维护，因为驱动来自源前后像而非 diff。
- **同批两侧移动**：l→[20,30) 与 r→[25,35) 的 image 同批到达，l 因自身变化入选，重建只看批后 r=[25,35) → covered=5 → [1,2]；不产生中间态。
- **墓碑复活**：r2 墓碑→复活，有效区间无→[3,10)，触及命中 l，重建恢复 7/10；l 墓碑→复活同理，先删 cov_state（查询得 null）再在复活批重建。
- **被忽略修订**：旧 rev 重投不进 `accepted`（intervaljoin/model.py:39-40），无 image、无维护、无重复累计；同版异内容整批 REVISION_CONFLICT 回滚。
- **无匹配行不是覆盖贡献**：无匹配行只在左记录零匹配时发射（intervaljoin/model.py:61-67），表示没有覆盖；covered 只来自匹配交集的并集，维护器根本不消费关联输出行。

## 5. 事务行为：自管与外层（预测走查）

**自管路径**（现状保持）：apply 自行 BEGIN IMMEDIATE/COMMIT/ROLLBACK；成功后 coverage 立即可读新值；任一步失败（含两处 INJECTED）整批回滚，cov_* 与其余状态一起消失；重放只读不写。

**外层路径**（新增能力）：调用者在 `Worker.db` 上自行 BEGIN 后，apply/coverage/read/export_key 检测到 `in_transaction` 即切换保存点模式。apply 成功只 RELEASE 保存点——不提交调用者事务；事务内 coverage 在同一连接上读到未提交新值；调用者 ROLLBACK 后 heads/rows/outputs/cov_*/journal/receipts 全部回到外层事务前；INJECTED 或业务校验失败只 ROLLBACK TO 保存点，调用者事务保持可用。错误顺序、回执绑定、故障注入语义与自管路径逐项一致。

**规定走查（预测）**：以 joint 键完成其 seq=1、seq=2（l=[20,30)，r=[25,35)，coverage(l)=[5,10,[1,2]]，heads=2）后为起点——

1. 调用者 `BEGIN`；`apply(key,3,"outer-move",[r→[40,50)])` 成功返回；事务内 `coverage(key,"l")` 得 `{seq:3, covered:0, duration:10, ratio:[0,1]}`（新值可见）。
2. 调用者 `ROLLBACK`；`coverage(key,"l")` 回到 `{seq:2, covered:5, duration:10, ratio:[1,2]}`，read/export 均恢复。
3. 以相同请求与内容重试 `apply(key,3,"outer-move",...)`：heads 已回 2 故无 SEQ_GAP，回执已随回滚消失故无 REQUEST_CONFLICT，成功提交；coverage 得 seq=3 新值。
4. 故障走查：外层 BEGIN 后 `apply(key,4,"f1",[...],fail_at="after_state")` 抛 INJECTED，保存点回滚，外层事务仍存活，coverage 保持 seq=3 值；`after_receipt` 同理；调用者 ROLLBACK 后一切如旧。
5. 旧请求重放：外层事务内重放 seq=1 的 "seed"，返回旧关联响应（seq=1 的 remove/add），不推进 head、不写任何状态、不提交外层事务；随后 `coverage(key,"l")` 返回的是**当前** seq 的当前值而非历史值——重放不重复累计覆盖。

## 6. 基线实测证据（coverage_observe.py replay 逐值输出）

```json coverage-evidence
{"scenarios": [{"failures": [{"coverage_reference": {"b": {"covered": 0, "duration": 4, "ratio": [0, 1], "seq": 8}, "l": {"covered": 7, "duration": 10, "ratio": [7, 10], "seq": 8}}, "error": "INJECTED", "point": "after_state"}, {"coverage_reference": {"b": {"covered": 0, "duration": 4, "ratio": [0, 1], "seq": 8}, "l": {"covered": 7, "duration": 10, "ratio": [7, 10], "seq": 8}}, "error": "INJECTED", "point": "after_receipt"}], "head_after_replay": 8, "key": "overlap", "repeated_first": {"add": [["b", null, 100, 104, {}, null], ["l", "r1", 0, 7, {}, {}], ["l", "r2", 3, 10, {}, {}]], "key": "overlap", "remove": [], "seq": 1}, "steps": [{"coverage_reference": {"b": {"covered": 0, "duration": 4, "ratio": [0, 1], "seq": 1}, "l": {"covered": 10, "duration": 10, "ratio": [1, 1], "seq": 1}}, "join": {"key": "overlap", "rows": [["b", null, 100, 104, {}, null], ["l", "r1", 0, 7, {}, {}], ["l", "r2", 3, 10, {}, {}]], "seq": 1}, "response": {"add": [["b", null, 100, 104, {}, null], ["l", "r1", 0, 7, {}, {}], ["l", "r2", 3, 10, {}, {}]], "key": "overlap", "remove": [], "seq": 1}}, {"coverage_reference": {"b": {"covered": 0, "duration": 4, "ratio": [0, 1], "seq": 2}, "l": {"covered": 7, "duration": 10, "ratio": [7, 10], "seq": 2}}, "join": {"key": "overlap", "rows": [["b", null, 100, 104, {}, null], ["l", "r2", 3, 10, {}, {}]], "seq": 2}, "response": {"add": [], "key": "overlap", "remove": [["l", "r1", 0, 7, {}, {}]], "seq": 2}}, {"coverage_reference": {"b": {"covered": 0, "duration": 4, "ratio": [0, 1], "seq": 3}, "l": {"covered": 0, "duration": 10, "ratio": [0, 1], "seq": 3}}, "join": {"key": "overlap", "rows": [["b", null, 100, 104, {}, null], ["l", null, 0, 10, {}, null]], "seq": 3}, "response": {"add": [["l", null, 0, 10, {}, null]], "key": "overlap", "remove": [["l", "r2", 3, 10, {}, {}]], "seq": 3}}, {"coverage_reference": {"b": {"covered": 0, "duration": 4, "ratio": [0, 1], "seq": 4}, "l": {"covered": 0, "duration": 10, "ratio": [0, 1], "seq": 4}}, "join": {"key": "overlap", "rows": [["b", null, 100, 104, {}, null], ["l", null, 0, 10, {}, null], [null, "r1", 10, 12, null, {}]], "seq": 4}, "response": {"add": [[null, "r1", 10, 12, null, {}]], "key": "overlap", "remove": [], "seq": 4}}, {"coverage_reference": {"b": {"covered": 0, "duration": 4, "ratio": [0, 1], "seq": 5}, "l": {"covered": 7, "duration": 10, "ratio": [7, 10], "seq": 5}}, "join": {"key": "overlap", "rows": [["b", null, 100, 104, {}, null], ["l", "r2", 3, 10, {}, {}], [null, "r1", 10, 12, null, {}]], "seq": 5}, "response": {"add": [["l", "r2", 3, 10, {}, {}]], "key": "overlap", "remove": [["l", null, 0, 10, {}, null]], "seq": 5}}, {"coverage_reference": {"b": {"covered": 0, "duration": 4, "ratio": [0, 1], "seq": 6}, "l": {"covered": 7, "duration": 10, "ratio": [7, 10], "seq": 6}}, "join": {"key": "overlap", "rows": [["b", null, 100, 104, {}, null], ["l", "r2", 3, 10, {}, {}], [null, "r1", 10, 12, null, {}]], "seq": 6}, "response": {"add": [], "key": "overlap", "remove": [], "seq": 6}}, {"coverage_reference": {"b": {"covered": 0, "duration": 4, "ratio": [0, 1], "seq": 7}, "l": {"covered": null, "duration": null, "ratio": null, "seq": 7}}, "join": {"key": "overlap", "rows": [["b", null, 100, 104, {}, null], [null, "r1", 10, 12, null, {}], [null, "r2", 3, 10, null, {}]], "seq": 7}, "response": {"add": [[null, "r2", 3, 10, null, {}]], "key": "overlap", "remove": [["l", "r2", 3, 10, {}, {}]], "seq": 7}}, {"coverage_reference": {"b": {"covered": 0, "duration": 4, "ratio": [0, 1], "seq": 8}, "l": {"covered": 7, "duration": 10, "ratio": [7, 10], "seq": 8}}, "join": {"key": "overlap", "rows": [["b", null, 100, 104, {}, null], ["l", "r2", 3, 10, {}, {}], [null, "r1", 10, 12, null, {}]], "seq": 8}, "response": {"add": [["l", "r2", 3, 10, {}, {}]], "key": "overlap", "remove": [[null, "r2", 3, 10, null, {}]], "seq": 8}}]}, {"failures": [{"coverage_reference": {"b": {"covered": 0, "duration": 4, "ratio": [0, 1], "seq": 2}, "l": {"covered": 10, "duration": 15, "ratio": [2, 3], "seq": 2}}, "error": "INJECTED", "point": "after_state"}, {"coverage_reference": {"b": {"covered": 0, "duration": 4, "ratio": [0, 1], "seq": 2}, "l": {"covered": 10, "duration": 15, "ratio": [2, 3], "seq": 2}}, "error": "INJECTED", "point": "after_receipt"}], "head_after_replay": 2, "key": "denominator", "repeated_first": {"add": [["b", null, 100, 104, {}, null], ["l", "r", 0, 10, {}, {}]], "key": "denominator", "remove": [], "seq": 1}, "steps": [{"coverage_reference": {"b": {"covered": 0, "duration": 4, "ratio": [0, 1], "seq": 1}, "l": {"covered": 10, "duration": 10, "ratio": [1, 1], "seq": 1}}, "join": {"key": "denominator", "rows": [["b", null, 100, 104, {}, null], ["l", "r", 0, 10, {}, {}]], "seq": 1}, "response": {"add": [["b", null, 100, 104, {}, null], ["l", "r", 0, 10, {}, {}]], "key": "denominator", "remove": [], "seq": 1}}, {"coverage_reference": {"b": {"covered": 0, "duration": 4, "ratio": [0, 1], "seq": 2}, "l": {"covered": 10, "duration": 15, "ratio": [2, 3], "seq": 2}}, "join": {"key": "denominator", "rows": [["b", null, 100, 104, {}, null], ["l", "r", 0, 10, {}, {}]], "seq": 2}, "response": {"add": [], "key": "denominator", "remove": [], "seq": 2}}]}, {"failures": [{"coverage_reference": {"b": {"covered": 0, "duration": 4, "ratio": [0, 1], "seq": 2}, "l": {"covered": 5, "duration": 10, "ratio": [1, 2], "seq": 2}}, "error": "INJECTED", "point": "after_state"}, {"coverage_reference": {"b": {"covered": 0, "duration": 4, "ratio": [0, 1], "seq": 2}, "l": {"covered": 5, "duration": 10, "ratio": [1, 2], "seq": 2}}, "error": "INJECTED", "point": "after_receipt"}], "head_after_replay": 2, "key": "joint", "repeated_first": {"add": [["b", null, 100, 104, {}, null], ["l", "r", 0, 10, {}, {}]], "key": "joint", "remove": [], "seq": 1}, "steps": [{"coverage_reference": {"b": {"covered": 0, "duration": 4, "ratio": [0, 1], "seq": 1}, "l": {"covered": 10, "duration": 10, "ratio": [1, 1], "seq": 1}}, "join": {"key": "joint", "rows": [["b", null, 100, 104, {}, null], ["l", "r", 0, 10, {}, {}]], "seq": 1}, "response": {"add": [["b", null, 100, 104, {}, null], ["l", "r", 0, 10, {}, {}]], "key": "joint", "remove": [], "seq": 1}}, {"coverage_reference": {"b": {"covered": 0, "duration": 4, "ratio": [0, 1], "seq": 2}, "l": {"covered": 5, "duration": 10, "ratio": [1, 2], "seq": 2}}, "join": {"key": "joint", "rows": [["b", null, 100, 104, {}, null], ["l", "r", 25, 30, {}, {}]], "seq": 2}, "response": {"add": [["l", "r", 25, 30, {}, {}]], "key": "joint", "remove": [["l", "r", 0, 10, {}, {}]], "seq": 2}}]}]}
```

## 7. 成本分析（基线与新增分列，无性能倍数门槛）

规模变量（均按单键计）：L=有效左记录数，R=有效右记录数，J=关联输出行数，C=批内变更数，A=过滤后相关 image 数（A_L/A_R 分侧），F=受影响左记录数，q_l=与左记录 l 相交的右记录数，s_l=其分段数（≤2q_l+2）。

**原关联开销（现状，不优化）**：每批 `_rows` 全键读 O(L+R) 行并解析 JSON（intervaljoin/worker.py:25-27,59）；materialize 配对测试 O(L·R)、排序 O(J log J)（intervaljoin/model.py:50-68）；delta O(J log J)（intervaljoin/model.py:71-75）；outputs 整键删写 O(J)（intervaljoin/worker.py:64-65）。主导项 O(L·R)。

**新增分析开销**：

- 首次构建（每键一次）：全量源扫描 O(L+R)，索引写入 O((L+R) log)，逐左并集 Σ_l O(q_l log q_l)，最坏（全部右记录覆盖全部左记录）O(L·R log R)。
- 单批维护：image 过滤 O(C)；索引维护 O(A log)；受影响查询 O(A_R·(log L + F))；逐左重建 Σ_{l∈F} O(log R + q_l log q_l)；cov_state 写入 O(F log)。不扫描全部源行、不重建整键；无受影响左记录时为零。
- 查询：coverage() 为 heads+cov_state 两次主键查找 O(log)；不遍历右记录。
- 空间：每键 O(L+R) 行（cov_state、cov_lindex、cov_rindex 各随有效记录数）加每键 1 行 cov_meta。
- **如实的最坏成本**：一条热点右记录与全部 L 条左记录相交时，F=L，单批重建全部左记录，Σ_l O(q_l log q_l) 可达 O(L·R log R)，与一次基线 materialize 同阶——这是全部左记录真实受影响时的不可避免工作，不掩饰。另外区间相交查询 `start<e AND end>s` 只能利用一个范围列的索引，另一范围需过滤，可能多扫索引行。

## 8. 样例逐步预测（方案 B 的维护动作；数值口径与第 2 节实测一致）

下表"实测"列出自第 6 节证据；"预测"列是未实现方案的维护动作，其目标数值恰与全量参照一致，但**参照不是目标增量维护器的实测**。

**overlap**（probes：l，b；b 全程不受影响、不重算）：

| seq | 变更 | 预测：受影响左记录 | 预测：覆盖变化 | 事务后 seq |
|---|---|---|---|---|
| 1 | seed（首建） | 全量构建 l、b | l→[10,10,[1,1]]，b→[0,4,[0,1]] | 1 |
| 2 | r1 墓碑 | l（r1 旧区间 [0,7) 触及） | l→[7,10,[7,10]] | 2 |
| 3 | r2 墓碑 | l（[3,10) 触及） | l→[0,10,[0,1]] | 3 |
| 4 | r1 复活 [10,12) | 无（端点相接不相交，aff=∅，零维护） | 不变 | 4 |
| 5 | r2 复活 [3,10) | l | l→[7,10,[7,10]] | 5 |
| 6 | r1 旧 rev 重投 | 无（未入 accepted，无 image） | 不变 | 6 |
| 7 | l 墓碑 | l（删 cov_state） | l→[null,null,null] | 7 |
| 8 | l 复活 [0,10) | l（重建） | l→[7,10,[7,10]] | 8 |

**denominator**：seq=1 首建，l→[10,10,[1,1]]、b→[0,4,[0,1]]，seq=1。seq=2 l 扩为 [-5,10)：预测受影响左记录={l}（自身 image），重建得 [10,15,[2,3]]，seq=2。**为何不能只依赖关联净差**：实测该批响应 remove=[]、add=[]（coverage_observe.py:32-33 断言），关联输出逐行不变——交集 [0,10) 与匹配身份均未变，净差中根本没有 l 的任何信息，而分母 10→15 只存在于源记录前后像中；以净差为驱动必然遗漏分母更新。

**joint**：seq=1 首建同上，seq=1。seq=2 同批 l→[20,30)、r→[25,35)：预测受影响左记录={l}（自身 image；r 的触及范围 [0,10)∪[25,35) 亦命中批后 l），按批后状态一次重建得 [5,10,[1,2]]，seq=2；b 不受影响。

**重放与故障（三场景相同，预测与实测口径一致）**：首请求重放走回执路径，无 image、无维护，coverage 返回当前 seq 当前值；after_state/after_receipt 注入整批回滚，cov_* 与其余状态一起保持注入前；失败导出与覆盖值逐值不变（实测证据见第 6 节 failures 数组）。

## 9. 重跑与核验接通

- `python3 checks.py normal`：原关联、回执冲突、故障回滚回归（保持通过，未改动）。
- `python3 coverage_observe.py check`：公开覆盖样例+重放+失败隔离，输出 COVERAGE_BASELINE_OK。
- `python3 coverage_observe.py replay`：第 6 节证据的真实来源。
- `python3 reviewcheck.py verify`：重跑上述回归并核对本文档证据围栏与源码行号。
- `.pointwise-runtime.json`：保持 version=1、kind=cli、setup=[]；verify 改为 `["python3","reviewcheck.py","verify"]`，demo 改为 `["python3","coverage_observe.py","replay"]`。
