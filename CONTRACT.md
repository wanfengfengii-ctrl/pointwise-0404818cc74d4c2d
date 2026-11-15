# 计量校核固定分片基线

## 业务与仓库
左路 L 是计量区间，右路 R 是质检有效区间。key 是站点，同站点两路做全外区间关联。时间是任意整数，区间左闭右开，不展开到逐整数。没有水位、过期或迟到丢弃。不同 key 没有跨键事务或全局读一致性承诺。
源码 intervaljoin/model.py 是纯关联参照，worker.py 将一个完整 key 的状态、结果、日志和回执置于同一 SQLite 事务，cluster.py 按 key 固定哈希路由。样例是 samples.json；checks.py 的断言是已知正常关联业务。
本题沿用固定路由；新增覆盖分析与所属 key 位于同一个工作者数据库，不设计迁移或跨库协议。

## 已有入口、输入和输出
Python 入口：
    from intervaljoin import Cluster
    c = Cluster([path_a, path_b])
    c.apply(key, seq, request, changes, fail_at=None)
    c.read(key)
    c.workers[c.owner(key)].export_key(key)
    c.close()

path 是 SQLite 文件名；":memory:" 用于无文件回归。工作者是独立连接、独立数据库。Cluster 的工作者数量在创建时固定。
key/request 为非空字符串，seq 是逐 key 从 1 连续递增的正整数。changes 是对象列表，空列表合法，每项字段必须恰为：
    {"side":"L","id":"l1","rev":1,"start":0,"end":10,"value":{"reading":9},"deleted":false}
side 只能是 L/R；id 非空；rev 为正整数；start/end 为整数且 start<end；deleted 为布尔；value 为可规范 JSON 序列化的对象。整数不接受布尔。所有字段即使在墓碑中也必填。同一批 (side,id) 不得重复，身份只在 key 内唯一，不能跨 key 移动。非标准数值不可序列化。

同一 (key,side,id) 最高 rev 生效；更高版本可改区间、载荷，删除或复活。旧版本忽略；同版同内容忽略，同版异内容整批 REVISION_CONFLICT。保留墓碑与最高修订。事务中两侧共同变化时只比较批前和批后最终结果，不暴露中间匹配。
对有效 L/R 行，max(start)<min(end) 才匹配；输出交集区间。每对行只有一条结果。有效行完全没有匹配对时才输出一条整区间的未匹配结果；部分未覆盖时间不另输出。输出行依次是：
    [left_id_or_null,right_id_or_null,start,end,left_value_or_null,right_value_or_null]
结果按 model.canonical(row) 字符串排序。apply 返回 {"key":key,"seq":seq,"remove":[...],"add":[...]}；两组是批前后结果的集合净差并按同样规则排序。read 返回 {"key":key,"seq":seq,"rows":[...]}，从未写过的键为 seq=0、空结果。

请求精确绑定 (key,request) 与 {"seq":seq,"changes":changes} 的规范内容，规范化采用 model.canonical；列表次序也参与绑定。基础格式校验先执行；已存在回执的重放检查优先于连续 seq 和源修订判断。同绑定重试返回原响应，不改当前 head；不同绑定 REQUEST_CONFLICT。新请求 seq 不连续报 SEQ_GAP。任何失败不推进 head，也不留下状态、日志或回执。所有合法事务，包括空批和全旧修订，均推进 seq。
worker.receipts 的 digest 列实际存放规范内容字符串。journal 存整个已提交源批，包括被忽略的旧修订；当前不回收日志。export_key 在单个读事务内返回 key、seq、全部源行（含墓碑）、回执和 journal。它是整键导出，不是增量搬运接口。
fail_at="after_state" 或 "after_receipt" 注入 INJECTED，原事务整批回滚。

## 关键样例预期
samples.json 有同一站点的七笔事务。seq=1 得到 l1-r1@[5,10)；seq=2 新增 l2-r2@[25,30)。seq=3 将 r1 移到 [22,27)，必须撤销 l1-r1，同时出现 l1 的未匹配行及 l2-r1，原 l2-r2 保留。seq=4 撤销 r2，l2 仍匹配 r1，不得错误生成 l2 未匹配行。
seq=5 同批把 l2 改到 [40,50)、r1 改到 [45,55)，最终仍配对为 [45,50)，不输出两次单边更新造成的临时未匹配。seq=6 撤销最后的 r1，l2 才成为未匹配行。seq=7 复活 r2 到 [42,48)，l2 的未匹配行撤销，生成 l2-r2@[42,48)。l1 的未匹配行始终保留。

## 本轮新增分析口径（待设计，基线未实现）
本轮只交 DESIGN.md 和可重跑观察，不实现覆盖维护器，不修改关联业务源码及原回归。
拟议公开入口为 Cluster.coverage(key, left_id)，返回 {"seq":整数,"covered":整数或null,"duration":整数或null,"ratio":[分子,分母]或null}。key、left_id 为非空字符串，非法抛 ValueError("IDENTITY")；从未写过的 key 为 seq=0。有效左记录的 duration=end-start；covered 为所有有效右记录与它的交集的并集长度，不能把交集长度相加。ratio 为 covered/duration 的约分非负整数对，零覆盖返回 [0,1]。左记录不存在或已删除时 covered、duration、ratio 均为 null，但 seq 仍为该键当前值。value 不参与覆盖计算，端点相接不算重叠。
例如 L=[0,10)，R1=[0,7)、R2=[3,10)，covered=10、duration=10、ratio=[1,1]；删除 R1 后为 7、10、[7,10]。L=[0,10)、R=[0,10)，仅将 L 改为 [-5,10)，关联净差为空，但须得到 10、15、[2,3]。

单次源修订必须同时维护关联业务与覆盖分析；即使关联净差为空，也不能遗漏左侧区间变化。沿用最高 rev、墓碑、复活、同版冲突、逐键 seq 和精确回执。旧请求重放返回旧关联响应，但 coverage 返回当前 seq 的当前值；不得重复累计覆盖。空批和全旧修订批仍推进 seq。两侧同批修改只呈现批后完整分析状态。
限定单进程串行调用、固定分片，每键分析位于其现有工作者数据库内。源行、关联结果、覆盖分析和 head 须同事务生效；任一步失败均不留新分析状态。方案还须支持调用者在所属 Worker.db 上开启外层事务：apply 成功后事务内 coverage 可见新值，外层回滚后恢复全部旧值，不能提交调用者的事务。当前 Worker.apply/read 自行 BEGIN/COMMIT，尚不支持这种组合；须定位所需调整，但本轮不得实现。外层事务中不得跨键或跨工作者调用。不新增历史覆盖查询、并发、掉电恢复或部署目标。

首次允许全量构建。之后新增分析维护仅访问受影响左记录、与变化区间相关的定位索引及必要覆盖边界，不得每批扫描全部源行、关联结果或重建整键分析。没有受影响的左记录不得重算。受影响允许保守候选：区间或删除状态变化的左记录，以及被变化右记录旧新区间触及的左记录；覆盖数值最终未变也可入选。仅载荷变化不要求重算。允许按受影响左记录重建其覆盖边界，不要求通用动态区间树；须如实给出热点右记录影响所有左记录时的最坏成本。coverage 读取已维护结果，不在查询时遍历右记录或重做并集。原关联的全量 materialize 暂不优化，其成本与新增分析成本分列；不能借原全扫描之便每批复制全量状态给分析组件。

## 已提供的非答案分析材料
coverage_ref.coverage(snapshot, left_id) 对 export_key 的单个完整快照作全量端点分段参照，输出上述拟议入口的结果。它没有持久状态，也没有增量或事务接入。
coverage_samples.json 是覆盖业务独立样例；每步 expected 是按 l、b 顺序列出的 [covered,duration,ratio]。查询顺序见 probes。
coverage_observe.py 调用真实 apply/read/export_key，再调用全量参照；输出每步实际关联响应、关联结果和覆盖参照。它不模拟目标维护器。
overlap 演示重叠撤销、最后覆盖消失、端点相接、复活、旧修订、左墓碑及左复活；denominator 演示净差为空仍改变分母；joint 演示同批两侧移动。每个场景还重放首请求并注入 after_state/after_receipt，核对失败后的完整导出与覆盖参照均未变。unrelated 左记录 b 的覆盖值保持零，但其 seq 随键推进。

## 文档与证据契约
DESIGN.md 须定位从 Cluster.apply 到修订裁决、关联物化、回执和提交的调用链，引用实际源码行号，并标出当前信息缺口及事务边界。
比较两种可行的信息接口：关联净差加必要源信息，或已接受源记录前后像；说明各自何时足够、如何确定受影响左记录，再推荐一种。不能仅比较库名或代码风格。
给出推荐方案的必要表/索引与状态不变量、批前批后信息契约、覆盖边界维护、coverage 读取及关键事务伪代码。解释重叠贡献撤销、完整左区间变化、同批两侧移动、墓碑复活和被忽略修订；无匹配行不是覆盖贡献。
说明 apply 自管事务与外层事务下的提交、回滚和查询行为，保留原错误顺序、回执绑定和故障注入语义。预测须走通“在 seq=2 外层事务内执行 seq=3 并查询，随后回滚，再重试 seq=3”以及两处故障和旧请求重放。
分列原关联开销和新增分析的构建、单批维护、查询及空间成本，明确定义规模变量；承认全部左记录真实受影响时的不可避免工作，不新增性能倍数门槛。
按 coverage_samples.json 各步列出方案的受影响左记录、覆盖变化及事务后 seq，并说明分母变化时为何不能只依赖关联净差。同一份文档必须清楚区分基线实际观察与未实现方案预测，不能把全量参照当成目标增量实测。
DESIGN.md 包含恰一个以 json coverage-evidence 为信息串的代码围栏，其中 JSON 逐值等于 python3 coverage_observe.py replay 的输出；源码引用采用 intervaljoin/worker.py:59 或 coverage_ref.py:5 这样的路径加行号。无需把证据转录为截图。

python3 checks.py normal 重跑原有关联、回执冲突和故障回滚断言。
python3 coverage_observe.py check 重跑公开覆盖样例、回执重放及失败隔离，成功输出 COVERAGE_BASELINE_OK；replay 输出真实详细结果。
python3 reviewcheck.py verify 重跑上述回归、核对文档观察证据及源码行号；检查器只核对可重跑证据，设计正确性仍须人工审查。
保留 .pointwise-runtime.json 的 version=1、kind=cli、setup=[]；交付时将 verify 改为 ["python3","reviewcheck.py","verify"]，demo 改为 ["python3","coverage_observe.py","replay"]，使核验接通文档，演示展示真实基线差异。初始清单核验已提供的正常业务及覆盖参照，不要求尚未交付的 DESIGN.md。
