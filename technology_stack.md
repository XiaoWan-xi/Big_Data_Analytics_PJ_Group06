# 技术栈与数据接口

P01 已在本地实测 CPython 3.12.14 与 PyArrow 25.0.1，后者固定在 [依赖清单](requirements.txt)。P02 起的选型待实施，五节点配置、版本和资源上限由 P02 实测。各发行版及许可证见 [第三方清单](THIRD_PARTY_NOTICES.md)。

## 按流程选型

| 流程与任务 | 工具 | 输入、处理与输出 | 运行位置和验证 |
|---|---|---|---|
| 输入准备（P01） | unar、Python、PyArrow、`tarfile` | 解包 RAR。按列分批读取 Parquet，按仓库 TAR 的成员清单建立 attempt 索引 | 本地实测两种模式，按参数读取源目录。可用教师脚本的 `iter_shards`、`load_runs`（pandas）或 `query`（DuckDB）独立抽查 |
| 批处理环境（P02） | 集群 Linux、JVM、Apache Spark、PySpark、`spark-submit` | 提交驱动程序并将清洗、聚合、分区写回分发给执行器 | 先盘点五台机器。已有可用 YARN 就沿用；空环境拟用 Spark Standalone，一台协调、四台工作。以实际执行器记录认定多机运行。[集群说明](https://spark.apache.org/docs/latest/cluster-overview.html) |
| 数据存取与清洗（P02） | 共享存储、Parquet、Spark SQL / DataFrame | 分片投影、类型和 UTC 转换、主键与状态审计、分区落盘。按读取模式比较分区方案，避免按 run 建目录 | 优先用已有 HDFS 或所有节点同路径可访问的共享挂载；没有共享存储时，先在 P02 明确部署可用方案并验证跨节点读写。[Spark Parquet 文档](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) |
| 特征工程（P03） | PySpark SQL、聚合函数、Window | 由 run 表构建工作流画像与连续周面板；按时间截点另算历史特征 | 全量在集群聚合，DuckDB/手算核验小样本；持久化特征使用 Parquet |
| 聚类（P04） | `pyspark.ml` 的 VectorAssembler、StandardScaler、KMeans、ClusteringEvaluator；scikit-learn 的 adjusted_rand_score | 数值向量 → 缩放 → 拟合多个 K 和种子 → 对照轮廓系数、簇规模、ARI；用原始单位解释中心与代表样本 | Spark 运行聚类；ARI 比较同一组工作流的两套标签。记录距离度量和实际缩放方式。[聚类与评估](https://spark.apache.org/docs/latest/ml-clustering.html) |
| 运行级异常（P05-T01） | PySpark SQL 窗口与分位数聚合，Python 样本复算 | 一个 run 一行。用同一工作流在评分前已观测到的运行估计耗时中位数、MAD 与重试分布，输出分项、综合分数及原因 | 对零 MAD 约定回退或缺失规则，对跳过及历史不足给出状态。按特征声明精确值或近似分位数的复核容差 |
| 生命周期与时间回测（P05-T02、P05-T03） | Spark SQL 周面板、持续性规则、时间顺序回放 | 一个工作流可有多个沉寂、恢复或突发区间，保存开始周、结束周和历史节律 | 在已知变化点上核验告警延迟，用稳定序列估计误报，标出末端截尾区间 |
| 日志指纹（P06） | Python 标准库 `tarfile`、`zipfile`、`io`、`re`；PyArrow/Parquet | 以仓库 TAR 为分工单元，依次读单个 ZIP 和文本成员；记录规则命中、有效行数、成员定位、解析状态，先聚合 attempt 再形成 run 特征 | 批处理按 TAR 分配工作；保留证据行号，避免在 driver 解压整批数据或把全文收集至内存 |
| 联合无监督分析（P07） | scikit-learn IsolationForest、pandas、DuckDB | 在同一有日志 run 集合上比较结构化与联合模型；对排名变化案例移除对应的日志特征组后重训，关联原始证据。将 `score_samples` 转为高分表示异常的统一方向 | 约三万条 run 的特征矩阵可在协调节点上建模。记录时间切分、种子、缺失率、排名变化及 Top-K 重合。[模型接口](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html) |
| 查询、演示与协作（P08/P09） | DuckDB、Streamlit、Plotly.py、Git | 从已产出结果按工作流、run 和时间范围查询，绘制原型分布、时间线、异常排名与证据摘要；Git 记录源码与文档版本 | 演示端读取汇总表和索引，支持无日志状态。Git 权限见 [项目规则](AGENTS.md) |

## 课程资料与查阅路径

按原文件名和幻灯片页码查找课程群或组内共享课件，开发时对照实际发行版。

| 对应任务 | 课程资料 | 项目中的用法与对照入口 |
|---|---|---|
| P02 集群与存储 | `week1.2.Hadoop概述.pptx` 第 17 页，`week2.1.HDFS.pptx` 第 19–25、44–50、51–54 页，`week6.1.spark-general.pptx` 第 27–30 页 | 根据资源管理器、存储和执行器的职责划分节点。若采用 HDFS，核验输入与产物对各节点可见。按仓库保留日志 TAR，避免上传大量单条日志文件。[Spark 集群部署](https://spark.apache.org/docs/latest/cluster-overview.html) |
| P02-T05、P03、P05-T01 分区与窗口 | `week2.2.Mapreduce.pptx` 第 16、20–29 页，`week6.2.spark原理.pptx` 第 14–21、26–27 页 | 用按键重分区、宽依赖、Shuffle 和 Stage 理解聚合与窗口的成本。在同一全量作业比较两种分区设置，记录执行器、Shuffle 和 Stage 耗时。历史窗口依据实际可见时间构造。[Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html) · [Spark SQL 窗口函数](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html) |
| P04 画像聚类 | `week3.2.Kmeans.pptx` 第 3–8 页，`week4.1.机器学习.pptx` 第 50–56 页，`week4.2.聚类.pptx` 第 4–10 页 | 参照“分配到最近中心、重算质心”的迭代关系解释 Spark MLlib 结果。课件对距离和初始中心的讨论用于设计量纲、K 和随机种子的对照实验。[Spark MLlib 聚类](https://spark.apache.org/docs/latest/ml-clustering.html) |
| P06–P07 日志文本特征备选 | `week3.1.tfidf.pptx` 第 2–7 页 | 课件区分单篇词频与全语料文档频率。若规则指纹遗漏较多可重复的模式，可把归一化日志模板作为词、run 作为文档，试验只在训练集合拟合的 TF-IDF；现有规则指纹继续提供可回查证据。[Spark MLlib TF-IDF](https://spark.apache.org/docs/latest/ml-features.html#tf-idf) |

## 表的粒度与分析口径

P01 的 `runs_input.parquet` 从 19 个源字段中读取 13 列，保留时间字符串、状态和空值，供 P02 清洗。`log_archive_count`、`log_file_count` 和 `empty_archive_count` 属于源数据统计。`attempt_index.parquet` 记录课程 TAR 中的 ZIP 位置，ZIP 内容及目标步骤由后续阶段核验。[输出模式与命令](README.md#开发者第一次运行)

下表为 attempt 索引的现有字段与 P02–P07 的下游约定。主键非空，ID 使用 int64，attempt 为从 1 起的 int32，路径保留原文及大小写。下游用 Parquet 微秒 UTC 时间戳，Spark 会话设为 UTC，周键取周一日期。

| 粒度与产物 | 唯一键 | 最小字段、单位与空值 |
|---|---|---|
| run · `runs_clean` | `run_id` | `repo_id`、`workflow_path`、可空的 `workflow_id`，`created_at_utc` 作为归周时间。缺失或无法解析的创建时间交 P02 隔离。保留可空的 `updated_at_utc`、`event`、`status`、`conclusion`、`run_attempts`。从创建到更新的 `elapsed_seconds` 为浮点秒，更新时间缺失或倒序时记空并纳入质量统计 |
| 工作流 · `workflow_profile` | `repo_id, workflow_path` | `run_count`、`first_seen_utc`、`last_seen_utc` 和观测窗口。保留可空的 `workflow_id` 作一致性检查，同键多 ID 时报告冲突。计数为非负整数，比例取 0 到 1，未知与零分母记空 |
| 工作流周 · `workflow_week` | `repo_id, workflow_path, week_start_utc` | 周为 UTC 周一开始的半开七天区间。用 `observation_state` 区分 `complete`、`partial`、`before_first_observation`。全周可观测且无运行时 `run_count=0`，首次观测前为 null，不完整周保留已观测计数与状态 |
| attempt · `attempt_index`、`attempt_fingerprint` | `run_id, attempt` | 索引含 `repo_id`、输入根目录相对 `tar_path`、TAR 内 `tar_member` 与 ZIP 字节数 `archive_bytes`。指纹保留 `parse_status`（`parsed`、`no_target_member` 或 `failed`）、有效行数 `eligible_lines` 和信号计数。已解析零命中为 0，未能解析的特征为 null |
| 证据行 · `log_evidence` | `run_id, attempt, member, line_no` | `member` 是 ZIP 内文件名，`line_no` 是解码后原成员从 1 起算的行号，成员和行号不因过滤重排。同一行的多种命中放在其标签列表，展示字段为脱敏摘录 |

按工作流键和 UTC 周键将 run 分别连到一条画像与一条周记录。证据按 `run_id, attempt` 连接并保留归档与成员定位。run 级输出先聚合证据到 attempt，再聚合到 `run_fingerprint`，保留归档数、解析成功数和特征分母。与 `runs_clean` 左连，无归档行的日志特征为空。核对总行数与唯一 run 数均等于主表。模型分数、参数和证据键随批次保存。

样本取各场景最小的 run ID。单次工作流按全表计数，多 attempt 以至少两个已索引 ZIP 为准。另按键选最多 12 个有归档且有 20–150 次运行的工作流，保留完整记录。场景可共享 run，输出校验 run ID 唯一。批次 ID 由输入相对路径及内容 SHA-256、模式、接口版本和样本参数决定。写完隐藏目录后一次性重命名发布。复用时检查完成状态、批次 ID、产物列表和文件大小。

- **时间。** `updated_at - created_at` 表示从创建到最后更新的观测耗时，可能涵盖等待或重跑。报告标清代理量；精确到某执行阶段的结论须用日志时间戳核验。UTC 小时仅支持 UTC 口径的结论。
- **结果。** 跳过率取 `skipped / 全部 run`。建议失败率定义为 `failure / (success + failure)`，命名为“成功/失败结论中的失败率”；其余结论另列。分母为零记缺失。重试率取已知尝试次数的 run 中 `run_attempts > 1` 的比例，重跑次数为尝试次数减一；日志里的进程内重试另算。
- **覆盖。** 运行画像以 11,396 个有 run 的工作流为总体。教师提供的日志子集覆盖 475 个工作流；子集未提供归档、归档缺少目标步骤、已解析且零命中分别编码。对比实验的两组均取同一批可用 run。
- **时间切分。** 全期画像用于解释整体原型。回测时另建只用既往数据的特征与缩放参数；对只保留最新快照的结论、尝试数和耗时，用最后更新时间约束可见性。不能复原的中间 attempt 状态记作未知。

## 环境定版与复现

P02 由万明浩登记节点 OS/架构、JDK 发行版及主版本、Spark/PySpark 精确版本、Python 版本、共享路径、执行器数量与内存。按实装 Spark 选择兼容的 Java/Python，保持驱动与执行器 Python 小版本一致。把实测入口写入 README，冻结运行、实验和演示依赖。P08 全量复跑保存输入清单、代码版本、参数、时间范围和 Spark 应用 ID。

### 五节点巡检与环境现状（2026-11-22 实测）

- **统一基线**：五台节点（`10.176.62.226` ~ `10.176.62.230`，登录用户 `root`）均为 CentOS Linux 7 (Core) x86_64（内核 `3.10.0-327.el7.x86_64`），均安装 Oracle JDK `1.8.0_471`，系统默认 Python 为 `2.7.5`（暂未发现 `python3` 或 `conda`）；当前各节点均无现成网络共享挂载（`mount` 无 NFS/HDFS 共享挂载）。
- **节点明细**：
  - `node1`（`10.176.62.226`）：4 核（Xeon E5649 @ 2.53GHz）/ 7.6G 内存（可用 ~2.0G）；`/` 50G（可用 39G）、`/home` 48G（可用 48G）；`/opt/spark` 指向 `spark-2.0.0-bin-hadoop2.7`，另有 `spark-3.5.7-bin-hadoop3` 与 `/opt/hadoop`；当前无 Hadoop/Spark 守护进程运行。
  - `node2`（`10.176.62.227`）：4 核 / 7.6G 内存（可用 ~1.1G）；`/` 50G（可用 16G）、`/home` 48G（可用 34G）；`/opt/spark` 指向 `spark-2.0.0-bin-hadoop2.7`，另有 `spark-3.5.7-bin-hadoop3`；运行着 `NameNode`、`SecondaryNameNode`、`ResourceManager`。
  - `node3`（`10.176.62.228`）：8 核 / 15G 内存（可用 ~14G）；`/` 50G（可用 30G）、`/home` 48G（可用 6.1G，已用 88%）；`/opt/spark` 指向 `spark-2.0.0-bin-hadoop2.7`，另有 `spark-3.5.7-bin-hadoop3`；运行着 `DataNode`、`NodeManager`；已验证可 SSH 连接。
  - `node4`（`10.176.62.229`）：4 核 / 7.6G 内存（可用 ~1.3G）；`/` 50G（可用 29G）、`/home` 48G（可用 6.1G，已用 88%）；`/opt/spark` 指向 `spark-2.0.0-bin-hadoop2.7`，另有 `spark-3.5.7-bin-hadoop3`；运行着 `DataNode`、`NodeManager`。
  - `node5`（`10.176.62.230`）：4 核 / 7.6G 内存（可用 ~5.7G）；`/` 50G（可用 41G）、`/home` 48G（可用 48G）；`/opt/spark` 指向 `spark-2.4.8-bin-hadoop2.7`，另有 `spark-2.0.0-bin-hadoop2.7`、`spark-3.5.7-bin-hadoop3`；运行着 Spark Standalone `Master`/`Worker`（`spark://10.176.62.230:7077`）及 `NameNode`/`SecondaryNameNode`/`DataNode`（单机 `localhost` HDFS，容量约 49.98GB，可用约 40.89GB）。
- **存储与跨节点可读写路径（已实测）**：五台机器未安装 NFS 工具且无网络挂载，但 `node5`（`10.176.62.230`）运行着可用 HDFS（`Live datanodes (1): node5`，总容量 49.98 GB，可用 40.89 GB）。已在 `node5` 实测 `hdfs dfs -mkdir`、`-put` 写入、`-cat` 读出与 `-rm` 清理闭环，统一采用 **`hdfs://`** 路径（如 `hdfs:///ghawtrace/...`）作为跨节点作业的可读写数据路径（同时避开 `node3`、`node4` 已用 88% 的本地 `/home` 分区）。
- **选定部署方式与各机角色（P02-T01 定版）**：采用 **Spark Standalone** 模式，显式指定统一发行版 `/opt/spark-2.4.8-bin-hadoop2.7`（避免各机 `/opt/spark` 软链接指向 `2.0.0` 与 `2.4.8` 不一致，同时脚本兼容系统默认的 Python `2.7.5`）。各机角色划分及实测依据如下：
  - **协调 / Driver 提交节点：`node3`（`10.176.62.228`）**
    - **角色**：承担作业提交、全局任务调度以及 Driver 端结果收集汇总（同时可按余量挂载 Worker）。
    - **安排依据**：五台机器中 `node3` 硬件配置最高（8 核 CPU、15G 内存，当前可用约 14G，其余四台均为 4 核 / 7.6G 内存），最能承受 Driver 在调度分片与汇总结果时的内存开销，避免发生 OOM；同时该节点 `/home` 分区已用 88%（仅剩 6.1G），因此让其发挥计算与调度优势，而将大文件读写统一放到 `hdfs://`，避开本地 `/home` 空间短板。
  - **存储 / Standalone Master & Worker 节点：`node5`（`10.176.62.230`）**
    - **角色**：承担 HDFS 数据存储（NameNode / DataNode）以及 Spark Standalone Master（`spark://10.176.62.230:7077`）与 Worker。
    - **安排依据**：该节点已有运行正常且可读写验证通过的 HDFS 服务（可用容量 40.89 GB），同时本地 `/` 可用 41G、`/home` 可用 48G、内存可用约 5.7G；为避免其同时承担繁重的 Driver 调度而与存储/Master 进程争抢资源，故将其定位为存储与集群控制中心，把主要 Driver 负载交给 `node3`。
  - **计算 Worker 节点：`node1`（`10.176.62.226`）、`node2`（`10.176.62.227`）、`node4`（`10.176.62.229`）**
    - **角色**：作为纯计算工作节点（Worker）接入 `spark://10.176.62.230:7077` 分担分布式任务。
    - **安排依据**：这三台均为 4 核 / 7.6G 内存机型（其中 `node2`、`node4` 另有历史进程占用部分内存，当前可用约 1.1G~1.3G，`node1` 可用约 2.0G 且无历史进程干扰），不适合承担高内存占用的 Driver 或存储主节点，作为计算 Worker 按实际可用内存分配 Executor 更为稳妥；同时注意避开 `node4` 仅剩 6.1G 的 `/home` 分区。
