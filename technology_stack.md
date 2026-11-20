# 技术栈与数据接口

本表供开发者选用同一套运行环境。教师脚本可以在本地抽查 Parquet。集群作业、模型及演示应用是第 6 组的实施方案，五台机器的配置、框架版本和资源上限均待 P02 实测。版本锁定后在本表填入已验证组合，在 [第三方清单](THIRD_PARTY_NOTICES.md) 填入对应发行版。

## 按流程选型

| 流程与任务 | 工具 | 输入、处理与输出 | 运行位置和验证 |
|---|---|---|---|
| 原材解包与抽查（P00/P01） | unar、Python、PyArrow、pandas、DuckDB | 解开外层 RAR；PyArrow 按列/分片读取，pandas 查看小样本，DuckDB 直接扫描 Parquet 实现独立核数。教师脚本有 `iter_shards`、`load_runs`、`query` 三个入口 | 本地或协调节点；原件只读，输入目录由调用方传入 |
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

以课程发布或小组共享的课件为准，按下表的原文件名和页码查找。页码按幻灯片顺序计，技术实现再对照 P02 实测发行版的接口文档。

| 对应任务 | 课程资料 | 项目中的用法与对照入口 |
|---|---|---|
| P02 集群与存储 | `week1.2.Hadoop概述.pptx` 第 17 页，`week2.1.HDFS.pptx` 第 19–25、44–50、51–54 页，`week6.1.spark-general.pptx` 第 27–30 页 | 根据资源管理器、存储和执行器的职责划分节点。若采用 HDFS，核验输入与产物对各节点可见。按仓库保留日志 TAR，避免上传大量单条日志文件。[Spark 集群部署](https://spark.apache.org/docs/latest/cluster-overview.html) |
| P02-T05、P03、P05-T01 分区与窗口 | `week2.2.Mapreduce.pptx` 第 16、20–29 页，`week6.2.spark原理.pptx` 第 14–21、26–27 页 | 用按键重分区、宽依赖、Shuffle 和 Stage 理解聚合与窗口的成本。在同一全量作业比较两种分区设置，记录执行器、Shuffle 和 Stage 耗时。历史窗口依据实际可见时间构造。[Spark Web UI](https://spark.apache.org/docs/latest/web-ui.html) · [Spark SQL 窗口函数](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html) |
| P04 画像聚类 | `week3.2.Kmeans.pptx` 第 3–8 页，`week4.1.机器学习.pptx` 第 50–56 页，`week4.2.聚类.pptx` 第 4–10 页 | 参照“分配到最近中心、重算质心”的迭代关系解释 Spark MLlib 结果。课件对距离和初始中心的讨论用于设计量纲、K 和随机种子的对照实验。[Spark MLlib 聚类](https://spark.apache.org/docs/latest/ml-clustering.html) |
| P06–P07 日志文本特征备选 | `week3.1.tfidf.pptx` 第 2–7 页 | 课件区分单篇词频与全语料文档频率。若规则指纹遗漏较多可重复的模式，可把归一化日志模板作为词、run 作为文档，试验只在训练集合拟合的 TF-IDF；现有规则指纹继续提供可回查证据。[Spark MLlib TF-IDF](https://spark.apache.org/docs/latest/ml-features.html#tf-idf) |

## 表的粒度与分析口径

以下为约定的逻辑表名，物理路径在 P01 配置。原始 runs 的 19 列含字符串时间戳和较大的 `github_metadata_json`，全量主线仅读必要列。

| 表 | 唯一键 | 关键字段或约束 |
|---|---|---|
| `runs_clean` | `run_id` | 保留 `repo_id`、`workflow_id`、原样 `workflow_path`、UTC 时间、事件、状态、结论与尝试次数；一个 run 只计一次 |
| `workflow_profile` | `repo_id, workflow_path` | 路径按原值精确匹配；保留 workflow_id 用于一致性检查。画像汇总观测窗口内的行为 |
| `workflow_week` | `repo_id, workflow_path, week_start_utc` | 显式记录完整周、零运行、样本量与观测边界；基线只使用评分时点之前已知的信息 |
| `attempt_fingerprint` / `run_fingerprint` | 分别为 `run_id, attempt` / `run_id` | 保存结构化解析状态、覆盖与分母；明确采用求和、最大值或按有效行数加权的字段，归并后与 runs 一对一连接 |
| `log_evidence` | `run_id, attempt, member, line_no` | 带仓库归档位置；每行可有多个信号标签，展示脱敏短摘录。区分整 job 文本与其步骤副本 |
| 模型结果 | 原型用工作流键；行为/联合分数用 run 键；生命周期用工作流与周/区间 | 保存运行批次、模型/规则版本、基线或对照、分数方向、原因及证据键；跨批查询同时指定批次 |

- **时间。** `updated_at - created_at` 表示从创建到最后更新的观测耗时，可能涵盖等待或重跑。报告标清代理量；精确到某执行阶段的结论须用日志时间戳核验。UTC 小时仅支持 UTC 口径的结论。
- **结果。** 跳过率取 `skipped / 全部 run`。建议失败率定义为 `failure / (success + failure)`，命名为“成功/失败结论中的失败率”；其余结论另列。分母为零记缺失。重试率取已知尝试次数的 run 中 `run_attempts > 1` 的比例，重跑次数为尝试次数减一；日志里的进程内重试另算。
- **覆盖。** 运行画像以 11,396 个有 run 的工作流为总体。教师提供的日志子集覆盖 475 个工作流；子集未提供归档、归档缺少目标步骤、已解析且零命中分别编码。对比实验的两组均取同一批可用 run。
- **时间切分。** 全期画像用于解释整体原型。回测时另建只用既往数据的特征与缩放参数；对只保留最新快照的结论、尝试数和耗时，用最后更新时间约束可见性。不能复原的中间 attempt 状态记作未知。

## 环境定版与复现

P02 由万明浩记录各节点 OS/架构、JDK 发行版及主版本、Spark 和 PySpark 精确版本、驱动与执行器的 Python 版本、共享路径、执行器数量和内存。根据实装 Spark 发行版选兼容的 Java/Python，保持驱动与执行器 Python 小版本一致。在 README 增补实测入口，同时按运行、实验和演示环境冻结依赖。P08 每次全量复跑保留输入清单、代码版本、参数、时间范围和 Spark 应用 ID；授课材料、开源软件和提交包的来源登记见 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)。
