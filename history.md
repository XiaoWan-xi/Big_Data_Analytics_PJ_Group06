# 项目开发历史

按日期与任务号追加。交付时登记实际实现者（含开发助手）与复核者、代码及版本、复跑命令和数据范围、结果位置与限制。多轮实验记参数变化与选用理由。[验收清单](plan.md)

## 2026-11-20 · P00-T01 · 课件与分组核对

- 开发助手依据腾讯文档第 6 组截图核对六名成员及五台机器的分配。主机配置和连通性待 P02 实测。[成员与分配](README.md#系统设计与开发顺序)
- 核对 `大数据分析技术项目-2026版.pptx` 第 4–6 页的选题、集群、汇报和提交要求。教师另明确按个人分工及完成情况评分。[课程交付](README.md#课程交付)

## 2026-11-20 · P00-T02 · 运行表、论文和日志核数

开发助手临时解包两份 RAR，以 PyArrow 逐片核对主键、工作流路径、日期、事件和结论，检查 TAR 中 ZIP 的成员数，再按 run ID 回查仓库键。P02 将独立复现下表基线。

| 检查 | 结果与口径 |
|---|---|
| 运行表 | 165 个 Parquet，19 列；4,112,526 行，run ID 去重亦为 4,112,526 |
| 工作流 | 2,969 个仓库；以仓库和精确路径计 11,396 个工作流；其中 1,186 个工作流在观测窗口只有一次运行 |
| 时间与结论 | UTC 创建时间从 2025-08-14 10:53:05 到 2026-09-21 20:38:02；`skipped` 有 2,410,478 行（58.6%），`success` 有 797,274 行，`failure` 有 526,740 行，另有其他结论 |
| 日志结构 | 92 个按仓库 ID 命名的 TAR；内部为 `<run_id>/attempt-<n>.zip`。30,427 个 ZIP 均非空，`run_id + attempt` 全部唯一 |
| 跨表关联 | 30,393 个不同 run，对应 475 个工作流；全部在运行表中找到，归档仓库与运行表一致 |
| 尝试编号 | 编号 1 有 30,290 个 ZIP，编号 2 有 128 个，编号 3 有 7 个，编号 4 有 2 个。每次尝试单独计数，保留的归档允许缺少较早尝试 |
| 文本抽查 | 在 run `28217390395` 的 attempt 1 中读到 `3_agent.txt`，共 3,603 行，含时间戳和步骤分组标记。可用归档、成员、行号回查信号；此项仅为格式抽查 |

论文全库的 562,877 个非空归档属于 561,367 个 run，受采集时点与日志留存限制，课程子集的分母见上表。教师脚本默认读 17 列，可加 `head_sha` 和 `github_metadata_json`。本地示例导入后覆盖其 Windows 路径。[课程材料与命令](README.md)

## 2026-11-20 · P00-T03 · 文档与六人开发分工

建立六份文档与个人交付约定，确定先接口、再并行产表及建模、最后集成。Git 授权写入 [AGENTS.md](AGENTS.md)，第三方清单先登记候选直接依赖，沿用指定示例的版本、用途、许可及依赖分类。

开发助手在 CPython 3.12.14、PyArrow 25.0.1、pandas 2.2.3 下用教师 `iter_shards` 读取前两片，共 50,000 行。P08 负责全量集群复现。

## 2026-11-20 · P05 / P07 · 异常检测分工与验收口径

确定刘陈昱负责运行级 Spark 检测与同批日志评分，龙启波负责工作流生命周期区间检测与时间回测。验收分别按 run 核对分数、按工作流区间核对告警、按共同样本比较排名并回查证据。开发待 P05 和 P07。

## 2026-11-21 · P00-T04 · 协作规则与课程资料索引

建立 A00 永久编号与影响项关联，固定每个 P 阶段和 A00 小项的分支。按原名和页码核对 HDFS、MapReduce、Spark、K-means、聚类和 TF-IDF。P02 增加 Stage/Shuffle 观测指标，TF-IDF 用作日志备选实验。[课程索引](technology_stack.md#课程资料与查阅路径)

初始化独立仓库，`main` 首次提交 `3b961e2` 包含六份文档和 `.gitignore`。课件与教师原件按 README 获取。

<a name="p01-acceptance"></a>

## 2026-11-21 · P01 · 数据接口、共用样本与批次入口验收

阶段主责刘陈昱，开发助手实现并执行本地验收。使用 CPython 3.12.14、PyArrow 25.0.1、165 个运行分片和 92 个仓库 TAR。按 [README](README.md#开发者第一次运行) 解包和复跑，样本保存在项目的 `.local/p01/`，入口为 [ghawtrace.pipeline](ghawtrace/pipeline.py)，自动化验收为 [test_p01.py](tests/test_p01.py)。

| 项目 | 核验步骤与实测结果 |
|---|---|
| P01-T01 · 连接约定 | 按 [数据接口](technology_stack.md#表的粒度与分析口径) 复核真实样本。747 个 run 归到 17 个工作流、202 个工作流周，按对应键连接均保留 747 行。直接左连 attempt 会扩到 750 行，按 run 聚合后左连保留 747 个唯一 run。合成用例另验证同一 run 两个 attempt、三个证据行与没有归档的 run。 |
| P01-T02 · 可复现样本 | 核验单次工作流、跳过、失败、有归档、无归档、多 attempt 六类锚点。跳过与无归档共用一条 run。样本共 747 个 run 和 294 个唯一 attempt。将其中 12 个完整工作流与全量表逐一对账。换输出目录重新生成，ID 清单和两张 Parquet 文件逐字节一致。 |
| P01-T03 · 输入准备与恢复 | 全量模式读写 4,112,526 个 run，索引 30,427 个 attempt，manifest 记录相对路径、内容 SHA-256、模式、参数和分步行数。同目录重跑样本和全量均返回 `reused: true`。测试在两张表写入后、完成清单写入前强制退出，只有隐藏临时目录。重试成功。两个进程同时写入时只发布一个批次。 |

`python -m unittest discover -s tests -v` 共 5 项通过。全量实测范围为本地输入准备，Spark 多节点作业和数据质量审计由 P02 验收。

## 2026-11-21 · P01 · 文档复核

逐一核对六份文档与代码、依赖、测试及版本管理范围。计划的 34 条验收保持原文，课程本地原件用路径标识。重新生成样本和全量批次，校验表结构、行数与复用结果。新旧样本清单和两张表逐字节一致。教师脚本双分片复读 50,000 行，5 项测试通过。

## 2026-11-22｜P02-T01 五节点环境与共享存储摸底

- **主责与分支**：万明浩，工作分支 `codex/p02`。
- **执行方法**：对 `10.176.62.226` ~ `10.176.62.230`（`node1` ~ `node5`）逐台执行系统、CPU/内存/磁盘、Java/Python/Spark/Hadoop、挂载与进程巡检，并在 `node5` 核查跨节点 SSH 连通性、`hdfs dfsadmin -report` 及 Hadoop/Spark 配置目录。
- **关键结果与发现**：
  - 五台机器均为 CentOS 7 x86_64、JDK `1.8.0_471`、默认 Python `2.7.5`（无 `python3`/`conda`），`node3` 资源最充裕（8 核 / 15G，可用 ~14G），`node5` 可用内存约 5.7G 且有一套单机 `localhost` HDFS
  - 各节点 `/opt/spark` 默认软链接不一致（`node1`~`node4` 为 `2.0.0`，`node5` 为 `2.4.8`，且各机均备有 `3.5.7`）；历史上存在多组守护进程残留；五台节点默认均无网络共享挂载，`node3`/`node4` 的 `/home` 剩余约 6.1G。

## 2026-11-22｜P02-T01 存储路径验证与部署定版

- **主责与分支**：万明浩，工作分支 `codex/p02`。
- **执行方法与证据**：
  1. 在 `node5` 核查 `nfs-server`、`rpcbind`、`exportfs`、`showmount` 与 `mount.nfs`，确认五台节点均未安装 NFS 服务及客户端工具。
  2. 在 `node5` 执行 HDFS 读写删全链路实测（`/opt/hadoop/bin/hdfs dfs -mkdir -p /tmp/p02_test` → 写入 `hello_p02_1795359151` → `-cat` 读回校验一致 → `-rm -r -f /tmp/p02_test` 清理），验证 `hdfs://` 路径可正常读写。

## 2026-11-23｜P02-T02 多节点 `spark-submit` 验证闭环与节点角色安排
- **节点角色调整原因**：
    1. 原 T1 草案计划由 `node5` 兼任 Master 与存储、`node3` 作 Driver、`node1/2/4` 作 Worker；实测 `node3`（Spark `2.0.0`）连接 `node5`（Spark `2.4.8`）Master 时因跨版本通信协议不兼容反复报 `java.io.EOFException`。
    2. 尝试在 `node3` 启动 Spark `2.0.0` Master 时，因 `/opt/spark/conf/spark-env.sh` 预设 `SPARK_MASTER_HOST='10.176.62.226'` 触发 `java.net.BindException: Cannot assign requested address`。
    3. 最终改由 `node1`（`10.176.62.226`）承担 Spark `2.0.0` Master 与提交端（Driver），`node5` 专注提供 HDFS 存储服务，同版本节点作为 Worker 接入。
- **P02-T02 分布式工作测试实测证据**：
    - 提交命令：`spark-submit --master spark://10.176.62.226:7077 --deploy-mode client --conf spark.hadoop.fs.defaultFS=hdfs://10.176.62.230:9000 /tmp/p02_t02_smoke.py ...`
  - **Application ID**：`app-20261122202423-0003`
  - **多节点执行器分布**：实测调度至 `10.176.62.228`（`node3`）与 `10.176.62.229`（`node4`）两个不同工作节点。
  - **读取 → 分组 → 写回结果**：读取 `hdfs://10.176.62.230:9000/tmp/p02_t02_in/sample.tsv`，按 `grp_key` 聚合得到 `alpha (row_cnt=2, val_sum=3)`、`beta (row_cnt=2, val_sum=8)`、`gamma (row_cnt=1, val_sum=4)`；写回 `hdfs://10.176.62.230:9000/tmp/p02_t02_out`，生成 `_SUCCESS` 及 2 个 `.snappy.parquet` 分片文件。
  - **存储排障记录**：初期使用简写 `hdfs:///...` 时因 `node1` 默认 `fs.defaultFS=hdfs://node1:9000` 触发 `Connection refused` 及 Worker 侧 `UnknownHostException: node1`，通过显式指定完整 `hdfs://10.176.62.230:9000` 路径及 `--conf spark.hadoop.fs.defaultFS=hdfs://10.176.62.230:9000` 解决。
  - **算力池扩容验证**：在 T2 验收通过后，进一步将同版本 `node2`（`10.176.62.227`，Spark `2.0.0`）作为 Worker 成功注册至 `spark://10.176.62.226:7077`，集群计算池扩展为 `node2`/`node3`/`node4` 共 3 个工作节点，为后续分布式清洗与覆盖率审计预留算力。

## 2026-11-23｜P02-T03 全量 `runs` 分布式清洗、对账与可复现验证闭环**
  - **兼容性排障与前置转码**：单片试跑暴露原始 Parquet 使用 `ZSTD` 压缩，Spark `2.0.0` 底层 Thrift 解析报 `Required field 'codec' was not present`。通过 `ghawtrace/convert_for_spark2.py` 严格按 P01 `RUN_INPUT_SCHEMA`（13 列，裁除体积巨大的 `github_metadata_json`）无损转为 `Parquet 1.0 + Snappy`（165 片总计 83.7 MB），单片试跑（25,000 行）随即一次通过。
  - **全量双跑实测证据**（3 Worker：`node2`/`node3`/`node4`，`--executor-cores 2 --executor-memory 1536m`，`spark.sql.shuffle.partitions=36`，JVM 时区 `UTC`）：
    - 第 1 遍：`app-20261123191035-0006` → `hdfs://10.176.62.230:9000/data/ghawtrace/p02_t03_run1`（报告：[run1_report](reports/p02/p02_t03_run1_report.json)）
    - 第 2 遍：`app-20261123192550-0007` → `hdfs://10.176.62.230:9000/data/ghawtrace/p02_t03_run2`（报告：[run2_report](reports/p02/p02_t03_run2_report.json)）
  - **清洗与对账结果**：
    - 全量读入核对：165 片共读入 `4,112,526` 行，`distinct_run_ids = 4,112,526`（与数据集论文总数一致）。
    - 保留与隔离明细：保留 `4,112,269` 行（`runs_clean`，99.994%），隔离 `257` 行（`runs_quarantine`，0.006%）。逐项校验结果：主键非法 `0`、主键重复 `0`、UTC 时间非法 `0`、负耗时 `0`、`status` 为空 `0`、`conclusion` 为空 `257`。
    - 隔离业务归因：257 条 `NULL_CONCLUSION` 全部来自数据抓取截止时尚未结束的运行（`queued`: 176、`pending`: 73、`in_progress`: 8），已完成（`completed`）记录零缺失。
    - 严格对账与重复运行一致：`4,112,269 + 257 == 4,112,526`（`reconciled_equals_input: true`）；两次全量运行规范化报告 `diff` 零差异，落盘回读指纹一致（`written_clean_rows=4,112,269`、`min_run_id=16963156550`、`max_run_id=35652296872`、`sum_duration_sec=71,829,545,485`）

## 2026-11-23｜P02-T05 全量聚合作业分区策略对比与最终配置固化**
- **实验设计**：基于 P02-T03 产出的全量干净表 `runs_clean`（`4,112,269` 行），执行相同的 `(repo_id, event, conclusion)` 全量分组聚合与 Parquet 落盘，固定资源配置（3 台 Worker，实测 8 个 Executor × 2 核 / 1536m），对比 `spark.sql.shuffle.partitions=200`（默认基线）与 `24`（调优配置），通过 Spark Web UI REST API 自动采集 Stage 与 Shuffle 指标。
  - **实测结果对比**：
    - **配置 A（`p=200`，`app-20261123195405-0008`）**：输入 `18` / 输出 `200` 分片（HDFS 200 个 Parquet 文件），耗时 `33.029 s`（Executor 累计 `107,660 ms`），`3` Stage / `219` Task，Shuffle 读写各 `3,894,637 B`（`89,631` 条），Spill `0 B`。
    - **配置 B（`p=24`，`app-20261123195451-0009`）**：输入 `18` / 输出 `24` 分片（HDFS 24 个 Parquet 文件），耗时 `23.906 s`（Executor 累计 `68,385 ms`），`3` Stage / `43` Task，Shuffle 读写各 `3,132,793 B`（`89,631` 条），Spill `0 B`。
  - **一致性与选型结论**：两组配置输出均为 `10,246` 行（`sum_run_cnt=4,112,269`、`sum_duration_sec=71,829,545,485`、`sum_nonempty_log_runs=561,366`），逐行内容 SHA-256 完全一致（`b9f0191f90f927ed87145432569e258c60678f62939069c74e5b55c9161ead3f`）。最终选定 `spark.sql.shuffle.partitions=24`：端到端提速 27.6%、CPU 时间降 36.5%、Task 数降 80.4%、Shuffle 字节降 19.6%、输出小文件降 88% 且无溢写（报告：[p200_report](reports/p02/p02_t05_p200_report.json)、[p24_report](reports/p02/p02_t05_p24_report.json)）。
