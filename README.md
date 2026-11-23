# Agent 运行行为画像与异常发现

大数据分析技术期末项目 · 第 6 组 · 选题 5

本项目从 GitHub Agent 工作流提取行为画像，识别单次运行与长期节律的异常，并用执行日志寻找仅凭运行指标难以发现的问题。最终在课程集群批处理，通过演示追溯到工作流、run 和日志行。

## 当前可用内容

P00 已核验原始数据。P01 已交付可重跑的输入准备与共用样本，供集群、特征和日志模块联调。P03 已交付工作流画像、连续周面板与按历史截点特征，本地实测 4,112,526 个 run、11,396 个工作流与 59 个 UTC 周。Spark 清洗、聚类、检测和演示见 [后续阶段](plan.md)。

课程资料可从课程群或组内共享目录按以下文件名获取。Git 记录项目代码和文档，原件与生成批次保存在本地或共享存储。

| 材料 | 内容 |
|---|---|
| `大数据分析技术项目-2026版.pptx` | 第 4 页是选题，第 5–6 页是集群、汇报与提交要求 |
| `GHAWTrace_ref/runs.rar` | 165 个 Parquet 分片，4,112,526 个唯一 run，覆盖 2,969 个仓库和 11,396 个有记录的工作流 |
| `GHAWTrace_ref/logs.rar` | 92 个仓库 TAR，内含 30,427 个非空 attempt ZIP，对应 30,393 个 run 和 475 个工作流，均可关联运行表 |
| `GHAWTrace_ref/2611.06866v1.pdf` | 采集、日志留存与案例。25,014 个核验工作流中有 13,618 个没有观测到运行 |
| `GHAWTrace_ref/read_runs.py` | 教师提供的整表、逐分片及 DuckDB SQL 示例，默认使用 Windows 路径 |

## 开发者第一次运行

组员可直接使用共享的已完成批次。自行生成时使用 Python 3.11 或更高版本（实测为 3.12.14）和 `unar`。若已有 `runs/*.parquet` 与 `logs/*.tar`，将 `P01_INPUT` 指向该目录并跳过解包。

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt

P01_INPUT="$PWD/.local/ghawtrace"
mkdir -p "$P01_INPUT"
unar -q -D -o "$P01_INPUT" GHAWTrace_ref/runs.rar
unar -q -D -o "$P01_INPUT" GHAWTrace_ref/logs.rar

python -m ghawtrace.pipeline --input-root "$P01_INPUT" --output-root .local/p01 --mode sample
python -m unittest discover -s tests -v
```

- 两种模式均需读取课程原始分片和 TAR。将 `--mode sample` 改为 `--mode full` 可准备全量输入。命令返回 `batch_dir` 与 `reused`。已完成批次的目录为非隐藏目录，包含两张 Parquet 表和状态为 `success` 的 `manifest.json`。
- 课程样本有 **747 个 run 与 294 个 attempt 索引**。`sample_manifest.json` 列出 run ID、六类样例和 `cohorts` 中的 12 个完整工作流。做周序列或工作流统计时用这 12 个工作流。合成边界案例在测试中单独生成。
- `runs_input.parquet` 保留原始列投影，`attempt_index.parquet` 通过 `tar_path` 和 `tar_member` 定位源 TAR 与 ZIP。同一批次满足完成条件时会复用。[数据与批次约定](technology_stack.md#表的粒度与分析口径)

### P03 工作流画像与周特征

P03 读取 P01 批次目录（优先 `runs_input.parquet`）、P02 的 `runs_clean`、单个 Parquet 或嵌套分区目录（如 `year=2026/month=01/part-00000.parquet`），写出四张表：`workflow_profile.parquet`、`workflow_event_mix.parquet`、`workflow_week.parquet` 和 `workflow_asof.parquet`。样本批次约 1–3 秒，全量 4,112,526 个 run 约 1 分钟。

```sh
P01_BATCH="$(ls -d .local/p01/* | head -n 1)"   # 样本或全量批次均可
python -m ghawtrace.feature_pipeline --runs-input "$P01_BATCH" --output-root .local/p03
```

- 命令返回 `batch_dir` 与 `reused`。批次目录含四张 Parquet 和 `manifest.json`，记录输入 SHA-256、参数、分步行数、11 项对账结果及产物行数和字节数；重复运行同一输入会复用已完成批次。入口冻结输入文件清单，发布前核验分片成员、输入快照、样本清单和截点文件哈希未变。
- 画像以 `(repo_id, workflow_path)` 为键，含事件与 UTC 小时分布、活跃天数、运行间隔、completed run 的耗时与重试分位、跳过率和失败率；进行中/排队的 run 只计入活动类计数和 `open_run_count`。周面板按 UTC 周一对齐 59 周，区分 `complete`、`partial`、`before_first_observation` 和样本覆盖不足时的 `unknown`；`workflow_asof` 只给截点前已出现的工作流出全局行，结果类特征要求 completed、结论非空且 `updated_at` 早于截点，供 P05、P07 复用。字段与分母见[数据口径](technology_stack.md#表的粒度与分析口径)，手算验收见 [test_p03.py](tests/test_p03.py)。
- P01 样本会在输入目录内自动读取 `sample_manifest.json`，把 12 个 cohort 工作流视为完整观测；其余锚点工作流有 run 的周记 `partial`，无观测的周记 `unknown`（null），不会伪造零运行周。单文件或改名目录可用 `--sample-manifest` 显式传入。`--cutoffs my_cutoffs.parquet` 可指定 `cutoff_id`、`cutoff_utc`（可选 `repo_id`、`workflow_path` 限定单个工作流，两列必须同时为空或同时非空），`--cutoff-grid none` 可跳过截点表。

### 教师脚本双分片抽查（可选）

在 macOS/Linux 的项目虚拟环境中安装 `pyarrow` 和 `pandas`，用 `unar` 解出两个分片抽查。仅需教师脚本和 `runs.rar`。

```sh
export PJ_SAMPLE_DIR="$(mktemp -d)"
unar -q -D -o "$PJ_SAMPLE_DIR" GHAWTrace_ref/runs.rar \
  runs/part-00000.parquet runs/part-00001.parquet
python3 - <<'PY'
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path("GHAWTrace_ref").resolve()))
import read_runs

read_runs.RUNS_DIR = str(Path(os.environ["PJ_SAMPLE_DIR"]) / "runs")
columns = ["run_id", "event", "conclusion", "created_at", "run_attempts"]
shard_sizes = [len(frame) for frame in read_runs.iter_shards(columns)]
assert shard_sizes == [25_000, 25_000], shard_sizes
print(f"{len(shard_sizes)} 个分片，共 {sum(shard_sizes):,} 行")
PY
```

预期输出 `2 个分片，共 50,000 行`。核对全表时，将 `RUNS_DIR` 指向已解包的 165 个 Parquet 分片所在目录，安装 `duckdb` 后调用 `read_runs.query("SELECT count(*) AS n FROM runs")`，预期为 4,112,526。

## 系统设计与开发顺序

- **集群与基础分析。** 万明浩盘点五节点并跑通 Spark 作业。黄志成先手算事件比例和失败率，再从清洗表产出工作流画像、连续周面板和历史特征。季雨昊用画像开发聚类与稳定性实验。刘陈昱实现按历史评分的运行级检测器，龙启波检测周序列的沉寂、恢复与突发并负责时间回测。
- **日志与演示。** 张锦稷以 P01 的 TAR 索引为起点，审计覆盖、解析指纹和定位证据。刘陈昱比较同一批可用 run 的结构化与日志联合评分，并集成原型分布、时间线和证据查询。
- **环境与资料。** 全量运行创建时间为 2025-08-14 至 2026-09-21（UTC）。P02 盘点课程分配的五台机器（`10.176.62.226` 至 `10.176.62.230`，登录用户 `root`）后定节点角色。Week 6 的 Spark 课件和 Week 3–4 的聚类资料可按文件名与页码查阅。[课程索引](technology_stack.md#课程资料与查阅路径)

分工和验收见 [计划](plan.md)，输入输出见 [技术栈](technology_stack.md)。开始或恢复时切到对应 P 阶段或 [A00](plan.md#a00) 小项的固定分支。[协作与 Git 规则](AGENTS.md) 约定任务、分支和授权。验收后更新计划与 [历史](history.md)，调整依赖时更新 [第三方清单](THIRD_PARTY_NOTICES.md)。

## 课程交付

每组最多六人，使用集群搭建实验环境，报告与系统演示合计 20–30 分钟。PPT 讲清项目完成内容、过程、数据集、经验与小组分工。按老师对个人评分的说明，每人都展示自己的课程技术实现和完成证据。最终提交**源代码及使用说明、PPT、项目文档**；具体截止时间与提交入口以教师后续通知为准。
