# 第三方软件与材料声明

本清单记录第 6 组项目所用的第三方材料、直接依赖和环境工具。P01 的运行依赖在 [requirements.txt](requirements.txt) 中固定。“本地实测”指已运行的本地示例和验收环境，“待锁定”由后续安装或打包结果回填。各项目的完整版权与许可声明以所用发行版附带的文件为准。

## 数据集、论文与课程材料

| 材料 | 作者、版本或来源 | 本项目用途与声明 |
|---|---|---|
| GH-AWTrace | Jasem Khelifi、Hao Li、Ali Ouni、Mohammed Sayagh、Mohamed Aymen Saied；本地快照由教师以 `runs.rar`、`logs.rar` 提供 | 用于运行画像和日志分析；课堂日志包是原数据集的子集。本地包对应的上游修订号尚未取得。作者说明源仓库各有条款，数据集卡未给第三方工作流文件和日志统一授权。[上游数据集及引用说明](https://huggingface.co/datasets/jaykay9999/GHAWTrace) |
| *GH-AWTrace: A Large-Scale Dataset of GitHub Agentic Workflow Execution Logs* | 同上五位作者，2026，arXiv:2611.06866v1 | 数据来源、采集方法与使用限制的引文。项目文档和 PPT 引用论文，复用图表或长段材料时另核具体来源。[论文 v1](https://arxiv.org/abs/2611.06866v1) |
| 运行表读取脚本 | 教师提供，[本地原件](GHAWTrace_ref/read_runs.py)，文件中未标明独立版本、作者或开源许可证 | 小样本验证和字段读取参考。若提交或公开衍生代码，区分团队实现与教师原件，并按课堂允许的范围处理 |
| 2026 版项目介绍 PPT | 大数据分析技术课程提供，[本地原件](大数据分析技术项目-2026版.pptx) | 用于核对选题与考核要求；未见单独的开源许可说明 |

技术报告建议引文：Khelifi, J., Li, H., Ouni, A., Sayagh, M., and Saied, M. (2026). *GH-AWTrace: A Large-Scale Dataset of GitHub Agentic Workflow Execution Logs*. arXiv:2611.06866v1. [作者论文](https://arxiv.org/abs/2611.06866v1)

## 分析、实验与演示的直接依赖

| 上游项目 / 包 | 使用范围 | 当前版本依据 | 上游主许可证与依据 |
|---|---|---|---|
| Apache Spark / PySpark（含 Spark SQL、MLlib） | 分布式清洗、特征、聚类 | 方案选定，待集群定版 | Apache-2.0，发行版还带有组件声明。[LICENSE](https://github.com/apache/spark/blob/master/LICENSE) · [NOTICE](https://github.com/apache/spark/blob/master/NOTICE) |
| Apache Arrow / pyarrow | 教师脚本与 P01 Parquet 读写 | P01 固定并实测 25.0.1，集群待锁定 | Apache-2.0，随附组件许可见文件后部。[LICENSE](https://github.com/apache/arrow/blob/main/LICENSE.txt) |
| pandas | 教师读取示例、有限规模的结果表 | 本地实测 2.2.3，集群待锁定 | BSD-3-Clause。[LICENSE](https://github.com/pandas-dev/pandas/blob/main/LICENSE) |
| DuckDB / duckdb | 教师 SQL 接口、独立复算与结果查询 | 教师脚本已引用，项目版本待锁定 | MIT。[LICENSE](https://github.com/duckdb/duckdb/blob/main/LICENSE) |
| scikit-learn | ARI、Isolation Forest、联合实验 | 计划引入，待锁定 | BSD-3-Clause。[COPYING](https://github.com/scikit-learn/scikit-learn/blob/main/COPYING) |
| Streamlit | Demo 查询与页面交互 | 计划引入，待锁定 | Apache-2.0。[LICENSE](https://github.com/streamlit/streamlit/blob/develop/LICENSE) |
| Plotly.py / plotly | 原型分布、运行与生命周期时间线 | 计划引入，待锁定 | MIT。[LICENSE](https://github.com/plotly/plotly.py/blob/main/LICENSE.txt) |

## 运行环境和开发工具

| 项目 | 使用情形及版本状态 | 许可依据 |
|---|---|---|
| CPython 及标准库 | 本地示例实测 3.12.14；集群待定。`tarfile`、`zipfile`、`io`、`re` 随解释器提供 | PSF License Agreement 及发行版列出的历史和组件许可。[官方许可](https://docs.python.org/3/license.html) |
| The Unarchiver CLI / unar、lsar | 本地解包与检查 RAR；系统工具，开发环境自行提供 | 本机安装包附 LGPL v2.1 文本，Homebrew 配方标注 LGPL-2.1-or-later。[上游命令行工具](https://theunarchiver.com/command-line) · [配方](https://formulae.brew.sh/formula/unar) |
| JVM / JDK | 运行 Spark；供应方、版本、发行条款在 P02 盘点后填写 | 以所选发行版的 LICENSE 和第三方声明为准 |
| Apache Hadoop（条件使用） | 采用现有 YARN 或 HDFS 时登记实际版本；随 Spark 提供的客户端按实装包记录 | Apache-2.0，另含组件许可。[LICENSE](https://github.com/apache/hadoop/blob/trunk/LICENSE.txt) |
| Git | 计划用于本地版本管理；初始化时记录开发工具版本 | GPL v2，具体以项目 COPYING 为准。[COPYING](https://github.com/git/git/blob/master/COPYING) |

Parquet 一栏由实际读写实现（Spark、PyArrow 或 DuckDB）覆盖。团队自产的特征、模型和文档另行确定项目授权范围。

## 锁定、更新与提交

1. P02 确定环境后，按分析、演示、开发工具分别记录包名、精确版本和来源；保存依赖清单及解析出的传递版本。只在某个成员机器上跑过的版本继续标为本地实测。
2. 新增或移除依赖时更新此表。如复制上游实现、示例、图片或其他资产，一并登记原作者、原地址、修订号、修改情况和许可文件位置。
3. P09 对照实际提交包和安装环境核查直接与传递依赖。分发第三方文件时保留其要求的版权、LICENSE、NOTICE 或其他声明；仅由集群外部提供的工具注明运行环境来源。把核对版本和日期记入 [history.md](history.md)。

登记结构参考刘陈昱另一项目的第三方声明，保留运行、开发、传递依赖与版本依据的区分。[参考文档](https://github.com/lcy0121/dsh-plugin-compat-check/blob/main/THIRD_PARTY_NOTICES.md)
