# datawarehouse
Flink实时数仓，基于flink1.12

1.启动集群步骤
sudo systemctl start docker
/home/atguigu/docker_bigdata/contains.sh start

2.启动日志服务器
hadoop.sh start
zk.sh start
kafka.sh start
log.sh start



# 📊 Data Warehouse Project

## 🚀 项目简介

本项目是一个完整的数据仓库（Data Warehouse）建设实践，涵盖数据采集、数据处理、建模分层、数据服务等核心流程，旨在构建一个高效、稳定、可扩展的数据分析平台。

项目围绕离线数仓 / 实时数仓场景设计，支持业务数据的统一管理与分析，为 BI 报表、数据分析和业务决策提供支撑。

---

## 🏗️ 技术架构

### 🔧 核心技术栈

- 数据采集：Kafka / Flume / CDC
- 计算引擎：Spark / Flink
- 存储层：Hive / HDFS / Iceberg / Paimon
- 调度系统：Airflow / DolphinScheduler
- 数据服务：MySQL / OLAP（如 StarRocks / ClickHouse）
- 可视化：BI 工具（如 Superset / 观远BI）

---

## 📐 数仓分层设计

本项目采用经典的数据仓库分层架构：


# 📊 Data Warehouse Project

## 🚀 项目简介

本项目是一个完整的数据仓库（Data Warehouse）建设实践，涵盖数据采集、数据处理、建模分层、数据服务等核心流程，旨在构建一个高效、稳定、可扩展的数据分析平台。

项目围绕离线数仓 / 实时数仓场景设计，支持业务数据的统一管理与分析，为 BI 报表、数据分析和业务决策提供支撑。

---

## 🏗️ 技术架构

### 🔧 核心技术栈

- 数据采集：Kafka / Flume / CDC
- 计算引擎：Spark / Flink
- 存储层：Hive / HDFS / Iceberg / Paimon
- 调度系统：Airflow / DolphinScheduler
- 数据服务：MySQL / OLAP（如 StarRocks / ClickHouse）
- 可视化：BI 工具（如 Superset / 观远BI）

---

## 📐 数仓分层设计

本项目采用经典的数据仓库分层架构：


### 🧱 各层说明

#### 1️⃣ ODS（Operational Data Store）
- 存储原始数据（接近业务系统）
- 保留数据原貌，不做复杂加工
- 支持增量/全量同步

#### 2️⃣ DWD（Data Warehouse Detail）
- 数据清洗、标准化处理
- 统一数据口径
- 构建明细事实表

#### 3️⃣ DWS（Data Warehouse Summary）
- 轻度汇总（按主题域）
- 提升查询性能
- 面向分析场景

#### 4️⃣ ADS（Application Data Service）
- 面向业务应用
- 提供指标数据（报表 / 大屏 / API）

---

## 🔄 数据流程

```text
数据源 → 数据采集 → ODS → 数据清洗 → DWD → 汇总加工 → DWS → 应用层（ADS）


```

---

## ⚙️ 运行环境准备

### 🐳 1. 启动 Docker 集群

```bash
sudo systemctl start docker
/home/atguigu/docker_bigdata/contains.sh start
```

---

### 🧱 2. 启动基础服务

#### （1）启动 HDFS

```bash
hadoop.sh start
```

#### （2）启动 Zookeeper

```bash
zk.sh start
```

#### （3）启动 Kafka

```bash
kafka.sh start
```

#### （4）启动日志采集服务

```bash
log.sh start
```

---

## 🚀 启动顺序说明

```text
Docker → HDFS → Zookeeper → Kafka → 日志采集 → Flink任务
```

---

## 📌 注意事项

* 请确保 Docker 已正常运行
* Kafka 依赖 Zookeeper，需先启动
* 日志服务需在 Kafka 启动后运行
* 所有服务建议按顺序启动，避免依赖问题

---

## 🔍 服务检查

```bash
jps
```

或

```bash
ps -ef | grep kafka
```

