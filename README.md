# telemetry — 工业遥测批量写入与数据质量核算库

供上层采集网关服务直接调用的 Python 库（无接口层）。数据点、隔离记录、
质量统计全部持久化在本地 SQLite，支持多线程并发写入与查询。

## 快速开始

```python
from telemetry import TelemetryService, DataPoint

svc = TelemetryService("telemetry.db")          # 或 ":memory:"

# 1. 注册指标（注册后不可改；重复注册抛 ValueError）
svc.register_metric(
    name="temperature",          # 1-128 位 ASCII 字母/数字/下划线
    tag_keys=["site", "line"],   # 允许的标签键
    min_value=-50.0, max_value=150.0,   # 数值上下限（含边界）
    interval_ms=60_000,          # 采样间隔：1s~1h 且整除 1 小时
    retention_days=30,           # 30 / 90 / None（永久）
)

# 2. 批量写入（1~5000 条；now 由调用方给出，Unix 毫秒）
result = svc.write_batch([
    DataPoint("temperature", 1_700_000_000_000, 23.5, {"site": "A"}),
    DataPoint("temperature", 1_700_000_060_000, 24.1, {"site": "A"}),
], now_ms=1_700_000_100_000)
# result.stored / .duplicates / .quarantined / .rejected / .batch_rejected

# 3. 小时质量报告（UTC 整点；单小时或连续区间）
rep = svc.hourly_report("temperature", 1_700_000_000_000, now_ms)
reps = svc.report_range("temperature", start_hour_ms, end_hour_ms, now_ms)
# rep.stored / duplicates / duplicate_rate / expected_slots /
# rep.filled_slots / missing_rate / quarantined / status

# 4. 清理过期整小时（点、隔离、统计一起删；序列最早/最晚时间不变）
svc.cleanup(now_ms)

# 5. 按时间范围读回 [start, end)
svc.read_points("temperature", start_ms, end_ms)
svc.read_quarantine("temperature", start_ms, end_ms)
```

## 语义要点

- **批次判定**：格式不合格记录数 **严格大于** 整批 10% 时整批拒绝
  （`batch_rejected=True`，逐条给出原因，什么都不生效）；否则只丢弃不合格
  记录。批次不在 1~5000 条范围内直接整批拒绝。
- **隔离**：格式合格但带未注册标签键或数值越界的点进隔离区，保留原始内容
  与原因，不参与去重与质量统计（隔离点不占用序列的毫秒位置）。
- **去重**：同一序列（指标名 + 标签集合，与顺序无关、区分大小写）同一毫秒
  已存过点时，后到算重复，保留先到。
- **应到槽数**：每条序列从已存最早点到最晚点之间、落在该小时的槽数之和；
  断线期间的小时算全缺，补传推开最早/最晚后沿途小时报告随之变化。
- **比率**：分母为 0 时 `duplicate_rate` / `missing_rate` 为 `None`；
  已清理的小时 `status == HourStatus.EXPIRED`。
- **清理标准**：小时结束时刻 ≤ now − 保留期 的整小时被删除，与写入时的
  过期校验使用同一标准，因此清理后不可能再写入已过期小时。

## 并发与崩溃一致性

- 库内一把可重入锁串行化所有写入 / 清理 / 查询；任何时刻查到的报告都
  对应完整批次，并发写完的结果等价于按某顺序逐批写入。
- 每批写入、每次清理各为一个 SQLite 事务（WAL 模式）：进程中途被杀，
  重启后每批要么完整生效、要么完全没生效，报告与已存数据一致。
- 单指标序列数上限 2000；超限时该批抛 `ValueError` 并整体回滚。

## 性能设计

报告不扫原始点：`hourly_stats`（每指标每小时聚合计数）+
`series_hour`（每序列每小时已填槽数，`(metric, hour)` 索引）在写入事务内
增量维护；应到槽数由序列最早/最晚时间算术得出。查报告耗时不随已存点数
增长（实测：百万点下单小时报告 <1ms，30 天逐小时报告 <0.5s，
单批 5000 点写入 <0.3s）。

## 测试

```bash
python3 -m pytest tests/ -q
```

`tests/reference.py` 是与实现独立的朴素参照模型；每个场景结束后，
所有小时的报告、读回的点与隔离记录都和参照模型从被接受批次从头算出的
结果逐字段比对。覆盖：同批/跨批重复、补传填空洞并推开最早最晚、
10% 边界、隔离与重复交织、多线程并发写入、清理与写入并发、
持久化重开与批次原子回滚、格式校验边界。
