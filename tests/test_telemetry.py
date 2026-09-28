"""验收测试：6 个规定场景 + 边界/持久化/读回，全部与朴素参照模型逐小时比对。"""
from __future__ import annotations

import math
import os
import sys
import tempfile
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from telemetry import (  # noqa: E402
    DataPoint,
    HourStatus,
    QuarantineReason,
    RejectReason,
    TelemetryService,
)
from reference import RefModel, hour_of  # noqa: E402

HOUR = 3_600_000
DAY = 86_400_000
NOW = 1_700_000_000_000
BASE = NOW - NOW % HOUR


def pt(metric, ts, value, **tags):
    return DataPoint(metric, ts, float(value), tags)


def assert_reports_equal(a, b):
    attrs = ["status", "stored", "duplicates", "duplicate_rate",
             "expected_slots", "filled_slots", "missing_rate",
             "quarantined"]
    for attr in attrs:
        va, vb = getattr(a, attr), getattr(b, attr)
        assert va == vb, (attr, va, vb, a.hour_start_ms)


def _point_sort_key(p):
    return (p.ts_ms, p.value, tuple(sorted(p.tags.items())))


def assert_reads_equal(svc, ref, metric, start_ms, end_ms):
    got = sorted(svc.read_points(metric, start_ms, end_ms),
                 key=_point_sort_key)
    want = sorted(ref.read_points(metric, start_ms, end_ms),
                  key=_point_sort_key)
    assert got == want
    gq = sorted(svc.read_quarantine(metric, start_ms, end_ms),
                key=lambda t: _point_sort_key(t[0]))
    wq = sorted(ref.read_quarantine(metric, start_ms, end_ms),
                key=lambda t: _point_sort_key(t[0]))
    assert [(p.ts_ms, p.value, p.tags, r) for p, r in gq] == \
        [(p.ts_ms, p.value, p.tags, r) for p, r in wq]


def assert_full_consistency(svc, ref, metric, hour_from, hour_to, now):
    """每个小时报告 + 点/隔离读回全部对齐。"""
    reports = svc.report_range(metric, hour_from, hour_to, now)
    assert len(reports) == (hour_to - hour_from) // HOUR
    for i, report in enumerate(reports):
        hour = hour_from + i * HOUR
        expected = ref.hourly_report(metric, hour, now)
        assert_reports_equal(report, expected)
    assert_reads_equal(svc, ref, metric, hour_from, hour_to)


def make_pair(path=":memory:"):
    return TelemetryService(path), RefModel()


def register_both(ref, svc, name, tag_keys, lo=0.0, hi=100.0,
              interval_ms=60_000, retention_days=30):
    svc.register_metric(name, tag_keys, lo, hi, interval_ms, retention_days)
    ref.register_metric(name, tag_keys, lo, hi, interval_ms, retention_days)


def write_both(svc, ref, points, now):
    """写入并让参照模型只应用同样被接受的批次；拒绝原因也要一致。"""
    result = svc.write_batch(points, now)
    rejected_ref, reasons_ref = ref.apply_batch(points, now)
    assert result.batch_rejected == rejected_ref
    got_reasons = {r.index: r.reason for r in result.rejected}
    assert got_reasons == reasons_ref
    return result


class TestTelemetry:
    def test_1_same_and_cross_batch_duplicates(self):
        svc, ref = make_pair()
        register_both(ref, svc, "m1", ["host"], interval_ms=60_000)
        ts0 = BASE - 2 * HOUR
        r = write_both(svc, ref, [
            pt("m1", ts0, 1.0, host="a"),
            pt("m1", ts0, 9.0, host="a"),          # 同批重复，先到保留
            pt("m1", ts0 + 60_000, 2.0, host="a"),
            pt("m1", ts0, 3.0, host="b"),          # 不同序列，不算重复
        ], NOW)
        assert r.stored == 3 and r.duplicates == 1
        assert svc.read_points("m1", ts0, ts0 + 1) == [
            pt("m1", ts0, 1.0, host="a"),
            pt("m1", ts0, 3.0, host="b")]
        r = write_both(svc, ref, [
            pt("m1", ts0, 5.0, host="a"),          # 跨批重复
            pt("m1", ts0, 9.0, host="b"),          # 跨批重复
            pt("m1", ts0 + 120_000, 4.0, host="a"),
        ], NOW)
        assert r.stored == 1 and r.duplicates == 2
        r = write_both(svc, ref, [
            pt("m1", ts0, 1.0, host="a"),
            pt("m1", ts0, 1.0),                    # 无标签：不同序列
        ], NOW)
        assert r.stored == 1 and r.duplicates == 1
        assert_full_consistency(svc, ref, "m1",
                                BASE - 3 * HOUR, BASE + HOUR, NOW)

    def test_2_backfill_gaps_and_extend_range(self):
        svc, ref = make_pair()
        register_both(ref, svc, "m2", ["line"], interval_ms=60_000)
        ts0 = BASE - 6 * HOUR
        write_both(svc, ref, [
            pt("m2", ts0, 1.0, line="L1"),
            pt("m2", ts0 + 5 * HOUR, 5.0, line="L1"),
        ], NOW)
        r = svc.hourly_report("m2", ts0 + HOUR, NOW)
        assert r.filled_slots == 0
        assert math.isclose(r.missing_rate, 1.0)
        backfill = [pt("m2", ts0 + h * HOUR + 1000, 2.0, line="L1")
                    for h in range(1, 5)]
        backfill += [
            pt("m2", ts0 - HOUR + 2000, 0.5, line="L1"),
            pt("m2", ts0 + 6 * HOUR + 3000, 6.0, line="L1")]
        write_both(svc, ref, backfill, NOW)
        assert_full_consistency(svc, ref, "m2",
                                ts0 - 2 * HOUR, ts0 + 8 * HOUR, NOW)

    def test_3_ten_percent_boundary(self):
        svc, ref = make_pair()
        register_both(ref, svc, "m3", ["x"], interval_ms=60_000)
        ts0 = BASE - HOUR
        good = lambda i: pt("m3", ts0 + i * 60_000, 1.0, x="a")  # noqa: E731
        bad = lambda i: pt("m3", ts0 + i * 60_000, float("nan"), x="a")  # noqa: E731

        assert svc.write_batch([], NOW).batch_rejected
        big = [good(i) for i in range(5001)]
        assert svc.write_batch(big, NOW).batch_rejected

        # 恰好 10%：接受，只丢弃不合格
        r = write_both(svc, ref, [bad(0)] + [good(i) for i in range(1, 11)],
                       NOW)
        assert not r.batch_rejected and len(r.rejected) == 1
        assert r.rejected[0].reason is RejectReason.NON_FINITE_VALUE
        r = write_both(svc, ref, [bad(i) for i in range(20, 22)] +
                       [good(i) for i in range(22, 40)], NOW)
        assert not r.batch_rejected and len(r.rejected) == 2
        # 刚超过 10%：整批拒绝并逐条给原因
        before = svc.hourly_report("m3", ts0, NOW)
        r = write_both(svc, ref, [bad(i) for i in range(40, 42)] +
                       [good(i) for i in range(42, 50)], NOW)
        assert r.batch_rejected and len(r.rejected) == 2
        after = svc.hourly_report("m3", ts0, NOW)
        assert after.stored == before.stored
        assert_full_consistency(svc, ref, "m3", BASE - 2 * HOUR,
                                BASE + HOUR, NOW)

    def test_4_quarantine_and_duplicates_interleaved(self):
        svc, ref = make_pair()
        register_both(ref, svc, "m4", ["host"], lo=0.0, hi=100.0,
                      interval_ms=60_000)
        ts0 = BASE - HOUR
        r = write_both(svc, ref, [
            pt("m4", ts0, 1.0, host="a"),                # 存
            pt("m4", ts0 + 1, 150.0, host="a"),          # 越界：隔离
            pt("m4", ts0 + 60_000, 1.0, host="a",
               rogue="x"),                               # 未注册标签：隔离
            pt("m4", ts0, 2.0, host="a"),                # 重复：不存
            pt("m4", ts0 + 1, 150.0, host="a"),          # 隔离点不参与去重
            pt("m4", ts0 + 1, 50.0, host="a"),           # 与隔离点同毫秒，
                                                         # 合格：正常存
            pt("m4", ts0 + 1, 50.0, host="a"),           # 再到：重复
        ], NOW)
        assert r.stored == 2 and r.duplicates == 2
        assert len(r.quarantined) == 3
        assert {q.reason for q in r.quarantined} == {
            QuarantineReason.VALUE_OUT_OF_RANGE,
            QuarantineReason.UNREGISTERED_TAG_KEY}
        assert_full_consistency(svc, ref, "m4", BASE - 2 * HOUR,
                                BASE + HOUR, NOW)

    def test_5_concurrent_writers_overlap(self):
        svc, ref = make_pair()
        register_both(ref, svc, "m5", ["host"], interval_ms=60_000,
                      retention_days=None)
        ts0 = BASE - 12 * HOUR
        lock = threading.Lock()
        errors = []

        def batches_for(seed):
            # 各线程覆盖相同的小时与序列，时间戳大量重叠；
            # 重叠点的值相同，保证最终状态与写入次序无关。
            batches = []
            for h in range(12):
                batch = []
                for host in ("a", "b", "c", "d"):
                    for slot in range(10):
                        if (seed + h + slot) % 3 != 0:
                            batch.append(pt(
                                "m5", ts0 + h * HOUR + slot * 60_000,
                                1.0 + ((h + slot) % 5), host=host))
                batches.append(batch)
            return batches

        def writer(seed):
            try:
                for batch in batches_for(seed):
                    with lock:
                        write_both(svc, ref, batch, NOW)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=writer, args=(s,))
                   for s in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors, errors
        assert_full_consistency(svc, ref, "m5", ts0 - HOUR,
                                ts0 + 13 * HOUR, NOW)

    def test_6_cleanup_concurrent_with_writes(self):
        svc, ref = make_pair()
        register_both(ref, svc, "m6", ["host"], interval_ms=60_000,
                      retention_days=30)
        now0 = NOW
        now1 = NOW + 2 * DAY
        initial = []
        for h in range(-35, 1):
            for host in ("a", "b"):
                initial.append(pt("m6", now0 + h * HOUR + 1000, 1.0,
                                  host=host))
        write_both(svc, ref, initial, now0)
        lock = threading.Lock()
        errors = []

        def writer(seed):
            try:
                for k in range(20):
                    batch = [
                        pt("m6", now1 - 3 * 60_000 - k * 60_000, 2.0,
                           host=("a" if seed % 2 else "b")),
                        pt("m6", now1 - 2 * HOUR + k * 60_000, 3.0,
                           host="c"),
                    ]
                    with lock:
                        write_both(svc, ref, batch, now1)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        def cleaner():
            try:
                for _ in range(8):
                    with lock:
                        svc.cleanup(now1)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=writer, args=(s,))
                   for s in range(4)] + [threading.Thread(target=cleaner)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors, errors
        svc.cleanup(now1)
        ref.cleanup(now1)
        # 过期小时不能有任何残留
        max_hour = now1 - 30 * DAY - HOUR
        assert svc.read_points("m6", 0, max_hour + HOUR) == []
        assert svc.read_quarantine("m6", 0, max_hour + HOUR) == []
        assert_full_consistency(svc, ref, "m6",
                                hour_of(now0 - 35 * HOUR),
                                hour_of(now1) + HOUR, now1)
        expired = svc.hourly_report("m6", max_hour, now1)
        assert expired.status is HourStatus.EXPIRED

    def test_7_persistence_and_atomic_batch(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "t.db")
            svc = TelemetryService(path)
            ref = RefModel()
            register_both(ref, svc, "m7", ["host"], interval_ms=60_000)
            ts0 = BASE - 4 * HOUR
            write_both(svc, ref, [pt("m7", ts0 + i * 60_000, 1.0, host="a")
                                  for i in range(30)], NOW)
            write_both(svc, ref, [pt("m7", ts0, 150.0, host="a")], NOW)
            svc.close()
            # “重启”：重开同一文件，报告与已存数据必须一致
            svc = TelemetryService(path)
            assert_full_consistency(svc, ref, "m7", ts0 - HOUR,
                                    ts0 + 2 * HOUR, NOW)
            # 序列上限：写满 2000 条序列后，新序列的批整体回滚
            svc.register_metric("m8", ["host"], 0.0, 100.0, 60_000, 30)
            ref.register_metric("m8", ["host"], 0.0, 100.0, 60_000, 30)
            fill = [pt("m8", ts0, 1.0, host=f"h{i}") for i in range(2000)]
            write_both(svc, ref, fill, NOW)
            overflow = [pt("m8", ts0, 1.0, host="h2000"),
                        pt("m8", ts0 + 60_000, 1.0, host="h0")]
            try:
                svc.write_batch(overflow, NOW)
                raise AssertionError("expected ValueError")
            except ValueError:
                pass
            # 回滚：h0 在 ts0+60s 的点也没有留下
            assert svc.read_points("m8", ts0 + 1, ts0 + 2 * HOUR) == []
            assert_full_consistency(svc, ref, "m8", ts0 - HOUR,
                                    ts0 + 2 * HOUR, NOW)
            svc.close()

    def test_8_validation_edges(self):
        svc, ref = make_pair()
        register_both(ref, svc, "edge", ["k"], lo=-10.0, hi=10.0,
                      interval_ms=1000, retention_days=90)
        ts0 = BASE - HOUR
        # 8 条不合格，补足到 86 条使不合格占比 <= 10%
        padding = [pt("edge", ts0 + (i + 12) * 1000, 1.0, k="v")
                   for i in range(72)]
        r = svc.write_batch([
            pt("edge", ts0, 1.0, k="v"),                       # 存
            pt("edge", ts0 + 1000, -10.0, k="v"),              # 边界：存
            pt("edge", ts0 + 2000, 10.0, k="v"),               # 边界：存
            pt("edge", ts0 + 3000, 10.000001, k="v"),          # 越界：隔离
            pt("edge", ts0 + 4000, 1.0, k="v" * 257),          # 值过长
            pt("edge", ts0 + 5000, 1.0, **{"k" * 65: "v"}),    # 键过长
            pt("edge", ts0 + 6000, 1.0, **{"": "v"}),          # 空键
            pt("edge", ts0 + 7000, float("inf"), k="v"),       # 非有限
            pt("edge", NOW + 11 * 60_000, 1.0, k="v"),         # 未来超10分钟
            pt("edge", NOW + 9 * 60_000, 1.0, k="v"),          # 未来9分钟：存
            pt("edge", NOW - 90 * DAY - HOUR, 1.0, k="v"),     # 过期
            pt("edge", NOW - 90 * DAY, 1.0, k="v"),            # 边界：存
            pt("bad-name", ts0, 1.0),                          # 非法名
            pt("ghost", ts0, 1.0),                             # 未注册
        ] + padding, NOW)
        assert not r.batch_rejected
        assert r.stored == 5 + 72 and len(r.quarantined) == 1
        assert len(r.rejected) == 8
        # 标签超过 8 个
        many = {f"t{i}": "v" for i in range(9)}
        r = svc.write_batch([DataPoint("edge", ts0, 1.0, many)], NOW)
        assert r.rejected[0].reason is RejectReason.TOO_MANY_TAGS
        # 注册参数校验
        for bad_interval in (999, 3_600_001, 1001, 7 * 60_000):
            try:
                svc.register_metric("z1", [], 0, 1, bad_interval, 30)
                raise AssertionError(bad_interval)
            except ValueError:
                pass
        for bad_retention in (0, 7, 365):
            try:
                svc.register_metric("z2", [], 0, 1, 1000, bad_retention)
                raise AssertionError(bad_retention)
            except ValueError:
                pass
        try:
            svc.register_metric("edge", [], 0, 1, 1000, 30)  # 重复注册
            raise AssertionError("duplicate register")
        except ValueError:
            pass
        # 整点归属：恰在整点的点归入新小时
        svc2, ref2 = make_pair()
        register_both(ref2, svc2, "al", [], interval_ms=60_000)
        write_both(svc2, ref2, [pt("al", BASE, 1.0)], NOW)
        prev = svc2.hourly_report("al", BASE - HOUR, NOW)
        cur = svc2.hourly_report("al", BASE, NOW)
        assert prev.stored == 0 and cur.stored == 1
        assert_full_consistency(svc2, ref2, "al", BASE - HOUR,
                                BASE + HOUR, NOW)
