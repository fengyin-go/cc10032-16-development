"""序号16 业务测试：分层样本数据 ↔ 调试会话 ↔ 调试异常报告的可追溯闭环。

覆盖需求：
  1. 读取源事件 + 分层采样，样本绑定唯一采样批次与策略版本；
  2. 调试会话引用完整样本，按目标算子记录中间结果/异常标记，生成唯一当前报告；
  3. 同批次重复启动调试不产生多个当前报告；
  4. 序号16 连续/重复导入只入库一次（幂等），事务失败回滚并退回原文件；
  5. 两种文件边界在序号16下生成不同策略版本；
  6. 调试期间采样上限/比例调整：按原样本完成 或 按新比例重采样再开一轮，
     最终只确定一个与样本版本一致的当前报告；
  7. 责任审核人查看分层数量，调试页打开报告与中间结果；
  8. 导出按采样批次核对样本与报告；再次上传校验；其他页面接收同源导出/台账；
  9. 序号16 故障重开沿同批样本重建当前报告；
 10. 历史 data_sampler / debug_trace 接口与数据保持不变。
"""
from __future__ import annotations

import pytest

from app.store import store
from app.services.sample_trace import (
    SLOT_REF,
    SampleTraceService,
    checksum_of,
    sample_trace_service,
)


# ---------------------------------------------------------------------------
# 测试夹具与构造器
# ---------------------------------------------------------------------------

def make_events(count: int, *, regions: int = 5, base: float = 10.0) -> list[dict]:
    return [
        {
            "event_id": f"ev_{i}",
            "event_time": 1_000 + i,
            "source": "source_16",
            "payload": {
                "user_id": f"u{i % 4}",
                "amount": round(base + i * 3.0, 2),
                "region": f"r{i % regions}",
            },
        }
        for i in range(count)
    ]


def make_file(
    boundary: str = "boundary-A",
    count: int = 40,
    *,
    ratio: str = "0.25",
    limit: str = "100",
    group_key: str = "region",
    name: str | None = None,
    source_page: str = "其他页面",
) -> dict:
    return {
        "file_name": name or f"s16_{boundary}.json",
        "file_boundary": boundary,
        "source_page": source_page,
        "events": make_events(count),
        "sampler": {
            "策略名称": f"策略_{boundary}",
            "采样方式": "stratified",
            "采样比例": ratio,
            "分组键": group_key,
            "样本上限": limit,
            "保留特征": "amount,region,user_id",
            "采样窗口": "60",
        },
    }


@pytest.fixture(autouse=True)
def reset_trace_registries():
    """每个用例前后清空序号16注册表，保证相互独立。"""
    store._sample_batches.clear()
    store._trace_sessions.clear()
    store._debug_reports.clear()
    store._trace_ledger.clear()
    yield
    store._sample_batches.clear()
    store._trace_sessions.clear()
    store._debug_reports.clear()
    store._trace_ledger.clear()


# ---------------------------------------------------------------------------
# 1. 分层采样与批次/策略版本绑定
# ---------------------------------------------------------------------------

class TestStratifiedBatch:
    def test_import_builds_stratified_batch_with_strata_counts(self):
        result = sample_trace_service.import_file(
            make_file(count=40), reviewer="审核人甲",
        )
        assert result["ok"] is True
        batch = sample_trace_service.get_batch(result["batch_id"])
        assert batch["slot_ref"] == SLOT_REF
        # 分层数量台账：5 个 region 分层
        assert len(batch["strata"]) == 5
        assert sum(s["sampled"] for s in batch["strata"]) == batch["sample_count"]
        for stratum in batch["strata"]:
            assert stratum["sampled"] <= stratum["population"]

    def test_each_sample_binds_single_batch_and_strategy_version(self):
        result = sample_trace_service.import_file(make_file())
        batch = sample_trace_service.get_batch(result["batch_id"])
        for sample in batch["samples"]:
            assert sample["batch_id"] == batch["batch_id"]
            assert sample["strategy_version"] == batch["strategy_version"]
            assert sample["slot_ref"] == SLOT_REF

    def test_sample_limit_is_respected(self):
        result = sample_trace_service.import_file(
            make_file(count=60, ratio="1.0", limit="12"),
        )
        batch = sample_trace_service.get_batch(result["batch_id"])
        assert batch["sample_count"] <= 12

    def test_sample_features_are_recorded(self):
        result = sample_trace_service.import_file(make_file())
        batch = sample_trace_service.get_batch(result["batch_id"])
        features = batch["sample_features"]
        assert features["count"] == batch["sample_count"]
        assert {"mean", "stddev", "min", "max", "p50", "p95"}.issubset(features)

    def test_two_file_boundaries_produce_distinct_versions(self):
        first = sample_trace_service.import_file(make_file("boundary-A"))
        second = sample_trace_service.import_file(make_file("boundary-B", count=25))
        assert first["ok"] and second["ok"]
        assert first["batch_id"] != second["batch_id"]
        assert first["strategy_version"] != second["strategy_version"]
        assert first["file_boundary"] == "boundary-A"
        assert second["file_boundary"] == "boundary-B"


# ---------------------------------------------------------------------------
# 2. 导入幂等与事务回滚
# ---------------------------------------------------------------------------

class TestImportIdempotencyAndTransaction:
    def test_repeated_import_is_idempotent(self):
        payload = make_file()
        first = sample_trace_service.import_file(payload)
        second = sample_trace_service.import_file(payload)
        assert first["ok"] and second["ok"]
        assert second.get("idempotent") is True
        assert second["batch_id"] == first["batch_id"]
        # 只入库一次：批次与导入台账都只有一条
        assert len(sample_trace_service.list_batches()) == 1
        import_ledgers = [
            x for x in sample_trace_service.list_ledger() if x["direction"] == "import"
        ]
        assert len(import_ledgers) == 1

    def test_source_fingerprint_drives_idempotency(self):
        payload = make_file()
        first = sample_trace_service.import_file(payload)
        # 仅改文件名但文件边界与采样策略不变 → 同源文件，仍按幂等处理
        same_boundary = dict(payload)
        same_boundary["file_name"] = "renamed.json"
        renamed = sample_trace_service.import_file(same_boundary)
        assert renamed["batch_id"] == first["batch_id"]
        # 完全相同的字节再次导入，自然幂等
        identical = sample_trace_service.import_file(payload)
        assert identical["batch_id"] == first["batch_id"]
        # 改变文件边界 → 不同源文件，生成新批次与新版本
        other_boundary = make_file("boundary-Z")
        assert sample_trace_service.import_file(other_boundary)["batch_id"] != first["batch_id"]

    def test_transaction_failure_rolls_back_and_returns_original_file(self):
        batches_before = len(store.sample_batches())
        ledger_before = len(store.trace_ledger())
        bad_file = {"file_name": "bad.json", "file_boundary": "bx", "events": "not-a-list"}
        result = sample_trace_service.import_file(bad_file)
        assert result["ok"] is False
        assert result.get("rolled_back") is True
        assert result["original_file"] == bad_file  # 退回原文件
        assert len(store.sample_batches()) == batches_before
        assert len(store.trace_ledger()) == ledger_before

    def test_missing_events_field_fails_transaction(self):
        result = sample_trace_service.import_file({"file_boundary": "x"})
        assert result["ok"] is False and result["rolled_back"] is True
        assert sample_trace_service.list_batches() == []

    def test_reviewer_recorded_on_batch(self):
        result = sample_trace_service.import_file(make_file(), reviewer="责任审核人A")
        batch = sample_trace_service.get_batch(result["batch_id"])
        assert batch["reviewer"] == "责任审核人A"


# ---------------------------------------------------------------------------
# 3. 调试会话引用完整样本 + 唯一当前报告
# ---------------------------------------------------------------------------

class TestDebugSessionAndReport:
    def test_session_references_complete_sample(self):
        imported = sample_trace_service.import_file(make_file())
        started = sample_trace_service.start_debug_session(
            imported["batch_id"], "window_agg_op",
        )
        assert started["ok"]
        session = sample_trace_service.get_session(started["session"]["session_id"])
        assert session["complete_sample_bound"] is True
        batch = sample_trace_service.get_batch(imported["batch_id"])
        assert len(session["samples"]) == len(batch["samples"])
        assert set(session["sample_ids"]) == {s["sample_id"] for s in batch["samples"]}

    def test_report_records_intermediate_results_and_anomaly_marks_per_operator(self):
        imported = sample_trace_service.import_file(make_file())
        started = sample_trace_service.start_debug_session(
            imported["batch_id"], "filter_op",
        )
        report = sample_trace_service.get_report(started["report_id"])
        assert report["target_operator"] == "filter_op"
        assert report["is_current"] is True
        batch = sample_trace_service.get_batch(imported["batch_id"])
        assert len(report["intermediate_results"]) == batch["sample_count"]
        for row in report["intermediate_results"]:
            assert row["target_operator"] == "filter_op"
            assert "expected" in row and "actual" in row and "diff" in row
            assert row["anomaly_mark"] in ("normal", "threshold_breach")
        # 异常标记与中间结果中的异常行数一致
        anomalous_rows = [
            r for r in report["intermediate_results"]
            if r["anomaly_mark"] != "normal"
        ]
        assert report["anomaly_count"] == len(anomalous_rows)
        assert len(report["anomaly_marks"]) == report["anomaly_count"]

    def test_report_version_matches_sample_version(self):
        imported = sample_trace_service.import_file(make_file())
        started = sample_trace_service.start_debug_session(imported["batch_id"], "op")
        report = sample_trace_service.get_report(started["report_id"])
        assert report["sample_version"] == imported["strategy_version"]
        assert report["strategy_version"] == imported["strategy_version"]

    def test_repeated_debug_start_keeps_single_current_report(self):
        imported = sample_trace_service.import_file(make_file())
        bid = imported["batch_id"]
        first = sample_trace_service.start_debug_session(bid, "op")
        second = sample_trace_service.start_debug_session(bid, "op")
        assert second.get("idempotent") is True
        current = [
            r for r in store.debug_reports().values()
            if r["batch_id"] == bid and r["is_current"]
        ]
        assert len(current) == 1
        assert current[0]["report_id"] == first["report_id"]
        # 运行中会话也只有一个
        running = [
            s for s in store.trace_sessions().values() if s["status"] == "运行中"
        ]
        assert len(running) == 1

    def test_complete_session_keeps_report_current_and_versions_aligned(self):
        imported = sample_trace_service.import_file(make_file())
        started = sample_trace_service.start_debug_session(imported["batch_id"], "op")
        sid = started["session"]["session_id"]
        completed = sample_trace_service.complete_session(sid)
        assert completed["ok"]
        report = sample_trace_service.get_report(started["report_id"])
        assert report["is_current"] is True
        assert report["completed_at"] is not None
        session = sample_trace_service.get_session(sid)
        assert session["status"] == "已完成"

    def test_start_debug_without_sample_fails(self):
        # 单事件极低比例仍至少保留 1 条；构造空事件文件
        empty = make_file(count=0)
        imported = sample_trace_service.import_file(empty)
        # 空事件无法分层，采样数为 0
        batch = sample_trace_service.get_batch(imported["batch_id"])
        assert batch["sample_count"] == 0
        result = sample_trace_service.start_debug_session(imported["batch_id"], "op")
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# 4. 调试期间策略上限/比例调整
# ---------------------------------------------------------------------------

class TestLimitChangeDuringDebug:
    def test_finish_with_original_sample_then_resample_rerun(self):
        imported = sample_trace_service.import_file(make_file(ratio="0.2", limit="20"))
        bid = imported["batch_id"]
        started = sample_trace_service.start_debug_session(bid, "op")
        # 方式一：按原样本完成当前报告
        completed = sample_trace_service.complete_session(
            started["session"]["session_id"],
        )
        assert completed["ok"]
        # 方式二：按新比例重新采样再开一轮
        rerun = sample_trace_service.resample_and_rerun(
            bid, "op", new_ratio=0.8, new_limit=30,
        )
        assert rerun["ok"]
        new_bid = rerun["batch_id"]
        assert new_bid != bid
        new_batch = sample_trace_service.get_batch(new_bid)
        old_batch = sample_trace_service.get_batch(bid)
        assert old_batch["superseded_by"] == new_bid
        assert new_batch["strategy_version"] != old_batch["strategy_version"]
        assert float(new_batch["strategy_snapshot"]["采样比例"]) == pytest.approx(0.8)
        # 新批次只有一个当前报告，且与新样本版本一致
        current = sample_trace_service.get_current_report(new_bid)
        assert current["report_id"] == rerun["report_id"]
        assert current["sample_version"] == new_batch["strategy_version"]
        # 旧批次的报告已被降级为历史，不再是当前
        old_current = sample_trace_service.get_current_report(bid)
        assert old_current.get("ok") is False

    def test_resample_preserves_traceability_chain(self):
        imported = sample_trace_service.import_file(make_file())
        rerun = sample_trace_service.resample_and_rerun(
            imported["batch_id"], "op", new_limit=5,
        )
        sessions = sample_trace_service.list_sessions()
        rerun_session = next(
            s for s in sessions if s["session_id"]
            == store.debug_reports()[rerun["report_id"]]["session_id"]
        )
        assert rerun_session["batch_id"] == rerun["batch_id"]
        assert rerun_session["complete_sample_bound"] is True


# ---------------------------------------------------------------------------
# 5. 故障重开
# ---------------------------------------------------------------------------

class TestFaultReopen:
    def test_reopen_rebuilds_current_report_on_same_sample(self):
        imported = sample_trace_service.import_file(make_file())
        bid = imported["batch_id"]
        started = sample_trace_service.start_debug_session(bid, "op")
        original_report_id = started["report_id"]
        reopened = sample_trace_service.fault_reopen(bid, "op")
        assert reopened["ok"]
        assert reopened["report_id"] != original_report_id
        # 版本不变（同批样本）
        assert reopened["strategy_version"] == imported["strategy_version"]
        current = sample_trace_service.get_current_report(bid)
        assert current["report_id"] == reopened["report_id"]
        # 旧报告归档为历史
        old = store.debug_reports()[original_report_id]
        assert old["is_current"] is False and old["status"] == "历史"
        # 仍然只有一个当前报告
        currents = [
            r for r in store.debug_reports().values()
            if r["batch_id"] == bid and r["is_current"]
        ]
        assert len(currents) == 1

    def test_healthy_report_cannot_be_reopened(self):
        clean = {
            "file_name": "clean.json",
            "file_boundary": "clean",
            "events": make_events(8, regions=1),
            "sampler": {
                "采样方式": "stratified", "采样比例": "1.0", "分组键": "region",
                "样本上限": "20", "保留特征": "amount,region,user_id",
            },
        }
        imported = sample_trace_service.import_file(clean)
        started = sample_trace_service.start_debug_session(imported["batch_id"], "op")
        report = sample_trace_service.get_report(started["report_id"])
        assert report["anomaly_count"] == 0
        result = sample_trace_service.fault_reopen(imported["batch_id"], "op")
        assert result["ok"] is False


# ---------------------------------------------------------------------------
# 6. 导出、校验与台账
# ---------------------------------------------------------------------------

class TestExportVerifyAndLedger:
    def test_export_contains_samples_and_reports_with_checksum(self):
        imported = sample_trace_service.import_file(make_file())
        sample_trace_service.start_debug_session(imported["batch_id"], "op")
        package = sample_trace_service.export_batch(imported["batch_id"])
        assert package["slot_ref"] == SLOT_REF
        assert package["file_checksum"] == checksum_of(package)
        assert len(package["samples"]) == package["batch"]["sample_count"]
        assert package["current_report_id"] is not None
        assert any(
            r["report_id"] == package["current_report_id"]
            for r in package["debug_reports"]
        )

    def test_reupload_verification_consistent(self):
        imported = sample_trace_service.import_file(make_file())
        sample_trace_service.start_debug_session(imported["batch_id"], "op")
        package = sample_trace_service.export_batch(imported["batch_id"])
        verification = sample_trace_service.verify_export_file(package)
        assert verification["consistent"] is True
        assert all(item["passed"] for item in verification["checks"])

    def test_tampered_export_is_detected(self):
        imported = sample_trace_service.import_file(make_file())
        sample_trace_service.start_debug_session(imported["batch_id"], "op")
        package = sample_trace_service.export_batch(imported["batch_id"])
        tampered = dict(package)
        tampered["batch"] = dict(package["batch"], sample_count=999)
        verification = sample_trace_service.verify_export_file(tampered)
        assert verification["consistent"] is False
        failed = [c for c in verification["checks"] if not c["passed"]]
        assert any(c["item"] == "sample_count" for c in failed)

    def test_checksum_tampering_detected(self):
        imported = sample_trace_service.import_file(make_file())
        package = sample_trace_service.export_batch(imported["batch_id"])
        tampered = dict(package)
        tampered["file_checksum"] = "deadbeef"
        verification = sample_trace_service.verify_export_file(tampered)
        assert verification["consistent"] is False

    def test_export_ledger_records_directions_and_receivable(self):
        imported = sample_trace_service.import_file(make_file(source_page="其他页面"))
        sample_trace_service.start_debug_session(imported["batch_id"], "op")
        sample_trace_service.export_batch(imported["batch_id"])
        ledger = sample_trace_service.list_ledger()
        directions = {row["direction"] for row in ledger}
        assert directions == {"import", "export"}
        # 其他页面接收同源导出文件：导出台账可按批次追溯
        export_row = next(row for row in ledger if row["direction"] == "export")
        assert export_row["batch_id"] == imported["batch_id"]
        assert export_row["file_checksum"]

    def test_exported_file_can_be_received_by_another_page_and_imported(self):
        imported = sample_trace_service.import_file(make_file())
        sample_trace_service.start_debug_session(imported["batch_id"], "op")
        package = sample_trace_service.export_batch(imported["batch_id"])
        # 模拟其他页面同源接收：换来源页后再次导入（内容相同 → 幂等）
        received = dict(package)
        received["source_page"] = "外部页面"
        again = sample_trace_service.import_file(received, source_page="外部页面")
        assert again["ok"] is True
        assert again["batch_id"] == imported["batch_id"]  # 同源文件仍只生效一次

    def test_compare_export_handling_tradeoffs(self):
        # 首次接收：未导入，回流导入会建批次；只读校验无副作用
        first_file = make_file()
        comparison = sample_trace_service.compare_export_handling(first_file)
        assert comparison["ok"]
        assert comparison["already_imported"] is False
        modes = {o["mode"]: o for o in comparison["options"]}
        assert modes["verify"]["side_effect"] is False
        assert modes["import"]["would_create_batch"] is True

        # 真正导入后再比较：回流导入不再建批次，建议优先只读校验
        imported = sample_trace_service.import_file(first_file)
        sample_trace_service.start_debug_session(imported["batch_id"], "op")
        package = sample_trace_service.export_batch(imported["batch_id"])
        after = sample_trace_service.compare_export_handling(package)
        assert after["already_imported"] is True
        assert {o["mode"] for o in after["options"]} == {"verify", "import"}
        import_option = next(o for o in after["options"] if o["mode"] == "import")
        assert import_option["would_create_batch"] is False
        # 比较动作本身不写台账
        assert len(sample_trace_service.list_ledger()) == 2


# ---------------------------------------------------------------------------
# 7. 调用链编排与历史兼容性
# ---------------------------------------------------------------------------

class TestPipelineChainAndBackwardCompatibility:
    def test_pipeline_import_chain_locates_import_and_verifies_export(self):
        from app.pipeline import import_sample_trace_chain

        result = import_sample_trace_chain(make_file(), "chain_operator")
        assert result["ok"] is True
        assert result["import"]["ok"]
        assert result["debug_session"]["ok"]
        assert result["verification"]["consistent"] is True
        assert result["export_checksum"]

    def test_historical_modules_unchanged(self):
        # 序号16 注册表与历史模块表隔离，历史接口数据结构不受影响
        sampler_rows = store.rows("data_sampler")
        trace_rows = store.rows("debug_trace")
        assert sampler_rows and all("策略名称" in r for r in sampler_rows)
        assert trace_rows and all("会话名称" in r for r in trace_rows)
        # 历史模块仍可走通用服务基类查询
        from app.services import service_for

        sampler_service = service_for("data_sampler")
        items, total = sampler_service.list_entries(page=1, size=5)
        assert total == len(sampler_rows)
        assert items

    def test_list_views_are_serializable_summaries(self):
        imported = sample_trace_service.import_file(make_file())
        sample_trace_service.start_debug_session(imported["batch_id"], "op")
        batches = sample_trace_service.list_batches()
        sessions = sample_trace_service.list_sessions()
        assert len(batches) == 1 and "samples" not in batches[0]  # 列表不含重样本
        assert len(sessions) == 1
        detail = sample_trace_service.get_batch(imported["batch_id"])
        assert "samples" in detail and "reports" in detail
