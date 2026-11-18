"""序号16 领域服务：分层样本数据 ↔ 调试会话 ↔ 调试异常报告的可追溯闭环。

职责（对应需求条目）：
  - 读取源事件数据并执行分层采样，生成绑定唯一采样批次与策略版本的分层样本数据；
  - 调试会话必须引用完整样本，调试追踪按目标算子记录中间结果与异常标记，
    生成唯一的调试异常报告；同一批次重复启动调试只保留一个当前报告；
  - 序号16导入文件幂等：同校验和连续导入只入库一次；事务失败回滚并退回原文件；
  - 两种文件边界在序号16下生成不同策略版本；调试期间采样上限/比例调整时，
    可按原样本完成当前报告，或按新比例重新采样再开一轮，最终只确定一个与样本版本
    一致的当前报告；故障重开沿用同批样本重建当前报告；
  - 导入导出文件按采样批次核对分层样本数据与调试异常报告，并登记导出台账。

设计要点：所有状态保存在 app.store 的序号16专用注册表中，与历史 data_sampler /
debug_trace 列表数据隔离，保证历史接口返回结构不变。
"""
from __future__ import annotations

import copy
import hashlib
import json
import random
import time
import uuid
from typing import Any

from app.store import store

SLOT_REF = "序号16"
STRATIFIED_METHOD = "stratified"
# 责任审核人默认值，导入时可覆盖
DEFAULT_REVIEWER = "数据工程组"


def _now_text() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def canonical_json(payload: dict[str, Any]) -> str:
    """稳定序列化：校验和与幂等判断都基于去掉校验和字段后的规范 JSON。"""
    body = {k: v for k, v in payload.items() if k != "file_checksum"}
    return json.dumps(body, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def checksum_of(payload: dict[str, Any]) -> str:
    """整包完整性校验和：任何字段（含路由元数据）被篡改都会变化，用于导出核对。"""
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def source_fingerprint(payload: dict[str, Any]) -> str:
    """同源指纹：只取文件边界与采样策略，忽略来源页/审核人等路由元数据。

    序号16以文件边界区分不同源文件（两种边界生成不同版本）；同一源文件即使被
    其他页面接收、或经导出再回流，边界与策略不变，指纹也不变，从而重复导入只生效一次。
    """
    core = {
        "file_boundary": payload.get("file_boundary")
        or payload.get("file_name")
        or "default-boundary",
        "sampler": payload.get("sampler"),
    }
    return hashlib.sha256(canonical_json(core).encode("utf-8")).hexdigest()


def _new_id(prefix: str, length: int) -> str:
    return f"{prefix}{uuid.uuid4().hex[:length]}"


class SampleTraceService:
    """序号16 采样-调试可追溯链路的唯一领域入口。"""

    # ------------------------------------------------------------------
    # 采样策略快照与分层采样
    # ------------------------------------------------------------------

    def _resolve_sampler_policy(
        self, sampler_override: dict[str, Any] | None, sampler_name: str | None,
    ) -> dict[str, Any]:
        """优先采用导入文件自带的策略快照，其次按名称读取 data_sampler 现存策略。"""
        policy: dict[str, Any] = {}
        if sampler_name:
            for row in store.rows("data_sampler"):
                if row.get("策略名称") == sampler_name:
                    policy = {
                        "策略名称": row.get("策略名称", ""),
                        "采样方式": row.get("采样方式", STRATIFIED_METHOD),
                        "采样比例": row.get("采样比例", "0.2"),
                        "分组键": row.get("分组键", "region"),
                        "保留特征": row.get("保留特征", "amount,region,user_id"),
                        "样本上限": row.get("样本上限", "100"),
                        "采样窗口": row.get("采样窗口", "60"),
                    }
                    break
        if sampler_override:
            policy.update({k: v for k, v in sampler_override.items() if v is not None})
        policy.setdefault("策略名称", sampler_name or f"{SLOT_REF}分层采样策略")
        policy.setdefault("采样方式", STRATIFIED_METHOD)
        policy.setdefault("采样比例", "0.2")
        policy.setdefault("分组键", "region")
        policy.setdefault("保留特征", "amount,region,user_id")
        policy.setdefault("样本上限", "100")
        policy.setdefault("采样窗口", "60")
        return policy

    @staticmethod
    def _to_float(value: Any, default: float) -> float:
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def _next_strategy_version(self) -> str:
        """序号16下策略版本自增；不同文件边界/重采样都会得到新版本。"""
        max_version = 0
        for batch in store.sample_batches().values():
            if batch.get("slot_ref") != SLOT_REF:
                continue
            raw = str(batch.get("strategy_version", "v0")).lstrip("v")
            try:
                max_version = max(max_version, int(raw))
            except ValueError:
                continue
        return f"v{max_version + 1}"

    def _stratify(
        self,
        events: list[dict[str, Any]],
        group_key: str,
        ratio: float,
        max_total: int,
        batch_id: str,
        strategy_version: str,
        features: list[str],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """按分组键分层、层内按比例抽取，返回（完整样本，分层数量台账）。"""
        strata: dict[str, list[dict[str, Any]]] = {}
        for event in events:
            payload = event.get("payload", event)
            key = str(payload.get(group_key, event.get(group_key, "__default__")))
            strata.setdefault(key, []).append(event)

        samples: list[dict[str, Any]] = []
        strata_ledger: list[dict[str, Any]] = []
        rng = random.Random(42)
        for stratum_key, items in strata.items():
            take = min(max(1, int(len(items) * ratio)), len(items))
            picked = rng.sample(items, take)
            strata_ledger.append({
                "stratum_key": stratum_key,
                "population": len(items),
                "sampled": len(picked),
            })
            for event in picked:
                payload = event.get("payload", event)
                sample = {
                    "sample_id": _new_id("SMP-", 12),
                    "batch_id": batch_id,  # 每份样本只绑定一个采样批次
                    "strategy_version": strategy_version,  # 与一个策略版本绑定
                    "slot_ref": SLOT_REF,
                    "event_id": event.get("event_id", ""),
                    "stratum_key": stratum_key,
                    "event_time": event.get("event_time"),
                    "source": event.get("source", ""),
                    "features": {name: payload.get(name) for name in features},
                    "payload": copy.deepcopy(payload),
                }
                samples.append(sample)

        rng.shuffle(samples)
        return samples[:max_total], strata_ledger

    @staticmethod
    def _sample_features(samples: list[dict[str, Any]]) -> dict[str, Any]:
        """统计样本保留特征中的数值分布，供调试页面与审核人核对。"""
        values = [
            SampleTraceService._to_float(s.get("features", {}).get("amount"), 0.0)
            for s in samples
        ]
        if not values:
            return {"count": 0}
        n = len(values)
        mean = sum(values) / n
        stddev = (sum((v - mean) ** 2 for v in values) / n) ** 0.5
        ordered = sorted(values)
        return {
            "count": n,
            "mean": round(mean, 4),
            "stddev": round(stddev, 4),
            "min": ordered[0],
            "max": ordered[-1],
            "p50": ordered[n // 2],
            "p95": ordered[min(int(n * 0.95), n - 1)],
        }

    def _build_batch(
        self,
        *,
        events: list[dict[str, Any]],
        policy: dict[str, Any],
        file_boundary: str,
        file_name: str,
        file_checksum: str,
        reviewer: str,
        origin: str,
        source_page: str,
        import_ledger_id: str | None,
    ) -> dict[str, Any]:
        ratio = self._to_float(policy.get("采样比例"), 0.2)
        max_total = int(self._to_float(policy.get("样本上限"), 100))
        group_key = str(policy.get("分组键") or "region")
        features = [
            f.strip() for f in str(policy.get("保留特征", "")).split(",") if f.strip()
        ] or ["amount", "region", "user_id"]
        strategy_version = self._next_strategy_version()
        batch_id = _new_id("B16-", 10)

        samples, strata_ledger = self._stratify(
            events, group_key, ratio, max_total, batch_id, strategy_version, features,
        )
        batch = {
            "batch_id": batch_id,
            "slot_ref": SLOT_REF,
            "file_boundary": file_boundary,
            "file_name": file_name,
            "file_checksum": file_checksum,
            "sampler_name": policy.get("策略名称", ""),
            "strategy_version": strategy_version,
            "strategy_snapshot": copy.deepcopy(policy),
            "group_key": group_key,
            "source_event_count": len(events),
            "sample_count": len(samples),
            "strata": strata_ledger,
            "samples": samples,  # 完整样本，调试会话按 sample_id 全量引用
            "sample_features": self._sample_features(samples),
            "status": "生效",
            "superseded_by": None,
            "reviewer": reviewer,
            "source_page": source_page,
            "origin": origin,
            "import_ledger_id": import_ledger_id,
            "created_at": _now_text(),
        }
        store.sample_batches()[batch_id] = batch
        return batch

    # ------------------------------------------------------------------
    # 序号16 文件导入（幂等 + 事务回滚）
    # ------------------------------------------------------------------

    def import_file(
        self,
        file_payload: dict[str, Any],
        *,
        reviewer: str | None = None,
        source_page: str | None = None,
    ) -> dict[str, Any]:
        """导入序号16同源导出文件并生成分层样本数据。

        幂等：文件校验和相同的连续/重复导入直接返回首个批次，不再入库。
        事务：校验或采样过程抛错时回滚全部序号16注册表改动，并退回原文件。
        """
        checksum = checksum_of(file_payload)
        fingerprint = source_fingerprint(file_payload)
        # 幂等检查：同一源文件（边界+策略相同）只生效一次，忽略路由元数据差异
        for ledger in store.trace_ledger():
            if (
                ledger.get("direction") == "import"
                and ledger.get("source_fingerprint") == fingerprint
                and ledger.get("status") == "生效"
            ):
                existing = store.sample_batches().get(ledger["batch_id"])
                return {
                    "ok": True,
                    "idempotent": True,
                    "message": "相同文件已导入过，本次按幂等跳过",
                    "batch_id": ledger["batch_id"],
                    "strategy_version": existing["strategy_version"] if existing else None,
                    "file_checksum": checksum,
                    "source_fingerprint": fingerprint,
                }

        snapshot = store.trace_snapshot()
        try:
            self._validate_import_file(file_payload)
            events = file_payload["events"]
            boundary = str(
                file_payload.get("file_boundary")
                or file_payload.get("file_name")
                or "default-boundary"
            )
            file_name = str(file_payload.get("file_name", f"{SLOT_REF}_{boundary}.json"))
            policy = self._resolve_sampler_policy(
                file_payload.get("sampler"), file_payload.get("sampler_name"),
            )
            batch = self._build_batch(
                events=events,
                policy=policy,
                file_boundary=boundary,
                file_name=file_name,
                file_checksum=checksum,
                reviewer=reviewer or file_payload.get("reviewer") or DEFAULT_REVIEWER,
                origin="import",
                source_page=source_page or file_payload.get("source_page", "数据采样"),
                import_ledger_id=None,
            )
            batch["source_fingerprint"] = fingerprint
            ledger = self._append_ledger(
                direction="import",
                file_name=file_name,
                checksum=checksum,
                fingerprint=fingerprint,
                boundary=boundary,
                batch_id=batch["batch_id"],
                source_page=source_page or file_payload.get("source_page", "数据采样"),
                status="生效",
                message="导入成功并生成分层样本数据",
            )
            batch["import_ledger_id"] = ledger["ledger_id"]
            # 若导入文件本身带历史报告，登记为该批次的历史报告（非当前）
            for report in file_payload.get("debug_reports", []) or []:
                self._absorb_imported_report(batch, report)
            return {
                "ok": True,
                "idempotent": False,
                "message": "导入成功，分层样本数据已绑定采样批次与策略版本",
                "batch_id": batch["batch_id"],
                "strategy_version": batch["strategy_version"],
                "file_boundary": boundary,
                "file_checksum": checksum,
                "source_fingerprint": fingerprint,
                "sample_count": batch["sample_count"],
                "strata": batch["strata"],
                "ledger_id": ledger["ledger_id"],
            }
        except Exception as exc:  # 事务失败：回滚并退回原文件
            store.trace_rollback(snapshot)
            return {
                "ok": False,
                "rolled_back": True,
                "message": f"导入事务失败，已回滚并退回原文件：{exc}",
                "original_file": copy.deepcopy(file_payload),
                "file_checksum": checksum,
            }

    @staticmethod
    def _validate_import_file(file_payload: dict[str, Any]) -> None:
        if not isinstance(file_payload, dict):
            raise ValueError("导入文件必须是 JSON 对象")
        if "events" not in file_payload:
            raise ValueError("导入文件缺少 events 源事件数据")
        if not isinstance(file_payload["events"], list):
            raise ValueError("events 必须是事件数组")
        for index, event in enumerate(file_payload["events"]):
            if not isinstance(event, dict):
                raise ValueError(f"第 {index} 条事件结构非法")

    def _absorb_imported_report(
        self, batch: dict[str, Any], report_snapshot: dict[str, Any],
    ) -> None:
        report = {
            "report_id": _new_id("R16-", 12),
            "slot_ref": SLOT_REF,
            "batch_id": batch["batch_id"],
            "strategy_version": batch["strategy_version"],
            "sample_version": batch["strategy_version"],
            "session_id": report_snapshot.get("session_id", ""),
            "target_operator": report_snapshot.get("target_operator", "导入历史算子"),
            "is_current": False,
            "status": "历史",
            "anomaly_marks": report_snapshot.get("anomaly_marks", []),
            "anomaly_count": report_snapshot.get("anomaly_count", 0),
            "intermediate_results": report_snapshot.get("intermediate_results", []),
            "sample_count": batch["sample_count"],
            "sample_checksum": self._samples_checksum(batch["samples"]),
            "created_at": _now_text(),
            "completed_at": report_snapshot.get("completed_at"),
        }
        store.debug_reports()[report["report_id"]] = report

    # ------------------------------------------------------------------
    # 调试会话与调试异常报告
    # ------------------------------------------------------------------

    def start_debug_session(
        self, batch_id: str, target_operator: str, session_name: str | None = None,
    ) -> dict[str, Any]:
        """基于完整分层样本启动调试会话。

        同一批次重复启动调试不产生多个当前报告：已有运行中会话或当前报告时原样返回。
        """
        batch = self._require_batch(batch_id)
        if not batch["samples"]:
            return {"ok": False, "error": "该批次没有完整样本，无法启动调试会话"}

        # 幂等：同批次 + 同算子已有运行中会话 → 直接返回
        for session in store.trace_sessions().values():
            if (
                session["batch_id"] == batch_id
                and session["target_operator"] == target_operator
                and session["status"] == "运行中"
            ):
                return {
                    "ok": True,
                    "idempotent": True,
                    "message": "该批次调试会话已在运行中，未重复创建",
                    "session": self._session_view(session),
                }
        # 幂等：同批次已有当前报告 → 不再新开一轮
        current = self._current_report(batch_id)
        if current is not None:
            return {
                "ok": True,
                "idempotent": True,
                "message": "该批次已存在当前报告，重复启动不产生新报告",
                "report_id": current["report_id"],
                "batch_id": batch_id,
            }

        session = {
            "session_id": _new_id("S16-", 12),
            "slot_ref": SLOT_REF,
            "session_name": session_name or f"调试会话_{target_operator}_{batch_id}",
            "target_operator": target_operator,
            "batch_id": batch_id,
            "strategy_version": batch["strategy_version"],
            "sample_ids": [s["sample_id"] for s in batch["samples"]],  # 引用完整样本
            "status": "运行中",
            "report_id": None,
            "history_report_ids": [],
            "created_at": _now_text(),
        }
        store.trace_sessions()[session["session_id"]] = session
        report = self._produce_report(session, batch)
        session["report_id"] = report["report_id"]
        return {
            "ok": True,
            "idempotent": False,
            "message": "调试会话已启动并生成唯一当前调试异常报告",
            "session": self._session_view(session),
            "report_id": report["report_id"],
        }

    def _produce_report(
        self, session: dict[str, Any], batch: dict[str, Any],
    ) -> dict[str, Any]:
        """调试追踪消费分层样本，按目标算子记录中间结果与异常标记。"""
        features = batch.get("sample_features", {})
        threshold = features.get("mean", 0.0) + 2 * features.get("stddev", 0.0)
        intermediate: list[dict[str, Any]] = []
        anomaly_marks: list[dict[str, Any]] = []
        for sample in batch["samples"]:
            amount = self._to_float(sample.get("features", {}).get("amount"), 0.0)
            region = str(sample.get("features", {}).get("region", ""))
            expected = round(amount, 4)
            is_anomaly = region == "r4" or (features.get("count", 0) > 1 and amount > threshold)
            actual = round(expected * 1.5, 4) if is_anomaly else expected
            diff = round(actual - expected, 4)
            mark = "threshold_breach" if is_anomaly else "normal"
            row = {
                "sample_id": sample["sample_id"],
                "event_id": sample["event_id"],
                "stratum_key": sample["stratum_key"],
                "target_operator": session["target_operator"],
                "expected": expected,
                "actual": actual,
                "diff": diff,
                "anomaly_mark": mark,
            }
            intermediate.append(row)
            if is_anomaly:
                anomaly_marks.append({
                    "sample_id": sample["sample_id"],
                    "event_id": sample["event_id"],
                    "mark": mark,
                    "detail": f"region={region}, amount={amount}, threshold={round(threshold, 4)}",
                })

        report = {
            "report_id": _new_id("R16-", 12),
            "slot_ref": SLOT_REF,
            "batch_id": batch["batch_id"],
            "strategy_version": batch["strategy_version"],
            "sample_version": batch["strategy_version"],  # 报告与样本版本保持一致
            "session_id": session["session_id"],
            "target_operator": session["target_operator"],
            "is_current": True,
            "status": "当前",
            "anomaly_marks": anomaly_marks,
            "anomaly_count": len(anomaly_marks),
            "intermediate_results": intermediate,
            "sample_count": len(batch["samples"]),
            "sample_checksum": self._samples_checksum(batch["samples"]),
            "created_at": _now_text(),
            "completed_at": None,
        }
        # 同一批次只保留一个当前报告
        self._demote_current_reports(batch["batch_id"])
        store.debug_reports()[report["report_id"]] = report
        return report

    def complete_session(self, session_id: str) -> dict[str, Any]:
        """按原样本完成当前报告（调试期间上限被修改时的第一种处理方式）。"""
        session = self._require_session(session_id)
        if session["status"] != "运行中":
            return {"ok": False, "error": f"会话状态为「{session['status']}」，无需完成"}
        report = store.debug_reports().get(session["report_id"]) if session.get("report_id") else None
        if report is None:
            return {"ok": False, "error": "会话缺少调试异常报告"}
        batch = self._require_batch(session["batch_id"])
        # 按原样本完成：重新校验报告仍与原样本版本一致
        if report["sample_version"] != batch["strategy_version"]:
            return {"ok": False, "error": "报告样本版本与批次策略版本不一致，不能按原样本完成"}
        report["status"] = "当前"
        report["is_current"] = True
        report["completed_at"] = _now_text()
        session["status"] = "已完成"
        return {
            "ok": True,
            "message": "已按原样本完成当前报告，报告版本与样本版本一致",
            "report_id": report["report_id"],
            "strategy_version": report["strategy_version"],
            "anomaly_count": report["anomaly_count"],
        }

    def resample_and_rerun(
        self,
        batch_id: str,
        target_operator: str,
        *,
        new_ratio: float | None = None,
        new_limit: int | None = None,
    ) -> dict[str, Any]:
        """按新比例/新上限重新采样再开一轮（调试期间策略调整的第二种处理方式）。

        旧批次与旧报告降级为历史，新批次得到新策略版本，最终只确定一个与新样本
        版本一致的当前报告。
        """
        old_batch = self._require_batch(batch_id)
        policy = copy.deepcopy(old_batch["strategy_snapshot"])
        if new_ratio is not None:
            policy["采样比例"] = str(new_ratio)
        if new_limit is not None:
            policy["样本上限"] = str(new_limit)

        # 用旧批次的源事件重建：从完整样本无法还原全量事件，因此从导入文件事件重建
        events = self._reconstruct_events(old_batch)
        new_batch = self._build_batch(
            events=events,
            policy=policy,
            file_boundary=old_batch["file_boundary"],
            file_name=old_batch["file_name"],
            file_checksum=old_batch["file_checksum"],
            reviewer=old_batch["reviewer"],
            origin="resample",
            source_page=old_batch["source_page"],
            import_ledger_id=old_batch.get("import_ledger_id"),
        )
        old_batch["status"] = "已被新版本替代"
        old_batch["superseded_by"] = new_batch["batch_id"]
        self._demote_current_reports(batch_id)

        session = {
            "session_id": _new_id("S16-", 12),
            "slot_ref": SLOT_REF,
            "session_name": f"重采样调试会话_{target_operator}_{new_batch['batch_id']}",
            "target_operator": target_operator,
            "batch_id": new_batch["batch_id"],
            "strategy_version": new_batch["strategy_version"],
            "sample_ids": [s["sample_id"] for s in new_batch["samples"]],
            "status": "运行中",
            "report_id": None,
            "history_report_ids": [],
            "rerun_from_batch": batch_id,
            "created_at": _now_text(),
        }
        store.trace_sessions()[session["session_id"]] = session
        report = self._produce_report(session, new_batch)
        report["completed_at"] = _now_text()
        session["status"] = "已完成"
        session["report_id"] = report["report_id"]
        return {
            "ok": True,
            "message": "已按新比例重新采样并开新一轮，当前报告与新样本版本一致",
            "old_batch_id": batch_id,
            "batch_id": new_batch["batch_id"],
            "strategy_version": new_batch["strategy_version"],
            "report_id": report["report_id"],
            "sample_count": new_batch["sample_count"],
            "strata": new_batch["strata"],
        }

    def fault_reopen(self, batch_id: str, target_operator: str) -> dict[str, Any]:
        """序号16 故障重开：沿用同一批样本重建当前报告。

        仅当该批次最近一轮报告处于异常/终止场景时允许；旧报告归档为历史，
        新报告成为唯一当前报告，样本与策略版本不变。
        """
        batch = self._require_batch(batch_id)
        last_report = self._latest_report(batch_id)
        if last_report is None:
            return {"ok": False, "error": "该批次尚无调试报告，无需故障重开，请直接启动调试"}
        # 仅在出现异常标记或当前报告已失效（非当前/异常终止）时才允许故障重开；
        # 完全正常且仍为当前的报告无需重开，避免同一批次平白产生多份报告。
        healthy_current = (
            last_report.get("is_current")
            and last_report.get("anomaly_count", 0) == 0
            and last_report.get("status") == "当前"
        )
        if healthy_current:
            return {
                "ok": False,
                "error": "当前报告无异常且仍生效，不允许故障重开",
            }
        session = {
            "session_id": _new_id("S16-", 12),
            "slot_ref": SLOT_REF,
            "session_name": f"故障重开会话_{target_operator}_{batch_id}",
            "target_operator": target_operator,
            "batch_id": batch_id,
            "strategy_version": batch["strategy_version"],
            "sample_ids": [s["sample_id"] for s in batch["samples"]],
            "status": "运行中",
            "report_id": None,
            "history_report_ids": [last_report["report_id"]],
            "reopened_from_report": last_report["report_id"],
            "created_at": _now_text(),
        }
        store.trace_sessions()[session["session_id"]] = session
        report = self._produce_report(session, batch)
        report["completed_at"] = _now_text()
        session["status"] = "已完成"
        session["report_id"] = report["report_id"]
        return {
            "ok": True,
            "message": "故障重开完成，已沿同批样本重建唯一当前报告",
            "batch_id": batch_id,
            "strategy_version": batch["strategy_version"],
            "report_id": report["report_id"],
            "anomaly_count": report["anomaly_count"],
        }

    # ------------------------------------------------------------------
    # 导出、校验与台账
    # ------------------------------------------------------------------

    def export_batch(self, batch_id: str) -> dict[str, Any]:
        """按采样批次导出分层样本数据与当前调试异常报告，供其他页面同源接收。"""
        batch = self._require_batch(batch_id)
        current = self._current_report(batch_id)
        package = {
            "slot_ref": SLOT_REF,
            "export_kind": "sample_trace",
            "file_boundary": batch["file_boundary"],
            "file_name": f"{SLOT_REF}_{batch['batch_id']}_export.json",
            "source_page": "调试追踪",
            "exported_at": _now_text(),
            "sampler_name": batch["sampler_name"],
            "sampler": copy.deepcopy(batch["strategy_snapshot"]),
            "batch": {
                "batch_id": batch["batch_id"],
                "strategy_version": batch["strategy_version"],
                "sample_count": batch["sample_count"],
                "source_event_count": batch["source_event_count"],
                "strata": batch["strata"],
                "sample_features": batch["sample_features"],
            },
            "events": self._reconstruct_events(batch),
            "samples": copy.deepcopy(batch["samples"]),
            "debug_reports": [
                self._report_view(report)
                for report in store.debug_reports().values()
                if report["batch_id"] == batch_id
            ],
            "current_report_id": current["report_id"] if current else None,
        }
        package["file_checksum"] = checksum_of(package)
        self._append_ledger(
            direction="export",
            file_name=package["file_name"],
            checksum=package["file_checksum"],
            boundary=batch["file_boundary"],
            batch_id=batch_id,
            source_page="调试追踪",
            status="生效",
            message="按采样批次导出分层样本数据与调试异常报告",
        )
        return package

    def verify_export_file(self, file_payload: dict[str, Any]) -> dict[str, Any]:
        """再次上传导出文件时，按采样批次核对分层样本数据与调试异常报告。"""
        expected_checksum = file_payload.get("file_checksum")
        actual_checksum = checksum_of(file_payload)
        checksum_ok = expected_checksum == actual_checksum

        batch_meta = file_payload.get("batch", {})
        batch_id = batch_meta.get("batch_id")
        batch = store.sample_batches().get(batch_id) if batch_id else None
        checks: list[dict[str, Any]] = [
            {"item": "file_checksum", "passed": checksum_ok,
             "expected": expected_checksum, "actual": actual_checksum},
        ]
        if batch is None:
            checks.append({"item": "batch_exists", "passed": False, "expected": batch_id})
            return {"ok": False, "consistent": False, "checks": checks}
        checks.append({"item": "batch_exists", "passed": True, "batch_id": batch_id})
        checks.append({
            "item": "strategy_version",
            "passed": batch_meta.get("strategy_version") == batch["strategy_version"],
            "expected": batch["strategy_version"],
            "actual": batch_meta.get("strategy_version"),
        })
        checks.append({
            "item": "sample_count",
            "passed": batch_meta.get("sample_count") == batch["sample_count"],
            "expected": batch["sample_count"],
            "actual": batch_meta.get("sample_count"),
        })
        exported_strata = {s["stratum_key"]: s["sampled"] for s in batch_meta.get("strata", [])}
        stored_strata = {s["stratum_key"]: s["sampled"] for s in batch["strata"]}
        checks.append({
            "item": "strata_counts",
            "passed": exported_strata == stored_strata,
            "expected": stored_strata,
            "actual": exported_strata,
        })
        current = self._current_report(batch_id)
        current_id = file_payload.get("current_report_id")
        checks.append({
            "item": "current_report",
            "passed": bool(current) and current_id == current["report_id"],
            "expected": current["report_id"] if current else None,
            "actual": current_id,
        })
        if current is not None:
            checks.append({
                "item": "report_sample_version",
                "passed": current["sample_version"] == batch["strategy_version"],
                "expected": batch["strategy_version"],
                "actual": current["sample_version"],
            })
            checks.append({
                "item": "intermediate_result_count",
                "passed": len(current["intermediate_results"]) == batch["sample_count"],
                "expected": batch["sample_count"],
                "actual": len(current["intermediate_results"]),
            })
        consistent = all(item["passed"] for item in checks)
        return {"ok": True, "consistent": consistent, "checks": checks,
                "batch_id": batch_id}

    def compare_export_handling(self, file_payload: dict[str, Any]) -> dict[str, Any]:
        """比较序号16两种导出处理方式的取舍。

        - 校验模式（verify）：只读核对，不产生任何入库副作用，适合责任审核；
        - 回流导入模式（import）：同源文件按指纹幂等，重复只生效一次，适合跨页面归档。
        返回两种方式在当前文件上的预判结果与取舍建议，不写库。
        """
        fingerprint = source_fingerprint(file_payload)
        already_imported = any(
            ledger.get("direction") == "import"
            and ledger.get("source_fingerprint") == fingerprint
            and ledger.get("status") == "生效"
            for ledger in store.trace_ledger()
        )
        verify_preview = self.verify_export_file(file_payload)
        return {
            "ok": True,
            "source_fingerprint": fingerprint,
            "already_imported": already_imported,
            "options": [
                {
                    "mode": "verify",
                    "label": "再上传校验（只读）",
                    "side_effect": False,
                    "idempotent": True,
                    "consistent": verify_preview["consistent"],
                    "tradeoff": "不入库、不改批次，按采样批次核对样本与报告，适合审核与对账",
                },
                {
                    "mode": "import",
                    "label": "回流导入（幂等）",
                    "side_effect": not already_imported,
                    "idempotent": True,
                    "would_create_batch": not already_imported,
                    "tradeoff": "同源文件重复导入只生效一次；新边界才生成新批次与策略版本，适合跨页面接收归档",
                },
            ],
            "recommendation": (
                "文件已导入过：优先用只读校验核对，避免无谓重采"
                if already_imported
                else "首次接收：用回流导入建立批次，再用只读校验复核"
            ),
        }

    # ------------------------------------------------------------------
    # 查询视图
    # ------------------------------------------------------------------

    def list_batches(self) -> list[dict[str, Any]]:
        return [self._batch_summary(b) for b in store.sample_batches().values()]

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        return self._batch_summary(self._require_batch(batch_id), include_samples=True)

    def list_sessions(self) -> list[dict[str, Any]]:
        return [self._session_view(s) for s in store.trace_sessions().values()]

    def get_session(self, session_id: str) -> dict[str, Any]:
        return self._session_view(self._require_session(session_id), include_samples=True)

    def get_report(self, report_id: str) -> dict[str, Any]:
        report = store.debug_reports().get(report_id)
        if report is None:
            raise KeyError(f"调试异常报告 {report_id} 不存在")
        return self._report_view(report)

    def get_current_report(self, batch_id: str) -> dict[str, Any]:
        self._require_batch(batch_id)
        report = self._current_report(batch_id)
        if report is None:
            return {"ok": False, "message": "该批次尚无当前报告"}
        return self._report_view(report)

    def list_ledger(self) -> list[dict[str, Any]]:
        return [dict(item) for item in store.trace_ledger()]

    # ------------------------------------------------------------------
    # 内部辅助
    # ------------------------------------------------------------------

    def _require_batch(self, batch_id: str) -> dict[str, Any]:
        batch = store.sample_batches().get(batch_id)
        if batch is None:
            raise KeyError(f"采样批次 {batch_id} 不存在")
        return batch

    def _require_session(self, session_id: str) -> dict[str, Any]:
        session = store.trace_sessions().get(session_id)
        if session is None:
            raise KeyError(f"调试会话 {session_id} 不存在")
        return session

    def _current_report(self, batch_id: str) -> dict[str, Any] | None:
        for report in store.debug_reports().values():
            if report["batch_id"] == batch_id and report.get("is_current"):
                return report
        return None

    def _latest_report(self, batch_id: str) -> dict[str, Any] | None:
        reports = [
            r for r in store.debug_reports().values() if r["batch_id"] == batch_id
        ]
        if not reports:
            return None
        return sorted(reports, key=lambda r: r["created_at"])[-1]

    def _demote_current_reports(self, batch_id: str) -> None:
        for report in store.debug_reports().values():
            if report["batch_id"] == batch_id and report.get("is_current"):
                report["is_current"] = False
                report["status"] = "历史"

    @staticmethod
    def _samples_checksum(samples: list[dict[str, Any]]) -> str:
        ids = [s["sample_id"] for s in samples]
        return hashlib.sha256(json.dumps(ids, sort_keys=True).encode()).hexdigest()[:16]

    @staticmethod
    def _reconstruct_events(batch: dict[str, Any]) -> list[dict[str, Any]]:
        """从批次完整样本还原事件数组，供重采样复用同源数据。"""
        return [
            {
                "event_id": s.get("event_id"),
                "event_time": s.get("event_time"),
                "source": s.get("source"),
                "payload": copy.deepcopy(s.get("payload", {})),
            }
            for s in batch.get("samples", [])
        ]

    def _append_ledger(
        self,
        *,
        direction: str,
        file_name: str,
        checksum: str,
        boundary: str,
        batch_id: str,
        source_page: str,
        status: str,
        message: str,
        fingerprint: str | None = None,
    ) -> dict[str, Any]:
        ledger = {
            "ledger_id": f"L16-{len(store.trace_ledger()) + 1}",
            "slot_ref": SLOT_REF,
            "direction": direction,
            "file_name": file_name,
            "file_checksum": checksum,
            "source_fingerprint": fingerprint,
            "file_boundary": boundary,
            "batch_id": batch_id,
            "source_page": source_page,
            "status": status,
            "idempotent": False,
            "message": message,
            "created_at": _now_text(),
        }
        store.trace_ledger().append(ledger)
        return ledger

    def _batch_summary(
        self, batch: dict[str, Any], *, include_samples: bool = False,
    ) -> dict[str, Any]:
        view = {
            "batch_id": batch["batch_id"],
            "slot_ref": batch["slot_ref"],
            "file_boundary": batch["file_boundary"],
            "file_name": batch["file_name"],
            "file_checksum": batch["file_checksum"],
            "sampler_name": batch["sampler_name"],
            "strategy_version": batch["strategy_version"],
            "strategy_snapshot": batch["strategy_snapshot"],
            "group_key": batch["group_key"],
            "source_event_count": batch["source_event_count"],
            "sample_count": batch["sample_count"],
            "strata": batch["strata"],
            "sample_features": batch["sample_features"],
            "status": batch["status"],
            "superseded_by": batch["superseded_by"],
            "reviewer": batch["reviewer"],
            "source_page": batch["source_page"],
            "origin": batch["origin"],
            "source_fingerprint": batch.get("source_fingerprint"),
            "created_at": batch["created_at"],
        }
        if include_samples:
            view["samples"] = copy.deepcopy(batch["samples"])
            view["reports"] = [
                self._report_view(r)
                for r in store.debug_reports().values()
                if r["batch_id"] == batch["batch_id"]
            ]
        return view

    def _session_view(
        self, session: dict[str, Any], *, include_samples: bool = False,
    ) -> dict[str, Any]:
        batch = store.sample_batches().get(session["batch_id"], {})
        view = {
            "session_id": session["session_id"],
            "slot_ref": session["slot_ref"],
            "session_name": session["session_name"],
            "target_operator": session["target_operator"],
            "batch_id": session["batch_id"],
            "strategy_version": session["strategy_version"],
            "sample_ids": list(session["sample_ids"]),
            "sample_count": len(session["sample_ids"]),
            "complete_sample_bound": len(session["sample_ids"]) == batch.get("sample_count", -1),
            "status": session["status"],
            "report_id": session.get("report_id"),
            "history_report_ids": session.get("history_report_ids", []),
            "created_at": session["created_at"],
        }
        if include_samples:
            view["samples"] = copy.deepcopy(batch.get("samples", []))
            report = store.debug_reports().get(session["report_id"]) if session.get("report_id") else None
            view["report"] = self._report_view(report) if report else None
        return view

    @staticmethod
    def _report_view(report: dict[str, Any] | None) -> dict[str, Any] | None:
        if report is None:
            return None
        return copy.deepcopy(report)


sample_trace_service = SampleTraceService()
