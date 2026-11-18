"""序号16 路由：分层样本导入导出、采样批次、调试会话与异常报告接口。

与通用 /api/{module} 资源路由并列，独立挂载在 /api/sample-trace 前缀下，
不改变 data_sampler / debug_trace 历史接口的返回结构。
"""
from __future__ import annotations

import json
from urllib.parse import quote
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from app.services.sample_trace import sample_trace_service

router = APIRouter(prefix="/api/sample-trace", tags=["序号16 采样调试链路"])


def _guard(result: dict[str, Any]) -> dict[str, Any]:
    """领域服务把业务失败收敛为 ok=False，路由层透传而不抛 500。"""
    return result


@router.post("/imports")
def import_file(payload: dict[str, Any]) -> dict[str, Any]:
    """序号16 导入文件：生成分层样本数据，重复导入幂等，失败回滚退回原文件。"""
    reviewer = payload.pop("reviewer", None) if isinstance(payload, dict) else None
    source_page = payload.pop("source_page", None) if isinstance(payload, dict) else None
    file_payload = payload.get("file") if isinstance(payload, dict) and "file" in payload else payload
    return _guard(
        sample_trace_service.import_file(
            file_payload, reviewer=reviewer, source_page=source_page,
        )
    )


@router.get("/batches")
def list_batches() -> dict[str, Any]:
    items = sample_trace_service.list_batches()
    return {"items": items, "total": len(items)}


@router.get("/batches/{batch_id}")
def get_batch(batch_id: str) -> dict[str, Any]:
    try:
        return sample_trace_service.get_batch(batch_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/batches/{batch_id}/debug-sessions")
def start_debug_session(batch_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    target = str(payload.get("target_operator", "")).strip()
    if not target:
        raise HTTPException(status_code=422, detail="缺少目标算子 target_operator")
    try:
        return _guard(
            sample_trace_service.start_debug_session(
                batch_id, target, payload.get("session_name"),
            )
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/debug-sessions")
def list_sessions() -> dict[str, Any]:
    items = sample_trace_service.list_sessions()
    return {"items": items, "total": len(items)}


@router.get("/debug-sessions/{session_id}")
def get_session(session_id: str) -> dict[str, Any]:
    try:
        return sample_trace_service.get_session(session_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/debug-sessions/{session_id}/complete")
def complete_session(session_id: str) -> dict[str, Any]:
    try:
        return _guard(sample_trace_service.complete_session(session_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/batches/{batch_id}/resample")
def resample(batch_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    target = str(payload.get("target_operator", "")).strip()
    if not target:
        raise HTTPException(status_code=422, detail="缺少目标算子 target_operator")
    try:
        return _guard(
            sample_trace_service.resample_and_rerun(
                batch_id,
                target,
                new_ratio=payload.get("new_ratio"),
                new_limit=payload.get("new_limit"),
            )
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/batches/{batch_id}/fault-reopen")
def fault_reopen(batch_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    target = str(payload.get("target_operator", "")).strip()
    if not target:
        raise HTTPException(status_code=422, detail="缺少目标算子 target_operator")
    try:
        return _guard(sample_trace_service.fault_reopen(batch_id, target))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/batches/{batch_id}/reports/current")
def current_report(batch_id: str) -> dict[str, Any]:
    try:
        return sample_trace_service.get_current_report(batch_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/reports/{report_id}")
def get_report(report_id: str) -> dict[str, Any]:
    try:
        return sample_trace_service.get_report(report_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/batches/{batch_id}/export")
def export_batch(batch_id: str) -> StreamingResponse:
    """调试追踪详情页提供文件下载：分层样本数据 + 调试异常报告。"""
    try:
        package = sample_trace_service.export_batch(batch_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    body = json.dumps(package, ensure_ascii=False, indent=2)
    filename = package["file_name"]
    ascii_fallback = f"{batch_id}_export.json"
    disposition = (
        f"attachment; filename=\"{ascii_fallback}\"; "
        f"filename*=UTF-8''{quote(filename)}"
    )
    return StreamingResponse(
        iter([body.encode("utf-8")]),
        media_type="application/json; charset=utf-8",
        headers={"Content-Disposition": disposition},
    )


@router.post("/exports/verify")
def verify_export(payload: dict[str, Any]) -> dict[str, Any]:
    """再次上传同源导出文件时按采样批次核对样本与报告，不产生入库副作用。"""
    file_payload = payload.get("file") if "file" in payload else payload
    return sample_trace_service.verify_export_file(file_payload)


@router.post("/exports/compare-handling")
def compare_handling(payload: dict[str, Any]) -> dict[str, Any]:
    """比较只读校验与幂等回流导入两种导出处理方式的取舍，不写库。"""
    file_payload = payload.get("file") if "file" in payload else payload
    return sample_trace_service.compare_export_handling(file_payload)


@router.get("/ledger")
def list_ledger() -> dict[str, Any]:
    items = sample_trace_service.list_ledger()
    return {"items": items, "total": len(items)}
