"""流式补数 时间线接口：返回作业时间序列结构。

跨模块交接：补数文件在入库前先读取合并批次清单，已合并/已入批的文件不再追加。
"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload
from app.services.backfill_stream import BackfillStreamService
from app.services.compact_batch import compact_batch_service

router = APIRouter(prefix="/api/backfill_stream", tags=["流式补数"])
service = BackfillStreamService()
LIST_FIELDS = ["任务编号", "目标表名", "起始偏移", "截止偏移", "重放速率", "并行度", "覆盖策略", "结果校验"]
STATUSES = ["草稿", "运行中", "已完成", "已取消"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def list_entries() -> dict[str, Any]:
    items = service.get_timeline()
    return {"items": items}


@router.get("/export")
def export_entries() -> StreamingResponse:
    items = service.get_timeline()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    all_keys = ["id"]
    if items:
        all_keys = list(items[0].keys())
    writer.writerow(all_keys)
    for item in items:
        writer.writerow([csv_cell(item.get(k, "")) for k in all_keys])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=backfill_stream.csv"})


@router.get("/manifest")
def get_manifest(target_table: str = Query(default="orders")) -> dict:
    """读取合并批次清单：补数页与合并清理工作台共用同一份交接对象。"""
    return compact_batch_service.manifest(target_table)


@router.post("/intake")
def append_intake(payload: dict[str, Any]) -> dict[str, Any]:
    """追加补数文件：先读清单，已合并文件拒绝再次进入，未完成批次优先续做。"""
    target_table = str(payload.get("target_table") or "").strip()
    if not target_table:
        return {"ok": False, "message": "缺少 target_table", "decision": None}
    raw_files = payload.get("files")
    if isinstance(raw_files, str):
        files = [raw_files]
    elif isinstance(raw_files, list):
        files = [str(item) for item in raw_files]
    else:
        files = []
    if not files:
        # 未显式给文件时，生成一个尚未出现在清单里的补数文件，便于页面一键验证。
        files = [compact_batch_service.next_backfill_file(target_table)]
    result = compact_batch_service.append_backfill_files(
        target_table,
        files,
        backfill_id=payload.get("backfill_id"),
        task_no=payload.get("task_no"),
    )
    result["manifest"] = compact_batch_service.manifest(target_table)
    return result


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"补数任务 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="补数任务已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
