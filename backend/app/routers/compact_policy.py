"""合并清理接口：维护清理策略，覆盖提交校验、标记生效、停用策略等动作。

同时暴露合并批次清单：生成批次、模拟中断、续做未完成批次、查看快照与下载清单。
"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.compact_batch import compact_batch_service
from app.services.compact_policy import CompactPolicyService

router = APIRouter(prefix="/api/compact_policy", tags=["合并清理"])

service = CompactPolicyService()

LIST_FIELDS = ["策略编号", "目标表名", "触发条件", "合并并发", "目标文件大小", "最小文件数", "保留快照数", "执行时间窗口"]
STATUSES = ["草稿", "待校验", "已生效", "已停用"]


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按策略编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、已生效、已停用"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、策略编号关键字与状态过滤合并清理列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="compact_policy.csv"'})


@router.post("/manifests/conflict-preview")
def preview_manifest_conflict(payload: dict[str, Any]) -> dict:
    """冲突判定预览：先完成旧批次 vs 先接收新补数文件，两种做法形成不同清单版本。"""
    target_table = str(payload.get("target_table") or "orders").strip()
    raw_files = payload.get("files")
    files = [str(item) for item in raw_files] if isinstance(raw_files, list) else None
    return compact_batch_service.conflict_preview(target_table, files)


@router.get("/manifests")
def get_manifest(target_table: str = Query(default="orders")) -> dict:
    """读取合并批次清单：批次、已合并文件、续做边界、快照统计与书签。"""
    return compact_batch_service.manifest(target_table)


@router.post("/manifests/generate")
def generate_manifest(payload: dict[str, Any]) -> ActionResult:
    """重新读取清理策略并生成合并批次清单；已有未完成批次时拒绝生成新批次。"""
    target_table = str(payload.get("target_table") or "").strip()
    policy_id = payload.get("policy_id")
    source_files = payload.get("source_files")
    if not target_table and policy_id is None:
        return ActionResult(ok=False, message="缺少 target_table 或 policy_id")
    batches, message = compact_batch_service.generate_batches(
        target_table=target_table or None,
        policy_id=int(policy_id) if policy_id is not None else None,
        source_files=list(source_files) if isinstance(source_files, list) else None,
    )
    table = str((batches[0].get("target_table") if batches else target_table) or "")
    if not batches:
        if "没有新的源文件" in message:
            # 没有新文件不是冲突：清单保持不变，页面只需提示。
            return ActionResult(
                ok=True, message=message,
                entry=compact_batch_service.manifest(table) if table else None)
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=compact_batch_service.manifest(table))


@router.get("/manifests/snapshot")
def get_manifest_snapshot(target_table: str = Query(default="orders")) -> dict:
    """快照统计：按文件去重计数，重复文件单独列出。"""
    return compact_batch_service.snapshot(target_table)


@router.get("/manifests/export")
def export_manifest(target_table: str = Query(default="orders")) -> StreamingResponse:
    """下载合并批次清单，并把下载书签和当前版本记录到清单上。"""
    items, manifest = compact_batch_service.export_payload(target_table)
    output = io.StringIO()
    output.write("﻿")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow([
        "id", "batch_no", "target_table", "target_file", "status", "version",
        "source_files", "merged_files", "pending_files", "resume_boundary",
        "bookmark", "manifest_version",
    ])
    for item in items:
        row = [
            item.get("id"), item.get("batch_no"), item.get("target_table"), item.get("target_file"),
            item.get("status"), item.get("version"),
            "|".join(str(file) for file in item.get("source_files", [])),
            "|".join(str(file) for file in item.get("merged_files", [])),
            "|".join(str(file) for file in item.get("pending_files", [])),
            item.get("resume_boundary") or "",
            manifest["bookmark"], manifest["version"],
        ]
        writer.writerow([("'" + str(value)) if str(value).startswith(("=", "+", "-", "@")) else value for value in row])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="compact_manifest_{target_table}.csv"',
            "X-Manifest-Bookmark": str(manifest["bookmark"]),
            "X-Manifest-Version": str(manifest["version"]),
        })


@router.post("/manifests/{batch_id}/interrupt")
def interrupt_manifest(batch_id: int, payload: dict[str, Any] | None = None) -> ActionResult:
    """模拟合并中断：先把已合并文件、目标文件和续做边界写入清单。"""
    merge_count = (payload or {}).get("merge_count")
    batch, message = compact_batch_service.interrupt_batch(
        batch_id, int(merge_count) if merge_count is not None else None)
    if batch is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=batch)


@router.post("/manifests/{batch_id}/resume")
def resume_manifest(batch_id: int, payload: dict[str, Any] | None = None) -> ActionResult:
    """只续做未完成批次；已合并文件不会被再次合并。"""
    merge_count = (payload or {}).get("merge_count")
    batch, message = compact_batch_service.resume_batch(
        batch_id, int(merge_count) if merge_count is not None else None)
    if batch is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=batch)


@router.post("/manifests/resume")
def resume_table_manifests(payload: dict[str, Any]) -> ActionResult:
    """按表续做全部未完成批次，并返回最新清单。"""
    target_table = str(payload.get("target_table") or "").strip()
    if not target_table:
        return ActionResult(ok=False, message="缺少 target_table")
    batches, messages = compact_batch_service.resume_table(target_table)
    return ActionResult(
        ok=bool(batches),
        message="；".join(messages),
        entry=compact_batch_service.manifest(target_table),
    )


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条清理策略明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"清理策略 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条清理策略，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="清理策略已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条清理策略执行提交校验、标记生效、停用策略；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
