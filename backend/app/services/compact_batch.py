"""合并批次清单：合并清理与流式补数之间的交接对象。

清单把「哪些源文件已经合并进哪个目标文件」「中断后续做从哪里开始」写成
可恢复的记录，保证：

1. 同一源文件集合在一个合并批次中只处理一次，跨批次也不会重复分配；
2. 合并中断时先落盘已合并文件、目标文件与续做边界，恢复时只续做未完成批次；
3. 补数文件进入表之前先读清单，已合并/已入批的文件不再追加；
4. 快照统计按去重后的文件口径计数，不因重跑或续做重复计数。

清单只依赖内存 store，和其余模块保持同样的实现方式。
"""
from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import datetime
from typing import Any

from app.store import store

BATCH_MODULE = "compact_batch_manifests"
INTAKE_MODULE = "backfill_file_intake"
DOWNLOAD_MODULE = "compact_manifest_downloads"
POLICY_MODULE = "compact_policy"

STATUS_PENDING = "待合并"
STATUS_RUNNING = "进行中"
STATUS_INTERRUPTED = "中断"
STATUS_DONE = "已完成"
UNFINISHED_STATUSES = (STATUS_PENDING, STATUS_RUNNING, STATUS_INTERRUPTED)

# 中断窗口内收到新补数文件时的固定判定：先完成旧批次，新文件进入下一批。
CONFLICT_POLICY = "先完成旧批次"
NEXT_BATCH_DECISION = "接收新补数文件"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _unique(items: list[Any]) -> list[Any]:
    """保留首次出现顺序去重，None/空串直接丢弃。"""
    seen: set[Any] = set()
    result: list[Any] = []
    for item in items:
        if item is None:
            continue
        if isinstance(item, str):
            item = item.strip()
            if not item:
                continue
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _as_int(value: Any, default: int) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


class CompactBatchService:
    """合并批次清单的生成、中断、恢复、补数交接与快照统计。"""

    # ───────────────────────────────
    # 读取
    # ───────────────────────────────

    def _batch_rows(self) -> list[dict[str, Any]]:
        return store.rows(BATCH_MODULE)

    def batches_for(self, target_table: str | None = None) -> list[dict[str, Any]]:
        rows = self._batch_rows()
        if target_table:
            rows = [row for row in rows if row.get("target_table") == target_table]
        return sorted(rows, key=lambda row: _as_int(row.get("id"), 0))

    def find_batch(self, batch_id: int) -> dict[str, Any] | None:
        return next(
            (row for row in self._batch_rows() if _as_int(row.get("id"), 0) == batch_id),
            None,
        )

    def unfinished_batches(self, target_table: str) -> list[dict[str, Any]]:
        return [
            row for row in self.batches_for(target_table)
            if row.get("status") in UNFINISHED_STATUSES
        ]

    def latest_batch(self, target_table: str) -> dict[str, Any] | None:
        batches = self.batches_for(target_table)
        return batches[-1] if batches else None

    def _assigned_files(self, target_table: str, exclude_batch_id: int | None = None) -> set[str]:
        """所有批次的源文件集合：一个文件只能属于一个批次/一个目标文件。"""
        assigned: set[str] = set()
        for row in self.batches_for(target_table):
            if exclude_batch_id is not None and _as_int(row.get("id"), 0) == exclude_batch_id:
                continue
            assigned.update(str(item) for item in row.get("source_files", []))
            assigned.update(str(item) for item in row.get("merged_files", []))
        return assigned

    def _find_policy(self, target_table: str | None, policy_id: int | None) -> dict[str, Any] | None:
        if policy_id is not None:
            policy = store.find(POLICY_MODULE, policy_id)
            if policy is None:
                return None
            if target_table and policy.get("目标表名") != target_table:
                return None
            return policy
        if not target_table:
            return None
        return next(
            (row for row in store.rows(POLICY_MODULE) if row.get("目标表名") == target_table),
            None,
        )

    # ───────────────────────────────
    # 生成批次清单
    # ───────────────────────────────

    def _candidate_files(self, target_table: str, min_files: int) -> list[str]:
        """按清理策略的最小文件数准备一批待合并源文件（真实项目里来自文件系统）。

        数量取 min_files 的两倍，保证能形成不止一个批次；命名稳定，便于演示
        中断续做与重复合并检测。
        """
        total = max(min_files * 2, 6)
        return [f"warehouse/{target_table}/raw/part-{index:04d}.parquet" for index in range(1, total + 1)]

    def _queued_intake(self, target_table: str) -> list[dict[str, Any]]:
        return [
            row for row in store.rows(INTAKE_MODULE)
            if row.get("target_table") == target_table and row.get("status") == "待合并"
        ]

    def _next_batch_seq(self, target_table: str) -> int:
        seqs = [
            _as_int(row.get("seq"), 0) for row in self.batches_for(target_table)
        ]
        return max(seqs, default=0) + 1

    def _next_id(self) -> int:
        return max((_as_int(row.get("id"), 0) for row in self._batch_rows()), default=0) + 1

    def generate_batches(
        self,
        *,
        target_table: str | None = None,
        policy_id: int | None = None,
        source_files: list[str] | None = None,
    ) -> tuple[list[dict[str, Any]], str]:
        """重新读取清理策略并生成合并批次清单。

        生成前会重新读取策略行（并发、最小文件数、目标文件大小等），不会复用
        上一次生成时的缓存；已经进入其他批次的源文件会被排除。若该表还有未
        完成批次，则拒绝生成新批次，先续做旧批次。
        """
        policy = self._find_policy(target_table, policy_id)
        if policy is None:
            return [], f"目标表 {target_table or policy_id} 未配置合并清理策略"
        table = str(policy.get("目标表名") or target_table or "")
        if not table:
            return [], "清理策略缺少目标表名，无法生成合并批次清单"

        unfinished = self.unfinished_batches(table)
        if unfinished:
            names = "、".join(str(row.get("batch_no")) for row in unfinished)
            return [], f"存在未完成批次 {names}，请先续做未完成批次"

        concurrency = max(1, _as_int(policy.get("合并并发"), 1))
        min_files = max(1, _as_int(policy.get("最小文件数"), 1))
        candidates = list(source_files or self._candidate_files(table, min_files))
        candidates.extend(str(row.get("file")) for row in self._queued_intake(table))
        # 同一源文件集合只处理一次：请求内去重 + 排除已分配文件。
        assigned = self._assigned_files(table)
        candidates = [item for item in _unique(candidates) if item not in assigned]
        if not candidates:
            return [], f"表 {table} 没有新的源文件需要合并"

        chunk_size = min_files
        chunk_count = min(concurrency, (len(candidates) + chunk_size - 1) // chunk_size)
        created: list[dict[str, Any]] = []
        for offset in range(chunk_count):
            chunk = candidates[offset * chunk_size:(offset + 1) * chunk_size]
            if not chunk:
                continue
            created.append(self._create_batch(table, policy, chunk))

        # 被本次批次接收的补数文件标记为已入批，避免下一次重复追加。
        accepted = {file for batch in created for file in batch["source_files"]}
        for row in self._queued_intake(table):
            if str(row.get("file")) in accepted:
                row["status"] = "已入批"
                row["batch_no"] = next(
                    batch["batch_no"] for batch in created
                    if str(row.get("file")) in batch["source_files"]
                )
                row["updated_at"] = _now()

        return created, f"已生成 {len(created)} 个合并批次"

    def _create_batch(
        self, target_table: str, policy: dict[str, Any], source_files: list[str],
    ) -> dict[str, Any]:
        seq = self._next_batch_seq(target_table)
        now = _now()
        batch = {
            "id": self._next_id(),
            "seq": seq,
            "batch_no": f"CBM-{target_table}-{seq:04d}",
            "target_table": target_table,
            "target_file": f"warehouse/{target_table}/merged/part-{seq:04d}.parquet",
            "policy_id": policy.get("id"),
            "policy_code": policy.get("策略编号"),
            "policy_snapshot": {
                "策略编号": policy.get("策略编号"),
                "触发条件": policy.get("触发条件"),
                "合并并发": policy.get("合并并发"),
                "目标文件大小": policy.get("目标文件大小"),
                "最小文件数": policy.get("最小文件数"),
                "保留快照数": policy.get("保留快照数"),
            },
            "status": STATUS_PENDING,
            "version": 1,
            "source_files": list(source_files),
            "merged_files": [],
            "pending_files": list(source_files),
            "skipped_files": [],
            "resume_boundary": None,
            "conflict_policy": CONFLICT_POLICY,
            "created_at": now,
            "updated_at": now,
            "interrupted_at": None,
            "resumed_at": None,
        }
        self._batch_rows().append(batch)
        return batch

    # ───────────────────────────────
    # 中断与续做
    # ───────────────────────────────

    def interrupt_batch(
        self, batch_id: int, merge_count: int | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """模拟合并中断：把已完成的源文件、目标文件与续做边界写入清单。"""
        batch = self.find_batch(batch_id)
        if batch is None:
            return None, f"合并批次 {batch_id} 不存在"
        if batch.get("status") == STATUS_DONE:
            return None, f"合并批次 {batch['batch_no']} 已完成，不会重复合并"
        pending = list(batch.get("pending_files", []))
        if not pending:
            return None, f"合并批次 {batch['batch_no']} 没有待合并文件"

        if merge_count is None:
            merge_count = max(1, len(pending) // 2)
        merge_count = max(0, min(_as_int(merge_count, 0), len(pending)))
        committed = pending[:merge_count]
        remaining = pending[merge_count:]

        batch["merged_files"] = _unique(list(batch.get("merged_files", [])) + committed)
        batch["pending_files"] = remaining
        batch["status"] = STATUS_DONE if not remaining else STATUS_INTERRUPTED
        batch["resume_boundary"] = committed[-1] if committed else batch.get("resume_boundary")
        batch["version"] = _as_int(batch.get("version"), 1) + 1
        batch["interrupted_at"] = _now()
        batch["updated_at"] = batch["interrupted_at"]
        batch["last_action"] = f"中断（已合并 {len(committed)} 个文件）"
        return batch, (
            f"批次 {batch['batch_no']} 已中断，记录 {len(committed)} 个已合并文件，"
            f"续做边界 {batch['resume_boundary'] or '无'}"
        )

    def resume_batch(
        self, batch_id: int, merge_count: int | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """只续做未完成批次，已合并文件不会再次进入合并集合。"""
        batch = self.find_batch(batch_id)
        if batch is None:
            return None, f"合并批次 {batch_id} 不存在"
        if batch.get("status") == STATUS_DONE:
            return None, f"合并批次 {batch['batch_no']} 已完成，已合并文件不会重复合并"
        pending = list(batch.get("pending_files", []))
        merged = _unique(list(batch.get("merged_files", [])))
        blocked = self._assigned_files(
            str(batch.get("target_table")), exclude_batch_id=_as_int(batch.get("id"), 0))
        # 已合并文件即使出现在待合并列表里也要跳过，保证同一文件只进一个目标文件。
        skipped = [file for file in pending if file in merged or file in blocked]
        resumable = [file for file in pending if file not in merged and file not in blocked]
        if merge_count is not None:
            resumable = resumable[:max(0, _as_int(merge_count, 0))]

        if not resumable:
            batch["pending_files"] = skipped
            batch["skipped_files"] = _unique(list(batch.get("skipped_files", [])) + skipped)
            batch["status"] = STATUS_INTERRUPTED
            batch["updated_at"] = _now()
            return batch, f"批次 {batch['batch_no']} 没有可续做的文件（已合并文件已跳过）"

        batch["merged_files"] = _unique(merged + resumable)
        batch["pending_files"] = [
            file for file in pending if file not in batch["merged_files"] and file not in skipped
        ]
        batch["skipped_files"] = _unique(list(batch.get("skipped_files", [])) + skipped)
        batch["resume_boundary"] = batch["merged_files"][-1]
        batch["status"] = STATUS_DONE if not batch["pending_files"] else STATUS_RUNNING
        batch["version"] = _as_int(batch.get("version"), 1) + 1
        batch["resumed_at"] = _now()
        batch["updated_at"] = batch["resumed_at"]
        batch["last_action"] = f"续做（本次合并 {len(resumable)} 个文件）"
        return batch, (
            f"批次 {batch['batch_no']} 已续做 {len(resumable)} 个文件，"
            f"续做边界 {batch['resume_boundary']}"
        )

    def run_batch(self, batch_id: int) -> tuple[dict[str, Any] | None, str]:
        """完整执行一个批次（用于补数后自动清理），与续做共用去重口径。"""
        batch = self.find_batch(batch_id)
        if batch is None:
            return None, f"合并批次 {batch_id} 不存在"
        if batch.get("status") == STATUS_DONE:
            return batch, f"批次 {batch['batch_no']} 已完成，复用已有合并结果"
        return self.resume_batch(batch_id)

    def resume_table(self, target_table: str) -> tuple[list[dict[str, Any]], list[str]]:
        """续做整张表的全部未完成批次，返回批次与逐条说明。"""
        resumed: list[dict[str, Any]] = []
        messages: list[str] = []
        for batch in self.unfinished_batches(target_table):
            entry, message = self.resume_batch(_as_int(batch.get("id"), 0))
            messages.append(message)
            if entry is not None:
                resumed.append(entry)
        if not resumed and not messages:
            messages.append(f"表 {target_table} 没有未完成批次")
        return resumed, messages

    # ───────────────────────────────
    # 流式补数交接
    # ───────────────────────────────

    def append_backfill_files(
        self,
        target_table: str,
        files: list[str],
        *,
        backfill_id: int | None = None,
        task_no: str | None = None,
    ) -> dict[str, Any]:
        """补数文件入库前先读清单，已合并/已入批的文件不再追加。

        如果此时还有未完成批次，按固定判定「先完成旧批次」处理：新文件不加入
        旧批次，而是排队等待下一批，旧批次的已合并文件保持不动。
        """
        requested = _unique([str(file) for file in files])
        if not requested:
            return {"ok": False, "message": "没有需要追加的补数文件", "decision": None}

        assigned = self._assigned_files(target_table)
        merged_assigned = {
            str(file)
            for row in self.batches_for(target_table)
            for file in row.get("merged_files", [])
        }
        queued = {str(row.get("file")) for row in self._queued_intake(target_table)}

        accepted: list[str] = []
        duplicates: list[str] = []
        already_queued: list[str] = []
        for file in requested:
            if file in merged_assigned:
                duplicates.append(file)
            elif file in assigned:
                duplicates.append(file)
            elif file in queued:
                already_queued.append(file)
            else:
                accepted.append(file)

        now = _now()
        unfinished = self.unfinished_batches(target_table)
        decision = CONFLICT_POLICY if unfinished else NEXT_BATCH_DECISION
        intake_rows = store.rows(INTAKE_MODULE)
        next_id = max((_as_int(row.get("id"), 0) for row in intake_rows), default=0) + 1
        for file in accepted:
            intake_rows.append({
                "id": next_id,
                "target_table": target_table,
                "file": file,
                "status": "待合并",
                "decision": decision,
                "backfill_id": backfill_id,
                "task_no": task_no,
                "batch_no": None,
                "created_at": now,
                "updated_at": now,
            })
            next_id += 1

        if duplicates:
            message = (
                f"文件 {('、'.join(duplicates))} 已在合并批次清单中，"
                "同一文件只能进入一个目标文件，已拒绝重复追加"
            )
        elif already_queued and not accepted:
            message = (
                f"文件 {('、'.join(already_queued))} 已在待入批队列中，"
                "不会重复追加"
            )
        elif unfinished:
            message = (
                f"存在未完成批次 {('、'.join(str(row.get('batch_no')) for row in unfinished))}，"
                f"按「{CONFLICT_POLICY}」处理：新补数文件排队进入下一批"
            )
        else:
            message = "新补数文件已接收，可在下一次生成合并批次清单时入批"

        return {
            "ok": not duplicates,
            "decision": decision,
            "target_table": target_table,
            "accepted_files": accepted,
            "queued_files": accepted + already_queued,
            "rejected_duplicates": duplicates,
            "already_queued": already_queued,
            "unfinished_batches": [
                {"batch_no": row.get("batch_no"), "status": row.get("status")}
                for row in unfinished
            ],
            "message": message,
        }

    def queued_files(self, target_table: str) -> list[dict[str, Any]]:
        return list(self._queued_intake(target_table))

    def next_backfill_file(self, target_table: str) -> str:
        """为页面「追加一个文件」生成一个尚未出现在清单里的文件名。"""
        assigned = self._assigned_files(target_table)
        assigned.update(str(row.get("file")) for row in store.rows(INTAKE_MODULE))
        seq = 1
        while True:
            candidate = f"warehouse/{target_table}/backfill/part-{seq:04d}.parquet"
            if candidate not in assigned:
                return candidate
            seq += 1

    def conflict_preview(self, target_table: str, files: list[str] | None = None) -> dict[str, Any]:
        """冲突判定预览：两种做法会形成不同版本的批次清单。

        - 先完成旧批次：新文件排队，旧批次续做后另起新批次；
        - 先接收新补数文件：新文件并入旧批次的待合并集合，清单版本不同。

        预览只做计算，不写清单。
        """
        requested = _unique([str(file) for file in (files or [])]) or [self.next_backfill_file(target_table)]
        assigned = self._assigned_files(target_table)
        queued = {str(row.get("file")) for row in self._queued_intake(target_table)}
        accepted = [
            file for file in requested
            if file not in assigned and file not in queued
        ]
        unfinished = self.unfinished_batches(target_table)
        current = self.manifest_version(target_table)

        if unfinished and accepted:
            old_first_version = current + len(unfinished) + 1
            new_first_version = current + len(unfinished)
            note = "同一批补数文件在两种做法下会形成不同清单版本，需按固定判定执行"
        else:
            old_first_version = current + (len(unfinished) or (1 if accepted else 0))
            new_first_version = old_first_version
            note = "当前没有未完成批次与新文件的冲突，两种做法版本一致"

        return {
            "target_table": target_table,
            "current_version": current,
            "candidate_files": requested,
            "accepted_files": accepted,
            "unfinished_batches": [row.get("batch_no") for row in unfinished],
            "policy": CONFLICT_POLICY,
            "branches": [
                {
                    "decision": CONFLICT_POLICY,
                    "description": "旧批次续做完成后再生成新批次，新文件排队等待",
                    "version": old_first_version,
                    "selected": True,
                },
                {
                    "decision": NEXT_BATCH_DECISION,
                    "description": "新文件并入旧批次待合并集合，旧批次边界随之改变",
                    "version": new_first_version,
                    "selected": False,
                },
            ],
            "version_diff": new_first_version - old_first_version,
            "same_version": old_first_version == new_first_version,
            "note": note,
        }

    # ───────────────────────────────
    # 快照统计与版本
    # ───────────────────────────────

    def snapshot(self, target_table: str) -> dict[str, Any]:
        """按文件去重口径统计快照，重复文件会单独暴露而不是重复计数。"""
        batches = self.batches_for(target_table)
        source_files: list[str] = []
        merged_files: list[str] = []
        for batch in batches:
            source_files.extend(str(file) for file in batch.get("source_files", []))
            merged_files.extend(str(file) for file in batch.get("merged_files", []))

        source_counter = Counter(source_files)
        merged_counter = Counter(merged_files)
        duplicate_files = sorted(
            file for file, count in source_counter.items() if count > 1
        ) + sorted(
            file for file, count in merged_counter.items() if count > 1 and file not in source_counter
        )
        duplicate_files = _unique(duplicate_files)

        return {
            "target_table": target_table,
            "batch_count": len(batches),
            "unfinished_batch_count": len(self.unfinished_batches(target_table)),
            "source_file_count": len(source_files),
            "unique_file_count": len(set(source_files)),
            "merged_file_count": len(set(merged_files)),
            "snapshot_file_count": len(set(source_files)),
            "queued_file_count": len(self._queued_intake(target_table)),
            "duplicate_files": duplicate_files,
            "duplicate_count": len(duplicate_files),
            "target_files": _unique([str(batch.get("target_file")) for batch in batches]),
            "version": self.manifest_version(target_table),
            "batches": [
                {
                    "batch_no": batch.get("batch_no"),
                    "status": batch.get("status"),
                    "version": batch.get("version"),
                    "merged_count": len(batch.get("merged_files", [])),
                    "pending_count": len(batch.get("pending_files", [])),
                    "resume_boundary": batch.get("resume_boundary"),
                }
                for batch in batches
            ],
        }

    def manifest_version(self, target_table: str) -> int:
        """清单当前版本：每个批次版本之和，任何写入或续做都会推进版本。"""
        return sum(_as_int(batch.get("version"), 1) for batch in self.batches_for(target_table))

    def build_bookmark(self, target_table: str) -> str:
        payload = json.dumps(
            [
                {
                    "batch_no": batch.get("batch_no"),
                    "version": batch.get("version"),
                    "status": batch.get("status"),
                    "source_files": batch.get("source_files"),
                    "merged_files": batch.get("merged_files"),
                    "pending_files": batch.get("pending_files"),
                }
                for batch in self.batches_for(target_table)
            ],
            ensure_ascii=False,
            sort_keys=True,
        )
        digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:10]
        return f"BOOKMARK-{target_table}-v{self.manifest_version(target_table)}-{digest}"

    def manifest(self, target_table: str) -> dict[str, Any]:
        """清单交接视图：批次、快照、版本与下载书签。"""
        snapshot = self.snapshot(target_table)
        download = self.last_download(target_table)
        return {
            "target_table": target_table,
            "version": snapshot["version"],
            "bookmark": self.build_bookmark(target_table),
            "last_download": download,
            "snapshot": snapshot,
            "items": self.batches_for(target_table),
            "queued_files": self.queued_files(target_table),
            "conflict_policy": CONFLICT_POLICY,
        }

    # ───────────────────────────────
    # 清单下载
    # ───────────────────────────────

    def record_download(self, target_table: str) -> dict[str, Any]:
        now = _now()
        row = {
            "id": max((_as_int(item.get("id"), 0) for item in store.rows(DOWNLOAD_MODULE)), default=0) + 1,
            "target_table": target_table,
            "version": self.manifest_version(target_table),
            "bookmark": self.build_bookmark(target_table),
            "downloaded_at": now,
        }
        store.rows(DOWNLOAD_MODULE).append(row)
        return row

    def last_download(self, target_table: str) -> dict[str, Any] | None:
        rows = [
            row for row in store.rows(DOWNLOAD_MODULE)
            if row.get("target_table") == target_table
        ]
        return rows[-1] if rows else None

    def export_payload(self, target_table: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """导出前先记录下载书签和版本，再返回清单快照。"""
        manifest = self.manifest(target_table)
        download = self.record_download(target_table)
        manifest["last_download"] = download
        manifest["bookmark"] = download["bookmark"]
        manifest["version"] = download["version"]
        return manifest["items"], manifest

    # ───────────────────────────────
    # 测试辅助
    # ───────────────────────────────

    def reset(self) -> None:
        """清空清单相关表，供测试隔离使用。"""
        for module in (BATCH_MODULE, INTAKE_MODULE, DOWNLOAD_MODULE):
            store.rows(module).clear()


compact_batch_service = CompactBatchService()
