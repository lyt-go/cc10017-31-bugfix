"""合并批次清单测试：中断续做、重复合并、补数交接与快照去重。"""
from __future__ import annotations

import unittest

from app.pipeline_engine import pipeline_engine
from app.services.compact_batch import (
    DOWNLOAD_MODULE,
    INTAKE_MODULE,
    compact_batch_service as service,
)
from app.store import store


class TestCompactBatchManifest(unittest.TestCase):
    """合并批次清单：同一源文件集合只处理一次，中断后只续做未完成批次。"""

    def setUp(self) -> None:
        service.reset()
        # 深拷贝一份策略，测试内改动不影响其他用例。
        self.policy = next(
            row for row in store.rows("compact_policy") if row.get("目标表名") == "orders")
        self.table = "orders"

    def tearDown(self) -> None:
        service.reset()

    def test_generate_reads_policy_and_writes_manifest(self) -> None:
        """生成批次时重新读取策略，批次里保留策略快照与目标文件。"""
        batches, message = service.generate_batches(target_table=self.table)
        self.assertTrue(batches, message)
        batch = batches[0]
        for field in ("batch_no", "target_file", "source_files", "pending_files", "resume_boundary", "version"):
            self.assertIn(field, batch)
        self.assertEqual(batch["policy_id"], self.policy["id"])
        self.assertEqual(batch["policy_snapshot"]["最小文件数"], self.policy["最小文件数"])
        self.assertEqual(len(batch["source_files"]), len(set(batch["source_files"])))

    def test_interrupt_persists_merged_files_and_boundary(self) -> None:
        """中断时先把已合并文件、目标文件与续做边界落盘。"""
        batch = service.generate_batches(target_table=self.table)[0][0]
        version_before = batch["version"]
        source_files = list(batch["source_files"])
        target_file = batch["target_file"]
        interrupted, message = service.interrupt_batch(batch["id"], merge_count=2)
        self.assertIsNotNone(interrupted)
        self.assertEqual(interrupted["status"], "中断")
        self.assertEqual(len(interrupted["merged_files"]), 2)
        self.assertEqual(interrupted["pending_files"], source_files[2:])
        self.assertEqual(interrupted["resume_boundary"], source_files[1])
        self.assertEqual(interrupted["target_file"], target_file)
        self.assertGreater(interrupted["version"], version_before)
        self.assertIn("续做边界", message)

    def test_resume_only_finishes_unfinished_batches(self) -> None:
        """续做只处理未完成批次，已经合并的文件不会再次进入合并集合。"""
        batches, _ = service.generate_batches(target_table=self.table)
        first = service.interrupt_batch(batches[0]["id"], merge_count=2)[0]
        merged_before = list(first["merged_files"])

        resumed, messages = service.resume_table(self.table)
        self.assertEqual(len(resumed), len(batches))
        completed = service.find_batch(batches[0]["id"])
        self.assertEqual(completed["status"], "已完成")
        self.assertEqual(completed["merged_files"][:2], merged_before)
        # 已合并文件不重复，且都只属于一个目标文件。
        self.assertEqual(len(completed["merged_files"]), len(set(completed["merged_files"])))

        again, again_messages = service.resume_table(self.table)
        self.assertEqual(again, [])
        self.assertIn("没有未完成批次", again_messages[0])
        self.assertEqual(len(messages), len(batches))
        self.assertTrue(all("续做" in message for message in messages))

    def test_same_source_set_is_not_remerged(self) -> None:
        """同一源文件集合重复申请生成批次时不会重复进入合并。"""
        first, _ = service.generate_batches(target_table=self.table)
        source = list(first[0]["source_files"])
        second, message = service.generate_batches(target_table=self.table, source_files=source)
        self.assertEqual(second, [])
        self.assertIn("未完成批次", message)

        service.resume_table(self.table)
        third, message = service.generate_batches(target_table=self.table, source_files=source)
        self.assertEqual(third, [])
        self.assertIn("没有新的源文件", message)
        self.assertEqual(service.snapshot(self.table)["duplicate_count"], 0)

    def test_snapshot_counts_duplicates_once(self) -> None:
        """快照统计按去重口径计数；人为制造重复文件时能单独暴露。"""
        batches, _ = service.generate_batches(target_table=self.table)
        service.resume_table(self.table)
        snapshot = service.snapshot(self.table)
        self.assertEqual(snapshot["duplicate_count"], 0)
        self.assertEqual(snapshot["source_file_count"], snapshot["unique_file_count"])
        self.assertEqual(
            snapshot["merged_file_count"],
            len({file for batch in service.batches_for(self.table) for file in batch["merged_files"]}),
        )

        # 直接破坏清单模拟历史重复写入，统计不应把同一文件算两次。
        duplicate = batches[0]["source_files"][0]
        service.batches_for(self.table)[1]["source_files"].append(duplicate)
        broken = service.snapshot(self.table)
        self.assertIn(duplicate, broken["duplicate_files"])
        self.assertEqual(broken["unique_file_count"], broken["source_file_count"] - 1)

    def test_backfill_rejects_already_merged_files(self) -> None:
        """补数页追加文件前先读清单，已合并文件被拒绝再次追加。"""
        batch = service.generate_batches(target_table=self.table)[0][0]
        service.resume_batch(batch["id"])
        merged_file = batch["source_files"][0]

        result = service.append_backfill_files(self.table, [merged_file])
        self.assertFalse(result["ok"])
        self.assertIn(merged_file, result["rejected_duplicates"])
        self.assertEqual(result["accepted_files"], [])
        self.assertEqual(service.snapshot(self.table)["duplicate_count"], 0)

    def test_backfill_waits_for_old_batch_first(self) -> None:
        """恢复窗口相遇时先完成旧批次，新补数文件排队进入下一批。"""
        batches, _ = service.generate_batches(target_table=self.table)
        service.interrupt_batch(batches[0]["id"], merge_count=2)
        new_file = f"warehouse/{self.table}/backfill/part-9001.parquet"

        result = service.append_backfill_files(self.table, [new_file])
        self.assertTrue(result["ok"])
        self.assertEqual(result["decision"], "先完成旧批次")
        self.assertEqual(result["accepted_files"], [new_file])
        # 新文件不混入旧批次，旧批次已合并文件保持不动。
        first = service.find_batch(batches[0]["id"])
        self.assertNotIn(new_file, first["source_files"])
        self.assertEqual(len(first["merged_files"]), 2)
        # 排队文件在旧批次续做完成后才进入下一批。
        service.resume_table(self.table)
        queued = service.queued_files(self.table)
        self.assertEqual([row["file"] for row in queued], [new_file])
        next_batches, _ = service.generate_batches(target_table=self.table)
        self.assertEqual(next_batches[0]["source_files"], [new_file])
        self.assertEqual(service.queued_files(self.table), [])
        self.assertEqual(service.snapshot(self.table)["duplicate_count"], 0)

    def test_conflict_preview_shows_two_versions(self) -> None:
        """恢复窗口相遇时，两种做法形成不同版本的合并批次清单。"""
        batches, _ = service.generate_batches(target_table=self.table)
        service.interrupt_batch(batches[0]["id"], merge_count=2)
        new_file = f"warehouse/{self.table}/backfill/part-9101.parquet"

        preview = service.conflict_preview(self.table, [new_file])
        self.assertEqual(preview["policy"], "先完成旧批次")
        self.assertFalse(preview["same_version"])
        branches = {branch["decision"]: branch for branch in preview["branches"]}
        self.assertTrue(branches["先完成旧批次"]["selected"])
        self.assertIn("接收新补数文件", branches)
        self.assertNotEqual(
            branches["先完成旧批次"]["version"], branches["接收新补数文件"]["version"])
        # 预览不写清单。
        self.assertEqual(service.manifest_version(self.table), preview["current_version"])

        # 实际执行固定判定后，版本与选定分支一致。
        service.append_backfill_files(self.table, [new_file])
        service.resume_table(self.table)
        service.generate_batches(target_table=self.table)
        self.assertEqual(service.manifest_version(self.table), preview["branches"][0]["version"])
        self.assertEqual(service.snapshot(self.table)["duplicate_count"], 0)

    def test_manifest_keeps_bookmark_and_version_on_download(self) -> None:
        """清单下载保留下载书签与当前版本。"""
        service.generate_batches(target_table=self.table)
        manifest = service.manifest(self.table)
        self.assertGreater(manifest["version"], 0)
        self.assertTrue(manifest["bookmark"].startswith(f"BOOKMARK-{self.table}-v"))

        items, exported = service.export_payload(self.table)
        self.assertTrue(items)
        self.assertEqual(exported["last_download"]["target_table"], self.table)
        self.assertEqual(exported["last_download"]["version"], exported["version"])
        self.assertEqual(exported["last_download"]["bookmark"], exported["bookmark"])
        self.assertEqual(len(store.rows(DOWNLOAD_MODULE)), 1)
        # 下载不改变清单版本，刷新页面后书签保持一致。
        self.assertEqual(service.manifest(self.table)["version"], exported["version"])

    def test_pipeline_reuses_manifest_on_rerun(self) -> None:
        """补数后合并重复执行不会重复合并，也不会重复统计快照文件。"""
        first = pipeline_engine.run_backfill_then_compact(1)
        self.assertTrue(first["ok"])
        first_snapshot = service.snapshot(self.table)
        self.assertGreater(first_snapshot["batch_count"], 0)
        self.assertEqual(first_snapshot["duplicate_count"], 0)

        second = pipeline_engine.run_backfill_then_compact(1)
        self.assertTrue(second["ok"])
        second_snapshot = service.snapshot(self.table)
        self.assertEqual(first["result"]["batch_nos"], second["result"]["batch_nos"])
        self.assertEqual(first_snapshot["source_file_count"], second_snapshot["source_file_count"])
        self.assertEqual(second_snapshot["duplicate_count"], 0)
        # 同一补数任务同一批次只留一条产物记录。
        produced = [
            row for row in store.rows("compact_results")
            if row.get("backfill_id") == 1
        ]
        self.assertEqual(len(produced), 1)
        self.assertEqual(len(store.rows(INTAKE_MODULE)), 0)

    def test_maintenance_status_exposes_manifest_to_both_pages(self) -> None:
        """维护链返回同一份清单，补数页与合并清理页读到相同版本。"""
        service.generate_batches(target_table=self.table)
        status = pipeline_engine.maintenance_chain_status(self.table)
        self.assertIn("manifest", status)
        self.assertEqual(status["manifest"]["version"], service.manifest(self.table)["version"])
        self.assertEqual(status["snapshot"]["duplicate_count"], 0)


if __name__ == "__main__":
    unittest.main()
