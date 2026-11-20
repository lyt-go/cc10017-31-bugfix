<template>
  <section class="manifest-panel" :data-manifest-mode="mode" data-testid="manifest-panel" :data-target-table="manifest.target_table">
    <header class="manifest-head">
      <div>
        <h3>合并批次清单</h3>
        <p class="manifest-desc">
          合并清理与流式补数共用的交接对象：同一源文件只进入一个目标文件，中断后只续做未完成批次。
        </p>
      </div>
      <label class="manifest-target">
        <span>目标表</span>
        <select v-model="targetTable" data-testid="manifest-target" @change="reload">
          <option v-for="table in tableOptions" :key="table" :value="table">{{ table }}</option>
        </select>
      </label>
    </header>

    <p v-if="message" class="manifest-message" data-testid="manifest-message">{{ message }}</p>
    <p v-if="errorMessage" class="error-text" data-testid="manifest-error">{{ errorMessage }}</p>

    <div class="manifest-meta">
      <span>当前版本：<strong data-testid="manifest-version">v{{ manifest.version }}</strong></span>
      <span>书签：<code data-testid="manifest-bookmark">{{ manifest.bookmark || '—' }}</code></span>
      <span data-testid="manifest-last-download">
        最近下载：{{ manifest.last_download ? `${manifest.last_download.bookmark} · v${manifest.last_download.version}` : '尚未下载' }}
      </span>
    </div>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">合并批次</span>
        <strong class="stat-value" data-testid="manifest-batch-count">{{ snapshot.batch_count }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">未完成批次</span>
        <strong class="stat-value" data-testid="manifest-unfinished-count">{{ snapshot.unfinished_batch_count }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">快照源文件（去重）</span>
        <strong class="stat-value" data-testid="manifest-snapshot-files">{{ snapshot.unique_file_count }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">重复计数文件</span>
        <strong class="stat-value" data-testid="manifest-duplicate-count">{{ snapshot.duplicate_count }}</strong>
      </article>
    </div>

    <div class="page-actions manifest-actions">
      <button class="btn primary" type="button" data-testid="manifest-generate" @click="generate">
        生成合并批次清单
      </button>
      <button class="btn" type="button" data-testid="manifest-resume-all" @click="resumeAll">
        续做未完成批次
      </button>
      <button class="btn" type="button" data-testid="manifest-export" @click="downloadManifest">
        下载批次清单
      </button>
    </div>

    <table class="data-table manifest-table">
      <thead>
        <tr>
          <th>批次号</th><th>状态</th><th>版本</th><th>目标文件</th>
          <th>已合并</th><th>待合并</th><th>续做边界</th><th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="batch in manifest.items"
          :key="batch.batch_no"
          :data-testid="`manifest-batch-${batch.batch_no}`"
          :data-status="batch.status"
        >
          <td>{{ batch.batch_no }}</td>
          <td>{{ batch.status }}</td>
          <td>{{ batch.version }}</td>
          <td class="manifest-file">{{ batch.target_file }}</td>
          <td>{{ batch.merged_files.length }}</td>
          <td>{{ batch.pending_files.length }}</td>
          <td class="manifest-file">{{ batch.resume_boundary || '—' }}</td>
          <td class="row-actions">
            <button
              class="link" type="button"
              :data-testid="`manifest-interrupt-${batch.batch_no}`"
              :disabled="batch.status === '已完成'"
              @click="interrupt(batch)"
            >模拟中断</button>
            <button
              class="link" type="button"
              :data-testid="`manifest-resume-${batch.batch_no}`"
              :disabled="batch.status === '已完成'"
              @click="resume(batch)"
            >继续合并</button>
          </td>
        </tr>
        <tr v-if="!manifest.items.length">
          <td colspan="8" class="empty-state">暂无合并批次，可先生成合并批次清单</td>
        </tr>
      </tbody>
    </table>

    <div v-if="mode === 'intake'" class="intake-box">
      <label>
        <span>追加补数文件</span>
        <input
          v-model="intakeFile"
          data-testid="manifest-intake-input"
          :placeholder="`warehouse/${targetTable}/backfill/part-0001.parquet`"
        />
      </label>
      <button class="btn" type="button" data-testid="manifest-intake-append" @click="appendIntake">
        追加文件
      </button>
      <span class="intake-decision" data-testid="manifest-intake-decision">
        判定：{{ intakeDecision || '—' }}
      </span>
    </div>

    <div v-if="mode === 'intake'" class="intake-queue">
      <strong>待入批补数文件</strong>
      <ul>
        <li v-for="row in manifest.queued_files" :key="row.file" :data-testid="`manifest-queued-${row.file}`">
          {{ row.file }} — {{ row.decision }}
        </li>
        <li v-if="!manifest.queued_files.length" class="empty-state">暂无排队文件</li>
      </ul>
    </div>

    <div v-if="conflict" class="conflict-box" data-testid="manifest-conflict">
      <strong>冲突判定：{{ conflict.policy }}</strong>
      <p>{{ conflict.note }}</p>
      <ul>
        <li v-for="branch in conflict.branches" :key="branch.decision" :data-testid="`manifest-branch-${branch.decision}`">
          {{ branch.decision }} — {{ branch.description }}（清单版本 v{{ branch.version }}{{ branch.selected ? '，选定' : '' }}）
        </li>
      </ul>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { request } from '@/api/client'

const props = withDefaults(defineProps<{ mode?: 'workbench' | 'intake'; table?: string }>(), {
  mode: 'workbench',
  table: 'orders',
})

type Batch = {
  id: number
  batch_no: string
  status: string
  version: number
  target_file: string
  merged_files: string[]
  pending_files: string[]
  source_files: string[]
  resume_boundary: string | null
}

type Manifest = {
  target_table: string
  version: number
  bookmark: string
  last_download: { bookmark: string; version: number; downloaded_at: string } | null
  conflict_policy: string
  items: Batch[]
  queued_files: { file: string; decision: string; status: string }[]
  snapshot: {
    batch_count: number
    unfinished_batch_count: number
    unique_file_count: number
    duplicate_count: number
  }
}

const targetTable = ref(props.table)
const tableOptions = ref<string[]>([props.table])
const manifest = ref<Manifest>({
  target_table: props.table,
  version: 0,
  bookmark: '',
  last_download: null,
  conflict_policy: '先完成旧批次',
  items: [],
  queued_files: [],
  snapshot: { batch_count: 0, unfinished_batch_count: 0, unique_file_count: 0, duplicate_count: 0 },
})
const snapshot = ref(manifest.value.snapshot)
const message = ref('')
const errorMessage = ref('')
const intakeFile = ref('')
const intakeDecision = ref('')
const conflict = ref<{
  policy: string
  note: string
  branches: { decision: string; description: string; version: number; selected: boolean }[]
} | null>(null)

function applyManifest(payload: Manifest) {
  manifest.value = payload
  snapshot.value = payload.snapshot
}

async function reload() {
  errorMessage.value = ''
  conflict.value = null
  try {
    const response = await request(`/api/backfill_stream/manifest?target_table=${encodeURIComponent(targetTable.value)}`)
    if (!response.ok) throw new Error('合并批次清单读取失败')
    applyManifest(await response.json())
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '合并批次清单读取失败'
  }
}

async function loadTables() {
  try {
    const response = await request('/api/compact_policy?page=1&size=200')
    if (!response.ok) return
    const payload = await response.json()
    const tables = Array.from(
      new Set((payload.items ?? []).map((row: Record<string, string>) => row['目标表名']).filter(Boolean)),
    ) as string[]
    if (tables.length) {
      tableOptions.value = tables
      if (!tables.includes(targetTable.value)) targetTable.value = tables[0]
    }
  } catch {
    tableOptions.value = [props.table]
  }
}

async function generate() {
  message.value = ''
  errorMessage.value = ''
  try {
    const response = await request('/api/compact_policy/manifests/generate', {
      method: 'POST',
      body: JSON.stringify({ target_table: targetTable.value }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '生成合并批次清单失败')
    message.value = payload.message
    if (payload.entry) applyManifest(payload.entry)
    else await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '生成合并批次清单失败'
  }
}

async function runBatchAction(batch: Batch, action: 'interrupt' | 'resume') {
  message.value = ''
  errorMessage.value = ''
  try {
    const response = await request(`/api/compact_policy/manifests/${batch.id}/${action}`, {
      method: 'POST',
      body: JSON.stringify({}),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '批次操作失败')
    message.value = payload.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次操作失败'
  }
}

function interrupt(batch: Batch) {
  return runBatchAction(batch, 'interrupt')
}

function resume(batch: Batch) {
  return runBatchAction(batch, 'resume')
}

async function resumeAll() {
  message.value = ''
  errorMessage.value = ''
  try {
    const response = await request('/api/compact_policy/manifests/resume', {
      method: 'POST',
      body: JSON.stringify({ target_table: targetTable.value }),
    })
    const payload = await response.json()
    message.value = payload.message || (payload.ok ? '续做完成' : '没有可续做的批次')
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '续做未完成批次失败'
  }
}

async function downloadManifest() {
  message.value = ''
  errorMessage.value = ''
  try {
    const response = await request(`/api/compact_policy/manifests/export?target_table=${encodeURIComponent(targetTable.value)}`)
    if (!response.ok) throw new Error('下载合并批次清单失败')
    const bookmark = response.headers.get('X-Manifest-Bookmark') ?? ''
    const version = response.headers.get('X-Manifest-Version') ?? ''
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = `compact_manifest_${targetTable.value}.csv`
    link.click()
    URL.revokeObjectURL(url)
    message.value = `清单已下载，书签 ${bookmark}，版本 v${version}`
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '下载合并批次清单失败'
  }
}

async function appendIntake() {
  message.value = ''
  errorMessage.value = ''
  const requested = intakeFile.value.trim()
  try {
    const response = await request('/api/backfill_stream/intake', {
      method: 'POST',
      body: JSON.stringify({
        target_table: targetTable.value,
        files: requested ? [requested] : [],
      }),
    })
    const payload = await response.json()
    if (!response.ok) throw new Error(payload.message || '追加补数文件失败')
    intakeDecision.value = payload.decision ?? ''
    message.value = payload.message
    if (payload.ok && requested) intakeFile.value = ''
    if (payload.manifest) applyManifest(payload.manifest)
    await loadConflict(payload.accepted_files?.length ? [requested] : [])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '追加补数文件失败'
  }
}

async function loadConflict(files: string[]) {
  try {
    const response = await request('/api/compact_policy/manifests/conflict-preview', {
      method: 'POST',
      body: JSON.stringify({ target_table: targetTable.value, files }),
    })
    if (!response.ok) return
    conflict.value = await response.json()
  } catch {
    conflict.value = null
  }
}

onMounted(async () => {
  await loadTables()
  await reload()
})
</script>
