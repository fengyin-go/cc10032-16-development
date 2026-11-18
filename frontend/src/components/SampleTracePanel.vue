<template>
  <section class="sample-trace" data-testid="sample-trace-panel">
    <div class="analysis-form">
      <div>
        <strong>序号16 · 分层采样与导入台账</strong>
        <p class="page-desc">
          导入同源事件文件生成分层样本数据，每份样本绑定唯一采样批次与策略版本；
          重复导入只入库一次，事务失败自动回滚并退回原文件。
        </p>
      </div>
      <div class="row-actions">
        <button class="btn primary" type="button" data-testid="open-import" @click="openImport">
          序号16导入
        </button>
        <button class="btn" type="button" data-testid="refresh-batches" @click="reload">
          刷新批次
        </button>
      </div>
    </div>

    <p v-if="message" class="page-foot">
      <span :class="message.error ? 'error-text' : ''">{{ message.text }}</span>
    </p>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">采样批次</span>
        <strong class="stat-value">{{ batches.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">分层样本总数</span>
        <strong class="stat-value">{{ totalSamples }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">生效/被替代</span>
        <strong class="stat-value">{{ activeCount }} / {{ supersededCount }}</strong>
      </article>
    </div>

    <table class="data-table" data-testid="batch-table">
      <thead>
        <tr>
          <th>采样批次</th>
          <th>文件边界</th>
          <th>策略版本</th>
          <th>分层数量</th>
          <th>样本数/源事件</th>
          <th>责任审核人</th>
          <th>来源页面</th>
          <th>状态</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="batch in batches" :key="batch.batch_id" :data-testid="`batch-${batch.batch_id}`">
          <td>{{ batch.batch_id }}</td>
          <td>{{ batch.file_boundary }}</td>
          <td>{{ batch.strategy_version }}</td>
          <td>
            <span v-for="s in batch.strata" :key="s.stratum_key" class="strata-chip">
              {{ s.stratum_key }}:{{ s.sampled }}/{{ s.population }}
            </span>
          </td>
          <td>{{ batch.sample_count }} / {{ batch.source_event_count }}</td>
          <td>{{ batch.reviewer }}</td>
          <td>{{ batch.source_page }}</td>
          <td>{{ batch.status }}</td>
          <td class="row-actions">
            <button class="link" type="button" @click="viewDetail(batch.batch_id)">详情</button>
            <button
              class="link"
              type="button"
              data-testid="goto-debug"
              @click="goDebug(batch)"
            >
              去调试
            </button>
          </td>
        </tr>
        <tr v-if="!batches.length">
          <td colspan="9" class="empty-state">暂无序号16分层样本数据，请先导入文件</td>
        </tr>
      </tbody>
    </table>

    <div v-if="selected" class="modal-backdrop" data-testid="batch-detail-modal">
      <div class="modal">
        <h3>批次详情 · {{ selected.batch_id }}</h3>
        <dl class="dag-detail">
          <dt>策略版本</dt><dd>{{ selected.strategy_version }}</dd>
          <dt>文件边界</dt><dd>{{ selected.file_boundary }}</dd>
          <dt>分组键</dt><dd>{{ selected.group_key }}</dd>
          <dt>样本特征</dt><dd>{{ featureText(selected) }}</dd>
          <dt>文件校验和</dt><dd class="checksum">{{ selected.file_checksum }}</dd>
          <dt>同源指纹</dt><dd class="checksum">{{ selected.source_fingerprint ?? '—' }}</dd>
        </dl>
        <h4>分层数量（责任审核人核对）</h4>
        <table class="data-table">
          <thead><tr><th>分层键</th><th>层内总量</th><th>抽样数</th></tr></thead>
          <tbody>
            <tr v-for="s in selected.strata" :key="s.stratum_key">
              <td>{{ s.stratum_key }}</td>
              <td>{{ s.population }}</td>
              <td>{{ s.sampled }}</td>
            </tr>
          </tbody>
        </table>
        <div class="page-actions" style="margin-top: 12px">
          <button class="btn ghost" type="button" @click="selected = null">关闭</button>
        </div>
      </div>
    </div>

    <div v-if="showImport" class="modal-backdrop" data-testid="import-modal">
      <form class="modal" @submit.prevent="submitImport">
        <h3>序号16 导入分层样本文件</h3>
        <label class="create-field">
          <span>导入方式</span>
          <select v-model="importMode" data-testid="import-mode">
            <option value="upload">上传同源导出 JSON 文件</option>
            <option value="demo">生成演示源事件（无文件时）</option>
          </select>
        </label>

        <template v-if="importMode === 'upload'">
          <label class="create-field">
            <span>选择文件</span>
            <input type="file" accept="application/json,.json" data-testid="import-file"
                   @change="onFileChange" required />
          </label>
        </template>
        <template v-else>
          <label class="create-field">
            <span>文件边界</span>
            <input v-model="demoForm.boundary" data-testid="demo-boundary" required />
          </label>
          <label class="create-field">
            <span>源事件数量</span>
            <input v-model.number="demoForm.count" type="number" min="1" data-testid="demo-count" />
          </label>
          <label class="create-field">
            <span>采样比例</span>
            <input v-model="demoForm.ratio" data-testid="demo-ratio" />
          </label>
          <label class="create-field">
            <span>样本上限</span>
            <input v-model.number="demoForm.limit" type="number" min="1" data-testid="demo-limit" />
          </label>
        </template>

        <label class="create-field">
          <span>责任审核人</span>
          <input v-model="reviewer" data-testid="import-reviewer" placeholder="数据工程组" />
        </label>
        <label class="create-field">
          <span>来源页面</span>
          <input v-model="sourcePage" data-testid="import-source-page" placeholder="其他页面" />
        </label>

        <p v-if="importError" class="error-text">{{ importError }}</p>
        <div class="page-actions">
          <button class="btn" type="button" @click="showImport = false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-import" :disabled="submitting">
            {{ submitting ? '导入中…' : '确认导入' }}
          </button>
        </div>
      </form>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'

import {
  importSampleTrace,
  listBatches,
  getBatch,
  type BatchSummary,
} from '@/api/sample-trace'
import { readJsonFile } from '@/api/sample-trace-file'

const router = useRouter()
const batches = ref<BatchSummary[]>([])
const selected = ref<BatchSummary | null>(null)
const showImport = ref(false)
const submitting = ref(false)
const importMode = ref<'upload' | 'demo'>('upload')
const importError = ref('')
const reviewer = ref('')
const sourcePage = ref('其他页面')
const pickedFile = ref<Record<string, unknown> | null>(null)
const message = ref<{ text: string; error: boolean } | null>(null)

const demoForm = ref({ boundary: 'boundary-A', count: 40, ratio: '0.25', limit: 60 })

const totalSamples = computed(() =>
  batches.value.reduce((sum, b) => sum + b.sample_count, 0),
)
const activeCount = computed(() => batches.value.filter((b) => b.status === '生效').length)
const supersededCount = computed(() =>
  batches.value.filter((b) => b.superseded_by !== null).length,
)

function featureText(batch: BatchSummary): string {
  const f = batch.sample_features
  return `n=${f.count} 均值=${f.mean ?? '-'} 标准差=${f.stddev ?? '-'} P95=${f.p95 ?? '-'}`
}

async function reload() {
  try {
    batches.value = await listBatches()
  } catch (error) {
    message.value = {
      text: error instanceof Error ? error.message : '采样批次读取失败',
      error: true,
    }
  }
}

function openImport() {
  showImport.value = true
  importError.value = ''
  pickedFile.value = null
}

async function onFileChange(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  if (!file) return
  try {
    pickedFile.value = await readJsonFile(file)
    importError.value = ''
  } catch (error) {
    pickedFile.value = null
    importError.value = error instanceof Error ? error.message : '文件读取失败'
  }
}

function buildDemoFile(): Record<string, unknown> {
  const events = Array.from({ length: demoForm.value.count }, (_, i) => ({
    event_id: `ev_${i}`,
    event_time: 1000 + i,
    source: 'demo_source_16',
    payload: {
      user_id: `u${i % 4}`,
      amount: Math.round((10 + i * 2) * 100) / 100,
      region: `r${i % 5}`,
    },
  }))
  return {
    file_name: `s16_${demoForm.value.boundary}.json`,
    file_boundary: demoForm.value.boundary,
    source_page: sourcePage.value || '数据采样',
    events,
    sampler: {
      策略名称: `策略_${demoForm.value.boundary}`,
      采样方式: 'stratified',
      采样比例: demoForm.value.ratio,
      分组键: 'region',
      样本上限: String(demoForm.value.limit),
      保留特征: 'amount,region,user_id',
      采样窗口: '60',
    },
  }
}

async function submitImport() {
  importError.value = ''
  let payload: Record<string, unknown> | null = null
  if (importMode.value === 'upload') {
    if (!pickedFile.value) {
      importError.value = '请先选择有效的 JSON 文件'
      return
    }
    payload = pickedFile.value
  } else {
    payload = buildDemoFile()
  }
  submitting.value = true
  try {
    const result = await importSampleTrace(
      payload,
      reviewer.value || undefined,
      sourcePage.value || undefined,
    )
    message.value = { text: result.message, error: !result.ok }
    if (result.ok) {
      showImport.value = false
      await reload()
    } else {
      importError.value = result.message
    }
  } catch (error) {
    importError.value = error instanceof Error ? error.message : '导入失败'
  } finally {
    submitting.value = false
  }
}

async function viewDetail(batchId: string) {
  try {
    selected.value = await getBatch(batchId)
  } catch (error) {
    message.value = {
      text: error instanceof Error ? error.message : '批次详情读取失败',
      error: true,
    }
  }
}

function goDebug(batch: BatchSummary) {
  void router.push({
    path: '/debug_trace',
    query: { batch: batch.batch_id, slot: batch.slot_ref },
  })
}

onMounted(reload)
</script>

<style scoped>
.sample-trace { margin-top: 16px; }
.strata-chip {
  display: inline-block;
  margin: 1px 4px 1px 0;
  padding: 1px 6px;
  background: #eef4ff;
  border: 1px solid #cfe0ff;
  border-radius: 10px;
  font-size: 12px;
  white-space: nowrap;
}
.checksum {
  font-family: monospace;
  font-size: 11px;
  word-break: break-all;
}
</style>
