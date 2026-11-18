<template>
  <section class="debug-trace-panel" data-testid="debug-trace-panel">
    <div class="analysis-form">
      <div>
        <strong>序号16 · 调试追踪与异常报告</strong>
        <p class="page-desc">
          调试会话引用完整分层样本，按目标算子记录中间结果与异常标记并生成唯一当前报告；
          同批次重复启动不产生多份当前报告，支持按原样本完成、按新比例重采样与故障重开。
        </p>
      </div>
      <div class="row-actions">
        <button class="btn" type="button" data-testid="refresh-sessions" @click="reload">
          刷新
        </button>
      </div>
    </div>

    <p v-if="message" class="page-foot">
      <span :class="message.error ? 'error-text' : ''">{{ message.text }}</span>
    </p>

    <div v-if="activeBatch" class="analysis-form" data-testid="active-batch">
      <div>
        <div>当前采样批次：<strong>{{ activeBatch.batch_id }}</strong></div>
        <div class="page-desc">
          策略版本 {{ activeBatch.strategy_version }} · 样本 {{ activeBatch.sample_count }} ·
          分层 {{ activeBatch.strata.length }} · 状态 {{ activeBatch.status }}
        </div>
      </div>
      <div class="row-actions">
        <input v-model="targetOperator" data-testid="target-operator" placeholder="目标算子" />
        <button class="btn primary" type="button" data-testid="start-debug" @click="startSession">
          启动调试
        </button>
        <button class="btn" type="button" data-testid="resample" @click="resample">
          按新比例重采样
        </button>
        <button class="btn" type="button" data-testid="fault-reopen" @click="reopen">
          故障重开
        </button>
        <button class="btn" type="button" data-testid="download-export" @click="download">
          文件下载
        </button>
        <button class="btn" type="button" data-testid="open-verify" @click="showVerify = true">
          再上传校验
        </button>
      </div>
    </div>

    <div v-else class="analysis-form">
      <span class="page-desc">
        未从采样页面携带批次。请先到「数据采样」页面打开序号16批次并点击“去调试”，
        或在下方会话中选择批次。
      </span>
    </div>

    <h4>调试会话（可追溯到完整样本）</h4>
    <table class="data-table" data-testid="session-table">
      <thead>
        <tr>
          <th>会话</th>
          <th>目标算子</th>
          <th>采样批次</th>
          <th>样本版本</th>
          <th>完整样本</th>
          <th>状态</th>
          <th>当前报告</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="s in sessions" :key="s.session_id" :data-testid="`session-${s.session_id}`">
          <td>{{ s.session_name }}</td>
          <td>{{ s.target_operator }}</td>
          <td>
            <button class="link" type="button" @click="selectBatch(s.batch_id)">{{ s.batch_id }}</button>
          </td>
          <td>{{ s.strategy_version }}</td>
          <td>{{ s.complete_sample_bound ? `完整(${s.sample_count})` : `部分(${s.sample_count})` }}</td>
          <td>{{ s.status }}</td>
          <td>{{ s.report_id ?? '—' }}</td>
          <td class="row-actions">
            <button class="link" type="button" @click="openSession(s.session_id)">报告/中间结果</button>
            <button
              v-if="s.status === '运行中'"
              class="link"
              type="button"
              @click="complete(s.session_id)"
            >
              按原样本完成
            </button>
          </td>
        </tr>
        <tr v-if="!sessions.length">
          <td colspan="8" class="empty-state">暂无调试会话</td>
        </tr>
      </tbody>
    </table>

    <h4>导入导出台账</h4>
    <table class="data-table" data-testid="ledger-table">
      <thead>
        <tr>
          <th>台账</th><th>方向</th><th>文件名</th><th>文件边界</th>
          <th>采样批次</th><th>来源页面</th><th>状态</th><th>时间</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in ledger" :key="row.ledger_id" :data-testid="`ledger-${row.ledger_id}`">
          <td>{{ row.ledger_id }}</td>
          <td>{{ row.direction === 'import' ? '导入' : '导出' }}</td>
          <td>{{ row.file_name }}</td>
          <td>{{ row.file_boundary }}</td>
          <td>{{ row.batch_id }}</td>
          <td>{{ row.source_page }}</td>
          <td>{{ row.status }}</td>
          <td>{{ row.created_at }}</td>
        </tr>
        <tr v-if="!ledger.length">
          <td colspan="8" class="empty-state">暂无导入导出台账</td>
        </tr>
      </tbody>
    </table>

    <!-- 报告与中间结果 -->
    <div v-if="openedSession" class="modal-backdrop" data-testid="report-modal">
      <div class="modal modal-wide">
        <h3>调试异常报告 · {{ openedSession.report?.report_id ?? '无报告' }}</h3>
        <template v-if="openedSession.report">
          <dl class="dag-detail">
            <dt>目标算子</dt><dd>{{ openedSession.report.target_operator }}</dd>
            <dt>样本版本/报告版本</dt>
            <dd>{{ openedSession.report.sample_version }} / {{ openedSession.report.strategy_version }}</dd>
            <dt>当前状态</dt>
            <dd>{{ openedSession.report.is_current ? '当前报告' : '历史报告' }}（{{ openedSession.report.status }}）</dd>
            <dt>异常数</dt><dd>{{ openedSession.report.anomaly_count }}</dd>
            <dt>样本校验和</dt><dd class="checksum">{{ openedSession.report.sample_checksum }}</dd>
          </dl>
          <h4>中间结果差异</h4>
          <table class="data-table">
            <thead>
              <tr>
                <th>样本</th><th>事件</th><th>分层</th><th>期望</th>
                <th>实际</th><th>差异</th><th>异常标记</th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="row in openedSession.report.intermediate_results"
                :key="row.sample_id"
                :data-anomaly="row.anomaly_mark"
              >
                <td>{{ row.sample_id }}</td>
                <td>{{ row.event_id }}</td>
                <td>{{ row.stratum_key }}</td>
                <td>{{ row.expected }}</td>
                <td>{{ row.actual }}</td>
                <td>{{ row.diff }}</td>
                <td :class="row.anomaly_mark === 'normal' ? '' : 'error-text'">
                  {{ row.anomaly_mark }}
                </td>
              </tr>
            </tbody>
          </table>
        </template>
        <div class="page-actions" style="margin-top: 12px">
          <button class="btn ghost" type="button" @click="openedSession = null">关闭</button>
        </div>
      </div>
    </div>

    <!-- 再上传校验 -->
    <div v-if="showVerify" class="modal-backdrop" data-testid="verify-modal">
      <div class="modal">
        <h3>按采样批次校验导出文件</h3>
        <label class="create-field">
          <span>选择序号16导出 JSON</span>
          <input type="file" accept="application/json,.json" data-testid="verify-file"
                 @change="onVerifyFile" />
        </label>
        <table v-if="verifyResult" class="data-table">
          <thead><tr><th>校验项</th><th>结果</th><th>期望</th><th>实际</th></tr></thead>
          <tbody>
            <tr v-for="c in verifyResult.checks" :key="c.item">
              <td>{{ c.item }}</td>
              <td :class="c.passed ? '' : 'error-text'">{{ c.passed ? '通过' : '不一致' }}</td>
              <td>{{ formatValue(c.expected) }}</td>
              <td>{{ formatValue(c.actual) }}</td>
            </tr>
          </tbody>
        </table>
        <p v-if="verifyResult" :class="verifyResult.consistent ? '' : 'error-text'">
          {{ verifyResult.consistent ? '分层样本数据与调试异常报告核对一致' : '校验未通过，请核对采样批次内容' }}
        </p>
        <div v-if="comparison" class="analysis-card" data-testid="handling-comparison">
          <strong>两种导出处理取舍</strong>
          <p class="page-desc">{{ comparison.recommendation }}</p>
          <table class="data-table">
            <thead><tr><th>方式</th><th>入库副作用</th><th>说明</th></tr></thead>
            <tbody>
              <tr v-for="opt in comparison.options" :key="opt.mode">
                <td>{{ opt.label }}</td>
                <td>{{ opt.side_effect ? '有（首次会建批次）' : '无（只读）' }}</td>
                <td>{{ opt.tradeoff }}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <p v-if="verifyError" class="error-text">{{ verifyError }}</p>
        <div class="page-actions" style="margin-top: 12px">
          <button class="btn ghost" type="button" @click="showVerify = false">关闭</button>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'

import {
  compareExportHandling,
  completeSession,
  exportBatchUrl,
  faultReopen,
  getBatch,
  getSession,
  listBatches,
  listLedger,
  listSessions,
  resampleBatch,
  startDebugSession,
  verifyExportFile,
  type BatchSummary,
  type HandlingComparison,
  type LedgerRow,
  type SessionView,
  type VerifyResult,
} from '@/api/sample-trace'
import { readJsonFile } from '@/api/sample-trace-file'

const route = useRoute()
const sessions = ref<SessionView[]>([])
const ledger = ref<LedgerRow[]>([])
const batches = ref<BatchSummary[]>([])
const activeBatch = ref<BatchSummary | null>(null)
const openedSession = ref<SessionView | null>(null)
const targetOperator = ref('window_agg')
const message = ref<{ text: string; error: boolean } | null>(null)
const showVerify = ref(false)
const verifyResult = ref<VerifyResult | null>(null)
const comparison = ref<HandlingComparison | null>(null)
const verifyError = ref('')

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return '—'
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

async function reload() {
  try {
    const [sessionList, ledgerList, batchList] = await Promise.all([
      listSessions(),
      listLedger(),
      listBatches(),
    ])
    sessions.value = sessionList
    ledger.value = ledgerList
    batches.value = batchList
    const queryBatch = typeof route.query.batch === 'string' ? route.query.batch : ''
    const targetId = queryBatch || activeBatch.value?.batch_id || batchList[0]?.batch_id
    if (targetId) await selectBatch(targetId)
  } catch (error) {
    message.value = {
      text: error instanceof Error ? error.message : '调试数据读取失败',
      error: true,
    }
  }
}

async function selectBatch(batchId: string) {
  activeBatch.value = await getBatch(batchId)
}

async function startSession() {
  if (!activeBatch.value) return
  try {
    const result = await startDebugSession(
      activeBatch.value.batch_id,
      targetOperator.value,
    )
    message.value = {
      text: String(result.message ?? '调试会话已处理'),
      error: result.ok === false,
    }
    await reload()
  } catch (error) {
    message.value = { text: error instanceof Error ? error.message : '启动失败', error: true }
  }
}

async function complete(sessionId: string) {
  try {
    const result = await completeSession(sessionId)
    message.value = {
      text: String(result.message ?? '已完成'),
      error: result.ok === false,
    }
    await reload()
  } catch (error) {
    message.value = { text: error instanceof Error ? error.message : '完成失败', error: true }
  }
}

async function resample() {
  if (!activeBatch.value) return
  const ratioText = window.prompt('输入新的采样比例（0~1）', '0.6')
  if (ratioText === null) return
  const limitText = window.prompt('输入新的样本上限', String(activeBatch.value.sample_count))
  if (limitText === null) return
  try {
    const result = await resampleBatch(
      activeBatch.value.batch_id,
      targetOperator.value,
      Number(ratioText),
      Number(limitText),
    )
    message.value = {
      text: String(result.message ?? '已重新采样'),
      error: result.ok === false,
    }
    await reload()
  } catch (error) {
    message.value = { text: error instanceof Error ? error.message : '重采样失败', error: true }
  }
}

async function reopen() {
  if (!activeBatch.value) return
  try {
    const result = await faultReopen(activeBatch.value.batch_id, targetOperator.value)
    message.value = {
      text: String(result.message ?? '故障重开已处理'),
      error: result.ok === false,
    }
    await reload()
  } catch (error) {
    message.value = { text: error instanceof Error ? error.message : '故障重开失败', error: true }
  }
}

function download() {
  if (!activeBatch.value) return
  const anchor = document.createElement('a')
  anchor.href = exportBatchUrl(activeBatch.value.batch_id)
  anchor.download = ''
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  message.value = { text: '已发起序号16导出文件下载', error: false }
}

async function onVerifyFile(event: Event) {
  verifyError.value = ''
  verifyResult.value = null
  comparison.value = null
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  if (!file) return
  try {
    const parsed = await readJsonFile(file)
    const [verify, compare] = await Promise.all([
      verifyExportFile(parsed),
      compareExportHandling(parsed),
    ])
    verifyResult.value = verify
    comparison.value = compare
  } catch (error) {
    verifyError.value = error instanceof Error ? error.message : '校验失败'
  }
}

async function openSession(sessionId: string) {
  openedSession.value = await getSession(sessionId)
}

onMounted(reload)
</script>

<style scoped>
.modal-wide { width: min(880px, 96vw); }
.checksum { font-family: monospace; font-size: 11px; word-break: break-all; }
.row-actions input {
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 6px 8px;
}
</style>
