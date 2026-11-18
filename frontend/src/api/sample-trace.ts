/**
 * 序号16 采样-调试可追溯链路接口。
 * 与通用资源接口并列，独立挂在 /api/sample-trace 前缀下。
 */
import { request } from './client'

const BASE = '/api/sample-trace'

export type StratumCount = {
  stratum_key: string
  population: number
  sampled: number
}

export type SampleFeatures = {
  count: number
  mean?: number
  stddev?: number
  min?: number
  max?: number
  p50?: number
  p95?: number
}

export type BatchSummary = {
  batch_id: string
  slot_ref: string
  file_boundary: string
  file_name: string
  file_checksum: string
  sampler_name: string
  strategy_version: string
  strategy_snapshot: Record<string, string>
  group_key: string
  source_event_count: number
  sample_count: number
  strata: StratumCount[]
  sample_features: SampleFeatures
  status: string
  superseded_by: string | null
  reviewer: string
  source_page: string
  origin: string
  source_fingerprint: string | null
  created_at: string
  samples?: Record<string, unknown>[]
  reports?: DebugReport[]
}

export type IntermediateRow = {
  sample_id: string
  event_id: string
  stratum_key: string
  target_operator: string
  expected: number
  actual: number
  diff: number
  anomaly_mark: string
}

export type AnomalyMark = {
  sample_id: string
  event_id: string
  mark: string
  detail: string
}

export type DebugReport = {
  report_id: string
  slot_ref: string
  batch_id: string
  strategy_version: string
  sample_version: string
  session_id: string
  target_operator: string
  is_current: boolean
  status: string
  anomaly_marks: AnomalyMark[]
  anomaly_count: number
  intermediate_results: IntermediateRow[]
  sample_count: number
  sample_checksum: string
  created_at: string
  completed_at: string | null
}

export type SessionView = {
  session_id: string
  slot_ref: string
  session_name: string
  target_operator: string
  batch_id: string
  strategy_version: string
  sample_ids: string[]
  sample_count: number
  complete_sample_bound: boolean
  status: string
  report_id: string | null
  history_report_ids: string[]
  created_at: string
  samples?: Record<string, unknown>[]
  report?: DebugReport | null
}

export type LedgerRow = {
  ledger_id: string
  slot_ref: string
  direction: string
  file_name: string
  file_checksum: string
  source_fingerprint: string | null
  file_boundary: string
  batch_id: string
  source_page: string
  status: string
  idempotent: boolean
  message: string
  created_at: string
}

export type ImportResult = {
  ok: boolean
  idempotent?: boolean
  rolled_back?: boolean
  message: string
  batch_id?: string
  strategy_version?: string
  file_boundary?: string
  file_checksum?: string
  sample_count?: number
  strata?: StratumCount[]
  ledger_id?: string
  original_file?: Record<string, unknown>
}

export type VerifyResult = {
  ok: boolean
  consistent: boolean
  batch_id?: string
  checks: { item: string; passed: boolean; expected?: unknown; actual?: unknown }[]
}

async function postJson<T>(path: string, body: Record<string, unknown> = {}): Promise<T> {
  const response = await request(`${BASE}${path}`, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  if (!response.ok) throw new Error(`接口返回 ${response.status}，操作未生效`)
  return (await response.json()) as T
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(`${BASE}${path}`)
  if (!response.ok) throw new Error(`接口返回 ${response.status}，数据读取失败`)
  return (await response.json()) as T
}

export function importSampleTrace(
  file: Record<string, unknown>,
  reviewer?: string,
  sourcePage?: string,
): Promise<ImportResult> {
  return postJson('/imports', { file, reviewer, source_page: sourcePage })
}

export async function listBatches(): Promise<BatchSummary[]> {
  const payload = await getJson<{ items: BatchSummary[]; total: number }>('/batches')
  return payload.items
}

export function getBatch(batchId: string): Promise<BatchSummary> {
  return getJson(`/batches/${encodeURIComponent(batchId)}`)
}

export function startDebugSession(
  batchId: string,
  targetOperator: string,
  sessionName?: string,
): Promise<Record<string, unknown>> {
  return postJson(`/batches/${encodeURIComponent(batchId)}/debug-sessions`, {
    target_operator: targetOperator,
    session_name: sessionName,
  })
}

export async function listSessions(): Promise<SessionView[]> {
  const payload = await getJson<{ items: SessionView[]; total: number }>('/debug-sessions')
  return payload.items
}

export function getSession(sessionId: string): Promise<SessionView> {
  return getJson(`/debug-sessions/${encodeURIComponent(sessionId)}`)
}

export function completeSession(sessionId: string): Promise<Record<string, unknown>> {
  return postJson(`/debug-sessions/${encodeURIComponent(sessionId)}/complete`)
}

export function resampleBatch(
  batchId: string,
  targetOperator: string,
  newRatio?: number,
  newLimit?: number,
): Promise<Record<string, unknown>> {
  return postJson(`/batches/${encodeURIComponent(batchId)}/resample`, {
    target_operator: targetOperator,
    new_ratio: newRatio,
    new_limit: newLimit,
  })
}

export function faultReopen(
  batchId: string,
  targetOperator: string,
): Promise<Record<string, unknown>> {
  return postJson(`/batches/${encodeURIComponent(batchId)}/fault-reopen`, {
    target_operator: targetOperator,
  })
}

export function getCurrentReport(batchId: string): Promise<DebugReport | { ok: false; message: string }> {
  return getJson(`/batches/${encodeURIComponent(batchId)}/reports/current`)
}

export function getReport(reportId: string): Promise<DebugReport> {
  return getJson(`/reports/${encodeURIComponent(reportId)}`)
}

export function verifyExportFile(file: Record<string, unknown>): Promise<VerifyResult> {
  return postJson('/exports/verify', { file })
}

export type HandlingComparison = {
  ok: boolean
  source_fingerprint: string
  already_imported: boolean
  recommendation: string
  options: {
    mode: string
    label: string
    side_effect: boolean
    idempotent: boolean
    consistent?: boolean
    would_create_batch?: boolean
    tradeoff: string
  }[]
}

export function compareExportHandling(
  file: Record<string, unknown>,
): Promise<HandlingComparison> {
  return postJson('/exports/compare-handling', { file })
}

export async function listLedger(): Promise<LedgerRow[]> {
  const payload = await getJson<{ items: LedgerRow[]; total: number }>('/ledger')
  return payload.items
}

/** 调试追踪详情页文件下载地址。 */
export function exportBatchUrl(batchId: string): string {
  const apiBase = import.meta.env.VITE_API_BASE ?? ''
  return `${apiBase}${BASE}/batches/${encodeURIComponent(batchId)}/export`
}
