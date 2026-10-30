// 与后端 BFF（interfaces/web_api.py）契约对齐的 TS 类型（契约先行，手工对齐 Pydantic 读模型）

export interface Me {
  principal_id: string
  name: string
  roles: string[]
  plaintext: boolean
}

export interface DiagnosisAccuracy {
  correct: number
  incorrect: number
  rate: number | null
}

export interface LlmSummary {
  requested: number
  success_rate: number | null
  parse_failure: number
  avg_latency_ms: number | null
  tokens: { input: number; output: number; cache_read: number; reasoning: number }
}

export type CircuitInfo = {
  state: string
  timeout_rate: number
} | null

export interface Overview {
  alerts_state: Record<string, number>
  diagnosis_accuracy: DiagnosisAccuracy
  evaluation: { passed: number; blocked: Record<string, number> }
  llm: LlmSummary
  degradation_level: number | null
  worker_backlog: number | null
  kafka_lag: number | null
  approval_pending: number | null
  circuit: CircuitInfo
}

export interface SystemInfo {
  circuit: CircuitInfo
  degradation_level: number | null
  kafka_lag: number | null
  worker_backlog: number | null
  stale_runs: { run_id: string; alert_id: string; status: string; started_at: string; updated_at: string }[]
}

export interface AlertRow {
  alert_id: string
  state: string
  updated_at: number
  [key: string]: unknown
}

export interface AlertsPageData {
  total: number
  page: number
  limit: number
  items: AlertRow[]
}

export interface LlmUsage {
  input_tokens: number
  output_tokens: number
  cached_read_tokens: number
  reasoning_tokens: number
}

export interface LlmCallEvent {
  call_id: string
  timestamp: string
  stage: string | null
  role: string | null
  provider: string
  model: string
  prompt_chars: number
  prompt_hash: string
  prompt?: string | null
  response_chars: number
  response_hash: string
  response?: string | null
  usage: LlmUsage | null
  latency_ms: number
  outcome: string
  failure_type: string | null
  error_summary: string | null
  alert_id: string | null
}

export interface LlmCallsPageData {
  items: LlmCallEvent[]
  next_cursor: number | null
  plaintext: boolean
}

export interface ApprovalRow {
  approval_id: string
  pipeline_run_id: string
  alert_id: string
  status: string
  decider: string | null
  decide_reason: string | null
  created_at: string | null
  timeout_at: string | null
  eval_summary: Record<string, unknown>
  proposal: Record<string, unknown> | null
}

export interface ApprovalsPageData {
  items: ApprovalRow[]
  plaintext: boolean
}

export interface TraceData {
  alert_id: string
  run: { run_id: string; status: string; started_at: string } | null
  node_timeline: { node: string; phase: string; timestamp: string | null; outcome: string }[]
  llm_calls: LlmCallEvent[]
  approvals: ApprovalRow[]
  audit_events: { event_id: string; event_type: string; timestamp: string | null; severity: string; outcome: string }[]
  plaintext: boolean
}
