import { useState } from 'react'
import { useLlmCalls } from '../../data/hooks'
import { CallEventTable } from '../../components/CallEventTable'

const STAGES = ['', 'analyze', 'retry', 'judge', 'improve', 'feedback']
const OUTCOMES = ['', 'success', 'error']
const PROVIDERS = ['', 'openai', 'ollama']

export function LlmCallsPage({ plaintext }: { plaintext: boolean }) {
  const [alertId, setAlertId] = useState('')
  const [stage, setStage] = useState('')
  const [provider, setProvider] = useState('')
  const [outcome, setOutcome] = useState('')
  const [cursor, setCursor] = useState(0)

  const { data, isLoading, isError } = useLlmCalls({
    alert_id: alertId || undefined,
    stage: stage || undefined,
    provider: provider || undefined,
    outcome: outcome || undefined,
    cursor: cursor || undefined,
  })

  const filterKey = `${alertId}|${stage}|${provider}|${outcome}`
  const [lastFilter, setLastFilter] = useState(filterKey)
  if (filterKey !== lastFilter) {
    setLastFilter(filterKey)
    setCursor(0) // 筛选变化时重置分页
  }

  return (
    <div>
      <h2>LLM 调用观测</h2>
      <div className="toolbar">
        <input placeholder="告警 ID" value={alertId} onChange={(e) => setAlertId(e.target.value)} />
        <select value={stage} onChange={(e) => setStage(e.target.value)}>
          {STAGES.map((s) => <option key={s} value={s}>{s || 'stage 全部'}</option>)}
        </select>
        <select value={provider} onChange={(e) => setProvider(e.target.value)}>
          {PROVIDERS.map((p) => <option key={p} value={p}>{p || 'provider 全部'}</option>)}
        </select>
        <select value={outcome} onChange={(e) => setOutcome(e.target.value)}>
          {OUTCOMES.map((o) => <option key={o} value={o}>{o || 'outcome 全部'}</option>)}
        </select>
        {data && !plaintext && <span className="badge neutral">无明文权限（仅 hash/长度）</span>}
      </div>

      {isLoading && <div className="muted">加载中…</div>}
      {isError && <div className="error-text">调用日志加载失败</div>}
      {data && <CallEventTable events={data.items} />}

      <div className="toolbar">
        <button disabled={!cursor} onClick={() => setCursor(Math.max(0, cursor - 50))}>上一页</button>
        <button disabled={!data?.next_cursor} onClick={() => data?.next_cursor !== null && setCursor(data!.next_cursor!)}>
          下一页（更新）
        </button>
        {cursor > 0 && <span className="muted">cursor={cursor}</span>}
      </div>
    </div>
  )
}
