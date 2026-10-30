import type { LlmCallEvent } from '../api/contract'
import { StatusBadge } from './StatusBadge'
import { PlainTextGate } from './PlainTextGate'

interface CallEventTableProps {
  events: LlmCallEvent[]
  onAlertClick?: (alertId: string | null) => void
  expandPlaintext?: boolean // TraceWindow 内默认可展开
}

/** LLM 调用事件表（规格 ③：时间/告警/stage/role/model/latency/tokens/outcome/failure_type） */
export function CallEventTable({ events, onAlertClick, expandPlaintext = false }: CallEventTableProps) {
  if (events.length === 0) return <div className="muted">暂无调用事件</div>
  return (
    <table className="table">
      <thead>
        <tr>
          <th>时间</th><th>告警</th><th>stage</th><th>role</th><th>provider/model</th>
          <th>耗时</th><th>tokens(in/out)</th><th>结果</th><th>失败类型</th>
        </tr>
      </thead>
      <tbody>
        {events.map((e) => (
          <tr key={e.call_id}>
            <td className="mono">{e.timestamp?.replace('T', ' ')}</td>
            <td>
              {e.alert_id ? (
                <button className="link" onClick={() => onAlertClick?.(e.alert_id)}>{e.alert_id}</button>
              ) : '-'}
            </td>
            <td>{e.stage ?? '-'}</td>
            <td>{e.role ?? '-'}</td>
            <td className="mono">{e.provider}/{e.model}</td>
            <td>{e.latency_ms}ms</td>
            <td>{e.usage ? `${e.usage.input_tokens}/${e.usage.output_tokens}` : '-'}</td>
            <td><StatusBadge value={e.outcome} /></td>
            <td>{e.failure_type ?? '-'}</td>
          </tr>
        ))}
      </tbody>
      {expandPlaintext && (
        <tbody>
          {events.map((e) => (
            <tr key={`${e.call_id}-detail`} className="detail-row">
              <td colSpan={9}>
                <div className="detail-grid">
                  <div>
                    <div className="detail-label">prompt</div>
                    <PlainTextGate event={e} field="prompt" hashField={e.prompt_hash} charsField={e.prompt_chars} />
                  </div>
                  <div>
                    <div className="detail-label">response</div>
                    <PlainTextGate event={e} field="response" hashField={e.response_hash} charsField={e.response_chars} />
                  </div>
                </div>
                {e.error_summary && <div className="error-text">错误：{e.error_summary}</div>}
              </td>
            </tr>
          ))}
        </tbody>
      )}
    </table>
  )
}
