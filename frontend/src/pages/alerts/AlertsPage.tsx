import { useState } from 'react'
import { useAlerts, useTrace } from '../../data/hooks'
import { StatusBadge } from '../../components/StatusBadge'
import { TraceWindow } from '../../components/TraceWindow'

const STATES = ['', 'running', 'suspended', 'completed', 'failed']

export function AlertsPage() {
  const [state, setState] = useState('')
  const [selected, setSelected] = useState<string | null>(null)
  const { data, isLoading, isError } = useAlerts(state || undefined)
  const { data: trace, isLoading: traceLoading } = useTrace(selected)

  return (
    <div>
      <h2>告警流水线</h2>
      <div className="toolbar">
        <label>状态：</label>
        <select value={state} onChange={(e) => setState(e.target.value)}>
          {STATES.map((s) => <option key={s} value={s}>{s || '全部'}</option>)}
        </select>
        {data && <span className="muted">共 {data.total} 条</span>}
      </div>

      <div className="grid split">
        <div>
          {isLoading && <div className="muted">加载中…</div>}
          {isError && <div className="error-text">告警列表加载失败</div>}
          {data && data.items.length === 0 && <div className="muted">暂无告警</div>}
          {data && data.items.length > 0 && (
            <table className="table">
              <thead><tr><th>告警 ID</th><th>状态</th><th>更新时间</th></tr></thead>
              <tbody>
                {data.items.map((a) => (
                  <tr key={a.alert_id} className={selected === a.alert_id ? 'selected' : ''}
                      onClick={() => setSelected(a.alert_id)}>
                    <td className="mono">{a.alert_id}</td>
                    <td><StatusBadge value={a.state} /></td>
                    <td className="mono">{a.updated_at ? new Date(a.updated_at * 1000).toLocaleString() : '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        <div>
          {!selected && <div className="muted">点击左侧告警查看全链路 Trace</div>}
          {selected && traceLoading && <div className="muted">Trace 加载中…</div>}
          {selected && trace && <TraceWindow trace={trace} />}
        </div>
      </div>
    </div>
  )
}
