import type { TraceData } from '../api/contract'
import { StatusBadge } from './StatusBadge'
import { CallEventTable } from './CallEventTable'

/** 单告警全链路串联窗口（规格 §7 Trace 窗口：节点时间线主轴 + 产物窗口） */
export function TraceWindow({ trace }: { trace: TraceData }) {
  return (
    <div className="trace">
      <div className="trace-header">
        <span className="mono strong">{trace.alert_id}</span>
        {trace.run && <>run: <span className="mono">{trace.run.run_id}</span> <StatusBadge value={trace.run.status} /></>}
        {!trace.plaintext && <span className="badge neutral">无明文权限</span>}
      </div>

      <section>
        <h4>流水线节点时间线</h4>
        {trace.node_timeline.length === 0
          ? <div className="muted">暂无审计事件（节点时间线由审计推导）</div>
          : (
            <div className="timeline">
              {trace.node_timeline.map((n, i) => (
                <div key={i} className="timeline-row">
                  <StatusBadge value={n.phase === 'start' ? 'warn' : n.outcome} />
                  <span className="mono">{n.node}.{n.phase}</span>
                  <span className="muted mono">{n.timestamp?.replace('T', ' ')}</span>
                </div>
              ))}
            </div>
          )}
      </section>

      <section>
        <h4>LLM 调用链（{trace.llm_calls.length}）</h4>
        <CallEventTable events={trace.llm_calls} expandPlaintext />
      </section>

      <section>
        <h4>审批记录（{trace.approvals.length}）</h4>
        {trace.approvals.length === 0
          ? <div className="muted">无审批</div>
          : trace.approvals.map((a) => (
            <div key={a.approval_id} className="timeline-row">
              <span className="mono">{a.approval_id}</span>
              <StatusBadge value={a.status} />
              {a.decider && <span>by {a.decider}</span>}
              {a.decide_reason && <span className="muted">“{a.decide_reason}”</span>}
            </div>
          ))}
      </section>

      <section>
        <h4>审计事件（{trace.audit_events.length}）</h4>
        <div className="timeline">
          {trace.audit_events.map((e) => (
            <div key={e.event_id} className="timeline-row">
              <span className="mono">{e.event_type}</span>
              <StatusBadge value={e.outcome} />
              <span className="muted mono">{e.timestamp?.replace('T', ' ')}</span>
            </div>
          ))}
        </div>
      </section>
    </div>
  )
}
