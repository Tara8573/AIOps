import { useState } from 'react'
import { useApprovals, useDecideApproval } from '../../data/hooks'
import { StatusBadge } from '../../components/StatusBadge'

const TABS = ['pending', 'approved', 'rejected', 'resumed', 'expired']

/** 角色判断（规格 §8：approval.decide → operator/admin；RBAC 关闭时后端全放行） */
function canDecide(roles: string[]): boolean {
  return roles.includes('admin') || roles.includes('operator')
}

export function ApprovalsPage({ roles }: { roles: string[] }) {
  const [tab, setTab] = useState('pending')
  const [reason, setReason] = useState('')
  const [target, setTarget] = useState<string | null>(null)
  const { data, isLoading, isError } = useApprovals(tab)
  const decide = useDecideApproval()
  const decidable = canDecide(roles)

  const submit = (approvalId: string, decision: 'approved' | 'rejected') => {
    decide.mutate({ approvalId, decision, reason, decider: undefined })
    setTarget(null)
    setReason('')
  }

  return (
    <div>
      <h2>审批中心</h2>
      <div className="toolbar">
        {TABS.map((t) => (
          <button key={t} className={tab === t ? 'active' : ''} onClick={() => setTab(t)}>{t}</button>
        ))}
        {!decidable && <span className="badge neutral">只读（无 approval.decide 权限）</span>}
      </div>

      {isLoading && <div className="muted">加载中…</div>}
      {isError && <div className="error-text">审批列表加载失败</div>}
      {data && data.items.length === 0 && <div className="muted">暂无 {tab} 记录</div>}

      {data && data.items.map((a) => (
        <div key={a.approval_id} className="card approval">
          <div className="approval-header">
            <span className="mono strong">{a.approval_id}</span>
            <span className="mono muted">{a.alert_id}</span>
            <StatusBadge value={a.status} />
            {a.timeout_at && tab === 'pending' && (
              <span className="muted">超时: {new Date(a.timeout_at).toLocaleString()}</span>
            )}
          </div>

          {a.eval_summary && Object.keys(a.eval_summary).length > 0 && (
            <div className="muted">
              评估: {JSON.stringify(a.eval_summary)}
            </div>
          )}

          {a.proposal && (
            <details>
              <summary>诊断方案预览</summary>
              <pre className="plaintext">{JSON.stringify(a.proposal, null, 2)}</pre>
            </details>
          )}

          {a.decider && (
            <div className="muted">决策人: {a.decider} · “{a.decide_reason}”</div>
          )}

          {tab === 'pending' && decidable && (
            <div className="toolbar">
              {target !== a.approval_id ? (
                <>
                  <button className="ok" onClick={() => setTarget(a.approval_id)}>批准</button>
                  <button className="err" onClick={() => setTarget(a.approval_id)}>拒绝</button>
                </>
              ) : (
                <>
                  <input placeholder="决策原因（可选）" value={reason} onChange={(e) => setReason(e.target.value)} />
                  <button className="ok" disabled={decide.isPending}
                          onClick={() => submit(a.approval_id, 'approved')}>确认批准</button>
                  <button className="err" disabled={decide.isPending}
                          onClick={() => submit(a.approval_id, 'rejected')}>确认拒绝</button>
                  <button onClick={() => setTarget(null)}>取消</button>
                </>
              )}
            </div>
          )}
          {decide.isError && <div className="error-text">{decide.error.message}</div>}
        </div>
      ))}
    </div>
  )
}
