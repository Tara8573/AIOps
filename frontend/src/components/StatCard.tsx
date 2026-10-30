interface StatCardProps {
  label: string
  value: string | number | null | undefined
  hint?: string
  tone?: 'default' | 'ok' | 'warn' | 'err'
}

export function StatCard({ label, value, hint, tone = 'default' }: StatCardProps) {
  return (
    <div className={`card stat ${tone}`}>
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value ?? '-'}</div>
      {hint !== undefined && <div className="stat-hint">{hint}</div>}
    </div>
  )
}

/** 简单水平条形分布（P0 不引入图表库） */
export function BarDistribution({ data }: { data: Record<string, number> }) {
  const entries = Object.entries(data)
  const max = Math.max(1, ...entries.map(([, v]) => v))
  if (entries.length === 0) return <div className="muted">暂无数据</div>
  return (
    <div className="bars">
      {entries.map(([k, v]) => (
        <div key={k} className="bar-row">
          <span className="bar-label">{k}</span>
          <div className="bar-track">
            <div className="bar-fill" style={{ width: `${(v / max) * 100}%` }} />
          </div>
          <span className="bar-value">{v}</span>
        </div>
      ))}
    </div>
  )
}
