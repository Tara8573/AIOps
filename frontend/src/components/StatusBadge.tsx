interface StatusBadgeProps {
  value: string | null | undefined
  map?: Record<string, string> // value → css class
}

const DEFAULT_MAP: Record<string, string> = {
  // 告警/流水线状态
  running: 'ok', completed: 'ok', passed: 'ok', success: 'ok', approved: 'ok',
  resumed: 'ok', closed: 'ok', NORMAL: 'ok',
  suspended: 'warn', pending: 'warn', warn: 'warn', half_open: 'warn', L1: 'warn',
  failed: 'err', error: 'err', rejected: 'err', expired: 'err', open: 'err', L2: 'err',
}

export function StatusBadge({ value, map }: StatusBadgeProps) {
  if (value === null || value === undefined || value === '') return <span className="badge">-</span>
  const cls = (map ?? DEFAULT_MAP)[value] ?? 'neutral'
  return <span className={`badge ${cls}`}>{value}</span>
}
