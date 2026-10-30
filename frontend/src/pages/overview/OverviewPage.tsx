import { useOverview } from '../../data/hooks'
import { StatCard, BarDistribution } from '../../components/StatCard'
import { StatusBadge } from '../../components/StatusBadge'

const DEGRADATION_LABEL: Record<number, string> = { 0: 'NORMAL', 1: 'L1', 2: 'L2' }

export function OverviewPage() {
  const { data, isLoading, isError } = useOverview()
  if (isLoading) return <div className="muted">加载中…</div>
  if (isError || !data) return <div className="error-text">概览加载失败</div>

  const acc = data.diagnosis_accuracy
  return (
    <div>
      <h2>概览</h2>
      <div className="grid cards">
        <StatCard label="诊断准确率" tone={acc.rate !== null && acc.rate >= 0.8 ? 'ok' : 'warn'}
          value={acc.rate === null ? '-' : `${(acc.rate * 100).toFixed(1)}%`}
          hint={`correct ${acc.correct} / incorrect ${acc.incorrect}`} />
        <StatCard label="评估通过 / 拦截" tone="default"
          value={`${data.evaluation.passed} / ${Object.values(data.evaluation.blocked).reduce((a, b) => a + b, 0)}`}
          hint={Object.entries(data.evaluation.blocked).map(([k, v]) => `${k}:${v}`).join(' ') || '无拦截'} />
        <StatCard label="LLM 调用成功率" tone={data.llm.success_rate === null ? 'default' : data.llm.success_rate >= 0.95 ? 'ok' : 'warn'}
          value={data.llm.success_rate === null ? '-' : `${(data.llm.success_rate * 100).toFixed(1)}%`}
          hint={`avg ${data.llm.avg_latency_ms ?? '-'}ms · parse失败 ${data.llm.parse_failure}`} />
        <StatCard label="Token 用量" tone="default"
          value={`${data.llm.tokens.input} / ${data.llm.tokens.output}`}
          hint={`cache ${data.llm.tokens.cache_read} · reasoning ${data.llm.tokens.reasoning}`} />
        <StatCard label="降级档" tone={data.degradation_level === 0 ? 'ok' : data.degradation_level ? 'err' : 'default'}
          value={data.degradation_level === null ? '-' : DEGRADATION_LABEL[data.degradation_level] ?? data.degradation_level} />
        <StatCard label="审批挂起" tone={(data.approval_pending ?? 0) > 0 ? 'warn' : 'ok'} value={data.approval_pending ?? '-'} />
        <StatCard label="Worker 积压" tone={(data.worker_backlog ?? 0) > 100 ? 'warn' : 'ok'} value={data.worker_backlog ?? '-'} />
        <StatCard label="Kafka Lag" tone={(data.kafka_lag ?? 0) > 1000 ? 'warn' : 'ok'} value={data.kafka_lag ?? '-'} />
      </div>

      <div className="grid two">
        <section>
          <h3>告警状态分布</h3>
          <BarDistribution data={data.alerts_state} />
        </section>
        <section>
          <h3>熔断器</h3>
          {data.circuit ? (
            <div className="timeline-row">
              <StatusBadge value={data.circuit.state} />
              <span>失败率（60s 窗口）: {(data.circuit.timeout_rate * 100).toFixed(1)}%</span>
            </div>
          ) : (
            <div className="muted">进程内模式未接入（独立部署时经 pipeline 注入）</div>
          )}
        </section>
      </div>
    </div>
  )
}
