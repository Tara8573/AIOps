// 页面注册表（规格 §3：插件式自注册 {route, nav, component, requiredRoles}；新增页面不动框架）
import type { ComponentType } from 'react'
import { OverviewPage } from './overview/OverviewPage'
import { AlertsPage } from './alerts/AlertsPage'
import { LlmCallsPage } from './llm-calls/LlmCallsPage'
import { ApprovalsPage } from './approvals/ApprovalsPage'

export interface PageModule {
  route: string
  nav: string
  component: ComponentType<PageProps>
  requiredRoles?: string[] // undefined = 所有已认证角色可见
}

export interface PageProps {
  roles: string[]
  plaintext: boolean
}

export const PAGES: PageModule[] = [
  { route: '/', nav: '概览', component: OverviewPage },
  { route: '/alerts', nav: '告警流水线', component: AlertsPage },
  { route: '/llm-calls', nav: 'LLM 调用观测', component: LlmCallsPage },
  { route: '/approvals', nav: '审批中心', component: ApprovalsPage },
]

export function roleVisible(required: string[] | undefined, roles: string[]): boolean {
  if (!required || required.length === 0) return true
  return roles.some((r) => required.includes(r))
}
