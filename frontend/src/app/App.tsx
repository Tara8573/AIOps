import { useState } from 'react'
import { BrowserRouter, NavLink, Navigate, Route, Routes } from 'react-router-dom'
import { QueryClient, QueryClientProvider, useQueryClient } from '@tanstack/react-query'
import { api, clearToken, setToken } from '../api/client'
import { useMe } from '../data/hooks'
import type { Me } from '../api/contract'
import { PAGES, roleVisible } from '../pages/registry'

const queryClient = new QueryClient({
  defaultOptions: { queries: { refetchOnWindowFocus: false } },
})

/** 登录页（规格 §8：仅 API Key 输入 + GET /me 角色探测）。
 *  匿名后端（AIOPS_WEB_API_AUTH=0）不会进入本页（/me 直接 200）。 */
function Login({ onOk }: { onOk: () => void }) {
  const qc = useQueryClient()
  const [key, setKey] = useState('')
  const [error, setError] = useState('')

  const login = async () => {
    setToken(key.trim())
    try {
      await api.get<Me>('/api/v1/me')
      await qc.invalidateQueries({ queryKey: ['me'] }) // 刷新缓存的 401 错误
      onOk()
    } catch {
      clearToken()
      setError('API Key 无效或服务不可达')
    }
  }

  return (
    <div className="login-wrap">
      <div className="card login">
        <h2>AIOps 平台登录</h2>
        <input
          type="password"
          placeholder="API Key"
          value={key}
          onChange={(e) => setKey(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && login()}
        />
        <button onClick={login}>登录</button>
        {error && <div className="error-text">{error}</div>}
      </div>
    </div>
  )
}

function Shell() {
  // /me 探测：200 → 布局（匿名后端免登录）；401/失败 → 登录页
  const { data: me, isLoading, isError } = useMe()

  if (isLoading) return <div className="login-wrap muted">认证中…</div>
  if (isError || !me) return <Login onOk={() => { /* invalidate 后 /me 重新成功，本分支自动退出 */ }} />

  const logout = () => {
    clearToken()
    queryClient.clear()
    window.location.reload()
  }

  return (
    <div className="layout">
      <aside className="sidebar">
        <div className="brand">AIOps 平台</div>
        <nav>
          {PAGES.filter((p) => roleVisible(p.requiredRoles, me.roles)).map((p) => (
            <NavLink key={p.route} to={p.route} end={p.route === '/'}>
              {p.nav}
            </NavLink>
          ))}
        </nav>
        <div className="sidebar-footer">
          <div className="muted">{me.name || me.principal_id}</div>
          <div className="muted">{me.roles.join(', ')}</div>
          <button className="link" onClick={logout}>退出</button>
        </div>
      </aside>
      <main className="content">
        <Routes>
          {PAGES.map((p) => (
            <Route
              key={p.route}
              path={p.route}
              element={<p.component roles={me.roles} plaintext={me.plaintext} />}
            />
          ))}
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </main>
    </div>
  )
}

export function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Shell />
      </BrowserRouter>
    </QueryClientProvider>
  )
}
