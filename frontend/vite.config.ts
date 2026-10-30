import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// BFF 代理：/api → 本地 Web API（AIOPS_WEB_API_PORT，默认 9093）
export default defineConfig({
  plugins: [react()],
  server: {
    port: 3080,
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:9093',
        changeOrigin: true,
      },
    },
  },
})
