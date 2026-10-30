import { useState } from 'react'
import type { LlmCallEvent } from '../api/contract'

/** 明文展示门（规格 §7 PlainTextGate：无明文权限时仅渲染 hash/长度 + 占位） */
export function PlainTextGate({
  event,
  field,
  hashField,
  charsField,
}: {
  event: LlmCallEvent
  field: 'prompt' | 'response'
  hashField: string
  charsField: number
}) {
  const [open, setOpen] = useState(false)
  const text = event[field]
  if (text === undefined) {
    return <span className="muted">无明文权限（{hashField.slice(0, 8)}… / {charsField} 字符）</span>
  }
  if (text === null) {
    return <span className="muted">未采集明文（{hashField.slice(0, 8)}… / {charsField} 字符）</span>
  }
  return (
    <div>
      <button className="link" onClick={() => setOpen(!open)}>
        {open ? '收起' : `展开明文（${text.length}/${charsField} 字符，已脱敏）`}
      </button>
      {open && <pre className="plaintext">{text}</pre>}
    </div>
  )
}
