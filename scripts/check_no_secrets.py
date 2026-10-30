"""凭据明文扫描（FR-8 验收：凭据不出现在配置文件/环境变量明文）。

扫描 config/*.yaml、.env、.env.example 中的硬编码凭据（排除引用形态与占位符）。
用法:
    python scripts/check_no_secrets.py          # 扫描，发现明文凭据 exit 1
"""

import os
import re
import sys
from pathlib import Path

try:  # GBK 控制台兼容（✓/✗ 输出）
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent

# 凭据字段（出现即检查值形态）
_SECRET_FIELDS = re.compile(
    r"^\s*(?:-\s+)?(?:api_key|api_token|password|token|sasl_password|jwt_secret|secret)\s*[:=]\s*(.+)$",
    re.IGNORECASE,
)

# 合法形态：空/引用/占位符/环境变量引用
_SAFE_VALUES = re.compile(
    r"^(?:\s*$|\"\"|''|\$\{[^}]*\}|env://\S+|vault://\S+|<[^>]*>|your-|\{\{.*\}\}|.*example.*|.*placeholder.*|test-key.*|sk-xxx.*)$",
    re.IGNORECASE,
)


def scan_file(path: Path) -> list:
    findings = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return findings
    for i, line in enumerate(lines, 1):
        if line.strip().startswith("#"):
            continue
        m = _SECRET_FIELDS.match(line)
        if not m:
            continue
        value = m.group(1).strip()
        if _SAFE_VALUES.match(value):
            continue
        findings.append((str(path), i, line.strip()[:80]))
    return findings


def main():
    targets = []
    for pattern in ("config/*.yaml", ".env", ".env.example"):
        targets.extend(ROOT.glob(pattern))

    all_findings = []
    for path in targets:
        all_findings.extend(scan_file(path))

    if all_findings:
        print(f"[NoSecrets] ✗ 发现 {len(all_findings)} 处疑似明文凭据（FR-8 验收失败）:")
        for path, line, text in all_findings:
            print(f"  {path}:{line}: {text}")
        print("\n请改为 secretRef 引用（vault://path#key 或 env://VAR），见 FR-8 凭据集中管理。")
        sys.exit(1)

    print(f"[NoSecrets] ✓ 未发现明文凭据（扫描 {len(targets)} 个文件）")


if __name__ == "__main__":
    main()
