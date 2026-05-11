#!/usr/bin/env python3
"""检查 AIOps 环境变量配置状态"""

import os
from dotenv import load_dotenv

# 加载 .env 文件
load_dotenv()


def check_env(var_name, required=True):
    """检查环境变量"""
    value = os.getenv(var_name)
    if value:
        # 隐藏敏感信息
        if "KEY" in var_name or "PASSWORD" in var_name or "DSN" in var_name:
            display = value[:8] + "****" if len(value) > 8 else "****"
        else:
            display = value
        return True, display
    return False, None


def main():
    print("=" * 50)
    print("AIOps 环境变量配置检查")
    print("=" * 50)

    configs = [
        # (变量名, 是否必需, 说明)
        ("OPENAI_API_KEY", False, "OpenAI API 密钥"),
        ("OPENAI_BASE_URL", False, "OpenAI API 地址"),
        ("OLLAMA_BASE_URL", False, "Ollama 服务地址"),
        ("AIOPS_PG_DSN", False, "PostgreSQL 连接串"),
        ("AIOPS_USE_PG_KB", False, "是否使用 PG 知识库"),
        ("AIOPS_LOG_LEVEL", False, "日志级别"),
        ("AIOPS_EMBEDDING_PROVIDER", False, "Embedding 提供商"),
    ]

    results = []
    for var_name, required, desc in configs:
        ok, value = check_env(var_name, required)
        status = "✓" if ok else "✗"
        results.append((status, var_name, value or "未设置", desc))

    # 打印结果
    for status, var, value, desc in results:
        print(f"  [{status}] {desc}")
        print(f"       {var} = {value}")

    print("=" * 50)

    # 测试 LLM 连接
    print("\nLLM 服务可用性测试:")

    # 测试 OpenAI
    try:
        import httpx

        api_key = os.getenv("OPENAI_API_KEY")
        if api_key and api_key != "your_openai_api_key_here":
            resp = httpx.get(
                os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1") + "/models",
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=5,
            )
            print(f"  [✓] OpenAI API 连接成功")
        else:
            print(f"  [✗] OpenAI API 密钥未配置")
    except Exception as e:
        print(f"  [✗] OpenAI API 连接失败: {e}")

    # 测试 Ollama
    try:
        base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
        resp = httpx.get(f"{base_url}/api/tags", timeout=3)
        if resp.status_code == 200:
            models = [m["name"] for m in resp.json().get("models", [])]
            print(f"  [✓] Ollama 连接成功，可用模型: {', '.join(models[:3])}")
        else:
            print(f"  [✗] Ollama 连接失败")
    except Exception as e:
        print(f"  [✗] Ollama 连接失败: {e}")

    print("\n" + "=" * 50)


if __name__ == "__main__":
    main()
