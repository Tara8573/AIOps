import os
import json
import threading

import httpx
from typing import Optional
from core.observability import metrics
from interfaces.base import ILLMClient


class OpenAILLMClient(ILLMClient):
    """OpenAI API 客户端实现"""

    # LLMO-2.2: 观测层经 provider_name 识别 provider（只读能力，不改业务逻辑）
    provider_name = "openai"

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model: str = "gpt-4",
        temperature: float = 0.7,
        max_tokens: int = 2000,
        timeout: int = 60,
    ):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.base_url = base_url or os.getenv(
            "OPENAI_BASE_URL", "https://api.openai.com/v1"
        )
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.timeout = timeout
        # LLMO-2.2: 线程级最近调用 usage（并发池线程隔离，包装层同线程读取）
        self._tls = threading.local()

        if not self.api_key:
            raise ValueError(
                "OpenAI API Key 未配置，请设置 OPENAI_API_KEY 环境变量或传入 api_key 参数"
            )

        self._limits = httpx.Limits(
            max_connections=int(os.getenv("AIOPS_HTTP_MAX_CONNECTIONS", "20")),
            max_keepalive_connections=int(
                os.getenv("AIOPS_HTTP_MAX_KEEPALIVE_CONNECTIONS", "10")
            ),
        )
        self._client = httpx.Client(
            base_url=self.base_url,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            timeout=self.timeout,
            limits=self._limits,
        )

    def generate_proposal(self, prompt: str) -> str:
        """调用 OpenAI Chat Completions API 生成提案"""
        # LLMO-2.2: 线程级 usage 记录（不影响业务逻辑）
        self._tls.last_usage = None
        try:
            metrics.incr("llm.provider.openai.request")
            response = self._client.post(
                "/chat/completions",
                json={
                    "model": self.model,
                    "messages": [
                        {
                            "role": "system",
                            "content": "你是一个专业的 AIOps 运维专家，擅长分析告警并提供解决方案。请始终以 JSON 格式响应。",
                        },
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": self.temperature,
                    "max_tokens": self.max_tokens,
                    "response_format": {"type": "json_object"},
                },
            )
            response.raise_for_status()

            result = response.json()
            # LLMO-2.2: 保存原始 usage 供观测层读取（LLMO-2.2 disjoint 拆分在包装层完成）
            self._tls.last_usage = result.get("usage")
            content = result["choices"][0]["message"]["content"]
            metrics.incr("llm.provider.openai.success")
            return content

        except httpx.HTTPStatusError as e:
            metrics.incr("llm.provider.openai.http_error")
            raise RuntimeError(
                f"OpenAI API 请求失败: {e.response.status_code} - {e.response.text}"
            )
        except httpx.RequestError as e:
            metrics.incr("llm.provider.openai.network_error")
            raise RuntimeError(f"OpenAI API 网络错误: {str(e)}")
        except Exception as e:
            metrics.incr("llm.provider.openai.exception")
            raise RuntimeError(f"OpenAI API 调用异常: {str(e)}")

    def get_last_usage(self) -> Optional[dict]:
        """LLMO-2.2: 返回最近一次调用（本线程）的原始 usage dict；无则 None。"""
        return getattr(self._tls, "last_usage", None)

    def close(self):
        """关闭 HTTP 客户端"""
        self._client.close()


class OllamaLLMClient(ILLMClient):
    """Ollama 本地模型客户端实现"""

    # LLMO-2.2: 观测层经 provider_name 识别 provider（只读能力，不改业务逻辑）
    provider_name = "ollama"

    def __init__(
        self,
        base_url: Optional[str] = None,
        model: str = "qwen2.5:7b",
        temperature: float = 0.7,
        timeout: int = 120,
    ):
        self.base_url = base_url or os.getenv(
            "OLLAMA_BASE_URL", "http://localhost:11434"
        )
        self.model = model
        self.temperature = temperature
        self.timeout = timeout
        # LLMO-2.2: 线程级最近调用 usage（并发池线程隔离，包装层同线程读取）
        self._tls = threading.local()

        self._client = httpx.Client(
            base_url=self.base_url,
            timeout=self.timeout,
            limits=httpx.Limits(
                max_connections=int(os.getenv("AIOPS_HTTP_MAX_CONNECTIONS", "20")),
                max_keepalive_connections=int(
                    os.getenv("AIOPS_HTTP_MAX_KEEPALIVE_CONNECTIONS", "10")
                ),
            ),
        )

    def generate_proposal(self, prompt: str) -> str:
        """调用 Ollama API 生成提案"""
        # LLMO-2.2: 线程级 usage 记录（prompt_eval_count/eval_count 映射，规格 P2）
        self._tls.last_usage = None
        try:
            metrics.incr("llm.provider.ollama.request")
            response = self._client.post(
                "/api/generate",
                json={
                    "model": self.model,
                    "prompt": f"你是一个专业的 AIOps 运维专家。请始终以 JSON 格式响应。\n\n{prompt}",
                    "stream": False,
                    "options": {"temperature": self.temperature},
                },
            )
            response.raise_for_status()

            result = response.json()
            # LLMO-2.2: Ollama usage 映射（prompt_eval_count→input, eval_count→output，无缓存拆分）
            self._tls.last_usage = {
                "prompt_tokens": result.get("prompt_eval_count") or 0,
                "completion_tokens": result.get("eval_count") or 0,
            }
            metrics.incr("llm.provider.ollama.success")
            return result.get("response", "")

        except httpx.HTTPStatusError as e:
            metrics.incr("llm.provider.ollama.http_error")
            raise RuntimeError(
                f"Ollama API 请求失败: {e.response.status_code} - {e.response.text}"
            )
        except httpx.RequestError as e:
            metrics.incr("llm.provider.ollama.network_error")
            raise RuntimeError(f"Ollama API 网络错误: {str(e)}")
        except Exception as e:
            metrics.incr("llm.provider.ollama.exception")
            raise RuntimeError(f"Ollama API 调用异常: {str(e)}")

    def get_last_usage(self) -> Optional[dict]:
        """LLMO-2.2: 返回最近一次调用（本线程）的 usage dict（Ollama 映射）；无则 None。"""
        return getattr(self._tls, "last_usage", None)

    def close(self):
        """关闭 HTTP 客户端"""
        self._client.close()


class LLMClientFactory:
    """LLM 客户端工厂类"""

    @staticmethod
    def create(provider: str = "openai", **kwargs) -> ILLMClient:
        """
        创建 LLM 客户端实例

        Args:
            provider: 提供商类型，支持 "openai", "ollama"
            **kwargs: 传递给具体客户端的参数

        Returns:
            ILLMClient 实例
        """
        providers = {"openai": OpenAILLMClient, "ollama": OllamaLLMClient}

        if provider not in providers:
            raise ValueError(
                f"不支持的 LLM 提供商: {provider}，可选: {list(providers.keys())}"
            )

        return providers[provider](**kwargs)
