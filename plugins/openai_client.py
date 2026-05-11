import os
import json
import httpx
from typing import Optional
from interfaces.base import ILLMClient


class OpenAILLMClient(ILLMClient):
    """OpenAI API 客户端实现"""

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

        if not self.api_key:
            raise ValueError(
                "OpenAI API Key 未配置，请设置 OPENAI_API_KEY 环境变量或传入 api_key 参数"
            )

        self._client = httpx.Client(
            base_url=self.base_url,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            timeout=self.timeout,
        )

    def generate_proposal(self, prompt: str) -> str:
        """调用 OpenAI Chat Completions API 生成提案"""
        try:
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
            content = result["choices"][0]["message"]["content"]
            return content

        except httpx.HTTPStatusError as e:
            raise RuntimeError(
                f"OpenAI API 请求失败: {e.response.status_code} - {e.response.text}"
            )
        except httpx.RequestError as e:
            raise RuntimeError(f"OpenAI API 网络错误: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"OpenAI API 调用异常: {str(e)}")

    def close(self):
        """关闭 HTTP 客户端"""
        self._client.close()


class OllamaLLMClient(ILLMClient):
    """Ollama 本地模型客户端实现"""

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

        self._client = httpx.Client(base_url=self.base_url, timeout=self.timeout)

    def generate_proposal(self, prompt: str) -> str:
        """调用 Ollama API 生成提案"""
        try:
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
            return result.get("response", "")

        except httpx.HTTPStatusError as e:
            raise RuntimeError(
                f"Ollama API 请求失败: {e.response.status_code} - {e.response.text}"
            )
        except httpx.RequestError as e:
            raise RuntimeError(f"Ollama API 网络错误: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"Ollama API 调用异常: {str(e)}")

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
