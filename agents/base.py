from abc import ABC, abstractmethod
from typing import Any, Dict


class BaseAgent(ABC):
    """所有 Pipeline Agent 的基类。

    每个 Agent：
    - 通过构造函数注入依赖（不经由 state["pipeline"]）
    - 构建自己的 LangGraph StateGraph
    - 对外暴露 Python 方法，内部运行子图
    """

    @abstractmethod
    def name(self) -> str:
        """Agent 标识符。"""
        ...

    @abstractmethod
    def build_graph(self):
        """构建并返回编译后的 LangGraph StateGraph。"""
        ...

    def _invoke(self, initial_state: Dict[str, Any]) -> Dict[str, Any]:
        """运行 Agent 的子图。"""
        graph = self.build_graph()
        return graph.invoke(initial_state)
