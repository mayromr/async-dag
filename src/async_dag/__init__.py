"""
A simple library for running complex DAG of async tasks efficiently.
Take a look at `TaskManager` to get started.
"""

from .execution_result import ExecutionResult
from .task_manager import TaskManager, build_dag
from .task_node import TaskNode

__all__ = ["ExecutionResult", "TaskManager", "TaskNode", "build_dag"]
