from __future__ import annotations

from typing import Any, Literal
from pydantic import BaseModel, Field


NodeType = Literal[
    "trigger",
    "set",
    "transform",
    "validate",
    "condition",
    "http",
    "delay",
    "notify",
    "json_store",
]


class WorkflowNode(BaseModel):
    id: str = Field(min_length=1, max_length=80)
    type: NodeType
    label: str = Field(min_length=1, max_length=120)
    config: dict[str, Any] = Field(default_factory=dict)
    retries: int = Field(default=0, ge=0, le=5)
    retry_delay_seconds: float = Field(default=0.5, ge=0, le=30)


class WorkflowEdge(BaseModel):
    source: str
    target: str
    when: bool | None = None


class WorkflowDefinition(BaseModel):
    nodes: list[WorkflowNode]
    edges: list[WorkflowEdge]

    def validate_graph(self) -> None:
        ids = [node.id for node in self.nodes]
        if len(ids) != len(set(ids)):
            raise ValueError("Node IDs must be unique")
        known = set(ids)
        for edge in self.edges:
            if edge.source not in known or edge.target not in known:
                raise ValueError(f"Unknown node in edge: {edge.source}->{edge.target}")
        indegree = {node_id: 0 for node_id in ids}
        adjacency: dict[str, list[str]] = {node_id: [] for node_id in ids}
        for edge in self.edges:
            indegree[edge.target] += 1
            adjacency[edge.source].append(edge.target)
        queue = [node_id for node_id, degree in indegree.items() if degree == 0]
        visited = 0
        while queue:
            current = queue.pop()
            visited += 1
            for target in adjacency[current]:
                indegree[target] -= 1
                if indegree[target] == 0:
                    queue.append(target)
        if visited != len(ids):
            raise ValueError("Workflow graph contains a cycle")


def check_trigger(trigger_type: str, webhook_slug: str | None, schedule_seconds: int | None) -> None:
    if trigger_type == "webhook" and not webhook_slug:
        raise ValueError("A webhook workflow needs webhook_slug")
    if trigger_type == "schedule" and not schedule_seconds:
        raise ValueError("A scheduled workflow needs schedule_seconds")


class WorkflowCreate(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    description: str = Field(default="", max_length=500)
    trigger_type: Literal["manual", "webhook", "schedule"] = "manual"
    webhook_slug: str | None = Field(default=None, max_length=80)
    schedule_seconds: int | None = Field(default=None, ge=10, le=86400)
    is_enabled: bool = True
    definition: WorkflowDefinition


class RunRequest(BaseModel):
    payload: dict[str, Any] = Field(default_factory=dict)


class SecretCreate(BaseModel):
    name: str = Field(pattern=r"^[A-Z0-9_\-]{2,80}$")
    value: str = Field(min_length=1, max_length=5000)


class WorkflowPatch(BaseModel):
    name: str | None = Field(default=None, min_length=2, max_length=120)
    description: str | None = Field(default=None, max_length=500)
    trigger_type: Literal["manual", "webhook", "schedule"] | None = None
    webhook_slug: str | None = Field(default=None, max_length=80)
    schedule_seconds: int | None = Field(default=None, ge=10, le=86400)
    is_enabled: bool | None = None
    definition: WorkflowDefinition | None = None
