from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import re
import time
from pathlib import Path
from typing import Any

import httpx

from .crypto import SecretBox
from .models import WorkflowDefinition, WorkflowNode
from .repository import Repository


class WorkflowExecutionError(RuntimeError):
    pass


def stable_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def get_path(payload: dict[str, Any], path: str, default: Any = None) -> Any:
    if path in {"$", "$."}:
        return payload
    if not path.startswith("$."):
        return payload.get(path, default)
    current: Any = payload
    for part in path[2:].split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return default
    return current


def set_path(payload: dict[str, Any], path: str, value: Any) -> None:
    parts = path[2:].split(".") if path.startswith("$.") else path.split(".")
    current = payload
    for part in parts[:-1]:
        current = current.setdefault(part, {})
    current[parts[-1]] = value


class EventHub:
    def __init__(self) -> None:
        self._subscribers: dict[int, set[asyncio.Queue[dict[str, Any]]]] = {}

    async def publish(self, run_id: int, event: dict[str, Any]) -> None:
        for queue in list(self._subscribers.get(run_id, set())):
            await queue.put(event)

    def subscribe(self, run_id: int) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._subscribers.setdefault(run_id, set()).add(queue)
        return queue

    def unsubscribe(self, run_id: int, queue: asyncio.Queue[dict[str, Any]]) -> None:
        subscribers = self._subscribers.get(run_id)
        if not subscribers:
            return
        subscribers.discard(queue)
        if not subscribers:
            self._subscribers.pop(run_id, None)


class WorkflowEngine:
    def __init__(self, repository: Repository, secret_box: SecretBox, output_dir: Path) -> None:
        self.repo = repository
        self.secret_box = secret_box
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.events = EventHub()
        self._tasks: set[asyncio.Task[Any]] = set()

    def enqueue(self, run_id: int) -> None:
        task = asyncio.create_task(self.execute_run(run_id))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def execute_run(self, run_id: int) -> None:
        run = self.repo.get_run(run_id)
        if not run:
            return
        workflow = self.repo.get_workflow(run["workflow_id"])
        if not workflow:
            self.repo.set_run_status(run_id, "failed", error="Workflow not found")
            return
        definition = WorkflowDefinition.model_validate(workflow["definition"])
        self.repo.set_run_status(run_id, "running")
        await self.events.publish(run_id, {"type": "run.started", "run_id": run_id})
        try:
            output = await self._execute_definition(run_id, definition, run["input"] or {})
        except Exception as exc:
            message = str(exc)
            self.repo.set_run_status(run_id, "failed", error=message)
            self.repo.create_dead_letter(run_id, message, run["input"] or {})
            await self.events.publish(run_id, {"type": "run.failed", "run_id": run_id, "error": message})
            return
        self.repo.set_run_status(run_id, "succeeded", output=output)
        await self.events.publish(run_id, {"type": "run.succeeded", "run_id": run_id, "output": output})

    async def _execute_definition(self, run_id: int, definition: WorkflowDefinition, initial_payload: dict[str, Any]) -> dict[str, Any]:
        nodes = {node.id: node for node in definition.nodes}
        incoming: dict[str, list[Any]] = {node_id: [] for node_id in nodes}
        outgoing: dict[str, list[Any]] = {node_id: [] for node_id in nodes}
        for edge in definition.edges:
            incoming[edge.target].append(edge)
            outgoing[edge.source].append(edge)

        order = self._topological_order(nodes, definition)
        outputs: dict[str, dict[str, Any]] = {}
        statuses: dict[str, str] = {}

        for node_id in order:
            node = nodes[node_id]
            in_edges = incoming[node_id]
            if not in_edges:
                payload = copy.deepcopy(initial_payload)
            else:
                eligible_outputs: list[dict[str, Any]] = []
                for edge in in_edges:
                    upstream = outputs.get(edge.source)
                    if statuses.get(edge.source) != "succeeded" or upstream is None:
                        continue
                    if edge.when is None or bool(upstream.get("_condition")) is edge.when:
                        eligible_outputs.append(upstream)
                if not eligible_outputs:
                    statuses[node_id] = "skipped"
                    step_id = self.repo.create_step(run_id, node.id, node.type, "skipped", 1, None)
                    self.repo.finish_step(step_id, "skipped", None, None, None, None, 0)
                    await self.events.publish(run_id, {"type": "step.skipped", "node_id": node.id, "label": node.label})
                    continue
                payload = copy.deepcopy(eligible_outputs[-1])

            result = await self._execute_with_retries(run_id, node, payload)
            outputs[node_id] = result
            statuses[node_id] = "succeeded"

        successful = [outputs[node_id] for node_id in order if node_id in outputs]
        return successful[-1] if successful else initial_payload

    def _topological_order(self, nodes: dict[str, WorkflowNode], definition: WorkflowDefinition) -> list[str]:
        indegree = {node_id: 0 for node_id in nodes}
        adjacency = {node_id: [] for node_id in nodes}
        for edge in definition.edges:
            indegree[edge.target] += 1
            adjacency[edge.source].append(edge.target)
        queue = [node_id for node_id, degree in indegree.items() if degree == 0]
        order: list[str] = []
        while queue:
            current = queue.pop(0)
            order.append(current)
            for target in adjacency[current]:
                indegree[target] -= 1
                if indegree[target] == 0:
                    queue.append(target)
        if len(order) != len(nodes):
            raise WorkflowExecutionError("Workflow graph contains a cycle")
        return order

    async def _execute_with_retries(self, run_id: int, node: WorkflowNode, payload: dict[str, Any]) -> dict[str, Any]:
        attempts = node.retries + 1
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            step_id = self.repo.create_step(run_id, node.id, node.type, "running", attempt, payload)
            started = time.perf_counter()
            await self.events.publish(run_id, {"type": "step.started", "node_id": node.id, "label": node.label, "attempt": attempt})
            try:
                output = await self._execute_node(node, copy.deepcopy(payload), run_id)
            except Exception as exc:
                duration = int((time.perf_counter() - started) * 1000)
                self.repo.finish_step(step_id, "failed", None, str(exc), stable_hash(payload), None, duration)
                await self.events.publish(run_id, {"type": "step.failed", "node_id": node.id, "label": node.label, "attempt": attempt, "error": str(exc)})
                last_error = exc
                if attempt < attempts:
                    await asyncio.sleep(node.retry_delay_seconds * attempt)
                    continue
                raise WorkflowExecutionError(f"{node.label}: {exc}") from exc
            duration = int((time.perf_counter() - started) * 1000)
            self.repo.finish_step(step_id, "succeeded", output, None, stable_hash(payload), stable_hash(output), duration)
            await self.events.publish(run_id, {"type": "step.succeeded", "node_id": node.id, "label": node.label, "attempt": attempt, "duration_ms": duration, "output": output})
            return output
        raise WorkflowExecutionError(str(last_error or "Unknown workflow error"))

    def _resolve_secret_refs(self, value: Any) -> Any:
        if isinstance(value, str):
            pattern = re.compile(r"\$\{SECRET:([A-Z0-9_\-]+)\}")
            def repl(match: re.Match[str]) -> str:
                name = match.group(1)
                ciphertext = self.repo.get_secret_ciphertext(name)
                if not ciphertext:
                    raise WorkflowExecutionError(f"Secret {name} not found")
                return self.secret_box.decrypt(ciphertext)
            return pattern.sub(repl, value)
        if isinstance(value, dict):
            return {k: self._resolve_secret_refs(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._resolve_secret_refs(v) for v in value]
        return value

    async def _execute_node(self, node: WorkflowNode, payload: dict[str, Any], run_id: int) -> dict[str, Any]:
        config = self._resolve_secret_refs(node.config)
        if node.type == "trigger":
            return payload
        if node.type == "set":
            result = copy.deepcopy(payload)
            for key, value in config.get("values", {}).items():
                set_path(result, key, value)
            return result
        if node.type == "transform":
            return self._transform(payload, config)
        if node.type == "validate":
            self._validate(payload, config)
            return payload
        if node.type == "condition":
            result = copy.deepcopy(payload)
            result["_condition"] = self._condition(payload, config)
            return result
        if node.type == "delay":
            await asyncio.sleep(float(config.get("seconds", 0.1)))
            return payload
        if node.type == "notify":
            result = copy.deepcopy(payload)
            notifications = list(result.get("_notifications", []))
            notifications.append({"channel": config.get("channel", "ops"), "message": config.get("message", node.label)})
            result["_notifications"] = notifications
            return result
        if node.type == "json_store":
            filename = config.get("filename", f"run-{run_id}-{node.id}.json")
            safe_name = re.sub(r"[^A-Za-z0-9_.-]", "_", filename)
            target = self.output_dir / safe_name
            target.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
            result = copy.deepcopy(payload)
            result["_stored_file"] = str(target)
            return result
        if node.type == "http":
            method = str(config.get("method", "GET")).upper()
            url = str(config["url"])
            headers = config.get("headers", {})
            body = config.get("body", payload)
            timeout = float(config.get("timeout_seconds", 10))
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.request(method, url, headers=headers, json=body)
                if response.status_code >= 400:
                    raise WorkflowExecutionError(f"HTTP {response.status_code}: {response.text[:200]}")
                try:
                    response_data: Any = response.json()
                except ValueError:
                    response_data = {"text": response.text}
            result = copy.deepcopy(payload)
            result[config.get("result_key", "http_response")] = response_data
            result["_http_status"] = response.status_code
            return result
        raise WorkflowExecutionError(f"Unsupported node type: {node.type}")

    def _transform(self, payload: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        result = copy.deepcopy(payload)
        for rule in config.get("operations", []):
            source = get_path(result, rule.get("source", "$"))
            op = rule.get("op", "copy")
            value = source
            if op == "lower" and source is not None:
                value = str(source).strip().lower()
            elif op == "upper" and source is not None:
                value = str(source).strip().upper()
            elif op == "strip" and source is not None:
                value = str(source).strip()
            elif op == "int" and source is not None:
                value = int(float(source))
            elif op == "float" and source is not None:
                value = float(source)
            elif op == "round" and source is not None:
                value = round(float(source), int(rule.get("digits", 2)))
            elif op == "default" and (source is None or source == ""):
                value = rule.get("value")
            elif op == "copy":
                value = source
            set_path(result, rule["target"], value)
        return result

    def _validate(self, payload: dict[str, Any], config: dict[str, Any]) -> None:
        missing = [field for field in config.get("required", []) if get_path(payload, field) in {None, ""}]
        if missing:
            raise WorkflowExecutionError(f"Missing required fields: {', '.join(missing)}")
        for rule in config.get("rules", []):
            value = get_path(payload, rule["field"])
            if value is None:
                continue
            if "regex" in rule and not re.search(rule["regex"], str(value)):
                raise WorkflowExecutionError(rule.get("message", f"Invalid value for {rule['field']}"))
            if "min" in rule and float(value) < float(rule["min"]):
                raise WorkflowExecutionError(rule.get("message", f"{rule['field']} below minimum"))
            if "max" in rule and float(value) > float(rule["max"]):
                raise WorkflowExecutionError(rule.get("message", f"{rule['field']} above maximum"))
            if "in" in rule and value not in rule["in"]:
                raise WorkflowExecutionError(rule.get("message", f"Unexpected value for {rule['field']}"))

    def _condition(self, payload: dict[str, Any], config: dict[str, Any]) -> bool:
        left = get_path(payload, config.get("field", "$"))
        op = config.get("op", "eq")
        right = config.get("value")
        if op == "eq": return left == right
        if op == "neq": return left != right
        if op == "gt": return float(left) > float(right)
        if op == "gte": return float(left) >= float(right)
        if op == "lt": return float(left) < float(right)
        if op == "lte": return float(left) <= float(right)
        if op == "contains": return str(right) in str(left)
        if op == "exists": return left is not None
        raise WorkflowExecutionError(f"Unsupported condition operator: {op}")
