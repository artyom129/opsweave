# Architecture

OpsWeave separates workflow definition, persistence, execution and presentation so each part can evolve independently.

## Runtime path

1. A trigger creates a durable `runs` row.
2. The engine validates and topologically sorts the workflow DAG.
3. Each node attempt gets its own `step_runs` record.
4. Inputs and outputs are hashed for lightweight lineage and forensic comparison.
5. Transient failures use the node retry policy.
6. Terminal failures create a dead-letter record containing the original run payload.
7. WebSocket subscribers receive live step/run events.

## Modules

- `models.py` — public workflow contracts and graph primitives.
- `repository.py` — domain persistence API; JSON encoding stays out of runtime logic.
- `engine.py` — DAG ordering, branching, retry behavior and node implementations.
- `scheduler.py` — background trigger loop for interval-based workflows.
- `crypto.py` — secret encryption/decryption boundary.
- `main.py` — HTTP, WebSocket and server-rendered UI boundary.

## Reliability model

A run is durable before execution starts. Each node attempt is written before work begins and finalized with status, timing, error, payload hashes and output. This means a failed workflow leaves an execution trail instead of disappearing into application logs.

Dead letters are intentionally separate from failed run rows: operators can review, replay and resolve them without modifying historical execution data.

## Branching semantics

Conditional edges can specify `when: true` or `when: false`. A condition node stores its decision in `_condition`; downstream nodes whose incoming edge does not match are marked `skipped`. This preserves a complete visual execution timeline.

## Secret handling

Secret values are encrypted with Fernet using a key derived from `OPSWEAVE_SECRET_KEY`. The API only lists secret names and timestamps. Runtime node configuration resolves `${SECRET:NAME}` just before execution.

## Local-first deployment

SQLite in WAL mode keeps the project easy to deploy while allowing concurrent reads from the dashboard during workflow execution. The repository boundary makes migration to PostgreSQL straightforward if the system needs multi-instance workers later.
