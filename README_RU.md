# OpsWeave

OpsWeave — локальная платформа оркестрации автоматизаций и data/API-процессов.

Это не одиночный Python-скрипт, а небольшой control plane: workflow-графы, webhook и scheduled triggers, retry, branching, история каждого шага, WebSocket-события, encrypted secrets, dead-letter queue и replay неудачных запусков.

## Что демонстрирует проект

- FastAPI backend и REST API
- движок исполнения DAG
- условные ветки
- retries и recovery
- SQLite + audit log
- encrypted secret vault
- live execution telemetry
- web dashboard
- webhook integrations
- scheduled jobs
- Docker
- pytest и GitHub Actions

Быстрый запуск:

```bash
pip install -r requirements.txt
python scripts/seed_demo.py
uvicorn app.main:app --reload
```

После запуска открой `http://127.0.0.1:8000`.
