# API Examples

## List workflows

```bash
curl http://127.0.0.1:8000/api/workflows
```

## Manual run

```bash
curl -X POST http://127.0.0.1:8000/api/workflows/1/run \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"order_id":"PO-42","amount":1200}}'
```

## Vendor order webhook

```bash
curl -X POST http://127.0.0.1:8000/api/webhooks/vendor-orders \
  -H 'Content-Type: application/json' \
  -d '{
    "order_id":"PO-1042",
    "supplier":"Northstar Components",
    "amount":"7425.50",
    "currency":"usd",
    "contact_email":"ops@northstar.example"
  }'
```

## Inspect a run

```bash
curl http://127.0.0.1:8000/api/runs/2
```

## Store a secret

```bash
curl -X POST http://127.0.0.1:8000/api/secrets \
  -H 'Content-Type: application/json' \
  -d '{"name":"CRM_TOKEN","value":"secret-value"}'
```
