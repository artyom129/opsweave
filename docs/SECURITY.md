# Security Notes

OpsWeave is intended as a local-first portfolio/reference implementation, not a public multi-tenant SaaS.

Production deployments should:

- set a strong `OPSWEAVE_SECRET_KEY`;
- enable `OPSWEAVE_WRITE_API_KEY` or place the application behind an authenticated reverse proxy;
- terminate TLS at the proxy/load balancer;
- restrict webhook exposure and add per-webhook signatures where required;
- store the SQLite file on encrypted persistent storage;
- avoid putting secret values directly into workflow JSON;
- apply outbound HTTP allow-lists if workflows are authored by untrusted users.

The current secret list endpoint never returns ciphertext or plaintext values.
