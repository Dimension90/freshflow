# Docker Compose

The root `compose.yaml` is the local full-stack entry point. It includes stateful
dependencies, migrations, Go services, ETA, observability and the nginx-served
web UI on `http://localhost:8089`.

Run from the repository root:

```powershell
docker compose up --build -d --wait
pwsh -NoProfile -File scripts/smoke.ps1
docker compose down
```
