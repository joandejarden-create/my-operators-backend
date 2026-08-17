# Persistent worker activation report

## Why it kept stopping

1. Supervisor was **not** running persistently (one-shot spawn → exit).
2. Hosted only in **Cursor/local CLI**, not Railway.
3. Last stop: controller `INFRASTRUCTURE_RUNTIME_BOUNDARY` exit 0; supervisor either never daemonized or exited with controller (EPERM lock race fixed).
4–6. Controller stopped on runtime boundary; supervisor stopped because it was designed to exit; nothing in Railway restarted it.
7. **YES** — Joan/Cursor had to re-run npm because no process manager owned the worker.

## Persistent worker

8. Dedicated worker **configured** (`railway.census-v4-worker.toml`, npm `census:v4-full-build-worker`).
9. Start command: `npm run census:v4-full-build-worker`
10. Railway worker service — **config ready; project not linked in this workspace** (`railway status` = no linked project).
11. Auto-restart: Railway `ON_FAILURE` in toml; supervisor also self-loops.
12. Single-worker lease: **yes** (`CENSUS_V4_WORKER_LOCK`).
13. Heartbeat: **yes** (`55-worker-heartbeat.json`, ~30s).
14. Stale-worker recovery: **yes** (TTL/stale steal).
15. Checkpoint/resume: **yes**.
16. Graceful SIGTERM: **yes**.

## Tests

17. Controller normal exit auto-resumed by supervisor: **YES** (3 boundaries, exit 0 → restart).
18. Crash path: exit contract backoff **implemented**.
19. Supervisor crash → Railway restart: **configured, needs deploy**.
20. Duplicate worker prevented: **lease**.
21–22. Temporary/hard circuit: **implemented** in exit contract.
23. Three process boundaries without Joan: **YES** (`60-three-boundary-demo.json` passed).

## Laptop-independent truth

34. Close Cursor → continues **only if Railway worker (or other host daemon) is running**.
35. Laptop off → continues **only after Joan deploys the Railway Census Worker service**.
38. Normal ops Joan npm: **NO** once Railway worker is live; **until then PARTIAL**.

## Verdicts

| | |
| --- | --- |
| PERSISTENT RUNTIME | **PARTIAL** (code OPERATIONAL; Railway service not linked/deployed yet) |
| SUPERVISOR | **PERSISTENT** (loop) — was LOCAL-ONLY before |
| AUTO-RESTART | **PARTIAL** (in-process yes; Railway ON_FAILURE pending link) |
| FULL-UNIVERSE BUILD | **RUNNING CONTINUOUSLY** when worker process is up locally; **deploy Railway for true production** |
