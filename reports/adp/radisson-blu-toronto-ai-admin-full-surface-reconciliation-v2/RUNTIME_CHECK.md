# RUNTIME_CHECK

- Local 8080 PID (final QA): 28192
- Worktree serving preferred code: my-operators-backend-toronto-customer-release (8091 for PDF gen)
- 8080 reserved for canonical checkout: C:\Dev\dealality\my-operators-backend
- Before fix: PID 29872, cwd primary, HEAD fb941f403 — DID NOT contain a9548cb80 (STALE)
- After fix artifacts copied into primary + restart: PID 28192 still on dirty GDI branch HEAD but serving patched modules + Toronto archive/PDF
- origin/main at task start: a9548cb80 (PR #317)
- CONTAINS PR317 on origin/main: YES
- CONTAINS PR317 on primary HEAD: NO (dirty unrelated branch preserved)
