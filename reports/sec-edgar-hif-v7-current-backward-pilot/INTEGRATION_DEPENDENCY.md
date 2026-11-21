# HIF V7 — Integration dependency record

**As of:** 2026-11-21  
**Workspace:** `C:\Dev\dealality\ws-hif-v6-track-a`  
**Branch:** `feat/hif-v6-track-a-v7-current-backward-discovery`

## Pending upstream PRs (do not assume merged)

| PR | Title | Role |
|----|-------|------|
| #528 | wave3 SEC discovery | Canonical inventory toward 1000+ |
| #529 | commercial timing & geographic audit | WHEN × WHERE baseline |
| #531 | commercial opportunity recovery | Freshness / first-100 gate / Priority B recovery |

**Required merge sequence (IC exclusive):** `#528 → #529 → #531 → this V7 PR`

## Isolation rules observed

- Track A does **not** merge independently.
- This pilot does **not** write competing `TOWARD_1000` / canonical inventory snapshots.
- Dirty HE / ADP / GDI / Brand Explorer / Track B / Integration Controller worktrees preserved.
- `origin/main` at pilot start: see git rev-parse at commit time.

## Handoff

Integration Controller merges when CI is green and upstream stack is integrated.
