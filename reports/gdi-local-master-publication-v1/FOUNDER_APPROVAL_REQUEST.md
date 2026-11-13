# Founder approval request — Bethesda local-master publication (v2)

## Authority
Frozen local authenticated GDI snapshot (server PID 31436 / build `0b05c7e3…`) is the master.  
Live re-check vs freeze: **0 diffs**. Candidate was rebuilt from that freeze only (not a newer live capture).

## Approve this exact fingerprint

| Field | Value |
|-------|-------|
| Hotel | Bethesda Marriott `recLuxvwwxID7U2B8` |
| Version | `gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2` |
| **Fingerprint to approve** | `df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66` |
| Inventory | All 67 / Hotel 61 / Competitive 10 |

## Why not the earlier candidate `b45106b7…`

v1 omitted `scalingDrawerModel` / `segmentOverviewModel` from the publication allowlist → **12 drawers** incomplete in the package.  
v1 bytes retained on disk; STAGED now points at **v2**. CURRENT unchanged.

## Still live externally (wrong vs local master)

`441773211494cebaf50c03ef8921ccac6c57f31ac5cb2398585352b425e36f7f`

### Concise change summary (local master → customer)

| Example | Local master / v2 candidate | Current external (`441773…`) |
|---------|-----------------------------|------------------------------|
| Potomac Memorial | Build Account | Qualify Opportunity |
| Bethesda Premier Cup | Build Account | Qualify Opportunity |
| RIC | Monitor | Qualify Opportunity |
| Contacts / SVG / summaries | Frozen from local master | Prior package values |

## Pre-activation verification (complete)

- Frozen local ↔ running local: **0** presentation diffs  
- Frozen local ↔ v2 package (action/contact/icons/title/summary/need-date/drawer presence): **0** diffs  
- `npm run test:gdi-local-master-publication-parity`: **PASS**  
- CURRENT still `441773…`  
- Railway lean: bundled package under `lib/…/bundled-publication-packages/` is **not** ignored  
- PR #264 CI: Admin tabs, GDI mirror, recovery suite, gitleaks — **SUCCESS**

## What your approval authorizes

1. Merge PR #264  
2. Deploy verified `main` to Railway  
3. Atomically switch Bethesda `CURRENT` → `df8da65a…`  
4. Preserve prior package for rollback + existing share token + feedback  
5. Production browser certify vs frozen local master  

**Reply with exact approval of fingerprint**  
`df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66`

Until then: **no CURRENT switch, no CLIENT READY.**
