# AVA Runtime Redeploy Survival — Verdict

**Question:** Do a newly generated audit, its PDF, recommendation decisions, and founder approval history survive a fresh Railway deployment **without** being included in the new deployment artifact?

## Verdict: **FAIL**

Probe: `AVA_RUNTIME_REDEPLOY_SURVIVAL_PROBE.json`  
Script: `scripts/ava-runtime-redeploy-survival-probe-v1.mjs`  
Deploy: **not performed**

---

## Demonstration

### Where records are stored today

| Record | Path on the application filesystem |
|---|---|
| Audit meta, versions, decisions, founder flags | `data/ai-visibility-audit-action-plan/production/store/{propertyId}/` |
| Staged PDFs | `reports/ai-visibility-audit-action-plan/archive/{propertyId}/current.pdf` |
| Lean ADP baselines (packaged) | `data/ai-visibility-audit-action-plan/certified-baselines/` |

There is **no** Railway volume, **no** object store, and **no** Airtable AVA table. Persistence is ordinary files under the service working tree.

### How recovery works today

Records survive a redeploy **only if** they are copied back into the deploy workstation/git tree and uploaded again with `railway up` / `npm run deploy:production`.

There is **no automatic recovery** for runtime writes omitted from the next artifact.

### Probe result (simulated Railway replace)

Architecture fact (repo docs): Railway CLI upload **replaces the entire service filesystem**.

1. **Deploy A** installs a packaged Spice Island audit.  
2. **Runtime** creates a new hotel audit with PDF, APPROVE/DEFER decisions, and `FOUNDER_APPROVED_FOR_CUSTOMER`.  
3. **Deploy B** uploads only the packaged artifact (**runtime hotel omitted**).  
4. Live filesystem is replaced by Deploy B.

| Check | Before Deploy B | After Deploy B (runtime omitted from artifact) |
|---|---|---|
| Runtime audit meta | Present | **Gone** |
| Runtime PDF | Present | **Gone** |
| Founder approval history | Present | **Gone** |
| Recommendation decisions | Present | **Gone** |
| Packaged Spice Island (included in artifact) | Present | Still present |

Therefore runtime persistence **depends on Railway’s ephemeral/replaced application filesystem**.

---

## What still works

Packaged seed data (38 lean baselines + six migrated audits/PDFs) **does** survive redeploy **when included** in the allowlisted upload. That is deploy-artifact durability, not runtime durability.

---

## Smallest durable-storage fix

**Recommendation: Railway Volume** mounted over the runtime write roots.

| Item | Value |
|---|---|
| Primary mount | `/app/data/ai-visibility-audit-action-plan/production` |
| PDF mount (or same volume with both trees) | `/app/reports/ai-visibility-audit-action-plan/archive` |
| Code change | Minimal/none if paths stay as today; ensure process cwd/`ROOT` resolves to mounted paths |
| Why smallest | Keeps current file-store API; no S3/Airtable schema; lean baselines can remain in the git/deploy artifact |
| Alternative | R2/S3 for JSON+PDF blobs (better multi-instance later; larger change) |

Until a volume (or object store) is live, **do not treat production as safe for post-deploy audit generation, decisions, or founder approvals** unless every runtime change is re-packaged into the next deploy artifact.

---

## Corrected deployment posture

| Data class | Durable without re-upload? |
|---|---|
| Lean certified baselines in artifact | Yes (if allowlisted and uploaded) |
| Six migrated audits in artifact | Yes (if allowlisted and uploaded) |
| New audits / PDFs / decisions / founder history created on the server after deploy | **No — FAIL** |

**STOP.** No production deploy. Runtime durability remains a blocker for safe live AVA operation beyond seed artifacts.
