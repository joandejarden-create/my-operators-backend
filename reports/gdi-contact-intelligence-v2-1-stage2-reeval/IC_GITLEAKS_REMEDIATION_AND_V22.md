# IC — GDI V2.1 Gitleaks remediation + V2.2 readiness

**As of:** 2026-11-23  
**PR:** [#554](https://github.com/joandejarden-create/my-operators-backend/pull/554)  
**Worktree:** `C:\Users\joand\AppData\Local\Temp\dealality-gdi-contact-intel-v2-1-20261123`  
**Remediation commit:** `dbb8e573a`  
**Primary Integration checkout:** preserved dirty/detached (not force-synced)

---

## Phase 1 — Gitleaks root cause

Secret scan workflow `45142167469` reported **7** `generic-api-key` findings.

| # | File | Line | Classification |
|---|---|---:|---|
| 1 | `V21_UNRESOLVED_CLASSIFICATION.json` | 713 | FP |
| 2 | `V21_PHASE1_CLASSIFICATION_FROM_BASELINE.json` | 775 | FP |
| 3 | `STAGE2_COHORT_FREEZE.json` | 825 | FP |
| 4 | `metrics-run/gdi-public-contact-research-metrics-v1.json` | 1220 | FP |
| 5 | `STAGE2_EVAL_RESULTS.json` | 2887 | FP |
| 6 | `V21_REEVAL_RESULTS.json` | 3276 | FP |
| 7 | `V21_REEVAL_RESULTS.json` | 4684 | FP |

**What the values are:** all seven hits are the **same** field `personIdentityKey` holding a **16-char hex digest** produced by:

`sha256(lower(name)|lower(org|domain)).digest('hex').slice(0,16)`  
(`lib/group-demand-intelligence/workflow/contact-field-provenance-v1.js`)

**Not credentials:** no Airtable PAT (`pat…`), no provider API keys, no bearer tokens.  
**Not plaintext PII in the secret match:** the scanner matched the digest string, not email/phone.  
**Should remain committed:** required for frozen-cohort reproducibility and cross-hotel reuse proofs.  
**Containment / rotation:** **not required** — no actual secrets committed.

---

## Phase 2 — Remediation (minimal)

Preferring field-scoped allowlist (repo pattern for `identity_key` / `census_identity_key`) over path-wide rule disable or metric-breaking redaction.

Change in `.gitleaks.toml` (global allowlist regexes):

- `(?i)"personIdentityKey"\s*:`
- `(?i)"person_identity_key"\s*:`

**Not done:** disable `generic-api-key` rule; broad hex allowlist; rewrite history; redact digests (would break freeze reproducibility).

Local verification: `gitleaks detect origin/main..HEAD` → **no leaks found**.  
`npm run dealality:ci-precheck` → **PASS**.

Evaluation metrics / freeze artifacts **unchanged**.

---

## Phase 3 — V2.1 functionality reconfirmed

From frozen `V21_REEVAL_SUMMARY.json` (not altered):

| Metric | Expected | Observed |
|---|---:|---:|
| Verified direct emails | 33 | **33** |
| Verified direct phones | 32 | **32** |
| Unresolved | 34 | **34** |
| Cross-hotel reuse events | 9 | **9** |
| Hypothetical persistence-eligible | 39/80 | **39** |
| Dry-run selected (of 10 requested) | 8 | **8** |
| Paid spend | $0 | **$0** |
| Actual persistence gate | 0 (writes disabled) | **0** |

Local tests (this IC pass):

- `test:gdi-public-contact-employer-domain-v1` — PASS  
- `test:gdi-public-contact-persistence-readiness-v1` — PASS  
- `test:gdi-contact-intelligence-v2-1-cross-hotel-reuse` — PASS  
- `test:gdi-public-contact-discovery-v1` — PASS  

No unauthorized provider calls; no live Airtable writes.

---

## Phase 4 — Merge / staging (status)

- Reconcile against `origin/main`: PR MERGEABLE; IC uses exceptional clean WT (primary dirty preserved).  
- Gates required before merge: GitHub **gitleaks PASS** + existing green suites (Admin / Radar / Spice / GDI recovery).  
- Merge via `dealality:remote-pr-merge` when CI green (no product-chat merge).  
- Staging sync via established post-merge / integrated-staging process after merge.  
- **STOP** before live contact writes, paid research, production deploy.

---

## Phase 5 — V2.2 recommendations (no activation)

1. **Why 33/80 email?** ~47.8% public mailto discovery on researched set; remaining lack official-page direct email within free fetch budget; 10 A-class placeholders are not real people; many HIGH unresolved still have thin seed URLs.

2. **Free research diminishing returns?** Yes. Fetches 112→144 for +1 phone / flat email. Further free crawl is weakly justified without better official staff/contact seeds.

3. **Selective licensed enrichment?** Evaluate only after A-class purge + seed-URL quality pass; keep Surfe-first waterfall; Findymail inactive; no activation this gate.

4. **Reuse across weekly runs?** Process-local cache fixed same-run reuse (0→9). Cross-week needs governed durable person-index keyed by `personIdentityKey` (Airtable or secured store) — **not** authorized yet.

5. **Are embedded `primaryContact*` enough?** Sufficient for opportunity-scoped channels + provenance; **insufficient alone** for canonical multi-hotel person SoT without `personIdentityKey` + source/verification/rights on each opportunity (or person-index).

6. **Which of 39 meet every non-write requirement?** The 39 pass hypothetical readiness excluding only `LIVE_WRITES_DISABLED`. Remaining blockers on others: no verified public channel (34), missing source evidence (20), employer/role unverified (11). Of the 39, only **8** also had verified direct email+phone + loadable opportunity for the dry-run shortlist.

7. **Eight dry-run pilot candidates (future controlled live-write):**  
   NAMT · Broadway League · ARIAS U.S. · CAME · SIOR · NYSSBA · SSSP · Gobierno de Jalisco / SIOP contacts listed in FOUNDER_REPORT_V2_1 §12. Destination: embedded `primaryContact*` only; fixture/authorized subset; idempotent; no new Contacts table until founder SoT decision.

---

## Final IC states

- Security: false-positive remediated; **no credential rotation**  
- Product: V2.1 metrics intact  
- Persistence: `PERSISTENCE_DESIGNED` · writes DISABLED  
- Customer: `CUSTOMER_BLOCKED`  
- **STOP for founder review** before live contact writes, paid enrichment, or production changes
