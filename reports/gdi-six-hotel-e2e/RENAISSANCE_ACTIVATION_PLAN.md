# Renaissance GDI activation package

**Hotel:** Renaissance New York Times Square Hotel  
**Canonical hotelId:** `recG66DQJKP2c0UNh`  
**Workroot:** `C:\Dev\dealality\my-operators-backend` @ `origin/main` `2c58fef4`  
**Authorization status:** **AUTHORIZED** — separate GDI **$12** total cap (research, contact enrichment, qualified canonical publication, verification).  
**Execution status:** **AUTHORIZED_NOT_STARTED** (authorization ≠ execution).  
**Owning plan:** this file. Do not launch duplicates if another Cursor task already owns live execution.  
**Independent of ADP:** ADP Renaissance uses a separate **$15** authorization (`reports/adp-six-hotel-e2e/renaissance/` — COMPLETE as of PR #110). Do not combine ledgers or draw Mayflower/SD $11 funds.

Intended outcome when executed under this plan: **qualified production delivery** through the standard coordinator — not draft-only generation. Sales validation follows publication and must not be fabricated.

---

## 1. Verified configuration & baseline

| Item | Status |
|------|--------|
| HPC identity | `recG66DQJKP2c0UNh` (control match in V4 search) |
| GDI config | `config/group-demand-intelligence/hotels/recG66DQJKP2c0UNh.json` — ONBOARDED |
| Rooms / meeting | 310 rooms · ~5000 sq ft (ADP/HI-backed; re-verify on activation) |
| Market | Manhattan Midtown / Times Square |
| Corpus | 29 unique opportunities (Airtable ↔ local reconciled 2026-10-16) |
| Hotel-facing eligible | **2** |
| Legacy/ready surface | **8** (includes the 2 hotel-facing) |
| Reviewed package | **0** (hotel not on REVIEWED_PACKAGE allowlist) |

Baseline evidence: `BASELINE_RECONCILIATION.md` / `.csv`

---

## 2. Hotel × segment scope & priority gaps

Source: `HOTEL_SEGMENT_BRIEFS_PLAN_ONLY.json` (plan-only; no paid discovery).

| Priority | Segment | Coverage posture |
|----------|---------|------------------|
| P0 | Local Meetings & Catering | Applicable; not yet researched with credible hotel-specific prospects |
| P1 | Associations / government | Partial — strengthen NAMT, SIOR, related candidates |
| P1 | Corporate meetings / training | Partial — strengthen existing conference/expo candidates |
| P2 | Scaling companies | Partial / gap |
| P2 | Entertainment / social (if brief marks applicable) | Gap fill only with territory fit |
| P3 | Sports / overflow | Reuse-first; reject local-calendar noise |

Stopping rule: declared geography/category coverage logged — **not** count-triggered padding.

---

## 3. Mandatory contact-depth stage

Coordinator stages that must run before claiming sales-ready facing delivery:

- `identify_people` → `enrich_contacts` → `buyer_contact_enrichment`  
- Surfe only with `--authorize-surfe` **and** dry-run isolation (PR #108: `dryRun` forces Surfe off)  
- Eligibility re-eval: `evaluate_eligibility` after contact routes exist  
- Do not mark Ready/ACTIONABLE by inventing housing control

---

## 4. Authorized first-hotel budget cap

**$12.00** Renaissance GDI-only (see `PAID_RESEARCH_BUDGET_PROPOSAL.md` §A).  
**Founder-authorized.** Separate from Mayflower/SD $11, $2 pilot, and ADP $15.  
Other five hotels’ proposed GDI **$30** remain **not approved**.

---

## 5. Exact commands supported by current coordinator

Coordinator: `scripts/gdi-research-to-page-workflow-v1.mjs`

**Supported flags (from script header / argv):**  
`--hotel` · `--horizon` · `--budget-usd` · `--budget-searches` · `--budget-pages` · `--target-env` · `--adapter` · `--package-dir` · `--out-dir` · `--resume-run` · `--stage` · `--authorize-publish` · `--authorize-pe-persist` · `--dry-run` · `--run-discovery` · `--authorize-paid-discovery` · `--authorize-surfe` · `--authorize-native` · `--skip-airtable-reuse` · `--jev-shadow` · `--discovery-mode` (`plan` / `live-mock` / `live`)

**Unsupported / do not invent:** hotel-specific `--six-hotel-batch`, auto-publish schedules, invented stage names, bypass of `--authorize-*`.

### Recommended execution sequence (authorized — do not re-ask founder “go”)

```bash
# 0) Unpaid readiness — no authorize flags
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel recG66DQJKP2c0UNh --dry-run

# 1) Config + reuse only
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel recG66DQJKP2c0UNh --stage validate_config
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel recG66DQJKP2c0UNh --stage reuse_evidence

# 2) Discovery plan (unpaid) — do NOT pass --authorize-paid-discovery yet
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel recG66DQJKP2c0UNh --run-discovery --discovery-mode plan --stage discover_gaps

# 3) Authorized $12 GDI cap — paid discovery (owning execution task only):
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel recG66DQJKP2c0UNh \
  --run-discovery --authorize-paid-discovery --discovery-mode live \
  --budget-usd 12 --stage discover_gaps

# 4) Fit → people → contacts (Surfe only if justified within reserve)
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel recG66DQJKP2c0UNh --stage assess_fit
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel recG66DQJKP2c0UNh --stage identify_people
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel recG66DQJKP2c0UNh --stage enrich_contacts
# optional: add --authorize-surfe only with contact-gap evidence (never with sole reliance on dry-run)

# 5) Cards / eligibility / preview
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel recG66DQJKP2c0UNh --stage synthesize_hotel_relevance
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel recG66DQJKP2c0UNh --stage evaluate_eligibility
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel recG66DQJKP2c0UNh --stage generate_cards
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel recG66DQJKP2c0UNh --stage prepare_manifest
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel recG66DQJKP2c0UNh --stage preview_verify

# 6) Production publish — explicit second gate
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel recG66DQJKP2c0UNh --authorize-publish --stage apply_publish
```

Resume: `--resume-run <runId>` with stage ledger under `reports/gdi-repeatable-workflow-v1/runs/` (or `--out-dir`).

**Honest gaps:** `weekly_refresh` / `paid_multisegment` stages exist but are **not** the default first-hotel path for this package; use only if separately authorized. `apply_corporate_packages` / `apply_pe_canonical` require PE packages on disk + authorize flags.

---

## 6. Qualification & publication checks

- `evaluateProspectPublicationEligibility` → HOTEL_FACING before claiming facing delivery  
- Drawer completeness via `generate_cards` / drawer contract  
- No provider metadata in customer prose  
- Score precedence: withheld/unscored assessment must not resurrect stale numeric scores (PR #108)  
- Dry-run must not paid-call or production-write (PR #108)

---

## 7. Canonical persistence & backup

- Prefer Airtable canonical persistence (hotel already `persistence: airtable`)  
- Before apply_publish: backup/export current `opportunities.json` + Airtable ID list (`BASELINE_RECONCILIATION.csv` is pre-activation freeze)  
- Invalidate read cache after publish (`invalidateGdiHotelReadCache` path inside coordinator apply)  
- Do not invent reviewed-package stamps for Renaissance unless hotel is explicitly allowlisted

---

## 8. Local / production verification

| Check | When |
|-------|------|
| Local dry-run stages | Before any authorize flag |
| Airtable read-back count + facing IDs | After publish |
| Production app SHA | Confirm deploy still includes #108 readiness (`248a3098` as of 2026-10-16) |
| Authenticated AI Admin | **Separate ops check** — not a research prerequisite unless execution depends on that UI |

---

## 9. Durable execution history & weekly handoff

- Coordinator run ledger: stage-completion JSON under run dir  
- Seed gaps only into weekly backlog (`WEEKLY_REFRESH_BACKLOG_SEED.json`) — **no** unattended paid weekly schedule  
- Handoff note: remaining corpus dispositions from `BASELINE_RECONCILIATION.csv`; do not re-label “internalish”

---

## 10. Stop / resume conditions

**Stop:** Renaissance $12 exhausted/reserved; priority segments meet stopping criteria; provider/auth failure; kill switch / parallel flag off; founder kill; do not spend to empty cap.  
**Resume:** `--resume-run <id>` from last completed stage; resume known provider run IDs — no duplicate submissions.  
**Do not:** activate remaining five under this package; create HPC for other hotels; touch Mayflower/San Diego envelope.

---

## Authorization vs execution (binding)

| Item | Status |
|------|--------|
| GDI $12 authorization | **AUTHORIZED** (founder) |
| GDI execution | **AUTHORIZED_NOT_STARTED** until an owning task runs the sequence above |
| ADP $15 | **COMPLETE** (separate product/ledger) |
| Other five hotels / proposed GDI $30 | **Not approved** |
| Waterstone distribution | **HOLD** intact |
| Further founder “go” for this GDI $12 scope | **Not required** |

This accounting/docs task does **not** own live GDI execution and must not launch duplicate paid runs.
