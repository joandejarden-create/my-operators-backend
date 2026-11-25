# P0 — Wyndham GDI Commercial Conversion Remediation

**Hotel:** Wyndham San Diego Bayside `recnPB3Q02NO8Ma1o`  
**Scope:** Dry-run qualification fix inside canonical FQO + persistence filter  
**Paid spend:** $0 · **Airtable writes:** 0 · **STAGED/CURRENT:** unchanged · **Bethesda:** frozen · **Surfe:** not invoked  

**STOP** after this report — awaiting founder auth for Surfe clicks and/or write/STAGED.

---

## 1. Record-level reconciliation: 59 → 56 → 12 → **18**

| Layer | Count | Meaning |
|---|---:|---|
| Machine `FINALIZATION_PASS` (candidate `…_fb10174d`) | **59** | Hotel-wide disposition; includes historical retain + soft stamps |
| Prior VALIDATED dry-run (buggy historical blanket) | **~42–56** | `allowHistoricalSchemaCompat` wrongly set `historicalApprovedInventory=true` for **every** row |
| Prior ad-hoc “strict commercial” scrub | **12** | Outside FQO (CLEAN 11 + ACG); useful diagnostic, not the engine |
| **Post-fix FQO / VALIDATED dry-run** | **18** | Enforced commercial-quality FQO (11 CLEAN + 7 new) |

### What the 59 → 18 exclusions are (n=41)

| Issue class | Count | False negative? |
|---|---:|---|
| `MISSING_RESEARCH` (pathway / fit / competitive) | 22 | No — need evidence or Surfe-assisted timing |
| `INCORRECT_EXTRACTION` (non-person / role-glued / place titles) | 14 | No — legitimate fails |
| `MISSING_RESEARCH_OR_MISLABELED_CHANNEL` (functional mailbox / unlabeled route) | 5 | Rarely — only if a true labeled org route was discarded |

Full row audit: `01-RECORD_RECONCILIATION_59.json`, `02-EXCLUSIONS_AFTER_FIX.json`.

**Do not write the 59 or the old 56.** Post-fix write-set dry-run kept = **18**.

---

## 2. Root-cause breakdown

1. **Persistence filter bug (primary 56 inflation):** `filterValidatedFqoPersistenceWriteSet` passed `historicalApprovedInventory: allowHistoricalSchemaCompat` globally, soft-passing undated / open-account gates for new scrapes.
2. **Continuation over-stamps:** `futureDemandPathway=true` + scrape `askToBeDirectedTo` blobs let machine PASS without commercial-grade channels.
3. **Functional inbox + company phone soft-pass:** `exhibits@`, `westhousing@`, etc. were rejected as email, but scraped switchboard phones still counted as `DIRECT_PROFESSIONAL_PHONE`.
4. **False person extraction:** event titles, venues (`Nolita House`), `Sponsorship Opportunities`, role-glued names (`… Senior` / `… Regional`).
5. **Demand-type gap (partial):** recurring scaling / planner / partnership rows failed when logic demanded a dated event; fixed inside FQO via `hasRecurringRelationshipPathway` (lodging evidence required; hiring-only still fails).
6. **Segment labels null** on most candidate rows → distribution shows `unlabeled` (presentation metadata gap, not FQO math).

---

## 3. Commercial-quality enforcement (canonical FQO — no parallel gate)

Integrated into `gdi-finalized-qualified-opportunity-contract-v1.js`:

| Enforcement | Auditable reason / gate |
|---|---|
| Non-person / place / event / venue name tokens | `NON_PERSON_*` / `NAMED_COMMERCIAL_PERSON` |
| Role-glued trailing titles | `ROLE_GLUED_PERSON_NAME` |
| Expanded functional mailboxes (`exhibits@`, `westhousing@`, `hqc-inquiry@`, …) | `GENERIC_OR_FUNCTIONAL_ORGANIZATION_INBOX` |
| Functional email + company phone without LinkedIn | `PHONE_WITH_FUNCTIONAL_EMAIL_REQUIRES_LINKEDIN_OR_LABELED_ROUTE` |
| SignalHire / FAQ scrape “ask to be directed” labels rejected | substantive org-route check |
| LinkedIn buried in provenance promoted to `PERSON_LINKEDIN` | channel provenance |
| Bare `futureDemandPathway` cannot reopen past-dated one-time events | `NO_VALID_FUTURE_SOURCING_PATHWAY` |
| Recurring relationship pathway (demand-type aware) | `FUTURE_SOURCING_PATHWAY.recurringRelationshipPathway` |

Persistence fix: historical soft-path only for `_historicalApprovedInventory` / explicit `historicalApprovedOpportunityIds` — **not** the whole set.

Tests: `scripts/test-gdi-finalized-qualified-opportunity-v1.mjs`, `scripts/test-gdi-validated-fqo-write-set-policy-v1.mjs` — **PASS**.

---

## 4. Demand-type compatibility

| Category | Finding |
|---|---|
| Scaling companies | Can qualify without dated event when lodging/account language + named WHO/HOW exist; hiring-only still fails |
| Professional meeting planners | Supported via recurring segment list + planner/DMC language |
| Travel trade / housing intermediaries | Supported; functional housing inboxes require LinkedIn or labeled route |
| Event venues / preferred lodging | Supported as partnership/referral when lodging evidence present; venue-as-person names rejected |
| Corporate meetings / training | Supported via recurring pathway |
| Projects / infrastructure | Supported with project-team lodging language |
| Relocation / extended stay | Supported; careers-only listings rejected |

**Rule retained:** no room-night inference from hiring, proximity, or org listings alone.

---

## 5. CLEAN 11 near-miss reconciliation

| Question | Answer |
|---|---|
| Duplicate opportunities in candidate 59? | **Yes** — CLEAN 11 IDs are inside the 59; prior “near-miss” list re-listed the same certified contacts |
| Lost prior contact provenance? | **No** for CLEAN 11 |
| Material commercial-quality defect on CLEAN 11? | **None** under post-fix FQO — all **11 still pass** |
| Silent downgrade? | **Forbidden / not done** — Wyndham STAGED remains `…_3283ee92` |

Evidence: `03-CLEAN11_RECONCILE.json`.

---

## 6. Contact extraction / lineage defects

| Class | Count (of 41 fails) |
|---|---:|
| False person extraction | 14 |
| Functional mailbox only | 5 |
| Other (mostly pathway / fit / competitive) | 22 |

Additional residual risks on some **new passes** (presentation review, not silent drop):

- Weak program titles (`Housing & Travel`, `Global gifting`)
- Possible email/name mismatch (`Leandra Machado` ↔ `jcrisci@…`, `Mitchellee` ↔ `kavin@…`) — Surfe/manual verify recommended before STAGED expansion

---

## 7. Code-fix PR (this branch)

**Branch:** `feat/gdi-wyndham-commercial-conversion-fqo-v1`

Files:

- `lib/.../gdi-finalized-qualified-opportunity-contract-v1.js`
- `lib/.../gdi-full-e2e-research-persistence-bridge-v1.js`
- `scripts/test-gdi-finalized-qualified-opportunity-v1.mjs`
- `scripts/test-gdi-validated-fqo-write-set-policy-v1.mjs`
- `scripts/gdi-wyndham-sd-bayside-commercial-conversion-remediation-v1.mjs`
- `reports/gdi-wyndham-sd-bayside-commercial-conversion-remediation-v1/*`

No new engine. No parallel classifier. Integration Controller owns merge.

---

## 8. Strict dry-run FQO after corrections

| Metric | Value |
|---|---:|
| FQO pass | **18** |
| CLEAN 11 retained | **11** |
| New FQO | **7** |
| False-positive machine passes removed vs 59 | **41** |
| Old buggy validated blanket | ~42 (would still over-count without fix) |
| Aspirational 25–35 | **Not yet** — blocked on WHO/HOW research, not discovery |

### New FQO (7)

1. Lockheed Martin WEST 2027 exhibitor team — Michael Pluff (LinkedIn)  
2. L3Harris WEST 2027 exhibitor team — Dan Brosnan (LinkedIn)  
3. AWWA Hotel and Travel — Joanne Romero Gaglia (LinkedIn)  
4. Housing & Travel — Mitchellee Delosreyes (*title weak; verify channel*)  
5. Global gifting — Leandra Machado (*title/email verify*)  
6. ACG San Diego — Judy Susser  
7. Create The Future Awards 2026 / Downtown Partnership — Kathryn Newstead  

---

## 9. Per-segment distribution

Candidate rows largely lack `segmentId` → pass distribution reports **`unlabeled: 18`**.  
Commercially, CLEAN 11 + new adds span LMC, scientific/association meetings, WEST competitive housing, scaling (ESET in CLEAN 11), and chapter/account (ACG).  
Scaling / planners / travel-trade / venues remain **under-converted** pending Surfe WHO pass on the near-miss queue.

---

## 10. Remaining near-miss / Surfe queue

**26** ranked explicit-click Surfe candidates: `04-SURFE_RANKED_QUEUE.json`  
Policy: Surfe **explicit-click only** — not auto-invoked. No Findymail/Agent.

Priority themes: functional-mailbox rows with real demand; named persons lacking usable channel; pathway-held rows with lodging evidence.

---

## 11. Recommended next action

1. **Merge this FQO/persistence PR** via Integration Controller.  
2. Founder auth for **Surfe explicit-click** on the ranked queue (no new Exa required first).  
3. After Surfe: dry-run requalify → target **25–35+** without forcing count.  
4. Separate auth for Airtable `VALIDATED_FQO_ONLY` write + Wyndham STAGED of founder-approved package (**not** raw 59).  

**Persistence classification:** still `PROTOTYPE` until write/STAGED authorized and verified.

### Integrity

| Pointer | Status |
|---|---|
| Bethesda STAGED/CURRENT | Unchanged |
| Wyndham STAGED | CLEAN 11 `…_3283ee92` |
| Wyndham CURRENT | `null` |
| Production Airtable | 0 writes |
