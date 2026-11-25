# GDI Competitive Account Discovery (CAD) V1

**Phase 3A PR-A** — contract + classifier + tests.  
**Auth:** `FOUNDER_GO AUTHORIZE_GDI_CAD_PHASE_3A_PR_A`

**Code:**
- `lib/group-demand-intelligence/workflow/gdi-competitive-account-discovery-contract-v1.js`
- `lib/group-demand-intelligence/workflow/gdi-competitive-account-discovery-classifier-v1.js`
- `scripts/test-gdi-competitive-account-discovery-v1.mjs`

**Parent proposal:** `reports/gdi-competitive-account-discovery-phase-3a-v1/FOUNDER_COMPETITIVE_ACCOUNT_DISCOVERY_PROPOSAL.md`  
**Parent architecture:** [Three-Layer Intelligence Contract](./GDI_THREE_LAYER_INTELLIGENCE_CONTRACT_V1.md)

---

## Purpose

Explain, for a demand account: **who currently captures lodging demand**, **when it may next be competitively available**, **who influences purchase**, and **what our hotel should do** — without weakening FQO or inventing open demand.

CAD is a **Layer-1 Research SoT enrichment classifier**, not a parallel opportunity engine.

---

## Classifications (permanent)

| Class | Meaning |
|---|---|
| `HISTORICAL_COMPETITIVE_INTEL` | Verified/inferred competitor relationship or historical demand **without** a defensible current pursuit |
| `EMERGING_COMPETITIVE_OPPORTUNITY` | Credible future/recurring/overflow pathway, but hotel qualification incomplete |
| `FQO_CANDIDATE` | Competitive evidence + plausible pursuit sufficient to **enter** six-rule FQO evaluation |

**`FQO_CANDIDATE` ≠ FQO PASS ≠ persistence authorization ≠ STAGED/CURRENT activation.**

### Attainability (separate)

`WINNABLE` · `OVERFLOW_ONLY` · `MONITOR` · `UNATTAINABLE` — evidence + confidence required.

### Sourcing window

`CONFIRMED` · `INFERRED` · `UNKNOWN`  
Dates only when evidenced. Fabricated / basis-less CONFIRMED or INFERRED windows downgrade to UNKNOWN.

---

## Hard rules

1. Historical venue ≠ incumbent contract (`isEventVenueOnlyMention`).
2. Competitor relationship ≠ available demand.
3. Emerging ≠ FQO PASS.
4. Named person + usable outreach remain mandatory for FQO PASS (existing contract).
5. Cross-hotel `programKey` reuse **enriches**; it does **not** transfer hotel qualification.
6. Preserve intelligence under Three-Layer lossless SoT merge.
7. No new tables / engines; reuse competitor-commitment mapping + recurring-meeting vocabulary.

---

## Integration mapping (this PR vs later)

| Concern | PR-A (this) | Later |
|---|---|---|
| Data contract + enums + sanitize/validate | Yes | — |
| Deterministic classifier + attach helper | Yes | — |
| Competitor-commitment / venue-only reuse | Yes | — |
| Dedupe prefer-enrich helper | Yes | — |
| Regression tests | Yes | — |
| Read-only Bethesda/Wyndham pilot scoring | **PR-B** | — |
| Wire into `gdi-canonical-full-e2e-entry-v1` | **PR-C** | — |
| Founder-authorized SoT enrich apply | **PR-D** | — |

Attach field on opportunities: `competitiveAccountDiscovery` (Research SoT).  
FQO continues to use `evaluateFinalizedQualifiedOpportunity` unchanged.

---

## Tests

```bash
npm run test:gdi-competitive-account-discovery-v1
```

Covers: venue-only false incumbent, historical without pursuit, emerging without hotel action, FQO_CANDIDATE non-pass claims, fabricated sourcing dates, dedupe non-transfer of qualification.

---

## Non-goals (PR-A)

- Airtable / STAGED / CURRENT / paid research mutations  
- Bethesda or Wyndham certified inventory changes  
- Weakening six-rule FQO  
- Broad rebuild of demand engine  

**COMPETITIVE INTELLIGENCE CLASSIFIED. FQO UNCHANGED. CERTIFIED HOTELS FROZEN.**
