# Staged ADP paid-research budget proposal (six-hotel batch)

**Status:** PROPOSAL ONLY — not approved. No paid ADP calls in preparation.  
**Separate from:** GDI six-hotel **$42** · Mayflower/San Diego GDI **$11** ledger · historical GDI **$2** pilot.

Do **not** reuse GDI funds or treat an empty/reconstructed GDI ledger as ADP authorization. Use a **new durable ADP authorization ledger** when approved (reserve → attribute → reconcile → dedupe retries → fail closed). Do not overwrite GDI ledger files.

## Pricing basis (code-configured estimates)

From `PROVIDER_CONFIGS`: OpenAI $0.03 · Gemini $0.02 · Perplexity $0.05 · Claude $0.03 per call (= **$0.13 / scenario** if all four run).  
Actual billed = UNKNOWN until provider invoices / usage logs reconciled. Estimates ≠ reserved ≠ billed.

---

## Proposed total cap — **$72.00 USD**

| Stage | USD | Notes |
|------:|----:|-------|
| **A. First-hotel (Renaissance) cap** | **15.00** | Prove shared ADP path end-to-end |
| **B. Remaining-five allocation** | **57.00** | Provisional; four hotels identity-blocked |
| **Proposed batch total** | **72.00** | Revised proposal vs any prior informal numbers |

---

## A. Renaissance first-hotel cap — **$15.00**

| Bucket | USD |
|--------|----:|
| Full multi-provider refresh (est. ~80 scenarios × 4 ≈ $10.40) | 10.50 |
| Retry / timeout resume / partial-provider recovery reserve | 2.50 |
| Report/PDF generation compute contingency (non-LLM where applicable) | 1.00 |
| Unknown billed buffer | 1.00 |
| **Cap** | **15.00** |

**Depth:** Stable core intents + justified property-specific; branded probes separated; identity confusable exclusions enforced.  
**Unpaid before paid:** `--dry-run` coordinator; profile/census link audit; scenario list freeze to manifest; compare prior period methodology.  
**Stop:** Cap exhausted/reserved; sampling policy met; provider auth failure; do not spend to empty.  
**Deferred if exhausted:** Extra repeated sampling waves, optional fifth provider, non-priority adventure/wellness expansion.

**Why first:** Only hotel that is identity-ready + ADP profile + certified history **without** external distribution HOLD — best proof of shared path before spending on blocked Marriott onboardings.

---

## B. Remaining-five — **$57.00** (provisional)

| Hotel | Provisional | Discovery/research | Retry reserve | Dependency |
|-------|------------:|-------------------:|-------------:|------------|
| Waterstone Boca | **14.00** | 12.00 | 2.00 | Profile ready; **publication HOLD** — research≠auto client re-issue |
| Falls Church (WASFP) | **11.00** | 9.50 | 1.50 | HPC + ADP profile + census link |
| Arlington Capital View (WASPY) | **11.00** | 9.50 | 1.50 | HPC + profile; Residence Inn exclusion |
| Charlotte City Center (CLTCC) | **11.00** | 9.50 | 1.50 | HPC + profile; meeting-fact verification |
| Tampa Airport (TPAAP) | **10.00** | 8.50 | 1.50 | HPC + profile; terminal≠gate constraint |
| **Sum** | **57.00** | | | |

Shared-market note: WASFP↔WASPY may share some **regional** query drafting labor; **paid provider calls remain hotel-scoped** (no cross-hotel double-count of one response as two hotels’ evidence).

Allocations **dependent on unresolved hotel profiles** — do not equal-split remaining funds blindly after Renaissance.

---

## C. Total

**A $15 + B $57 = $72** proposed batch envelope.

## Ledger rules (when approved)

1. Create ADP-specific authorization ledger path (do not init by wiping GDI `authorization-ledger-v1.json`).  
2. Reserve before submission; attribute every chargeable task to hotel × provider × scenario/runId.  
3. Reconcile estimated vs reserved vs actual billed vs UNKNOWN.  
4. Deduplicate retries / resumed jobs.  
5. Fail closed on missing authorization.  
6. No recurring spend / auto-renewal / unattended schedules.

## Approval gate

Reply with: (1) approve **$72** staged (or revise), (2) **ACTIVATE ADP** for Renaissance-only or named subset, (3) acknowledge Waterstone HOLD for any customer re-issue, (4) steward HPC before Marriott ADP spend.
