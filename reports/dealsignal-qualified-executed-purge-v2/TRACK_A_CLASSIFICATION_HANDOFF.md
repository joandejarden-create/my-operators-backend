# Track A — DealSignal research classification handoff
## Permanent Qualified vs Historical standard (V2)

**Status:** CUSTOMER_BLOCKED  
**Reader gate:** `lib/hotel-intelligence/deal-opportunities/hif-deal-opportunities-commercial-eligibility-v1.js`  
**Do not delete historical research.** Keep it out of Qualified via classification tags.

---

## Why historical rows polluted Qualified

Track A (and related discovery batches) correctly wrote principal fields:

- `COMMERCIAL_OPPORTUNITY_BCT`
- `COMMERCIAL_OPPORTUNITY_MCT`

…and often labeled evidence `relevance=VERIFIED_RELEVANT`.

The reader previously treated **any** principal BCT/MCT field as `PRINCIPAL_OPEN_SIGNAL` → Qualified, unless narrow closed markers appeared in **notes only**.

Structured outcome tags such as `timing=CLOSED` / `timing=ACTIVE_COMMITTED` lived in **valueObserved** and were ignored.  
**VERIFIED ≠ verified open opportunity.**

---

## Required classification tags (every commercial finding)

Append pipe tags on `valueObserved` or `notes` (both are scanned):

| Tag | Allowed values (examples) | Meaning |
|-----|---------------------------|---------|
| `timing=` | `CLOSED`, `CLOSED_AS_OF_EXTENSION`, `ACTIVE_COMMITTED`, `COMMITTED`, `0_6_MONTHS`, `6_18_MONTHS`, `12_36_MONTHS`, `18_36_MONTHS`, `UNKNOWN` | Decision state / horizon |
| `relevance=` | `VERIFIED_RELEVANT`, `EVIDENCE_SUPPORTED_POTENTIAL`, `RELATIONSHIP_TIMING`, `HISTORICAL_ONLY`, `RETENTION_ONLY` | Why the row exists |
| `priority=` | `PRIORITY_A`, `PRIORITY_B`, … | Engagement ranking (does **not** grant Qualified) |
| `first100=` | `YES` / `NO` | First-100 pipeline flag |

### Timing → Qualified impact

- `timing=CLOSED|CLOSED_AS_OF_*|ACTIVE_COMMITTED|COMMITTED|EXECUTED|RETAINED|RENEWED|AWARDED|COMPLETED` → **never Qualified** (historical / committed destination).
- Horizon tags alone do **not** qualify; need unresolved prospective decision + actionable pathway.

---

## Six research classes (use these labels in notes)

1. **Historical executed decisions** — conversion/reflag/appointment already happened.  
   Tags: `timing=CLOSED` + narrative of what/when/parties. Destination: historical_executed.

2. **Retained / renewed partnerships** — MFA/franchise/HMA extended; brand retained.  
   Tags: `timing=CLOSED` or `CLOSED_AS_OF_EXTENSION`; say “retention, not conversion”. Destination: historical_retained.

3. **Committed future agreements** — brand/operator already selected for future opening/reflag.  
   Tags: `timing=ACTIVE_COMMITTED`. Destination: historical_committed.

4. **Emerging prospective decisions** — early relationship / long horizon / weak pathway.  
   Tags: `relevance=RELATIONSHIP_TIMING` or `EMERGING_*` in notes. Destination: watch_emerging (**not** Qualified).

5. **Credible early engagement** — pre-RFP but affirmative owner-led evidence.  
   Include: `OPEN_PROCESS` or clear soliciting/evaluating/owner-led language **without** negation. May Qualify.

6. **Verified actionable opportunities** — unresolved BCT/MCT with credible competitive pathway.  
   Tags: principal field + non-closed timing + open-process evidence. → Qualified.

---

## Hard rules for Track A writers

1. Do **not** put executed deals on Qualified by using principal BCT/MCT fields alone.
2. Always set `timing=` for closed/committed/retained outcomes.
3. Do **not** invent month/day from year-only disclosures.
4. Do **not** treat future contractual expiry as an open selection (`EXPIRY_ONLY_INSUFFICIENT`).
5. Do **not** confuse master franchise / portfolio MFA with property ownership.
6. Separate events by property + agreement + decision date + outcome (no false merge with a later open opportunity).
7. Preserve source URLs; historical drawers must not say “approach owner to compete” for completed decisions.
8. Track B `find_tb_*` contractual tables stay watch unless explicit `OPEN_PROCESS` / `FIRST_100` / `do_not_requalify`.

---

## Reader behavior (Integration-owned)

Qualified requires **all** of:

1. Relevant BCT/MCT commercial decision  
2. Prospective and unresolved  
3. Commercially actionable (expiry-only insufficient; unknown contacts OK)

Historical intelligence remains in Emerging / All Intelligence with status labels:

- Historical — executed  
- Historical — retained  
- Historical — committed  

No Airtable deletes. No new schema required for this gate.
