# Renaissance ADP refresh — founder summary

**Hotel:** Renaissance New York Times Square Hotel (`adp_renaissance_times_square` / `recG66DQJKP2c0UNh`)  
**Authorization:** `$15` ADP-only (`auth_adp_renaissance_ts_15usd_v1`)  
**Period:** `adp_period_adp_renaissance_times_square_20261016083441_2ff16a`  
**Status:** Research + certify + filesystem publish + PDF + monthly review + local share verified. Production share verify pending deploy of published artifacts / production signing secret.  
**Not activated:** other five hotels · Waterstone HOLD · ADP `$57` · GDI `$12` Renaissance.

---

## A. What research actually ran?

Fresh live multi-provider monitoring: **80 scenarios × 4 providers = 320 calls**, all successful (0 failed).  
Entry: `run-ai-demand-positioning.mjs --apply` with resume after a hung first process.  
Period CERTIFIED and published to filesystem SoT. Monthly review `2026-10` **v2** generated; current-report PDF + monthly-review PDF attached.

## B. Which providers and scenarios were completed or omitted?

| Provider | Model | Completed | Omitted |
|----------|-------|----------:|---------|
| openai | gpt-4o | 80/80 | — |
| gemini | gemini-3.6-flash | 80/80 | — |
| perplexity | sonar | 80/80 | — |
| claude | claude-sonnet-4-6 | 80/80 | — |

No providers omitted. Scenario mix: business 16 · leisure 16 · couples 11 · group_meeting 15 · family 6 · celebration 9 · wellness 3 · adventure 4 (matrix: `QUERY_PROVIDER_SAMPLE_MATRIX.json`).

## C. What changed versus the previous assessment, where comparable?

| | Prior (2026-10-05) | New (2026-10-16) |
|--|--:|--:|
| Demand capture | 40.0% (26/65) | 45.0% (36/80) |
| Lost demand | 39 | 44 |
| Competitive observed | 20 | 19 |
| Payload actions | 2 | 2 |

**Scope disclosure:** denominator moved **65 → 80** (builder universe). Do **not** treat the rate delta as a certified improvement/decline. Intent-level patterns that remain comparable in direction: family still **0%**; business/leisure mid; group_meeting relatively weaker; celebration relatively stronger. Marquis displacement action persists.

## D. Strongest findings and recommended hotel actions

**Findings (evidence-backed):**
- Solid mid-pack capture on Times Square business/leisure/celebration intents; **family** still invisible in this sample.
- AI-observed competitors remain Times Square peers (Marquis displacement recurring).
- Broadway / walking-distance representation flagged for improvement in the new action set.

**Payload actions (reconciled, not reset):**
1. Improve AI representation of walking distance to Broadway.  
2. Address displacement by New York Marriott Marquis.

Monthly review expands to a 5-action corrective posture library pack (`adp_mr_renaissance_times_square_2026-10_v2`) without wiping prior edition backup.

## E. Are page, action plan, PDF, and share persisted and verified?

| Surface | Status |
|---------|--------|
| Published snapshot + CERTIFIED manifest | Yes (filesystem) |
| Current report PDF | Yes · 341,976 bytes · `%PDF` + `%%EOF` |
| Monthly review + PDF | Yes · v2 · PDF READY |
| Local share API | Yes · 200 · period + 45.0% |
| Production share | **Pending** — local DEV-signed token rejected on Railway (`invalid_share_signature`); needs deploy of artifacts + issue with production secret |
| Airtable Live upsert | **Blocked** — payload exceeds Airtable long-text limit; filesystem SoT retained |

## F. Were existing customer inputs and concurrent GDI work preserved?

- Prior published edition backed up under `renaissance/backups/pre-refresh-20261016/`.  
- GDI profile not wholesale-overwritten; ADP did not create/reclassify GDI opportunities.  
- Mayflower/SD and Waterstone HOLD untouched.  
- Historical `$2` GDI pilot and Mayflower/SD `$11` unused.

## G. Estimated cost, actual known cost, and elapsed time?

| Item | USD |
|------|----:|
| Authorized | 15.00 |
| Estimated incurred | **10.53** |
| Safely available | **4.47** |
| Actual billed | **UNKNOWN** |

Research wall-clock (not concurrent-summed): ~**65 minutes** including hang + resume (approx 08:34–09:39 UTC).

## H. Did any step require bespoke code or manual payload rewriting?

**Scoped shared fixes (not Renaissance-only workarounds):** ADP authorization ledger; run-script budget + certify publish + resume; runner resume/hard-timeout; repo-root paths; null-safe publish return.  
**Manual:** kill hung process; resume; Airtable upsert blocked by platform limit (documented, not hand-rewritten payload).  
No customer payload hand-editing.

## I. Is the ADP process ready for the next hotel?

**Conditionally yes** for the next *authorized* hotel after: (1) merge/deploy of shared fixes + published Ren artifacts, (2) production share re-issue with production secret, (3) explicit activation + separate budget.  
Waterstone remains HOLD for external distribution. Four Marriott hotels remain identity-blocked. Proposed `$57` remains **unapproved**.
