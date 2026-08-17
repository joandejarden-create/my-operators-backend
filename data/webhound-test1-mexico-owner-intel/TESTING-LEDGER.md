# Dealality Webhound Testing Ledger

**Testing pool (funded credits):** $55.00  
**Rule:** Do not spend uncommitted balance without explicit approval. Do not auto-launch follow-ups.

| Test | Objective | Budget authorized | Actual spend | Core question | Outcome | Key learning | Another test justified? | Cumulative spend (funded pool) | Remaining testing balance |
|------|-----------|-------------------|--------------|---------------|---------|--------------|-------------------------|--------------------------------|---------------------------|
| 1 | Mexico early pre-decision discovery (broad scout) | $5 (included free run) | ~$4.88 (free run; **not** deducted from $55) | Can Webhound find owners before hotel decisions become public? | Promising, but not outreach-ready | SEMARNAT-via-press discovery works; ownership/contact fail | Yes — enrichment (Test 2) | $0.00 from funded pool | $55.00 (pool untouched) |
| 2 | Outreach-ready packages for Oleum + Venado | **$5.00** | **$4.84** | Can Webhound turn known early project signals into actionable owner relationships? | **Partial pass** | Narrow enrichment >> rediscovery | Yes — BCS gov (Test 3) | **$4.84** | **$50.16** |
| 3 | Los Cabos / East Cape government-record early discovery | **$5.00** | **$4.86** | Is SEMARNAT/public-record early discovery repeatable in BCS? | **Partial yes** | Gaceta pattern portable | Yes — non-gov (Test 4) | **$9.70** | **$45.30** |
| 4 | Non-government early owner signal discovery | **$5.00** | **$4.80** | Can Webhound find owners via non-gov signals before filings? | **Weak / inconsistent** | Investor/hiring useful; gov still best project engine | Yes — outreach (Test 5) | **$14.50** | **$40.50** |
| 5 | Outreach readiness (Venado → Colorada → Oleum) | **$5.00** | **$4.87** | Relationship-ready packages without email hunting? | **Promising / Partial** | Institutional platforms work; opaque owners don’t | Yes — BE validation (Test 6) | **$19.37** | **~$35.63** |
| 6 | **BE independent validation** (Indigo, Kimpton, Tribute, Avani, Individuals) Mexico/CALA-first | **$10.00** | **$6.11** | Can Webhound independently reconstruct BE-grade intel + support census reconcile? | **Pass with material corrections proposed** | Freshness/reflag/status hygiene ROI; not SoT; Avani BE gap; Aluna↔Tres Ríos conflict | **Only with explicit approval** — apply dry-runs or Test 7 | **~$25.48** | **~$29.52** |

## Pool math (funded credits only)

| Item | Amount |
|------|--------|
| Starting funded pool | $55.00 |
| Tests 2–5 actual | −$19.37 |
| Test 6 actual | −$6.11 |
| **Cumulative funded spend** | **~$25.48** |
| **Webhound available credits (live)** | **~$29.52** |
| Uncommitted (no Test 7) | **~$29.52** |

## Notes

- Test 1 free-run does **not** reduce the $55 pool.
- Test 6: `5922518e-c870-432f-ac29-700c99ff8dd4` — eval `TEST6-EVALUATION.md`, reconcile `TEST6-RECONCILIATION.md`
- Test 6 proposed corrections **not applied** to Airtable/SoT.
- **Test 7 not authorized.** Paid Webhound testing paused pending Research Engine V2 work.
- Forensic review: `RESEARCH-ENGINE-FORENSIC-REVIEW.md` + canvas `webhound-vs-dealality-research-engine.canvas.tsx`
- Do not auto-launch Test 7.
