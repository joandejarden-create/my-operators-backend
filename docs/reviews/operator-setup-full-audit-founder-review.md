# Operator Setup Full Audit — Founder Review

## Why this audit was needed

Operator Explorer intelligence is mature (36 Publishable Production, Assignments/Presence/BR filled), but **Operator Setup form tables still look empty** in Airtable — blocking confidence before Fit v2.1.

## Snapshot

| Item | Count |
| ---- | ----: |
| Operator Setup tables | 14 |
| Setup fields | 597 |
| Masters | 46 (Prod 36 / Research 1 / TF 9) |
| Meaningful fields critically sparse (Prod) | 380 |
| Current active DIRECT+DERIVED Prod completeness | 20.6% |
| Projected after cleanup | 37.8% |
| Active DIRECT+DERIVED fields in KPI | 138 |
| Dry-run backfill mutations proposed | 59 |

## Why are my Operator Setup tables still empty?

Quantified root causes among **all** sparse Production fields (471):

- **Writer/pipeline missing (F):** 68.4%
- **Correctly blank (I):** 16.3%
- **Derivation intentionally deferred (C):** 5.5%
- **Never researched (A):** 4%
- **Other (J):** 2.3%
- **Research exists elsewhere (OE intel) (B):** 2.3%
- **Legacy field (D):** 1.1%

Meaningful-only sparse (389): research missing (A) 4.9% · pipeline (F) 82.8% · deferred derivation (C) 6.7% · exists elsewhere (B) 2.8%.

In plain language:

1. **Research moved into Operator Intelligence** and was **not written back** into Setup summary fields (sync/derivation gap).
2. **Derivation was deferred** by design when OE normalized entities launched.
3. **Explorer deepen packs** heavily filled Arbor / Hotel Equities / a few goldens — not the full Production universe.
4. Many blanks are **workflow or Fit-preference fields** that should stay empty.
5. Genuine “never researched” blanks are a minority once workflow/section pipeline gaps are separated.

## Classifications

- **DIRECT:** 22
- **UNKNOWN:** 18
- **WORKFLOW ONLY:** 76
- **RESEARCHED SUMMARY:** 335
- **DERIVED:** 116
- **FIT-SPECIFIC:** 18
- **OBSOLETE / DUPLICATE:** 12

## Table verdict

See `reports/operator-setup-founder-table-verdict.md`.

## Source-of-truth

See `docs/data/operator-setup-source-of-truth-policy.md` — Setup becomes stable facts + derived summaries; OE intel remains evidence SoT.

## Fit

Fit still keys off sparse Setup fields (Active Countries, structures, chain scales) while OE already has the data → **mapping/backfill problem**, plus OE diagnostic threshold asg≥6 keeps Fit Data Ready diagnostic at 4.

## Proposed cleanup phases

A Direct → B Derived sync → C Researched summaries → D Fit consumer migration → E Deprecation.

## Exact founder approvals required

1. Source-of-truth policy
2. Field classifications
3. Table keep/populate/derive/deprecate verdicts
4. Direct backfill
5. Derived summary sync
6. Researched-summary backfill scope
7. Fields intentionally blank
8. Deprecation candidates
9. Fit consumer migration direction
10. Cleanup apply phase

## Recommended next phase

**Phase A+B apply (safe direct + derived Setup sync)** after approvals — still before broad Fit v2.1 scoring changes. Optionally parallel: Fit adapter preference for OE intel (Path A from graduation).

## Confirmations

- **No Airtable cleanup writes** in this audit
- **No Operator Fit / scoring changes**
- Owner pilot remains disabled
