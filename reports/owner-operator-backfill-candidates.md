# Owner-Operator Backfill Candidates

**Date:** 2026-06-04  
**Status:** Report-only — **no production Airtable updates performed**  
**Purpose:** Identify companies that should be reclassified as **Owner-Operator** after schema + code deploy.

---

## How to generate live data

This report defines methodology and placeholder structure. Run against Airtable (manual views or script) when API access is available:

```bash
# Suggested future script: scripts/report-owner-operator-backfill-candidates.mjs
# Requires: AIRTABLE_API_KEY, AIRTABLE_BASE_ID
```

### Candidate signals (any two or more → manual review)

| Signal | Source |
|--------|--------|
| A | Company Type = **Hotel Owner** AND linked **Operator Setup - Master** with `submission_status` = Active |
| B | Company Type = **Hotel Management Company** AND deal sponsor fields / ownership portfolio fields populated on Company Profile |
| C | Ecosystem role = **Owner** AND operator narrative in description OR Active Operator Setup |
| D | Company name / description contains `owner-operator`, `vertically integrated`, `own and operate` |
| E | Same `company_name` on Active Master AND Company Profile sponsor on ≥1 deal |
| F | `primaryServiceModel` includes Third-Party Management but Company Type = Hotel Owner |

### Exclusions

- Test operators (`OPERATOR_EXPLORER_HIDE_TEST_RECORDS` name hints)
- Inactive Master (`submission_status` ≠ Active)
- Duplicate company names — resolve before merge

---

## Recommended classification rules (after fields exist)

| Current Company Type | Signals | Recommended Type | Workspace Access | Third-Party Mgmt (initial) |
|---------------------|---------|------------------|------------------|----------------------------|
| Hotel Owner | Active Operator Setup + third-party in profile | Owner-Operator | Owner, Operator | From Operator Profile / interview |
| Hotel Management Company | Owns/develops assets in Company Profile | Owner-Operator | Owner, Operator | Yes / Selectively |
| Hotel Owner | Active Operator, own-portfolio only copy | Owner-Operator | Owner, Operator | **No** |
| Either | Brand + operate (ecosystem Both) | **Not** Owner-Operator — use Brand type + tags | Brand, Operator | N/A |

---

## Candidate table (populate from Airtable)

| Company Name | Current Company Type | Current Ecosystem Role | Recommended Company Type | Recommended Workspace Access | Owner Data? | Operator Data? | Third-Party Mgmt | Confidence | Manual Review | Notes |
|--------------|---------------------|------------------------|--------------------------|------------------------------|-------------|----------------|------------------|------------|---------------|-------|
| *TODO: HE/CALA example* | Hotel Management Company or Hotel Owner | Operator or Owner | Owner-Operator | Owner, Operator | Y | Y (Active Master) | Selectively | High | Y | CALA vertically integrated |
| *TODO: Arbor Lodging* | TBD | TBD | Owner-Operator | Owner, Operator | Y | Y | Yes | High | Y | Fixture copy references owner-operator |
| *TODO: GHL / Posadas* | TBD | TBD | Owner-Operator | Owner, Operator | Y | Y | Case-by-case | Medium | Y | External research |
| … | | | | | | | | | | |

---

## CALA / fixture references (codebase only, not live Airtable)

These names appear in repo fixtures/docs as operator/owner-operator narratives — use as **seed list** for manual Airtable lookup:

- Arbor Lodging (fixtures: `operator-operating-explorer-arbor-cala.json`, form inventory)
- HE/CALA (multiple operator explorer fixtures)
- Antillano Norte (operator materials — verify type)

---

## Manual review questions

1. Do they **accept third-party management** today (not only own portfolio)?
2. Should they appear in **Operator Explorer** for owners seeking operators?
3. Should **same legal entity** receive operator deal requests while also sponsoring deals?
4. Any **conflict flags** for specific markets?

---

## Post-backfill verification

- [ ] `/api/me` returns correct `workspaceAccess` for pilot users
- [ ] Explorer eligibility matches third-party availability
- [ ] Partner Directory shows **Owner-Operator** badge (after UI)
- [ ] No duplicate Company Profile records created

---

*Populate candidate rows after Airtable export. Do not mass-update without stakeholder sign-off.*
