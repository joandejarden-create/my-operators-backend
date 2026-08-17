# Brand Explorer Everhome Source Stewardship + Registry Normalization v32C

Everhome-only writer (`recqkkrsevi4r9ibj`) following v32B source capture. Stewards Source Library governance and creates pending Brand Asset Registry records linked to existing presentation visuals — without presentation copy changes, image approvals, activation, or Company Validated changes.

## Commands

```bash
npm run brand-explorer-everhome-source-registry-normalization-writer -- \
  --brand everhome-suites --dry-run
```

Apply (all gates required):

```bash
npm run brand-explorer-everhome-source-registry-normalization-writer -- \
  --brand everhome-suites \
  --apply \
  --approve-brand-explorer-v32C-everhome-source-registry-normalization \
  --confirm-no-company-validation-claim \
  --confirm-no-presentation-copy-changes \
  --confirm-no-image-approval \
  --confirm-everhome-only
```

## Scope

| In scope | Out of scope |
|----------|--------------|
| Everhome Source Library stewardship (12 rows) | WoodSpring / Suburban |
| Brand Asset Registry create/link (pending review) | Presentation copy edits |
| Internal-language audit (report only) | Image approvals |
| Momentum/openings classification (report only) | Active-profile activation |
| | Company Validated |

## Factory rules

1. **Source stewardship** — `Approved for Explorer Use = Yes` only for durable official/trade/property sources; permission notes explicitly state *not company validation*.
2. **FDD / Item 19** — remain `internal_research_only`; never owner-facing.
3. **Registry creates** — default `Needs Usage Review`, `Pending Review`, `Candidate Only`; no automatic approvals.
4. **Temporary Airtable URLs** — never written as Source Page URL; image attachment URLs omitted from registry `Source URL` when ephemeral.
5. **Duplicates** — reported only; no deletes.

## Dry-run snapshot (2026-07-13)

- Sources audited: **12**
- Source updates proposed: **9** (v32B rows still `Approved for Explorer Use = No`)
- Registry rows existing: **0** (confirmed)
- Registry creates proposed: **31** (visual slots + discovery candidates)
- Internal-language findings: reported for v32D cleanup
- Next writer: **v32D** — Everhome presentation backfill

## Post-run verification

```bash
npm run brand-explorer-final-qa-auditor -- --brand everhome-suites --dry-run
npm run brand-explorer-complete-build -- --brand everhome-suites --dry-run --target-quality active-profile
npm run brand-explorer-visual-display-defect-audit -- --brand everhome-suites --dry-run
npm run test:partner-intelligence-publish-readiness
npm run test:partner-intelligence-profile-governance-publish
```

## Reports

- `reports/brand-explorer-everhome-source-registry-normalization-writer.json`
- `reports/brand-explorer-everhome-source-registry-normalization-writer.md`

## Reference

- v32B: `brand-explorer-choice-extended-stay-source-capture-writer`
- v31G pattern: `brand-explorer-radisson-individuals-asset-registry-normalization-writer`
- v31B discovery: `brand-explorer-brand-asset-registry-discovery-writer`
