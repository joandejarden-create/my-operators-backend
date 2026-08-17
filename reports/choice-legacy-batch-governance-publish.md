# Choice Legacy Mini-Batch Governance Publish v1

Generated: 2026-07-06T22:31:04.347Z
Mode: **apply**
Airtable modified: **yes**
Target table: **Brand Setup - Brand Basics**

## Executive summary

| Metric | Count |
|--------|------:|
| Total brands | 3 |
| Eligible for governance publish | 3 |
| Blocked | 0 |
| Skipped (apply) | 0 |
| Split-out recommended | 0 |

**Proposed chip:** AI-Assisted Profile · **Source Basis:** Company Materials

### Expected posture

| Field | Value |
|-------|-------|
| Validation Status | Company Published |
| Usage Permission | Platform Display Allowed |
| External Display Status | Show Trust Label |
| Explorer chip | AI-Assisted Profile |
| Source Basis | Company Materials |
| Company Validated | unchanged (false) |

### Batch apply command

```bash
npm run choice-legacy-batch-governance-publish -- --apply --approve-choice-legacy-batch-governance-publish
```

### Post-apply verification

```bash
npm run audit-partner-intelligence-publish-readiness
npm run active-brand-governance-upgrade -- --dry-run
npm run test:partner-intelligence-publish-readiness
npm run test:partner-intelligence-profile-governance-publish
```

## Brands

### Comfort Inn & Suites

- Record: `recOzH5iAE1xEjyD0`
- Approved sources: **3** · Approved facts: **4** · Pending facts: **3**
- Company-controlled sources: **yes**
- Readiness: **eligible** · Change class: **new**
- Eligible for batch apply: **yes**
- Split out: **no**
- Expected chip: **AI-Assisted Profile** · Source Basis: **Company Materials**

**Proposed governance fields:**

| Field | Value |
|-------|-------|
| validationStatus | "Company Published" |
| usagePermission | "Platform Display Allowed" |
| sourceType | "Company PDF / Brochure" |
| sourceRegion | "CALA-Specific" |
| confidenceLevel | "High" |
| lastReviewedDate | "2026-07-06" |
| externalDisplayStatus | "Show Trust Label" |
| companyValidated | false |
| companyValidationDate | — |

**Expected subtitle:** `Last Reviewed: Jul 6, 2026 · Source Basis: Company Materials · Region: CALA-specific`

**Field diff (would update):**

| Field | From | To |
|-------|------|-----|
| `Validation Status` | — | "Company Published" |
| `Usage Permission` | — | "Platform Display Allowed" |
| `Source Type` | — | "Company PDF / Brochure" |
| `Source Region` | — | "CALA-Specific" |
| `Last Reviewed Date` | — | "2026-07-06" |
| `Confidence Level` | — | "High" |
| `Evidence Notes` | — | "PI sources (3): Comfort Inn & Suites — Choice development brochure (local); Comfort Inn & Suites — Choice consumer brand page; Comfort Inn & Suites — Choice press kit / media center. Approved facts: 4." |
| `External Display Status` | — | "Show Trust Label" |
| `Internal Notes` | — | "PI profile-governance publish 2026-07-06 (brand:recOzH5iAE1xEjyD0)." |

### Everhome Suites

- Record: `recqkkrsevi4r9ibj`
- Approved sources: **3** · Approved facts: **5** · Pending facts: **3**
- Company-controlled sources: **yes**
- Readiness: **eligible** · Change class: **new**
- Eligible for batch apply: **yes**
- Split out: **no**
- Expected chip: **AI-Assisted Profile** · Source Basis: **Company Materials**

**Proposed governance fields:**

| Field | Value |
|-------|-------|
| validationStatus | "Company Published" |
| usagePermission | "Platform Display Allowed" |
| sourceType | "Company PDF / Brochure" |
| sourceRegion | "CALA-Specific" |
| confidenceLevel | "High" |
| lastReviewedDate | "2026-07-06" |
| externalDisplayStatus | "Show Trust Label" |
| companyValidated | false |
| companyValidationDate | — |

**Expected subtitle:** `Last Reviewed: Jul 6, 2026 · Source Basis: Company Materials · Region: CALA-specific`

**Field diff (would update):**

| Field | From | To |
|-------|------|-----|
| `Validation Status` | — | "Company Published" |
| `Usage Permission` | — | "Platform Display Allowed" |
| `Source Type` | — | "Company PDF / Brochure" |
| `Source Region` | — | "CALA-Specific" |
| `Last Reviewed Date` | — | "2026-07-06" |
| `Confidence Level` | — | "High" |
| `Evidence Notes` | — | "PI sources (3): Everhome Suites — franchise development presentation (local); Everhome Suites — Choice consumer brand page; Everhome Suites — Choice press kit / media center. Approved facts: 5." |
| `External Display Status` | — | "Show Trust Label" |
| `Internal Notes` | — | "PI profile-governance publish 2026-07-06 (brand:recqkkrsevi4r9ibj)." |

### Quality Inn

- Record: `recd8o4k1JddhkRWW`
- Approved sources: **3** · Approved facts: **6** · Pending facts: **2**
- Company-controlled sources: **yes**
- Readiness: **eligible** · Change class: **new**
- Eligible for batch apply: **yes**
- Split out: **no**
- Expected chip: **AI-Assisted Profile** · Source Basis: **Company Materials**

**Proposed governance fields:**

| Field | Value |
|-------|-------|
| validationStatus | "Company Published" |
| usagePermission | "Platform Display Allowed" |
| sourceType | "Company PDF / Brochure" |
| sourceRegion | "CALA-Specific" |
| confidenceLevel | "High" |
| lastReviewedDate | "2026-07-06" |
| externalDisplayStatus | "Show Trust Label" |
| companyValidated | false |
| companyValidationDate | — |

**Expected subtitle:** `Last Reviewed: Jul 6, 2026 · Source Basis: Company Materials · Region: CALA-specific`

**Field diff (would update):**

| Field | From | To |
|-------|------|-----|
| `Validation Status` | — | "Company Published" |
| `Usage Permission` | — | "Platform Display Allowed" |
| `Source Type` | — | "Company PDF / Brochure" |
| `Source Region` | — | "CALA-Specific" |
| `Last Reviewed Date` | — | "2026-07-06" |
| `Confidence Level` | — | "High" |
| `Evidence Notes` | — | "PI sources (3): Quality Inn — Choice development brochure (local); Quality Inn — Choice consumer brand page; Quality Inn — Choice press kit / media center. Approved facts: 6." |
| `External Display Status` | — | "Show Trust Label" |
| `Internal Notes` | — | "PI profile-governance publish 2026-07-06 (brand:recd8o4k1JddhkRWW)." |

## Apply result

- Applied: **3**
- Skipped: **0**
- Errors: **0**

## Does not do

- Rebuild Explorer content or overwrite Brand Setup content fields
- Approve more facts or change Human Review Status
- Set Company Validated or Company Validation Date
- Change UI, scoring, BAS, OAS, OCS, Deal Readiness, or schema
- Publish Source-Informed / Reviewed Sources posture for official Choice company materials
- Downgrade stronger live governance
