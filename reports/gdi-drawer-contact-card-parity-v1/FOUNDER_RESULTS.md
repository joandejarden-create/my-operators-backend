# GDI drawer contact-card parity — founder results

## Root cause (contact card)

**DTO / projection gap on publication-package + locked customer-facing detail paths** — not a CSS redesign gap and not proven as cache-alone.

- Approved comparison applies `applyUnifiedWhoToContactStandard({ force: true })`, so the drawer receives `scalingTwoContactPresentation` and renders the structured Primary Contact card.
- Main / external GDI package + locked paths returned `primaryContact` without building `scalingTwoContactPresentation`.
- Client `whoShouldSalesContactHtml` then fell back to the older plain-text Who to Contact block.

## Security amendment (founder gate before merge)

Detail-path helper initially called `recoverPublicContactChannels()`, which fetched opportunity source URLs during authenticated customer drawer requests (SSRF / latency risk).

**Amendment:**

1. Customer detail path (`ensureDrawerContactParityPresentation`) builds the structured two-contact card **only from stored authorized fields**.
2. Missing email/phone stay explicitly missing — no live web recovery side effect.
3. Mock Surfe fixture channels (`alex.buyer@fixture.example` / `+1-202-555-0199`) are stripped on the customer path.
4. Research-pipeline `recoverPublicContactChannels` (comparison / controlled research only) now uses SSRF-hardened outbound fetch: DNS/IP checks, hop-by-hop redirect validation, timeouts, response-size limits; rejects loopback / private / link-local / cloud-metadata destinations.
5. Regression: `npm run test:gdi-drawer-contact-card-parity-v1` asserts no outbound on detail, SSRF rejects, mock refusal, package/locked/detail parity from the same stored data.

## EUROGUSS / Gissella data path

| Layer | Result |
|---|---|
| Publication package / local inventory `primaryContact` | Name/title/org only — **no** email/phone persisted |
| Detail path after amendment | Structured Primary Contact card from name/title/org; channels absent until durable persistence supplies them |
| Live source fetch on customer detail | **Removed** |
| Airtable research-inventory read-back | **Not certified** (see separate follow-up) |

Do **not** claim Gissella’s public email/phone are durably persisted until an authorized Airtable read-back succeeds.

## Separate follow-up — Airtable persistence / channel durability

Re-probed from IC `.env` (read-only):

| Check | Result |
|---|---|
| `GDI_OPPORTUNITIES_AIRTABLE_BASE_ID` (`appa2cE7FTRmIbB32`) + table `GDI Hotel Research Opportunities` | **Accessible** (`select` ok) |
| Main product base `AIRTABLE_BASE_ID` / alt for that table name | `403 NOT_AUTHORIZED` (wrong base for this table) |
| EUROGUSS research row `paid3_565ce4588e6d` | Record exists (`recqjHnN2XV56rvmZ`) |
| Persisted `primaryContact.email` / `phone` | **null** — channels never written into research payload |

Earlier `INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND` is consistent with probing the **wrong base** (or unloaded `ws-gdi` env) rather than a missing GDI research table on the correct base. Durability gap for Gissella channels is **data not stored**, not drawer rendering.

No all-hotel bulk updates until a certified research-pipeline write + read-back puts verified public channels into Airtable (and then package/inventory). No paid Surfe. No publication expansion. No schema changes in this PR.

## Next

PR #467 → CI → Integration Controller merge → deploy → authenticated external EUROGUSS drawer visual check → **STOP for founder visual review**.

External visual parity is **not** declared complete until that authenticated check passes.
