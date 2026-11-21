# GDI Contact Intelligence Foundation — Integration & Persistence Readiness

**Date:** 2026-11-21  
**Stream:** GDI (`ws-gdi` preserved dirty; work in exceptional WT)  
**Branch:** `feat/gdi-contact-intelligence-foundation-integration-v1`  
**Supersedes merge path for:** #484 + #534 (combined onto current `main`)

---

## 1. #484 and #534 reconciliation results

| PR | Scope | Status before | Behind `main` | Blocker |
| --- | --- | --- | --- | --- |
| **#484** | Google → Exa → Surfe discovery, missing-channel resolver, identity preflight, PCD upgrades, reports | OPEN / MERGEABLE / **BEHIND** | ~218 | **gitleaks FAIL** (`linkedin-client-id` FP on `apolloLinkedIn` assignment in `gdi-missing-channel-resolver-v1.js`) |
| **#534** | Permanent verification statuses, discovery workflow, research metrics, 20-eval | OPEN / MERGEABLE / **BEHIND** | ~7 | Behind tip only (checks green) |

**Overlap:** Complementary, not duplicate. #484 owns discovery/provider order; #534 owns verification classification + bounded workflow + metrics. Both reuse PCD 2.0 / channel hierarchy / person identity keys.

**Dependency:** #534 does **not** require #484 commits to run tests, but the canonical paid path should call #484’s `resolveMissingContactChannels` (now wired). Integration order: **#534 then #484 onto current main**, then persistence readiness.

**Why “not mergeable” in UI:** Both were technically MERGEABLE but **behind**; #484 also failed required gitleaks.

**Dirty `ws-gdi`:** Left untouched on `feat/gdi-google-style-contact-search-before-surfe-v1` with local modifications preserved.

---

## 2. Exact integration order and conflicts

1. Exceptional WT from `origin/main`
2. Merge `feat/gdi-public-source-contact-discovery-permanent-v1` (#534) — **clean**
3. Merge `feat/gdi-google-style-contact-search-before-surfe-v1` (#484) — **clean** (ort auto-merge)
4. Fix gitleaks FP + allowlist
5. Add persistence readiness module (writes OFF)
6. Wire workflow → missing-channel resolver (Surfe/Apollo OFF on this path)
7. 30-contact free eval + tests

**Conflicts:** None during merge.

---

## 3. Implemented contact discovery workflow (canonical)

Single pipeline (`runPublicContactDiscoveryWorkflow`):

1. Resolve verified named contact identity (`looksLikePersonName` + person key)
2. Check existing / cross-hotel process cache for same `personIdentityKey`
3. Bounded free URL recovery from authoritative sources
4. When paid public search authorized: **`resolveMissingContactChannels`** (Google → Exa; Surfe/Apollo **off**)
5. Verify work emails / business phones (`assessPublicContactVerification`)
6. Assess provenance + retention/display permissions (`assessPublicContactPersistenceReadiness`)
7. Persist only if founder flags allow (default: **refuse**)
8. Offer Surfe for remaining gaps via `decideEnrichmentChannels` (never auto-pay)
9. FullEnrich remains separately gated elsewhere (not in this path)

---

## 4. Verification and rejection results

Statuses: `VERIFIED_DIRECT` | `VERIFIED_ORGANIZATION_ROUTE` | `CANDIDATE_UNVERIFIED` | `NOT_FOUND` | `STALE_OR_CONFLICTED`

- Guessed format-only emails → `CANDIDATE_UNVERIFIED` (not persistable)
- Provider origins (Surfe/FE/PDL/Prospeo/Apollo) → never public-verified
- Role mailboxes → organization route, not direct
- Persistence always blocked while `GDI_PUBLIC_CONTACT_LIVE_WRITE_ENABLED === false`

---

## 5. 30-contact evaluation (free / already-authorized only)

Source: existing GDI hotel opportunity JSON (`ws-gdi` data root).  
**No Surfe / FullEnrich / PDL / Prospeo / paid public search.**

| Metric | Value |
| --- | ---: |
| Cohort size | 30 |
| Distinct person keys | 30 |
| Multi-hotel people | 15 |
| New verified direct emails | **+1** |
| New verified direct phones | **+4** |
| Verified org email routes (after) | 2 |
| Wrong-person/employer rejections | 0 (identity threshold unchanged) |
| Avg pages fetched / contact | 1.2 |
| Avg research ms | 812 |
| Research spend USD | **$0** |
| Cost / incremental verified email | **$0** |
| Cost / incremental verified phone | **$0** |
| Cross-hotel process reuses | 0 (cold cache; 15 multi-hotel eligible) |
| Surfe email still needed | 15 |
| Surfe phone still needed | 14 |
| Surfe email avoided (already verified) | 15 |
| Est. Surfe email dependency reduction | 6.3% of missing-email subset |
| Persistence gates all blocked | **true** |
| Paid search used | 0 |
| Persist attempted | 0 |

Artifact: `reports/gdi-contact-intelligence-foundation-integration-v1/EVAL_30_RESULTS.json`

---

## 6. Permitted public-source persistence coverage

| Channel | Persist-eligible when verified + rights | Live write today |
| --- | --- | --- |
| Verified direct work email (public source + URL) | Yes (gate) | **NO** |
| Verified direct business phone | Yes (gate) | **NO** |
| Verified org contact route | Yes (gate; classified separately) | **NO** |
| Surfe / FullEnrich / PDL / Prospeo / Apollo | **Never** unless separate storage rights | **NO** |
| Guessed / syntax-only email | **Never** | **NO** |

---

## 7. Proposed Airtable storage destinations

**Do not create a new Contacts table.**

Reuse existing:

- **GDO** (`tblRuReslJMwsfRQj`) embedded `primaryContact*` fields
- **GDI Hotel Research Opportunities** inventory contact block / slim payload
- Deduplicate via `personIdentityKey` (name+org) + opportunity id links
- Evidence: source URL, source type, verification status, last verified date
- Rights: retain + customer-display flags (proposed metadata; not yet schema-added)

Dry-run proposer: `proposeAirtablePublicContactUpsert` + `assessPublicContactPersistenceReadiness`.

---

## 8. Remaining schema and authorization decisions (STOP for founder)

1. Turn on `GDI_PUBLIC_CONTACT_LIVE_WRITE_ENABLED` only after field mapping review  
2. Confirm GDO / research-inventory coverage for: `sourceUrl`, `verificationStatus`, `personIdentityKey`, retain/display rights, freshness  
3. Choose typed inventory columns vs slim payload contact block  
4. Authorize Airtable cross-hotel reuse read path (beyond process cache)  
5. Any schema change requires founder approval  
6. No live contact writes until read-back gates pass  

---

## 9. Cross-hotel reuse design

- Key: `publicContactPersonKey` / provenance `personIdentityKey` (name + organization)  
- Process cache in workflow for same-run reuse  
- Durable reuse proposed via GDO / inventory lookup by person key (not implemented live)  
- 15/30 eval people already appear on multiple hotels → high reuse potential once Airtable read path is authorized  

---

## 10. Regression and CI results (local)

| Gate | Result |
| --- | --- |
| `test:gdi-public-source-contact-discovery-permanent-v1` | PASS (9) |
| `test:gdi-public-contact-persistence-readiness-v1` | PASS (4) |
| `test:gdi-contact-identity-preflight-v1` | PASS |
| `test:gdi-three-case-public-contact-search-before-surfe-v1` | PASS |
| gitleaks FP fix | Applied (`apolloProfileUrl` + allowlist) |
| Live writes | Disabled |
| Paid enrichment | Not initiated |

GitHub CI on this PR is source of truth after push.

---

## 11. PR and staging status

- Integration PR opened from this branch (see GitHub).  
- Recommend IC: merge this PR; then close #484 and #534 as superseded.  
- GDI chat does **not** merge.  
- `localhost:8080` staging refresh: use canonical boot **after** IC merge + post-merge sync; **no production deploy**.  
- `ws-gdi` remains dirty on Google-style branch until post-merge sync.

---

## 12. Remaining PDL licensing blockers

- Signed PDL Order / embedded-product storage rights: **NOT FOUND** (prior assessment)  
- PDL API key present; HEAD probe showed credits remaining — **no enrich called**  
- Customer display / persistence of PDL-derived contacts: **BLOCKED**  
- Same block applies to Surfe / FullEnrich / Prospeo storage until separately authorized  

---

## Acceptance criterion status

| Criterion | Status |
| --- | --- |
| One integrated evidence-backed discovery + verification pipeline | **MET** (workflow + resolver + verification) |
| Find legitimate business details before Surfe | **MET** (public-first; Surfe offer-only) |
| Distinguish retainable vs transient | **MET** (persistence readiness + provider blocks) |
| Safely prepare verified contacts for canonical persistence | **MET** (dry-run / writes OFF) |
| No new paid enrichment or live writes | **MET** |

**Final state for GDI chat:** `AWAITING_INTEGRATION_CONTROLLER`

**Airtable persistence classification:** `PERSISTENCE_DESIGNED` (not `PERSISTENCE_VALIDATED` / not `FULL_E2E_VALIDATED`)
