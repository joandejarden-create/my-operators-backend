# Founder report — Restore original alternative-person contact research

**Contract:** `gdi-buyer-alternative-person-research-v1`  
**PR:** #585 (coherent addendum on same branch)  
**Tip:** pending push  
**Mode:** $0 implementation + mock certification — **STOP before paid Exa**  
**STAGED / CURRENT:** unchanged this phase  

## Modules recovered and reconnected (no new engine)

| Existing module | Role in reconnect |
|---|---|
| `scaling-buyer-research-strategies-v1.js` | `BUYER_RESEARCH_STRATEGY_SEQUENCE` + `selectNextBuyerResearchStrategy` |
| `contact-completeness-v1.js` | `preferredRolesForOpportunity` → `ROLE_PRIORITY_BY_FAMILY` |
| `gdi-commercial-contact-selection-v1.js` | `selectBestCommercialContacts` + `projectSelectionOntoOpportunity` |
| `secondary-named-contact-v1.js` | secondary selection / attach |
| `gdi-named-contact-coverage-standard-v1.js` | min-1 / target-2 + coverage metadata |
| `gdi-opportunity-contact-lineage-reuse-v1.js` | identity reuse |
| `gdi-who-how-contact-intelligence-restore-v1.js` | HOW after alternative ranking (preserved) |
| `gdi-original-full-e2e-execution-restore-v1.js` | strict `isPlausibleBuyerPersonName` junk gate |

**New thin orchestrator only:**  
`workflow/gdi-buyer-alternative-person-research-v1.js` → stage `BUYER_ALTERNATIVE_PERSON_RESEARCH`

## Exact code changes

1. `gdi-buyer-alternative-person-research-v1.js` — harvest → rank → ladder loop → secondary → coverage  
2. `original-full-e2e-research-chain-v1.js` — stage after `CONTACT_RESEARCH`, before `HOW_CONTACT_CHANNEL_DISCOVERY`  
3. `gdi-who-how-contact-intelligence-restore-v1.js` — sequence constant includes alternative stage  
4. `scripts/test-gdi-buyer-alternative-person-research-v1.mjs` — 18 permanent scenarios  
5. `scripts/gdi-design-hotels-saved-evidence-alternative-person-audit-v1.mjs` — $0 audit of 110  
6. `package.json` — test + audit scripts  

## Strategy ladder before / after

| | Before (#585 tip pre-this commit) | After |
|---|---|---|
| Full E2E stages | WHO + CONTACT + HOW | WHO + CONTACT + **BUYER_ALTERNATIVE_PERSON_RESEARCH** + HOW |
| Ladder in chain | Disconnected | Executed when no qualifying named person |
| Stop after 1 WHO miss | Yes (live recovery) | No — advances strategies |
| Ranking | Library only | Always runs on harvested candidates |
| Junk-tolerant retain | No | Yes (`isPlausibleBuyerPersonName` filter + commercial ranking) |

## Person kinds preserved

`DIRECT_BUYER` · `VERIFIED_VIABLE_ALTERNATIVE` · `SECONDARY_CONTACT` · `UNVERIFIED_PERSON` · `ORGANIZATIONAL_ROUTE`

Viable alternative may satisfy min-1 WHO when ideal direct buyer unavailable (evidenced commercial route). Org homepage cannot.

## Public HOW

Unchanged from prior #585 restore: runs **after** alternative ranking; Findymail/Surfe refused; email not required for WHO.

## $0 saved-evidence audit (110 unresolved rows)

Source: `LIVE_RECOVERY_REPORT.json` · **no STAGED writes**

| Metric | n |
|---|---:|
| Rows scanned | 110 |
| Unresolved prior | 109 |
| Had plausible candidates among junk | **11** |
| Recoverable named via ranking (finalizeEligible) | **10** |
| Of which already on STAGED (Lori/Karla/Joni) | **3** |
| **Net-new candidates from saved evidence** | **7** |
| Still unresolved after $0 ranking | **99** |
| Ladder would execute | 100 |
| Highlighted Craig Davis / Amy Yoder | 1 (Amy selected primary; Craig secondary — **verify employer before accept**) |

**Net-new candidates (verification still required):** Murillo Flores · Jason Ware · Andrew Sieupresad · Amy Yoder · Michelle Trimble · Steve Millington · Jens Ihsen  

**Interpretation:** **7** additional named contacts appear recoverable from **existing paid evidence** via junk-tolerant ranking — **not** claimed as live yield and **not** staged. Employer/role verification required before any STAGED apply. Remaining **~99** need authorized live ladder research.

## Regression tests

`npm run test:gdi-buyer-alternative-person-research-v1` → **CERTIFICATION_PASS (18/18)**  
Also reconfirmed: `test:gdi-who-how-contact-intelligence-restore-v1` PASS · `test:gdi-named-contact-coverage-standard-v1` PASS  

## Full E2E modes certified (orchestration reachability)

Declared workflows: new hotel · existing · portfolio · leisure · resume · replay · **cohort_execute**  
All share the same Node chain stage; PowerShell `Invoke-GdiPortfolioProcessing` remains a thin launcher.

## Remaining limitations

1. $0 path filters saved evidence by strategy hints — does not fetch new pages without paid auth.  
2. Commercial classification still requires evidence flags; title alone never becomes DIRECT_HOTEL_BUYER.  
3. Recoverable-10 are **candidates for verification**, not auto-staged.  
4. No STAGED mutation this phase (per GO).  
5. Leisure path inherits chain only when it uses `original-full-e2e-research-chain`.  

## Proposed targeted live plan (after founder GO)

1. Authorize remaining Exa budget (~$10.71 prior rem) for **ladder-driven** alternative research on the 99 still-unresolved only.  
2. Cap per-opp; stop-loss unchanged.  
3. Apply only after employer + relevance verification.  
4. No Findymail/Surfe; no Airtable; STAGED only with explicit GO.

## STOP

Awaiting founder review before any paid execution or STAGED changes.
