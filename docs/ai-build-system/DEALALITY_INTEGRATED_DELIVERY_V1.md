# Dealality Integrated Delivery V1

**Contract:** `DEALALITY_INTEGRATED_DELIVERY_V1`  
**Applies to:** Every Dealality development stream (GDI, ADP, Hotel Explorer, Brand Explorer, Integration Controller coordination)

## Founder decision

Every implementation assignment includes the complete development, integration, and local delivery lifecycle.

The founder reviews product enhancements through the **canonical integrated local application on port 8080**.

An enhancement that exists only in a feature branch, open PR, standalone fixture, or temporary development server is **not** ready for founder review.

## Mandatory default execution sequence

1. Existing architecture audit  
2. Pre-change non-regression baseline  
3. Implementation  
4. Automated tests  
5. Functional validation  
6. PR creation or update  
7. Integration Controller coordination  
8. Required CI completion  
9. Dependency and conflict resolution  
10. Integration Controller-authorized merge to `main`  
11. Post-merge synchronization  
12. Canonical integrated staging refresh on port 8080  
13. Actual integrated browser validation  
14. Previously accepted functionality verification  
15. Founder review handoff  

These steps are part of the original implementation assignment. Do **not** routinely require separate founder prompts for integration, merge coordination, synchronization, or local server restoration.

## Integration Controller governance

- IC retains **exclusive merge authority** (`npm run dealality:remote-pr-merge -- --pr <n>`).
- The development stream must **initiate and coordinate** the established IC handoff automatically where supported.
- Do not bypass CI, merge protections, or authorization requirements.
- If integration requires a separate authorized action the development stream cannot initiate, return **`BLOCKED`** with the exact outstanding action — do **not** describe the implementation as complete or ready for founder review.

## Canonical local server (:8080)

Port **8080** is the single cumulative founder review environment. It must always preserve:

- Previously accepted functionality and UI design  
- Existing authorized hotel intelligence  
- Latest successfully integrated enhancements  
- Correct STAGED inventory  
- Existing authentication and ACL controls  

Individual streams must **not** replace `:8080` with isolated feature branches. Use assigned development ports for isolated testing. Reuse the canonical integrated workspace ownership controls (`DEALALITY_CANONICAL_PORT_8080_OWNERSHIP_V1`).

## Permanent non-regression

Apply `DEALALITY_ADDITIVE_NON_REGRESSION_V1`. Validate the **cumulative** application, not merely the newly implemented feature.

## GDI-specific completion

Additionally enforce **FULL E2E → STAGED → FOUNDER APPROVAL → CURRENT**.

Successfully qualified intelligence must be durably persisted, staged, and visible in the actual integrated application. A completed research artifact or proposed write-set alone is not operational completion.

## Distinctions (do not conflate)

| Layer | Meaning |
|---|---|
| Code implementation | Feature branch work |
| Merge to main | IC-authorized integration |
| Local integrated availability | Canonical `:8080` serves merged tip |
| Founder product acceptance | Founder UI/review sign-off |
| Production deployment | Separate authorization |
| Customer-facing intelligence publication | CURRENT / customer publish — separate authorization |

Normal code integration and local availability are included in the implementation assignment, subject to IC governance. Production deployment, CURRENT publication, additional paid research, and other restricted actions remain separately authorized.

## Completion status

Do **not** routinely stop at:

- `PR OPEN`  
- `AWAITING_INTEGRATION_CONTROLLER`  
- `IMPLEMENTATION COMPLETE`  
- `FEATURE BRANCH VALIDATED`  

**Normal successful completion status:**

```text
INTEGRATED_ON_8080 — AWAITING_FOUNDER_REVIEW
```

Return this only after verifying:

1. Required integration completed (merged to `main`)  
2. Main synchronized into the owning product home  
3. Correct canonical workspace serving port 8080  
4. Requested functionality visible and working  
5. Existing functionality preserved  
6. Relevant inventory accessible  
7. Integrated browser evidence captured  

Otherwise return `PARTIAL` or `BLOCKED` with exact outstanding requirements.

## Final closeout package

Every implementation closeout must include:

- What changed  
- PR number  
- Merge SHA  
- Canonical serving commit  
- Port 8080 workspace identity  
- Direct local application URL  
- Integrated browser screenshots  
- Regression results  
- Existing functionality preservation assessment  
- Relevant inventory reconciliation  
- Any remaining limitations  

## Inter-prompt non-regression (binding)

Before any implementation assignment mutates code:

```powershell
cd C:\Dev\dealality\ws-gdi   # or the registered stream home
npm run dealality:assignment-preflight -- --stream gdi
```

Contract: `DEALALITY_INTER_PROMPT_NON_REGRESSION_V1`. On failure: STOP; reclaim/restore canonical `:8080` (`npm run dealality:gdi-local-8080`, `npm run dealality:canonical-8080-guard -- --once`); revalidate. Ownership uses **cwd + serverScriptDir** (foreign `server.js` under an Integration cwd is denied). Cumulative capability baseline: `lib/dealality-dev-os/gdi-cumulative-functionality-manifest-v1.json`.

## Related contracts

- [DEALALITY_ADDITIVE_NON_REGRESSION_PRINCIPLES.md](./DEALALITY_ADDITIVE_NON_REGRESSION_PRINCIPLES.md)  
- [PR_COMPLETION_AND_WORKSPACE_OWNERSHIP.md](./PR_COMPLETION_AND_WORKSPACE_OWNERSHIP.md)  
- [POST_MERGE_WORKSPACE_SYNC.md](./POST_MERGE_WORKSPACE_SYNC.md)  
- [GDI_FULL_E2E_COMPLETION_AND_PUBLICATION_LIFECYCLE_V1.md](./GDI_FULL_E2E_COMPLETION_AND_PUBLICATION_LIFECYCLE_V1.md)  
- Canonical `:8080` ownership: `lib/dealality-dev-os/canonical-port-8080-ownership-v1.js`  
- Validator: `npm run dealality:validate-gdi-cumulative-8080`  
- Assignment preflight: `npm run dealality:assignment-preflight`  
- Live guard: `npm run dealality:canonical-8080-guard`  
 
