# Bethesda 29 — Commercial Quality Cleanup Scorecard

Hotel: Bethesda Marriott (`recLuxvwwxID7U2B8`)  
Contract: `gdi-bethesda-29-commercial-quality-cleanup-v1`  
Canonical store SHA (unchanged): `fd438549ff030ffc6f219469a526ec9b88ad8a704871fa01784d5650468226ad`

## Category before → after (historical 29)

**Root cause:** internal union projector set `opportunityType` to archival `RECOVERY_CATEGORY` enums (`LOCAL_CATERING`, `POTENTIAL_PARTNERSHIPS`, …). GDI type filters expect `demandSegmentId` + canonical `opportunityType` / PLP–PEP flags.

| Filter (before defect) | Count | Filter (after) | Count |
|---|---:|---|---:|
| Local Catering & Day Meetings | 8 | Local Catering & Day Meetings | 8 |
| Corporate Celebrations & Private Events | 2 | Corporate Celebrations & Private Events | 2 |
| **Other / Unclassified** | **6** | Preferred Lodging Partnership | 4 |
| Travel Trade & Group Intermediaries | 2 | Professional Event Planner Partnership | 2 |
| Corporate Meetings & Incentives | 5 | Travel Trade & Group Intermediaries | 2 |
| Sports & Entertainment | 1 | Corporate Meetings & Incentives | 3 |
| Relocation & Extended Stay | 2 | Association / Conference / Events | 5 |
| Association / Conference / Events | 3 | Sports & Entertainment | 1 |
| | | Relocation & Extended Stay | 2 |
| **Other / Unclassified** | | **0** | |

Association remaps: archival `ASSOCIATION_EVENTS` rows that had been routed as `corporate_meetings_incentives` restored to `associations_conferences_events` (IFE / GEF class).

Canonical audit: the sole remaining **Other / Unclassified** in the unified 88-list is `gdi_opp_asa_advance_2027_disqualified` (CLOSED_DISQUALIFIED ASA ADVANCE) — left unchanged.

## Maturity (honest — no fabricated Sales Ready)

| Status | Count |
|---|---:|
| Sales Ready eligible | 0 |
| Account Development eligible | 0 |
| Research Continuation | 16 |
| Partnership Watch | 8 |
| Overflow Watch | 5 |
| Meeting structural commercial bar (internal research-stage) | 3 |
| Still needing additional / paid research | 29 |

## Evidence recovery posture

- Prefer curated attestation + V20 finalize acceptance rows; no invented contacts or room-night claims.
- Functional inboxes preserved where attested (e.g. PDA `training@pda.org`, IWLCA Halpern Travel route).
- Competitor-host programmes remain Overflow Watch.
- Partnership rows remain Partnership Watch — not presented as existing partnerships.

## Proposed paid research (NOT AUTHORIZED)

- Max suggested: **$25 USD**
- Provider: Exa targeted contact + current-cycle verification
- ~7 candidate IDs (Chamber, SHRM, PMI, PDA, IWLCA, Potomac Events, Cohera)
- Questions: named commercial owner, next-cycle date/pattern, independent URL, overflow accessibility
- Do not spend until founder approval. Do not reuse historical paid balances.
