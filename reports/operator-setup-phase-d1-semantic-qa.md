# Phase D.1 Post-Cleanup Semantic QA

| Metric | Before D.1 | After D.1 |
| ------ | ---------: | --------: |
| Phase-D invalid/generic narratives | 577 | **0** |
| Detector hits on live (incl. pre-D text) | — | 3 |
| KEEP preserved | 86 | 86 confirmed |
| RESTORE | 3 | 3 applied |
| HOLD unchanged | 58 | 58 |

## Phase-D invalid remaining

**0.** All 577 CLEAR mutations applied (197 field clears + 380 created-row deletes).

## Detector residual samples (NOT Phase D)

These match the generic detector but existed **pre-Phase-D** on Arbor golden and were correctly **preserved**:

- Governance `recq4QFkLdohGgU5E` (Arbor) — `infra_systems_technology` / `infra_asset_management_reporting` instructional placeholder text
- Infrastructure Platform `recgfR5WirM9ebj4w` (Arbor) — body instructional placeholder

D.1 must not destroy legitimate pre-existing content. Treat as **pre-D golden residual**, not Phase D failure.

## Golden regression

- Hotel Equities: PASS
- Arbor: PASS
- Playa: PASS (tagline restored)
- Accor: PASS (tagline restored)
- Royalton: PASS (tagline restored)
- Highgate: PASS
- Aimbridge: PASS
- Marriott: PASS
- OxoHotel: PASS

## OE regression

PASS — Assignments unchanged (155 / 155). No OE table writes.

## Semantic-valid coverage (D.1 outcome)

| Bucket | Count |
| ------ | ----: |
| Valid populated (KEEP structured) | 86 |
| Honest blank / unknown (CLEARed Phase D filler) | 577 |
| Hold (scaffold headlines) | 58 |
| Invalid Phase-D generic remaining | **0** |
| N/A | 0 |

Lower populated-cell count is expected and correct.
