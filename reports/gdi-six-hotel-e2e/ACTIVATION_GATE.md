# Six-hotel batch — activation gate

**Batch status (other five):** QUEUED — proposed GDI **$30** / ADP **$57** **not approved**  
**Renaissance GDI:** **AUTHORIZED_NOT_STARTED** — separate **$12** (owning plan `RENAISSANCE_ACTIVATION_PLAN.md`)  
**Renaissance ADP:** **COMPLETE** — separate **$15** (`reports/adp-six-hotel-e2e/renaissance/`)  
**Code baseline:** `origin/main` `2c58fef4`  
**Active hotels left alone:** Mayflower (`rec0eTEnCqlsGJJt9`), Wyndham San Diego Bayside (`recnPB3Q02NO8Ma1o`) — $11 envelope with Surfe accounting hold; do not transfer

## Do not launch the other five until

1. Founder approves those hotels’ paid envelopes (proposed GDI $30 / ADP $57 remain **unapproved**).  
2. Readiness fixes: already on main/production (weekly opts, score precedence, dry-run isolation, budget fail-closed).  
3. For each hotel without HPC: steward Census create/verify **and** GDI config through normal process (payloads in `HOTEL_IDENTITY_RESOLUTION.md` — no invented IDs).

## Renaissance GDI (already authorized)

No further founder “go” is required for the **$12** GDI scope. Authorization ≠ execution.  
If a separate Cursor task owns live execution, do **not** duplicate. If not started, status remains **AUTHORIZED_NOT_STARTED** under `RENAISSANCE_ACTIVATION_PLAN.md`.

## Hotels ready vs blocked

| Hotel | Canonical ID | Ready? |
|-------|--------------|--------|
| Renaissance NY Times Square | `recG66DQJKP2c0UNh` | **GDI authorized** — execution OWNERSHIP per activation plan; ADP complete |
| Waterstone Boca | `recgMYovrrZDJMqzX` | Config ready; distribution **HOLD**; research not approved under $30/$57 |
| Falls Church Fairview Park | — | **No** — VERIFIED_ABSENT in HPC |
| Arlington Capital View | — | **No** — VERIFIED_ABSENT |
| Charlotte City Center | — | **No** — VERIFIED_ABSENT |
| Tampa Airport Marriott | — | **No** — VERIFIED_ABSENT |

## Activation command pattern (Renaissance GDI)

```bash
node scripts/gdi-research-to-page-workflow-v1.mjs --hotel recG66DQJKP2c0UNh --dry-run
# then scoped live stages with explicit authorize flags per RENAISSANCE_ACTIVATION_PLAN.md:
# --authorize-paid-discovery / --authorize-surfe / --authorize-publish --budget-usd 12
```

## Concurrent work preserved

- Mayflower / SD research & publication + $11 ledger (hold intact; do not overwrite incorrectly)  
- Renaissance ADP complete package  
- AI Admin presentation fixes  
- Customer feedback / actions / outcomes / stable IDs  
- Waterstone distribution HOLD  
