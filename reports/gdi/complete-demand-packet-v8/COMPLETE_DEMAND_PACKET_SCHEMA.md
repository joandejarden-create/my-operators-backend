# Complete Demand Packet Schema

## Six pillars (all required for COMPLETE)

| Pillar | Meaning |
|--------|---------|
| A | Named demand entity (real organization/group) |
| B | Defined group / hotel-demand motion |
| C | Buyer / organizer path |
| D | Future decision point |
| E | Hotel / lodging evidence (not phone co-occurrence alone) |
| F | Target hotel fit |

## Quality states

- **COMPLETE_STRONG** — expensive completion allowed
- **COMPLETE_PLAUSIBLE** — expensive completion allowed
- **PARTIAL_PACKET** — research intelligence; completion only if 4/6 + success match
- **SIGNAL_ONLY** — not a packet
- **REJECTED** / **DUPLICATE**

## Hard rules

- Rotation series alone ≠ Complete Demand Packet
- Phone co-occurrence ≠ lodging evidence
- Apify output = SIGNAL until page-validated
- Jev only after COMPLETE_* (or high-potential PARTIAL with success match)
