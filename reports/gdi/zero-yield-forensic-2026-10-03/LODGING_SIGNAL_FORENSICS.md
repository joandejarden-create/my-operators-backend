# Lodging Signal Forensics — 2026-10-03

## Distribution by hotel

### YOTEL
- NO_LODGING_SIGNAL: 3
- EVENT_HOUSING_SIGNAL: 10

### SPICE
- EVENT_HOUSING_SIGNAL: 23
- NO_LODGING_SIGNAL: 43
- MULTI_DAY_EVENT_SIGNAL: 11

### AC
- NO_LODGING_SIGNAL: 6
- EVENT_HOUSING_SIGNAL: 24
- MULTI_DAY_EVENT_SIGNAL: 2

### BETHESDA
- MULTI_DAY_EVENT_SIGNAL: 12
- NO_LODGING_SIGNAL: 6
- EVENT_HOUSING_SIGNAL: 32
- DIRECT_ROOM_BLOCK_EVIDENCE: 4

### RENAISSANCE
- MULTI_DAY_EVENT_SIGNAL: 8
- EVENT_HOUSING_SIGNAL: 8
- NO_LODGING_SIGNAL: 11
- DIRECT_ROOM_BLOCK_EVIDENCE: 2

### HILTON
- EVENT_HOUSING_SIGNAL: 16
- DIRECT_ROOM_BLOCK_EVIDENCE: 19
- NO_LODGING_SIGNAL: 10

## Rule observation (no change)

`customer-surface-revalidation-v1.lodgingProof()`:

- CREDIBLE only when status matches CONFIRMED/STRONG/OFFICIAL **and** `roomBlockMentioned|housingPageFound`
- WEAK when mentioned flags true
- **overflowMentioned alone → NONE** (explicit comment in code)
- Surface then kills non-exhibitor rows with `!hasHotelOpportunityThesis && lodging.level === NONE` as market entity / demand generator

## Does GDI require direct room-block too early?

**Partially yes for progression past surface**, not for customer-truth dilution:

| Signal class | Allows deeper research? | Survives customer surface today? |
|---|---|---|
| DIRECT_ROOM_BLOCK_EVIDENCE | Yes | Usually yes if thesis present |
| STRONG_LODGING_INFERENCE | Should | Only if mentioned flags or non-boilerplate thesis |
| EVENT_HOUSING_SIGNAL | Should | Sometimes via thesis language scan |
| TRAVELING_DELEGATION_SIGNAL | Research yes | No alone |
| MULTI_DAY_EVENT_SIGNAL | Research yes | No alone |
| NO_LODGING_SIGNAL | No | No |

**Recommendation (forensic only):** allow STRONG_LODGING_INFERENCE / EVENT_HOUSING_SIGNAL to continue **internal research lanes** without granting customer-ready. Do **not** weaken customer-facing truth.

Subject hotels are dominated by NO_LODGING_SIGNAL + template theses — lodging is a top kill gate, but largely because evidence was never stamped, not only because DIRECT is required.

## AidEx counter-example (research stamp gap, not contract change)

AidEx Geneva has public accommodation URL (`aid-expo.com/accommodation`) and overflow thesis language, but `lodgingEvidence.roomBlockMentioned|housingPageFound` was never stamped → `lodgingProof()=NONE`. Combined with the surface motion-copy bug (boilerplate `summaryWhyMatters`), this commercially plausible row dies at surface despite NAMED_DIRECT WHO.
