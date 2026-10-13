# Bethesda / NYC vs YOTEL / Spice / AC — 2026-10-03

## Headline counts

| Hotel | Discovered | Entity | Lodging signal | Surface eligible (structural) | Customer ready (structural) | Stamped customer-facing |
|---|---:|---:|---:|---:|---:|---:|
| BETHESDA | 54 | 51 | 40 | 30 | 30 | 35 |
| RENAISSANCE | 29 | 22 | 15 | 9 | 9 | 11 |
| HILTON | 45 | 34 | 19 | 11 | 11 | 12 |
| YOTEL | 13 | 11 | 4 | 0 | 0 | 0 |
| SPICE | 77 | 69 | 8 | 0 | 0 | 0 |
| AC | 32 | 22 | 6 | 0 | 0 | 0 |

## Source families

### BETHESDA
{
  "none": 25,
  "web_org": 20,
  "government": 7,
  "university": 2
}

### RENAISSANCE
{
  "none": 16,
  "web_org": 6,
  "platform": 6,
  "university": 1
}

### HILTON
{
  "web_org": 22,
  "platform": 7,
  "none": 10,
  "government": 3,
  "university": 3
}

### YOTEL
{
  "web_org": 13
}

### SPICE
{
  "web_org": 71,
  "social": 1,
  "government": 3,
  "none": 2
}

### AC
{
  "web_org": 28,
  "university": 1,
  "government": 1,
  "none": 1,
  "platform": 1
}

## Lodging / WHO contrast

- **BETHESDA**: DIRECT_ROOM_BLOCK=4, NO_LODGING=6, WHO_PERSON=29, NOT_RESEARCHED=15
- **RENAISSANCE**: DIRECT_ROOM_BLOCK=2, NO_LODGING=11, WHO_PERSON=22, NOT_RESEARCHED=6
- **HILTON**: DIRECT_ROOM_BLOCK=19, NO_LODGING=10, WHO_PERSON=27, NOT_RESEARCHED=15
- **YOTEL**: DIRECT_ROOM_BLOCK=0, NO_LODGING=3, WHO_PERSON=1, NOT_RESEARCHED=11
- **SPICE**: DIRECT_ROOM_BLOCK=0, NO_LODGING=43, WHO_PERSON=0, NOT_RESEARCHED=75
- **AC**: DIRECT_ROOM_BLOCK=0, NO_LODGING=6, WHO_PERSON=1, NOT_RESEARCHED=27

## Differences

1. **Discovery breadth:** Bethesda (54) / Hilton (45) / Renaissance (29) vs YOTEL (13) — YOTEL severely thin. Spice (77) is wide but low quality. AC (32) mid.
2. **Source families:** Controls denser `web_org` + event platforms with housing pages; subjects skew thin/generic/directory.
3. **Lodging evidence:** Controls retain DIRECT/STRONG stamps; subjects mostly NO_LODGING_SIGNAL.
4. **WHO:** Controls have researched paths; subjects heavily NOT_RESEARCHED.
5. **Placement:** Not the differentiator.
6. **Surface eligibility:** Controls pass KEEP_ACTIVE at scale; subjects almost never.
7. **Qualification gates:** Same code path — difference is **evidence depth / enrichment maturity**, not alternate thresholds.

PIPELINE DIFFERENCE FOUND: **YES** — same gates, unequal research/enrichment completion.
