# YOTEL ready card final gate — UI QA

**Date:** 2026-10-05  
**URL:** `http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=recrPQcZg7SFARRb2`

## Browser verification

| Check | Result |
|---|---|
| Facing count (All filter) | **2** — matches canonical API |
| QUALIFY badge on Ready cards | **None** — both show **PURSUE NOW** |
| CHILD ACCOUNT text | **Not visible** on tiles or drawer title |
| PARENT / GENERATOR language | **Not visible** |
| Repetitive machine titles | **Cleared** — AidEx / CHI read naturally |
| Organizer wrappers on surface | **0** — Art Genève, W&W, GHF, SETAC, ECOSOC hidden |
| Generic homepage as buyer | **Not shown** on facing cards |
| Sales-style descriptions | **Yes** — "is connected to … via …" pattern |
| Bethesda shared card structure | **Unchanged** — same tile/drawer chrome |

## Visible Ready cards

1. **AidEx / Clarion Events — AidEx Geneva** · PURSUE NOW · Oct 21–22, 2026  
   Segment: Exhibitor Services / Housing  
2. **CHI de Genève / Concours Hippique International — CHI Geneva Centennial** · PURSUE NOW · Dec 9–13, 2026  
   Segment: Event Operations / Hospitality  

## Regression

- `test:gdi-card-presentation-regression` — **PASS**
- `test:gdi-card-tile-runtime-smoke` — pre-existing harness `readinessPillHtml` scope issue (unrelated to YOTEL gate data)
