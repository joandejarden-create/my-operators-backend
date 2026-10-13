# Production PDF QA — Cover Parity V2

## Status

| Step | Result |
|------|--------|
| Local ADP reference PDF generated | PASS |
| Local GDI PDF generated | PASS |
| Page-1 geometry match | PASS (see `COVER_GEOMETRY.json`) |
| Railway / production deploy | **NOT RUN** (awaiting explicit deploy) |
| Admin → GDI Reports → View PDF (production binary) | **NOT RUN** |

## Post-deploy checklist

1. Deploy commit containing shared cover module + GDI host/CSS changes.
2. Open AI Demand Admin → GDI Reports → Bethesda Marriott → **View PDF**.
3. Compare page 1 against ADP Bethesda monthly review PDF.
4. Confirm:
   - Full cover panel height matches ADP
   - Bottom footer band height matches ADP
   - GDI slot content (top line, disclaimer, descriptor) present
   - Page count unchanged for same data snapshot
5. Confirm archived v4 (and earlier) still readable in Report Archive; v5 is current.

## Rollback

Revert GDI PDF generator to prior commit; current pointer remains at last archived version until regenerated.
