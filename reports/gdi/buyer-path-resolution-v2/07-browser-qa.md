# Browser QA — Buyer Path Resolution V2

## YOTEL Geneva Lake (`recrPQcZg7SFARRb2`)

- URL: `/group-demand-intelligence.html?hotelId=recrPQcZg7SFARRb2`
- Customer-facing count: **4** (was 2)
- **Key Travel** — visible, tags ACTIONABLE + PURSUE NOW, modeled room-band, lodging not verified, humanitarian travel buyer path
- **Kuehne + Nagel** — visible, tags ACTIONABLE + PURSUE NOW, Emergency & Relief buyer path, lodging not verified
- Action filter: Pursue Now (4)
- No speculative “needs N rooms” claims observed
- Venue/organizer leakage: none on new cards (AidEx Clarion / CHI remain prior ACTIONABLE rows)

**Result: PASS**

## W Rome (`rece0or38cxo3Fymb`)

- URL: `/group-demand-intelligence.html?hotelId=rece0or38cxo3Fymb`
- Customer-facing QUALIFIED pilot rows: **5** (unchanged — V2 dry-run only)
- Red Bull / Azimut / ABB / Anycubic / Banca Ifis remain QUALIFIED on live bag
- No unintended ACTIONABLE promotions applied

**Result: PASS** (no apply of V2 candidates)

## API / persistence consistency

- Persistence mode on apply: **airtable** (+ FS mirror)
- API `/api/group-demand-intelligence/hotels/recrPQcZg7SFARRb2/opportunities` returns Key Travel + K+N with `gdiMaturityState=ACTIONABLE`, `bookingWindowStatus=CONTACT_NOW`, `lodgingVerified=false`
- W Rome API still returns 5 QUALIFIED
