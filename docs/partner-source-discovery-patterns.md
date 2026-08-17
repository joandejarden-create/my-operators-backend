# Partner Source Discovery — Search Patterns (MVP)

**See also:** [partner-reference-material-collection-guide.md](./partner-reference-material-collection-guide.md) — full workflow, folder layout, CLI tools.

**Status:** Documentation + CLI capture. Phase 8 API `discovery/capture` optional — use `npm run partner-reference:download` today.

**Principles:**

- Do not scrape aggressively.
- Do not bypass gated materials.
- Do not use confidential materials without permission.
- Do not publish extracted facts automatically.
- Do not treat third-party PDFs as verified.
- Do not overwrite existing approved Explorer fields.
- All discoveries → `Partner Intelligence - Source Library` with Status **Found** or **Captured**, Verified Source = **No**.

---

## Brand source patterns

Replace `[brand]` with brand name; `[parent]` with parent company site where applicable.

### Site-restricted (official development portals)

```
site:hotel-development.marriott.com [brand] pdf
site:development.ihg.com [brand] development brochure pdf
site:development.wyndhamhotels.com [brand] one sheet pdf
site:assets.group.accor.com [brand] development brochure pdf
site:media.radissonhotels.net [brand] development pdf
site:development.hilton.com [brand] pdf
site:hyatt.com [brand] development pdf
```

### General (verify domain before capture)

```
"[brand name]" "development brochure" hotel
"[brand name]" "owner" "franchise" PDF
"[brand name]" "one sheet" "development"
"[brand name]" "FDD" PDF
"[brand name]" "franchise disclosure document" PDF
"[brand name]" "prototype" hotel development
"[brand name]" "conversion" "franchise" hotel PDF
```

### Parent company portals (when brand-specific search fails)

```
site:marriott.com [brand] development
site:hilton.com [brand] franchise development
site:hyatt.com [brand] development opportunities
site:ihg.com [brand] development
site:choicehotels.com [brand] development
site:wyndhamhotels.com [brand] development
site:accor.com [brand] development
site:bestwestern.com [brand] development
site:minorhotels.com [brand] development
site:sonesta.com [brand] development
```

---

## Operator source patterns

Replace `[operator name]` with legal/marketing name; `[operator domain]` with official website domain.

```
"[operator name]" "hotel management" brochure PDF
"[operator name]" "owner" presentation hotel management
"[operator name]" "capabilities deck" hotel management
"[operator name]" "portfolio" "hotel management"
"[operator name]" "case study" hotel
"[operator name]" "pre-opening" hotel management
"[operator name]" "revenue management" hotel owner
"[operator name]" "CALA" hotel management
"[operator name]" "Latin America" hotel management
"[operator name]" "Caribbean" hotel management
site:[operator domain] portfolio
site:[operator domain] case studies
site:[operator domain] leadership
site:[operator domain] hotel management services
site:[operator domain] filetype:pdf
```

### Initial operator domains (verify before use)

| Operator | Suggested domain |
|----------|------------------|
| Hotel Equities | hotelequities.com |
| Arbor Lodging | arborlodging.com |
| GHL | ghl.com |
| Aimbridge | aimbridgehospitality.com |
| Highgate | highgate.com |
| Remington | remingtonhotels.com |
| Pyramid Global Hospitality | pyramidglobal.com |
| Davidson Hospitality | davidsonhospitality.com |
| Playa Hotels & Resorts | playaresorts.com |
| Driftwood | driftwoodhospitality.com |
| HEI Hotels & Resorts | heihotels.com |
| Concord Hospitality | concordhotels.com |
| Valor Hospitality | valorhospitality.com |
| GF Hotels & Resorts | gfhotels.com |
| PM Hotel Group | pmhotelgroup.com |
| Schulte Hospitality | schultehospitality.com |
| HVMG | hvmg.com |

---

## Source quality defaults on capture

| Discovery type | Default Source Quality | Verified Source? |
|----------------|------------------------|------------------|
| Official development PDF on brand portal | Medium (pending review) | No |
| Official operator deck on company site | Medium | No |
| Press release on official domain | Medium | No |
| Third-party PDF host | Low | No |
| Trade publication | Low/Medium | No |
| Confirmed current FDD (manual verify) | High | Yes (after review) |

---

## API shape (Phase 8)

`POST /api/partner-intelligence/discovery/capture`

```json
{
  "profileType": "Brand",
  "brandId": "recXXXXXXXX",
  "sourceTitle": "Radisson Blu CALA development one-sheet",
  "sourceUrl": "https://…",
  "searchPatternUsed": "site:media.radissonhotels.net Radisson Blu development pdf",
  "suggestedSourceType": "Development Brochure",
  "suggestedSourceOrigin": "Public Web",
  "notes": "Found via manual Google search 2026-06-10"
}
```

Creates Source Library row: Status = **Found**, does **not** run extraction.

---

## File archive location

Downloaded files should be saved under:

`G:\My Drive\Dealality™\Platform Design & Build\Brand Reference Material\{Company Name}\`

Mirror env: `PARTNER_REFERENCE_ROOT` (same path).

Store relative path in Source Library **Local File Path** field.
