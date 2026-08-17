# Brand Explorer Radisson Individuals Momentum Source-Link Parity v31M-R2

- Generated: 2026-07-10T23:42:57.776Z
- Brand: **Radisson Individuals by Choice**
- v31M-R2 exists: **yes**
- Mode: **dry-run**

## Tribute momentum source/link rules

Tribute uses varied official sources (property pages, PRNewswire, trade press, Marriott newsroom) with URL-specific frontend labels — not one generic announcement label for all rows.
- Label storage: **generated_by_frontend_from_body_url**
- UI generation: **frontend_parses_body_url_and_generates_label**

- **Crystal Cove Opens As Tribute Portfolio's First All-Inclusive Resort** · third_party_news · [source](https://www.prnewswire.com/news-releases/crystal-cove-welcomes-a-new-era-of-indie-spirited-island-escapes-as-the-first-tribute-portfolio-allinclusive-resort-302686362.html) · UI label: *View Marriott Announcement*
- **Hotel Rumbao Reopens In Old San Juan Under Tribute Portfolio** · third_party_news · [source](https://www.hotel-online.com/press_releases/release/driftwood-capital-celebrates-rebranding-of-its-245-key-hotel-rumbao-in-historic-old-san-juan-puerto-rico/) · UI label: *View Owner Announcement*
- **NEMI Milan Joins Tribute Portfolio Collection** · third_party_news · [source](https://www.journaldespalaces.com/en/pressrelease-78419-italy-tribute-portfolio-hotels-expands-italian-offering-with-nemi-milan.html) · UI label: *View Article*
- **Recoleta Grand Debuts Tribute Portfolio In Buenos Aires** · third_party_news · [source](https://www.prnewswire.com/news-releases/tribute-portfolio-debuts-in-buenos-aires-with-the-opening-of-recoleta-grand-buenos-aires-a-tribute-portfolio-hotel-302473568.html) · UI label: *View Marriott Announcement*
- **Loma Medellín Joins Tribute Portfolio In Colombia** · other · [source](https://colombia.ladevi.info/negocios/marriott-international-y-oxohotel-amplian-la-oferta-hotelera-medellin-n94379) · UI label: *View Article*
- **Humano Lima Opens As Tribute Portfolio Hotel In Peru** · official_press_release · [source](https://www.hotel-online.com/news/humano-lima-a-tribute-portfolio-hotel-opens-its-doors-in-miraflores) · UI label: *View Article*

## Radisson momentum audit

### Radisson Individuals Expands Across CALA
- Record: `rec0an5blfW4FtMfE`
- Source URL: https://media.choicehotels.com/Radisson-Individuals-press-kit
- Source category: official_brand_press_kit
- Legacy UI label: *View Choice Hotels announcement*
- Parity UI label: *View Choice Hotels press kit*

### Colombia Urban and Heritage Markets Add Individuals Properties
- Record: `recb0WzRRu6jrev4c`
- Source URL: https://www.choicehotels.com/colombia/cartagena/radisson-individuals-hotels/cb017
- Source category: official_property_listing
- Legacy UI label: *View Choice Hotels announcement*
- Parity UI label: *View Cartagena property listing*

### Panama Capital Corridor Extends Individuals Reach
- Record: `recpIgmBNBEMXVEda`
- Source URL: https://www.choicehotels.com/panama/panama-city/radisson-individuals-hotels/pa006
- Source category: official_property_listing
- Legacy UI label: *View Choice Hotels announcement*
- Parity UI label: *View Panama City property listing*

## Link label before/after

- `rec0an5blfW4FtMfE`: *View Choice Hotels announcement* → *View Choice Hotels press kit*
- `recb0WzRRu6jrev4c`: *View Choice Hotels announcement* → *View Cartagena property listing*
- `recpIgmBNBEMXVEda`: *View Choice Hotels announcement* → *View Panama City property listing*

## API/frontend

- Frontend patched: **yes**
- public/js/brand-explorer-atelier-from-api.js momentumAnnouncementLinkLabel — Choice press kit, property listing, and press release labels before generic announcement fallback.
- Root cause: Link labels are not stored in Airtable — frontend momentumAnnouncementLinkLabel() mapped all choicehotels.com URLs to generic View Choice Hotels announcement.

## Governance

- Company Validated untouched: **yes**
- Airtable modified: **no**
- Labels differentiated: **yes**

## Exact apply command

```bash
npm run brand-explorer-radisson-individuals-momentum-source-link-parity-writer -- --brand radisson-individuals-by-choice --apply --approve-brand-explorer-v31M-R2-momentum-source-link-parity --founder-reviewed-radisson-individuals-momentum-sources --confirm-no-company-validation-claim
```
