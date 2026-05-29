# Choice Privileges — consumer web reference

**Sources (crawled):**
- https://www.choicehotels.com/choice-privileges/benefits
- https://www.choicehotels.com/choice-privileges/earn-points
- https://www.choicehotels.com/choice-privileges/redeem-points
- https://www.choicehotels.com/choice-privileges/partners

**Fixtures:** `fixtures/choice-privileges-web/`  
**Crawl:** `node scripts/crawl-choice-privileges-web.mjs`

Use for **all CHI brands** (system-wide program — same copy in Loyalty tab / `loyalty.*` presentation slots). Brand-specific **% of rooms from loyalty** still comes from each brand’s **FDD Item 19**, not these pages.

## Facts for Brand Setup / Explorer (system-level)

| Topic | Consumer site (2025–2026) | Notes vs prior CHI materials |
|-------|---------------------------|------------------------------|
| Program name | Choice Privileges® | Already in Airtable |
| Portfolio reach | **7,100+** properties worldwide | Media center cited ~7,400–7,500; dev materials ~70M members — keep members from corporate deck unless updated |
| Base earn | Up to **10 points per $1** direct | |
| Card accelerator | **16x–22x** on stays with Choice Privileges Mastercard | |
| Redeem floor | Reward nights from **8,000 points** | |
| Elite tiers | Member → Gold (5n/10k) → Platinum (15n/30k) → Diamond (35n/70k) → **Titanium (55n/110k)** | Titanium is newer than many FDDs |
| Fast Gold | **5 nights** (or card) | |
| Return & earn | **1,000 pts** after 2nd & 3rd qualifying stays/year | |
| Member discount | **10%+** member rates | |
| Recognition | U.S. News **#1** hotel rewards program (14 evaluated) | Marketing claim |

## Partners (for `loyalty.ecosystem` / materials tab)

Bluegreen Vacations, Penn Gaming Hotels, Westgate Resorts, Preferred Hotels & Resorts (~300), Avis/Budget, cruises (5x), e-Rewards surveys, GetYourGuide, entertainment tickets, airline mile exchange, Amex/Capital One/Wells Fargo/Citi point transfers, charity redemption.

## What this does **not** replace

- Per-brand **Choice Privileges Contribution %** and **CRS / Enterprise %** → FDD Item 19 only.
- **Direct / OTA / CAC** estimates where FDD is silent.
- Full **Brand Explorer presentation** blocks — still need per-brand authoring for non-loyalty tabs.

## Suggested presentation slots to refresh (all 22 CHI brands)

| Slot key | Suggested content source |
|----------|-------------------------|
| `loyalty.hero_title` | Choice Privileges — Loyalty at a Glance |
| `loyalty.ecosystem` | 7,100+ hotels, 20+ brands, partners paragraph |
| `loyalty.owner_lens` | System program lifts direct/CRS; brand FDD % in KPI strip |
| `loyalty.earn` | 10 pts/$1; 5-night Gold; Titanium; Return & earn bullets |
| `loyalty.proof` | Multiple rows: U.S. News #1, 8k min night, partners |

Apply via `Brand Setup - Brand Explorer Presentation`:

```bash
npm run apply-choice-loyalty-presentation-batch -- --dry-run
npm run apply-choice-loyalty-presentation-batch
```

Refreshes all `loyalty.*` slots for Parent Company = Choice Hotels International (replaces existing loyalty rows; does not delete Image attachments on other slots).
