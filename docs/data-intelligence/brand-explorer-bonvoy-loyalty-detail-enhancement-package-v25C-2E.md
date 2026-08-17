# Brand Explorer Bonvoy Loyalty Detail Enhancement Package v25C-2E

- Generated: 2026-07-09T17:55:43.585Z
- Brand: **Tribute Portfolio** (`recCvV0PuZOi8c3hC`)
- v25C-2E exists: **yes**
- Airtable modified: **no**

## Current weakness

Tribute loyalty rows satisfy the required-section contract (earn, redeem, 6 elite tiers, 2 proof rows) but remain materially thinner than Curio/Radisson reference sections: single-sentence earn/redeem, elite tiers with no qualification or benefit detail, and proof rows that do not connect Bonvoy scale to owner affiliation decisions.

## Gap table (Tribute vs reference brands)

| Metric | Tribute | Reference benchmark | Notes |
|--------|---------|---------------------|-------|
| Earn depth (bullet lines) | 1 | 4 | Tribute uses one narrative sentence; references use 3–4 mechanic bullets. |
| Redeem depth (bullet lines) | 1 | 3.3 | Tribute redeem row is generic; references enumerate reward-night and flexibility mechanics. |
| Elite tier detail score (1–5) | 1 | 4.5 | Tribute tiers are name-only placeholders; references include qualification thresholds and illustrative benefits. |
| Owner-facing usefulness | 2 | 4 | Reference copy connects mechanics to owner distribution/underwriting context; Tribute copy is guest-generic. |
| Source confidence | 3 | 4 | Tribute approved facts are thin headline extractions from one Bonvoy page; references use denser program-specific facts. |
| UI density score (1–5) | 3 | 5 | Section passes contract minimum but reads sparse in side-by-side Explorer comparison. |
| Readiness for row enhancement | Conservative yes / Rich blocked | Reference-native | Conservative patch can run now; rich tier detail needs pending fact approval first. |

## Conservative copy (approved facts only)

### loyalty.earn

Earn and redeem points that take you everywhere you want to go.
Enjoy exclusive discounted rates for Marriott Bonvoy Members.
Participation, earn rates, and channel rules follow published Bonvoy terms for each property.

Illustrative examples only — actual earn/redeem rules vary by market, brand, property, and booking channel.

### loyalty.redeem

Earn and redeem points that take you everywhere you want to go.
Reward-night availability and point requirements vary by property, date, and brand participation.
Confirm redemption mechanics for a specific Tribute asset against current Bonvoy materials.

Illustrative examples only — actual earn/redeem rules vary by market, brand, property, and booking channel.

## Rich copy (pending facts — founder approval required)

### loyalty.earn

Members earn Marriott Bonvoy points on eligible hotel charges at participating properties when enrolled and booked per program rules.
Earn and redeem points that take you everywhere you want to go.
Enjoy exclusive discounted rates for Marriott Bonvoy Members.
Complimentary in-room Wi-Fi may be available when booking through Marriott websites or the Marriott Bonvoy app, where offered.
Earn toward free nights and discounted member rates within the Marriott Bonvoy program.
Direct Marriott / Bonvoy booking paths are where member-rate and published Wi-Fi benefits are typically positioned—confirm systems and parity rules for the property.

Illustrative examples only — actual earn/redeem rules vary by market, brand, property, and booking channel.

## Exact next step

```bash
npm run brand-explorer-bonvoy-loyalty-row-enhancement-writer -- --brand tribute-portfolio --package conservative --dry-run
```