# Brand Explorer Remaining Editorial Slot Completion Writer v21B

Generated: 2026-07-08T22:38:37.518Z
Mode: **dry-run** · Airtable modified: **no**
Brand: Tribute Portfolio `recCvV0PuZOi8c3hC`

## Scope
- Slot keys targeted: **13**
- Write targets (rows): **15**
- Would create: **0**
- Would update: **0**
- Matched (no-op): **15**
- Leaked excluded slots: **0**
- Wording risks remain: **no**

## insight.similar row model
- Model: **multiple_presentation_rows** (3 rows)
- Rationale: Completed brands (e.g. Curio) and Brand Explorer UI (explorerCardRowsForSlot) use multiple Brand Explorer Presentation rows sharing slot key insight.similar — Title = peer brand name, Body = qualitative diligence subtitle. v21B creates/updates three rows, not one structured Body row.

## Score projection
- Projected score after apply: **73/100**
- Completed-brand comparable after apply: **no**

## Guardrails
- Images untouched: **yes**
- Brand Basics untouched: **yes**
- Company Validated untouched: **yes**

## Preflight (sample)
- `hero.benefit_zones` · matched · risk: clear · title: — · body: Conversion & repositioning · Resort & leisure destinations · Urban character ma...
- `hero.operator_compat` · matched · risk: clear · title: — · body: Full-service, resort, and lifestyle operators experienced with soft-collection ...
- `insight.similar[0]` · matched · risk: clear · title: Curio Collection by Hilton · body: (Hilton · soft collection · conversion-oriented peer for diligence)
- `insight.similar[1]` · matched · risk: clear · title: Autograph Collection · body: (Marriott · cross-parent soft collection · independent-character benchmark)
- `insight.similar[2]` · matched · risk: clear · title: Unbound Collection by Hyatt · body: (Hyatt · independent-character collection · experiential positioning peer)
- `overview.bestAt.1` · matched · risk: clear · title: Conversion & Repositioning · body: Independent and boutique assets where local identity and design narrative are t...
- `overview.bestAt.2` · matched · risk: clear · title: Resort & Leisure · body: Experience-led resorts and leisure destinations where F&B, design, and sense of...
- `overview.bestAt.3` · matched · risk: clear · title: Urban Character · body: Distinctive urban hotels where neighborhood story, design point of view, and in...
- `overview.differentiators.commercial` · matched · risk: clear · title: — · body: Marriott Bonvoy participation Marriott reservation and commercial support Colle...
- `overview.differentiators.identity` · matched · risk: clear · title: — · body: Independent character and local sense of place Design-forward guest experience ...
- `overview.owner_experience` · matched · risk: clear · title: — · body: Owners retain design and local programming latitude within collection standards...
- `overview.scenarios` · matched · risk: clear · title: — · body: Resort and leisure repositioning where an independent or tired resort needs Mar...
- `overview.why_value` · matched · risk: clear · title: — · body: Preserves independent identity while adding Bonvoy and Marriott commercial infr...
- `standards.conversion` · matched · risk: clear · title: — · body: Owner diligence framing: sequence conversion PIP scope, heritage or design cons...
- `standards.deal_inputs` · matched · risk: clear · title: — · body: Room count and mix · New build vs. conversion · Prior flag · PIP scope and timi...

## Apply command (gated)

```bash
npm run brand-explorer-remaining-editorial-slot-completion-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-editorial-slot-completion-v21B
```
