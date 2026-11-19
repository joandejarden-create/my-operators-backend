# GDI Slice 2A — Persistence Honesty Results

Generated: 2026-11-19T12:42:51.608Z

## Safety
- Paid calls: 0
- Production Airtable writes: 0
- Schema mutations: none

## Before / after field retention
### National Association of Development Organizations (NADO) / DDAA (`gdi_opp_nado_ddaa_washcon_2028`)
- Contacts before → after: Brittany Salazar, Kar'ron Grant, NADO conference staff → Brittany Salazar, Kar'ron Grant, NADO conference staff
- Evidence URLs retained: 3/3
- Payload chars: 2720 (limit 95000)

### International Society for Cell & Gene Therapy (`rb_gdi_disc_dc428c1bd4c4e412`)
- Contacts before → after: Giovanni Guizzetti → Giovanni Guizzetti
- Evidence URLs retained: 15/15
- Payload chars: 6372 (limit 95000)

### ICPIC — International Consortium for Prevention & Infection Control (`gdi_opp_yotel_fe2e_icpic_20270914`)
- Contacts before → after: Lara Schneider → Lara Schneider
- Evidence URLs retained: 23/23
- Payload chars: 6942 (limit 95000)


## Guards
- Thin processed row fails closed: **PASS**
- ORS reload recovers full opportunity: **PASS**
- Idempotent retry → UNCHANGED: **PASS**
- Sales/confidential keys stripped: **PASS**

## Remaining gaps / FS risks
- Typed contact/evidence columns on GDI Hotel Research Opportunities still JSON-only
- DG/Program/Signal/Fit relationship wiring deferred to Slice 2B
- Airtable DG reuse remains off by default
- ORS files under data/group-demand-intelligence/hotels/*/opportunity-research-state are gitignored and local-workspace-only unless mirrored
- Non-http source paths in evidence are flagged as filesystemOnlyEvidenceRefs and are not production-durable URLs

## Slice 2B
READY_FOR_FOUNDER_REVIEW — Slice 2A dry-run honesty complete; do not start 2B without approval
