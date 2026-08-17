# Operator Explorer — batch Masters (2026-07-24)

Founder list processed: Tafer, Grupo Presidente, Highgate, Grupo Hotelero Santa Fe, Arriva AHG, Arbor Lodging, Brittain BRH, Atlantica AHI.

## Status

| Operator | Master | Domain | Queue | Notes |
| --- | --- | --- | --- | --- |
| Arbor Lodging (CALA) | `recF5Z87OAqFgndoq` | arborlodging.com | **quality baseline** | Already golden — not re-created |
| Tafer Hotels & Resorts | `recJ6NPSYveCTo3At` | taferresorts.com | queued | Scaffolded |
| Grupo Presidente | `recJtFkhjaO57rSDC` | grupopresidente.com.mx | queued | Scaffolded |
| Highgate | `recLjxtxIIVJaGbXK` | highgate.com | queued | Scaffolded — label CALA vs enterprise |
| Grupo Hotelero Santa Fe | `reckyv9O0Y3auYpJJ` | gsf-hotels.com | queued | Scaffolded |
| Arriva Hospitality Group (AHG) | `reck6gjQd3wdeugmZ` | arrivahotels.mx | queued | Scaffolded |
| Brittain Resorts & Hotels (BRH) | `receHCdI6CEsJqdG4` | brittainresorts.com | queued | US Southeast — confirm CALA relevance |
| Atlantica Hotels International (AHI) | `recfwDdU5t9h4uFnZ` | atlanticahotels.com.br | queued | Scaffolded |

Also already in factory: GHL + Aimbridge (`factory_ready` / founder review).

## Commands

```bash
# Masters (already applied)
npm run create-operator-explorer-batch-masters -- --dry-run

# Scaffold stubs
npm run operator-explorer-factory-init -- --operators tafer-hotels-resorts,grupo-presidente,highgate,grupo-hotelero-santa-fe,arriva-hospitality-group,brittain-resorts-hotels,atlantica-hotels-international --dry-run

# Next: Tab Factory content (Arbor/HE bar) per operator
npm run operator-explorer-os -- --dry-run
```

Explorer URLs: `/operator-explorer-gold-mock.html?id={recordId}`
