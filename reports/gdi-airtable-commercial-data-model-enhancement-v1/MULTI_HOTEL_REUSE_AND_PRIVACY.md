# Multi-hotel reuse and privacy

## Shared (global)

Demand Generators, Programs, Signals, Private Event Venues/Signals, public evidence — reusable across hotels.

## Hotel-private

GDO rows (`hotelId`), Hotel Fit rows, Decisions/Events, FS `feedback.json`, commercial profiles, need periods, sales notes, rates, outcomes.

## Rules

1. One credible demand occurrence → one shared org/program/signal + N hotel GDOs.  
2. Never merge hotel GDOs into one global opportunity.  
3. Never sync hotel feedback into Demand Generators.  
4. Customer publication packages remain hotel-scoped SoT for customer UI.  
5. YOTEL shows disjoint FS vs Airtable IDs — reconcile carefully; no silent import (PR #363).
