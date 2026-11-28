# Memmo Príncipe Real — Proposed Competitive Hotel Set

**Status:** PROPOSED — awaiting founder approval (Phase C item **A**)  
**Subject hotel:** Memmo Príncipe Real · `rec1t35e3sPerzxYu` · 41 keys · Design Hotels · Príncipe Real, Lisbon  
**Config write:** **not applied** until founder GO  

No Lisbon peer GDI hotel configs exist today. No Hotel Explorer competitive-set rows were found for Memmo. Canonical census IDs below are **TBD** until HPC resolve (read-only search authorized after GO; do not invent `rec…` IDs).

---

## Proposed peers

| # | Full hotel name | Class | Canonical ID | Location | Brand / affiliation | Scale | Why include |
|---|---|---|---|---|---|---|---|
| 1 | Memmo Alfama | Direct / sibling brand | TBD | Alfama, Lisbon | Design Hotels | Boutique (~small) | Same Design Hotels family; lifestyle + leisure + small-group overlap |
| 2 | Palácio Ramalhete | Direct boutique | TBD | Estrela / near Príncipe Real | Independent luxury boutique | Small palace hotel | Geographic adjacency; similar boutique scale and leisure/corporate mix |
| 3 | Independente Palácio São Bento | Lifestyle boutique | TBD | São Bento, Lisbon | Independente / lifestyle | Palace-hostel hybrid | Creative / lifestyle / leisure demand overlap; not identical product |
| 4 | The Vintage Lisbon | Upscale boutique | TBD | Central Lisbon | Lifestyle / upscale boutique | Mid boutique | Central Lisbon boutique; small-group + corporate transient overlap |
| 5 | Hotel da Estrela | Aspirational / nearby | TBD | Estrela, Lisbon | Small luxury | Small luxury | Nearby small luxury; aspirational peer for positioning |

### Classification legend

- **Direct** — commercially substitutable for many Memmo RFPs (scale + neighborhood + lifestyle).  
- **Aspirational** — higher or adjacent positioning; useful CAD signal, not always win/loss peer.  
- **Other commercially relevant nearby** — demand corridors / creative leisure overlap without claiming identical STR set.

---

## What founder must decide

1. Accept / edit / remove peers above.  
2. Confirm whether **Memmo Alfama** (sibling Design Hotels) is allowed in CAD competitive research.  
3. Authorize config write of approved peers into `competitiveContext.directCompetitors` / `aspirationalCompetitors` (and optional `relevantGroupDemandAlternatives`).  
4. Optionally authorize HPC lookup to attach canonical hotel IDs where available — **without** creating duplicate hotel records.

Until approved and written, Full E2E CAD with `requireCompetitiveSet: true` remains **BLOCKED_NO_COMPETITIVE_SET** (correct fail-closed behavior).

---

## After approval

Competitive set is read automatically by:

- `runCanonicalCadOptInStage` / Full E2E Live CAD  
- Weekly refresh CAD path  
- Gold-standard gate `GDI_COMPETITIVE_SET_CONFIGURED`  

No Memmo-specific one-shot engine.
