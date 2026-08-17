# TEST 6 — Webhound vs Dealality reconciliation (READ-ONLY)

**Date:** 2026-08-04  
**Session:** `5922518e-c870-432f-ac29-700c99ff8dd4`  
**Scope:** Mexico / CALA — Hotel Indigo, Kimpton, Tribute Portfolio, Avani, Radisson Individuals Americas  
**Rule:** Proposed corrections only. **No SoT / Airtable writes.**

**Classification key:** Dealality appears correct · Webhound appears correct · Both partially correct · Evidence conflicting · Unable to determine

## Snapshot

| Brand | BE 54 Active/Live | Dealality census (affiliation) | Webhound verified |
|-------|-------------------|--------------------------------|-------------------|
| Hotel Indigo | Yes `recegXrqaPiSLGCIe` | 16 / 15 CALA | 8 (MX+CALA focus) |
| Kimpton Hotels | Yes `recCKuXCmGvxHPfb3` | 12 / 11 CALA | 5 |
| Tribute Portfolio | Yes `recCvV0PuZOi8c3hC` | 11 / 10 CALA | 7 |
| Avani | **No** | Census only (Cancún Airport) | 2 |
| Radisson Individuals by Choice | Yes `recRyvM8OmLlDj9G7` | 14 / 14 CALA | 16 CALA |

---

## 1. Hotel Indigo

| Item | Classification | Evidence / note | Proposed correction |
|------|----------------|-----------------|---------------------|
| La Paz, Guadalajara Expo, Guanajuato Open | Both partially correct | Aligned open | None on status |
| Playa del Carmen Pipeline → WH Operating | **Webhound appears correct** | IHG Mar 2026 MX press + directory | Verify bookability → Status Open |
| Tijuana Downtown Pipeline → WH Operating | **Webhound appears correct** | ihg.com/tijgc | Verify → Status Open |
| CDMX Downtown / SMA / Tulum / GDL Providencia pipelines | Dealality appears correct | WH noted pipelines but incomplete census of them | Keep Dealality pipelines; optional WH enrich |
| Lima Miraflores, Barbados Bridgetown Pipeline → WH Operating | **Webhound appears correct** | WH openings | Status hygiene |
| Grand Cayman dates/keys conflict inside WH | Evidence conflicting | May 2024 vs Dec 2025; 132 vs 282 | Manual directory check before any key update |
| Parent IHG / Choice N/A | Both partially correct | Aligned | None |

---

## 2. Kimpton

| Item | Classification | Evidence / note | Proposed correction |
|------|----------------|-----------------|---------------------|
| Virgilio, Mas Olas, El Castelar Open | Both partially correct | Aligned; Mas Olas Highgate operator from WH | Add Highgate operator if SoT field empty |
| **Aluna Tulum Open in Dealality; absent in WH** | **Evidence conflicting** | Steward: no safe IHG directory match (`tulka`) | Re-verify Aluna; may be stale Open |
| **Tres Ríos Riviera Maya in WH; absent Dealality amenity set** | **Evidence conflicting** | IHG `pcmht`; first Kimpton AI | Add census row if directory live; do not merge with Aluna |
| Seafire, Las Mercedes, East Cape / Monterrey pipelines | Dealality appears correct | WH CALA incomplete | Keep Dealality; optional follow-up WH CALA pass |
| Grand Roatán Operating | Both partially correct | Vista Capital owner from WH | Enrich owner if empty |
| Parent IHG | Both partially correct | Aligned | None |

---

## 3. Tribute Portfolio

| Item | Classification | Evidence / note | Proposed correction |
|------|----------------|-----------------|---------------------|
| Mystique Holbox Tribute | Both partially correct | Blue Diamond/Royalton from WH | Enrich operator if empty |
| Casa Nizuc Cancún pipeline (Sep 2026) | **Webhound appears correct** | WH pipeline; mixed-use Aldea Nizuc | Add pipeline census candidate |
| Mexico City Alameda / Mérida Tribute rows | Dealality appears correct / Unable | WH MX incomplete | Keep pending Marriott verify |
| Tulum Tribute vs Design Hotels page TQOXT | **Evidence conflicting** | Steward URL Design Hotels | Fix wrong brand assignment if confirmed |
| Crystal Cove + Turtle Beach Barbados Autograph/Elegant → WH Tribute 2026 | **Webhound appears correct** | Marriott/PR Newswire | Re-affiliate to Tribute if directory confirms |
| Rumbao, Ermita, Humano Lima | Both / WH richer on Lima | Driftwood on Rumbao | Enrich as needed |
| Parent Marriott soft collection | Both partially correct | Aligned | None |

---

## 4. Avani

| Item | Classification | Evidence / note | Proposed correction |
|------|----------------|-----------------|---------------------|
| **Absent from BE Active/Live** | Dealality product state | Not WH error | **Propose Tab Factory intake** (no auto-create) |
| Avani Cancún Airport Open | Both partially correct | NH conversion | Enrich conversion note |
| Avani Bogotá Zona T | Both partially correct | NH conversion | Ensure census Affiliation Avani |
| Parent Minor / Choice N/A | Both partially correct | Aligned | None |
| No further Americas pipeline | Both partially correct | WH | None |

---

## 5. Radisson Individuals Americas (Choice)

| Item | Classification | Evidence / note | Proposed correction |
|------|----------------|-----------------|---------------------|
| Zero Mexico | Both partially correct | Aligned | None |
| Choice Americas franchisor + RHG outside Americas | Both partially correct; WH richer | Aug 2022 acquisition; Choice Privileges merge | Strengthen BE regional structure narrative |
| Faranda CO/PA spine | Both partially correct | Dealality 14 vs WH 16 | Add V Grand Medellín + Faranda Collection Cartagena if still on Choice.com |
| 15 vs 16 Americas count tension | Evidence conflicting | Fixture 15 hotels vs WH 16 CALA rows | Resolve geography (US vs CALA) before total updates |
| Oct 2024 relaunch | Both partially correct | Choice media | Momentum refresh if stale |

---

## Cross-Airtable contradiction themes

1. Census Status Pipeline vs live brand directory Open (Indigo, possibly others).  
2. Census Affiliation Autograph/Elegant vs live Tribute (Barbados).  
3. Census Affiliation Tribute vs Design Hotels property page (Tulum).  
4. Census brand present (Avani) without Brand Explorer profile.  
5. Kimpton property identity conflict (Aluna vs Tres Ríos).  
6. OE: Faranda operates Individuals CALA but no Faranda OE pack (known gap).

---

## Do not apply

No corrections applied in this task. Next step if approved: dry-run steward packs per brand, then gated apply.
