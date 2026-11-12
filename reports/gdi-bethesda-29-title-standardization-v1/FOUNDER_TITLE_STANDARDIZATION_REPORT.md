# Bethesda 29 — Opportunity Title Standardization V1

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Contract:** `gdi-bethesda-29-title-standardization-v1`  
**Baseline:** GitHub main after PR #182 (`f7072eba7`)

## 1. Existing GDI title convention

Strong canonical Bethesda titles use:

`{Specific event / activity} {year?} — {honest status / venue / lodging context}`

Examples from the live 59-row store:

- `AFCEA Bethesda Health IT Summit 2027 — booked at Bethesda North Marriott (overflow / 2028 cycle)`
- `SHOW 2026 — NIH Research Conference on Sleep and the Health of Women (Oct 14–16)`
- `NIH SBPO Vendor Outreach Sessions — recurring Bethesda on-site pattern (next date TBD after April 2026 session)`

Titles are **not** organization + taxonomy category. Category lives in filter/label fields; the title carries commercial specificity.

## 2–4. Before / after crosswalk (29)

| # | ID | Before | After | Changed |
|---|----|--------|-------|---------|
| 1 | `disp_…_greater_bethesda_chamber_of_commerce` | Greater Bethesda Chamber of Commerce — Local Catering & Day Meetings | Greater Bethesda Chamber — Off the Clock & membership receptions | Y |
| 2 | `gdi_e2d_770e3b44ad97c1ae` | Greater Bethesda Chamber of Commerce — Local Catering & Day Meetings | Greater Bethesda Chamber — Local meetings research continuation | Y* |
| 3 | `disp_…_montgomery_county_shrm` | … — Local Catering & Day Meetings | Montgomery County SHRM — Chapter meetings & holiday programs | Y |
| 4 | `disp_…_pmi_montgomery_county_maryland_chapter` | … — Local Catering & Day Meetings | PMI Montgomery County — Monthly dinner meetings & FedTALKS | Y |
| 5 | `disp_…_city_of_rockville` | … — Local Catering & Day Meetings | City of Rockville — Civic meetings research continuation | Y* |
| 6 | `disp_…_nature_forward` | … — Local Catering & Day Meetings | Nature Forward — Community meetings research continuation | Y* |
| 7 | `gdi_s4912_59382d1e71a2` | Montgomery College — AI Skills Summit — Local Catering… | Montgomery College AI Skills Summit — Future edition research | Y |
| 8 | `gdi_s4912_d55ee8a8e9ca` | NIDDK — Meetings & Workshops portfolio — Local Catering… | NIDDK Meetings & Workshops — Scientific program portfolio | Y |
| 9 | `gdi_s4912_6c8a62552bfa` | Maryland Tech Council — ICON Awards — Corporate Celebrations… | Maryland Tech Council ICON Awards 2027 — Bethesda North host (overflow watch) | Y |
| 10 | `gdi_s4912_6ffc303bf55a` | … An Evening for Hope Gala — Corporate Celebrations… | Children's Inn at NIH — An Evening for Hope Gala (2027 venue TBD) | Y |
| 11 | `disp_…_strathmore` | Strathmore — Preferred Lodging Partnership (Partnership Watch) | Strathmore — Preferred lodging partnership watch | Y |
| 12 | `disp_…_cohera_…` | Cohera … — Travel Trade… (Partnership Watch) | Cohera — DMV group intermediary partnership watch | Y |
| 13 | `disp_…_linder_global_events` | … — Travel Trade… (Partnership Watch) | LINDER Global Events — Travel-trade partnership watch | Y |
| 14 | `gdi_e2d_bc0a94d7c6007f36` | Visit Montgomery — Preferred Lodging… (Partnership Watch) | Visit Montgomery — Destination lodging partnership watch | Y |
| 15 | `gdi_plp_live_fa2063dc1195` | Rock Creek Mansion — Preferred Lodging… | Rock Creek Mansion — Preferred lodging partnership watch | Y |
| 16 | `gdi_plp_live_4958569d826e` | The Cordell — Preferred Lodging… | The Cordell — Preferred lodging partnership watch | Y |
| 17 | `gdi_pep_live_af0711e711ba` | Howerton+Wooten… — Professional Event Planner… | Howerton+Wooten Events — Planner partnership watch | Y |
| 18 | `gdi_pep_live_c165c897d233` | Potomac Events / Potomac Meetings and Events | Potomac Meetings and Events — Planner partnership watch | Y |
| 19 | `disp_…_aledade` | Aledade — Corporate Meetings & Incentives | Aledade — Corporate meetings research continuation | Y* |
| 20 | `disp_…_iwlca_…` | IWLCA… — Sports & Entertainment | IWLCA Champions Cup 2027 — SoccerPlex stay-to-play (overflow watch) | Y |
| 21 | `disp_…_parenteral_drug_association_pda_` | PDA — Corporate Meetings & Incentives | PDA TRI Bethesda training — December 2026 lodging research | Y |
| 22 | `gdi_s4912_66a1f89e5afb` | NIH Intramural Postdoctoral… — Relocation & Extended Stay | NIH OITE Postdoctoral Fellows — Relocation & extended stay | Y |
| 23 | `gdi_s4912_364713d41c67` | NIH IRP / DIS — Relocation & Extended Stay | NIH Division of International Services — Relocation & extended stay | Y |
| 24 | `gdi_s4912_732496c2f8aa` | CASSS — 2027 Cell & Gene… (Overflow Watch) | CASSS Cell & Gene Therapy Symposium 2027 — Bethesda North host (overflow watch) | Y |
| 25 | `gdi_s4912_512deebd0963` | 2027 U.S. IFE Conference — Overflow / preferred lodging… | 2027 U.S. IFE Conference — Bethesda North host (overflow watch) | Y |
| 26 | `gdi_beth_v16_evt_3c8228774517` | 2027 U.S. IFE Conference | 2027 U.S. IFE Conference — Overflow lodging research | Y |
| 27 | `gdi_beth_v16_evt_90a6ddcb10ca` | Kappa Kappa Psi & Tau Beta Sigma National Convention 2027 | *(unchanged — already meets standard)* | N |
| 28 | `gdi_beth_v16_evt_0ab91e208405` | IFPAC-2027 — Advanced Manufacturing Science & Innovation | *(unchanged — already meets standard)* | N |
| 29 | `gdi_s4912_b0ea89e0716d` | Global Exposome Summit 2027 | Global Exposome Summit 2027 — Overflow lodging research | Y |

\*Intentionally limited specificity where evidence does not support a sharper activity claim.

Full machine crosswalk: `reports/gdi-bethesda-29-title-standardization-v1/00-title-crosswalk.json`.

## 5. Titles corrected

- **27** titles rewritten to the commercial standard
- **2** already met the standard (kept)
- **4** intentionally remain relatively generic (`research continuation`) due to missing activity evidence

## 6. Root cause + fix

Poor titles came from cleanup-era `titleFor` → `{Org} — {Category Label}`.

**Narrow fix:**

1. Evidence-grounded map in `bethesda-29-title-standardization-v1.js`
2. Research-stage merge apply (title / displayTitle / drawerHeaderTitle only)
3. `titleFor` in commercial cleanup now prefers standardized titles and no longer bolts taxonomy onto titles
4. Previous titles preserved under `titleStandardization.previousTitle` + provenance

## 7–9. Integrity

| Check | Result |
|-------|--------|
| Unified internal list | **88** (59 + 29) |
| Historical titles match map | **29 / 29** |
| Canonical `opportunities.json` | **unchanged** SHA `fd4539d9…` (restored 59-row) |
| Five Scaling Companies IDs | **preserved** |
| Exa batch on PDA / Chamber / Potomac | **`exaAuthorizedBatch` + contacts preserved** |
| Customer publication | **not authorized** / not Sales Ready |
| Paid research | **none** |

## 10–12. Delivery

- Feature branch → PR → merge (this package)
- Local `:8080` restarted on merged main after merge
- Browser verify uses **mocked** `/api/me` (Memberstack not claimed)
