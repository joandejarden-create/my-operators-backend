# Company Profile — Year Founded audit

Generated from Airtable Company Profile (`tblItyfH6MlOnMKZ9`). Field: **Year Founded** (single-line text).

## Summary

| Category | Count |
|----------|------:|
| Total companies | 76 |
| Had year (review only) | 68 |
| Empty before populate | 8 |
| **Populated (high confidence)** | **7** |
| **Left blank (no verified source)** | **1** |

---

## Empty → populated (applied unless you reverted)

| Company | Year | Source note |
|---------|------|-------------|
| Arbor Lodging | **2006** | HospitalityNet / industry profiles cite 2006 founding (NVN merge 2017). |
| Bridgepoint Hospitality (Caribbean) Ltd | **2005** | Assumed same hospitality investor as [bridgepointhospitality.com](https://www.bridgepointhospitality.com/) (“formed in 2005”). Confirm if this Barbados entity is distinct. |
| Invest Costa Rica | **2010** | Investor listings / firm profile. |
| Mullen Real Estate Capital | **2021** | [mullencap.com](https://mullencap.com/about/) / Tracxn. |
| Newbond Holdings | **2021** | [newbond.com](https://www.newbond.com/team) / Tracxn. |
| Sonesta International Hotels Corporation | **1937** | A.M. Sonnabend / Preston Beach Hotel origin (rebranded Sonesta 1970). |
| Sygnus Group | **2016** | Incorporated June 2016; public launch July 2017. |

### Still empty (needs your input)

| Company | Why blank |
|---------|-----------|
| **Cachagua Group** | Website is “Launching Soon” only; no founding year found. Do not guess. |

---

## Populated years — please confirm before any changes

**Do not update these in Airtable until you confirm.** Status: **OK** = matches common public “founded” narrative; **Review** = likely wrong or off by 1 year; **Ambiguous** = defensible multiple dates (heritage vs current entity).

### Review — recommend changing if you want “tourism/hospitality entity” year

| Company | Current | Suggested | Notes |
|---------|---------|-----------|-------|
| **Grupo Iberostar** | 1877 | **1956** | 1877 = Fluxá family shoe workshop; **Grupo Iberostar tourism founded 1956** (official). |
| **Festiva Resorts** | 1999 | **2000** | PRNewswire / industry sources: founded summer **2000**. |

### Ambiguous — keep current unless you standardize a rule

| Company | Current | Alternatives | Notes |
|---------|---------|--------------|-------|
| **InterContinental Hotels Group** | 1777 | 2003 / 2004 | 1777 = Bass Brewery lineage (IHG corporate history); **IHG plc as hotel group ≈ 2003**. |
| **Whitbread** | 1742 | ~2015 (Premier Inn focus) | 1742 = Samuel Whitbread brewery; hospitality REIT narrative is much later. |
| **Marriott International** | 1927 | 1957 | 1927 = Hot Shoppes; **first Marriott hotel 1957**. |
| **Ryman Hospitality Properties** | 1925 | 2012 | 1925 = Ryman Auditorium; **hospitality REIT spin ~2012**. |
| **Radisson Hotel Group** | 1960 | 1962 | Brand site uses 1960; Wikipedia/Carlson hotel purchase often cited **1962**. |

### OK — spot-checked (no change suggested)

Accor 1967, Aimbridge 2003, AO Hospitality Advisors 2023, Ashford 2003, Barbados Hotel & Tourism Association 1952, Barceló 1931, Bay Gardens 1995, Braemar 2013, BWH 1946, Caesars 1937, Chatham Lodging 2010, Choice 1939, Dealality demos 2020, Design Hotels 1993, Diamond Resorts 2007, DiamondRock 2004, Driftwood 2015, DRI Hotels 2007, Eurostar Hotels Group 2005, Extended Stay 1995, Four Seasons 1960, G6 1962, GHL 1964, Grandi 1995, H World 2005, Highgate 1988, Hilton 1919, Host 1993, Hotel Equities 1989, Hyatt 1957, IAS 1995, Leading Hotels 1928, Limestone 2018, Mandarin Oriental 1963, Marriott 1927, Meliá 1956, MGM 1986, Minor 1978, Orange Lake 1982, Park Hotels 2017, Preferred 1968, Red Roof 1973, Remington 1968, RLJ 2011, Rosewood 1979, Scandic 1963, Shangri-La 1971, SLH 1989, Steigenberger 1930, Strategic Hotels 1997, Summit 2011, Sunstone 1995, TUI 1923, Vail 1962, Welk 1964, Westgate 1982, WorldHotels 1971, Wyndham 1981, Xenia 2015, Grupo Marta 1960.

*(“OK” = no obvious error on quick check; not a full audit of all 68.)*

---

## How to apply or re-run

```bash
node scripts/audit-company-year-founded.mjs
node scripts/populate-company-year-founded.mjs          # dry-run
node scripts/populate-company-year-founded.mjs --apply  # write empties only
```

To fix **Review** rows after you confirm, update **Year Founded** in Airtable or ask for a one-off patch script with your chosen years.
