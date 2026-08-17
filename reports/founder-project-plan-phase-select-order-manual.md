# Fix Phase section order in Airtable views

## The issue

**Roadmap Sort** on each row is correct (e.g. `1.01`, `5.03`). Row order is wrong when the view **groups by Phase (name)** because Airtable uses the **Phase select option list order**, which was:

1. Strategy & Foundations  
2. **Platform Design** ← too early  
3. **Platform Build**  
4. Content & GTM  
…  
10. **GTM / Outreach** ← should be ~5  

That is separate from **Phase Number** (1–15) and **Roadmap Sort**.

## Quick fix (recommended)

In **Executive Roadmap (Ordered)**:

1. **Remove** group by **Phase** (name) if set.
2. **Sort:** `Roadmap Sort` → ascending (only sort rule).
3. **Optional sections:** Group by **Phase Number** → ascending (NOT Phase name).

Phase Number groups: `1`, `2`, `3` … `15` in roadmap order.

## Optional: fix Phase select option order

So **Group by Phase** (name) also works:

1. Founder Project Plan → **Phase** column → **Edit field**.
2. Drag options into this order:

| # | Phase |
|---|--------|
| 1 | Strategy & Foundations |
| 2 | Product Definition |
| 3 | Strategy & Design |
| 4 | Resources / Collateral |
| 5 | GTM / Outreach |
| 6 | Pilot Conversion |
| 7 | Pilot Delivery |
| 8 | Product / Access |
| 9 | Platform Design |
| 10 | Platform Build |
| 11 | Content & GTM |
| 12 | Testing & Pilot |
| 13 | Launch & Operations |
| 14 | Scale & Optimize |
| 15 | Later |

3. Leave unused options (Pilot Readiness, Airtable / Data) at the bottom.
4. Save.

## Verify

- Sort **Roadmap Sort** ↑ → rows run `1.01…1.33`, then `2.01…`, then `5.01…`, etc.
- Group **Phase Number** ↑ → sections 1, 2, 3, 5, 6… in order.
