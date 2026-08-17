# Why Operator Lists Differ

## Exact current counts (Airtable-backed reconciliation)

| List | Count | What it is |
| ---- | ----: | ---------- |
| Airtable Operator Masters | **46** | Every Master row |
| Production | **24** | Record Purpose Production |
| Research | **13** | Record Purpose Research |
| Test Fixtures | **9** | Synthetic |
| Real operators | **37** | Production + Research |
| Calibration 01 | **27** | Architecture test cohort |
| Brand Basics parents | **34** | Brand-parent discovery set |
| Explorer content-complete | **17** | Content gates pass |
| Explorer Publishable (canonical) | **9** | Production ∩ content-complete |
| Fit Data Ready (diagnostic) | **4** | Diagnostic only |

## Simple examples

- **MxM** is not a Master — it is an alias of Marriott International (Managed).
- A **Test Fixture** exists in Airtable (46) but is not in the real universe (37).
- A **Brand Basics parent** like Preferred Hotels may have **No Direct Management** — no Operator Master required.
- A **Research** operator (e.g. Four Seasons) may be **content-complete** but still **not Explorer Publishable** until Production graduation.
- **Calibration 27** is a research sample, not the whole Master universe (46).
