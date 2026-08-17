# Airtable View Audit — Operator Setup - Master

| View name | Table | Filter logic | Record count | Universe represented | Misleading? |
| --------- | ----- | ------------ | -----------: | -------------------- | ----------- |
| Grid view | Operator Setup - Master | grid (API does not expose filter formula for all view types) | 46 (unfiltered Grid) | Full Master table when no filter | Neutral default — not an OE universe |

## Finding

Live meta API currently exposes only **Grid view** on `Operator Setup - Master`. There is **no** dedicated OE Production / Research / Publishable view yet — founder confusion is expected if comparing Calibration-27 docs, Brand Basics-34 sheets, and the unfiltered 46-row Grid.

If a Webflow/internal page titled “Operating Companies” shows a subset, it is almost certainly **application filters**, not Record Purpose views (those views do not exist yet).
