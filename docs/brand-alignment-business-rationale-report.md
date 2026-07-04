# Brand Alignment — business rationale layer (implementation report)

## Files changed

| File | Change |
|------|--------|
| `lib/brand-alignment-rationale.js` | Owner-facing rationale paragraphs, business **What Supports Review** bullets, pathway-specific **Key Consideration**, deduped brand questions, softened commercial-incentive labels, **Alignment Factors Reviewed** (technical) |
| `public/js/brand-alignment-snapshot.js` | Brand card sections A–F; technical factors separated; page-1 table uses business **Key Consideration** only |
| `public/css/brand-alignment-snapshot.css` | Technical factor subsection styling |
| `scripts/test-brand-alignment-summary.mjs` | Validation for new fields, banned phrasing, live/mock checks |
| `lib/sample-opportunity-deal-schema.js` | `buildAirtableFieldMap`, `formatAirtableFieldMapMarkdown` |
| `fixtures/sample-deals/harborline-airport-amsterdam.example.json` | Airtable-ready Amsterdam-style demo (fictional **Harborline Airport Hotel**) |
| `fixtures/sample-deals/sparse-deal-minimal.example.json` | Sparse-input QA fixture |
| `fixtures/sample-deals/no-brand-universe.example.json` | Empty brand universe QA fixture |
| `scripts/print-sample-deal-airtable-map.mjs` | Markdown Airtable map export |

**Not changed (per request):** Match Score New, numeric scoring engine, brand universe rules, modal/iframe shell, print/PDF, Deal Readiness Snapshot.

---

## Technical vs business separation

| Layer | Where it appears | Content |
|-------|------------------|---------|
| **Owner-facing rationale** | Brand card §B; page-1 **Key Consideration** | Paragraph: tier + business themes + pathway implication + validation close |
| **What Supports Review** | Brand card §C | Generic business bullets (no `For [Brand], Chain scale…` repetition) |
| **What Needs Validation** | Brand card §D | Weak-factor + default validation bullets |
| **What Could Weaken Alignment** | Brand card §E | Neutral watchouts |
| **Owner Questions This Brand Raises** | Brand card §F | Pathway-specific only (generic items moved to shared section) |
| **Alignment Factors Reviewed** | Brand card (technical subsection) | Factor label + assessment + short owner explanation |
| **Common Questions to Clarify Before Outreach** | Page 2 §3 | Shared list (`COMMON_QUESTIONS_BEFORE_OUTREACH`) |

Removed from prominent card body: duplicate **Main Alignment Signals** list and **Potential Alignment Signals** as primary rationale.

---

## Before / after examples (mock)

### Curio Collection by Hilton

**Before (undesired pattern):**  
“appears aligned based on current inputs for Chain scale alignment, Project type compatibility, Development stage alignment.”

**After — alignmentRationale:**  
“Curio Collection by Hilton currently shows a Higher Alignment Signal because target positioning and chain scale, project type fit, development stage timing, and owner preference alignment appear directionally compatible with available brand reference data. Curio Collection by Hilton may be relevant if the owner wants Hilton distribution and loyalty while keeping a distinct property story and design narrative. Before outreach, the owner should validate brand standards and PIP expectations, deal setup completeness, building type / asset form, and operating model compatibility.”

**After — keyConsideration:**  
“Collection-style path may be relevant if the owner wants distribution support while preserving property identity; standards and PIP expectations should be confirmed.”

**After — whatSupportsReview (sample):**
- Target positioning appears directionally aligned with the brand's chain scale.
- Project type appears compatible with available brand reference data.
- Development stage appears compatible with the brand's typical pathway expectations.

### Moderate / conditional (e.g. Radisson RED mock)

**After — keyConsideration:**  
“Lifestyle positioning appears directionally aligned; validate F&B, public space, and programming expectations before outreach.”

**After — alignmentRationale (opening):**  
“Radisson RED currently shows a Higher Alignment Signal because project type fit, target positioning and chain scale, and owner preference alignment appear directionally compatible…”

### Live Amsterdam deal (`recjS6htuIpEBmzFE`)

**Four Points by Sheraton (conditional):**
- **keyConsideration:** “Conditional alignment on current inputs; clarify positioning, operating model, and commercial assumptions before outreach.”
- **Rationale:** Uses business theme list (project type fit, development stage timing, owner preference alignment, operating model fit) — not raw factor-name repetition as the main message.

---

## Repeated questions

Generic questions (soft vs hard brand, affiliation vs operator, loyalty vs flexibility, market review, standards/PIP) live only in **Common Questions to Clarify Before Outreach** on page 2. Per-brand **Owner Questions** are filtered when they match that shared set.

---

## Commercial incentives

Factor labels renamed to **Commercial incentive alignment** (not “Key money willingness”). Copy uses “commercial incentive assumptions should be confirmed directly with the brand” — no implication that key money will be offered.

---

## Sample Airtable fixtures

| Fixture | Purpose |
|---------|---------|
| `fixtures/sample-deals/harborline-airport-amsterdam.example.json` | Advancing / conditional alignment; fictional project name; 6 brand review candidates; intentional PIP/soft-hard gaps |
| `fixtures/sample-deals/sparse-deal-minimal.example.json` | Discovery / sparse inputs |
| `fixtures/sample-deals/no-brand-universe.example.json` | Empty `targetListRows` |

Export Markdown map:

```bash
node scripts/validate-sample-deal-fixture.mjs fixtures/sample-deals/harborline-airport-amsterdam.example.json
node scripts/print-sample-deal-airtable-map.mjs fixtures/sample-deals/harborline-airport-amsterdam.example.json
```

---

## Validation

```bash
node scripts/test-brand-alignment-summary.mjs
```

Confirms: no recommendation language, no old factor-list rationale pattern, `alignmentFactorsReviewed` present, business bullets in `whatSupportsReview`, differentiated Curio vs Radisson RED key considerations.

---

## Confirmation

No “recommended”, “best fit”, “prioritize this brand”, or “selected brand” recommendation language was added. Copy uses **may merit review**, **appears directionally compatible**, and **owner/advisor validation** framing throughout.
