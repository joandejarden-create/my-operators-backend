# Operator Explorer — Arbor + Hotel Equities Quality Baseline

> **Status:** Binding golden quality freeze  
> **Freeze id:** `frozen_2_operator_quality_baseline`  
> **Decision date:** 2026-07-24  
> **Usage rules:** `docs/data-intelligence/operator-explorer-protected-baseline-rules.md`

Arbor Lodging (CALA) and Hotel Equities (CALA) are the **product quality baselines** for all future Operator Explorer profiles. New operators must reach the same quality **tab by tab and field by field** — not merely “has content.”

---

## 1. Frozen operators

| Slug | Display name | Master record ID | Explorer |
|------|--------------|------------------|----------|
| `arbor-lodging-cala` | Arbor Lodging (CALA) | `recF5Z87OAqFgndoq` | `/operator-explorer-gold-mock.html?id=recF5Z87OAqFgndoq` |
| `hotel-equities-cala` | Hotel Equities (CALA) | `recWPKu5laVZxsvpn` | `/operator-explorer-gold-mock.html?id=recWPKu5laVZxsvpn` |

**Registry:** `lib/partner-intelligence/operator-explorer-quality-baseline.js`  
**PI pilot registry:** `api/lib/partner-intelligence-explorer-field-registry.js` → `PILOT_OPERATORS`

---

## 2. What “same quality” means

Ask for every visible rendered field on every publishable tab:

> Is this field complete, operator-specific, owner-useful, source-supported or intentionally suppressed, and **comparable to Arbor / Hotel Equities**?

Hard fails (same spirit as Brand Explorer Tab Factory):

- Visible empty fields, empty cards, title-only cards
- Generic copy that could apply to another operator by name-swap
- Unsupported zeros presented as facts
- Parent/enterprise umbrella copy dominating CALA-specific sections without labeling
- “Tab loaded” without section pattern parity

`auditPass = true` only when `failFindings === 0`. A patch plan is **not** a pass.

---

## 3. Publishable tabs in scope (10)

Aligned to `OPERATOR_EXPLORER_TABS` / registry catalog:

1. Profile & Positioning  
2. Operating Platform  
3. Brand & Relationships  
4. Markets & Footprint  
5. Owner Engagement & Reporting  
6. Infrastructure & Data  
7. Leadership  
8. Project Fit & Deal Profile  
9. Proof & Track Record  
10. Operator Materials  

**Out of publish-scope quality merge (still must not blank-break UI):**

- Dealality Insights (platform-derived)  
- Alignment Context (OAS; optional)

---

## 4. Fixture / content packs (reference)

### Arbor Lodging (CALA)

| Pack | Path |
|------|------|
| Profile | `fixtures/operator-profile-explorer-arbor-cala.json` |
| Operating | `fixtures/operator-operating-explorer-arbor-cala.json` |
| Brand | `fixtures/operator-brand-explorer-arbor-cala.json` |
| Markets | `fixtures/operator-markets-explorer-arbor-cala.json` |
| Engagement | `fixtures/operator-engagement-explorer-arbor-cala.json` |
| Leadership | `fixtures/operator-leadership-explorer-arbor-cala.json` |
| Best fit | `fixtures/operator-best-fit-arbor-cala.json` |
| Diligence Q&A | `fixtures/operator-diligence-qa-arbor-cala.json` |

### Hotel Equities (CALA)

| Pack | Path |
|------|------|
| Operating | `fixtures/operator-operating-explorer-he-cala.json` |
| Brand | `fixtures/operator-brand-explorer-he-cala.json` |
| Infrastructure | `fixtures/operator-infrastructure-explorer-he-cala.json` |
| Engagement | `fixtures/operator-engagement-explorer-he-cala.json` |
| Leadership | `fixtures/operator-leadership-explorer-he-cala.json` |
| Recognition | `fixtures/operator-recognition-explorer-he-cala.json` |

Use **both** packs when judging parity. Where one operator is thicker on a tab, that thickness sets the bar; the other remains protected and must not regress.

---

## 5. Relationship to Partner Intelligence

| Layer | Role |
|-------|------|
| **PI Source Library + Extracted Facts** | Evidence / stewardship |
| **Profile governance publish** | Trust chip / Validation Status / Confidence |
| **Operator Explorer Tab Factory + this baseline** | Owner-facing tab/field product quality |

A profile can be PI-eligible and still **fail** Operator Explorer quality gates. Both layers are required for `active_profile_ready`.

---

## 6. Commands (as implemented)

```bash
# Unit contract: expected baseline set + record IDs
npm run test:operator-explorer-quality-baseline
npm run test:operator-explorer-tab-factory-audit

# Fixture / live / merged tab-factory audit (dry-run)
npm run operator-explorer-tab-factory-audit -- --operators arbor-lodging-cala,hotel-equities-cala --source=fixtures --dry-run
npm run operator-explorer-tab-factory-audit -- --operators arbor-lodging-cala,hotel-equities-cala --source=merged --dry-run
```

Reports: `reports/operator-explorer-tab-factory-audit.{json,md}`

**Note:** `--source=fixtures` scores fixture packs only. Many Master/Profile scalars live in Airtable — use `--source=merged` for product-facing completeness vs the full registry.
---

## 7. Baseline revision protocol

To change the protected set or intentionally rewrite a golden:

1. Explicit task stating baseline revision.  
2. Dry-run audits before/after.  
3. Update this MD + `operator-explorer-quality-baseline.js` (+ fixture notes).  
4. Re-run `test:operator-explorer-quality-baseline`.  
5. Do **not** absorb regressions silently.

---

## Change impact

**High** — quality SoT for Operator Explorer factory and all future operator profiles.

**Rollback:** Restore prior freeze MD + JS registry; disable factory writes that touched goldens.
