# Founder Evidence — Universal Heading Conjunction (`&`)

**Date:** 2026-11-19  
**Workspace:** `C:\Dev\dealality\ws-brand-explorer`  
**URL:** `http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1`  
**Airtable writes:** **0**  
**Curio / Ascend:** **NOT APPROVED**

---

## Founder rule

White section / subsection / card titles use Proper Title Case and join related concepts with **`&`**, not standalone word `And`.

| Incorrect | Correct |
|-----------|---------|
| Commercial Platform And Lobby Experience | Commercial Platform & Lobby Experience |
| Independent & Soft-Brand Conversion | *(already correct — preserved)* |

Body paragraphs keep natural language `and`. Compact tokens such as `F&B` are preserved.

---

## Implementation

Extended **existing** `BE_EDITORIAL_TITLE_CASE_V1` only (`lib/brand-explorer/be-editorial-title-case-v1.js` + browser mirror):

1. `normalizeHeadingConjunctionAmpersand` — spaced `and`/`And`/`AND` → ` & `
2. Applied inside `toEditorialTitleCase` (headings only; sentence-final `.` / long prose skipped)
3. Cumulative assembler runs Title Case **last** on RIGHT so advisory writers cannot reintroduce `And`
4. Insight advisory source titles updated to `&`
5. Highlight Changes: `normTitleIdentity` unifies `and`/`&` for card pairing; fingerprint keeps raw text so And→& is **MODIFIED**, not silent unchanged / remove+add

Contract: `configs/brand-explorer-permanent-content-standards-contract-v1.json` → `typographyAndWriting.sectionHeadings.conjunction = "&"`

---

## Browser validation

### Curio — Overview · Where This Brand Creates the Most Value

| Side | `overview.scenario.2` title | Highlight |
|------|-----------------------------|-----------|
| LEFT (CURRENT) | Commercial Platform **And** Lobby Experience | preserved storage |
| RIGHT (PROPOSED) | Commercial Platform **&** Lobby Experience | `data-becs-hl=modified` |

`Independent & Soft-Brand Conversion` unchanged on both sides.

Screenshot: `screenshots/01-curio-scenario-and-to-ampersand.png`

### Ascend — Economics

RIGHT section/card titles use `&` (`Opening & Conversion Path`, `Cash & Capital Rhythm`, `Term, Renewal & Exit Risk`, `Application & Feasibility`, `Design & Standards`). No standalone heading `And` on RIGHT. `Local F&B Preserved` remains compact.

Screenshot: `screenshots/02-ascend-headings-ampersand.png`

---

## Gates

```bash
npm run test:be-editorial-title-case-v1
```

Fixtures:

- `fixtures/brand-explorer-title-case-regressions/failing-founder-screenshot-headings.json`
- `fixtures/brand-explorer-title-case-regressions/heading-conjunction-ampersand.json`

Lesson: `BE-QL-HEADING-AMPERSAND-001` → `BROWSER_VERIFIED`
