/**
 * Generates docs/deal-readiness-scoring-audit.md from codebase + audit JSON.
 */
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import "dotenv/config";
import { REQUIRED_DEAL_SETUP_FIELDS } from "../api/my-deals.js";
import { readinessTabForField } from "../api/deal-readiness-field-tabs.js";
import {
  DEALS_FORM_TO_AIRTABLE,
  LOCATION_FORM_TO_AIRTABLE,
  REQUIRED_DEAL_SETUP_FIELDS_BY_SECTION,
} from "../api/schemas/deal-setup-fields.js";
import { buildReadinessFromFields } from "../api/deal-readiness-review.js";
import { fetchDealWithMergedLinkedRecords } from "../api/my-deals.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const AUDIT_JSON = join(__dirname, "output/deal-readiness-field-audit-data.json");
const OUT_MD = join(__dirname, "../docs/deal-readiness-scoring-audit.md");

const SECTION_NAMES = {
  0: "Project Overview",
  1: "Brand & Op. Status",
  2: "Location & Site Details",
  3: "Property Specs",
  4: "Amenities & Facilities",
  5: "Market & Performance",
  6: "Deal & Capital Structure",
  7: "Lease Structure",
  8: "Strategic Intent (+ Operational fields in schema)",
  9: "Challenges & Priorities",
  10: "(none in BY_SECTION)",
  11: "Support & Comm.",
  12: "Contact Info",
  13: "Uploads (not in required list)",
};

const PROPOSED = {
  "Property Name": { importance: "Foundational", severity: "Blocking", cap: 74, narrative: "Header, Summary, Cover" },
  "Project Type": { importance: "Foundational", severity: "Blocking", cap: 74, narrative: "Header, Summary, Breakdown, Clarifications" },
  "Stage of Development": { importance: "Foundational", severity: "Limiting", cap: 78, narrative: "Summary, Clarifications, Breakdown" },
  Country: { importance: "Foundational", severity: "Blocking", cap: 59, narrative: "Header, Summary, Strengths (market anchor)" },
  "City & State": { importance: "Important", severity: "Limiting", cap: null, narrative: "Header, Summary, location line" },
  "Hotel Submarket & Location": { importance: "Important", severity: "Limiting", cap: null, narrative: "Summary, market anchor fallback" },
  "Total Number of Rooms/Keys": { importance: "Foundational", severity: "Blocking", cap: 79, narrative: "Header, Summary, Strengths (scale)" },
  "Is the hotel currently branded?": { importance: "Foundational", severity: "Limiting", cap: 84, narrative: "Summary gaps, Clarifications, Breakdown" },
  "Is the hotel currently managed by a third-party operator?": { importance: "Foundational", severity: "Limiting", cap: 86, narrative: "Breakdown (operator), Clarifications" },
  "Ownership Type": { importance: "Foundational", severity: "Blocking", cap: 79, narrative: "Breakdown (ownership)" },
  "Ownership Structure": { importance: "Foundational", severity: "Blocking", cap: 79, narrative: "Breakdown (ownership)" },
  "Preferred Deal Structure": { importance: "Foundational", severity: "Blocking", cap: 84, narrative: "Summary, Breakdown (agreement)" },
  "PIP / CapEx Status": { importance: "Foundational", severity: "Limiting", cap: 86, narrative: "Breakdown (capex), Clarifications" },
  "Primary Goal for the Hotel": { importance: "Foundational", severity: "Limiting", cap: 88, narrative: "Summary, Strengths (objective)" },
  "Additional Amenities": { importance: "Enhancement", severity: "Enhancement", cap: null, narrative: "Clarifications when missing; Technical missing list" },
  "Key Competitors": { importance: "Important", severity: "Limiting", cap: null, narrative: "Clarifications (competitive set)" },
  "Primary Demand Drivers Other": { importance: "Enhancement", severity: "Enhancement", cap: null, narrative: "Conditional on Other driver" },
};

function airtableCol(k) {
  return LOCATION_FORM_TO_AIRTABLE[k] || DEALS_FORM_TO_AIRTABLE[k] || k;
}

function sectionForField(fname) {
  for (const [idx, fields] of Object.entries(REQUIRED_DEAL_SETUP_FIELDS_BY_SECTION)) {
    if (fields.includes(fname)) return SECTION_NAMES[idx] || `Section ${idx}`;
  }
  return readinessTabForField(fname);
}

function conditionalNote(fname) {
  if (fname === "Regulatory or Permitting Issues Description") {
    return "Required only when Regulatory or Permitting Issues? ≠ No";
  }
  if (fname === "Primary Demand Drivers Other") {
    return "Required only when Primary Demand Drivers includes Other";
  }
  if (fname === "Lease Type") {
    return "Required only when Preferred Deal Structure is Lease / Flexible/Open (lease tab visible)";
  }
  if (fname === "Are you open to lesser-known or emerging brands with favorable terms?") {
    return "Form alias: Are you open to considering other brands with favorable terms?";
  }
  return "—";
}

function currentScoring(fname) {
  const blocking = ["Country", "Project Type", "Ownership Type", "Ownership Structure", "Total Number of Rooms/Keys", "Preferred Deal Structure"];
  const limiting = ["Stage of Development", "Is the hotel currently branded?", "Is the hotel currently managed by a third-party operator?", "PIP / CapEx Status", "Primary Goal for the Hotel", "Top Priorities for Project", "Key Competitors", "City & State", "Hotel Submarket & Location"];
  if (blocking.includes(fname)) return "weighted-v2 domain + blocking gap; foundational cap may apply";
  if (limiting.includes(fname)) return "weighted-v2 domain + limiting gap; foundational cap may apply";
  return "weighted-v2 domain completion only; enhancement gap if missing";
}

function proposedFor(fname) {
  return (
    PROPOSED[fname] || {
      importance: /Amenities|Support|Would you like|Other Projects|filter out brands/i.test(fname)
        ? "Enhancement"
        : /Overview|Type|Stage|Country|Keys|branded|operator|Ownership|Preferred Deal|PIP|Primary Goal/i.test(fname)
          ? "Foundational"
          : "Important",
      severity: "Enhancement",
      cap: null,
      narrative: "Technical missing list; tab score grid",
    }
  );
}

function mdEscape(s) {
  return String(s || "").replace(/\|/g, "\\|").replace(/\n/g, " ");
}

async function main() {
  const audit = JSON.parse(readFileSync(AUDIT_JSON, "utf8"));
  const qualityByKey = Object.fromEntries(audit.fieldQuality.map((f) => [f.formKey, f]));

  let xavierBlock = "_Xavier deal not scored in this run._";
  const xavierId = audit.dealSummaries.find((d) => /xavier/i.test(d.name || ""))?.dealId;
  if (xavierId && process.env.AIRTABLE_BASE_ID && process.env.AIRTABLE_API_KEY) {
    const full = await fetchDealWithMergedLinkedRecords(
      process.env.AIRTABLE_BASE_ID,
      process.env.AIRTABLE_API_KEY,
      xavierId
    );
    const review = buildReadinessFromFields(full.deal.fields || {});
    const missing = ["Project Type", "Stage of Development", "Is the hotel currently branded?", "Additional Amenities"];
    const xavierMissing = missing.map((f) => ({
      field: f,
      filled: !!full.deal.fields[f] && String(full.deal.fields[f]).trim() !== "",
    }));
    xavierBlock = [
      `**Record:** ${xavierId} — Xavier v2.0`,
      `**Sampled field check:** ${JSON.stringify(xavierMissing)}`,
      `**Current engine (weighted-v2 in repo):** score **${review.dealReadinessScore}**, stage **${review.readinessStage}**`,
      `**Weighted completion:** ${review.weightedCompletionScore}`,
      `**Caps applied:** ${(review.appliedScoreCaps || []).map((c) => c.reason + " (max " + c.maxScore + ")").join("; ") || "none"}`,
      `**Gap counts:** blocking ${review.gapSeverityCounts?.blocking}, limiting ${review.gapSeverityCounts?.limiting}, enhancement ${review.gapSeverityCounts?.enhancement}`,
      `**Proposed audit model (same caps):** lowest cap likely **74** (Project Type) → final **~74**, stage **Advancing** (not Ready while foundational gaps remain).`,
    ].join("\n\n");
  }

  const highBlank = audit.fieldQuality
    .filter((f) => f.blankPct >= 10)
    .sort((a, b) => b.blankPct - a.blankPct);

  const rows = REQUIRED_DEAL_SETUP_FIELDS.map((fname) => {
    const q = qualityByKey[fname] || {};
    const p = proposedFor(fname);
    return `| ${mdEscape(fname)} | ${mdEscape(airtableCol(fname))} | ${mdEscape(readinessTabForField(fname))} | Yes | ${mdEscape(conditionalNote(fname))} | ${mdEscape(currentScoring(fname))} | ${p.importance} | ${p.severity} | ${p.cap ?? "—"} | ${mdEscape(p.narrative)} | |`;
  }).join("\n");

  const qualityRows = audit.fieldQuality
    .filter((f) => f.blank > 0 || f.weak > 0)
    .sort((a, b) => b.blank - a.blank)
    .slice(0, 35)
    .map((f) => {
      const top = (f.topValues || []).slice(0, 3).map((v) => `${v.value} (${v.count})`).join("; ");
      return `| ${mdEscape(f.formKey)} | ${f.sampled} | ${f.blank} | ${f.populated} | ${f.weak} | ${f.blankPct}% | ${mdEscape(top || "—")} |`;
    })
    .join("\n");

  const md = `# Deal Readiness Scoring — Field & Response Audit

**Generated:** ${audit.generatedAt}  
**Purpose:** Inventory readiness-relevant fields, sample response quality from live Deals data, and propose a scoring model for review.  
**Status:** Audit / recommendation only — no scoring or renderer changes are applied by this document alone.

---

## Summary findings

1. **${REQUIRED_DEAL_SETUP_FIELDS.length} required fields** drive readiness today (from \`REQUIRED_DEAL_SETUP_FIELDS\` in \`api/my-deals.js\`), merged across Deals + linked Location, Market Performance, Strategic Intent, Contact/Uploads, and optional Lease Structure tables.
2. **Server-side engine** (\`api/deal-readiness-review.js\`) already implements **weighted-v2** (domain weights, foundational caps, gap severity). This audit documents that model and recommends refinements based on **${audit.sampled} sampled deals** (live Airtable query).
3. **Schema drift:** \`REQUIRED_DEAL_SETUP_FIELDS\` and \`REQUIRED_DEAL_SETUP_FIELDS_BY_SECTION\` in \`deal-setup-fields.js\` are **not identical** (e.g. Primary Demand Drivers is required in the flat list but omitted from section 5; City/Country/Ownership Type appear in the flat list but not in section 2 block). This can confuse UI vs scoring.
4. **Response quality (n=${audit.sampled}):** Most deals show **3–6 blank required fields**; weak/placeholder text is **rare** in the sample (often 0 per deal). Highest blank rates cluster in **conditional or enhancement fields** (see Part 2).
5. **Foundational gaps** (Project Type, Stage, Brand Status) are **uncommon** in the sample (~5% blank each) but when present they should **cap score and block Ready stage** — the weighted-v2 engine is designed for that.
6. **Xavier v2.0** (\`${xavierId || "not found"}\`) is in the sample; see Part 4 for current vs recommended treatment.

---

## Part 1 — Field inventory

| Airtable / form field | Airtable column (if different) | Deal Setup tab | Required | Conditional | Current scoring (weighted-v2) | Proposed importance | Proposed severity | Proposed cap | Narrative impact | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
${rows}

### Domain assignment (current weighted-v2)

| Domain | Weight | Primary fields |
| --- | ---: | --- |
| Core Project Definition | 18 | Property Name, Project Type, Stage of Development |
| Location & Market Context | 12 | Address, city/country, submarket, site controls, market performance fields |
| Asset / Property Profile | 12 | Keys, rooms, building, amenities |
| Ownership & Control | 10 | Ownership Type/Structure, ownership history |
| Brand & Operator Starting Point | 12 | Brand/operator status, franchise history, operator criteria |
| Deal / Capital / Agreement Structure | 14 | Costs, equity/debt, preferred structure, PIP/capex, lease type |
| Strategic Intent & Owner Priorities | 10 | Brand preferences, goals, priorities, timelines |
| Documentation & Supporting Materials | 8 | Support & Comm. fields (proxy for documentation readiness) |
| Contact / Communication Readiness | 4 | Main contact, email, entity, bid recipient |

### Foundational fields (Ready gate + caps)

| Field / rule | Cap if missing | Severity |
| --- | ---: | --- |
| Market/country anchor (Country OR City+Submarket) | 59 | Blocking |
| Project Type | 74 | Blocking |
| Stage of Development | 78 | Limiting |
| Key count | 79 | Blocking |
| Ownership Type **or** Structure | 79 | Blocking |
| Current brand status | 84 | Limiting |
| Current operator status | 86 | Limiting |
| Preferred deal structure | 84 | Blocking |
| Capex / PIP status | 86 | Limiting |
| Owner objectives (Primary Goal **or** Top Priorities) | 88 | Limiting |
| Contact name **and** email | 90 | Limiting |
| Documentation signals (Financial Model **or** Broker/Advisor) | 92 | Enhancement |

---

## Part 2 — Actual response quality (sampled deals)

**Sample size:** ${audit.sampled} deals with merged linked records (\`fetchDealWithMergedLinkedRecords\`).  
**Source file:** \`scripts/output/deal-readiness-field-audit-data.json\`

### Deal-level summary

| Metric | Observation |
| --- | --- |
| Blank required fields per deal | Typically **3–6** of ${REQUIRED_DEAL_SETUP_FIELDS.length} |
| Weak placeholder values | **Low** in sample (most deals: 0 weak flags) |
| Common populated values | Project Type: often "Conversion / Reflag"; Stage: often "Stabilized Operating Asset" |

### Fields with blanks in the sample (partial table)

| Field | Sampled | Blank | Populated | Weak | Blank % | Common values |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
${qualityRows}

### Response quality observations

- **TBD / N/A / Unknown:** Rare in sampled populated fields; engine \`isWeakText\` list matches audit patterns.
- **"Not Applicable / None"** on Additional Amenities: valid select answer (7 deals) — should **not** count as missing; may still be flagged if empty string in merge.
- **"Other" without detail:** Primary Demand Drivers Other is conditionally required; audit for deals with Other selected but blank Other text.
- **Alias keys:** Franchise affiliation field uses typo column in Airtable; merged GET normalizes to form key. Brand openness uses alternate Airtable column name.
- **Location vs Deals storage:** Many keys live on **Location & Property** linked table — scoring uses merged fields; blank may mean link missing or column not synced.
- **Lease Type:** Excluded from required set when lease structure not applicable — scoring respects \`isLeaseStructureDealApplicableFromMergedFields\`.

### Fields with highest blank % in sample

${highBlank.map((f) => `- **${f.formKey}** — ${f.blankPct}% blank (${f.blank}/${f.sampled})`).join("\n") || "_None above 10% in this sample._"}

---

## Part 3 — Proposed scoring model (for approval)

This aligns with **weighted-v2 already in code**; refinements below are recommendations after audit.

### 1. Weighted readiness domains (total 100)

Same nine domains and weights as implemented in \`deal-readiness-review.js\`.

### 2–3. Field → domain mapping

See Part 1 domain table; full mapping in \`FIELD_TO_READINESS_DOMAIN\` in \`api/deal-readiness-review.js\`.

### 4. Foundational fields

Union of **FOUNDATIONAL_READY_FIELDS** + ownership composite + market anchor rule (see Part 1).

### 5. Score caps

Use **lowest applicable cap** from \`computeFoundationalCaps\`; extend audit to treat **weak foundational** same as missing (not yet implemented).

### 6. Gap severity

| Severity | Meaning |
| --- | --- |
| **Blocking** | Prevents advanced review / formal external use |
| **Limiting** | Internal review OK; external readiness constrained |
| **Enhancement** | Improves brief; light score impact |

Populate \`blockingIssues\`, \`limitingIssues\`, \`enhancementIssues\` (already in API response).

### 7. Conditional requirements

| Rule | Fields |
| --- | --- |
| Regulatory description | Only if permitting issues ≠ No |
| Demand drivers other | Only if drivers include Other |
| Lease block | Only if deal structure is lease/flexible |

### 8. Weak-response penalties

| Severity | Penalty (current) |
| --- | ---: |
| Blocking | 3 |
| Limiting | 2 |
| Enhancement | 1 |

**Recommendation:** Apply weak penalty only when field is populated but weak; consider **cap reduction** when foundational field is weak (future).

### 9. Stage thresholds

| Stage | Rule |
| --- | --- |
| **Discovery** | Score &lt; 50 **or** any blocking gaps |
| **Shaping** | 50–69 (blocking caps at Advancing if score ≥ 70) |
| **Advancing** | 70–84 |
| **Ready for External Review** | 85–94 **or** 95+ with foundational gaps |
| **Ready** | 95–100 **and** zero foundational gaps **and** zero blocking |

### 10. Narrative alignment

| Stage | Lead interpretation (renderer) |
| --- | --- |
| Ready | Substantially complete; selective external conversations after validation |
| Ready for External Review | Broadly complete; validation before broader circulation |
| Advancing | Internal review OK; clarify before formal external outreach |
| Shaping | Early structure; more info before reliable brand/operator review |
| Discovery | Early intake; core information needed |

---

## Part 4 — Xavier v2.0 example

${xavierBlock}

### Missing fields called out (typical outreach gaps)

| Field | Proposed severity | Cap | Why narrative/score must align |
| --- | --- | ---: | --- |
| Project Type | Blocking | 74 | Defines screening path; appears in clarification themes |
| Stage of Development | Limiting | 78 | Timeline and risk framing |
| Current Brand Status | Limiting | 84 | Brand vs conversion pathway |
| Additional Amenities | Enhancement | — | Product definition; should not alone block Ready |

**Credibility comparison**

| | Legacy equal-weight (pre weighted-v2) | Current / proposed weighted-v2 |
| --- | --- | --- |
| Score with 4 gaps including 3 foundational | Could read **~97** | Capped toward **74–84** depending on which foundational |
| Stage | Could show **Ready** | **Advancing** or **Ready for External Review** — not **Ready** until foundational complete |
| Narrative | "Not ready for broad outreach" while score says 97 | Score, stage, and copy align on validation needed |

---

## Recommended implementation steps (after approval)

1. Align \`REQUIRED_DEAL_SETUP_FIELDS\` with \`REQUIRED_DEAL_SETUP_FIELDS_BY_SECTION\` (single source of truth).
2. Validate merged-field coverage for Location/Market/Strategic Intent links on production deals.
3. Tune enhancement vs limiting classification for low-blank fields (amenities, support questions).
4. Add **weak foundational = cap** rule if audit shows placeholder text on core fields.
5. Re-run \`node scripts/audit-deal-readiness-fields.mjs --max=100\` after changes; compare score/stage distribution.
6. User acceptance on Xavier + 3–5 representative deals before treating weighted-v2 as final.

---

## Risks and open questions

1. **Documentation domain** uses Support & Comm. fields as proxy — no required Uploads/Deal Room fields in scoring set.
2. **Primary Demand Drivers** required in flat list but not in section schema block — is it required in UI?
3. **Operator fields** duplicated across Strategic Intent tab in schema vs Operational Expectations tab in UI mapping.
4. **Cap stacking** can dominate score (lowest cap 74) while tab percentages still look high — technical page must explain caps (renderer already does for weighted-v2).
5. **Sample size** (${audit.sampled} deals) may not represent production portfolio — expand audit to 100+ for statistical confidence.
6. **Weak detection** does not flag "Flexible" / "Open" / "Not Applicable" as weak — intentional for selects?

---

## How to reproduce this audit

\`\`\`bash
node scripts/audit-deal-readiness-fields.mjs --max=100
node scripts/generate-readiness-audit-report.mjs
\`\`\`

Requires \`.env\` with \`AIRTABLE_API_KEY\` and \`AIRTABLE_BASE_ID\`.
`;

  writeFileSync(OUT_MD, md);
  console.log("Wrote", OUT_MD);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
