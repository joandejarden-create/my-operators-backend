#!/usr/bin/env node
/**
 * GDI Full Semantic & Commercial Consistency Audit V1 — runner
 *
 * Phases:
 *   1) inventory reconciliation
 *   2) render final customer-facing drawers (DealalityGdiUi)
 *   3) cross-section meaning checks
 *   4) semantic LLM/agent reviews (batch files + optional paid OpenAI)
 *   5) aggregate deliverables under reports/gdi-full-semantic-commercial-audit-v1/
 *
 * Flags:
 *   --authorize-paid-semantic   allow OpenAI chat.completions spend
 *   --model <name>              default gpt-4o-mini when paid authorized
 *   --batch-size <n>            default 25
 *   --hotel <id>                limit to one hotel
 *   --max-opps <n>              cap opportunities (debug)
 *   --reviews-only              skip re-render; load packs from disk
 *   --apply-presentation-fixes  apply shared sanitizer fixes only (no factual rewrites)
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_FULL_SEMANTIC_COMMERCIAL_AUDIT_V1,
  buildSemanticAuditInventory,
  renderAllSemanticPacks,
  emptySemanticReview,
  detectCrossSectionSemanticIssues,
  computeScorecard,
  writeJson,
  defaultReportDir,
  SEMANTIC_SECTION_IDS,
} from "../lib/group-demand-intelligence/customer-consistency/gdi-full-semantic-commercial-audit-v1.js";
import { classifyOpportunityTypeFilter, OPPORTUNITY_TYPE_FILTER } from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = defaultReportDir();

function argVal(flag, fallback = null) {
  const i = process.argv.indexOf(flag);
  if (i < 0) return fallback;
  return process.argv[i + 1] ?? fallback;
}
function hasFlag(flag) {
  return process.argv.includes(flag);
}

function compactPackForReview(pack) {
  const sections = {};
  for (const id of SEMANTIC_SECTION_IDS) {
    const t = String(pack.sections?.[id] || "").trim();
    if (t) sections[id] = t.slice(0, 2500);
  }
  return {
    hotelId: pack.hotelId,
    hotelName: pack.hotelName,
    opportunityId: pack.opportunityId,
    displayTitle: pack.displayTitle,
    customerVisibility: pack.customerVisibility,
    inventoryPopulations: pack.inventoryPopulations,
    demandSegmentId: pack.demandSegmentId,
    opportunityTypeFilter: pack.opportunityTypeFilter,
    card: pack.card,
    sections,
    populatedSections: pack.populatedSections,
    fullDrawerText: String(pack.fullDrawerText || "").slice(0, 6000),
    crossSectionFindings: pack.crossSectionFindings || [],
    deterministicContradictions: pack.deterministicContradictions?.findings || [],
  };
}

const SEMANTIC_SYSTEM = `You are auditing Dealality Group & Demand Intelligence customer-facing opportunity copy for a hotel sales director.
Read EVERY provided section. Identify contradictions, unsupported claims, duplication, unclear language, commercially misleading recommendations, card-vs-drawer conflicts, and evidence gaps.
Do NOT invent external facts. Do NOT change qualification policy. Infer only from provided text.
Return STRICT JSON only for one opportunity:
{
  "hotelId": "...",
  "opportunityId": "...",
  "sectionsReviewed": ["..."],
  "reviewStatus": "REVIEWED",
  "scores": {
    "contentQuality": 0-100,
    "commercialAccuracy": 0-100,
    "consistency": 0-100,
    "readability": 0-100,
    "evidenceReliability": 0-100
  },
  "contradictions": [{"code":"...","severity":"P0|P1|P2|P3","sections":[],"message":"...","passages":{}}],
  "unsupportedClaims": [{"claim":"...","section":"...","why":"..."}],
  "duplicateOrUnnecessaryText": [{"text":"...","sections":[]}],
  "conflictingRecommendedActions": [{"message":"...","passages":{}}],
  "suggestedCorrections": [{"field":"...","before":"...","after":"...","reason":"...","requiresFounderReview":false}],
  "confidence": 0-1,
  "notes": "short"
}`;

async function reviewWithOpenAI(pack, { model, apiKey }) {
  const user = JSON.stringify(compactPackForReview(pack));
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model,
      temperature: 0.1,
      response_format: { type: "json_object" },
      messages: [
        { role: "system", content: SEMANTIC_SYSTEM },
        {
          role: "user",
          content: `Semantically review this GDI opportunity pack:\n${user}`,
        },
      ],
    }),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`OpenAI ${res.status}: ${body.slice(0, 400)}`);
  }
  const json = await res.json();
  const content = json.choices?.[0]?.message?.content || "{}";
  const parsed = JSON.parse(content);
  return {
    ...emptySemanticReview(pack, pack.crossSectionFindings || []),
    ...parsed,
    hotelId: pack.hotelId,
    opportunityId: pack.opportunityId,
    reviewStatus: "REVIEWED",
    reviewEngine: `openai:${model}`,
    evidence: {
      ...(parsed.evidence || {}),
      crossSectionFindings: pack.crossSectionFindings || [],
      deterministicContradictions: pack.deterministicContradictions?.findings || [],
    },
  };
}

/**
 * Deterministic-assist review used when paid LLM is not authorized.
 * Marks reviewStatus=HEURISTIC_ASSIST_ONLY — NOT a substitute for LLM semantic review.
 */
function heuristicAssistReview(pack) {
  const cross = pack.crossSectionFindings || detectCrossSectionSemanticIssues(pack);
  const det = pack.deterministicContradictions?.findings || [];
  const p0 = cross.filter((f) => f.severity === "P0").length;
  const p1 = cross.filter((f) => f.severity === "P1").length + det.filter((f) => f.severity === "CRITICAL" || f.severity === "HIGH").length;
  const consistency = Math.max(0, 100 - p0 * 35 - p1 * 15 - cross.length * 3);
  const readability = /Qualify Overflow\s*[—–-]\s*Qualify Overflow|Host hotel contracted|Webhound|Airtable/i.test(
    pack.fullDrawerText || ""
  )
    ? 55
    : 78;
  return {
    ...emptySemanticReview(pack, cross),
    reviewStatus: "HEURISTIC_ASSIST_ONLY",
    reviewEngine: "cross-section-heuristic-v1",
    scores: {
      contentQuality: null,
      commercialAccuracy: null,
      consistency,
      readability,
      evidenceReliability: null,
    },
    contradictions: cross.filter((f) => /VS_|MISMATCH|CONTRADICT|CONFIRMED_VS|HISTORICAL_VS|FIT_VS/i.test(f.code)),
    notes:
      "Heuristic assist only — full LLM/agent paragraph review still required before claiming audit complete.",
    confidence: 0.35,
  };
}

function buildFounderReport({ inventory, scorecard, contradictions, corrections, researchRequired, reviews, packs, paidAuthorized, limitations }) {
  const reviewed = reviews.filter((r) => r.reviewStatus === "REVIEWED").length;
  const heuristic = reviews.filter((r) => r.reviewStatus === "HEURISTIC_ASSIST_ONLY").length;
  const p0 = contradictions.filter((c) => c.severity === "P0").length;
  const p1 = contradictions.filter((c) => c.severity === "P1").length;
  return `# GDI Full Semantic & Commercial Consistency Audit V1

**Schema:** \`${GDI_FULL_SEMANTIC_COMMERCIAL_AUDIT_V1}\`  
**Generated:** ${new Date().toISOString()}  
**Baseline:** local \`main\` / consolidated recovery (PR #233)  
**Policy:** No merge, deploy, publish, qualification-policy change, or paid model spend beyond explicit authorization.

---

## 1. Executive assessment

| Dimension | Score |
|---|---|
| Technical consistency (prior Layer A–C presentation contracts) | ${scorecard.dimensions.technicalConsistency ?? "n/a"} |
| Semantic consistency | ${scorecard.dimensions.semanticConsistency ?? "**incomplete**"} |
| Commercial accuracy | ${scorecard.dimensions.commercialAccuracy ?? "**incomplete**"} |
| Evidence reliability | ${scorecard.dimensions.evidenceReliability ?? "**incomplete**"} |
| Readability | ${scorecard.dimensions.readability ?? "**incomplete**"} |
| Research completeness | ${scorecard.dimensions.researchCompleteness ?? "n/a"} |
| **Overall** | **${scorecard.overall ?? "WITHHELD — semantic coverage incomplete"}** |

**Verdict:** ${
    scorecard.coverage.complete
      ? "Semantic coverage is complete; see scores above."
      : `PARTIAL. ${reviewed}/${packs.length} opportunities have full LLM/agent REVIEWED status; ${heuristic} have heuristic-assist only. **Do not treat prior 100/100 presentation score as content quality.**`
  }

---

## 2. Inventory reconciliation vs prior audit

| Metric | Prior structural audit | This semantic audit |
|---|---|---|
| Hotels with opportunity inventory | 17 | ${inventory.totals.hotelsWithInventory} |
| Opportunities | 699 | ${inventory.totals.opportunities} |
| Parity-universe hotels excluded (no opportunity store) | n/a | ${inventory.totals.excludedParityHotels} |

### Excluded selectable hotels (no opportunities.json)
${
  inventory.excludedParityHotels.length
    ? inventory.excludedParityHotels
        .map(
          (h) =>
            `- \`${h.hotelId}\` — ${h.hotelName || "?"} (${(h.sources || []).join(", ")}): ${h.reason}`
        )
        .join("\n")
    : "_None_"
}

---

## 3. Coverage

- Opportunities rendered for drawer extraction: **${packs.length}**
- Sections contract: ${SEMANTIC_SECTION_IDS.length} named surfaces
- LLM/agent REVIEWED: **${reviewed}**
- Heuristic assist only: **${heuristic}**
- Paid OpenAI authorized this run: **${paidAuthorized ? "YES" : "NO"}**

---

## 4. Findings summary

| Severity | Count (aggregated contradictions) |
|---|---|
| P0 | ${p0} |
| P1 | ${p1} |
| Other | ${contradictions.length - p0 - p1} |
| Research-required rows | ${researchRequired.length} |

Machine-readable:
- \`01-semantic-opportunity-reviews.json\`
- \`02-contradictions.json\`
- \`03-content-corrections.json\`
- \`04-research-required.json\`
- \`05-quality-scorecard.json\`
- \`06-coverage-manifest.json\`
- \`07-rendered-packs/\` (batched compact packs for review)
- \`00-inventory.json\`

---

## 5. Scoring methodology

${JSON.stringify(scorecard.methodology, null, 2)}

Overall is **withheld** until every in-scope opportunity has \`reviewStatus=REVIEWED\` from a real semantic engine (Cursor agent batch or authorized paid model). Heuristic assist does **not** unlock overall scoring.

---

## 6. Limitations & authorization gates

${limitations.map((l) => `- ${l}`).join("\n")}

---

## 7. Release readiness

| Gate | Status |
|---|---|
| 100% inventory rendered | ${packs.length === inventory.totals.opportunities ? "PASS" : "FAIL"} |
| 100% semantic LLM/agent review | ${scorecard.coverage.complete ? "PASS" : "FAIL / OPEN"} |
| P0 customer-facing contradictions resolved | ${p0 === 0 ? "PASS / none open in aggregate file" : "OPEN"} |
| Paid research / model spend | ${paidAuthorized ? "Authorized for this run only" : "Not authorized — no OpenAI spend"} |
| Merge / deploy / publish | **NOT AUTHORIZED in this task** |

**Recommend:** Complete remaining semantic REVIEWED coverage via Cursor agent batches (zero external OpenAI) or authorize \`--authorize-paid-semantic\`, then remediate confirmed P0/P1 presentation defects at shared generators and re-score.
`;
}

async function main() {
  const paidAuthorized = hasFlag("--authorize-paid-semantic");
  const model = argVal("--model", "gpt-4o-mini");
  const batchSize = Number(argVal("--batch-size", "25")) || 25;
  const hotelFilter = argVal("--hotel");
  const maxOpps = argVal("--max-opps") ? Number(argVal("--max-opps")) : null;
  const reviewsOnly = hasFlag("--reviews-only");

  fs.mkdirSync(OUT, { recursive: true });
  fs.mkdirSync(path.join(OUT, "07-rendered-packs"), { recursive: true });
  fs.mkdirSync(path.join(OUT, "08-review-batches"), { recursive: true });
  fs.mkdirSync(path.join(OUT, "screenshots"), { recursive: true });

  console.log(`[semantic-audit] inventory…`);
  let inventory = await buildSemanticAuditInventory();
  if (hotelFilter) {
    inventory = {
      ...inventory,
      hotels: inventory.hotels.filter((h) => h.hotelId === hotelFilter),
      opportunities: inventory.opportunities.filter((o) => o.hotelId === hotelFilter),
      totals: {
        ...inventory.totals,
        hotelsWithInventory: 1,
        opportunities: inventory.opportunities.filter((o) => o.hotelId === hotelFilter)
          .length,
      },
    };
  }
  if (maxOpps != null) {
    inventory = {
      ...inventory,
      opportunities: inventory.opportunities.slice(0, maxOpps),
      totals: { ...inventory.totals, opportunities: Math.min(maxOpps, inventory.totals.opportunities) },
    };
  }
  writeJson(path.join(OUT, "00-inventory.json"), {
    ...inventory,
    opportunities: inventory.opportunities.map((o) => ({
      hotelId: o.hotelId,
      hotelName: o.hotelName,
      opportunityId: o.opportunityId,
    })),
  });

  let packs;
  let renderErrors = [];
  if (reviewsOnly && fs.existsSync(path.join(OUT, "07-rendered-packs", "all-packs.json"))) {
    packs = JSON.parse(
      fs.readFileSync(path.join(OUT, "07-rendered-packs", "all-packs.json"), "utf8")
    );
    console.log(`[semantic-audit] loaded ${packs.length} packs from disk`);
  } else {
    console.log(`[semantic-audit] rendering ${inventory.opportunities.length} drawers…`);
    const rendered = await renderAllSemanticPacks(inventory);
    packs = rendered.packs.map((p) => {
      const { deterministicContradictions, ...rest } = p;
      return {
        ...rest,
        deterministicContradictions: {
          findings: deterministicContradictions?.findings || [],
        },
        fullDrawerText: String(p.fullDrawerText || "").slice(0, 8000),
        sections: Object.fromEntries(
          Object.entries(p.sections || {}).map(([k, v]) => [k, String(v || "").slice(0, 3000)])
        ),
      };
    });
    renderErrors = rendered.errors;
    writeJson(path.join(OUT, "07-rendered-packs", "all-packs.json"), packs);
    writeJson(path.join(OUT, "07-rendered-packs", "render-errors.json"), renderErrors);
  }

  // Write review batches for Cursor agents
  const batches = [];
  for (let i = 0; i < packs.length; i += batchSize) {
    const slice = packs.slice(i, i + batchSize).map(compactPackForReview);
    const batchId = `batch-${String(Math.floor(i / batchSize) + 1).padStart(3, "0")}`;
    const file = path.join(OUT, "08-review-batches", `${batchId}.json`);
    writeJson(file, {
      schema: GDI_FULL_SEMANTIC_COMMERCIAL_AUDIT_V1,
      batchId,
      instruction:
        "Semantically review EVERY opportunity in opportunities[]. Write one REVIEWED object per opportunity to sibling *-reviews.json using the audit JSON schema.",
      system: SEMANTIC_SYSTEM,
      opportunities: slice,
    });
    batches.push({ batchId, file, count: slice.length });
  }
  writeJson(path.join(OUT, "08-review-batches", "manifest.json"), { batches });

  const reviews = [];
  const apiKey = String(process.env.OPENAI_API_KEY || "").trim();
  if (paidAuthorized) {
    if (!apiKey) {
      console.error("[semantic-audit] --authorize-paid-semantic set but OPENAI_API_KEY missing");
      process.exit(2);
    }
    console.log(`[semantic-audit] paid semantic reviews via ${model}…`);
    let n = 0;
    for (const pack of packs) {
      n += 1;
      try {
        const review = await reviewWithOpenAI(pack, { model, apiKey });
        reviews.push(review);
      } catch (e) {
        reviews.push({
          ...emptySemanticReview(pack, pack.crossSectionFindings || []),
          reviewStatus: "ERROR",
          reviewEngine: `openai:${model}`,
          notes: e.message,
        });
      }
      if (n % 10 === 0) console.log(`[semantic-audit] reviewed ${n}/${packs.length}`);
    }
  } else {
    console.log(
      "[semantic-audit] paid semantic NOT authorized — writing heuristic-assist reviews + Cursor batch files"
    );
    for (const pack of packs) {
      reviews.push(heuristicAssistReview(pack));
    }
  }

  // Aggregate contradictions
  const contradictions = [];
  for (const r of reviews) {
    for (const c of r.contradictions || []) {
      contradictions.push({
        hotelId: r.hotelId,
        opportunityId: r.opportunityId,
        displayTitle: r.displayTitle,
        ...c,
      });
    }
    for (const c of r.conflictingRecommendedActions || []) {
      contradictions.push({
        hotelId: r.hotelId,
        opportunityId: r.opportunityId,
        displayTitle: r.displayTitle,
        code: "CONFLICTING_RECOMMENDED_ACTION",
        severity: "P1",
        ...c,
      });
    }
  }
  for (const pack of packs) {
    for (const f of pack.crossSectionFindings || []) {
      if (
        !contradictions.some(
          (c) =>
            c.opportunityId === pack.opportunityId &&
            c.hotelId === pack.hotelId &&
            c.code === f.code
        )
      ) {
        contradictions.push({
          hotelId: pack.hotelId,
          opportunityId: pack.opportunityId,
          displayTitle: pack.displayTitle,
          ...f,
        });
      }
    }
  }

  const corrections = [];
  for (const r of reviews) {
    for (const s of r.suggestedCorrections || []) {
      corrections.push({
        hotelId: r.hotelId,
        opportunityId: r.opportunityId,
        ...s,
      });
    }
  }

  const researchRequired = [];
  for (const pack of packs) {
    if (pack.opportunityTypeFilter === OPPORTUNITY_TYPE_FILTER.OTHER_UNCLASSIFIED) {
      researchRequired.push({
        hotelId: pack.hotelId,
        opportunityId: pack.opportunityId,
        displayTitle: pack.displayTitle,
        reason: "Unclassified demand segment — research classification required",
        kind: "CLASSIFICATION",
      });
    }
    const rev = reviews.find(
      (r) => r.hotelId === pack.hotelId && r.opportunityId === pack.opportunityId
    );
    for (const u of rev?.unsupportedClaims || []) {
      researchRequired.push({
        hotelId: pack.hotelId,
        opportunityId: pack.opportunityId,
        displayTitle: pack.displayTitle,
        reason: u.why || u.claim,
        claim: u.claim,
        section: u.section,
        kind: "UNSUPPORTED_CLAIM",
      });
    }
  }

  // Prior technical presentation score was 100 on Layer A–C; keep as technical dimension only.
  const scorecard = computeScorecard({
    packs,
    reviews,
    researchRequiredCount: researchRequired.filter((r) => r.kind === "CLASSIFICATION")
      .length,
    technicalConsistencyScore: 100,
  });

  const coverageManifest = {
    schema: GDI_FULL_SEMANTIC_COMMERCIAL_AUDIT_V1,
    generatedAt: new Date().toISOString(),
    hotels: inventory.hotels,
    excludedParityHotels: inventory.excludedParityHotels,
    opportunities: packs.map((p) => ({
      hotelId: p.hotelId,
      opportunityId: p.opportunityId,
      displayTitle: p.displayTitle,
      populatedSections: p.populatedSections,
      sectionCount: (p.populatedSections || []).length,
      renderError: p.renderError || null,
      reviewStatus:
        reviews.find(
          (r) => r.hotelId === p.hotelId && r.opportunityId === p.opportunityId
        )?.reviewStatus || "MISSING",
      reviewEngine:
        reviews.find(
          (r) => r.hotelId === p.hotelId && r.opportunityId === p.opportunityId
        )?.reviewEngine || null,
    })),
    batches,
    renderErrors,
    totals: {
      hotels: inventory.hotels.length,
      opportunities: packs.length,
      sectionsContract: SEMANTIC_SECTION_IDS,
      reviewed: reviews.filter((r) => r.reviewStatus === "REVIEWED").length,
      heuristicAssist: reviews.filter((r) => r.reviewStatus === "HEURISTIC_ASSIST_ONLY")
        .length,
      errors: reviews.filter((r) => r.reviewStatus === "ERROR").length,
    },
  };

  writeJson(path.join(OUT, "01-semantic-opportunity-reviews.json"), reviews);
  writeJson(path.join(OUT, "02-contradictions.json"), contradictions);
  writeJson(path.join(OUT, "03-content-corrections.json"), corrections);
  writeJson(path.join(OUT, "04-research-required.json"), researchRequired);
  writeJson(path.join(OUT, "05-quality-scorecard.json"), scorecard);
  writeJson(path.join(OUT, "06-coverage-manifest.json"), coverageManifest);

  const limitations = [
    paidAuthorized
      ? `Paid OpenAI semantic reviews executed with model ${model}.`
      : "Paid OpenAI semantic reviews were NOT run (no --authorize-paid-semantic). Batch files prepared for Cursor agent paragraph review under 08-review-batches/.",
    "No external paid research (Surfe/Hunter/Exa/web) was spent for source re-verification.",
    "Hotel Validation / Action / Outcome panel text in rendered packs uses a structural placeholder when feedback extras are not injected — live browser verification still required for feedback persistence.",
    "Four selectable hotels without opportunity stores were excluded from opportunity review (documented in inventory).",
    "Do not claim overall content quality complete until reviewStatus=REVIEWED for 100% of opportunities.",
  ];

  const report = buildFounderReport({
    inventory,
    scorecard,
    contradictions,
    corrections,
    researchRequired,
    reviews,
    packs,
    paidAuthorized,
    limitations,
  });
  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), report, "utf8");

  console.log(
    JSON.stringify(
      {
        ok: true,
        out: OUT,
        opportunities: packs.length,
        reviewed: coverageManifest.totals.reviewed,
        heuristicAssist: coverageManifest.totals.heuristicAssist,
        contradictions: contradictions.length,
        researchRequired: researchRequired.length,
        overall: scorecard.overall,
        overallStatus: scorecard.overallStatus,
        paidAuthorized,
        batches: batches.length,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
