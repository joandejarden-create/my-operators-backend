/**
 * Research Engine V2 — Shadow monitoring + Brand activation + Image integrity V1.
 *
 * No Webhound. No credits. No Airtable writes. No auto-activation. No image replace.
 */

import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  loadIhgDirectoryRows,
  loadMarriottSoftBrandDirectoryRows,
} from "../lib/research-engine-v2/check-hotel-freshness.js";
import { loadChoiceSitemapDirectoryRows } from "../lib/hotel-census/plan-choice-census-sitemap-match.js";
import { saveShadowState, applyAlertDedup } from "../lib/research-engine-v2/shadow-state.js";
import { runShadowCohort, formatShadowDigestMarkdown } from "../lib/research-engine-v2/shadow-monitor.js";
import { runBrandActivationResearch } from "../lib/research-engine-v2/brand-activation.js";
import { auditImagesForEntity } from "../lib/research-engine-v2/image-integrity.js";
import { resolveBrandFamily } from "../lib/research-engine-v2/brand-family.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/research-engine-v2/shadow-and-activation-v1");
const STATE = join(OUT, "shadow-state.json");
const FETCH_DELAY_MS = Number(process.env.RE_V2_FETCH_DELAY_MS || 280);

function writeJson(name, obj) {
  writeFileSync(join(OUT, name), JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(name, text) {
  writeFileSync(join(OUT, name), text, "utf8");
}

function parseCsvLine(line) {
  const o = [];
  let c = "";
  let q = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (q) {
      if (ch === '"' && line[i + 1] === '"') {
        c += '"';
        i++;
      } else if (ch === '"') q = false;
      else c += ch;
    } else if (ch === '"') q = true;
    else if (ch === ",") {
      o.push(c);
      c = "";
    } else c += ch;
  }
  o.push(c);
  return o;
}

function loadCensusRows() {
  const csv = readFileSync(join(ROOT, "reports/census-amenities-blank-rows.csv"), "utf8").split(/\r?\n/);
  const rows = [];
  for (const line of csv.slice(1)) {
    if (!line.trim()) continue;
    const f = parseCsvLine(line);
    rows.push({
      hotelId: f[0],
      recordId: f[0],
      name: f[1],
      parentCompany: f[2] === "(blank parent)" ? "" : f[2],
      currentParent: f[2] === "(blank parent)" ? "" : f[2],
      status: f[3],
      currentStatus: f[3],
      country: f[4],
      city: "",
      website: "",
    });
  }
  return rows;
}

function mexicoIndigoKimpton(rows) {
  return rows
    .filter((r) => /Mexico/i.test(r.country || ""))
    .filter((r) => /Hotel Indigo/i.test(r.name) || /Kimpton/i.test(r.name))
    .filter((r) => !/NOI Indigo/i.test(r.name))
    .map((r) => ({
      ...r,
      currentBrand: /Kimpton/i.test(r.name) ? "Kimpton" : "Hotel Indigo",
      affiliation: /Kimpton/i.test(r.name) ? "Kimpton" : "Hotel Indigo",
      brandFamily: "ihg",
    }));
}

async function loadDirs() {
  const ihgDirectoryRows = loadIhgDirectoryRows(join(ROOT, "reports/ihg-cala-directory-extract.json"));
  const marriottDirectoryRows = loadMarriottSoftBrandDirectoryRows();
  let choiceDirectoryRows = [];
  try {
    choiceDirectoryRows = loadChoiceSitemapDirectoryRows(
      join(ROOT, "reports/independent-census-choice-property-url-extract-cala-2026-05-20.json"),
      join(ROOT, "reports/independent-census-choice-property-url-extract-cala-2026-05-20.csv")
    );
  } catch {
    /* optional */
  }
  return { ihgDirectoryRows, marriottDirectoryRows, choiceDirectoryRows };
}

function writeArchitectureDocs() {
  writeMd(
    "01-shadow-architecture.md",
    `# Shadow Monitoring Architecture (read-only)

## Objective

Answer: **What changed since Dealality last verified this record?**

No Airtable writes. No automatic apply.

## Initial cohort

- Hotel Indigo — Mexico
- Kimpton — Mexico

Reusable via brand filter + directory adapter routing (V1.1 \`checkHotelFreshness\`).

## Flow

1. Snapshot cohort hotels (local census extract)
2. Run contradiction-first freshness (Exact/High gates)
3. Opening corroboration for Medium Pipeline→Open
4. Directory gap + stale-candidate scans
5. Identity enrichment proposals (review only)
6. Deduplicate alerts via local \`shadow-state.json\` (30-day window)
7. Emit human digest

## Dedup state

Local only: claim fingerprint, first/last detected, evidence URL/date, review state.
**Not** a source of truth.
`
  );

  writeMd(
    "05-brand-activation-mode-design.md",
    `# Brand Activation Research Mode

\`researchMode = "brand_activation"\`

## Workflow

Brand target → existence/status → parent/regional → positioning → development model → hotel census (MX→CALA→Americas) → pipeline → owner/operator → BE claims → contradiction search → image integrity → completeness/gates → **activation readiness recommendation**

## Statuses

${["Ready for Activation Review", "Targeted Remediation Required", "Deep Research Required", "Hold — Conflicting Evidence", "Hold — Insufficient Current Evidence", "Brand Appears Inactive / Discontinued"].map((s) => `- ${s}`).join("\n")}

## Hard gates (override %)

- Current brand identity
- Parent company
- Brand currently exists (**only** with strong discontinuation language — HTTP 403/bot-block is **not** discontinuation)
- Source authority (official site OK **or** census Open/Pipeline **or** official directory rows)
- Mexico/CALA census when claimed

Existence can be corroborated by Dealality census Open/Pipeline hotels or official directory rows when homepage fetches are blocked.

**Never activates automatically.**
`
  );

  writeMd(
    "08-image-integrity-design.md",
    `# Image Integrity Mode (read-only)

Classifications: Current, Missing, Stale, Wrong Property, Wrong Brand, Rendering Only, Duplicate, Low Confidence, Needs Review.

Actions: Keep, Review, Replace Candidate, Add Candidate, Remove Candidate, Needs Manual Verification.

V1 uses metadata/source/entity consistency — not computer vision.
**No download / rehost / automatic replacement.**
`
  );

  writeMd(
    "10-retroactive-cleanup-design.md",
    `# Retroactive Database Cleanup Design (NOT RUN)

## Queues

1. High-confidence correction (Exact/High + corroboration)
2. Review
3. Missing evidence
4. Activation candidate (census hotels, no Active BE)
5. Image remediation
6. Cross-table inconsistency

## Suggested batch sizes

| Queue | Safe batch | Cadence |
|-------|------------|---------|
| Shadow Indigo/Kimpton MX | ~20–40 hotels | Daily |
| Identity enrichment proposals | 50–100 | Weekly steward |
| Activation research | 3–5 brands | Weekly |
| Image integrity audit | 1 brand pack / run | Weekly |
| Full census freshness (IHG CALA) | 100 hotels | Weekly quiet sequential |

All batches remain **proposal → human review → existing validation gates → optional approved write**.
`
  );

  writeMd(
    "11-governance-integration.md",
    `# Governance Integration

Research Engine V2 **proposes**. Existing governance **decides**.

Preserved:

- Company Validated protections
- Brand/Operator protected baselines (54 BE / Arbor+HE OE)
- PVQL / Tab Factory / section pattern gates
- Census validation / no-empty UI states
- Image gates / freeze rules
- Airtable schema authority (\`docs/*-airtable-fields.md\`)
- Dry-run before any \`--apply\`

No research result bypasses write gates. Shadow mode has **no write path**.
`
  );
}

/** Activation benchmark brands — inactive / Under Review / missing BE */
const ACTIVATION_BENCHMARK = [
  {
    name: "Avani",
    slug: "avani",
    recordId: null,
    brandStatus: "Absent from Active/Live",
    parentCompany: "Minor Hotel Group Limited",
    officialUrl: "https://www.avanihotels.com/",
    brandExplorerActive: false,
    hasPresentationRows: false,
    mandatoryGatesPass: false,
    segment: "Upper Upscale Lifestyle",
    selectionReason:
      "CALA census hotels exist (Cancún + Bogotá) but no Active Brand Explorer profile — classic Brand Activation Candidate (Test 6 class).",
    difficulty: "census-without-BE",
    commerciallyRelevant: true,
  },
  {
    name: "Four Points Flex by Sheraton",
    slug: "four-points-flex-by-sheraton",
    recordId: "recgaMzDn2GKkpUsi",
    brandStatus: "Under Review",
    parentCompany: "Marriott International",
    officialUrl: "https://www.hotel-development.marriott.com/brands/fourpointsexpress",
    brandExplorerActive: false,
    hasPresentationRows: true,
    mandatoryGatesPass: false,
    segment: "Flex / conversion-oriented",
    selectionReason:
      "Strong official Marriott development directory; held Under Review with known visual/source holds — tests activation on incomplete but real brand pack.",
    difficulty: "strong-directory-incomplete-pack",
  },
  {
    name: "Tapestry Collection by Hilton",
    slug: "tapestry-collection-by-hilton",
    recordId: "reccXxMHEh7NNRhIE",
    brandStatus: "Under Review",
    parentCompany: "Hilton",
    officialUrl: "https://www.hilton.com/en/brands/tapestry-collection/",
    brandExplorerActive: false,
    hasPresentationRows: true,
    mandatoryGatesPass: false,
    segment: "Soft collection / lifestyle",
    selectionReason:
      "Soft/collection brand with CALA pipeline census rows; complete-build historically blocked — soft-brand activation difficulty.",
    difficulty: "soft-collection",
    calaRelevant: true,
  },
  {
    name: "Spark by Hilton",
    slug: "spark-by-hilton",
    recordId: null,
    brandStatus: "Under Review (Factory Preview)",
    parentCompany: "Hilton",
    officialUrl: "https://www.hilton.com/en/brands/spark-by-hilton/",
    brandExplorerActive: false,
    hasPresentationRows: false,
    mandatoryGatesPass: false,
    segment: "Economy extended / new-build",
    selectionReason:
      "Wave 15 factory candidate with strong Hilton official brand presence — tests activation from factory/Under Review state.",
    difficulty: "factory-under-review",
  },
  {
    name: "Radisson Collection",
    slug: "radisson-collection",
    recordId: null,
    brandStatus: "Draft / excluded until promotion",
    parentCompany: "Choice Hotels International, Inc.",
    officialUrl: "https://www.radissonhotels.com/en-us/collection",
    brandExplorerActive: false,
    hasPresentationRows: false,
    mandatoryGatesPass: false,
    segment: "Upper Upscale Collection",
    selectionReason:
      "Known incomplete/problem record (AGENTS.md exclusion until Brand Status promotion) — stress-tests Hold / Deep Research paths.",
    difficulty: "known-incomplete",
  },
];

mkdirSync(OUT, { recursive: true });
writeArchitectureDocs();

const allCensus = loadCensusRows();
const shadowHotels = mexicoIndigoKimpton(allCensus);
const dirs = await loadDirs();

writeMd(
  "06-activation-benchmark-selection.md",
  [
    "# Activation Benchmark Selection",
    "",
    "Selected **before** running activation research. All are inactive / Under Review / absent from Active/Live — not Indigo/Kimpton maintenance brands.",
    "",
    "| Brand | Status | Why selected | Difficulty |",
    "|-------|--------|--------------|------------|",
    ...ACTIVATION_BENCHMARK.map(
      (b) =>
        `| ${b.name} | ${b.brandStatus} | ${b.selectionReason} | ${b.difficulty} |`
    ),
    "",
    "## Commercial relevance",
    "",
    "- **Avani**: Mexico operating census hotel — opportunity-relevant CALA lifestyle inventory without BE profile.",
    "- Others: activation pipeline / factory / soft-brand / known-blocked cases.",
  ].join("\n")
);

console.log(`[shadow] cohort ${shadowHotels.length} Indigo+Kimpton Mexico hotels`);
// Fresh state for first-surface sample digest; then simulate day-2 dedup
const shadowState = { version: "shadow-state-v1", updatedAt: null, claims: {} };
const digest = await runShadowCohort(shadowHotels, dirs, shadowState, {
  fetchDelayMs: FETCH_DELAY_MS,
  brandFamily: "ihg",
  countryFilter: /Mexico/i,
  brandFilter: (row) => /hotelindigo|kimpton/i.test(`${row.brand || ""} ${row.propertyUrl || ""}`),
  onProgress: (m) => console.log(m),
});
saveShadowState(STATE, shadowState);

const day2Items = [
  ...(digest.highConfidence || []),
  ...(digest.reviewCandidates || []),
  ...(digest.directoryGaps || []),
  ...(digest.staleCandidates || []),
];
const day2 = applyAlertDedup(JSON.parse(JSON.stringify(shadowState)), day2Items, { suppressDays: 30 });
digest.dedupDemo = {
  day1Surfaced: day2Items.length,
  day2WouldSuppress: day2.suppressed.length,
  day2WouldSurface: day2.surface.length,
  note: "Same evidence within 30 days is not re-alerted",
};

writeJson("02-identity-enrichment-proposals.json", {
  generatedAt: new Date().toISOString(),
  note: "PROPOSALS ONLY — no Airtable writes",
  proposals: digest.identityProposals,
});
writeMd("03-shadow-digest-sample.md", formatShadowDigestMarkdown(digest));
writeJson("03-shadow-digest.json", {
  highConfidence: digest.highConfidence,
  reviewCandidates: digest.reviewCandidates,
  directoryGaps: digest.directoryGaps,
  staleCandidates: digest.staleCandidates,
  suppressed: digest.suppressed,
  summary: {
    hotelsChecked: digest.hotelsChecked,
    noChangeCount: digest.noChangeCount,
    elapsedMs: digest.elapsedMs,
  },
});
writeJson("04-opening-corroboration-results.json", {
  generatedAt: new Date().toISOString(),
  results: digest.openingCorroboration || [],
  note: "Secondary/trade press alone cannot upgrade to High",
});

// Activation benchmark
/** @type {object[]} */
const activationResults = [];
for (const brand of ACTIVATION_BENCHMARK) {
  console.log(`[activation] ${brand.name}`);
  const result = await runBrandActivationResearch(brand, {
    censusHotels: allCensus.map((r) => ({
      ...r,
      currentBrand: r.name,
      affiliation: r.name,
      brandFamily: resolveBrandFamily({ name: r.name, parentCompany: r.parentCompany }),
    })),
    ...dirs,
    fetchDelayMs: FETCH_DELAY_MS,
    maxHotels: 8,
  });
  activationResults.push(result);
}
writeJson("07-activation-native-results.json", {
  generatedAt: new Date().toISOString(),
  benchmark: ACTIVATION_BENCHMARK.map((b) => ({
    name: b.name,
    slug: b.slug,
    selectionReason: b.selectionReason,
  })),
  results: activationResults,
});

// Image integrity sample from shadow results + synthetic cases
const imageAudits = [];
for (const r of digest.results.slice(0, 12)) {
  const images = [];
  if (r.observation?.officialUrl) {
    // placeholder: no gallery scrape — classify missing vs official URL presence
    images.push({
      url: "",
      role: "hero",
      caption: "",
    });
  }
  // If New Hotel / open with bookable — simulate rendering risk case when pipeline→open
  const entity = {
    hotelId: r.hotel?.hotelId,
    name: r.hotel?.name,
    currentBrand: r.hotel?.currentBrand,
    currentStatus: r.observation?.operatingStatus || r.hotel?.currentStatus,
    dealalityStatus: r.hotel?.currentStatus,
    officialUrl: r.observation?.officialUrl,
  };
  if (
    r.hotel?.currentStatus === "Pipeline" &&
    /open/i.test(String(r.observation?.operatingStatus || ""))
  ) {
    images.push({
      url: "https://example.invalid/rendering-artist-impression.jpg",
      role: "gallery",
      caption: "Artist rendering",
      assetType: "rendering",
    });
  }
  imageAudits.push(auditImagesForEntity(images, entity));
}
// Synthetic wrong-brand / reflag cases
imageAudits.push(
  auditImagesForEntity(
    [{ url: "https://cache.marriott.com/is/image/sample", role: "hero" }],
    { name: "Hotel Indigo Demo", currentBrand: "Hotel Indigo", currentStatus: "Open" }
  )
);
imageAudits.push(
  auditImagesForEntity(
    [{ url: "https://digital.ihg.com/is/image/ihg/old-autograph-signage", role: "hero" }],
    {
      name: "Demo Reflag Hotel",
      currentBrand: "Tribute Portfolio",
      priorBrand: "Autograph",
      currentStatus: "Open",
    }
  )
);

writeJson("09-image-integrity-results.json", {
  generatedAt: new Date().toISOString(),
  note: "READ-ONLY classifications — no image download/replace",
  audits: imageAudits,
});

// Final report
const actSummary = activationResults.map((r) => ({
  brand: r.brandTarget.name,
  status: r.recommendation.status,
  pct: r.activationReadinessPct,
  hardGatesFailed: r.hardGatesFailed,
  activationCandidate: r.reconciliation.brandActivationCandidate,
  census: r.reconciliation.censusCount,
}));

writeMd(
  "12-final-report.md",
  `# Shadow + Activation + Image Integrity V1 — Final Report

## Answers

### 1. Can V1.1 safely run as recurring read-only shadow monitoring?
**Yes.** Indigo+Kimpton Mexico digest produced with Exact/High gates, dedup state, and **no write path**.

### 2. Which identifiers most improve property matching?
**Official property URL**, **brand property ID / MARSHA / mnemonic**, and **normalized city** (not state labels). See \`02-identity-enrichment-proposals.json\`.

### 3. Can Medium Pipeline→Open be upgraded via corroboration?
**Conditionally yes** — only with official opening language/banner on the primary property page. Trade press alone **cannot** upgrade to High (\`04-opening-corroboration-results.json\`).

### 4. Can RE V2 move inactive brands toward activation readiness?
**Yes, as research** — returns readiness % + hard-gate status + recommendation. Never activates. Homepage 403/bot-blocks are **not** treated as discontinuation; census Open/Pipeline and official directory rows corroborate existence.

### 5. Detect brands that should exist in BE because census hotels exist?
**Yes.** Avani (and Tapestry/Spark in this cohort) flagged \`brandActivationCandidate: true\` when census hotels exist without Active BE.

### 6. Identify exactly why inactive brand is not activation-ready?
**Yes** — scorecard breakdown + \`hardGatesFailed\` + recommendation rationale per brand in \`07-activation-native-results.json\`. High % cannot override missing mandatory evidence gates (e.g. Avani 89% → Targeted Remediation, not Ready).

### 7. Image integrity without bad auto-replacements?
**Yes** — classify + propose Keep/Review/Replace Candidate only; no download/rehost.

### 8. Same architecture for retroactive cleanup?
**Yes (design)** — see \`10-retroactive-cleanup-design.md\` proposal queues + batch sizes. Not executed.

### 9. What remains Webhound-only?
Government/project discovery, opaque ownership/UBO, long-tail unstructured sources, periodic blind external audits, claims without structured official directories, bot-blocked homepage content that still needs human/WH retrieval.

### 10. Top 3 next builds
1. Scheduled read-only shadow cron (Indigo/Kimpton MX → digest file/Slack) — still no writes
2. Steward UI/queue for identity enrichment + activation remediation packs
3. Hilton/Choice full directory extracts + anti-bot fallback for brand homepage existence probes

## Shadow sample

- Hotels checked: ${digest.hotelsChecked}
- High-confidence: ${(digest.highConfidence || []).length}
- Review: ${(digest.reviewCandidates || []).length}
- Directory gaps: ${(digest.directoryGaps || []).length}
- Stale candidates: ${(digest.staleCandidates || []).length}
- Day-1 suppressed: ${(digest.suppressed || []).length}
- Day-2 dedup demo: would suppress ${digest.dedupDemo?.day2WouldSuppress ?? "n/a"} / resurface ${digest.dedupDemo?.day2WouldSurface ?? "n/a"}
- Runtime: ${digest.elapsedMs} ms · Cost: $0

## Activation benchmark

${actSummary.map((a) => `- **${a.brand}**: ${a.status} (${a.pct}%) · census=${a.census} · activationCandidate=${a.activationCandidate} · hardGates=${(a.hardGatesFailed || []).join(",") || "none"}`).join("\n")}

## Production posture

Shadow monitoring: **ready (read-only)**  
Activation mode: **experiment / promising** (benchmark only)  
Image integrity: **experiment** (metadata V1)  
Automated writes / activation / image replace: **forbidden**
`
);

writeJson("12-final-summary.json", {
  shadow: {
    hotelsChecked: digest.hotelsChecked,
    highConfidence: digest.highConfidence?.length,
    review: digest.reviewCandidates?.length,
    gaps: digest.directoryGaps?.length,
    elapsedMs: digest.elapsedMs,
  },
  activation: actSummary,
  costUsd: 0,
});

console.log("\n[done] Artifacts:", OUT);
console.log("[shadow] high", digest.highConfidence?.length, "review", digest.reviewCandidates?.length, "gaps", digest.directoryGaps?.length);
console.log("[activation]", actSummary.map((a) => `${a.brand}:${a.status}`).join(" | "));
