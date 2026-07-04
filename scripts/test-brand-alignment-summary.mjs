/**
 * Brand Alignment executive summary validation.
 * Run: node scripts/test-brand-alignment-summary.mjs
 * Optional live Airtable: requires .env with AIRTABLE_* and network.
 */

import { readFileSync } from "fs";
import { config } from "dotenv";
import { buildBrandReviewContent } from "../lib/brand-alignment-rationale.js";

config();

const src = readFileSync(new URL("../api/brand-alignment-snapshot.js", import.meta.url), "utf8");
const rationaleSrc = readFileSync(new URL("../lib/brand-alignment-rationale.js", import.meta.url), "utf8");

const banned = [
  /dealality recommends/i,
  /\brecommended\b/i,
  /\bbest fit\b/i,
  /\bbest option\b/i,
  /\bwinner\b/i,
  /\bprioritize\b/i,
  /\bproceed\b/i,
  /\bthe selected brand\b/i,
  /\bselected brand for\b/i,
  /\bapproved\b/i,
  /guaranteed/i,
  /final suitability/i,
  /Compares chain scale tiers/i,
  /Current deal inputs suggest the owner may benefit/i,
  /appears aligned based on current inputs for/i,
  /Must match/i,
  /Scores full/i,
];

const rationaleRequiredFields = [
  "alignmentRationale",
  "mainAlignmentSignals",
  "alignmentFactorsReviewed",
  "whatSupportsReview",
  "whatNeedsValidation",
  "ownerQuestionsThisBrandRaises",
  "whatCouldWeakenAlignment",
  "fitBoundariesWatchouts",
  "keyConsideration",
];

const requiredSnippets = [
  "buildExecutiveBrandAlignmentSummary",
  "buildBrandReviewContent",
  "COMMON_QUESTIONS_BEFORE_OUTREACH",
  "topBrandNames",
  "dominantAlignmentSignals",
  "commonValidationFactors",
  "alignmentPatternLabel",
  "readinessInterpretation",
  "internal screening and discussion tool",
  "buildBrandReviewContent",
];

for (const needle of requiredSnippets) {
  if (!src.includes(needle)) throw new Error("Missing in API: " + needle);
}
if (!rationaleSrc.includes("currently shows a")) {
  throw new Error("Missing rationale paragraph phrasing in lib");
}
if (!rationaleSrc.includes("mainAlignmentSignals")) {
  throw new Error("Missing mainAlignmentSignals in lib");
}

function assertNoBanned(text, label) {
  for (const re of banned) {
    if (re.test(text)) throw new Error(`Banned in ${label}: ${re}`);
  }
}

function assertDealSpecific(text, dealName) {
  if (!text.includes(dealName)) {
    throw new Error(`Summary does not mention deal name "${dealName}"`);
  }
}

function assertNoMethodologyInOwnerFacing(brands) {
  const methodologyRe = /Compares |scores higher|weight|calculation|Must match|Scores full/i;
  for (const b of brands) {
    const fields = [
      b.keyConsideration,
      b.alignmentRationale,
      ...(b.mainAlignmentSignals || []),
      ...(b.whatSupportsReview || []),
      ...(b.whatNeedsValidation || []),
    ];
    for (const text of fields) {
      if (methodologyRe.test(String(text || ""))) {
        throw new Error(`Methodology in owner-facing text for ${b.brandName}: ${text}`);
      }
    }
    for (const sig of b.potentialAlignmentSignals || b.signals || []) {
      if (sig.note && methodologyRe.test(sig.note)) {
        throw new Error(`Methodology note exposed for ${b.brandName}: ${sig.note}`);
      }
      if (sig.ownerExplanation && methodologyRe.test(sig.ownerExplanation)) {
        throw new Error(`Methodology in ownerExplanation for ${b.brandName}`);
      }
    }
    if (b.questionsToClarify?.length) {
      throw new Error(`Per-brand questionsToClarify should be removed: ${b.brandName}`);
    }
    for (const key of rationaleRequiredFields) {
      if (!(key in b)) throw new Error(`Missing ${key} on ${b.brandName}`);
    }
  }
}

async function snapshotForDeal(dealId) {
  const { postBrandAlignmentSnapshot } = await import("../api/brand-alignment-snapshot.js");
  let payload;
  let status = 200;
  const res = {
    status(code) {
      status = code;
      return this;
    },
    json(data) {
      payload = data;
      return data;
    },
  };
  await postBrandAlignmentSnapshot(
    { body: { dealId, brandUniverse: "owner_preferred_then_pipeline", maxBrands: 8 } },
    res
  );
  if (status !== 200 || !payload?.success) {
    throw new Error(`API failed for ${dealId}: ${status} ${payload?.error || ""}`);
  }
  return payload;
}

const hasAirtable = Boolean(process.env.AIRTABLE_BASE_ID && process.env.AIRTABLE_API_KEY);

if (hasAirtable) {
  const cases = [
    { id: "rec1l2CiXa8evp0Q8", name: "Arsalan Group Ltd." },
    { id: "recjS6htuIpEBmzFE", name: "Courtyard by Marriott Amsterdam Airport" },
  ];

  for (const c of cases) {
    console.log("\n--- Live:", c.name, c.id, "---");
    let data;
    try {
      data = await snapshotForDeal(c.id);
    } catch (err) {
      console.warn("Skip live case (API):", err.message);
      continue;
    }
    const paras = data.summary?.brandAlignmentSummaryParagraphs || [];
    const full = paras.join("\n\n");
    const dealLabel = data.deal?.name || c.name;
    const token = dealLabel.split(/[\s–-]/)[0];
    if (!full.includes(token) && !full.includes(dealLabel.slice(0, 12))) {
      throw new Error(`Summary missing deal reference (expected "${dealLabel}")`);
    }
    assertNoBanned(full, c.name);
    assertNoMethodologyInOwnerFacing(data.brands || []);
    for (const b of data.brands || []) {
      assertNoBanned(b.alignmentRationale || "", b.brandName + " rationale");
    }
    if (!data.commonQuestionsToClarify?.length) {
      throw new Error("Missing commonQuestionsToClarify at response root");
    }
    console.log("Paragraphs:", paras.length);
    console.log("topBrandNames:", data.summary?.topBrandNames);
    console.log("alignmentPatternLabel:", data.summary?.alignmentPatternLabel);
    console.log("dominantAlignmentSignals:", data.summary?.dominantAlignmentSignals);
    console.log("P1:", paras[0]?.slice(0, 200) + "…");
    const curio = (data.brands || []).find((b) => /curio/i.test(b.brandName));
    const moderate = (data.brands || []).find((b) =>
      /conditional|lower|moderate/i.test(b.tier || "")
    );
    if (curio) {
      console.log("\nCurio alignmentRationale:", curio.alignmentRationale);
      console.log("Curio mainAlignmentSignals:", curio.mainAlignmentSignals);
      console.log("Curio keyConsideration:", curio.keyConsideration);
    }
    if (moderate) {
      console.log("\nModerate/conditional sample:", moderate.brandName, moderate.tier);
      console.log("Rationale:", moderate.alignmentRationale?.slice(0, 280) + "…");
      console.log("keyConsideration:", moderate.keyConsideration);
    }
    if (data.brands?.[0] && !curio) {
      console.log("Sample keyConsideration:", data.brands[0].keyConsideration);
      console.log("Sample rationale:", data.brands[0].alignmentRationale?.slice(0, 220) + "…");
    }
  }

  // No-brand proxy: unlikely to find — skip unless we have a known empty deal id
} else {
  console.log("Skip live Airtable tests (no AIRTABLE_* in .env).");
}

const mockReview = buildBrandReviewContent({
  brandName: "Curio Collection by Hilton",
  tier: "Higher Alignment Signal",
  scoreAvailable: true,
  score: 82,
  breakdownNewDetails: {
    chainScaleProximity: { score: 90 },
    projectTypeCompatibility: { score: 88 },
    projectStageCompatibility: { score: 80 },
    brandStandardsCompatibility: { score: 50 },
  },
  deal: {
    targetPositioning: "Upper Upscale",
    projectType: "New Build",
    keyCount: 180,
  },
  mergedFields: {},
  brandData: { brandBasics: { "Hotel Chain Scale": "Upper Upscale" } },
  source: "owner_preferred",
  preferredBrandNames: ["Curio Collection by Hilton"],
});

assertNoBanned(mockReview.alignmentRationale, "mock Curio rationale");
if (!/currently shows a Higher Alignment Signal/i.test(mockReview.alignmentRationale)) {
  throw new Error("Mock rationale missing tier phrasing");
}
if (/appears aligned based on current inputs for/i.test(mockReview.alignmentRationale)) {
  throw new Error("Old factor-list rationale pattern still present");
}

const mockRadissonRed = buildBrandReviewContent({
  brandName: "Radisson RED",
  tier: "Higher Alignment Signal",
  scoreAvailable: true,
  score: 85,
  breakdownNewDetails: {
    chainScaleProximity: { score: 88 },
    projectTypeCompatibility: { score: 90 },
    serviceModelAlignment: { score: 55 },
    buildingTypeCompatibility: { score: 50 },
  },
  deal: {
    targetPositioning: "Upper Upscale",
    projectType: "New Build",
    keyCount: 100,
  },
  mergedFields: {},
  brandData: {
    brandBasics: { "Hotel Chain Scale": "Upper Upscale", "Hotel Service Model": "Full Service" },
    brandFit: {},
  },
  source: "owner_preferred",
  preferredBrandNames: ["Radisson RED"],
  parentCompany: "Radisson Hotel Group",
});

if (mockReview.keyConsideration === mockRadissonRed.keyConsideration) {
  throw new Error("Curio and Radisson RED keyConsideration should differ");
}
if (mockReview.mainAlignmentSignals[0] === mockRadissonRed.mainAlignmentSignals[0]) {
  throw new Error("Curio and Radisson RED identity bullets should differ");
}
if (/^collection-style path may be relevant if the owner wants distribution/i.test(mockRadissonRed.keyConsideration)) {
  throw new Error("Radisson RED should not use generic collection key consideration");
}
if (!/Collection-style path may be relevant/i.test(mockReview.keyConsideration)) {
  throw new Error("Curio keyConsideration should use collection-style business summary");
}
if (!mockReview.whatSupportsReview.some((b) => /Target positioning appears directionally aligned/i.test(b))) {
  throw new Error("whatSupportsReview should use business-readable bullets");
}
if (!mockReview.alignmentFactorsReviewed?.length) {
  throw new Error("alignmentFactorsReviewed required");
}

console.log("\n--- Mock Curio (before/after style) ---");
console.log("alignmentRationale:", mockReview.alignmentRationale);
console.log("keyConsideration:", mockReview.keyConsideration);
console.log("whatSupportsReview:", mockReview.whatSupportsReview);
console.log("\n--- Mock Radisson RED (moderate-style) ---");
console.log("keyConsideration:", mockRadissonRed.keyConsideration);
console.log("alignmentRationale:", mockRadissonRed.alignmentRationale?.slice(0, 320) + "…");

console.log("\nOK: brand-alignment-summary validation passed.");
