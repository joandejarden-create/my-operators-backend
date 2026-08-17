/**
 * Brand Explorer Remaining Editorial Slot Review Package v21A.
 *
 * Review-only package for post-v20B manual_review_required slots.
 * No Airtable writes — prepares founder-reviewed copy for a future v21B writer.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";

export const WRITER_VERSION = "21A";
export const REPORT_JSON_NAME = "brand-explorer-remaining-editorial-slot-review-package.json";
export const REPORT_MD_NAME = "brand-explorer-remaining-editorial-slot-review-package.md";
export const DOC_MD_NAME = "brand-explorer-remaining-editorial-slot-review-package-v21A.md";

const REMAINING_PLAN_PATH = "reports/brand-explorer-slot-completion-remaining-plan.json";
const MANIFEST_PATH = "reports/brand-explorer-slot-standard-manifest.json";
const PLANNER_PATH = "reports/brand-explorer-slot-completion-planner.json";
const COVERAGE_PATH = "reports/brand-explorer-presentation-slot-coverage-audit.json";

const DEFAULT_BRAND_ID = "recCvV0PuZOi8c3hC";
const DEFAULT_BRAND_NAME = "Tribute Portfolio";
const REVIEW_STATUS =
  "AI-drafted / pending founder review; Not company-validated; Not Marriott-validated";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const EDITORIAL_BATCH = "v21A_editorial_founder_review";
const EVIDENCE_BATCH = "v21A_moved_source_evidence_required";
const MEDIA_BATCH = "v21A_moved_media_required";
const BLANK_BATCH = "v21A_moved_intentionally_blank";

const REFERENCE_BRANDS = [
  "Radisson Blu by Choice",
  "Radisson by Choice",
  "Kimpton Hotels",
  "Curio Collection by Hilton",
  "Ascend Hotel Collection",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return String(v).trim() !== "";
}

function toText(v) {
  if (Array.isArray(v)) return v.filter(hasVal).map(String).join(", ");
  return hasVal(v) ? String(v).trim() : "";
}

function short(text, max = 180) {
  const s = toText(text).replace(/\s+/g, " ");
  return s.length > max ? `${s.slice(0, max - 1)}...` : s;
}

function normalizeBrandInput(raw) {
  const normalized = toText(raw).toLowerCase().trim();
  if (!normalized) return DEFAULT_BRAND_ID;
  if (normalized === "tribute-portfolio" || normalized === "tribute portfolio") return DEFAULT_BRAND_ID;
  return toText(raw).trim();
}

function readJson(relPath) {
  const abs = path.join(ROOT, relPath);
  if (!fs.existsSync(abs)) return null;
  try {
    return JSON.parse(fs.readFileSync(abs, "utf8"));
  } catch {
    return null;
  }
}

function listFixtureFiles() {
  const dir = path.join(ROOT, "fixtures");
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir)
    .filter((f) => f.startsWith("brand-explorer-presentation-") && f.endsWith(".json"))
    .map((f) => `fixtures/${f}`);
}

function tabFromSlot(slotKey) {
  if (slotKey.startsWith("overview.") || slotKey.startsWith("hero.")) return "Overview";
  if (slotKey.startsWith("standards.")) return "Owner Considerations";
  if (slotKey.startsWith("footprint.")) return "Footprint & Growth";
  if (slotKey.startsWith("insight.")) return "Dealality Insight";
  return "Unknown";
}

function sectionFromSlot(slotKey) {
  if (slotKey.startsWith("overview.bestAt.")) return "What This Brand Is Best At card";
  if (slotKey.startsWith("overview.differentiators.")) return "Key Differentiators column";
  if (slotKey === "overview.why_value") return "Why this brand wins";
  if (slotKey === "overview.scenarios") return "Typical use-case scenarios";
  if (slotKey === "hero.benefit_zones") return "Hero Typical Benefit Zones";
  if (slotKey === "hero.operator_compat") return "Hero operator compatibility";
  if (slotKey === "insight.similar") return "Similar Brands peer cards";
  if (slotKey === "standards.conversion") return "Conversion / adaptive reuse framing";
  if (slotKey === "standards.deal_inputs") return "Deal diligence input checklist";
  if (slotKey === "footprint.momentum") return "Recent Momentum timeline";
  return tabFromSlot(slotKey);
}

function isReferenceBrandPaste(text) {
  return /(Radisson Blu by Choice|Kimpton Hotels|Curio Collection by Hilton|Ascend Hotel Collection|Everhome Suites|by Choice|by Hilton):/i.test(
    toText(text)
  );
}

function currentTributeSlotValue(brand, slotKey) {
  const blocks = brand?.brandExplorerPresentation || brand?.presentation || [];
  const hits = (Array.isArray(blocks) ? blocks : []).filter((b) => toText(b.slotKey || b.slot_key) === slotKey);
  if (!hits.length) {
    return { present: false, title: "", body: "", merged: "", recordIds: [] };
  }
  return {
    present: true,
    title: toText(hits[0]?.title),
    body: hits.map((b) => toText(b.body)).filter(hasVal).join("\n\n"),
    merged: hits
      .map((b) => [toText(b.title), toText(b.body)].filter(hasVal).join(": "))
      .filter(hasVal)
      .join("\n\n"),
    recordIds: hits.map((b) => toText(b.recordId || b.id)).filter(hasVal),
  };
}

function loadFixtureExamples(slotKey) {
  const examples = [];
  const dir = path.join(ROOT, "fixtures");
  if (!fs.existsSync(dir)) return examples;
  for (const file of fs.readdirSync(dir)) {
    if (!file.startsWith("brand-explorer-presentation-") || !file.endsWith(".json")) continue;
    try {
      const json = JSON.parse(fs.readFileSync(path.join(dir, file), "utf8"));
      const rows = json.slots || json.presentation || json.rows || [];
      for (const row of rows) {
        if (toText(row.slotKey) !== slotKey) continue;
        const title = toText(row.title);
        const body = toText(row.body);
        const brandGuess = file.replace("brand-explorer-presentation-", "").replace(/\.json$/, "");
        examples.push({
          sourceFixture: `fixtures/${file}`,
          brandLabel: brandGuess,
          title,
          body: short(body, 320),
        });
      }
    } catch {
      // skip unreadable fixture
    }
  }
  return examples.slice(0, 6);
}

function detectWordingRisks(slotKey, title, body) {
  const risks = [];
  const combined = `${title}\n${body}`;
  const patterns = [
    { rx: /(Radisson Blu by Choice|Kimpton Hotels|Curio Collection by Hilton|Ascend Hotel Collection):/i, label: "reference-brand paste" },
    { rx: /\bguarantee\b/i, label: "guarantee language" },
    { rx: /\d+%/, label: "numeric percentage without cited source" },
    { rx: /\$\d/, label: "dollar figure without cited source" },
    { rx: /\d+\+?\s*(hotels|properties|openings|members)/i, label: "scale figure without source" },
    { rx: /recent(ly)?\s+(opened|opening|growth|momentum|pipeline)/i, label: "unsupported momentum/opening claim" },
    { rx: /equivalent to|same as|matches performance/i, label: "equivalency claim" },
    { rx: /Fortune|Condé Nast|award-winning/i, label: "third-party accolade without source" },
    { rx: /Illustrative mechanics only/i, label: "placeholder language" },
    { rx: /pending founder review/i, label: "meta placeholder in body" },
  ];
  for (const { rx, label } of patterns) {
    if (rx.test(combined)) risks.push({ type: "wording", message: label });
  }
  if (slotKey === "footprint.momentum" && hasVal(body)) {
    risks.push({ type: "evidence", message: "momentum slot requires dated source-backed announcement rows" });
  }
  return risks;
}

function reclassifySlot(slotKey, manifestRow) {
  if (slotKey === "footprint.momentum") {
    return {
      finalClassification: EVIDENCE_BATCH,
      movedOutOfManualReview: true,
      classificationReason:
        "Recent Momentum requires source-backed opening/PR items with date, property name, and verifiable URL—cannot draft without stewarded Marriott announcements.",
      evidenceNeeded:
        "Source-backed Tribute Portfolio opening or signing announcements with headline, date, description, and Marriott or hotel URL per footprint.momentum slot contract.",
      proposedTitle: "",
      proposedBody: "",
      proposedRows: [],
      safeForV21BWriter: false,
    };
  }
  if (slotKey === "overview.portfolio_context") {
    return {
      finalClassification: BLANK_BATCH,
      movedOutOfManualReview: true,
      classificationReason: "Collection portfolio-context slot should remain blank for Tribute until relevance changes.",
      proposedTitle: "",
      proposedBody: "",
      proposedRows: [],
      safeForV21BWriter: false,
    };
  }
  return null;
}

function draftEditorialCopy(slotKey) {
  const drafts = {
    "hero.benefit_zones": {
      title: "",
      body: "Conversion & repositioning · Resort & leisure destinations · Urban character markets",
      sourceBasis: "Tribute positioning themes + completed-brand hero benefit-zone pattern (high-level only)",
    },
    "hero.operator_compat": {
      title: "",
      body: "Full-service, resort, and lifestyle operators experienced with soft-collection conversions, design narrative, and Marriott systems cutover.",
      sourceBasis: "Tribute soft-collection operating posture + Kimpton/Curio operator-compat pattern (Tribute wording)",
    },
    "overview.bestAt.1": {
      title: "Conversion & Repositioning",
      body: "Independent and boutique assets where local identity and design narrative are the product—not a commodity limited-service reflag.",
      sourceBasis: "Tribute conversion positioning + completed-brand bestAt card pattern",
    },
    "overview.bestAt.2": {
      title: "Resort & Leisure",
      body: "Experience-led resorts and leisure destinations where F&B, design, and sense of place support upper-upscale underwriting.",
      sourceBasis: "Tribute resort/leisure fit + completed-brand bestAt card pattern",
    },
    "overview.bestAt.3": {
      title: "Urban Character",
      body: "Distinctive urban hotels where neighborhood story, design point of view, and independent programming justify collection affiliation.",
      sourceBasis: "Tribute urban boutique fit + completed-brand bestAt card pattern",
    },
    "overview.differentiators.identity": {
      title: "",
      body: "Independent character and local sense of place\nDesign-forward guest experience\nBoutique and lifestyle operating posture",
      sourceBasis: "Marriott Tribute consumer themes + soft-collection identity differentiator pattern",
    },
    "overview.differentiators.commercial": {
      title: "",
      body: "Marriott Bonvoy participation\nMarriott reservation and commercial support\nCollection affiliation without erasing property story",
      sourceBasis: "Tribute Bonvoy/distribution themes + completed-brand commercial differentiator pattern",
    },
    "overview.owner_experience": {
      title: "",
      body: "Owners retain design and local programming latitude within collection standards—affiliation adds Marriott commercial systems, QA rhythm, and Bonvoy participation rather than a prototype-led reflag.",
      sourceBasis: "Tribute soft-collection owner journey + v19 planner draft (reviewed for v21A)",
    },
    "overview.scenarios": {
      title: "",
      body: "Resort and leisure repositioning where an independent or tired resort needs Marriott distribution while preserving experiential identity.\n\nUrban boutique conversion where design narrative, F&B, and local programming support upper-upscale ADR with Bonvoy participation.\n\nMulti-asset sponsors aligning several independent-character conversions under one Marriott soft-collection flag with consistent QA rhythm.",
      sourceBasis: "Tribute scenario patterns + Curio/Kimpton scenario structure (Tribute-specific, no property examples)",
    },
    "overview.why_value": {
      title: "",
      body: "Preserves independent identity while adding Bonvoy and Marriott commercial infrastructure\nSuited to conversion of boutique, lifestyle, and resort assets with clear local point of view\nSupports premium positioning when market tier and operating complexity fit full-service underwriting\nOwners should model franchise fees, PIP scope, loyalty economics, and ramp timing explicitly—not assume automatic uplift",
      sourceBasis: "Tribute owner-value themes + soft-collection why-value pattern (no performance stats)",
    },
    "standards.conversion": {
      title: "",
      body: "Owner diligence framing: sequence conversion PIP scope, heritage or design constraints, systems cutover, and brand QA milestones before underwriting affiliation economics—collection standards preserve guest-quality consistency while allowing property-specific design and local programming within Marriott guidelines.",
      sourceBasis: "Owner Considerations conversion framing + Kimpton/Curio diligence pattern (not a hard standard claim)",
    },
    "standards.deal_inputs": {
      title: "",
      body: "Room count and mix · New build vs. conversion · Prior flag · PIP scope and timing · F&B / restaurant scope · Meeting space · Market tier · Loyalty and fee economics · Operator capability · Heritage / design constraints",
      sourceBasis: "Completed-brand deal-input checklist pattern (qualitative diligence list only)",
    },
    "insight.similar": {
      title: "",
      body: "",
      sourceBasis: "Qualitative peer comparison cards for owner diligence—not equivalency claims",
      proposedRows: [
        {
          title: "Curio Collection by Hilton",
          body: "(Hilton · soft collection · conversion-oriented peer for diligence)",
        },
        {
          title: "Autograph Collection",
          body: "(Marriott · cross-parent soft collection · independent-character benchmark)",
        },
        {
          title: "Unbound Collection by Hyatt",
          body: "(Hyatt · independent-character collection · experiential positioning peer)",
        },
      ],
    },
  };
  return drafts[slotKey] || null;
}

function buildReviewRow(slotKey, tributeBrand, manifestRow, plannerRow) {
  const reclass = reclassifySlot(slotKey, manifestRow);
  const current = currentTributeSlotValue(tributeBrand, slotKey);
  const fixtureExamples = loadFixtureExamples(slotKey);
  const manifestExamples = (manifestRow?.referenceExamples || []).map((ex) => short(ex, 320));
  const completedBrandExamples = [...manifestExamples, ...fixtureExamples.map((f) => `${f.brandLabel}: ${f.title ? `${f.title} | ` : ""}${f.body}`)].slice(0, 5);

  if (reclass) {
    return {
      slotKey,
      tab: manifestRow?.tab || tabFromSlot(slotKey),
      section: manifestRow?.displaySection || sectionFromSlot(slotKey),
      currentTributeValue: current,
      completedBrandExamples,
      referenceBrandsUsing: manifestRow?.brandsUsing || [],
      visibleInUi: true,
      proposedTitle: reclass.proposedTitle,
      proposedBody: reclass.proposedBody,
      proposedRows: reclass.proposedRows || [],
      sourceBasis: reclass.evidenceNeeded || "",
      reviewStatus: REVIEW_STATUS,
      wordingRisks: [],
      hasWordingRisks: false,
      safeForV21BWriter: reclass.safeForV21BWriter,
      movedOutOfManualReview: reclass.movedOutOfManualReview,
      finalClassification: reclass.finalClassification,
      classificationReason: reclass.classificationReason,
      evidenceNeeded: reclass.evidenceNeeded || "",
    };
  }

  const draft = draftEditorialCopy(slotKey) || {
    title: toText(plannerRow?.proposedTitle || plannerRow?.title),
    body: toText(plannerRow?.proposedBody || plannerRow?.body),
    sourceBasis: plannerRow?.sourceBasis || "v19 planner draft (reviewed for v21A)",
    proposedRows: [],
  };

  const proposedTitle = toText(draft.title);
  const proposedBody = toText(draft.body);
  const proposedRows = draft.proposedRows || [];
  const wordingRisks = detectWordingRisks(slotKey, proposedTitle, proposedBody);
  for (const row of proposedRows) {
    wordingRisks.push(...detectWordingRisks(slotKey, row.title, row.body));
  }
  const hasReferencePaste =
    isReferenceBrandPaste(proposedBody) ||
    isReferenceBrandPaste(proposedTitle) ||
    proposedRows.some((r) => isReferenceBrandPaste(r.body) || isReferenceBrandPaste(r.title));

  if (hasReferencePaste) {
    wordingRisks.push({ type: "wording", message: "reference-brand language detected in proposed copy" });
  }

  const safeForV21BWriter =
    wordingRisks.length === 0 &&
    (hasVal(proposedBody) || proposedRows.some((r) => hasVal(r.body) || hasVal(r.title)));

  return {
    slotKey,
    tab: manifestRow?.tab || tabFromSlot(slotKey),
    section: manifestRow?.displaySection || sectionFromSlot(slotKey),
    currentTributeValue: current,
    completedBrandExamples,
    referenceBrandsUsing: manifestRow?.brandsUsing || REFERENCE_BRANDS,
    visibleInUi: true,
    proposedTitle,
    proposedBody,
    proposedRows,
    sourceBasis: draft.sourceBasis,
    reviewStatus: REVIEW_STATUS,
    wordingRisks,
    hasWordingRisks: wordingRisks.length > 0,
    safeForV21BWriter,
    movedOutOfManualReview: false,
    finalClassification: EDITORIAL_BATCH,
    classificationReason: "Qualitative owner-facing editorial copy safe for AI draft + founder review.",
    evidenceNeeded: "",
  };
}

async function fetchBrand(brandIdOrName) {
  const req = { query: { brandId: brandIdOrName, refresh: "1" }, headers: {} };
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.payload = payload;
      return this;
    },
  };
  await getBrandLibraryBrandById(req, res);
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function scoreProjection(currentPresent, additionalSlots, totalRequired = 110) {
  const projectedPresent = currentPresent + additionalSlots;
  return Math.max(0, Math.round((projectedPresent / totalRequired) * 80));
}

export async function buildBrandExplorerRemainingEditorialSlotReviewPackageReport(options = {}) {
  const brandIdOrName = normalizeBrandInput(options.brandIdOrName);
  const remainingPlan = readJson(REMAINING_PLAN_PATH);
  if (!remainingPlan) {
    throw new Error(`Missing remaining slot plan: ${REMAINING_PLAN_PATH}. Run brand-explorer-slot-completion-writer dry-run first.`);
  }

  const originalManualReviewSlots = (remainingPlan.remainingSlotsGrouped?.manual_review_required || []).slice();
  if (!originalManualReviewSlots.length) {
    throw new Error("No manual_review_required slots found in remaining-slot plan.");
  }

  const manifest = readJson(MANIFEST_PATH);
  const planner = readJson(PLANNER_PATH);
  const coverage = readJson(COVERAGE_PATH);
  const manifestBySlot = new Map((manifest?.slotStandardManifestRows || []).map((r) => [r.slotKey, r]));
  const plannerBySlot = new Map((planner?.batch1DraftCopy || []).map((r) => [r.slotKey, r]));

  const tribute = await fetchBrand(brandIdOrName);
  if (!tribute) throw new Error(`Unable to read brand: ${brandIdOrName}`);

  const reviewRows = originalManualReviewSlots.map((slotKey) =>
    buildReviewRow(slotKey, tribute, manifestBySlot.get(slotKey), plannerBySlot.get(slotKey))
  );

  const keptForEditorial = reviewRows.filter((r) => r.finalClassification === EDITORIAL_BATCH);
  const movedToEvidence = reviewRows.filter((r) => r.finalClassification === EVIDENCE_BATCH);
  const movedToMedia = reviewRows.filter((r) => r.finalClassification === MEDIA_BATCH);
  const movedToBlank = reviewRows.filter((r) => r.finalClassification === BLANK_BATCH);

  const v21BSlots = keptForEditorial.filter((r) => r.safeForV21BWriter).map((r) => r.slotKey).sort();
  const slotsWithRisks = keptForEditorial.filter((r) => r.hasWordingRisks);
  const proposedCopyHasReferenceBrandLanguage = reviewRows.some(
    (r) =>
      isReferenceBrandPaste(r.proposedBody) ||
      isReferenceBrandPaste(r.proposedTitle) ||
      (r.proposedRows || []).some((pr) => isReferenceBrandPaste(pr.body) || isReferenceBrandPaste(pr.title))
  );
  const unsupportedClaimsRemain = reviewRows.some((r) =>
    (r.wordingRisks || []).some((w) => /percentage|dollar|scale figure|momentum|opening|accolade/i.test(w.message))
  );

  const v21BWriterSafeToBuild =
    v21BSlots.length > 0 &&
    slotsWithRisks.length === 0 &&
    !proposedCopyHasReferenceBrandLanguage &&
    movedToEvidence.every((r) => r.slotKey === "footprint.momentum");

  const currentPresent = coverage?.tributeSlotKeysPresent?.length || 88;
  const projectedScoreAfterV21B = scoreProjection(currentPresent, v21BSlots.length);
  const comparableAfterV21B = projectedScoreAfterV21B >= 85;

  return {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    imagesUntouched: true,
    copyUntouched: true,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    marriottValidationImplied: false,
    filesRead: [
      "AGENTS.md",
      REMAINING_PLAN_PATH,
      "reports/brand-explorer-slot-completion-remaining-plan.md",
      "reports/brand-explorer-slot-completion-reconciliation.md",
      "reports/brand-explorer-slot-completion-reconciliation.json",
      "reports/brand-explorer-slot-completion-writer.md",
      "reports/brand-explorer-slot-completion-writer.json",
      "reports/brand-explorer-slot-completion-review-package.md",
      "reports/brand-explorer-slot-completion-review-package.json",
      MANIFEST_PATH,
      "reports/brand-explorer-slot-standard-manifest.md",
      COVERAGE_PATH,
      "reports/brand-explorer-presentation-slot-coverage-audit.md",
      PLANNER_PATH,
      "docs/brand-explorer-presentation-slots.md",
      "api/brand-library.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
      ...listFixtureFiles(),
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-remaining-editorial-slot-review-package.js",
      "scripts/brand-explorer-remaining-editorial-slot-review-package.mjs",
      "docs/data-intelligence/brand-explorer-remaining-editorial-slot-review-package-v21A.md",
      "reports/brand-explorer-remaining-editorial-slot-review-package.md",
      "reports/brand-explorer-remaining-editorial-slot-review-package.json",
      "package.json",
    ],
    v21AReviewPackageExists: true,
    brand: { recordId: DEFAULT_BRAND_ID, name: DEFAULT_BRAND_NAME },
    originalManualReviewSlotCount: originalManualReviewSlots.length,
    originalManualReviewSlots,
    slotsKeptForEditorialFounderReview: keptForEditorial.map((r) => r.slotKey),
    slotsMovedToEvidenceRequired: movedToEvidence.map((r) => r.slotKey),
    slotsMovedToMediaRequired: movedToMedia.map((r) => r.slotKey),
    slotsMovedToIntentionallyBlank: movedToBlank.map((r) => r.slotKey),
    reviewPackageRows: reviewRows,
    proposedCopyBySlot: reviewRows
      .filter((r) => r.finalClassification === EDITORIAL_BATCH)
      .map((r) => ({
        slotKey: r.slotKey,
        proposedTitle: r.proposedTitle,
        proposedBody: r.proposedBody,
        proposedRows: r.proposedRows,
        reviewStatus: r.reviewStatus,
      })),
    wordingRisksBySlot: reviewRows
      .filter((r) => r.hasWordingRisks)
      .map((r) => ({ slotKey: r.slotKey, risks: r.wordingRisks })),
    wordingRisksRemain: slotsWithRisks.length > 0,
    unsupportedClaimsRemain,
    proposedCopyReferenceBrandLanguagePresent: proposedCopyHasReferenceBrandLanguage,
    v21BWriterSafeToBuild,
    v21BApplyBatchAfterReview: v21BSlots,
    v21BApplyBatchCount: v21BSlots.length,
    projectedSlotCoverageScoreAfterV21B: projectedScoreAfterV21B,
    currentSlotCoverageScore: remainingPlan.slotCoverageScore ?? coverage?.slotCoverageScore ?? null,
    currentManifestScore: remainingPlan.manifestScore ?? manifest?.revisedRealisticTributeCompletionScore ?? null,
    tributeCompletedBrandComparableAfterV21B: comparableAfterV21B,
    exactDryRunCommand:
      "npm run brand-explorer-remaining-editorial-slot-review-package -- --brand tribute-portfolio --dry-run",
  };
}

export function buildBrandExplorerRemainingEditorialSlotReviewPackageMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Remaining Editorial Slot Review Package v21A");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **no**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push("");
  lines.push("## Summary");
  lines.push(`- Original manual-review slots: **${report.originalManualReviewSlotCount}**`);
  lines.push(`- Kept for editorial founder review: **${report.slotsKeptForEditorialFounderReview.length}**`);
  lines.push(`- Moved to evidence_required: **${report.slotsMovedToEvidenceRequired.length}**`);
  lines.push(`- Moved to media_required: **${report.slotsMovedToMediaRequired.length}**`);
  lines.push(`- Moved to intentionally_blank: **${report.slotsMovedToIntentionallyBlank.length}**`);
  lines.push(`- v21B writer safe to build: **${report.v21BWriterSafeToBuild ? "yes" : "no"}**`);
  lines.push(`- Projected slot coverage after v21B: **${report.projectedSlotCoverageScoreAfterV21B}/100**`);
  lines.push(
    `- Completed-brand comparable after v21B: **${report.tributeCompletedBrandComparableAfterV21B ? "yes" : "no"}**`
  );
  lines.push("");
  lines.push("## Proposed copy by slot");
  for (const row of report.reviewPackageRows || []) {
    if (row.finalClassification !== "v21A_editorial_founder_review") continue;
    lines.push(`### \`${row.slotKey}\``);
    if (row.proposedTitle) lines.push(`- Title: ${short(row.proposedTitle, 120)}`);
    if (row.proposedBody) lines.push(`- Body: ${short(row.proposedBody, 280)}`);
    if (row.proposedRows?.length) {
      for (const pr of row.proposedRows) {
        lines.push(`- Row: **${pr.title}** — ${short(pr.body, 120)}`);
      }
    }
    lines.push(`- Review status: ${row.reviewStatus}`);
    lines.push(`- Safe for v21B: **${row.safeForV21BWriter ? "yes" : "no"}**`);
    lines.push("");
  }
  if (report.slotsMovedToEvidenceRequired.length) {
    lines.push("## Moved to evidence_required");
    for (const slotKey of report.slotsMovedToEvidenceRequired) {
      const row = report.reviewPackageRows.find((r) => r.slotKey === slotKey);
      lines.push(`- \`${slotKey}\`: ${row?.classificationReason || "source evidence required"}`);
    }
    lines.push("");
  }
  lines.push("## v21B apply list (if approved)");
  for (const slotKey of report.v21BApplyBatchAfterReview || []) {
    lines.push(`- \`${slotKey}\``);
  }
  lines.push("");
  return lines.join("\n");
}
