import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";

export const WRITER_VERSION = "20A";
export const REPORT_JSON_NAME = "brand-explorer-slot-completion-review-package.json";
export const REPORT_MD_NAME = "brand-explorer-slot-completion-review-package.md";
export const DOC_MD_NAME = "brand-explorer-slot-completion-review-package-v20A.md";

const V19_REPORT_JSON = "brand-explorer-slot-completion-planner.json";
const DEFAULT_BRAND_ID = "recCvV0PuZOi8c3hC";
const REVIEW_STATUS =
  "AI-drafted / pending founder review; Not company-validated; Not Marriott-validated";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const PRESENTATION_FIELDS = {
  slotKey: "Slot Key",
  title: "Title",
  body: "Body",
  brand: "Brand",
  brandName: "Brand Name",
  active: "Active",
  sortOrder: "Sort Order",
};

const BATCH_1 = "batch_1_safe_editorial_human_review";
const BATCH_2_SOURCE = "batch_2_source_evidence_required";

const LOYALTY_SOURCE_EVIDENCE_SLOTS = new Set(["loyalty.earn", "loyalty.redeem", "loyalty.elite"]);

const TARGET_TABS = new Set([
  "Commercial Engine",
  "Operating Model",
  "Value to Owners",
  "Loyalty Program",
  "Footprint & Growth",
]);

const FOOTPRINT_EDITORIAL_SLOTS = new Set([
  "footprint.editorial",
  "footprint.editorial_bullets",
  "footprint.geo.summary",
  "footprint.growth_editorial",
  "footprint.growth_fit",
  "footprint.growth_themes",
  "footprint.growth.narrative",
  "footprint.momentum_label",
  "footprint.portfolio_mix",
  "footprint.region.am",
  "footprint.region.apac",
  "footprint.region.cala",
  "footprint.region.eu",
  "footprint.region.mea",
]);

const EXCLUDED_SLOT_KEYS = new Set([
  "footprint.momentum",
  "footprint.openings",
  "loyalty.proof",
  "materials.caseStudy",
  "overview.proof_operator",
  "standards.last_reviewed",
  "standards.requirement",
]);

const EXCLUDED_SLOT_PATTERNS = [
  /^economics\./i,
  /^overview\.proof\./i,
  /^materials\.gallery\./i,
  /openings/i,
];

const TAB_ORDER = [
  "Commercial Engine",
  "Operating Model",
  "Value to Owners",
  "Loyalty Program",
  "Footprint & Growth",
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

function short(text, max = 200) {
  const s = toText(text).replace(/\s+/g, " ");
  return s.length > max ? `${s.slice(0, max - 1)}...` : s;
}

function wordCount(text) {
  return toText(text).split(/\s+/).filter(Boolean).length;
}

function normalizeBrandInput(raw) {
  const normalized = toText(raw).toLowerCase().trim();
  if (!normalized) return DEFAULT_BRAND_ID;
  if (normalized === "tribute-portfolio" || normalized === "tribute portfolio") return DEFAULT_BRAND_ID;
  return toText(raw).trim();
}

function tabFromSlot(slotKey) {
  if (slotKey.startsWith("overview.") || slotKey.startsWith("hero.")) return "Overview";
  if (slotKey.startsWith("valueOwners.")) return "Value to Owners";
  if (slotKey.startsWith("operations.")) return "Operating Model";
  if (slotKey.startsWith("standards.")) return "Owner Considerations";
  if (slotKey.startsWith("commercial.")) return "Commercial Engine";
  if (slotKey.startsWith("economics.")) return "Economics & Obligations";
  if (slotKey.startsWith("loyalty.")) return "Loyalty Program";
  if (slotKey.startsWith("footprint.")) return "Footprint & Growth";
  if (slotKey.startsWith("materials.")) return "Brand Materials";
  if (slotKey.startsWith("insight.")) return "Dealality Insight";
  return "Unknown";
}

function sectionFromSlot(slotKey) {
  if (/^commercial\.lever\./i.test(slotKey)) return "Commercial strength card";
  if (/^commercial\.kpi\./i.test(slotKey)) return "Commercial KPI strip";
  if (/^operations\.flexibility\./i.test(slotKey)) return "Flexibility indicator";
  if (/^operations\.model\./i.test(slotKey)) return "Operating model field";
  if (/^operations\.compliance\./i.test(slotKey)) return "Compliance & oversight";
  if (/^footprint\.region\./i.test(slotKey)) return "Regional footprint card";
  if (/^valueOwners\.lifecycle\./i.test(slotKey)) return "Owner lifecycle phase";
  if (/^loyalty\.implications\./i.test(slotKey)) return "Loyalty owner implication";
  return tabFromSlot(slotKey);
}

function isExcludedSlot(slotKey, writeBatch) {
  const key = toText(slotKey);
  if (EXCLUDED_SLOT_KEYS.has(key)) return { excluded: true, reason: "explicit v20A exclusion (source/media/proof/openings)" };
  if (EXCLUDED_SLOT_PATTERNS.some((rx) => rx.test(key))) {
    return { excluded: true, reason: "matches excluded pattern (economics/proof/media/openings)" };
  }
  if (writeBatch && writeBatch !== BATCH_1) {
    return { excluded: true, reason: `not v19 Batch 1 (${writeBatch})` };
  }
  const tab = tabFromSlot(key);
  if (!TARGET_TABS.has(tab)) {
    return { excluded: true, reason: `tab not in v20A first wave (${tab})` };
  }
  if (tab === "Footprint & Growth" && !FOOTPRINT_EDITORIAL_SLOTS.has(key)) {
    return { excluded: true, reason: "footprint slot outside editorial-first-wave allowlist" };
  }
  return { excluded: false, reason: "" };
}

function isReferenceBrandPaste(text) {
  return /(Radisson Blu by Choice|Kimpton Hotels|Curio Collection by Hilton|Ascend Hotel Collection|Everhome Suites|by Choice|by Hilton):/i.test(
    toText(text)
  );
}

function tributeRegionLabels(brand) {
  const raw = brand?.regionOffered;
  const list = Array.isArray(raw) ? raw.map((r) => toText(r)).filter(hasVal) : toText(raw) ? [toText(raw)] : [];
  return list;
}

function regionOfferedIncludes(brand, regionCode) {
  const labels = tributeRegionLabels(brand).join(" ").toLowerCase();
  const map = {
    am: ["north america", "americas", "united states", "canada", "usa"],
    cala: ["caribbean", "latin america", "cala"],
    eu: ["europe"],
    mea: ["middle east", "africa", "mea"],
    apac: ["asia pacific", "asia", "apac"],
  };
  const needles = map[regionCode] || [];
  if (!labels) return true;
  return needles.some((n) => labels.includes(n));
}

const TRIBUTE_OPERATIONS_MODEL = {
  primary_model:
    "Franchise or management affiliation with Marriott systems, Bonvoy, and commercial stack—common on independent full-service and resort conversions.",
  management_option:
    "Third-party management common; owner-operated where the operator can run full-service, resort, or lifestyle complexity.",
  typical_ownership:
    "Institutional and entrepreneurial owners funding boutique, resort, or lifestyle conversions with design narrative and PIP discipline.",
  brand_involvement:
    "Marriott collection involvement on design narrative, conversion milestones, and QA—higher touch than select-service prototypes.",
  systems_integration:
    "Marriott PMS, CRS, Bonvoy, and brand reporting integration—confirm cutover scope, timing, and fee line items in franchise materials.",
  pre_opening:
    "Conversion PIP, design sign-off, Bonvoy onboarding, and opening QA before stabilization—not a light reflag path.",
  staffing_intensity:
    "Full-service or resort staffing—F&B, front office, and experience roles scale with market tier and F&B scope.",
  fb_complexity:
    "Moderate–high when restaurant and bar are part of the property identity—kitchen and labor are primary underwriting inputs.",
  training:
    "Marriott brand training and collection service standards—budget per approved franchise fee schedule.",
  reporting_discipline:
    "Franchise reporting cadence and revenue management discipline required for portfolio benchmarking.",
  qa_rhythm:
    "QA at opening and recurring per collection standards—conversion assets often carry heavier initial compliance scope.",
  technology:
    "Marriott technology and distribution stack required beyond headline fees—budget PMS, CRS, and digital marketing line items.",
};

const TRIBUTE_OPERATIONS_COMPLIANCE = {
  qa_cadence:
    "QA at opening and periodic collection reviews—conversion properties should plan for PIP and design-compliance cycles.",
  training_rigor:
    "Marriott opening training and ongoing service standards—confirm training fees and remediation paths in franchise materials.",
  reporting:
    "Financial and quality reporting through brand-mandated tools—confirm deadlines and audit rights in the agreement.",
  brand_interaction:
    "Design narrative review, standards compliance, and QA with Marriott collection teams—budget owner and management time.",
};

const FOOTPRINT_REGION_NAMES = {
  am: "Americas",
  cala: "CALA",
  eu: "Europe",
  mea: "MEA",
  apac: "APAC",
};

const FOOTPRINT_REGION_CODE_TO_KEY = {
  am: "AM",
  cala: "CALA",
  eu: "EU",
  mea: "MEA",
  apac: "APAC",
};

function sourceBackedRegionFootprint(brand, regionCode) {
  const key = FOOTPRINT_REGION_CODE_TO_KEY[regionCode];
  if (!key) return null;
  const rd = brand?.footprint?.regionalDistribution?.[key];
  if (!rd || typeof rd !== "object") return null;
  const hotels = Number(rd.hotels) || 0;
  const rooms = Number(rd.rooms) || 0;
  const pipelineHotels = Number(rd.pipelineHotels) || 0;
  const conversionHotels = Number(rd.conversionHotels) || 0;
  if (hotels > 0 || rooms > 0 || pipelineHotels > 0 || conversionHotels > 0) {
    return { hotels, rooms, pipelineHotels, conversionHotels };
  }
  return null;
}

function remediateV20ACopy(slotKey, proposedTitle, proposedBody, tributeBrand) {
  const key = toText(slotKey);
  const oldTitle = toText(proposedTitle);
  const oldBody = toText(proposedBody);
  let title = oldTitle;
  let body = oldBody;
  let sourceBasis = "";
  let writeReadinessBatch = BATCH_1;
  let tributeSpecificRationale = "";
  let movedToSourceEvidence = false;

  if (LOYALTY_SOURCE_EVIDENCE_SLOTS.has(key)) {
    movedToSourceEvidence = true;
    writeReadinessBatch = BATCH_2_SOURCE;
    sourceBasis = "Marriott Bonvoy program mechanics and tier structure—requires stewarded loyalty facts before display copy";
    tributeSpecificRationale =
      "Earn/redeem mechanics and elite tier rows need approved Bonvoy source facts; weak placeholder copy removed.";
    return {
      title: "",
      body: "",
      sourceBasis,
      writeReadinessBatch,
      tributeSpecificRationale,
      movedToSourceEvidence,
      oldTitle,
      oldBody,
      evidenceNeeded:
        key === "loyalty.elite"
          ? "Bonvoy elite tier names and owner-facing tier benefits from approved loyalty materials."
          : "Bonvoy earn/redeem mechanic bullets from approved Marriott Bonvoy page or stewarded loyalty facts—no invented mechanics.",
    };
  }

  if (key === "commercial.intro") {
    body =
      "Tribute Portfolio is Marriott's soft collection for independent hotels—owners retain identity and local programming while adding Bonvoy access, Marriott reservation paths, and commercial support. Underwrite on conversion scope, market tier, and net fees—not limited-service reflag economics.";
    sourceBasis = "Marriott Tribute positioning themes + soft-collection owner pattern (Curio/Kimpton structure, Tribute wording)";
    tributeSpecificRationale = "Owner-facing framing tied to Tribute conversion economics—not generic visibility uplift.";
  } else if (key === "commercial.lever.distribution") {
    title = "Distribution";
    body =
      "Marriott reservation infrastructure and brand.com paths extend reach for independent assets that historically relied on local and OTA mix.\n\nProject impact: Useful when your comp set already indexes on branded distribution; model net contribution after franchise and loyalty costs.";
    sourceBasis = "Completed-brand commercial lever pattern + Tribute independent-to-affiliated conversion lens";
    tributeSpecificRationale = "Removed generic global-distribution claim; emphasizes independent baseline and net-fee underwriting.";
  } else if (key === "commercial.lever.revenue_management") {
    title = "Revenue Management";
    body =
      "Revenue tools and brand commercial support aligned to collection positioning and market tier.\n\nProject impact: Underwrite ADR and channel-mix changes explicitly—affiliation shifts mix; it does not replace local product investment.";
    sourceBasis = "Completed-brand commercial lever pattern + Tribute owner underwriting caution";
    tributeSpecificRationale = "Removed uplift/guarantee tone; states mix change without performance promise.";
  } else if (key.startsWith("operations.model.")) {
    const field = key.replace("operations.model.", "");
    body = TRIBUTE_OPERATIONS_MODEL[field] || body;
    if (isReferenceBrandPaste(oldBody) || isReferenceBrandPaste(body)) {
      body = TRIBUTE_OPERATIONS_MODEL[field] || "Tribute Portfolio operating model field—confirm with Marriott collection standards.";
    }
    sourceBasis = "Marriott Operational Support / Brand Standards themes + Curio-style soft-collection operating model pattern (Tribute-specific)";
    tributeSpecificRationale = "Replaced pasted reference-brand copy with Tribute soft-collection operating model language.";
  } else if (key.startsWith("operations.compliance.")) {
    const field = key.replace("operations.compliance.", "");
    body = TRIBUTE_OPERATIONS_COMPLIANCE[field] || body;
    if (isReferenceBrandPaste(oldBody)) {
      body = TRIBUTE_OPERATIONS_COMPLIANCE[field] || "Tribute Portfolio compliance field—confirm with franchise agreement.";
    }
    sourceBasis = "Marriott collection QA/compliance themes + soft-collection conversion pattern (Tribute-specific)";
    tributeSpecificRationale = "Replaced pasted reference-brand compliance copy with Tribute collection QA framing.";
  } else if (key.startsWith("footprint.region.")) {
    const code = key.split(".").pop();
    const regionName = FOOTPRINT_REGION_NAMES[code] || code.toUpperCase();
    title = regionName;
    const regions = tributeRegionLabels(tributeBrand);
    const footprintFacts = sourceBackedRegionFootprint(tributeBrand, code);
    const listedInRegionOffered = regions.length > 0 && regionOfferedIncludes(tributeBrand, code);
    if (footprintFacts) {
      body = `Brand Footprint setup shows Tribute Portfolio distribution in ${regionName}—underwrite conversion scope, market tier, and operating complexity for your asset; do not treat this card as a performance forecast.`;
      sourceBasis = `Brand Setup - Brand Footprint regional distribution (${regionName})`;
      tributeSpecificRationale =
        "Regional card uses source-backed footprint fields only—no invented hotel counts or market claims.";
    } else if (listedInRegionOffered) {
      body = `${regionName} is listed in Tribute Region Offered. Owner diligence: confirm market tier, conversion scope, and operating complexity for your asset—do not infer hotel density from this card alone.`;
      sourceBasis = `Brand Basics Region Offered (${regions.join(", ")})—high-level only`;
      tributeSpecificRationale =
        "High-level regional card from Region Offered without fake market-specific footprint claims.";
    } else {
      body = `Confirm ${regionName} development interest and market tier with Marriott development before underwriting this region.`;
      sourceBasis = "No Region Offered or footprint regional facts for this card";
      tributeSpecificRationale = "Kept high-level without invented regional specificity.";
    }
  } else {
    sourceBasis = "";
    tributeSpecificRationale = "";
  }

  return {
    title,
    body,
    sourceBasis,
    writeReadinessBatch,
    tributeSpecificRationale,
    movedToSourceEvidence,
    oldTitle,
    oldBody,
    evidenceNeeded: "",
  };
}

function loadV19Report() {
  const reportPath = path.join(ROOT, "reports", V19_REPORT_JSON);
  if (!fs.existsSync(reportPath)) {
    throw new Error(`Missing v19 planner report at reports/${V19_REPORT_JSON}. Run brand-explorer-slot-completion-planner first.`);
  }
  return JSON.parse(fs.readFileSync(reportPath, "utf8"));
}

function listFixtureFiles() {
  const fixturesDir = path.join(ROOT, "fixtures");
  if (!fs.existsSync(fixturesDir)) return [];
  return fs
    .readdirSync(fixturesDir)
    .filter((n) => /^brand-explorer-presentation-.*\.json$/i.test(n))
    .map((n) => `fixtures/${n}`)
    .sort();
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

function currentTributeSlotValue(brand, slotKey) {
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  const hits = blocks.filter((b) => b && String(b.slotKey) === String(slotKey));
  if (!hits.length) {
    return { present: false, title: "", body: "", merged: "", recordIds: [] };
  }
  const parts = hits.map((b) => {
    const title = toText(b.title);
    const body = toText(b.body);
    return [title, body].filter(hasVal).join(title && body ? ": " : "");
  });
  return {
    present: true,
    title: toText(hits[0]?.title),
    body: hits.map((b) => toText(b.body)).filter(hasVal).join("\n\n"),
    merged: parts.filter(hasVal).join("\n\n"),
    recordIds: hits.map((b) => toText(b.recordId)).filter(hasVal),
  };
}

function detectWordingRisks(slotKey, title, body) {
  const risks = [];
  const combined = `${title}\n${body}`;
  const wc = wordCount(combined);

  const genericPatterns = [
    { rx: /helps owners lift/i, label: "generic uplift framing" },
    { rx: /distinctive style and local flavor/i, label: "repeated Marriott marketing phrase" },
    { rx: /pending founder review/i, label: "meta placeholder language in body" },
    { rx: /Owner-facing .+ summary for Tribute Portfolio/i, label: "generic operations placeholder" },
    { rx: /Illustrative mechanics only/i, label: "placeholder loyalty mechanics" },
    { rx: /source-backed rows required/i, label: "incomplete loyalty tier content" },
    { rx: /Adapt from reference:/i, label: "unresolved reference adaptation" },
    { rx: /Editorial placeholder/i, label: "unresolved placeholder copy" },
    { rx: /(Radisson Blu by Choice|Kimpton Hotels|Curio Collection by Hilton|Ascend Hotel Collection):/i, label: "completed-brand reference copy pasted verbatim — must rewrite for Tribute" },
  ];
  const overclaimPatterns = [
    { rx: /\bguarantee\b/i, label: "guarantee language" },
    { rx: /\balways\b/i, label: "absolute claim" },
    { rx: /\bleading\b/i, label: "superlative claim" },
    { rx: /improves visibility/i, label: "performance uplift implication" },
    { rx: /not a guarantee of uplift/i, label: "still implies uplift path (review tone)" },
  ];
  const unsupportedPatterns = [
    { rx: /\d+%/, label: "numeric percentage without cited source" },
    { rx: /\$\d/, label: "dollar figure without cited source" },
    { rx: /\d+\+?\s*members/i, label: "member scale figure without source" },
  ];

  for (const { rx, label } of genericPatterns) {
    if (rx.test(combined)) risks.push({ type: "generic", message: label });
  }
  for (const { rx, label } of overclaimPatterns) {
    if (rx.test(combined)) risks.push({ type: "overclaimed", message: label });
  }
  for (const { rx, label } of unsupportedPatterns) {
    if (rx.test(combined)) risks.push({ type: "unsupported", message: label });
  }

  const maxWords = /^commercial\.lever\./i.test(slotKey) ? 95 : /^operations\.model\./i.test(slotKey) ? 55 : 80;
  if (wc > maxWords) {
    risks.push({ type: "too_long", message: `body+title ${wc} words (guideline ≤${maxWords})` });
  }
  if (/^commercial\.kpi\./i.test(slotKey) && wc < 3) {
    risks.push({ type: "generic", message: "KPI strip may be too thin versus completed-brand examples" });
  }
  if (/^footprint\.region\./i.test(slotKey) && /Selective presence/i.test(body)) {
    risks.push({ type: "generic", message: "regional card uses deprecated selective-presence template" });
  }

  return risks;
}

function scoreFromPresentKeys(presentCount, totalRequiredKeys) {
  const reqTotal = totalRequiredKeys || 1;
  const base = (presentCount / reqTotal) * 80;
  return Math.max(0, Math.round(base));
}

function buildReviewRow(v19Plan, tributeBrand) {
  const slotKey = v19Plan.slotKey;
  const current = currentTributeSlotValue(tributeBrand, slotKey);
  const remediation = remediateV20ACopy(
    slotKey,
    v19Plan.proposedTitle,
    v19Plan.proposedBody,
    tributeBrand
  );
  const proposedTitle = remediation.title;
  const proposedBody = remediation.body;
  const completedBrandPattern = (v19Plan.completedBrandExamples || [])[0] || "";
  const wordingRisks = remediation.movedToSourceEvidence
    ? []
    : detectWordingRisks(slotKey, proposedTitle, proposedBody);
  const sourceBasis =
    remediation.sourceBasis || v19Plan.sourceBasisAvailable || "";

  return {
    slotKey,
    tab: v19Plan.tab || tabFromSlot(slotKey),
    section: v19Plan.section || sectionFromSlot(slotKey),
    completedBrandExamples: v19Plan.completedBrandExamples || [],
    completedBrandPatternUsed: completedBrandPattern,
    currentTributeValue: {
      present: current.present,
      title: current.title,
      body: current.body,
      merged: current.merged,
      recordIds: current.recordIds,
    },
    proposedTitle,
    proposedBody,
    copyRemediation: {
      applied: Boolean(remediation.tributeSpecificRationale || remediation.movedToSourceEvidence),
      oldProposedTitle: remediation.oldTitle,
      oldProposedBody: remediation.oldBody,
      tributeSpecificRationale: remediation.tributeSpecificRationale,
      evidenceNeeded: remediation.evidenceNeeded || "",
    },
    sourceBasis,
    reviewStatus: REVIEW_STATUS,
    writeReadinessBatch: remediation.writeReadinessBatch,
    safeForBatch1: remediation.writeReadinessBatch === BATCH_1,
    movedToSourceEvidenceRequired: remediation.movedToSourceEvidence,
    visibleInUi: v19Plan.visibleInUi !== false,
    airtableTable: PRESENTATION_TABLE,
    airtableFieldsNeeded: {
      required: [PRESENTATION_FIELDS.slotKey, PRESENTATION_FIELDS.body, PRESENTATION_FIELDS.brand],
      optional: [PRESENTATION_FIELDS.title, PRESENTATION_FIELDS.sortOrder, PRESENTATION_FIELDS.active, PRESENTATION_FIELDS.brandName],
      fieldMap: PRESENTATION_FIELDS,
    },
    writableLater: remediation.writeReadinessBatch === BATCH_1,
    v20BOperation: current.present ? "update_existing_presentation_row_by_slot_key" : "create_presentation_row",
    wordingRisks,
    hasWordingRisks: wordingRisks.length > 0,
  };
}

export async function buildBrandExplorerSlotCompletionReviewPackageReport(options = {}) {
  const brandIdOrName = normalizeBrandInput(options.brandIdOrName);
  const v19 = loadV19Report();
  const tribute = await fetchBrand(brandIdOrName);
  if (!tribute) throw new Error(`Unable to read target brand: ${brandIdOrName}`);

  const v19Plans = Array.isArray(v19.slotCompletionPlans) ? v19.slotCompletionPlans : [];
  const batch1Keys = new Set(v19.batch1SafeEditorialHumanReview || []);

  const selected = [];
  const excluded = [];
  const movedToSourceEvidence = [];
  const remediatedSlots = [];

  for (const plan of v19Plans) {
    const writeBatch = plan.writeReadinessBatch || (batch1Keys.has(plan.slotKey) ? BATCH_1 : "");
    const exclusion = isExcludedSlot(plan.slotKey, writeBatch);
    if (exclusion.excluded) {
      excluded.push({
        slotKey: plan.slotKey,
        tab: plan.tab || tabFromSlot(plan.slotKey),
        writeReadinessBatch: writeBatch,
        reason: exclusion.reason,
      });
      continue;
    }
    const row = buildReviewRow(plan, tribute);
    if (row.movedToSourceEvidenceRequired) {
      movedToSourceEvidence.push(row);
      excluded.push({
        slotKey: row.slotKey,
        tab: row.tab,
        writeReadinessBatch: BATCH_2_SOURCE,
        reason: "moved to source_evidence_required after copy remediation (loyalty mechanics/tiers)",
      });
      if (row.copyRemediation?.applied) remediatedSlots.push(row);
      continue;
    }
    selected.push(row);
    if (row.copyRemediation?.applied) remediatedSlots.push(row);
  }

  for (const slotKey of batch1Keys) {
    if (selected.some((r) => r.slotKey === slotKey) || excluded.some((r) => r.slotKey === slotKey)) continue;
    const exclusion = isExcludedSlot(slotKey, BATCH_1);
    if (exclusion.excluded) {
      excluded.push({ slotKey, tab: tabFromSlot(slotKey), writeReadinessBatch: BATCH_1, reason: exclusion.reason });
    }
  }

  const selectedByTab = {};
  for (const tab of TAB_ORDER) selectedByTab[tab] = [];
  for (const row of selected) {
    if (!selectedByTab[row.tab]) selectedByTab[row.tab] = [];
    selectedByTab[row.tab].push(row.slotKey);
  }

  const baselineScore = v19.v18Baseline?.revisedRealisticTributeCompletionScore ?? v19.revisedScoreIfBatch1Applied ?? 9;
  const totalRequired = (v19.v18Baseline?.requiredSlotsTributeAlreadyHas?.length || 0) +
    (v19.v18Baseline?.totalMissingRequiredCandidateSlots || 101);
  const alreadyHasCount = v19.v18Baseline?.requiredSlotsTributeAlreadyHas?.length || 9;
  const projectedPresent = alreadyHasCount + selected.length;
  const projectedScore = scoreFromPresentKeys(projectedPresent, totalRequired || 110);
  const scoreGain = projectedScore - baselineScore;
  const stillMissingAfterV20A = (v19.v18Baseline?.totalMissingRequiredCandidateSlots || 101) - selected.length;
  const comparableAfterV20A = projectedScore >= 85 && stillMissingAfterV20A === 0;

  const v20BSlots = selected.filter((r) => r.safeForBatch1).map((r) => r.slotKey).sort((a, b) => a.localeCompare(b));
  const slotsWithRisks = selected.filter((r) => r.hasWordingRisks);
  const slotsRemovedFromBatch1 = movedToSourceEvidence.map((r) => r.slotKey);
  const proposedCopyHasReferenceBrandLanguage = selected.some(
    (r) => isReferenceBrandPaste(r.proposedBody) || isReferenceBrandPaste(r.proposedTitle)
  );
  const batch1CriticalWordingRisksClear = slotsWithRisks.length === 0;
  const v20BWriterSafeToBuild =
    batch1CriticalWordingRisksClear &&
    !proposedCopyHasReferenceBrandLanguage &&
    selected.every((r) => r.safeForBatch1) &&
    movedToSourceEvidence.every((r) => LOYALTY_SOURCE_EVIDENCE_SLOTS.has(r.slotKey));

  const radissonPasteFixedSlots = remediatedSlots
    .filter(
      (r) =>
        (r.slotKey.startsWith("operations.model.") || r.slotKey.startsWith("operations.compliance.")) &&
        isReferenceBrandPaste(r.copyRemediation?.oldProposedBody)
    )
    .map((r) => r.slotKey);

  return {
    writerVersion: WRITER_VERSION,
    copyRemediationComplete: true,
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
      `reports/${REPORT_JSON_NAME}`,
      `reports/${V19_REPORT_JSON}`,
      "reports/brand-explorer-slot-completion-planner.md",
      "reports/brand-explorer-slot-standard-manifest.md",
      "docs/brand-explorer-presentation-slots.md",
      "lib/partner-intelligence/brand-explorer-slot-completion-review-package.js",
      "api/brand-library.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
      ...listFixtureFiles(),
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-slot-completion-review-package.js",
      "scripts/brand-explorer-slot-completion-review-package.mjs",
      "docs/data-intelligence/brand-explorer-slot-completion-review-package-v20A.md",
      "reports/brand-explorer-slot-completion-review-package.md",
      "reports/brand-explorer-slot-completion-review-package.json",
      "package.json",
    ],
    v20AReviewPackageExists: true,
    brand: v19.brand || { recordId: DEFAULT_BRAND_ID, name: "Tribute Portfolio" },
    v19Source: {
      batch1Count: batch1Keys.size,
      baselineScore,
      totalRequiredSlots: totalRequired || 110,
      alreadyHasRequiredSlots: v19.v18Baseline?.requiredSlotsTributeAlreadyHas || [],
    },
    slotsSelectedCount: selected.length,
    safeBatch1SlotCount: selected.length,
    slotsRemovedFromBatch1,
    slotsMovedToSourceEvidenceRequired: movedToSourceEvidence.map((r) => ({
      slotKey: r.slotKey,
      tab: r.tab,
      evidenceNeeded: r.copyRemediation?.evidenceNeeded || "",
      oldProposedBody: r.copyRemediation?.oldProposedBody || "",
    })),
    copyRemediationRecords: remediatedSlots.map((r) => ({
      slotKey: r.slotKey,
      tab: r.tab,
      oldProposedTitle: r.copyRemediation?.oldProposedTitle || "",
      oldProposedBody: r.copyRemediation?.oldProposedBody || "",
      newProposedTitle: r.proposedTitle,
      newProposedBody: r.proposedBody,
      tributeSpecificRationale: r.copyRemediation?.tributeSpecificRationale || "",
      sourceBasis: r.sourceBasis,
      safeForBatch1: r.safeForBatch1,
      movedToSourceEvidenceRequired: r.movedToSourceEvidenceRequired,
      remainingWordingRisks: r.wordingRisks,
    })),
    radissonPasteFixedSlots,
    operationsModelCopySummary: selected
      .filter((r) => r.slotKey.startsWith("operations.model."))
      .map((r) => ({ slotKey: r.slotKey, body: short(r.proposedBody, 160) })),
    operationsComplianceCopySummary: selected
      .filter((r) => r.slotKey.startsWith("operations.compliance."))
      .map((r) => ({ slotKey: r.slotKey, body: short(r.proposedBody, 160) })),
    slotsSelected: v20BSlots,
    slotsSelectedByTab: selectedByTab,
    slotsExcluded: excluded.sort((a, b) => a.slotKey.localeCompare(b.slotKey)),
    slotsExcludedCount: excluded.length,
    reviewPackageRows: selected,
    proposedCopyBySlot: selected.map((r) => ({
      slotKey: r.slotKey,
      tab: r.tab,
      title: r.proposedTitle,
      body: r.proposedBody,
      completedBrandPatternUsed: r.completedBrandPatternUsed,
      sourceBasis: r.sourceBasis,
      reviewStatus: r.reviewStatus,
    })),
    wordingRisksBySlot: slotsWithRisks.map((r) => ({
      slotKey: r.slotKey,
      risks: r.wordingRisks,
    })),
    wordingRisksSummary: {
      slotsWithRisks: slotsWithRisks.length,
      totalRiskFlags: slotsWithRisks.reduce((n, r) => n + r.wordingRisks.length, 0),
    },
    estimatedScoreGainIfFirstWaveApplied: scoreGain,
    projectedScoreIfFirstWaveApplied: projectedScore,
    baselineScoreBeforeFirstWave: baselineScore,
    tributeCompletedBrandComparableAfterFirstWave: comparableAfterV20A,
    v20BGatedWriterShouldBeBuilt: v20BWriterSafeToBuild,
    v20BWriterSafeToBuild,
    batch1CriticalWordingRisksClear,
    proposedCopyReferenceBrandLanguagePresent: proposedCopyHasReferenceBrandLanguage,
    v20BApplyBatchAfterReview: v20BWriterSafeToBuild ? v20BSlots : [],
    v20BPrerequisites: [
      "Founder review of all proposed copy in this package",
      "Resolve or accept wording risk flags",
      "Explicit --approve-brand-explorer-slot-completion-v20B flag (future)",
      "No Company Validated / Company Validation Date changes",
      "No image attachments in v20B first wave",
    ],
    exactNextCommand:
      "npm run brand-explorer-slot-completion-review-package -- --brand tribute-portfolio --dry-run",
  };
}

export function buildBrandExplorerSlotCompletionReviewPackageMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Slot Completion Review Package v20A");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push("");
  lines.push("## Package scope");
  lines.push(`- v19 Batch 1 source slots: **${report.v19Source.batch1Count}**`);
  lines.push(`- v20A selected for review (post-remediation): **${report.slotsSelectedCount}**`);
  lines.push(`- Safe Batch 1 slots: **${report.safeBatch1SlotCount}**`);
  lines.push(`- Removed from Batch 1 (source evidence): **${report.slotsRemovedFromBatch1.length}**`);
  lines.push(`- Copy remediation complete: **${report.copyRemediationComplete ? "yes" : "no"}**`);
  lines.push(`- Excluded from v20A: **${report.slotsExcludedCount}**`);
  lines.push("");
  lines.push("## Score projection");
  lines.push(`- Baseline (v19): **${report.baselineScoreBeforeFirstWave}/100**`);
  lines.push(`- If v20A first wave applied: **${report.projectedScoreIfFirstWaveApplied}/100** (+${report.estimatedScoreGainIfFirstWaveApplied})`);
  lines.push(
    `- Completed-brand comparable after v20A: **${report.tributeCompletedBrandComparableAfterFirstWave ? "yes" : "no"}**`
  );
  lines.push(`- v20B gated writer recommended: **${report.v20BGatedWriterShouldBeBuilt ? "yes" : "no"}**`);
  lines.push(`- v20B writer safe to build: **${report.v20BWriterSafeToBuild ? "yes" : "no"}**`);
  lines.push(`- Batch 1 critical wording risks clear: **${report.batch1CriticalWordingRisksClear ? "yes" : "no"}**`);
  lines.push(
    `- Reference-brand language in proposed copy: **${report.proposedCopyReferenceBrandLanguagePresent ? "yes" : "no"}**`
  );
  lines.push("");
  if (report.radissonPasteFixedSlots?.length) {
    lines.push("## Radisson paste remediated");
    report.radissonPasteFixedSlots.forEach((s) => lines.push(`- ${s}`));
    lines.push("");
  }
  if (report.slotsRemovedFromBatch1?.length) {
    lines.push("## Moved to source evidence required");
    report.slotsRemovedFromBatch1.forEach((s) => lines.push(`- ${s}`));
    lines.push("");
  }
  lines.push("## Selected slots by tab");
  TAB_ORDER.forEach((tab) => {
    const slots = report.slotsSelectedByTab[tab] || [];
    if (!slots.length) return;
    lines.push(`### ${tab} (${slots.length})`);
    slots.forEach((s) => lines.push(`- ${s}`));
    lines.push("");
  });
  lines.push("## Excluded slots (sample)");
  report.slotsExcluded.slice(0, 40).forEach((row) => {
    lines.push(`- \`${row.slotKey}\` (${row.tab}) — ${row.reason}`);
  });
  if (report.slotsExcluded.length > 40) {
    lines.push(`- …${report.slotsExcluded.length - 40} more in JSON`);
  }
  lines.push("");
  lines.push("## Wording risks");
  lines.push(`- Slots flagged: **${report.wordingRisksSummary.slotsWithRisks}**`);
  lines.push(`- Total risk flags: **${report.wordingRisksSummary.totalRiskFlags}**`);
  report.wordingRisksBySlot.slice(0, 20).forEach((row) => {
    lines.push(`- \`${row.slotKey}\`: ${row.risks.map((r) => `${r.type}:${r.message}`).join("; ")}`);
  });
  lines.push("");
  lines.push("## Proposed copy (sample)");
  report.proposedCopyBySlot.slice(0, 15).forEach((row) => {
    lines.push(`### ${row.slotKey}`);
    if (row.title) lines.push(`- Title: ${short(row.title, 120)}`);
    lines.push(`- Body: ${short(row.body, 280)}`);
    lines.push(`- Pattern: ${short(row.completedBrandPatternUsed, 120)}`);
    lines.push(`- Review: ${row.reviewStatus}`);
    lines.push("");
  });
  if (report.proposedCopyBySlot.length > 15) {
    lines.push(`_Full copy for all ${report.proposedCopyBySlot.length} slots in JSON \`proposedCopyBySlot\`._`);
    lines.push("");
  }
  lines.push("## v20B apply batch (after review)");
  if (report.v20BApplyBatchAfterReview.length) {
    report.v20BApplyBatchAfterReview.forEach((s) => lines.push(`- ${s}`));
  } else {
    lines.push("- None yet — resolve remaining wording risks or complete founder review first");
  }
  lines.push("");
  lines.push("## Guardrails");
  lines.push("- No Airtable writes in v20A");
  lines.push("- Company Validated / Company Validation Date untouched");
  lines.push("- Not company-validated; Not Marriott-validated");
  lines.push("");
  lines.push("## Next command");
  lines.push("");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  lines.push("");
  return lines.join("\n");
}
