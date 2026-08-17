/**
 * Brand Explorer WoodSpring Presentation Cleanup + Required Section Backfill v33B.
 *
 * Copy-only presentation repairs, missing-section creates, and scenario quarantine.
 * No image fields, source library, registry approvals, or Company Validated changes.
 *
 * @see docs/data-intelligence/brand-explorer-woodspring-presentation-cleanup-backfill-writer-v33B.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { listPartnerSources } from "./airtable-source.js";
import { listRegistryAssetsForBrand } from "./brand-asset-registry-workflow.js";
import {
  detectWrongBrandSignageRisk,
  getDiscoveryBrandConfig,
} from "./brand-explorer-brand-asset-image-governance.js";
import {
  EXTERNAL_DISPLAY_STATUS_QUARANTINE,
  HIDDEN_EXTERNAL_DISPLAY_STATUSES,
} from "./brand-explorer-radisson-individuals-openings-suppression-writer.js";
import { isTemporaryAirtableUrl } from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import { scanCopySafety } from "./brand-explorer-choice-expansion-partial-profile-backfill-writer.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";

export const WRITER_VERSION = "v33B";
export const STAGING_RUN_ID = "v33B-woodspring-presentation-cleanup-backfill";
export const REPORT_JSON_NAME = "brand-explorer-woodspring-presentation-cleanup-backfill-writer.json";
export const REPORT_MD_NAME = "brand-explorer-woodspring-presentation-cleanup-backfill-writer.md";
export const DOC_MD_NAME = "brand-explorer-woodspring-presentation-cleanup-backfill-writer-v33B.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v33B-woodspring-presentation-cleanup-backfill";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_IMAGE_FIELDS = "--confirm-no-image-field-changes";
export const APPLY_FLAG_NO_IMAGE_APPROVAL = "--confirm-no-image-approval";
export const APPLY_FLAG_NO_SOURCE_LIBRARY = "--confirm-no-source-library-changes";
export const APPLY_FLAG_WOODSPRING_ONLY = "--confirm-woodspring-only";

export const TARGET_BRAND = Object.freeze({
  slug: "woodspring-suites",
  recordId: "recsOd51NzRPYsMko",
  name: "WoodSpring Suites",
});

export const PROTECTED_BRAND_SLUGS = Object.freeze([
  "everhome-suites",
  "suburban-studios",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const MIX_SLOT = "footprint.portfolio_mix";
const OPENINGS_SLOT = "footprint.openings";
const MOMENTUM_SLOT = "footprint.momentum";

const MIN_FEATURED_WORDS = 25;
const MIN_SCENARIO_WORDS = 15;
const MIN_OVERVIEW_SCENARIO_WORDS = 22;

const BLOCKED_PATCH_FIELDS = new Set([
  "Image",
  "Images",
  "Scenario Image",
  "Attachments",
  "Company Validated",
  "Company Validation Date",
]);

const FDD_RE = /\b(fdd|item\s*19|franchise disclosure document|franchise disclosure)\b/i;
const PERFORMANCE_RE =
  /\b(roi|irr|cap rate|noi|revpar|adr forecast|guaranteed returns?|published %|performance guarantee)\b/i;
const COMPANY_VALIDATION_RE =
  /company validated|company-approved|official sign-off|validated by choice/i;

const INTERNAL_LANGUAGE_PATTERNS = [
  { id: "fdd", re: /\bfdd\b/i },
  { id: "item_19", re: /\bitem\s*19\b/i },
  { id: "franchise_disclosure", re: /\bfranchise disclosure\b/i },
  { id: "franchise_disclosure_document", re: /\bfranchise disclosure document\b/i },
  { id: "confirm_fees", re: /\bconfirm fees\b/i },
  { id: "confirm_flag", re: /\bconfirm flag\b/i },
  { id: "performance_representation", re: /\bperformance representation\b/i },
  { id: "active_property_page", re: /\bactive property page\b/i },
  { id: "consumer_path", re: /\bconsumer path\b/i },
  { id: "source_data", re: /\bsource data\b/i },
  { id: "metadata", re: /\bmetadata\b/i },
  { id: "extraction", re: /\bextraction\b/i },
  { id: "source_capture", re: /\bsource[- ]capture\b/i },
  { id: "internal", re: /\binternal\b/i },
  { id: "booking_path", re: /\bbooking path\b/i },
  { id: "census", re: /\bcensus\b/i },
];

const SANITIZE_REPLACEMENTS = [
  { re: /\bowner should confirm in (the )?fdd\b/gi, replace: "owners should validate during commercial model review" },
  { re: /\bverify with (the )?fdd\b/gi, replace: "confirm during owner diligence" },
  { re: /\bconfirm in (the )?fdd\b/gi, replace: "confirm during owner diligence" },
  { re: /\bconfirm choice privileges[^.]*item 19[^.]*\./gi, replace: "Review Choice Privileges participation during owner diligence." },
  { re: /\bfranchise disclosure document\b/gi, replace: "commercial model review materials" },
  { re: /\bfranchise disclosure\b/gi, replace: "commercial model review" },
  { re: /\bitem\s*19\b/gi, replace: "operating economics review" },
  { re: /\bitem\s*20\b/gi, replace: "agreement terms review" },
  { re: /\bperformance representation\b/gi, replace: "operating performance considerations" },
  { re: /\bconfirm fees\b/gi, replace: "fee structure diligence" },
  { re: /\bconfirm flag\b/gi, replace: "brand participation diligence" },
  { re: /\bactive property page\b/gi, replace: "official property positioning" },
  { re: /\bconsumer path\b/gi, replace: "guest booking channels" },
  { re: /\bbooking path\b/gi, replace: "distribution channels" },
  { re: /\bsource[- ]capture\b/gi, replace: "reference review" },
  { re: /\bsource data\b/gi, replace: "reference materials" },
  { re: /\bcensus\b/gi, replace: "portfolio footprint reference" },
  { re: /\bmetadata\b/gi, replace: "profile details" },
  { re: /\bextraction\b/gi, replace: "reference review" },
  { re: /\bfdd\b/gi, replace: "owner diligence materials" },
  { re: /\bnot a quote or substitute for[^.]*\./gi, replace: "Owners should complete commercial model review with qualified advisors." },
  { re: /\billustrative only[^.]*\./gi, replace: "Use for owner planning context only—not performance guidance." },
];

export const WOODSPRING_BACKFILL = Object.freeze({
  "overview.featured_application": {
    title: "Extended-Stay Owner Fit",
    body:
      "WoodSpring Suites is a Choice extended-stay brand positioned around simple, longer-stay lodging with in-room kitchens and a practical operating model. For owners, the brand is most relevant when evaluating economy or midscale extended-stay demand, weekly-stay guest segments, and markets where operational simplicity and Choice distribution support matter.",
  },
  "overview.portfolio_context": {
    title: "2",
    body:
      "WoodSpring Suites sits within Choice Hotels' extended-stay portfolio—oriented to weekly and longer-stay guests with kitchen-equipped suites, lean public space, and economy-to-midscale extended-stay positioning for owner diligence on market demand and operating model fit.",
  },
  portfolio_context: {
    title: "Extended-Stay Portfolio Context",
    body:
      "WoodSpring participates in Choice's extended-stay platform alongside other weekly-stay brands—owners compare prototype fit, operating simplicity, and distribution support when selecting an extended-stay affiliation path.",
  },
  demand_scenario: {
    title: "Longer-Stay Demand Context",
    body:
      "WoodSpring targets weekly and longer-stay demand from workforce rotations, relocation, medical travel, education assignments, and suburban or highway-corridor extended-stay corridors. Owners should compare local longer-stay demand sources, rate sensitivity, operating model fit, and competitive extended-stay supply before selecting a brand path.",
  },
  "overview.scenario.1": {
    title: "Extended-Stay Corridor Conversion",
    body:
      "Markets with steady weekly-stay demand near employment, medical, or logistics corridors—WoodSpring fits when owners underwrite kitchen-equipped suites, lean housekeeping models, and Choice extended-stay operating standards.",
  },
  "overview.scenario.2": {
    title: "Weekly-Demand Growth Market",
    body:
      "Suburban or secondary markets with growing longer-stay guest segments—WoodSpring suits sponsors evaluating simple suite product, select-service extended-stay positioning, and Choice platform distribution.",
  },
  "overview.scenario.3.replacement": {
    title: "Extended-Stay Competitive Positioning",
    body:
      "Owners comparing extended-stay brands in a corridor—WoodSpring fits when weekly-stay demand, simple suite operations, and Choice distribution support a practical extended-stay operating model without upscale public-space intensity.",
  },
  "valueOwners.scenario.1": {
    title: "Longer-Stay Demand Fit",
    body:
      "WoodSpring aligns when local demand supports weekly and monthly stays—owners should validate guest mix, stay length, and competitive extended-stay supply during market diligence.",
  },
  "valueOwners.scenario.2": {
    title: "Operating Model Simplicity",
    body:
      "Kitchen-equipped suites with limited F&B and lean public space—WoodSpring suits owners prioritizing operational simplicity within Choice extended-stay standards.",
  },
  "valueOwners.scenario.3": {
    title: "Choice Platform Context",
    body:
      "Choice Hotels distribution and extended-stay platform support—owners evaluate affiliation lift, guest recognition context, and development resources during brand selection.",
  },
  "valueOwners.scenario.4": {
    title: "Extended-Stay Competitive Positioning",
    body:
      "Economy and midscale extended-stay corridors—WoodSpring competes when prototype fit, weekly-rate positioning, and operating model discipline match owner underwriting assumptions.",
  },
  "loyalty.ecosystem": {
    title: "Choice Privileges Ecosystem",
    body:
      "WoodSpring participates in Choice Privileges, connecting extended-stay guests to enterprise and transient demand across the Choice network. Owners should evaluate loyalty contribution and channel mix during commercial model review—not as a performance guarantee.",
  },
  "loyalty.proof": {
    title: "Loyalty Demand Context",
    body:
      "Choice Privileges supports guest recognition and booking channels across the Choice platform. Owners should assess loyalty mix and distribution reach during underwriting without treating program scale as property-level guidance.",
  },
  "loyalty.redeem": {
    title: "Redemption & Channel Mix",
    body:
      "Extended-stay owners should understand how Choice Privileges redemption and channel participation affect booking economics. Validate channel and loyalty mix assumptions during owner diligence.",
  },
  "loyalty.kpi.mix": {
    title: "Loyalty Mix Considerations",
    body:
      "Loyalty-driven bookings can supplement extended-stay demand but vary by market and operator. Owners should review channel assumptions during commercial model review without treating program participation as forecast guidance.",
  },
  "standards.intro": {
    title: "Brand Standards Overview",
    body:
      "WoodSpring standards emphasize kitchen-equipped suites, extended-stay operating models, and practical prototype requirements suited to weekly and longer-stay guests. Owners should review brand participation requirements and development specifications during diligence.",
  },
  "economics.intro": {
    title: "Economics & Owner Considerations",
    body:
      "Extended-stay underwriting should focus on weekly rate positioning, housekeeping intensity, and conversion scope. Owners should complete commercial model review with qualified advisors—Dealality does not present specific fee amounts or performance representations.",
  },
  "economics.fee.operate": {
    title: "Operating Cost Considerations",
    body:
      "Extended-stay operating economics depend on housekeeping model, utility recovery, and weekly-rate positioning. Owners should validate operating assumptions during commercial model review without relying on generic disclosure excerpts.",
  },
  "economics.fee.join": {
    title: "Initial Investment Considerations",
    body:
      "Joining costs vary by conversion scope, market, and prototype alignment. Owners should review development and conversion estimates during diligence with qualified advisors.",
  },
  "commercial.intro": {
    title: "Commercial Positioning",
    body:
      "WoodSpring targets extended-stay demand with kitchen-equipped suites and Choice platform distribution. Owners should align commercial positioning to weekly-stay corridors and operating model fit during diligence.",
  },
  "commercial.theme": {
    title: "Extended-Stay Commercial Theme",
    body:
      "Simple suite product and weekly-stay orientation within Choice's extended-stay platform—owners evaluate market demand, prototype fit, and operating model discipline during brand selection.",
  },
  "insight.summary": {
    title: "Owner Planning Summary",
    body:
      "WoodSpring Suites fits owner diligence when longer-stay demand, economy extended-stay positioning, and Choice distribution support align with market underwriting—confirm prototype and operating assumptions during commercial model review.",
  },
  "overview.scenarios": {
    title: "Value Scenario Overview",
    body:
      "WoodSpring value scenarios focus on longer-stay demand fit, operating model simplicity, and Choice platform context—owners compare weekly-stay corridors, suite product requirements, and competitive extended-stay supply during diligence.",
  },
  "economics.kpi.fee_stack": {
    title: "Fee Stack Diligence",
    body:
      "Owners should map franchise, marketing, and technology fees during underwriting. Dealality summarizes considerations only—confirm fee stack details with Choice development representatives.",
  },
  "economics.fee": {
    title: "Fee Structure Diligence",
    body:
      "Owners should validate franchise and operating fee components during commercial model review. Confirm economics with Choice development counsel during owner diligence.",
  },
  "footprint.growth.narrative": {
    title: "Geographic Footprint",
    body:
      "WoodSpring maintains a U.S.-oriented presence within the Choice extended-stay portfolio. Owners should compare market fit, competitive extended-stay supply, and corridor demand drivers when evaluating affiliation.",
  },
  "footprint.editorial": {
    title: "Footprint Context",
    body:
      "WoodSpring's footprint reflects Choice's extended-stay platform strategy in North America. Owners should validate local extended-stay supply and demand orientation during market diligence.",
  },
});

export const WOODSPRING_PORTFOLIO_MIX_CHIPS = Object.freeze([
  { title: "Extended-Stay Positioning", body: "High", sort: 0 },
  { title: "Weekly-Stay Demand Orientation", body: "High", sort: 1 },
  { title: "Simple Suite Product", body: "High", sort: 2 },
  { title: "Choice Platform Context", body: "Moderate", sort: 3 },
  { title: "Owner Fit / Market Fit Considerations", body: "Moderate", sort: 4 },
]);

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-woodspring-source-registry-readiness-writer.json",
  "reports/brand-explorer-choice-extended-stay-batch-readiness-audit.json",
  "reports/brand-explorer-choice-extended-stay-source-capture-writer.json",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "lib/partner-intelligence/brand-explorer-final-qa-auditor.js",
  "lib/partner-intelligence/brand-explorer-complete-build-orchestrator.js",
  "live WoodSpring Presentation / Facts / Sources / Registry / API",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-woodspring-presentation-cleanup-backfill-writer.js",
  "scripts/brand-explorer-woodspring-presentation-cleanup-backfill-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "package.json",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function wordCount(text) {
  return nz(text).split(/\s+/).filter(Boolean).length;
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

export function v33bWriterExists() {
  return fs.existsSync(
    path.join(ROOT, "lib/partner-intelligence/brand-explorer-woodspring-presentation-cleanup-backfill-writer.js")
  );
}

export function sanitizeWoodspringCopy(text) {
  let out = nz(text);
  if (!out) return out;
  for (const rule of SANITIZE_REPLACEMENTS) {
    out = out.replace(rule.re, rule.replace);
  }
  return out
    .split("\n")
    .map((line) => line.replace(/[ \t]{2,}/g, " ").trim())
    .join("\n")
    .trim();
}

function inferSection(slotKey) {
  const sk = nz(slotKey);
  if (sk.startsWith("overview.")) return "overview";
  if (sk.startsWith("valueOwners.")) return "valueOwners";
  if (sk === OPENINGS_SLOT) return "footprint.openings";
  if (sk === MOMENTUM_SLOT || sk.startsWith("footprint.momentum.")) return "footprint.momentum";
  if (sk.startsWith("footprint.")) return "geographic_footprint";
  if (sk.startsWith("loyalty.")) return "loyalty";
  if (sk.startsWith("economics.")) return "economics";
  if (sk.startsWith("standards.")) return "standard_detail";
  if (sk === MIX_SLOT) return "portfolio_mix";
  if (sk === "portfolio_context" || sk.startsWith("overview.portfolio")) return "portfolio_context";
  if (sk === "demand_scenario" || sk.startsWith("demand.")) return "demand_scenario";
  return sk.split(".")[0] || "other";
}

function scanInternalLanguage(text, recordId, slotKey) {
  const findings = [];
  for (const pat of INTERNAL_LANGUAGE_PATTERNS) {
    const m = nz(text).match(pat.re);
    if (m) {
      findings.push({ recordId, slotKey, patternId: pat.id, phrase: m[0] });
    }
  }
  return findings;
}

function isThinCopy(slotKey, title, body) {
  const wc = wordCount(body);
  if (slotKey === "overview.featured_application") return wc < MIN_FEATURED_WORDS;
  if (slotKey.startsWith("overview.scenario")) return wc < MIN_OVERVIEW_SCENARIO_WORDS;
  if (slotKey.startsWith("valueOwners.scenario")) return wc < MIN_SCENARIO_WORDS;
  if (!body && title) return true;
  if (!body && !title) return true;
  return wc < 10 && /loyalty|economics|standards|portfolio|demand|footprint/i.test(slotKey);
}

function presentationFields({ slotKey, title, body, sort, brandRecordId, brandName, externalDisplayStatus }) {
  const fields = {
    "Slot Key": slotKey,
    Title: title || "",
    Body: body,
    "Brand Name": brandName,
    Brand: [brandRecordId],
    Active: true,
    "Sort Order": sort ?? 0,
  };
  if (externalDisplayStatus) fields["External Display Status"] = externalDisplayStatus;
  return fields;
}

function validatePresentationPatch(fields, { slotKey = "", allowVisibility = false } = {}) {
  const errors = [];
  for (const key of Object.keys(fields)) {
    if (BLOCKED_PATCH_FIELDS.has(key)) errors.push(`blocked_field:${key}`);
    if (key === "External Display Status" && !allowVisibility) {
      errors.push("visibility_change_not_allowed");
    }
    if (
      (slotKey === OPENINGS_SLOT || slotKey === MOMENTUM_SLOT || slotKey.startsWith("footprint.momentum.")) &&
      (key === "Body" || key === "Title")
    ) {
      errors.push("openings_momentum_deferred_v33C");
    }
  }
  const combined = `${fields.Title || ""}\n${fields.Body || ""}`;
  if (FDD_RE.test(combined)) errors.push("fdd_language");
  if (PERFORMANCE_RE.test(combined)) errors.push("performance_claim");
  if (COMPANY_VALIDATION_RE.test(combined)) errors.push("company_validation_implication");
  const safety = scanCopySafety(combined);
  if (safety.length) errors.push(`copy_safety:${safety.join(",")}`);
  return errors;
}

async function fetchBrandApiShape(brandIdOrName) {
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
  if (res.statusCode !== 200 || !res.payload?.brand) return null;
  return res.payload.brand;
}

async function airtableFetch(baseId, apiKey, tableName, init = {}, recordId = "") {
  const url = recordId
    ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}/${recordId}`
    : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  return { res, json };
}

async function listWoodspringPresentationRows(baseId, apiKey, brandRecordId, brandName) {
  const formula = `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`;
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const listUrl = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`;
    const listRes = await fetch(listUrl, { headers: { Authorization: `Bearer ${apiKey}` } });
    const listJson = await listRes.json().catch(() => ({}));
    if (!listRes.ok) throw new Error(listJson.error?.message || `List failed: ${listRes.status}`);
    records.push(...(listJson.records || []));
    offset = listJson.offset || "";
  } while (offset);

  return records.map((rec) => {
    const f = rec.fields || {};
    const imageKeys = ["Image", "Images", "Scenario Image", "Attachments"];
    let imageUrl = "";
    for (const key of imageKeys) {
      if (Array.isArray(f[key]) && f[key][0]?.url) {
        imageUrl = nz(f[key][0].url);
        break;
      }
    }
    return {
      recordId: rec.id,
      slotKey: nz(f["Slot Key"]),
      title: nz(f.Title),
      body: nz(f.Body),
      sortOrder: f["Sort Order"],
      active: f.Active,
      externalDisplayStatus: nz(f["External Display Status"]),
      imageUrl,
      hasImage: Boolean(imageUrl),
      summaryUrl: nz(f["Summary URL"]),
      rawFields: f,
    };
  });
}

function isScenarioQuarantineCandidate(row, brandConfig) {
  if (row.slotKey !== "overview.scenario.3") return false;
  const text = `${row.title} ${row.body}`;
  if (/\beverhome\b/i.test(text)) return true;
  const wrong = detectWrongBrandSignageRisk(text, brandConfig);
  if (wrong) return true;
  return false;
}

function proposeCopyPatch(row, brandRecordId, brandName, brandConfig) {
  if (
    row.slotKey === OPENINGS_SLOT ||
    row.slotKey === MOMENTUM_SLOT ||
    row.slotKey.startsWith("footprint.momentum.")
  ) {
    return null;
  }

  if (isScenarioQuarantineCandidate(row, brandConfig)) {
    return null;
  }

  const beforeTitle = row.title;
  const beforeBody = row.body;
  const internalBefore = scanInternalLanguage(
    `${beforeTitle}\n${beforeBody}`,
    row.recordId,
    row.slotKey
  );

  let proposedTitle = sanitizeWoodspringCopy(beforeTitle);
  let proposedBody = sanitizeWoodspringCopy(beforeBody);

  const slotBackfill =
    row.slotKey === "overview.scenario.3" ? null : WOODSPRING_BACKFILL[row.slotKey];
  if (internalBefore.length > 0 && slotBackfill?.body) {
    proposedTitle = slotBackfill.title || proposedTitle;
    proposedBody = slotBackfill.body;
  }

  const backfillKey = slotBackfill;
  const needsBackfill =
    backfillKey &&
    (isThinCopy(row.slotKey, proposedTitle, proposedBody) ||
      internalBefore.length > 0 ||
      (backfillKey.title && !proposedTitle) ||
      FDD_RE.test(`${proposedTitle}\n${proposedBody}`) ||
      PERFORMANCE_RE.test(`${proposedTitle}\n${proposedBody}`));

  if (needsBackfill) {
    if (backfillKey.title) proposedTitle = backfillKey.title;
    if (backfillKey.body) proposedBody = backfillKey.body;
  }

  if (proposedTitle === beforeTitle && proposedBody === beforeBody && !internalBefore.length) {
    return null;
  }

  let fields = presentationFields({
    slotKey: row.slotKey,
    title: proposedTitle,
    body: proposedBody,
    sort: row.sortOrder ?? 0,
    brandRecordId,
    brandName,
  });
  let validationErrors = validatePresentationPatch(fields, { slotKey: row.slotKey });

  if (
    validationErrors.length &&
    (internalBefore.length || validationErrors.some((e) => /fdd|performance|copy_safety/.test(e)))
  ) {
    const forced = WOODSPRING_BACKFILL[row.slotKey];
    if (forced?.body) {
      proposedTitle = forced.title || proposedTitle;
      proposedBody = forced.body;
      fields = presentationFields({
        slotKey: row.slotKey,
        title: proposedTitle,
        body: proposedBody,
        sort: row.sortOrder ?? 0,
        brandRecordId,
        brandName,
      });
      validationErrors = validatePresentationPatch(fields, { slotKey: row.slotKey });
    }
  }

  if (validationErrors.length) {
    return { invalid: true, recordId: row.recordId, slotKey: row.slotKey, validationErrors };
  }

  if (proposedTitle === beforeTitle && proposedBody === beforeBody) return null;

  return {
    action: "copy_patch",
    recordId: row.recordId,
    slotKey: row.slotKey,
    section: inferSection(row.slotKey),
    before: { title: beforeTitle, body: beforeBody },
    after: { title: proposedTitle, body: proposedBody },
    internalLanguageBefore: internalBefore,
    internalLanguageAfter: scanInternalLanguage(
      `${proposedTitle}\n${proposedBody}`,
      row.recordId,
      row.slotKey
    ),
    fields,
    fixReason: internalBefore.length ? "internal_language_cleanup" : "thin_copy_backfill",
  };
}

function proposeScenarioQuarantine(row, brandConfig) {
  if (!isScenarioQuarantineCandidate(row, brandConfig)) return null;
  if (row.externalDisplayStatus === EXTERNAL_DISPLAY_STATUS_QUARANTINE) return null;

  const fields = { "External Display Status": EXTERNAL_DISPLAY_STATUS_QUARANTINE };
  const errors = validatePresentationPatch(fields, {
    slotKey: row.slotKey,
    allowVisibility: true,
  });
  if (errors.length) return null;

  return {
    action: "quarantine",
    recordId: row.recordId,
    slotKey: row.slotKey,
    section: "overview.scenario",
    before: { externalDisplayStatus: row.externalDisplayStatus || null },
    after: { externalDisplayStatus: EXTERNAL_DISPLAY_STATUS_QUARANTINE },
    fields,
    fixReason: "wrong_brand_or_everhome_reference_quarantine",
    imageUntouched: true,
  };
}

function auditBlockersFromReports() {
  const readJson = (name) => {
    try {
      return JSON.parse(fs.readFileSync(path.join(ROOT, "reports", name), "utf8"));
    } catch {
      return null;
    }
  };
  const finalQa = readJson("brand-explorer-final-qa-auditor.json");
  const completeBuild = readJson("brand-explorer-complete-build-orchestrator.json");
  const visual = readJson("brand-explorer-visual-display-defect-audit.json");

  const wsQa = (finalQa?.brandReports || []).find(
    (b) => b.brand?.slug === TARGET_BRAND.slug || b.brand?.recordId === TARGET_BRAND.recordId
  );
  const blockers = [];

  for (const d of wsQa?.defects || []) {
    blockers.push({
      defectType: d.type || d.patternId,
      severity: d.severity,
      section: inferSection(d.slotKey || d.surface || ""),
      slot: d.slotKey || null,
      recordId: d.recordId || null,
      title: null,
      bodyExcerpt: d.excerpt || d.message || "",
      imageStatus: null,
      sourceSupport: "v33A approved sources",
      issueClass: /image|visual|registry/i.test(`${d.type} ${d.message}`)
        ? "image_or_registry"
        : "content",
      proposedAction:
        d.patternId?.includes("item_19") || d.carryoverClassification?.includes("internal")
          ? "v33B internal-language cleanup"
          : d.recommendedFixBatch || "v33B backfill",
    });
  }

  const visualDefects = (visual?.defects || []).filter(
    (d) => d.brand?.slug === TARGET_BRAND.slug || d.brand?.recordId === TARGET_BRAND.recordId
  );
  for (const d of visualDefects) {
    blockers.push({
      defectType: d.defectType,
      severity: d.severity,
      section: d.section || "",
      slot: d.slotKey || null,
      recordId: d.recordId || null,
      title: d.title || null,
      bodyExcerpt: d.description || "",
      imageStatus: d.imageStatus || null,
      sourceSupport: null,
      issueClass: "image",
      proposedAction: d.remediationBatch || "v33D image governance",
    });
  }

  const wsBuild = (completeBuild?.brandResults || []).find(
    (b) => b.resolvedSlug === TARGET_BRAND.slug || b.recordId === TARGET_BRAND.recordId
  );
  for (const b of wsBuild?.remainingBlockers || completeBuild?.remainingBlockers || []) {
    blockers.push({
      defectType: b.type || "complete_build_blocker",
      severity: b.severity || "high",
      section: b.section || "",
      slot: null,
      recordId: null,
      issueClass: /openings|momentum/i.test(`${b.section} ${b.message}`) ? "scoring" : "content",
      proposedAction: b.recommendedWriter || "v33B/v33C",
      message: b.message,
    });
  }

  return blockers;
}

function buildOpeningsMomentumCandidates(sources) {
  return (sources || [])
    .filter((s) => s.approvedForExplorerUse === "Yes" || s.approvedForExplorerUse === true)
    .map((s) => ({
      recordId: s.id || s.recordId,
      sourceTitle: s.sourceTitle || s.title,
      sourceUrl: s.sourceUrl || s.url,
      momentumAppropriate: /press|development|news/i.test(`${s.sourceType} ${s.sourceTitle}`),
      openingsAppropriate: false,
      v33cScope: true,
      note: "v33C — openings/momentum build; do not create property cards in v33B without clear property evidence",
    }));
}

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-woodspring-presentation-cleanup-backfill-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_IMAGE_FIELDS,
    APPLY_FLAG_NO_IMAGE_APPROVAL,
    APPLY_FLAG_NO_SOURCE_LIBRARY,
    APPLY_FLAG_WOODSPRING_ONLY,
  ].join(" ");
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer WoodSpring Presentation Cleanup + Backfill v33B");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- v33B exists: **${report.v33bWriterExists ? "yes" : "no"}**`);
  lines.push(`- Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`- Image fields untouched: **${report.imageFieldsUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Work summary");
  lines.push(`- Copy patches: **${report.rowsPatched.length}**`);
  lines.push(`- Creates: **${report.rowsCreated.length}**`);
  lines.push(`- Quarantined: **${report.rowsQuarantined.length}**`);
  lines.push(`- Internal language before: **${report.internalLanguageCleanup.before.length}**`);
  lines.push(`- Internal language after (projected): **${report.internalLanguageCleanup.afterProjected.length}**`);
  lines.push("");
  lines.push("## Readiness projection");
  lines.push(`- Final QA: ${report.expectedFinalQaResult}`);
  lines.push(`- Complete Build: ${report.expectedCompleteBuildResult}`);
  lines.push(`- Visual defects: ${report.expectedVisualDefectResult}`);
  lines.push(`- Next writer: ${report.recommendedNextWriter}`);
  if (report.exactApplyCommand) {
    lines.push("");
    lines.push("## Apply command");
    lines.push("```bash");
    lines.push(report.exactApplyCommand);
    lines.push("```");
  }
  return lines.join("\n");
}

export async function buildBrandExplorerWoodspringPresentationCleanupBackfillWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  noValidationClaim = false,
  noImageFieldChanges = false,
  noImageApproval = false,
  noSourceLibrary = false,
  woodspringOnly = false,
} = {}) {
  const slug = nz(brandArg).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Protected brand cannot be modified by v33B: ${slug}`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v33B is WoodSpring-only. Requested: ${brandArg}`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandConfig = getDiscoveryBrandConfig(TARGET_BRAND.slug);
  const brandBasicsBefore = await fetchBrandBasics(TARGET_BRAND.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);
  const brandApi = await fetchBrandApiShape(TARGET_BRAND.recordId);
  if (!brandApi) throw new Error("Could not load WoodSpring API shape");

  const presentationRows = await listWoodspringPresentationRows(
    baseId,
    apiKey,
    TARGET_BRAND.recordId,
    TARGET_BRAND.name
  );
  const sourcesPage = await listPartnerSources({ brandId: TARGET_BRAND.recordId, limit: 100 });
  const sources = sourcesPage.sources || [];
  const registryAssets = await listRegistryAssetsForBrand(TARGET_BRAND.recordId).catch(() => []);
  const apiBlocks = brandApi.brandExplorer?.blocks || [];
  const apiBlockById = new Map(apiBlocks.map((b) => [b.recordId, b]));

  const presentationBlockerAudit = auditBlockersFromReports();
  const rowsPatched = [];
  const rowsCreated = [];
  const rowsQuarantined = [];
  const rowsSkipped = [];
  const safetyBlockers = [];

  for (const row of presentationRows) {
    const quarantine = proposeScenarioQuarantine(row, brandConfig);
    if (quarantine) rowsQuarantined.push(quarantine);

    const patch = proposeCopyPatch(row, TARGET_BRAND.recordId, TARGET_BRAND.name, brandConfig);
    if (!patch) continue;
    if (patch.invalid) {
      rowsSkipped.push({
        recordId: patch.recordId,
        slotKey: patch.slotKey,
        reason: patch.validationErrors.join("; "),
      });
      continue;
    }
    rowsPatched.push(patch);
  }

  const slotHasVisible = (slotKey) =>
    presentationRows.some(
      (r) =>
        r.slotKey === slotKey &&
        !HIDDEN_EXTERNAL_DISPLAY_STATUSES.includes(r.externalDisplayStatus) &&
        r.active !== false
    ) ||
    rowsCreated.some((c) => c.slotKey === slotKey);

  const createIfMissing = (slotKey, backfillKey, sort = 0, reason) => {
    if (slotHasVisible(slotKey)) return;
    const backfill = WOODSPRING_BACKFILL[backfillKey || slotKey];
    if (!backfill) return;
    const fields = presentationFields({
      slotKey,
      title: backfill.title || "",
      body: backfill.body,
      sort,
      brandRecordId: TARGET_BRAND.recordId,
      brandName: TARGET_BRAND.name,
    });
    const errors = validatePresentationPatch(fields, { slotKey });
    if (errors.length) {
      safetyBlockers.push(`create_validation:${slotKey}:${errors.join(";")}`);
      return;
    }
    rowsCreated.push({ slotKey, fields, reason, section: inferSection(slotKey) });
  };

  createIfMissing("overview.featured_application", "overview.featured_application", 0, "featured_application_backfill");
  createIfMissing("overview.portfolio_context", "overview.portfolio_context", 0, "portfolio_context_backfill");
  createIfMissing("portfolio_context", "portfolio_context", 0, "portfolio_context_alt_backfill");
  createIfMissing("demand_scenario", "demand_scenario", 0, "demand_scenario_backfill");

  for (let i = 1; i <= 4; i++) {
    createIfMissing(`valueOwners.scenario.${i}`, `valueOwners.scenario.${i}`, i, "value_creation_scenario_backfill");
  }

  const scenario3QuarantinePlanned = rowsQuarantined.some((q) => q.slotKey === "overview.scenario.3");
  if (scenario3QuarantinePlanned) {
    const backfill = WOODSPRING_BACKFILL["overview.scenario.3.replacement"];
    const fields = presentationFields({
      slotKey: "overview.scenario.3",
      title: backfill.title,
      body: backfill.body,
      sort: 3,
      brandRecordId: TARGET_BRAND.recordId,
      brandName: TARGET_BRAND.name,
    });
    const errors = validatePresentationPatch(fields, { slotKey: "overview.scenario.3" });
    if (!errors.length && !rowsCreated.some((c) => c.reason === "scenario_3_replacement_after_quarantine")) {
      rowsCreated.push({
        slotKey: "overview.scenario.3",
        fields,
        reason: "scenario_3_replacement_after_quarantine",
        section: "overview.scenario",
        replacesQuarantinedRecord: rowsQuarantined.find((q) => q.slotKey === "overview.scenario.3")?.recordId,
      });
    } else if (errors.length) {
      safetyBlockers.push(`scenario_3_replacement_create:${errors.join(";")}`);
    }
  }

  const portfolioMixBefore = presentationRows.filter((r) => r.slotKey === MIX_SLOT);
  const portfolioMixResult = { before: portfolioMixBefore.length, after: [] };
  for (const chip of WOODSPRING_PORTFOLIO_MIX_CHIPS) {
    const existing =
      portfolioMixBefore.find((r) => nz(r.title) === chip.title) ||
      portfolioMixBefore.find((r) => Number(r.sortOrder) === chip.sort);

    if (!existing) {
      const fields = presentationFields({
        slotKey: MIX_SLOT,
        title: chip.title,
        body: chip.body,
        sort: chip.sort,
        brandRecordId: TARGET_BRAND.recordId,
        brandName: TARGET_BRAND.name,
      });
      const errors = validatePresentationPatch(fields, { slotKey: MIX_SLOT });
      if (errors.length) {
        safetyBlockers.push(`portfolio_mix_create:${chip.title}`);
        continue;
      }
      rowsCreated.push({
        slotKey: MIX_SLOT,
        fields,
        reason: "portfolio_mix_chip_backfill",
        section: "portfolio_mix",
      });
      portfolioMixResult.after.push({ action: "create", title: chip.title });
    } else if (nz(existing.body) !== chip.body || (!nz(existing.title) && chip.title)) {
      const fields = {};
      if (!nz(existing.title)) fields.Title = chip.title;
      if (nz(existing.body) !== chip.body) fields.Body = chip.body;
      const errors = validatePresentationPatch(fields, { slotKey: MIX_SLOT });
      if (!errors.length) {
        rowsPatched.push({
          action: "copy_patch",
          recordId: existing.recordId,
          slotKey: MIX_SLOT,
          section: "portfolio_mix",
          before: { title: existing.title, body: existing.body },
          after: { title: chip.title, body: chip.body },
          fields,
          fixReason: "portfolio_mix_chip_normalize",
        });
        portfolioMixResult.after.push({ action: "patch", recordId: existing.recordId, title: chip.title });
      }
    } else {
      portfolioMixResult.after.push({ action: "preserve", recordId: existing.recordId, title: existing.title });
    }
  }

  const internalBefore = presentationRows.flatMap((r) =>
    scanInternalLanguage(`${r.title}\n${r.body}`, r.recordId, r.slotKey)
  );
  const internalAfterProjected = [
    ...rowsPatched.flatMap((u) => u.internalLanguageAfter || []),
    ...presentationRows
      .filter((r) => !rowsPatched.some((u) => u.recordId === r.recordId))
      .flatMap((r) => scanInternalLanguage(`${r.title}\n${r.body}`, r.recordId, r.slotKey)),
  ];

  const openingsMomentumCandidates = buildOpeningsMomentumCandidates(sources);

  const applyBlockers = [...safetyBlockers];
  if (apply) {
    if (!approveBatch) applyBlockers.push("missing_approve_flag");
    if (!noValidationClaim) applyBlockers.push("missing_confirm_no_company_validation_claim");
    if (!noImageFieldChanges) applyBlockers.push("missing_confirm_no_image_field_changes");
    if (!noImageApproval) applyBlockers.push("missing_confirm_no_image_approval");
    if (!noSourceLibrary) applyBlockers.push("missing_confirm_no_source_library_changes");
    if (!woodspringOnly) applyBlockers.push("missing_confirm_woodspring_only");
  }

  const scenario3Row = presentationRows.find((r) => r.slotKey === "overview.scenario.3");
  if (
    scenario3Row &&
    isScenarioQuarantineCandidate(scenario3Row, brandConfig) &&
    scenario3Row.externalDisplayStatus !== EXTERNAL_DISPLAY_STATUS_QUARANTINE &&
    !rowsQuarantined.some((q) => q.recordId === scenario3Row.recordId)
  ) {
    applyBlockers.push("wrong_brand_scenario_still_visible");
  }

  for (const item of [...rowsPatched, ...rowsCreated]) {
    const combined = `${item.fields?.Title || ""}\n${item.fields?.Body || ""}`;
    if (FDD_RE.test(combined)) applyBlockers.push(`fdd_would_remain:${item.slotKey}`);
    if (PERFORMANCE_RE.test(combined)) applyBlockers.push(`performance_claim:${item.slotKey}`);
  }

  const hasWork = rowsPatched.length > 0 || rowsCreated.length > 0 || rowsQuarantined.length > 0;
  const dryRunClean = applyBlockers.length === 0 && hasWork;

  let airtableModified = false;
  const applyResults = { patched: [], created: [], quarantined: [], errors: [] };
  let imageFieldsChanged = false;

  const canApply =
    apply &&
    approveBatch &&
    noValidationClaim &&
    noImageFieldChanges &&
    noImageApproval &&
    noSourceLibrary &&
    woodspringOnly &&
    applyBlockers.length === 0;

  if (canApply) {
    for (const update of rowsQuarantined) {
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          { method: "PATCH", body: JSON.stringify({ fields: update.fields, typecast: true }) },
          update.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `PATCH failed: ${res.status}`);
        applyResults.quarantined.push(update.recordId);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ type: "quarantine", recordId: update.recordId, message: err.message });
      }
    }

    for (const update of rowsPatched) {
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          { method: "PATCH", body: JSON.stringify({ fields: update.fields, typecast: true }) },
          update.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `PATCH failed: ${res.status}`);
        applyResults.patched.push(update.recordId);
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ type: "patch", recordId: update.recordId, message: err.message });
      }
    }

    for (const create of rowsCreated) {
      try {
        const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
          method: "POST",
          body: JSON.stringify({ fields: create.fields, typecast: true }),
        });
        if (!res.ok) throw new Error(json.error?.message || `POST failed: ${res.status}`);
        applyResults.created.push({ slotKey: create.slotKey, recordId: json.id });
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ type: "create", slotKey: create.slotKey, message: err.message });
      }
    }
  }

  const brandBasicsAfter = canApply ? await fetchBrandBasics(TARGET_BRAND.recordId) : brandBasicsBefore;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);

  const finalQaReport = await buildBrandExplorerFinalQaAuditorReport({
    brandIdOrName: TARGET_BRAND.slug,
  }).catch(() => null);
  const completeBuildReport = await buildBrandExplorerCompleteBuildOrchestratorReport({
    brandIdOrName: TARGET_BRAND.slug,
    targetQuality: "active-profile",
  }).catch(() => null);
  const visualReport = await buildBrandExplorerVisualDisplayDefectAuditReport({
    brandIdOrName: TARGET_BRAND.recordId,
  }).catch(() => null);

  const wsQa = (finalQaReport?.brandReports || []).find(
    (b) => b.brand?.slug === TARGET_BRAND.slug
  );
  const wsBuild = (completeBuildReport?.brandResults || [])[0];

  const imagePreservation = presentationRows
    .filter((r) => r.hasImage)
    .map((r) => ({
      recordId: r.recordId,
      slotKey: r.slotKey,
      imageUrl: r.imageUrl,
      tempUrl: isTemporaryAirtableUrl(r.imageUrl),
      apiExposed: Boolean(apiBlockById.get(r.recordId)?.imageUrl),
      action: "preserve_no_image_field_writes",
      v33dNote: isTemporaryAirtableUrl(r.imageUrl) ? "v33D durable image governance" : null,
    }));

  const report = {
    writerVersion: WRITER_VERSION,
    stagingRunId: STAGING_RUN_ID,
    v33bWriterExists: v33bWriterExists(),
    generatedAt: new Date().toISOString(),
    mode: apply ? "apply" : "dry-run",
    brand: TARGET_BRAND,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    presentationBlockerAudit,
    internalLanguageCleanup: {
      before: internalBefore,
      afterProjected: internalAfterProjected,
    },
    rowsPatched: rowsPatched.map((r) => ({
      recordId: r.recordId,
      slotKey: r.slotKey,
      section: r.section,
      fixReason: r.fixReason,
      before: r.before,
      after: r.after,
    })),
    rowsCreated: rowsCreated.map((r) => ({
      slotKey: r.slotKey,
      section: r.section,
      reason: r.reason,
      title: r.fields?.Title,
      bodyPreview: nz(r.fields?.Body).slice(0, 160),
    })),
    rowsQuarantined: rowsQuarantined.map((r) => ({
      recordId: r.recordId,
      slotKey: r.slotKey,
      fixReason: r.fixReason,
      imageUntouched: r.imageUntouched,
    })),
    overviewFeaturedApplicationResult: {
      existed: presentationRows.some((r) => r.slotKey === "overview.featured_application"),
      action: rowsCreated.some((c) => c.slotKey === "overview.featured_application")
        ? "create"
        : rowsPatched.some((p) => p.slotKey === "overview.featured_application")
          ? "patch"
          : presentationRows.some((r) => r.slotKey === "overview.featured_application")
            ? "preserve"
            : "missing",
    },
    portfolioContextResult: {
      overviewPortfolioContext: rowsCreated.some((c) => c.slotKey === "overview.portfolio_context")
        ? "create"
        : "present_or_preserved",
      portfolioContextSlot: rowsCreated.some((c) => c.slotKey === "portfolio_context") ? "create" : "check_api",
    },
    portfolioMixResult,
    standardDetailResult: {
      patches: rowsPatched.filter((r) => r.section === "standard_detail").length,
      note: "standards.* rows cleaned via internal-language sanitization",
    },
    demandScenarioResult: {
      action: rowsCreated.some((c) => c.slotKey === "demand_scenario") ? "create" : "present_or_preserved",
    },
    loyaltyCleanupResult: {
      patches: rowsPatched.filter((r) => r.section === "loyalty").length,
    },
    geographicFootprintResult: {
      patches: rowsPatched.filter((r) => r.section === "geographic_footprint").length,
    },
    scenarioQuarantineResult: {
      quarantined: rowsQuarantined,
      replacementCreated: rowsCreated.some((c) => c.reason === "scenario_3_replacement_after_quarantine"),
      scenario12TempImageFlag: imagePreservation.filter(
        (i) => /^overview\.scenario\.[12]$/.test(i.slotKey) && i.tempUrl
      ),
    },
    valueCreationScenarioResult: {
      created: rowsCreated.filter((c) => c.slotKey?.startsWith("valueOwners.scenario")).length,
      target: 4,
    },
    openingsMomentumV33cCandidates: openingsMomentumCandidates,
    imageFieldsUntouched: !imageFieldsChanged,
    imagePreservation,
    registryReadOnly: registryAssets.length,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    companyValidatedSnapshots: { before: companyValidatedBefore, after: companyValidatedAfter },
    airtableModified,
    applyBlockers,
    dryRunClean,
    applyResults,
    expectedFinalQaResult: wsQa
      ? `${wsQa.overallReadiness || "blocked"} (${wsQa.readinessScore ?? "?"})`
      : "projected improvement — re-run after apply",
    expectedCompleteBuildResult: wsBuild
      ? `readyForActiveProfile: ${wsBuild.readyForActiveProfile}`
      : "blocked — openings/momentum remain v33C",
    expectedVisualDefectResult: visualReport
      ? `${visualReport.defectCount ?? visualReport.defects?.length ?? "?"} defects`
      : "unchanged image defects — v33D",
    remainingBlockers: [
      "footprint.openings — v33C",
      "footprint.momentum — v33C",
      "temporary scenario images — v33D",
      "registry approval — v33D",
      "final fact stewardship — v33E",
    ],
    recommendedNextWriter: "v33C — WoodSpring openings / momentum build",
    exactApplyCommand: dryRunClean ? buildApplyCommand() : null,
    exactDryRunCommand: `npm run brand-explorer-woodspring-presentation-cleanup-backfill-writer -- --brand ${TARGET_BRAND.slug} --dry-run`,
    applyGuardrails: {
      woodspringOnly: true,
      noImageFieldChanges: true,
      noImageApproval: true,
      noSourceLibraryChanges: true,
      noCompanyValidationClaims: true,
    },
    markdown: "",
  };

  report.markdown = buildMarkdown(report);
  return report;
}
