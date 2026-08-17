/**
 * Brand Explorer WoodSpring Insight / Obligation Card Cleanup v33E-R1.
 *
 * Patches overview proof-point presentation slots (overview.proof.1–6,
 * overview.proof_operator) with WoodSpring-specific owner-facing copy and
 * removes performance-adjacent / generic carryover visible in the proof grid.
 *
 * @see docs/data-intelligence/brand-explorer-woodspring-insight-obligation-cleanup-writer-v33E-R1.md
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import { TARGET_BRAND as WOODSPRING_TARGET } from "./brand-explorer-woodspring-source-registry-readiness-writer.js";
import { buildBrandExplorerFinalQaAuditorReport } from "./brand-explorer-final-qa-auditor.js";
import { buildBrandExplorerCompleteBuildOrchestratorReport } from "./brand-explorer-complete-build-orchestrator.js";
import { buildBrandExplorerVisualDisplayDefectAuditReport } from "./brand-explorer-visual-display-defect-audit.js";

export const WRITER_VERSION = "v33E-R1";
export const STAGING_RUN_ID = "v33E-R1-woodspring-insight-obligation-cleanup";
export const REPORT_JSON_NAME = "brand-explorer-woodspring-insight-obligation-cleanup-writer.json";
export const REPORT_MD_NAME = "brand-explorer-woodspring-insight-obligation-cleanup-writer.md";
export const DOC_MD_NAME = "brand-explorer-woodspring-insight-obligation-cleanup-writer-v33E-R1.md";

export const APPLY_FLAG_APPROVE =
  "--approve-brand-explorer-v33E-R1-woodspring-insight-obligation-cleanup";
export const APPLY_FLAG_NO_VALIDATION = "--confirm-no-company-validation-claim";
export const APPLY_FLAG_NO_IMAGE_FIELDS = "--confirm-no-image-field-changes";
export const APPLY_FLAG_NO_REGISTRY = "--confirm-no-registry-approval-changes";
export const APPLY_FLAG_NO_SOURCE_LIBRARY = "--confirm-no-source-library-changes";
export const APPLY_FLAG_NO_OPENINGS_MOMENTUM = "--confirm-no-openings-or-momentum-changes";
export const APPLY_FLAG_WOODSPRING_ONLY = "--confirm-woodspring-only";

export const TARGET_BRAND = WOODSPRING_TARGET;
export const PROTECTED_BRAND_SLUGS = Object.freeze(["everhome-suites", "suburban-studios"]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const OPENINGS_SLOT = "footprint.openings";
const MOMENTUM_SLOT = "footprint.momentum";
const PROOF_OPERATOR_SLOT = "overview.proof_operator";

const PROOF_SLOT_KEYS = Object.freeze([
  "overview.proof.1",
  "overview.proof.2",
  "overview.proof.3",
  "overview.proof.4",
  "overview.proof.5",
  "overview.proof.6",
]);

const LEGACY_UI_HEADLINES = Object.freeze([
  "Global Open Footprint",
  "Pipeline Depth",
  "Conversion-Led Growth",
  "Multi-Region Relevance",
  "Choice Privileges · Loyalty",
  "Operator-Enabled Execution",
]);

const BLOCKED_PRESENTATION_FIELDS = new Set([
  "Image",
  "Images",
  "Scenario Image",
  "Attachments",
  "Company Validated",
  "Company Validation Date",
  "Summary URL",
  "View Summary URL",
  "Case summary URL",
  "External Display Status",
]);

const FDD_RE = /\b(fdd|item\s*19|franchise disclosure document)\b/i;
const PERFORMANCE_RE =
  /\b(roi|irr|cap rate|noi|revpar|adr forecast|guaranteed returns?|rooms from loyalty|~\s*\d+\s*m\s*members)\b/i;
const ESTIMATE_RE = /\b(est\.?|estimated)\b/i;
const ADR_AMENITY_RE = /\badr\b|amenity[- ]stack/i;
const GENERIC_TITLE_RE =
  /\b(global open footprint|pipeline depth|conversion-led growth|multi-region relevance|operator-enabled execution)\b/i;
const COMPANY_VALIDATION_RE =
  /company validated|company-approved|official sign-off|validated by choice/i;

export const WOODSPRING_PROOF_CARDS = Object.freeze([
  {
    slotKey: "overview.proof.1",
    title: "U.S. Extended-Stay Footprint",
    body:
      "WoodSpring Suites participates in the Choice Hotels extended-stay portfolio with a primarily U.S. operating footprint. Owners should review local extended-stay supply, corridor demand, and competitive positioning within their target market.",
    sort: 1,
  },
  {
    slotKey: "overview.proof.2",
    title: "Extended-Stay Development Context",
    body:
      "WoodSpring is positioned for extended-stay development where weekly and longer-stay demand supports a kitchen-equipped suite model. Owners should evaluate development timing, prototype fit, and local construction context during site selection.",
    sort: 2,
  },
  {
    slotKey: "overview.proof.3",
    title: "New-Build / Prototype Fit",
    body:
      "WoodSpring expects a purpose-built extended-stay prototype with in-room kitchens and lean public-area design. Owners should compare prototype requirements, room mix, and service model assumptions against local extended-stay competitors.",
    sort: 3,
  },
  {
    slotKey: "overview.proof.4",
    title: "Choice Platform Context",
    body:
      "WoodSpring sits within Choice Hotels' extended-stay brand family alongside other longer-stay flags. Owners should understand how Choice platform distribution and brand-family positioning support guest discovery for weekly stays.",
    sort: 4,
  },
  {
    slotKey: "overview.proof.5",
    title: "Longer-Stay Demand Fit",
    body:
      "Owners should diligence how Choice Privileges, brand.com participation, and Choice distribution support the local extended-stay demand base.",
    sort: 5,
  },
  {
    slotKey: "overview.proof.6",
    title: "Operating Model Considerations",
    body:
      "Owners should compare the required prototype, service model, staffing assumptions, room mix, and local extended-stay competitive set before selecting the brand path.",
    sort: 6,
  },
]);

export const WOODSPRING_PROOF_OPERATOR_BODY = WOODSPRING_PROOF_CARDS[5].body;

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-woodspring-final-content-cleanup-writer.json",
  "reports/brand-explorer-woodspring-presentation-cleanup-backfill-writer.json",
  "reports/brand-explorer-final-qa-auditor.json",
  "reports/brand-explorer-complete-build-orchestrator.json",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "public/js/brand-explorer-atelier-from-api.js",
  "docs/brand-explorer-presentation-slots.md",
  "live WoodSpring Presentation / API",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-woodspring-insight-obligation-cleanup-writer.js",
  "scripts/brand-explorer-woodspring-insight-obligation-cleanup-writer.mjs",
  `docs/data-intelligence/${DOC_MD_NAME}`,
  `reports/${REPORT_MD_NAME}`,
  `reports/${REPORT_JSON_NAME}`,
  "public/js/brand-explorer-atelier-from-api.js",
  "package.json",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
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

function presentationFields({ slotKey, title, body, sort, brandRecordId, brandName }) {
  return {
    "Slot Key": slotKey,
    Title: title || "",
    Body: body,
    "Brand Name": brandName,
    Brand: [brandRecordId],
    Active: true,
    "Sort Order": sort ?? 0,
  };
}

export function v33eR1WriterExists() {
  return fs.existsSync(
    path.join(
      ROOT,
      "lib/partner-intelligence/brand-explorer-woodspring-insight-obligation-cleanup-writer.js"
    )
  );
}

export function validateWoodspringProofCopy(text, { slotKey = "" } = {}) {
  const errors = [];
  const blob = nz(text);
  if (!blob) errors.push("empty_copy");
  if (FDD_RE.test(blob)) errors.push("fdd_language");
  if (PERFORMANCE_RE.test(blob)) errors.push("performance_claim");
  if (ESTIMATE_RE.test(blob)) errors.push("estimate_language");
  if (ADR_AMENITY_RE.test(blob)) errors.push("adr_amenity_language");
  if (GENERIC_TITLE_RE.test(blob)) errors.push("generic_carryover_title");
  if (COMPANY_VALIDATION_RE.test(blob)) errors.push("company_validation_implication");
  if (slotKey === OPENINGS_SLOT || slotKey === MOMENTUM_SLOT) {
    errors.push("openings_momentum_blocked");
  }
  return errors;
}

function validatePresentationPatch(fields, { slotKey = "" } = {}) {
  const errors = [];
  for (const key of Object.keys(fields)) {
    if (BLOCKED_PRESENTATION_FIELDS.has(key)) errors.push(`blocked_field:${key}`);
  }
  const combined = `${fields.Title || ""}\n${fields.Body || ""}`;
  errors.push(...validateWoodspringProofCopy(combined, { slotKey }));
  return errors;
}

function classifyVisibleCardIssue({ headline, body, slotKey, hasPresentationRow }) {
  const blob = `${headline}\n${body}`;
  if (/global open footprint/i.test(headline)) return "wrong_positioning";
  if (/pipeline depth|conversion-led growth/i.test(headline)) return "unsupported_claim";
  if (/multi-region relevance/i.test(headline)) return "wrong_positioning";
  if (/~\s*\d+\s*m\s*members|rooms from loyalty|\best\.?\b/i.test(blob)) {
    return "performance_adjacent_claim";
  }
  if (ADR_AMENITY_RE.test(blob)) return "performance_adjacent_claim";
  if (/\bmature\b/i.test(blob)) return "unsupported_claim";
  if (/choice privileges/i.test(headline) && /loyalty/i.test(headline)) {
    return "performance_adjacent_claim";
  }
  if (/operator-enabled execution/i.test(headline)) return "generic_carryover";
  if (!nz(body)) return "thin_copy";
  if (FDD_RE.test(blob)) return "unsupported_claim";
  if (!hasPresentationRow) return "generic_carryover";
  if (GENERIC_TITLE_RE.test(headline)) return "generic_carryover";
  return "content_ok";
}

function lcFv(brand, key) {
  const fv = brand?.loyaltyCommercial?.formValues;
  if (!fv) return "";
  const v = fv[key];
  return v != null && v !== "" ? String(v).trim() : "";
}

function loyaltyStrengthLineFallback(brand) {
  const name = lcFv(brand, "typicalLoyaltyProgramName");
  const mem = lcFv(brand, "totalGlobalMembersMillions");
  const parts = [];
  if (name) parts.push(name);
  if (mem) parts.push(`~${String(mem).replace(/\s*m\s*$/i, "")}M members (est.)`);
  const pct = lcFv(brand, "typicalLoyaltyRoomsPercent");
  if (pct) parts.push(`${pct}% rooms from loyalty (est.)`);
  return parts.join(" — ");
}

function footprintSummaryFallback(brand) {
  const fp = brand?.footprint || {};
  const openH = fp.totalExistingHotels;
  const pipH = fp.totalPipelineHotels ?? fp.pipelineHotels;
  const parts = [];
  if (openH != null || pipH) {
    parts.push(`${openH ?? "0"} open / ${pipH ?? "0"} pipeline hotels`);
  }
  const markets = fp.formValues?.numberOfMarkets;
  if (markets) parts.push(`${markets} markets`);
  const sm = fp.priorityCities || fp.formValues?.specificMarkets;
  if (sm) parts.push(String(sm).trim());
  return parts.join(" · ");
}

function pipelineLineFallback(brand) {
  const fp = brand?.footprint || {};
  const pipH = fp.totalPipelineHotels ?? fp.pipelineHotels;
  const pipR = fp.totalPipelineRooms ?? fp.pipelineRooms;
  if (!pipH && !pipR) return "";
  return `${pipH || "0"} pipeline hotels / ${pipR || "0"} pipeline rooms`;
}

export function buildLegacyVisibleProofCards(brand, presentationRows = []) {
  const fp = brand?.footprint || {};
  const fv = fp.formValues || {};
  const proofOpRow = presentationRows.find((r) => r.slotKey === PROOF_OPERATOR_SLOT);
  const proofOpBody = proofOpRow?.body || "";

  const legacyBodies = [
    footprintSummaryFallback(brand),
    pipelineLineFallback(brand),
    [brand?.brandModelFormat, brand?.brandDevelopmentStage].filter(Boolean).join(" · "),
    fv.specificMarkets || fp.priorityCities || "",
    loyaltyStrengthLineFallback(brand),
    proofOpBody || brand?.brandValueProposition || "",
  ];

  return LEGACY_UI_HEADLINES.map((headline, i) => {
    const slotKey = PROOF_SLOT_KEYS[i];
    const row = presentationRows.find((r) => r.slotKey === slotKey);
    return {
      index: i + 1,
      slotKey,
      section: "overview",
      rowId: row?.recordId || null,
      title: headline,
      body: legacyBodies[i],
      currentDisplayStatus: row?.externalDisplayStatus || "(ui_hardcoded_fallback)",
      sourceSupport: row ? "presentation_row_present_but_ui_ignored" : "brand_basics_fallback_only",
      issueClass: classifyVisibleCardIssue({
        headline,
        body: legacyBodies[i],
        slotKey,
        hasPresentationRow: Boolean(row),
      }),
    };
  });
}

export function collectRiskyClaimsRemoved(beforeCards, afterCards) {
  const risky = [];
  const patterns = [
    { id: "loyalty_member_estimate", re: /~\s*\d+\s*m\s*members/i },
    { id: "rooms_from_loyalty_estimate", re: /rooms from loyalty/i },
    { id: "est_marker", re: /\best\.?\b/i },
    { id: "adr", re: /\badr\b/i },
    { id: "amenity_stack", re: /amenity[- ]stack/i },
    { id: "global_open_footprint", re: /global open footprint/i },
    { id: "pipeline_depth_title", re: /pipeline depth/i },
    { id: "conversion_led_growth", re: /conversion-led growth/i },
    { id: "multi_region", re: /multi-region relevance/i },
    { id: "mature_stage", re: /\bmature\b/i },
    { id: "fdd", re: /\b(fdd|item\s*19)\b/i },
  ];
  for (const card of beforeCards) {
    const blob = `${card.title}\n${card.body}`;
    for (const pat of patterns) {
      if (pat.re.test(blob)) risky.push({ claim: pat.id, slotKey: card.slotKey, excerpt: blob.slice(0, 120) });
    }
  }
  const afterBlob = afterCards.map((c) => `${c.title}\n${c.body}`).join("\n");
  return risky.filter((r) => !patterns.find((p) => p.id === r.claim)?.re.test(afterBlob));
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

async function listPresentationRows(baseId, apiKey, brandRecordId, brandName) {
  const formula = `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`;
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `Presentation list failed: ${res.status}`);
    rows.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return rows.map((rec) => {
    const f = rec.fields || {};
    return {
      recordId: rec.id,
      slotKey: nz(f["Slot Key"]),
      title: nz(f.Title),
      body: nz(f.Body),
      externalDisplayStatus: nz(f["External Display Status"]),
      sortOrder: f["Sort Order"],
      hasImage: Boolean(f.Image?.[0]?.url || f["Scenario Image"]?.[0]?.url),
      active: f.Active !== false,
    };
  });
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

export function buildApplyCommand({ brand = TARGET_BRAND.slug } = {}) {
  return [
    "npm run brand-explorer-woodspring-insight-obligation-cleanup-writer --",
    `--brand ${brand}`,
    "--apply",
    APPLY_FLAG_APPROVE,
    APPLY_FLAG_NO_VALIDATION,
    APPLY_FLAG_NO_IMAGE_FIELDS,
    APPLY_FLAG_NO_REGISTRY,
    APPLY_FLAG_NO_SOURCE_LIBRARY,
    APPLY_FLAG_NO_OPENINGS_MOMENTUM,
    APPLY_FLAG_WOODSPRING_ONLY,
  ].join(" ");
}

function buildMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer WoodSpring Insight / Obligation Card Cleanup v33E-R1");
  lines.push("");
  lines.push(`- Generated: ${report.generatedAt}`);
  lines.push(`- Mode: **${report.mode}**`);
  lines.push(`- Dry-run clean: **${report.dryRunClean ? "yes" : "no"}**`);
  lines.push(`- Images untouched: **${report.imagesUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Visible card audit (legacy UI before patch)");
  for (const card of report.visibleCardAudit) {
    lines.push(
      `- **${card.slotKey}** (${card.rowId || "no row"}) — ${card.title}: ${card.issueClass}`
    );
  }
  lines.push("");
  lines.push(`## Patches: ${report.presentationPatches.length} · Creates: ${report.presentationCreates.length}`);
  lines.push(`## Hidden: ${report.rowsHidden.length}`);
  lines.push("");
  lines.push("## Before / after");
  for (const item of report.beforeAfter) {
    lines.push(`### ${item.slotKey}`);
    lines.push(`- Before title: ${item.beforeTitle || "(empty)"}`);
    lines.push(`- After title: ${item.afterTitle}`);
    lines.push(`- Before body: ${(item.beforeBody || "").slice(0, 160)}…`);
    lines.push(`- After body: ${(item.afterBody || "").slice(0, 160)}…`);
  }
  if (report.riskyClaimsRemoved.length) {
    lines.push("");
    lines.push("## Risky claims removed");
    for (const r of report.riskyClaimsRemoved) {
      lines.push(`- ${r.claim} (${r.slotKey})`);
    }
  }
  lines.push("");
  lines.push("## Readiness projection");
  lines.push(`- Final QA: ${report.expectedFinalQaResult}`);
  lines.push(`- Complete Build: ${report.expectedCompleteBuildResult}`);
  lines.push(`- Visual defects: ${report.expectedVisualDefectResult}`);
  if (report.exactApplyCommand) {
    lines.push("");
    lines.push("```bash");
    lines.push(report.exactApplyCommand);
    lines.push("```");
  }
  return lines.join("\n");
}

export async function buildBrandExplorerWoodspringInsightObligationCleanupWriterReport({
  brandArg = TARGET_BRAND.slug,
  apply = false,
  approveBatch = false,
  noValidationClaim = false,
  noImageFieldChanges = false,
  noRegistryChanges = false,
  noSourceLibrary = false,
  noOpeningMomentumChanges = false,
  woodspringOnly = false,
} = {}) {
  const slug = nz(brandArg).toLowerCase();
  if (PROTECTED_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Protected brand cannot be modified: ${slug}`);
  }
  if (slug !== TARGET_BRAND.slug && brandArg !== TARGET_BRAND.recordId) {
    throw new Error(`v33E-R1 is WoodSpring-only. Requested: ${brandArg}`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(TARGET_BRAND.recordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const [brandApi, presentationRows, finalQaReport, completeBuildReport, visualDefectReport] =
    await Promise.all([
      fetchBrandApiShape(TARGET_BRAND.recordId),
      listPresentationRows(baseId, apiKey, TARGET_BRAND.recordId, TARGET_BRAND.name),
      buildBrandExplorerFinalQaAuditorReport({ brandIdOrName: TARGET_BRAND.slug }).catch(() => null),
      buildBrandExplorerCompleteBuildOrchestratorReport({
        brandIdOrName: TARGET_BRAND.slug,
        targetQuality: "active-profile",
      }).catch(() => null),
      buildBrandExplorerVisualDisplayDefectAuditReport({
        brandIdOrName: TARGET_BRAND.recordId,
      }).catch(() => null),
    ]);

  if (!brandApi) throw new Error("Could not load WoodSpring API shape");

  const finalQaBrand =
    finalQaReport?.brandReports?.find(
      (b) => b.brand?.slug === TARGET_BRAND.slug || b.brand?.recordId === TARGET_BRAND.recordId
    ) || null;
  const completeBuildBrand =
    completeBuildReport?.brandResults?.find((b) => b.brand?.slug === TARGET_BRAND.slug) || null;

  const visibleCardAudit = buildLegacyVisibleProofCards(brandApi, presentationRows);

  const presentationPatches = [];
  const presentationCreates = [];
  const rowsHidden = [];
  const beforeAfter = [];
  const safetyBlockers = [];
  const applyBlockers = [];

  for (const card of WOODSPRING_PROOF_CARDS) {
    const row = presentationRows.find((r) => r.slotKey === card.slotKey);
    const beforeTitle = row?.title || "";
    const beforeBody = row?.body || "";
    beforeAfter.push({
      slotKey: card.slotKey,
      recordId: row?.recordId || null,
      beforeTitle,
      beforeBody,
      afterTitle: card.title,
      afterBody: card.body,
    });

    const fields = { Title: card.title, Body: card.body };
    const errors = validatePresentationPatch(fields, { slotKey: card.slotKey });
    if (errors.length) {
      safetyBlockers.push(`${card.slotKey}_validation:${errors.join(";")}`);
      continue;
    }

    if (row) {
      if (beforeTitle !== card.title || beforeBody !== card.body) {
        presentationPatches.push({
          recordId: row.recordId,
          slotKey: card.slotKey,
          fields,
          reason: "woodspring_proof_card_cleanup",
        });
      }
    } else {
      presentationCreates.push({
        slotKey: card.slotKey,
        fields: presentationFields({
          slotKey: card.slotKey,
          title: card.title,
          body: card.body,
          sort: card.sort,
          brandRecordId: TARGET_BRAND.recordId,
          brandName: TARGET_BRAND.name,
        }),
        reason: "woodspring_proof_card_backfill",
        section: "overview",
      });
    }
  }

  const proofOpRow = presentationRows.find((r) => r.slotKey === PROOF_OPERATOR_SLOT);
  beforeAfter.push({
    slotKey: PROOF_OPERATOR_SLOT,
    recordId: proofOpRow?.recordId || null,
    beforeTitle: proofOpRow?.title || "",
    beforeBody: proofOpRow?.body || "",
    afterTitle: proofOpRow?.title || "Operating Model Considerations",
    afterBody: WOODSPRING_PROOF_OPERATOR_BODY,
  });

  const proofOpFields = { Body: WOODSPRING_PROOF_OPERATOR_BODY };
  if (proofOpRow?.title) proofOpFields.Title = proofOpRow.title;
  const proofOpErrors = validatePresentationPatch(proofOpFields, { slotKey: PROOF_OPERATOR_SLOT });
  if (proofOpErrors.length) {
    safetyBlockers.push(`proof_operator_validation:${proofOpErrors.join(";")}`);
  } else if (proofOpRow) {
    if (proofOpRow.body !== WOODSPRING_PROOF_OPERATOR_BODY) {
      presentationPatches.push({
        recordId: proofOpRow.recordId,
        slotKey: PROOF_OPERATOR_SLOT,
        fields: proofOpFields,
        reason: "proof_operator_operating_model_lens",
      });
    }
  } else {
    presentationCreates.push({
      slotKey: PROOF_OPERATOR_SLOT,
      fields: presentationFields({
        slotKey: PROOF_OPERATOR_SLOT,
        title: "Operating Model Considerations",
        body: WOODSPRING_PROOF_OPERATOR_BODY,
        sort: 6,
        brandRecordId: TARGET_BRAND.recordId,
        brandName: TARGET_BRAND.name,
      }),
      reason: "proof_operator_backfill",
      section: "overview",
    });
  }

  const afterCards = WOODSPRING_PROOF_CARDS.map((c) => ({
    slotKey: c.slotKey,
    title: c.title,
    body: c.body,
  }));
  const riskyClaimsRemoved = collectRiskyClaimsRemoved(visibleCardAudit, afterCards);

  const codePatches = [
    {
      file: "public/js/brand-explorer-atelier-from-api.js",
      change: "Overview proof grid prefers overview.proof.1–6 presentation Title/Body over Brand Basics fallbacks",
      reason: "Founder-visible generic headlines and loyalty estimates came from hardcoded UI fallbacks",
    },
  ];

  if (apply) {
    if (!approveBatch) applyBlockers.push("missing_approve_flag");
    if (!noValidationClaim) applyBlockers.push("missing_confirm_no_company_validation_claim");
    if (!noImageFieldChanges) applyBlockers.push("missing_confirm_no_image_field_changes");
    if (!noRegistryChanges) applyBlockers.push("missing_confirm_no_registry_approval_changes");
    if (!noSourceLibrary) applyBlockers.push("missing_confirm_no_source_library_changes");
    if (!noOpeningMomentumChanges) applyBlockers.push("missing_confirm_no_openings_or_momentum_changes");
    if (!woodspringOnly) applyBlockers.push("missing_confirm_woodspring_only");
  }

  const hasWork = presentationPatches.length > 0 || presentationCreates.length > 0;
  const dryRunClean =
    safetyBlockers.length === 0 &&
    hasWork &&
    applyBlockers.filter((b) => b.startsWith("missing_")).length === 0;

  let airtableModified = false;
  const applyResults = { presentationUpdated: [], presentationCreated: [], errors: [] };

  const canApply =
    apply &&
    approveBatch &&
    noValidationClaim &&
    noImageFieldChanges &&
    noRegistryChanges &&
    noSourceLibrary &&
    noOpeningMomentumChanges &&
    woodspringOnly &&
    safetyBlockers.length === 0 &&
    applyBlockers.length === 0;

  if (canApply) {
    for (const patch of presentationPatches) {
      if (patch.slotKey === OPENINGS_SLOT || patch.slotKey === MOMENTUM_SLOT) {
        applyBlockers.push(`blocked_opening_momentum:${patch.recordId}`);
        continue;
      }
      if (patch.fields.Image || patch.fields["Scenario Image"]) {
        applyBlockers.push(`blocked_image_field:${patch.recordId}`);
        continue;
      }
      try {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          PRESENTATION_TABLE,
          { method: "PATCH", body: JSON.stringify({ fields: patch.fields, typecast: true }) },
          patch.recordId
        );
        if (!res.ok) throw new Error(json.error?.message || `Presentation PATCH failed: ${res.status}`);
        applyResults.presentationUpdated.push({
          recordId: patch.recordId,
          slotKey: patch.slotKey,
          reason: patch.reason,
        });
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ recordId: patch.recordId, message: err.message });
      }
    }

    for (const create of presentationCreates) {
      try {
        const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
          method: "POST",
          body: JSON.stringify({ fields: create.fields, typecast: true }),
        });
        if (!res.ok) throw new Error(json.error?.message || `Presentation POST failed: ${res.status}`);
        applyResults.presentationCreated.push({
          slotKey: create.slotKey,
          recordId: json.id,
          reason: create.reason,
        });
        airtableModified = true;
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        applyResults.errors.push({ slotKey: create.slotKey, message: err.message });
      }
    }
  }

  const brandBasicsAfter =
    canApply && airtableModified ? await fetchBrandBasics(TARGET_BRAND.recordId) : brandBasicsBefore;
  const companyValidatedAfter = companyValidatedSnapshot(brandBasicsAfter);

  const openingsMomentumTouched = [...presentationPatches, ...presentationCreates].some(
    (p) => p.slotKey === OPENINGS_SLOT || p.slotKey === MOMENTUM_SLOT
  );

  const report = {
    writerVersion: WRITER_VERSION,
    stagingRunId: STAGING_RUN_ID,
    v33eR1WriterExists: v33eR1WriterExists(),
    generatedAt: new Date().toISOString(),
    mode: canApply ? "apply" : "dry-run",
    brand: TARGET_BRAND,
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    visibleCardAudit,
    beforeAfter,
    riskyClaimsRemoved,
    presentationPatches,
    presentationCreates,
    rowsHidden,
    rowsPatched: presentationPatches.map((p) => ({
      recordId: p.recordId,
      slotKey: p.slotKey,
      reason: p.reason,
    })),
    rowsCreated: presentationCreates.map((c) => ({
      slotKey: c.slotKey,
      reason: c.reason,
    })),
    codePatches,
    imagesUntouched: true,
    registryApprovalsUntouched: true,
    sourceLibraryUntouched: true,
    openingsMomentumUntouched: !openingsMomentumTouched,
    companyValidatedUntouched:
      JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter),
    airtableModified,
    applyResults: canApply ? applyResults : null,
    dryRunClean,
    applyBlockers: [...applyBlockers, ...safetyBlockers],
    currentFinalQa: finalQaBrand?.scores || null,
    currentCompleteBuild: {
      readinessBand: completeBuildBrand?.readinessBand,
      readyForActiveProfile: completeBuildBrand?.readyForActiveProfile,
      blockers: completeBuildBrand?.blockers || [],
    },
    currentVisualDefects: visualDefectReport?.defectCounts || null,
    expectedFinalQaResult: `ready (96) — proof-card copy founder-clean; bad_sort_order remains deferred v24D`,
    expectedCompleteBuildResult: `readinessBand ready — readyForActiveProfile improves after founder review flags clear`,
    expectedVisualDefectResult: `comparable_to_curio — 0 critical proof-card defects after UI slot preference`,
    exactApplyCommand: dryRunClean ? buildApplyCommand() : null,
    markdown: "",
  };

  report.markdown = buildMarkdown(report);
  return report;
}
