/**
 * Brand Explorer Portfolio Mix + Portfolio Context Normalization Writer v25C-4C.
 *
 * Normalizes footprint.portfolio_mix to Radisson-style chips and repairs Tribute
 * overview.portfolio_context rendering support. Dry-run by default.
 *
 * @see docs/data-intelligence/brand-explorer-portfolio-mix-context-normalization-writer-v25C-4C.md
 */
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";
import {
  PORTFOLIO_CONTEXT_SLOT,
  PORTFOLIO_CONTEXT_TIER,
  RELATIVE_POSITIONING_BODY,
} from "./brand-explorer-tribute-visible-content-repair-writer.js";

export const WRITER_VERSION = "25C-4C";
export const REPORT_JSON_NAME = "brand-explorer-portfolio-mix-context-normalization-writer.json";
export const REPORT_MD_NAME = "brand-explorer-portfolio-mix-context-normalization-writer.md";
export const DOC_MD_NAME = "brand-explorer-portfolio-mix-context-normalization-writer-v25C-4C.md";

export const APPLY_FLAG_TRIBUTE = "--approve-brand-explorer-v25C-4C-portfolio-mix-context";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-portfolio-mix-context-copy";
export const APPLY_FLAG_NO_STATS = "--confirm-no-unsupported-portfolio-statistics";
export const APPLY_FLAG_ALL_ACTIVE = "--approve-brand-explorer-v25C-4C-all-active-portfolio-mix-normalization";
export const APPLY_FLAG_ALL_ACTIVE_FOUNDER = "--founder-reviewed-all-active-portfolio-mix-copy";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";
const MIX_SLOT_ARCHIVED = "footprint.portfolio_mix.archived";
const RELATIVE_POSITIONING_SLOT = "overview.relative_positioning";
const MIX_SLOT = "footprint.portfolio_mix";
const MIN_TRIBUTE_MIX_CHIPS = 3;
const RADISSON_SIGNAL_RE = /^(high|moderate|low|selective)$/i;

export const ACTIVE_BRAND_AUDIT_TARGETS = [
  { slug: "tribute-portfolio", recordId: TRIBUTE_RECORD_ID, name: BRAND_NAME },
  { slug: "curio-collection", recordId: "receQkxgjlezsc1xg", name: "Curio Collection by Hilton" },
  { slug: "kimpton", recordId: "recCKuXCmGvxHPfb3", name: "Kimpton Hotels" },
  { slug: "radisson-blu", recordId: "recWPEvxBQxVVzSq3", name: "Radisson Blu by Choice" },
  { slug: "radisson", recordId: "recywbx1YQSTCPqW1", name: "Radisson by Choice" },
  { slug: "ascend", recordId: "reclkgOzvAcBheUSo", name: "Ascend Hotel Collection" },
];

const UNSUPPORTED_PERCENTAGE_RE = /(?:~|≈|about|roughly|approximately)?\s*\d+\s*%|illustrative mix/i;

const CURIO_HILTON_LEAK_RE =
  /curio collection by hilton|exactly like nothing else|hilton honors|tapestry collection/i;

const GENERIC_LADDER_RE = /lower-scale brands|mid-scale brands|upper-scale brands/i;

const PROTECTED_SLOT_KEYS = new Set([
  "footprint.momentum",
  "footprint.openings",
  "loyalty.earn",
  "loyalty.redeem",
  "loyalty.elite",
  "loyalty.proof",
  "valueOwners.scenario.1",
  "valueOwners.scenario.2",
  "valueOwners.scenario.3",
  "valueOwners.scenario.4",
  "standards.requirement",
  "standards.intro",
]);

const PROTECTED_SLOT_PREFIXES = [
  /^loyalty\./i,
  /^footprint\.momentum/i,
  /^footprint\.openings/i,
  /^valueOwners\.scenario\./i,
  /^standards\.requirement/i,
];

export const TRIBUTE_PORTFOLIO_MIX_CHIPS = [
  { title: "Urban Lifestyle", body: "High", sort: 1 },
  { title: "Resort / Leisure-Adjacent", body: "Moderate", sort: 2 },
  { title: "Conversion / Repositioning", body: "High", sort: 3 },
  { title: "Secondary Market", body: "Selective", sort: 4 },
  { title: "New Build Prototype-Led", body: "Low", sort: 5 },
];

/** Radisson-style chips — founder-reviewed, no unsupported percentages. */
export const CURIO_PORTFOLIO_MIX_CHIPS = [
  { title: "Urban", body: "High", sort: 1 },
  { title: "Leisure / Resort-Adjacent", body: "High", sort: 2 },
  { title: "Secondary Market", body: "Moderate", sort: 3 },
  { title: "New Build Prototype-Led", body: "Low", sort: 4 },
  { title: "Conversion / Repositioning", body: "High", sort: 5 },
];

export const KIMPTON_PORTFOLIO_MIX_CHIPS = [
  { title: "Urban", body: "High", sort: 1 },
  { title: "Leisure / Resort-Adjacent", body: "Moderate", sort: 2 },
  { title: "Secondary Market", body: "Selective", sort: 3 },
  { title: "New Build Prototype-Led", body: "Low", sort: 4 },
  { title: "Conversion / Repositioning", body: "High", sort: 5 },
];

export const ASCEND_PORTFOLIO_MIX_CHIPS = [
  { title: "Urban", body: "Moderate", sort: 1 },
  { title: "Leisure / Resort-Adjacent", body: "Moderate", sort: 2 },
  { title: "Secondary Market", body: "High", sort: 3 },
  { title: "New Build Prototype-Led", body: "Low", sort: 4 },
  { title: "Conversion / Repositioning", body: "High", sort: 5 },
];

export const BRAND_MIX_PACKAGES = {
  [TRIBUTE_RECORD_ID]: TRIBUTE_PORTFOLIO_MIX_CHIPS,
  receQkxgjlezsc1xg: CURIO_PORTFOLIO_MIX_CHIPS,
  recCKuXCmGvxHPfb3: KIMPTON_PORTFOLIO_MIX_CHIPS,
  reclkgOzvAcBheUSo: ASCEND_PORTFOLIO_MIX_CHIPS,
};

export const TRIBUTE_PORTFOLIO_CONTEXT_NARRATIVE =
  "Tribute Portfolio sits within Marriott's independent/lifestyle collection conversation. For owners, it can be relevant when an asset should retain local character and a less standardized identity while still accessing Marriott distribution, Bonvoy loyalty, and brand affiliation.\n\n• Best suited to character-led independent or lifestyle assets.\n• Often relevant in conversion, repositioning, resort/leisure, and urban boutique discussions.\n• Should be compared against other Marriott soft/lifestyle collection paths based on asset quality, market, owner control, operating model, and brand standards.";

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-tribute-visible-content-repair-writer.md",
  "reports/brand-explorer-tribute-visible-content-repair-writer.json",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-required-section-population-contract.json",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "reports/brand-explorer-required-section-source-capture-package.md",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "live Brand Explorer Presentation rows for active brands",
  "live Brand Basics for active brands",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-portfolio-mix-context-normalization-writer.js",
  "scripts/brand-explorer-portfolio-mix-context-normalization-writer.mjs",
  "docs/data-intelligence/brand-explorer-portfolio-mix-context-normalization-writer-v25C-4C.md",
  "reports/brand-explorer-portfolio-mix-context-normalization-writer.md",
  "reports/brand-explorer-portfolio-mix-context-normalization-writer.json",
  "public/js/brand-explorer-atelier-from-api.js",
  "package.json",
];

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeBody(v) {
  return nz(v).replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();
}

function escapeFormulaValue(v) {
  return String(v).replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

function isProtectedSlot(slotKey) {
  const key = nz(slotKey);
  if (PROTECTED_SLOT_KEYS.has(key)) return true;
  return PROTECTED_SLOT_PREFIXES.some((rx) => rx.test(key));
}

function isRadissonStyleChipRow(row) {
  const title = nz(row?.title);
  const body = nz(row?.body);
  return Boolean(title) && RADISSON_SIGNAL_RE.test(body);
}

export function classifyPortfolioMixRows(rows) {
  const mixRows = (rows || []).filter((r) => r.slotKey === MIX_SLOT);
  if (!mixRows.length) return "missing";

  const radissonComplete =
    mixRows.length >= 3 && mixRows.every(isRadissonStyleChipRow);
  if (radissonComplete) return "radisson_style_complete";

  if (mixRows.length === 1) {
    const only = mixRows[0];
    const combined = `${only.title} ${only.body}`;
    if (UNSUPPORTED_PERCENTAGE_RE.test(combined) || nz(only.body).length > 48) {
      return "sentence_style_needs_conversion";
    }
    return "single_chip_needs_expansion";
  }

  if (mixRows.some((r) => UNSUPPORTED_PERCENTAGE_RE.test(`${r.title} ${r.body}`))) {
    return "sentence_style_needs_conversion";
  }

  if (mixRows.some((r) => !nz(r.title) || nz(r.body).length > 48)) {
    return "sentence_style_needs_conversion";
  }

  if (mixRows.length >= 2 && mixRows.filter(isRadissonStyleChipRow).length >= 2) {
    return "source/founder_review_required";
  }

  return "source/founder_review_required";
}

function diagnosePortfolioContext(rows, brandName) {
  const ctxRow = (rows || []).find((r) => r.slotKey === PORTFOLIO_CONTEXT_SLOT);
  const relRow = (rows || []).find((r) => r.slotKey === RELATIVE_POSITIONING_SLOT);
  const tierTitle = nz(ctxRow?.title);
  const body = nz(ctxRow?.body);

  let rootCause = null;
  if (!ctxRow) {
    rootCause = "row_missing";
  } else if (!body) {
    rootCause = "body_field_missing";
  } else if (ctxRow.active === false) {
    rootCause = "row_inactive";
  } else {
    rootCause = "frontend_mapping_issue";
  }

  return {
    rowExists: Boolean(ctxRow),
    recordId: ctxRow?.recordId || null,
    title: tierTitle,
    bodyPreview: body.slice(0, 160),
    active: ctxRow?.active ?? null,
    legacyRelativePositioningRow: relRow
      ? { recordId: relRow.recordId, bodyPreview: nz(relRow.body).slice(0, 120) }
      : null,
    rootCause,
    rootCauseDetail:
      rootCause === "frontend_mapping_issue"
        ? "overview.portfolio_context row exists (v25C-4A) with tier Title and Body, but Portfolio Context on Overview tab only renders buildPortfolioLadderCellsHtml() — Body is not displayed in that section. Marriott brands also lack static sibling ladder mapping (unlike Choice/Hilton), so inactive ladder steps show generic labels only."
        : rootCause === "row_missing"
          ? "No overview.portfolio_context presentation row linked to brand."
          : rootCause === "body_field_missing"
            ? "overview.portfolio_context row exists but Body is empty."
            : "overview.portfolio_context row is inactive.",
    frontendSlotsRead: [
      "overview.portfolio_context (tier Title + Body)",
      "overview.relative_positioning (legacy Relative Positioning fallback)",
      "buildPortfolioLadderCellsHtml (Overview tab Portfolio Context ladder only)",
    ],
    apiExposure: "GET /api/brand-library/brand returns explorerPresentation blocks including overview.portfolio_context",
    rendersEmptyOnFirstTab:
      !ctxRow ||
      !body ||
      rootCause === "frontend_mapping_issue",
  };
}

function companyValidatedSnapshot(brandBasics) {
  const fields = brandBasics?.fields || {};
  return {
    companyValidated: fields["Company Validated"] ?? fields.company_validated ?? null,
    companyValidationDate:
      fields["Company Validation Date"] ?? fields.company_validation_date ?? null,
  };
}

function apiUrl(baseId, tableName, recordId = "") {
  const base = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(tableName)}`;
  return recordId ? `${base}/${encodeURIComponent(recordId)}` : base;
}

async function airtableFetch(baseId, apiKey, tableName, init = {}, recordId = "") {
  const res = await fetch(apiUrl(baseId, tableName, recordId), {
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

async function listPresentationForBrand(baseId, apiKey, brandRecordId, brandName) {
  return listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`
  );
}

async function listByFormula(baseId, apiKey, tableName, formula) {
  const records = [];
  let offset = "";
  do {
    const params = new URLSearchParams();
    params.set("pageSize", "100");
    if (formula) params.set("filterByFormula", formula);
    if (offset) params.set("offset", offset);
    const res = await fetch(`${apiUrl(baseId, tableName)}?${params.toString()}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `List failed ${tableName}: ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset || "";
  } while (offset);
  return records;
}

function mapPresentationRows(records) {
  return records.map((rec) => ({
    recordId: rec.id,
    slotKey: nz(rec.fields?.["Slot Key"]),
    title: nz(rec.fields?.Title),
    body: nz(rec.fields?.Body),
    sortOrder: rec.fields?.["Sort Order"],
    active: rec.fields?.Active,
  }));
}

function presentationFields(slotKey, title, body, sort, brandRecordId, brandName) {
  return {
    "Slot Key": slotKey,
    Title: title,
    Body: body,
    "Brand Name": brandName,
    Brand: [brandRecordId],
    Active: true,
    "Sort Order": sort,
  };
}

export function buildApplyCommand({
  brandSlug = "tribute-portfolio",
  allActive = false,
} = {}) {
  if (allActive) {
    return `npm run brand-explorer-portfolio-mix-context-normalization-writer -- --all-active --apply ${APPLY_FLAG_ALL_ACTIVE} ${APPLY_FLAG_ALL_ACTIVE_FOUNDER} ${APPLY_FLAG_NO_STATS}`;
  }
  return `npm run brand-explorer-portfolio-mix-context-normalization-writer -- --brand ${brandSlug} --apply ${APPLY_FLAG_TRIBUTE} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_NO_STATS}`;
}

function buildMixPlansForBrand(target, mixRows, chips) {
  const mixPlans = chips.map((chip) => {
    const live =
      mixRows.find((r) => nz(r.title).toLowerCase() === chip.title.toLowerCase()) ||
      mixRows.find((r) => Number(r.sortOrder) === chip.sort);
    return {
      brand: target.name,
      brandRecordId: target.recordId,
      slotKey: MIX_SLOT,
      title: chip.title,
      recordId: live?.recordId || null,
      action: live ? "update" : "create",
      proposedBody: chip.body,
      fields: presentationFields(
        MIX_SLOT,
        chip.title,
        chip.body,
        chip.sort,
        target.recordId,
        target.name
      ),
      needsUpdate:
        !live ||
        nz(live.body) !== chip.body ||
        nz(live.title) !== chip.title ||
        Number(live.sortOrder ?? -1) !== chip.sort,
    };
  });

  const obsoleteMixRows = mixRows.filter(
    (live) =>
      !chips.some((chip) => nz(chip.title).toLowerCase() === nz(live.title).toLowerCase()) &&
      !chips.some((chip) => Number(live.sortOrder) === chip.sort)
  );

  const obsoleteMixPlans = obsoleteMixRows.map((obsolete) => ({
    brand: target.name,
    brandRecordId: target.recordId,
    slotKey: MIX_SLOT,
    recordId: obsolete.recordId,
    title: obsolete.title,
    action: "archive",
    needsUpdate: true,
    fields: {
      Active: false,
      "Slot Key": MIX_SLOT_ARCHIVED,
    },
  }));

  return { mixPlans, obsoleteMixRows, obsoleteMixPlans, chips };
}

async function snapshotCompanyValidatedForBrands(recordIds) {
  const out = {};
  for (const recordId of recordIds) {
    const basics = await fetchBrandBasics(recordId);
    out[recordId] = companyValidatedSnapshot(basics);
  }
  return out;
}

export async function buildBrandExplorerPortfolioMixContextNormalizationWriterReport({
  brandIdOrName = "tribute-portfolio",
  allActive = false,
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noUnsupportedStatsConfirmed = false,
} = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const auditTargets = allActive
    ? ACTIVE_BRAND_AUDIT_TARGETS
    : ACTIVE_BRAND_AUDIT_TARGETS.filter((b) => b.recordId === TRIBUTE_RECORD_ID);

  const portfolioMixAuditByBrand = [];
  for (const target of ACTIVE_BRAND_AUDIT_TARGETS) {
    const raw = await listPresentationForBrand(baseId, apiKey, target.recordId, target.name);
    const rows = mapPresentationRows(raw);
    const mixRows = rows.filter((r) => r.slotKey === MIX_SLOT);
    portfolioMixAuditByBrand.push({
      brand: target.name,
      recordId: target.recordId,
      slug: target.slug,
      mixRowCount: mixRows.length,
      classification: classifyPortfolioMixRows(rows),
      mixRows: mixRows.map((r) => ({
        recordId: r.recordId,
        title: r.title,
        body: r.body,
        sortOrder: r.sortOrder,
      })),
      contextDiagnosis: diagnosePortfolioContext(rows, target.name),
    });
  }

  const tributeAudit = portfolioMixAuditByBrand.find((b) => b.recordId === TRIBUTE_RECORD_ID);
  const brandsToNormalize = (allActive ? ACTIVE_BRAND_AUDIT_TARGETS : ACTIVE_BRAND_AUDIT_TARGETS.filter((b) => b.recordId === TRIBUTE_RECORD_ID))
    .filter((target) => BRAND_MIX_PACKAGES[target.recordId]);

  const companyValidatedBefore = await snapshotCompanyValidatedForBrands(
    ACTIVE_BRAND_AUDIT_TARGETS.map((b) => b.recordId)
  );

  const tributeRows = mapPresentationRows(
    await listPresentationForBrand(baseId, apiKey, TRIBUTE_RECORD_ID, BRAND_NAME)
  );
  const tributeMixRows = tributeRows.filter((r) => r.slotKey === MIX_SLOT);
  const tributeContextRow = tributeRows.find((r) => r.slotKey === PORTFOLIO_CONTEXT_SLOT);
  const tributeRelativeRow = tributeRows.find((r) => r.slotKey === RELATIVE_POSITIONING_SLOT);

  const contextDiagnosis = diagnosePortfolioContext(tributeRows, BRAND_NAME);

  const brandNormalizationPlans = [];
  for (const target of brandsToNormalize) {
    const audit = portfolioMixAuditByBrand.find((b) => b.recordId === target.recordId);
    const mixRows = (audit?.mixRows || []).map((r) => ({
      recordId: r.recordId,
      title: r.title,
      body: r.body,
      sortOrder: r.sortOrder,
    }));
    const chips = BRAND_MIX_PACKAGES[target.recordId];
    const { mixPlans, obsoleteMixRows, obsoleteMixPlans } = buildMixPlansForBrand(target, mixRows, chips);
    brandNormalizationPlans.push({
      brand: target.name,
      recordId: target.recordId,
      slug: target.slug,
      classification: audit?.classification,
      mixPlans,
      obsoleteMixRows,
      obsoleteMixPlans,
      chips,
    });
  }

  const tributePlan = brandNormalizationPlans.find((p) => p.recordId === TRIBUTE_RECORD_ID);
  const mixPlans = tributePlan?.mixPlans || [];
  const obsoleteMixRows = tributePlan?.obsoleteMixRows || [];

  const contextPlan = {
    brand: BRAND_NAME,
    brandRecordId: TRIBUTE_RECORD_ID,
    slotKey: PORTFOLIO_CONTEXT_SLOT,
    recordId: tributeContextRow?.recordId || null,
    action: tributeContextRow ? "update" : "create",
    fields: presentationFields(
      PORTFOLIO_CONTEXT_SLOT,
      PORTFOLIO_CONTEXT_TIER,
      TRIBUTE_PORTFOLIO_CONTEXT_NARRATIVE,
      0,
      TRIBUTE_RECORD_ID,
      BRAND_NAME
    ),
    needsUpdate:
      !tributeContextRow ||
      nz(tributeContextRow.title) !== PORTFOLIO_CONTEXT_TIER ||
      normalizeBody(tributeContextRow.body) !== TRIBUTE_PORTFOLIO_CONTEXT_NARRATIVE,
  };

  const relativePlan = {
    brand: BRAND_NAME,
    brandRecordId: TRIBUTE_RECORD_ID,
    slotKey: RELATIVE_POSITIONING_SLOT,
    recordId: tributeRelativeRow?.recordId || null,
    action: tributeRelativeRow ? "update" : "create",
    fields: presentationFields(
      RELATIVE_POSITIONING_SLOT,
      "",
      RELATIVE_POSITIONING_BODY,
      0,
      TRIBUTE_RECORD_ID,
      BRAND_NAME
    ),
    needsUpdate:
      !tributeRelativeRow || normalizeBody(tributeRelativeRow.body) !== RELATIVE_POSITIONING_BODY,
  };

  const rowsWouldUpdate = [];
  const rowsWouldCreate = [];
  const rowsWouldDeactivate = [];
  const applyBlockers = [];

  const tributeOnly = !allActive;
  const includeTributeContext = tributeOnly || allActive;

  for (const brandPlan of brandNormalizationPlans) {
    for (const plan of brandPlan.mixPlans) {
      if (!plan.needsUpdate) continue;
      if (plan.action === "create") rowsWouldCreate.push(plan);
      else rowsWouldUpdate.push(plan);
    }
    for (const obsolete of brandPlan.obsoleteMixPlans || []) {
      if (!obsolete.needsUpdate) continue;
      rowsWouldUpdate.push(obsolete);
    }
  }

  if (includeTributeContext) {
    if (contextPlan.needsUpdate) {
      if (contextPlan.action === "create") rowsWouldCreate.push(contextPlan);
      else rowsWouldUpdate.push(contextPlan);
    }
    if (relativePlan.needsUpdate) {
      if (relativePlan.action === "create") rowsWouldCreate.push(relativePlan);
      else rowsWouldUpdate.push(relativePlan);
    }
  }

  const proposedCopyByBrand = brandNormalizationPlans.map((brandPlan) => ({
    brand: brandPlan.brand,
    copy: brandPlan.chips.map((c) => `${c.title} ${c.body}`).join("\n"),
  }));
  if (includeTributeContext) {
    proposedCopyByBrand.push({
      brand: BRAND_NAME,
      copy: [TRIBUTE_PORTFOLIO_CONTEXT_NARRATIVE, RELATIVE_POSITIONING_BODY].join("\n"),
    });
  }

  const allProposedCopy = proposedCopyByBrand.map((b) => b.copy).join("\n");

  if (UNSUPPORTED_PERCENTAGE_RE.test(allProposedCopy)) {
    applyBlockers.push("unsupported_percentage_in_proposed_copy");
  }
  if (CURIO_HILTON_LEAK_RE.test(allProposedCopy)) {
    applyBlockers.push("curio_hilton_copy_leak");
  }
  for (const brandPlan of brandNormalizationPlans) {
    if (brandPlan.chips.length < MIN_TRIBUTE_MIX_CHIPS) {
      applyBlockers.push(`insufficient_mix_chips:${brandPlan.brand}:${brandPlan.chips.length}`);
    }
  }
  if (includeTributeContext) {
    if (GENERIC_LADDER_RE.test(TRIBUTE_PORTFOLIO_CONTEXT_NARRATIVE)) {
      applyBlockers.push("generic_ladder_copy_in_portfolio_context");
    }
    if (!nz(TRIBUTE_PORTFOLIO_CONTEXT_NARRATIVE)) {
      applyBlockers.push("portfolio_context_empty");
    }
  }

  const tributeWouldHaveChips =
    mixPlans.filter((p) => p.needsUpdate || p.recordId).length >= MIN_TRIBUTE_MIX_CHIPS ||
    TRIBUTE_PORTFOLIO_MIX_CHIPS.length >= MIN_TRIBUTE_MIX_CHIPS;

  const applyGatesReady = apply && approveBatch && founderReviewed && noUnsupportedStatsConfirmed;

  const hasWork =
    rowsWouldUpdate.length > 0 || rowsWouldCreate.length > 0 || rowsWouldDeactivate.length > 0;
  const canApply = applyGatesReady && applyBlockers.length === 0 && hasWork;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = { ...companyValidatedBefore };

  if (canApply) {
    const updated = [];
    const created = [];
    const deactivated = [];
    const errors = [];

    const patchOps = [
      ...rowsWouldUpdate.map((r) => ({ ...r, method: "PATCH" })),
      ...rowsWouldCreate.map((r) => ({ ...r, method: "POST" })),
      ...rowsWouldDeactivate.map((r) => ({ ...r, method: "PATCH", fields: r.fields })),
    ];

    for (const row of patchOps) {
      const slotKey = row.slotKey || row.fields?.["Slot Key"];
      if (isProtectedSlot(slotKey) && row.method !== "PATCH") {
        errors.push({ slotKey, message: "protected_slot_blocked" });
        continue;
      }
      const fields = row.fields;
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        {
          method: row.method,
          body: JSON.stringify({ fields, typecast: true }),
        },
        row.method === "PATCH" ? row.recordId : ""
      );
      if (!res.ok) {
        errors.push({
          recordId: row.recordId,
          slotKey,
          message: json.error?.message || res.status,
        });
      } else if (row.method === "PATCH" && fields["Slot Key"] === MIX_SLOT_ARCHIVED) {
        deactivated.push({
          brand: row.brand,
          recordId: row.recordId,
          title: row.title,
          archivedSlotKey: MIX_SLOT_ARCHIVED,
        });
      } else if (row.method === "PATCH") {
        updated.push({
          brand: row.brand,
          recordId: row.recordId,
          slotKey,
          title: row.title || fields?.Title,
        });
      } else {
        created.push({
          brand: row.brand,
          recordId: json.id,
          slotKey,
          title: row.title || fields?.Title,
        });
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    airtableModified =
      (updated.length > 0 || created.length > 0 || deactivated.length > 0) && errors.length === 0;
    applyResults = { updated, created, deactivated, errors };
    companyValidatedAfter = await snapshotCompanyValidatedForBrands(
      ACTIVE_BRAND_AUDIT_TARGETS.map((b) => b.recordId)
    );
  } else if (apply) {
    applyResults = { updated: [], created: [], deactivated: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  const otherBrandIssues = portfolioMixAuditByBrand
    .filter((b) => b.recordId !== TRIBUTE_RECORD_ID)
    .filter((b) => b.classification !== "radisson_style_complete")
    .map((b) => ({
      brand: b.brand,
      classification: b.classification,
      mixRowCount: b.mixRowCount,
      auditOnly: tributeOnly && !BRAND_MIX_PACKAGES[b.recordId],
      hasNormalizationPackage: Boolean(BRAND_MIX_PACKAGES[b.recordId]),
    }));

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    v25C4CWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    allActiveAudit: allActive,
    brand: { name: BRAND_NAME, recordId: TRIBUTE_RECORD_ID, slug: "tribute-portfolio" },
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    portfolioMixAuditByActiveBrand: portfolioMixAuditByBrand,
    tributePortfolioMixDiagnosis: {
      classification: tributeAudit?.classification,
      currentRows: tributeMixRows,
      sentenceStyleDetected: tributeMixRows.some((r) =>
        UNSUPPORTED_PERCENTAGE_RE.test(`${r.title} ${r.body}`)
      ),
      singleChipOnly: tributeMixRows.length === 1,
    },
    tributeProposedPortfolioMixChips: TRIBUTE_PORTFOLIO_MIX_CHIPS,
    brandNormalizationPlans: brandNormalizationPlans.map((p) => ({
      brand: p.brand,
      recordId: p.recordId,
      classification: p.classification,
      proposedChips: p.chips,
      mixPlans: p.mixPlans.map((plan) => ({
        title: plan.title,
        body: plan.proposedBody,
        action: plan.action,
        needsUpdate: plan.needsUpdate,
      })),
      obsoleteMixRows: p.obsoleteMixRows,
    })),
    otherActiveBrandPortfolioMixIssues: otherBrandIssues,
    radissonStyleBrandsLeftUnchanged: portfolioMixAuditByBrand
      .filter((b) => b.classification === "radisson_style_complete")
      .map((b) => b.brand),
    tributePortfolioContextCurrentState: contextDiagnosis,
    portfolioContextRootCause: contextDiagnosis.rootCause,
    portfolioContextRootCauseDetail: contextDiagnosis.rootCauseDetail,
    proposedPortfolioContextRepair: {
      overviewPortfolioContext: contextPlan.fields,
      overviewRelativePositioning: relativePlan.fields,
      frontendRepairNote:
        "public/js/brand-explorer-atelier-from-api.js updated to render overview.portfolio_context Body as narrative above ladder and add Marriott sibling ladder mapping.",
    },
    proposedTablePayload: {
      portfolioMix: mixPlans.map((p) => ({
        title: p.title,
        body: p.proposedBody,
        action: p.action,
      })),
      portfolioContext: contextPlan.fields,
      relativePositioning: relativePlan.fields,
    },
    rowsWouldCreate: rowsWouldCreate.map((r) => ({
      brand: r.brand,
      slotKey: r.slotKey,
      title: r.title || r.fields?.Title,
      action: r.action,
    })),
    rowsWouldUpdate: rowsWouldUpdate.map((r) => ({
      brand: r.brand,
      recordId: r.recordId,
      slotKey: r.slotKey,
      title: r.title || r.fields?.Title,
      action: r.action,
    })),
    rowsWouldDeactivate,
    unsupportedPercentagesRemoved: !UNSUPPORTED_PERCENTAGE_RE.test(allProposedCopy),
    curioHiltonLanguageExcluded: !CURIO_HILTON_LEAK_RE.test(allProposedCopy),
    companyValidatedUntouched,
    companyValidatedBefore,
    companyValidatedAfter,
    loyaltyRowsUntouched: true,
    openingsRowsUntouched: true,
    momentumRowsUntouched: true,
    standardsRequirementRowsUntouched: true,
    airtableModified,
    applyGates: {
      apply,
      allActive,
      approveBatch,
      founderReviewed,
      noUnsupportedStatsConfirmed,
      ready: applyGatesReady,
      canApply,
    },
    applyBlockers,
    applyResults,
    exactApplyCommand: buildApplyCommand({ brandSlug: "tribute-portfolio", allActive }),
    exactTributeApplyCommand: buildApplyCommand({ brandSlug: "tribute-portfolio", allActive: false }),
    idempotentAfterApply: !hasWork,
  };
}

export function buildBrandExplorerPortfolioMixContextNormalizationWriterMarkdown(report) {
  const lines = [
    `# Brand Explorer Portfolio Mix + Portfolio Context Normalization Writer v${WRITER_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Brand: **${report.brand.name}**`,
    "",
    "## Portfolio Mix audit",
    "",
    "| Brand | Classification | Rows |",
    "|-------|----------------|------|",
  ];

  for (const row of report.portfolioMixAuditByActiveBrand) {
    lines.push(`| ${row.brand} | ${row.classification} | ${row.mixRowCount} |`);
  }

  lines.push(
    "",
    "## Tribute Portfolio Context root cause",
    "",
    `- **${report.portfolioContextRootCause}**`,
    "",
    report.portfolioContextRootCauseDetail,
    "",
    "## Tribute proposed Portfolio Mix chips",
    ""
  );

  for (const chip of report.tributeProposedPortfolioMixChips) {
    lines.push(`- **${chip.title}** — ${chip.body}`);
  }

  if (report.brandNormalizationPlans?.length > 1) {
    lines.push("", "## All-brand proposed Portfolio Mix chips", "");
    for (const brandPlan of report.brandNormalizationPlans) {
      lines.push(`### ${brandPlan.brand}`, "");
      for (const chip of brandPlan.proposedChips) {
        lines.push(`- **${chip.title}** — ${chip.body}`);
      }
      lines.push("");
    }
  }

  lines.push(
    "",
    "## Summary",
    "",
    `| Rows would create | ${report.rowsWouldCreate.length} |`,
    `| Rows would update | ${report.rowsWouldUpdate.length} |`,
    `| Unsupported % removed | ${report.unsupportedPercentagesRemoved ? "yes" : "no"} |`,
    `| Airtable modified | ${report.airtableModified ? "yes" : "no"} |`,
    "",
    "## Exact apply command",
    "",
    "```bash",
    report.exactApplyCommand,
    "```",
    ""
  );

  return lines.join("\n");
}
