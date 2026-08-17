/**
 * Brand Explorer Tribute Visible Content Repair Writer v25C-4A.
 *
 * Repairs Tribute Portfolio visible content defects: Curio tagline leakage,
 * empty Relative Positioning / Portfolio Context, and title-only Value Creation
 * Scenario cards. Dry-run by default.
 *
 * @see docs/data-intelligence/brand-explorer-tribute-visible-content-repair-writer-v25C-4A.md
 */
import { fetchBrandBasics } from "./tribute-portfolio-package-pipeline.js";
import {
  TRIBUTE_RECORD_ID,
  BRAND_NAME,
} from "./tribute-portfolio-brand-package.js";

export const WRITER_VERSION = "25C-4A";
export const REPORT_JSON_NAME = "brand-explorer-tribute-visible-content-repair-writer.json";
export const REPORT_MD_NAME = "brand-explorer-tribute-visible-content-repair-writer.md";
export const DOC_MD_NAME = "brand-explorer-tribute-visible-content-repair-writer-v25C-4A.md";

export const APPLY_FLAG_BATCH = "--approve-brand-explorer-v25C-4A-visible-content-repair";
export const APPLY_FLAG_FOUNDER = "--founder-reviewed-tribute-positioning-copy";
export const APPLY_FLAG_NO_CURIO = "--confirm-no-curio-tagline-leakage";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";
const BRAND_TAGLINE_FIELD = "Brand Tagline";

const CURIO_TAGLINE_RE = /exactly\s+like\s+nothing\s+else/i;
const CURIO_COPY_RE = /exactly\s+like\s+nothing\s+else|curio\s+collection\s+by\s+hilton/i;

const PROTECTED_SLOT_KEYS = new Set([
  "footprint.momentum",
  "footprint.openings",
  "loyalty.earn",
  "loyalty.redeem",
  "loyalty.elite",
  "loyalty.proof",
]);

const PROTECTED_SLOT_PREFIXES = [/^loyalty\./i, /^footprint\.momentum/i, /^footprint\.openings/i];

const GOVERNANCE_LABELS = [
  "Founder-reviewed positioning and owner-value copy",
  "Tagline from Marriott consumer source (Stay independent.)",
  "Not company-validated",
  "Not Marriott-validated",
];

/** Source-confirmed Tribute tagline — Marriott consumer / brand page (be.positioning.tagline). */
export const OFFICIAL_TRIBUTE_TAGLINE = "Stay independent.";
export const OFFICIAL_TAGLINE_SOURCE_FACT_KEY = "be.positioning.tagline";
export const OFFICIAL_TAGLINE_SOURCE_NOTE =
  "Directly stated on Marriott Tribute Portfolio consumer materials; extracted in tribute-portfolio-targeted-extract.";

export const PORTFOLIO_CONTEXT_SLOT = "overview.portfolio_context";
export const PORTFOLIO_CONTEXT_TIER = "2";

export const PORTFOLIO_CONTEXT_BODY =
  "Lifestyle soft-collection within Marriott International—Tribute Portfolio sits with Autograph Collection and Design Hotels as an independent-character path for hotels with distinctive local style and programming. Useful when owners want Marriott Bonvoy, global distribution, and commercial systems without a standardized full-service flag conversion—not select-service, extended-stay, or rigid prototype-led rebrands.";

/** Serves Overview snapshot Relative Positioning via overview.portfolio_context Body. */
export const RELATIVE_POSITIONING_BODY = PORTFOLIO_CONTEXT_BODY;

export const VALUE_SCENARIO_PACKAGES = [
  {
    slotKey: "valueOwners.scenario.1",
    sort: 0,
    title: "Independent Reflag",
    body:
      "Independent or soft-brand full-service hotels that already carry local story, design point of view, and F&B complexity—Tribute adds Marriott Bonvoy, reservations, and commercial scale while preserving individuality. Best when the asset can support collection standards without erasing neighborhood character.",
  },
  {
    slotKey: "valueOwners.scenario.2",
    sort: 1,
    title: "Tired Upscale Asset",
    body:
      "Mature upscale or upper-mid assets needing refresh capital and relevancy—Tribute reframing can reposition tired product around character, culinary, and experiential storytelling while unlocking Marriott affiliation economics versus standalone independent operation.",
  },
  {
    slotKey: "valueOwners.scenario.3",
    sort: 2,
    title: "Markets With Strong Brand Presence",
    body:
      "Gateway and regional markets where Marriott distribution and loyalty density matter—Tribute lets sponsors place a character-led hotel inside an established Marriott commercial stack without defaulting to a conventional Marriott Hotels & Resorts or select-service template.",
  },
  {
    slotKey: "valueOwners.scenario.4",
    sort: 3,
    title: "Third-Party Operator–Led",
    body:
      "Assets run by experienced third-party operators who can execute collection design compliance, F&B complexity, and Marriott systems cutover—Tribute fits when management partnership is in place and the sponsor needs brand affiliation for financing, sales, and loyalty lift.",
  },
];

const FILES_READ = [
  "AGENTS.md",
  "reports/brand-explorer-required-section-population-contract.md",
  "reports/brand-explorer-required-section-population-contract.json",
  "reports/brand-explorer-visual-display-defect-audit.md",
  "reports/brand-explorer-visual-display-defect-audit.json",
  "reports/brand-explorer-required-section-source-capture-package.md",
  "reports/brand-explorer-required-section-source-capture-package.json",
  "reports/tribute-portfolio-targeted-extract.md",
  "reports/tribute-portfolio-targeted-extract.json",
  "docs/brand-explorer-presentation-slots.md",
  "api/brand-library.js",
  "public/js/brand-explorer-atelier-from-api.js",
  "public/js/brand-explorer-gold-detail.js",
  "live Tribute Brand Basics / Brand Profile record",
  "live Tribute Brand Explorer Presentation rows",
  "live Tribute Source Library records",
  "live Tribute Partner Facts",
  "live Curio/Kimpton/Radisson/Ascend reference rows",
];

const FILES_CHANGED = [
  "lib/partner-intelligence/brand-explorer-tribute-visible-content-repair-writer.js",
  "scripts/brand-explorer-tribute-visible-content-repair-writer.mjs",
  "docs/data-intelligence/brand-explorer-tribute-visible-content-repair-writer-v25C-4A.md",
  "reports/brand-explorer-tribute-visible-content-repair-writer.md",
  "reports/brand-explorer-tribute-visible-content-repair-writer.json",
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

async function scanActiveBrandTaglines(baseId, apiKey) {
  const records = await listByFormula(
    baseId,
    apiKey,
    BRAND_BASICS_TABLE,
    "{Brand Status}='Active'"
  );
  return records.map((rec) => ({
    recordId: rec.id,
    brandName: nz(rec.fields?.["Brand Name"]),
    tagline: nz(rec.fields?.[BRAND_TAGLINE_FIELD]),
    hasCurioTaglineLeak: CURIO_TAGLINE_RE.test(nz(rec.fields?.[BRAND_TAGLINE_FIELD])),
  }));
}

export function buildApplyCommand(brandSlug = "tribute-portfolio") {
  return `npm run brand-explorer-tribute-visible-content-repair-writer -- --brand ${brandSlug} --apply ${APPLY_FLAG_BATCH} ${APPLY_FLAG_FOUNDER} ${APPLY_FLAG_NO_CURIO}`;
}

function diagnoseTaglineLeakage(brandBasics, presentationRows) {
  const basicsTagline = nz(brandBasics?.fields?.[BRAND_TAGLINE_FIELD]);
  const uiSurfaces = [
    {
      surface: "Brand Basics → Brand Tagline",
      field: BRAND_TAGLINE_FIELD,
      table: BRAND_BASICS_TABLE,
      value: basicsTagline,
      isCurioLeak: CURIO_TAGLINE_RE.test(basicsTagline),
      defectSource: "copied_seed_or_reference_brand_fixture_leakage",
    },
    {
      surface: "API → brandTaglineMotto",
      field: BRAND_TAGLINE_FIELD,
      table: BRAND_BASICS_TABLE,
      value: basicsTagline,
      isCurioLeak: CURIO_TAGLINE_RE.test(basicsTagline),
      defectSource: "brand_basics_field_maps_to_api_brandTaglineMotto",
    },
    {
      surface: "Frontend hero (brand-explorer-gold-detail.js renderHero)",
      field: BRAND_TAGLINE_FIELD,
      table: BRAND_BASICS_TABLE,
      value: basicsTagline,
      isCurioLeak: CURIO_TAGLINE_RE.test(basicsTagline),
      defectSource: "frontend_reads_brandTaglineMotto_from_basics",
    },
    {
      surface: "Frontend presentation hero positionLine",
      field: BRAND_TAGLINE_FIELD,
      table: BRAND_BASICS_TABLE,
      value: basicsTagline,
      isCurioLeak: CURIO_TAGLINE_RE.test(basicsTagline),
      defectSource: "frontend_reads_brandTaglineMotto_from_basics",
    },
    {
      surface: "Overview Featured Application featLead",
      field: BRAND_TAGLINE_FIELD,
      table: BRAND_BASICS_TABLE,
      value: basicsTagline,
      isCurioLeak: CURIO_TAGLINE_RE.test(basicsTagline),
      defectSource: "frontend_reads_brandTaglineMotto_from_basics",
    },
  ];

  const presentationCurioHits = presentationRows
    .filter((r) => CURIO_COPY_RE.test(`${r.title} ${r.body}`))
    .map((r) => ({
      surface: `Presentation row ${r.slotKey}`,
      recordId: r.recordId,
      slotKey: r.slotKey,
      value: CURIO_COPY_RE.test(r.title) ? r.title : r.body,
      isCurioLeak: true,
      defectSource: "presentation_row_copy",
    }));

  return {
    curioPhraseFound: CURIO_TAGLINE_RE.test(basicsTagline) || presentationCurioHits.length > 0,
    basicsTaglineCurrent: basicsTagline,
    uiSurfaces,
    presentationCurioHits,
    primaryDefectSource:
      CURIO_TAGLINE_RE.test(basicsTagline)
        ? "Brand Basics Brand Tagline contains Curio Collection tagline (seed/fixture leakage)"
        : presentationCurioHits.length
          ? "Presentation row contains Curio copy"
          : null,
  };
}

export async function buildBrandExplorerTributeVisibleContentRepairWriterReport({
  brandIdOrName = "tribute-portfolio",
  apply = false,
  approveBatch = false,
  founderReviewed = false,
  noCurioLeakageConfirmed = false,
} = {}) {
  const brandRecordId =
    nz(brandIdOrName).toLowerCase() === "tribute-portfolio" || !nz(brandIdOrName)
      ? TRIBUTE_RECORD_ID
      : nz(brandIdOrName);
  if (brandRecordId !== TRIBUTE_RECORD_ID) {
    throw new Error(`v25C-4A pilot supports Tribute Portfolio only (${TRIBUTE_RECORD_ID})`);
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const brandBasicsBefore = await fetchBrandBasics(brandRecordId);
  const companyValidatedBefore = companyValidatedSnapshot(brandBasicsBefore);

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(brandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(BRAND_NAME)}')`
  );

  const presentationRows = presentationRaw.map((rec) => ({
    recordId: rec.id,
    slotKey: nz(rec.fields?.["Slot Key"]),
    title: nz(rec.fields?.Title),
    body: nz(rec.fields?.Body),
    sortOrder: rec.fields?.["Sort Order"],
    active: rec.fields?.Active,
  }));

  const activeBrandTaglineAudit = await scanActiveBrandTaglines(baseId, apiKey);

  const taglineDiagnosis = diagnoseTaglineLeakage(brandBasicsBefore, presentationRows);

  const portfolioContextRow = presentationRows.find((r) => r.slotKey === PORTFOLIO_CONTEXT_SLOT);
  const relativeLegacyRow = presentationRows.find((r) => r.slotKey === "overview.relative_positioning");

  const portfolioContextCurrent = {
    exists: Boolean(portfolioContextRow),
    recordId: portfolioContextRow?.recordId || null,
    title: portfolioContextRow?.title || "",
    body: portfolioContextRow?.body || "",
    relativePositioningEmpty:
      !nz(portfolioContextRow?.body) && !nz(relativeLegacyRow?.body),
  };

  const scenarioRows = VALUE_SCENARIO_PACKAGES.map((pkg) => {
    const live = presentationRows.find((r) => r.slotKey === pkg.slotKey);
    return {
      ...pkg,
      recordId: live?.recordId || null,
      exists: Boolean(live),
      currentTitle: live?.title || "",
      currentBody: live?.body || "",
      titleOnly: Boolean(live?.title) && !nz(live?.body),
    };
  });

  const taglinePlan = {
    table: BRAND_BASICS_TABLE,
    recordId: brandRecordId,
    field: BRAND_TAGLINE_FIELD,
    currentValue: taglineDiagnosis.basicsTaglineCurrent,
    proposedValue: OFFICIAL_TRIBUTE_TAGLINE,
    officialTaglineConfirmed: true,
    positioningNotTagline: false,
    sourceFactKey: OFFICIAL_TAGLINE_SOURCE_FACT_KEY,
    sourceNote: OFFICIAL_TAGLINE_SOURCE_NOTE,
    needsUpdate: CURIO_TAGLINE_RE.test(taglineDiagnosis.basicsTaglineCurrent),
  };

  const portfolioContextPlan = {
    slotKey: PORTFOLIO_CONTEXT_SLOT,
    recordId: portfolioContextRow?.recordId || null,
    action: portfolioContextRow ? "update" : "create",
    fields: {
      "Slot Key": PORTFOLIO_CONTEXT_SLOT,
      Title: PORTFOLIO_CONTEXT_TIER,
      Body: PORTFOLIO_CONTEXT_BODY,
      "Brand Name": BRAND_NAME,
      Brand: [brandRecordId],
      Active: true,
      "Sort Order": 0,
    },
    relativePositioningBody: RELATIVE_POSITIONING_BODY,
    needsUpdate:
      !portfolioContextRow ||
      nz(portfolioContextRow.title) !== PORTFOLIO_CONTEXT_TIER ||
      normalizeBody(portfolioContextRow.body) !== PORTFOLIO_CONTEXT_BODY,
  };

  const scenarioPlans = scenarioRows.map((row) => ({
    slotKey: row.slotKey,
    recordId: row.recordId,
    action: row.exists ? "update" : "create",
    title: row.title,
    proposedBody: row.body,
    currentTitle: row.currentTitle || row.title,
    currentBody: row.currentBody,
    fields: {
      "Slot Key": row.slotKey,
      Title: row.title,
      Body: row.body,
      "Brand Name": BRAND_NAME,
      Brand: [brandRecordId],
      Active: true,
      "Sort Order": row.sort,
    },
    needsUpdate: !row.exists || !nz(row.currentBody) || normalizeBody(row.currentBody) !== row.body,
  }));

  const rowsWouldUpdate = [];
  const rowsWouldCreate = [];
  const fieldsWouldUpdate = [];
  const applyBlockers = [];

  if (taglinePlan.needsUpdate) {
    fieldsWouldUpdate.push(taglinePlan);
  }

  if (portfolioContextPlan.needsUpdate) {
    if (portfolioContextPlan.action === "create") {
      rowsWouldCreate.push(portfolioContextPlan);
    } else {
      rowsWouldUpdate.push(portfolioContextPlan);
    }
  }

  for (const plan of scenarioPlans) {
    if (!plan.needsUpdate) continue;
    if (plan.action === "create") rowsWouldCreate.push(plan);
    else rowsWouldUpdate.push(plan);
  }

  const proposedUiCopy = [
    taglinePlan.proposedValue,
    PORTFOLIO_CONTEXT_BODY,
    ...scenarioPlans.map((p) => p.proposedBody),
  ].join("\n");

  if (CURIO_COPY_RE.test(proposedUiCopy)) {
    applyBlockers.push("curio_copy_in_proposed_payload");
  }
  if (CURIO_TAGLINE_RE.test(taglineDiagnosis.basicsTaglineCurrent) && !taglinePlan.needsUpdate) {
    applyBlockers.push("curio_tagline_not_repaired");
  }
  if (!nz(PORTFOLIO_CONTEXT_BODY)) {
    applyBlockers.push("portfolio_context_empty");
  }
  if (!nz(RELATIVE_POSITIONING_BODY)) {
    applyBlockers.push("relative_positioning_empty");
  }
  if (scenarioPlans.some((p) => !nz(p.proposedBody))) {
    applyBlockers.push("value_scenario_missing_description");
  }

  const postApplyCurioRemaining =
    CURIO_TAGLINE_RE.test(taglinePlan.proposedValue) ||
    CURIO_COPY_RE.test(PORTFOLIO_CONTEXT_BODY) ||
    scenarioPlans.some((p) => CURIO_COPY_RE.test(p.proposedBody));

  if (postApplyCurioRemaining) {
    applyBlockers.push("curio_phrase_would_remain");
  }

  const applyGatesReady = apply && approveBatch && founderReviewed && noCurioLeakageConfirmed;
  const hasWork =
    taglinePlan.needsUpdate ||
    rowsWouldUpdate.length > 0 ||
    rowsWouldCreate.length > 0;
  const canApply = applyGatesReady && applyBlockers.length === 0 && hasWork;

  let airtableModified = false;
  let applyResults = null;
  let companyValidatedAfter = companyValidatedBefore;

  if (canApply) {
    const updatedBasics = [];
    const updatedPresentation = [];
    const createdPresentation = [];
    const errors = [];

    if (taglinePlan.needsUpdate) {
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        BRAND_BASICS_TABLE,
        {
          method: "PATCH",
          body: JSON.stringify({
            fields: { [BRAND_TAGLINE_FIELD]: taglinePlan.proposedValue },
            typecast: true,
          }),
        },
        brandRecordId
      );
      if (!res.ok) {
        errors.push({ target: "brand_basics_tagline", message: json.error?.message || res.status });
      } else {
        updatedBasics.push({ field: BRAND_TAGLINE_FIELD, value: taglinePlan.proposedValue });
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    for (const row of rowsWouldUpdate) {
      if (isProtectedSlot(row.slotKey || row.fields?.["Slot Key"])) {
        errors.push({ recordId: row.recordId, message: "protected_slot_blocked" });
        continue;
      }
      const { res, json } = await airtableFetch(
        baseId,
        apiKey,
        PRESENTATION_TABLE,
        {
          method: "PATCH",
          body: JSON.stringify({ fields: row.fields, typecast: true }),
        },
        row.recordId
      );
      if (!res.ok) {
        errors.push({
          recordId: row.recordId,
          slotKey: row.slotKey,
          message: json.error?.message || res.status,
        });
      } else {
        updatedPresentation.push({ recordId: row.recordId, slotKey: row.slotKey || row.fields?.["Slot Key"] });
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    for (const row of rowsWouldCreate) {
      const slotKey = row.slotKey || row.fields?.["Slot Key"];
      if (isProtectedSlot(slotKey)) {
        errors.push({ slotKey, message: "protected_slot_blocked" });
        continue;
      }
      const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
        method: "POST",
        body: JSON.stringify({ fields: row.fields, typecast: true }),
      });
      if (!res.ok) {
        errors.push({ slotKey, message: json.error?.message || res.status });
      } else {
        createdPresentation.push({ recordId: json.id, slotKey });
      }
      await new Promise((r) => setTimeout(r, 220));
    }

    airtableModified =
      (updatedBasics.length > 0 || updatedPresentation.length > 0 || createdPresentation.length > 0) &&
      errors.length === 0;
    applyResults = { updatedBasics, updatedPresentation, createdPresentation, errors };
    companyValidatedAfter = companyValidatedSnapshot(await fetchBrandBasics(brandRecordId));
  } else if (apply) {
    applyResults = { updatedBasics: [], updatedPresentation: [], createdPresentation: [], errors: [], blocked: true, blockers: applyBlockers };
  }

  const companyValidatedUntouched =
    JSON.stringify(companyValidatedBefore) === JSON.stringify(companyValidatedAfter);

  return {
    writerVersion: WRITER_VERSION,
    writerExists: true,
    v25C4AWriterExists: true,
    generatedAt: new Date().toISOString(),
    mode: apply ? (canApply ? "apply" : "apply_blocked") : "dry-run",
    brand: { name: BRAND_NAME, recordId: brandRecordId, slug: "tribute-portfolio" },
    marriottValidationImplied: false,
    governanceLabels: [...GOVERNANCE_LABELS],
    filesRead: FILES_READ,
    filesChanged: FILES_CHANGED,
    taglineLeakageDiagnosis: taglineDiagnosis,
    fieldsContainingExactlyLikeNothingElse: [
      ...taglineDiagnosis.uiSurfaces.filter((s) => s.isCurioLeak),
      ...taglineDiagnosis.presentationCurioHits,
    ],
    activeBrandTaglineAudit,
    proposedTaglineOrPositioning: {
      current: taglineDiagnosis.basicsTaglineCurrent,
      proposed: OFFICIAL_TRIBUTE_TAGLINE,
      officialTaglineConfirmed: true,
      isPositioningCopyNotTagline: false,
      sourceFactKey: OFFICIAL_TAGLINE_SOURCE_FACT_KEY,
      sourceNote: OFFICIAL_TAGLINE_SOURCE_NOTE,
    },
    relativePositioningCurrentState: {
      portfolioContextRow: portfolioContextCurrent,
      legacyRelativeRow: relativeLegacyRow
        ? { recordId: relativeLegacyRow.recordId, body: relativeLegacyRow.body }
        : null,
      empty: portfolioContextCurrent.relativePositioningEmpty,
    },
    proposedRelativePositioning: {
      slotKey: PORTFOLIO_CONTEXT_SLOT,
      servedVia: "overview.portfolio_context Body (frontend relativePositioningFromBrand)",
      body: RELATIVE_POSITIONING_BODY,
    },
    portfolioContextCurrentState: portfolioContextCurrent,
    proposedPortfolioContext: portfolioContextPlan,
    valueCreationScenarioCurrentState: scenarioRows.map((r) => ({
      slotKey: r.slotKey,
      recordId: r.recordId,
      title: r.currentTitle || r.title,
      body: r.currentBody,
      titleOnly: r.titleOnly,
    })),
    proposedFourScenarioDescriptions: scenarioPlans.map((p) => ({
      slotKey: p.slotKey,
      title: p.title,
      body: p.proposedBody,
    })),
    rowsWouldUpdate: rowsWouldUpdate.map((r) => ({
      recordId: r.recordId,
      slotKey: r.slotKey || r.fields?.["Slot Key"],
      action: r.action,
    })),
    rowsWouldCreate: rowsWouldCreate.map((r) => ({
      slotKey: r.slotKey || r.fields?.["Slot Key"],
      action: r.action,
    })),
    fieldsWouldUpdate,
    loyaltyRowsUntouched: true,
    openingsRowsUntouched: true,
    momentumRowsUntouched: true,
    imagesUntouched: true,
    companyValidatedUntouched,
    companyValidatedBefore,
    companyValidatedAfter,
    nonTargetSectionsModified: false,
    airtableModified,
    applyGates: {
      apply,
      approveBatch,
      founderReviewed,
      noCurioLeakageConfirmed,
      ready: applyGatesReady,
      canApply,
    },
    applyBlockers,
    applyResults,
    exactApplyCommand: buildApplyCommand("tribute-portfolio"),
    idempotentAfterApply: !hasWork,
    doesNotDo: [
      "Invent an official tagline without source support",
      "Modify loyalty, openings, or momentum rows",
      "Change images or Company Validated",
      "Imply Marriott validated anything",
      "Modify unrelated Brand Basics fields",
    ],
  };
}

export function buildBrandExplorerTributeVisibleContentRepairWriterMarkdown(report) {
  const lines = [
    `# Brand Explorer Tribute Visible Content Repair Writer v${WRITER_VERSION}`,
    "",
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Brand: **${report.brand.name}** (\`${report.brand.recordId}\`)`,
    `- v25C-4A exists: **${report.v25C4AWriterExists ? "yes" : "no"}**`,
    "",
    "## Tagline leakage",
    "",
    `- Curio phrase found: **${report.taglineLeakageDiagnosis.curioPhraseFound ? "yes" : "no"}**`,
    `- Primary source: ${report.taglineLeakageDiagnosis.primaryDefectSource || "none"}`,
    `- Current Brand Tagline: \`${report.proposedTaglineOrPositioning.current}\``,
    `- Proposed: \`${report.proposedTaglineOrPositioning.proposed}\` (official tagline confirmed: ${report.proposedTaglineOrPositioning.officialTaglineConfirmed ? "yes" : "no"})`,
    "",
    "## Relative Positioning & Portfolio Context",
    "",
    `- Portfolio context empty: **${report.portfolioContextCurrentState.empty ? "yes" : "no"}**`,
    `- Proposed tier: **${PORTFOLIO_CONTEXT_TIER}**`,
    "",
    report.proposedPortfolioContext.fields?.Body
      ? `> ${report.proposedPortfolioContext.fields.Body}`
      : "",
    "",
    "## Value Creation Scenarios",
    "",
  ];

  for (const s of report.proposedFourScenarioDescriptions) {
    lines.push(`### ${s.title}`, "", s.body, "");
  }

  lines.push(
    "## Summary",
    "",
    "| Metric | Value |",
    "|--------|-------|",
    `| Rows would update | ${report.rowsWouldUpdate.length} |`,
    `| Rows would create | ${report.rowsWouldCreate.length} |`,
    `| Brand Basics fields would update | ${report.fieldsWouldUpdate.length} |`,
    `| Airtable modified | ${report.airtableModified ? "yes" : "no"} |`,
    `| Company Validated untouched | ${report.companyValidatedUntouched ? "yes" : "no"} |`,
    "",
    "## Exact apply command",
    "",
    "```bash",
    report.exactApplyCommand,
    "```",
    ""
  );

  if (report.applyBlockers?.length) {
    lines.push("## Apply blockers", "");
    for (const b of report.applyBlockers) lines.push(`- ${b}`);
    lines.push("");
  }

  return lines.join("\n");
}
