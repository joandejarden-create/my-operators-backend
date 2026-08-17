/**
 * Brand Explorer Display Content Completion Writer v16.
 *
 * Display-level completion writer driven by v15 display-parity gaps.
 * Proposes (and optionally applies with gates) owner-facing copy for weak /
 * incomplete visible Brand Explorer sections. Does not change images, Brand
 * Website, sourceLinks, Company Validated, or Recent Openings without evidence.
 *
 * External copy guardrail: proposed `value` text is owner-facing Brand Explorer display
 * only — never internal governance, validation status, pipeline workflow, FDD/Item 19
 * stewardship notes, or "profile caveats" platform language.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { BRAND_ASSET_PILOT_CONFIG } from "./brand-asset-registry-workflow.js";
import {
  SECTION_DEFS,
  severityScore,
} from "./brand-explorer-display-parity-audit.js";

export const WRITER_VERSION = "16";
export const REPORT_JSON_NAME = "brand-explorer-display-content-completion-writer.json";
export const REPORT_MD_NAME = "brand-explorer-display-content-completion-writer.md";
export const DOC_MD_NAME = "brand-explorer-display-content-completion-writer-v16.md";
export const V15_REPORT_PATH = "reports/brand-explorer-display-parity-audit.json";

const DEFAULT_BRAND_KEY = "tribute-portfolio";
const DEFAULT_BRAND_RECORD_ID = "recCvV0PuZOi8c3hC";
const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const REQUIRED_APPLY_FLAG = "--approve-brand-explorer-display-content-completion";

/** Phrases that must never appear in proposed external display copy. */
const FORBIDDEN_EXTERNAL_COPY_PATTERNS = [
  /profile caveats/i,
  /internal[- ]review/i,
  /item\s*19/i,
  /conversion-first/i,
  /leading loyalty platform/i,
  /company-validated/i,
  /marriott-validated/i,
  /not source-backed in this profile/i,
  /explorer content/i,
  /dealality owner diligence/i,
  /pipeline stewardship/i,
  /profile governance/i,
];

function assertExternalDisplayCopy(text, sectionKey) {
  for (const pattern of FORBIDDEN_EXTERNAL_COPY_PATTERNS) {
    if (pattern.test(text)) {
      throw new Error(`External copy guardrail violated for ${sectionKey}: ${pattern}`);
    }
  }
}

const REFERENCE_FIXTURES = [
  "fixtures/brand-explorer-presentation-curio-full.json",
  "fixtures/brand-explorer-presentation-kimpton-full.json",
  "fixtures/brand-explorer-presentation-radisson-blu-full.json",
  "fixtures/brand-explorer-presentation-radisson-red-choice-full.json",
  "fixtures/brand-explorer-presentation-everhome-suites-full.json",
  "fixtures/brand-explorer-presentation-ascend-hotel-collection-full.json",
];

const PRESENTATION_WRITE_FIELDS = {
  slotKey: "Slot Key",
  title: "Title",
  body: "Body",
  brand: "Brand",
  brandName: "Brand Name",
  active: "Active",
  sortOrder: "Sort Order",
};

const BRAND_BASICS_WRITE_FIELDS = {
  positioning: "Brand Positioning",
  customerPromise: "Brand Customer Promise",
  valueProposition: "Brand Value Proposition",
};

const DISPLAY_MAPPING_ISSUES = [
  {
    sectionKey: "trustChip",
    tab: "Dealality Insight",
    section: "Trust chip / source basis",
    issue:
      "v15 audited top-level externalDisplayStatus/sourceType/confidenceLevel, but Brand Explorer hero renders trust via brand.governance (ProfileGovernanceTrustChip in brand-explorer-gold-detail.js).",
    proposedCodeFix:
      "Patch brand-explorer-display-parity-audit.js trustChip extractor to read brand.governance.displayLabel and displaySubtitle; do not write fake Airtable copy for this section.",
    writable: false,
    safeToApplyLater: false,
  },
  {
    sectionKey: "dataGapsCaveats",
    tab: "Dealality Insight",
    section: "Data gaps / caveats",
    issue:
      "v15 audited loadWarnings only; Dealality Insight tab reads insight.summary presentation slot first (dealalitySummaryFromBrand in brand-explorer-atelier-from-api.js).",
    proposedCodeFix:
      "Patch v15 dataGapsCaveats extractor to include insight.summary slot body and loadWarnings; content can be proposed to insight.summary when blank.",
    writable: true,
    safeToApplyLater: true,
  },
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function short(v, max = 280) {
  const s = nz(v).replace(/\s+/g, " ");
  return s.length > max ? `${s.slice(0, max - 1)}...` : s;
}

function wordCount(text) {
  return nz(text).split(/\s+/).filter(Boolean).length;
}

function escapeFormulaValue(v) {
  return String(v).replace(/'/g, "\\'");
}

function readJsonFromRepo(relPath) {
  const abs = path.join(ROOT, relPath);
  if (!fs.existsSync(abs)) return null;
  try {
    return JSON.parse(fs.readFileSync(abs, "utf8"));
  } catch {
    return null;
  }
}

function listAllReferenceFixtures() {
  const fixturesDir = path.join(ROOT, "fixtures");
  if (!fs.existsSync(fixturesDir)) return [];
  return fs
    .readdirSync(fixturesDir)
    .filter((n) => /^brand-explorer-presentation-.*\.json$/i.test(n))
    .map((n) => `fixtures/${n}`)
    .sort();
}

function fixtureSlotBody(fixturePath, slotKey) {
  const data = readJsonFromRepo(fixturePath);
  const rows = Array.isArray(data?.rows) ? data.rows : Array.isArray(data?.slots) ? data.slots : [];
  const hit = rows.find((r) => nz(r.slotKey) === slotKey);
  return hit ? nz(hit.body) : "";
}

function completedBrandPattern(slotKey) {
  for (const fixture of REFERENCE_FIXTURES) {
    const body = fixtureSlotBody(fixture, slotKey);
    if (body) return { fixture, body: short(body, 320) };
  }
  return { fixture: "", body: "" };
}

/** Joan/ChatGPT pending-founder-review display copy (v16 pre-apply patch). */
const PENDING_FOUNDER_REVIEW_STATUS =
  "AI-drafted / pending founder review; Not company-validated; Not Marriott-validated";

const PENDING_FOUNDER_OWNER_QUESTIONS = [
  "Which elements of the hotel's identity, design, F&B, and local programming can remain unique under Tribute Portfolio?",
  "What brand, systems, quality, and Bonvoy participation requirements are mandatory—and what PIP scope, timing, and cost should be planned before affiliation?",
  "How will conversion timeline and operating disruption affect ramp-year NOI and debt service coverage?",
  "What loyalty economics (Bonvoy contribution, program fees, channel mix) should be modeled versus your comp set and prior independent performance?",
  "Does the asset's market tier support full-service or resort operating complexity—not limited-service reflag economics?",
  "Who leads design narrative, standards compliance, and opening QA—and what third-party management experience exists on comparable Tribute properties?",
  "What exit, re-licensing, or change-of-ownership paths apply if affiliation terms or brand fit changes mid-hold?",
].join("\n");

const PENDING_FOUNDER_COPY = {
  guestValueProposition:
    "Discover independent hotels with distinctive character—each property keeps its local story, design point of view, and sense of place while guests access Marriott Bonvoy benefits and global reservation paths.",
  ownerValueOverview:
    "Guests: Independent hotels with distinctive style, local flavor, and memorable stays, with Bonvoy recognition and Marriott commercial reach.\n\nOwners: Marriott soft-collection affiliation that preserves property identity while adding Bonvoy, distribution, sales support, and operating systems—well suited to conversion of independent, boutique, lifestyle, and leisure assets.\n\nUnderwrite net contribution after franchise fees, loyalty program costs, PIP scope, and channel mix versus your competitive set and operating model.",
  developmentModel:
    "Soft-collection path for independent, boutique, and lifestyle hotels where the asset already has identity worth preserving. Conversion, selective new-build, and repositioning are available where design narrative and full-service or resort complexity fit Marriott collection criteria—confirm development paths, PIP scope, and commercial terms with Marriott development before underwriting.",
  footprintGeoIntro: (regionText) =>
    `Tribute Portfolio operates across Marriott's global regions—${regionText}—with independent boutique, resort, and lifestyle properties where local character and leisure demand support sustained performance. Evaluate fit on market tier, conversion scope, Bonvoy contribution potential, and operating complexity—not prototype-led select-service assumptions.`,
  brandPositioning:
    "Tribute Portfolio is Marriott's soft collection for independent hotels with distinctive style and local flavor—curated properties that keep their individuality while joining Marriott Bonvoy, global distribution, and commercial support.",
  brandCustomerPromise:
    "Independent hotels with distinctive style and local flavor—each stay feels one-of-a-kind, backed by Marriott Bonvoy and the reassurance of a global hospitality platform.",
  conversionScenario3:
    "Historic urban cores, landmark buildings, and architecturally distinctive conversions where sense of place is the product—soft-brand PIP and design narrative support premium full-service positioning when local ADR supports operating complexity. Confirm heritage constraints, conversion timeline, and Marriott development economics before modeling from standardized prototypes.",
  insightSummary:
    "Tribute Portfolio fits when you want Marriott soft-collection affiliation with retained independence, Bonvoy distribution, and a clear independent identity—not a prototype-led limited-service reflag. Best with boutique, lifestyle, or resort assets that can sustain design, F&B, and service investment through PIP and QA cycles. Weaker where market ADR cannot support full-service or resort operating complexity, conversion scope outruns local demand, or franchise and loyalty economics do not clear your hurdle after fees.",
};

function buildContentTargets() {
  return [
    {
      sectionKey: "guestValueProp",
      tab: "Overview",
      section: "Guest value proposition",
      targetType: "brandBasics",
      airtableField: BRAND_BASICS_WRITE_FIELDS.valueProposition,
      normalizedKey: "brandValueProposition",
      minWords: 35,
      allowOverwriteSeverities: ["weak"],
      buildDraft: () => ({
        value: PENDING_FOUNDER_COPY.guestValueProposition,
        sourceBasis:
          "Marriott consumer brand site + Bonvoy relationship themes; AI-drafted / pending founder review",
        reviewStatus: PENDING_FOUNDER_REVIEW_STATUS,
        requiresHumanReviewFlag: true,
      }),
      completedBrandPattern: "Curio Collection by Hilton: guest themes in overview.why_value — independence + Honors + culinary-forward positioning",
    },
    {
      sectionKey: "ownerValueProp",
      tab: "Value to Owners",
      section: "Owner value proposition",
      targetType: "presentation",
      slotKey: "valueOwners.overview",
      sortOrder: 0,
      minWords: 45,
      allowOverwriteSeverities: ["weak"],
      buildDraft: () => ({
        value: PENDING_FOUNDER_COPY.ownerValueOverview,
        sourceBasis:
          "Completed-brand pattern (Curio/Radisson Blu valueOwners.overview); AI-drafted / pending founder review",
        reviewStatus: PENDING_FOUNDER_REVIEW_STATUS,
        requiresHumanReviewFlag: true,
      }),
      completedBrandPattern: completedBrandPattern("valueOwners.overview").body,
    },
    {
      sectionKey: "developmentModel",
      tab: "Overview",
      section: "Development model",
      targetType: "presentation",
      slotKey: "overview.development_model",
      sortOrder: 0,
      minWords: 20,
      allowOverwriteSeverities: ["weak"],
      buildDraft: () => ({
        value: PENDING_FOUNDER_COPY.developmentModel,
        sourceBasis:
          "Marriott development materials (development-model themes); AI-drafted / pending founder review",
        reviewStatus: PENDING_FOUNDER_REVIEW_STATUS,
        requiresHumanReviewFlag: true,
      }),
      completedBrandPattern: completedBrandPattern("overview.development_model").body,
    },
    {
      sectionKey: "regionalRelevance",
      tab: "Footprint & Growth",
      section: "Market / regional relevance",
      targetType: "presentation",
      slotKey: "footprint.geo_intro",
      sortOrder: 10,
      minWords: 50,
      allowOverwriteSeverities: ["weak"],
      buildDraft: ({ tributeBrand }) => {
        const regions = tributeBrand?.regionOffered;
        const regionText = Array.isArray(regions)
          ? regions.map((r) => nz(r)).filter(Boolean).join(", ")
          : nz(regions) || "North America, Caribbean & Latin America, Europe, Middle East & Africa, Asia Pacific";
        return {
          value: PENDING_FOUNDER_COPY.footprintGeoIntro(regionText),
          sourceBasis:
            "Region Offered (Brand Basics) + completed-brand footprint.geo_intro pattern; AI-drafted / pending founder review",
          reviewStatus: PENDING_FOUNDER_REVIEW_STATUS,
          requiresHumanReviewFlag: true,
        };
      },
      completedBrandPattern: completedBrandPattern("footprint.geo_intro").body,
    },
    {
      sectionKey: "brandPositioning",
      tab: "Overview",
      section: "Brand positioning section",
      targetType: "brandBasics",
      airtableField: BRAND_BASICS_WRITE_FIELDS.positioning,
      normalizedKey: "brandPositioning",
      minWords: 22,
      allowOverwriteSeverities: ["minor polish"],
      buildDraft: () => ({
        value: PENDING_FOUNDER_COPY.brandPositioning,
        sourceBasis:
          "Marriott consumer brand positioning themes; AI-drafted / pending founder review",
        reviewStatus: PENDING_FOUNDER_REVIEW_STATUS,
        requiresHumanReviewFlag: true,
      }),
      completedBrandPattern: "Kimpton/Curio: owner-facing positioning with collection + parent context",
    },
    {
      sectionKey: "brandPromise",
      tab: "Overview",
      section: "Brand promise",
      targetType: "brandBasics",
      airtableField: BRAND_BASICS_WRITE_FIELDS.customerPromise,
      normalizedKey: "brandCustomerPromise",
      minWords: 18,
      allowOverwriteSeverities: ["minor polish"],
      buildDraft: () => ({
        value: PENDING_FOUNDER_COPY.brandCustomerPromise,
        sourceBasis:
          "Marriott tagline/promise themes; AI-drafted / pending founder review",
        reviewStatus: PENDING_FOUNDER_REVIEW_STATUS,
        requiresHumanReviewFlag: true,
      }),
      completedBrandPattern: "Completed brands: concise guest promise with loyalty hook",
    },
    {
      sectionKey: "conversionFit",
      tab: "Overview",
      section: "Conversion / adaptive reuse fit",
      targetType: "presentation",
      slotKey: "overview.scenario.3",
      title: "Adaptive Reuse & Heritage Repositioning",
      sortOrder: 0,
      minWords: 40,
      allowOverwriteSeverities: ["minor polish"],
      buildDraft: () => ({
        value: PENDING_FOUNDER_COPY.conversionScenario3,
        sourceBasis:
          "Completed-brand scenario.3 pattern (Curio); AI-drafted / pending founder review",
        reviewStatus: PENDING_FOUNDER_REVIEW_STATUS,
        requiresHumanReviewFlag: true,
      }),
      completedBrandPattern: completedBrandPattern("overview.scenario.3").body,
    },
    {
      sectionKey: "ownerQuestions",
      tab: "Owner Considerations",
      section: "Owner questions",
      targetType: "presentation",
      slotKey: "standards.questions",
      sortOrder: 30,
      minWords: 55,
      allowOverwriteSeverities: ["minor polish"],
      buildDraft: () => ({
        value: PENDING_FOUNDER_OWNER_QUESTIONS,
        sourceBasis:
          "Completed-brand standards.questions pattern; AI-drafted / pending founder review",
        reviewStatus: PENDING_FOUNDER_REVIEW_STATUS,
        requiresHumanReviewFlag: true,
      }),
      completedBrandPattern: "Radisson Blu by Choice: 5–7 diligence questions on standards vintage, PIP, and contractual paths",
    },
    {
      sectionKey: "dataGapsCaveats",
      tab: "Dealality Insight",
      section: "Data gaps / caveats",
      targetType: "presentation",
      slotKey: "insight.summary",
      sortOrder: 0,
      minWords: 40,
      allowOverwriteSeverities: ["not displayable", "missing"],
      buildDraft: () => ({
        value: PENDING_FOUNDER_COPY.insightSummary,
        sourceBasis:
          "Completed-brand insight.summary pattern (Curio fit / weaker-when framing); AI-drafted / pending founder review",
        reviewStatus: PENDING_FOUNDER_REVIEW_STATUS,
        requiresHumanReviewFlag: true,
      }),
      completedBrandPattern: completedBrandPattern("insight.summary").body,
    },
  ];
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

async function fetchAirtableTableSchemas(baseId, apiKey) {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${baseId}/tables`, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Schema fetch failed: ${res.status}`);
  const byName = new Map((json.tables || []).map((t) => [t.name, t]));
  return { tables: json.tables || [], byName };
}

function schemaField(table, fieldName) {
  if (!table) return null;
  return (table.fields || []).find((f) => f.name === fieldName) || null;
}

function normalizePresentationRows(records) {
  return (records || [])
    .map((rec) => {
      const f = rec.fields || {};
      return {
        recordId: rec.id,
        slotKey: nz(f["Slot Key"] || f.slot_key),
        title: nz(f.Title),
        body: nz(f.Body),
        imageAttachmentCount: Array.isArray(f.Image) ? f.Image.length : 0,
      };
    })
    .filter((r) => r.slotKey);
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
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function currentFromApi(brand, target) {
  if (target.targetType === "brandBasics") {
    const key = target.normalizedKey;
    return nz(brand?.[key]);
  }
  const blocks = Array.isArray(brand?.brandExplorer?.blocks) ? brand.brandExplorer.blocks : [];
  const rows = blocks.filter((b) => nz(b?.slotKey) === target.slotKey);
  if (target.title) {
    const row = rows[0];
    return [nz(row?.title), nz(row?.body)].filter(Boolean).join(" | ");
  }
  return rows.map((r) => nz(r.body)).filter(Boolean).join("\n\n");
}

function v15RowBySectionKey(v15, sectionKey) {
  const def = SECTION_DEFS.find((d) => d.key === sectionKey);
  if (!def) return null;
  const rows = v15?.sectionBySectionComparison || [];
  return rows.find((r) => r.key === sectionKey) || rows.find((r) => r.section === def.label) || null;
}

function shouldProposeUpdate({ v15Row, currentValue, target }) {
  const severity = v15Row?.tributeGapSeverity || "";
  if (!target.allowOverwriteSeverities.includes(severity)) return { propose: false, reason: `v15 severity ${severity} excluded` };
  if (!nz(currentValue)) return { propose: true, reason: "blank" };
  if (severity === "weak" && wordCount(currentValue) < target.minWords) {
    return { propose: true, reason: `weak section below ${target.minWords} words` };
  }
  if (severity === "minor polish" && wordCount(currentValue) < target.minWords) {
    return { propose: true, reason: `minor polish below ${target.minWords} words` };
  }
  if (severity === "not displayable" && target.sectionKey === "dataGapsCaveats") {
    return { propose: true, reason: "display path available via insight.summary" };
  }
  return { propose: false, reason: "existing content preserved (comparable or sufficient depth)" };
}

function projectDisplayParityScore(v15, proposedSectionKeys) {
  const rows = v15?.sectionBySectionComparison || [];
  if (!rows.length) return { score: null, comparable: false };
  const improved = new Set(proposedSectionKeys);
  const projected = rows.map((row) => {
    if (!improved.has(row.key)) return row.tributeGapSeverity;
    if (row.tributeGapSeverity === "weak" || row.tributeGapSeverity === "not displayable") return "minor polish";
    if (row.tributeGapSeverity === "minor polish") return "complete";
    return row.tributeGapSeverity;
  });
  const score = Math.round((projected.reduce((sum, sev) => sum + severityScore(sev), 0) / projected.length) * 100);
  const worst = projected.filter((s) => ["missing", "weak", "wrong content model", "not displayable"].includes(s));
  return { score, comparable: score >= 85 && worst.length === 0 };
}

export async function buildBrandExplorerDisplayContentCompletionWriterReport({
  brandKey = DEFAULT_BRAND_KEY,
  brandRecordId = DEFAULT_BRAND_RECORD_ID,
  apply = false,
  applyApproved = false,
  allowHumanReviewCopy = false,
} = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const v15 = readJsonFromRepo(V15_REPORT_PATH);
  if (!v15) throw new Error(`Missing required v15 report: ${V15_REPORT_PATH}`);

  const pilot = BRAND_ASSET_PILOT_CONFIG[brandKey] || BRAND_ASSET_PILOT_CONFIG[DEFAULT_BRAND_KEY];
  const resolvedBrandRecordId = pilot?.recordId || brandRecordId;
  const brandName = pilot?.brandName || "Tribute Portfolio";

  const mode = apply && applyApproved ? "apply" : "dry-run";
  const applyMode = apply && applyApproved;

  const tributeBrand = await fetchBrandApiShape(resolvedBrandRecordId);
  if (!tributeBrand) throw new Error("Unable to read Tribute normalized brand output.");

  const { byName: schemaByName } = await fetchAirtableTableSchemas(baseId, apiKey);
  const presentationTable = schemaByName.get(PRESENTATION_TABLE);
  const basicsTable = schemaByName.get(BRAND_BASICS_TABLE);

  const basics = await airtableFetch(baseId, apiKey, BRAND_BASICS_TABLE, { method: "GET" }, resolvedBrandRecordId);
  if (!basics.res.ok || !basics.json?.id) {
    throw new Error(`Brand Basics record not found: ${resolvedBrandRecordId}`);
  }
  const basicFields = basics.json.fields || {};

  const presentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(resolvedBrandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`
  );
  const presentationRows = normalizePresentationRows(presentationRaw);
  const presentationRowsBySlot = new Map(presentationRows.map((r) => [r.slotKey, r]));

  const contentTargets = buildContentTargets();
  const v15GapKeys = new Set(
    (v15.tributeMissingWeakWrongSections || []).map((r) => {
      const def = SECTION_DEFS.find((d) => d.label === r.section);
      return def?.key || "";
    })
  );

  const excludedSections = [];
  const proposals = [];
  const fieldsLeftUnchanged = [];
  const sourceBackedUpdates = [];
  const aiDraftedUpdates = [];

  for (const target of contentTargets) {
    const v15Row = v15RowBySectionKey(v15, target.sectionKey);
    if (!v15GapKeys.has(target.sectionKey)) {
      excludedSections.push({
        section: target.section,
        sectionKey: target.sectionKey,
        reason: "v15 marked complete — no rewrite",
      });
      continue;
    }

    const currentValue =
      target.targetType === "brandBasics"
        ? nz(basicFields[target.airtableField])
        : presentationRowsBySlot.get(target.slotKey)?.body || "";

    const currentApiValue = currentFromApi(tributeBrand, target);
    const decision = shouldProposeUpdate({
      v15Row,
      currentValue: currentValue || currentApiValue,
      target,
    });

    if (!decision.propose) {
      excludedSections.push({
        section: target.section,
        sectionKey: target.sectionKey,
        reason: decision.reason,
      });
      fieldsLeftUnchanged.push({
        section: target.section,
        slotKey: target.slotKey || "",
        field: target.airtableField || target.normalizedKey,
        reason: decision.reason,
      });
      continue;
    }

    const draft = target.buildDraft({ tributeBrand });
    assertExternalDisplayCopy(draft.value, target.sectionKey);
    const table = target.targetType === "brandBasics" ? BRAND_BASICS_TABLE : PRESENTATION_TABLE;
    const fieldName =
      target.targetType === "brandBasics" ? target.airtableField : PRESENTATION_WRITE_FIELDS.body;
    const tableSchema = target.targetType === "brandBasics" ? basicsTable : presentationTable;
    const fieldSchema = schemaField(tableSchema, fieldName);
    const row = target.slotKey ? presentationRowsBySlot.get(target.slotKey) : null;

    const writable = Boolean(tableSchema && fieldSchema);
    const proposal = {
      tab: target.tab,
      section: target.section,
      sectionKey: target.sectionKey,
      v15Severity: v15Row?.tributeGapSeverity || "",
      currentTributeContent: short(currentValue || currentApiValue, 400),
      completedBrandPattern: target.completedBrandPattern || completedBrandPattern(target.slotKey || "").body,
      proposedTributeContent: draft.value,
      sourceBasis: draft.sourceBasis,
      reviewStatus: draft.reviewStatus,
      airtableTable: table,
      slotKey: target.slotKey || "",
      fieldName,
      writable,
      safeToApplyLater: writable && (!draft.requiresHumanReviewFlag || allowHumanReviewCopy),
      requiresHumanReviewFlag: draft.requiresHumanReviewFlag,
      createRow: target.targetType === "presentation" && !row?.recordId,
      recordId: row?.recordId || (target.targetType === "brandBasics" ? resolvedBrandRecordId : null),
      sortOrder: target.sortOrder ?? 0,
      title: target.title || "",
    };

    proposals.push(proposal);
    if (/source-backed/i.test(draft.sourceBasis) && !draft.requiresHumanReviewFlag) {
      sourceBackedUpdates.push(proposal);
    } else {
      aiDraftedUpdates.push(proposal);
    }
  }

  const preflight = {
    presentationTableWritable: Boolean(presentationTable && schemaField(presentationTable, PRESENTATION_WRITE_FIELDS.body)),
    basicsTableWritable: Boolean(basicsTable && schemaField(basicsTable, BRAND_BASICS_WRITE_FIELDS.valueProposition)),
    targets: proposals.map((p) => ({
      section: p.section,
      table: p.airtableTable,
      fieldName: p.fieldName,
      slotKey: p.slotKey,
      writable: p.writable,
      safeToApplyLater: p.safeToApplyLater,
    })),
    preflightPassed: proposals.filter((p) => p.writable).length === proposals.length,
  };

  const applyBlockers = [];
  if (apply && !applyApproved) applyBlockers.push(`--apply requires ${REQUIRED_APPLY_FLAG}`);
  const blockedHumanReview = proposals.filter((p) => p.requiresHumanReviewFlag && !allowHumanReviewCopy);
  if (apply && blockedHumanReview.length) {
    applyBlockers.push("AI-drafted/human-review copy blocked without --allow-human-review-copy.");
  }
  if (!preflight.preflightPassed) {
    applyBlockers.push("Schema preflight: one or more target fields missing in Airtable.");
  }

  const filteredForApply = proposals.filter((p) => p.writable && (!p.requiresHumanReviewFlag || allowHumanReviewCopy));

  const applyResult = { updated: [], created: [], errors: [], blocked: false };
  if (applyMode && applyBlockers.length === 0) {
    for (const update of filteredForApply) {
      if (update.airtableTable === PRESENTATION_TABLE) {
        const fields = {
          [PRESENTATION_WRITE_FIELDS.slotKey]: update.slotKey,
          [PRESENTATION_WRITE_FIELDS.title]: update.title,
          [PRESENTATION_WRITE_FIELDS.body]: update.proposedTributeContent,
          [PRESENTATION_WRITE_FIELDS.brand]: [resolvedBrandRecordId],
          [PRESENTATION_WRITE_FIELDS.brandName]: brandName,
          [PRESENTATION_WRITE_FIELDS.active]: true,
          [PRESENTATION_WRITE_FIELDS.sortOrder]: update.sortOrder,
        };
        if (update.createRow) {
          const { res, json } = await airtableFetch(baseId, apiKey, PRESENTATION_TABLE, {
            method: "POST",
            body: JSON.stringify({ fields, typecast: true }),
          });
          if (!res.ok) {
            applyResult.errors.push({ section: update.section, message: json.error?.message || res.status });
          } else {
            applyResult.created.push({ table: PRESENTATION_TABLE, recordId: json.id, slotKey: update.slotKey });
          }
        } else if (update.recordId) {
          const { res, json } = await airtableFetch(
            baseId,
            apiKey,
            PRESENTATION_TABLE,
            {
              method: "PATCH",
              body: JSON.stringify({
                fields: { [PRESENTATION_WRITE_FIELDS.body]: update.proposedTributeContent },
                typecast: true,
              }),
            },
            update.recordId
          );
          if (!res.ok) {
            applyResult.errors.push({ section: update.section, message: json.error?.message || res.status });
          } else {
            applyResult.updated.push({ table: PRESENTATION_TABLE, recordId: update.recordId, slotKey: update.slotKey });
          }
        }
      } else if (update.airtableTable === BRAND_BASICS_TABLE && update.recordId) {
        const { res, json } = await airtableFetch(
          baseId,
          apiKey,
          BRAND_BASICS_TABLE,
          {
            method: "PATCH",
            body: JSON.stringify({
              fields: { [update.fieldName]: update.proposedTributeContent },
              typecast: true,
            }),
          },
          update.recordId
        );
        if (!res.ok) {
          applyResult.errors.push({ section: update.section, message: json.error?.message || res.status });
        } else {
          applyResult.updated.push({ table: BRAND_BASICS_TABLE, recordId: update.recordId, field: update.fieldName });
        }
      }
    }
  } else if (applyMode && applyBlockers.length > 0) {
    applyResult.blocked = true;
  }

  const projection = projectDisplayParityScore(v15, proposals.map((p) => p.sectionKey));

  const excludedByGuardrail = [
    { section: "Hero section", reason: "v15 complete — no image/hero changes" },
    { section: "Gallery / visual materials", reason: "images untouched per guardrail" },
    { section: "Source links / PDFs / materials", reason: "sourceLinks untouched per guardrail" },
    { section: "Recent openings / PR", reason: "no source-backed opening/date evidence" },
    { section: "Ideal asset / typical use case", reason: "v15 complete — v11 already promoted" },
    { section: "Brand Standards / Owner Considerations", reason: "v15 complete — v11 already promoted" },
    { section: "Where This Brand Creates the Most Value", reason: "strategic scenarios present; property names in gallery only" },
    { section: "Positioning summary", reason: "mirrors Brand Positioning — updated via positioning target only" },
  ];

  return {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode,
    airtableModified: applyMode && applyResult.updated.length + applyResult.created.length > 0,
    imagesUntouched: true,
    companyValidatedUntouched: true,
    brandWebsiteUntouched: true,
    sourceLinksUntouched: true,
    recentOpeningsRemainsBlank: true,
    filesRead: [
      "AGENTS.md",
      V15_REPORT_PATH,
      "reports/brand-explorer-display-parity-audit.md",
      "lib/partner-intelligence/brand-explorer-display-parity-audit.js",
      "reports/tribute-brand-explorer-content-promotion-writer.md",
      "reports/tribute-existing-brand-field-validation-audit.md",
      "reports/tribute-brand-explorer-content-parity-audit.md",
      "reports/tribute-portfolio-package-pipeline.md",
      "reports/brand-explorer-visual-qa-verification.md",
      "api/brand-library.js",
      "api/lib/partner-intelligence-field-map.js",
      "api/lib/partner-intelligence-explorer-field-registry.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
      "docs/brand-explorer-presentation-slots.md",
      "docs/data-intelligence/BRAND_PROFILE_DATA_MODEL.md",
      "docs/data-intelligence/brand-explorer-display-parity-audit-v15.md",
      ...listAllReferenceFixtures(),
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-display-content-completion-writer.js",
      "scripts/brand-explorer-display-content-completion-writer.mjs",
      "docs/data-intelligence/brand-explorer-display-content-completion-writer-v16.md",
      "reports/brand-explorer-display-content-completion-writer.md",
      "reports/brand-explorer-display-content-completion-writer.json",
      "package.json",
    ],
    v16WriterExists: true,
    brand: { key: brandKey, recordId: resolvedBrandRecordId, name: brandName },
    referenceBrandsInspected: v15.referenceBrandsInspected || [],
    tributeDisplayParityScoreV15: v15.tributeDisplayParityScore ?? 71,
    targetSectionsSelected: proposals.map((p) => p.section),
    sectionsExcluded: [...excludedSections, ...excludedByGuardrail],
    proposedCopyByTabSection: proposals,
    currentTributeCopyByTabSection: proposals.map((p) => ({
      tab: p.tab,
      section: p.section,
      content: p.currentTributeContent,
    })),
    completedBrandPatternsUsed: proposals.map((p) => ({
      section: p.section,
      pattern: p.completedBrandPattern,
    })),
    sourceBackedUpdates,
    aiDraftedHumanReviewUpdates: aiDraftedUpdates,
    displayMappingIssues: DISPLAY_MAPPING_ISSUES,
    airtableTargets: proposals.map((p) => ({
      section: p.section,
      table: p.airtableTable,
      slotKey: p.slotKey,
      fieldName: p.fieldName,
      recordId: p.recordId,
      createRow: p.createRow,
    })),
    writablePreflight: preflight,
    fieldsSlotsProposedForUpdate: proposals.map((p) => (p.slotKey ? `${p.slotKey} → ${p.fieldName}` : p.fieldName)),
    fieldsSlotsLeftUnchanged: fieldsLeftUnchanged,
    applyResult,
    applyBlockers,
    applyFlags: {
      applyRequested: apply,
      applyApproved,
      allowHumanReviewCopy,
      requiredApplyFlag: REQUIRED_APPLY_FLAG,
    },
    expectedDisplayParityScoreAfterApply: projection.score,
    completedBrandComparableAfterApply: projection.comparable,
    exactApplyCommand:
      "npm run brand-explorer-display-content-completion-writer -- --brand tribute-portfolio --apply --approve-brand-explorer-display-content-completion --allow-human-review-copy",
  };
}

export function buildBrandExplorerDisplayContentCompletionWriterMarkdown(report) {
  const lines = [
    `# Brand Explorer Display Content Completion Writer v${report.writerVersion}`,
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`,
    `Brand: ${report.brand?.name} \`${report.brand?.recordId}\``,
    "",
    "## Summary",
    `- v15 display-parity score: **${report.tributeDisplayParityScoreV15}/100**`,
    `- Target sections: **${report.targetSectionsSelected?.length || 0}**`,
    `- Expected score after apply: **${report.expectedDisplayParityScoreAfterApply ?? "n/a"}/100**`,
    `- Completed-brand comparable after apply: **${report.completedBrandComparableAfterApply ? "yes" : "no"}**`,
    "",
    "## Proposed copy by tab/section",
    "",
  ];

  for (const p of report.proposedCopyByTabSection || []) {
    lines.push(`### ${p.tab} — ${p.section}`);
    lines.push(`- v15 severity: ${p.v15Severity}`);
    lines.push(`- Current: ${p.currentTributeContent}`);
    lines.push(`- Pattern: ${short(p.completedBrandPattern, 200)}`);
    lines.push(`- Proposed: ${p.proposedTributeContent}`);
    lines.push(`- Source: ${p.sourceBasis}`);
    lines.push(`- Review: ${p.reviewStatus}`);
    lines.push(`- Target: \`${p.airtableTable}\` · ${p.slotKey || p.fieldName} · writable: ${p.writable}`);
    lines.push("");
  }

  lines.push("## Display mapping issues (not Airtable content)");
  for (const issue of report.displayMappingIssues || []) {
    lines.push(`- **${issue.section}**: ${issue.issue}`);
    lines.push(`  - Code fix: ${issue.proposedCodeFix}`);
  }

  lines.push("");
  lines.push("## Guardrails");
  lines.push(`- Images untouched: **${report.imagesUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(`- Recent openings blank: **${report.recentOpeningsRemainsBlank ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Apply command (if approved)");
  lines.push("```bash");
  lines.push(report.exactApplyCommand || "");
  lines.push("```");
  return lines.join("\n");
}
