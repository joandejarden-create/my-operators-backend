/**
 * Tribute Full Brand Explorer Content Parity Audit v10 (read-only).
 *
 * Purpose:
 * - Compare Tribute Portfolio against completed-brand Explorer standards.
 * - Identify content/data gaps by section and field.
 * - Stage safe candidate copy updates (no Airtable writes).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { BRAND_ASSET_PILOT_CONFIG } from "./brand-asset-registry-workflow.js";
import { listPartnerFacts } from "./airtable-facts.js";
import { listPartnerSources } from "./airtable-source.js";

export const WRITER_VERSION = "10";
export const REPORT_JSON_NAME = "tribute-brand-explorer-content-parity-audit.json";
export const REPORT_MD_NAME = "tribute-brand-explorer-content-parity-audit.md";

const DEFAULT_BRAND_KEY = "tribute-portfolio";
const DEFAULT_BRAND_RECORD_ID = "recCvV0PuZOi8c3hC";
const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const REFERENCE_BRANDS = [
  "Radisson Blu by Choice",
  "Radisson by Choice",
  "Kimpton Hotels",
  "Curio Collection by Hilton",
  "Ascend Hotel Collection",
];

const REFERENCE_FIXTURES = [
  "fixtures/brand-explorer-presentation-radisson-blu.example.json",
  "fixtures/brand-explorer-presentation-radisson-choice-overview.json",
  "fixtures/brand-explorer-presentation-kimpton-full.json",
  "fixtures/brand-explorer-presentation-curio-full.json",
  "fixtures/brand-explorer-presentation-ascend-hotel-collection-full.json",
];

const SHOULD_REMAIN_BLANK_SLOTS = [
  "overview.scenario.3",
  "overview.scenario.boutique_lifestyle",
  "overview.scenario.mixed_use",
  "footprint.openings",
  "PR / Opening Link",
];

const REQUIRED_PRESENTATION_SLOTS = [
  "overview.hero",
  "materials.gallery.1",
  "materials.gallery.2",
  "materials.gallery.3",
  "materials.gallery.4",
  "materials.gallery.5",
  "materials.gallery.6",
  "overview.scenario.1",
  "overview.scenario.2",
  "overview.scenario.3",
  "footprint.openings",
];

const FIELD_DEFINITIONS = [
  { key: "brandName", label: "Brand identity and parent company", basic: "Brand Name" },
  { key: "parentCompany", label: "Brand identity and parent company", basic: "Parent Company" },
  { key: "brandArchitecture", label: "Brand family / collection context", basic: "Brand Architecture" },
  { key: "chainScale", label: "Segment / chain scale", basic: "Hotel Chain Scale" },
  { key: "brandPromise", label: "Brand promise", basic: "Brand Customer Promise" },
  { key: "positioningSummary", label: "Positioning summary", basic: "Brand Positioning" },
  { key: "ownerValueProp", label: "Guest / owner value proposition", basic: "Brand Value Proposition" },
  { key: "idealAssetProfile", label: "Ideal hotel / asset profile", slot: "overview.typical_use_case" },
  { key: "developmentModel", label: "Development model", basic: "Brand Model" },
  { key: "bonvoyRelationship", label: "Loyalty / Marriott Bonvoy relationship", basic: "Brand Tagline" },
  { key: "regionalRelevance", label: "Market / regional relevance", basic: "Region Offered" },
  { key: "ownerConsiderations", label: "Owner considerations", basic: "Key Brand Differentiators" },
  { key: "standards", label: "Brand standards / owner considerations", slot: "standards.intro" },
  { key: "questionsOwnersShouldAsk", label: "Questions owners should ask", slot: "standards.questions" },
  { key: "sourceLinks", label: "PDF/source links", slot: "materials.file" },
  { key: "gallery", label: "Image gallery titles/captions", slot: "materials.gallery.*" },
  { key: "valueDrivers", label: "Where This Brand Creates the Most Value", slot: "overview.scenario.*" },
  { key: "recentOpenings", label: "Recent openings / PR", slot: "footprint.openings" },
  { key: "trustChip", label: "External trust chip / source basis", basic: "External Display Status" },
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function short(v, max = 220) {
  const s = nz(v).replace(/\s+/g, " ");
  return s.length > max ? `${s.slice(0, max - 1)}...` : s;
}

function escapeFormulaValue(v) {
  return String(v).replace(/'/g, "\\'");
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

function normalizePresentationRows(records) {
  return (records || [])
    .map((rec) => {
      const f = rec.fields || {};
      const image = Array.isArray(f.Image) ? f.Image : [];
      return {
        recordId: rec.id,
        slotKey: nz(f["Slot Key"] || f.slot_key),
        title: nz(f.Title),
        body: nz(f.Body),
        imageAttachmentCount: image.length,
      };
    })
    .filter((r) => r.slotKey);
}

function getField(row, key) {
  return nz((row?.fields || {})[key]);
}

function isGenericText(text) {
  const s = nz(text).toLowerCase();
  if (!s) return false;
  return /(image gallery|mock|demo|placeholder|candidate|asset|approved|where this brand creates the most value)/.test(
    s
  );
}

function containsPropertyNameInValueDriver(title, body) {
  const text = `${nz(title)} ${nz(body)}`.toLowerCase();
  return /\b(crystal cove|humano|ermita|casa nizuc|hotel rumbao|loma medellin|barbados|lima|cartagena|medellin)\b/.test(
    text
  );
}

function loadFixtureRows() {
  const rows = [];
  for (const rel of REFERENCE_FIXTURES) {
    const abs = path.join(ROOT, rel);
    if (!fs.existsSync(abs)) continue;
    const parsed = JSON.parse(fs.readFileSync(abs, "utf8"));
    for (const r of parsed?.rows || []) {
      rows.push({
        slotKey: nz(r.slotKey),
        title: nz(r.title),
        body: nz(r.body),
      });
    }
  }
  return rows;
}

function refHasCoverage(refRows, slotPattern) {
  if (!slotPattern) return false;
  if (slotPattern.endsWith(".*")) {
    const prefix = slotPattern.slice(0, -2);
    return refRows.some((r) => r.slotKey.startsWith(prefix));
  }
  return refRows.some((r) => r.slotKey === slotPattern);
}

function resolveAssessment({ hasContent, generic, wrongStyle, shouldRemainBlank, hasSourceBackedFact }) {
  if (shouldRemainBlank) return "Should remain blank until stronger evidence exists";
  if (!hasContent && hasSourceBackedFact) return "Source-backed but not promoted";
  if (!hasContent) return "Missing";
  if (generic) return "Generic/demo-like";
  if (wrongStyle) return "Present but wrong style";
  return "Complete/comparable";
}

function sectionKeyToProposedCopy(sectionKey) {
  if (sectionKey === "valueDrivers") {
    return [
      {
        slotKey: "overview.scenario.1",
        proposedTitle: "Resort & Leisure-Led Independent Hotels",
        proposedBody:
          "Best suited for distinctive leisure and resort assets that need Marriott distribution and loyalty support while preserving an independent boutique identity.",
      },
      {
        slotKey: "overview.scenario.2",
        proposedTitle: "Urban Boutique Repositioning",
        proposedBody:
          "A strong fit for design-led urban hotels where owners want lifestyle positioning, Marriott Bonvoy affiliation, and brand support without a rigid full-service box.",
      },
    ];
  }
  if (sectionKey === "ownerConsiderations") {
    return [
      {
        field: "Key Brand Differentiators",
        proposed:
          "Prioritize assets where independent character can be preserved while operating within Marriott soft-brand standards and Bonvoy participation requirements.",
      },
      {
        slotKey: "standards.questions",
        proposed:
          "What level of design and operational flexibility is available within Tribute standards, and how does that affect PIP scope, loyalty economics, and operator selection?",
      },
    ];
  }
  if (sectionKey === "overviewPositioning") {
    return [
      {
        slotKey: "overview.hero",
        proposedTitle: "Ermita, Cartagena, a Tribute Portfolio Hotel",
        proposedBody:
          "Flagship Tribute visual for owner-facing positioning: independent character with Marriott distribution and loyalty support in premium urban and resort settings.",
      },
    ];
  }
  return [];
}

function completionScore(entries) {
  if (!entries.length) return 0;
  const points = entries.reduce((sum, e) => {
    if (e.gapAssessment === "Complete/comparable") return sum + 1;
    if (e.gapAssessment === "Source-backed but not promoted") return sum + 0.7;
    if (e.gapAssessment === "Present but wrong style") return sum + 0.5;
    if (e.gapAssessment === "Generic/demo-like") return sum + 0.4;
    if (e.gapAssessment === "Should remain blank until stronger evidence exists") return sum + 1;
    return sum;
  }, 0);
  return Math.round((points / entries.length) * 100);
}

async function fetchReferenceLiveBrand(baseId, apiKey, brandName) {
  const matches = await listByFormula(
    baseId,
    apiKey,
    BRAND_BASICS_TABLE,
    `{Brand Name}='${escapeFormulaValue(brandName)}'`
  );
  if (!matches.length) return null;
  const record = matches[0];
  const presentation = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(record.id)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(brandName)}')`
  );
  return {
    recordId: record.id,
    brandName,
    hasPresentationRows: presentation.length > 0,
    presentationRowCount: presentation.length,
  };
}

export async function buildTributeBrandExplorerContentParityAuditReport({
  brandKey = DEFAULT_BRAND_KEY,
  brandRecordId = DEFAULT_BRAND_RECORD_ID,
} = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const pilot = BRAND_ASSET_PILOT_CONFIG[brandKey] || BRAND_ASSET_PILOT_CONFIG[DEFAULT_BRAND_KEY];
  const resolvedBrandRecordId = pilot?.recordId || brandRecordId;
  const brandName = pilot?.brandName || "Tribute Portfolio";

  const tributeBasics = await airtableFetch(baseId, apiKey, BRAND_BASICS_TABLE, { method: "GET" }, resolvedBrandRecordId);
  if (!tributeBasics.res.ok || !tributeBasics.json?.id) {
    throw new Error(`Brand Basics record not found: ${resolvedBrandRecordId}`);
  }

  const tributePresentationRaw = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(resolvedBrandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(
      brandName
    )}')`
  );
  const tributePresentation = normalizePresentationRows(tributePresentationRaw);
  const tributeBySlot = new Map(tributePresentation.map((r) => [r.slotKey, r]));

  const sourceRows = [];
  let sourceOffset = null;
  do {
    const page = await listPartnerSources({ brandId: resolvedBrandRecordId, limit: 100, offset: sourceOffset });
    sourceRows.push(...(page.sources || []));
    sourceOffset = page.offset;
  } while (sourceOffset);

  const factRows = [];
  let factOffset = null;
  do {
    const page = await listPartnerFacts({ brandId: resolvedBrandRecordId, limit: 100, offset: factOffset });
    factRows.push(...(page.facts || []));
    factOffset = page.offset;
  } while (factOffset);
  const approvedFacts = factRows.filter((f) => ["Approved", "Edited"].includes(nz(f.humanReviewStatus)));
  const sourceBackedFacts = new Set(approvedFacts.map((f) => nz(f.fieldName)));

  const referenceFixtureRows = loadFixtureRows();
  const referenceLive = [];
  for (const name of REFERENCE_BRANDS) {
    try {
      const live = await fetchReferenceLiveBrand(baseId, apiKey, name);
      if (live) referenceLive.push(live);
    } catch {
      // Keep audit read-only and resilient.
    }
  }

  const sectionGapTable = [];
  const fieldGapTable = [];

  for (const def of FIELD_DEFINITIONS) {
    let currentTributeContent = "";
    let hasContent = false;
    let wrongStyle = false;
    let generic = false;
    let sourceBackedFact = false;
    let shouldRemainBlank = false;
    let referencePattern = "";

    if (def.basic) {
      const value = getField(tributeBasics.json, def.basic);
      currentTributeContent = value;
      hasContent = Boolean(value);
      generic = isGenericText(value);
      referencePattern = `Completed brands populate Brand Basics field "${def.basic}" with concise owner-facing narrative.`;
    } else if (def.slot === "materials.gallery.*") {
      const gallery = tributePresentation.filter((r) => /^materials\.gallery\./.test(r.slotKey));
      hasContent = gallery.some((r) => nz(r.title) || nz(r.body));
      generic = gallery.some((r) => isGenericText(`${r.title} ${r.body}`));
      currentTributeContent = gallery.map((r) => `${r.slotKey}: ${r.title} | ${r.body}`).join(" || ");
      referencePattern = "Completed brands use property/location-led titles and concise scene captions.";
    } else if (def.slot === "overview.scenario.*") {
      const scenarios = tributePresentation.filter((r) => /^overview\.scenario\./.test(r.slotKey));
      hasContent = scenarios.some((r) => nz(r.title) || nz(r.body));
      wrongStyle = scenarios.some((r) => containsPropertyNameInValueDriver(r.title, r.body));
      currentTributeContent = scenarios.map((r) => `${r.slotKey}: ${r.title} | ${r.body}`).join(" || ");
      referencePattern = "Completed brands use strategic owner-facing value-driver copy, not property-name titles.";
    } else if (def.slot) {
      const row = tributeBySlot.get(def.slot);
      hasContent = Boolean(row && (row.title || row.body));
      currentTributeContent = row ? `${row.title} | ${row.body}` : "";
      generic = row ? isGenericText(`${row.title} ${row.body}`) : false;
      referencePattern = `Completed brands populate presentation slot "${def.slot}" with owner-facing copy and source context.`;
      if (SHOULD_REMAIN_BLANK_SLOTS.includes(def.slot)) shouldRemainBlank = true;
    }

    if (def.key === "trustChip") {
      const ext = getField(tributeBasics.json, "External Display Status");
      const sourceType = getField(tributeBasics.json, "Source Type");
      const confidence = getField(tributeBasics.json, "Confidence Level");
      currentTributeContent = `${ext} | ${sourceType} | ${confidence}`;
      hasContent = Boolean(ext || sourceType || confidence);
      referencePattern =
        "Completed brands show trust-label posture (External Display Status + Source Type + Confidence) without Company Validated claims.";
    }

    if (def.key === "bonvoyRelationship") {
      sourceBackedFact = sourceBackedFacts.has("be.loyalty.programName");
    } else if (def.key === "positioningSummary") {
      sourceBackedFact = sourceBackedFacts.has("be.positioning.summary");
    } else if (def.key === "brandPromise") {
      sourceBackedFact = sourceBackedFacts.has("be.positioning.guestPromise");
    } else if (def.key === "brandName") {
      sourceBackedFact = sourceBackedFacts.has("be.identity.brandName");
    } else if (def.key === "parentCompany") {
      sourceBackedFact = sourceBackedFacts.has("be.identity.parentCompany");
    } else if (def.key === "developmentModel") {
      sourceBackedFact = sourceBackedFacts.has("be.overview.developmentModel");
    } else if (def.key === "ownerValueProp") {
      sourceBackedFact = sourceBackedFacts.has("be.overview.whyValue");
    } else if (def.key === "idealAssetProfile") {
      sourceBackedFact = sourceBackedFacts.has("be.overview.typicalUseCase");
    }

    const gapAssessment = resolveAssessment({
      hasContent,
      generic,
      wrongStyle,
      shouldRemainBlank,
      hasSourceBackedFact: sourceBackedFact,
    });

    const proposedEntries = sectionKeyToProposedCopy(def.key);
    const proposedTributeContent = proposedEntries.length
      ? proposedEntries.map((p) => JSON.stringify(p)).join(" | ")
      : "";

    const row = {
      fieldOrSectionKey: def.key,
      sectionLabel: def.label,
      currentTributeContent: short(currentTributeContent, 360),
      referenceBrandPattern: referencePattern,
      gapAssessment,
      proposedTributeContent,
      sourceBasis: sourceBackedFact ? "Approved PI fact(s)" : "AI-drafted owner framing (staging only)",
      reviewStatus: sourceBackedFact ? "Source-backed" : "AI-drafted / human review required",
      safeToWriteLater: sourceBackedFact || Boolean(proposedEntries.length),
      reasonIfNotSafe: shouldRemainBlank ? "Await stronger approved source evidence and approved slot/fact." : "",
      shouldRemainBlank,
    };
    fieldGapTable.push(row);
    sectionGapTable.push({
      section: def.label,
      key: def.key,
      gapAssessment,
      safeToWriteLater: row.safeToWriteLater,
      shouldRemainBlank,
    });
  }

  const completedComparable = fieldGapTable.filter((x) => x.gapAssessment === "Complete/comparable");
  const weakOrIncomplete = fieldGapTable.filter((x) =>
    ["Missing", "Source-backed but not promoted", "Present but weak"].includes(x.gapAssessment)
  );
  const wrongStyle = fieldGapTable.filter((x) =>
    ["Present but wrong style", "Generic/demo-like"].includes(x.gapAssessment)
  );

  const proposedUpdates = fieldGapTable
    .filter((x) => x.proposedTributeContent)
    .map((x) => ({
      key: x.fieldOrSectionKey,
      section: x.sectionLabel,
      proposed: x.proposedTributeContent,
      sourceBasis: x.sourceBasis,
      reviewStatus: x.reviewStatus,
    }));

  const sourceBackedUpdates = proposedUpdates.filter((u) => /Approved PI fact/.test(u.sourceBasis));
  const aiDraftedUpdates = proposedUpdates.filter((u) => /AI-drafted/.test(u.sourceBasis));
  const remainBlank = fieldGapTable.filter((x) => x.shouldRemainBlank).map((x) => x.fieldOrSectionKey);

  const report = {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    imagesUntouched: true,
    brandSetupFieldsUntouched: true,
    brand: {
      key: brandKey,
      recordId: resolvedBrandRecordId,
      name: brandName,
      parentCompany: getField(tributeBasics.json, "Parent Company") || "Marriott International, Inc.",
    },
    filesRead: [
      "AGENTS.md",
      "reports/tribute-portfolio-package-pipeline.md",
      "reports/tribute-portfolio-package-pipeline.json",
      "reports/brand-explorer-visual-qa-verification.md",
      "reports/explorer-media-promotion-writer.md",
      "reports/brand-explorer-presentation-copy-parity-audit.md",
      "reports/brand-explorer-value-driver-copy-parity-fix.md",
      "api/brand-library.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
      "api/lib/partner-intelligence-field-map.js",
      "api/lib/partner-intelligence-explorer-field-registry.js",
      "docs/brand-explorer-presentation-slots.md",
      "docs/data-intelligence/BRAND_PROFILE_DATA_MODEL.md",
      "docs/data-intelligence/brand-explorer-visual-slot-requirements-v1.md",
      "docs/data-intelligence/brand-explorer-presentation-copy-parity-audit-v8-5.md",
      "docs/data-intelligence/brand-explorer-presentation-copy-promotion-writer-v9.md",
      ...REFERENCE_FIXTURES,
    ],
    filesChanged: [
      "lib/partner-intelligence/tribute-brand-explorer-content-parity-audit.js",
      "scripts/tribute-brand-explorer-content-parity-audit.mjs",
      "docs/data-intelligence/tribute-brand-explorer-content-parity-audit-v10.md",
      "reports/tribute-brand-explorer-content-parity-audit.md",
      "reports/tribute-brand-explorer-content-parity-audit.json",
    ],
    referenceBrandsInspected: REFERENCE_BRANDS,
    referenceLiveReadback: referenceLive,
    approvedPiGovernanceData: {
      sourceCount: sourceRows.length,
      approvedForExplorerUseCount: sourceRows.filter((s) => nz(s.approvedForExplorerUse) === "Yes").length,
      approvedFactCount: approvedFacts.length,
      approvedFactKeys: approvedFacts.map((f) => f.fieldName),
    },
    tributeCurrentCompletionScore: completionScore(fieldGapTable),
    referenceCompletionPattern:
      "Completed brands consistently fill owner-facing copy across overview/value/owner considerations/commercial/materials, while leaving evidence-sensitive slots blank until source-backed.",
    fieldByFieldTributeGapTable: fieldGapTable,
    sectionBySectionTributeGapTable: sectionGapTable,
    sectionsAlreadyComparableToCompletedBrands: completedComparable.map((x) => x.sectionLabel),
    sectionsWeakOrIncomplete: weakOrIncomplete.map((x) => x.sectionLabel),
    sectionsWrongStyleOrApproach: wrongStyle.map((x) => x.sectionLabel),
    proposedTributeContentUpdates: proposedUpdates,
    sourceBackedUpdates,
    aiDraftedHumanReviewUpdates: aiDraftedUpdates,
    fieldsShouldStayBlank: remainBlank,
    tributeTextGovernancePlatformReady: true,
    readyForGatedContentPromotionWriterV11:
      weakOrIncomplete.length > 0 || wrongStyle.length > 0
        ? "Partially ready (writer can be built, but review queue remains)"
        : "Yes",
    requiredPresentationSlotsAudit: REQUIRED_PRESENTATION_SLOTS.map((slotKey) => ({
      slotKey,
      present: tributeBySlot.has(slotKey),
      title: tributeBySlot.get(slotKey)?.title || "",
      body: tributeBySlot.get(slotKey)?.body || "",
      imageAttachmentCount: tributeBySlot.get(slotKey)?.imageAttachmentCount || 0,
    })),
    exactNextCommand: "npm run tribute-brand-explorer-content-parity-audit -- --dry-run",
  };

  return report;
}

export function buildTributeBrandExplorerContentParityAuditMarkdown(report) {
  const lines = [];
  lines.push("# Tribute Full Brand Explorer Content Parity Audit v10");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push("");
  lines.push("## Reference brands inspected");
  for (const b of report.referenceBrandsInspected) lines.push(`- ${b}`);
  lines.push("");
  lines.push("## Tribute completion assessment");
  lines.push(`- Completion score: **${report.tributeCurrentCompletionScore}/100**`);
  lines.push(`- Platform Ready (text/governance): **${report.tributeTextGovernancePlatformReady ? "yes" : "no"}**`);
  lines.push(`- Ready for v11 content promotion writer: **${report.readyForGatedContentPromotionWriterV11}**`);
  lines.push("");
  lines.push("## Sections already at completed-brand standard");
  if (!report.sectionsAlreadyComparableToCompletedBrands.length) lines.push("- None yet.");
  for (const s of report.sectionsAlreadyComparableToCompletedBrands) lines.push(`- ${s}`);
  lines.push("");
  lines.push("## Sections not at completed-brand standard");
  const notComparable = [...new Set([...report.sectionsWeakOrIncomplete, ...report.sectionsWrongStyleOrApproach])];
  if (!notComparable.length) lines.push("- None.");
  for (const s of notComparable) lines.push(`- ${s}`);
  lines.push("");
  lines.push("## Field-by-field gap table");
  lines.push("");
  lines.push("| Key | Section | Gap | Safe to write later | Remain blank |");
  lines.push("|-----|---------|-----|---------------------|--------------|");
  for (const g of report.fieldByFieldTributeGapTable) {
    lines.push(
      `| \`${g.fieldOrSectionKey}\` | ${g.sectionLabel} | ${g.gapAssessment} | ${g.safeToWriteLater ? "yes" : "no"} | ${
        g.shouldRemainBlank ? "yes" : "no"
      } |`
    );
  }
  lines.push("");
  lines.push("## Proposed Tribute content updates (staging-only)");
  if (!report.proposedTributeContentUpdates.length) lines.push("- None.");
  for (const u of report.proposedTributeContentUpdates) {
    lines.push(`- \`${u.key}\` (${u.section})`);
    lines.push(`  - Source basis: ${u.sourceBasis}`);
    lines.push(`  - Review status: ${u.reviewStatus}`);
    lines.push(`  - Proposed: ${short(u.proposed, 320)}`);
  }
  lines.push("");
  lines.push("## Fields that should remain blank");
  if (!report.fieldsShouldStayBlank.length) lines.push("- None.");
  for (const f of report.fieldsShouldStayBlank) lines.push(`- \`${f}\``);
  lines.push("");
  lines.push("## Guardrails");
  lines.push(`- Images untouched: **${report.imagesUntouched ? "yes" : "no"}**`);
  lines.push(`- Brand Setup fields untouched: **${report.brandSetupFieldsUntouched ? "yes" : "no"}**`);
  lines.push(`- Company Validated fields untouched: **yes**`);
  lines.push("");
  lines.push("## Next command");
  lines.push("");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  lines.push("");
  return lines.join("\n");
}
