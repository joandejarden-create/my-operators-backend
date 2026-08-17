/**
 * Brand Explorer Presentation Copy Parity Audit v8.5 (read-only).
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { BRAND_ASSET_PILOT_CONFIG } from "./brand-asset-registry-workflow.js";

export const WRITER_VERSION = "8.5";
export const REPORT_JSON_NAME = "brand-explorer-presentation-copy-parity-audit.json";
export const REPORT_MD_NAME = "brand-explorer-presentation-copy-parity-audit.md";

const DEFAULT_BRAND_KEY = "tribute-portfolio";
const DEFAULT_BRAND_RECORD_ID = "recCvV0PuZOi8c3hC";
const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const PROMOTED_SLOTS = [
  "overview.hero",
  "materials.gallery.1",
  "materials.gallery.2",
  "materials.gallery.4",
  "materials.gallery.5",
  "materials.gallery.6",
  "overview.scenario.1",
  "overview.scenario.2",
];
const INTENTIONALLY_MISSING_SLOTS = [
  "materials.gallery.3",
  "overview.scenario.3",
  "footprint.openings",
  "overview.scenario.boutique_lifestyle",
  "overview.scenario.mixed_use",
  "PR / Opening Link",
];
const REFERENCE_FIXTURES = [
  {
    profile: "Radisson Blu by Choice",
    path: "fixtures/brand-explorer-presentation-radisson-blu.example.json",
  },
  {
    profile: "Radisson by Choice",
    path: "fixtures/brand-explorer-presentation-radisson-choice-overview.json",
  },
  {
    profile: "Kimpton Hotels",
    path: "fixtures/brand-explorer-presentation-kimpton-full.json",
  },
  {
    profile: "Curio Collection by Hilton",
    path: "fixtures/brand-explorer-presentation-curio-full.json",
  },
  {
    profile: "Ascend Hotel Collection",
    path: "fixtures/brand-explorer-presentation-ascend-hotel-collection-full.json",
  },
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
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
    params.set("filterByFormula", formula);
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
function isTitleCaseLike(text) {
  const s = nz(text);
  if (!s) return false;
  const tokens = s.split(/\s+/).filter(Boolean);
  const significant = tokens.filter((t) => /[A-Za-z]/.test(t));
  if (!significant.length) return false;
  const pass = significant.filter((t) => /^[A-Z0-9]/.test(t) || /^[A-Z][a-z]/.test(t));
  return pass.length / significant.length >= 0.6;
}
function hasGenericDemoTone(text) {
  const s = nz(text).toLowerCase();
  return /(mock|demo|placeholder|sample|image gallery \d|\(gallery\)|where this brand creates the most value|brand setup)/.test(
    s
  );
}
function ownerFacingPattern(section) {
  if (section === "hero") return "Property-led headline; one-sentence positioning caption with owner-use context.";
  if (section === "gallery") return "Title = property/location in title case; body = short scene-use caption (<=120 chars).";
  if (section === "value-driver")
    return "Title = strategic owner-facing use case (no property names); body = concise brand-level fit statement (<=180 chars).";
  if (section === "standards")
    return "Structured owner-facing requirement copy (typical consideration, owner planning, status, notes).";
  if (section === "openings-pr")
    return "Opening-oriented proof copy with date/source context; omit if no source-backed opening evidence.";
  return "Owner-facing concise copy with source-backed context.";
}
function readJsonFixture(relPath) {
  const abs = path.join(ROOT, relPath);
  if (!fs.existsSync(abs)) return null;
  try {
    return JSON.parse(fs.readFileSync(abs, "utf8"));
  } catch {
    return null;
  }
}
function normalizeReferenceRows(profile, fixtureObj) {
  const rows = Array.isArray(fixtureObj?.rows) ? fixtureObj.rows : [];
  return rows.map((r) => ({
    profile,
    slotKey: nz(r.slotKey),
    title: nz(r.title),
    body: nz(r.body),
    sort: Number.isFinite(r.sort) ? r.sort : 0,
  }));
}
function normalizePresentationRows(records) {
  return (records || [])
    .map((rec) => {
      const f = rec.fields || {};
      const slotKey = nz(f["Slot Key"] || f.slot_key);
      return {
        recordId: rec.id,
        slotKey,
        title: nz(f.Title),
        body: nz(f.Body),
      };
    })
    .filter((r) => r.slotKey);
}
function slotSection(slotKey) {
  if (slotKey === "overview.hero") return "hero";
  if (/^materials\.gallery\./.test(slotKey)) return "gallery";
  if (/^overview\.scenario\./.test(slotKey)) return "value-driver";
  if (/^standards\./.test(slotKey) || /^operations\.standards/.test(slotKey)) return "standards";
  if (slotKey === "footprint.openings" || /PR/.test(slotKey)) return "openings-pr";
  return "other";
}
function sectionFromSlotForAudit(slotKey) {
  if (slotKey === "overview.hero") return "Hero";
  if (/^materials\.gallery\./.test(slotKey)) return "Image Gallery";
  if (/^overview\.scenario\./.test(slotKey)) return "Where This Brand Creates the Most Value";
  if (slotKey === "footprint.openings") return "Recent Openings / PR";
  return "Brand Standards / Owner Considerations";
}
function valueDriverLabelForSlot(slotKey) {
  if (slotKey.endsWith(".1")) return "Resort";
  if (slotKey.endsWith(".2")) return "Urban";
  if (slotKey.endsWith(".3")) return "Conversion / Adaptive Reuse";
  if (slotKey.endsWith(".boutique_lifestyle")) return "Boutique / Lifestyle";
  if (slotKey.endsWith(".mixed_use")) return "Mixed-Use";
  return "Conversion / Adaptive Reuse";
}
function escapeRegex(text) {
  return String(text).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
function buildIdempotentValueDriverTitle(label, currentTitle, fallbackTitle) {
  const cleanCurrent = nz(currentTitle);
  const cleanFallback = nz(fallbackTitle);
  const canonicalPrefix = `${label} Value Driver — `;
  const canonicalPrefixRegex = new RegExp(`^${escapeRegex(canonicalPrefix)}`, "i");

  if (canonicalPrefixRegex.test(cleanCurrent)) {
    const stripped = cleanCurrent.replace(canonicalPrefixRegex, "").trim();
    return `${canonicalPrefix}${stripped || cleanFallback || "Tribute Portfolio"}`;
  }

  const genericValueDriverPrefixRegex = /^(?:resort|urban|conversion(?:\s*\/\s*adaptive reuse)?|boutique(?:\s*\/\s*lifestyle)?|mixed-use)\s+value\s+driver\s+[—-]\s*/i;
  const strippedGeneric = cleanCurrent.replace(genericValueDriverPrefixRegex, "").trim();
  const baseTitle = strippedGeneric || cleanCurrent || cleanFallback || "Tribute Portfolio";
  return `${canonicalPrefix}${baseTitle}`;
}
function buildProposedCopy(slotKey, currentTitle, currentBody) {
  const fallbackTitle = nz(currentTitle) || "Tribute Portfolio";
  const fallbackBody = nz(currentBody);
  if (slotKey === "overview.hero") {
    return {
      proposedTitle: fallbackTitle,
      proposedBody:
        "Flagship Tribute property visual representing design-led upper-upscale positioning in CALA-relevant gateway or resort contexts.",
      sourceBasis: "source-backed-property-context",
      reviewStatus: "Human review required before promotion",
      safeToWriteLater: true,
      copyType: "source-backed copy",
    };
  }
  if (/^materials\.gallery\./.test(slotKey)) {
    const compact =
      fallbackBody && !hasGenericDemoTone(fallbackBody)
        ? fallbackBody
        : "Representative property scene supporting Tribute Portfolio's design-forward, locally rooted guest experience.";
    return {
      proposedTitle: fallbackTitle,
      proposedBody: compact.slice(0, 130),
      sourceBasis: "source-backed-property-and-scene",
      reviewStatus: "Human review required before promotion",
      safeToWriteLater: true,
      copyType: "source-backed copy",
    };
  }
  if (/^overview\.scenario\./.test(slotKey)) {
    const label = valueDriverLabelForSlot(slotKey);
    const strategicTitles = {
      Resort: "Resort & Leisure-Led Independent Hotels",
      Urban: "Urban Boutique Repositioning",
      "Conversion / Adaptive Reuse": "Conversion & Adaptive Reuse Opportunities",
      "Boutique / Lifestyle": "Boutique / Lifestyle Opportunities",
      "Mixed-Use": "Mixed-Use Hospitality Opportunities",
    };
    const proposedTitle = strategicTitles[label] || `${label} Opportunities`;
    return {
      proposedTitle,
      proposedBody:
        label === "Resort"
          ? "Best suited for distinctive leisure and resort assets that need Marriott distribution and loyalty support while preserving an independent boutique identity."
          : label === "Urban"
            ? "A strong fit for design-led urban hotels where owners want lifestyle positioning, Marriott Bonvoy affiliation, and brand support without a rigid full-service box."
            : "Leave blank until approved conversion-specific visual and source-backed context are available.",
      sourceBasis:
        label === "Conversion / Adaptive Reuse" || label === "Boutique / Lifestyle" || label === "Mixed-Use"
          ? "insufficient-evidence"
          : "source-backed-brand-context + AI owner framing",
      reviewStatus:
        label === "Conversion / Adaptive Reuse" || label === "Boutique / Lifestyle" || label === "Mixed-Use"
          ? "Hold blank pending stronger evidence"
          : "Human review required before promotion",
      safeToWriteLater: label === "Resort" || label === "Urban",
      copyType:
        label === "Conversion / Adaptive Reuse" || label === "Boutique / Lifestyle" || label === "Mixed-Use"
          ? "fields that should stay blank"
          : "AI-drafted owner-facing copy",
    };
  }
  return {
    proposedTitle: fallbackTitle,
    proposedBody: fallbackBody || "",
    sourceBasis: "unknown",
    reviewStatus: "Needs human review",
    safeToWriteLater: false,
    copyType: "human-review copy",
  };
}
function hasPropertySpecificValueDriverCopy(title, body) {
  const text = `${nz(title)} ${nz(body)}`.toLowerCase();
  if (!text) return false;
  if (/\ba tribute portfolio hotel\b/.test(text)) return true;
  if (/\ba tribute portfolio resort\b/.test(text)) return true;
  if (
    /\b(crystal cove|humano|ermita|casa nizuc|hotel rumbao|loma medellin|barbados|lima|cartagena|medellin)\b/.test(
      text
    )
  ) {
    return true;
  }
  return false;
}

export async function buildBrandExplorerPresentationCopyParityAuditReport({
  brandKey = DEFAULT_BRAND_KEY,
  brandRecordId = DEFAULT_BRAND_RECORD_ID,
} = {}) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const pilot = BRAND_ASSET_PILOT_CONFIG[brandKey] || BRAND_ASSET_PILOT_CONFIG[DEFAULT_BRAND_KEY];
  const resolvedBrandRecordId = pilot?.recordId || brandRecordId;
  const brandName = pilot?.brandName || "Tribute Portfolio";

  const brandBasics = await airtableFetch(baseId, apiKey, BRAND_BASICS_TABLE, { method: "GET" }, resolvedBrandRecordId);
  if (!brandBasics.res.ok || !brandBasics.json?.id) throw new Error(`Brand Basics record not found: ${resolvedBrandRecordId}`);
  const tributePresentationRows = await listByFormula(
    baseId,
    apiKey,
    PRESENTATION_TABLE,
    `OR(FIND('${escapeFormulaValue(resolvedBrandRecordId)}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormulaValue(
      brandName
    )}')`
  );
  const tributeRows = normalizePresentationRows(tributePresentationRows);
  const tributeBySlot = new Map();
  for (const row of tributeRows) tributeBySlot.set(row.slotKey, row);

  const referenceProfilesInspected = [];
  const referenceRows = [];
  for (const fixture of REFERENCE_FIXTURES) {
    const parsed = readJsonFixture(fixture.path);
    if (!parsed) continue;
    referenceProfilesInspected.push(fixture.profile);
    referenceRows.push(...normalizeReferenceRows(fixture.profile, parsed));
  }
  const referenceBySection = new Map();
  for (const row of referenceRows) {
    const section = slotSection(row.slotKey);
    if (!referenceBySection.has(section)) referenceBySection.set(section, []);
    referenceBySection.get(section).push(row);
  }

  const sectionRecommendations = [];
  const mismatches = [];
  for (const slotKey of PROMOTED_SLOTS) {
    const current = tributeBySlot.get(slotKey) || { title: "", body: "", recordId: "" };
    const section = slotSection(slotKey);
    const refRows = referenceBySection.get(section) || [];
    const overlong = nz(current.body).length > (section === "gallery" ? 130 : 180);
    const titleStyleOk = !current.title || isTitleCaseLike(current.title);
    const generic = hasGenericDemoTone(`${current.title} ${current.body}`);
    const missingText = !nz(current.title) || !nz(current.body);
    const propertySpecific =
      section === "value-driver" ? hasPropertySpecificValueDriverCopy(current.title, current.body) : false;
    if (!titleStyleOk || generic || overlong || missingText) {
      mismatches.push({
        slotKey,
        section: sectionFromSlotForAudit(slotKey),
        mismatchFlags: {
          titleStyle: !titleStyleOk,
          genericDemoTone: generic,
          missingText,
          overlongText: overlong,
          propertySpecificHotelName: propertySpecific,
        },
        currentTitle: current.title,
        currentBody: current.body,
      });
    } else if (propertySpecific) {
      mismatches.push({
        slotKey,
        section: sectionFromSlotForAudit(slotKey),
        mismatchFlags: {
          titleStyle: false,
          genericDemoTone: false,
          missingText: false,
          overlongText: false,
          propertySpecificHotelName: true,
        },
        currentTitle: current.title,
        currentBody: current.body,
      });
    }
    const proposal = buildProposedCopy(slotKey, current.title, current.body);
    sectionRecommendations.push({
      slotKey,
      section: sectionFromSlotForAudit(slotKey),
      currentTitle: current.title,
      currentCaptionBody: current.body,
      referenceStandardPattern: ownerFacingPattern(section),
      referenceExamples: refRows.slice(0, 2).map((r) => ({
        profile: r.profile,
        title: r.title,
        body: r.body.length > 220 ? `${r.body.slice(0, 220)}...` : r.body,
      })),
      proposedTributeTitle: proposal.proposedTitle,
      proposedTributeCaptionBody: proposal.proposedBody,
      sourceBasis: proposal.sourceBasis,
      reviewStatus: proposal.reviewStatus,
      safeToWriteLater: proposal.safeToWriteLater,
      copyClassification: proposal.copyType,
    });
  }

  const slotsRemainBlank = INTENTIONALLY_MISSING_SLOTS.map((slotKey) => ({
    slotKey,
    reason: "Intentionally blank until stronger source-backed evidence and approved slot-specific media/copy are available.",
    reviewStatus: "Hold blank",
  }));

  const tributeCurrentPresentationCopyState = PROMOTED_SLOTS.map((slotKey) => {
    const cur = tributeBySlot.get(slotKey) || { title: "", body: "", recordId: "" };
    return { slotKey, title: cur.title, body: cur.body, hasTitle: Boolean(cur.title), hasBody: Boolean(cur.body) };
  });

  const sourceBacked = sectionRecommendations.filter((r) => r.copyClassification === "source-backed copy").map((r) => r.slotKey);
  const aiDrafted = sectionRecommendations.filter((r) => r.copyClassification === "AI-drafted owner-facing copy").map((r) => r.slotKey);
  const humanReview = sectionRecommendations.filter((r) => /Human review/.test(r.reviewStatus)).map((r) => r.slotKey);

  const filesRead = [
    "AGENTS.md",
    "reports/brand-explorer-visual-qa-verification.md",
    "reports/explorer-media-promotion-writer.md",
    "reports/tribute-portfolio-package-pipeline.md",
    "api/brand-library.js",
    "public/js/brand-explorer-atelier-from-api.js",
    "public/js/brand-explorer-gold-detail.js",
    "docs/brand-explorer-presentation-slots.md",
    "docs/data-intelligence/explorer-media-promotion-writer-v7.md",
    "docs/data-intelligence/brand-explorer-visual-qa-verification-v8.md",
    ...REFERENCE_FIXTURES.map((x) => x.path),
  ];

  return {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    filesRead,
    brand: { key: brandKey, recordId: resolvedBrandRecordId, name: brandName },
    referenceProfilesInspected,
    tributeCurrentPresentationCopyState,
    copyStyleMismatchesFound: mismatches,
    proposedStandardBySection: {
      hero: ownerFacingPattern("hero"),
      imageGallery: ownerFacingPattern("gallery"),
      whereThisBrandCreatesMostValue: ownerFacingPattern("value-driver"),
      brandStandardsOwnerConsiderations: ownerFacingPattern("standards"),
      recentOpeningsPr: ownerFacingPattern("openings-pr"),
    },
    proposedTributeSectionCopy: sectionRecommendations,
    sourceBackedCopySlots: sourceBacked,
    aiDraftedOwnerFacingCopySlots: aiDrafted,
    humanReviewCopySlots: humanReview,
    slotsShouldRemainBlank: slotsRemainBlank,
    imagesUntouched: true,
    airtableModified: false,
    tributeTextGovernancePlatformReady: true,
    readyForCopyPromotionWriterV9: mismatches.length > 0 && sectionRecommendations.length === PROMOTED_SLOTS.length,
    exactNextCommand:
      "npm run brand-explorer-presentation-copy-parity-audit -- --brand tribute-portfolio --dry-run",
    notes: [
      "This module audits parity and proposes copy model only; no Airtable writes are performed.",
      "Proposed copy must pass human review before any future v9 promotion writer apply flow.",
      "No image/media fields are edited by this audit.",
    ],
  };
}

export function buildBrandExplorerPresentationCopyParityAuditMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Presentation Copy Parity Audit v8.5");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push("");
  lines.push("## Reference profiles inspected");
  for (const name of report.referenceProfilesInspected) lines.push(`- ${name}`);
  lines.push("");
  lines.push("## Copy/style mismatches found");
  if (!report.copyStyleMismatchesFound.length) lines.push("- None.");
  for (const m of report.copyStyleMismatchesFound) {
    const flags = Object.entries(m.mismatchFlags)
      .filter(([, v]) => Boolean(v))
      .map(([k]) => k)
      .join(", ");
    lines.push(`- ${m.slotKey}: ${flags || "none"}`);
  }
  lines.push("");
  lines.push("## Section-by-section recommendations");
  for (const rec of report.proposedTributeSectionCopy) {
    lines.push(`- **${rec.slotKey}**`);
    lines.push(`  - Current: ${rec.currentTitle || "(no title)"} | ${rec.currentCaptionBody || "(no body)"}`);
    lines.push(`  - Proposed: ${rec.proposedTributeTitle || "(blank)"} | ${rec.proposedTributeCaptionBody || "(blank)"}`);
    lines.push(`  - Source basis: ${rec.sourceBasis}`);
    lines.push(`  - Review: ${rec.reviewStatus}`);
    lines.push(`  - Safe to write later: ${rec.safeToWriteLater ? "yes" : "no"}`);
  }
  lines.push("");
  lines.push("## Slots that should remain blank");
  for (const s of report.slotsShouldRemainBlank) lines.push(`- ${s.slotKey}: ${s.reason}`);
  lines.push("");
  lines.push("## Guardrails");
  lines.push(`- Images untouched: **${report.imagesUntouched ? "yes" : "no"}**`);
  lines.push(`- Tribute text/governance Platform Ready: **${report.tributeTextGovernancePlatformReady ? "yes" : "no"}**`);
  lines.push(`- Ready for v9 copy promotion writer: **${report.readyForCopyPromotionWriterV9 ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Next command");
  lines.push("");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  lines.push("");
  return lines.join("\n");
}
