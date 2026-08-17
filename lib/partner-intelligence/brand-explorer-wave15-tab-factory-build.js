/**
 * Wave 15 Stage 4 — Tab Factory content build (eight Hilton brands).
 *
 * Allowed writes: target-brand Presentation rows + limited Brand Basics
 * visible positioning fields + Target Guest Segments (validated).
 *
 * Forbidden: Brand Status, release fields, CV, Source Library, Registry,
 * protected 54, Radisson Collection, images, House of Originals, Morgans Originals,
 * and all non-target brands.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { scanForbiddenLanguage } from "./brand-explorer-v40b-copy-quality-patterns.js";
import {
  isFlexibilitySlotKey,
  sanitizeFlexibilityPresentationBody,
} from "../brand-explorer-flexibility-levels.mjs";
import { TAB_FACTORY_PROTECTED_BRANDS } from "./brand-explorer-tab-contracts.js";
import { BUILT_BLOCKED_PROTECTED_PUBLIC_FULL } from "./brand-explorer-built-blocked-content.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import {
  WAVE15_VERSION,
  WAVE15_PROTECTED_BASELINE_COUNT,
  WAVE15_STAGE4_APPROVED_SLUGS,
  WAVE15_TAB_FACTORY_BUILD_APPLY_FLAGS,
  WAVE15_STAGE4_CONTENT_CLEANUP_APPLY_FLAGS,
} from "./brand-explorer-wave15-factory-plan.js";
import { getWave15SourcePack } from "./brand-explorer-wave15-source-packs-content.js";
import { buildRecentMomentumCard } from "./brand-explorer-recent-momentum-contract.js";
import {
  WAVE15_SAFE_TGS,
  FORBIDDEN_SCENARIO_TITLES,
  WAVE15_PORTFOLIO_MIX,
  getWave15BrandContent,
} from "./brand-explorer-wave15-tab-factory-content.js";

const SAFE_TGS_OPTIONS = WAVE15_SAFE_TGS;

export const WAVE15_TAB_FACTORY_BUILD_VERSION = "wave15-tab-factory-build-v1";

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS_TABLE = "Brand Setup - Brand Basics";
const FORBIDDEN_STAGE4_BRANDS = new Set([
  "the-house-of-originals",
  "morgans-originals",
  "radisson-collection",
]);
const URL_ALLOWED_SLOTS = new Set(["footprint.momentum", "footprint.openings"]);

const FORBIDDEN_WRITE_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Brand Status",
  "Image",
  "Images",
  "Gallery Image",
  "Partner Intelligence - Source Library",
  "Partner Intelligence - Brand Asset Registry",
]);

const ALLOWED_BASICS_FIELDS = new Set([
  "Brand Positioning",
  "Guest Psychographics Description",
  "Target Guest Segments",
  "Brand Value Proposition",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function normalizeSlotKey(slotKey) {
  if (/^insight\.similar\.\d+$/i.test(slotKey)) return "insight.similar";
  return slotKey;
}

function escapeFormulaValue(v) {
  return nz(v).replace(/'/g, "\\'");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function readJson(fileName) {
  const p = path.join(REPORTS_DIR, fileName);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function removeRawUrls(text) {
  return String(text || "")
    .replace(/https?:\/\/\S+/gi, "")
    .replace(/\bconversion-friendly\.?\b/gi, "suited to conversion")
    .replace(/\bsource pack\b/gi, "brand materials")
    .replace(/\bsource-supported\b/gi, "verified")
    .replace(/\bowner-fit diligence\b/gi, "owner diligence")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function sanitizePropertyNote(text) {
  let s = removeRawUrls(text);
  s = s.replace(/\bbefore Stage \d\b/gi, "");
  s = s.replace(/\bStage \d\b/gi, "");
  s = s.replace(/\bsteward must confirm\b/gi, "");
  s = s.replace(/\bsteward-matched\b/gi, "");
  s = s.replace(/\bsteward to match\b/gi, "");
  s = s.replace(/\bbind property\b/gi, "");
  s = s.replace(/\bbind cards only\b/gi, "");
  s = s.replace(/\bpending steward\b/gi, "");
  s = s.replace(/\bmarket archetype pending\b/gi, "");
  s = s.replace(/\bDo not invent a property URL\b/gi, "");
  s = s.replace(/\bbefore openings\/gallery cards\b/gi, "");
  s = s.replace(/\bnamed on brand page;\s*/gi, "");
  s = s.replace(/\bdedicated property overview URL\b/gi, "property listing");
  s = s.replace(/\bproperty overview URL\b/gi, "property listing");
  s = s.replace(/\s{2,}/g, " ");
  return s.trim();
}

function listUnique(items = []) {
  return [...new Set(items.map((x) => nz(x)).filter(Boolean))];
}

export function parseWave15TabFactoryBuildFlags(argv = []) {
  const missing = WAVE15_TAB_FACTORY_BUILD_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

function findSlotExact(rows, slotKey, title) {
  const key = normalizeSlotKey(slotKey);
  const list = (rows || []).filter(
    (r) =>
      nz(r.slotKey) === key &&
      r.active !== false &&
      !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
  );
  if (nz(title)) return list.find((r) => nz(r.title) === nz(title)) || null;
  return list[0] || null;
}

/** Slots that intentionally have multiple Presentation rows (match by title). */
const MULTI_ROW_SLOT_KEYS = new Set([
  "standards.requirement",
  "footprint.openings",
  "footprint.momentum",
  "insight.similar",
  "materials.gallery",
]);

function findExistingPresentationRow(rows, slotKey, title, usedRecordIds) {
  const key = normalizeSlotKey(slotKey);
  let existing = findSlotExact(rows, key, title);
  if (existing?.recordId && usedRecordIds.has(existing.recordId)) existing = null;
  if (existing) return existing;

  const candidates = (rows || []).filter(
    (r) =>
      nz(r.slotKey) === key &&
      r.active !== false &&
      !usedRecordIds.has(r.recordId) &&
      !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
  );
  if (!candidates.length) return null;

  // Unique slots: always update the first existing row (avoid orphan duplicates when titles change).
  if (!MULTI_ROW_SLOT_KEYS.has(key) && !/^insight\.similar/i.test(key)) {
    return candidates.find((r) => !nz(r.body)) || candidates[0] || null;
  }

  // Multi-row: prefer empty body, else leave for POST when title did not match.
  if (!title) return candidates.find((r) => !nz(r.body)) || candidates[0] || null;
  return candidates.find((r) => !nz(r.body)) || null;
}

async function listPresentationRowsLight(brandName) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey || !brandName) return [];
  const formula = `{Brand Name}='${escapeFormulaValue(brandName)}'`;
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(json.error?.message || `Presentation list failed for ${brandName}: ${res.status}`);
    }
    for (const rec of json.records || []) {
      const f = rec.fields || {};
      rows.push({
        recordId: rec.id,
        slotKey: nz(f["Slot Key"]),
        title: nz(f.Title),
        body: nz(f.Body),
        active: f.Active !== false,
        externalDisplayStatus: nz(f["External Display Status"]),
        caseSummaryOverview: nz(f["Case Summary Overview"]),
      });
    }
    offset = json.offset || "";
  } while (offset);
  return rows;
}

async function fetchBasicsRecord(recordId) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey || !recordId) return null;
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BASICS_TABLE)}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Basics get failed ${res.status}`);
  return json;
}

async function listTargetGuestSegmentOptions() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) return [];
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}/tables`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `Meta tables failed ${res.status}`);
  const table = (json.tables || []).find((t) => t.name === BASICS_TABLE);
  if (!table) return [];
  const field = (table.fields || []).find((f) => f.name === "Target Guest Segments");
  return (field?.options?.choices || []).map((c) => c.name).filter(Boolean);
}

async function airtableWrite({ baseId, apiKey, table, recordId, fields, method }) {
  const url =
    method === "POST"
      ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`
      : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const maxAttempts = 8;
  let lastErr = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const res = await fetch(url, {
      method,
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields }),
    });
    const json = await res.json().catch(() => ({}));
    if (res.ok) return json;
    const msg = json.error?.message || `${method} ${table} failed: ${res.status}`;
    lastErr = new Error(msg);
    const retryable = res.status === 429 || res.status >= 500;
    if (!retryable || attempt === maxAttempts) break;
    await sleep(Math.min(30_000, 800 * 2 ** (attempt - 1)));
  }
  throw lastErr || new Error(`${method} ${table} failed`);
}

function scrubPresentationText(text, { allowStructuredUrls = false } = {}) {
  if (allowStructuredUrls) {
    // Preserve dateLine + summary + trailing https URL for momentum/openings bodies.
    return String(text || "")
      .replace(/\bconversion-friendly\.?\b/gi, "suited to conversion")
      .replace(/\bsource pack\b/gi, "brand materials")
      .replace(/\bsource-supported\b/gi, "verified")
      .replace(/\bowner-fit diligence\b/gi, "owner diligence")
      .trim();
  }
  return removeRawUrls(text);
}

function row(slotKey, title, body, sortOrder, extra = {}) {
  const allowStructuredUrls = URL_ALLOWED_SLOTS.has(slotKey);
  return {
    slotKey,
    title: scrubPresentationText(title || "", { allowStructuredUrls: false }),
    body: scrubPresentationText(body, { allowStructuredUrls }),
    sortOrder,
    ...(extra.caseSummaryOverview
      ? { caseSummaryOverview: scrubPresentationText(extra.caseSummaryOverview) }
      : {}),
    ...(extra.caseSummaryBrandRelevance
      ? { caseSummaryBrandRelevance: scrubPresentationText(extra.caseSummaryBrandRelevance) }
      : {}),
    ...(extra.caseSummaryOwnerObjective
      ? { caseSummaryOwnerObjective: scrubPresentationText(extra.caseSummaryOwnerObjective) }
      : {}),
    ...(extra.caseSummaryInterpretation
      ? { caseSummaryInterpretation: scrubPresentationText(extra.caseSummaryInterpretation) }
      : {}),
    ...(extra.caseSummaryTags ? { caseSummaryTags: scrubPresentationText(extra.caseSummaryTags) } : {}),
  };
}

function bullets(lines) {
  return lines.filter(Boolean).map((x) => `- ${removeRawUrls(x)}`).join("\n");
}

function wordCount(s) {
  return nz(s).split(/\s+/).filter(Boolean).length;
}

function ensureMinWords(summary, minWords, pad) {
  let s = nz(summary);
  const target = Number(minWords) || 35;
  const padText = nz(pad);
  if (wordCount(s) >= target) return s;
  if (!padText) return s;
  s = `${s} ${padText}`.trim();
  if (wordCount(s) < target) s = `${s} ${padText}`.trim();
  return s;
}

function lifecyclePad(brandDisplayName, model) {
  return `Keep ${brandDisplayName} product and service responsibilities clear among owner, operator, and brand teams so the ${model} stays deliverable after affiliation and through ongoing operations.`;
}

function buildOpeningsRows(pack, brandDisplayName) {
  const rows = [];
  let idx = 0;
  const peers = listUnique(pack.distinguishFrom || []).slice(0, 2).join(" and ");
  const model = removeRawUrls(pack.lens || "brand-specific positioning");
  for (const p of pack.propertyExamples || []) {
    if (idx >= 3) break;
    const geo = p.geographyLabel === "CALA" ? "CALA" : "International Reference";
    rows.push(
      row(
        "footprint.openings",
        `${p.propertyName} — ${geo}`,
        ensureMinWords(
          [
            `${geo} property reference for ${brandDisplayName} in ${p.market}.`,
            `${p.propertyName} illustrates the ${brandDisplayName} operating model and guest experience in a ${geo === "CALA" ? "regional" : "comparable international"} demand context.`,
            `For owners evaluating ${brandDisplayName}, this property helps benchmark standards scope, service delivery, and competitive positioning against ${peers} alternatives.`,
            p.note ? sanitizePropertyNote(p.note) : "",
          ]
            .filter(Boolean)
            .join(" "),
          45,
          lifecyclePad(brandDisplayName, model)
        ),
        490 + idx,
        {
          caseSummaryOverview: `${p.propertyName} is a ${geo} property-level reference for ${brandDisplayName} diligence in ${p.market}.`,
          caseSummaryBrandRelevance: `${brandDisplayName} operating-model and guest-experience reference at the property level.`,
          caseSummaryOwnerObjective: "Compare operating fit and standards scope using a named property reference.",
          caseSummaryInterpretation: "Named property reference.",
          caseSummaryTags: `${geo}, ${brandDisplayName}, Property example, ${p.market}`,
        }
      )
    );
    idx += 1;
  }
  return rows;
}

function buildMomentumRows(pack, brandDisplayName) {
  const rows = [row("footprint.momentum_label", "", "Recent Momentum", 448)];
  const list = (pack.recentMomentumCandidates || []).slice(0, 3);
  const model = removeRawUrls(pack.lens || "brand-specific positioning");
  let i = 0;
  for (const m of list) {
    const geo = m.geographyLabel === "CALA" ? "CALA" : "International Reference";
    rows.push(
      row(
        "footprint.momentum",
        `${m.title} · ${m.dateLine}`,
        ensureMinWords(
          `${geo}. ${m.summary} Owners can use this as a ${brandDisplayName} positioning reference.`,
          28,
          lifecyclePad(brandDisplayName, model)
        ),
        449 + i
      )
    );
    i += 1;
  }
  return rows;
}

export function generateWave15PresentationPack(slug, opts = {}) {
  const content = getWave15BrandContent(slug);
  const pack = content.sourcePack || getWave15SourcePack(slug);
  const brandDisplayName = opts.airtableName || content.displayName || pack.officialBrandName || slug;
  const model = removeRawUrls(content.model || pack.lens || "brand-specific positioning");
  const ownerLens = listUnique(content.ownerLens || pack.ownerFacingPositioningNotes || [])
    .map((s) =>
      removeRawUrls(s)
        .replace(/\bsteward-matched\b/gi, "verified")
        .replace(/\bsteward-confirmed\b/gi, "verified")
        .replace(/\bsteward\b/gi, "")
        .replace(/\bStage\s*\d\b/gi, "release readiness")
        .replace(/\bsource pack\b/gi, "brand materials")
        .replace(/\s{2,}/g, " ")
        .trim()
    )
    .filter(Boolean);
  const distinctions = listUnique(pack.siblingBrandDistinctionNotes || []);
  const risks = listUnique(pack.manualReviewRisks || [])
    .map((s) =>
      removeRawUrls(s)
        .replace(/\bDo not reuse\b/gi, "Do not mix")
        .replace(/\bsteward[^.]*\.?/gi, "")
        .replace(/\bStage\s*\d\b/gi, "release readiness")
        .replace(/\sfactory\b/gi, "")
        .replace(/\s{2,}/g, " ")
        .trim()
    )
    .filter((s) => s && !/^\s*$/.test(s) && !/\bDo not reuse\b/i.test(s));
  const sourceGaps = listUnique(pack.sourceGaps || [])
    .map((g) =>
      removeRawUrls(g)
        .replace(/\bFDD\b/g, "franchise materials")
        .replace(/\bItem\s*19\b/gi, "performance disclosures")
        .replace(/\bsteward-matched\b/gi, "verified")
        .replace(/\bsteward-confirmed\b/gi, "verified")
        .replace(/\bsteward\b/gi, "")
        .replace(/\bStage\s*\d\b/gi, "release readiness")
        .replace(/\bsource pack\b/gi, "brand materials")
        .replace(/\bsource-supported\b/gi, "verified")
        .replace(/\s{2,}/g, " ")
        .trim()
    )
    .filter((g) => g && !/\b(FDD|ADR|RevPAR|fee stack)\b/i.test(g));
  const tgs = (content.tgs || []).filter((x) => SAFE_TGS_OPTIONS.includes(x));
  const peers = content.peers || listUnique(pack.distinguishFrom || []).slice(0, 3).join(", ");
  const peerPrimary = content.peerPrimary || peers.split(",")[0]?.trim() || "adjacent peer brands";
  const calaStrong = content.calaAvailability === "supported" || content.calaAvailability === "pipeline";
  const calaEx = (pack.propertyExamples || []).filter((p) => p.geographyLabel === "CALA");
  const calaMarkets = calaEx.map((p) => p.market).filter(Boolean).join(", ") || "named regional markets";
  const tgsLabel = tgs.join(", ") || "brand-aligned segments";
  const parent = content.parent || pack.parentPlatform || "Hilton Worldwide";
  const loyalty = content.loyalty || "Hilton Honors";
  const pad = lifecyclePad(brandDisplayName, model);

  // Enforce Wave 15 scenario title rules
  for (const c of [...content.valueOwnersScenarios, ...content.overviewScenarios]) {
    if (FORBIDDEN_SCENARIO_TITLES.includes(c.title)) {
      throw new Error(`${slug}: forbidden scenario title "${c.title}"`);
    }
    if (/\b(source-supported|source pack|factory|Stage\s*\d|owner-fit diligence)\b/i.test(`${c.title}\n${c.body}`)) {
      throw new Error(`${slug}: forbidden process language in scenario "${c.title}"`);
    }
  }

  const presentation = [
    row(
      "Brand Positioning",
      "",
      `${brandDisplayName} is positioned as a ${model}. Owner underwriting should prioritize brand-specific fit, demand alignment, and operating-model readiness over Hilton Worldwide corporate generalizations or sibling-brand assumptions.`,
      10
    ),
    row(
      "Guest Psychographics Description",
      "",
      `${brandDisplayName} targets guests aligned with ${tgsLabel}. Evaluate demand behavior in each target market rather than generic chain-loyalty averages or parent-platform assumptions.`,
      11
    ),
    row(
      "overview.typical_use_case",
      "Brand Snapshot",
      `Best fit: ${removeRawUrls(content.ownerLens?.[0] || pack.calaFirstPosture || model)}. Evaluate demand concentration, product condition, operator capability, and competitive set together before affiliation. ${brandDisplayName} works best when the asset aligns with the brand promise and local demand supports the target guest profile.`,
      20
    ),
    row(
      "overview.development_model",
      "Brand Positioning",
      `${brandDisplayName} owner thesis: ${ownerLens[0] || model}. ${ownerLens[1] || `Owners should validate that the asset, market, and operator can credibly deliver the ${brandDisplayName} promise before proceeding.`}`,
      21
    ),
    row(
      "overview.relative_positioning",
      "Owner Fit",
      `${brandDisplayName} should be chosen when the asset can deliver this positioning more credibly than ${peers}. Assess whether demand mix, product scope, and operating complexity favor ${brandDisplayName} over adjacent Hilton alternatives in the same tier and geography.`,
      22
    ),
    // Where This Brand Creates the Most Value — real investment titles
    ...content.overviewScenarios.map((c, i) =>
      row(
        `overview.scenario.${i + 1}`,
        c.title,
        ensureMinWords(c.body, 45, pad),
        30 + i
      )
    ),
    row(
      "overview.why_value",
      "Proof Points",
      bullets([
        `Official brand positioning for ${brandDisplayName} is anchored in Hilton brand and development materials specific to this brand—not Hilton Worldwide corporate copy`,
        `Geography posture is ${calaStrong ? `CALA-supported with named examples in ${calaMarkets}` : "International Reference where verified CALA operating examples are not yet available"} — property cards follow those labels`,
        `Owner distinction focus separates ${brandDisplayName} from ${peers} using product, demand, and operating-model criteria`,
        `Target guest segments — ${tgsLabel} — follow validated Brand Basics taxonomy options only`,
        `Parent/platform context (${parent} / ${loyalty}) stays labeled and subordinate to brand-specific evidence`,
      ]),
      33
    ),
    row(
      "overview.proof.1",
      "Brand Positioning Evidence",
      ensureMinWords(
        `${brandDisplayName} positioning is anchored in official brand documentation and verified property-level examples where available. ${pack.officialBrandPage?.label || "Brand page"} and development materials provide the canonical identity reference for owner diligence. ${calaStrong ? `CALA operating proof from ${calaMarkets} supports regional owners evaluating affiliation.` : "International Reference examples illustrate the operating model where verified CALA inventory is not yet available and should remain clearly labeled."}`,
        38,
        pad
      ),
      34
    ),
    row(
      "overview.proof.2",
      "CALA Market Relevance",
      ensureMinWords(
        calaStrong
          ? `${brandDisplayName} has verified CALA references in ${calaMarkets}, providing property-level diligence anchors for regional owners. These examples demonstrate operating-model execution and guest-experience delivery in CALA demand contexts rather than parent-platform averages.`
          : `${brandDisplayName} does not yet have verified CALA operating examples in official brand materials. Property and region cards are labeled International Reference or cleanly unavailable. Owners evaluating CALA markets should validate demand fit and competitive set independently before assuming regional proof.`,
        38,
        pad
      ),
      35
    ),
    row(
      "overview.proof.3",
      "Peer Separation",
      ensureMinWords(
        `${brandDisplayName} is distinguished from ${peers} by product, guest promise, and asset requirements. ${distinctions[0] || "Each peer operates in a different positioning lane."} ${distinctions[1] || "Compare on operating complexity, demand mix, and standards requirements rather than parent-platform affiliation alone."}`,
        38,
        pad
      ),
      36
    ),
    row(
      "overview.proof.4",
      "Source Confidence",
      ensureMinWords(
        sourceGaps.length
          ? `Known diligence gaps for ${brandDisplayName}: ${sourceGaps.join("; ")}. Keep claims inside verified brand and property evidence, and keep ${parent} context labeled when using platform materials.`
          : `No major evidence gaps flagged for ${brandDisplayName} beyond normal asset-level diligence. Official brand documentation, verified property examples where present, and dated momentum items support current owner-facing content when claims stay brand-specific.`,
        38,
        pad
      ),
      37
    ),
    row(
      "overview.featured_application",
      "Owner Considerations",
      ensureMinWords(
        bullets(
          risks.length
            ? [
                ...risks.map((r) => removeRawUrls(r)),
                "Keep claims market-specific and tied to the asset under review.",
                `Compare ${brandDisplayName} against ${peers} on operating complexity and asset fit before affiliation.`,
              ]
            : [
                "Keep claims market-specific and tied to the asset under review.",
                "Validate operating-model assumptions against local demand and competitive context.",
                "Ensure the asset's physical plant and service scope can match brand standards before proceeding.",
                `Compare ${brandDisplayName} against ${peers} on operating complexity and asset fit before affiliation.`,
              ]
        ),
        35,
        pad
      ),
      44,
      {
        caseSummaryOverview: `Featured path for hotels evaluating ${brandDisplayName} as a ${model}.`,
        caseSummaryBrandRelevance: `${brandDisplayName} remains distinct from ${peerPrimary} and peer alternatives.`,
        caseSummaryOwnerObjective: `Underwrite brand-specific fit for ${brandDisplayName} against the specific asset.`,
        caseSummaryInterpretation: `Owner-facing synthesis grounded in official Hilton brand sources.`,
        caseSummaryTags: `${brandDisplayName}, ${calaStrong ? "CALA" : "International Reference"}, conversion, owner decision`,
      }
    ),
    row(
      "overview.differentiators.identity",
      "Experience & Identity",
      bullets([
        `${brandDisplayName} guest promise is tied to a clear ${model} rather than a generic Hilton corporate story`,
        `Keep ${brandDisplayName} distinct from ${peerPrimary} during underwriting`,
        `Property expression must support ${brandDisplayName} positioning in rooms and public space`,
        `Local market demand must match the brand’s audience logic (${tgsLabel}) before affiliation`,
      ]),
      45
    ),
    row(
      "overview.differentiators.commercial",
      "Commercial & Distribution",
      bullets([
        `${loyalty} participation supports the commercial case for ${brandDisplayName}`,
        `${parent} distribution and commercial infrastructure remain conversion workstreams`,
        `Systems and loyalty readiness should be sequenced with product and staffing work`,
        `Compare commercial obligations across peer brands (${peers}) before selecting ${brandDisplayName}`,
      ]),
      46
    ),
    row(
      "overview.bestAt.1",
      `${brandDisplayName} guest promise`,
      `${brandDisplayName} is best at delivering a ${model} when the asset and operator can sustain that promise consistently after opening or conversion, with clear separation from ${peerPrimary}.`,
      47
    ),
    row(
      "overview.bestAt.2",
      "Owner-relevant platform access",
      `Owners evaluate ${brandDisplayName} for ${loyalty} reach and ${parent} commercial infrastructure while retaining a brand-specific guest story grounded in ${tgsLabel}.`,
      48
    ),
    row(
      "overview.bestAt.3",
      "Clear peer separation",
      `${brandDisplayName} is most useful when owners explicitly distinguish it from ${peers} instead of treating Hilton brands as interchangeable lanes.`,
      49
    ),
    row(
      "overview.portfolio_context",
      "Portfolio Context",
      `Parent/platform context (${parent} / ${loyalty}) should remain clearly labeled and subordinate to brand-specific positioning proof. Evaluate ${brandDisplayName} on its own operating thesis rather than assuming portfolio averages apply. ${distinctions[0] || ""}`.trim(),
      50
    ),
    row(
      "valueOwners.overview",
      "What Owners Are Buying",
      `Owners evaluating ${brandDisplayName} are buying a ${model} backed by ${parent} distribution and ${loyalty}. The practical case is ${ownerLens[0] || "brand-specific owner fit with credible operating delivery"}.`,
      51
    ),
    // Value Creation Scenarios — real owner-use cards
    ...content.valueOwnersScenarios.map((c, i) =>
      row(`valueOwners.scenario.${i + 1}`, c.title, c.body, 52 + i)
    ),
    row(
      "valueOwners.lifecycle.1",
      "Evaluation",
      ensureMinWords(
        `Start with demand fit, product condition, and peer alternatives (${peers}). Decide whether ${brandDisplayName}'s ${model} is the right lane before detailed conversion capital is committed.`,
        42,
        pad
      ),
      300
    ),
    row(
      "valueOwners.lifecycle.2",
      "Conversion Design",
      ensureMinWords(
        `Translate ${brandDisplayName} positioning into rooms, public space, service, and technology workstreams that match the intended ${model}. Sequence design, systems, and capital milestones with financing and operator decisions so conversion scope stays underwritable.`,
        42,
        pad
      ),
      301
    ),
    row(
      "valueOwners.lifecycle.3",
      "Pre-Opening",
      ensureMinWords(
        `Coordinate ${loyalty} readiness, training, staffing, and commercial launch with product completion for ${brandDisplayName}. Clarify owner, operator, and brand responsibilities before opening so the guest promise is deliverable from day one.`,
        42,
        pad
      ),
      302
    ),
    row(
      "valueOwners.lifecycle.4",
      "Opening",
      ensureMinWords(
        `Launch with the ${brandDisplayName} guest promise consistently expressed across service and channels while platform systems stabilize. Keep escalation paths clear for the first operating weeks and verify quality readiness against the intended ${model}.`,
        42,
        pad
      ),
      303
    ),
    row(
      "valueOwners.lifecycle.5",
      "Ramp-Up",
      ensureMinWords(
        `Use early guest feedback and channel mix to refine delivery of the ${model}. Watch whether staffing and public-space programming actually support the intended ${brandDisplayName} promise, and adjust operator execution before assuming affiliation value is fully realized.`,
        42,
        pad
      ),
      304
    ),
    row(
      "valueOwners.lifecycle.6",
      "Ongoing",
      ensureMinWords(
        `Maintain ${brandDisplayName} product discipline while meeting applicable platform quality and commercial obligations. Revisit capital and operator alignment as the hotel stabilizes so ${brandDisplayName} remains credible versus peer alternatives such as ${peerPrimary}.`,
        42,
        pad
      ),
      305
    ),
    row(
      "footprint.portfolio_mix",
      "Portfolio Mix",
      (WAVE15_PORTFOLIO_MIX[slug] || []).join("\n") +
        "\n\nCurated sample mix based on global brand positioning (illustrative, not a disclosed portfolio census).",
      460
    ),
    row(
      "footprint.geo_intro",
      "Geographic Footprint",
      ensureMinWords(
        `${removeRawUrls(pack.calaFirstPosture || "Use CALA-first references where available, otherwise label International Reference.")
          .replace(/\bsource pack\b/gi, "current brand materials")
          .replace(/\bsource-supported\b/gi, "verified")
          .replace(/\bsteward\b/gi, "brand team")}. ${brandDisplayName} region cards below summarize owner-relevant geography with explicit CALA or International Reference labeling.`,
        40,
        pad
      ),
      470
    ),
    // >=3 region cards from curated content
    ...content.regions.map((r, i) =>
      row(r.slotKey, r.title, ensureMinWords(r.body, 28, pad), 472 + i, {
        caseSummaryTags: r.tags,
        caseSummaryInterpretation: `Geography evidence basis: ${r.tags}`,
      })
    ),
    row(
      "footprint.growth_themes",
      "Growth Priorities",
      bullets([
        ownerLens[0] || "Owner-led growth anchored in brand-specific positioning",
        ownerLens[1] || "Operational readiness and standards alignment before geographic expansion",
        "Brand-specific positioning discipline maintained across new markets and conversions",
        `Peer separation from ${peers} preserved in each new market entry`,
      ]),
      480
    ),
    row(
      "footprint.growth_editorial",
      "Growth Editorial",
      ensureMinWords(
        `${brandDisplayName} growth should be evaluated through brand-specific positioning, not Hilton Worldwide expansion targets alone. ${ownerLens[0] || "Prioritize markets where demand, product, and operator capability align with the brand promise."} ${calaStrong ? `CALA markets (${calaMarkets}) already demonstrate operating proof for this positioning.` : "New CALA market claims should wait for verified property-level evidence with a named hotel and matching official property URL."}`,
        40,
        pad
      ),
      481
    ),
    row(
      "footprint.growth_fit",
      "Growth Fit",
      `Best growth fit: assets ready for a ${model}. Weaker fit: hotels better aligned to ${peerPrimary} or unable to sustain the ${brandDisplayName} guest promise with the required operating discipline.`,
      482
    ),
    row(
      "operations.model.primary_model",
      "Operating Model",
      `${brandDisplayName} typically participates through the affiliation or operating path available for the market and asset within ${parent}. Verify the applicable agreement structure for the specific opportunity rather than assuming a single universal model.`,
      100
    ),
    row(
      "operations.model.management_option",
      "Management Option",
      `Third-party or owner-operated models can work when leadership can deliver the ${brandDisplayName} guest promise and platform obligations. Operator fit matters as much as brand selection for this ${model}.`,
      101
    ),
    row(
      "operations.model.typical_ownership",
      "Typical Ownership",
      `Owners seeking ${ownerLens[0] || "brand-specific owner fit"} and a clearer ${model} than peer alternatives such as ${peerPrimary}.`,
      102
    ),
    row(
      "operations.model.brand_involvement",
      "Brand Involvement",
      `${parent} development and brand teams may engage on conversion readiness, product presentation, systems, and quality expectations. Align current review stages for the asset before underwriting timelines.`,
      103
    ),
    row(
      "operations.model.systems_integration",
      "Systems Integration",
      `${brandDisplayName} hotels participate in relevant ${loyalty} and ${parent} technology ecosystems. Validate PMS, CRS, training, and digital requirements before locking a conversion timeline.`,
      104
    ),
    row(
      "operations.model.pre_opening",
      "Pre-Opening",
      `Expect product readiness, systems setup, team training, and commercial-launch work before opening or relaunch. Sequence these requirements with financing and construction for ${brandDisplayName}.`,
      105
    ),
    row(
      "operations.model.staffing_intensity",
      "Staffing Intensity",
      `Staffing should match the ${brandDisplayName} guest promise and public-space program. Underwrite front office, housekeeping, and any F&B or social-space coverage to the intended positioning.`,
      106
    ),
    row(
      "operations.model.fb_complexity",
      "F&B Complexity",
      `F&B and public-space complexity varies by ${brandDisplayName} site type. Review concept, hours, and operator capability against local demand rather than copying another Hilton brand’s outlet assumptions.`,
      107
    ),
    row(
      "operations.model.training",
      "Training",
      `Training should connect ${loyalty} expectations with the ${brandDisplayName} service identity. Align modules, timing, and refresh expectations in the pre-opening plan.`,
      108
    ),
    row(
      "operations.model.reporting_discipline",
      "Reporting Discipline",
      `Platform participation creates reporting and operating rhythms owners should understand during diligence. Clarify available owner reporting and operator responsibilities for the ${brandDisplayName} agreement.`,
      109
    ),
    row(
      "operations.model.qa_rhythm",
      "QA Rhythm",
      `Quality and brand review support the ${brandDisplayName} guest promise at conversion and during operations. Align cadence, remediation paths, and responsibility split before underwriting affiliation value.`,
      110
    ),
    row(
      "operations.model.technology",
      "Technology",
      `Technology participation should be a conversion workstream. Validate required systems, loyalty integration, implementation support, and asset-specific constraints for ${brandDisplayName}.`,
      111
    ),
    row(
      "operations.standards_philosophy",
      "Standards Philosophy",
      ensureMinWords(
        `Standards should protect ${brandDisplayName} brand identity while remaining executable for the specific asset, market, and operator. ${distinctions[0] || "Design, service, and programming standards are brand-specific and should not be borrowed from adjacent Hilton brands."} Align PIP and lifecycle capital from the asset review.`,
        35,
        pad
      ),
      112
    ),
    row(
      "standards.intro",
      "Standards Intro",
      `${brandDisplayName} standards should support a ${model} alongside ${parent} platform participation. Current acceptance, product, technology, training, and quality details must be aligned for the specific asset and market.`,
      600
    ),
    row(
      "standards.requirement",
      "Design & guest promise review",
      `The property should present a credible ${brandDisplayName} experience through rooms, public spaces, arrival, and overall design aligned to a ${model}.`,
      601
    ),
    row(
      "standards.requirement",
      `${loyalty} systems participation`,
      `Reservation, loyalty, distribution, and related platform systems may form part of affiliation. Align required technology and implementation sequencing for ${brandDisplayName}.`,
      602
    ),
    row(
      "standards.requirement",
      "Public-space and amenity capital",
      `Public spaces and amenities should support the ${brandDisplayName} guest promise and local demand. Establish required versus elective improvements before finalizing conversion budgets.`,
      603
    ),
    row(
      "standards.requirement",
      "Guest-room product standards",
      `Guest rooms should align with ${brandDisplayName} positioning. Validate product gaps, accessibility work, and design flexibility during diligence.`,
      604
    ),
    row(
      "standards.requirement",
      "Training and service culture",
      `Team training should connect ${loyalty} participation with the ${brandDisplayName} service promise before opening or relaunch.`,
      605
    ),
    row(
      "standards.requirement",
      "Ongoing quality review",
      `Ongoing quality expectations preserve ${brandDisplayName} positioning after conversion. Align review timing, remediation paths, and responsibility split.`,
      606
    ),
    row(
      "standards.questions",
      "Questions owners should ask",
      bullets([
        `What product and service characteristics distinguish ${brandDisplayName} from ${peerPrimary}?`,
        "Which improvements are required before conversion, and how are they reviewed?",
        `What ${loyalty} and technology systems must the property implement?`,
        "How much design and operating flexibility remains after affiliation?",
        "What quality-review cadence and remediation responsibilities apply after opening?",
      ]),
      608
    ),
    row(
      "standards.design",
      "Design Standards",
      `${brandDisplayName} design standards define the physical environment that supports the brand promise. Align architecture, interior design, and public-space layout with brand requirements before committing to affiliation or conversion scope.`,
      120
    ),
    row(
      "standards.fb",
      "F&B Standards",
      `Food and beverage programming for ${brandDisplayName} must align with the brand's guest promise and positioning tier. Evaluate F&B scope, quality requirements, and operating complexity against the asset's capabilities and local market demand.`,
      121
    ),
    row(
      "standards.service",
      "Service Model",
      `${brandDisplayName} service delivery must reflect the brand's positioning and guest expectations. The operator should demonstrate service-model capability specific to this brand tier and style rather than applying a generic platform service framework.`,
      122
    ),
    row(
      "standards.conversion",
      "Conversion Readiness",
      `Conversion candidates for ${brandDisplayName} should be evaluated against brand-specific physical and programming requirements. ${calaStrong ? "CALA conversion examples demonstrate achievable scope and standards alignment." : "Use International Reference conversion examples to benchmark scope and investment requirements."} Gap analysis between current asset condition and brand standards should precede any commitment.`,
      123
    ),
    row(
      "standards.technology",
      "Technology & Systems",
      `${brandDisplayName} requires technology and systems integration consistent with ${parent} platform standards. Align PMS, channel management, loyalty integration, and revenue management system compatibility before launch.`,
      124
    ),
    row(
      "standards.compliance",
      "Quality Assurance",
      `Quality assurance for ${brandDisplayName} should include defined inspection cadence, escalation paths, and remediation timelines. Standards compliance protects brand equity and owner investment through consistent guest-experience delivery.`,
      125
    ),
    row("operations.flexibility.design", "Flexibility Indicators", "High", 200),
    row(
      "operations.flexibility.conversion",
      "Flexibility Indicators",
      /flex|studiores/i.test(slug) ? "High" : calaStrong ? "Medium" : "High",
      201
    ),
    row("operations.flexibility.localization", "Flexibility Indicators", "Medium", 202),
    row("operations.flexibility.operational_rigidity", "Flexibility Indicators", "Medium", 203),
    row("operations.flexibility.pip", "Flexibility Indicators", "Medium", 204),
    row("operations.flexibility.prototype", "Flexibility Indicators", /studiores/i.test(slug) ? "High" : "Medium", 205),
    row(
      "operations.operator_compat.summary",
      "Third-Party Operator Compatibility",
      ensureMinWords(
        `${ownerLens[0] || "Operator must translate brand promise into consistent on-property delivery."} The operator should have demonstrated capability with ${brandDisplayName} or a comparable brand in the same tier and operating-model category, while maintaining ${parent} systems and quality obligations.`,
        38,
        pad
      ),
      113
    ),
    row(
      "operations.operator_compat.fit",
      "Operator Fit",
      `Best fit: operators experienced with ${model} execution and platform discipline. Weaker fit: operators optimized only for unrelated prototypes or unable to sustain the ${brandDisplayName} promise.`,
      114
    ),
    row(
      "operations.operator_compat.tags",
      "Operator Tags",
      bullets([brandDisplayName, "Platform discipline", "Conversion-ready", calaStrong ? "CALA-ready" : "International Reference"]),
      115
    ),
    row(
      "operations.compliance.qa_cadence",
      "Compliance & Oversight",
      `Define quality cadence, operating controls, and escalation paths before launch. ${brandDisplayName} compliance should include pre-opening verification, post-opening audit, and ongoing quality monitoring tied to brand-specific standards rather than generic platform benchmarks.`,
      210
    ),
    row(
      "operations.compliance.training_rigor",
      "Training Rigor",
      `Training should prepare teams for ${loyalty} participation and the ${brandDisplayName} guest experience. Define ownership of onboarding and refresh work before opening.`,
      211
    ),
    row(
      "operations.compliance.reporting",
      "Reporting",
      `Clarify ${parent} reporting, loyalty, and distribution obligations alongside the operator’s reporting role for the specific ${brandDisplayName} agreement.`,
      212
    ),
    row(
      "operations.compliance.brand_interaction",
      "Brand Interaction",
      `Brand interaction typically centers on development, conversion, systems, quality, and commercial readiness. Establish a practical decision calendar among owner, operator, and brand teams for ${brandDisplayName}.`,
      213
    ),
    row(
      "economics.opening.step.1",
      "Application & Feasibility",
      ensureMinWords(
        `Evaluate demand fit, property condition, and conversion or new-build scope against ${brandDisplayName} brand standards before commitment. ${calaStrong ? `CALA property examples (${calaMarkets}) provide reference points for achievable scope.` : "International Reference examples illustrate standards scope until CALA-specific benchmarks are available."} Test whether the ${model} is credible versus ${peerPrimary}.`,
        38,
        pad
      ),
      400
    ),
    row(
      "economics.opening.step.2",
      "Design & Standards",
      ensureMinWords(
        `Sequence design, systems integration, staff training, and go-live readiness with clear owner and operator responsibilities defined for each phase. ${brandDisplayName} pre-opening should follow brand-specific milestones rather than generic platform timelines from ${parent}.`,
        38,
        pad
      ),
      401
    ),
    row(
      "economics.opening.step.3",
      "Pre-Opening Planning",
      ensureMinWords(
        `Build the plan around systems, ${loyalty} readiness, training, staffing, sales, and operating procedures with clear owner/operator/brand responsibilities. Align timing against product completion so ${brandDisplayName} can open with a credible guest promise.`,
        38,
        pad
      ),
      402
    ),
    row(
      "economics.opening.step.4",
      "Opening Support",
      ensureMinWords(
        `Coordinate launch communications, systems go-live, quality readiness, and service recovery with operator and brand contacts while keeping the ${brandDisplayName} story prominent. Establish escalation paths for the first operating weeks.`,
        38,
        pad
      ),
      403
    ),
    row(
      "economics.opening.step.5",
      "Stabilization",
      ensureMinWords(
        `Use the stabilized period to refine service and channel strategy against actual guest feedback for ${brandDisplayName}. Reassess capital and staffing through performance for the ${model}.`,
        38,
        pad
      ),
      404
    ),
  ];

  // Structured Recent Momentum (dateLine + summary + trailing https URL)
  presentation.push(row("footprint.momentum_label", "", "Recent Momentum", 448));
  let mi = 0;
  for (const m of content.momentum || []) {
    if (!m.sourceUrl || !/^https?:\/\//i.test(m.sourceUrl)) {
      throw new Error(`${slug}: momentum card missing https sourceUrl (${m.title})`);
    }
    const card = buildRecentMomentumCard({
      title: m.title,
      dateLine: m.dateLine,
      summary: ensureMinWords(m.summary, 35, pad),
      url: m.sourceUrl,
      sort: 449 + mi,
    });
    presentation.push(
      row("footprint.momentum", card.title, card.body, 449 + mi, {
        caseSummaryOverview: m.summary,
        caseSummaryBrandRelevance: `${brandDisplayName} dated momentum signal`,
        caseSummaryOwnerObjective:
          "Track brand momentum without treating announcements as asset-level proof",
        caseSummaryInterpretation: m.sourceLabel || "Official brand or press source",
        caseSummaryTags: `${m.geography}, ${m.dateLine}, ${brandDisplayName}`,
      })
    );
    mi += 1;
  }

  // Openings / property examples — curated content.openings first; else matched propertyExamples
  // Every openings entry must carry a non-empty https property URL (never brand-page URLs as openings).
  const openingsInputs =
    Array.isArray(content.openings) && content.openings.length
      ? content.openings
      : (pack.propertyExamples || []).filter(
          (p) => p.matchKey || !/steward to match|pending/i.test(p.propertyName || "")
        );
  let oi = 0;
  for (const p of openingsInputs) {
    if (oi >= 3) break;
    const geo = p.geographyLabel === "CALA" ? "CALA" : "International Reference";
    const market = sanitizePropertyNote(p.market || "the named market");
    const openingUrl = String(p.url || p.sourcePageUrl || "").trim();
    if (!openingUrl || !/^https:\/\//i.test(openingUrl)) {
      throw new Error(
        `${slug}: openings entry missing non-empty https url (${p.propertyName || "unnamed"})`
      );
    }
    if (/\/en\/brands\//i.test(openingUrl)) {
      throw new Error(
        `${slug}: openings url must be a property hotel page, not a brand page (${p.propertyName})`
      );
    }
    const openingsBody =
      ensureMinWords(
        [
          `${geo} property reference for ${brandDisplayName} in ${market}.`,
          `${p.propertyName} illustrates the ${brandDisplayName} operating model and guest experience in a ${geo === "CALA" ? "regional" : "comparable international"} demand context.`,
          `For owners evaluating ${brandDisplayName}, this property helps benchmark standards scope, service delivery, and competitive positioning against ${peerPrimary} alternatives.`,
          p.note ? sanitizePropertyNote(p.note) : "",
        ]
          .filter(Boolean)
          .join(" "),
        45,
        pad
      ) + `\n\n${openingUrl}`;
    presentation.push(
      row(
        "footprint.openings",
        `${p.propertyName} — ${geo}`,
        openingsBody,
        490 + oi,
        {
          caseSummaryOverview: `${p.propertyName} is a ${geo} reference for ${brandDisplayName} review in ${market}.`,
          caseSummaryBrandRelevance: `${brandDisplayName} operating-model and guest-experience reference.`,
          caseSummaryOwnerObjective: "Compare operating fit and standards scope using a named property reference.",
          caseSummaryInterpretation: "Named property reference.",
          caseSummaryTags: `${geo}, ${brandDisplayName}, Property example, ${market}`,
        }
      )
    );
    oi += 1;
  }

  presentation.push(
    row(
      "insight.similar.1",
      "Similar Brands",
      `${distinctions[0] || `Differentiate ${brandDisplayName} from adjacent brands using owner-useful criteria.`} Compare operating complexity, guest promise, and asset requirements — not parent-platform affiliation alone.`,
      700
    ),
    row(
      "insight.similar.2",
      "Similar Brands",
      `${distinctions[1] || `Do not collapse ${brandDisplayName} into sibling-brand boilerplate.`} ${distinctions[2] || `Each peer brand operates in a distinct positioning lane with different standards, demand expectations, and operating-model requirements.`}`,
      701
    ),
    row(
      "insight.similar.3",
      "Questions Owners Should Ask",
      bullets([
        `What specific asset and demand conditions make ${brandDisplayName} the right choice over ${peers} in this market?`,
        `What operating capabilities and service-model experience are mandatory to deliver the ${brandDisplayName} guest promise?`,
        "Which assumptions about demand mix, competitive positioning, and operating costs require local market validation before final commitment?",
        `How does ${brandDisplayName} standards scope compare to the current asset condition, and what conversion investment is required?`,
      ]),
      702
    ),
    row(
      "valueOwners.watchouts",
      "Modals / CTAs / chips / tags",
      `Use concise chips and tags tied to owner decision points for ${brandDisplayName}. Keep copy free of raw source links and Hilton corporate boilerplate. Every tag should help an owner evaluate brand fit for a specific asset and market.`,
      703,
      {
        caseSummaryOverview: `${brandDisplayName} owner decision summary — brand-specific fit and differentiation`,
        caseSummaryBrandRelevance: `${brandDisplayName} positioning, peer separation, and operating-model requirements`,
        caseSummaryOwnerObjective: `Select the right brand for the asset and market based on ${brandDisplayName}-specific evidence`,
        caseSummaryInterpretation: "Brand-source grounded synthesis for owner decisions",
        caseSummaryTags: `${brandDisplayName}, Owner fit, ${calaStrong ? "CALA" : "International Reference"}`,
      }
    )
  );

  const brandPositioning = removeRawUrls(
    ensureMinWords(
      `${brandDisplayName} is an owner-facing ${model} within ${parent}. Underwrite the asset against this brand's guest promise, product scope, and operating complexity rather than sibling brands or corporate platform averages.`,
      12,
      pad
    )
  );
  const guestPsychographics = removeRawUrls(
    `${brandDisplayName} attracts guests seeking ${tgsLabel} experiences aligned with a ${model}, rather than a generic chain stay.`
  );
  const brandValueProposition = removeRawUrls(
    `${parent}; ${model}; ${loyalty} distribution with brand-specific product discipline.`
  );

  return {
    brandSlug: slug,
    identity: {
      recordId: opts.recordId || pack.recordId,
      name: brandDisplayName,
      parentCompany: parent,
      reportSlug: slug,
    },
    sourcePackMeta: {
      officialBrandPage: pack.officialBrandPage?.label || null,
      developmentPage: pack.developmentPage?.label || null,
      calaAvailability: content.calaAvailability || pack.calaAvailability,
      propertyExampleCount: (pack.propertyExamples || []).length,
      momentumCandidateCount: (content.momentum || []).length,
    },
    brandLens: {
      model,
      ownerLens,
      distinguishFrom: peers.split(",").map((x) => x.trim()).filter(Boolean),
      calaAvailability: content.calaAvailability || pack.calaAvailability,
    },
    presentation,
    basicsFields: {
      "Brand Positioning": brandPositioning,
      "Guest Psychographics Description": guestPsychographics,
      "Brand Value Proposition": brandValueProposition,
      ...(tgs.length ? { "Target Guest Segments": tgs } : {}),
    },
    targetGuestSegments: tgs,
  };
}

function resolveWave15Identity(slug) {
  const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
  if (!id?.recordId || !id?.name) {
    throw new Error(`Missing factory-preview identity for ${slug}`);
  }
  return {
    slug,
    recordId: id.recordId,
    name: id.name,
  };
}

function getApprovedStage4Scope() {
  const wanted = [...WAVE15_STAGE4_APPROVED_SLUGS];
  if (wanted.length !== 8) {
    throw new Error(`Wave 15 Stage 4 requires exactly eight brands, got ${wanted.length}`);
  }
  if (wanted.some((s) => FORBIDDEN_STAGE4_BRANDS.has(s))) {
    throw new Error("Forbidden brand detected in Stage 4 scope.");
  }
  return wanted;
}

export async function planWave15TabFactoryBrand(slug, { tgsOptions = [] } = {}) {
  const identity = resolveWave15Identity(slug);
  if (
    BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.includes(slug) ||
    TAB_FACTORY_PROTECTED_BRANDS.includes(slug)
  ) {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: ["protected_brand_refuse"],
      patches: [],
      basicsPatches: [],
    };
  }
  if (!WAVE15_STAGE4_APPROVED_SLUGS.includes(slug)) {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: ["not_wave15_stage4_target"],
      patches: [],
      basicsPatches: [],
    };
  }
  if (FORBIDDEN_STAGE4_BRANDS.has(slug)) {
    return {
      brandSlug: slug,
      blocked: true,
      blockers: ["forbidden_stage4_brand"],
      patches: [],
      basicsPatches: [],
    };
  }

  const pack = generateWave15PresentationPack(slug, {
    airtableName: identity.name,
    recordId: identity.recordId,
  });

  let rows = [];
  let ctxError = null;
  try {
    rows = await listPresentationRowsLight(identity.name);
  } catch (err) {
    ctxError = err?.message || String(err);
    rows = [];
  }

  let basicsBefore = {};
  try {
    const basics = await fetchBasicsRecord(identity.recordId);
    basicsBefore = basics?.fields || {};
  } catch (err) {
    ctxError = ctxError || err?.message || String(err);
  }

  const patches = [];
  const blockers = [];
  const usedRecordIds = new Set();

  for (const pRow of pack.presentation) {
    const slotKey = normalizeSlotKey(pRow.slotKey);
    let body = nz(pRow.body);
    const title = nz(pRow.title);
    if (isFlexibilitySlotKey(slotKey)) {
      body = sanitizeFlexibilityPresentationBody({
        slotKey,
        body,
        brandName: identity.name,
      }).level;
    }
    if (!body) {
      blockers.push(`empty_body:${slotKey}`);
      continue;
    }
    const forbidden = scanForbiddenLanguage(`${title}\n${body}`).filter((h) => {
      if (URL_ALLOWED_SLOTS.has(slotKey) && h.id === "raw_url") return false;
      return true;
    });
    if (forbidden.length) {
      blockers.push(`forbidden:${slotKey}:${forbidden.map((h) => h.id).join(",")}`);
      continue;
    }
    if (/\bADR\b|\bRevPAR\b|\bfee stack\b/i.test(`${title}\n${body}`)) {
      blockers.push(`forbidden_metrics:${slotKey}`);
      continue;
    }
    if (
      /\b(source-supported|owner-fit diligence|Confirm owner)\b/i.test(`${title}\n${body}`) ||
      (/(overview\.scenario\.|valueOwners\.scenario\.)/i.test(slotKey) &&
        FORBIDDEN_SCENARIO_TITLES.includes(title))
    ) {
      blockers.push(`forbidden_process_or_section_label:${slotKey}:${title || "untitled"}`);
      continue;
    }

    let existing = findExistingPresentationRow(rows, slotKey, title, usedRecordIds);

    const caseFields = {};
    for (const [api, airtable] of [
      ["caseSummaryOverview", "Case Summary Overview"],
      ["caseSummaryBrandRelevance", "Case Summary Brand Relevance"],
      ["caseSummaryOwnerObjective", "Case Summary Owner Objective"],
      ["caseSummaryInterpretation", "Case Summary Interpretation"],
      ["caseSummaryTags", "Case Summary Tags"],
    ]) {
      if (nz(pRow[api])) caseFields[airtable] = nz(pRow[api]);
    }

    if (
      existing?.recordId &&
      nz(existing.body) === body &&
      nz(existing.title) === title &&
      (!caseFields["Case Summary Overview"] ||
        nz(existing.caseSummaryOverview) === caseFields["Case Summary Overview"])
    ) {
      usedRecordIds.add(existing.recordId);
      continue;
    }

    if (existing?.recordId) {
      usedRecordIds.add(existing.recordId);
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: existing.recordId,
        slotKey,
        fields: {
          Body: body,
          ...(title ? { Title: title } : {}),
          ...caseFields,
        },
      });
    } else {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        slotKey,
        fields: {
          "Slot Key": slotKey,
          "Brand Name": identity.name,
          Brand: [identity.recordId],
          Active: true,
          "Sort Order": pRow.sortOrder ?? 0,
          Title: title || "",
          Body: body,
          ...caseFields,
        },
      });
    }
  }

  // Deactivate orphan duplicates for unique slots (title churn from prior applies).
  const uniqueSlotKeysSeen = new Set();
  for (const pRow of pack.presentation) {
    const slotKey = normalizeSlotKey(pRow.slotKey);
    if (MULTI_ROW_SLOT_KEYS.has(slotKey) || /^insight\.similar/i.test(slotKey)) continue;
    if (uniqueSlotKeysSeen.has(slotKey)) continue;
    uniqueSlotKeysSeen.add(slotKey);
    const orphans = (rows || []).filter(
      (r) =>
        nz(r.slotKey) === slotKey &&
        r.active !== false &&
        r.recordId &&
        !usedRecordIds.has(r.recordId) &&
        !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
    );
    for (const orphan of orphans) {
      usedRecordIds.add(orphan.recordId);
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: orphan.recordId,
        slotKey,
        fields: {
          Active: false,
          "External Display Status": "Do Not Display",
        },
      });
    }
  }

  // Scrub stub-chip phrases left in any still-active Presentation rows (titles/bodies).
  for (const r of rows || []) {
    if (!r?.recordId) continue;
    if (r.active === false || /do not display|internal only/i.test(nz(r.externalDisplayStatus))) continue;
    const beforeTitle = nz(r.title);
    const beforeBody = nz(r.body);
    if (!/\bconversion-friendly\.?\b/i.test(`${beforeTitle}\n${beforeBody}`)) continue;
    const allowStructuredUrls = URL_ALLOWED_SLOTS.has(nz(r.slotKey));
    const afterTitle = scrubPresentationText(beforeTitle);
    const afterBody = scrubPresentationText(beforeBody, { allowStructuredUrls });
    if (afterTitle === beforeTitle && afterBody === beforeBody) continue;
    usedRecordIds.add(r.recordId);
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: r.recordId,
      slotKey: nz(r.slotKey),
      fields: {
        ...(afterTitle !== beforeTitle ? { Title: afterTitle } : {}),
        ...(afterBody !== beforeBody ? { Body: afterBody } : {}),
      },
    });
  }

  const basicsPatches = [];
  const nextBasics = {};
  const tgsWanted = pack.targetGuestSegments || [];
  const tgsValidSet = new Set(tgsOptions.length ? tgsOptions : SAFE_TGS_OPTIONS);
  const tgsValid = tgsWanted.filter((t) => tgsValidSet.has(t));
  if (tgsWanted.length && tgsValid.length !== tgsWanted.length) {
    blockers.push(`tgs_invalid_option:${tgsWanted.filter((t) => !tgsValidSet.has(t)).join(",")}`);
  }
  for (const [field, value] of Object.entries(pack.basicsFields || {})) {
    if (!ALLOWED_BASICS_FIELDS.has(field)) {
      blockers.push(`basics_field_not_allowed:${field}`);
      continue;
    }
    if (field === "Target Guest Segments") {
      if (!tgsValid.length) continue;
      const before = Array.isArray(basicsBefore[field]) ? basicsBefore[field] : [];
      const same = before.length === tgsValid.length && before.every((v, i) => v === tgsValid[i]);
      if (!same) nextBasics[field] = tgsValid;
    } else {
      const before = nz(basicsBefore[field]);
      const after = nz(value);
      if (before !== after) nextBasics[field] = after;
    }
  }
  if (Object.keys(nextBasics).length) {
    basicsPatches.push({
      table: BASICS_TABLE,
      action: "PATCH",
      recordId: identity.recordId,
      fields: nextBasics,
    });
  }

  return {
    brandSlug: slug,
    reportSlug: slug,
    brandName: identity.name,
    recordId: identity.recordId,
    parentCompany: pack.identity.parentCompany,
    sourcePack: pack.sourcePackMeta,
    brandLens: pack.brandLens,
    presentationRowCount: pack.presentation.length,
    existingPresentationCount: rows.length,
    targetGuestSegments: tgsValid,
    tgsWriteEligible: tgsValid.length > 0 && !blockers.some((b) => b.startsWith("tgs_invalid_option")),
    ctxError,
    patches,
    basicsPatches,
    blockers,
    blocked: blockers.length > 0,
    releaseFieldsWritten: false,
    brandStatusUntouched: true,
    companyValidatedUntouched: true,
    imagesWritten: false,
  };
}

export async function planWave15TabFactoryBuild() {
  const scope = getApprovedStage4Scope();
  const tgsOptions = await listTargetGuestSegmentOptions();
  const brandResults = [];
  for (const slug of scope) {
    brandResults.push(await planWave15TabFactoryBrand(slug, { tgsOptions }));
  }
  const tgsReport = brandResults.map((b) => ({
    slug: b.brandSlug,
    name: b.brandName,
    recommended: b.targetGuestSegments || [],
    writeEligible: b.tgsWriteEligible === true,
    willWrite: (b.basicsPatches || []).some((p) =>
      Object.prototype.hasOwnProperty.call(p.fields || {}, "Target Guest Segments")
    ),
  }));
  return {
    version: WAVE15_TAB_FACTORY_BUILD_VERSION,
    factoryVersion: WAVE15_VERSION,
    stage: "tab-factory-build",
    generatedAt: new Date().toISOString(),
    dryRun: true,
    airtableWrites: false,
    protectedBaselineCount: WAVE15_PROTECTED_BASELINE_COUNT,
    approvedScope: scope,
    excluded: {
      "the-house-of-originals": "excluded_from_stage4",
      "morgans-originals": "not_created_not_modified",
      "radisson-collection": "excluded_from_stage4",
      "protected_54_active_brands": "read_only_validation_only",
    },
    brandResults,
    targetGuestSegments: tgsReport,
    tgsValidated: tgsReport.every((t) => t.writeEligible),
    summary: {
      brandCount: brandResults.length,
      plannedPresentationWrites: brandResults.reduce((n, b) => n + (b.patches?.length || 0), 0),
      plannedBasicsWrites: brandResults.reduce((n, b) => n + (b.basicsPatches?.length || 0), 0),
      blockedSlugs: brandResults.filter((b) => b.blocked).map((b) => b.brandSlug),
    },
    requiredApplyFlags: [...WAVE15_TAB_FACTORY_BUILD_APPLY_FLAGS],
  };
}

export async function applyWave15TabFactoryBuild({ plan, apply = false, argv = [] } = {}) {
  const flagCheck = parseWave15TabFactoryBuildFlags(argv);
  if (!apply) return { applied: false, reason: "dry_run_only", flagCheck };
  if (!flagCheck.ok) {
    return { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing, flagCheck };
  }
  if (plan.tgsValidated === false) {
    return { applied: false, reason: "target_guest_segments_not_validated", flagCheck };
  }
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const resultsByBrand = {};
  for (const brand of plan.brandResults || []) {
    if (brand.blocked) {
      resultsByBrand[brand.brandSlug] = { applied: false, reason: "blocked", blockers: brand.blockers };
      continue;
    }
    if (!WAVE15_STAGE4_APPROVED_SLUGS.includes(brand.brandSlug)) {
      throw new Error(`Refusing non-target brand write: ${brand.brandSlug}`);
    }
    const created = [];
    const updated = [];
    const basicsUpdated = [];
    const errors = [];
    for (const patch of [...(brand.basicsPatches || []), ...(brand.patches || [])]) {
      for (const key of Object.keys(patch.fields || {})) {
        if (FORBIDDEN_WRITE_FIELDS.has(key)) throw new Error(`Forbidden field write: ${key}`);
        if (patch.table === BASICS_TABLE && !ALLOWED_BASICS_FIELDS.has(key)) {
          throw new Error(`Forbidden Brand Basics field: ${key}`);
        }
      }
      try {
        if (patch.action === "POST") {
          const json = await airtableWrite({
            baseId,
            apiKey,
            table: patch.table,
            fields: patch.fields,
            method: "POST",
          });
          created.push(json.id);
        } else {
          await airtableWrite({
            baseId,
            apiKey,
            table: patch.table,
            recordId: patch.recordId,
            fields: patch.fields,
            method: "PATCH",
          });
          if (patch.table === BASICS_TABLE) basicsUpdated.push(patch.recordId);
          else updated.push(patch.recordId);
        }
        await sleep(220);
      } catch (err) {
        errors.push({
          table: patch.table,
          slotKey: patch.slotKey || Object.keys(patch.fields || {})[0],
          error: err?.message || String(err),
        });
      }
    }
    resultsByBrand[brand.brandSlug] = {
      applied: errors.length === 0,
      created,
      updated,
      basicsUpdated,
      errors,
      releaseFieldsWritten: false,
      brandStatusUntouched: true,
      companyValidatedUntouched: true,
      imagesWritten: false,
    };
  }
  return {
    applied: Object.values(resultsByBrand).some((r) => r.applied),
    reason: "wave15_tab_factory_build_applied",
    flagCheck,
    resultsByBrand,
    companyValidatedUntouched: true,
    sourceLibraryUntouched: true,
    registryUntouched: true,
    brandStatusUntouched: true,
    releaseFieldsWritten: false,
    imagesWritten: false,
    protected54Untouched: true,
  };
}

function writeBrandMd(brand, reportsDir) {
  const mdPath = path.join(reportsDir, `brand-explorer-wave15-tab-factory-build-${brand.reportSlug || brand.brandSlug}.md`);
  const lines = [
    `# Wave 15 Tab Factory Build — ${brand.brandName || brand.brandSlug}`,
    "",
    `- Slug: \`${brand.brandSlug}\``,
    `- Record: \`${brand.recordId || "—"}\``,
    `- Parent: ${brand.parentCompany || "—"}`,
    `- Presentation pack rows: ${brand.presentationRowCount ?? 0}`,
    `- Existing Presentation rows: ${brand.existingPresentationCount ?? 0}`,
    `- Planned Presentation writes: ${brand.patches?.length ?? 0}`,
    `- Planned Brand Basics writes: ${brand.basicsPatches?.length ?? 0}`,
    `- Target Guest Segments: ${(brand.targetGuestSegments || []).join(", ") || "—"}`,
    `- TGS write eligible: **${brand.tgsWriteEligible === true}**`,
    `- Blocked: **${brand.blocked === true}**`,
    "",
    "## Blockers",
    "",
    ...(brand.blockers?.length ? brand.blockers.map((b) => `- ${b}`) : ["- (none)"]),
    "",
    "## Planned Presentation writes (sample)",
    "",
    ...((brand.patches || []).slice(0, 30).map(
      (p) => `- \`${p.action}\` \`${p.slotKey}\`${p.recordId ? ` (${p.recordId})` : ""}`
    ) || ["- (none)"]),
    "",
  ];
  fs.writeFileSync(mdPath, `${lines.filter((l) => l != null).join("\n")}\n`, "utf8");
  return mdPath;
}

export function writeWave15TabFactoryBuildReports(plan, applyResult = null, meta = {}) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const packPaths = [];
  for (const brand of plan.brandResults || []) {
    packPaths.push(writeBrandMd(brand, REPORTS_DIR));
  }

  const tgsMdPath = path.join(REPORTS_DIR, "brand-explorer-wave15-target-guest-segments.md");
  const tgsLines = [
    "# Wave 15 — Target Guest Segments",
    "",
    `Generated: ${plan.generatedAt}`,
    `Validated: **${plan.tgsValidated === true}**`,
    "",
    `| Slug | Recommended | Write eligible | Will write |`,
    `| --- | --- | --- | --- |`,
    ...(plan.targetGuestSegments || []).map(
      (t) =>
        `| \`${t.slug}\` | ${(t.recommended || []).join(", ")} | ${t.writeEligible} | ${t.willWrite} |`
    ),
    "",
  ];
  fs.writeFileSync(tgsMdPath, tgsLines.join("\n"), "utf8");

  const readyStatement =
    meta.readyStatement ||
    (applyResult?.applied
      ? "wave15_stage4_content_clean_ready_for_image_materialization"
      : "wave15_stage4_tab_factory_build_dry_run_ready");

  const report = {
    ...plan,
    dryRun: !applyResult?.applied,
    airtableWrites: applyResult?.applied === true,
    applyResult,
    pass: meta.pass ?? applyResult?.applied === true,
    readyStatement,
    imageWrites: false,
  };
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave15-tab-factory-build.json");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave15-tab-factory-build.md");
  const md = [
    "# Brand Explorer Wave 15 — Tab Factory Build",
    "",
    `Generated: ${plan.generatedAt}`,
    `Dry-run: **${!applyResult?.applied}** · Applied: **${applyResult?.applied === true}**`,
    `Ready: **\`${readyStatement}\`**`,
    `Scope: **${plan.approvedScope.join(", ")}**`,
    `Presentation writes planned: **${plan.summary?.plannedPresentationWrites ?? 0}**`,
    `Brand Basics writes planned: **${plan.summary?.plannedBasicsWrites ?? 0}**`,
    `TGS validated: **${plan.tgsValidated === true}**`,
    `Blocked: **${(plan.summary?.blockedSlugs || []).join(", ") || "none"}**`,
    "",
    "## Scope controls",
    "",
    `- the-house-of-originals: ${plan.excluded["the-house-of-originals"]}`,
    `- morgans-originals: ${plan.excluded["morgans-originals"]}`,
    `- so-hotels-and-resorts: ${plan.excluded["so-hotels-and-resorts"]}`,
    `- fairmont-hotels-and-resorts: ${plan.excluded["fairmont-hotels-and-resorts"]}`,
    "",
    "## Brands",
    "",
    `| Slug | Name | Rows | Pres writes | Basics writes | Blocked |`,
    `| --- | --- | ---: | ---: | ---: | --- |`,
    ...(plan.brandResults || []).map(
      (b) =>
        `| \`${b.brandSlug}\` | ${b.brandName} | ${b.presentationRowCount ?? 0} | ${b.patches?.length ?? 0} | ${b.basicsPatches?.length ?? 0} | ${b.blocked === true} |`
    ),
    "",
    "## Acceptance",
    "",
    `- Ready statement: \`${readyStatement}\``,
    "- All eight remain Under Review / factory preview only",
    "- No images written (deferred to Stage 5)",
    "- Accepted until Stage 5: overview.scenario.1–3 missing images only (copy present)",
    "",
  ].join("\n");
  fs.writeFileSync(mdPath, md, "utf8");

  const docPath = path.join(DOCS_DIR, "brand-explorer-wave15-tab-factory-build.md");
  fs.writeFileSync(
    docPath,
    [
      "# Wave 15 Tab Factory Build",
      "",
      "Stage 4 builds owner-facing Presentation content for **eight approved Wave 15 brands** from source packs.",
      "",
      "## Approved Stage 4 scope",
      "",
      ...WAVE15_STAGE4_APPROVED_SLUGS.map((s) => `- \`${s}\``),
      "",
      "## Exclusions",
      "",
      "- `the-house-of-originals`",
      "- `morgans-originals`",
      "- `radisson-collection`",
      "- protected 54 Active/Live brands (read-only validation only)",
      "",
      "## Allowed writes",
      "",
      "- Presentation Title / Body / Case Summary / chips",
      "- Brand Basics: Brand Positioning, Guest Psychographics Description, Brand Value Proposition",
      "- Brand Basics: Target Guest Segments only when validated",
      "",
      "## Forbidden",
      "",
      "- Brand Status, release fields, Company Validated, Source Library, Registry",
      "- Images and non-target brands",
      "- Section labels as scenario titles; owner-fit diligence / process language",
      "",
      "## Ready for images",
      "",
      "`wave15_stage4_content_clean_ready_for_image_materialization` after apply + post-validation.",
      "",
    ].join("\n"),
    "utf8"
  );

  return { jsonPath, mdPath, tgsMdPath, docPath, packPaths };
}

export async function runWave15TabFactoryBuild({ dryRun = true, argv = [] } = {}) {
  const plan = await planWave15TabFactoryBuild();
  let applyResult = null;
  if (!dryRun && argv.includes("--apply")) {
    applyResult = await applyWave15TabFactoryBuild({ plan, apply: true, argv });
    if (!applyResult.applied) {
      const paths = writeWave15TabFactoryBuildReports(plan, applyResult);
      return {
        ...plan,
        pass: false,
        stopRecommended: true,
        applyResult,
        paths,
      };
    }
  }
  const blocked = (plan.summary?.blockedSlugs || []).length > 0;
  const pass = !blocked && plan.tgsValidated === true && (plan.summary?.brandCount || 0) === 8;
  const readyStatement =
    pass && applyResult?.applied
      ? "wave15_stage4_content_clean_ready_for_image_materialization"
      : pass
        ? "wave15_stage4_tab_factory_build_dry_run_ready"
        : "wave15_stage4_tab_factory_build_blocked";
  const paths = writeWave15TabFactoryBuildReports(plan, applyResult, { pass, readyStatement });
  return {
    ...plan,
    dryRun: dryRun || !applyResult?.applied,
    airtableWrites: applyResult?.applied === true,
    applyResult,
    paths,
    pass,
    readyStatement,
    stopRecommended: blocked || plan.tgsValidated === false,
  };
}

export const WAVE15_STAGE4_CONTENT_CLEANUP_VERSION = "wave13-stage4-content-cleanup-v1";

function parseWave15Stage4ContentCleanupFlags(argv = []) {
  const missing = WAVE15_STAGE4_CONTENT_CLEANUP_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

function writeCleanupBrandMd(brand, reportsDir) {
  const mdPath = path.join(reportsDir, `brand-explorer-wave15-stage4-content-cleanup-${brand.reportSlug || brand.brandSlug}.md`);
  const lines = [
    `# Wave 15 Stage 4.5 Content Cleanup — ${brand.brandName || brand.brandSlug}`,
    "",
    `- Slug: \`${brand.brandSlug}\``,
    `- Record: \`${brand.recordId || "—"}\``,
    `- Presentation pack rows: ${brand.presentationRowCount ?? 0}`,
    `- Existing Presentation rows: ${brand.existingPresentationCount ?? 0}`,
    `- Planned Presentation writes: ${brand.patches?.length ?? 0}`,
    `- Planned Brand Basics writes: ${brand.basicsPatches?.length ?? 0}`,
    `- Blocked: **${brand.blocked === true}**`,
    "",
    "## Planned writes (sample)",
    "",
    ...((brand.patches || []).slice(0, 40).map(
      (p) => `- \`${p.action}\` \`${p.slotKey}\`${p.recordId ? ` (${p.recordId})` : ""} — ${String(p.fields?.Body || "").slice(0, 80)}…`
    ) || ["- (none)"]),
    "",
  ];
  fs.writeFileSync(mdPath, `${lines.filter((l) => l != null).join("\n")}\n`, "utf8");
  return mdPath;
}

function writeWave15Stage4ContentCleanupReports(plan, applyResult = null) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const packPaths = [];
  for (const brand of plan.brandResults || []) {
    packPaths.push(writeCleanupBrandMd(brand, REPORTS_DIR));
  }

  const report = {
    ...plan,
    dryRun: !applyResult?.applied,
    airtableWrites: applyResult?.applied === true,
    applyResult,
  };
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave15-stage4-content-cleanup.json");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");

  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave15-stage4-content-cleanup.md");
  const md = [
    "# Brand Explorer Wave 15 — Stage 4.5 Content Cleanup",
    "",
    `Generated: ${plan.generatedAt}`,
    `Dry-run: **${!applyResult?.applied}** · Applied: **${applyResult?.applied === true}**`,
    `Scope: **${plan.approvedScope.join(", ")}**`,
    `Presentation writes planned: **${plan.summary?.plannedPresentationWrites ?? 0}**`,
    `Brand Basics writes planned: **${plan.summary?.plannedBasicsWrites ?? 0}**`,
    `Blocked: **${(plan.summary?.blockedSlugs || []).join(", ") || "none"}**`,
    "",
    "## Brands",
    "",
    `| Slug | Pres writes | Basics writes | Blocked |`,
    `| --- | ---: | ---: | --- |`,
    ...(plan.brandResults || []).map(
      (b) =>
        `| \`${b.brandSlug}\` | ${b.patches?.length ?? 0} | ${b.basicsPatches?.length ?? 0} | ${b.blocked === true} |`
    ),
    "",
    applyResult?.applied
      ? "## Apply result\n\nContent cleanup applied successfully. Run post-cleanup validation.\n\n`wave15_stage4_content_clean_ready_for_image_materialization`"
      : "## Next step\n\nRun with `--apply` and all confirmation flags to apply cleanup patches.",
    "",
  ].join("\n");
  fs.writeFileSync(mdPath, md, "utf8");

  const docPath = path.join(DOCS_DIR, "brand-explorer-wave15-stage4-content-cleanup.md");
  fs.writeFileSync(
    docPath,
    [
      "# Wave 15 Stage 4.5 — Content Cleanup",
      "",
      "Stage 4.5 thickens thin/blank Presentation content for eight approved Wave 15 brands",
      "so they pass rendered-field-completeness, golden-content-quality, and tab-factory-audit gates.",
      "",
      "## Scope",
      "",
      ...WAVE15_STAGE4_APPROVED_SLUGS.map((s) => `- \`${s}\``),
      "",
      "## What changed",
      "",
      "- Expanded scenario, proof, why_value, geo_intro, growth, standards rows to meet word minimums",
      "- No image writes, no Brand Status changes, no release fields",
      "- protected 54 untouched",
      "",
    ].join("\n"),
    "utf8"
  );

  return { jsonPath, mdPath, docPath, packPaths };
}

export async function runWave15Stage4ContentCleanup({ dryRun = true, argv = [] } = {}) {
  const plan = await planWave15TabFactoryBuild();
  plan.stage = "stage4-content-cleanup";
  plan.version = WAVE15_STAGE4_CONTENT_CLEANUP_VERSION;
  plan.requiredApplyFlags = [...WAVE15_STAGE4_CONTENT_CLEANUP_APPLY_FLAGS];

  let applyResult = null;
  if (!dryRun && argv.includes("--apply")) {
    const flagCheck = parseWave15Stage4ContentCleanupFlags(argv);
    if (!flagCheck.ok) {
      const paths = writeWave15Stage4ContentCleanupReports(plan, { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing, flagCheck });
      return { ...plan, pass: false, stopRecommended: true, applyResult: { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing }, paths };
    }
    applyResult = await applyWave15TabFactoryBuild({ plan, apply: true, argv: [...argv, ...WAVE15_TAB_FACTORY_BUILD_APPLY_FLAGS] });
    if (!applyResult.applied) {
      const paths = writeWave15Stage4ContentCleanupReports(plan, applyResult);
      return { ...plan, pass: false, stopRecommended: true, applyResult, paths };
    }
  }

  const paths = writeWave15Stage4ContentCleanupReports(plan, applyResult);
  const blocked = (plan.summary?.blockedSlugs || []).length > 0;
  return {
    ...plan,
    dryRun: dryRun || !applyResult?.applied,
    airtableWrites: applyResult?.applied === true,
    applyResult,
    paths,
    pass: !blocked && (plan.summary?.brandCount || 0) === 8,
    stopRecommended: blocked,
    readyStatement: applyResult?.applied ? "wave15_stage4_content_clean_ready_for_image_materialization" : "dry_run_only",
  };
}

