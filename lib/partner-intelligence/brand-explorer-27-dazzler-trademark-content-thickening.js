/**
 * Brand Explorer — Dazzler + Trademark content thickness cleanup for 27-brand PVQL.
 *
 * Allowed writes (target brands only):
 * - Presentation Title / Body
 * - Case Summary fields (openings thin cards)
 * - Brand Basics Guest Psychographics / Brand Positioning (audience gate)
 *
 * Forbidden: images, CV, Source Library, Registry, Brand Status, release fields,
 * Tapestry, protected 24, Radisson Collection, broad rewrites.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getFullBuildContent, resolveFullBuildSlug } from "./brand-explorer-full-build-content.js";
import {
  listPresentationRowsLight,
  resolveLane2BrandIdentity,
} from "./brand-explorer-lane2-common.js";
import { scanForbiddenLanguage } from "./brand-explorer-v40b-copy-quality-patterns.js";
import {
  buildRecentMomentumCard,
  RECENT_MOMENTUM_DEFAULT_LABEL,
  withRecentMomentumSortOrder,
} from "./brand-explorer-recent-momentum-contract.js";
import { LANE2_PROPERTY_CATALOG_BY_SLUG } from "./brand-explorer-lane2-property-catalog.js";
import { auditBrandRenderedFieldCompleteness } from "./brand-explorer-rendered-field-completeness-audit.js";

export const CONTENT_THICKEN_VERSION = "27-dazzler-trademark-content-thickening-v1";

export const TARGET_SLUGS = Object.freeze([
  "dazzler-by-wyndham",
  "trademark-collection-by-wyndham",
]);

export const FORBIDDEN_BRAND_SLUGS = Object.freeze([
  "tapestry-collection-by-hilton",
  "radisson-collection",
]);

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-content-thickening",
  "--confirm-target-brands-only",
  "--confirm-targeted-field-fixes-only",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-changes",
  "--confirm-no-image-writes",
  "--confirm-no-protected-24-brand-changes",
  "--confirm-no-tapestry-changes",
  "--confirm-no-broad-rewrites",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS_TABLE = "Brand Setup - Brand Basics";

const FORBIDDEN_WRITE_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Brand Status",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Image",
  "Source Library Status",
  "Registry Approval",
  "Registry Status",
]);

const THICKEN_SLOTS = Object.freeze([
  "overview.scenario.1",
  "overview.scenario.2",
  "overview.scenario.3",
  "overview.why_value",
  "overview.proof.1",
  "overview.proof.2",
  "overview.proof.3",
  "overview.proof.4",
  "Brand Positioning",
  "Guest Psychographics Description",
  "operations.operator_compat.tags",
  "valueOwners.lifecycle.3",
  "valueOwners.lifecycle.4",
  "valueOwners.lifecycle.5",
  "economics.opening.step.2",
]);

const BASICS_CHIP_FIELDS = Object.freeze({
  "dazzler-by-wyndham": {
    "Brand Value Proposition":
      "Defined design lifestyle identity; Wyndham Rewards distribution; upscale urban and regional commercial fit; design-review gated affiliation.",
    "Key Brand Differentiators":
      "Design-led lifestyle template distinct from Trademark Collection; Latin America urban concentration historically; Wyndham Rewards; aesthetic standards diligence.",
  },
  "trademark-collection-by-wyndham": {
    "Brand Value Proposition":
      "Retain property identity; Wyndham Rewards loyalty; accessible soft-brand conversion path; independent-character distribution reach.",
    "Key Brand Differentiators":
      "Each property keeps its name and character; more flexible than Dazzler's defined lifestyle template; Wyndham systems; standards diligence still applies.",
  },
});

const SHORT = Object.freeze({
  "dazzler-by-wyndham": "dazzler",
  "trademark-collection-by-wyndham": "trademark",
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function words(text) {
  return nz(text)
    .split(/\s+/)
    .filter(Boolean).length;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function scrubStubChipLanguage(text) {
  return nz(text)
    .replace(/\bconversion-friendly\.?\b/gi, "accessible conversion path")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function validateOwnerCopy(body, context, { allowUrls = false } = {}) {
  const cleaned = scrubStubChipLanguage(body);
  const forbidden = scanForbiddenLanguage(cleaned).filter((hit) => {
    if (allowUrls && hit.id === "raw_url") return false;
    return true;
  });
  if (forbidden.length) {
    return { ok: false, cleaned, reason: `forbidden:${forbidden.map((f) => f.id || f.label).join(",")}` };
  }
  if (words(cleaned) < 8) {
    return { ok: false, cleaned, reason: `too_thin_${words(cleaned)}` };
  }
  return { ok: true, cleaned, reason: null, context };
}

function isHidden(row) {
  return (
    row?.active === false ||
    /do not display|internal only/i.test(nz(row?.externalDisplayStatus))
  );
}

function packRow(pack, slotKey) {
  const rows = pack?.presentation || pack?.rows || [];
  return rows.find((r) => nz(r.slotKey) === slotKey) || null;
}

function packRows(pack, slotKey) {
  const rows = pack?.presentation || pack?.rows || [];
  return rows.filter((r) => nz(r.slotKey) === slotKey);
}

function visibleRows(rows, slotKey) {
  return (rows || []).filter((r) => nz(r.slotKey) === slotKey && !isHidden(r));
}

export function resolveTargetBrands(requested = []) {
  const list = (requested || []).map((s) => resolveFullBuildSlug(s) || s).filter(Boolean);
  if (!list.length) return [...TARGET_SLUGS];
  for (const s of list) {
    if (!TARGET_SLUGS.includes(s)) {
      throw new Error(`Targets only: ${TARGET_SLUGS.join(", ")} (refused ${s})`);
    }
    if (FORBIDDEN_BRAND_SLUGS.includes(s)) {
      throw new Error(`Forbidden brand: ${s}`);
    }
  }
  return list;
}

export function parseContentThickenApplyFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

function momentumCardsFor(brandSlug) {
  const catalog = LANE2_PROPERTY_CATALOG_BY_SLUG[brandSlug] || [];
  const p = (i) => catalog[i] || catalog[0] || null;
  if (brandSlug === "dazzler-by-wyndham") {
    return withRecentMomentumSortOrder([
      buildRecentMomentumCard({
        title: `${p(0)?.propertyName || "Dazzler Buenos Aires Palermo"} urban lifestyle affiliation signal`,
        dateLine: "2024",
        summary: `${p(0)?.propertyName || "Dazzler Palermo"} shows Dazzler's design-led upscale lifestyle path in a Latin America gateway market for owners comparing defined aesthetic identity with Wyndham Rewards distribution.`,
        url:
          p(0)?.sourcePageUrl ||
          "https://www.wyndhamhotels.com/dazzler/buenos-aires-argentina/dazzler-palermo-buenos-aires/overview",
        sort: 1,
      }),
      buildRecentMomentumCard({
        title: `${p(1)?.propertyName || "Dazzler Buenos Aires Recoleta"} design-forward urban reference`,
        dateLine: "2023",
        summary: `${p(1)?.propertyName || "Dazzler Recoleta"} illustrates Dazzler's urban lifestyle guest promise for owners underwriting design intensity and Wyndham commercial systems on a city asset.`,
        url:
          p(1)?.sourcePageUrl ||
          "https://www.wyndhamhotels.com/dazzler/buenos-aires-argentina/dazzler-recoleta/overview",
        sort: 2,
      }),
      buildRecentMomentumCard({
        title: `${p(2)?.propertyName || "Dazzler Buenos Aires San Martin"} gateway lifestyle signal`,
        dateLine: "2022",
        summary: `${p(2)?.propertyName || "Dazzler San Martin"} is a gateway urban reference for owners evaluating Dazzler's defined design template and Wyndham Rewards participation on a commercial Latin America site.`,
        url:
          p(2)?.sourcePageUrl ||
          "https://www.wyndhamhotels.com/dazzler/buenos-aires-argentina/dazzler-san-martin/overview",
        sort: 3,
      }),
    ]);
  }
  return withRecentMomentumSortOrder([
    buildRecentMomentumCard({
      title: `${p(0)?.propertyName || "MB Hotel Miami Beach"} Trademark Collection affiliation signal`,
      dateLine: "2024",
      summary: `${p(0)?.propertyName || "MB Hotel"} shows Trademark Collection's independent-character soft-brand path for owners comparing Wyndham Rewards distribution while retaining property-specific identity.`,
      url:
        p(0)?.sourcePageUrl ||
        "https://www.wyndhamhotels.com/trademark/miami-beach-florida/mb-hotel-trademark-collection-by-wyndham/overview",
      sort: 1,
    }),
    buildRecentMomentumCard({
      title: `${p(1)?.propertyName || "Chula Vista Resort"} Trademark Collection destination reference`,
      dateLine: "2023",
      summary: `${p(1)?.propertyName || "Chula Vista Resort"} illustrates Trademark Collection's destination-leisure conversion fit for owners underwriting property-specific guest experiences alongside Wyndham systems.`,
      url:
        p(1)?.sourcePageUrl ||
        "https://www.wyndhamhotels.com/trademark/wisconsin-dells-wisconsin/chula-vista-resort-trademark/overview",
      sort: 2,
    }),
    buildRecentMomentumCard({
      title: `${p(2)?.propertyName || "The Walden"} Trademark Collection leisure signal`,
      dateLine: "2022",
      summary: `${p(2)?.propertyName || "The Walden"} is a leisure-market Trademark Collection reference for owners evaluating independent identity retention, design-review scope, and Wyndham platform access.`,
      url:
        p(2)?.sourcePageUrl ||
        "https://www.wyndhamhotels.com/trademark/pigeon-forge-tennessee/the-walden-trademark-collection/overview",
      sort: 3,
    }),
  ]);
}

function structuredMomentumOk(rows) {
  const existing = visibleRows(rows, "footprint.momentum");
  return existing.filter((m) => {
    const body = nz(m.body);
    const hasUrl = /https?:\/\//i.test(body);
    const hasDate = /^\d{4}/m.test(body) || /\bQ[1-4]\s+\d{4}\b/i.test(body);
    const diligence =
      /owner diligence|directional themes|illustrative activity|confirm current activity|directional collection/i.test(
        body
      );
    return hasUrl && hasDate && !diligence && words(body) >= 20 && nz(m.title);
  }).length;
}

async function listPresentationRowsDetailed(brandName) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) return [];
  const formula = `{Brand Name}='${nz(brandName).replace(/'/g, "\\'")}'`;
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `Presentation list failed: ${res.status}`);
    for (const rec of json.records || []) {
      const f = rec.fields || {};
      const image = f.Image;
      rows.push({
        recordId: rec.id,
        slotKey: nz(f["Slot Key"]),
        title: nz(f.Title),
        body: nz(f.Body),
        caseSummaryOverview: nz(f["Case Summary Overview"]),
        caseSummaryBrandRelevance: nz(f["Case Summary Brand Relevance"]),
        caseSummaryOwnerObjective: nz(f["Case Summary Owner Objective"]),
        caseSummaryInterpretation: nz(f["Case Summary Interpretation"]),
        caseSummaryTags: nz(f["Case Summary Tags"]),
        externalDisplayStatus: nz(f["External Display Status"]),
        active: f.Active !== false,
        imageUrl: Array.isArray(image) && image[0]?.url ? nz(image[0].url) : "",
        sortOrder: f["Sort Order"] || 0,
      });
    }
    offset = json.offset || "";
  } while (offset);
  return rows;
}

function openingsCaseSummaryFields(brandSlug, propertyName) {
  if (brandSlug === "dazzler-by-wyndham") {
    return {
      "Case Summary Overview": `${propertyName} is an official Dazzler by Wyndham property reference for owners comparing design-led upscale lifestyle affiliation in Latin America urban markets.`,
      "Case Summary Brand Relevance":
        "Shows Dazzler's defined design personality and guest-experience intensity under Wyndham Rewards distribution — distinct from Trademark Collection's independent soft-brand path.",
      "Case Summary Owner Objective":
        "Use when underwriting design capital, aesthetic standards, and whether the asset can support Dazzler's lifestyle guest promise without diluting brand identity.",
      "Case Summary Interpretation":
        "Confirm current design-review criteria, systems scope, and market authorization with Wyndham development before underwriting conversion or new-build fit.",
      "Case Summary Tags": "Dazzler, Lifestyle, Urban, Wyndham Rewards, Property example",
    };
  }
  return {
    "Case Summary Overview": `${propertyName} is an official Trademark Collection by Wyndham property reference for owners comparing independent-character soft-brand affiliation.`,
    "Case Summary Brand Relevance":
      "Shows Trademark Collection's property-specific identity retention with Wyndham Rewards distribution — distinct from Dazzler's defined lifestyle design template.",
    "Case Summary Owner Objective":
      "Use when underwriting conversion scope, owner control over positioning, and whether the asset can support collection guest-experience expectations.",
    "Case Summary Interpretation":
      "Confirm current acceptance criteria, improvement expectations, and systems participation with Wyndham development before underwriting affiliation fit.",
    "Case Summary Tags": "Trademark Collection, Soft brand, Independent, Wyndham Rewards, Property example",
  };
}

export async function planContentThickeningForBrand(brandSlug) {
  const slug = resolveFullBuildSlug(brandSlug) || brandSlug;
  if (!TARGET_SLUGS.includes(slug)) {
    throw new Error(`Targets only: ${TARGET_SLUGS.join(", ")}`);
  }
  if (FORBIDDEN_BRAND_SLUGS.includes(slug)) {
    throw new Error(`Forbidden brand write: ${slug}`);
  }

  const identity = resolveLane2BrandIdentity(slug);
  const pack = getFullBuildContent(slug);
  if (!pack) throw new Error(`No full-build content pack for ${slug}`);

  const rows = await listPresentationRowsDetailed(identity.name);
  const patches = [];
  const blockers = [];
  const failureIds = [];

  let auditSummary = null;
  try {
    const audit = await auditBrandRenderedFieldCompleteness(slug);
    auditSummary = {
      releaseQualityDecision: audit.releaseQualityDecision,
      failFindings: audit.failFindings,
      summary: audit.summary,
    };
    const fails = Array.isArray(audit.failFindings)
      ? audit.failFindings
      : Array.isArray(audit.failures)
        ? audit.failures
        : [];
    for (const f of fails) failureIds.push(typeof f === "string" ? f : String(f?.id || f?.fieldId || f));
  } catch (err) {
    blockers.push(`completeness_audit:${err.message}`);
  }

  // 1) Basics audience / positioning / chips
  const basicsFields = {};
  const pos = packRow(pack, "Brand Positioning");
  const aud = packRow(pack, "Guest Psychographics Description");
  if (aud?.body) {
    const v = validateOwnerCopy(aud.body, "Guest Psychographics Description");
    if (v.ok) {
      basicsFields["Guest Psychographics Description"] = v.cleaned;
      failureIds.push("positioning.audience");
    } else {
      blockers.push(`basics_audience:${v.reason}`);
    }
  }
  if (pos?.body) {
    const v = validateOwnerCopy(pos.body, "Brand Positioning");
    if (v.ok) {
      basicsFields["Brand Positioning"] = v.cleaned;
      failureIds.push("positioning.positioning");
    }
  }
  const chips = BASICS_CHIP_FIELDS[slug];
  if (chips) {
    for (const [field, value] of Object.entries(chips)) {
      const v = validateOwnerCopy(value, field);
      if (v.ok) {
        basicsFields[field] = v.cleaned;
        failureIds.push(`basics.${field}`);
      }
    }
  }
  if (Object.keys(basicsFields).length) {
    patches.push({
      table: BASICS_TABLE,
      action: "PATCH",
      recordId: identity.recordId,
      slotKey: null,
      reason: "basics_audience_positioning_chips_thickness",
      fields: basicsFields,
      failureIds: [
        ...new Set(failureIds.filter((x) => x.startsWith("positioning.") || x.startsWith("basics."))),
      ],
      sanitizedPayloadPreview: Object.fromEntries(
        Object.entries(basicsFields).map(([k, v]) => [k, String(v).slice(0, 140)])
      ),
    });
  }

  // 2) Overview thickness slots from pack (includes Brand Positioning + tags)
  for (const slotKey of THICKEN_SLOTS) {
    if (slotKey === "Guest Psychographics Description") continue;
    const packEntry = packRow(pack, slotKey);
    if (!packEntry?.body) continue;
    const v = validateOwnerCopy(packEntry.body, slotKey, { allowUrls: false });
    if (!v.ok) {
      blockers.push(`${slotKey}:${v.reason}`);
      continue;
    }
    const existing = visibleRows(rows, slotKey);
    if (existing[0]?.recordId) {
      const fields = { Body: v.cleaned };
      if (packEntry.title) fields.Title = scrubStubChipLanguage(packEntry.title);
      else if (/\bconversion-friendly\.?\b/i.test(nz(existing[0].title))) {
        fields.Title = scrubStubChipLanguage(existing[0].title);
      }
      const bodyChanged = nz(existing[0].body) !== v.cleaned;
      const titleChanged = fields.Title != null && nz(existing[0].title) !== nz(fields.Title);
      if (bodyChanged || titleChanged) {
        patches.push({
          table: PRESENTATION_TABLE,
          action: "PATCH",
          recordId: existing[0].recordId,
          slotKey,
          reason: "thicken_presentation_body",
          fields,
          failureIds: [slotKey],
          beforePreview: nz(existing[0].body).slice(0, 120),
          afterPreview: v.cleaned.slice(0, 120),
        });
      }
      for (const extra of existing.slice(1)) {
        patches.push({
          table: PRESENTATION_TABLE,
          action: "PATCH",
          recordId: extra.recordId,
          slotKey,
          reason: "hide_thin_duplicate_slot",
          fields: { Active: false, "External Display Status": "Do Not Display" },
          failureIds: [slotKey],
        });
      }
    } else {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        slotKey,
        reason: "create_missing_thickness_slot",
        fields: {
          "Slot Key": slotKey,
          "Brand Name": identity.name,
          Brand: [identity.recordId],
          Active: true,
          "Sort Order": packEntry.sortOrder ?? 0,
          Title: scrubStubChipLanguage(packEntry.title || ""),
          Body: v.cleaned,
        },
        failureIds: [slotKey],
      });
    }
  }

  // 3) Scrub conversion-friendly stub chips in any visible presentation body/title
  for (const row of rows) {
    if (isHidden(row) || !row.recordId) continue;
    const scrubbedBody = scrubStubChipLanguage(row.body);
    const scrubbedTitle = scrubStubChipLanguage(row.title);
    if (scrubbedBody === nz(row.body) && scrubbedTitle === nz(row.title)) continue;
    if (THICKEN_SLOTS.includes(row.slotKey) || row.slotKey === "footprint.momentum") {
      // thickness / momentum paths already handle these slots
      if (THICKEN_SLOTS.includes(row.slotKey)) continue;
    }
    const fields = {};
    if (scrubbedBody !== nz(row.body)) fields.Body = scrubbedBody;
    if (scrubbedTitle !== nz(row.title)) fields.Title = scrubbedTitle;
    if (!Object.keys(fields).length) continue;
    const v = validateOwnerCopy(fields.Body || row.body, row.slotKey, {
      allowUrls: row.slotKey === "footprint.momentum" || row.slotKey === "footprint.openings",
    });
    if (!v.ok && fields.Body) continue;
    if (fields.Body) fields.Body = v.cleaned;
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: row.recordId,
      slotKey: row.slotKey,
      reason: "scrub_conversion_friendly_stub",
      fields,
      failureIds: ["stub_chip:conversion-friendly"],
    });
  }

  // 4) Recent Momentum structured cards (section pattern parity)
  if (structuredMomentumOk(rows) < 2) {
    for (const m of visibleRows(rows, "footprint.momentum")) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: m.recordId,
        slotKey: "footprint.momentum",
        reason: "hide_diligence_filler_momentum",
        fields: { Active: false, "External Display Status": "Do Not Display" },
        failureIds: ["section_pattern_parity.recent_momentum"],
      });
    }
    const labelRows = visibleRows(rows, "footprint.momentum_label");
    const anyLabel = (rows || []).find((r) => r.slotKey === "footprint.momentum_label");
    if (labelRows[0]) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: labelRows[0].recordId,
        slotKey: "footprint.momentum_label",
        reason: "momentum_label_contract",
        fields: { Body: RECENT_MOMENTUM_DEFAULT_LABEL, Title: labelRows[0].title || "" },
        failureIds: ["section_pattern_parity.recent_momentum"],
      });
    } else if (anyLabel?.recordId) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: anyLabel.recordId,
        slotKey: "footprint.momentum_label",
        reason: "reactivate_momentum_label",
        fields: { Active: true, Body: RECENT_MOMENTUM_DEFAULT_LABEL },
        failureIds: ["section_pattern_parity.recent_momentum"],
      });
    } else {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        slotKey: "footprint.momentum_label",
        reason: "create_momentum_label",
        fields: {
          "Slot Key": "footprint.momentum_label",
          "Brand Name": identity.name,
          Brand: [identity.recordId],
          Active: true,
          "Sort Order": 1,
          Title: "",
          Body: RECENT_MOMENTUM_DEFAULT_LABEL,
        },
        failureIds: ["section_pattern_parity.recent_momentum"],
      });
    }

    // Prefer pack momentum if already structured; else catalog cards
    const packMomentum = packRows(pack, "footprint.momentum").filter((r) =>
      /https?:\/\//i.test(nz(r.body))
    );
    const cards =
      packMomentum.length >= 2
        ? packMomentum.map((r, i) => ({
            title: r.title,
            body: r.body,
            sort: r.sortOrder || i + 1,
          }))
        : momentumCardsFor(slug);

    for (const card of cards) {
      const v = validateOwnerCopy(card.body, "footprint.momentum", { allowUrls: true });
      if (!v.ok) {
        blockers.push(`momentum:${v.reason}`);
        continue;
      }
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        slotKey: "footprint.momentum",
        reason: "create_structured_momentum_card",
        fields: {
          "Slot Key": "footprint.momentum",
          "Brand Name": identity.name,
          Brand: [identity.recordId],
          Active: true,
          "Sort Order": card.sort || 1,
          Title: card.title,
          Body: v.cleaned,
        },
        failureIds: ["section_pattern_parity.recent_momentum"],
      });
    }
  }

  // 5) Openings case summaries when thin/empty (no image writes)
  for (const row of visibleRows(rows, "footprint.openings").slice(0, 4)) {
    const thin =
      words(row.caseSummaryOverview) < 12 ||
      words(row.caseSummaryBrandRelevance) < 8 ||
      words(row.caseSummaryOwnerObjective) < 8;
    if (!thin && !/\bconversion-friendly\.?\b/i.test(
      [row.caseSummaryOverview, row.caseSummaryBrandRelevance, row.caseSummaryOwnerObjective].join(" ")
    )) {
      continue;
    }
    const propertyName = nz(row.title).split("—")[0].trim() || nz(row.title) || "Property example";
    const fields = openingsCaseSummaryFields(slug, propertyName);
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: row.recordId,
      slotKey: "footprint.openings",
      reason: "thicken_openings_case_summary",
      fields,
      failureIds: ["footprint.openings.case_summary"],
    });
  }

  // Guard forbidden fields
  for (const p of patches) {
    for (const key of Object.keys(p.fields || {})) {
      if (FORBIDDEN_WRITE_FIELDS.has(key)) {
        delete p.fields[key];
        blockers.push(`stripped_forbidden_field:${key}`);
      }
    }
  }

  const hardBlockers = blockers.filter(
    (b) =>
      b.startsWith("basics_audience:") ||
      /^overview\.(scenario|why_value|proof)/.test(b) ||
      b.startsWith("momentum:")
  );

  return {
    brandSlug: slug,
    brandName: identity.name,
    recordId: identity.recordId,
    shortName: SHORT[slug],
    auditSummary,
    patches,
    blockers,
    blocked: hardBlockers.length > 0 && patches.length === 0,
    failureIds: [...new Set(failureIds)],
    counts: {
      presentationPatches: patches.filter((p) => p.table === PRESENTATION_TABLE).length,
      basicsPatches: patches.filter((p) => p.table === BASICS_TABLE).length,
      momentumCreates: patches.filter((p) => p.reason === "create_structured_momentum_card").length,
      total: patches.length,
    },
  };
}

async function airtableWrite({ baseId, apiKey, table, recordId, fields, method }) {
  const url =
    method === "POST"
      ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}`
      : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `${method} ${table} failed: ${res.status}`);
  return json;
}

export async function planContentThickening({ brands = TARGET_SLUGS } = {}) {
  const resolved = resolveTargetBrands(brands);
  const brandResults = [];
  for (const brandSlug of resolved) {
    brandResults.push(await planContentThickeningForBrand(brandSlug));
  }
  return {
    version: CONTENT_THICKEN_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    brands: resolved,
    brandResults,
    summary: {
      totalPatches: brandResults.reduce((n, b) => n + b.patches.length, 0),
      blockedBrands: brandResults.filter((b) => b.blocked).length,
      writes: false,
    },
  };
}

export async function applyContentThickening({ plan, apply = false, argv = [] } = {}) {
  const flagCheck = parseContentThickenApplyFlags(argv);
  if (!apply) return { applied: false, reason: "dry_run_only", flagCheck };
  if (!flagCheck.ok) {
    return { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing, flagCheck };
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const resultsByBrand = {};
  for (const brand of plan.brandResults || []) {
    if (!TARGET_SLUGS.includes(brand.brandSlug)) {
      throw new Error(`Refuse non-target brand write: ${brand.brandSlug}`);
    }
    if (FORBIDDEN_BRAND_SLUGS.includes(brand.brandSlug)) {
      throw new Error(`Refuse forbidden brand write: ${brand.brandSlug}`);
    }
    if (brand.blocked) {
      resultsByBrand[brand.brandSlug] = {
        applied: false,
        reason: "blocked",
        blockers: brand.blockers,
      };
      continue;
    }
    const created = [];
    const updated = [];
    const errors = [];
    for (const patch of brand.patches) {
      for (const key of Object.keys(patch.fields || {})) {
        if (FORBIDDEN_WRITE_FIELDS.has(key)) {
          throw new Error(`Forbidden field write: ${key}`);
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
          created.push({ recordId: json.id, slotKey: patch.slotKey, reason: patch.reason });
        } else {
          await airtableWrite({
            baseId,
            apiKey,
            table: patch.table,
            recordId: patch.recordId,
            fields: patch.fields,
            method: "PATCH",
          });
          updated.push({
            recordId: patch.recordId,
            slotKey: patch.slotKey,
            reason: patch.reason,
          });
        }
        await sleep(220);
      } catch (err) {
        errors.push({ slotKey: patch.slotKey, reason: patch.reason, message: err.message });
      }
    }
    resultsByBrand[brand.brandSlug] = {
      applied: errors.length === 0,
      created,
      updated,
      errors,
    };
  }
  return { applied: true, resultsByBrand, flagCheck };
}

function brandMd(brand, applyRow) {
  const lines = [
    `# ${brand.brandName} (${brand.brandSlug}) — content thickening`,
    "",
    `Version: ${CONTENT_THICKEN_VERSION}`,
    "",
    "## Planned patches",
    "",
    `- Total: **${brand.patches.length}**`,
    `- Presentation: **${brand.counts.presentationPatches}**`,
    `- Basics: **${brand.counts.basicsPatches}**`,
    `- Momentum creates: **${brand.counts.momentumCreates}**`,
    `- Blocked: **${brand.blocked}**`,
    "",
  ];
  if (brand.blockers?.length) {
    lines.push("### Blockers", "");
    for (const b of brand.blockers) lines.push(`- ${b}`);
    lines.push("");
  }
  lines.push("### Patch list", "");
  for (const p of brand.patches) {
    lines.push(
      `- \`${p.action}\` ${p.table} · ${p.slotKey || "basics"} · ${p.reason}${
        p.recordId ? ` · ${p.recordId}` : ""
      }`
    );
  }
  if (applyRow) {
    lines.push("", "## Apply result", "");
    lines.push(`- Applied: **${applyRow.applied}**`);
    lines.push(`- Created: **${applyRow.created?.length || 0}**`);
    lines.push(`- Updated: **${applyRow.updated?.length || 0}**`);
    lines.push(`- Errors: **${applyRow.errors?.length || 0}**`);
    for (const e of applyRow.errors || []) {
      lines.push(`  - ${e.slotKey || e.reason}: ${e.message}`);
    }
  }
  lines.push(
    "",
    "## Field mapping",
    "",
    "- Presentation: `Title`, `Body`, Case Summary* (openings only)",
    "- Basics: `Guest Psychographics Description`, `Brand Positioning`",
    "- No Image / CV / Source / Registry / Brand Status / release writes",
    ""
  );
  return lines.join("\n");
}

export function writeContentThickeningReports(plan, applyResult = null) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const report = {
    ...plan,
    dryRun: !(applyResult?.applied === true),
    applyResult: applyResult || { applied: false, reason: "dry_run_only" },
    summary: {
      ...plan.summary,
      writes: applyResult?.applied === true,
    },
  };

  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-27-dazzler-trademark-content-thickening.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-27-dazzler-trademark-content-thickening.md");
  const docsPath = path.join(DOCS_DIR, "brand-explorer-27-dazzler-trademark-content-thickening.md");

  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const md = [
    `# Dazzler + Trademark Content Thickening (27-brand PVQL)`,
    "",
    `Generated: ${report.generatedAt}`,
    `Version: ${CONTENT_THICKEN_VERSION}`,
    `dryRun=${report.dryRun}`,
    "",
    "## Summary",
    "",
    `- Brands: ${report.brands.join(", ")}`,
    `- Total patches: **${report.summary.totalPatches}**`,
    `- Blocked brands: **${report.summary.blockedBrands}**`,
    `- Writes: **${report.summary.writes}**`,
    "",
    "## Scope",
    "",
    "- Targets only: `dazzler-by-wyndham`, `trademark-collection-by-wyndham`",
    "- Does not touch Tapestry, protected 24, Radisson Collection",
    "- No Company Validated / Source Library / Registry / Brand Status / release / image writes",
    "",
  ];
  for (const b of report.brandResults) {
    md.push(`### ${b.brandSlug}`);
    md.push(`- Patches: ${b.patches.length}`);
    md.push(`- Blocked: ${b.blocked}`);
    md.push(`- Basics patches: ${b.counts.basicsPatches}`);
    md.push(`- Momentum creates: ${b.counts.momentumCreates}`);
    md.push("");
  }
  fs.writeFileSync(mdPath, md.join("\n"));
  fs.writeFileSync(docsPath, md.join("\n"));

  const perBrand = {};
  for (const b of report.brandResults) {
    const applyRow = applyResult?.resultsByBrand?.[b.brandSlug] || null;
    const short = SHORT[b.brandSlug] || b.brandSlug;
    const p = path.join(REPORTS_DIR, `brand-explorer-27-${short}-content-thickening.md`);
    fs.writeFileSync(p, brandMd(b, applyRow));
    perBrand[b.brandSlug] = p;
  }

  return { jsonPath, mdPath, docsPath, perBrand };
}

export { listPresentationRowsLight };
