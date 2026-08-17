/**
 * Brand Explorer — Built-but-Blocked Tab Factory Remediation
 *
 * Field-level Presentation (+ limited Basics audience/positioning) patches for
 * the 7 content_remediation_needed brands. No Company Validated / Source /
 * Registry / release writes. Never touches protected public-full or true-incomplete.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import { evaluateTabFactoryFromPayload } from "./brand-explorer-tab-factory-evaluate.js";
import { isOwnerFacingPresentationRow } from "./brand-explorer-public-visibility-quality-lock.js";
import { scanForbiddenLanguage } from "./brand-explorer-v40b-copy-quality-patterns.js";
import {
  buildResidualOwnerCopyPatchPlan,
  scrubResidualOwnerFacingCopy,
} from "./brand-explorer-residual-owner-copy-remediation.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "./brand-explorer-image-role-match.js";
import { ALL_INVENTORY_FIELDS } from "./brand-explorer-rendered-field-completeness-inventory.js";
import {
  BUILT_BLOCKED_TARGETS,
  BUILT_BLOCKED_WAVE1,
  BUILT_BLOCKED_WAVE2,
  BUILT_BLOCKED_PROTECTED_PUBLIC_FULL,
  BUILT_BLOCKED_TRUE_INCOMPLETE,
  BUILT_BLOCKED_IDENTITIES,
  BUILT_BLOCKED_CONTENT_BY_SLUG,
  BUILT_BLOCKED_BASICS_BY_SLUG,
} from "./brand-explorer-built-blocked-content.js";

export const REMEDIATION_VERSION = "built-blocked-remediation-v1";
export const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
export const BASICS_TABLE = "Brand Setup - Brand Basics";
export const REPORT_JSON = "brand-explorer-built-blocked-remediation.json";
export const REPORT_MD = "brand-explorer-built-blocked-remediation.md";

export {
  BUILT_BLOCKED_TARGETS,
  BUILT_BLOCKED_WAVE1,
  BUILT_BLOCKED_WAVE2,
  BUILT_BLOCKED_PROTECTED_PUBLIC_FULL,
  BUILT_BLOCKED_TRUE_INCOMPLETE,
};

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-built-blocked-remediation",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-release-field-changes",
  "--confirm-protected-public-full-unchanged",
  "--confirm-field-level-fixes-only",
  "--confirm-no-broad-rewrite",
]);

const FORBIDDEN_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
]);

const EXTERNAL_DISPLAY_STATUS_QUARANTINE = "Do Not Display";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function wordCount(text) {
  return nz(text).split(/\s+/).filter(Boolean).length;
}

export function parseBuiltBlockedApplyFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

async function fetchBrandApi(slug) {
  const identity = BUILT_BLOCKED_IDENTITIES[slug];
  const lookupId = identity?.recordId || slug;
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
  await getBrandLibraryBrandById({ query: { brandId: lookupId }, headers: {} }, res);
  if (!res.payload?.brand) throw new Error(`Brand fetch failed for ${slug} (${lookupId})`);
  return res.payload.brand;
}

function findVisibleSlots(blocks, slotKey) {
  return (blocks || []).filter(
    (b) =>
      nz(b.slotKey) === slotKey &&
      b.active !== false &&
      !/do not display|internal only/i.test(nz(b.externalDisplayStatus))
  );
}

function scrubBody(text, slotKey, brandSlug) {
  const scrub = scrubResidualOwnerFacingCopy(text, { slotKey, brandSlug });
  return scrub.after || nz(text);
}

function caseFieldsFromItem(item, slotKey, brandSlug) {
  const caseFields = {};
  const map = [
    ["caseSummaryOverview", "Case Summary Overview"],
    ["caseSummaryBrandRelevance", "Case Summary Brand Relevance"],
    ["caseSummaryOwnerObjective", "Case Summary Owner Objective"],
    ["caseSummaryInterpretation", "Case Summary Interpretation"],
    ["caseSummaryTags", "Case Summary Tags"],
  ];
  for (const [api, airtable] of map) {
    if (item[api]) caseFields[airtable] = scrubBody(item[api], slotKey, brandSlug);
  }
  return caseFields;
}

function buildFieldGatePatches({ brandSlug, brandName, recordId, blocks, content }) {
  const patches = [];
  const blockers = [];
  const touchedSlots = [];
  const slotIndex = new Map();

  for (const item of content || []) {
    const slotKey = nz(item.slotKey);
    if (!slotKey || slotKey === "Body / Case Summary") continue;
    // Basics-only fields are applied via Basics table patches, not Presentation.
    if (
      slotKey === "Guest Psychographics Description" ||
      slotKey === "Brand Positioning"
    ) {
      continue;
    }

    const idx = slotIndex.get(slotKey) || 0;
    slotIndex.set(slotKey, idx + 1);

    let body = scrubBody(item.body, slotKey, brandSlug);
    let title = item.title ? scrubBody(item.title, slotKey, brandSlug) : "";
    const caseFields = caseFieldsFromItem(item, slotKey, brandSlug);
    const corpus = [title, body, ...Object.values(caseFields)].join("\n");
    const forbidden = scanForbiddenLanguage(corpus);
    if (forbidden.length) {
      blockers.push({
        slotKey,
        index: idx,
        forbidden: forbidden.map((h) => h.id || h.label),
      });
      continue;
    }

    const existing = findVisibleSlots(blocks, slotKey);
    const primary = existing[idx] || null;
    const fields = {
      Body: body,
      ...(title ? { Title: title } : {}),
      ...caseFields,
    };

    const unchanged =
      primary &&
      nz(primary.body) === nz(body) &&
      (!title || nz(primary.title) === nz(title)) &&
      Object.entries(caseFields).every(([k, v]) => {
        const apiKey =
          k === "Case Summary Overview"
            ? "caseSummaryOverview"
            : k === "Case Summary Brand Relevance"
              ? "caseSummaryBrandRelevance"
              : k === "Case Summary Owner Objective"
                ? "caseSummaryOwnerObjective"
                : k === "Case Summary Interpretation"
                  ? "caseSummaryInterpretation"
                  : k === "Case Summary Tags"
                    ? "caseSummaryTags"
                    : null;
        return apiKey ? nz(primary[apiKey]) === nz(v) : true;
      });

    if (!unchanged) {
      if (primary?.recordId) {
        patches.push({
          table: PRESENTATION_TABLE,
          action: "PATCH",
          recordId: primary.recordId,
          brandSlug,
          slotKey,
          reason: "built_blocked_field_gate",
          fields,
          sanitizedPayloadPreview: {
            Title: title ? title.slice(0, 80) : undefined,
            Body: body.slice(0, 120) + (body.length > 120 ? "…" : ""),
            wordCount: wordCount(body),
          },
        });
      } else {
        patches.push({
          table: PRESENTATION_TABLE,
          action: "POST",
          recordId: null,
          brandSlug,
          slotKey,
          reason: "built_blocked_create_slot",
          fields: {
            "Slot Key": slotKey,
            "Brand Name": brandName,
            Brand: [recordId],
            Active: true,
            "Sort Order": item.sortOrder ?? 20 + idx,
            Title: title || "",
            Body: body,
            ...caseFields,
          },
          sanitizedPayloadPreview: {
            Body: body.slice(0, 120) + (body.length > 120 ? "…" : ""),
            wordCount: wordCount(body),
          },
        });
      }
      touchedSlots.push(slotKey);
    }

    if (slotKey === "footprint.portfolio_mix" && existing.length > 1) {
      for (const extra of existing.slice(1)) {
        if (!extra.recordId) continue;
        patches.push({
          table: PRESENTATION_TABLE,
          action: "PATCH",
          recordId: extra.recordId,
          brandSlug,
          slotKey,
          reason: "quarantine_extra_portfolio_mix_chip",
          fields: { "External Display Status": EXTERNAL_DISPLAY_STATUS_QUARANTINE },
          sanitizedPayloadPreview: {
            "External Display Status": EXTERNAL_DISPLAY_STATUS_QUARANTINE,
          },
        });
      }
    }
  }

  return { patches, blockers, touchedSlots };
}

function buildQuarantinePatches(brandSlug, blocks) {
  const patches = [];
  // Quarantine visible openings with no image (blocks property uniqueness / role).
  for (const row of findVisibleSlots(blocks, "footprint.openings")) {
    if (nz(row.imageUrl)) continue;
    if (!row.recordId) continue;
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: row.recordId,
      brandSlug,
      slotKey: "footprint.openings",
      reason: "quarantine_opening_missing_image",
      fields: { "External Display Status": EXTERNAL_DISPLAY_STATUS_QUARANTINE },
      sanitizedPayloadPreview: {
        "External Display Status": EXTERNAL_DISPLAY_STATUS_QUARANTINE,
        title: nz(row.title).slice(0, 60),
      },
    });
  }
  // Quarantine wrong-brand Country openings titled with Radisson.
  if (brandSlug === "country-inn-suites") {
    for (const row of findVisibleSlots(blocks, "footprint.openings")) {
      if (!row.recordId) continue;
      if (!/radisson/i.test(nz(row.title))) continue;
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: row.recordId,
        brandSlug,
        slotKey: "footprint.openings",
        reason: "quarantine_wrong_brand_opening_title",
        fields: { "External Display Status": EXTERNAL_DISPLAY_STATUS_QUARANTINE },
        sanitizedPayloadPreview: { title: nz(row.title).slice(0, 60) },
      });
    }
  }
  // Fill empty materials.file body so file-card meta is not oe-dd--empty (no Source Library status change).
  for (const row of findVisibleSlots(blocks, "materials.file")) {
    if (!row.recordId) continue;
    if (nz(row.body)) continue;
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: row.recordId,
      brandSlug,
      slotKey: "materials.file",
      reason: "fill_empty_materials_file_meta",
      fields: {
        Body: `${nz(row.title) || "Brand material"} · directional owner context from brand materials—confirm current PIP and commercial terms directly; not a performance forecast.`,
      },
      sanitizedPayloadPreview: {
        Body: "directional owner context meta for file card",
      },
    });
  }
  return patches;
}

/**
 * Fill empty materials.gallery.* Image fields from other Presentation images
 * already on the brand (openings / scenarios / files) — no Registry writes.
 */
function buildGalleryFillFromExistingPatches(brandSlug, brandName, recordId, blocks) {
  const patches = [];
  const gallerySlots = [];
  for (let i = 1; i <= 6; i++) {
    const rows = findVisibleSlots(blocks, `materials.gallery.${i}`);
    // Also consider inactive/empty rows that exist in full blocks list
    const any = (blocks || []).find((b) => nz(b.slotKey) === `materials.gallery.${i}`);
    gallerySlots.push(rows[0] || any || null);
  }
  const needSlots = [];
  for (let i = 1; i <= 6; i++) {
    const row = gallerySlots[i - 1];
    if (row?.recordId && nz(row.imageUrl)) continue;
    needSlots.push({ row, idx: i });
  }
  if (!needSlots.length) return { patches, imageBlocker: null };

  const used = new Set(
    gallerySlots.filter((g) => nz(g?.imageUrl)).map((g) => nz(g.imageUrl).split("?")[0])
  );
  const donors = [];
  for (const b of blocks || []) {
    const sk = nz(b.slotKey);
    if (/^materials\.gallery\./.test(sk)) continue;
    if (!nz(b.imageUrl)) continue;
    if (/do not display|internal only/i.test(nz(b.externalDisplayStatus))) continue;
    const key = nz(b.imageUrl).split("?")[0];
    if (!key || used.has(key)) continue;
    const priority = /^footprint\.openings$/.test(sk)
      ? 1
      : /^overview\.scenario\./.test(sk)
        ? 2
        : /^materials\.caseStudy/.test(sk)
          ? 3
          : /^materials\.file/.test(sk)
            ? 4
            : 5;
    donors.push({ b, key, priority });
  }
  donors.sort((a, b) => a.priority - b.priority);

  for (const need of needSlots) {
    const donor = donors.find((d) => !used.has(d.key));
    if (!donor) break;
    used.add(donor.key);
    const title =
      nz(need.row?.title) ||
      nz(donor.b.title) ||
      `Gallery ${need.idx} · reassigned from ${donor.b.slotKey}`;
    const body =
      nz(need.row?.body) ||
      `Directional gallery photography reassigned from existing Presentation inventory (${donor.b.slotKey}) for Brand Explorer gallery distinctiveness. Confirm asset identity directly; not a performance forecast.`;
    if (need.row?.recordId) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: need.row.recordId,
        brandSlug,
        slotKey: `materials.gallery.${need.idx}`,
        reason: "fill_gallery_from_existing_presentation_image",
        fields: {
          Image: [{ url: donor.b.imageUrl }],
          ...(nz(need.row.title) ? {} : { Title: title }),
          ...(nz(need.row.body) ? {} : { Body: body }),
        },
        sanitizedPayloadPreview: {
          to: `materials.gallery.${need.idx}`,
          from: donor.b.slotKey,
        },
      });
    } else {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        brandSlug,
        slotKey: `materials.gallery.${need.idx}`,
        reason: "create_gallery_from_existing_presentation_image",
        fields: {
          "Slot Key": `materials.gallery.${need.idx}`,
          "Brand Name": brandName,
          Brand: [recordId],
          Active: true,
          "Sort Order": 10 + need.idx,
          Title: title,
          Body: body,
          Image: [{ url: donor.b.imageUrl }],
        },
        sanitizedPayloadPreview: {
          to: `materials.gallery.${need.idx}`,
          from: donor.b.slotKey,
        },
      });
    }
  }

  const remaining = needSlots.length - patches.length;
  return {
    patches,
    imageBlocker:
      remaining > 0
        ? `gallery_fill_short_by_${remaining}_need_external_approved_inventory`
        : null,
  };
}

/**
 * Create visible footprint.openings from scenario/gallery donors when property count &lt; 3.
 */
function buildOpeningCreateFromDonorsPatches(brandSlug, brandName, recordId, blocks) {
  const openings = findVisibleSlots(blocks, "footprint.openings").filter((r) => nz(r.imageUrl));
  if (openings.length >= 3) return { patches: [], imageBlocker: null };

  const used = new Set(openings.map((o) => nz(o.imageUrl).split("?")[0]));
  const donors = [];
  for (const b of blocks || []) {
    const sk = nz(b.slotKey);
    if (!nz(b.imageUrl)) continue;
    if (/do not display|internal only/i.test(nz(b.externalDisplayStatus))) continue;
    if (sk === "footprint.openings") continue;
    const key = nz(b.imageUrl).split("?")[0];
    if (!key || used.has(key)) continue;
    used.add(key);
    donors.push(b);
    if (donors.length + openings.length >= 3) break;
  }
  if (donors.length + openings.length < 3) {
    return {
      patches: [],
      imageBlocker: `property_examples_need_3_distinct; have=${openings.length} donors=${donors.length}`,
    };
  }

  const patches = [];
  let need = 3 - openings.length;
  // Also try to attach image to existing opening rows missing Image before creating new ones.
  for (const row of findVisibleSlots(blocks, "footprint.openings")) {
    if (!row.recordId || nz(row.imageUrl) || need <= 0) continue;
    const donor = donors.shift();
    if (!donor) break;
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: row.recordId,
      brandSlug,
      slotKey: "footprint.openings",
      reason: "fill_opening_image_from_existing",
      fields: {
        Image: [{ url: donor.imageUrl }],
        "Case Summary Overview":
          nz(row.caseSummaryOverview) ||
          `${nz(row.title) || "Property example"} — directional property photography for owner product review; not a performance forecast.`,
        "Case Summary Brand Relevance":
          "Supports Brand Explorer property-example distinctiveness for this brand profile.",
        "Case Summary Owner Objective":
          "Help owners visualize typical product cues before conversion or new-build capital commitments.",
        "Case Summary Interpretation":
          "Treat as visual context only; underwrite the specific asset against local comps and brand standards.",
        "Case Summary Tags": "property example · directional · owner review",
      },
      sanitizedPayloadPreview: { from: donor.slotKey, to: row.recordId },
    });
    need--;
  }
  for (const donor of donors) {
    if (need <= 0) break;
    patches.push({
      table: PRESENTATION_TABLE,
      action: "POST",
      recordId: null,
      brandSlug,
      slotKey: "footprint.openings",
      reason: "create_opening_from_existing_image",
      fields: {
        "Slot Key": "footprint.openings",
        "Brand Name": brandName,
        Brand: [recordId],
        Active: true,
        "Sort Order": 90 + need,
        Title: `${brandName} — Property example`,
        Body: `${brandName} property example — directional property photography reassigned from existing Presentation inventory for Brand Explorer property-example distinctiveness. Confirm asset identity and PIP scope directly; not a performance forecast.`,
        Image: [{ url: donor.imageUrl }],
        "Case Summary Overview":
          "Directional property photography example for owner review of product look-and-feel—not a named operating asset forecast.",
        "Case Summary Brand Relevance":
          "Supports Brand Explorer property-example distinctiveness using existing Presentation inventory for this brand.",
        "Case Summary Owner Objective":
          "Help owners visualize typical product cues before committing conversion or new-build capital.",
        "Case Summary Interpretation":
          "Treat as visual context only; underwrite the specific asset against local comps and brand standards.",
        "Case Summary Tags": "property example · inventory reassignment · directional",
      },
      sanitizedPayloadPreview: { from: donor.slotKey },
    });
    need--;
  }
  return { patches, imageBlocker: null };
}
function buildPropertyReassignPatches(brandSlug, brandName, recordId, blocks) {
  const patches = [];
  const openings = findVisibleSlots(blocks, "footprint.openings").filter((r) => nz(r.imageUrl));
  if (openings.length >= 3) return patches;

  const gallery = (blocks || []).filter(
    (b) =>
      /^materials\.gallery\.\d+$/.test(nz(b.slotKey)) &&
      b.active !== false &&
      nz(b.imageUrl) &&
      !/do not display|internal only/i.test(nz(b.externalDisplayStatus))
  );
  const used = new Set(openings.map((o) => nz(o.imageUrl).split("?")[0]));
  const donors = [];
  for (const g of gallery) {
    const key = nz(g.imageUrl).split("?")[0];
    if (!key || used.has(key)) continue;
    used.add(key);
    donors.push(g);
    if (donors.length + openings.length >= 3) break;
  }
  if (donors.length + openings.length < 3) {
    return {
      patches: [],
      imageBlocker: `property_examples_need_3_distinct; have=${openings.length} donors=${donors.length}`,
    };
  }

  let need = 3 - openings.length;
  for (const g of donors) {
    if (need <= 0) break;
    patches.push({
      table: PRESENTATION_TABLE,
      action: "POST",
      recordId: null,
      brandSlug,
      slotKey: "footprint.openings",
      reason: "reassign_gallery_image_to_property_opening",
      fields: {
        "Slot Key": "footprint.openings",
        "Brand Name": brandName,
        Brand: [recordId],
        Active: true,
        "Sort Order": 80 + need,
        Title: nz(g.title) || "Property example",
        Body: `${nz(g.title) || "Property example"} — directional property photography reassigned from gallery inventory for Brand Explorer property-example distinctiveness. Confirm asset identity and PIP scope directly; not a performance forecast.`,
        "Image": [{ url: g.imageUrl }],
        "Case Summary Overview":
          "Directional property photography example for owner review of product look-and-feel—not a named operating asset forecast.",
        "Case Summary Brand Relevance":
          "Supports Brand Explorer property-example distinctiveness using approved gallery inventory for this brand.",
        "Case Summary Owner Objective":
          "Help owners visualize typical product cues before committing conversion or new-build capital.",
        "Case Summary Interpretation":
          "Treat as visual context only; underwrite the specific asset against local comps and brand standards.",
        "Case Summary Tags": "property example · gallery reassignment · directional",
      },
      sanitizedPayloadPreview: {
        Title: (nz(g.title) || "Property example").slice(0, 60),
        imageReassignedFrom: g.slotKey,
      },
    });
    need--;
  }
  return { patches, imageBlocker: null };
}

function buildOpeningCaseSummaryPatches(brandSlug, blocks) {
  const patches = [];
  for (const row of findVisibleSlots(blocks, "footprint.openings")) {
    if (!row.recordId) continue;
    const fields = {};
    if (!nz(row.caseSummaryOverview)) {
      fields["Case Summary Overview"] =
        `${nz(row.title) || "Property example"} — directional property photography for owner product review; not a performance forecast.`;
      fields["Case Summary Brand Relevance"] =
        "Supports Brand Explorer property-example distinctiveness for this brand profile.";
      fields["Case Summary Owner Objective"] =
        "Help owners visualize typical product cues before conversion or new-build capital commitments.";
      fields["Case Summary Interpretation"] =
        "Treat as visual context only; underwrite the specific asset against local comps and brand standards.";
      fields["Case Summary Tags"] = "property example · directional · owner review";
    }
    const bodyWc = nz(row.body).split(/\s+/).filter(Boolean).length;
    if (bodyWc < 30) {
      fields.Body = `${nz(row.title) || "Property example"} — directional extended-stay property photography for owner product review. Use this visual to assess studio layout, kitchenette cues, and corridor presentation before conversion or new-build capital; confirm asset identity and PIP scope directly rather than treating the image as a performance forecast.`;
    }
    if (!Object.keys(fields).length) continue;
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: row.recordId,
      brandSlug,
      slotKey: "footprint.openings",
      reason: fields.Body
        ? "fill_opening_case_summary_and_body_depth"
        : "fill_opening_case_summary",
      fields,
      sanitizedPayloadPreview: {
        title: nz(row.title).slice(0, 60),
        fields: Object.keys(fields),
      },
    });
  }
  return patches;
}

/**
 * Copy an existing scenario/gallery image onto a scenario card missing Image (Presentation-only).
 */
function buildScenarioImageFillPatches(brandSlug, brandName, recordId, blocks) {
  const patches = [];
  const scenarios = [1, 2, 3].map((i) => {
    const rows = findVisibleSlots(blocks, `overview.scenario.${i}`);
    return rows[0] || null;
  });
  const donors = [];
  for (const s of scenarios) {
    if (s?.imageUrl) donors.push(s);
  }
  for (const g of blocks || []) {
    if (
      /^materials\.gallery\.\d+$/.test(nz(g.slotKey)) &&
      nz(g.imageUrl) &&
      !/do not display|internal only/i.test(nz(g.externalDisplayStatus))
    ) {
      donors.push(g);
    }
  }
  for (let i = 0; i < scenarios.length; i++) {
    const row = scenarios[i];
    if (!row?.recordId) continue;
    if (nz(row.imageUrl)) continue;
    const used = new Set(
      scenarios.filter((s) => nz(s?.imageUrl)).map((s) => nz(s.imageUrl).split("?")[0])
    );
    const donor = donors.find((d) => {
      const key = nz(d.imageUrl).split("?")[0];
      return key && !used.has(key);
    });
    if (!donor) continue;
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: row.recordId,
      brandSlug,
      slotKey: `overview.scenario.${i + 1}`,
      reason: "fill_scenario_missing_image",
      fields: {
        Image: [{ url: donor.imageUrl }],
      },
      sanitizedPayloadPreview: {
        from: donor.slotKey || donor.recordId,
        to: `overview.scenario.${i + 1}`,
      },
    });
  }
  return patches;
}

function mergeResidualPatches(brandSlug, blocks, contentPatches, allowedSlots = null) {
  const byId = new Map();
  for (const p of contentPatches) {
    if (p.recordId) byId.set(p.recordId, p);
  }
  const projected = (blocks || []).map((b) => {
    const p = byId.get(b.recordId);
    if (!p?.fields) return b;
    const next = { ...b };
    if (p.fields.Body != null) next.body = p.fields.Body;
    if (p.fields.Title != null) next.title = p.fields.Title;
    if (p.fields["External Display Status"] === EXTERNAL_DISPLAY_STATUS_QUARANTINE) {
      next.externalDisplayStatus = EXTERNAL_DISPLAY_STATUS_QUARANTINE;
    }
    return next;
  });

  const residual = buildResidualOwnerCopyPatchPlan({
    brandSlug,
    presentationRows: projected.filter(isOwnerFacingPresentationRow),
  });
  const contentIds = new Set(contentPatches.filter((p) => p.recordId).map((p) => p.recordId));
  const residualPatches = [];
  const grouped = new Map();
  const allowed = allowedSlots instanceof Set ? allowedSlots : null;
  for (const p of residual.patches || []) {
    if (!p.recordId || !p.safeForGenericApply) continue;
    if (contentIds.has(p.recordId) && p.field === "Body") continue;
    const row = projected.find((b) => b.recordId === p.recordId);
    const slot = nz(row?.slotKey);
    // Field-level only: residual scrub only on slots we are already remediating.
    if (allowed && slot && !allowed.has(slot)) continue;
    if (!grouped.has(p.recordId)) grouped.set(p.recordId, {});
    grouped.get(p.recordId)[p.field] = p.after;
  }
  for (const [recordId, fields] of grouped.entries()) {
    residualPatches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId,
      brandSlug,
      slotKey: projected.find((b) => b.recordId === recordId)?.slotKey || null,
      reason: "built_blocked_residual_scrub",
      fields,
      sanitizedPayloadPreview: Object.fromEntries(
        Object.entries(fields).map(([k, v]) => [k, String(v).slice(0, 100)])
      ),
    });
  }
  return residualPatches;
}

function buildDefectTable(brand, brandSlug, ownerFacing, html) {
  const inv = new Map(ALL_INVENTORY_FIELDS.map((f) => [f.fieldId, f]));
  const bySlot = new Map();
  for (const r of ownerFacing) {
    if (!bySlot.has(r.slotKey)) bySlot.set(r.slotKey, r);
  }
  const tf = evaluateTabFactoryFromPayload({
    brand,
    rows: ownerFacing,
    html,
    brandSlug,
  });
  const fails = (tf.completeness?.findings || []).filter(
    (f) =>
      !["pass", "cleanly_unavailable", "should_suppress"].includes(f.status) &&
      f.recommendedAction !== "suppress_component"
  );
  return fails.map((f) => {
    const invF = inv.get(f.fieldId) || {};
    const slot = f.sourceFieldName || invF.slotKey || null;
    const row = slot ? bySlot.get(slot) : null;
    return {
      brand: brandSlug,
      tab: invF.tabName || null,
      section: invF.sectionName || null,
      component: invF.componentType || null,
      field: f.fieldId,
      currentValue: nz(row?.body).slice(0, 160),
      status: f.status,
      failureReason: f.reason || null,
      sourceRecordId: row?.recordId || null,
      requiredFix: f.recommendedAction || "rewrite_owner_copy",
      proposedPatch: slot ? `Presentation Body/Title for ${slot}` : "Basics or suppress",
      slotKey: slot,
      wordCount: f.wordCount,
    };
  });
}

export async function planBrandBuiltBlockedRemediation(brandSlug) {
  if (BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.includes(brandSlug)) {
    throw new Error(`Refuse protected public-full brand: ${brandSlug}`);
  }
  if (BUILT_BLOCKED_TRUE_INCOMPLETE.includes(brandSlug)) {
    throw new Error(`Refuse true-incomplete brand: ${brandSlug}`);
  }
  if (!BUILT_BLOCKED_TARGETS.includes(brandSlug)) {
    throw new Error(`Targets only: ${BUILT_BLOCKED_TARGETS.join(", ")}`);
  }

  const identity = BUILT_BLOCKED_IDENTITIES[brandSlug];
  const content = BUILT_BLOCKED_CONTENT_BY_SLUG[brandSlug] || [];
  const brand = await fetchBrandApi(brandSlug);
  const brandName = brand.name || identity.name;
  const recordId = brand.id || identity.recordId;
  const blocks = brand.brandExplorer?.blocks || [];
  const ownerFacing = blocks.filter(isOwnerFacingPresentationRow);
  const html = renderBrandExplorerHtmlForTest(brand, {
    allPanels: true,
    internalPreview: true,
  });

  const beforeTf = evaluateTabFactoryFromPayload({
    brand,
    rows: ownerFacing,
    html,
    brandSlug,
  });
  const uniqueness = evaluateImageUniqueness({
    brand,
    presentationRows: ownerFacing,
    brandSlug,
  });
  const roleMatch = evaluateBrandImageRoleMatch({
    presentationRows: ownerFacing,
    brandSlug,
  });

  const defectTable = buildDefectTable(brand, brandSlug, ownerFacing, html);

  if (!content.length) {
    return {
      brandSlug,
      brandName,
      recordId,
      blocked: true,
      blockers: [{ reason: "empty_content_pack" }],
      patches: [],
      defectTable,
      before: {
        failFindings: beforeTf.failFindings,
        auditPass: beforeTf.auditPass,
        uniquenessPass: uniqueness.pass,
        rolePass: roleMatch.pass,
        gallery: uniqueness.galleryDistinctCount,
        scenario: uniqueness.scenarioDistinctCount,
        property: uniqueness.propertyExampleDistinctCount,
      },
      validation: { pass: false, failedChecks: ["empty_content_pack"] },
      wave: BUILT_BLOCKED_WAVE1.includes(brandSlug) ? 1 : 2,
    };
  }

  const fieldGate = buildFieldGatePatches({
    brandSlug,
    brandName,
    recordId,
    blocks: ownerFacing,
    content,
  });
  const quarantine = buildQuarantinePatches(brandSlug, ownerFacing);
  const galleryFill = buildGalleryFillFromExistingPatches(
    brandSlug,
    brandName,
    recordId,
    ownerFacing
  );
  const openingCreate = buildOpeningCreateFromDonorsPatches(
    brandSlug,
    brandName,
    recordId,
    ownerFacing
  );
  const reassign = buildPropertyReassignPatches(brandSlug, brandName, recordId, ownerFacing);
  const openingCases = buildOpeningCaseSummaryPatches(brandSlug, ownerFacing);
  const scenarioImages = buildScenarioImageFillPatches(
    brandSlug,
    brandName,
    recordId,
    ownerFacing
  );
  const allowedResidualSlots = new Set([
    ...fieldGate.touchedSlots,
    ...defectTable.map((d) => d.slotKey).filter(Boolean),
    ...(content || []).map((c) => nz(c.slotKey)).filter(Boolean),
    "footprint.openings",
    "materials.file",
  ]);
  const residual = mergeResidualPatches(
    brandSlug,
    ownerFacing,
    fieldGate.patches,
    allowedResidualSlots
  );

  const basicsPack = {
    ...(BUILT_BLOCKED_BASICS_BY_SLUG[brandSlug] || {}),
  };
  for (const item of content || []) {
    const sk = nz(item.slotKey);
    if (sk === "Guest Psychographics Description" || sk === "Brand Positioning") {
      if (nz(item.body)) basicsPack[sk] = item.body;
    }
  }
  const basicsPatches = [];
  if (Object.keys(basicsPack).length) {
    const fields = {};
    for (const [k, v] of Object.entries(basicsPack)) {
      const cleaned = scrubBody(v, k, brandSlug);
      if (scanForbiddenLanguage(cleaned).length) continue;
      fields[k] = cleaned;
    }
    if (Object.keys(fields).length) {
      basicsPatches.push({
        table: BASICS_TABLE,
        action: "PATCH",
        recordId,
        brandSlug,
        slotKey: null,
        reason: "built_blocked_basics_audience_positioning",
        fields,
        sanitizedPayloadPreview: Object.fromEntries(
          Object.entries(fields).map(([k, v]) => [k, String(v).slice(0, 100)])
        ),
      });
    }
  }

  // Prefer Basics + field gates before image creates so a bad Image write cannot skip Basics.
  const patches = [
    ...fieldGate.patches,
    ...quarantine,
    ...openingCases,
    ...scenarioImages,
    ...(galleryFill.patches || []),
    ...(openingCreate.patches || []),
    ...residual,
    ...basicsPatches,
    ...(reassign.patches || []),
  ];

  const blockers = [...fieldGate.blockers];
  const imageBlockers = [];
  if (galleryFill.imageBlocker) imageBlockers.push(galleryFill.imageBlocker);
  if (openingCreate.imageBlocker) imageBlockers.push(openingCreate.imageBlocker);
  if (reassign.imageBlocker) imageBlockers.push(reassign.imageBlocker);
  if (!uniqueness.pass) {
    imageBlockers.push(
      `image_uniqueness_fail g=${uniqueness.galleryDistinctCount} s=${uniqueness.scenarioDistinctCount} p=${uniqueness.propertyExampleDistinctCount}`
    );
  }
  if (!roleMatch.pass) imageBlockers.push("image_role_match_fail");

  return {
    brandSlug,
    brandName,
    recordId,
    wave: BUILT_BLOCKED_WAVE1.includes(brandSlug) ? 1 : 2,
    shouldRenderFullProfile: brand.shouldRenderFullProfile === true,
    displayState: brand.brandExplorerDisplayState,
    before: {
      failFindings: beforeTf.failFindings,
      emptyRenderFailFindings: beforeTf.emptyRenderFailFindings,
      auditPass: beforeTf.auditPass,
      uniquenessPass: uniqueness.pass,
      rolePass: roleMatch.pass,
      gallery: uniqueness.galleryDistinctCount,
      scenario: uniqueness.scenarioDistinctCount,
      property: uniqueness.propertyExampleDistinctCount,
      golden: beforeTf.golden,
    },
    defectTable,
    patches,
    blockers,
    imageBlockers,
    blocked: blockers.length > 0,
    touchedSlots: [...new Set(fieldGate.touchedSlots)],
    validation: {
      pass: blockers.length === 0,
      failedChecks: blockers.length ? ["content_forbidden_or_invalid"] : [],
      checks: {
        target_in_scope: true,
        protected_untouched: true,
        true_incomplete_untouched: true,
        no_release_fields: patches.every((p) =>
          Object.keys(p.fields || {}).every((k) => !FORBIDDEN_FIELDS.has(k))
        ),
        presentation_or_basics_only: patches.every(
          (p) => p.table === PRESENTATION_TABLE || p.table === BASICS_TABLE
        ),
      },
    },
  };
}

export async function planBuiltBlockedRemediation({ brands = BUILT_BLOCKED_TARGETS } = {}) {
  const plans = [];
  for (const slug of brands) {
    plans.push(await planBrandBuiltBlockedRemediation(slug));
  }
  const patchCount = plans.reduce((n, p) => n + (p.patches?.length || 0), 0);
  const blocked = plans.filter((p) => p.blocked);
  return {
    version: REMEDIATION_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    brands: plans,
    summary: {
      brandCount: plans.length,
      patchCount,
      blockedCount: blocked.length,
      wave1: BUILT_BLOCKED_WAVE1,
      wave2: BUILT_BLOCKED_WAVE2,
      protectedPublicFull: BUILT_BLOCKED_PROTECTED_PUBLIC_FULL,
      trueIncompleteExcluded: BUILT_BLOCKED_TRUE_INCOMPLETE,
    },
    validation: {
      pass: blocked.length === 0 && plans.every((p) => p.validation?.pass),
      failedChecks: blocked.flatMap((p) => p.validation?.failedChecks || []),
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
  if (!res.ok) {
    throw new Error(json.error?.message || `${method} failed ${recordId || table}: ${res.status}`);
  }
  return json;
}

export async function applyBuiltBlockedRemediation({
  report,
  apply = false,
  argv = [],
} = {}) {
  const flags = parseBuiltBlockedApplyFlags(argv);
  if (!apply) return { applied: false, reason: "dry_run_only", flags };
  if (!flags.ok) {
    return { applied: false, reason: "missing_apply_flags", missing: flags.missing, flags };
  }
  if (!report.validation?.pass) {
    return {
      applied: false,
      reason: "validation_failed",
      failedChecks: report.validation?.failedChecks || [],
    };
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");

  const results = [];
  for (const brand of report.brands || []) {
    if (BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.includes(brand.brandSlug)) {
      throw new Error(`Refuse write to protected public-full ${brand.brandSlug}`);
    }
    if (BUILT_BLOCKED_TRUE_INCOMPLETE.includes(brand.brandSlug)) {
      throw new Error(`Refuse write to true-incomplete ${brand.brandSlug}`);
    }
    for (const patch of brand.patches || []) {
      for (const key of Object.keys(patch.fields || {})) {
        if (FORBIDDEN_FIELDS.has(key)) throw new Error(`Refuse forbidden field: ${key}`);
      }
      if (![PRESENTATION_TABLE, BASICS_TABLE].includes(patch.table)) {
        throw new Error(`Refuse unexpected table: ${patch.table}`);
      }
      if (!BUILT_BLOCKED_TARGETS.includes(patch.brandSlug)) {
        throw new Error(`Refuse out-of-scope brand: ${patch.brandSlug}`);
      }
      const method = patch.action === "POST" ? "POST" : "PATCH";
      const json = await airtableWrite({
        baseId,
        apiKey,
        table: patch.table,
        recordId: patch.recordId,
        fields: patch.fields,
        method,
      });
      results.push({
        brandSlug: patch.brandSlug,
        recordId: patch.recordId || json.id || null,
        action: method,
        slotKey: patch.slotKey,
        reason: patch.reason,
        fields: Object.keys(patch.fields),
      });
    }
  }

  return {
    applied: true,
    results,
    companyValidatedUntouched: true,
    sourceLibraryUntouched: true,
    registryUntouched: true,
    releaseFieldsUntouched: true,
    protectedPublicFullUntouched: true,
    trueIncompleteUntouched: true,
  };
}

export async function verifyBuiltBlockedBrand(brandSlug) {
  const brand = await fetchBrandApi(brandSlug);
  const ownerFacing = (brand.brandExplorer?.blocks || []).filter(isOwnerFacingPresentationRow);
  const html = renderBrandExplorerHtmlForTest(brand, {
    allPanels: true,
    internalPreview: true,
  });
  const tf = evaluateTabFactoryFromPayload({
    brand,
    rows: ownerFacing,
    html,
    brandSlug,
  });
  const uniqueness = evaluateImageUniqueness({
    brand,
    presentationRows: ownerFacing,
    brandSlug,
  });
  const roleMatch = evaluateBrandImageRoleMatch({
    presentationRows: ownerFacing,
    brandSlug,
  });
  return {
    brandSlug,
    failFindings: tf.failFindings,
    empty: tf.emptyRenderFailFindings,
    auditPass: tf.auditPass,
    completenessPass: tf.completeness?.auditPass === true,
    provenancePass: tf.provenance?.pass === true,
    uniquenessPass: uniqueness.pass === true,
    rolePass: roleMatch.pass === true,
    gallery: uniqueness.galleryDistinctCount,
    scenario: uniqueness.scenarioDistinctCount,
    property: uniqueness.propertyExampleDistinctCount,
    golden: tf.golden,
    contentReady:
      tf.completeness?.auditPass === true &&
      tf.emptyScan?.pass === true &&
      tf.provenance?.pass === true,
    fullyReady:
      tf.auditPass === true && uniqueness.pass === true && roleMatch.pass === true,
  };
}

/** Short report suffix matching user deliverable names. */
export const REPORT_SLUG_ALIAS = Object.freeze({
  "country-inn-suites": "country",
  "quality-inn": "quality-inn",
  radisson: "radisson",
  "radisson-blu": "radisson-blu",
  "radisson-red": "radisson-red",
  "suburban-studios": "suburban",
  "woodspring-suites": "woodspring",
});

export function writeBuiltBlockedReports(report, applyResult = null) {
  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  fs.writeFileSync(
    jsonPath,
    JSON.stringify({ ...report, applyResult: applyResult || null, dryRun: !applyResult?.applied }, null, 2)
  );

  const lines = [
    `# Built-but-Blocked Remediation`,
    ``,
    `Version: \`${REMEDIATION_VERSION}\` · ${report.generatedAt}`,
    `Patches: **${report.summary?.patchCount ?? 0}** · Blocked: **${report.summary?.blockedCount ?? 0}** · Applied: **${applyResult?.applied === true}**`,
    ``,
    `Protected public-full (untouched): ${BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.join(", ")}`,
    `True incomplete (excluded): ${BUILT_BLOCKED_TRUE_INCOMPLETE.join(", ")}`,
    ``,
  ];

  for (const b of report.brands || []) {
    lines.push(`## ${b.brandName} (\`${b.brandSlug}\`) — Wave ${b.wave}`);
    lines.push(
      `- Before: failFindings=${b.before?.failFindings} empty=${b.before?.emptyRenderFailFindings} uniq=${b.before?.uniquenessPass} role=${b.before?.rolePass} g/s/p=${b.before?.gallery}/${b.before?.scenario}/${b.before?.property}`
    );
    lines.push(`- Patches: ${(b.patches || []).length} · Blocked: ${b.blocked}`);
    if (b.imageBlockers?.length) {
      lines.push(`- Image blockers: ${b.imageBlockers.join("; ")}`);
    }
    lines.push(``);
    lines.push(`| Tab | Field | Status | Slot | Required Fix |`);
    lines.push(`| --- | --- | --- | --- | --- |`);
    for (const d of (b.defectTable || []).slice(0, 60)) {
      lines.push(
        `| ${d.tab || "—"} | ${d.field} | ${d.status} | ${d.slotKey || "—"} | ${d.requiredFix} |`
      );
    }
    lines.push(``);

    const reportAlias = REPORT_SLUG_ALIAS[b.brandSlug] || b.brandSlug;
    const perPath = path.join(
      reportsDir,
      `brand-explorer-built-blocked-remediation-${reportAlias}.md`
    );
    fs.writeFileSync(
      perPath,
      [
        `# Built-blocked remediation — ${b.brandName}`,
        ``,
        `Slug: \`${b.brandSlug}\` · Record: \`${b.recordId}\` · Wave ${b.wave}`,
        ``,
        `## Before`,
        JSON.stringify(b.before, null, 2),
        ``,
        `## Defects (${(b.defectTable || []).length})`,
        `| Field | Status | WC | Slot | Fix |`,
        `| --- | --- | ---: | --- | --- |`,
        ...(b.defectTable || []).map(
          (d) =>
            `| ${d.field} | ${d.status} | ${d.wordCount ?? "—"} | ${d.slotKey || "—"} | ${d.requiredFix} |`
        ),
        ``,
        `## Patches (${(b.patches || []).length})`,
        ...(b.patches || []).map(
          (p) =>
            `- ${p.action} \`${p.slotKey || "basics"}\` ${p.recordId || "(create)"} — ${p.reason}`
        ),
      ].join("\n")
    );
  }

  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(mdPath, lines.join("\n"));

  const docsDir = path.join(ROOT, "docs", "data-intelligence");
  fs.mkdirSync(docsDir, { recursive: true });
  const docPath = path.join(docsDir, "brand-explorer-built-blocked-remediation.md");
  fs.writeFileSync(
    docPath,
    [
      `# Built-but-Blocked Tab Factory Remediation`,
      ``,
      `Field-level remediation for the 7 active brands with Presentation depth but Tab Factory / PVQL debt.`,
      ``,
      "## Run",
      "",
      "```bash",
      "npm run brand-explorer-built-blocked-remediation -- --dry-run",
      "npm run brand-explorer-built-blocked-remediation -- --brands radisson,radisson-blu --apply \\",
      "  --approve-built-blocked-remediation --confirm-no-company-validation-changes \\",
      "  --confirm-no-source-library-status-changes --confirm-no-registry-approval-changes \\",
      "  --confirm-no-release-field-changes --confirm-protected-public-full-unchanged \\",
      "  --confirm-field-level-fixes-only --confirm-no-broad-rewrite",
      "```",
      "",
      "Does not write release fields or make brands public-full. Founder review required before restore.",
      "",
      `Latest: ${report.generatedAt} · patches=${report.summary?.patchCount}`,
    ].join("\n")
  );

  return { jsonPath, mdPath, docPath };
}
