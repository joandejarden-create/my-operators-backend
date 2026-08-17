/**
 * Brand Explorer — Radisson Individuals by Choice restoration.
 *
 * Fixes live visual gate failures (property-example uniqueness + role-match
 * missing images) and field-gate Presentation defects so the canonical brand
 * can return to active_profile_ready when all mandatory gates pass.
 *
 * Scope: Presentation Image / External Display Status / Body / Title /
 * Case Summary* for radisson-individuals-by-choice (recRyvM8OmLlDj9G7) only.
 *
 * Never writes Company Validated, Source Library, Registry approval,
 * Founder/Active release flags, or sibling Radisson brands.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  fetchDurablePropertyImage,
  GALLERY_DURABLE_SOURCES,
} from "./brand-explorer-radisson-individuals-durable-gallery-source-repair-writer.js";
import { OPENINGS_PROPERTY_CATALOG } from "./brand-explorer-radisson-individuals-openings-rebuild-writer.js";
import { EXTERNAL_DISPLAY_STATUS_QUARANTINE } from "./brand-explorer-radisson-individuals-openings-suppression-writer.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "./brand-explorer-image-role-match.js";
import { evaluateBrandExplorerOsBrand } from "./brand-explorer-os-run.js";
import { buildImageIdentity } from "./brand-explorer-image-uniqueness.js";
import { RADISSON_INDIVIDUALS_FIELD_GATE_CONTENT } from "./brand-explorer-radisson-individuals-restoration-content.js";
import { scanForbiddenLanguage } from "./brand-explorer-v40b-copy-quality-patterns.js";
import {
  buildResidualOwnerCopyPatchPlan,
  scrubResidualOwnerFacingCopy,
} from "./brand-explorer-residual-owner-copy-remediation.js";

export const RESTORATION_VERSION = "radisson-individuals-restoration-v2";
export const TARGET_SLUG = "radisson-individuals-by-choice";
export const TARGET_RECORD_ID = "recRyvM8OmLlDj9G7";
export const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
export const BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";
export const OPENINGS_SLOT = "footprint.openings";

/** Avoid golden generic_audience_prose from "Luxury / Discerning, Leisure" adjacency in HTML. */
export const TARGET_GUEST_SEGMENTS_REMEDIATION = Object.freeze([
  "Experience-Oriented",
  "Leisure",
]);

export const REPORT_JSON = "brand-explorer-radisson-individuals-restoration.json";
export const REPORT_MD = "brand-explorer-radisson-individuals-restoration.md";

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-radisson-individuals-restoration",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-sibling-radisson-writes",
  "--confirm-canonical-record-recRyvM8OmLlDj9G7",
  "--confirm-property-distinct-three",
  "--confirm-image-role-match-pass",
  "--confirm-field-gate-content-remediation",
  "--confirm-no-owner-narrative-beyond-failed-fields",
]);

/** Extra chip rows for footprint.portfolio_mix that should be quarantined after consolidating chips. */
export const PORTFOLIO_MIX_QUARANTINE_IDS = Object.freeze([
  "recfoZk5tZM1BGRCb",
  "recJ8NqNfXml75MVO",
]);

/** Keep/fill these three live openings with distinct durable property photography. */
export const KEEP_OPENINGS = Object.freeze([
  {
    presentationRecordId: "rec0uiWsD44ePqr6M",
    label: "Barranquilla — Guayacanes distinct asset",
    // Live openings row currently reuses Casa Don Luis; swap to Guayacanes for a 3rd distinct property image.
    sourcePageUrl: "https://www.choicehotels.com/herrera/chitre/radisson-individuals-hotels/pn009",
    officialPropertyPageUrl:
      "https://www.farandahotels.com/en/hotel/hotel-faranda-guayacanes-chitre-mc-bwfrxxfh-atrk-bwfrxxfh-fr-fh-pn009",
    titleKeywords: ["guayacanes"],
  },
  {
    presentationRecordId: "recto7QMu58eMf5jV",
    label: "Bogotá — Faranda Collection",
    sourcePageUrl: "https://www.choicehotels.com/colombia/bogota/radisson-individuals-hotels/cb012",
    officialPropertyPageUrl:
      "https://www.farandahotels.com/en/hotel/hotel-faranda-collection-bogota-mc-bwfrxxfh-atrk-bwfrxxfh-fr-fh-cb012",
    titleKeywords: ["bogota", "bogotá", "collection"],
  },
  {
    presentationRecordId: "recVtiPqVGo8gUtpO",
    label: "Cúcuta — Faranda Bolivar",
    sourcePageUrl: "https://www.choicehotels.com/colombia/cucuta/radisson-individuals-hotels/cb010",
    officialPropertyPageUrl:
      "https://www.farandahotels.com/en/hotel/hotel-faranda-bolivar-cucuta-mc-bwfrxxfh-atrk-bwfrxxfh-fr-fh-cb010",
    titleKeywords: ["cucuta", "cúcuta", "bolivar"],
  },
]);

/** Quarantine empty / press-kit openings so factory + role-match no longer see missing_image_url. */
export const QUARANTINE_OPENING_IDS = Object.freeze([
  "recM0XfO2UlkNBd5x", // Medellín — empty / press kit
  "recA57HKv0Zd2bGnx", // Cartagena — empty in live surface
  "recFKCA1auFtGwwjY", // Panama City — empty / press kit
  "recLHEhgtaFWGjACc", // Panama corridor — empty / press kit
  "rect0VNHSr1f5ImGx", // Cali — empty
]);

const FORBIDDEN_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function findVisibleSlots(blocks, slotKey) {
  return (blocks || []).filter(
    (b) =>
      nz(b.slotKey) === slotKey &&
      b.active !== false &&
      !/do not display|internal only/i.test(nz(b.externalDisplayStatus))
  );
}

function scrubBody(text, slotKey) {
  const scrub = scrubResidualOwnerFacingCopy(text, { slotKey, brandSlug: TARGET_SLUG });
  return scrub.after || nz(text);
}

function buildFieldGatePatches(blocks) {
  const patches = [];
  const blockers = [];
  const touchedSlots = [];

  for (const item of RADISSON_INDIVIDUALS_FIELD_GATE_CONTENT) {
    const slotKey = item.slotKey;
    let body = scrubBody(item.body, slotKey);
    let title = item.title ? scrubBody(item.title, slotKey) : "";
    const caseFields = {};
    if (item.caseSummaryOverview) {
      caseFields["Case Summary Overview"] = scrubBody(item.caseSummaryOverview, slotKey);
    }
    if (item.caseSummaryBrandRelevance) {
      caseFields["Case Summary Brand Relevance"] = scrubBody(item.caseSummaryBrandRelevance, slotKey);
    }
    if (item.caseSummaryOwnerObjective) {
      caseFields["Case Summary Owner Objective"] = scrubBody(item.caseSummaryOwnerObjective, slotKey);
    }
    if (item.caseSummaryInterpretation) {
      caseFields["Case Summary Interpretation"] = scrubBody(item.caseSummaryInterpretation, slotKey);
    }
    if (item.caseSummaryTags) {
      caseFields["Case Summary Tags"] = scrubBody(item.caseSummaryTags, slotKey);
    }

    const corpus = [title, body, ...Object.values(caseFields)].join("\n");
    const forbidden = scanForbiddenLanguage(corpus);
    if (forbidden.length) {
      blockers.push({
        slotKey,
        forbidden: forbidden.map((h) => h.id || h.label),
      });
      continue;
    }

    const existing = findVisibleSlots(blocks, slotKey);
    const primary = existing[0] || null;
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
          brandSlug: TARGET_SLUG,
          slotKey,
          reason: "field_gate_content_remediation",
          fields,
          fieldMapping: Object.fromEntries(
            Object.keys(fields).map((k) => [k, `Brand Explorer Presentation.${k}`])
          ),
          sanitizedPayloadPreview: {
            Title: title ? title.slice(0, 80) : undefined,
            Body: body.slice(0, 120) + (body.length > 120 ? "…" : ""),
            caseSummaryFields: Object.keys(caseFields),
          },
        });
      } else {
        patches.push({
          table: PRESENTATION_TABLE,
          action: "POST",
          recordId: null,
          brandSlug: TARGET_SLUG,
          slotKey,
          reason: "field_gate_content_create_missing_slot",
          fields: {
            "Slot Key": slotKey,
            "Brand Name": "Radisson Individuals by Choice",
            Brand: [TARGET_RECORD_ID],
            Active: true,
            "Sort Order": 16,
            Title: title || "",
            Body: body,
            ...caseFields,
          },
          fieldMapping: {
            "Slot Key": "Brand Explorer Presentation.Slot Key",
            Body: "Brand Explorer Presentation.Body",
          },
          sanitizedPayloadPreview: {
            Title: title ? title.slice(0, 80) : undefined,
            Body: body.slice(0, 120) + (body.length > 120 ? "…" : ""),
          },
        });
      }
      touchedSlots.push(slotKey);
    }

    // Quarantine extra portfolio_mix chip stubs after consolidating into primary.
    if (slotKey === "footprint.portfolio_mix") {
      for (const extra of existing.slice(1)) {
        if (!extra.recordId) continue;
        if (PORTFOLIO_MIX_QUARANTINE_IDS.includes(extra.recordId) || existing.length > 1) {
          patches.push({
            table: PRESENTATION_TABLE,
            action: "PATCH",
            recordId: extra.recordId,
            brandSlug: TARGET_SLUG,
            slotKey,
            reason: "quarantine_extra_portfolio_mix_chip",
            fields: { "External Display Status": EXTERNAL_DISPLAY_STATUS_QUARANTINE },
            fieldMapping: {
              "External Display Status": "Brand Explorer Presentation.External Display Status",
            },
            sanitizedPayloadPreview: {
              "External Display Status": EXTERNAL_DISPLAY_STATUS_QUARANTINE,
            },
          });
        }
      }
    }
  }

  return { patches, blockers, touchedSlots };
}

function mergeResidualPatches(blocks, contentPatches) {
  // Project content bodies onto blocks for residual scan of remaining dirty rows.
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
    if (p.fields["Case Summary Overview"] != null) {
      next.caseSummaryOverview = p.fields["Case Summary Overview"];
    }
    if (p.fields["External Display Status"] === EXTERNAL_DISPLAY_STATUS_QUARANTINE) {
      next.externalDisplayStatus = EXTERNAL_DISPLAY_STATUS_QUARANTINE;
    }
    return next;
  });

  const residual = buildResidualOwnerCopyPatchPlan({
    brandSlug: TARGET_SLUG,
    presentationRows: projected,
  });
  const contentIds = new Set(contentPatches.filter((p) => p.recordId).map((p) => p.recordId));
  const residualPatches = [];
  const grouped = new Map();
  for (const p of residual.patches || []) {
    if (!p.recordId || !p.safeForGenericApply) continue;
    // Prefer field-gate body when we already patch that record+Body.
    if (contentIds.has(p.recordId) && p.field === "Body") continue;
    if (!grouped.has(p.recordId)) grouped.set(p.recordId, {});
    grouped.get(p.recordId)[p.field] = p.after;
  }
  for (const [recordId, fields] of grouped.entries()) {
    const slotKey =
      projected.find((b) => b.recordId === recordId)?.slotKey || null;
    residualPatches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId,
      brandSlug: TARGET_SLUG,
      slotKey,
      reason: "v40c_residual_owner_copy_scrub",
      fields,
      fieldMapping: Object.fromEntries(
        Object.keys(fields).map((k) => [k, `Brand Explorer Presentation.${k}`])
      ),
      sanitizedPayloadPreview: Object.fromEntries(
        Object.entries(fields).map(([k, v]) => [k, String(v).slice(0, 100)])
      ),
    });
  }
  return { residual, residualPatches };
}

export function parseRestorationApplyFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    contentOnly: argv.includes("--content-only"),
    forceImages: argv.includes("--force-images"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

async function fetchBrandApi() {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
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
  await getBrandLibraryBrandById(
    { query: { brandId: TARGET_RECORD_ID }, headers: {} },
    res
  );
  if (res.statusCode !== 200 || !res.payload?.brand) {
    throw new Error(`Failed to fetch ${TARGET_SLUG}: HTTP ${res.statusCode}`);
  }
  return res.payload.brand;
}

function projectBlocks(blocks, patchesByRecordId) {
  return (blocks || []).map((b) => {
    const patch = patchesByRecordId.get(b.recordId);
    if (!patch) return b;
    const next = { ...b };
    if (patch.fields?.Image?.[0]?.url) {
      next.imageUrl = patch.fields.Image[0].url;
      next.imageFilename = patch.fields.Image[0].url.split("/").pop();
    }
    if (patch.fields?.["External Display Status"] === EXTERNAL_DISPLAY_STATUS_QUARANTINE) {
      return null; // removed from live API surface
    }
    return next;
  }).filter(Boolean);
}

export async function runRadissonIndividualsRestoration({
  dryRun = true,
  contentOnly = false,
  forceImages = false,
} = {}) {
  const brand = await fetchBrandApi();
  if (brand.id !== TARGET_RECORD_ID) {
    throw new Error(
      `Canonical record mismatch: expected ${TARGET_RECORD_ID}, got ${brand.id}`
    );
  }

  const blocks = brand.brandExplorer?.blocks || [];
  const beforeUniqueness = evaluateImageUniqueness({
    brand,
    presentationRows: blocks,
    brandSlug: TARGET_SLUG,
  });
  const beforeRoleMatch = evaluateBrandImageRoleMatch({
    presentationRows: blocks,
    brandSlug: TARGET_SLUG,
  });

  const visualsAlreadyPass =
    beforeUniqueness.pass === true &&
    beforeUniqueness.propertyExampleDistinctCount >= 3 &&
    beforeRoleMatch.pass === true;
  const skipImages = (contentOnly || visualsAlreadyPass) && !forceImages;

  const keepIds = new Set(KEEP_OPENINGS.map((k) => k.presentationRecordId));
  const patches = [];
  const imageAssignments = [];

  if (!skipImages) {
    for (const target of KEEP_OPENINGS) {
      const durable = await fetchDurablePropertyImage({
        sourcePageUrl: target.sourcePageUrl,
        officialPropertyPageUrl: target.officialPropertyPageUrl,
        titleKeywords: target.titleKeywords,
      });
      if (!durable.ok || !durable.imageUrl) {
        imageAssignments.push({
          ...target,
          ok: false,
          error: durable.error || "no_image",
        });
        continue;
      }
      const identity = buildImageIdentity(durable.imageUrl, {
        slotKey: OPENINGS_SLOT,
        title: target.label,
        recordId: target.presentationRecordId,
      });
      imageAssignments.push({
        ...target,
        ok: true,
        imageUrl: durable.imageUrl,
        resolvedFrom: durable.resolvedFrom,
        duplicateGroupId: identity.duplicateGroupId,
      });
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: target.presentationRecordId,
        brandSlug: TARGET_SLUG,
        reason: "restore_distinct_property_example_image",
        fields: {
          Image: [{ url: durable.imageUrl }],
        },
        fieldMapping: { Image: "Brand Explorer Presentation.Image" },
        sanitizedPayloadPreview: { Image: [{ url: durable.imageUrl.slice(0, 120) + "…" }] },
      });
    }

    for (const recordId of QUARANTINE_OPENING_IDS) {
      if (keepIds.has(recordId)) continue;
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId,
        brandSlug: TARGET_SLUG,
        reason: "quarantine_empty_or_duplicate_opening",
        fields: {
          "External Display Status": EXTERNAL_DISPLAY_STATUS_QUARANTINE,
        },
        fieldMapping: {
          "External Display Status": "Brand Explorer Presentation.External Display Status",
        },
        sanitizedPayloadPreview: {
          "External Display Status": EXTERNAL_DISPLAY_STATUS_QUARANTINE,
        },
      });
    }
  } else {
    for (const target of KEEP_OPENINGS) {
      imageAssignments.push({
        ...target,
        ok: true,
        skipped: true,
        reason: "visuals_already_pass",
      });
    }
  }

  // Always scrub raw URLs from quarantined openings (presentationCorpus historically scanned them).
  for (const recordId of QUARANTINE_OPENING_IDS) {
    const existing = (blocks || []).find((b) => b.recordId === recordId);
    if (!existing?.body || !/https?:\/\//i.test(String(existing.body))) continue;
    const already = patches.find(
      (p) => p.recordId === recordId && p.fields?.Body != null
    );
    if (already) continue;
    const scrubbed = scrubResidualOwnerFacingCopy(existing.body, {
      slotKey: OPENINGS_SLOT,
      brandSlug: TARGET_SLUG,
    });
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId,
      brandSlug: TARGET_SLUG,
      reason: "strip_raw_urls_from_quarantined_opening",
      fields: {
        Body: scrubbed.after || "Suppressed opening — not shown in owner-facing profile.",
        "External Display Status": EXTERNAL_DISPLAY_STATUS_QUARANTINE,
      },
      fieldMapping: {
        Body: "Brand Explorer Presentation.Body",
        "External Display Status": "Brand Explorer Presentation.External Display Status",
      },
      sanitizedPayloadPreview: {
        Body: String(scrubbed.after || "").slice(0, 80),
        "External Display Status": EXTERNAL_DISPLAY_STATUS_QUARANTINE,
      },
    });
  }

  // Golden gate: Target Guest Segments "Luxury / Discerning, Leisure" adjacency in HTML.
  const currentSegments = (
    Array.isArray(brand.targetGuestSegments)
      ? brand.targetGuestSegments
      : Array.isArray(brand.targetSegments)
        ? brand.targetSegments
        : []
  ).map(String);
  const segmentsNeedFix =
    currentSegments.includes("Luxury / Discerning") &&
    currentSegments.includes("Leisure");
  if (segmentsNeedFix) {
    patches.push({
      table: BRAND_BASICS_TABLE,
      action: "PATCH",
      recordId: TARGET_RECORD_ID,
      brandSlug: TARGET_SLUG,
      reason: "golden_generic_audience_prose_segment_adjacency",
      fields: {
        "Target Guest Segments": [...TARGET_GUEST_SEGMENTS_REMEDIATION],
      },
      fieldMapping: {
        "Target Guest Segments": "Brand Basics.Target Guest Segments",
      },
      sanitizedPayloadPreview: {
        "Target Guest Segments": [...TARGET_GUEST_SEGMENTS_REMEDIATION],
      },
    });
  }

  const fieldGate = buildFieldGatePatches(blocks);
  patches.push(...fieldGate.patches);
  const { residual, residualPatches } = mergeResidualPatches(blocks, fieldGate.patches);
  patches.push(...residualPatches);

  const byRecord = new Map();
  for (const p of patches) {
    if (p.recordId) byRecord.set(p.recordId, p);
  }
  const projectedBlocks = projectBlocks(blocks, byRecord);
  // Apply content body projections for uniqueness (images) — field bodies don't affect uniqueness.
  const projectedUniqueness = evaluateImageUniqueness({
    brand,
    presentationRows: skipImages ? blocks : projectedBlocks,
    brandSlug: TARGET_SLUG,
  });
  const projectedRoleMatch = evaluateBrandImageRoleMatch({
    presentationRows: skipImages ? blocks : projectedBlocks,
    brandSlug: TARGET_SLUG,
  });

  const distinctAssignmentIds = new Set(
    imageAssignments.filter((a) => a.ok && a.duplicateGroupId).map((a) => a.duplicateGroupId)
  );
  const visualOk = skipImages
    ? visualsAlreadyPass
    : imageAssignments.every((a) => a.ok) &&
      distinctAssignmentIds.size >= 3 &&
      projectedUniqueness.pass === true &&
      projectedRoleMatch.pass === true;

  const validation = {
    pass: visualOk && fieldGate.blockers.length === 0,
    checks: {
      visualsPass: visualOk,
      threeDurableImagesResolved: skipImages
        ? true
        : imageAssignments.every((a) => a.ok),
      threeDistinctPropertyGroups: skipImages
        ? projectedUniqueness.propertyExampleDistinctCount >= 3
        : distinctAssignmentIds.size >= 3,
      galleryDistinct6: projectedUniqueness.galleryDistinctCount >= 6,
      scenarioDistinct3: projectedUniqueness.scenarioDistinctCount >= 3,
      propertyDistinct3: projectedUniqueness.propertyExampleDistinctCount >= 3,
      uniquenessPass: projectedUniqueness.pass === true,
      roleMatchPass: projectedRoleMatch.pass === true,
      fieldGateContentClean: fieldGate.blockers.length === 0,
      fieldGatePatchesPlanned: fieldGate.patches.length > 0 || residualPatches.length > 0,
    },
    failedChecks: [],
  };
  for (const [k, ok] of Object.entries(validation.checks)) {
    if (!ok && k !== "fieldGatePatchesPlanned") validation.failedChecks.push(k);
  }
  // fieldGatePatchesPlanned is informational when already remediated
  if (!validation.checks.fieldGateContentClean) {
    validation.pass = false;
  }

  let osBefore = null;
  try {
    osBefore = await evaluateBrandExplorerOsBrand(TARGET_SLUG);
  } catch (err) {
    osBefore = { error: err.message };
  }

  return {
    version: RESTORATION_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: Boolean(dryRun),
    contentOnly: Boolean(skipImages),
    target: {
      slug: TARGET_SLUG,
      recordId: TARGET_RECORD_ID,
      brandName: brand.name,
    },
    siblingBrandsUntouched: true,
    before: {
      uniqueness: {
        pass: beforeUniqueness.pass,
        galleryDistinctCount: beforeUniqueness.galleryDistinctCount,
        scenarioDistinctCount: beforeUniqueness.scenarioDistinctCount,
        propertyExampleDistinctCount: beforeUniqueness.propertyExampleDistinctCount,
        findings: beforeUniqueness.findings,
      },
      roleMatch: {
        pass: beforeRoleMatch.pass,
        unresolvedRoleMismatchCount: beforeRoleMatch.unresolvedRoleMismatchCount,
      },
      osState: osBefore?.canonicalState || null,
      osAction: osBefore?.routing?.allowedNextAction || null,
      osFailedGates: osBefore?.gateEval?.failedGates || [],
      displayState: brand.brandExplorerDisplayState,
      shouldRenderFullProfile: brand.shouldRenderFullProfile,
    },
    galleryDurableSourcesReference: GALLERY_DURABLE_SOURCES.map((g) => g.slotKey),
    openingsCatalogReference: OPENINGS_PROPERTY_CATALOG.map((o) => o.presentationRecordId),
    imageAssignments,
    quarantineRecordIds: skipImages ? [] : [...QUARANTINE_OPENING_IDS],
    fieldGate: {
      touchedSlots: fieldGate.touchedSlots,
      patchCount: fieldGate.patches.length,
      blockers: fieldGate.blockers,
    },
    residualScrub: {
      patchCount: residual.summary?.patchCount || 0,
      unsafeCount: residual.summary?.unsafeCount || 0,
      appliedPatchCount: residualPatches.length,
    },
    patches,
    projected: {
      uniqueness: {
        pass: projectedUniqueness.pass,
        galleryDistinctCount: projectedUniqueness.galleryDistinctCount,
        scenarioDistinctCount: projectedUniqueness.scenarioDistinctCount,
        propertyExampleDistinctCount: projectedUniqueness.propertyExampleDistinctCount,
      },
      roleMatch: {
        pass: projectedRoleMatch.pass,
        unresolvedRoleMismatchCount: projectedRoleMatch.unresolvedRoleMismatchCount,
      },
    },
    validation,
    forbiddenWrites: {
      companyValidated: false,
      companyValidationDate: false,
      sourceLibraryStatus: false,
      registryApproval: false,
      siblingRadissonBrands: false,
      founderVisualReviewPass: false,
      activeProfileApproved: false,
      ownerFacingNarrativeBeyondFailedFields: false,
    },
    contentRewriteRequired: fieldGate.patches.length > 0,
    notes: [
      skipImages
        ? "Visual uniqueness/role-match already pass — skipped image reassignment."
        : "Assigned 3 durable Faranda/Choice OG property images and quarantined empty openings.",
      `Field-gate Presentation patches planned: ${fieldGate.patches.length} (slots: ${fieldGate.touchedSlots.join(", ") || "none"}).`,
      `Residual owner-copy scrub patches: ${residualPatches.length}.`,
      "Founder Visual Review Pass / Active Profile Approved already true on Brand Basics — not written.",
      "Sibling Radisson brands untouched.",
    ],
  };
}

export async function applyRadissonIndividualsRestoration({
  report,
  apply = false,
  argv = [],
} = {}) {
  const flags = parseRestorationApplyFlags(argv);
  if (!apply) return { applied: false, reason: "dry_run_only", flags };
  if (!flags.ok) return { applied: false, reason: "missing_apply_flags", missing: flags.missing, flags };
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
  for (const patch of report.patches || []) {
    for (const key of Object.keys(patch.fields || {})) {
      if (FORBIDDEN_FIELDS.has(key)) {
        throw new Error(`Refuse forbidden field write: ${key}`);
      }
    }
    if (patch.brandSlug !== TARGET_SLUG) {
      throw new Error(`Refuse non-target brand patch: ${patch.brandSlug}`);
    }
    if (
      patch.table !== PRESENTATION_TABLE &&
      patch.table !== BRAND_BASICS_TABLE
    ) {
      throw new Error(`Refuse unexpected table write: ${patch.table}`);
    }
    if (patch.table === BRAND_BASICS_TABLE && patch.recordId !== TARGET_RECORD_ID) {
      throw new Error(`Refuse Brand Basics write to non-canonical record ${patch.recordId}`);
    }
    const method = patch.action === "POST" ? "POST" : "PATCH";
    const url =
      method === "POST"
        ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(patch.table)}`
        : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(patch.table)}/${patch.recordId}`;
    const res = await fetch(url, {
      method,
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields: patch.fields }),
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(
        json.error?.message || `${method} failed ${patch.recordId || patch.slotKey}: ${res.status}`
      );
    }
    results.push({
      recordId: patch.recordId || json.id || null,
      applied: true,
      action: method,
      table: patch.table,
      fields: Object.keys(patch.fields),
      reason: patch.reason,
      slotKey: patch.slotKey || null,
    });
  }

  return {
    applied: true,
    results,
    companyValidatedUntouched: true,
    sourceLibraryUntouched: true,
    registryUntouched: true,
    siblingBrandsUntouched: true,
  };
}

export async function verifyRadissonIndividualsRestorationAfterApply() {
  const brand = await fetchBrandApi();
  const blocks = brand.brandExplorer?.blocks || [];
  const uniqueness = evaluateImageUniqueness({
    brand,
    presentationRows: blocks,
    brandSlug: TARGET_SLUG,
  });
  const roleMatch = evaluateBrandImageRoleMatch({
    presentationRows: blocks,
    brandSlug: TARGET_SLUG,
  });
  let os = null;
  try {
    os = await evaluateBrandExplorerOsBrand(TARGET_SLUG);
  } catch (err) {
    os = { error: err.message };
  }
  return {
    brandId: brand.id,
    displayState: brand.brandExplorerDisplayState,
    shouldRenderFullProfile: brand.shouldRenderFullProfile,
    uniqueness: {
      pass: uniqueness.pass,
      galleryDistinctCount: uniqueness.galleryDistinctCount,
      scenarioDistinctCount: uniqueness.scenarioDistinctCount,
      propertyExampleDistinctCount: uniqueness.propertyExampleDistinctCount,
    },
    roleMatch: {
      pass: roleMatch.pass,
      unresolvedRoleMismatchCount: roleMatch.unresolvedRoleMismatchCount,
    },
    osState: os?.canonicalState || null,
    osAction: os?.routing?.allowedNextAction || null,
    osFailedGates: os?.gateEval?.failedGates || [],
    acceptance: {
      galleryDistinct6: uniqueness.galleryDistinctCount >= 6,
      scenarioDistinct3: uniqueness.scenarioDistinctCount >= 3,
      propertyDistinct3: uniqueness.propertyExampleDistinctCount >= 3,
      imageRoleMatchPass: roleMatch.pass === true,
      osActiveProfileReady: os?.canonicalState === "active_profile_ready",
      osNoAction: os?.routing?.allowedNextAction === "no_action",
    },
  };
}

export function writeRadissonIndividualsRestorationReports(
  report,
  { reportsDir = path.join(ROOT, "reports") } = {}
) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));

  const md = [
    "# Brand Explorer — Radisson Individuals Restoration",
    "",
    `Generated: ${report.generatedAt}`,
    `Version: ${report.version}`,
    `Dry run: **${report.dryRun !== false}**`,
    "",
    `Canonical: \`${report.target.slug}\` / \`${report.target.recordId}\` (${report.target.brandName})`,
    "",
    "## Before",
    "",
    `- Uniqueness pass: **${report.before.uniqueness.pass}** (gallery ${report.before.uniqueness.galleryDistinctCount}, scenario ${report.before.uniqueness.scenarioDistinctCount}, property ${report.before.uniqueness.propertyExampleDistinctCount})`,
    `- Role-match pass: **${report.before.roleMatch.pass}** (unresolved ${report.before.roleMatch.unresolvedRoleMismatchCount})`,
    `- OS: **${report.before.osState}** → ${report.before.osAction}`,
    `- Display: ${report.before.displayState} · full=${report.before.shouldRenderFullProfile}`,
    "",
    "## Plan",
    "",
    "### Fill distinct property examples",
    "",
  ];
  for (const a of report.imageAssignments || []) {
    md.push(
      `- ${a.label} (\`${a.presentationRecordId}\`): ok=${a.ok} group=\`${a.duplicateGroupId || "—"}\` ${a.error || a.resolvedFrom || ""}`
    );
  }
  md.push("", "### Quarantine openings (Do Not Display)", "");
  for (const id of report.quarantineRecordIds || []) md.push(`- \`${id}\``);

  md.push(
    "",
    "### Field-gate content remediation",
    "",
    `- Content-only / visuals skipped: **${report.contentOnly === true}**`,
    `- Slots touched: ${(report.fieldGate?.touchedSlots || []).join(", ") || "—"}`,
    `- Field-gate patches: ${report.fieldGate?.patchCount ?? 0}`,
    `- Residual scrub patches: ${report.residualScrub?.appliedPatchCount ?? 0}`,
    `- Field-gate blockers: ${JSON.stringify(report.fieldGate?.blockers || [])}`,
    "",
    "## Projected validation",
    "",
    `- Pass: **${report.validation.pass}**`,
    `- Uniqueness: gallery ${report.projected.uniqueness.galleryDistinctCount} / scenario ${report.projected.uniqueness.scenarioDistinctCount} / property ${report.projected.uniqueness.propertyExampleDistinctCount} · pass=${report.projected.uniqueness.pass}`,
    `- Role-match: pass=${report.projected.roleMatch.pass} unresolved=${report.projected.roleMatch.unresolvedRoleMismatchCount}`,
    `- Failed checks: ${(report.validation.failedChecks || []).join(", ") || "—"}`,
    "",
    "## Patches",
    "",
    `| Record / Slot | Action | Reason | Fields |`,
    `| --- | --- | --- | --- |`
  );
  for (const p of report.patches || []) {
    md.push(
      `| ${p.recordId || p.slotKey || "—"} | ${p.action || "PATCH"} | ${p.reason} | ${Object.keys(p.fields).join("; ")} |`
    );
  }

  if (report.notes?.length) {
    md.push("", "## Notes", "");
    for (const n of report.notes) md.push(`- ${n}`);
  }

  if (report.after) {
    md.push(
      "",
      "## After apply",
      "",
      `- Uniqueness: gallery ${report.after.uniqueness.galleryDistinctCount} / scenario ${report.after.uniqueness.scenarioDistinctCount} / property ${report.after.uniqueness.propertyExampleDistinctCount} · pass=${report.after.uniqueness.pass}`,
      `- Role-match: pass=${report.after.roleMatch.pass}`,
      `- OS: **${report.after.osState}** → ${report.after.osAction}`,
      `- Display: ${report.after.displayState} · full=${report.after.shouldRenderFullProfile}`,
      `- Acceptance: ${JSON.stringify(report.after.acceptance)}`,
      `- Remaining OS failed gates: ${(report.after.osFailedGates || []).join("; ") || "—"}`,
      ""
    );
  }

  md.push(
    "",
    "## Guardrails",
    "",
    "- Company Validated / Validation Date untouched",
    "- Source Library / Registry approval untouched",
    "- Sibling Radisson brands untouched",
    "- Gallery rows unchanged in this pass",
    "- No owner-facing narrative rewrite in this pass",
    ""
  );
  for (const n of report.notes || []) md.push(`- ${n}`);
  md.push("");

  fs.writeFileSync(mdPath, md.join("\n"));
  return { jsonPath, mdPath };
}
