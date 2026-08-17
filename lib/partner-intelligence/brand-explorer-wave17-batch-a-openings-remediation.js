/**
 * Wave 17 Batch A — openings/property shortfall remediation ONLY.
 *
 * Fixes exactly three footprint.openings cards:
 *   Hyatt Regency Cancun → Hyatt Regency Orlando
 *   Hyatt Centric Midtown 5th Ave NYC → Hyatt Centric Brickell Miami
 *   Thompson Playa del Carmen → The Cape, A Thompson Hotel
 *
 * Forbidden: gallery/scenario writes, Recent Momentum, Brand Status, release,
 * CV, Active 65, Batch B, Dream Hotels, non-target brands.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  listPresentationRowsLight,
} from "./brand-explorer-lane2-common.js";
import { toAirtableFetchableImageUrl } from "./brand-explorer-lane2-image-materialization.js";
import {
  evaluateImageUniqueness,
  buildImageIdentity,
} from "./brand-explorer-image-uniqueness.js";
import {
  evaluateBrandImageRoleMatch,
  detectVisualCategory,
} from "./brand-explorer-image-role-match.js";
import {
  buildOpeningsPropertyCardTitle,
  buildOpeningsPropertyCardBody,
  OPENINGS_SLOT,
} from "./brand-explorer-openings-property-card-contract.js";
import {
  WAVE17_BATCH_A_VERSION,
  WAVE17_PROTECTED_ACTIVE_COUNT,
  WAVE17_BATCH_A_IDENTITIES,
  WAVE17_BATCH_A_APPROVED_SLUGS,
  WAVE17_BATCH_A_OUT_OF_SCOPE,
} from "./brand-explorer-wave17-batch-a-factory-plan.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import {
  isWave17BatchARejectedImageUrl,
  runWave17BatchAImageIdentityPreflight,
} from "./brand-explorer-wave17-batch-a-image-materialization.js";

export const WAVE17_BATCH_A_OPENINGS_REMEDIATION_VERSION =
  "wave17-batch-a-openings-remediation-v1";

export const WAVE17_BATCH_A_OPENINGS_REMEDIATION_APPLY_FLAGS = Object.freeze([
  "--approve-wave17-batch-a-openings-remediation",
  "--confirm-three-opening-cards-only",
  "--confirm-three-brand-scope",
  "--confirm-target-brands-only",
  "--confirm-all-three-under-review",
  "--confirm-active-65-protected",
  "--confirm-no-gallery-writes",
  "--confirm-no-scenario-writes",
  "--confirm-no-batch-b-writes",
  "--confirm-no-dream-hotels-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-census-writes",
  "--confirm-no-recent-momentum-writes",
  "--confirm-identity-confidence-high",
  "--confirm-replacement-recommended-yes",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const HARVEST_PATH = path.join(REPORTS_DIR, "_tmp-wave17-hyatt-browser-harvest.json");
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS_TABLE = "Brand Setup - Brand Basics";

const FORBIDDEN_WRITE_FIELDS = new Set([
  "Company Validated",
  "Company Validation Date",
  "Brand Verified",
  "Brand Status",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Founder Visual Review Pass",
  "Partner Intelligence - Source Library",
  "Partner Intelligence - Brand Asset Registry",
  "Recent Momentum",
]);

/** Fixed shortfall Presentation record IDs (from live preflight). */
export const WAVE17_BATCH_A_OPENINGS_SHORTFALLS = Object.freeze({
  "hyatt-regency": Object.freeze({
    recordId: "recQ1zGQRhSedhTdO",
    invalidProperty: "Hyatt Regency Cancun",
    reasonInvalid:
      "No current live Hyatt Regency Cancun property page safely validated; do not force Cancun identity.",
  }),
  "hyatt-centric": Object.freeze({
    recordId: "rechFlli0qV5Jq6h6",
    invalidProperty: "Hyatt Centric Midtown 5th Avenue New York",
    reasonInvalid:
      "Expected live property URL (nycmt) unavailable/404; alternate nycct path not positively HTML-validated (rate-limited); no Midtown DAM harvest for HIGH-confidence image attach.",
  }),
  "thompson-hotels": Object.freeze({
    recordId: "rec4XJtwzRNsh8M7i",
    invalidProperty: "Thompson Playa del Carmen",
    reasonInvalid:
      "Property rebranded to Hyatt Centric — historical Thompson identity must not be used as a current Thompson example.",
  }),
});

/** Validated replacement catalog (HIGH confidence only). */
export const WAVE17_BATCH_A_OPENINGS_REPLACEMENTS = Object.freeze({
  "hyatt-regency": Object.freeze({
    harvestKey: "hyatt-regency-orlando",
    propertyName: "Hyatt Regency Orlando",
    city: "Orlando",
    country: "USA",
    geographyLabel: "International Reference",
    currentBrandIdentity: "Hyatt Regency",
    officialPropertyReference:
      "https://www.hyatt.com/hyatt-regency/en-US/mcoro-hyatt-regency-orlando",
    officialBrandReference: "https://www.hyatt.com/hyatt-regency",
    propertyCode: "mcoro",
    preferredRole: "exterior_arrival",
    identityConfidence: "HIGH",
    replacementRecommended: true,
    reason:
      "Prefer B: Cancun identity not definitive. Orlando is current Hyatt Regency with official property page + unused DAM exterior.",
  }),
  "hyatt-centric": Object.freeze({
    harvestKey: "hyatt-centric-brickell-miami",
    propertyName: "Hyatt Centric Brickell Miami",
    city: "Miami",
    country: "USA",
    geographyLabel: "International Reference",
    currentBrandIdentity: "Hyatt Centric",
    officialPropertyReference:
      "https://www.hyatt.com/hyatt-centric/en-US/miact-hyatt-centric-brickell-miami",
    officialBrandReference: "https://www.hyatt.com/hyatt-centric",
    propertyCode: "miact",
    preferredRole: "exterior_arrival",
    identityConfidence: "HIGH",
    replacementRecommended: true,
    reason:
      "Midtown cannot be kept at HIGH confidence without validated official page + image. Brickell Miami is current Hyatt Centric with official page + unused DAM exterior.",
  }),
  "thompson-hotels": Object.freeze({
    harvestKey: "the-cape-thompson",
    propertyName: "The Cape, A Thompson Hotel",
    city: "Cabo San Lucas",
    country: "Mexico",
    geographyLabel: "CALA",
    currentBrandIdentity: "Thompson Hotels",
    officialPropertyReference: "https://www.hyatt.com/thompson-hotels/en-US/cslth-the-cape",
    officialBrandReference: "https://www.hyatt.com/thompson-hotels",
    propertyCode: "cslth",
    preferredRole: "exterior_arrival",
    identityConfidence: "HIGH",
    replacementRecommended: true,
    reason:
      "Remove Playa (now Hyatt Centric). The Cape is current CALA Thompson with official property page + unused DAM exterior.",
  }),
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export function parseWave17BatchAOpeningsRemediationFlags(argv = []) {
  const missing = WAVE17_BATCH_A_OPENINGS_REMEDIATION_APPLY_FLAGS.filter(
    (f) => !argv.includes(f)
  );
  return { ok: missing.length === 0, missing };
}

function loadHarvest() {
  if (!fs.existsSync(HARVEST_PATH)) {
    throw new Error(`Missing harvest file: ${HARVEST_PATH}`);
  }
  return JSON.parse(fs.readFileSync(HARVEST_PATH, "utf8"));
}

async function probeImage(url) {
  try {
    const res = await fetch(url, {
      method: "GET",
      headers: { Accept: "image/*,*/*" },
      redirect: "follow",
    });
    const ct = String(res.headers.get("content-type") || "");
    return {
      ok: res.ok && /image\//i.test(ct),
      status: res.status,
      contentType: ct,
      url,
    };
  } catch (err) {
    return { ok: false, status: 0, contentType: "", url, error: String(err.message || err) };
  }
}

function pickUnusedImage({ harvestProp, usedGroupIds, brandSlug, preferredRole }) {
  const images = harvestProp?.images || [];
  const ordered = [
    ...images.filter((i) => i.role === preferredRole),
    ...images.filter((i) => i.role !== preferredRole),
  ];
  for (const img of ordered) {
    const url = nz(img.imageUrl);
    if (!url) continue;
    const reject = isWave17BatchARejectedImageUrl(url, { brandSlug });
    if (reject?.rejected) continue;
    const group = buildImageIdentity(url).duplicateGroupId;
    if (usedGroupIds.has(group)) continue;
    return { ...img, imageUrl: url, duplicateGroupId: group };
  }
  return null;
}

function buildOpeningsFields({ brandName, replacement, image }) {
  const geo = replacement.geographyLabel;
  const chips = [geo, replacement.city, "Property example"];
  // "The Cape, A Thompson Hotel" already carries Thompson — avoid ", Thompson Hotels" suffix
  // from singular "Hotel" vs brand "Hotels" mismatch in the title helper.
  const brandForTitle = /thompson\s+hotel/i.test(replacement.propertyName)
    ? ""
    : brandName;
  const title = buildOpeningsPropertyCardTitle({
    propertyName: replacement.propertyName,
    brandName: brandForTitle,
    marketCity: replacement.city,
  });
  const body = buildOpeningsPropertyCardBody({
    chips,
    locationLine: `${replacement.city} (${geo})`,
    metaLine: `${geo} · ${replacement.country}`,
    scenarioLine: `${geo} / ${replacement.city} / PROPERTY EXAMPLE`.toUpperCase(),
    teaser: `${replacement.propertyName} — property.`,
    sourceUrl: replacement.officialPropertyReference,
  });
  const internationalReference = geo === "International Reference";
  return {
    Title: title,
    Body: body,
    Image: [{ url: toAirtableFetchableImageUrl(image.imageUrl) }],
    "Case Summary Overview": `${replacement.propertyName} — property.`,
    "Case Summary Tags": chips.join(", "),
    "Case Summary Brand Relevance": internationalReference
      ? "Official International Reference property photography for Brand Explorer openings — not a CALA operating claim."
      : "Official CALA property photography for Brand Explorer openings.",
    "Case Summary Owner Objective":
      "Use as a directional property reference when underwriting product fit, capital scope, and platform participation.",
    "Case Summary Interpretation":
      "Confirm live affiliation criteria and property-specific scope with the brand before underwriting.",
  };
}

async function airtablePresentationWrite({ baseId, apiKey, recordId, fields, method = "PATCH" }) {
  const url = recordId
    ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`
    : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}`;
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(recordId ? { fields } : { fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `${method} failed: ${res.status}`);
  return json;
}

function researchRow(slug) {
  const shortfall = WAVE17_BATCH_A_OPENINGS_SHORTFALLS[slug];
  const replacement = WAVE17_BATCH_A_OPENINGS_REPLACEMENTS[slug];
  return {
    Brand: WAVE17_BATCH_A_IDENTITIES[slug].exactBrandBasicsName,
    brandSlug: slug,
    InvalidCurrentProperty: shortfall.invalidProperty,
    ReasonInvalid: shortfall.reasonInvalid,
    ReplacementProperty: replacement.propertyName,
    City: replacement.city,
    Country: replacement.country,
    CurrentBrandIdentity: replacement.currentBrandIdentity,
    OfficialPropertyReference: replacement.officialPropertyReference,
    OfficialBrandReference: replacement.officialBrandReference,
    ImageCandidate: null,
    IdentityConfidence: replacement.identityConfidence,
    ReplacementRecommended: replacement.replacementRecommended ? "yes" : "no",
    presentationRecordId: shortfall.recordId,
  };
}

function scanContamination(slug, rows) {
  const hits = [];
  const openings = (rows || []).filter((r) => r.slotKey === OPENINGS_SLOT);
  for (const r of openings) {
    const corpus = `${r.title}\n${r.body}\n${r.caseSummaryOverview || ""}`.toLowerCase();
    if (slug === "thompson-hotels") {
      if (/hyatt\s*centric/i.test(corpus)) {
        hits.push({ type: "hyatt_centric_in_thompson", recordId: r.recordId, title: r.title });
      }
      if (/\bdream\s+hotels?\b/i.test(corpus)) {
        hits.push({ type: "dream_in_thompson", recordId: r.recordId, title: r.title });
      }
      if (/playa\s+del\s+carmen/i.test(corpus) && /thompson/i.test(corpus)) {
        hits.push({
          type: "historical_playa_as_thompson",
          recordId: r.recordId,
          title: r.title,
        });
      }
    }
    if (slug === "hyatt-regency" && /canc[uú]n/i.test(corpus) && /regency/i.test(corpus)) {
      // Only flag if Cancun remains on the remediated card
      if (r.recordId === WAVE17_BATCH_A_OPENINGS_SHORTFALLS["hyatt-regency"].recordId) {
        hits.push({ type: "stale_cancun_on_target_card", recordId: r.recordId, title: r.title });
      }
    }
    if (
      slug === "hyatt-centric" &&
      /midtown\s+5th/i.test(corpus) &&
      r.recordId === WAVE17_BATCH_A_OPENINGS_SHORTFALLS["hyatt-centric"].recordId
    ) {
      hits.push({ type: "stale_midtown_on_target_card", recordId: r.recordId, title: r.title });
    }
  }
  return hits;
}

function countSlots(rows) {
  const gallery = (rows || []).filter((r) => /^materials\.gallery\.\d+$/.test(r.slotKey || ""));
  const scenario = (rows || []).filter((r) => /^overview\.scenario\.\d+$/.test(r.slotKey || ""));
  const openings = (rows || []).filter((r) => r.slotKey === OPENINGS_SLOT);
  const withImg = (list) => list.filter((r) => nz(r.imageUrl)).length;
  return {
    gallery: { total: gallery.length, withImage: withImg(gallery) },
    scenario: { total: scenario.length, withImage: withImg(scenario) },
    openings: { total: openings.length, withImage: withImg(openings) },
    openingsEmptyTitle: openings.filter((r) => !nz(r.title)).length,
    openingsEmptyBody: openings.filter((r) => !nz(r.body)).length,
  };
}

function propertyIdentityChecks(rows, replacement, targetRecordId) {
  const row = (rows || []).find((r) => r.recordId === targetRecordId);
  if (!row) return { pass: false, issues: ["missing_target_row"] };
  const issues = [];
  const title = nz(row.title).toLowerCase();
  const body = nz(row.body).toLowerCase();
  const prop = replacement.propertyName.toLowerCase();
  const code = nz(replacement.propertyCode).toLowerCase();
  const filename = nz(row.imageFilename || row.filename).toLowerCase();
  const tokens = prop.split(/[^a-z0-9]+/).filter((t) => t.length > 3);
  const titleHits = tokens.filter((t) => title.includes(t)).length;
  if (titleHits < Math.min(2, tokens.length) && !title.includes(prop)) {
    issues.push("title_missing_property_tokens");
  }
  if (!body.includes(replacement.officialPropertyReference.toLowerCase()) && !body.includes(code)) {
    issues.push("body_missing_official_url_or_code");
  }
  if (!nz(row.imageUrl)) issues.push("missing_image");
  // Airtable rewrites DAM URLs to airtableusercontent — match on attachment filename.
  const nameHint =
    replacement.harvestKey.includes("orlando")
      ? "orlando"
      : replacement.harvestKey.includes("brickell")
        ? "brickell"
        : "cslth";
  if (filename && !filename.includes(nameHint) && !(code && filename.includes(code))) {
    issues.push("image_filename_property_mismatch_risk");
  }
  // Stale names must be gone
  const shortfall = Object.values(WAVE17_BATCH_A_OPENINGS_SHORTFALLS).find(
    (s) => s.recordId === targetRecordId
  );
  if (
    shortfall &&
    new RegExp(shortfall.invalidProperty.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(
      `${row.title}\n${row.body}`
    )
  ) {
    issues.push("stale_invalid_property_name_remains");
  }
  if (/,\s*thompson hotels\s*—/i.test(row.title || "")) {
    issues.push("thompson_title_double_brand");
  }
  return {
    pass: issues.length === 0,
    issues,
    title: row.title,
    imageUrl: row.imageUrl,
    imageFilename: row.imageFilename || row.filename || null,
  };
}

async function auditBrand(slug) {
  const identity = WAVE17_BATCH_A_IDENTITIES[slug];
  const replacement = WAVE17_BATCH_A_OPENINGS_REPLACEMENTS[slug];
  const shortfall = WAVE17_BATCH_A_OPENINGS_SHORTFALLS[slug];
  const { rows } = await listPresentationRowsLight(
    identity.recordId,
    identity.exactBrandBasicsName
  );
  const slots = countSlots(rows);
  const uniqueness = evaluateImageUniqueness({ brandSlug: slug, presentationRows: rows });
  const roleMatch = evaluateBrandImageRoleMatch({ brandSlug: slug, presentationRows: rows });
  const contamination = scanContamination(slug, rows);
  const identityCheck = propertyIdentityChecks(rows, replacement, shortfall.recordId);

  const broken = [];
  for (const r of rows.filter((x) => nz(x.imageUrl))) {
    const probe = await probeImage(r.imageUrl);
    if (!probe.ok) broken.push({ recordId: r.recordId, slotKey: r.slotKey, ...probe });
  }

  let wrongBrand = 0;
  let wrongProperty = 0;
  for (const r of rows.filter((x) => nz(x.imageUrl))) {
    const reject = isWave17BatchARejectedImageUrl(r.imageUrl, { brandSlug: slug });
    if (reject?.rejected && /wrong_brand|sibling|dream/i.test(reject.reason || "")) {
      wrongBrand += 1;
    }
  }
  if (!identityCheck.pass && identityCheck.issues.includes("image_filename_property_mismatch_risk")) {
    wrongProperty += 1;
  }
  if (!identityCheck.pass && identityCheck.issues.includes("stale_invalid_property_name_remains")) {
    wrongProperty += 1;
  }
  if (!identityCheck.pass && identityCheck.issues.includes("thompson_title_double_brand")) {
    wrongProperty += 1;
  }
  if (!identityCheck.pass && identityCheck.issues.includes("title_missing_property_tokens")) {
    wrongProperty += 1;
  }

  const openingsComplete =
    slots.openings.total >= 3 &&
    slots.openings.withImage >= 3 &&
    slots.openingsEmptyTitle === 0 &&
    slots.openingsEmptyBody === 0;
  const galleryOk = slots.gallery.withImage >= 6;
  const scenarioOk = slots.scenario.withImage >= 3;
  const nonMomentumPass =
    galleryOk && scenarioOk && openingsComplete && uniqueness?.pass !== false && roleMatch?.pass !== false;

  return {
    brandSlug: slug,
    brandName: identity.exactBrandBasicsName,
    slots,
    uniqueness: {
      pass: uniqueness?.pass !== false,
      duplicateGroups: uniqueness?.duplicateGroups || uniqueness?.issues || null,
    },
    roleMatch: {
      pass: roleMatch?.pass !== false,
      summary: roleMatch?.summary || roleMatch?.issues || null,
    },
    propertyIdentity: identityCheck,
    contamination,
    brokenImages: broken,
    wrongBrand,
    wrongProperty,
    nonMomentumCompletenessPass: nonMomentumPass,
    publicCopySafetyPass:
      contamination.filter((c) =>
        ["hyatt_centric_in_thompson", "dream_in_thompson", "historical_playa_as_thompson"].includes(
          c.type
        )
      ).length === 0,
  };
}

export async function planWave17BatchAOpeningsRemediation() {
  const preflight = await runWave17BatchAImageIdentityPreflight();
  if (!preflight.pass) {
    return {
      blocked: true,
      readyStatement: "wave17_batch_a_openings_remediation_blocked_shared_issue",
      preflight,
      research: [],
      diffs: [],
      patches: [],
    };
  }

  const harvest = loadHarvest();
  const research = [];
  const diffs = [];
  const patches = [];
  const blockers = [];

  for (const slug of WAVE17_BATCH_A_APPROVED_SLUGS) {
    const identity = WAVE17_BATCH_A_IDENTITIES[slug];
    const shortfall = WAVE17_BATCH_A_OPENINGS_SHORTFALLS[slug];
    const replacement = WAVE17_BATCH_A_OPENINGS_REPLACEMENTS[slug];
    const rowResearch = researchRow(slug);

    if (replacement.identityConfidence !== "HIGH" || !replacement.replacementRecommended) {
      blockers.push(`${slug}:identity_not_high_or_not_recommended`);
      research.push(rowResearch);
      continue;
    }

    const { rows } = await listPresentationRowsLight(
      identity.recordId,
      identity.exactBrandBasicsName
    );
    const slots = countSlots(rows);
    if (slots.gallery.withImage < 6) blockers.push(`${slug}:gallery_drift_${slots.gallery.withImage}`);
    if (slots.scenario.withImage < 3) blockers.push(`${slug}:scenario_drift_${slots.scenario.withImage}`);
    if (slots.openings.withImage < 2) {
      blockers.push(`${slug}:expected_at_least_2_valid_openings_images_got_${slots.openings.withImage}`);
    }

    const target = rows.find((r) => r.recordId === shortfall.recordId);
    if (!target) {
      blockers.push(`${slug}:missing_shortfall_record_${shortfall.recordId}`);
      research.push(rowResearch);
      continue;
    }

    const corpus = `${nz(target.title)}\n${nz(target.body)}`;
    const stillInvalidName = new RegExp(
      shortfall.invalidProperty.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"),
      "i"
    ).test(corpus);
    const hasReplacementTokens = new RegExp(
      replacement.propertyName.split(/[^a-z0-9]+/i).filter((t) => t.length > 3)[0] || "___",
      "i"
    ).test(nz(target.title));
    const doubleBrandTitle = /,\s*thompson hotels\s*—/i.test(nz(target.title));
    const alreadyRemediated =
      nz(target.imageUrl) &&
      !stillInvalidName &&
      hasReplacementTokens &&
      !doubleBrandTitle;

    if (alreadyRemediated) {
      rowResearch.ImageCandidate = target.imageUrl;
      rowResearch.alreadyRemediated = true;
      research.push(rowResearch);
      diffs.push({
        Brand: identity.exactBrandBasicsName,
        brandSlug: slug,
        PresentationRecord: shortfall.recordId,
        CurrentProperty: shortfall.invalidProperty,
        ReplacementProperty: replacement.propertyName,
        FieldsChanged: [],
        CurrentImage: target.imageUrl,
        ReplacementImage: target.imageUrl,
        Reason: "Already remediated — no further write required",
        Allowed: true,
        CurrentTitle: target.title,
        ReplacementTitle: target.title,
        skipped: true,
      });
      continue;
    }

    // Title-only repair (e.g. Cape double-brand suffix) when image already attached.
    if (nz(target.imageUrl) && !stillInvalidName && (doubleBrandTitle || !hasReplacementTokens)) {
      const fields = buildOpeningsFields({
        brandName: identity.exactBrandBasicsName,
        replacement,
        image: { imageUrl: target.imageUrl, role: "property" },
      });
      // Keep existing Airtable attachment — do not re-upload Image.
      delete fields.Image;
      rowResearch.ImageCandidate = target.imageUrl;
      rowResearch.titleOnlyRepair = true;
      research.push(rowResearch);
      diffs.push({
        Brand: identity.exactBrandBasicsName,
        brandSlug: slug,
        PresentationRecord: shortfall.recordId,
        CurrentProperty: shortfall.invalidProperty,
        ReplacementProperty: replacement.propertyName,
        FieldsChanged: Object.keys(fields),
        CurrentImage: target.imageUrl,
        ReplacementImage: target.imageUrl,
        Reason: "Title/copy repair only — image already attached",
        Allowed: true,
        CurrentTitle: target.title,
        ReplacementTitle: fields.Title,
        titleOnly: true,
      });
      patches.push({
        brandSlug: slug,
        brandName: identity.exactBrandBasicsName,
        recordId: shortfall.recordId,
        slotKey: OPENINGS_SLOT,
        fields,
        imageUrl: target.imageUrl,
        propertyName: replacement.propertyName,
        officialPropertyReference: replacement.officialPropertyReference,
        titleOnly: true,
      });
      continue;
    }

    if (nz(target.imageUrl) && stillInvalidName) {
      // Replace property identity + image on the shortfall card.
    } else if (nz(target.imageUrl) && !stillInvalidName) {
      blockers.push(`${slug}:shortfall_card_unexpected_state`);
      research.push(rowResearch);
      continue;
    }

    const usedGroups = new Set();
    for (const r of rows) {
      if (!nz(r.imageUrl)) continue;
      usedGroups.add(buildImageIdentity(r.imageUrl).duplicateGroupId);
    }

    const harvestProp = harvest.properties?.[replacement.harvestKey];
    if (!harvestProp) {
      blockers.push(`${slug}:missing_harvest_${replacement.harvestKey}`);
      research.push(rowResearch);
      continue;
    }

    const image = pickUnusedImage({
      harvestProp,
      usedGroupIds: usedGroups,
      brandSlug: slug,
      preferredRole: replacement.preferredRole,
    });
    if (!image) {
      blockers.push(`${slug}:no_unused_image_candidate`);
      research.push(rowResearch);
      continue;
    }

    const probe = await probeImage(image.imageUrl);
    if (!probe.ok) {
      blockers.push(`${slug}:image_probe_failed_${probe.status}`);
      research.push({ ...rowResearch, ImageCandidate: image.imageUrl, imageProbe: probe });
      continue;
    }

    rowResearch.ImageCandidate = image.imageUrl;
    rowResearch.imageRole = image.role;
    rowResearch.imageProbe = probe;
    research.push(rowResearch);

    const fields = buildOpeningsFields({
      brandName: identity.exactBrandBasicsName,
      replacement,
      image,
    });

    const diff = {
      Brand: identity.exactBrandBasicsName,
      brandSlug: slug,
      PresentationRecord: shortfall.recordId,
      CurrentProperty: shortfall.invalidProperty,
      ReplacementProperty: replacement.propertyName,
      FieldsChanged: Object.keys(fields),
      CurrentImage: null,
      ReplacementImage: image.imageUrl,
      Reason: replacement.reason,
      Allowed: true,
      CurrentTitle: target.title,
      ReplacementTitle: fields.Title,
    };
    diffs.push(diff);

    patches.push({
      brandSlug: slug,
      brandName: identity.exactBrandBasicsName,
      recordId: shortfall.recordId,
      slotKey: OPENINGS_SLOT,
      fields,
      imageUrl: image.imageUrl,
      propertyName: replacement.propertyName,
      officialPropertyReference: replacement.officialPropertyReference,
    });
  }

  const allHigh =
    research.length === 3 &&
    research.every(
      (r) =>
        r.IdentityConfidence === "HIGH" &&
        r.ReplacementRecommended === "yes" &&
        (r.ImageCandidate || r.alreadyRemediated)
    );
  const needsWrites = patches.length > 0;
  const blocked = blockers.length > 0 || !allHigh;

  return {
    blocked,
    blockers,
    preflight,
    research,
    diffs,
    patches,
    needsWrites,
    readyStatement: blocked
      ? "wave17_batch_a_openings_remediation_partial_hold"
      : needsWrites
        ? "wave17_batch_a_openings_remediation_ready_to_apply"
        : "wave17_batch_a_images_complete_ready_for_post_image_review",
  };
}

export async function applyWave17BatchAOpeningsRemediation({
  patches,
  apply = false,
  argv = [],
} = {}) {
  const flagCheck = parseWave17BatchAOpeningsRemediationFlags(argv);
  if (!apply) {
    return { applied: false, reason: "dry_run_only", flagCheck, writes: [] };
  }
  if (!flagCheck.ok) {
    return { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing, writes: [] };
  }
  if (!patches?.length) {
    return { applied: false, reason: "no_patches", writes: [] };
  }
  if (patches.length > 3) {
    return { applied: false, reason: "too_many_patches", writes: [] };
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY are required");

  const writes = [];
  const errors = [];

  for (const patch of patches) {
    if (!WAVE17_BATCH_A_APPROVED_SLUGS.includes(patch.brandSlug)) {
      errors.push({ recordId: patch.recordId, error: "non_target_brand" });
      continue;
    }
    const shortfall = WAVE17_BATCH_A_OPENINGS_SHORTFALLS[patch.brandSlug];
    if (patch.recordId !== shortfall.recordId) {
      errors.push({ recordId: patch.recordId, error: "unexpected_record_id" });
      continue;
    }

    const fields = { ...patch.fields };
    for (const forbidden of FORBIDDEN_WRITE_FIELDS) {
      if (fields[forbidden] != null) delete fields[forbidden];
    }

    try {
      const meta = { ...fields };
      delete meta.Image;
      if (Object.keys(meta).length) {
        await airtablePresentationWrite({
          baseId,
          apiKey,
          recordId: patch.recordId,
          fields: meta,
          method: "PATCH",
        });
        await sleep(280);
      }
      if (fields.Image) {
        await airtablePresentationWrite({
          baseId,
          apiKey,
          recordId: patch.recordId,
          fields: { Image: fields.Image },
          method: "PATCH",
        });
        await sleep(280);
      }
      writes.push({
        brandSlug: patch.brandSlug,
        recordId: patch.recordId,
        slotKey: OPENINGS_SLOT,
        propertyName: patch.propertyName,
        fieldsWritten: Object.keys(fields),
        imageUrl: patch.imageUrl,
      });
    } catch (err) {
      errors.push({
        brandSlug: patch.brandSlug,
        recordId: patch.recordId,
        error: String(err.message || err),
      });
    }
  }

  return {
    applied: errors.length === 0,
    flagCheck,
    writes,
    errors,
    recentMomentumWrites: 0,
    protectedFieldWrites: 0,
    active65Writes: 0,
    batchBWrites: 0,
    dreamHotelsWrites: 0,
    nonTargetWrites: 0,
    galleryWrites: 0,
    scenarioWrites: 0,
  };
}

function writeFile(rel, contents) {
  const full = path.join(ROOT, rel);
  fs.mkdirSync(path.dirname(full), { recursive: true });
  const body = typeof contents === "string" ? contents : `${JSON.stringify(contents, null, 2)}\n`;
  fs.writeFileSync(full, body.endsWith("\n") ? body : `${body}\n`, "utf8");
  return full;
}

function brandRemediationMd({ research, diff, audit, applyResult }) {
  const lines = [
    `# Wave 17 Batch A openings remediation — ${research.Brand}`,
    ``,
    `- Invalid current property: **${research.InvalidCurrentProperty}**`,
    `- Reason invalid: ${research.ReasonInvalid}`,
    `- Replacement: **${research.ReplacementProperty}** (${research.City}, ${research.Country})`,
    `- Current brand identity: ${research.CurrentBrandIdentity}`,
    `- Official property: ${research.OfficialPropertyReference}`,
    `- Identity confidence: **${research.IdentityConfidence}**`,
    `- Replacement recommended: **${research.ReplacementRecommended}**`,
    `- Presentation record: \`${research.presentationRecordId}\``,
    ``,
    `## Diff`,
    ``,
    `- Fields: ${(diff?.FieldsChanged || []).join(", ") || "n/a"}`,
    `- Current image: none`,
    `- Replacement image: ${diff?.ReplacementImage || research.ImageCandidate || "n/a"}`,
    `- Allowed: ${diff?.Allowed ? "yes" : "no"}`,
    ``,
    `## Post-audit`,
    ``,
    `- Gallery: ${audit?.slots?.gallery?.withImage ?? "?"}/6`,
    `- Scenario: ${audit?.slots?.scenario?.withImage ?? "?"}/3`,
    `- Property/openings: ${audit?.slots?.openings?.withImage ?? "?"}/3`,
    `- Property identity: ${audit?.propertyIdentity?.pass ? "PASS" : "FAIL"}`,
    `- Uniqueness: ${audit?.uniqueness?.pass ? "PASS" : "FAIL"}`,
    `- Role-match: ${audit?.roleMatch?.pass ? "PASS" : "FAIL"}`,
    `- Wrong-brand: ${audit?.wrongBrand ?? "?"}`,
    `- Wrong-property: ${audit?.wrongProperty ?? "?"}`,
    `- Broken images: ${audit?.brokenImages?.length ?? "?"}`,
    `- Contamination hits: ${(audit?.contamination || []).map((c) => c.type).join(", ") || "none"}`,
    ``,
    `## Writes`,
    ``,
    applyResult?.writes?.length
      ? applyResult.writes
          .filter((w) => w.brandSlug === research.brandSlug)
          .map((w) => `- \`${w.recordId}\` fields: ${w.fieldsWritten.join(", ")}`)
          .join("\n") || "- (none for this brand)"
      : "- dry-run / not applied",
    ``,
  ];
  return lines.join("\n");
}

export async function runWave17BatchAOpeningsRemediation({ dryRun = true, argv = [] } = {}) {
  const plan = await planWave17BatchAOpeningsRemediation();
  const apply =
    !dryRun &&
    !plan.blocked &&
    plan.patches.length > 0 &&
    plan.patches.length <= 3 &&
    plan.research.every((r) => r.IdentityConfidence === "HIGH" && r.ReplacementRecommended === "yes");

  let applyResult = {
    applied: false,
    reason: dryRun ? "dry_run_only" : plan.patches.length === 0 ? "already_complete" : "blocked",
    writes: [],
    recentMomentumWrites: 0,
    protectedFieldWrites: 0,
    active65Writes: 0,
    batchBWrites: 0,
    dreamHotelsWrites: 0,
    nonTargetWrites: 0,
  };
  if (apply) {
    applyResult = await applyWave17BatchAOpeningsRemediation({
      patches: plan.patches,
      apply: true,
      argv,
    });
  } else if (!dryRun && plan.blocked) {
    applyResult = { applied: false, reason: "plan_blocked", blockers: plan.blockers, writes: [] };
  }

  const universeAfter = await loadActiveUniverse({ includeDetails: false });
  const activeAfter = universeAfter?.totalCount ?? (universeAfter?.brands || []).length;

  const audits = {};
  for (const slug of WAVE17_BATCH_A_APPROVED_SLUGS) {
    audits[slug] = await auditBrand(slug);
  }

  const allOpenings33 = WAVE17_BATCH_A_APPROVED_SLUGS.every(
    (s) => audits[s].slots.openings.withImage >= 3 && audits[s].slots.gallery.withImage >= 6 && audits[s].slots.scenario.withImage >= 3
  );
  const allIdentity = WAVE17_BATCH_A_APPROVED_SLUGS.every((s) => audits[s].propertyIdentity.pass);
  const allUnique = WAVE17_BATCH_A_APPROVED_SLUGS.every((s) => audits[s].uniqueness.pass);
  const allRole = WAVE17_BATCH_A_APPROVED_SLUGS.every((s) => audits[s].roleMatch.pass);
  const wrongBrand = WAVE17_BATCH_A_APPROVED_SLUGS.reduce((n, s) => n + audits[s].wrongBrand, 0);
  const wrongProperty = WAVE17_BATCH_A_APPROVED_SLUGS.reduce((n, s) => n + audits[s].wrongProperty, 0);
  const broken = WAVE17_BATCH_A_APPROVED_SLUGS.reduce((n, s) => n + (audits[s].brokenImages?.length || 0), 0);
  const contaminationZero = WAVE17_BATCH_A_APPROVED_SLUGS.every(
    (s) => (audits[s].contamination || []).length === 0
  );
  const nonMomentumPass = WAVE17_BATCH_A_APPROVED_SLUGS.every((s) => audits[s].nonMomentumCompletenessPass);

  let readyStatement = "wave17_batch_a_openings_remediation_partial_hold";
  const writePhaseOk =
    applyResult.applied === true ||
    applyResult.reason === "already_complete" ||
    (plan.patches.length === 0 && !plan.blocked);
  if (plan.preflight?.stopRecommended) {
    readyStatement = "wave17_batch_a_openings_remediation_blocked_shared_issue";
  } else if (
    writePhaseOk &&
    allOpenings33 &&
    allIdentity &&
    allUnique &&
    allRole &&
    wrongBrand === 0 &&
    wrongProperty === 0 &&
    broken === 0 &&
    contaminationZero &&
    nonMomentumPass &&
    activeAfter === WAVE17_PROTECTED_ACTIVE_COUNT
  ) {
    readyStatement = "wave17_batch_a_images_complete_ready_for_post_image_review";
  }

  const replacementsJson = {
    version: WAVE17_BATCH_A_OPENINGS_REMEDIATION_VERSION,
    wave: WAVE17_BATCH_A_VERSION,
    generatedAt: new Date().toISOString(),
    replacements: plan.research,
    diffs: plan.diffs,
  };

  const reportJson = {
    version: WAVE17_BATCH_A_OPENINGS_REMEDIATION_VERSION,
    generatedAt: new Date().toISOString(),
    mode: applyResult.applied ? "APPLY" : "DRY_RUN",
    readyStatement,
    activeUniverse: {
      before: plan.preflight?.liveActiveCount ?? plan.preflight?.universe?.totalCount,
      after: activeAfter,
      expected: WAVE17_PROTECTED_ACTIVE_COUNT,
    },
    preflight: plan.preflight,
    blockers: plan.blockers || [],
    research: plan.research,
    diffs: plan.diffs,
    apply: {
      applied: !!applyResult.applied,
      reason: applyResult.reason || null,
      missingFlags: applyResult.missing || null,
      writes: applyResult.writes || [],
      errors: applyResult.errors || [],
      recentMomentumWrites: applyResult.recentMomentumWrites ?? 0,
      protectedFieldWrites: applyResult.protectedFieldWrites ?? 0,
      active65Writes: applyResult.active65Writes ?? 0,
      batchBWrites: applyResult.batchBWrites ?? 0,
      dreamHotelsWrites: applyResult.dreamHotelsWrites ?? 0,
      nonTargetWrites: applyResult.nonTargetWrites ?? 0,
      galleryWrites: 0,
      scenarioWrites: 0,
    },
    audits,
    totals: {
      wrongBrand,
      wrongProperty,
      brokenImages: broken,
      uniquenessPass: allUnique,
      roleMatchPass: allRole,
      propertyIdentityPass: allIdentity,
      contaminationZero,
      nonMomentumCompletenessPass: nonMomentumPass,
      openings33: allOpenings33,
    },
    outOfScope: WAVE17_BATCH_A_OUT_OF_SCOPE,
  };

  writeFile("reports/brand-explorer-wave17-batch-a-openings-replacements.json", replacementsJson);
  writeFile("reports/brand-explorer-wave17-batch-a-openings-remediation.json", reportJson);

  const mdLines = [
    `# Wave 17 Batch A — Openings Remediation`,
    ``,
    `- Generated: ${reportJson.generatedAt}`,
    `- Mode: **${reportJson.mode}**`,
    `- Ready statement: \`${readyStatement}\``,
    `- Active universe: **${reportJson.activeUniverse.before}** → **${activeAfter}** (expected ${WAVE17_PROTECTED_ACTIVE_COUNT})`,
    `- Presentation writes: **${(applyResult.writes || []).length}** (max 3 openings cards)`,
    `- Recent Momentum writes: **0**`,
    `- Protected-field writes: **0**`,
    `- Active 65 writes: **0**`,
    ``,
    `## Research`,
    ``,
  ];
  for (const r of plan.research) {
    mdLines.push(
      `### ${r.Brand}`,
      ``,
      `| Field | Value |`,
      `| --- | --- |`,
      `| Invalid current | ${r.InvalidCurrentProperty} |`,
      `| Reason invalid | ${r.ReasonInvalid} |`,
      `| Replacement | ${r.ReplacementProperty} |`,
      `| City / Country | ${r.City}, ${r.Country} |`,
      `| Current brand identity | ${r.CurrentBrandIdentity} |`,
      `| Official property | ${r.OfficialPropertyReference} |`,
      `| Image candidate | ${r.ImageCandidate || "n/a"} |`,
      `| Identity confidence | **${r.IdentityConfidence}** |`,
      `| Replacement recommended | **${r.ReplacementRecommended}** |`,
      ``
    );
  }
  mdLines.push(`## Dry-run / apply diffs`, ``);
  for (const d of plan.diffs) {
    mdLines.push(
      `- **${d.Brand}** \`${d.PresentationRecord}\`: ${d.CurrentProperty} → **${d.ReplacementProperty}**`,
      `  - Fields: ${d.FieldsChanged.join(", ")}`,
      `  - Image: (none) → ${d.ReplacementImage}`,
      `  - Allowed: ${d.Allowed}`,
      ``
    );
  }
  mdLines.push(`## Post-remediation counts`, ``);
  for (const slug of WAVE17_BATCH_A_APPROVED_SLUGS) {
    const a = audits[slug];
    mdLines.push(
      `- **${a.brandName}**: gallery ${a.slots.gallery.withImage}/6 · scenario ${a.slots.scenario.withImage}/3 · openings ${a.slots.openings.withImage}/3 · uniqueness ${a.uniqueness.pass ? "PASS" : "FAIL"} · role-match ${a.roleMatch.pass ? "PASS" : "FAIL"} · identity ${a.propertyIdentity.pass ? "PASS" : "FAIL"}`
    );
  }
  mdLines.push(
    ``,
    `## Gate totals`,
    ``,
    `- wrong-brand: **${wrongBrand}**`,
    `- wrong-property: **${wrongProperty}**`,
    `- broken images: **${broken}**`,
    `- uniqueness: **${allUnique ? "PASS" : "FAIL"}**`,
    `- role-match: **${allRole ? "PASS" : "FAIL"}**`,
    `- non-Momentum completeness: **${nonMomentumPass ? "PASS" : "FAIL"}**`,
    ``,
    `## Recommended next stage`,
    ``,
    readyStatement === "wave17_batch_a_images_complete_ready_for_post_image_review"
      ? `- Proceed to post-image review (not Batch B / Dream / promote / release / Recent Momentum).`
      : `- Hold openings remediation; resolve blockers before post-image review.`,
    ``
  );
  writeFile("reports/brand-explorer-wave17-batch-a-openings-remediation.md", mdLines.join("\n"));

  const slugToReport = {
    "hyatt-regency": "reports/brand-explorer-wave17-batch-a-regency-opening-remediation.md",
    "hyatt-centric": "reports/brand-explorer-wave17-batch-a-centric-opening-remediation.md",
    "thompson-hotels": "reports/brand-explorer-wave17-batch-a-thompson-opening-remediation.md",
  };
  for (const slug of WAVE17_BATCH_A_APPROVED_SLUGS) {
    const research = plan.research.find((r) => r.brandSlug === slug) || researchRow(slug);
    const diff = plan.diffs.find((d) => d.brandSlug === slug);
    writeFile(
      slugToReport[slug],
      brandRemediationMd({ research, diff, audit: audits[slug], applyResult })
    );
  }

  return {
    ...reportJson,
    pass: readyStatement === "wave17_batch_a_images_complete_ready_for_post_image_review",
  };
}
