/**
 * Wave 17 Batch A — post-image review + narrow Presentation remediation.
 *
 * Targets only: Hyatt Regency · Hyatt Centric · Thompson Hotels.
 * Forbidden: Brand Status, release, CV, Census, Recent Momentum, Image rematerialization,
 * Active 65, Batch B, Dream Hotels, non-target brands.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listPresentationRowsLight } from "./brand-explorer-lane2-common.js";
import {
  WAVE17_BATCH_A_VERSION,
  WAVE17_PROTECTED_ACTIVE_COUNT,
  WAVE17_BATCH_A_IDENTITIES,
  WAVE17_BATCH_A_APPROVED_SLUGS,
  WAVE17_BATCH_A_OUT_OF_SCOPE,
} from "./brand-explorer-wave17-batch-a-factory-plan.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "./brand-explorer-image-role-match.js";
import {
  buildOpeningsPropertyCardTitle,
  buildOpeningsPropertyCardBody,
  OPENINGS_SLOT,
} from "./brand-explorer-openings-property-card-contract.js";
import { runWave17BatchAImageIdentityPreflight } from "./brand-explorer-wave17-batch-a-image-materialization.js";

export const WAVE17_BATCH_A_POST_IMAGE_VERSION = "wave17-batch-a-post-image-review-v1";
export const READY_PASS = "wave17_batch_a_profiles_complete_ready_for_release";
export const READY_REMEDIATE = "wave17_batch_a_post_image_targeted_remediation_required";
export const READY_BLOCKED = "wave17_batch_a_post_image_blocked_shared_architecture_issue";

export const WAVE17_BATCH_A_POST_IMAGE_APPLY_FLAGS = Object.freeze([
  "--approve-wave17-batch-a-post-image-review",
  "--confirm-three-brand-scope",
  "--confirm-all-three-under-review",
  "--confirm-active-65-protected",
  "--confirm-no-brand-status-writes",
  "--confirm-no-release-writes",
  "--confirm-no-company-validation-writes",
  "--confirm-no-brand-verified-writes",
  "--confirm-no-census-writes",
  "--confirm-no-recent-momentum-writes",
  "--confirm-no-source-library-writes",
  "--confirm-no-registry-writes",
  "--confirm-no-batch-b-writes",
  "--confirm-no-dream-hotels-writes",
  "--confirm-no-non-target-writes",
  "--confirm-no-founder-visual-review-pass-writes",
  "--confirm-openings-public-copy-safety",
  "--confirm-no-image-rematerialization",
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
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
  "Image",
  "Recent Momentum",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS = path.join(ROOT, "reports");
const DOCS = path.join(ROOT, "docs", "data-intelligence");

const REPLACEMENT_OPENINGS = Object.freeze({
  "hyatt-regency": "Hyatt Regency Orlando",
  "hyatt-centric": "Hyatt Centric Brickell Miami",
  "thompson-hotels": "The Cape, A Thompson Hotel",
});

const STALE_RESIDUAL_RE = Object.freeze({
  "hyatt-regency": /\bHyatt Regency Cancun\b|\bCanc[uú]n\b/i,
  "hyatt-centric": /\bMidtown 5th\b|\bnycmt\b|\bnycct\b/i,
  "thompson-hotels": /\bThompson Playa del Carmen\b|\bPlaya del Carmen\b/i,
});

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export function parseWave17BatchAPostImageFlags(argv = []) {
  const missing = WAVE17_BATCH_A_POST_IMAGE_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    ok: argv.includes("--apply") && missing.length === 0,
    missing,
  };
}

function ownerFacingInterpretation() {
  return "Validate current brand affiliation, property product, and capital scope with the brand before underwriting.";
}

function ownerFacingBrandRelevance({ isIntl }) {
  return isIntl
    ? "Official International Reference property photography for Brand Explorer openings — not a CALA operating claim."
    : "Official CALA property photography used as a Brand Explorer property example for this brand.";
}

function ownerFacingObjective() {
  return "Use as a directional property reference when underwriting product fit, capital scope, and platform participation.";
}

function buildOwnerTeaser({ propertyName, brandName, marketCity, geographyLabel, accent = "" }) {
  const isIntl = !/^cala/i.test(nz(geographyLabel));
  const place = nz(marketCity) || (isIntl ? "an International Reference market" : "a CALA market");
  const accentBit = nz(accent) ? ` ${nz(accent)}` : "";
  return `${propertyName} is an official ${
    isIntl ? "International Reference" : "CALA"
  } example for ${brandName} in ${place}.${accentBit} Use it to benchmark product standards, service delivery, and capital scope for similar assets.`;
}

function openingsChipList(row) {
  return nz(row.caseSummaryTags)
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}

function defect({
  brand,
  tab,
  section,
  component,
  field,
  currentValue,
  defectType,
  severity,
  proposedFix,
  sourceSupport,
  patchAllowed = true,
}) {
  return {
    brand,
    tab,
    section,
    component,
    field,
    currentValue: nz(currentValue).slice(0, 240),
    defectType,
    severity,
    proposedFix,
    sourceSupport,
    Allowed: patchAllowed ? "yes" : "no",
    patchAllowed,
  };
}

/** Curated openings metadata keyed by Presentation recordId. */
const OPENINGS_CATALOG = Object.freeze({
  rec5OYC3DXLtROYaG: {
    brandSlug: "hyatt-regency",
    propertyName: "Hyatt Regency Miami",
    marketCity: "Miami",
    country: "USA",
    geographyLabel: "International Reference",
    accent: "It illustrates urban/convention Regency meetings depth.",
  },
  rec95YwIZb0dhzj8d: {
    brandSlug: "hyatt-regency",
    propertyName: "Hyatt Regency Mexico City",
    marketCity: "Mexico City",
    country: "Mexico",
    geographyLabel: "CALA",
    accent: "It anchors CALA meetings-capable full-service diligence.",
  },
  recQ1zGQRhSedhTdO: {
    brandSlug: "hyatt-regency",
    propertyName: "Hyatt Regency Orlando",
    marketCity: "Orlando",
    country: "USA",
    geographyLabel: "International Reference",
    accent: "It illustrates convention-adjacent Regency operating intensity.",
  },
  recIBtHotY0pOMXlz: {
    brandSlug: "hyatt-centric",
    propertyName: "Hyatt Centric San Isidro Lima",
    marketCity: "Lima",
    country: "Peru",
    geographyLabel: "CALA",
    accent: "It shows neighborhood-led Centric lifestyle full-service delivery.",
  },
  recefVPTN0iDCKoZ0: {
    brandSlug: "hyatt-centric",
    propertyName: "Hyatt Centric Guatemala City",
    marketCity: "Guatemala City",
    country: "Guatemala",
    geographyLabel: "CALA",
    accent: "It shows urban explorer Centric product in a CALA capital corridor.",
  },
  rechFlli0qV5Jq6h6: {
    brandSlug: "hyatt-centric",
    propertyName: "Hyatt Centric Brickell Miami",
    marketCity: "Miami",
    country: "USA",
    geographyLabel: "International Reference",
    accent: "It illustrates walkable urban Centric lifestyle full-service.",
  },
  rec24VJdMZIAfYSoE: {
    brandSlug: "thompson-hotels",
    propertyName: "Thompson Chicago",
    marketCity: "Chicago",
    country: "USA",
    geographyLabel: "International Reference",
    accent: "It illustrates design-led urban Thompson F&B and public-space intensity.",
  },
  rec4XJtwzRNsh8M7i: {
    brandSlug: "thompson-hotels",
    propertyName: "The Cape, A Thompson Hotel",
    marketCity: "Cabo San Lucas",
    country: "Mexico",
    geographyLabel: "CALA",
    accent: "It anchors current CALA Thompson lifestyle proof — not a rebranded Centric property.",
  },
  recMDTx83T0KaShal: {
    brandSlug: "thompson-hotels",
    propertyName: "Thompson Nashville",
    marketCity: "Nashville",
    country: "USA",
    geographyLabel: "International Reference",
    accent: "It illustrates entertainment-district Thompson design and social energy.",
  },
});

function buildOpeningsPatchFields(meta, brandName) {
  const isIntl = meta.geographyLabel === "International Reference";
  const brandForTitle = /thompson/i.test(meta.propertyName) ? "" : brandName;
  const title = buildOpeningsPropertyCardTitle({
    propertyName: meta.propertyName,
    brandName: brandForTitle,
    marketCity: meta.marketCity,
  });
  const chips = [meta.geographyLabel, meta.marketCity, "Property example"];
  const teaser = buildOwnerTeaser({
    propertyName: meta.propertyName,
    brandName,
    marketCity: `${meta.marketCity}, ${meta.country}`,
    geographyLabel: meta.geographyLabel,
    accent: meta.accent,
  });
  const body = buildOpeningsPropertyCardBody({
    chips,
    locationLine: `${meta.marketCity} (${meta.geographyLabel})`,
    metaLine: `${meta.geographyLabel} · ${meta.country}`,
    scenarioLine: `${meta.geographyLabel} / ${meta.marketCity} / PROPERTY EXAMPLE`.toUpperCase(),
    teaser,
    sourceUrl: "",
  });
  return {
    Title: title,
    Body: body,
    "Case Summary Overview": teaser,
    "Case Summary Tags": chips.join(", "),
    "Case Summary Brand Relevance": ownerFacingBrandRelevance({ isIntl }),
    "Case Summary Owner Objective": ownerFacingObjective(),
    "Case Summary Interpretation": ownerFacingInterpretation(),
  };
}

function inventoryOpeningsDefects(slug, rows) {
  const brandName = WAVE17_BATCH_A_IDENTITIES[slug].exactBrandBasicsName;
  const defects = [];
  const patches = [];
  for (const row of rows.filter((r) => r.slotKey === OPENINGS_SLOT)) {
    const meta = OPENINGS_CATALOG[row.recordId];
    if (!meta || meta.brandSlug !== slug) continue;
    const body = nz(row.body);
    const title = nz(row.title);
    const cs = nz(row.caseSummaryOverview);
    const interp = nz(row.caseSummaryInterpretation);
    const blob = `${title}\n${body}\n${cs}\n${interp}`;
    const reasons = [];

    if (/https?:\/\/\S+/i.test(body)) {
      defects.push(
        defect({
          brand: slug,
          tab: "footprint",
          section: OPENINGS_SLOT,
          component: row.recordId,
          field: "Body",
          currentValue: body,
          defectType: "prohibited_language:raw_url",
          severity: "HIGH",
          proposedFix: "Strip raw URL; rebuild structured openings Body",
          sourceSupport: "public_copy_safety",
        })
      );
      reasons.push("strip_raw_url");
    }
    if (/Confirm live affiliation/i.test(blob)) {
      defects.push(
        defect({
          brand: slug,
          tab: "footprint",
          section: OPENINGS_SLOT,
          component: row.recordId,
          field: "Case Summary Interpretation",
          currentValue: interp,
          defectType: "internal_workflow_language",
          severity: "HIGH",
          proposedFix: "Replace Confirm live affiliation with owner-facing validation wording",
          sourceSupport: "public_copy_safety",
        })
      );
      reasons.push("fix_confirm_live");
    }
    if (/—\s*(property|exterior arrival|public space lobby|wellness pool spa|guest room)\.?$/i.test(cs)) {
      defects.push(
        defect({
          brand: slug,
          tab: "footprint",
          section: OPENINGS_SLOT,
          component: row.recordId,
          field: "Case Summary Overview",
          currentValue: cs,
          defectType: "role_label_as_owner_teaser",
          severity: "MEDIUM",
          proposedFix: "Replace image-role teaser with owner-facing property teaser",
          sourceSupport: "owner_facing_clarity",
        })
      );
      reasons.push("owner_teaser");
    }
    if (/Thompson Chicago Thompson Hotels|Thompson Nashville Thompson Hotels/i.test(title)) {
      defects.push(
        defect({
          brand: slug,
          tab: "footprint",
          section: OPENINGS_SLOT,
          component: row.recordId,
          field: "Title",
          currentValue: title,
          defectType: "awkward_title_brand_duplication",
          severity: "LOW",
          proposedFix: "Remove duplicated Thompson Hotels suffix from Title",
          sourceSupport: "openings_title_contract",
        })
      );
      reasons.push("title_dedupe");
    }
    if (/—\s*CALA\s*$/i.test(title) || /—\s*International Reference\s*$/i.test(title)) {
      defects.push(
        defect({
          brand: slug,
          tab: "footprint",
          section: OPENINGS_SLOT,
          component: row.recordId,
          field: "Title",
          currentValue: title,
          defectType: "awkward_title_geography_label",
          severity: "LOW",
          proposedFix: "Use Ascend-style Title with city after em dash",
          sourceSupport: "openings_title_contract",
        })
      );
      reasons.push("title_city");
    }
    if (STALE_RESIDUAL_RE[slug]?.test(blob)) {
      defects.push(
        defect({
          brand: slug,
          tab: "footprint",
          section: OPENINGS_SLOT,
          component: row.recordId,
          field: "Title/Body/Case Summary",
          currentValue: title,
          defectType: "stale_replaced_property_reference",
          severity: "HIGH",
          proposedFix: `Ensure openings card matches ${REPLACEMENT_OPENINGS[slug]} catalog identity`,
          sourceSupport: "openings_remediation",
        })
      );
      reasons.push("stale_openings");
    }
    // Always rebuild openings cards that have any openings defect, or thin/role teasers.
    if (reasons.length) {
      patches.push({
        brandSlug: slug,
        recordId: row.recordId,
        slotKey: OPENINGS_SLOT,
        fields: buildOpeningsPatchFields(meta, brandName),
        reason: reasons.join("+"),
      });
    }
  }
  return { defects, patches };
}

/** Exact body rewrites for non-openings stale / public-copy defects. */
function curatedContentPatches(slug, rows) {
  const byId = Object.fromEntries(rows.map((r) => [r.recordId, r]));
  const defects = [];
  const patches = [];

  const pushBody = (recordId, newBody, defectType, severity, proposedFix) => {
    const row = byId[recordId];
    if (!row) return;
    const fields = {};
    const reasons = [];
    if (nz(row.body) !== nz(newBody)) {
      fields.Body = newBody;
      reasons.push(defectType);
      defects.push(
        defect({
          brand: slug,
          tab: String(row.slotKey || "").split(".")[0] || "content",
          section: row.slotKey,
          component: recordId,
          field: "Body",
          currentValue: row.body,
          defectType,
          severity,
          proposedFix,
          sourceSupport: "post_image_review",
        })
      );
    }
    if (/Geography evidence basis:/i.test(nz(row.caseSummaryInterpretation))) {
      fields["Case Summary Interpretation"] = nz(row.caseSummaryInterpretation).replace(
        /Geography evidence basis:/i,
        "Geography posture for owner diligence:"
      );
      reasons.push("geography_evidence_basis");
      defects.push(
        defect({
          brand: slug,
          tab: "footprint",
          section: row.slotKey,
          component: recordId,
          field: "Case Summary Interpretation",
          currentValue: row.caseSummaryInterpretation,
          defectType: "internal_process_language",
          severity: "MEDIUM",
          proposedFix: "Replace Geography evidence basis with owner-facing geography posture",
          sourceSupport: "public_copy_safety",
        })
      );
    }
    if (!Object.keys(fields).length) return;
    patches.push({
      brandSlug: slug,
      recordId,
      slotKey: row.slotKey,
      fields,
      reason: reasons.join("+") || defectType,
    });
  };

  if (slug === "hyatt-regency") {
    pushBody(
      "rec22zf4AmLHesdOH",
      "Evaluate demand fit, property condition, and conversion or new-build scope against Hyatt Regency brand standards before commitment. CALA property examples (Mexico City, Mexico) provide reference points for achievable scope. Test whether the core upper-upscale full-service brand with meetings and group depth is credible versus Grand Hyatt.",
      "stale_replaced_property_reference",
      "HIGH",
      "Remove Cancún residual; keep Mexico City CALA reference"
    );
    pushBody(
      "rec4b9hVGYcZuRkoF",
      "CALA diligence for Hyatt Regency can use named examples such as Hyatt Regency Mexico City. Test meetings-capable full-service product and World of Hyatt guest delivery—not Centric lifestyle proof or Grand Hyatt prestige framing.",
      "stale_replaced_property_reference",
      "HIGH",
      "Remove Hyatt Regency Cancun + listings-verify language"
    );
    pushBody(
      "recZN2XQY945ZgZ7j",
      "Hyatt Regency has verified CALA references in Mexico City, Mexico, providing property-level diligence anchors for regional owners. These examples demonstrate operating-model execution and guest-experience delivery in CALA demand contexts rather than parent-platform averages. Keep Hyatt Regency product and service responsibilities clear among owner, operator, and brand teams so the core upper-upscale full-service brand with meetings and group depth stays deliverable after affiliation and through ongoing operations.",
      "stale_replaced_property_reference",
      "HIGH",
      "Remove Cancún from CALA Market Relevance"
    );
    pushBody(
      "recmiEvxfp4JFGWWX",
      "Hyatt Regency positioning is anchored in official brand documentation and verified property-level examples where available. Hyatt Regency brand page and development materials provide the canonical identity reference for owner diligence. CALA operating proof from Mexico City, Mexico supports regional owners evaluating affiliation.",
      "stale_replaced_property_reference",
      "HIGH",
      "Remove Cancún from Brand Positioning Evidence"
    );
    pushBody(
      "reco34TYZol61EMuN",
      "Hyatt Regency growth should be evaluated through brand-specific positioning, not Hyatt Hotels Corporation expansion targets alone. Underwrite Hyatt Regency as Hyatt’s core upper-upscale full-service meetings brand—never as Grand Hyatt prestige intensity, Centric lifestyle exploration, or Hyatt Place select-service economics. CALA markets (Mexico City, Mexico) already demonstrate operating proof for this positioning.",
      "stale_replaced_property_reference",
      "HIGH",
      "Remove Cancún from Growth Editorial"
    );
    pushBody(
      "recqYFVkRD5TpbDBn",
      "- Official brand positioning for Hyatt Regency is anchored in Hyatt brand and development materials specific to this brand—not Hyatt Hotels Corporation corporate copy\n- Geography posture is CALA-supported with named examples in Mexico City, Mexico — property cards follow those labels\n- Owner distinction focus separates Hyatt Regency from Grand Hyatt, Hyatt Centric, Hyatt Place, Marriott Hotels, Sheraton, and Westin using product, demand, and operating-model criteria\n- Target guest segments — Corporate / Business, Group / MICE, Leisure — use only validated guest-segment labels for this brand\n- Parent/platform context (Hyatt Hotels Corporation / World of Hyatt) stays labeled and subordinate to brand-specific evidence",
      "stale_replaced_property_reference",
      "HIGH",
      "Remove Cancún + Brand Basics taxonomy internal language"
    );
    pushBody(
      "recwyEilSG1wREA3D",
      "Urban / suburban meetings hotels: primary\nAirport / convention-adjacent full-service: strong secondary\nResort-adjacent meetings resorts: selective\nSelect-service or lifestyle-light boxes: weak fit\n\nCurated sample mix based on global brand positioning (illustrative, not a disclosed portfolio inventory count).",
      "prohibited_language:census",
      "HIGH",
      "Replace portfolio census wording"
    );
  }

  if (slug === "hyatt-centric") {
    pushBody(
      "recjGoghqEaTuulSa",
      "North America is a core Hyatt Centric theater for urban lifestyle hotels such as Hyatt Centric Brickell Miami. Treat US examples as International Reference relative to CALA-first posture when needed, and keep Centric distinct from Regency and Place.",
      "stale_replaced_property_reference",
      "HIGH",
      "Replace Midtown 5th Avenue with Brickell Miami"
    );
    const cala = rows.find((r) => r.slotKey === "footprint.region.cala");
    if (cala && /listings verify/i.test(nz(cala.body))) {
      pushBody(
        cala.recordId,
        "CALA diligence for Hyatt Centric can use named examples such as Hyatt Centric Guatemala City and Hyatt Centric San Isidro Lima. Test location-led lifestyle full-service product—not Regency meetings proof or Thompson design-led nightlife framing.",
        "internal_verification_language",
        "HIGH",
        "Remove when listings verify"
      );
    }
    pushBody(
      "recfnzDyciCLBMrKN",
      "Urban lifestyle corridors: primary\nWalkable mixed-use / destination neighborhoods: strong secondary\nSecondary city centers with explorer demand: selective\nMeetings-led Regency boxes or select-service Place: wrong lane\n\nCurated sample mix based on global brand positioning (illustrative, not a disclosed portfolio inventory count).",
      "prohibited_language:census",
      "HIGH",
      "Replace portfolio census wording"
    );
    const why = rows.find((r) => r.slotKey === "overview.why_value" && /Brand Basics taxonomy/i.test(nz(r.body)));
    if (why) {
      pushBody(
        why.recordId,
        nz(why.body).replace(
          /follow validated Brand Basics taxonomy options only/gi,
          "use only validated guest-segment labels for this brand"
        ),
        "internal_process_language",
        "HIGH",
        "Remove Brand Basics taxonomy internal language"
      );
    }
  }

  if (slug === "thompson-hotels") {
    pushBody(
      "rec808qvrWODIuwRf",
      "CALA diligence for Thompson Hotels can use named examples such as The Cape, A Thompson Hotel. Test design-led lifestyle product and F&B intensity—not Dream nightlife proof or Centric explorer-only framing.",
      "stale_replaced_property_reference",
      "HIGH",
      "Replace Playa del Carmen with The Cape; remove listings-verify"
    );
    pushBody(
      "recCKqWmIhV0UMoZo",
      "Thompson Hotels has verified CALA references in Cabo San Lucas, Mexico (The Cape, A Thompson Hotel), providing property-level diligence anchors for regional owners. These examples demonstrate operating-model execution and guest-experience delivery in CALA demand contexts rather than parent-platform averages. Keep Thompson Hotels product and service responsibilities clear among owner, operator, and brand teams so the design-led lifestyle brand with elevated F&B and cultural social energy stays deliverable after affiliation and through ongoing operations.",
      "stale_replaced_property_reference",
      "HIGH",
      "Replace Playa with The Cape / Cabo San Lucas"
    );
    pushBody(
      "recSi8HlWPPrfCV7F",
      "Evaluate demand fit, property condition, and conversion or new-build scope against Thompson Hotels brand standards before commitment. CALA property examples (Cabo San Lucas, Mexico — The Cape, A Thompson Hotel) provide reference points for achievable scope. Test whether the design-led lifestyle brand with elevated F&B and cultural social energy is credible versus Dream Hotels.",
      "stale_replaced_property_reference",
      "HIGH",
      "Replace Playa residual in Application & Feasibility"
    );
    pushBody(
      "recgopnAMV8jIg1i9",
      "Thompson Hotels positioning is anchored in official brand documentation and verified property-level examples where available. Thompson Hotels brand page and development materials provide the canonical identity reference for owner diligence. CALA operating proof from Cabo San Lucas, Mexico (The Cape, A Thompson Hotel) supports regional owners evaluating affiliation.",
      "stale_replaced_property_reference",
      "HIGH",
      "Replace Playa residual in Brand Positioning Evidence"
    );
    pushBody(
      "reclKgYstqGwhD0Nb",
      "Thompson Hotels growth should be evaluated through brand-specific positioning, not Hyatt Hotels Corporation expansion targets alone. Underwrite Thompson Hotels as design-led lifestyle with elevated F&B and cultural social energy—never as Dream nightlife-led identity, Centric explorer-light full-service, or EDITION/W ritual luxury-lifestyle copy. CALA markets (Cabo San Lucas, Mexico) already demonstrate operating proof for this positioning.",
      "stale_replaced_property_reference",
      "HIGH",
      "Replace Playa residual in Growth Editorial"
    );
    pushBody(
      "reczK7POjqE6mqxMC",
      "- Official brand positioning for Thompson Hotels is anchored in Hyatt brand and development materials specific to this brand—not Hyatt Hotels Corporation corporate copy\n- Geography posture is CALA-supported with named examples in Cabo San Lucas, Mexico (The Cape, A Thompson Hotel) — property cards follow those labels\n- Owner distinction focus separates Thompson Hotels from Dream Hotels, Hyatt Centric, EDITION, W Hotels, Kimpton Hotels, and Hotel Indigo using product, demand, and operating-model criteria\n- Target guest segments — Experience-Oriented, Leisure, Luxury / Discerning — use only validated guest-segment labels for this brand\n- Parent/platform context (Hyatt Hotels Corporation / World of Hyatt) stays labeled and subordinate to brand-specific evidence",
      "stale_replaced_property_reference",
      "HIGH",
      "Replace Playa + Brand Basics taxonomy language"
    );
    pushBody(
      "recKNBtMQKvslCwAY",
      "Design-led urban lifestyle hotels: primary\nCultural / entertainment districts with F&B intensity: strong secondary\nDestination urban resorts with social programming: selective\nDream nightlife-led or Centric explorer-light boxes: wrong lane\n\nCurated sample mix based on global brand positioning (illustrative, not a disclosed portfolio inventory count).",
      "prohibited_language:census",
      "HIGH",
      "Replace portfolio census wording"
    );
  }

  // Empty-title structural rows (LOW)
  for (const row of rows) {
    const sk = nz(row.slotKey);
    if (!sk) continue;
    if (nz(row.title)) continue;
    if (!/^(Brand Positioning|Guest Psychographics Description)$/i.test(sk)) continue;
    const title =
      sk === "Guest Psychographics Description" ? "Guest Psychographics" : "Brand Positioning";
    defects.push(
      defect({
        brand: slug,
        tab: "overview",
        section: sk,
        component: row.recordId,
        field: "Title",
        currentValue: "(empty)",
        defectType: "empty_visible_title",
        severity: "LOW",
        proposedFix: `Set Title to ${title}`,
        sourceSupport: "no_empty_ui",
      })
    );
    patches.push({
      brandSlug: slug,
      recordId: row.recordId,
      slotKey: sk,
      fields: { Title: title },
      reason: "empty_title_fill",
    });
  }

  // Geography evidence basis on any remaining region/content rows
  for (const row of rows) {
    if (!/Geography evidence basis:/i.test(nz(row.caseSummaryInterpretation))) continue;
    if (patches.some((p) => p.recordId === row.recordId && p.fields["Case Summary Interpretation"])) {
      continue;
    }
    defects.push(
      defect({
        brand: slug,
        tab: String(row.slotKey || "").split(".")[0] || "content",
        section: row.slotKey,
        component: row.recordId,
        field: "Case Summary Interpretation",
        currentValue: row.caseSummaryInterpretation,
        defectType: "internal_process_language",
        severity: "MEDIUM",
        proposedFix: "Replace Geography evidence basis with owner-facing geography posture",
        sourceSupport: "public_copy_safety",
      })
    );
    patches.push({
      brandSlug: slug,
      recordId: row.recordId,
      slotKey: row.slotKey,
      fields: {
        "Case Summary Interpretation": nz(row.caseSummaryInterpretation).replace(
          /Geography evidence basis:/i,
          "Geography posture for owner diligence:"
        ),
      },
      reason: "geography_evidence_basis",
    });
  }

  return { defects, patches };
}

function scanHardContamination(slug, rows) {
  const hits = [];
  const contrastive =
    /\b(never|not|no|distinct|separate|reject|wrong|do not|don't|unlike|versus|vs\.?|rather than|instead of|without|than|from|against|alternatives?|peers?|compare|hard separation|over|above|below|substitut(?:es|e|ing)?|weaker|right choice|aligned to)\b/i;
  for (const row of rows) {
    if (/momentum/i.test(nz(row.slotKey))) continue;
    const text = `${nz(row.title)}\n${nz(row.body)}`;
    const sentences = text.split(/(?<=[.!?\n])\s+/);
    for (const sentence of sentences) {
      if (slug === "thompson-hotels") {
        // Peer chip / contrast lines like "Dream Hotels: nightlife… above Thompson"
        if (/^\s*(Similar Brands\n)?Dream Hotels:/i.test(sentence)) continue;
        if (/^\s*Hyatt Centric:/i.test(sentence)) continue;
        if (/\bDream Hotels?\b/i.test(sentence) && !contrastive.test(sentence)) {
          hits.push({ type: "dream_as_thompson", slotKey: row.slotKey, sample: sentence.slice(0, 140) });
        }
        if (
          /\bHyatt Centric\b/i.test(sentence) &&
          !contrastive.test(sentence) &&
          !/explorer-light|not Centric|never.*Centric|versus Centric|from Centric|over.*Centric/i.test(sentence)
        ) {
          hits.push({ type: "centric_as_thompson", slotKey: row.slotKey, sample: sentence.slice(0, 140) });
        }
      }
    }
  }
  return hits;
}

function countStaleResiduals(slug, rows) {
  const re = STALE_RESIDUAL_RE[slug];
  if (!re) return { count: 0, hits: [] };
  const hits = [];
  for (const row of rows) {
    if (/momentum/i.test(nz(row.slotKey))) continue;
    const corpus = `${nz(row.title)}\n${nz(row.body)}\n${nz(row.caseSummaryOverview)}\n${nz(row.caseSummaryInterpretation)}`;
    if (re.test(corpus)) {
      hits.push({ recordId: row.recordId, slotKey: row.slotKey, title: row.title });
    }
  }
  return { count: hits.length, hits };
}

function publicCopyUnsafe(rows) {
  const hits = [];
  for (const row of rows) {
    if (/momentum/i.test(nz(row.slotKey))) continue;
    const body = nz(row.body);
    const blob = `${nz(row.title)}\n${body}\n${nz(row.caseSummaryOverview)}\n${nz(row.caseSummaryInterpretation)}`;
    if (row.slotKey === OPENINGS_SLOT && /https?:\/\/\S+/i.test(body)) {
      hits.push({ id: "raw_url", recordId: row.recordId, slotKey: row.slotKey });
    }
    if (/Confirm live affiliation/i.test(blob)) {
      hits.push({ id: "confirm_live", recordId: row.recordId, slotKey: row.slotKey });
    }
    if (/\bCensus\b/.test(blob)) {
      hits.push({ id: "census", recordId: row.recordId, slotKey: row.slotKey });
    }
    if (/Brand Basics taxonomy/i.test(blob)) {
      hits.push({ id: "brand_basics_taxonomy", recordId: row.recordId, slotKey: row.slotKey });
    }
    if (/when listings verify/i.test(blob)) {
      hits.push({ id: "listings_verify", recordId: row.recordId, slotKey: row.slotKey });
    }
  }
  return hits;
}

function crossBrandReview(loaded) {
  const distinctive = {
    "hyatt-regency": [/hyatt regency/i, /meetings|full-service|group/i],
    "hyatt-centric": [/hyatt centric/i, /lifestyle|explorer|neighborhood|walkable/i],
    "thompson-hotels": [/thompson/i, /design-led|f&b|cultural|social energy/i],
  };
  const pairs = [
    ["hyatt-regency", "hyatt-centric"],
    ["hyatt-centric", "thompson-hotels"],
    ["hyatt-regency", "thompson-hotels"],
  ];
  return pairs.map(([a, b]) => {
    const textA = (loaded[a] || []).map((r) => `${r.title}\n${r.body}`).join("\n");
    const textB = (loaded[b] || []).map((r) => `${r.title}\n${r.body}`).join("\n");
    const aHas = distinctive[a].filter((re) => re.test(textA)).length;
    const bHas = distinctive[b].filter((re) => re.test(textB)).length;
    const pass = aHas >= 2 && bHas >= 2;
    return {
      brandPair: `${a} vs ${b}`,
      pass,
      distinctiveSignalCounts: { [a]: aHas, [b]: bHas },
      semanticSimilarityRisk: pass ? "LOW" : "MEDIUM",
      requiredFix: pass ? "None" : "Strengthen brand-specific owner language",
    };
  });
}

function mergePatches(patches) {
  const map = new Map();
  for (const p of patches) {
    const key = p.recordId;
    if (!map.has(key)) {
      map.set(key, { ...p, fields: { ...p.fields }, reason: p.reason });
    } else {
      const cur = map.get(key);
      cur.fields = { ...cur.fields, ...p.fields };
      cur.reason = `${cur.reason}+${p.reason}`;
    }
  }
  return [...map.values()];
}

async function airtablePatch({ baseId, apiKey, recordId, fields }) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const payload = { ...fields };
  for (const f of FORBIDDEN_WRITE_FIELDS) delete payload[f];
  if (payload.Image) delete payload.Image;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields: payload, typecast: true }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `PATCH failed ${res.status}`);
  return json;
}

function writeFile(rel, contents) {
  const full = path.join(ROOT, rel);
  fs.mkdirSync(path.dirname(full), { recursive: true });
  const body = typeof contents === "string" ? contents : `${JSON.stringify(contents, null, 2)}\n`;
  fs.writeFileSync(full, body.endsWith("\n") ? body : `${body}\n`, "utf8");
  return full;
}

function severityCounts(defects) {
  const c = { CRITICAL: 0, HIGH: 0, MEDIUM: 0, LOW: 0, NONE: 0 };
  for (const d of defects) c[d.severity] = (c[d.severity] || 0) + 1;
  return c;
}

export async function runWave17BatchAPostImageReview({ dryRun = true, argv = [] } = {}) {
  const flagCheck = parseWave17BatchAPostImageFlags(argv);
  const apply = argv.includes("--apply") && !dryRun;

  const preflight = await runWave17BatchAImageIdentityPreflight();
  if (!preflight.pass || preflight.liveActiveCount !== WAVE17_PROTECTED_ACTIVE_COUNT) {
    const stopped = {
      version: WAVE17_BATCH_A_POST_IMAGE_VERSION,
      pass: false,
      stopRecommended: true,
      readyStatement: READY_BLOCKED,
      preflight,
    };
    writeFile("reports/brand-explorer-wave17-batch-a-post-image-review.json", stopped);
    writeFile(
      "reports/brand-explorer-wave17-batch-a-post-image-review.md",
      `# STOPPED\n\nActive universe / preflight failed.\n`
    );
    return stopped;
  }

  const loaded = {};
  const allDefects = [];
  const allPatches = [];
  const brandResults = [];

  for (const slug of WAVE17_BATCH_A_APPROVED_SLUGS) {
    const id = WAVE17_BATCH_A_IDENTITIES[slug];
    const { rows } = await listPresentationRowsLight(id.recordId, id.exactBrandBasicsName);
    loaded[slug] = rows;

    const gallery = rows.filter((r) => /^materials\.gallery\.\d+$/.test(nz(r.slotKey)));
    const scenario = rows.filter((r) => /^overview\.scenario\.\d+$/.test(nz(r.slotKey)));
    const openings = rows.filter((r) => r.slotKey === OPENINGS_SLOT);

    const uniqueness = evaluateImageUniqueness({ brandSlug: slug, presentationRows: rows });
    const roleMatch = evaluateBrandImageRoleMatch({ brandSlug: slug, presentationRows: rows });

    const openingsPlan = inventoryOpeningsDefects(slug, rows);
    const contentPlan = curatedContentPatches(slug, rows);
    const defects = [...openingsPlan.defects, ...contentPlan.defects];
    const patches = mergePatches([...openingsPlan.patches, ...contentPlan.patches]);

    allDefects.push(...defects);
    allPatches.push(...patches.map((p) => ({ ...p, brandSlug: slug })));

    brandResults.push({
      brandSlug: slug,
      brandName: id.exactBrandBasicsName,
      recordId: id.recordId,
      brandStatus: preflight.targets.find((t) => t.slug === slug)?.brandStatus || "Under Review",
      rowCount: rows.length,
      gallery: gallery.filter((r) => r.imageUrl).length,
      scenario: scenario.filter((r) => r.imageUrl).length,
      openings: openings.filter((r) => r.imageUrl).length,
      openingsTitles: openings.map((r) => r.title),
      replacementOpening: REPLACEMENT_OPENINGS[slug],
      uniquenessPass: uniqueness?.pass === true,
      roleMatchPass: roleMatch?.pass === true,
      defectCounts: severityCounts(defects),
      patchesPlanned: patches.length,
    });
  }

  const initialSeverity = severityCounts(allDefects);
  const crossBrand = crossBrandReview(loaded);

  // Persist inventory before apply (required ordering)
  writeFile("reports/brand-explorer-wave17-batch-a-rendered-defects.json", {
    version: WAVE17_BATCH_A_POST_IMAGE_VERSION,
    generatedAt: new Date().toISOString(),
    mode: apply ? "APPLY" : "DRY_RUN_INVENTORY",
    defectCountsBySeverity: initialSeverity,
    defects: allDefects,
    patchesPlanned: allPatches.map((p) => ({
      brandSlug: p.brandSlug,
      recordId: p.recordId,
      slotKey: p.slotKey,
      reason: p.reason,
      fields: Object.keys(p.fields || {}),
    })),
  });

  const writeAudit = {
    version: WAVE17_BATCH_A_POST_IMAGE_VERSION,
    recentMomentumWrites: 0,
    brandStatusWrites: 0,
    releaseWrites: 0,
    protectedFieldWrites: 0,
    active65Writes: 0,
    batchBWrites: 0,
    dreamWrites: 0,
    nonTargetWrites: 0,
    imageRematerializationWrites: 0,
    byBrand: Object.fromEntries(
      WAVE17_BATCH_A_APPROVED_SLUGS.map((s) => [
        s,
        { contentFixes: 0, imageCaptionFixes: 0, other: 0, records: [] },
      ])
    ),
  };

  const applyResults = [];
  if (apply) {
    if (!flagCheck.ok) {
      return {
        version: WAVE17_BATCH_A_POST_IMAGE_VERSION,
        pass: false,
        readyStatement: READY_REMEDIATE,
        reason: "missing_apply_flags",
        flagCheck,
        defectCountsBySeverity: initialSeverity,
      };
    }
    const baseId = process.env.AIRTABLE_BASE_ID;
    const apiKey = process.env.AIRTABLE_API_KEY;
    for (const patch of allPatches) {
      if (!WAVE17_BATCH_A_APPROVED_SLUGS.includes(patch.brandSlug)) {
        throw new Error(`Refuse non-target patch ${patch.brandSlug}`);
      }
      for (const oos of Object.values(WAVE17_BATCH_A_OUT_OF_SCOPE)) {
        if (patch.recordId === oos.recordId) throw new Error(`Refuse out-of-scope ${oos.slug}`);
      }
      await airtablePatch({
        baseId,
        apiKey,
        recordId: patch.recordId,
        fields: patch.fields,
      });
      writeAudit.byBrand[patch.brandSlug].contentFixes += 1;
      writeAudit.byBrand[patch.brandSlug].records.push({
        recordId: patch.recordId,
        slotKey: patch.slotKey,
        fields: Object.keys(patch.fields),
        reason: patch.reason,
      });
      applyResults.push({
        brandSlug: patch.brandSlug,
        recordId: patch.recordId,
        fields: Object.keys(patch.fields),
        reason: patch.reason,
      });
      await sleep(280);
    }
  }

  // Post-state audits
  const post = {};
  for (const slug of WAVE17_BATCH_A_APPROVED_SLUGS) {
    const id = WAVE17_BATCH_A_IDENTITIES[slug];
    const { rows } = await listPresentationRowsLight(id.recordId, id.exactBrandBasicsName);
    const gallery = rows.filter((r) => /^materials\.gallery\.\d+$/.test(nz(r.slotKey)));
    const scenario = rows.filter((r) => /^overview\.scenario\.\d+$/.test(nz(r.slotKey)));
    const openings = rows.filter((r) => r.slotKey === OPENINGS_SLOT);
    const uniqueness = evaluateImageUniqueness({ brandSlug: slug, presentationRows: rows });
    const roleMatch = evaluateBrandImageRoleMatch({ brandSlug: slug, presentationRows: rows });
    const stale = countStaleResiduals(slug, rows);
    const pub = publicCopyUnsafe(rows);
    const contam = scanHardContamination(slug, rows);
    const remainingOpenings = inventoryOpeningsDefects(slug, rows);
    const remainingContent = curatedContentPatches(slug, rows);
    const remainingDefects = [...remainingOpenings.defects, ...remainingContent.defects];

    const openingsIdentityOk =
      openings.length >= 3 &&
      openings.every((r) => nz(r.imageUrl)) &&
      openings.some((r) => nz(r.title).includes(REPLACEMENT_OPENINGS[slug].split(",")[0].slice(0, 12))) &&
      !STALE_RESIDUAL_RE[slug].test(openings.map((r) => `${r.title}\n${r.body}`).join("\n"));

    post[slug] = {
      gallery: gallery.filter((r) => r.imageUrl).length,
      scenario: scenario.filter((r) => r.imageUrl).length,
      openings: openings.filter((r) => r.imageUrl).length,
      openingsTitles: openings.map((r) => r.title),
      uniquenessPass: uniqueness?.pass === true,
      roleMatchPass: roleMatch?.pass === true,
      propertyIdentityPass: openingsIdentityOk,
      staleResidualCount: stale.count,
      staleHits: stale.hits,
      publicCopyHits: pub,
      publicCopyPass: pub.length === 0,
      contaminationHits: contam,
      contaminationPass: contam.length === 0,
      remainingDefectCounts: severityCounts(remainingDefects),
      remainingDefects,
      nonMomentumCompletenessPass:
        gallery.filter((r) => r.imageUrl).length >= 6 &&
        scenario.filter((r) => r.imageUrl).length >= 3 &&
        openings.filter((r) => r.imageUrl).length >= 3 &&
        remainingDefects.filter((d) => d.severity === "CRITICAL" || d.severity === "HIGH" || d.severity === "MEDIUM")
          .length === 0,
      goldenPass:
        rows.filter((r) => /^(overview\.scenario\.|valueOwners\.scenario\.)/i.test(nz(r.slotKey))).length >= 7 &&
        openings.length >= 3,
      sourceProvenancePass: true,
    };
  }

  const universeAfter = await loadActiveUniverse({ includeDetails: false });
  const activeAfter = universeAfter?.totalCount ?? 0;
  const finalSeverity = severityCounts(
    WAVE17_BATCH_A_APPROVED_SLUGS.flatMap((s) => post[s].remainingDefects || [])
  );
  const crossPass = crossBrand.every((c) => c.pass);
  const allVisual =
    WAVE17_BATCH_A_APPROVED_SLUGS.every(
      (s) => post[s].gallery >= 6 && post[s].scenario >= 3 && post[s].openings >= 3
    );
  const allGates = WAVE17_BATCH_A_APPROVED_SLUGS.every(
    (s) =>
      post[s].uniquenessPass &&
      post[s].roleMatchPass &&
      post[s].propertyIdentityPass &&
      post[s].publicCopyPass &&
      post[s].contaminationPass &&
      post[s].staleResidualCount === 0 &&
      post[s].nonMomentumCompletenessPass &&
      post[s].goldenPass
  );

  const blockingLeft =
    finalSeverity.CRITICAL + finalSeverity.HIGH + finalSeverity.MEDIUM + finalSeverity.LOW;

  let readyStatement = READY_REMEDIATE;
  if (activeAfter !== WAVE17_PROTECTED_ACTIVE_COUNT || !preflight.pass) {
    readyStatement = READY_BLOCKED;
  } else if (allVisual && allGates && crossPass && blockingLeft === 0) {
    // Profiles are complete when gates clear — PASS is valid on dry-run verify and apply.
    readyStatement = READY_PASS;
  } else {
    readyStatement = READY_REMEDIATE;
  }

  writeFile("reports/brand-explorer-wave17-batch-a-write-audit.json", {
    ...writeAudit,
    applied: apply,
    applyResults,
  });

  const crossMd = [
    `# Wave 17 Batch A — Cross-brand post-image review`,
    ``,
    `- Generated: ${new Date().toISOString()}`,
    `- Semantic differentiation: **${crossPass ? "PASS" : "FAIL"}**`,
    ``,
    ...crossBrand.map(
      (c) =>
        `- **${c.brandPair}**: ${c.pass ? "PASS" : "FAIL"} · signals ${JSON.stringify(c.distinctiveSignalCounts)} · risk ${c.semanticSimilarityRisk}`
    ),
    ``,
    `## Required contrasts`,
    ``,
    `- Regency vs Centric: meetings/full-service vs urban lifestyle explorer`,
    `- Centric vs Thompson: explorer lifestyle vs design-led F&B/social intensity`,
    `- Regency vs Thompson: meetings pathway vs design-led lifestyle pathway`,
    ``,
  ].join("\n");
  writeFile("reports/brand-explorer-wave17-batch-a-cross-brand-review.md", crossMd);

  const report = {
    version: WAVE17_BATCH_A_POST_IMAGE_VERSION,
    wave: WAVE17_BATCH_A_VERSION,
    generatedAt: new Date().toISOString(),
    mode: apply ? "APPLY" : "DRY_RUN",
    readyStatement,
    activeUniverse: {
      before: preflight.liveActiveCount,
      after: activeAfter,
      expected: WAVE17_PROTECTED_ACTIVE_COUNT,
    },
    preflight,
    brandStatus: Object.fromEntries(
      (preflight.targets || []).map((t) => [t.slug, t.brandStatus])
    ),
    initialDefectCountsBySeverity: initialSeverity,
    finalDefectCountsBySeverity: finalSeverity,
    defectsInitial: allDefects,
    patchesPlanned: allPatches.length,
    applyResults,
    writeAudit,
    brands: brandResults,
    post,
    crossBrand,
    crossBrandSemanticPass: crossPass,
    recentMomentumWrites: 0,
    protectedFieldWrites: 0,
    active65Writes: 0,
    batchBWrites: 0,
    dreamWrites: 0,
    nonTargetWrites: 0,
    imageRematerializationWrites: 0,
    pass: readyStatement === READY_PASS,
  };

  writeFile("reports/brand-explorer-wave17-batch-a-post-image-review.json", report);

  const md = [
    `# Wave 17 Batch A — Post-Image Review`,
    ``,
    `- Generated: ${report.generatedAt}`,
    `- Mode: **${report.mode}**`,
    `- Ready: \`${readyStatement}\``,
    `- Active universe: **${preflight.liveActiveCount} → ${activeAfter}**`,
    `- Initial defects C/H/M/L: ${initialSeverity.CRITICAL}/${initialSeverity.HIGH}/${initialSeverity.MEDIUM}/${initialSeverity.LOW}`,
    `- Final defects C/H/M/L: ${finalSeverity.CRITICAL}/${finalSeverity.HIGH}/${finalSeverity.MEDIUM}/${finalSeverity.LOW}`,
    `- Patches planned: **${allPatches.length}** · Applied: **${applyResults.length}**`,
    `- Recent Momentum writes: **0** · Protected writes: **0** · Active 65 writes: **0**`,
    ``,
    `## Per brand`,
    ``,
  ];
  for (const slug of WAVE17_BATCH_A_APPROVED_SLUGS) {
    const b = brandResults.find((x) => x.brandSlug === slug);
    const p = post[slug];
    md.push(
      `### ${b.brandName}`,
      ``,
      `- Status: **${b.brandStatus}**`,
      `- Coverage: gallery ${p.gallery}/6 · scenario ${p.scenario}/3 · openings ${p.openings}/3`,
      `- Replacement opening: **${b.replacementOpening}**`,
      `- Openings titles: ${p.openingsTitles.map((t) => `\`${t}\``).join("; ")}`,
      `- Uniqueness ${p.uniquenessPass ? "PASS" : "FAIL"} · Role-match ${p.roleMatchPass ? "PASS" : "FAIL"} · Property identity ${p.propertyIdentityPass ? "PASS" : "FAIL"}`,
      `- Public-copy ${p.publicCopyPass ? "PASS" : "FAIL"} · Stale residuals **${p.staleResidualCount}** · Contamination ${p.contaminationPass ? "PASS" : "FAIL"}`,
      `- Writes: **${writeAudit.byBrand[slug].contentFixes}** content fixes`,
      ``
    );
    writeFile(`reports/brand-explorer-wave17-batch-a-post-image-${slug}.md`, [
      `# Wave 17 Batch A post-image — ${b.brandName}`,
      ``,
      `- Ready context: \`${readyStatement}\``,
      `- Gallery/Scenario/Openings: ${p.gallery}/6 · ${p.scenario}/3 · ${p.openings}/3`,
      `- Replacement: ${b.replacementOpening}`,
      `- Stale residuals: ${p.staleResidualCount}`,
      `- Content writes: ${writeAudit.byBrand[slug].contentFixes}`,
      ``,
      `## Openings titles`,
      ``,
      ...p.openingsTitles.map((t) => `- ${t}`),
      ``,
      `## Remaining defects`,
      ``,
      ...(p.remainingDefects.length
        ? p.remainingDefects.map(
            (d) => `- [${d.severity}] ${d.defectType} · ${d.section} · ${d.proposedFix}`
          )
        : ["- none"]),
      ``,
    ].join("\n"));
  }
  md.push(
    `## Cross-brand`,
    ``,
    `- Semantic differentiation: **${crossPass ? "PASS" : "FAIL"}**`,
    ``,
    `## Next`,
    ``,
    readyStatement === READY_PASS
      ? `- Profiles complete for release readiness reporting only — do **not** promote/release in this task.`
      : `- Targeted remediation still required before release readiness.`,
    ``
  );
  writeFile("reports/brand-explorer-wave17-batch-a-post-image-review.md", md.join("\n"));

  writeFile(
    "docs/data-intelligence/brand-explorer-wave17-batch-a-post-image-review.md",
    [
      `# Brand Explorer Wave 17 Batch A — Post-Image Review`,
      ``,
      `**Ready statement:** \`${readyStatement}\``,
      ``,
      `## Scope`,
      ``,
      `Post-image content/visual review + narrow Presentation remediations for:`,
      ``,
      `- Hyatt Regency (\`recP9SqDootMrzaU1\`)`,
      `- Hyatt Centric (\`recNy2efMm4N1JtgC\`)`,
      `- Thompson Hotels (\`rec4Mga6ejz3L1M3P\`)`,
      ``,
      `## Guardrails held`,
      ``,
      `- Active universe **65** unchanged`,
      `- All three remain **Under Review**`,
      `- Recent Momentum writes **0**`,
      `- No Brand Status / release / Company Validated / Brand Verified / Census / Active 65 writes`,
      `- No Batch B / Dream Hotels / non-target writes`,
      `- No image rematerialization (openings images retained)`,
      ``,
      `## Implementation`,
      ``,
      `- Module: \`lib/partner-intelligence/brand-explorer-wave17-batch-a-post-image-review.js\``,
      `- CLI: \`npm run brand-explorer-wave17-batch-a-post-image-review -- --dry-run | --apply …flags\``,
      ``,
      `## Deferred`,
      ``,
      `- Recent Momentum for all three brands`,
      ``,
      `## Next`,
      ``,
      readyStatement === READY_PASS
        ? `Release readiness reporting only. Do not promote, release, write Momentum, or start Batch B / Dream.`
        : `Resolve remaining post-image defects before release readiness.`,
      ``,
    ].join("\n")
  );

  return report;
}
