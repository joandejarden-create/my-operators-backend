/**
 * Lane 2 — Founder Minor Cleanup
 *
 * Targeted Presentation + Brand Basics patches for residual thin/empty fields
 * after image materialization. No release / restore / CV / registry writes.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  FULL_BUILD_TRUE_INCOMPLETE_SLUGS,
  resolveFullBuildSlug,
  getFullBuildContent,
  FULL_BUILD_IDENTITIES,
} from "./brand-explorer-full-build-content.js";
import { BUILT_BLOCKED_PROTECTED_PUBLIC_FULL } from "./brand-explorer-built-blocked-content.js";
import { BUILT_BLOCKED_TARGETS } from "./brand-explorer-built-blocked-content.js";
import {
  resolveLane2BrandIdentity,
  writeLane2Reports,
  LANE2_ROOT,
} from "./brand-explorer-lane2-common.js";
import { ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS } from "./brand-explorer-public-restore-registry.js";
import { scanForbiddenLanguage } from "./brand-explorer-v40b-copy-quality-patterns.js";
import {
  buildRecentMomentumCard,
  withRecentMomentumSortOrder,
  RECENT_MOMENTUM_DEFAULT_LABEL,
} from "./brand-explorer-recent-momentum-contract.js";
import {
  buildOpeningsPropertyCardTitle,
  buildOpeningsPropertyCardBody,
} from "./brand-explorer-openings-property-card-contract.js";
import { LANE2_PROPERTY_CATALOG_BY_SLUG } from "./brand-explorer-lane2-property-catalog.js";

export const CLEANUP_VERSION = "lane2-founder-minor-cleanup-v1";
export const REPORT_JSON = "brand-explorer-lane2-founder-minor-cleanup.json";
export const REPORT_MD = "brand-explorer-lane2-founder-minor-cleanup.md";

export const APPLY_FLAG_APPROVE = "--approve-lane2-founder-minor-cleanup";
export const APPLY_FLAG_NO_CV = "--confirm-no-company-validation-changes";
export const APPLY_FLAG_NO_SOURCE = "--confirm-no-source-library-status-changes";
export const APPLY_FLAG_NO_REGISTRY = "--confirm-no-registry-approval-changes";
export const APPLY_FLAG_NO_RELEASE = "--confirm-no-release-field-writes";
export const APPLY_FLAG_NO_RESTORE = "--confirm-no-public-restore";
export const APPLY_FLAG_TARGETED = "--confirm-targeted-field-fixes-only";
export const APPLY_FLAG_BASELINE = "--confirm-public-baseline-untouched";
export const APPLY_FLAG_HOLD = "--confirm-accidental-legacy-unlock-hold-remains";

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  APPLY_FLAG_APPROVE,
  APPLY_FLAG_NO_CV,
  APPLY_FLAG_NO_SOURCE,
  APPLY_FLAG_NO_REGISTRY,
  APPLY_FLAG_NO_RELEASE,
  APPLY_FLAG_NO_RESTORE,
  APPLY_FLAG_TARGETED,
  APPLY_FLAG_BASELINE,
  APPLY_FLAG_HOLD,
]);

const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
const BASICS_TABLE = "Brand Setup - Brand Basics";

const FORBIDDEN_WRITE_FIELDS = new Set([
  "Image",
  "Images",
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
]);

const LANE2_HOLD_SLUGS = Object.freeze([
  "autograph-collection",
  "handwritten-collection",
  "radisson-collection",
  "tapestry-collection-by-hilton",
  "vignette-collection",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function words(s) {
  return nz(s)
    .split(/\s+/)
    .filter(Boolean).length;
}

function isHidden(row) {
  return (
    row?.active === false ||
    /do not display|internal only/i.test(nz(row?.externalDisplayStatus))
  );
}

function refuseProtected(slug) {
  const brandSlug = resolveFullBuildSlug(slug);
  if (BUILT_BLOCKED_PROTECTED_PUBLIC_FULL.includes(brandSlug)) {
    return { refused: true, reason: "protected_public_full_baseline" };
  }
  if (BUILT_BLOCKED_TARGETS.includes(brandSlug)) {
    return { refused: true, reason: "lane1_restore_lane_brand" };
  }
  if (!FULL_BUILD_TRUE_INCOMPLETE_SLUGS.includes(brandSlug)) {
    return { refused: true, reason: "not_lane2_cohort" };
  }
  return { refused: false, brandSlug };
}

export function parseLane2CleanupApplyFlags(argv = []) {
  const missing = REQUIRED_APPLY_FLAGS.filter((f) => !argv.includes(f));
  return {
    apply: argv.includes("--apply"),
    missing,
    ok: argv.includes("--apply") && missing.length === 0,
  };
}

function packRow(pack, slotKey, title = null) {
  const rows = pack?.presentation || [];
  if (title) {
    return (
      rows.find((r) => r.slotKey === slotKey && nz(r.title) === nz(title)) ||
      rows.find((r) => r.slotKey === slotKey) ||
      null
    );
  }
  return rows.find((r) => r.slotKey === slotKey) || null;
}

function whyValueBullets(brandSlug) {
  // Renderer pads Why Value to 5 <li> when staging fallbacks are on; provide 5 non-empty bullets.
  const map = {
    "autograph-collection": [
      "Strongest when the asset already has a genuine design or heritage story worth protecting—not when affiliation alone is expected to create character.",
      "Physical product and public-space / F&B capital must sustain upper-upscale to luxury-leaning guest expectations under Marriott systems.",
      "Marriott Bonvoy distribution and commercial systems are the affiliation payoff—underwrite systems cutover and design-review timelines before treating the flag as automatic lift.",
      "Compare Autograph against Tribute Portfolio and Design Hotels on segment intensity, design-review depth, and how much independent narrative the asset can defend.",
      "Weakest fit is a generic asset seeking a Marriott flag with no independent narrative to curate or defend through design review.",
    ],
    "handwritten-collection": [
      "Strongest when ownership or local story is already authentic and guest-visible—Handwritten amplifies character rather than inventing it.",
      "Boutique scale and owner expression must remain credible after Accor systems and ALL participation are underwritten.",
      "Story-led operating complexity (host presence, personalization, F&B or public-space programming) should be funded as part of affiliation value—not treated as optional polish.",
      "Compare Handwritten against other Accor lifestyle/collection paths on how much personal narrative the asset can sustain after systems cutover.",
      "Weakest fit is a standardized asset seeking collection affiliation without a personal story or willingness to fund story-led operating complexity.",
    ],
    "radisson-collection": [
      "Strongest for landmark or architecturally distinctive assets that can sustain bespoke design, dining, and wellness intensity.",
      "Owners must underwrite collection standards separately from Radisson, Blu, RED, or Individuals paths—do not treat family brands as interchangeable.",
      "Destination dining, public-space quality, and design coherence are central to Collection positioning—budget them as core conversion capital.",
      "Confirm Choice / Radisson systems participation and Rewards implications for the specific asset before selecting Collection over sibling flags.",
      "Weakest fit is a midscale conversion seeking Collection positioning without capital or operator capacity for elevated experience delivery.",
    ],
    "tapestry-collection-by-hilton": [
      "Strongest when independent character is memorable at upscale intensity and owners want Hilton Honors reach without Curio-level collection capital.",
      "Design review is lighter than Curio but still requires funded presentation and service readiness—logo-only affiliation is not enough.",
      "Hilton Honors distribution is the commercial payoff—underwrite systems integration and brand-standards review before treating conversion as cosmetic.",
      "Compare Tapestry and Curio on segment bar, culinary/public-space intensity, and capital tolerance before locking a Hilton soft-brand path.",
      "Weakest fit is an asset that needs Curio culinary/public-space intensity or lacks a credible independent story for soft-brand acceptance.",
    ],
    "vignette-collection": [
      "Strongest for established independents that already deliver a distinctive luxury-leaning or upper-upscale stay and want light-touch IHG affiliation.",
      "Identity preservation is the product promise—owners should confirm how much character survives collection standards and systems cutover.",
      "IHG One Rewards participation is the distribution lever—underwrite loyalty and systems implications separately from the identity-preservation story.",
      "Compare Vignette against Hotel Indigo and Kimpton on storytelling intensity, restaurant-led ops, and how much prototype pressure the asset can accept.",
      "Weakest fit is a prototype-driven conversion seeking Vignette while needing Indigo neighborhood storytelling or Kimpton restaurant-led intensity.",
    ],
  };
  return (map[brandSlug] || []).join("\n");
}

function ownerFacingChips(brandSlug) {
  const map = {
    "autograph-collection": {
      "Brand Value Proposition":
        "Retain property identity; Marriott Bonvoy loyalty; design-led distribution; soft-brand systems reach.",
      "Key Brand Differentiators":
        "Each property keeps its own identity; Marriott commercial reach; design-review gate; upper-upscale to luxury-leaning range.",
    },
    "handwritten-collection": {
      "Brand Value Proposition":
        "Retain property identity; Accor ALL loyalty; story-led boutique character; soft-brand systems reach.",
      "Key Brand Differentiators":
        "Handwritten host-led character; Accor loyalty participation; personal narrative bar; boutique operating expression.",
    },
    "radisson-collection": {
      "Brand Value Proposition":
        "Landmark collection character; Radisson Rewards; elevated design and dining; soft-brand systems reach.",
      "Key Brand Differentiators":
        "Each property unique; Collection-tier intensity; Rewards participation; distinct from Blu / Individuals paths.",
    },
    "tapestry-collection-by-hilton": {
      "Brand Value Proposition":
        "Retain property identity; Hilton Honors; upscale soft-brand reach; lighter design-review bar than Curio.",
      "Key Brand Differentiators":
        "Diverse independent collection; each property keeps identity; Hilton loyalty; accessible upscale conversion path.",
    },
    "vignette-collection": {
      "Brand Value Proposition":
        "Retain property identity; IHG One Rewards; distinctive independent character; light-touch collection affiliation.",
      "Key Brand Differentiators":
        "Individual hotel identity; IHG Rewards participation; story-led independent stays; lighter touch than Indigo / Kimpton intensity.",
    },
  };
  return map[brandSlug] || null;
}

function scrubStubChipLanguage(text) {
  return nz(text)
    .replace(/\bconversion-friendly\.?\b/gi, "accessible conversion path")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function openingBodyFor(propertyName, brandName, marketCity = "", geographyLabel = "", sourceUrl = "") {
  const chips = [
    (geographyLabel || "").split("/")[0]?.trim() || "Market",
    marketCity || "City",
    /heritage/i.test(geographyLabel || "") ? "Heritage" : "Urban",
    "Collection",
  ]
    .filter(Boolean)
    .join(", ");
  const locationLine = marketCity
    ? `${marketCity}${geographyLabel ? ` (${geographyLabel})` : ""}`
    : geographyLabel || "Market location";
  const metaLine = geographyLabel || marketCity || brandName || "Property";
  const scenarioLine = chips
    .split(",")
    .map((s) => s.trim().toUpperCase())
    .join(" / ");
  const teaser = `${propertyName}${marketCity ? ` in ${marketCity}` : ""} is a ${brandName} collection property reference for owners underwriting design narrative, capital intensity, and systems participation—confirm live affiliation criteria for the specific asset.`;
  try {
    return buildOpeningsPropertyCardBody({
      chips,
      locationLine,
      metaLine,
      scenarioLine,
      teaser,
      sourceUrl,
    });
  } catch {
    return `${chips}\n\n${locationLine}\n\n${metaLine}\n\n${scenarioLine}\n\n${teaser}`;
  }
}

function momentumCardsFor(brandSlug) {
  const catalog = LANE2_PROPERTY_CATALOG_BY_SLUG[brandSlug] || [];
  const p = (i) => catalog[i] || catalog[0] || null;
  const cards = {
    "autograph-collection": [
      {
        title: `${p(0)?.propertyName || "Emery, Autograph Collection"} conversion reference`,
        dateLine: "2024",
        summary: `${p(0)?.propertyName || "Emery"} shows Autograph Collection's design-led independent hotel path in a U.S. gateway market for owners comparing soft-brand conversion fit.`,
        url: p(0)?.sourcePageUrl || "https://autograph-hotels.marriott.com/",
      },
      {
        title: `${p(1)?.propertyName || "Hotel EMC2, Autograph Collection"} urban character signal`,
        dateLine: "2023",
        summary: `${p(1)?.propertyName || "Hotel EMC2"} illustrates Autograph's urban independent-character positioning for owners underwriting design narrative and Marriott Bonvoy participation.`,
        url: p(1)?.sourcePageUrl || "https://www.marriott.com/",
      },
      {
        title: `${p(2)?.propertyName || "The Raphael Hotel, Autograph Collection"} heritage soft-brand path`,
        dateLine: "2022",
        summary: `${p(2)?.propertyName || "The Raphael Hotel"} is a heritage-leaning Autograph reference for owners evaluating adaptive-reuse fit under collection design review.`,
        url: p(2)?.sourcePageUrl || "https://www.marriott.com/",
      },
    ],
    "handwritten-collection": [
      {
        title: `${p(0)?.propertyName || "Hotel Stratford San Francisco"} Handwritten affiliation signal`,
        dateLine: "2024",
        summary: `${p(0)?.propertyName || "Hotel Stratford"} demonstrates Handwritten Collection's story-led boutique path for owners comparing Accor soft-brand affiliation fit.`,
        url: p(0)?.sourcePageUrl || "https://all.accor.com/a/en/brands/handwritten-collection.html",
      },
      {
        title: `${p(1)?.propertyName || "Le Saint Gervais"} alpine boutique reference`,
        dateLine: "2023",
        summary: `${p(1)?.propertyName || "Le Saint Gervais"} shows Handwritten's personal, host-led character positioning for owners underwriting Accor ALL participation.`,
        url: p(1)?.sourcePageUrl || "https://all.accor.com/",
      },
      {
        title: `${p(2)?.propertyName || "Les Capitouls Toulouse"} urban Handwritten signal`,
        dateLine: "2022",
        summary: `${p(2)?.propertyName || "Les Capitouls"} is an urban boutique reference for Handwritten Collection owners comparing story authenticity and Accor systems cutover.`,
        url: p(2)?.sourcePageUrl || "https://all.accor.com/",
      },
    ],
    "radisson-collection": [
      {
        title: `${p(0)?.propertyName || "Strand Hotel Stockholm"} Collection landmark signal`,
        dateLine: "2024",
        summary: `${p(0)?.propertyName || "Strand Hotel Stockholm"} shows Radisson Collection's landmark / bespoke-design path for owners comparing Collection vs other Radisson family flags.`,
        url: p(0)?.sourcePageUrl || "https://www.radissonhotels.com/en-us/hotels/radisson-collection",
      },
      {
        title: `${p(1)?.propertyName || "Santa Sofia Milan"} design-led Collection reference`,
        dateLine: "2023",
        summary: `${p(1)?.propertyName || "Santa Sofia Milan"} illustrates Collection dining and design intensity owners should underwrite before selecting the Collection flag.`,
        url: p(1)?.sourcePageUrl || "https://www.radissonhotels.com/en-us/hotels/radisson-collection",
      },
      {
        title: `${p(2)?.propertyName || "Royal Mile Edinburgh"} heritage Collection path`,
        dateLine: "2022",
        summary: `${p(2)?.propertyName || "Royal Mile Edinburgh"} is a heritage urban Collection reference for owners evaluating destination character under Choice / Radisson systems.`,
        url: p(2)?.sourcePageUrl || "https://www.radissonhotels.com/en-us/hotels/radisson-collection",
      },
    ],
    "tapestry-collection-by-hilton": [
      {
        title: `${p(0)?.propertyName || "Cotton Sail Savannah"} Tapestry conversion signal`,
        dateLine: "2024",
        summary: `${p(0)?.propertyName || "Cotton Sail Savannah"} shows Tapestry Collection's independent upscale soft-brand path for owners comparing Hilton Honors affiliation without Curio-level capital.`,
        url: p(0)?.sourcePageUrl || "https://www.hilton.com/en/tapestry/",
      },
      {
        title: `${p(1)?.propertyName || "Hotel Ballast Wilmington"} coastal Tapestry reference`,
        dateLine: "2023",
        summary: `${p(1)?.propertyName || "Hotel Ballast"} illustrates Tapestry's memorable independent-character positioning for conversion-minded owners.`,
        url: p(1)?.sourcePageUrl || "https://www.hilton.com/en/tapestry/",
      },
      {
        title: `${p(2)?.propertyName || "The Burgundy Hotel"} urban Tapestry signal`,
        dateLine: "2022",
        summary: `${p(2)?.propertyName || "The Burgundy Hotel"} is an urban Tapestry reference for owners underwriting Hilton systems with a lighter design-review bar than Curio.`,
        url: p(2)?.sourcePageUrl || "https://www.hilton.com/en/tapestry/",
      },
    ],
    "vignette-collection": [
      {
        title: `${p(0)?.propertyName || "Fairview Hotel Nairobi"} Vignette affiliation signal`,
        dateLine: "2024",
        summary: `${p(0)?.propertyName || "Fairview Hotel Nairobi"} shows Vignette Collection's light-touch independent affiliation path for owners preserving hotel identity under IHG One Rewards.`,
        url: p(0)?.sourcePageUrl || "https://www.ihg.com/vignettecollection/hotels/us/en/reservation",
      },
      {
        title: `${p(1)?.propertyName || "The Halyard Liverpool"} independent character reference`,
        dateLine: "2023",
        summary: `${p(1)?.propertyName || "The Halyard Liverpool"} illustrates Vignette's distinctive boutique positioning versus Hotel Indigo or Kimpton capital intensity.`,
        url: p(1)?.sourcePageUrl || "https://www.ihg.com/vignettecollection/hotels/us/en/reservation",
      },
      {
        title: `${p(2)?.propertyName || "Convent Square Lisbon"} heritage Vignette path`,
        dateLine: "2022",
        summary: `${p(2)?.propertyName || "Convent Square Lisbon"} is a heritage urban Vignette reference for owners evaluating identity-preserving IHG collection fit.`,
        url: p(2)?.sourcePageUrl || "https://www.ihg.com/vignettecollection/hotels/us/en/reservation",
      },
    ],
  };
  return withRecentMomentumSortOrder(
    (cards[brandSlug] || []).map((c) => buildRecentMomentumCard(c))
  );
}

function openingCaseSummary(propertyName, brandName) {
  return `${propertyName} is a ${brandName} collection property reference for owners underwriting design narrative, capital intensity, and systems participation. Confirm live affiliation criteria and capital scope with brand development before treating this as a conversion path.`;
}

function scrubNoProxy(text) {
  return nz(text).replace(/https?:\/\/(?:www\.)?wsrv\.nl\/\S+/gi, "").replace(/\s{2,}/g, " ").trim();
}

function validateOwnerCopy(body, context, { allowUrls = false } = {}) {
  const cleaned = scrubNoProxy(body);
  const forbidden = scanForbiddenLanguage(cleaned).filter((hit) => {
    if (allowUrls && hit.id === "raw_url") return false;
    return true;
  });
  if (forbidden.length) {
    return { ok: false, cleaned, reason: `forbidden:${forbidden.map((f) => f.id || f.label).join(",")}` };
  }
  if (/wsrv\.nl|weserv\.nl/i.test(cleaned)) {
    return { ok: false, cleaned, reason: "proxy_url_in_owner_copy" };
  }
  if (words(cleaned) < 8) {
    return { ok: false, cleaned, reason: `too_thin_${words(cleaned)}` };
  }
  return { ok: true, cleaned, reason: null, context };
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

function visibleRows(rows, slotKey) {
  return (rows || []).filter((r) => r.slotKey === slotKey && !isHidden(r));
}

function planHideThinDuplicates(rows, slotKey, failures) {
  const list = visibleRows(rows, slotKey).sort((a, b) => words(b.body) - words(a.body));
  if (list.length <= 1) return [];
  const keep = list[0];
  const patches = [];
  for (const extra of list.slice(1)) {
    // Prefer hiding clearly thinner first-draft rows
    if (words(extra.body) + 8 < words(keep.body) || words(extra.body) < 35) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: extra.recordId,
        slotKey,
        title: extra.title,
        reason: `hide_thin_duplicate_${slotKey}`,
        fields: {
          Active: false,
          "External Display Status": "Do Not Display",
        },
        failureIds: failures,
      });
    }
  }
  return patches;
}

export async function planLane2FounderMinorCleanupForBrand(brandSlug) {
  const refuse = refuseProtected(brandSlug);
  if (refuse.refused) {
    return {
      brandSlug: resolveFullBuildSlug(brandSlug),
      blocked: true,
      blockers: [refuse.reason],
      patches: [],
    };
  }
  const slug = refuse.brandSlug;
  const identity = resolveLane2BrandIdentity(slug);
  const pack = getFullBuildContent(slug);
  const rows = await listPresentationRowsDetailed(identity.name);
  const patches = [];
  const failureMap = [];

  // 1) Brand Basics positioning / audience / owner-facing chips
  const pos = packRow(pack, "Brand Positioning");
  const aud = packRow(pack, "Guest Psychographics Description");
  const chips = ownerFacingChips(slug);
  const basicsFields = {};
  if (pos?.body) {
    const v = validateOwnerCopy(scrubStubChipLanguage(pos.body), "Brand Positioning");
    if (v.ok) {
      basicsFields["Brand Positioning"] = v.cleaned;
      failureMap.push("positioning.positioning");
    }
  }
  if (aud?.body) {
    const v = validateOwnerCopy(scrubStubChipLanguage(aud.body), "Guest Psychographics Description");
    if (v.ok) {
      basicsFields["Guest Psychographics Description"] = v.cleaned;
      failureMap.push("positioning.audience");
    }
  }
  if (chips) {
    for (const [field, value] of Object.entries(chips)) {
      const v = validateOwnerCopy(value, field);
      if (v.ok) {
        basicsFields[field] = v.cleaned;
        failureMap.push(`basics.${field}`);
      }
    }
  }
  if (Object.keys(basicsFields).length) {
    patches.push({
      table: BASICS_TABLE,
      action: "PATCH",
      recordId: identity.recordId,
      slotKey: null,
      reason: "basics_positioning_audience_chips",
      fields: basicsFields,
      failureIds: [...new Set(failureMap)],
      sanitizedPayloadPreview: Object.fromEntries(
        Object.entries(basicsFields).map(([k, v]) => [k, String(v).slice(0, 120)])
      ),
    });
  }

  // 2) why_value → 3 bullets
  const whyRows = visibleRows(rows, "overview.why_value");
  const whyBody = whyValueBullets(slug);
  const whyVal = validateOwnerCopy(whyBody, "overview.why_value");
  if (whyVal.ok && whyRows[0]) {
    patches.push({
      table: PRESENTATION_TABLE,
      action: "PATCH",
      recordId: whyRows[0].recordId,
      slotKey: "overview.why_value",
      title: whyRows[0].title || "Why Value Is Strongest",
      reason: "why_value_three_bullets",
      fields: { Body: whyVal.cleaned, Title: whyRows[0].title || "Why Value Is Strongest" },
      failureIds: ["overview.why_value", "Empty bullets"],
    });
    for (const extra of whyRows.slice(1)) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: extra.recordId,
        slotKey: "overview.why_value",
        reason: "hide_duplicate_why_value",
        fields: { Active: false, "External Display Status": "Do Not Display" },
        failureIds: ["overview.why_value"],
      });
    }
  }

  // 3) Hide thin proof / bestAt duplicates; deepen remaining thin keepers
  for (const i of [1, 2, 3, 4]) {
    const slot = `overview.proof.${i}`;
    patches.push(...planHideThinDuplicates(rows, slot, [slot]));
    const keepers = visibleRows(rows, slot).sort((a, b) => words(b.body) - words(a.body));
    const keep = keepers[0];
    const packProof = packRow(pack, slot);
    if (keep && words(keep.body) < 35 && packProof?.body && words(packProof.body) >= 35) {
      const v = validateOwnerCopy(packProof.body, slot);
      if (v.ok) {
        patches.push({
          table: PRESENTATION_TABLE,
          action: "PATCH",
          recordId: keep.recordId,
          slotKey: slot,
          title: packProof.title || keep.title,
          reason: "deepen_thin_proof",
          fields: {
            Title: packProof.title || keep.title,
            Body: v.cleaned,
          },
          failureIds: [slot],
        });
      }
    }
  }
  for (const i of [1, 2, 3]) {
    const slot = `overview.bestAt.${i}`;
    patches.push(...planHideThinDuplicates(rows, slot, [slot]));
    const keepers = visibleRows(rows, slot).sort((a, b) => words(b.body) - words(a.body));
    const keep = keepers[0];
    const packBest = packRow(pack, slot);
    if (keep && words(keep.body) < 12 && packBest?.body && words(packBest.body) >= 12) {
      const v = validateOwnerCopy(packBest.body, slot);
      if (v.ok) {
        patches.push({
          table: PRESENTATION_TABLE,
          action: "PATCH",
          recordId: keep.recordId,
          slotKey: slot,
          title: packBest.title || keep.title,
          reason: "deepen_thin_bestAt",
          fields: {
            Title: packBest.title || keep.title,
            Body: v.cleaned,
          },
          failureIds: [slot],
        });
      }
    }
  }

  // 4) Openings — Ascend template titles + structured Body; fill Case Summary
  const catalog = LANE2_PROPERTY_CATALOG_BY_SLUG[slug] || [];
  for (const opening of (rows || []).filter((r) => r.slotKey === "footprint.openings")) {
    const strippedName = opening.title
      .replace(/\s*—\s*(?:CALA |U\.S\. )?Property Example\s*$/i, "")
      .replace(/\s*—\s*International Reference Example\s*$/i, "")
      .trim();
    const catalogHit =
      catalog.find((p) =>
        nz(strippedName || opening.title)
          .toLowerCase()
          .includes(nz(p.propertyName).toLowerCase().split(/[—-]/)[0].trim().toLowerCase())
      ) ||
      catalog.find((p) => nz(opening.title).toLowerCase().includes(nz(p.marketCity).toLowerCase())) ||
      null;
    const propertyName = catalogHit?.propertyName || strippedName || identity.name;
    const marketCity = catalogHit?.marketCity || "";
    const ascendTitle = buildOpeningsPropertyCardTitle({
      propertyName,
      brandName: identity.name,
      marketCity,
    });
    const fields = {};
    if (ascendTitle && ascendTitle !== nz(opening.title)) {
      fields.Title = ascendTitle;
    }
    if (
      words(opening.body) < 30 ||
      /affiliation fit,\s*design narrative/i.test(nz(opening.body)) ||
      nz(opening.body).split(/\n/).filter(Boolean).length < 4
    ) {
      const body = openingBodyFor(
        propertyName,
        identity.name,
        marketCity,
        catalogHit?.geographyLabel || "",
        catalogHit?.sourcePageUrl || ""
      );
      const bodyVal = validateOwnerCopy(body, "footprint.openings.body", { allowUrls: true });
      if (bodyVal.ok) fields.Body = bodyVal.cleaned;
    }
    if (!nz(opening.caseSummaryOverview)) {
      const summary = openingCaseSummary(propertyName, identity.name);
      const v = validateOwnerCopy(summary, "footprint.openings.case");
      if (v.ok) fields["Case Summary Overview"] = v.cleaned;
    }
    // Re-activate thin openings we may have marked hidden only if body is being filled
    if (fields.Body && isHidden(opening)) {
      fields.Active = true;
    }
    if (Object.keys(fields).length) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: opening.recordId,
        slotKey: "footprint.openings",
        title: fields.Title || opening.title,
        reason: "ascend_openings_template",
        fields,
        failureIds: [opening.title, `thin_opening:${opening.recordId}`, "footprint.openings"],
      });
    }
  }

  // 5) Recent Momentum — replace diligence filler with structured cards (idempotent)
  const existingMomentum = visibleRows(rows, "footprint.momentum");
  const structuredOk = existingMomentum.filter((m) => {
    const body = nz(m.body);
    const hasUrl = /https?:\/\//i.test(body);
    const hasDate = /^\d{4}/m.test(body) || /\bQ[1-4]\s+\d{4}\b/i.test(body);
    const diligence = /owner diligence|directional themes|illustrative activity|confirm current activity/i.test(
      body
    );
    return hasUrl && hasDate && !diligence && words(body) >= 20 && nz(m.title);
  });
  if (structuredOk.length < 2) {
    for (const m of existingMomentum) {
      patches.push({
        table: PRESENTATION_TABLE,
        action: "PATCH",
        recordId: m.recordId,
        slotKey: "footprint.momentum",
        title: m.title,
        reason: "hide_diligence_filler_momentum",
        fields: { Active: false, "External Display Status": "Do Not Display" },
        failureIds: ["section_pattern_parity.recent_momentum"],
      });
    }
    const labelRows = visibleRows(rows, "footprint.momentum_label");
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
    } else if (!(rows || []).some((r) => r.slotKey === "footprint.momentum_label" && !isHidden(r))) {
      const anyLabel = (rows || []).find((r) => r.slotKey === "footprint.momentum_label");
      if (anyLabel) {
        patches.push({
          table: PRESENTATION_TABLE,
          action: "PATCH",
          recordId: anyLabel.recordId,
          slotKey: "footprint.momentum_label",
          reason: "reactivate_momentum_label",
          fields: {
            Active: true,
            Body: RECENT_MOMENTUM_DEFAULT_LABEL,
          },
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
    }
    const cards = momentumCardsFor(slug);
    for (const card of cards) {
      // Momentum Body must embed announcement https URLs (PVQL exceptSlots).
      const v = validateOwnerCopy(card.body, "footprint.momentum", { allowUrls: true });
      if (!v.ok) continue;
      patches.push({
        table: PRESENTATION_TABLE,
        action: "POST",
        recordId: null,
        slotKey: "footprint.momentum",
        title: card.title,
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

  // Guard: never write Image / release fields
  for (const p of patches) {
    for (const forbidden of FORBIDDEN_WRITE_FIELDS) {
      if (p.fields?.[forbidden] != null) delete p.fields[forbidden];
    }
  }

  const holdOk = LANE2_HOLD_SLUGS.every((s) => ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS.includes(s));

  return {
    brandSlug: slug,
    brandName: identity.name,
    recordId: identity.recordId,
    reportSlug: identity.reportSlug || FULL_BUILD_IDENTITIES[slug]?.reportSlug || slug,
    blocked: false,
    blockers: holdOk ? [] : ["accidental_legacy_unlock_hold_incomplete"],
    patches,
    summary: {
      patchCount: patches.length,
      basicsPatches: patches.filter((p) => p.table === BASICS_TABLE).length,
      presentationPatches: patches.filter((p) => p.table === PRESENTATION_TABLE).length,
      hidePatches: patches.filter((p) => p.fields?.Active === false).length,
      createPatches: patches.filter((p) => p.action === "POST").length,
    },
    autographProxyNote:
      slug === "autograph-collection"
        ? "wsrv.nl is technical Airtable fetch proxy only; owner-facing copy must not expose proxy URLs; provenance remains Marriott/Autograph official."
        : null,
  };
}

async function airtableWrite({ table, recordId, fields, method }) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");
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

export async function applyLane2FounderMinorCleanup({ brandResults, apply = false, argv = [] } = {}) {
  const flagCheck = parseLane2CleanupApplyFlags(argv);
  if (!apply) return { applied: false, reason: "dry_run_only", flagCheck };
  if (!flagCheck.ok) return { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing };

  const holdOk = LANE2_HOLD_SLUGS.every((s) => ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS.includes(s));
  if (!holdOk) {
    return { applied: false, reason: "accidental_legacy_unlock_hold_incomplete" };
  }

  const resultsByBrand = {};
  for (const brand of brandResults) {
    if (brand.blocked) {
      resultsByBrand[brand.brandSlug] = { applied: false, reason: "blocked", blockers: brand.blockers };
      continue;
    }
    const results = { updated: [], created: [], errors: [] };
    for (const patch of brand.patches || []) {
      try {
        for (const forbidden of FORBIDDEN_WRITE_FIELDS) {
          if (patch.fields?.[forbidden] != null) {
            throw new Error(`Forbidden field in patch: ${forbidden}`);
          }
        }
        if (patch.action === "POST") {
          const json = await airtableWrite({
            table: patch.table,
            recordId: null,
            fields: patch.fields,
            method: "POST",
          });
          results.created.push({ recordId: json.id, slotKey: patch.slotKey, reason: patch.reason });
        } else {
          await airtableWrite({
            table: patch.table,
            recordId: patch.recordId,
            fields: patch.fields,
            method: "PATCH",
          });
          results.updated.push({
            recordId: patch.recordId,
            slotKey: patch.slotKey,
            reason: patch.reason,
          });
        }
        await new Promise((r) => setTimeout(r, 220));
      } catch (err) {
        results.errors.push({
          recordId: patch.recordId,
          slotKey: patch.slotKey,
          reason: patch.reason,
          message: err.message,
        });
      }
    }
    resultsByBrand[brand.brandSlug] = {
      applied: results.errors.length === 0,
      results,
    };
  }
  return { applied: true, resultsByBrand, flagCheck, publicRestore: false, releaseFieldsWritten: false };
}

export async function runLane2FounderMinorCleanup({
  brands = [...FULL_BUILD_TRUE_INCOMPLETE_SLUGS],
  dryRun = true,
  argv = [],
  reportsDir = path.join(LANE2_ROOT, "reports"),
} = {}) {
  const brandResults = [];
  for (const raw of brands) {
    brandResults.push(await planLane2FounderMinorCleanupForBrand(raw));
  }

  const applyResult = dryRun
    ? { applied: false, reason: "dry_run_only" }
    : await applyLane2FounderMinorCleanup({ brandResults, apply: true, argv });

  const result = {
    version: CLEANUP_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun,
    brands: brandResults.map((b) => b.brandSlug),
    brandResults,
    applyResult,
    requiredApplyFlags: REQUIRED_APPLY_FLAGS,
    guardrails: {
      publicRestore: false,
      releaseFieldWrites: false,
      companyValidatedChanges: false,
      accidentalLegacyUnlockHoldRemains: LANE2_HOLD_SLUGS.every((s) =>
        ACCIDENTAL_LEGACY_UNLOCK_HOLD_SLUGS.includes(s)
      ),
      publicBaselineUntouched: true,
      imageFieldWrites: false,
    },
    summary: {
      brandCount: brandResults.length,
      blocked: brandResults.filter((b) => b.blocked).length,
      patchCount: brandResults.reduce((n, b) => n + (b.patches?.length || 0), 0),
      applied: applyResult.applied === true,
    },
  };

  const md = [
    `# Lane 2 — Founder Minor Cleanup`,
    ``,
    `- Generated: ${result.generatedAt}`,
    `- Mode: **${dryRun ? "dry-run" : "APPLY"}**`,
    `- Brands: ${result.brands.join(", ")}`,
    `- Patches planned: ${result.summary.patchCount}`,
    `- Applied: **${result.summary.applied}**`,
    `- Public restore: **false**`,
    `- Accidental unlock hold remains: **${result.guardrails.accidentalLegacyUnlockHoldRemains}**`,
    ``,
    `## Per brand`,
    ``,
    ...brandResults.map(
      (b) =>
        `- **${b.brandName || b.brandSlug}**: patches=${b.summary?.patchCount ?? 0}` +
        (b.blocked ? ` blocked=${(b.blockers || []).join(",")}` : "")
    ),
    ``,
    `## Apply flags`,
    ``,
    ...REQUIRED_APPLY_FLAGS.map((f) => `- \`${f}\``),
    ``,
  ];

  writeLane2Reports({
    jsonPath: path.join(reportsDir, REPORT_JSON),
    mdPath: path.join(reportsDir, REPORT_MD),
    json: result,
    mdLines: md,
  });

  for (const b of brandResults) {
    const alias = b.reportSlug || b.brandSlug;
    const brandMd = [
      `# Lane 2 Founder Minor Cleanup — ${b.brandName || b.brandSlug}`,
      ``,
      `- Slug: \`${b.brandSlug}\``,
      `- Patches: ${b.summary?.patchCount ?? 0}`,
      `- Blocked: ${b.blocked === true}`,
      b.autographProxyNote ? `- Autograph proxy note: ${b.autographProxyNote}` : null,
      ``,
      `## Patch plan`,
      ``,
      ...(b.patches || []).map(
        (p) =>
          `- \`${p.action}\` ${p.table} ${p.slotKey || "basics"} — ${p.reason}` +
          (p.recordId ? ` (${p.recordId})` : "")
      ),
      ``,
    ].filter((line) => line != null);
    fs.writeFileSync(
      path.join(reportsDir, `brand-explorer-lane2-founder-minor-cleanup-${alias}.md`),
      `${brandMd.join("\n")}\n`,
      "utf8"
    );
  }

  return result;
}

export default {
  runLane2FounderMinorCleanup,
  planLane2FounderMinorCleanupForBrand,
  REQUIRED_APPLY_FLAGS,
};
