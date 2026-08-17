/**
 * v42 — Brand Explorer Founder Visual Review Packet + Release Recommendation.
 *
 * Read-only. Surfaces tab readiness, visual assets, copy quality, brand lenses,
 * and a release recommendation for human judgment. Does not unlock or approve.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getActiveProfileBrandConfig } from "./brand-explorer-active-profile-brand-config.js";
import { getDiscoveryBrandConfig } from "./brand-explorer-brand-asset-image-governance.js";
import { loadBrandFactoryContext } from "./brand-explorer-active-profile-factory.js";
import { evaluateBrandExternalQualityLock } from "./brand-explorer-display-quality-lock.js";
import { renderBrandExplorerHtmlForTest } from "./brand-explorer-atelier-render-test-loader.js";
import {
  evaluateGalleryRule,
  evaluatePropertyExampleRule,
} from "./brand-explorer-active-profile-factory-rules.js";
import { classifyPropertyExampleImage } from "./brand-explorer-footprint-opening-image-governance.js";
import { evaluateBrandExplorerOsBrand } from "./brand-explorer-os-run.js";
import { GRADUATED_LIFESTYLE_COHORT_SLUGS } from "./brand-explorer-os-state-machine.js";
import {
  V40B_BRAND_COPY_PROFILES,
  scanForbiddenLanguage,
  scanMechanicalCopy,
  evaluateBrandCopySignals,
  detectRepeatedBoilerplate,
} from "./brand-explorer-v40b-copy-quality-patterns.js";
import { evaluateImageUniqueness } from "./brand-explorer-image-uniqueness.js";
import { evaluateBrandImageRoleMatch } from "./brand-explorer-image-role-match.js";
import { BUILT_BLOCKED_IDENTITIES } from "./brand-explorer-built-blocked-content.js";
import { evaluateTabFactoryFromPayload } from "./brand-explorer-tab-factory-evaluate.js";
import { isOwnerFacingPresentationRow } from "./brand-explorer-public-visibility-quality-lock.js";

export const V42_VERSION = "v42";

export const V42_DEFAULT_BRANDS = Object.freeze([
  "everhome-suites",
  "kimpton",
  "radisson-individuals-by-choice",
]);

export const V42_INCOMPLETE_CONTROL = Object.freeze([...GRADUATED_LIFESTYLE_COHORT_SLUGS]);

export const REPORT_JSON = "brand-explorer-v42-founder-visual-review.json";
export const REPORT_MD = "brand-explorer-v42-founder-visual-review.md";

const GALLERY_MIN = 6;
const PROPERTY_EXAMPLE_MIN = 3;

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

/** Founder-facing tab inventory (internal preview panels). */
export const V42_REVIEW_TABS = Object.freeze([
  {
    id: "atelier-overview",
    key: "overview",
    label: "Overview",
    slotPrefixes: ["overview."],
  },
  {
    id: "atelier-value-owners",
    key: "value_to_owners",
    label: "Value to owners",
    slotPrefixes: ["value.", "owner_value."],
  },
  {
    id: "atelier-ops",
    key: "operating_model",
    label: "Operating model",
    slotPrefixes: ["operations.", "ops."],
  },
  {
    id: "atelier-standards-owner",
    key: "owner_considerations",
    label: "Owner considerations",
    slotPrefixes: ["standards.", "owner."],
  },
  {
    id: "atelier-commercial",
    key: "commercial_engine",
    label: "Commercial engine",
    slotPrefixes: ["commercial."],
  },
  {
    id: "atelier-economics",
    key: "economics_obligations",
    label: "Economics & obligations",
    slotPrefixes: ["economics."],
  },
  {
    id: "atelier-loyalty",
    key: "loyalty",
    label: "Loyalty",
    slotPrefixes: ["loyalty."],
  },
  {
    id: "atelier-footprint",
    key: "footprint_growth",
    label: "Footprint & growth",
    slotPrefixes: ["footprint."],
  },
  {
    id: "atelier-materials",
    key: "brand_materials",
    label: "Brand materials",
    slotPrefixes: ["materials."],
  },
  {
    id: "atelier-insight",
    key: "dealality_insight",
    label: "Dealality insight",
    slotPrefixes: ["insight."],
  },
]);

/** Brand-specific founder judgment lenses (questions + signal regexes). */
export const V42_BRAND_LENSES = Object.freeze({
  "everhome-suites": {
    brandName: "Everhome Suites",
    questions: [
      {
        id: "extended_stay_fit",
        question: "Does it clearly communicate extended-stay fit?",
        re: /\bextended[- ]stay\b/i,
      },
      {
        id: "room_product_diligence",
        question:
          "Does it explain room product / longer-stay demand / owner diligence without generic franchise boilerplate?",
        re: /\b(kitchen|suite|longer[- ]stay|housekeeping|room product|diligence|operating model)\b/i,
      },
      {
        id: "operating_model_useful",
        question: "Does the operating model feel useful?",
        re: /\b(operating model|labor|housekeeping|prototype|PIP|extended[- ]stay)\b/i,
      },
    ],
  },
  kimpton: {
    brandName: "Kimpton Hotels",
    questions: [
      {
        id: "lifestyle_premium",
        question: "Does it feel lifestyle / experience-led / premium?",
        re: /\b(lifestyle|boutique|experience[- ]led|design[- ]led|premium|individual)\b/i,
      },
      {
        id: "avoid_fdd_dry",
        question: "Does it avoid dry legal/FDD language?",
        avoidRe: /\b(FDD|Item\s*19|franchise disclosure|fee stack|LOI)\b/i,
      },
      {
        id: "ops_complexity_value",
        question:
          "Does it communicate operating complexity and brand value without unsupported claims?",
        re: /\b(F&B|food and beverage|restaurant|bar|operating complexity|ops|brand value)\b/i,
      },
    ],
  },
  "radisson-individuals-by-choice": {
    brandName: "Radisson Individuals by Choice",
    questions: [
      {
        id: "soft_brand_collection",
        question: "Does it feel like a soft-brand / collection opportunity?",
        re: /\b(soft[- ]brand|collection|individuals|conversion)\b/i,
      },
      {
        id: "conversion_flex",
        question: "Does it communicate conversion/flexibility value?",
        re: /\b(conversion|flexib|Choice|owner)\b/i,
      },
      {
        id: "avoid_fee_loi",
        question: "Does it avoid fee-stack or LOI boilerplate?",
        avoidRe: /\b(fee stack|LOI|Item\s*19|FDD)\b/i,
      },
    ],
  },
  "hotel-indigo": {
    brandName: "Hotel Indigo",
    questions: [
      {
        id: "neighborhood_lifestyle",
        question: "Does it communicate neighborhood / lifestyle / local discovery fit?",
        re: /\b(neighborhood|lifestyle|local discovery|boutique)\b/i,
      },
      {
        id: "ihg_hotel_indigo",
        question: "Does it keep Hotel Indigo / IHG framing clear?",
        re: /\b(Hotel Indigo|IHG)\b/i,
      },
      {
        id: "owner_diligence",
        question: "Does Owner Considerations read as brand-fit / design / operating diligence?",
        re: /\b(owner planning|brand fit|design|PIP|operating|diligence)\b/i,
      },
    ],
  },
  "mgallery-collection": {
    brandName: "MGallery Collection",
    questions: [
      {
        id: "collection_framing",
        question: "Does it use Accor / MGallery collection framing?",
        re: /\b(MGallery|Accor|collection)\b/i,
      },
      {
        id: "local_character_conversion",
        question: "Does it cover local character and conversion / repositioning suitability?",
        re: /\b(local character|conversion|reposition|brand standards)\b/i,
      },
      {
        id: "avoid_fdd",
        question: "Does it avoid FDD / fee-stack boilerplate?",
        avoidRe: /\b(FDD|Item\s*19|franchise disclosure|fee stack)\b/i,
      },
    ],
  },
  "small-luxury-hotels-of-the-world": {
    brandName: "Small Luxury Hotels of the World",
    questions: [
      {
        id: "consortium_affiliation",
        question: "Does it read as independent luxury consortium / affiliation?",
        re: /\b(independent|consortium|affiliation|membership|SLH)\b/i,
      },
      {
        id: "quality_membership",
        question: "Does it cover quality expectations and membership diligence?",
        re: /\b(quality|membership|inspection|owner)\b/i,
      },
      {
        id: "avoid_fdd",
        question: "Does it avoid FDD / fee-stack boilerplate?",
        avoidRe: /\b(FDD|Item\s*19|franchise disclosure|fee stack)\b/i,
      },
    ],
  },
  "country-inn-suites": {
    brandName: "Country Inn & Suites by Choice",
    questions: [
      {
        id: "upper_midscale_select",
        question: "Does it read as upper-midscale select-service (comfort / breakfast / travel consistency)?",
        re: /\b(upper[- ]midscale|select[- ]service|breakfast|suite|comfort|travel consistency)\b/i,
      },
      {
        id: "avoid_lifestyle_collection",
        question: "Does it avoid lifestyle / collection / soft-brand framing?",
        avoidRe: /\b(lifestyle collection|soft[- ]brand|boutique collection)\b/i,
      },
      {
        id: "choice_not_radisson_family",
        question: "Does it stay Country / Choice (not Radisson-family confusion)?",
        re: /\b(Country Inn|Choice Privileges|Choice)\b/i,
      },
    ],
  },
  "quality-inn": {
    brandName: "Quality Inn",
    questions: [
      {
        id: "midscale_value_q",
        question: "Does it emphasize midscale Value Q / conversion utility (not premium)?",
        re: /\b(Value Q|midscale|conversion|breakfast|essentials)\b/i,
      },
      {
        id: "avoid_over_premium",
        question: "Does it avoid over-premium / lifestyle positioning?",
        avoidRe: /\b(upper[- ]upscale|luxury lifestyle|full[- ]service design)\b/i,
      },
      {
        id: "owner_utility",
        question: "Does owner utility (standards, distribution, conversion path) come through?",
        re: /\b(conversion|PIP|Choice Privileges|distribution|prototype|standards)\b/i,
      },
    ],
  },
  radisson: {
    brandName: "Radisson by Choice",
    questions: [
      {
        id: "core_radisson_clarity",
        question: "Is this clearly core Radisson (not Blu / RED / Collection / Individuals)?",
        re: /\b(Radisson by Choice|core Radisson|full[- ]service)\b/i,
      },
      {
        id: "sibling_distinction",
        question: "Does copy explicitly distinguish from Blu / RED / Collection / Individuals?",
        re: /\b(distinct from|not Blu|not RED|not Collection|Individuals)\b/i,
      },
      {
        id: "upper_upscale_fs",
        question: "Does upper-upscale / full-service positioning come through?",
        re: /\b(upper[- ]upscale|full[- ]service|meetings|F&B)\b/i,
      },
    ],
  },
  "radisson-blu": {
    brandName: "Radisson Blu by Choice",
    questions: [
      {
        id: "blu_design_forward",
        question: "Does it read as design-forward upper-upscale Blu (not generic Radisson)?",
        re: /\b(Radisson Blu|design[- ]forward|upper[- ]upscale|Nordic|Enticing Moments)\b/i,
      },
      {
        id: "sibling_distinction",
        question: "Does copy explicitly distinguish Blu from RED / core Radisson / Collection?",
        re: /\b(distinct from|not RED|core Radisson|Radisson Collection)\b/i,
      },
      {
        id: "full_service_international",
        question: "Does full-service / international gateway-destination fit come through?",
        re: /\b(full[- ]service|gateway|destination|design|meetings)\b/i,
      },
    ],
  },
  "radisson-red": {
    brandName: "Radisson RED by Choice",
    questions: [
      {
        id: "red_lifestyle_energy",
        question: "Does it read as lifestyle / social-energy / select-service RED?",
        re: /\b(Radisson RED|lifestyle|social|select[- ]service|OUIBar|KTCHN|flex)\b/i,
      },
      {
        id: "sibling_distinction",
        question: "Does copy explicitly distinguish RED from Blu / Collection / core Radisson?",
        re: /\b(distinct from|Radisson Blu|Collection|core Radisson)\b/i,
      },
      {
        id: "urban_upscale",
        question: "Does urban upscale / social public-space fit come through?",
        re: /\b(urban|social|lobby|upscale|lifestyle)\b/i,
      },
    ],
  },
  "suburban-studios": {
    brandName: "Suburban Studios",
    questions: [
      {
        id: "economy_extended_stay",
        question: "Does it read as economy extended-stay / weekly-stay studios?",
        re: /\b(extended[- ]stay|weekly|studio|kitchenette|economy)\b/i,
      },
      {
        id: "avoid_select_service_generic",
        question: "Does it avoid generic select-service / midscale nightly framing?",
        avoidRe: /\b(upper[- ]midscale select[- ]service|full[- ]service meetings)\b/i,
      },
      {
        id: "conversion_lean_ops",
        question: "Does conversion / lean staffing / kitchen utility come through?",
        re: /\b(conversion|kitchen|lean|weekly|employment)\b/i,
      },
    ],
  },
  "woodspring-suites": {
    brandName: "WoodSpring Suites",
    questions: [
      {
        id: "economy_extended_lean",
        question: "Does it read as economy extended-stay with lean ops / weekly demand?",
        re: /\b(extended[- ]stay|weekly|kitchen|lean|economy|WoodSpring)\b/i,
      },
      {
        id: "operating_simplicity",
        question: "Does operating simplicity / conversion-new-build implication come through?",
        re: /\b(prototype|conversion|new[- ]build|lean staffing|housekeeping)\b/i,
      },
      {
        id: "avoid_generic_hotel",
        question: "Does it avoid generic full-service hotel brand language?",
        avoidRe: /\b(full[- ]service ballroom|lifestyle boutique|upper[- ]upscale design)\b/i,
      },
    ],
  },
});

const MAP_PRESENTATION_FIELDS = Object.freeze({
  title: "Title",
  body: "Body",
  caseSummaryOverview: "Case Summary Overview",
  caseSummaryBrandRelevance: "Case Summary Brand Relevance",
  caseSummaryOwnerObjective: "Case Summary Owner Objective",
  caseSummaryInterpretation: "Case Summary Interpretation",
  caseSummaryTags: "Case Summary Tags",
});

const QUALITY_FLAGS = Object.freeze([
  { id: "generic", re: /\b(world[- ]class|best[- ]in[- ]class|leading brand|premier brand)\b/i, label: "generic" },
  {
    id: "too_legalistic",
    re: /\b(franchise disclosure|disclosure document|Item\s*7|Item\s*19|letter of intent)\b/i,
    label: "too legalistic",
  },
  {
    id: "too_vague",
    re: /\b(confirm .{10,80}during brand engagement|orientation only)\b/i,
    label: "too vague",
  },
  {
    id: "too_technical",
    re: /\b(RevPAR|GOP margin|net contribution|fee stack)\b/i,
    label: "too technical",
  },
]);

/** Fields that can meaningfully be “vague after scrub” (ignore short titles/tags). */
const VAGUE_SCAN_FIELDS = Object.freeze([
  "body",
  "caseSummaryOverview",
  "caseSummaryBrandRelevance",
  "caseSummaryOwnerObjective",
  "caseSummaryInterpretation",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function resolveConfig(slug) {
  return getActiveProfileBrandConfig(slug) || getDiscoveryBrandConfig(slug) || null;
}

/** Resolve Airtable Basics record ID for slug (config → built-blocked identities → slug). */
export function resolveFounderReviewLookupId(slug) {
  const config = resolveConfig(slug);
  if (config?.recordId) return config.recordId;
  const builtBlocked = BUILT_BLOCKED_IDENTITIES[slug];
  if (builtBlocked?.recordId) return builtBlocked.recordId;
  return slug;
}

function buildInternalPreviewUrls(recordId, slug) {
  const id = recordId || slug;
  const q = `brandId=${encodeURIComponent(id)}&beInternalPreview=1`;
  return {
    query: "?beInternalPreview=1",
    localPath: `/brand-explorer-combined.html?${q}`,
    productionUrl: `https://www.dealality.com/brand-explorer-combined?${q}`,
    apiPath: `/api/brand-library/brand?brandId=${encodeURIComponent(id)}`,
  };
}

function readJsonIfExists(p) {
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch (err) {
    console.warn(`[v42] failed to parse ${p}: ${err.message}`);
    return null;
  }
}

async function fetchBrandApiShape(slug) {
  const { getBrandLibraryBrandById } = await import("../../api/brand-library.js");
  const lookupId = resolveFounderReviewLookupId(slug);
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
  if (res.statusCode !== 200 || !res.payload?.brand) {
    throw new Error(
      `brand API fetch failed for ${slug} (lookup=${lookupId}): HTTP ${res.statusCode}`
    );
  }
  return res.payload.brand;
}

function stripHtml(html) {
  return nz(html)
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/\s(?:href|src|srcset|data-src)=["'][^"']*["']/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/\s+/g, " ")
    .trim();
}

function extractTabPanelHtml(fullHtml, tabId) {
  const re = new RegExp(
    `<section[^>]*data-atelier-panel="${tabId}"[^>]*>([\\s\\S]*?)<\\/section>`,
    "i"
  );
  const m = fullHtml.match(re);
  return m ? m[1] : "";
}

function summarizeDom(panelHtml) {
  const text = stripHtml(panelHtml);
  const emptyMessages = (panelHtml.match(/be-atelier-tab-empty-message|No Brand Setup Fields Are Populated/gi) || [])
    .length;
  const placeholders = (panelHtml.match(/be-atelier-placeholder|will appear|not published|confirm requirements/gi) || [])
    .length;
  const imgCount = (panelHtml.match(/<img\b/gi) || []).length;
  const cardHints = (panelHtml.match(/be-atelier-card|explorer-detail-card|oe-card|dealality-editorial-card/gi) || [])
    .length;
  return {
    textLength: text.length,
    textSample: text.slice(0, 280),
    emptyMessages,
    placeholders,
    imgCount,
    cardHints,
    text,
  };
}

function rowsForTab(presentationRows, tab) {
  return (presentationRows || []).filter((r) => {
    const sk = nz(r.slotKey);
    return tab.slotPrefixes.some((p) => sk.startsWith(p));
  });
}

function collectRowTexts(row) {
  const texts = [];
  for (const apiKey of Object.keys(MAP_PRESENTATION_FIELDS)) {
    const val = nz(row[apiKey]);
    if (val) texts.push(val);
  }
  return texts;
}

function collectBodyTexts(row) {
  const texts = [];
  for (const apiKey of VAGUE_SCAN_FIELDS) {
    const val = nz(row[apiKey]);
    if (val) texts.push(val);
  }
  return texts;
}

function isScrubBoilerplateVague(text) {
  const t = nz(text).trim();
  if (!t) return false;
  if (/^Confirm .{10,120}during brand engagement/i.test(t) && t.length < 220) return true;
  if (/orientation only/i.test(t) && t.length < 280) return true;
  if (/\bparticipation cost categories\b/i.test(t)) return true;
  if (/\bletter of intent or commercial proposal\b/i.test(t)) return true;
  return false;
}

function scoreTabReadiness({
  tab,
  panelHtml,
  presentationRows,
  visualAssets = null,
}) {
  const dom = summarizeDom(panelHtml);
  const rows = rowsForTab(presentationRows, tab).filter(
    (r) => !/do not display|internal only/i.test(nz(r.externalDisplayStatus))
  );
  const rowTexts = rows.flatMap(collectRowTexts);
  const bodyTexts = rows.flatMap(collectBodyTexts);
  const corpus = [dom.text, ...rowTexts].join("\n");

  const forbidden = scanForbiddenLanguage(corpus);
  const mechanical = scanMechanicalCopy(corpus);
  const mechanicalHigh = mechanical.filter((h) => h.severity === "high");
  const mechanicalMedium = mechanical.filter((h) => h.severity === "medium");
  const scrubVagueBodies = bodyTexts.filter(isScrubBoilerplateVague);
  const shortBodies = bodyTexts.filter((t) => nz(t).length > 0 && nz(t).length < 40);
  const qualityFlags = [];
  for (const flag of QUALITY_FLAGS) {
    if (flag.re.test(corpus)) qualityFlags.push(flag.label);
  }

  const visibleIssues = [];
  const copyConcerns = [];
  const imageConcerns = [];
  const emptyCardConcerns = [];
  const remainingJudgment = [];

  if (forbidden.length) {
    visibleIssues.push(`Forbidden owner language: ${forbidden.map((h) => h.label).join(", ")}`);
  }
  if (mechanicalHigh.length || mechanicalMedium.length) {
    copyConcerns.push(
      ...[...mechanicalHigh, ...mechanicalMedium]
        .slice(0, 6)
        .map((h) => `${h.note || h.id}${h.severity ? ` (${h.severity})` : ""}`)
    );
  } else if (mechanical.length) {
    remainingJudgment.push(
      `Low-severity scrub phrasing present (${mechanical.length}) — taste pass only`
    );
  }
  if (scrubVagueBodies.length) {
    copyConcerns.push(
      `${scrubVagueBodies.length} scrub-boilerplate body/case-summary field(s) in this tab`
    );
  }
  if (shortBodies.length >= 3) {
    remainingJudgment.push(
      `${shortBodies.length} short body fields (<40 chars) — founder taste, not auto-fail`
    );
  } else if (shortBodies.length === 1 || shortBodies.length === 2) {
    remainingJudgment.push(`${shortBodies.length} short body field(s) — confirm usefulness`);
  }
  if (qualityFlags.includes("too legalistic") || qualityFlags.includes("too vague")) {
    copyConcerns.push(`Tone flags: ${[...new Set(qualityFlags)].join(", ")}`);
  } else if (qualityFlags.length) {
    remainingJudgment.push(`Tone flags for founder taste: ${[...new Set(qualityFlags)].join(", ")}`);
  }
  if (dom.emptyMessages > 0) {
    emptyCardConcerns.push("Empty-tab message rendered");
  }
  if (dom.placeholders >= 2) {
    emptyCardConcerns.push(`${dom.placeholders} placeholder / confirm-requirements cue(s)`);
  }
  if (dom.textLength < 80 && rows.length === 0) {
    emptyCardConcerns.push("Very thin DOM + no Presentation rows for tab");
  }

  // Prefer API visual counts over DOM <img> (atelier often uses background/CSS images).
  if (tab.key === "brand_materials" && visualAssets) {
    if (!visualAssets.gallery.ready) {
      imageConcerns.push(
        `Gallery imageUrls: ${visualAssets.gallery.count}/${GALLERY_MIN} (API)`
      );
    } else {
      remainingJudgment.push(
        `Gallery ${visualAssets.gallery.count}/6 confirmed via API — founder should spot-check image quality`
      );
    }
  }
  if (tab.key === "footprint_growth" && visualAssets) {
    if (!visualAssets.propertyExamples.ready) {
      imageConcerns.push(
        `Property example imageUrls: ${visualAssets.propertyExamples.count}/${PROPERTY_EXAMPLE_MIN} (API)`
      );
    } else if (visualAssets.propertyExamples.needsFounderDecision) {
      remainingJudgment.push(visualAssets.propertyExamples.founderQuestion);
    } else {
      remainingJudgment.push(
        `Property examples ${visualAssets.propertyExamples.count}/3 confirmed via API — founder should spot-check`
      );
    }
  }

  if (tab.key === "economics_obligations") {
    remainingJudgment.push("Confirm economics read as owner diligence, not fee schedule / FDD");
  }
  if (tab.key === "operating_model") {
    remainingJudgment.push("Confirm operating model is brand-specific and useful");
  }
  if (tab.key === "dealality_insight") {
    remainingJudgment.push("Confirm Dealality insight is interpretive, not brochure fluff");
  }

  let status = "pass";
  if (forbidden.length || (dom.emptyMessages > 0 && rows.length === 0 && dom.textLength < 40)) {
    status = "fail";
  } else if (
    mechanicalHigh.length ||
    scrubVagueBodies.length >= 2 ||
    emptyCardConcerns.length ||
    imageConcerns.length ||
    mechanicalMedium.length >= 2 ||
    qualityFlags.includes("too legalistic")
  ) {
    status = "concern";
  } else if (copyConcerns.length || qualityFlags.includes("too vague")) {
    status = "concern";
  }

  return {
    tabId: tab.id,
    key: tab.key,
    label: tab.label,
    status,
    visibleIssues,
    copyConcerns,
    imageConcerns,
    emptyCardConcerns,
    remainingJudgmentItems: remainingJudgment,
    presentationRowCount: rows.length,
    domSummary: {
      textLength: dom.textLength,
      textSample: dom.textSample,
      emptyMessages: dom.emptyMessages,
      placeholders: dom.placeholders,
      imgCount: dom.imgCount,
      cardHints: dom.cardHints,
      scrubVagueBodies: scrubVagueBodies.length,
      shortBodies: shortBodies.length,
    },
    forbiddenHits: forbidden,
    mechanicalHits: mechanical.slice(0, 12),
  };
}

function loadV40CApplyReport() {
  return readJsonIfExists(
    path.join(ROOT, "reports", "brand-explorer-v40c-economics-chrome-remediation.json")
  );
}

function loadOsReport() {
  return readJsonIfExists(path.join(ROOT, "reports", "brand-explorer-v41-os-consolidation.json"));
}

function changedRowsFromV40C(v40cReport, brandSlug) {
  const brand = (v40cReport?.brandResults || []).find((b) => b.brandSlug === brandSlug);
  if (!brand) {
    return {
      applyExecuted: v40cReport?.applyExecuted === true,
      recordsTouched: null,
      patchCount: null,
      sampleSlots: [],
      note: "v40C apply report missing brand entry",
    };
  }
  const patches = brand.residualPresentation?.patches || brand.residualPlan?.patches || [];
  const apply = brand.applyResult || {};
  return {
    applyExecuted: v40cReport?.applyExecuted === true,
    recordsTouched: apply.recordsTouched ?? null,
    patchCount: brand.residualPresentation?.summary?.patchCount ?? patches.length,
    sampleSlots: [...new Set(patches.map((p) => p.slotKey).filter(Boolean))].slice(0, 24),
    fieldsTouched: ["Title", "Body", "Case Summary Overview", "Case Summary Brand Relevance", "Case Summary Owner Objective", "Case Summary Interpretation"],
    note: "v40C scrubbed Presentation owner-copy residual only (no unlock / no active approval).",
  };
}

function evaluateBrandLenses(brandSlug, corpus) {
  const lens = V42_BRAND_LENSES[brandSlug];
  if (!lens) {
    return { brandSlug, questions: [], pass: false, missing: ["no_lens"] };
  }
  const answers = lens.questions.map((q) => {
    if (q.avoidRe) {
      const hit = q.avoidRe.test(corpus);
      return {
        id: q.id,
        question: q.question,
        status: hit ? "fail" : "pass",
        detail: hit ? "Avoid pattern still present" : "Avoid pattern clear",
      };
    }
    const present = q.re.test(corpus);
    return {
      id: q.id,
      question: q.question,
      status: present ? "pass" : "concern",
      detail: present ? "Signal present in internal preview / Presentation" : "Signal thin or missing — founder judgment",
    };
  });
  return {
    brandSlug,
    brandName: lens.brandName,
    questions: answers,
    failCount: answers.filter((a) => a.status === "fail").length,
    concernCount: answers.filter((a) => a.status === "concern").length,
    pass: answers.every((a) => a.status === "pass"),
  };
}

function recommendRelease({
  forbiddenPass,
  galleryReady,
  propertyExamplesReady,
  logoOrGenericProperty,
  tabStatuses,
  mechanicalHigh,
  brandLenses,
  brandSignals,
  extraPropertyExamples,
  osState,
  externalLockPass,
}) {
  const tabFails = tabStatuses.filter((t) => t.status === "fail");
  const tabConcerns = tabStatuses.filter((t) => t.status === "concern");

  if (!externalLockPass) {
    return {
      recommendation: "not_owner_ready",
      rationale: "External quality lock failed — profile may be leaking before active release.",
    };
  }
  if (!forbiddenPass || !galleryReady || !propertyExamplesReady || logoOrGenericProperty) {
    return {
      recommendation: "not_owner_ready",
      rationale: !forbiddenPass
        ? "Forbidden owner language still visible in internal preview / Presentation."
        : logoOrGenericProperty
          ? "Property examples include logo or generic graphic assets."
          : `Visual minimums incomplete (gallery ready=${galleryReady}, property examples ready=${propertyExamplesReady}).`,
    };
  }
  if (tabFails.length || mechanicalHigh > 0 || (brandLenses.failCount || 0) > 0) {
    return {
      recommendation: "remediation_required",
      rationale: tabFails.length
        ? `Tab fail(s): ${tabFails.map((t) => t.label).join(", ")}.`
        : mechanicalHigh > 0
          ? "High-severity mechanical scrub artifacts remain."
          : "Brand lens avoid-pattern failures remain.",
    };
  }
  const hardTabConcerns = tabConcerns.filter(
    (t) =>
      (t.copyConcerns || []).length > 0 ||
      (t.imageConcerns || []).length > 0 ||
      (t.emptyCardConcerns || []).length > 0 ||
      (t.visibleIssues || []).length > 0
  );

  if (
    (brandSignals.missingExpected || []).length >= 2 ||
    (brandLenses.failCount || 0) > 0 ||
    hardTabConcerns.length >= 4
  ) {
    return {
      recommendation: "remediation_required",
      rationale: "Multiple brand-positioning or hard tab concerns need copy/visual cleanup before approve.",
    };
  }
  if (
    extraPropertyExamples.needsFounderDecision ||
    hardTabConcerns.length > 0 ||
    tabConcerns.length > 0 ||
    (brandLenses.concernCount || 0) > 0 ||
    (brandSignals.missingExpected || []).length > 0 ||
    osState !== "founder_review_ready"
  ) {
    return {
      recommendation: "approve_after_minor_cleanup",
      rationale: extraPropertyExamples.needsFounderDecision
        ? `Visual/copy largely ready; founder must decide whether ${extraPropertyExamples.count} property examples (min ${PROPERTY_EXAMPLE_MIN}) are acceptable or extras should be hidden.`
        : hardTabConcerns.length
          ? `Minor tab concerns remain (${hardTabConcerns.map((t) => t.label).join(", ")}); soft approve after founder taste pass.`
          : "Soft approve after minor founder judgment items (taste / brand lens).",
    };
  }
  return {
    recommendation: "approve_for_active_release",
    rationale:
      "Internal preview owner-copy clean, visuals meet minimums, brand lenses present, tabs pass. Active release still requires explicit founder approval (not applied by v42).",
  };
}

function buildVisualAssetConfirmation(brandApi, registryAssets, brandConfig) {
  const blocks = brandApi?.brandExplorer?.blocks || [];
  const galleryRule = evaluateGalleryRule(brandApi);
  const propertyRule = evaluatePropertyExampleRule(brandApi, registryAssets, brandConfig);

  const openings = blocks.filter((b) => nz(b.slotKey) === "footprint.openings" && nz(b.imageUrl));
  const openingDetails = openings.map((b) => {
    const registry = registryAssets?.find?.(
      (a) => a?.presentationRecordId === b.recordId || a?.recordId === b.registryRecordId
    );
    const cls = classifyPropertyExampleImage(b.imageUrl, {
      registrySourceUrl: registry?.sourceUrl || "",
      registryNotes: [registry?.sourceNotes, registry?.reviewNotes].filter(Boolean).join("\n"),
    });
    return {
      recordId: b.recordId || null,
      title: nz(b.title),
      imageUrl: nz(b.imageUrl).slice(0, 160),
      classification: cls.category,
      isLogo: cls.isLogo === true,
      isGeneric: cls.isGenericBrand === true || cls.isLifestyle === true,
      sectionLabelOk: /property example|opening|recent/i.test(nz(b.title)) || nz(b.title).length > 0,
    };
  });

  const logoAsProperty = openingDetails.filter((d) => d.isLogo);
  const genericAsProperty = openingDetails.filter((d) => d.isGeneric);
  const count = openings.length;
  const extras = Math.max(0, count - PROPERTY_EXAMPLE_MIN);

  return {
    gallery: {
      count: galleryRule.withImageUrl,
      required: GALLERY_MIN,
      ready: galleryRule.withImageUrl >= GALLERY_MIN,
      logoOrGenericSlots: galleryRule.logoOrGenericSlots || [],
    },
    propertyExamples: {
      count,
      required: PROPERTY_EXAMPLE_MIN,
      ready: count >= PROPERTY_EXAMPLE_MIN,
      extras,
      needsFounderDecision: extras > 0,
      founderQuestion:
        extras > 0
          ? `Founder: are ${count} property examples acceptable, or should ${extras} extra(s) be hidden?`
          : null,
      noLogosAsPropertyExamples: logoAsProperty.length === 0,
      noGenericGraphicsAsPropertyExamples: genericAsProperty.length === 0,
      sectionLabelAccurate: openingDetails.every((d) => d.sectionLabelOk),
      details: openingDetails,
      defects: propertyRule.defects || [],
    },
    galleryRulePass: galleryRule.pass === true,
    propertyRulePass: propertyRule.pass === true || (count >= PROPERTY_EXAMPLE_MIN && logoAsProperty.length === 0),
  };
}

async function auditIncompleteControl() {
  const results = [];
  for (const brandSlug of V42_INCOMPLETE_CONTROL) {
    try {
      const brandApi = await fetchBrandApiShape(brandSlug);
      const html = renderBrandExplorerHtmlForTest(brandApi, {
        allPanels: true,
        internalPreview: false,
      });
      const ql = evaluateBrandExternalQualityLock(brandApi, html, { brandSlug });
      const pass =
        brandApi.shouldRenderFullProfile !== true &&
        ql.profileInPreparationRendered === true &&
        (ql.tabsRenderedExternally || []).length <= 1;
      results.push({
        brandSlug,
        displayState: brandApi.brandExplorerDisplayState,
        shouldRenderFullProfile: brandApi.shouldRenderFullProfile === true,
        profileInPreparationRendered: ql.profileInPreparationRendered === true,
        tabsRendered: (ql.tabsRenderedExternally || []).length,
        pass,
      });
    } catch (err) {
      results.push({ brandSlug, pass: false, error: err.message });
    }
  }
  return {
    allLocked: results.every((r) => r.pass),
    results,
  };
}

/**
 * Build founder visual review packet for one brand.
 */
export async function auditBrandFounderVisualReview(brandSlug) {
  const config = resolveConfig(brandSlug);
  const ctx = await loadBrandFactoryContext(brandSlug).catch(() => null);
  const brandApi = await fetchBrandApiShape(brandSlug);
  const presentationRows = ctx?.presentationRows || brandApi?.brandExplorer?.blocks || [];
  const registryAssets = ctx?.registryAssets || [];

  const osBrand = await evaluateBrandExplorerOsBrand(brandSlug).catch((err) => ({
    error: err.message,
    canonicalState: null,
    routing: null,
  }));

  const internalHtml = renderBrandExplorerHtmlForTest(brandApi, {
    allPanels: true,
    internalPreview: true,
  });
  const externalHtml = renderBrandExplorerHtmlForTest(brandApi, {
    allPanels: true,
    internalPreview: false,
  });
  const externalQl = evaluateBrandExternalQualityLock(brandApi, externalHtml, {
    brandSlug,
    brandBasics: ctx?.brandBasics,
  });

  const ownerFacing = (brandApi?.brandExplorer?.blocks || []).filter(isOwnerFacingPresentationRow);
  const uniqueness = evaluateImageUniqueness({
    brand: brandApi,
    presentationRows: ownerFacing,
    brandSlug,
  });
  const roleMatch = evaluateBrandImageRoleMatch({
    presentationRows: ownerFacing,
    brandSlug,
  });
  const tabFactory = evaluateTabFactoryFromPayload({
    brand: brandApi,
    rows: ownerFacing,
    html: internalHtml,
    brandSlug,
  });
  const recordId = brandApi.id || config?.recordId || BUILT_BLOCKED_IDENTITIES[brandSlug]?.recordId || null;
  const previewUrls = buildInternalPreviewUrls(recordId, brandSlug);

  const internalText = stripHtml(internalHtml);
  const visuals = buildVisualAssetConfirmation(brandApi, registryAssets, config);
  const tabReviews = V42_REVIEW_TABS.map((tab) => {
    const panelHtml = extractTabPanelHtml(internalHtml, tab.id);
    return scoreTabReadiness({
      tab,
      panelHtml,
      presentationRows,
      visualAssets: visuals,
    });
  });

  const presentationCorpus = (presentationRows || [])
    .filter((r) => !/do not display|internal only/i.test(nz(r.externalDisplayStatus)))
    .flatMap(collectRowTexts)
    .join("\n");
  const corpus = [internalText, presentationCorpus].join("\n\n");

  const forbidden = scanForbiddenLanguage(corpus);
  const mechanical = scanMechanicalCopy(corpus);
  const mechanicalHigh = mechanical.filter((h) => h.severity === "high").length;
  const brandSignals = evaluateBrandCopySignals(brandSlug, corpus);
  const brandLenses = evaluateBrandLenses(brandSlug, corpus);
  const repeated = detectRepeatedBoilerplate(
    (presentationRows || []).flatMap(collectRowTexts)
  );

  const qualityTone = [];
  for (const flag of QUALITY_FLAGS) {
    if (flag.re.test(corpus)) qualityTone.push(flag.label);
  }

  const v40c = loadV40CApplyReport();
  const changedRows = changedRowsFromV40C(v40c, brandSlug);

  const release = recommendRelease({
    forbiddenPass: forbidden.length === 0,
    galleryReady: visuals.gallery.ready,
    propertyExamplesReady: visuals.propertyExamples.ready,
    logoOrGenericProperty:
      !visuals.propertyExamples.noLogosAsPropertyExamples ||
      !visuals.propertyExamples.noGenericGraphicsAsPropertyExamples,
    tabStatuses: tabReviews,
    mechanicalHigh,
    brandLenses,
    brandSignals,
    extraPropertyExamples: visuals.propertyExamples,
    osState: osBrand.canonicalState,
    externalLockPass:
      externalQl.externalQualityLockPass === true || externalQl.profileInPreparationRendered === true,
  });

  const remainingJudgment = [];
  if (visuals.propertyExamples.needsFounderDecision) {
    remainingJudgment.push(visuals.propertyExamples.founderQuestion);
  }
  for (const q of brandLenses.questions || []) {
    if (q.status !== "pass") remainingJudgment.push(q.question);
  }
  for (const t of tabReviews) {
    for (const item of t.remainingJudgmentItems || []) remainingJudgment.push(`[${t.label}] ${item}`);
  }
  remainingJudgment.push("Do not set active-profile approval until founder explicitly approves");
  remainingJudgment.push("Company Validated must remain untouched unless true company validation exists");
  remainingJudgment.push("Public-full restore must wait for explicit restore command after founder approval");

  const risks = [];
  if (forbidden.length) risks.push(`Forbidden: ${forbidden.map((h) => h.label).join(", ")}`);
  if (mechanicalHigh) risks.push(`${mechanicalHigh} high-severity mechanical hits`);
  if (repeated.length) risks.push("Repeated diligence boilerplate across rows");
  if (qualityTone.length) risks.push(`Tone: ${[...new Set(qualityTone)].join(", ")}`);
  if (!visuals.propertyExamples.noLogosAsPropertyExamples) risks.push("Logo used as property example");
  if (!visuals.propertyExamples.noGenericGraphicsAsPropertyExamples) {
    risks.push("Generic graphic used as property example");
  }
  if (!uniqueness.pass) {
    risks.push(
      `Image uniqueness short g/s/p=${uniqueness.galleryDistinctCount}/${uniqueness.scenarioDistinctCount}/${uniqueness.propertyExampleDistinctCount}`
    );
  }
  if (!roleMatch.pass) risks.push("Image role-match failures");

  const gateSummary = {
    tabFactoryAuditPass: tabFactory.auditPass === true,
    renderedFieldCompletenessPass: tabFactory.completeness?.auditPass === true,
    noEmptyRenderedComponentsPass: tabFactory.emptyScan?.pass === true,
    sourceProvenancePass: tabFactory.provenance?.pass === true,
    goldenContentQualityPass: tabFactory.golden?.pass === true,
    imageUniquenessPass: uniqueness.pass === true,
    imageRoleMatchPass: roleMatch.pass === true,
    fullyReady:
      tabFactory.completeness?.auditPass === true &&
      tabFactory.emptyScan?.pass === true &&
      tabFactory.provenance?.pass === true &&
      tabFactory.golden?.pass === true &&
      uniqueness.pass === true &&
      roleMatch.pass === true,
  };

  const publicRestoreReadiness = {
    decision: "hold_no_public_restore",
    readyForFounderApprovalCommand:
      release.recommendation === "approve_for_active_release" ||
      release.recommendation === "approve_after_minor_cleanup",
    restoreAllowedNow: false,
    note: "Founder visual review packet only — do not write release fields or restore public-full until explicit restore command after founder approval.",
  };

  return {
    brandSlug,
    brandName: brandApi.name || V42_BRAND_LENSES[brandSlug]?.brandName || brandSlug,
    recordId,
    displayState: brandApi.brandExplorerDisplayState,
    shouldRenderFullProfile: brandApi.shouldRenderFullProfile === true,
    os: {
      canonicalState: osBrand.canonicalState || null,
      allowedNextAction: osBrand.routing?.allowedNextAction || null,
      founderReviewAllowed: osBrand.routing?.founderReviewAllowed === true,
      activeReleaseAllowed: osBrand.routing?.activeReleaseAllowed === true,
      error: osBrand.error || null,
    },
    internalPreview: {
      enabled: true,
      ...previewUrls,
      htmlLength: internalHtml.length,
      panelsFound: (internalHtml.match(/data-atelier-panel="/g) || []).length,
      forbiddenPass: forbidden.length === 0,
    },
    gateSummary,
    imageDistinctCounts: {
      galleryDistinct: uniqueness.galleryDistinctCount,
      scenarioDistinct: uniqueness.scenarioDistinctCount,
      propertyExampleDistinct: uniqueness.propertyExampleDistinctCount,
      uniquenessPass: uniqueness.pass === true,
      roleMatchPass: roleMatch.pass === true,
    },
    publicRestoreReadiness,
    externalLock: {
      profileInPreparation: externalQl.profileInPreparationRendered === true,
      fullProfileLeaked: brandApi.shouldRenderFullProfile === true,
      pass:
        externalQl.externalQualityLockPass === true || externalQl.profileInPreparationRendered === true,
      note: "External lock PASS only proves Profile in Preparation — not owner-ready copy.",
    },
    tabs: tabReviews,
    tabStatusCounts: {
      pass: tabReviews.filter((t) => t.status === "pass").length,
      concern: tabReviews.filter((t) => t.status === "concern").length,
      fail: tabReviews.filter((t) => t.status === "fail").length,
    },
    visualAssets: visuals,
    copyQuality: {
      forbiddenPass: forbidden.length === 0,
      forbiddenHits: forbidden,
      checks: {
        noFdd: !forbidden.some((h) => h.id === "fdd"),
        noLoi: !forbidden.some((h) => h.id === "loi"),
        noItem19: !forbidden.some((h) => h.id === "item_19"),
        noFeeStack: !forbidden.some((h) => h.id === "fee_stack"),
        noNetContribution: !forbidden.some((h) => h.id === "net_contribution"),
        noRawUrls: !forbidden.some((h) => h.id === "raw_url"),
        noSourceNotes: !forbidden.some((h) => h.id === "sources_block" || h.id === "source_line"),
        noMechanicalRepeated: repeated.length === 0,
      },
      mechanical: {
        hitCount: mechanical.length,
        highSeverityCount: mechanicalHigh,
        hits: mechanical.slice(0, 20),
        repeatedBoilerplate: repeated,
      },
      toneFlags: [...new Set(qualityTone)],
      brandSignals,
      soundsNaturalOwnerFacing: forbidden.length === 0 && mechanicalHigh === 0,
    },
    brandLenses,
    changedRows,
    risks,
    remainingJudgmentItems: [...new Set(remainingJudgment)],
    releaseRecommendation: release,
    guardrails: {
      airtableWrites: false,
      activeProfileApproval: false,
      companyValidatedChanges: false,
      unlock: false,
      sourceLibraryChanges: false,
      registryChanges: false,
      imageFieldChanges: false,
      activeReleaseApplied: false,
    },
  };
}

export async function runBrandExplorerFounderVisualReview({
  brands = V42_DEFAULT_BRANDS,
  dryRun = true,
} = {}) {
  if (!dryRun) {
    throw new Error("v42 founder visual review is read-only. Use --dry-run only.");
  }

  const brandResults = [];
  for (const brandSlug of brands) {
    brandResults.push(await auditBrandFounderVisualReview(brandSlug));
  }

  const incompleteControl = await auditIncompleteControl();
  const osReport = loadOsReport();

  const summary = {
    brandsReviewed: brandResults.length,
    approveForActiveRelease: brandResults.filter(
      (b) => b.releaseRecommendation.recommendation === "approve_for_active_release"
    ).length,
    approveAfterMinorCleanup: brandResults.filter(
      (b) => b.releaseRecommendation.recommendation === "approve_after_minor_cleanup"
    ).length,
    remediationRequired: brandResults.filter(
      (b) => b.releaseRecommendation.recommendation === "remediation_required"
    ).length,
    notOwnerReady: brandResults.filter(
      (b) => b.releaseRecommendation.recommendation === "not_owner_ready"
    ).length,
    incompleteControlLocked: incompleteControl.allLocked,
    anyActiveReleaseApplied: false,
    anyUnlock: false,
  };

  return {
    version: V42_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: true,
    brands,
    purpose:
      "Founder-facing visual review packet after v40C apply. Human judgment gate before active release.",
    brandResults,
    incompleteControl,
    priorOsSnapshot: osReport
      ? {
          generatedAt: osReport.generatedAt || null,
          summary: osReport.summary || null,
        }
      : null,
    summary,
    guardrails: {
      airtableWrites: false,
      activeProfileApproval: false,
      companyValidatedChanges: false,
      unlock: false,
      sourceLibraryChanges: false,
      registryChanges: false,
      imageFieldChanges: false,
      activeReleaseApplied: false,
    },
  };
}

export function renderFounderVisualReviewMarkdown(brand) {
  const lines = [
    `# Founder Visual Review — ${brand.brandName}`,
    "",
    `Slug: \`${brand.brandSlug}\` · Record: \`${brand.recordId || "n/a"}\``,
    `Generated: ${new Date().toISOString()} · ${V42_VERSION}`,
    "",
    "> Review via **internal preview** (`?beInternalPreview=1`). External owners still see **Profile in Preparation** only.",
    "",
    "## Internal / founder preview",
    "",
    `- Production: ${brand.internalPreview?.productionUrl || "n/a"}`,
    `- Local: ${brand.internalPreview?.localPath || "n/a"}`,
    `- API: ${brand.internalPreview?.apiPath || "n/a"}`,
    "",
    "## Gate summary",
    "",
    `- Fully ready: **${brand.gateSummary?.fullyReady === true}**`,
    `- Tab Factory: ${brand.gateSummary?.tabFactoryAuditPass === true}`,
    `- Rendered field completeness: ${brand.gateSummary?.renderedFieldCompletenessPass === true}`,
    `- No empty rendered components: ${brand.gateSummary?.noEmptyRenderedComponentsPass === true}`,
    `- Source provenance: ${brand.gateSummary?.sourceProvenancePass === true}`,
    `- Golden content quality: ${brand.gateSummary?.goldenContentQualityPass === true}`,
    `- Image uniqueness: ${brand.gateSummary?.imageUniquenessPass === true}`,
    `- Image role-match: ${brand.gateSummary?.imageRoleMatchPass === true}`,
    "",
    "## Image distinct counts",
    "",
    `- Gallery distinct: **${brand.imageDistinctCounts?.galleryDistinct ?? "n/a"}** (min 6)`,
    `- Scenario distinct: **${brand.imageDistinctCounts?.scenarioDistinct ?? "n/a"}** (min 3)`,
    `- Property example distinct: **${brand.imageDistinctCounts?.propertyExampleDistinct ?? "n/a"}** (min 3)`,
    "",
    "## Public restore readiness",
    "",
    `**${brand.publicRestoreReadiness?.decision || "hold_no_public_restore"}**`,
    "",
    brand.publicRestoreReadiness?.note || "No public-full restore in this step.",
    "",
    "## Release recommendation",
    "",
    `**${brand.releaseRecommendation.recommendation}**`,
    "",
    brand.releaseRecommendation.rationale,
    "",
    "> v42 does **not** apply active release, set approval, change Company Validated, or unlock.",
    "",
    "## OS routing",
    "",
    `- Canonical state: **${brand.os.canonicalState || "n/a"}**`,
    `- Allowed next action: \`${brand.os.allowedNextAction || "n/a"}\``,
    `- Founder review allowed: ${brand.os.founderReviewAllowed ? "yes" : "no"}`,
    `- Active release allowed by OS: ${brand.os.activeReleaseAllowed ? "yes" : "no"}`,
    "",
    "## What changed (v40C)",
    "",
    `- Apply executed: ${brand.changedRows.applyExecuted ? "yes" : "no / unknown"}`,
    `- Records patched: ${brand.changedRows.recordsTouched ?? "n/a"}`,
    `- Residual patches: ${brand.changedRows.patchCount ?? "n/a"}`,
    `- ${brand.changedRows.note}`,
    brand.changedRows.sampleSlots?.length
      ? `- Sample slots: ${brand.changedRows.sampleSlots.slice(0, 12).map((s) => `\`${s}\``).join(", ")}`
      : "",
    "",
    "## Visual asset confirmation",
    "",
    `- Gallery imageUrls: **${brand.visualAssets.gallery.count}/${GALLERY_MIN}** (${brand.visualAssets.gallery.ready ? "ready" : "short"})`,
    `- Property example imageUrls: **${brand.visualAssets.propertyExamples.count}/${PROPERTY_EXAMPLE_MIN}** (${brand.visualAssets.propertyExamples.ready ? "ready" : "short"})`,
    `- No logos as property examples: **${brand.visualAssets.propertyExamples.noLogosAsPropertyExamples ? "yes" : "no"}**`,
    `- No generic graphics as property examples: **${brand.visualAssets.propertyExamples.noGenericGraphicsAsPropertyExamples ? "yes" : "no"}**`,
    `- Section labels accurate: **${brand.visualAssets.propertyExamples.sectionLabelAccurate ? "yes" : "review"}**`,
    brand.visualAssets.propertyExamples.founderQuestion
      ? `- **${brand.visualAssets.propertyExamples.founderQuestion}**`
      : "- Extra property examples: none (at minimum)",
    "",
  ];

  if (brand.visualAssets.propertyExamples.details?.length) {
    lines.push("### Property example inventory", "");
    for (const d of brand.visualAssets.propertyExamples.details) {
      lines.push(
        `- ${d.title || "(untitled)"} · class=\`${d.classification}\`${d.isLogo ? " · **LOGO**" : ""}${d.isGeneric ? " · **GENERIC**" : ""}`
      );
    }
    lines.push("");
  }

  lines.push("## Internal preview — tab readiness", "");
  lines.push(
    `| Tab | Status | Issues |`,
    `| --- | --- | --- |`
  );
  for (const t of brand.tabs) {
    const issues = [
      ...t.visibleIssues,
      ...t.copyConcerns.slice(0, 2),
      ...t.imageConcerns,
      ...t.emptyCardConcerns,
    ]
      .slice(0, 3)
      .join("; ");
    lines.push(`| ${t.label} | **${t.status}** | ${issues || "—"} |`);
  }
  lines.push("");

  for (const t of brand.tabs) {
    lines.push(`### ${t.label} (\`${t.status}\`)`, "");
    lines.push(
      `- DOM: ${t.domSummary.textLength} chars · ${t.domSummary.imgCount} imgs · ${t.domSummary.cardHints} card hints · ${t.presentationRowCount} Presentation rows`
    );
    if (t.domSummary.textSample) {
      lines.push(`- Sample: ${t.domSummary.textSample.slice(0, 180)}${t.domSummary.textSample.length > 180 ? "…" : ""}`);
    }
    if (t.visibleIssues.length) {
      for (const i of t.visibleIssues) lines.push(`- Visible: ${i}`);
    }
    if (t.copyConcerns.length) {
      for (const i of t.copyConcerns) lines.push(`- Copy: ${i}`);
    }
    if (t.imageConcerns.length) {
      for (const i of t.imageConcerns) lines.push(`- Image: ${i}`);
    }
    if (t.emptyCardConcerns.length) {
      for (const i of t.emptyCardConcerns) lines.push(`- Empty: ${i}`);
    }
    if (t.remainingJudgmentItems.length) {
      for (const i of t.remainingJudgmentItems) lines.push(`- Judgment: ${i}`);
    }
    if (
      !t.visibleIssues.length &&
      !t.copyConcerns.length &&
      !t.imageConcerns.length &&
      !t.emptyCardConcerns.length
    ) {
      lines.push("- No automated concerns.");
    }
    lines.push("");
  }

  lines.push("## Copy quality", "");
  lines.push(
    brand.copyQuality.forbiddenPass
      ? "- Forbidden language: **pass** (no FDD / LOI / Item 19 / fee stack / net contribution / raw URLs / source notes)"
      : `- Forbidden language: **fail** — ${brand.copyQuality.forbiddenHits.map((h) => h.label).join(", ")}`
  );
  lines.push(
    `- Mechanical hits: ${brand.copyQuality.mechanical.hitCount} (high: ${brand.copyQuality.mechanical.highSeverityCount})`
  );
  if (brand.copyQuality.toneFlags.length) {
    lines.push(`- Tone flags: ${brand.copyQuality.toneFlags.join(", ")}`);
  } else {
    lines.push("- Tone flags: none automated");
  }
  lines.push(
    `- Sounds natural / owner-facing (automated): **${brand.copyQuality.soundsNaturalOwnerFacing ? "yes" : "review"}**`
  );
  lines.push("");

  lines.push("## Brand-specific lenses", "");
  for (const q of brand.brandLenses.questions || []) {
    lines.push(`- **${q.status}** — ${q.question}`);
    lines.push(`  - ${q.detail}`);
  }
  for (const e of brand.copyQuality.brandSignals?.expected || []) {
    lines.push(`- Expected · ${e.label}: ${e.present ? "present" : "**missing**"}`);
  }
  lines.push("");

  lines.push("## Risks", "");
  if (!brand.risks.length) lines.push("- None flagged beyond normal founder taste pass.");
  else for (const r of brand.risks) lines.push(`- ${r}`);
  lines.push("");

  lines.push("## Remaining founder judgment", "");
  for (const item of brand.remainingJudgmentItems) lines.push(`- ${item}`);
  lines.push("");

  lines.push(
    "## External lock",
    "",
    `- Profile in Preparation: **${brand.externalLock.profileInPreparation ? "yes" : "no"}**`,
    `- Full profile leaked: **${brand.externalLock.fullProfileLeaked ? "yes" : "no"}**`,
    "",
    "## Guardrails",
    "",
    "- No Airtable writes",
    "- No active-profile approval",
    "- No Company Validated changes",
    "- No unlock",
    "- No Source Library / Registry / image-field changes",
    "- No active release apply",
    ""
  );

  return lines.filter((l) => l !== undefined && l !== null).join("\n");
}

export function writeV42Reports(report, reportsDir = path.join(ROOT, "reports")) {
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, REPORT_JSON);
  const mdPath = path.join(reportsDir, REPORT_MD);
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf8");

  const md = [
    "# v42 Brand Explorer Founder Visual Review",
    "",
    `Generated: ${report.generatedAt}`,
    "",
    "Read-only founder packet after v40C remediation. Internal preview is the review surface. **No active release applied.**",
    "",
    "## Summary",
    "",
    `- Brands reviewed: ${report.summary.brandsReviewed}`,
    `- approve_for_active_release: ${report.summary.approveForActiveRelease}`,
    `- approve_after_minor_cleanup: ${report.summary.approveAfterMinorCleanup}`,
    `- remediation_required: ${report.summary.remediationRequired}`,
    `- not_owner_ready: ${report.summary.notOwnerReady}`,
    `- Incomplete brands locked: **${report.summary.incompleteControlLocked ? "yes" : "no"}**`,
    "",
    "## Per-brand recommendations",
    "",
  ];

  for (const b of report.brandResults) {
    md.push(`### ${b.brandName} (\`${b.brandSlug}\`)`);
    md.push(`- **${b.releaseRecommendation.recommendation}**`);
    md.push(`- ${b.releaseRecommendation.rationale}`);
    md.push(
      `- Tabs: pass=${b.tabStatusCounts.pass} concern=${b.tabStatusCounts.concern} fail=${b.tabStatusCounts.fail}`
    );
    md.push(
      `- Gallery ${b.imageDistinctCounts?.galleryDistinct ?? b.visualAssets.gallery.count}/6 · scenario ${b.imageDistinctCounts?.scenarioDistinct ?? "—"}/3 · property ${b.imageDistinctCounts?.propertyExampleDistinct ?? b.visualAssets.propertyExamples.count}/3`
    );
    md.push(`- FullyReady: ${b.gateSummary?.fullyReady === true}`);
    md.push(`- Preview: ${b.internalPreview?.productionUrl || "n/a"}`);
    md.push(`- Public restore: **${b.publicRestoreReadiness?.decision || "hold_no_public_restore"}**`);
    md.push(`- OS: ${b.os.canonicalState} → ${b.os.allowedNextAction}`);
    md.push("");
  }

  md.push("## Incomplete brand control", "");
  for (const r of report.incompleteControl.results || []) {
    md.push(
      `- \`${r.brandSlug}\`: ${r.pass ? "locked" : "**NOT LOCKED**"} · display=${r.displayState || "n/a"} · prep=${r.profileInPreparationRendered === true}`
    );
  }
  md.push("");
  md.push("## Guardrails");
  md.push("- No Airtable writes · no active approval · no Company Validated · no unlock · no active release");
  md.push("");

  fs.writeFileSync(mdPath, md.join("\n"), "utf8");

  const founderPaths = {};
  for (const b of report.brandResults) {
    const fname = `brand-explorer-v42-founder-review-${b.brandSlug}.md`;
    const fpath = path.join(reportsDir, fname);
    fs.writeFileSync(fpath, renderFounderVisualReviewMarkdown(b), "utf8");
    founderPaths[b.brandSlug] = fpath;
  }

  return { jsonPath, mdPath, founderPaths };
}
