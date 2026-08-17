import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import {
  buildBrandExplorerSlotStandardManifestReport,
} from "./brand-explorer-slot-standard-manifest.js";

export const WRITER_VERSION = "19";
export const REPORT_JSON_NAME = "brand-explorer-slot-completion-planner.json";
export const REPORT_MD_NAME = "brand-explorer-slot-completion-planner.md";
export const DOC_MD_NAME = "brand-explorer-slot-completion-planner-v19.md";

const DEFAULT_BRAND_ID = "recCvV0PuZOi8c3hC";
const REVIEW_STATUS =
  "AI-drafted / pending founder review; Not company-validated; Not Marriott-validated";

const BATCH = {
  safeEditorial: "batch_1_safe_editorial_human_review",
  sourceEvidence: "batch_2_source_evidence_required",
  mediaAsset: "batch_3_media_or_asset_required",
  remainBlank: "batch_4_should_remain_blank",
  uncertain: "batch_5_classification_uncertain",
};

const TAB_ORDER = [
  "Overview",
  "Value to Owners",
  "Operating Model",
  "Owner Considerations",
  "Commercial Engine",
  "Economics & Obligations",
  "Loyalty Program",
  "Footprint & Growth",
  "Brand Materials",
  "Dealality Insight",
];

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");

function hasVal(v) {
  if (v == null) return false;
  if (Array.isArray(v)) return v.length > 0;
  return String(v).trim() !== "";
}

function toText(v) {
  if (Array.isArray(v)) return v.filter(hasVal).map(String).join(", ");
  return hasVal(v) ? String(v).trim() : "";
}

function short(text, max = 200) {
  const s = toText(text).replace(/\s+/g, " ");
  return s.length > max ? `${s.slice(0, max - 1)}...` : s;
}

function normalizeBrandInput(raw) {
  const normalized = toText(raw).toLowerCase().trim();
  if (!normalized) return DEFAULT_BRAND_ID;
  if (normalized === "tribute-portfolio" || normalized === "tribute portfolio") return DEFAULT_BRAND_ID;
  return toText(raw).trim();
}

function tabFromSlot(slotKey) {
  if (slotKey.startsWith("overview.") || slotKey.startsWith("hero.")) return "Overview";
  if (slotKey.startsWith("valueOwners.")) return "Value to Owners";
  if (slotKey.startsWith("operations.")) return "Operating Model";
  if (slotKey.startsWith("standards.")) return "Owner Considerations";
  if (slotKey.startsWith("commercial.")) return "Commercial Engine";
  if (slotKey.startsWith("economics.")) return "Economics & Obligations";
  if (slotKey.startsWith("loyalty.")) return "Loyalty Program";
  if (slotKey.startsWith("footprint.")) return "Footprint & Growth";
  if (slotKey.startsWith("materials.")) return "Brand Materials";
  if (slotKey.startsWith("insight.")) return "Dealality Insight";
  return "Unknown";
}

function sectionFromSlot(slotKey) {
  if (/^overview\.scenario\./i.test(slotKey)) return "Overview scenarios";
  if (/^overview\.bestAt\./i.test(slotKey)) return "What This Brand Is Best At";
  if (/^overview\.proof\./i.test(slotKey)) return "Proof grid";
  if (/^commercial\.lever\./i.test(slotKey)) return "Commercial strength card";
  if (/^commercial\.kpi\./i.test(slotKey)) return "Commercial KPI strip";
  if (/^operations\.flexibility\./i.test(slotKey)) return "Flexibility indicator";
  if (/^operations\.model\./i.test(slotKey)) return "Operating model field";
  if (/^operations\.compliance\./i.test(slotKey)) return "Compliance & oversight";
  if (/^footprint\.region\./i.test(slotKey)) return "Regional footprint card";
  if (/^valueOwners\.lifecycle\./i.test(slotKey)) return "Owner lifecycle phase";
  if (/^economics\.opening\.step\./i.test(slotKey)) return "Opening timeline step";
  return tabFromSlot(slotKey);
}

async function fetchBrand(brandIdOrName) {
  const req = { query: { brandId: brandIdOrName, refresh: "1" }, headers: {} };
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(payload) {
      this.payload = payload;
      return this;
    },
  };
  await getBrandLibraryBrandById(req, res);
  if (res.statusCode >= 400 || !res.payload?.brand) return null;
  return res.payload.brand;
}

function listFixtureFiles() {
  const fixturesDir = path.join(ROOT, "fixtures");
  if (!fs.existsSync(fixturesDir)) return [];
  return fs
    .readdirSync(fixturesDir)
    .filter((n) => /^brand-explorer-presentation-.*\.json$/i.test(n))
    .map((n) => `fixtures/${n}`)
    .sort();
}

function classifyWriteBatch(slotKey, manifestRow) {
  const key = toText(slotKey);
  if (manifestRow?.shouldRemainBlank || key === "footprint.openings") return BATCH.remainBlank;
  if (/^materials\.gallery\./i.test(key)) return BATCH.mediaAsset;
  if (/^economics\./i.test(key)) return BATCH.sourceEvidence;
  if (/^loyalty\.kpi\./i.test(key)) return BATCH.sourceEvidence;
  if (key === "loyalty.proof") return BATCH.sourceEvidence;
  if (key === "materials.caseStudy") return BATCH.sourceEvidence;
  if (/^overview\.proof\./i.test(key) || key === "overview.proof_operator") return BATCH.uncertain;
  if (key === "footprint.momentum") return BATCH.uncertain;
  if (key === "standards.last_reviewed" || key === "standards.requirement") return BATCH.uncertain;
  if (manifestRow?.needsSourceBackedEvidence && !/^commercial\./i.test(key) && !/^operations\.flexibility\./i.test(key)) {
    return BATCH.sourceEvidence;
  }
  return BATCH.safeEditorial;
}

function evidenceNeededForSlot(slotKey) {
  const key = toText(slotKey);
  if (/^economics\./i.test(key)) {
    return "Tribute Portfolio FDD (2026) stewarded facts: fee categories, term/renewal themes, opening cost rhythm, negotiability posture—human review required; no dollar claims without approved extracted facts.";
  }
  if (/^loyalty\.kpi\./i.test(key)) {
    return "Bonvoy public page + approved loyalty facts: member scale, participating hotels, markets, typical loyalty mix—numeric values only from approved PI facts.";
  }
  if (key === "loyalty.proof") {
    return "Source-backed loyalty proof headlines (program scale, owner benefit themes)—no invented performance claims.";
  }
  if (key === "materials.caseStudy") {
    return "Approved property-level case study with verifiable asset, location, situation, brand-fit narrative, and optional external URL; image attachment if used.";
  }
  if (key === "footprint.momentum") {
    return "Source-backed opening/PR items with date, property name, and Marriott or hotel URL—do not use JS-shell newsroom without rendered capture.";
  }
  return "";
}

function mediaAssetNeededForSlot(slotKey) {
  const key = toText(slotKey);
  if (/^materials\.gallery\./i.test(key)) {
    return `Approved gallery image for ${key} (lobby, guest room, F&B, or lifestyle)—attach only after asset governance approval.`;
  }
  if (/^overview\.scenario\.[1-3]$/i.test(key)) {
    return `Scenario card hero image for ${key} if visual parity required—body may exist without image.`;
  }
  if (key === "materials.caseStudy") {
    return "Optional case-study thumbnail image after narrative is source-approved.";
  }
  return "";
}

function contentTypeForSlot(slotKey) {
  const key = toText(slotKey);
  if (/^operations\.flexibility\./i.test(key)) return "canonical_flex_level_label";
  if (/^commercial\.lever\./i.test(key)) return "commercial_lever_narrative_plus_impact";
  if (/^commercial\.kpi\./i.test(key)) return "kpi_label_and_directional_value";
  if (/^commercial\.demand$/i.test(key)) return "demand_scenario_card";
  if (/^loyalty\.elite$/i.test(key)) return "tier_card_rows";
  if (/^loyalty\.(earn|redeem)$/i.test(key)) return "bullet_list";
  if (/^footprint\.region\./i.test(key)) return "region_status_card";
  if (/^footprint\.(growth_themes|growth_fit|editorial_bullets)$/i.test(key)) return "bullet_or_tag_list";
  if (/^valueOwners\.lifecycle\./i.test(key)) return "lifecycle_phase_line";
  if (/^economics\./i.test(key)) return "economics_or_legal_narrative";
  if (/^insight\.similar$/i.test(key)) return "peer_brand_card_row";
  return "editorial_paragraph_or_short_line";
}

function sourceBasisForSlot(slotKey, tributeBrand) {
  const key = toText(slotKey);
  if (/^economics\./i.test(key)) return "Tribute FDD + Fee Structure/Deal Terms (stewarded; held internal until approved)";
  if (/^loyalty\./i.test(key)) return "Marriott Bonvoy page + Loyalty & Commercial setup fields";
  if (/^commercial\./i.test(key)) return "Completed-brand commercial pattern + Tribute positioning (AI-drafted)";
  if (/^footprint\./i.test(key)) return "Region Offered + footprint setup + completed-brand editorial pattern";
  if (/^operations\./i.test(key)) return "Operational Support/Standards themes + completed-brand operating model pattern";
  if (key === "hero.benefit_zones") return "Key differentiators + footprint conversion themes";
  if (key === "hero.operator_compat") return "Operational support + operator-compat pattern";
  if (key === "insight.similar") return "Illustrative peer brands (non-equivalency)";
  return `Marriott consumer/development themes + ${toText(tributeBrand?.name) || "Tribute"} profile context`;
}

const COMMERCIAL_LEVER_COPY = {
  distribution:
    "Marriott global distribution and reservation infrastructure extends reach beyond independent channels.\n\nProject impact: Improves visibility for conversion and leisure assets where independent booking mix limits comp-set RevPAR.",
  revenue_management:
    "Revenue management support aligned with collection positioning and market tier.\n\nProject impact: Helps owners underwrite ADR and channel mix after affiliation—not a guarantee of uplift.",
  digital_marketing:
    "Digital demand generation and brand campaign participation through Marriott commercial stack.\n\nProject impact: Supports ramp and stabilization when local awareness is thin post-conversion.",
  corporate_group:
    "Corporate and group sales access through Marriott commercial organization where program participation applies.\n\nProject impact: Relevant for urban and blended-demand assets; confirm program scope for your market.",
  leisure_destination:
    "Leisure and resort demand programs suited to independent character properties.\n\nProject impact: Supports Tribute's lifestyle and resort use cases when seasonality and experience investment are already planned.",
  international:
    "International guest sourcing through Bonvoy and Marriott network where properties participate.\n\nProject impact: Useful for gateway and resort markets with inbound demand; model contribution net of fees.",
  sales_catering:
    "Group and catering sales support where on-property meeting space exists.\n\nProject impact: Material for full-service conversions with meeting inventory—not limited-service prototypes.",
  reputation_qa:
    "Reputation and quality programs supporting guest review performance.\n\nProject impact: Helps protect ADR during ramp; operating discipline still owner-led.",
  data_analytics:
    "Commercial reporting and benchmarking access through brand systems.\n\nProject impact: Improves underwriting discipline versus purely independent reporting—confirm data access terms.",
};

function buildBatch1Draft(slotKey, tributeBrand, refExample) {
  const key = toText(slotKey);
  const regionText =
    Array.isArray(tributeBrand?.regionOffered) && tributeBrand.regionOffered.length
      ? tributeBrand.regionOffered.join(", ")
      : "North America, Caribbean & Latin America, Europe, Middle East & Africa, Asia Pacific";

  if (key === "commercial.intro") {
    return {
      title: "",
      body: "Tribute Portfolio helps owners lift project visibility through Marriott distribution, Bonvoy participation, and collection positioning—while preserving the hotel's independent identity, design narrative, and local programming.",
      url: "",
      imageRequirement: "",
    };
  }
  if (key === "commercial.differentiator") {
    return {
      title: "",
      body: "Soft-collection affiliation that keeps property individuality while adding Marriott commercial reach and Bonvoy guest demand.",
      url: "",
      imageRequirement: "",
    };
  }
  if (key === "commercial.theme") {
    return {
      title: "",
      body: "Conversion and repositioning of independent, boutique, and lifestyle assets\nResort and leisure destinations with experience-led ADR\nUrban and blended-demand markets with distinctive design narrative",
      url: "",
      imageRequirement: "",
    };
  }
  if (key.startsWith("commercial.lever.")) {
    const lever = key.replace("commercial.lever.", "");
    const body = COMMERCIAL_LEVER_COPY[lever] || `Commercial lever narrative for ${lever}.\n\nProject impact: Owner underwriting lens—confirm with Marriott development materials.`;
    const title = lever
      .split("_")
      .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
      .join(" ");
    return { title, body, url: "", imageRequirement: "" };
  }
  if (key.startsWith("commercial.kpi.")) {
    const labels = {
      channels: ["Distribution channels", "Global + digital + GDS paths"],
      campaigns: ["Campaign rhythm", "Brand-led demand programs"],
      b2b: ["B2B programs", "Corporate & group where active"],
      lens: ["Owner lens", "Model net contribution after fees and loyalty costs"],
    };
    const suffix = key.replace("commercial.kpi.", "");
    const pair = labels[suffix] || ["KPI", "Directional owner-facing label"];
    return { title: pair[0], body: pair[1], url: "", imageRequirement: "" };
  }
  if (key === "commercial.demand") {
    return {
      title: "Resort & leisure conversion",
      body: "Moderate–strong",
      url: "",
      imageRequirement: "",
    };
  }
  if (key === "overview.why_value") {
    return {
      title: "",
      body: "Preserves independent identity while adding Bonvoy and Marriott distribution\nSuited to conversion of boutique, lifestyle, and resort assets\nSupports premium positioning when local ADR supports operating complexity\nOwner underwriting should model fees, PIP, and loyalty economics explicitly",
      url: "",
      imageRequirement: "",
    };
  }
  if (key === "overview.owner_experience") {
    return {
      title: "",
      body: "Owners retain design and local programming latitude within collection standards—affiliation adds commercial systems, QA rhythm, and Bonvoy participation rather than a prototype-led reflag.",
      url: "",
      imageRequirement: "",
    };
  }
  if (key === "overview.differentiators.identity") {
    return {
      title: "",
      body: "Independent character and local sense of place\nDesign-forward guest experience\nBoutique and lifestyle operating posture",
      url: "",
      imageRequirement: "",
    };
  }
  if (key === "overview.differentiators.commercial") {
    return {
      title: "",
      body: "Marriott Bonvoy participation\nGlobal distribution and sales support\nCollection positioning without losing property story",
      url: "",
      imageRequirement: "",
    };
  }
  if (key === "overview.proof_operator") {
    return {
      title: "",
      body: "Third-party management compatibility depends on full-service or resort operating capability, design narrative discipline, and experience with soft-brand conversion QA.",
      url: "",
      imageRequirement: "",
    };
  }
  if (key.startsWith("overview.bestAt.")) {
    const idx = key.split(".").pop();
    const cards = {
      1: { title: "Conversion & Repositioning", body: "Independent and boutique assets where identity is the product." },
      2: { title: "Resort & Leisure", body: "Experience-led destinations with ADR supported by design and F&B investment." },
      3: { title: "Urban Character", body: "Distinctive urban hotels where local narrative supports premium positioning." },
    };
    const card = cards[idx] || { title: "Strategic fit", body: "Owner-facing use case—confirm market tier." };
    return { title: card.title, body: card.body, url: "", imageRequirement: "" };
  }
  if (key === "hero.benefit_zones") {
    return {
      title: "",
      body: "Conversion & repositioning, resort & leisure destinations, urban character markets",
      url: "",
      imageRequirement: "",
    };
  }
  if (key === "hero.operator_compat") {
    return {
      title: "",
      body: "Full-service and resort operators with soft-brand conversion and design narrative experience.",
      url: "",
      imageRequirement: "",
    };
  }
  if (key === "valueOwners.watchouts") {
    return {
      title: "",
      body: "PIP scope and timing can dominate conversion economics\nFull-service complexity may exceed limited-service underwriting\nLoyalty and franchise fees require explicit net contribution modeling\nHeritage and design constraints can extend timeline and disrupt ramp-year NOI",
      url: "",
      imageRequirement: "",
    };
  }
  if (key.startsWith("valueOwners.lifecycle.")) {
    const phases = {
      1: { title: "Evaluate", body: "Confirm collection fit, market tier, and conversion scope." },
      2: { title: "Design narrative", body: "Align identity preservation with standards and PIP." },
      3: { title: "Affiliation", body: "Plan conversion timeline, disruption, and ramp assumptions." },
      4: { title: "Opening QA", body: "Standards compliance and operating readiness." },
      5: { title: "Stabilize", body: "Model channel mix and Bonvoy contribution versus comp set." },
      6: { title: "Hold / exit", body: "Understand re-licensing and change-of-control paths." },
    };
    const idx = key.split(".").pop();
    const phase = phases[idx] || { title: "Phase", body: "Lifecycle detail." };
    return { title: phase.title, body: phase.body, url: "", imageRequirement: "" };
  }
  if (key === "valueOwners.scenarios") {
    return {
      title: "",
      body: "Resort repositioning with experience-led ADR\nUrban boutique conversion with design narrative\nLeisure destination affiliation preserving local programming",
      url: "",
      imageRequirement: "",
    };
  }
  if (key.startsWith("operations.flexibility.")) {
    const levels = {
      design: "High",
      conversion: "Very high",
      localization: "High",
      operational_rigidity: "Moderate",
      pip: "Moderate",
      prototype: "Low",
    };
    const suffix = key.replace("operations.flexibility.", "");
    return { title: "", body: levels[suffix] || "Moderate", url: "", imageRequirement: "" };
  }
  if (key === "operations.standards_philosophy") {
    return {
      title: "",
      body: "Collection standards preserve guest-quality consistency while allowing property-specific design, F&B, and local programming within Marriott QA and brand guidelines.",
      url: "",
      imageRequirement: "",
    };
  }
  if (key === "operations.operator_compat.summary") {
    return {
      title: "",
      body: "Best with operators experienced in full-service or resort operations, soft-brand conversions, and design-forward repositioning—not limited-service prototype operators.",
      url: "",
      imageRequirement: "",
    };
  }
  if (key === "operations.operator_compat.fit") {
    return {
      title: "",
      body: "Strong fit when the asset already delivers distinctive experience and can absorb standards, QA, and Bonvoy program participation.",
      url: "",
      imageRequirement: "",
    };
  }
  if (key === "operations.operator_compat.tags") {
    return {
      title: "",
      body: "Full-service\nResort\nDesign-forward\nConversion-experienced\nThird-party management",
      url: "",
      imageRequirement: "",
    };
  }
  if (key.startsWith("operations.model.") || key.startsWith("operations.compliance.")) {
    const label = key.split(".").pop().replace(/_/g, " ");
    const refHint = refExample ? short(refExample, 120) : "";
    return {
      title: "",
      body: refHint || `Owner-facing ${label} summary for Tribute Portfolio—align with Operational Support and Brand Standards themes; pending founder review.`,
      url: "",
      imageRequirement: "",
    };
  }
  if (key.startsWith("loyalty.") && !key.startsWith("loyalty.kpi.")) {
    if (key === "loyalty.hero_title") {
      return { title: "", body: "Marriott Bonvoy — Loyalty at a Glance", url: "", imageRequirement: "" };
    }
    if (key === "loyalty.ecosystem") {
      return {
        title: "",
        body: "Bonvoy connects independent-character Tribute stays to Marriott's global loyalty ecosystem—supporting repeat and cross-stay demand without erasing property individuality.",
        url: "",
        imageRequirement: "",
      };
    }
    if (key === "loyalty.owner_lens") {
      return {
        title: "",
        body: "Model loyalty contribution net of program fees and channel mix; treat Bonvoy as demand support—not a substitute for local product investment.",
        url: "",
        imageRequirement: "",
      };
    }
    if (key === "loyalty.earn" || key === "loyalty.redeem") {
      return {
        title: "",
        body: "Illustrative mechanics only—replace with source-approved Bonvoy program bullets after stewardship review.",
        url: "",
        imageRequirement: "",
      };
    }
    if (key.startsWith("loyalty.implications.")) {
      const area = key.split(".").pop();
      return {
        title: "",
        body: `Owner implication (${area}): plan systems, staffing, and P&L treatment for Bonvoy participation—confirm with approved loyalty materials.`,
        url: "",
        imageRequirement: "",
      };
    }
    if (key === "loyalty.elite") {
      return { title: "Elite tiers", body: "Program tier structure—source-backed rows required.", url: "", imageRequirement: "" };
    }
  }
  if (key.startsWith("footprint.")) {
    if (key === "footprint.editorial") {
      return {
        title: "",
        body: `Tribute Portfolio's footprint reflects independent boutique, lifestyle, and resort properties across ${regionText}—evaluate fit on conversion scope, market tier, and operating complexity rather than prototype density.`,
        url: "",
        imageRequirement: "",
      };
    }
    if (key === "footprint.editorial_bullets") {
      return {
        title: "",
        body: "Independent and boutique character properties\nResort and leisure destinations\nConversion-friendly urban and blended-demand markets",
        url: "",
        imageRequirement: "",
      };
    }
    if (key === "footprint.growth_editorial") {
      return {
        title: "",
        body: "Growth prioritizes markets where independent assets can sustain design, F&B, and service investment through affiliation—not standardized limited-service rollout.",
        url: "",
        imageRequirement: "",
      };
    }
    if (key === "footprint.growth_themes") {
      return { title: "", body: "Conversion\nResort & leisure\nUrban character\nExperience-led repositioning", url: "", imageRequirement: "" };
    }
    if (key === "footprint.growth_fit") {
      return {
        title: "",
        body: "Boutique conversions with established ADR\nResort assets with experience investment plan\nUrban independents with design narrative",
        url: "",
        imageRequirement: "",
      };
    }
    if (key === "footprint.momentum_label") {
      return { title: "", body: "Recent momentum — source-backed openings when available", url: "", imageRequirement: "" };
    }
    if (key.startsWith("footprint.region.")) {
      const regionNames = { am: "Americas", cala: "CALA", eu: "Europe", mea: "MEA", apac: "APAC" };
      const code = key.split(".").pop();
      const name = regionNames[code] || code.toUpperCase();
      return {
        title: name,
        body: `Selective presence\n\nIndependent and resort-oriented properties in ${name} where market tier supports collection operating complexity.`,
        url: "",
        imageRequirement: "",
      };
    }
    if (key === "footprint.geo.summary") {
      return {
        title: "",
        body: `Geographic presence across ${regionText} with emphasis on conversion-friendly boutique, lifestyle, and resort markets.`,
        url: "",
        imageRequirement: "",
      };
    }
    if (key === "footprint.portfolio_mix") {
      return { title: "Urban", body: "Moderate", url: "", imageRequirement: "" };
    }
    if (key === "footprint.growth.narrative") {
      return {
        title: "",
        body: "Affiliation growth follows asset quality and conversion readiness—not uniform prototype expansion.",
        url: "",
        imageRequirement: "",
      };
    }
  }
  if (key === "insight.similar") {
    return {
      title: "Curio Collection by Hilton",
      body: "(Hilton · soft collection · conversion-oriented)",
      url: "",
      imageRequirement: "",
    };
  }
  if (key.startsWith("standards.")) {
    if (key === "standards.conversion") {
      return {
        title: "",
        body: "Conversion PIP scope, heritage constraints, and timeline should be modeled before affiliation—standards flexibility preserves identity within collection QA.",
        url: "",
        imageRequirement: "",
      };
    }
    if (key === "standards.deal_inputs") {
      return {
        title: "",
        body: "Market tier validation\nPIP scope and timing\nLoyalty and fee economics\nOperator capability\nHeritage/design constraints",
        url: "",
        imageRequirement: "",
      };
    }
  }
  if (key === "overview.scenarios") {
    return {
      title: "",
      body: "Resort & leisure repositioning\nUrban boutique conversion\nExperience-led independent affiliation",
      url: "",
      imageRequirement: "",
    };
  }

  return {
    title: "",
    body: refExample
      ? `Adapt from reference: ${short(refExample, 180)}`
      : "Editorial placeholder—pending founder review against completed-brand pattern.",
    url: "",
    imageRequirement: "",
  };
}

function scoreFromPresentKeys(presentRequiredKeys, totalRequiredKeys, optionalPresent = 0, optionalTotal = 1) {
  const reqTotal = totalRequiredKeys || 1;
  const reqHas = presentRequiredKeys.length;
  const optTotal = optionalTotal || 1;
  const base = (reqHas / reqTotal) * 80 + (optionalPresent / optTotal) * 20;
  return Math.max(0, Math.round(base));
}

export async function buildBrandExplorerSlotCompletionPlannerReport(options = {}) {
  const brandIdOrName = normalizeBrandInput(options.brandIdOrName);
  const manifest = await buildBrandExplorerSlotStandardManifestReport({ brandIdOrName });
  const tribute = await fetchBrand(brandIdOrName);
  if (!tribute) throw new Error(`Unable to read target brand: ${brandIdOrName}`);

  const manifestByKey = new Map(manifest.slotStandardManifestRows.map((r) => [r.slotKey, r]));
  const missingKeys = manifest.requiredSlotsTributeMissing.slice();
  const alreadyHas = new Set(manifest.requiredSlotsTributeAlreadyHas);

  const slotPlans = [];
  for (const slotKey of missingKeys) {
    const manifestRow = manifestByKey.get(slotKey) || {};
    const writeBatch = classifyWriteBatch(slotKey, manifestRow);
    const refExample = (manifestRow.referenceExamples || [])[0] || "";
    const tributeShouldHave = !manifestRow.shouldRemainBlank && writeBatch !== BATCH.remainBlank;
    const draft =
      writeBatch === BATCH.safeEditorial ? buildBatch1Draft(slotKey, tribute, refExample) : null;

    slotPlans.push({
      slotKey,
      tab: tabFromSlot(slotKey),
      section: sectionFromSlot(slotKey),
      completedBrandExamples: manifestRow.referenceExamples || [],
      referenceBrandsUsing: manifestRow.brandsUsing || [],
      visibleInUi: manifestRow.visibleInUi !== false,
      tributeShouldHaveIt: tributeShouldHave,
      contentTypeNeeded: contentTypeForSlot(slotKey),
      sourceBasisAvailable: sourceBasisForSlot(slotKey, tribute),
      proposedTitle: draft?.title || "",
      proposedBody: draft?.body || "",
      proposedUrl: draft?.url || "",
      proposedImageRequirement: draft?.imageRequirement || mediaAssetNeededForSlot(slotKey),
      reviewStatus: draft ? REVIEW_STATUS : writeBatch === BATCH.sourceEvidence ? "hold pending source evidence" : "no draft — batch policy",
      writeReadinessBatch: writeBatch,
      futureWriterShouldCreateOrUpdate:
        writeBatch === BATCH.safeEditorial || writeBatch === BATCH.sourceEvidence || writeBatch === BATCH.uncertain,
      v18Classification: manifestRow.classification || "",
      v19Reclassified: writeBatch !== BATCH.safeEditorial && manifestRow.aiDraftHumanReviewAcceptable,
      evidenceNeeded: evidenceNeededForSlot(slotKey),
      mediaAssetNeeded: mediaAssetNeededForSlot(slotKey),
      noCopyShouldBeDrafted: writeBatch !== BATCH.safeEditorial,
    });
  }

  const byTab = {};
  for (const tab of TAB_ORDER) byTab[tab] = [];
  for (const row of slotPlans) {
    if (!byTab[row.tab]) byTab[row.tab] = [];
    byTab[row.tab].push(row.slotKey);
  }

  const batch1 = slotPlans.filter((r) => r.writeReadinessBatch === BATCH.safeEditorial);
  const batch2 = slotPlans.filter((r) => r.writeReadinessBatch === BATCH.sourceEvidence);
  const batch3 = slotPlans.filter((r) => r.writeReadinessBatch === BATCH.mediaAsset);
  const batch4 = slotPlans.filter((r) => r.writeReadinessBatch === BATCH.remainBlank);
  const batch5 = slotPlans.filter((r) => r.writeReadinessBatch === BATCH.uncertain);

  const totalRequired = manifest.requiredSlotsForCompletedBrandParity.length;
  const afterBatch1Present = new Set([
    ...alreadyHas,
    ...batch1.map((r) => r.slotKey),
  ]);
  const afterEvidencePresent = new Set([
    ...afterBatch1Present,
    ...batch2.map((r) => r.slotKey),
    ...batch5.filter((r) => r.tributeShouldHaveIt).map((r) => r.slotKey),
  ]);

  const scoreAfterBatch1 = scoreFromPresentKeys([...afterBatch1Present], totalRequired);
  const scoreAfterEvidenceSupported = scoreFromPresentKeys([...afterEvidencePresent], totalRequired);
  const stillMissingAfterBatch1 = missingKeys.filter((k) => !afterBatch1Present.has(k));
  const comparableAfterBatch1 =
    scoreAfterBatch1 >= 85 && stillMissingAfterBatch1.length === 0;

  const v20FirstSlots = batch1.map((r) => r.slotKey).sort((a, b) => a.localeCompare(b));

  return {
    writerVersion: WRITER_VERSION,
    generatedAt: new Date().toISOString(),
    mode: "dry-run",
    airtableModified: false,
    imagesUntouched: true,
    copyUntouched: true,
    companyValidatedUntouched: true,
    companyValidationDateUntouched: true,
    marriottValidationImplied: false,
    filesRead: [
      "AGENTS.md",
      "reports/brand-explorer-slot-standard-manifest.json",
      "reports/brand-explorer-slot-standard-manifest.md",
      "lib/partner-intelligence/brand-explorer-slot-standard-manifest.js",
      "reports/brand-explorer-presentation-slot-coverage-audit.md",
      "reports/brand-explorer-presentation-slot-coverage-audit.json",
      "reports/brand-explorer-display-parity-audit.md",
      "reports/brand-explorer-display-content-completion-writer.md",
      "reports/tribute-portfolio-package-pipeline.md",
      "reports/brand-explorer-visual-qa-verification.md",
      "api/brand-library.js",
      "api/lib/partner-intelligence-field-map.js",
      "api/lib/partner-intelligence-explorer-field-registry.js",
      "public/js/brand-explorer-atelier-from-api.js",
      "public/js/brand-explorer-gold-detail.js",
      "docs/brand-explorer-presentation-slots.md",
      "docs/data-intelligence/BRAND_PROFILE_DATA_MODEL.md",
      ...listFixtureFiles(),
    ],
    filesChanged: [
      "lib/partner-intelligence/brand-explorer-slot-completion-planner.js",
      "scripts/brand-explorer-slot-completion-planner.mjs",
      "docs/data-intelligence/brand-explorer-slot-completion-planner-v19.md",
      "reports/brand-explorer-slot-completion-planner.md",
      "reports/brand-explorer-slot-completion-planner.json",
      "package.json",
    ],
    v19PlannerExists: true,
    brand: manifest.brand,
    v18Baseline: {
      totalMissingRequiredCandidateSlots: missingKeys.length,
      requiredSlotsTributeAlreadyHas: manifest.requiredSlotsTributeAlreadyHas,
      revisedRealisticTributeCompletionScore: manifest.revisedRealisticTributeCompletionScore,
      tributeCompletedBrandComparableUnderManifest: manifest.tributeCompletedBrandComparableUnderManifest,
    },
    missingSlotsGroupedByTab: byTab,
    batch1SafeEditorialHumanReview: batch1.map((r) => r.slotKey),
    batch2SourceEvidenceRequired: batch2.map((r) => r.slotKey),
    batch3MediaOrAssetRequired: batch3.map((r) => r.slotKey),
    batch4ShouldRemainBlank: batch4.map((r) => r.slotKey),
    batch5ClassificationUncertain: batch5.map((r) => r.slotKey),
    batch1ProposedCopy: batch1.map((r) => ({
      slotKey: r.slotKey,
      tab: r.tab,
      title: r.proposedTitle,
      body: r.proposedBody,
      reviewStatus: r.reviewStatus,
    })),
    slotsWhereNoCopyDrafted: slotPlans.filter((r) => r.noCopyShouldBeDrafted).map((r) => r.slotKey),
    evidenceNeededBySlot: batch2
      .concat(batch5)
      .filter((r) => hasVal(r.evidenceNeeded))
      .map((r) => ({ slotKey: r.slotKey, evidenceNeeded: r.evidenceNeeded })),
    mediaAssetsNeededBySlot: slotPlans
      .filter((r) => hasVal(r.mediaAssetNeeded) || hasVal(r.proposedImageRequirement))
      .map((r) => ({
        slotKey: r.slotKey,
        mediaAssetNeeded: r.mediaAssetNeeded || r.proposedImageRequirement,
      })),
    revisedScoreIfBatch1Applied: scoreAfterBatch1,
    revisedScoreIfEvidenceSupportedCompleted: scoreAfterEvidenceSupported,
    tributeCompletedBrandComparableAfterBatch1: comparableAfterBatch1,
    v20WriterShouldBeBuilt: true,
    v20WriterBatchPlan: {
      batch1_editorial_human_review: batch1.map((r) => r.slotKey),
      batch2_after_source_stewardship: batch2.map((r) => r.slotKey),
      batch3_after_asset_approval: batch3.map((r) => r.slotKey),
      batch4_never_without_evidence: batch4.map((r) => r.slotKey),
      batch5_manual_classification_review: batch5.map((r) => r.slotKey),
    },
    v20CreateOrUpdateSlotsFirst: v20FirstSlots,
    slotCompletionPlans: slotPlans,
    exactNextCommand: "npm run brand-explorer-slot-completion-planner -- --brand tribute-portfolio --dry-run",
  };
}

export function buildBrandExplorerSlotCompletionPlannerMarkdown(report) {
  const lines = [];
  lines.push("# Brand Explorer Slot Completion Planner v19");
  lines.push("");
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.mode}** · Airtable modified: **${report.airtableModified ? "yes" : "no"}**`);
  lines.push(`Brand: ${report.brand.name} \`${report.brand.recordId}\``);
  lines.push("");
  lines.push("## v18 baseline");
  lines.push(`- Missing required/candidate slots: **${report.v18Baseline.totalMissingRequiredCandidateSlots}**`);
  lines.push(`- v18 score: **${report.v18Baseline.revisedRealisticTributeCompletionScore}/100**`);
  lines.push(`- Already has: **${report.v18Baseline.requiredSlotsTributeAlreadyHas.length}** required slots`);
  lines.push("");
  lines.push("## Missing slots by tab");
  TAB_ORDER.forEach((tab) => {
    const slots = report.missingSlotsGroupedByTab[tab] || [];
    if (!slots.length) return;
    lines.push(`### ${tab} (${slots.length})`);
    slots.forEach((s) => lines.push(`- ${s}`));
    lines.push("");
  });
  lines.push("## Write-readiness batches");
  lines.push(`- Batch 1 safe editorial: **${report.batch1SafeEditorialHumanReview.length}**`);
  lines.push(`- Batch 2 source evidence: **${report.batch2SourceEvidenceRequired.length}**`);
  lines.push(`- Batch 3 media/asset: **${report.batch3MediaOrAssetRequired.length}**`);
  lines.push(`- Batch 4 remain blank: **${report.batch4ShouldRemainBlank.length}**`);
  lines.push(`- Batch 5 uncertain: **${report.batch5ClassificationUncertain.length}**`);
  lines.push("");
  lines.push("## Score projections");
  lines.push(`- If Batch 1 applied: **${report.revisedScoreIfBatch1Applied}/100**`);
  lines.push(`- If evidence-supported slots later completed: **${report.revisedScoreIfEvidenceSupportedCompleted}/100**`);
  lines.push(
    `- Completed-brand comparable after Batch 1 only: **${report.tributeCompletedBrandComparableAfterBatch1 ? "yes" : "no"}**`
  );
  lines.push(`- v20 writer should be built: **${report.v20WriterShouldBeBuilt ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Batch 1 proposed copy (sample)");
  report.batch1ProposedCopy.slice(0, 25).forEach((row) => {
    lines.push(`### ${row.slotKey}`);
    if (row.title) lines.push(`- Title: ${short(row.title, 120)}`);
    lines.push(`- Body: ${short(row.body, 280)}`);
    lines.push(`- Review: ${row.reviewStatus}`);
    lines.push("");
  });
  if (report.batch1ProposedCopy.length > 25) {
    lines.push(`_…${report.batch1ProposedCopy.length - 25} additional Batch 1 drafts in JSON._`);
    lines.push("");
  }
  lines.push("## v20 first-wave slots");
  report.v20CreateOrUpdateSlotsFirst.forEach((s) => lines.push(`- ${s}`));
  lines.push("");
  lines.push("## Guardrails");
  lines.push(`- No Airtable writes: **${report.airtableModified ? "no" : "yes"}**`);
  lines.push(`- Company Validated untouched: **${report.companyValidatedUntouched ? "yes" : "no"}**`);
  lines.push(`- Marriott validation implied: **${report.marriottValidationImplied ? "yes" : "no"}**`);
  lines.push("");
  lines.push("## Next command");
  lines.push("");
  lines.push("```bash");
  lines.push(report.exactNextCommand);
  lines.push("```");
  lines.push("");
  return lines.join("\n");
}
