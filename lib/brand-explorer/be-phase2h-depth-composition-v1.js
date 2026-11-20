/**
 * Phase 2H — evidence→field composition for substantive depth.
 * Uses evidence packs + Basics claims. Never copies production Presentation bodies.
 * Baseline inventory is a coverage checklist only.
 */
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { repoRoot } from "./canonical/product-shell.js";
import {
  filterNonLogoImageryEvidence,
  captionFromPropertyImageUrl,
  isBrandLogoGalleryAsset,
  isGenericGalleryCaption,
} from "./be-permanent-six-image-gallery-standard-v1.js";

export const BE_PHASE2H_DEPTH_COMPOSITION_V1 = "BE_PHASE2H_DEPTH_COMPOSITION_V1";

const REGION_LABELS = {
  am: "Americas",
  cala: "Caribbean & Latin America",
  eu: "Europe",
  mea: "Middle East & Africa",
  apac: "Asia Pacific",
};

function nz(s) {
  return s == null ? "" : String(s).trim();
}

function byId(pack) {
  const m = new Map();
  for (const e of pack?.evidence || []) {
    if (e?.id) m.set(e.id, e);
  }
  return m;
}

function scrubCustomer(text) {
  return nz(text)
    .replace(/do not invent[^.]*\.?/gi, "")
    .replace(/Evidence-grounded reconstruction[^.]*\.?/gi, "")
    .replace(/Focus this card on[^.]*\.?/gi, "")
    .replace(/RESEARCH_REQUIRED[^.]*\.?/gi, "")
    .replace(/diligence framing only\.?/gi, "")
    .replace(/\s+/g, " ")
    .replace(/\s+([,.])/g, "$1")
    .trim();
}

function claim(map, id) {
  return scrubCustomer(map.get(id)?.claim);
}

function splitParts(text) {
  return nz(text)
    .split(/[;|•\n]+|(?<=\.)\s+/)
    .map((s) => s.replace(/^[-–—]\s*/, "").trim())
    .filter((s) => s.length > 12);
}

/**
 * Inventory distinct claims in a body for coverage checklist (read-only).
 */
export function inventoryClaims(body) {
  return splitParts(body).map((text, i) => ({
    index: i + 1,
    text,
    wordCount: text.split(/\s+/).filter(Boolean).length,
  }));
}

/**
 * Compose multi-reason Why Value Is Strongest from evidence (bullet lines).
 */
export function composeWhyValue(pack, opts = {}) {
  const map = byId(pack);
  const name = opts.brandName || claim(map, "identity.brand_name") || "Brand";
  const parent = opts.parentCompany || claim(map, "identity.parent") || "parent";
  const loyalty = claim(map, "loyalty.program") || `${parent} loyalty`;
  const lines = [];

  const vp = claim(map, "basics.value_prop");
  if (vp) lines.push(vp);

  const diff = claim(map, "basics.differentiators");
  if (diff) {
    for (const p of splitParts(diff).slice(0, 3)) {
      if (!lines.some((l) => l.toLowerCase().includes(p.slice(0, 24).toLowerCase()))) lines.push(p);
    }
  }

  const honors = claim(map, "loyalty.honors_value");
  if (honors) lines.push(honors);

  const culinary = (pack.evidence || []).find((e) => /culinary|dining|local bold/i.test(e.claim || ""));
  if (culinary?.claim) lines.push(nz(culinary.claim));

  const pillars = claim(map, "basics.pillars");
  if (pillars) {
    lines.push(`${name} brand pillars (${pillars}) should show up in conversion design and guest narrative — underwrite to the brand standards document, not marketing alone.`);
  }

  const audience = claim(map, "basics.audience");
  if (audience) {
    lines.push(`Demand lenses span ${audience}; owners should match corridor demand to property character before shortlisting.`);
  }

  // Deduplicate near-identical lines
  const out = [];
  const seen = new Set();
  for (const line of lines) {
    const key = line.toLowerCase().replace(/[^a-z0-9]+/g, " ").replace(/\s+/g, " ").trim().slice(0, 80);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(line);
  }

  return {
    slotKey: "overview.why_value",
    title: "Why Value Is Strongest in These Scenarios",
    body: out.join("\n"),
    insightCount: out.length,
    evidenceIds: ["basics.value_prop", "basics.differentiators", "loyalty.honors_value", "basics.pillars", "basics.audience"].filter(
      (id) => map.has(id),
    ),
    researchGaps: out.length < 4 ? [{ reason: "why_value_needs_additional_owner_value_claims", have: out.length, want: 4 }] : [],
  };
}

/**
 * Distinct operating-model leaf bodies (no shared diligence stamp).
 */
export function composeOperatingModel(pack, opts = {}) {
  const map = byId(pack);
  const name = opts.brandName || claim(map, "identity.brand_name") || "Brand";
  const parent = opts.parentCompany || claim(map, "identity.parent") || "parent";
  const loyalty = claim(map, "loyalty.program") || `${parent} loyalty`;
  const model = claim(map, "basics.brand_model") || "Collection Brand";
  const service = claim(map, "basics.service_model") || "Full-Service";
  const standards = claim(map, "standards.diligence");

  const leaves = {
    "operations.model.primary_model": {
      title: "Primary Model",
      body: `${model} affiliation under ${parent}: ${name} properties keep local character while joining ${parent} commercial and ${loyalty} systems. Confirm current franchise/collection agreement class with counsel.`,
    },
    "operations.model.management_option": {
      title: "Management Option",
      body: `Third-party management is common when the operator can protect property character inside ${parent} systems. Owner-operated models need credible leadership and service discipline for ${service.toLowerCase()} collection assets.`,
    },
    "operations.model.typical_ownership": {
      title: "Typical Ownership",
      body: `Independent owners and investor groups seeking soft-brand / collection affiliation with ${parent} distribution rather than a hard-brand prototype.`,
    },
    "operations.model.brand_involvement": {
      title: "Brand Involvement",
      body: standards
        ? scrubCustomer(standards)
        : `Brand involvement centers on design review, brand standards, QA cadence, and commercial-system cutover — not day-to-day property management unless contracted separately.`,
    },
    "operations.model.systems_integration": {
      title: "Systems Integration",
      body: `Expect ${parent} CRS/brand.com distribution, ${loyalty} participation, and property-system cutover as part of affiliation. Request current tech stack and interface list in diligence.`,
    },
    "operations.model.pre_opening": {
      title: "Pre-Opening / Conversion",
      body: `Conversion path typically includes design narrative approval, PIP scope, and brand readiness before launch. Timeline and capital intensity are asset-specific — confirm in the deal file.`,
    },
    "operations.model.staffing_fnb": {
      title: "Staffing & F&B",
      body: `Collection / lifestyle positioning usually implies stronger F&B and design staffing intensity than limited-service brands. Underwrite labor and concept approval against the standards manual.`,
    },
    "operations.model.qa_reporting": {
      title: "QA & Reporting",
      body: `Brand QA cadence and owner reporting flow through ${parent} systems. Soft-brand flexibility does not remove inspection or reporting obligations — confirm frequency in current manuals.`,
    },
  };

  const philosophy = {
    slotKey: "operations.standards_philosophy",
    title: "Standards Philosophy",
    body: [
      `${name} balances distinctive property character with ${parent} consistency.`,
      `Underwrite to the brand standards document and design narrative — not marketing renderings alone.`,
      scrubCustomer(standards) ||
        `Confirm design review, FF&E, F&B concept approval, and QA expectations before conversion capital is locked.`,
    ]
      .filter(Boolean)
      .join("\n"),
  };

  return { leaves, philosophy, evidenceIds: ["basics.brand_model", "basics.service_model", "standards.diligence", "loyalty.program"] };
}

/**
 * Loyalty architecture with distinct owner lenses (no stamp reuse).
 */
export function composeLoyalty(pack, opts = {}) {
  const map = byId(pack);
  const name = opts.brandName || claim(map, "identity.brand_name") || "Brand";
  const parent = opts.parentCompany || claim(map, "identity.parent") || "parent";
  const program = claim(map, "loyalty.program") || `${name} participates in ${parent} loyalty.`;
  const value = claim(map, "loyalty.honors_value");
  const tiers = claim(map, "loyalty.honors_tiers");
  const earn = claim(map, "loyalty.earn_detail");
  const redeem = claim(map, "loyalty.redeem_detail");

  return {
    blocks: [
      { slotKey: "loyalty.program", title: "Loyalty Program", body: program },
      { slotKey: "loyalty.ecosystem", title: "Loyalty Ecosystem", body: value || `${name} taps ${parent} member demand and portfolio redemption liquidity — confirm contribution economics in diligence.` },
      {
        slotKey: "loyalty.owner_lens",
        title: "Owner Lens",
        body: [
          `Model loyalty as net room revenue after member discounts, elite benefits, and program chargebacks.`,
          value || `${name} guests receive ${parent} loyalty benefits while the property keeps independent character.`,
          `Request current contribution / reimbursement mechanics before underwriting RevPAR lift.`,
        ].join("\n"),
      },
      { slotKey: "loyalty.earn", title: "Earn", body: earn || `Guests earn points on qualifying stays — confirm current earn rules on the official program site.` },
      { slotKey: "loyalty.redeem", title: "Redeem", body: redeem || `Points redeem across the ${parent} portfolio — confirm current redeem rules before publish.` },
      { slotKey: "loyalty.elite", title: "Elite", body: tiers || `Elite tiers — confirm current tier names on the official program site.` },
      {
        slotKey: "loyalty.implications.pnl",
        title: "P&L Implications",
        body: `Underwrite member-rate mix, elite cost, and program fees as P&L line items — not as free demand. Use property comps with similar ${parent} soft-brand participation.`,
      },
      {
        slotKey: "loyalty.implications.ops",
        title: "Ops Implications",
        body: `Front-office and CRM processes must honor member recognition and elite benefits at a collection-quality standard without erasing local service culture.`,
      },
      {
        slotKey: "loyalty.implications.systems",
        title: "Systems Implications",
        body: `Loyalty participation requires PMS/CRS interfaces and correct rate/benefit configuration at cutover — treat as a critical path item in conversion planning.`,
      },
    ],
    researchGaps: !value || !tiers ? [{ reason: "loyalty_scale_metrics_need_dated_official_source" }] : [],
  };
}

/**
 * Geographic footprint — never stamp brand-wide HPC into regional slots.
 */
export function composeFootprint(pack, opts = {}) {
  const map = byId(pack);
  const name = opts.brandName || claim(map, "identity.brand_name") || "Brand";
  const offered = claim(map, "basics.region_offered");
  const open = claim(map, "metric.open_hotels");
  const regional = opts.regionalHpc || {};

  const blocks = [
    {
      slotKey: "footprint.geo_intro",
      title: "Geographic Footprint",
      body: offered
        ? `${name} Region Offered (Brand Basics): ${offered}. Brand-level HPC inventory is reported separately from regional cards.`
        : `${name} geographic posture should be confirmed on Brand Basics / development materials before customer publish.`,
    },
  ];

  if (open) {
    blocks.push({
      slotKey: "metric.open_hotels",
      title: "Brand-level HPC Open (observed)",
      body: `${open} — this is brand-wide observed inventory, not a regional count.`,
    });
  }

  const offeredLower = offered.toLowerCase();
  for (const [code, label] of Object.entries(REGION_LABELS)) {
    const slotKey = `footprint.region.${code}`;
    const reg = regional[code];
    const mentioned =
      (code === "am" && /north america|americas|united states|canada/i.test(offeredLower)) ||
      (code === "cala" && /caribbean|latin|cala|mexico|colombia/i.test(offeredLower)) ||
      (code === "eu" && /europe/i.test(offeredLower)) ||
      (code === "mea" && /middle east|africa|mea/i.test(offeredLower)) ||
      (code === "apac" && /asia|pacific|apac/i.test(offeredLower));

    if (reg && (reg.openHotels > 0 || reg.pipelineHotels > 0)) {
      blocks.push({
        slotKey,
        title: label,
        body: [
          "Established presence",
          `Observed HPC for ${label}: Open hotels=${reg.openHotels}; keys=${reg.openKeys || 0}; pipeline hotels=${reg.pipelineHotels || 0}. Regional filter applied — not a restatement of brand-wide totals.`,
        ].join("\n\n"),
      });
    } else if (mentioned) {
      blocks.push({
        slotKey,
        title: label,
        body: [
          "Directional presence",
          `${label} appears on Region Offered for ${name}. Regional HPC counts are not verified in this pack — do not infer brand-wide totals as ${label} inventory. Confirm corridor assignment in the deal file.`,
        ].join("\n\n"),
      });
    } else {
      blocks.push({
        slotKey,
        title: label,
        body: [
          "Coverage limited",
          `No verified regional HPC rollup for ${label} in this reconstruction. Brand-wide open/pipeline totals are withheld from this card to avoid false regional precision.`,
        ].join("\n\n"),
      });
    }
  }

  return { blocks, researchGaps: Object.keys(regional).length ? [] : [{ reason: "regional_hpc_rollup_unavailable" }] };
}

/**
 * Materials: keep verified images; case studies require named evidence — else incomplete.
 */
export function composeMaterials(pack, existingBlocks = [], opts = {}) {
  const map = byId(pack);
  const name = opts.brandName || claim(map, "identity.brand_name") || "Brand";
  const out = [];
  const gaps = [];

  // Permanent six-image hotel gallery: never inject brand logos or generic "Property imagery" captions.
  const images = filterNonLogoImageryEvidence(
    (pack.evidence || []).filter((e) => /^official\.image\.\d+$/.test(e.id) && /^https?:\/\//i.test(e.claim || "")),
  );
  for (let i = 0; i < 6; i++) {
    const slotKey = `materials.gallery.${i + 1}`;
    const prev = existingBlocks.find((b) => b.slotKey === slotKey);
    const prevUrl = prev?.imageUrl || null;
    const prevOk =
      prevUrl &&
      !isBrandLogoGalleryAsset(prevUrl, prev?.galleryMeta) &&
      prev?.title &&
      !isGenericGalleryCaption(prev.title, prev.body) &&
      !/^Gallery\s*\d+$/i.test(prev.title);
    const img = images[i];
    const imageUrl = prevOk ? prevUrl : img ? img.claim : prevUrl && !isBrandLogoGalleryAsset(prevUrl) ? prevUrl : null;
    if (imageUrl && !isBrandLogoGalleryAsset(imageUrl)) {
      const caption =
        (prevOk && prev.title) ||
        captionFromPropertyImageUrl(imageUrl) ||
        null;
      if (!caption) {
        gaps.push({ slotKey, reason: "gallery_caption_underivable_block_generic" });
        out.push({ slotKey, title: "Imagery unavailable", body: "", imageUrl: null });
      } else {
        out.push({
          slotKey,
          title: caption,
          body: caption,
          imageUrl,
        });
      }
    } else {
      gaps.push({ slotKey, reason: "gallery_image_url_missing_or_logo_rejected" });
      out.push({ slotKey, title: "Imagery unavailable", body: "", imageUrl: null });
    }
  }

  const namedOfficial = (pack.evidence || []).find((e) => /Hotel|Resort|Collection/.test(e.claim || "") && /official\.sentence/i.test(e.id));
  const existingCases = existingBlocks.filter((b) => b.slotKey === "materials.caseStudy" && nz(b.body).length > 40 && !/Source Library|diligence|Do not invent/i.test(b.body));
  if (existingCases.length) {
    for (const c of existingCases.slice(0, 5)) out.push({ ...c });
  } else if (namedOfficial) {
    out.push({
      slotKey: "materials.caseStudy",
      title: "Official portfolio examples",
      body: `${nz(namedOfficial.claim)}\n\nTreat named properties on official pages as reference examples only — confirm licensing, conversion type, and performance context before owner-facing case-study use.`,
    });
    gaps.push({ slotKey: "materials.caseStudy", reason: "full_case_study_dossier_not_in_evidence_pack" });
  } else {
    gaps.push({ slotKey: "materials.caseStudy", reason: "named_case_study_evidence_missing" });
  }

  const fileSlots = existingBlocks.filter((b) => /^materials\.file/.test(b.slotKey) && nz(b.body));
  for (const f of fileSlots) out.push({ ...f });

  if (!fileSlots.length) {
    gaps.push({ slotKey: "materials.file", reason: "verified_supporting_materials_missing" });
  }

  return { blocks: out, researchGaps: gaps, brandName: name };
}

/**
 * Apply Phase 2H depth composition onto a candidate overlay (mutates copy).
 */
export function enrichCandidateWithDepth(overlay, pack, opts = {}) {
  const blocks = (overlay.brandExplorer?.blocks || []).map((b) => ({ ...b }));
  const bySlot = new Map(blocks.map((b) => [b.slotKey, b]));
  const researchGaps = [];

  const upsert = (slotKey, title, body, extra = {}) => {
    const prev = bySlot.get(slotKey);
    const next = {
      ...(prev || { slotKey, sort: blocks.length + 1 }),
      slotKey,
      title: title || prev?.title || "",
      body: nz(body),
      ...extra,
    };
    if (prev) {
      Object.assign(prev, next);
    } else {
      blocks.push(next);
      bySlot.set(slotKey, next);
    }
  };

  const why = composeWhyValue(pack, opts);
  upsert(why.slotKey, why.title, why.body);
  researchGaps.push(...why.researchGaps.map((g) => ({ slotKey: why.slotKey, ...g })));

  const ops = composeOperatingModel(pack, opts);
  for (const [slotKey, leaf] of Object.entries(ops.leaves)) {
    upsert(slotKey, leaf.title, leaf.body);
  }
  upsert(ops.philosophy.slotKey, ops.philosophy.title, ops.philosophy.body);

  const loyalty = composeLoyalty(pack, opts);
  for (const b of loyalty.blocks) upsert(b.slotKey, b.title, b.body);
  researchGaps.push(...loyalty.researchGaps.map((g) => ({ slotKey: "loyalty.program", ...g })));

  const fp = composeFootprint(pack, opts);
  for (const b of fp.blocks) upsert(b.slotKey, b.title, b.body);
  researchGaps.push(...fp.researchGaps.map((g) => ({ slotKey: "footprint.geo", ...g })));

  // Strip leftover Phase 2E/2F brand-wide HPC stamps from non-metric footprint slots.
  for (const b of blocks) {
    const key = nz(b.slotKey);
    if (!/^footprint\.(growth|momentum|openings|portfolio)/.test(key)) continue;
    if (/HPC Open hotels matched=/i.test(b.body || "")) {
      researchGaps.push({ slotKey: key, reason: "brand_wide_hpc_stamp_removed_pending_theme_research" });
      b.body = "";
      b.title = scrubCustomer(b.title || key.split(".").pop().replace(/_/g, " "));
    }
  }
  if (!nz(bySlot.get("footprint.growth_themes")?.body)) {
    upsert(
      "footprint.growth_themes",
      "Growth themes",
      `${opts.brandName || "Brand"} growth thesis should be conversion- and corridor-specific. Brand-level HPC open/pipeline totals belong on brand metrics — not as growth-theme copy. Confirm priority corridors in development materials.`,
    );
  }

  const mats = composeMaterials(pack, blocks, opts);
  // Remove prior caseStudy stubs before re-adding
  for (let i = blocks.length - 1; i >= 0; i--) {
    if (blocks[i].slotKey === "materials.caseStudy" && /Source Library|diligence|Do not invent/i.test(blocks[i].body || "")) {
      blocks.splice(i, 1);
    }
  }
  for (const b of mats.blocks) {
    if (/^materials\.gallery\./.test(b.slotKey) || b.slotKey === "materials.caseStudy" || /^materials\.file/.test(b.slotKey)) {
      const prev = bySlot.get(b.slotKey);
      if (prev && /^materials\.gallery\./.test(b.slotKey)) {
        prev.title = b.title;
        prev.body = b.body;
        prev.imageUrl = b.imageUrl || prev.imageUrl;
      } else if (!prev) {
        blocks.push(b);
        bySlot.set(b.slotKey, b);
      } else if (b.slotKey === "materials.caseStudy") {
        prev.title = b.title;
        prev.body = b.body;
      }
    }
  }
  researchGaps.push(...mats.researchGaps);

  // Differentiators: split basics.differentiators into identity/commercial when thin
  const map = byId(pack);
  const diff = claim(map, "basics.differentiators");
  const parts = splitParts(diff);
  if (parts[0]) upsert("overview.differentiators.identity", "Identity", parts[0]);
  if (parts[1]) upsert("overview.differentiators.commercial", "Commercial", parts.slice(1).join(" "));
  else if (claim(map, "loyalty.honors_value")) {
    upsert("overview.differentiators.commercial", "Commercial", claim(map, "loyalty.honors_value"));
  }

  return {
    enriched: {
      ...overlay,
      candidateVersion: "phase2h-component-parity",
      phase2hContract: BE_PHASE2H_DEPTH_COMPOSITION_V1,
      enrichedAt: new Date().toISOString(),
      researchGaps,
      brandExplorer: { ...(overlay.brandExplorer || {}), blocks },
      writes: 0,
      usedPresentationBodiesAsInput: false,
      founderApprovalStatus: "NOT_FOUNDER_APPROVED",
    },
    researchGaps,
  };
}

export function loadEvidencePack(rel) {
  const p = join(repoRoot(), rel);
  if (!existsSync(p)) return null;
  return JSON.parse(readFileSync(p, "utf8"));
}
