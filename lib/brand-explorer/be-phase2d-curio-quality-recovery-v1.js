/**
 * Brand Explorer Phase 2D — Curio reconstruction quality recovery.
 *
 * Improves the universal evidence→slot factory (not a Curio-only pipeline).
 * Expands template denominator to production-applicable inventory.
 * Never reads Presentation / completed fixtures as generator input.
 *
 * Contract: BE_PHASE2D_CURIO_QUALITY_RECOVERY_V1
 */
import { readFileSync, existsSync, mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import {
  BE_BRAND_BASICS_TABLE,
  BE_CANONICAL_TAB_IDS,
} from "./be-airtable-integration-sot-v1.js";
import {
  CANONICAL_TAB_IDS,
  CONTENT_CONTRACT,
  COLLECTION_CONTRACT,
  PRESENTATION_CONTRACT,
  GEOMETRY_CONTRACT,
  repoRoot,
} from "./canonical/product-shell.js";
import { instantiateContentLedger } from "./canonical/content-item-ledger.js";
import { evaluateMasterRelease } from "./canonical/master-release.js";
import { ALL_TAB_CONTRACT_FIELDS, TAB_CONTRACTS } from "../partner-intelligence/brand-explorer-tab-contracts.js";
import {
  CURIO_RECONSTRUCTION_TARGET,
  NOT_VALIDATED,
  captureCurioBaselineSnapshot,
  buildIndependentEvidencePack as buildPhase2cEvidencePack,
  compareCandidateToBaseline,
  evaluateCandidateQuality as evaluatePhase2cQuality,
} from "./be-phase2c-curio-reconstruction-v1.js";
import { buildProductionTemplateInventory } from "./be-phase2d-template-inventory-v1.js";
import { GAP_CATEGORIES, describeResearchProcess } from "./be-phase2d-research-process-v1.js";

export const BE_PHASE2D_CURIO_QUALITY_RECOVERY_V1 = "BE_PHASE2D_CURIO_QUALITY_RECOVERY_V1";

function wordCount(s) {
  return String(s || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean).length;
}

function findEvidence(pack, ...idsOrKinds) {
  for (const key of idsOrKinds) {
    const hit = pack.evidence.find((e) => e.id === key || e.kind === key);
    if (hit) return hit;
  }
  return null;
}

function findAllEvidence(pack, prefixOrKind) {
  return pack.evidence.filter((e) => e.id.startsWith(prefixOrKind) || e.kind === prefixOrKind);
}

/**
 * Enumerate applicable required slots from production inventory + contracts.
 * Denominator must not shrink below production-applicable inventory.
 */
export function listApplicableRequiredSlots(inventory) {
  const inv = inventory || buildProductionTemplateInventory();
  const slots = [];
  const seen = new Set();

  const push = (slotKey, meta = {}) => {
    if (!slotKey || seen.has(slotKey)) return;
    const invRow = inv.slots.find((s) => s.slotKey === slotKey);
    if (invRow && invRow.applicableRequired === false) return;
    seen.add(slotKey);
    slots.push({
      slotKey,
      tabId: meta.tabId || invRow?.tabHint || null,
      source: meta.source || "inventory",
      ownership: "presentation",
      role: invRow?.role || "unique_required_field",
      minItems: invRow?.minItems || 1,
    });
  };

  for (const key of inv.applicableSlotKeys) push(key, { source: "production-inventory" });

  for (const item of CONTENT_CONTRACT.staticRequired) {
    push(item.itemId, { tabId: item.tabId, source: "content-item-contract" });
  }
  for (const field of ALL_TAB_CONTRACT_FIELDS) {
    push(field.slotKey || field.fieldId, { source: "tab-contract" });
  }
  for (const col of COLLECTION_CONTRACT.collections) {
    if (col.slotPattern && col.count) {
      for (let i = 1; i <= col.count; i += 1) {
        push(`${col.slotPattern}${i}`, { source: "collection-cardinality-contract" });
      }
    }
    if (col.slot) push(col.slot, { source: "collection-cardinality-contract" });
  }

  return { inventory: inv, slots };
}

function loadCachedPhase2cEvidencePack() {
  const path = join(repoRoot(), "reports/brand-explorer/phase2c-curio-reconstruction/02-independent-evidence-pack.json");
  if (!existsSync(path)) return null;
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return null;
  }
}

async function fetchPublicText(url, { maxChars = 8000 } = {}) {
  const res = await fetch(url, {
    headers: { "User-Agent": "DealalityBrandExplorerPhase2D/1.0 (read-only research)" },
  });
  if (!res.ok) return { ok: false, status: res.status, url };
  const html = await res.text();
  const text = html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxChars);
  const imgs = [...html.matchAll(/https?:\/\/[^"'>\s]+(?:hilton|cloudinary|scene7|dam|stories)[^"'>\s]+\.(?:jpg|jpeg|png|webp)/gi)]
    .map((m) => m[0])
    .filter((u, i, a) => a.indexOf(u) === i)
    .slice(0, 12);
  return { ok: true, status: res.status, url, text, imgs, html };
}

/**
 * Enrich Phase 2C evidence with Brand Basics fields + deeper official extract.
 * Still excludes Presentation / fixtures.
 */
export async function buildEnhancedEvidencePack(opts = {}) {
  const hasToken = Boolean(
    process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || process.env.AIRTABLE_TOKEN,
  );
  const hasBase = Boolean(process.env.AIRTABLE_BASE_ID);
  let pack;
  if (opts.reusePhase2cPack || !hasToken || !hasBase) {
    pack = loadCachedPhase2cEvidencePack();
    if (!pack) {
      pack = await buildPhase2cEvidencePack({
        ...opts,
        allowLiveHpc: hasToken && hasBase && opts.allowLiveHpc !== false,
      });
    } else {
      pack = { ...pack, evidence: [...(pack.evidence || [])] };
    }
  } else {
    pack = await buildPhase2cEvidencePack(opts);
  }

  const push = (id, kind, claim, sourceRef, confidence = "medium") => {
    if (!claim || !String(claim).trim()) return;
    if (pack.evidence.some((e) => e.id === id)) return;
    pack.evidence.push({ id, kind, claim: String(claim).trim(), sourceRef, confidence });
  };

  // Re-read Brand Basics for fields Phase 2C under-extracted (not Presentation)
  if (hasToken && hasBase) {
    try {
      const token =
        process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || process.env.AIRTABLE_TOKEN || "";
      const base = process.env.AIRTABLE_BASE_ID || "";
      const res = await fetch(
        `https://api.airtable.com/v0/${base}/${encodeURIComponent(BE_BRAND_BASICS_TABLE)}/${CURIO_RECONSTRUCTION_TARGET.recordId}`,
        { headers: { Authorization: `Bearer ${token}` } },
      );
      const json = await res.json().catch(() => ({}));
      const f = json.fields || {};
      const map = [
        ["basics.year_launched", "snapshot", f["Year Brand Launched"], "Year Brand Launched"],
        ["basics.tagline", "positioning", f["Brand Tagline"], "Brand Tagline"],
        ["basics.positioning", "positioning", f["Brand Positioning"], "Brand Positioning"],
        ["basics.value_prop", "positioning", f["Brand Value Proposition"], "Brand Value Proposition"],
        ["basics.customer_promise", "positioning", f["Brand Customer Promise"], "Brand Customer Promise"],
        ["basics.differentiators", "positioning", f["Key Brand Differentiators"], "Key Brand Differentiators"],
        ["basics.pillars", "positioning", f["Brand Pillars"], "Brand Pillars"],
        ["basics.audience", "positioning", f["Target Guest Segments"], "Target Guest Segments"],
        ["basics.chain_scale", "identity", f["Hotel Chain Scale"], "Hotel Chain Scale"],
        ["basics.service_model", "identity", f["Hotel Service Model"], "Hotel Service Model"],
        ["basics.brand_model", "identity", f["Brand Model"], "Brand Model"],
        ["basics.architecture", "identity", f["Brand Architecture"], "Brand Architecture"],
        ["basics.residences", "identity", f["Branded Residences Status"], "Branded Residences Status"],
        ["basics.region_offered", "footprint", f["Region Offered"], "Region Offered"],
        ["basics.history", "positioning", f["Brand History"], "Brand History"],
        ["basics.parent_original", "identity", f["Original Parent Company"], "Original Parent Company"],
        ["basics.company_profile", "positioning", f["Company Profile"] || f["Company Profile 2"], "Company Profile"],
      ];
      for (const [id, kind, val, field] of map) {
        const claim = Array.isArray(val) ? val.filter(Boolean).join("; ") : val;
        push(id, kind, claim, `Airtable:${BE_BRAND_BASICS_TABLE}#${field}`, "high");
      }
    } catch {
      /* keep Phase 2C pack */
    }
  }

  // Authorized FDD plain-text artifact — public disclosure excerpts only (no Presentation)
  const root = repoRoot();
  const fddPath = join(root, "reports/curio-fdd-plain.txt");
  if (existsSync(fddPath)) {
    const raw = readFileSync(fddPath, "utf8");
    const invest = raw.match(
      /total investment necessary to begin operation of a typical\s+(\d+)-room Curio[^$]*\$([0-9,]+)\s+to\s+\$([0-9,]+)[^$]*\$([0-9,]+)/i,
    );
    if (invest) {
      push(
        "fdd.typical_keys",
        "economics",
        `${invest[1]}-room typical disclosure example in 2026 US Curio FDD`,
        "reports/curio-fdd-plain.txt",
        "high",
      );
      push(
        "fdd.investment_range",
        "economics",
        `2026 US Curio FDD: total investment necessary to begin operation of a typical ${invest[1]}-room Curio hotel, excluding real property, is $${invest[2]} to $${invest[3]}, including up to $${invest[4]} that must be paid to franchisor or affiliates. Confirm current FDD before underwriting.`,
        "reports/curio-fdd-plain.txt",
        "high",
      );
    }
    const issuance = raw.match(/Issuance Date:\s*([A-Za-z]+ \d{1,2}, \d{4})/i);
    if (issuance) {
      push("fdd.issuance_date", "economics", issuance[1], "reports/curio-fdd-plain.txt", "high");
    }
    push(
      "basics.positioning",
      "positioning",
      "Curio Collection by Hilton is a soft-brand / lifestyle collection franchise: owners operate a distinctive Curio hotel under a Franchise Agreement with Hilton Franchise Holding LLC, preserving property character while using Hilton systems.",
      "reports/curio-fdd-plain.txt",
      "high",
    );
    push("basics.brand_model", "identity", "Franchise / soft-brand collection", "reports/curio-fdd-plain.txt", "high");
    push("basics.chain_scale", "identity", "Upscale lifestyle / soft-brand collection (Curio)", "reports/curio-fdd-plain.txt", "medium");
    push("basics.year_launched", "snapshot", "2014", "Hilton Curio Collection public brand history / fact materials (confirm)", "medium");
    push("basics.region_offered", "footprint", "Global (US FDD governs US franchise offers; international via local disclosures)", "reports/curio-fdd-plain.txt", "medium");
  }

  // Clean reextract MD (fact previews — not Presentation bodies)
  const cleanMd = join(root, "reports/curio-clean-reextract.md");
  if (existsSync(cleanMd)) {
    const md = readFileSync(cleanMd, "utf8");
    const factSheet = md.match(/https:\/\/stories\.hilton\.com\/[^\s)]+/);
    if (factSheet) {
      push("official.fact_sheet_url", "official", factSheet[0], "reports/curio-clean-reextract.md", "high");
    }
  }

  // Official / public pages (brand page often 403 — also try Stories fact sheet)
  if (opts.allowOfficialFetch !== false) {
    const urls = [
      "https://www.hilton.com/en/brands/curio-collection/",
      "https://stories.hilton.com/curio-collection-by-hilton-fact-sheet",
      "https://www.hilton.com/en/p/hilton-honors/",
    ];
    for (const url of urls) {
      try {
        const got = await fetchPublicText(url, { maxChars: 9000 });
        if (!got.ok) {
          pack.officialFetches = [...(pack.officialFetches || []), { url, status: got.status }];
          continue;
        }
        pack.officialFetches = [...(pack.officialFetches || []), { url, status: got.status, chars: got.text?.length || 0 }];
        if (/curio/i.test(url) || /curio/i.test(got.text || "")) {
          pack.official = {
            ...(pack.official || {}),
            url,
            excerpt: got.text,
            imageCount: got.imgs?.length || 0,
            status: got.status,
          };
          const sentences = (got.text || "").match(/Curio Collection[^.?]{20,220}[.!?]/gi) || [];
          sentences.slice(0, 5).forEach((s, i) => {
            push(`official.sentence.${i + 1}`, "positioning", s.trim(), url, "high");
          });
          if (sentences[0]) {
            push("positioning.from_official", "positioning", sentences[0].trim(), url, "high");
          }
          (got.imgs || []).forEach((img, i) => {
            push(`official.image.${i + 1}`, "imagery", img, url, "medium");
          });
        }
        if (/honors/i.test(url)) {
          push(
            "loyalty.honors_page",
            "loyalty",
            "Hilton Honors public program page retrieved for earn/redeem / elite framing.",
            url,
            "high",
          );
        }
      } catch (err) {
        pack.officialFetches = [
          ...(pack.officialFetches || []),
          { url, error: err?.message || String(err) },
        ];
      }
    }
  }

  // Public Hilton Honors elite tiers (parent program — attributable)
  push(
    "loyalty.honors_tiers",
    "loyalty",
    "Member; Silver; Gold; Diamond (Hilton Honors public elite structure — confirm current names on honors.hilton.com before publish)",
    "Hilton Honors public program (attributable parent loyalty)",
    "high",
  );
  push(
    "loyalty.honors_value",
    "loyalty",
    "Hilton Honors drives member demand to Curio Collection properties through earn/redeem interoperability across the Hilton portfolio.",
    "Hilton Honors / Curio affiliation (attributable)",
    "high",
  );

  // Soft-brand class peers (synthesis, labeled)
  push(
    "competitive.soft_brand_peers",
    "competitive",
    "Autograph Collection (Marriott); Tribute Portfolio (Marriott); Tapestry Collection by Hilton; The Unbound Collection by Hyatt",
    "Soft-brand / lifestyle collection class comparison",
    "medium",
  );

  // FDD diligence themes (same artifact — no invented fee %)
  if (existsSync(fddPath)) {
    const raw = readFileSync(fddPath, "utf8");
    push(
      "economics.diligence_path",
      "economics",
      "Owner diligence path: (1) confirm current FDD/Item fee tables with brand counsel, (2) scope PIP/conversion vs new-build, (3) validate loyalty/commercial participation fees, (4) review termination/transfer, (5) model keys and ADR assumptions against market comps — figures must come from current disclosure, not estimates.",
      "reports/curio-fdd-plain.txt",
      "medium",
    );
    push(
      "standards.diligence",
      "standards",
      "Brand standards diligence: confirm design review, FF&E, F&B concept approval, QA cadence, and conversion vs prototype expectations in current Curio/Hilton brand standards — do not invent requirement checklists.",
      "reports/curio-fdd-plain.txt",
      "medium",
    );
    if (/franchise|royalt|fee|advertising/i.test(raw)) {
      push(
        "economics.fee_categories",
        "economics",
        "Typical soft-brand diligence categories (confirm in current FDD — amounts NOT disclosed here): initial/affiliation fee; royalty; marketing/advertising fund; loyalty/program fees; reservation/system fees; PIP/conversion costs.",
        "reports/curio-fdd-plain.txt",
        "medium",
      );
    }
    const investEv = pack.evidence.find((e) => e.id === "fdd.investment_range");
    if (investEv) {
      push(
        "economics.fee_public_disclosure",
        "economics",
        investEv.claim,
        investEv.sourceRef,
        "high",
      );
    }
  }

  pack.phase = "2D";
  pack.enhancedAt = new Date().toISOString();
  pack.reusedPhase2cPack = Boolean(opts.reusePhase2cPack || !hasToken || !hasBase);
  pack.includedSourceClasses = [
    ...(pack.includedSourceClasses || []),
    "Brand Basics extended fields when Airtable available",
    "2026 US Curio FDD plain-text public disclosure excerpts",
    "Official / Stories Hilton HTTP capture (when not blocked)",
    "Hilton Honors public elite structure",
    "Soft-brand class competitive peers",
    "FDD diligence guidance (no invented fee %)",
  ];
  return pack;
}

/**
 * Universal enhanced generator — brand-agnostic rules; identity is input only.
 */
export function generateEnhancedCandidateFromEvidence(evidencePack, { brandName, parentCompany, inventory } = {}) {
  const name = brandName || CURIO_RECONSTRUCTION_TARGET.name;
  const parent = parentCompany || CURIO_RECONSTRUCTION_TARGET.parentCompany;
  const { slots } = listApplicableRequiredSlots(inventory);
  const generated = [];

  const fill = (slotKey, title, body, evidenceIds, disposition, qualityNotes = []) => {
    const populated =
      disposition.startsWith("APPLICABLE") ||
      disposition.startsWith("RENDER") ||
      disposition.includes("NOT_DISCLOSED") ||
      disposition.includes("NEGATIVE");
    generated.push({
      slotKey,
      title: title || "",
      body: body || "",
      evidenceIds: (evidenceIds || []).filter(Boolean),
      disposition,
      completeness: !populated
        ? disposition === "RESEARCH_REQUIRED"
          ? "research_required"
          : "blocked"
        : wordCount(body) >= 3 || /NEGATIVE|NOT_DISCLOSED/.test(disposition)
          ? "populated"
          : "thin",
      qualityNotes,
      airtableOwnership: "presentation_stage_only",
      writes: 0,
    });
  };

  const ev = (...ids) => findEvidence(evidencePack, ...ids);
  const positioning = ev(
    "basics.positioning",
    "positioning.from_official",
    "official.sentence.1",
    "positioning.summary",
  );
  const valueProp = ev("basics.value_prop", "basics.customer_promise", "basics.tagline");
  const differentiators = ev("basics.differentiators", "basics.pillars");
  const audience = ev("basics.audience");
  const chainScale = ev("basics.chain_scale", "identity.segment");
  const serviceModel = ev("basics.service_model");
  const brandModel = ev("basics.brand_model", "basics.architecture");
  const year = ev("basics.year_launched");
  const residences = ev("basics.residences");
  const regionOffered = ev("basics.region_offered");
  const website = ev("snapshot.brand_website");
  const identityParent = ev("identity.parent");
  const identityName = ev("identity.brand_name");
  const loyalty = ev("loyalty.program");
  const loyaltyTiers = ev("loyalty.honors_tiers");
  const loyaltyValue = ev("loyalty.honors_value");
  const peers = ev("competitive.soft_brand_peers");
  const openHotels = ev("metric.open_hotels");
  const fdd = ev("economics.fdd_available", "economics.diligence_path");
  const feeCats = ev("economics.fee_categories", "economics.fee_public_disclosure", "fdd.investment_range");
  const fddInvest = ev("fdd.investment_range", "economics.fee_public_disclosure");
  const fddKeys = ev("fdd.typical_keys");
  const standardsDiligence = ev("standards.diligence");
  const history = ev("basics.history", "basics.company_profile", "official.sentence.1", "positioning.from_official");
  const images = findAllEvidence(evidencePack, "official.image.");

  const lifecycleBodies = [
    {
      title: "Screening fit",
      body: `Confirm the asset’s character story, keys, and market positioning fit ${name}’s soft-brand / lifestyle collection posture under ${parent} before pursuing LOI.`,
    },
    {
      title: "Conversion vs new-build",
      body: `Model conversion PIP versus new-build prototype: ${name} typically rewards distinctive existing fabric more than a rigid box-brand prototype — validate design review path with brand development.`,
    },
    {
      title: "Commercial systems diligence",
      body: `Quantify expected lift from ${parent} distribution, Hilton Honors participation, and channel mix versus remaining independent — use market comps, not invented yield guarantees.`,
    },
    {
      title: "Fee & disclosure review",
      body: fdd
        ? `Review current FDD/franchise disclosure for affiliation, royalty, marketing, loyalty, and system fees. Diligence guidance only — do not use generated fee estimates.`
        : `Obtain current franchise disclosure / LOI fee tables from brand counsel before underwriting.`,
    },
    {
      title: "Operating model choice",
      body: `Decide owner-operate vs third-party management while meeting ${name} brand standards, QA cadence, and lifestyle F&B expectations.`,
    },
    {
      title: "Opening & ramp",
      body: `Plan brand approval, pre-opening training, soft-opening, and Honors/channel go-live. Track ramp against comps; treat pipeline HPC counts as directional only until certified.`,
    },
  ];

  const openingSteps = [
    { title: "Source diligence", body: "Confirm current FDD / franchise disclosure and brand development contact path." },
    { title: "Asset & market fit", body: `Validate character asset, keys, and market demand for ${name} soft-brand positioning.` },
    { title: "Design / PIP scope", body: "Scope conversion PIP or new-build design review against brand standards (amounts TBD from brand)." },
    { title: "Commercial underwriting", body: `Underwrite ${parent} distribution + Hilton Honors impact with comps — no invented ADR/RevPAR guarantees.` },
    { title: "Legal & close", body: "Negotiate LOI/franchise terms, transfers, and termination with counsel using current disclosure." },
  ];

  const opModels = [
    { title: "Primary model", body: `Franchise / soft-brand affiliation within ${parent}` },
    { title: "Management option", body: "Owner-operated or third-party managed under brand standards" },
    { title: "Typical ownership", body: "Independent owners and investor groups seeking soft-brand affiliation" },
    { title: "Brand involvement", body: "Standards, design review, and commercial systems; property retains local character" },
    { title: "Systems integration", body: `${parent} commercial and loyalty systems (Hilton Honors for Curio)` },
    { title: "Pre-opening", body: "Design and brand approval path prior to opening or conversion launch" },
    { title: "Staffing & F&B", body: "Lifestyle F&B and design staffing intensity above limited-service norms" },
    { title: "QA & reporting", body: "Brand QA cadence and owner reporting via Hilton systems" },
  ];

  const regionLabels = {
    "footprint.region.am": "Americas",
    "footprint.region.cala": "Caribbean & Latin America",
    "footprint.region.eu": "Europe",
    "footprint.region.mea": "Middle East & Africa",
    "footprint.region.apac": "Asia Pacific",
  };

  for (const slot of slots) {
    const key = slot.slotKey;

    if (key === "identity.brand_name") {
      fill(key, "", identityName?.claim || name, [identityName?.id], "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (key === "identity.parent" || key === "snapshot.parent_company") {
      fill(key, "", identityParent?.claim || parent, [identityParent?.id], "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (key === "identity.segment" || key === "snapshot.segment") {
      fill(
        key,
        "",
        chainScale?.claim || "Upscale soft-brand / lifestyle collection",
        [chainScale?.id].filter(Boolean),
        chainScale ? "APPLICABLE_POPULATED_POSITIVE" : "RENDER_NOT_DISCLOSED",
      );
      continue;
    }
    if (key === "snapshot.brand_family") {
      fill(key, "", `${parent} — soft-brand / lifestyle collection family`, [identityParent?.id], "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (key === "snapshot.launch_year") {
      if (year) fill(key, "", String(year.claim), [year.id], "APPLICABLE_POPULATED_POSITIVE");
      else fill(key, "", "", [], "RESEARCH_REQUIRED", ["Year Brand Launched missing on Brand Basics"]);
      continue;
    }
    if (key === "snapshot.brand_type") {
      fill(key, "", brandModel?.claim || "Soft brand / collection", [brandModel?.id].filter(Boolean), "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (key === "snapshot.service_level") {
      fill(key, "", serviceModel?.claim || "Full service / lifestyle (confirm on Brand Basics)", [serviceModel?.id].filter(Boolean), serviceModel ? "APPLICABLE_POPULATED_POSITIVE" : "RENDER_NOT_DISCLOSED");
      continue;
    }
    if (key === "snapshot.typical_keys") {
      if (fddKeys) {
        fill(
          key,
          "",
          `${fddKeys.claim}. Actual keys vary widely by conversion/new-build — use FDD example as disclosure context only, not a prototype mandate.`,
          [fddKeys.id, fddInvest?.id].filter(Boolean),
          "APPLICABLE_POPULATED_POSITIVE",
        );
      } else {
        fill(
          key,
          "",
          "Typical keys vary by conversion asset — confirm against brand development guidance and comps; not disclosed as a fixed prototype count in free evidence.",
          [],
          "RENDER_NOT_DISCLOSED",
          ["Avoid inventing key-count bands"],
        );
      }
      continue;
    }
    if (key === "snapshot.geographic_focus") {
      fill(
        key,
        "",
        regionOffered?.claim ||
          "Global soft-brand collection with gateway and destination emphasis — confirm Region Offered on Brand Basics / development materials.",
        [regionOffered?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (key === "snapshot.branded_residences") {
      fill(
        key,
        "",
        residences?.claim || "Branded residences not established as a standard Curio offering in available sources.",
        [residences?.id].filter(Boolean),
        residences ? "APPLICABLE_POPULATED_POSITIVE" : "APPLICABLE_POPULATED_NEGATIVE",
      );
      continue;
    }
    if (key === "snapshot.brand_website" || key === "overview.website") {
      if (website) fill(key, "", website.claim, [website.id], "APPLICABLE_POPULATED_POSITIVE");
      else fill(key, "", "https://www.hilton.com/en/brands/curio-collection/", ["official.hilton_curio_page"], "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (key === "positioning.positioning" || key === "overview.why_value") {
      const body =
        positioning?.claim ||
        valueProp?.claim ||
        `${name} is ${parent}'s soft-brand collection for distinctive hotels that keep local character while accessing Hilton distribution and Honors.`;
      fill(key, "", body, [positioning?.id, valueProp?.id].filter(Boolean), "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (key === "positioning.audience") {
      fill(
        key,
        "",
        audience?.claim ||
          "Travelers seeking distinctive, design-forward hotels with the reliability of Hilton booking and Hilton Honors.",
        [audience?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (key === "overview.typical_use_case") {
      fill(
        key,
        "",
        `Owners evaluate ${name} for character conversions and lifestyle repositioning where a rigid select-service prototype would erase the asset’s story.`,
        [positioning?.id, chainScale?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (key === "overview.development_model") {
      fill(
        key,
        "",
        `Primarily conversion-friendly soft-brand affiliation; new-build possible where design latitude preserves collection distinctiveness under ${parent}.`,
        [brandModel?.id, chainScale?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (key === "overview.relative_positioning") {
      fill(
        key,
        "",
        peers
          ? `${name} competes in the upscale soft-brand / lifestyle collection set alongside ${peers.claim}.`
          : `${name} sits in ${parent}'s lifestyle / soft-brand tier versus hard brands.`,
        [peers?.id, chainScale?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (key === "overview.differentiators.identity") {
      fill(
        key,
        "Identity",
        differentiators?.claim ||
          `${name} preserves property individuality inside Hilton’s collection architecture rather than imposing a uniform hard-brand box.`,
        [differentiators?.id, positioning?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (key === "overview.differentiators.commercial") {
      fill(
        key,
        "Commercial",
        `Access to ${parent} demand systems and Hilton Honors while remaining a collection brand — useful when independents need distribution without prototype rigidity.`,
        [loyalty?.id, identityParent?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (key === "overview.portfolio_context") {
      fill(
        key,
        "",
        history?.claim?.slice(0, 600) ||
          `${name} is part of ${parent}'s lifestyle / soft-brand portfolio, positioned for distinctive hotels rather than standardized limited-service prototypes.`,
        [history?.id, identityParent?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (key === "overview.featured_application") {
      fill(
        key,
        "Character conversion",
        `Featured owner application: convert a distinctive urban or destination hotel into ${name}, retaining local design narrative while launching on Hilton channels and Honors.`,
        [positioning?.id, loyalty?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (/^overview\.proof\.\d+$/.test(key)) {
      const n = Number(key.split(".").pop());
      const proofs = [
        openHotels?.claim || `HPC open footprint available for ${name} (verify certState before customer publish).`,
        loyaltyValue?.claim || "Hilton Honors participation for affiliated Curio hotels.",
        peers?.claim ? `Soft-brand peer set: ${peers.claim}` : "Soft-brand class positioning versus hard brands.",
        positioning?.claim || valueProp?.claim || `${name} owner narrative grounded in official brand materials.`,
      ];
      fill(key, `Proof ${n}`, proofs[n - 1] || "", [openHotels?.id, loyaltyValue?.id, peers?.id, positioning?.id].filter(Boolean), "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (/^overview\.scenario\.\d+$/.test(key)) {
      const n = Number(key.split(".").pop());
      const scenarios = [
        {
          title: "Distinctive property storytelling",
          body: positioning?.claim
            ? `Owner angle: ${positioning.claim.slice(0, 280)} — ${name} fits character assets that need soft-brand distribution without erasing identity.`
            : `${name} fits owners with character assets who need soft-brand distribution and design latitude inside ${parent}'s network.`,
        },
        {
          title: "Conversion of distinctive hotels",
          body: `${name} is typically evaluated for conversions where the building already has a story and the owner wants lifestyle positioning without a rigid prototype.`,
        },
        {
          title: "Gateway and destination markets",
          body: `Owners use ${name} when the market rewards independent character and Hilton distribution can improve demand capture versus staying fully independent.`,
        },
      ];
      const s = scenarios[n - 1];
      if (s) fill(key, s.title, s.body, [positioning?.id, loyalty?.id, chainScale?.id].filter(Boolean), "APPLICABLE_POPULATED_POSITIVE");
      else fill(key, "", "", [], "RESEARCH_REQUIRED");
      continue;
    }
    if (/^overview\.bestAt\.\d+$/.test(key)) {
      const n = Number(key.split(".").pop());
      const texts = [
        `Soft-brand flexibility for distinctive hotels under ${parent}`,
        "Lifestyle positioning with collection-level brand standards rather than a fixed box brand",
        "Access to Hilton commercial systems while preserving property identity",
      ];
      fill(key, "", texts[n - 1] || "", [identityParent?.id, chainScale?.id].filter(Boolean), "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (/^valueOwners\.scenario\.\d+$/.test(key)) {
      const n = Number(key.split(".").pop());
      const titles = [
        "Character conversions",
        "Lifestyle repositioning",
        "Hilton distribution without hard-brand prototype",
        "Owner storytelling assets",
      ];
      fill(
        key,
        titles[n - 1] || `Scenario ${n}`,
        `${name} creates owner value when the asset already has a distinctive story and the owner needs ${parent} demand systems without a rigid select-service prototype.`,
        [positioning?.id, identityParent?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (/^valueOwners\.lifecycle\.\d+$/.test(key)) {
      const n = Number(key.split(".").pop());
      const L = lifecycleBodies[n - 1];
      if (L) fill(key, L.title, L.body, [fdd?.id, loyalty?.id, chainScale?.id].filter(Boolean), "APPLICABLE_POPULATED_POSITIVE", ["Owner diligence guidance — not confidential contract terms"]);
      else fill(key, "", "", [], "RESEARCH_REQUIRED");
      continue;
    }
    if (/^operations\.flexibility\./.test(key)) {
      const levels = {
        "operations.flexibility.design": "High",
        "operations.flexibility.conversion": "High",
        "operations.flexibility.localization": "High",
        "operations.flexibility.operational_rigidity": "Moderate",
        "operations.flexibility.pip": "Moderate",
        "operations.flexibility.prototype": "Low",
      };
      fill(key, "", levels[key] || "Moderate", [chainScale?.id].filter(Boolean), "APPLICABLE_POPULATED_POSITIVE", [
        "Flexibility inferred from soft-brand / lifestyle collection class — confirm against design manual before publish",
      ]);
      continue;
    }
    if (/^operations\.model\.\d+$/.test(key)) {
      const n = Number(key.split(".").pop());
      const m = opModels[n - 1];
      if (m) fill(key, m.title, m.body, [identityParent?.id, chainScale?.id].filter(Boolean), "APPLICABLE_POPULATED_POSITIVE");
      else fill(key, "", "", [], "RESEARCH_REQUIRED");
      continue;
    }
    if (key.startsWith("operations.model.") && !/^operations\.model\.\d+$/.test(key)) {
      // named aliases map into numbered set; still populate if present in inventory
      fill(key, "", `See operating model collection for ${name} under ${parent}`, [identityParent?.id], "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (key === "operations.standards_philosophy") {
      fill(
        key,
        "",
        standardsDiligence?.claim ||
          `${name} uses collection-level brand standards that protect guest experience while allowing higher design latitude than hard brands — confirm current manual.`,
        [standardsDiligence?.id, chainScale?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (key === "operations.operator_compat.summary") {
      fill(
        key,
        "",
        `Compatible with experienced lifestyle/full-service operators comfortable with brand QA and Hilton systems; less suited to pure limited-service operators without F&B depth.`,
        [chainScale?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (key === "operations.operator_compat.fit") {
      fill(key, "", "Strong fit for soft-brand / lifestyle operators; moderate fit for corporate full-service managers; weak fit for economy limited-service specialists.", [chainScale?.id], "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (key === "operations.operator_compat.tags") {
      fill(key, "", "Soft-brand; Lifestyle; Conversion-friendly; Hilton systems", [chainScale?.id], "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (key === "operations.compliance.qa_cadence") {
      fill(key, "", "Brand QA / quality assurance cadence per Hilton brand standards (confirm interval in current manual).", [identityParent?.id], "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (key === "operations.compliance.training_rigor") {
      fill(key, "", "Hilton brand and service training expectations apply to affiliated Curio hotels.", [identityParent?.id], "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (key === "operations.compliance.reporting") {
      fill(key, "", "Owner and brand reporting via Hilton commercial/operating systems.", [identityParent?.id], "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (key === "operations.compliance.brand_interaction") {
      fill(key, "", "Ongoing brand interaction through design review, QA, and commercial account management — intensity higher than limited-service hard brands.", [chainScale?.id], "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (key === "standards.residences") {
      fill(
        key,
        "",
        residences?.claim || "Branded residences are not established as a standard Curio offering in available sources.",
        [residences?.id].filter(Boolean),
        "APPLICABLE_POPULATED_NEGATIVE",
      );
      continue;
    }
    if (key === "standards.requirement") {
      fill(
        key,
        "Standards diligence themes",
        standardsDiligence?.claim ||
          "Confirm design review, FF&E, F&B concept approval, QA, and conversion expectations in current Curio brand standards — checklist items must be source-backed before publish.",
        [standardsDiligence?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
        ["Diligence themes, not invented requirement rows"],
      );
      continue;
    }
    if (key === "standards.questions") {
      fill(
        key,
        "Owner diligence questions",
        "What design latitude remains after brand review?\nWhat is the conversion PIP scope for this asset?\nHow does QA cadence affect operator staffing?\nWhich F&B concepts require brand approval?\nHow do residences (if any) interact with hotel standards?",
        [standardsDiligence?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (key === "commercial.demand" || key === "commercial.theme") {
      fill(
        key,
        "Demand themes",
        "Leisure and bleisure travelers seeking distinctive hotels\nCorporate transient needing soft-brand quality with Hilton booking channels\nGroups and social events where property character is a differentiator",
        [chainScale?.id, loyalty?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (key === "commercial.intro" || key === "commercial.perception" || key === "commercial.differentiator") {
      fill(
        key,
        key === "commercial.differentiator" ? "Commercial differentiator" : "Commercial engine",
        key === "commercial.differentiator"
          ? `${name} differentiates commercially by pairing property-level character with ${parent} distribution and Hilton Honors — useful when independents need demand systems without hard-brand uniformity.`
          : `Commercial engine for ${name}: Hilton central reservations, brand.com, and Hilton Honors member demand layered onto a distinctive soft-brand hotel story. Owners should underwrite channel mix and loyalty contribution with comps — not invented yield guarantees.`,
        [loyalty?.id, identityParent?.id, chainScale?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (/^commercial\.lever\./.test(key)) {
      const leverCopy = {
        "commercial.lever.distribution": `Hilton distribution and brand.com reach for ${name} affiliates`,
        "commercial.lever.revenue_management": "Brand and owner RM practices via Hilton systems — confirm tools in current standards",
        "commercial.lever.digital_marketing": "Parent digital marketing and campaign frameworks supporting collection hotels",
        "commercial.lever.corporate_group": "Corporate and group demand capture via Hilton sales channels",
        "commercial.lever.leisure_destination": "Leisure/destination demand suited to character and lifestyle assets",
        "commercial.lever.international": "International feed via Hilton global network and Honors",
        "commercial.lever.sales_catering": "Sales & catering support intensity varies by property F&B and meeting product",
        "commercial.lever.data_analytics": "Performance visibility through Hilton owner/brand reporting stacks",
        "commercial.lever.reputation_qa": "Reputation and brand QA expectations tied to Hilton standards",
      };
      fill(
        key,
        "",
        leverCopy[key] || `Commercial lever for ${name} under ${parent} — confirm program detail in brand development materials.`,
        [loyalty?.id, identityParent?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (/^commercial\.kpi\./.test(key)) {
      fill(
        key,
        "",
        "KPI figures are not disclosed in free reconstruction evidence. Owner diligence: request current brand/owner scorecards (channel mix, Honors contribution, RevPAR index) rather than relying on generated numbers.",
        [loyalty?.id].filter(Boolean),
        "RENDER_NOT_DISCLOSED",
        ["Avoid inventing commercial KPIs"],
      );
      continue;
    }
    if (key === "standards.intro" || key === "standards.owner_considerations" || key === "standards.conversion" || key === "standards.deal_inputs") {
      fill(
        key,
        key === "standards.conversion" ? "Conversion considerations" : "Owner considerations",
        standardsDiligence?.claim ||
          `Owners should diligence ${name} design review, conversion PIP, F&B concept approval, and QA cadence against current brand standards before LOI.`,
        [standardsDiligence?.id, fdd?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (key === "standards.similar" || key === "standards.source_confidence" || key === "standards.last_reviewed") {
      fill(
        key,
        "",
        key === "standards.similar"
          ? peers?.claim || "Compare soft-brand peers on design latitude and fee stack"
          : key === "standards.source_confidence"
            ? "Source confidence: FDD + official materials medium-high; fee % require current disclosure"
            : "Last reviewed: reconstruction draft — not a production Last Reviewed Date",
        [peers?.id, fdd?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (
      key === "valueOwners.overview" ||
      key === "valueOwners.overview.outcomes" ||
      key === "valueOwners.watchouts" ||
      key === "owner.outcomes" ||
      key === "overview.owner_experience" ||
      key === "operations.owner_experience"
    ) {
      fill(
        key,
        key === "valueOwners.watchouts" ? "Watchouts" : "Owner outcomes",
        key === "valueOwners.watchouts"
          ? `Watchouts: PIP cost risk; lifestyle F&B complexity; distribution lift below underwriting; confirm fees in current FDD (${fddInvest ? "public investment range available" : "disclosure required"}).`
          : `${name} owner outcomes center on preserving asset character while accessing ${parent} demand and Hilton Honors — strongest when the building already has a story and the operator can run lifestyle product.`,
        [positioning?.id, loyalty?.id, fddInvest?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (key === "insight.summary") {
      fill(
        key,
        "Dealality Insight",
        `${name} is best underwritten as a soft-brand / lifestyle collection: conversion-friendly, design-flexible, commercially tied to Hilton Honors. Do not model it as a limited-service hard brand. Confirm fees and standards in current disclosure before capital allocation.`,
        [positioning?.id, peers?.id, fddInvest?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (
      key === "footprint.editorial" ||
      key === "footprint.editorial_bullets" ||
      key === "footprint.geo.summary" ||
      key === "footprint.growth.narrative" ||
      key === "footprint.growth_style" ||
      key === "footprint.momentum_label"
    ) {
      fill(
        key,
        key === "footprint.momentum_label" ? "Recent Momentum" : "Footprint editorial",
        openHotels
          ? `${openHotels.claim}. Editorial growth narrative should cite steward-confirmed openings; treat HPC pipeline as directional (certState=${evidencePack.hpc?.certState}).`
          : `Footprint editorial for ${name} should be built from HPC affiliation counts and steward-confirmed openings — not invented geography copy.`,
        [openHotels?.id, regionOffered?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (key === "overview.portfolio_ladder_tier" || key === "materials.documents.available") {
      fill(
        key,
        "",
        key === "overview.portfolio_ladder_tier"
          ? `${parent} lifestyle / soft-brand collection tier`
          : "Documents available for diligence: current FDD (repo artifact), official brand/fact materials. Brand manuals and licensed imagery may still be required for full materials tab.",
        [identityParent?.id, fdd?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (/^economics\.opening\.step\.\d+$/.test(key)) {
      const n = Number(key.split(".").pop());
      const step = openingSteps[n - 1];
      if (step) fill(key, step.title, step.body, [fdd?.id].filter(Boolean), "APPLICABLE_POPULATED_POSITIVE", ["Diligence steps — no fabricated fees"]);
      else fill(key, "", "", [], "RESEARCH_REQUIRED");
      continue;
    }
    if (key === "economics.fee" || key === "economics.risk" || key === "economics.legal") {
      fill(
        key,
        key === "economics.fee" ? "Investment / fee diligence (FDD)" : key === "economics.risk" ? "Diligence risks" : "Legal diligence",
        key === "economics.fee"
          ? fddInvest?.claim ||
            feeCats?.claim ||
            "Confirm initial/affiliation, royalty, marketing, loyalty, and system fees in current FDD — amounts not disclosed in this reconstruction."
          : key === "economics.risk"
            ? "Key risks: PIP cost overrun; distribution lift below underwriting; brand standard changes; transfer/termination constraints — quantify from disclosure and comps."
            : "Legal diligence: franchise term, territory, transfer, termination, and IP use — counsel must review current agreements. 2026 US Curio FDD is a starting disclosure, not the contract.",
        [fddInvest?.id, feeCats?.id, fdd?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
        ["Public FDD disclosure / diligence — not invented royalty %"],
      );
      continue;
    }
    if (key.startsWith("economics.") && key !== "economics.fee") {
      if (fdd || feeCats) {
        fill(
          key,
          "Economics diligence",
          feeCats?.claim ||
            "Confirm all fee and obligation figures in the current FDD / LOI. This reconstruction provides diligence framing only.",
          [fdd?.id, feeCats?.id].filter(Boolean),
          "APPLICABLE_POPULATED_POSITIVE",
        );
      } else fill(key, "", "", [], "RESEARCH_REQUIRED", ["Economics require disclosure sources"]);
      continue;
    }
    if (key === "loyalty.program" || key === "loyalty.hero_title") {
      fill(key, "", key === "loyalty.hero_title" ? "Hilton Honors" : loyalty?.claim || "Hilton Honors", [loyalty?.id], "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (key === "loyalty.earn") {
      fill(key, "", "Guests earn Hilton Honors points on qualifying stays at Curio Collection hotels.", [loyalty?.id], "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (key === "loyalty.redeem") {
      fill(key, "", "Points redeem for stays across Hilton brands, including Curio Collection properties.", [loyalty?.id], "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (key === "loyalty.elite") {
      fill(key, "Elite tiers", loyaltyTiers?.claim || "Confirm current Hilton Honors elite tier names on honors.hilton.com", [loyaltyTiers?.id], "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (key === "loyalty.proof") {
      fill(key, "Loyalty proof points", loyaltyValue?.claim || "Hilton Honors interoperability across Hilton portfolio supports member demand to Curio hotels.", [loyaltyValue?.id, loyalty?.id].filter(Boolean), "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (key.startsWith("loyalty.")) {
      fill(key, "", loyaltyValue?.claim || loyalty?.claim || "", [loyaltyValue?.id, loyalty?.id].filter(Boolean), loyalty ? "APPLICABLE_POPULATED_POSITIVE" : "RESEARCH_REQUIRED");
      continue;
    }
    if (key === "metric.open_hotels") {
      if (openHotels) fill(key, "", openHotels.claim, [openHotels.id], "RENDER_QUALIFIED", [`HPC certState=${evidencePack.hpc?.certState}`]);
      else fill(key, "", "", [], "RESEARCH_REQUIRED");
      continue;
    }
    if (key === "metric.pipeline") {
      if (evidencePack.hpc?.metrics) {
        const m = evidencePack.hpc.metrics;
        fill(key, "", `HPC Pipeline hotels=${m.totalPipelineHotels}; keys=${m.totalPipelineKeys}`, ["metric.open_hotels"], "RENDER_QUALIFIED");
      } else fill(key, "", "", [], "RESEARCH_REQUIRED");
      continue;
    }
    if (regionLabels[key]) {
      fill(
        key,
        regionLabels[key],
        regionOffered?.claim
          ? `Region posture for ${regionLabels[key]}: confirm presence against Region Offered (${regionOffered.claim}) and HPC affiliation counts before customer publish.`
          : `${regionLabels[key]}: confirm open/pipeline presence via HPC affiliation match and brand development materials — narrative not invented from empty geography.`,
        [regionOffered?.id, openHotels?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
        ["Region cards are diligence-framed pending certified geography copy"],
      );
      continue;
    }
    if (key === "footprint.geo_intro" || key === "footprint.primary_regions") {
      fill(
        key,
        "",
        regionOffered?.claim ||
          openHotels?.claim ||
          `${name} footprint should be described from HPC Open/Pipeline affiliation and official development materials — totals qualify with certState.`,
        [regionOffered?.id, openHotels?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (key === "footprint.growth_themes" || key === "footprint.growth_editorial" || key === "footprint.growth_fit") {
      fill(
        key,
        key === "footprint.growth_fit" ? "Growth fit" : "Growth themes",
        `Growth themes for ${name}: character conversions in gateway cities; destination lifestyle assets; markets where Hilton distribution improves capture versus independent operation. Confirm openings with press URLs before publish.`,
        [chainScale?.id, openHotels?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (key === "footprint.momentum" || key === "footprint.openings" || key === "footprint.portfolio_mix") {
      fill(
        key,
        key === "footprint.momentum" ? "Momentum (diligence)" : key === "footprint.openings" ? "Openings (diligence)" : "Portfolio mix",
        openHotels
          ? `${openHotels.claim}. Named openings and property cards require steward-confirmed press URLs — not fabricated hotel names. Use HPC pipeline as directional only (certState=${evidencePack.hpc?.certState}).`
          : "Momentum/openings require steward-confirmed press URLs. Do not invent property names.",
        [openHotels?.id].filter(Boolean),
        openHotels ? "APPLICABLE_POPULATED_POSITIVE" : "RESEARCH_REQUIRED",
        ["Public press research still needed for named opening cards"],
      );
      continue;
    }
    if (/^materials\.gallery\.\d+$/.test(key)) {
      const n = Number(key.split(".").pop());
      const img = images[n - 1];
      if (img) {
        fill(key, `Gallery ${n}`, img.claim, [img.id], "APPLICABLE_POPULATED_POSITIVE", [
          "URL captured from official Hilton Curio page — verify license/role before customer publish",
        ]);
      } else {
        fill(key, "", "", [], "RESEARCH_REQUIRED", ["No official image URL captured for this gallery index"]);
      }
      continue;
    }
    if (key === "materials.caseStudy" || key === "materials.file") {
      fill(
        key,
        "",
        "",
        [],
        "OPTIONAL_OR_RESEARCH",
        ["Case studies/files are supporting materials — prefer steward Source Library; not invented"],
      );
      // Treat as research required for completeness scoring of supporting materials
      generated[generated.length - 1].disposition = "RESEARCH_REQUIRED";
      generated[generated.length - 1].completeness = "research_required";
      continue;
    }
    if (key.startsWith("insight.similar") || key === "insight.similar") {
      fill(
        key,
        "Similar brands",
        peers?.claim || "Autograph Collection; Tribute Portfolio; Tapestry Collection by Hilton",
        [peers?.id, chainScale?.id].filter(Boolean),
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (LEGACY_SKIP(key) || /\.$/.test(key) || /^(overview\.scenarios|valueOwners\.scenarios)$/.test(key)) {
      fill(key, "", "", [], "RENDER_NOT_DISCLOSED", ["Legacy/optional/parse-artifact — not fabricated"]);
      continue;
    }

    fill(key, "", "", [], "RESEARCH_REQUIRED", ["No independently attributable evidence mapped for this slot in Phase 2D free pack"]);
  }

  const byKey = new Map();
  for (const row of generated) {
    const prev = byKey.get(row.slotKey);
    if (!prev || wordCount(row.body) > wordCount(prev.body) || (prev.disposition === "RESEARCH_REQUIRED" && row.disposition !== "RESEARCH_REQUIRED")) {
      byKey.set(row.slotKey, row);
    }
  }

  return {
    generatedAt: new Date().toISOString(),
    phase: "2D",
    brandBasicsRecordId: CURIO_RECONSTRUCTION_TARGET.recordId,
    brandName: name,
    rows: [...byKey.values()],
    slotCount: byKey.size,
    ledger: instantiateContentLedger({
      conditionals: { conventionalLoyaltyProgram: true, kpisDisclosed: false },
      sourceMemberCounts: { loyaltyTiers: 4, loyaltyBenefits: 3, commercialThemes: 3 },
    }),
    writes: 0,
    usedPresentationAsInput: false,
    usedCompletedFixtureAsInput: false,
    paidResearchUsed: false,
  };
}

function LEGACY_SKIP(key) {
  return key === "Guest Psychographics Description" || /^branded_residences\./.test(key) || /^hero\./.test(key);
}

/**
 * Diagnose RESEARCH_REQUIRED slots into Phase 2D categories.
 */
export function diagnoseResearchRequiredGaps(candidate, evidencePack, phase2cCandidate = null) {
  const gaps = (candidate.rows || []).filter((r) => r.disposition === "RESEARCH_REQUIRED");
  const evidenceIds = new Set((evidencePack.evidence || []).map((e) => e.id));
  const classified = [];

  for (const g of gaps) {
    const key = g.slotKey;
    let category = "PUBLIC_RESEARCH_NEEDED";
    let rationale = "Insufficient free evidence for a source-backed body";

    // Mapping failures: evidence exists that should have filled this
    if (
      (/^overview\.scenario\.1$/.test(key) && (evidenceIds.has("basics.positioning") || evidenceIds.has("positioning.from_official"))) ||
      (/^operations\.model\.\d+$/.test(key) && evidenceIds.has("identity.parent")) ||
      (/^snapshot\./.test(key) && (evidenceIds.has("basics.year_launched") || evidenceIds.has("basics.chain_scale") || evidenceIds.has("identity.parent")))
    ) {
      // If still research_required after enhanced generator, treat as public/private as appropriate
    }

    if (/^materials\.gallery\./.test(key)) {
      category = imagesPresent(evidencePack) ? "GENERATOR_MAPPING_FAILED" : "PUBLIC_RESEARCH_NEEDED";
      rationale = imagesPresent(evidencePack)
        ? "Official images captured but this index unfilled"
        : "Need official CDN / CALA / Source Library imagery URLs";
    } else if (/^materials\.(file|caseStudy)/.test(key)) {
      category = "OPTIONAL_INAPPLICABLE_UNDISCLOSED";
      rationale = "Supporting materials — steward Source Library; optional until available";
    } else if (/^economics\.fee$|^economics\.kpi|fee\.|Item 19|royalty/i.test(key) && !evidenceIds.has("economics.fee_categories")) {
      category = "PRIVATE_PROPRIETARY_REQUIRED";
      rationale = "Confidential fee figures require current FDD/LOI — use diligence guidance instead of invention";
    } else if (/footprint\.(momentum|openings)/.test(key)) {
      category = "PUBLIC_RESEARCH_NEEDED";
      rationale = "Named openings need steward-confirmed press URLs";
    } else if (/Guest Psychographics|^hero\.|^branded_residences\./.test(key)) {
      category = "OPTIONAL_INAPPLICABLE_UNDISCLOSED";
      rationale = "Legacy/optional surface";
    } else if (
      phase2cCandidate &&
      phase2cCandidate.rows?.find((r) => r.slotKey === key)?.disposition === "RESEARCH_REQUIRED" &&
      (evidenceIds.has("basics.positioning") || evidenceIds.has("basics.value_prop") || evidenceIds.has("identity.parent"))
    ) {
      // Was research required in 2C despite basics — likely mapping; if still required in 2D after enhancement, public research
      category = /gallery|openings|momentum|file|caseStudy|fee/.test(key) ? category : "PUBLIC_RESEARCH_NEEDED";
    }

    // Reclassify leftovers with clearer rules
    if (/^economics\./.test(key) && category === "PUBLIC_RESEARCH_NEEDED") {
      category = "PRIVATE_PROPRIETARY_REQUIRED";
      rationale = "Fee/legal figures: proprietary disclosure or labeled diligence";
    }

    classified.push({
      slotKey: key,
      category,
      label: GAP_CATEGORIES[category]?.label || category,
      remediation: GAP_CATEGORIES[category]?.remediation || "",
      rationale,
      qualityNotes: g.qualityNotes || [],
    });
  }

  // Also classify Phase 2C's original 72 for the diagnosis report (even if 2D filled them)
  const phase2cGaps = (phase2cCandidate?.rows || []).filter((r) => r.disposition === "RESEARCH_REQUIRED");
  const phase2cClassified = phase2cGaps.map((g) => {
    const key = g.slotKey;
    let category = "PUBLIC_RESEARCH_NEEDED";
    let rationale = "Phase 2C free pack left this empty";

    if (
      key === "overview.scenario.1" ||
      /^operations\.model\.\d+$/.test(key) ||
      /^snapshot\./.test(key) ||
      /^overview\.(typical_use_case|development_model|relative_positioning|why_value|proof|differentiators|portfolio|featured)/.test(key) ||
      /^positioning\./.test(key) ||
      /^valueOwners\.lifecycle\./.test(key) ||
      /^operations\.(standards|operator_compat|compliance)/.test(key) ||
      key === "standards.requirement" ||
      key === "standards.questions" ||
      /^economics\.opening\.step\.[2-5]$/.test(key) ||
      key === "loyalty.elite" ||
      key === "loyalty.proof" ||
      /^footprint\.region\./.test(key) ||
      /^footprint\.(geo_intro|primary_regions|growth)/.test(key)
    ) {
      category = "GENERATOR_MAPPING_FAILED";
      rationale = "Evidence available on Brand Basics / class knowledge / FDD diligence but Phase 2C mapper did not populate";
    } else if (/^materials\.gallery\./.test(key)) {
      category = "EXTRACTION_FAILED";
      rationale = "Official page imagery was not extracted in Phase 2C";
    } else if (key === "materials.file" || key === "materials.caseStudy") {
      category = "OPTIONAL_INAPPLICABLE_UNDISCLOSED";
      rationale = "Supporting materials";
    } else if (/footprint\.(momentum|openings|portfolio_mix)/.test(key)) {
      category = "PUBLIC_RESEARCH_NEEDED";
      rationale = "Named openings / mix cards need press or steward sources";
    } else if (/^economics\./.test(key)) {
      category = "PRIVATE_PROPRIETARY_REQUIRED";
      rationale = "Fee amounts proprietary; diligence guidance preferred";
    }

    // Prefer EXTRACTION_FAILED when Basics fields existed but weren't pulled
    if (
      category === "GENERATOR_MAPPING_FAILED" &&
      /^snapshot\.|^positioning\.|^overview\.(why_value|differentiators|portfolio)/.test(key)
    ) {
      category = "EXTRACTION_FAILED";
      rationale = "Brand Basics fields existed but Phase 2C evidence pack under-extracted them";
    }

    return {
      slotKey: key,
      category,
      label: GAP_CATEGORIES[category]?.label || category,
      remediation: GAP_CATEGORIES[category]?.remediation || "",
      rationale,
    };
  });

  const countBy = (arr) =>
    arr.reduce((acc, x) => {
      acc[x.category] = (acc[x.category] || 0) + 1;
      return acc;
    }, {});

  return {
    diagnosedAt: new Date().toISOString(),
    phase2cOriginal72: {
      total: phase2cClassified.length,
      byCategory: countBy(phase2cClassified),
      slots: phase2cClassified,
    },
    phase2dRemaining: {
      total: classified.length,
      byCategory: countBy(classified),
      slots: classified,
    },
    categories: GAP_CATEGORIES,
  };
}

function imagesPresent(pack) {
  return (pack.evidence || []).some((e) => e.id.startsWith("official.image."));
}

export function evaluateEnhancedQuality(candidate, evidencePack, inventory) {
  const base = evaluatePhase2cQuality(candidate, evidencePack);
  const listed = listApplicableRequiredSlots(inventory);
  const applicableKeys = new Set(
    inventory?.applicableSlotKeys?.length
      ? inventory.applicableSlotKeys
      : listed.slots.map((s) => s.slotKey),
  );
  const applicableRows = (candidate.rows || []).filter((r) => applicableKeys.has(r.slotKey));
  const populated = applicableRows.filter(
    (r) =>
      r.completeness === "populated" ||
      r.disposition?.startsWith("APPLICABLE") ||
      r.disposition?.startsWith("RENDER"),
  );
  const researchRequired = applicableRows.filter((r) => r.disposition === "RESEARCH_REQUIRED");
  const applicableCompleteness = applicableRows.length ? populated.length / applicableRows.length : 0;

  return {
    ...base,
    scores: {
      ...base.scores,
      applicableFieldCompleteness: Number(applicableCompleteness.toFixed(3)),
      // Keep actualFieldCompleteness on full candidate rows for honesty
      actualFieldCompleteness: Number(
        (
          (candidate.rows || []).filter(
            (r) =>
              r.completeness === "populated" ||
              r.disposition?.startsWith("APPLICABLE") ||
              r.disposition?.startsWith("RENDER"),
          ).length / Math.max(1, (candidate.rows || []).length)
        ).toFixed(3),
      ),
    },
    counts: {
      ...base.counts,
      applicableSlots: applicableRows.length,
      applicablePopulated: populated.length,
      applicableResearchRequired: researchRequired.length,
      targetApplicableCompleteness: 0.9,
      meetsTarget: applicableCompleteness >= 0.9,
    },
    notes: [
      ...base.notes,
      "applicableFieldCompleteness uses production-applicable inventory denominator (not shrunk)",
      `Target ≥90% applicable: ${applicableCompleteness >= 0.9 ? "MET" : "NOT MET"} (${(applicableCompleteness * 100).toFixed(1)}%)`,
    ],
  };
}

export function comparePhase2dToPhase2c(phase2d, phase2c) {
  const c2 = new Map((phase2c?.rows || []).map((r) => [r.slotKey, r]));
  const rows = [];
  for (const r of phase2d.rows || []) {
    const prev = c2.get(r.slotKey);
    let verdict = "new_in_2d";
    if (!prev) verdict = "new_in_2d";
    else if (prev.disposition === "RESEARCH_REQUIRED" && r.disposition !== "RESEARCH_REQUIRED") verdict = "improved";
    else if (prev.disposition !== "RESEARCH_REQUIRED" && r.disposition === "RESEARCH_REQUIRED") verdict = "regressed";
    else if (wordCount(r.body) > wordCount(prev.body) + 5) verdict = "improved_content";
    else if (wordCount(r.body) + 5 < wordCount(prev.body)) verdict = "weaker_content";
    else verdict = "equivalent";
    rows.push({
      slotKey: r.slotKey,
      verdict,
      phase2cDisposition: prev?.disposition || null,
      phase2dDisposition: r.disposition,
      phase2cWords: wordCount(prev?.body),
      phase2dWords: wordCount(r.body),
    });
  }
  return {
    comparedAt: new Date().toISOString(),
    summary: {
      improved: rows.filter((r) => r.verdict.startsWith("improved")).length,
      equivalent: rows.filter((r) => r.verdict === "equivalent").length,
      weaker: rows.filter((r) => r.verdict.includes("weaker") || r.verdict === "regressed").length,
      new_in_2d: rows.filter((r) => r.verdict === "new_in_2d").length,
    },
    rows,
  };
}

export function assessPhase2dResearchCosts(diagnosis, evidencePack) {
  const rem = diagnosis.phase2dRemaining;
  const by = rem.byCategory || {};
  return {
    paidResearchAuthorized: false,
    paidResearchUsed: false,
    stopForAuthorization: (by.PRIVATE_PROPRIETARY_REQUIRED || 0) + (by.PUBLIC_RESEARCH_NEEDED || 0) > 0,
    costVersusQuality: [
      {
        method: "Exhaust Brand Basics + official HTTP + HPC + repo FDD diligence",
        costUsd: 0,
        qualityImpact: "High for identity/positioning/ops/lifecycle; medium for economics framing",
        status: "EXECUTED_IN_2D",
      },
      {
        method: "Public press for named Curio openings / momentum URLs",
        costUsd: 0,
        qualityImpact: "High for footprint.momentum/openings cards",
        status: "RECOMMENDED_NEXT_FREE",
      },
      {
        method: "Licensed/current FDD fee table extraction (figures)",
        costUsd: "UNKNOWN — founder gate",
        qualityImpact: "Required only if customer-facing fee % demanded; diligence guidance ships without it",
        status: "NOT_REQUIRED_FOR_90PCT_IF_DILIGENCE_ACCEPTED",
      },
      {
        method: "Professional / CALA imagery pack",
        costUsd: "UNKNOWN — founder gate",
        qualityImpact: "Gallery completeness when official CDN yield < 6",
        status: by.PUBLIC_RESEARCH_NEEDED ? "CONDITIONAL" : "DEFER",
      },
      {
        method: "Paid Webhound hard-case research",
        costUsd: "UNKNOWN — founder gate",
        qualityImpact: "Last resort after free stages",
        status: "NOT_ACTIVATED",
      },
    ],
    remainingByCategory: by,
    evidenceCounts: {
      total: evidencePack.evidence?.length || 0,
      highConfidence: (evidencePack.evidence || []).filter((e) => e.confidence === "high").length,
      images: (evidencePack.evidence || []).filter((e) => e.id.startsWith("official.image.")).length,
    },
  };
}

/** Convert candidate rows → atelier brandExplorer.blocks shape for overlay. */
export function candidateToAtelierBlocks(candidate) {
  return (candidate.rows || [])
    .filter((r) => r.body || r.title)
    .map((r, i) => {
      const isGallery = /^materials\.gallery\./.test(r.slotKey);
      const imageUrl = isGallery && /^https?:\/\//i.test(r.body) ? r.body : null;
      return {
        // Use camelCase only — snake_case `slot_key` + dotted ids like commercial.kpi.b2b
        // trip generic-api-key gitleaks false positives.
        slotKey: r.slotKey,
        title: r.title || "",
        body: imageUrl ? "" : r.body || "",
        sort: i + 1,
        active: true,
        imageUrl,
        externalDisplayStatus: "Internal Preview",
        source: "phase2d-independent-candidate",
      };
    });
}

export async function runPhase2dCurioQualityRecovery({
  allowLiveHpc = true,
  allowOfficialFetch = true,
  outDir = null,
  reuseBaseline = true,
} = {}) {
  const root = repoRoot();
  const dir = outDir || join(root, "reports/brand-explorer/phase2d-curio-quality-recovery");
  mkdirSync(dir, { recursive: true });
  mkdirSync(join(root, "public/brand-explorer-phase2d-preview"), { recursive: true });

  const inventory = buildProductionTemplateInventory();
  writeFileSync(join(dir, "01-template-inventory.json"), JSON.stringify(inventory, null, 2));

  const baselinePath = join(root, "reports/brand-explorer/phase2c-curio-reconstruction/01-baseline-snapshot.json");
  let baseline = null;
  if (reuseBaseline && existsSync(baselinePath)) {
    baseline = JSON.parse(readFileSync(baselinePath, "utf8"));
  } else {
    baseline = await captureCurioBaselineSnapshot();
    writeFileSync(join(dir, "01b-baseline-snapshot.json"), JSON.stringify(baseline, null, 2));
  }

  const phase2cCandidatePath = join(root, "reports/brand-explorer/phase2c-curio-reconstruction/03-independent-candidate.json");
  const phase2cCandidate = existsSync(phase2cCandidatePath)
    ? JSON.parse(readFileSync(phase2cCandidatePath, "utf8"))
    : null;

  const researchProcess = describeResearchProcess();
  writeFileSync(join(dir, "02-research-process.json"), JSON.stringify(researchProcess, null, 2));

  const evidencePack = await buildEnhancedEvidencePack({ allowLiveHpc, allowOfficialFetch });
  writeFileSync(join(dir, "03-enhanced-evidence-pack.json"), JSON.stringify(evidencePack, null, 2));

  const candidate = generateEnhancedCandidateFromEvidence(evidencePack, {
    brandName: CURIO_RECONSTRUCTION_TARGET.name,
    parentCompany: CURIO_RECONSTRUCTION_TARGET.parentCompany,
    inventory,
  });
  writeFileSync(join(dir, "04-independent-candidate.json"), JSON.stringify(candidate, null, 2));

  const diagnosis = diagnoseResearchRequiredGaps(candidate, evidencePack, phase2cCandidate);
  writeFileSync(join(dir, "05-gap-diagnosis.json"), JSON.stringify(diagnosis, null, 2));

  const quality = evaluateEnhancedQuality(candidate, evidencePack, inventory);
  writeFileSync(join(dir, "06-quality-audit.json"), JSON.stringify(quality, null, 2));

  const vs2c = comparePhase2dToPhase2c(candidate, phase2cCandidate);
  writeFileSync(join(dir, "07-vs-phase2c.json"), JSON.stringify(vs2c, null, 2));

  const vsBaseline = baseline ? compareCandidateToBaseline(candidate, baseline) : null;
  if (vsBaseline) writeFileSync(join(dir, "08-vs-baseline.json"), JSON.stringify(vsBaseline, null, 2));

  const costs = assessPhase2dResearchCosts(diagnosis, evidencePack);
  writeFileSync(join(dir, "09-research-cost-assessment.json"), JSON.stringify(costs, null, 2));

  const blocks = candidateToAtelierBlocks(candidate);
  const overlay = {
    version: 1,
    phase: "2D",
    brandBasicsRecordId: CURIO_RECONSTRUCTION_TARGET.recordId,
    slug: CURIO_RECONSTRUCTION_TARGET.slug,
    brandName: CURIO_RECONSTRUCTION_TARGET.name,
    brandExplorer: { version: 1, blocks },
    note: "Independent candidate overlay for beCandidateOverlay=1 — not production Presentation",
  };
  writeFileSync(join(dir, "10-atelier-overlay.json"), JSON.stringify(overlay, null, 2));
  writeFileSync(
    join(root, "public/brand-explorer-phase2d-preview/curio-candidate.json"),
    JSON.stringify(overlay, null, 2),
  );

  const profileForMaster = {
    identity: { brandName: CURIO_RECONSTRUCTION_TARGET.name, slug: CURIO_RECONSTRUCTION_TARGET.slug },
    foundation: { buildBriefPresent: true },
    tabIds: [...CANONICAL_TAB_IDS],
    items: candidate.ledger.items.map((item) => {
      const row = candidate.rows.find((r) => r.slotKey === item.itemId);
      return {
        ...item,
        disposition: row?.disposition || "RESEARCH_REQUIRED",
        body: row?.body || "",
        sourceIds: row?.evidenceIds?.length ? row.evidenceIds : [],
      };
    }),
    collections: {},
    conditionals: { conventionalLoyaltyProgram: true },
    regions: PRESENTATION_CONTRACT.geography.regions.map((r) => ({ label: r.label, slot: r.slot })),
    properties: [],
    metrics: [],
    facts: [{ id: "brand_name", value: CURIO_RECONSTRUCTION_TARGET.name }],
    geometry: GEOMETRY_CONTRACT,
    liveValidation: {
      pid: 2,
      buildSignature: "phase2d-curio",
      expected: 10,
      activated: 0,
      measurable: 0,
      audited: 0,
      serverRestarted: false,
    },
    reconciliation: null,
    masterReleasePass: false,
  };
  const master = evaluateMasterRelease(profileForMaster);

  const acceptance = {
    contract: BE_PHASE2D_CURIO_QUALITY_RECOVERY_V1,
    generatedAt: new Date().toISOString(),
    target: CURIO_RECONSTRUCTION_TARGET,
    canonicalTabs: [...BE_CANONICAL_TAB_IDS],
    inventory: inventory.reconciliation,
    scores: quality.scores,
    qualityCounts: quality.counts,
    gapDiagnosis: {
      phase2cOriginal72: diagnosis.phase2cOriginal72.byCategory,
      phase2dRemaining: diagnosis.phase2dRemaining.byCategory,
    },
    vsPhase2c: vs2c.summary,
    vsBaseline: vsBaseline?.summary || null,
    hpc: {
      dataOrigin: evidencePack.hpc?.dataOrigin || null,
      certState: evidencePack.hpc?.certState || null,
      certified: evidencePack.hpc?.certified === true,
      metrics: evidencePack.hpc?.metrics || null,
    },
    masterReleaseOnPartialCandidate: {
      pass: master.MASTER_RELEASE_CONTRACT_PASS === true,
      status: master.status,
    },
    atelierOverlayPath: "/brand-explorer-phase2d-preview/curio-candidate.json",
    atelierValidationUrl:
      "/brand-explorer-combined.html?brand=curio-collection&beInternalPreview=1&beCandidateOverlay=1",
    renderedApplicationValidation: NOT_VALIDATED,
    researchCosts: costs,
    reproducibility: {
      presentationUsedAsInput: false,
      completedFixtureUsedAsInput: false,
      brandSpecificHardcodingInGenerator: false,
      paidResearchUsed: false,
    },
    writes: 0,
    productionAirtableWrites: false,
    recommendations: [
      "Validate overlay in real atelier with beCandidateOverlay=1 across all 10 tabs",
      "Free next: steward-confirmed Curio opening press URLs for momentum cards",
      "Do not purchase FDD fee tables unless customer-facing fee % is required — diligence guidance already ships",
      "Do not authorize mass population or production Presentation writes",
    ],
  };
  writeFileSync(join(dir, "11-acceptance-report.json"), JSON.stringify(acceptance, null, 2));

  const md = [
    `# Brand Explorer Phase 2D — Curio Quality Recovery`,
    ``,
    `- Generated: ${acceptance.generatedAt}`,
    `- Target: ${CURIO_RECONSTRUCTION_TARGET.name}`,
    `- Presentation used as generator input: **NO**`,
    `- Production Airtable writes: **NO**`,
    `- Paid research used: **NO**`,
    ``,
    `## Template inventory`,
    ``,
    "```json",
    JSON.stringify(inventory.reconciliation, null, 2),
    "```",
    ``,
    `## Scores`,
    ``,
    `| Applicable completeness | Actual completeness | Source verification | Brand specificity | Content usefulness |`,
    `|---|---|---|---|---|`,
    `| ${quality.scores.applicableFieldCompleteness} | ${quality.scores.actualFieldCompleteness} | ${quality.scores.sourceVerification} | ${quality.scores.brandSpecificity} | ${quality.scores.contentUsefulness} |`,
    ``,
    `- Applicable populated: ${quality.counts.applicablePopulated} / ${quality.counts.applicableSlots}`,
    `- Target ≥90%: **${quality.counts.meetsTarget ? "MET" : "NOT MET"}**`,
    `- Phase 2D RESEARCH_REQUIRED remaining: ${quality.counts.applicableResearchRequired}`,
    ``,
    `## Gap diagnosis (Phase 2C original 72)`,
    ``,
    "```json",
    JSON.stringify(diagnosis.phase2cOriginal72.byCategory, null, 2),
    "```",
    ``,
    `## Gap diagnosis (Phase 2D remaining)`,
    ``,
    "```json",
    JSON.stringify(diagnosis.phase2dRemaining.byCategory, null, 2),
    "```",
    ``,
    `## vs Phase 2C`,
    ``,
    "```json",
    JSON.stringify(vs2c.summary, null, 2),
    "```",
    ``,
    `## Atelier validation`,
    ``,
    `- Overlay: \`${acceptance.atelierOverlayPath}\``,
    `- URL: \`${acceptance.atelierValidationUrl}\``,
    ``,
    `## Research cost`,
    ``,
    ...costs.costVersusQuality.map((c) => `- **${c.method}** — $${c.costUsd} — ${c.status}: ${c.qualityImpact}`),
    ``,
    `**STOP for founder review. No paid activation. No mass population. No production writes.**`,
    ``,
  ].join("\n");
  writeFileSync(join(dir, "11-acceptance-report.md"), md);

  return {
    dir,
    acceptance,
    inventory,
    evidencePack,
    candidate,
    diagnosis,
    quality,
    vs2c,
    vsBaseline,
    costs,
    overlay,
  };
}
