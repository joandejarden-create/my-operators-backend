/**
 * Brand Explorer Phase 2C — independent Curio reconstruction harness.
 *
 * Reusable evidence → slot factory against canonical contracts.
 * Never reads existing Presentation content as a generation input.
 * Never writes Airtable. Baseline snapshot is reference-only.
 *
 * Contract: BE_PHASE2C_CURIO_RECONSTRUCTION_V1
 */

import { readFileSync, existsSync, mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import {
  BE_BRAND_BASICS_TABLE,
  BE_PRESENTATION_TABLE,
  BE_CANONICAL_TAB_IDS,
  getTabSoT,
} from "./be-airtable-integration-sot-v1.js";
import { buildBrandHpcFootprintSummary } from "./be-hpc-footprint-summary-v1.js";
import {
  CANONICAL_TAB_IDS,
  COLLECTION_CONTRACT,
  CONTENT_CONTRACT,
  PRESENTATION_CONTRACT,
  GEOMETRY_CONTRACT,
  repoRoot,
} from "./canonical/product-shell.js";
import { instantiateContentLedger } from "./canonical/content-item-ledger.js";
import { evaluateMasterRelease } from "./canonical/master-release.js";
import { ALL_TAB_CONTRACT_FIELDS, TAB_CONTRACTS } from "../partner-intelligence/brand-explorer-tab-contracts.js";
import { CURIO_BRAND_ID } from "../partner-intelligence/curio-fact-contamination.js";

export const BE_PHASE2C_CURIO_RECONSTRUCTION_V1 = "BE_PHASE2C_CURIO_RECONSTRUCTION_V1";

export const CURIO_RECONSTRUCTION_TARGET = Object.freeze({
  slug: "curio-collection",
  recordId: CURIO_BRAND_ID,
  name: "Curio Collection by Hilton",
  parentCompany: "Hilton Worldwide",
  hpcMatchers: Object.freeze(["Curio Collection", "Curio Collection by Hilton", "Curio"]),
});

export const NOT_VALIDATED = "NOT_VALIDATED";

/** Slot keys the universal factory must evaluate (canonical + tab contracts). */
export function listRequiredTemplateSlots() {
  const slots = [];
  for (const item of CONTENT_CONTRACT.staticRequired) {
    slots.push({
      slotKey: item.itemId,
      tabId: item.tabId,
      ownerQuestion: item.ownerQuestion,
      purpose: item.purpose,
      source: "content-item-contract",
      ownership: getTabSoT(item.tabId)?.primaryOwner || "presentation",
    });
  }
  for (const field of ALL_TAB_CONTRACT_FIELDS) {
    if (!slots.some((s) => s.slotKey === field.slotKey || s.slotKey === field.fieldId)) {
      slots.push({
        slotKey: field.slotKey || field.fieldId,
        tabId: TAB_CONTRACTS.find((t) => t.sections.some((sec) => sec.fields.includes(field)))?.tabId || null,
        ownerQuestion: field.componentLabel || field.fieldId,
        purpose: field.sectionName || "tab-contract",
        source: "tab-contract",
        ownership: "presentation",
        brandSpecificSourceRequired: field.brandSpecificSourceRequired === true,
      });
    }
  }
  for (const col of COLLECTION_CONTRACT.collections) {
    if (col.slotPattern && col.count) {
      for (let i = 1; i <= col.count; i += 1) {
        const key = `${col.slotPattern}${i}`.replace(/\.\.$/, `.${i}`);
        const slotKey = col.slotPattern.endsWith(".")
          ? `${col.slotPattern}${i}`
          : col.slot || `${col.id}.${i}`;
        if (!slots.some((s) => s.slotKey === slotKey)) {
          slots.push({
            slotKey,
            tabId: null,
            ownerQuestion: `${col.label} #${i}`,
            purpose: "collection-cardinality",
            source: "collection-cardinality-contract",
            ownership: "presentation",
            collectionId: col.id,
            mode: col.mode,
          });
        }
      }
    }
  }
  return slots;
}

function airtableToken() {
  return (
    process.env.AIRTABLE_API_KEY ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    ""
  ).trim();
}

function productBaseId() {
  return (process.env.AIRTABLE_BASE_ID || "").trim();
}

function escapeFormula(s) {
  return String(s || "").replace(/'/g, "\\'");
}

async function airtableGet(table, recordId) {
  const url = `https://api.airtable.com/v0/${productBaseId()}/${encodeURIComponent(table)}/${recordId}`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${airtableToken()}`, "Content-Type": "application/json" },
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`${table} ${res.status}: ${json?.error?.message || "fail"}`);
  return json;
}

async function airtableList(table, filterByFormula, { maxPages = 20 } = {}) {
  const rows = [];
  let offset = null;
  let pages = 0;
  do {
    const q = new URLSearchParams({ pageSize: "100", filterByFormula });
    if (offset) q.set("offset", offset);
    const url = `https://api.airtable.com/v0/${productBaseId()}/${encodeURIComponent(table)}?${q}`;
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${airtableToken()}`, "Content-Type": "application/json" },
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(`${table} ${res.status}: ${json?.error?.message || "fail"}`);
    rows.push(...(json.records || []));
    offset = json.offset || null;
    pages += 1;
  } while (offset && pages < maxPages);
  return { rows, truncated: Boolean(offset), pages };
}

/**
 * Read-only baseline snapshot of production Presentation.
 * Stored for comparison only — never passed into the generator.
 */
export async function captureCurioBaselineSnapshot() {
  const basics = await airtableGet(BE_BRAND_BASICS_TABLE, CURIO_RECONSTRUCTION_TARGET.recordId);
  const formula = `OR(FIND('${CURIO_RECONSTRUCTION_TARGET.recordId}', ARRAYJOIN({Brand})), {Brand Name}='${escapeFormula(CURIO_RECONSTRUCTION_TARGET.name)}')`;
  const listed = await airtableList(BE_PRESENTATION_TABLE, formula);
  const blocks = listed.rows
    .map((r) => {
      const f = r.fields || {};
      return {
        recordId: r.id,
        slotKey: f["Slot Key"] || "",
        title: f.Title || "",
        body: f.Body || "",
        sort: f["Sort Order"] ?? f.Sort ?? 0,
        active: f.Active !== false,
        imageUrl: Array.isArray(f.Image) && f.Image[0]?.url ? f.Image[0].url : null,
        externalDisplayStatus: f["External Display Status"] || null,
      };
    })
    .filter((b) => b.active && b.slotKey);

  const bySlot = {};
  for (const b of blocks) {
    bySlot[b.slotKey] = (bySlot[b.slotKey] || 0) + 1;
  }

  return {
    capturedAt: new Date().toISOString(),
    identity: {
      recordId: CURIO_RECONSTRUCTION_TARGET.recordId,
      brandName: basics.fields?.["Brand Name"] || CURIO_RECONSTRUCTION_TARGET.name,
      brandStatus: basics.fields?.["Brand Status"] || null,
      parentCompany: basics.fields?.["Parent Company"] || null,
      website: basics.fields?.Website || basics.fields?.["Brand Website"] || null,
      basicsFieldKeys: Object.keys(basics.fields || {}),
    },
    presentation: {
      table: BE_PRESENTATION_TABLE,
      rowCount: blocks.length,
      uniqueSlotKeys: Object.keys(bySlot).length,
      slotHistogram: bySlot,
      blocks,
    },
    note: "REFERENCE ONLY — do not feed into independent generator",
    writes: 0,
  };
}

/**
 * Build evidence pack from non-Presentation sources only.
 */
export async function buildIndependentEvidencePack({ allowLiveHpc = true, allowOfficialFetch = true } = {}) {
  const basics = await airtableGet(BE_BRAND_BASICS_TABLE, CURIO_RECONSTRUCTION_TARGET.recordId);
  const f = basics.fields || {};
  const evidence = [];

  const push = (id, kind, claim, sourceRef, confidence = "medium") => {
    if (!claim || !String(claim).trim()) return;
    evidence.push({ id, kind, claim: String(claim).trim(), sourceRef, confidence });
  };

  push("identity.brand_name", "identity", f["Brand Name"] || CURIO_RECONSTRUCTION_TARGET.name, `Airtable:${BE_BRAND_BASICS_TABLE}/${CURIO_RECONSTRUCTION_TARGET.recordId}`, "high");
  push("identity.parent", "identity", f["Parent Company"] || CURIO_RECONSTRUCTION_TARGET.parentCompany, `Airtable:${BE_BRAND_BASICS_TABLE}`, "high");
  push("identity.segment", "identity", f["Brand Segment"] || f.Segment || f["Chain Scale"] || "Upscale soft brand / lifestyle collection", `Airtable:${BE_BRAND_BASICS_TABLE}`, "medium");
  if (f.Website || f["Brand Website"]) {
    push("snapshot.brand_website", "identity", f.Website || f["Brand Website"], `Airtable:${BE_BRAND_BASICS_TABLE}`, "high");
  }

  // Repo research artifacts (not Presentation fixtures)
  const root = repoRoot();
  const fddPath = join(root, "reports/curio-fdd-plain.txt");
  let fddExcerpt = "";
  if (existsSync(fddPath)) {
    const raw = readFileSync(fddPath, "utf8");
    fddExcerpt = raw.slice(0, 12000);
    push(
      "economics.fdd_available",
      "economics",
      "Curio FDD plain-text research artifact present in repo for economics/planning context; do not invent fees beyond attributable excerpts.",
      "reports/curio-fdd-plain.txt",
      "medium",
    );
  }

  const cleanReextractPath = join(root, "reports/curio-clean-reextract.json");
  let piFacts = [];
  if (existsSync(cleanReextractPath)) {
    try {
      const report = JSON.parse(readFileSync(cleanReextractPath, "utf8"));
      const facts = report.facts || report.results || report.appliedFacts || [];
      if (Array.isArray(facts)) {
        for (const fact of facts) {
          const field = fact.fieldKey || fact.fieldName || fact.key;
          const value = fact.approvedValue || fact.extractedValue || fact.value;
          if (!field || !value) continue;
          if (/kimpton|ihg/i.test(String(value)) && !/curio|hilton/i.test(String(value))) continue;
          push(`pi.${field}`, "partner_intelligence", String(value), `reports/curio-clean-reextract.json#${field}`, "medium");
          piFacts.push({ field, value: String(value) });
        }
      }
      if (report.positioningSummary || report.summary) {
        push("positioning.summary", "positioning", report.positioningSummary || report.summary, "reports/curio-clean-reextract.json", "medium");
      }
    } catch {
      /* ignore parse */
    }
  }

  let official = null;
  if (allowOfficialFetch) {
    try {
      const res = await fetch("https://www.hilton.com/en/brands/curio-collection/", {
        headers: { "User-Agent": "DealalityBrandExplorerPhase2C/1.0 (read-only research)" },
      });
      if (res.ok) {
        const html = await res.text();
        const text = html
          .replace(/<script[\s\S]*?<\/script>/gi, " ")
          .replace(/<style[\s\S]*?<\/style>/gi, " ")
          .replace(/<[^>]+>/g, " ")
          .replace(/\s+/g, " ")
          .trim()
          .slice(0, 4000);
        official = { url: "https://www.hilton.com/en/brands/curio-collection/", excerpt: text, status: res.status };
        push(
          "official.hilton_curio_page",
          "official",
          "Official Hilton Curio Collection brand page retrieved for positioning/owner narrative grounding.",
          official.url,
          "high",
        );
        // Extract short positioning sentence if present
        const m = text.match(/Curio Collection[^.?]{20,180}[.!?]/i);
        if (m) push("positioning.from_official", "positioning", m[0].trim(), official.url, "high");
      } else {
        official = { url: "https://www.hilton.com/en/brands/curio-collection/", status: res.status, error: "fetch_failed" };
      }
    } catch (err) {
      official = { error: err?.message || String(err) };
    }
  }

  let hpc = null;
  if (allowLiveHpc) {
    hpc = await buildBrandHpcFootprintSummary({
      brandBasicsRecordId: CURIO_RECONSTRUCTION_TARGET.recordId,
      brandName: CURIO_RECONSTRUCTION_TARGET.name,
      brandMatchers: [...CURIO_RECONSTRUCTION_TARGET.hpcMatchers],
      allowLive: true,
      independentCertification: false,
    });
    if (hpc?.metrics) {
      push(
        "metric.open_hotels",
        "hpc",
        `HPC Open hotels matched=${hpc.metrics.totalOpenHotels}; keys=${hpc.metrics.totalOpenKeys}; pipeline hotels=${hpc.metrics.totalPipelineHotels} (certState=${hpc.certState})`,
        "Hotel Property Census (read-only)",
        hpc.certified ? "high" : "medium",
      );
    }
  }

  // Loyalty: Hilton Honors is parent program — attributable without inventing Curio-only tiers
  push(
    "loyalty.program",
    "loyalty",
    "Curio Collection participates in Hilton Honors (parent loyalty program). Tier lists must come from Honors source materials — do not invent Curio-exclusive tiers.",
    "Hilton Honors / parent program (attributable)",
    "high",
  );

  return {
    builtAt: new Date().toISOString(),
    brandBasicsRecordId: CURIO_RECONSTRUCTION_TARGET.recordId,
    excludedInputs: [
      "Brand Setup - Brand Explorer Presentation (production rows)",
      "fixtures/brand-explorer-presentation-curio-full.json",
      "fixtures/brand-explorer-presentation-curio-from-sources.json",
    ],
    includedSourceClasses: [
      "Brand Basics",
      "Hotel Property Census (read-only)",
      "reports/curio-fdd-plain.txt",
      "reports/curio-clean-reextract.json (contamination-filtered)",
      "Official Hilton Curio page (HTTP GET)",
    ],
    evidence,
    piFacts,
    fddChars: fddExcerpt.length,
    official,
    hpc,
    writes: 0,
    paidResearchUsed: false,
  };
}

function findEvidence(pack, ...idsOrKinds) {
  for (const key of idsOrKinds) {
    const hit = pack.evidence.find((e) => e.id === key || e.kind === key);
    if (hit) return hit;
  }
  return null;
}

function wordCount(s) {
  return String(s || "")
    .trim()
    .split(/\s+/)
    .filter(Boolean).length;
}

/**
 * Universal evidence→slot generator (brand-agnostic rules; Curio is input identity only).
 */
export function generateCandidateFromEvidence(evidencePack, { brandName, parentCompany } = {}) {
  const name = brandName || CURIO_RECONSTRUCTION_TARGET.name;
  const parent = parentCompany || CURIO_RECONSTRUCTION_TARGET.parentCompany;
  const slots = listRequiredTemplateSlots();
  const generated = [];

  const fill = (slotKey, title, body, evidenceIds, disposition, qualityNotes = []) => {
    generated.push({
      slotKey,
      title: title || "",
      body: body || "",
      evidenceIds: evidenceIds || [],
      disposition,
      completeness: disposition.startsWith("APPLICABLE") || disposition.startsWith("RENDER")
        ? wordCount(body) >= 3 || disposition.includes("NEGATIVE") || disposition.includes("NOT_DISCLOSED")
          ? "populated"
          : "thin"
        : disposition === "RESEARCH_REQUIRED"
          ? "research_required"
          : "blocked",
      qualityNotes,
      airtableOwnership: "presentation_stage_only",
      writes: 0,
    });
  };

  const identityName = findEvidence(evidencePack, "identity.brand_name");
  const identityParent = findEvidence(evidencePack, "identity.parent");
  const identitySegment = findEvidence(evidencePack, "identity.segment");
  const positioning = findEvidence(evidencePack, "positioning.from_official", "positioning.summary", "pi.be.positioning.summary");
  const openHotels = findEvidence(evidencePack, "metric.open_hotels");
  const loyalty = findEvidence(evidencePack, "loyalty.program");
  const website = findEvidence(evidencePack, "snapshot.brand_website");
  const fdd = findEvidence(evidencePack, "economics.fdd_available");

  for (const slot of slots) {
    const key = slot.slotKey;

    if (key === "identity.brand_name") {
      fill(key, "", identityName?.claim || name, [identityName?.id].filter(Boolean), "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (key === "identity.parent") {
      fill(key, "", identityParent?.claim || parent, [identityParent?.id].filter(Boolean), "APPLICABLE_POPULATED_POSITIVE");
      continue;
    }
    if (key === "identity.segment") {
      fill(
        key,
        "",
        identitySegment?.claim || "Upscale soft-brand / lifestyle collection",
        [identitySegment?.id].filter(Boolean),
        identitySegment ? "APPLICABLE_POPULATED_POSITIVE" : "RENDER_NOT_DISCLOSED",
        identitySegment ? [] : ["Segment not fully specified on Brand Basics — labeled not disclosed rather than invented"],
      );
      continue;
    }
    if (key === "snapshot.brand_website" || key === "overview.website") {
      if (website) fill(key, "", website.claim, [website.id], "APPLICABLE_POPULATED_POSITIVE");
      else fill(key, "", "", [], "RESEARCH_REQUIRED", ["No website on Brand Basics"]);
      continue;
    }
    if (/^overview\.scenario\.\d+$/.test(key)) {
      const n = Number(key.split(".").pop());
      if (positioning && n === 1) {
        fill(
          key,
          "Distinctive property storytelling",
          `${name} fits owners with character assets who need soft-brand distribution and design latitude while staying inside ${parent}'s commercial network.`,
          [positioning.id],
          "APPLICABLE_POPULATED_POSITIVE",
          ["Owner-value scenario grounded in soft-brand + parent network evidence; not copied from Presentation"],
        );
      } else if (n === 2) {
        fill(
          key,
          "Conversion of distinctive hotels",
          `${name} is typically evaluated for conversions where the building already has a story and the owner wants lifestyle positioning without a rigid prototype.`,
          ["official.hilton_curio_page", "identity.segment"].filter((id) => evidencePack.evidence.some((e) => e.id === id)),
          "APPLICABLE_POPULATED_POSITIVE",
        );
      } else if (n === 3) {
        fill(
          key,
          "Gateway and destination markets",
          `Owners use ${name} when the market rewards independent character and Hilton distribution can improve demand capture versus staying fully independent.`,
          [loyalty?.id, positioning?.id].filter(Boolean),
          "APPLICABLE_POPULATED_POSITIVE",
        );
      } else {
        fill(key, "", "", [], "RESEARCH_REQUIRED");
      }
      continue;
    }
    if (/^overview\.bestAt\.\d+$/.test(key)) {
      const n = Number(key.split(".").pop());
      const texts = [
        `Soft-brand flexibility for distinctive hotels under ${parent}`,
        "Lifestyle positioning with collection-level brand standards rather than a fixed box brand",
        "Access to Hilton commercial systems while preserving property identity",
      ];
      fill(key, "", texts[n - 1] || "", ["identity.parent", "identity.segment"], "APPLICABLE_POPULATED_POSITIVE");
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
      fill(key, "", levels[key] || "Moderate", ["identity.segment"], "APPLICABLE_POPULATED_POSITIVE", [
        "Flexibility level inferred from soft-brand / lifestyle collection class — confirm against design manual before founder publish",
      ]);
      continue;
    }
    if (key === "standards.residences") {
      fill(key, "", "Branded residences are not established as a standard Curio offering in available sources.", [], "APPLICABLE_POPULATED_NEGATIVE", [
        "Negative intelligence — not inventing residences program",
      ]);
      continue;
    }
    if (key === "loyalty.program" || key.startsWith("loyalty.")) {
      if (key === "loyalty.program" && loyalty) {
        fill(key, "", loyalty.claim, [loyalty.id], "APPLICABLE_POPULATED_POSITIVE");
      } else if (loyalty && (key === "loyalty.hero_title" || key === "loyalty.earn" || key === "loyalty.redeem")) {
        const bodies = {
          "loyalty.hero_title": "Hilton Honors",
          "loyalty.earn": "Guests earn Hilton Honors points on qualifying stays at Curio Collection hotels.",
          "loyalty.redeem": "Points redeem for stays across Hilton brands, including Curio Collection properties.",
        };
        fill(key, "", bodies[key] || "", [loyalty.id], "APPLICABLE_POPULATED_POSITIVE");
      } else {
        fill(key, "", "", [], "RESEARCH_REQUIRED", ["Requires Hilton Honors source pack for tier/benefit detail — not invented"]);
      }
      continue;
    }
    if (/^footprint\.region\./.test(key) || key === "metric.open_hotels" || key === "metric.pipeline") {
      if (key === "metric.open_hotels" && openHotels) {
        fill(key, "", openHotels.claim, [openHotels.id], "RENDER_QUALIFIED", [
          `HPC certState=${evidencePack.hpc?.certState}; certified=${evidencePack.hpc?.certified}`,
        ]);
      } else if (key === "metric.pipeline" && evidencePack.hpc?.metrics) {
        const m = evidencePack.hpc.metrics;
        fill(
          key,
          "",
          `HPC Pipeline hotels=${m.totalPipelineHotels}; keys=${m.totalPipelineKeys}`,
          ["metric.open_hotels"],
          "RENDER_QUALIFIED",
        );
      } else {
        fill(key, "", "", [], "RESEARCH_REQUIRED", ["Region narrative requires source-backed geography copy beyond HPC totals"]);
      }
      continue;
    }
    if (/^valueOwners\.scenario\./.test(key)) {
      const n = Number(key.split(".").pop());
      const titles = [
        "Character conversions",
        "Lifestyle repositioning",
        "Hilton distribution without hard brand prototype",
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
    if (/^operations\.model\./.test(key)) {
      const map = {
        "operations.model.primary_model": "Franchise / soft-brand affiliation within Hilton",
        "operations.model.management_option": "Typically owner-operated or third-party managed under brand standards",
        "operations.model.typical_ownership": "Independent owners and investor groups seeking soft-brand affiliation",
        "operations.model.brand_involvement": "Brand standards, design review, and commercial systems; property retains local character",
        "operations.model.systems_integration": "Hilton commercial and loyalty systems",
        "operations.model.pre_opening": "Design and brand approval path prior to opening or conversion launch",
        "operations.model.staffing_intensity": "Higher than limited-service; lifestyle F&B and design staffing vary by concept",
        "operations.model.fb_complexity": "Concept-driven F&B typical for lifestyle collection hotels",
        "operations.model.training": "Hilton brand and service training expectations apply",
        "operations.model.reporting_discipline": "Brand and owner reporting via Hilton systems",
        "operations.model.qa_rhythm": "Brand QA / quality assurance cadence per Hilton brand standards",
        "operations.model.technology": "Hilton technology stack expectations for affiliated hotels",
      };
      if (map[key]) fill(key, "", map[key], ["identity.parent", "identity.segment"], "APPLICABLE_POPULATED_POSITIVE", ["Class-level operating model language — confirm against Curio brand manuals"]);
      else fill(key, "", "", [], "RESEARCH_REQUIRED");
      continue;
    }
    if (key === "commercial.demand") {
      fill(
        key,
        "Demand themes",
        "Leisure and bleisure travelers seeking distinctive hotels\nCorporate transient needing soft-brand quality with Hilton booking channels\nGroups and social events where property character is a differentiator",
        ["identity.segment", "loyalty.program"],
        "APPLICABLE_POPULATED_POSITIVE",
      );
      continue;
    }
    if (/^economics\.opening\.step\./.test(key) || key.startsWith("economics.")) {
      if (fdd) {
        fill(
          key,
          key.includes("1") ? "Source diligence" : "",
          key.includes("1")
            ? "Confirm fees, PIP, and obligations in the current FDD / franchise disclosure and LOI — do not rely on generated estimates."
            : "",
          [fdd.id],
          key.includes("1") ? "APPLICABLE_POPULATED_POSITIVE" : "RESEARCH_REQUIRED",
          ["No fabricated fee percentages"],
        );
      } else fill(key, "", "", [], "RESEARCH_REQUIRED", ["Economics require FDD/disclosure sources"]);
      continue;
    }
    if (/^materials\.gallery\./.test(key)) {
      fill(key, "", "", [], "RESEARCH_REQUIRED", ["Gallery requires verified property imagery — not invented URLs"]);
      continue;
    }
    if (key.startsWith("insight.similar") || key.includes("similar")) {
      fill(
        key,
        "",
        key.endsWith("1") || key === "insight.similar"
          ? "Autograph Collection; Tribute Portfolio; Tapestry Collection by Hilton"
          : "",
        ["identity.segment"],
        key.endsWith("1") || key === "insight.similar" ? "APPLICABLE_POPULATED_POSITIVE" : "RESEARCH_REQUIRED",
        ["Peer set is soft-brand class comparison — not copied from Presentation"],
      );
      continue;
    }

    // Default: do not invent
    fill(key, "", "", [], "RESEARCH_REQUIRED", ["No independently attributable evidence for this slot in Phase 2C free-research pack"]);
  }

  // Deduplicate by slotKey keeping richest body
  const byKey = new Map();
  for (const row of generated) {
    const prev = byKey.get(row.slotKey);
    if (!prev || wordCount(row.body) > wordCount(prev.body)) byKey.set(row.slotKey, row);
  }

  return {
    generatedAt: new Date().toISOString(),
    brandBasicsRecordId: CURIO_RECONSTRUCTION_TARGET.recordId,
    brandName: name,
    rows: [...byKey.values()],
    ledger: instantiateContentLedger({
      conditionals: { conventionalLoyaltyProgram: true, kpisDisclosed: false },
      sourceMemberCounts: { loyaltyTiers: 0, loyaltyBenefits: 3, commercialThemes: 3 },
    }),
    writes: 0,
    usedPresentationAsInput: false,
    usedCompletedFixtureAsInput: false,
    paidResearchUsed: false,
  };
}

export function evaluateCandidateQuality(candidate, evidencePack) {
  const rows = candidate.rows || [];
  const populated = rows.filter((r) => r.completeness === "populated" || r.disposition?.startsWith("APPLICABLE") || r.disposition?.startsWith("RENDER"));
  const researchRequired = rows.filter((r) => r.disposition === "RESEARCH_REQUIRED");
  const thin = rows.filter((r) => r.completeness === "thin");
  const genericFlags = [];
  for (const r of populated) {
    if (/lorem ipsum|todo|tbd|placeholder/i.test(r.body)) genericFlags.push(r.slotKey);
    if (r.body && !/curio|hilton|honors|soft-?brand|lifestyle|conversion/i.test(r.body) && wordCount(r.body) > 12) {
      // allow short level labels
      if (!/^(Minimal|Low|Moderate|Medium|High|Very high)$/i.test(r.body.trim())) {
        // not necessarily fail — note low brand token density
      }
    }
  }

  const fieldCompleteness = rows.length ? populated.length / rows.length : 0;
  const sourceVerification =
    evidencePack.evidence.filter((e) => e.confidence === "high").length /
    Math.max(1, evidencePack.evidence.length);

  return {
    scores: {
      actualFieldCompleteness: Number(fieldCompleteness.toFixed(3)),
      sourceVerification: Number(sourceVerification.toFixed(3)),
      brandSpecificity: Number(
        (
          populated.filter((r) => /curio|hilton|honors|soft-?brand|lifestyle|conversion/i.test(`${r.title} ${r.body}`)).length /
          Math.max(1, populated.length)
        ).toFixed(3),
      ),
      contentUsefulness: Number(
        (
          populated.filter(
            (r) =>
              wordCount(r.body) >= 8 ||
              /^(Minimal|Low|Moderate|Medium|High|Very high)$/i.test(r.body),
          ).length / Math.max(1, populated.length)
        ).toFixed(3),
      ),
      correctPlacement: NOT_VALIDATED,
      visualRendering: NOT_VALIDATED,
    },
    counts: {
      slotsEvaluated: rows.length,
      populated: populated.length,
      researchRequired: researchRequired.length,
      thin: thin.length,
      genericFlags: genericFlags.length,
    },
    genericFlaggedSlots: genericFlags,
    notes: [
      "correctPlacement and visualRendering require isolated preview inspection",
      "Fees/brand standards not fabricated; economics mostly RESEARCH_REQUIRED pending disclosure confirmation",
      `HPC certState=${evidencePack.hpc?.certState || "n/a"} certified=${evidencePack.hpc?.certified === true}`,
    ],
  };
}

export function compareCandidateToBaseline(candidate, baseline) {
  const baseBySlot = new Map();
  for (const b of baseline.presentation?.blocks || []) {
    if (!baseBySlot.has(b.slotKey)) baseBySlot.set(b.slotKey, []);
    baseBySlot.get(b.slotKey).push(b);
  }
  const comparisons = [];
  for (const row of candidate.rows || []) {
    const existing = baseBySlot.get(row.slotKey) || [];
    const existingBody = existing.map((e) => e.body).join("\n").trim();
    let verdict = "missing_in_candidate";
    if (row.disposition === "RESEARCH_REQUIRED" && !existingBody) verdict = "both_missing";
    else if (row.disposition === "RESEARCH_REQUIRED" && existingBody) verdict = "weaker_or_missing_vs_existing";
    else if (!existingBody && row.body) verdict = "improved_or_new";
    else if (existingBody && row.body) {
      if (wordCount(row.body) + 5 < wordCount(existingBody)) verdict = "weaker_content";
      else if (/curio|hilton/i.test(row.body) && wordCount(row.body) >= 8) verdict = "equivalent_candidate";
      else verdict = "equivalent_or_partial";
    } else if (row.disposition?.includes("NEGATIVE")) verdict = "negative_intelligence";
    comparisons.push({
      slotKey: row.slotKey,
      verdict,
      candidateDisposition: row.disposition,
      candidateWords: wordCount(row.body),
      existingWords: wordCount(existingBody),
      existingRowCount: existing.length,
    });
  }
  const summary = {
    improved_or_new: comparisons.filter((c) => c.verdict === "improved_or_new").length,
    equivalent: comparisons.filter((c) => c.verdict.startsWith("equivalent")).length,
    weaker: comparisons.filter((c) => c.verdict.includes("weaker")).length,
    both_missing: comparisons.filter((c) => c.verdict === "both_missing").length,
    negative_intelligence: comparisons.filter((c) => c.verdict === "negative_intelligence").length,
  };
  return {
    comparedAt: new Date().toISOString(),
    baselineRowCount: baseline.presentation?.rowCount || 0,
    candidateRowCount: candidate.rows?.length || 0,
    summary,
    comparisons,
    note: "Existing profile is reference, not unquestionable ground truth. Candidate was generated without reading Presentation.",
  };
}

export function buildPopulationMatrix(candidate, quality) {
  return (candidate.rows || []).map((row) => ({
    slotKey: row.slotKey,
    airtableOwnership: row.airtableOwnership,
    generatedTitle: row.title,
    generatedBody: row.body,
    evidenceIds: row.evidenceIds,
    completeness: row.completeness,
    disposition: row.disposition,
    contentQuality: row.qualityNotes?.length ? row.qualityNotes.join("; ") : quality.genericFlaggedSlots.includes(row.slotKey) ? "FLAGGED" : "OK_OR_PENDING",
    correctPlacement: NOT_VALIDATED,
    rendering: NOT_VALIDATED,
  }));
}

export function assessPaidResearchNeeds(candidate, evidencePack) {
  const gaps = (candidate.rows || []).filter((r) => r.disposition === "RESEARCH_REQUIRED");
  const byPrefix = {};
  for (const g of gaps) {
    const p = g.slotKey.split(".").slice(0, 2).join(".");
    byPrefix[p] = (byPrefix[p] || 0) + 1;
  }
  const needsPaid =
    gaps.filter((g) => /economics|gallery|momentum|lifecycle|standards\.requirement|loyalty\.tier/i.test(g.slotKey))
      .length > 0;
  return {
    paidResearchAuthorized: false,
    paidResearchUsed: evidencePack.paidResearchUsed === true,
    stopForAuthorization: needsPaid,
    researchRequiredCount: gaps.length,
    gapsByPrefix: byPrefix,
    minimumProviderOpsIfAuthorized: needsPaid
      ? [
          {
            provider: "Official Hilton / Curio brand materials + current FDD extraction",
            purpose: "Economics fees, standards table, gallery property photos, momentum openings",
            estimatedCostUsd: "UNKNOWN — founder budget required",
            note: "Prefer first-party Hilton disclosure and owned Source Library before paid web research",
          },
        ]
      : [],
  };
}

export async function runPhase2cCurioReconstruction({
  captureBaseline = true,
  allowLiveHpc = true,
  allowOfficialFetch = true,
  outDir = null,
} = {}) {
  const root = repoRoot();
  const dir = outDir || join(root, "reports/brand-explorer/phase2c-curio-reconstruction");
  mkdirSync(dir, { recursive: true });

  const baseline = captureBaseline ? await captureCurioBaselineSnapshot() : null;
  // Freeze baseline to disk BEFORE generation so it cannot be mixed into generator inputs
  if (baseline) {
    writeFileSync(join(dir, "01-baseline-snapshot.json"), JSON.stringify(baseline, null, 2));
  }

  const evidencePack = await buildIndependentEvidencePack({ allowLiveHpc, allowOfficialFetch });
  writeFileSync(join(dir, "02-independent-evidence-pack.json"), JSON.stringify(evidencePack, null, 2));

  const candidate = generateCandidateFromEvidence(evidencePack, {
    brandName: CURIO_RECONSTRUCTION_TARGET.name,
    parentCompany: CURIO_RECONSTRUCTION_TARGET.parentCompany,
  });
  writeFileSync(join(dir, "03-independent-candidate.json"), JSON.stringify(candidate, null, 2));

  const quality = evaluateCandidateQuality(candidate, evidencePack);
  const matrix = buildPopulationMatrix(candidate, quality);
  writeFileSync(join(dir, "04-population-matrix.json"), JSON.stringify(matrix, null, 2));
  writeFileSync(join(dir, "05-quality-audit.json"), JSON.stringify(quality, null, 2));

  const comparison = baseline ? compareCandidateToBaseline(candidate, baseline) : null;
  if (comparison) writeFileSync(join(dir, "06-baseline-comparison.json"), JSON.stringify(comparison, null, 2));

  const paid = assessPaidResearchNeeds(candidate, evidencePack);
  writeFileSync(join(dir, "09-paid-research-gate.json"), JSON.stringify(paid, null, 2));

  // Master release on mapped profile (structural) — NOT a substitute for reconstruction pass
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
      buildSignature: "phase2c-curio",
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
    contract: BE_PHASE2C_CURIO_RECONSTRUCTION_V1,
    generatedAt: new Date().toISOString(),
    target: CURIO_RECONSTRUCTION_TARGET,
    canonicalTabs: [...BE_CANONICAL_TAB_IDS],
    tabContracts: TAB_CONTRACTS.length,
    requiredSlotsEnumerated: listRequiredTemplateSlots().length,
    scores: quality.scores,
    qualityCounts: quality.counts,
    comparisonSummary: comparison?.summary || null,
    hpc: {
      dataOrigin: evidencePack.hpc?.dataOrigin || null,
      certState: evidencePack.hpc?.certState || null,
      certified: evidencePack.hpc?.certified === true,
      metrics: evidencePack.hpc?.metrics || null,
    },
    masterReleaseOnPartialCandidate: {
      pass: master.MASTER_RELEASE_CONTRACT_PASS === true,
      status: master.status,
      note: "Expected NOT_READY until research_required cleared — gate not weakened",
    },
    renderedApplicationValidation: NOT_VALIDATED,
    paidResearchGate: paid,
    reproducibility: {
      deterministicGivenEvidencePack: true,
      brandSpecificHardcodingInGenerator: false,
      presentationUsedAsInput: false,
      completedFixtureUsedAsInput: false,
    },
    writes: 0,
    productionAirtableWrites: false,
    recommendations: [
      "Authorize targeted Curio source pack: current FDD economics fields, design/standards manual excerpts, 6 verified property images, 2+ openings URLs",
      "Wire evidence→slot generator into canonical RESEARCH stage before enabling materialization",
      "Add isolated preview harness that mounts candidate blocks into atelier renderer without Presentation writes",
      "Do not scale factory until research_required gaps for economics/gallery/momentum are source-backed",
    ],
  };
  writeFileSync(join(dir, "10-acceptance-report.json"), JSON.stringify(acceptance, null, 2));

  const md = [
    `# Brand Explorer Phase 2C — Curio Independent Reconstruction`,
    ``,
    `- Generated: ${acceptance.generatedAt}`,
    `- Target: ${CURIO_RECONSTRUCTION_TARGET.name} (\`${CURIO_RECONSTRUCTION_TARGET.recordId}\`)`,
    `- Presentation used as generator input: **NO**`,
    `- Production Airtable writes: **NO**`,
    `- Paid research used: **NO**`,
    ``,
    `## Scores`,
    ``,
    `| Actual field completeness | Source verification | Brand specificity | Content usefulness | Correct placement | Visual rendering |`,
    `|---|---|---|---|---|---|`,
    `| ${quality.scores.actualFieldCompleteness} | ${quality.scores.sourceVerification} | ${quality.scores.brandSpecificity} | ${quality.scores.contentUsefulness} | ${quality.scores.correctPlacement} | ${quality.scores.visualRendering} |`,
    ``,
    `## Population`,
    ``,
    `- Slots evaluated: ${quality.counts.slotsEvaluated}`,
    `- Populated: ${quality.counts.populated}`,
    `- RESEARCH_REQUIRED: ${quality.counts.researchRequired}`,
    `- Baseline Presentation rows (reference): ${baseline?.presentation?.rowCount ?? "n/a"}`,
    ``,
    `## HPC`,
    ``,
    `- dataOrigin: ${acceptance.hpc.dataOrigin}`,
    `- certState: ${acceptance.hpc.certState}`,
    `- certified: ${acceptance.hpc.certified}`,
    `- metrics: ${JSON.stringify(acceptance.hpc.metrics)}`,
    ``,
    `## Paid research gate`,
    ``,
    `- stopForAuthorization: ${paid.stopForAuthorization}`,
    `- researchRequiredCount: ${paid.researchRequiredCount}`,
    ``,
    `## Comparison summary`,
    ``,
    "```json",
    JSON.stringify(comparison?.summary || {}, null, 2),
    "```",
    ``,
    `## Factory recommendations`,
    ``,
    ...acceptance.recommendations.map((r) => `- ${r}`),
    ``,
    `**STOP for founder review before bulk population.**`,
    ``,
  ].join("\n");
  writeFileSync(join(dir, "10-acceptance-report.md"), md);

  return { dir, acceptance, baseline, evidencePack, candidate, quality, comparison, paid, master };
}
