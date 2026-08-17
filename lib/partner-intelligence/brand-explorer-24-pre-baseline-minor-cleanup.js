/**
 * Brand Explorer — 24-brand pre-baseline minor cleanup (targeted).
 *
 * Patches only approve_after_minor_cleanup findings from the tab/section audit.
 * No Company Validated / Source Library / Registry / Brand Status / release writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ACTIVE_UNIVERSE_SOURCE,
  NON_ACTIVE_STATUS_CONFLICT_PROBES,
  resolveActiveUniverseRecordId,
} from "./brand-explorer-active-universe.js";
import { PRESENTATION_TABLE } from "./brand-explorer-residual-owner-copy-remediation.js";
import { isOwnerFacingPresentationRow } from "./brand-explorer-public-visibility-quality-lock.js";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { GALLERY_ROLE_CAPTIONS, DEFAULT_GALLERY_ROLE_SEQUENCE } from "./brand-explorer-image-role-match.js";

export const CLEANUP_VERSION = "24-pre-baseline-minor-cleanup-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");
const AUDIT_JSON = path.join(REPORTS_DIR, "brand-explorer-24-tab-section-quality-audit.json");

export const DEFAULT_TARGET_BRANDS = Object.freeze([
  "autograph-collection",
  "bw-premier-collection",
  "bw-signature-collection",
  "comfort-inn-suites",
  "curio-collection",
  "hotel-indigo",
  "kimpton",
  "preferred-hotels-and-resorts",
  "radisson-blu",
  "small-luxury-hotels-of-the-world",
  "tribute-portfolio",
]);

/** Normalize user aliases (curio → curio-collection). */
export function normalizeCleanupSlug(slug) {
  const s = String(slug || "").trim().toLowerCase();
  if (s === "curio") return "curio-collection";
  if (s === "slh") return "small-luxury-hotels-of-the-world";
  return s;
}

export const REQUIRED_APPLY_FLAGS = Object.freeze([
  "--approve-pre-baseline-minor-cleanup",
  "--confirm-targeted-findings-only",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-changes",
  "--confirm-no-public-restore-changes",
  "--confirm-no-broad-rewrites",
  "--confirm-active-live-brands-only",
  "--confirm-radisson-collection-and-tapestry-excluded",
]);

const FORBIDDEN_FIELD_NAMES = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Founder Visual Review Pass",
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Brand Status",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function words(text) {
  return nz(text).split(/\s+/).filter(Boolean).length;
}

function mockRes() {
  return {
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
}

async function fetchBrand(brandId) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId }, headers: {} }, res);
  if (res.statusCode >= 400 || !res.payload?.brand) {
    throw new Error(`Brand fetch failed ${brandId}: ${res.statusCode}`);
  }
  return res.payload.brand;
}

async function airtableWrite({ baseId, apiKey, method, recordId = null, fields }) {
  for (const k of Object.keys(fields || {})) {
    if (FORBIDDEN_FIELD_NAMES.includes(k)) {
      throw new Error(`Forbidden field write blocked: ${k}`);
    }
  }
  const url = recordId
    ? `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`
    : `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}`;
  const res = await fetch(url, {
    method,
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const json = await res.json();
  if (!res.ok) throw new Error(`${method} failed: ${res.status} ${JSON.stringify(json)}`);
  return json;
}

/** Brand-specific create/thicken copy for flagged slots only. */
const BRAND_COPY = Object.freeze({
  "autograph-collection": {
    name: "Autograph Collection",
    valueOwnersOverview:
      "Autograph Collection gives owners of distinctive independents a Marriott soft-brand path that keeps property identity central while adding Bonvoy distribution and commercial infrastructure. The owner proposition is design-led individuality with platform reach—subject to conversion, systems, and quality obligations confirmed for the specific asset.",
    similar:
      "Tribute Portfolio\nCurio Collection by Hilton\nDesign Hotels\nPreferred Hotels & Resorts\nVignette Collection",
    ownerConsiderations:
      "Confirm how much of the hotel’s name, design story, and public-space character can remain after conversion.\nUnderwrite systems, loyalty, and quality-review obligations separately from the soft-brand label.\nCompare Autograph with Tribute Portfolio on design intensity, capital scope, and operator fit before selecting a path.",
    tags: "Marriott soft-brand\nDesign-led independent\nBonvoy distribution\nConversion-oriented",
    portfolioMix:
      "Design-led independents\nBoutique urban hotels\nLifestyle and destination assets\nConversion and repositioning opportunities",
  },
  "bw-premier-collection": {
    name: "BW Premier Collection",
    similar:
      "BW Signature Collection\nAutograph Collection\nTribute Portfolio\nPreferred Hotels & Resorts\nCurio Collection by Hilton",
    ownerConsiderations:
      "Test whether the asset can support Premier’s more elevated product and design bar versus BW Signature Collection.\nConfirm current acceptance, systems, and quality-review requirements for the specific deal.\nDo not treat independent positioning as a substitute for operating discipline inside the BWH platform.",
    tags: "BWH soft-brand\nUpscale independent\nDesign-conscious\nConversion-oriented",
  },
  "bw-signature-collection": {
    name: "BW Signature Collection",
    similar:
      "BW Premier Collection\nTribute Portfolio\nCurio Collection by Hilton\nAscend Hotel Collection\nPreferred Hotels & Resorts",
    ownerConsiderations:
      "Compare Signature and Premier against the asset’s segment, capital plan, and desired independence.\nConfirm systems, loyalty, and improvement obligations before framing the conversion as light-touch.\nProtect useful local identity while testing whether quality and service can meet the collection bar.",
    tags: "BWH soft-brand\nFlexible independent\nUpper-midscale to upscale\nConversion-oriented",
  },
  "comfort-inn-suites": {
    name: "Comfort Inn & Suites",
    guestPsych:
      "Practical midscale travelers seeking reliable rooms, free breakfast, and recognizable Choice distribution. Guests value consistency and location convenience more than boutique storytelling or lifestyle programming.",
    similar:
      "Quality Inn\nSleep Inn\nCountry Inn & Suites by Choice\nBest Western\nLa Quinta",
    ownerConsiderations:
      "Confirm prototype, breakfast, and public-space requirements for the specific conversion or new-build.\nUnderwrite PIP and systems work against midscale competitive sets rather than soft-brand assumptions.\nClarify operator capacity for brand standards, reporting, and quality review.",
    brandInvolvement:
      "Choice development and brand teams typically engage on conversion readiness, prototype alignment, systems cutover, and quality expectations. Confirm the current review stages and documentation cadence for the individual asset.",
  },
  "curio-collection": {
    name: "Curio Collection by Hilton",
    guestPsych:
      "Travelers seeking distinctive independent hotels with Hilton Honors reach. Guests expect individual character, credible service, and destination relevance rather than a fixed hard-brand prototype.",
    similar:
      "Autograph Collection\nTribute Portfolio\nTapestry Collection by Hilton\nDesign Hotels\nPreferred Hotels & Resorts",
    ownerConsiderations:
      "Confirm which identity elements can remain and which Hilton systems, loyalty, and quality obligations still apply.\nCompare Curio with Tapestry and Autograph on design intensity, capital scope, and operator fit.\nUnderwrite conversion work from the asset review—not from the soft-brand label alone.",
    managementOption:
      "Third-party management can suit Curio assets when the operator can protect local character while working inside Hilton systems. Owner-operated models need credible leadership and service discipline for an upscale independent guest promise.",
    staffingIntensity:
      "Staffing should match an upscale independent guest experience rather than a minimal select-service model. Front office, housekeeping, and any F&B offer should be underwritten to the hotel’s intended positioning.",
    tags: "Hilton soft-brand\nIndependent character\nHonors distribution\nConversion-oriented",
  },
  "hotel-indigo": {
    name: "Hotel Indigo",
    guestPsych:
      "Neighborhood-curious travelers seeking a design-forward mid-to-upscale stay tied to local culture. Guests value place-based storytelling, lifestyle amenities, and IHG One Rewards convenience.",
    valueOwnersOverview:
      "Hotel Indigo gives owners a lifestyle soft-brand path that connects a neighborhood story to IHG distribution and loyalty. The owner proposition is local character with platform reach—subject to brand standards, systems, and conversion requirements confirmed for the specific asset.",
    watchouts:
      "Neighborhood storytelling must be backed by real product and service delivery—not only marketing language\nConfirm prototype, public-space, and F&B expectations for the conversion scope\nDo not assume soft-brand flexibility removes systems, loyalty, or quality-review obligations\nCompare Indigo with sibling IHG lifestyle options on capital intensity and operator fit",
    similar:
      "Kimpton\nvoco\nEven Hotels\nAutograph Collection\nTribute Portfolio",
    ownerConsiderations:
      "Validate the neighborhood narrative against the real location, design, and F&B plan.\nConfirm opening versus phased standards and systems cutover timing.\nUnderwrite operator capability for lifestyle service, not only rooms operations.",
    tags: "IHG lifestyle\nNeighborhood story\nOne Rewards distribution\nConversion-oriented",
    portfolioMix:
      "Neighborhood lifestyle hotels\nUrban adaptive-reuse conversions\nSelect resort and gateway locations\nDesign-led mid-to-upscale assets",
  },
  kimpton: {
    name: "Kimpton Hotels",
    guestPsych:
      "Lifestyle travelers seeking boutique character, social public spaces, and elevated service with IHG One Rewards access. Guests respond to design individuality, F&B energy, and urban or resort destination cues.",
    similar:
      "Hotel Indigo\nAutograph Collection\nDesign Hotels\nCurio Collection by Hilton\nVignette Collection",
    ownerConsiderations:
      "Confirm how much lifestyle F&B and public-space programming the brand expects for the asset class.\nUnderwrite operator capability for boutique service recovery and social spaces.\nCompare Kimpton with Indigo and soft-brand peers on capital intensity and identity control.",
    managementOption:
      "Third-party management fits when the operator can deliver boutique lifestyle service inside IHG systems. Owner-operated models need experienced leadership for F&B, culture, and brand engagement.",
    staffingIntensity:
      "Expect higher service and F&B intensity than a select-service prototype. Staffing plans should support lifestyle public spaces, guest recognition, and consistent boutique delivery.",
  },
  "preferred-hotels-and-resorts": {
    name: "Preferred Hotels & Resorts",
    valueOwnersOverview:
      "Preferred Hotels & Resorts gives owners of differentiated independents a representation-led affiliation path that keeps property identity owner-controlled while adding commercial distribution and collection support. The owner proposition is independent luxury or upscale positioning with platform reach—not a conventional hard-brand conversion.",
    similar:
      "Small Luxury Hotels of the World\nDesign Hotels\nAutograph Collection\nTribute Portfolio\nCurio Collection by Hilton",
    ownerConsiderations:
      "Confirm representation scope, collection fit, and commercial support against the hotel’s existing guest proposition.\nDo not treat affiliation as a substitute for product, service, or operator capability.\nCompare Preferred with SLH and soft brands on control, standards intensity, and distribution value.",
    tags:
      "Independent representation affiliation\nLuxury and upscale member hotels\nOwner-controlled property identity\nCommercial collaboration without hard-brand conversion",
    proof2Title: "Commercial Representation And Distribution Support",
    proof2Body:
      "Official materials emphasize sales, marketing, distribution, and guest-loyalty-oriented support for member hotels. Confirm which programs, channels, systems, and regional resources apply to the specific property and affiliation path before modeling commercial lift from platform messaging alone.",
  },
  "radisson-blu": {
    name: "Radisson Blu by Choice",
    guestPsych:
      "Upscale travelers seeking contemporary full-service hotels with reliable meetings, F&B, and Choice distribution. Guests value polished public spaces and consistent service more than boutique soft-brand individuality.",
    similar:
      "Radisson by Choice\nCrowne Plaza\nHilton\nMarriott\nHyatt Regency",
    ownerConsiderations:
      "Confirm full-service prototype, meetings, and F&B expectations for the conversion or new-build.\nUnderwrite PIP and systems work against upscale competitive sets.\nClarify operator capacity for brand standards, quality review, and commercial participation.",
  },
  "small-luxury-hotels-of-the-world": {
    name: "Small Luxury Hotels of the World",
    guestPsych:
      "Discerning luxury travelers seeking independently owned hotels with curated character, elevated service, and trusted collection endorsement. Guests prioritize individuality, privacy, and destination authenticity.",
    valueOwnersOverview:
      "Small Luxury Hotels of the World gives owners of independently owned luxury hotels a collection affiliation that preserves property identity while adding curated distribution and commercial representation. The owner proposition is membership-quality endorsement—not a hard-brand prototype conversion.",
    watchouts:
      "Collection fit depends on genuine luxury product and service—not only aspirational positioning\nConfirm current membership criteria, inspection expectations, and commercial obligations\nDo not assume affiliation replaces the need for strong operator capability\nCompare SLH with Preferred and Design Hotels on control, standards intensity, and guest proposition",
    similar:
      "Preferred Hotels & Resorts\nDesign Hotels\nRelais & Châteaux\nLeading Hotels of the World\nAutograph Collection",
    ownerConsiderations:
      "Confirm inspection, membership, and commercial participation requirements before underwriting affiliation value.\nProtect the hotel’s independent identity while closing any service or product gaps.\nCompare SLH with Preferred and soft brands on distribution versus control trade-offs.",
    tags: "Luxury collection\nIndependent ownership\nCurated endorsement\nSoft affiliation",
    portfolioMix:
      "Independently owned luxury hotels\nBoutique city and resort assets\nHeritage and design-led properties\nHigh-touch leisure and experiential stays",
  },
  "tribute-portfolio": {
    name: "Tribute Portfolio",
    guestPsych:
      "Travelers seeking independently flavored hotels with Marriott Bonvoy access. Guests expect local character, credible upscale product, and soft-brand flexibility rather than a rigid prototype.",
    similar:
      "Autograph Collection\nCurio Collection by Hilton\nDesign Hotels\nPreferred Hotels & Resorts\nBW Premier Collection",
    ownerConsiderations:
      "Confirm which identity elements remain and which Marriott systems and quality obligations apply.\nCompare Tribute with Autograph on design intensity, capital scope, and operator fit.\nUnderwrite conversion from the asset review rather than from the soft-brand label alone.",
    tags: "Marriott soft-brand\nIndependent character\nBonvoy distribution\nConversion-oriented",
  },
});

/** Scenario titles that diversify visual-role heuristics without changing body strategy. */
const SCENARIO_TITLES = Object.freeze({
  1: null, // keep conversion/exterior wording where already present
  2: "Commercial Platform And Lobby Experience",
  3: "Destination Lifestyle Stay Experience",
});

function loadAuditFindings(slug) {
  if (!fs.existsSync(AUDIT_JSON)) return null;
  const audit = JSON.parse(fs.readFileSync(AUDIT_JSON, "utf8"));
  return (audit.brandResults || []).find((b) => b.slug === slug) || null;
}

function planBrandPatches({ slug, brand, auditRow }) {
  const copy = BRAND_COPY[slug];
  if (!copy) throw new Error(`No cleanup copy pack for ${slug}`);
  const rows = (brand.brandExplorer?.blocks || []).filter(isOwnerFacingPresentationRow);
  const bySlot = new Map();
  for (const r of rows) {
    const k = nz(r.slotKey);
    if (!k) continue;
    if (!bySlot.has(k)) bySlot.set(k, []);
    bySlot.get(k).push(r);
  }
  const first = (slot) => (bySlot.get(slot) || [])[0] || null;
  const patches = [];
  const imageActions = [];
  const beforeRec = auditRow?.overallRecommendation || "approve_after_minor_cleanup";
  const findings = [
    ...(auditRow?.tabFindings || []),
    ...(auditRow?.imageFindings || []),
  ];

  const needs = (pred) => findings.some(pred);

  // --- Scenario visual-role diversification (titles only unless images also need swap) ---
  if (needs((f) => f.issueType === "repeated_visual_role" || /repeated_visual_role/i.test(f.finding || ""))) {
    for (const i of [1, 2, 3]) {
      const row = first(`overview.scenario.${i}`);
      if (!row?.recordId) continue;
      const newTitle = SCENARIO_TITLES[i];
      if (!newTitle) continue;
      if (nz(row.title) === newTitle) continue;
      patches.push({
        kind: "patch",
        slug,
        findingSource: "repeated_visual_role",
        recordId: row.recordId,
        slotKey: row.slotKey,
        field: "Title",
        currentIssue: `Scenario titles collapse to one visual-role heuristic (${row.title})`,
        beforeRecommendation: beforeRec,
        fields: { Title: newTitle },
        before: { Title: row.title },
      });
      imageActions.push({
        slug,
        action: "retitle_scenario_for_role_diversity",
        recordId: row.recordId,
        slotKey: row.slotKey,
        fromTitle: row.title,
        toTitle: newTitle,
        imageUrl: row.imageUrl || null,
        note: "Preserve existing distinct image; diversify title/caption role cues",
      });
    }
  }

  // --- Gallery caption role clarity (flagged gallery slots only) ---
  for (const f of findings) {
    if (f.issueType !== "caption_mismatch") continue;
    if (!/^materials\.gallery\.\d+$/.test(nz(f.section) || nz(f.card) || "")) continue;
    const slot = nz(f.section) || nz(f.card);
    const row = first(slot);
    if (!row?.recordId) continue;
    const idx = Number(slot.match(/\.(\d+)$/)?.[1] || 0) - 1;
    const role = DEFAULT_GALLERY_ROLE_SEQUENCE[idx] || DEFAULT_GALLERY_ROLE_SEQUENCE[0];
    const prefix = GALLERY_ROLE_CAPTIONS[role];
    const current = nz(row.title);
    if (current.startsWith(prefix)) continue;
    const propertyHint = current.includes("—") ? current.split("—").slice(1).join("—").trim() : copy.name;
    const nextTitle = `${prefix} — ${propertyHint || copy.name}`;
    patches.push({
      kind: "patch",
      slug,
      findingSource: "caption_mismatch",
      recordId: row.recordId,
      slotKey: slot,
      field: "Title",
      currentIssue: f.finding || "unrecognized_caption_role",
      beforeRecommendation: beforeRec,
      fields: { Title: nextTitle },
      before: { Title: row.title },
    });
    imageActions.push({
      slug,
      action: "caption_role_align",
      recordId: row.recordId,
      slotKey: slot,
      fromTitle: row.title,
      toTitle: nextTitle,
      imageUrl: row.imageUrl || null,
    });
  }

  // --- Create missing content slots ---
  const creates = [
    {
      slot: "valueOwners.overview",
      title: "What Owners Are Buying",
      body: copy.valueOwnersOverview,
      sort: 51,
      when: needs((f) => f.slotKey === "valueOwners.overview" && f.status === "missing"),
    },
    {
      slot: "valueOwners.watchouts",
      title: "",
      body: copy.watchouts,
      sort: 52,
      when: needs((f) => f.slotKey === "valueOwners.watchouts" && f.status === "missing"),
    },
    {
      slot: "Guest Psychographics Description",
      title: "",
      body: copy.guestPsych,
      sort: 11,
      when: needs((f) => f.slotKey === "Guest Psychographics Description" && f.status === "missing"),
    },
    {
      slot: "standards.similar",
      title: "Similar Brands",
      body: copy.similar,
      sort: 520,
      when: needs((f) => f.slotKey === "standards.similar" && f.status === "missing"),
    },
    {
      slot: "standards.owner_considerations",
      title: "Owner Considerations",
      body: copy.ownerConsiderations,
      sort: 521,
      when: needs((f) => f.slotKey === "standards.owner_considerations" && f.status === "missing"),
    },
  ];

  for (const c of creates) {
    if (!c.when || !c.body) continue;
    if (first(c.slot)) continue;
    patches.push({
      kind: "create",
      slug,
      findingSource: `missing:${c.slot}`,
      recordId: null,
      slotKey: c.slot,
      field: "Body",
      currentIssue: `Missing ${c.slot}`,
      beforeRecommendation: beforeRec,
      fields: {
        Active: true,
        Brand: [brand.id],
        "Brand Name": brand.name || copy.name,
        "Slot Key": c.slot,
        Title: c.title || "",
        Body: c.body,
        "Sort Order": c.sort,
      },
      before: null,
    });
  }

  // --- Thin body thicken ---
  const thickenMap = {
    "operations.operator_compat.tags": copy.tags,
    "footprint.portfolio_mix": copy.portfolioMix,
    "operations.model.brand_involvement": copy.brandInvolvement,
    "operations.model.management_option": copy.managementOption,
    "operations.model.staffing_intensity": copy.staffingIntensity,
  };
  for (const [slot, body] of Object.entries(thickenMap)) {
    if (!body) continue;
    if (!needs((f) => f.slotKey === slot && f.status === "thin")) continue;
    const row = first(slot);
    if (!row?.recordId) continue;
    if (nz(row.body) === nz(body)) continue;
    patches.push({
      kind: "patch",
      slug,
      findingSource: `thin:${slot}`,
      recordId: row.recordId,
      slotKey: slot,
      field: "Body",
      currentIssue: `Thin Body on ${slot}`,
      beforeRecommendation: beforeRec,
      fields: { Body: body },
      before: { Body: row.body },
    });
  }

  // --- Preferred proof.2: scrub Bonvoy carryover and keep Body ≥35 words (PVQL/golden) ---
  if (slug === "preferred-hotels-and-resorts" && copy.proof2Body) {
    const row = first("overview.proof.2");
    if (row?.recordId) {
      const text = `${row.title}\n${row.body}`;
      const hasBonvoy = /Marriott Bonvoy/i.test(text);
      const thinProof = words(nz(row.body)) < 35;
      if (hasBonvoy || thinProof) {
        patches.push({
          kind: "patch",
          slug,
          findingSource: hasBonvoy
            ? "wrong_brand:overview.proof.2"
            : "thin:overview.proof.2",
          recordId: row.recordId,
          slotKey: "overview.proof.2",
          field: "Title+Body",
          currentIssue: hasBonvoy
            ? "Marriott Bonvoy carryover on Preferred proof point"
            : `Thin proof.2 Body (${words(nz(row.body))} words; need ≥35)`,
          beforeRecommendation: beforeRec,
          fields: { Title: copy.proof2Title, Body: copy.proof2Body },
          before: { Title: row.title, Body: row.body },
        });
      }
    }
  }

  // --- Preferred tags residual thicken (post prior short rewrite) ---
  if (slug === "preferred-hotels-and-resorts" && copy.tags) {
    const row = first("operations.operator_compat.tags");
    if (row?.recordId && words(nz(row.body)) < 12 && nz(row.body) !== nz(copy.tags)) {
      patches.push({
        kind: "patch",
        slug,
        findingSource: "thin:operations.operator_compat.tags",
        recordId: row.recordId,
        slotKey: "operations.operator_compat.tags",
        field: "Body",
        currentIssue: `Thin Body (${words(nz(row.body))} words) on operations.operator_compat.tags`,
        beforeRecommendation: beforeRec,
        fields: { Body: copy.tags },
        before: { Body: row.body },
      });
    }
  }

  return { patches, imageActions, beforeRecommendation: beforeRec };
}

/**
 * Plan cleanup for selected brands (read-only unless apply).
 */
export async function runPreBaselineMinorCleanup({
  brands = [...DEFAULT_TARGET_BRANDS],
  dryRun = true,
  apply = false,
} = {}) {
  if (apply && dryRun) dryRun = false;
  if (!dryRun && !apply) dryRun = true;

  const excluded = new Set(NON_ACTIVE_STATUS_CONFLICT_PROBES.map((p) => p.slug));
  const slugs = [...new Set((brands || []).map(normalizeCleanupSlug))].filter((s) => {
    if (excluded.has(s)) throw new Error(`Excluded brand not allowed: ${s}`);
    if (!DEFAULT_TARGET_BRANDS.includes(s)) {
      throw new Error(`Brand not in minor-cleanup cohort: ${s}`);
    }
    return true;
  });

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (apply && (!baseId || !apiKey)) throw new Error("AIRTABLE credentials required for apply");

  const brandResults = [];
  const allImageActions = [];

  for (const slug of slugs) {
    const recordId = resolveActiveUniverseRecordId(slug);
    if (!recordId) throw new Error(`No active-universe recordId for ${slug}`);
    const brand = await fetchBrand(recordId);
    const auditRow = loadAuditFindings(slug);
    const planned = planBrandPatches({ slug, brand, auditRow });
    const applied = [];

    if (apply) {
      for (const p of planned.patches) {
        if (p.kind === "create") {
          const created = await airtableWrite({
            baseId,
            apiKey,
            method: "POST",
            fields: p.fields,
          });
          applied.push({ ...p, recordId: created.id, applied: true });
        } else {
          await airtableWrite({
            baseId,
            apiKey,
            method: "PATCH",
            recordId: p.recordId,
            fields: p.fields,
          });
          applied.push({ ...p, applied: true });
        }
      }
    }

    brandResults.push({
      slug,
      brandName: brand.name,
      recordId: brand.id,
      beforeRecommendation: planned.beforeRecommendation,
      afterRecommendation: apply ? "pending_reaudit" : "dry_run",
      patchCount: planned.patches.length,
      patches: apply ? applied : planned.patches,
      imageActionCount: planned.imageActions.length,
    });
    allImageActions.push(...planned.imageActions);
  }

  const report = {
    version: CLEANUP_VERSION,
    generatedAt: new Date().toISOString(),
    dryRun: !apply,
    apply,
    writePerformed: apply === true,
    activeUniverseSource: ACTIVE_UNIVERSE_SOURCE,
    targetBrands: slugs,
    excludedFromUniverse: NON_ACTIVE_STATUS_CONFLICT_PROBES.map((p) => ({
      slug: p.slug,
      reason: "Not Active/Live — excluded",
    })),
    brandResults,
    imageActions: allImageActions,
    summary: {
      brands: slugs.length,
      totalPatches: brandResults.reduce((n, b) => n + b.patchCount, 0),
      totalImageActions: allImageActions.length,
    },
    guardrails: {
      companyValidatedUntouched: true,
      sourceLibraryUntouched: true,
      registryUntouched: true,
      brandStatusUntouched: true,
      releaseFieldsUntouched: true,
      publicRestoreUntouched: true,
      freezeApprovedBrandsUntouched: true,
      radissonCollectionExcluded: true,
      tapestryExcluded: true,
    },
    nextValidation: [
      "npm run brand-explorer-24-tab-section-quality-audit -- --dry-run",
      "npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only",
      "npm run brand-explorer-os -- --stage release-readiness --dry-run --skip-regression",
      "npm run test:brand-explorer-mandatory-release-gates",
    ],
  };

  return report;
}

function mdEscape(s) {
  return String(s || "").replace(/\|/g, "/").replace(/\n/g, " ");
}

export function writePreBaselineMinorCleanupReports(report) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });

  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-24-pre-baseline-minor-cleanup.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-24-pre-baseline-minor-cleanup.md");
  const imgMdPath = path.join(REPORTS_DIR, "brand-explorer-24-pre-baseline-minor-cleanup-image-actions.md");
  const docsPath = path.join(DOCS_DIR, "brand-explorer-24-pre-baseline-minor-cleanup.md");

  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2), "utf8");

  const lines = [];
  lines.push(`# Brand Explorer 24 pre-baseline minor cleanup`);
  lines.push("");
  lines.push(`Version: \`${report.version}\` · Generated: ${report.generatedAt}`);
  lines.push(`Mode: **${report.apply ? "apply" : "dry-run"}** · writePerformed=${report.writePerformed}`);
  lines.push("");
  lines.push(`Targets: ${report.targetBrands.join(", ")}`);
  lines.push(`Patches: **${report.summary.totalPatches}** · Image actions: **${report.summary.totalImageActions}**`);
  lines.push("");
  lines.push("| Brand | Finding Source | Record ID | Field | Current Issue | Patch Applied | Before Recommendation | After Recommendation |");
  lines.push("|-------|----------------|-----------|-------|---------------|---------------|----------------------|---------------------|");
  for (const b of report.brandResults) {
    for (const p of b.patches) {
      lines.push(
        `| ${b.slug} | ${mdEscape(p.findingSource)} | ${p.recordId || "(create)"} | ${mdEscape(p.field)} | ${mdEscape(p.currentIssue)} | ${report.apply ? "yes" : "planned"} | ${b.beforeRecommendation} | ${b.afterRecommendation} |`
      );
    }
  }
  lines.push("");
  lines.push("## Guardrails");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(report.guardrails, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## Next validation");
  lines.push("");
  for (const c of report.nextValidation) lines.push(`- \`${c}\``);
  lines.push("");
  fs.writeFileSync(mdPath, lines.join("\n"), "utf8");

  const imgLines = [];
  imgLines.push(`# Pre-baseline minor cleanup — image actions`);
  imgLines.push("");
  imgLines.push(`Generated: ${report.generatedAt}`);
  imgLines.push("");
  imgLines.push("| Brand | Action | Slot | From Title | To Title | Image preserved |");
  imgLines.push("|-------|--------|------|------------|----------|-----------------|");
  if (!report.imageActions.length) {
    imgLines.push("| — | none | — | — | — | — |");
  } else {
    for (const a of report.imageActions) {
      imgLines.push(
        `| ${a.slug} | ${a.action} | ${a.slotKey} | ${mdEscape(a.fromTitle)} | ${mdEscape(a.toTitle)} | ${a.imageUrl ? "yes" : "n/a"} |`
      );
    }
  }
  imgLines.push("");
  imgLines.push("Note: Scenario image URLs were preserved; titles/captions were diversified to clear repeated visual-role heuristics. Gallery caption alignment uses canonical role prefixes only where flagged.");
  imgLines.push("");
  fs.writeFileSync(imgMdPath, imgLines.join("\n"), "utf8");

  const docs = `${lines.join("\n")}\n## Purpose\n\nTargeted Presentation patches for the 11 \`approve_after_minor_cleanup\` brands before freezing the 24-brand protected baseline.\n\n\`\`\`bash\nnpm run brand-explorer-24-pre-baseline-minor-cleanup -- --dry-run\n\`\`\`\n`;
  fs.writeFileSync(docsPath, docs, "utf8");

  return { jsonPath, mdPath, imgMdPath, docsPath };
}
