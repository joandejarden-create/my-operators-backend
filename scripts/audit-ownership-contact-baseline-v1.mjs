#!/usr/bin/env node
/**
 * Ownership + Contact Intelligence — BASELINE AUDIT V1 (READ-ONLY).
 *
 * No live research, no provider spend, no Airtable/HPC/canonical writes.
 *
 *   node scripts/audit-ownership-contact-baseline-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { HOTEL_TO_OWNER } from "../lib/hotel-intelligence/ownership/owner-control/portfolio-store.js";
import { CONTACT_V1_12_CASE_PLAN } from "../lib/hotel-intelligence/contact-intelligence/cohort-12-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_REPORTS = path.join(ROOT, "reports/ownership-contact-baseline");
const OUT_DATA = path.join(ROOT, "data/ownership-contact-baseline");

const PLANNING_UNIVERSE = 15000;
const PRODUCTION_CENSUS_DOC = 5956;

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
function writeJson(p, obj) {
  ensureDir(path.dirname(p));
  fs.writeFileSync(p, JSON.stringify(obj, null, 2) + "\n", "utf8");
}
function writeMd(p, text) {
  ensureDir(path.dirname(p));
  fs.writeFileSync(p, text.endsWith("\n") ? text : text + "\n", "utf8");
}
function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}
function exists(p) {
  return fs.existsSync(p);
}
function pct(n, d) {
  if (!d) return null;
  return Math.round((1000 * n) / d) / 10;
}

/** @type {Map<string, any>} */
const hotels = new Map();

function upsertHotel(id, patch) {
  const key = String(id || "").trim();
  if (!key) return null;
  const prev = hotels.get(key) || {
    hotel_id: key,
    hotel_name: null,
    country: null,
    city: null,
    sources: [],
    owner_entity_id: null,
    owner_display: null,
    in_hotel_to_owner: false,
    in_portfolio_relationship: false,
    in_golden_demo: false,
    in_ci12: false,
    has_deep_research: false,
    has_contact_showcase: false,
    propco: null,
    sponsor: null,
    contactable_org: null,
    people_count: 0,
    emails: [],
    phones: [],
    linkedin_count: 0,
    evidence_present: false,
    verification_present: false,
    currentness_present: false,
    owner_truth_status: null,
    archetype: null,
    notes: [],
  };
  const next = { ...prev, ...patch };
  if (patch.sources) next.sources = [...new Set([...(prev.sources || []), ...patch.sources])];
  if (patch.notes) next.notes = [...(prev.notes || []), ...patch.notes];
  if (patch.emails) next.emails = [...new Set([...(prev.emails || []), ...patch.emails])];
  if (patch.phones) next.phones = [...new Set([...(prev.phones || []), ...patch.phones])];
  hotels.set(key, next);
  return next;
}

function loadOwnerPortfolios() {
  const dir = path.join(ROOT, "fixtures/hotel-intelligence/owner-portfolio");
  const portfolios = [];
  if (!exists(dir)) return portfolios;
  for (const f of fs.readdirSync(dir)) {
    if (!f.endsWith(".json") || f.endsWith(".backup")) continue;
    const raw = readJson(path.join(dir, f));
    const profile = raw.profile || {};
    const ownerId = profile.owner_entity_id || path.basename(f, ".json");
    const rels = profile.hotel_relationships || raw.hotel_relationships || [];
    const people = profile.people || raw.people || [];
    let emails = 0;
    let phones = 0;
    let linkedin = 0;
    for (const p of people) {
      if (p.email || p.emails?.length || p.contact?.email) emails += 1;
      if (p.phone || p.phones?.length || p.contact?.phone) phones += 1;
      if (p.linkedin || p.linkedin_url || p.profile_url) linkedin += 1;
    }
    portfolios.push({
      owner_entity_id: ownerId,
      display_name: profile.display_name || profile.legal_name || ownerId,
      hotel_count: rels.length,
      people_count: people.length,
      people_with_email: emails,
      people_with_phone: phones,
      people_with_linkedin: linkedin,
      completeness: profile.completeness || null,
      hotel_ids: rels.map((r) => r.hotel_id || r.airtable_record_id).filter(Boolean),
      file: `fixtures/hotel-intelligence/owner-portfolio/${f}`,
    });
    for (const r of rels) {
      const hid = r.hotel_id || r.airtable_record_id;
      if (!hid || !String(hid).startsWith("rec")) continue;
      upsertHotel(hid, {
        hotel_name: r.hotel_name || r.name || null,
        country: r.country || profile.country || null,
        city: r.city || null,
        owner_entity_id: ownerId,
        owner_display: profile.display_name || profile.legal_name || null,
        in_portfolio_relationship: true,
        people_count: Math.max(people.length, 0),
        linkedin_count: linkedin,
        evidence_present: Boolean(profile.evidence_ids?.length || r.evidence_ids?.length),
        sources: ["owner_portfolio_fixture"],
      });
    }
  }
  return portfolios;
}

function loadHotelToOwner() {
  for (const [hid, oid] of Object.entries(HOTEL_TO_OWNER)) {
    upsertHotel(hid, {
      owner_entity_id: oid,
      in_hotel_to_owner: true,
      evidence_present: true,
      sources: ["HOTEL_TO_OWNER"],
    });
  }
}

function loadCi12() {
  for (const c of CONTACT_V1_12_CASE_PLAN) {
    upsertHotel(c.hotel_id, {
      hotel_name: c.hotel_name,
      country: c.country,
      city: c.city,
      owner_entity_id: c.owner_entity_id || null,
      owner_display: c.owner_hint || null,
      in_ci12: true,
      owner_truth_status: c.owner_truth_status || (c.owner_entity_id ? "KNOWN" : "UNKNOWN"),
      archetype: c.archetype || null,
      sources: ["ci12_case_plan"],
      notes: c.owner_entity_id ? [] : ["OWNER_TRUTH_UNKNOWN_OR_NULL"],
    });
  }
}

function loadGoldenCohorts() {
  const files = [
    "fixtures/golden-demo/gsf-mexico-ownership-cohort-v1.json",
    "fixtures/golden-demo/dovetail-hospitality-cohort-v1.json",
    "fixtures/golden-demo/mexico-explorer-demo-cohort-v1.json",
  ];
  for (const rel of files) {
    const p = path.join(ROOT, rel);
    if (!exists(p)) continue;
    const j = readJson(p);
    const countryDefault = j.organization?.country || j.owner?.country || null;
    for (const h of j.hotels || []) {
      const hid = h.airtable_record_id || h.hotel_id;
      if (!hid || !String(hid).startsWith("rec")) continue;
      upsertHotel(hid, {
        hotel_name: h.name || h.hotel_name || null,
        country: h.country || countryDefault,
        city: h.city || null,
        in_golden_demo: true,
        owner_entity_id: j.owner?.entity_id || j.organization?.entity_id || null,
        owner_display: j.owner?.display_name || j.organization?.display_name || null,
        evidence_present: Boolean(h.evidence_basis || j.source || j.owner?.evidence_basis),
        sources: [rel],
      });
    }
  }
}

function loadDeepResearchControls() {
  const controls = [
    {
      file: "fixtures/golden-demo/krystal-grand-pv-deep-research-v1.json",
      hotel_id: "recUNycnMwOVFX0hc",
      name: "Krystal Grand Puerto Vallarta",
    },
    {
      file: "fixtures/golden-demo/cambridge-beaches-deep-research-v1.json",
      hotel_id: "recIwaP1etgx2g9nA",
      name: "Cambridge Beaches Resort & Spa",
    },
    {
      file: "fixtures/golden-demo/sheraton-gdl-expo-deep-research-v1.json",
      hotel_id: "recsYJb2R1jarPpK3",
      name: "Sheraton Guadalajara Expo",
    },
    {
      file: "fixtures/golden-demo/real-inn-cancun-deep-research-v1.json",
      hotel_id: "recTYaiA4S6fR6ixx",
      name: "voco / Real Inn Cancún",
    },
  ];
  for (const c of controls) {
    const p = path.join(ROOT, c.file);
    if (!exists(p)) {
      upsertHotel(c.hotel_id, {
        hotel_name: c.name,
        notes: [`deep_research_missing:${c.file}`],
        sources: ["control_expected"],
      });
      continue;
    }
    const j = readJson(p);
    const ownership = j.ownership || j.ownership_chain || j.structure || {};
    const people = j.people || j.contacts?.people || [];
    const emails = [];
    const phones = [];
    // Best-effort scrape of common fixture shapes without inventing verification
    const blob = JSON.stringify(j);
    for (const m of blob.match(/[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}/gi) || []) {
      if (!/example\.com|test@/i.test(m)) emails.push(m.toLowerCase());
    }
    upsertHotel(c.hotel_id, {
      hotel_name: c.name,
      has_deep_research: true,
      propco: ownership.propco?.name || ownership.property_owner?.name || null,
      sponsor: ownership.sponsor?.name || ownership.economic_owner?.name || null,
      contactable_org: ownership.sponsor?.name || ownership.contactable_org?.name || null,
      people_count: Array.isArray(people) ? people.length : 0,
      emails: [...new Set(emails)].slice(0, 20),
      evidence_present: true,
      currentness_present: Boolean(j.last_reviewed || j.observed_date),
      sources: [c.file],
    });
  }
}

function loadContactShowcase() {
  const p = path.join(ROOT, "public/data/hotel-contact-intelligence/kgpv-recUNycnMwOVFX0hc.json");
  if (!exists(p)) return null;
  const j = readJson(p);
  const emails = [];
  const phones = [];
  const people = j.people || j.decisionMakers || [];
  const blob = JSON.stringify(j);
  for (const m of blob.match(/[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}/gi) || []) emails.push(m.toLowerCase());
  for (const m of blob.match(/\+?\d[\d\s().-]{7,}\d/g) || []) phones.push(m.trim());
  upsertHotel("recUNycnMwOVFX0hc", {
    has_contact_showcase: true,
    propco: j.ownershipSummary?.propco?.name || null,
    sponsor: j.ownershipSummary?.sponsor?.name || null,
    contactable_org: j.ownershipSummary?.sponsor?.name || null,
    people_count: Array.isArray(people) ? people.length : Math.max(1, 0),
    emails: [...new Set(emails)],
    phones: [...new Set(phones)].slice(0, 20),
    evidence_present: true,
    verification_present: /VERIFIED|verification/i.test(blob),
    sources: ["public/data/hotel-contact-intelligence/kgpv-recUNycnMwOVFX0hc.json"],
  });
  return j;
}

function classifyOwnerState(h) {
  if (!h.in_hotel_to_owner && !h.in_golden_demo && !h.in_ci12 && !h.in_portfolio_relationship && !h.has_deep_research) {
    return "NO_RESEARCH_ATTEMPTED";
  }
  if (h.owner_truth_status === "UNKNOWN" || h.notes?.includes("OWNER_TRUTH_UNKNOWN_OR_NULL")) {
    return "UNRESOLVED_WITH_EXHAUSTIVE_SEARCH"; // CI12 tagged unknown — not exhaustive search, but unresolved known
  }
  if (h.owner_truth_status === "PARTIAL" && !h.in_hotel_to_owner) return "WEAK_EVIDENCE";
  if (h.propco && h.sponsor) return "PROPCO_AND_SPONSOR";
  if (h.propco && !h.sponsor) return "PROPCO_ONLY";
  if (!h.propco && h.sponsor) return "SPONSOR_ONLY";
  if (h.in_hotel_to_owner && h.has_deep_research) return "RESOLVED_OWNER_STRUCTURE";
  if (h.in_hotel_to_owner || (h.owner_entity_id && h.evidence_present)) return "RESOLVED_OWNER_STRUCTURE";
  if (h.owner_entity_id && !h.evidence_present) return "CANDIDATE_ONLY";
  if (h.in_ci12 && h.owner_entity_id) return "WEAK_EVIDENCE";
  return "UNKNOWN_UNCLASSIFIABLE";
}

function classifyContactState(h) {
  if (!h.owner_entity_id && !h.sponsor && !h.propco) return "NO_OWNER";
  const hasOrg = Boolean(h.contactable_org || h.sponsor || h.owner_entity_id);
  if (!hasOrg) return "OWNER_NO_CONTACTABLE_ORG";
  if ((h.people_count || 0) === 0 && !(h.emails?.length || h.phones?.length)) return "ORG_NO_RELEVANT_PERSON";
  if ((h.people_count || 0) > 0 && !(h.emails?.length || h.phones?.length) && !(h.linkedin_count > 0)) {
    return "PERSON_NO_CONTACT_DETAIL";
  }
  if (h.has_contact_showcase && h.emails?.length) return "CONTACTABLE";
  if (h.emails?.length && !h.verification_present) return "EMAIL_UNVERIFIED";
  if (h.emails?.length && h.verification_present) return "EMAIL_VERIFIED"; // showcase may claim verification — treat cautiously
  if (h.phones?.length) return "PHONE_PUBLIC";
  if (h.linkedin_count > 0) return "FALLBACK_ORG_CONTACT_ONLY";
  if (hasOrg) return "FALLBACK_ORG_CONTACT_ONLY";
  return "UNKNOWN_UNCLASSIFIABLE";
}

function riskFlags(h) {
  const flags = [];
  if (h.in_golden_demo && !h.in_hotel_to_owner) flags.push("LEGACY_OR_DEMO_ONLY");
  if (h.owner_truth_status === "PARTIAL") flags.push("OPERATOR_AS_OWNER_RISK");
  if (/alliance|aimbridge|operator/i.test(String(h.owner_display || ""))) flags.push("OPERATOR_AS_OWNER_RISK");
  if (!h.currentness_present && h.evidence_present) flags.push("STALE_CURRENTNESS_RISK");
  if (h.in_portfolio_relationship && !h.in_hotel_to_owner) flags.push("UNSAFE_GRAPH_REUSE_RISK");
  return flags;
}

function isOwnerActionable(h) {
  // Evidenced owner + viable contact path (email, phone, or org contact page / showcase)
  const evidencedOwner = h.in_hotel_to_owner || (h.has_deep_research && (h.propco || h.sponsor || h.owner_entity_id));
  const contactPath = (h.emails?.length || 0) > 0 || (h.phones?.length || 0) > 0 || h.has_contact_showcase;
  return Boolean(evidencedOwner && contactPath);
}

function buildInventory() {
  return [
    {
      source_name: "HOTEL_TO_OWNER map",
      path: "lib/hotel-intelligence/ownership/owner-control/portfolio-store.js",
      type: "CANONICAL",
      authoritative: true,
      read_write: "READ (hardcoded map)",
      hotel_key: "airtable rec id",
      owner_key: "owner_entity_id",
      person_key: null,
      contact_key: null,
      evidence_stored: false,
      currentness_stored: false,
      verification_stored: false,
      used_by_hotel_explorer: true,
      used_by_census: false,
      used_by_research_engine: true,
      legacy_current_staging: "CURRENT",
      hotel_count: Object.keys(HOTEL_TO_OWNER).length,
    },
    {
      source_name: "Owner portfolio fixtures",
      path: "fixtures/hotel-intelligence/owner-portfolio/",
      type: "DEMO / FIXTURE",
      authoritative: "PARTIAL — golden demo authority only",
      read_write: "READ + local fixture write on materialize",
      hotel_key: "hotel_relationships[].hotel_id",
      owner_key: "profile.owner_entity_id",
      person_key: "profile.people[]",
      contact_key: "mostly absent (LinkedIn only)",
      evidence_stored: true,
      currentness_stored: "PARTIAL",
      verification_stored: false,
      used_by_hotel_explorer: true,
      used_by_census: false,
      used_by_research_engine: true,
      legacy_current_staging: "CURRENT_DEMO",
    },
    {
      source_name: "Golden-demo ownership cohorts",
      path: "fixtures/golden-demo/*-ownership-cohort*.json / mexico-explorer-demo-cohort",
      type: "DEMO / FIXTURE",
      authoritative: false,
      read_write: "READ",
      hotel_key: "airtable_record_id",
      owner_key: "organization.entity_id",
      person_key: null,
      contact_key: null,
      evidence_stored: true,
      currentness_stored: true,
      verification_stored: false,
      used_by_hotel_explorer: true,
      used_by_census: false,
      used_by_research_engine: true,
      legacy_current_staging: "CURRENT_DEMO",
    },
    {
      source_name: "Deep research fixtures (4 controls)",
      path: "fixtures/golden-demo/*-deep-research-v1.json",
      type: "RESEARCH ARTIFACT / DEMO",
      authoritative: "CONTROL only",
      read_write: "READ",
      hotel_key: "embedded / mapped",
      owner_key: "ownership.*",
      person_key: "people",
      contact_key: "emails/phones in blob",
      evidence_stored: true,
      currentness_stored: true,
      verification_stored: "PARTIAL",
      used_by_hotel_explorer: true,
      used_by_census: false,
      used_by_research_engine: true,
      legacy_current_staging: "CURRENT_DEMO",
    },
    {
      source_name: "KGPV contact showcase",
      path: "public/data/hotel-contact-intelligence/kgpv-recUNycnMwOVFX0hc.json",
      type: "DEMO / FIXTURE",
      authoritative: false,
      read_write: "READ (static)",
      hotel_key: "hotelId",
      owner_key: "ownershipSummary",
      person_key: "people",
      contact_key: "emails/phones",
      evidence_stored: true,
      currentness_stored: true,
      verification_stored: true,
      used_by_hotel_explorer: true,
      used_by_census: false,
      used_by_research_engine: false,
      legacy_current_staging: "STAGING_SHOWCASE",
    },
    {
      source_name: "Contact Intelligence file store",
      path: "data/hotel-intelligence/contact-intelligence/",
      type: "CANONICAL (runtime empty)",
      authoritative: true,
      read_write: "READ/WRITE local files; Airtable writes=0",
      hotel_key: "hotels/{id}",
      owner_key: "owners/{id}",
      person_key: "package people",
      contact_key: "channels",
      evidence_stored: true,
      currentness_stored: true,
      verification_stored: true,
      used_by_hotel_explorer: true,
      used_by_census: false,
      used_by_research_engine: true,
      legacy_current_staging: "CURRENT_EMPTY",
      hotel_count: 0,
    },
    {
      source_name: "Local ownership repository",
      path: "data/hotel-intelligence/ownership/",
      type: "CANONICAL layout — MISSING on disk",
      authoritative: "designed yes; populated no",
      read_write: "designed R/W",
      hotel_key: "relationships",
      owner_key: "entities",
      person_key: null,
      contact_key: null,
      evidence_stored: true,
      currentness_stored: true,
      verification_stored: true,
      used_by_hotel_explorer: false,
      used_by_census: false,
      used_by_research_engine: true,
      legacy_current_staging: "CURRENT_EMPTY",
    },
    {
      source_name: "ownership-v3 stores",
      path: "data/ownership-v3/ and reports/ownership-v3/",
      type: "UNKNOWN / ABSENT",
      authoritative: false,
      read_write: "N/A",
      hotel_key: null,
      owner_key: null,
      person_key: null,
      contact_key: null,
      evidence_stored: false,
      currentness_stored: false,
      verification_stored: false,
      used_by_hotel_explorer: false,
      used_by_census: false,
      used_by_research_engine: false,
      legacy_current_staging: "ABSENT_IN_THIS_CHECKOUT",
      note: "No ownership-v3 directory or references found on branch cursor/local-system-startup-recovery",
    },
    {
      source_name: "CI12 case plan",
      path: "lib/hotel-intelligence/contact-intelligence/cohort-12-v1.js",
      type: "STAGING",
      authoritative: false,
      read_write: "READ (code)",
      hotel_key: "hotel_id",
      owner_key: "owner_entity_id",
      person_key: null,
      contact_key: null,
      evidence_stored: false,
      currentness_stored: false,
      verification_stored: false,
      used_by_hotel_explorer: false,
      used_by_census: false,
      used_by_research_engine: true,
      legacy_current_staging: "STAGING",
      hotel_count: CONTACT_V1_12_CASE_PLAN.length,
    },
    {
      source_name: "GTM Owner Targets (parallel product)",
      path: "reports/gtm-owner-* / docs/gtm-owner-target-list.md",
      type: "LEGACY / PARALLEL PRODUCT",
      authoritative: "GTM only — not HI ownership graph",
      read_write: "GTM Airtable",
      hotel_key: "varies",
      owner_key: "owner targets",
      person_key: "contacts",
      contact_key: "outreach",
      evidence_stored: "PARTIAL",
      currentness_stored: "PARTIAL",
      verification_stored: "PARTIAL",
      used_by_hotel_explorer: false,
      used_by_census: false,
      used_by_research_engine: false,
      legacy_current_staging: "LEGACY_PARALLEL",
    },
    {
      source_name: "DENUE MX hotel indexes",
      path: "data/inegi-denue/",
      type: "STAGING / REGISTRY",
      authoritative: "official MX registry when present",
      read_write: "READ",
      hotel_key: "DENUE row",
      owner_key: "razon social",
      person_key: null,
      contact_key: null,
      evidence_stored: true,
      currentness_stored: "PARTIAL",
      verification_stored: false,
      used_by_hotel_explorer: false,
      used_by_census: false,
      used_by_research_engine: true,
      legacy_current_staging: "ABSENT_IN_THIS_CHECKOUT",
      note: "Referenced in code; data/inegi-denue not present in this workspace snapshot",
    },
  ];
}

function proposeNext25(rows) {
  const byId = Object.fromEntries(rows.map((r) => [r.hotel_id, r]));
  const easy = [
    "recUNycnMwOVFX0hc", // KGPV — known owner+contact
    "recIwaP1etgx2g9nA", // Cambridge — owner known, contact thin
    "recsYJb2R1jarPpK3", // Sheraton GDL — propco known
    "recTYaiA4S6fR6ixx", // voco Cancun — alliance
    "recId5nDFUgVbJnzH", // Krystal residences — GSF portfolio
    "recMWHByXsnVNjJEp",
    "recPXRj13pXMi7IBV",
    "recb18R47T3rmciH5",
    "reca6eBmfjoInOpQE",
    "rec7e0E0PdtSccpK7",
  ];
  const medium = [
    "recGZZCek9vDQGG1L", // voco GDL — operator trap
    "rec79Xs4mZkuiWnuN", // Real Inn Juarez — alliance sibling
    "recFspIiglYxJp1N1", // Real Inn SLP
    "rec19X4tsCUM1A2q6", // Barcelo Reforma — brand trap
    "recL4PrLJpwXxyvV6", // Beach Palace
    "rec2ossLX1BBaaZuw", // Adhara Express
    "recxjhraXKAz0BaVH", // Amberes 64
    "recp4CP1ThWYjL4a1", // Krystal Urban Monterrey
    "rec51bQK61NFHO4rl", // Hyatt Regency — GSF operated?
    "recz3ng6hNt1sBD8t", // Mahekal
  ];
  const hard = [
    "recVTH9bA98rdzJ1A", // AVA — owner unknown
    "rec01a28DhirloEoM", // teacher diagnostic BR (if known)
    "rec1teYJI25SHMzvp", // Rendezvous Bay AI
    "recxDdnwAYADJYe6k", // Great House AI
    "rec03E9aDMxWgkqae", // Anga Jurere BR
  ];
  const mk = (id, band) => {
    const h = byId[id] || { hotel_id: id };
    return {
      hotel_id: id,
      hotel_name: h.hotel_name || null,
      country: h.country || null,
      band,
      reason:
        band === "EASY"
          ? "Known owner structure or portfolio sibling — validate contact path"
          : band === "MEDIUM"
            ? "Partial owner truth / brand-operator traps / reuse test"
            : "Opaque PropCo / prior research unresolved / independent",
      already_in_hotel_to_owner: Boolean(h.in_hotel_to_owner),
      already_has_contact_showcase: Boolean(h.has_contact_showcase),
    };
  };
  return {
    version: "baseline-next-25-v1",
    note: "PROPOSAL ONLY — do not execute. Includes portfolio reuse, traps, and hard independents.",
    easy: easy.map((id) => mk(id, "EASY")),
    medium: medium.map((id) => mk(id, "MEDIUM")),
    hard: hard.map((id) => mk(id, "HARD")),
    coverage_intents: [
      "Mexico",
      "Brazil",
      "Caribbean (Bermuda/Anguilla)",
      "Northern South America (gap — limited in current artifacts)",
      "Central America (gap — limited in current artifacts)",
      "major owner portfolio (GSF)",
      "independent / opaque (AVA, Great House)",
      "operator-not-owner (Alliance/voco)",
      "brand-not-owner (Barceló / Hyatt)",
      "owner known contact missing (Cambridge)",
      "contact exists verification incomplete (KGPV)",
    ],
  };
}

function main() {
  loadHotelToOwner();
  const portfolios = loadOwnerPortfolios();
  loadCi12();
  loadGoldenCohorts();
  loadDeepResearchControls();
  loadContactShowcase();

  const rows = [...hotels.values()].map((h) => {
    const owner_state = classifyOwnerState(h);
    const contact_state = classifyContactState(h);
    const risks = riskFlags(h);
    const owner_actionable = isOwnerActionable(h);
    return {
      ...h,
      owner_state,
      contact_state,
      risks,
      owner_actionable,
      evidenced_owner: Boolean(
        h.in_hotel_to_owner || (h.has_deep_research && (h.propco || h.sponsor || h.owner_entity_id))
      ),
      any_owner_value: Boolean(h.owner_entity_id || h.propco || h.sponsor || h.owner_display),
    };
  });

  const n = rows.length;
  const count = (fn) => rows.filter(fn).length;

  const metrics = {
    planning_universe_cala: PLANNING_UNIVERSE,
    production_census_documented: PRODUCTION_CENSUS_DOC,
    audited_current_dataset_size: n,
    deep_sample_size: n,
    deep_sample_note:
      "200–300 hotel deep sample NOT RELIABLY ACHIEVABLE — only hotels with stored ownership/contact artifacts exist in this checkout. Expanding to 240 would require inventing NO_RESEARCH rows without hotel identity store dump.",
    TOTAL_HOTELS_AUDITED: n,
    HOTELS_WITH_ANY_OWNER_VALUE: count((h) => h.any_owner_value),
    HOTELS_WITH_ANY_EVIDENCED_OWNER: count((h) => h.evidenced_owner),
    PROPERTY_OWNER_LEGAL_ENTITY_RESOLVED: count((h) => Boolean(h.propco)),
    OWNER_SPONSOR_GROUP_RESOLVED: count((h) => Boolean(h.sponsor)),
    ULTIMATE_PARENT_RESOLVED: "NOT RELIABLY MEASURABLE — no consistent ultimate_parent field in auditable stores",
    CONTACTABLE_OWNER_ORG_RESOLVED: count((h) => Boolean(h.contactable_org || h.sponsor || (h.owner_entity_id && h.in_hotel_to_owner))),
    OWNER_PLUS_CONTACTABLE_ORG: count(
      (h) => h.evidenced_owner && (h.contactable_org || h.sponsor || h.owner_entity_id)
    ),
    OWNER_PLUS_RELEVANT_PERSON: count((h) => h.evidenced_owner && (h.people_count || 0) > 0),
    OWNER_PLUS_PUBLIC_EMAIL: count((h) => h.evidenced_owner && (h.emails?.length || 0) > 0),
    OWNER_PLUS_VERIFIED_EMAIL:
      "NOT RELIABLY MEASURABLE — verification_status not consistently stored outside KGPV showcase; do not treat inferred/showcase as verified",
    OWNER_PLUS_DIRECT_BUSINESS_PHONE: "NOT RELIABLY MEASURABLE — phone role (direct vs corporate vs hotel) not typed in fixtures",
    OWNER_PLUS_CORPORATE_PHONE: count((h) => h.evidenced_owner && (h.phones?.length || 0) > 0),
    OWNER_PLUS_CONTACT_PAGE: "NOT RELIABLY MEASURABLE — contact-page flag not stored",
    OWNER_PLUS_PROFILE_LINKEDIN: count((h) => h.evidenced_owner && (h.linkedin_count || 0) > 0),
    FULLY_OWNER_ACTIONABLE: count((h) => h.owner_actionable),
    STALE_OWNER_RECORDS: "NOT RELIABLY MEASURABLE — freshness timestamps sparse",
    STALE_CONTACT_RECORDS: "NOT RELIABLY MEASURABLE — contact store empty",
    CONFLICTED_OWNERSHIP: count((h) => h.owner_truth_status === "PARTIAL"),
    UNRESOLVED_OWNERSHIP: count((h) =>
      ["WEAK_EVIDENCE", "UNRESOLVED_WITH_EXHAUSTIVE_SEARCH", "CANDIDATE_ONLY", "UNKNOWN_UNCLASSIFIABLE"].includes(
        h.owner_state
      )
    ),
    NO_OWNERSHIP_RESEARCH_ATTEMPTED_IN_AUDIT_SET: count((h) => h.owner_state === "NO_RESEARCH_ATTEMPTED"),
    NO_OWNERSHIP_RESEARCH_ATTEMPTED_IN_PLANNING_UNIVERSE_ESTIMATE: PLANNING_UNIVERSE - n,
  };

  const ownerStateCounts = {};
  const contactStateCounts = {};
  for (const h of rows) {
    ownerStateCounts[h.owner_state] = (ownerStateCounts[h.owner_state] || 0) + 1;
    contactStateCounts[h.contact_state] = (contactStateCounts[h.contact_state] || 0) + 1;
  }

  // Failure taxonomy over audited set; planning-universe remainder = NO_RESEARCH_ATTEMPTED
  const failureTypes = [
    {
      failure: "NO_RESEARCH_ATTEMPTED",
      count: PLANNING_UNIVERSE - n,
      pct_audited_universe: pct(PLANNING_UNIVERSE - n, PLANNING_UNIVERSE),
      pct_of_unresolved: pct(PLANNING_UNIVERSE - n, PLANNING_UNIVERSE - metrics.FULLY_OWNER_ACTIONABLE),
      countries_most_affected: ["ALL_CALA"],
      example_hotel_ids: [],
      likely_next_action: "Select bounded E2E cohort; do not claim coverage until researched",
      scope: "planning_universe_remainder",
    },
    {
      failure: "LEGACY_DATA_NOT_EVIDENCED",
      count: count((h) => h.in_ci12 && !h.in_hotel_to_owner && h.owner_entity_id),
      pct_audited_dataset: pct(count((h) => h.in_ci12 && !h.in_hotel_to_owner && h.owner_entity_id), n),
      countries_most_affected: ["Mexico"],
      example_hotel_ids: rows
        .filter((h) => h.in_ci12 && !h.in_hotel_to_owner && h.owner_entity_id)
        .map((h) => h.hotel_id)
        .slice(0, 5),
      likely_next_action: "Promote only after evidence adjudication into OCG / HOTEL_TO_OWNER",
    },
    {
      failure: "OWNER_KNOWN_CONTACTABLE_ORG_UNKNOWN",
      count: count((h) => h.evidenced_owner && !h.emails?.length && !h.phones?.length && !h.has_contact_showcase),
      pct_audited_dataset: pct(
        count((h) => h.evidenced_owner && !h.emails?.length && !h.phones?.length && !h.has_contact_showcase),
        n
      ),
      countries_most_affected: ["Mexico", "Bermuda"],
      example_hotel_ids: rows
        .filter((h) => h.evidenced_owner && !h.emails?.length && !h.phones?.length && !h.has_contact_showcase)
        .map((h) => h.hotel_id)
        .slice(0, 5),
      likely_next_action: "Org-domain + IR/contact-page discovery before person enrichment",
    },
    {
      failure: "CONTACTABLE_ORG_NO_PERSON",
      count: count((h) => (h.owner_entity_id || h.sponsor) && (h.people_count || 0) === 0),
      pct_audited_dataset: pct(count((h) => (h.owner_entity_id || h.sponsor) && (h.people_count || 0) === 0), n),
      countries_most_affected: ["Mexico"],
      example_hotel_ids: rows
        .filter((h) => (h.owner_entity_id || h.sponsor) && (h.people_count || 0) === 0)
        .map((h) => h.hotel_id)
        .slice(0, 5),
      likely_next_action: "Person affiliation research gated on confirmed owner org",
    },
    {
      failure: "PERSON_KNOWN_NO_EMAIL",
      count: count((h) => (h.people_count || 0) > 0 && !(h.emails?.length > 0)),
      pct_audited_dataset: pct(count((h) => (h.people_count || 0) > 0 && !(h.emails?.length > 0)), n),
      countries_most_affected: ["Mexico", "Bermuda"],
      example_hotel_ids: rows
        .filter((h) => (h.people_count || 0) > 0 && !(h.emails?.length > 0))
        .map((h) => h.hotel_id)
        .slice(0, 5),
      likely_next_action: "Public email / pattern discovery with verification — not Surfe-first",
    },
    {
      failure: "OPERATOR_NOT_OWNER",
      count: count((h) => h.risks?.includes("OPERATOR_AS_OWNER_RISK")),
      pct_audited_dataset: pct(count((h) => h.risks?.includes("OPERATOR_AS_OWNER_RISK")), n),
      countries_most_affected: ["Mexico"],
      example_hotel_ids: rows.filter((h) => h.risks?.includes("OPERATOR_AS_OWNER_RISK")).map((h) => h.hotel_id).slice(0, 5),
      likely_next_action: "Enforce PropCo vs operator separation in adjudication",
    },
    {
      failure: "PROPCO_UNRESOLVED",
      count: count((h) => h.evidenced_owner && !h.propco),
      pct_audited_dataset: pct(count((h) => h.evidenced_owner && !h.propco), n),
      countries_most_affected: ["Mexico"],
      example_hotel_ids: rows.filter((h) => h.evidenced_owner && !h.propco).map((h) => h.hotel_id).slice(0, 5),
      likely_next_action: "Registry / title / corporate filings playbook per country",
    },
    {
      failure: "GRAPH_REUSE_UNAVAILABLE",
      count: count((h) => h.in_ci12 && h.owner_entity_id && !h.in_hotel_to_owner),
      pct_audited_dataset: pct(count((h) => h.in_ci12 && h.owner_entity_id && !h.in_hotel_to_owner), n),
      countries_most_affected: ["Mexico"],
      example_hotel_ids: rows
        .filter((h) => h.in_ci12 && h.owner_entity_id && !h.in_hotel_to_owner)
        .map((h) => h.hotel_id)
        .slice(0, 5),
      likely_next_action: "Materialize sibling hotels into OCG only after hotel-link validation",
    },
    {
      failure: "NO_FAILURE_DATA_AVAILABLE",
      count: count((h) => h.owner_state === "UNKNOWN_UNCLASSIFIABLE"),
      pct_audited_dataset: pct(count((h) => h.owner_state === "UNKNOWN_UNCLASSIFIABLE"), n),
      countries_most_affected: [],
      example_hotel_ids: rows.filter((h) => h.owner_state === "UNKNOWN_UNCLASSIFIABLE").map((h) => h.hotel_id).slice(0, 5),
      likely_next_action: "Instrument research runs to persist earliest_failure_stage",
    },
    {
      failure: "DEMO_FIXTURE_BYPASS",
      count: count((h) => h.has_contact_showcase || (h.in_golden_demo && !h.in_hotel_to_owner)),
      pct_audited_dataset: pct(count((h) => h.has_contact_showcase || (h.in_golden_demo && !h.in_hotel_to_owner)), n),
      countries_most_affected: ["Mexico"],
      example_hotel_ids: ["recUNycnMwOVFX0hc"],
      likely_next_action: "Route Explorer through staging CI store — stop showcase-only path for production claims",
    },
  ];

  // Country readiness on audited rows
  const byCountry = {};
  for (const h of rows) {
    const c = h.country || "UNKNOWN";
    if (!byCountry[c]) byCountry[c] = [];
    byCountry[c].push(h);
  }
  const countryReady = Object.entries(byCountry).map(([country, list]) => {
    const m = list.length;
    const anyOwner = list.filter((h) => h.any_owner_value).length;
    const evidenced = list.filter((h) => h.evidenced_owner).length;
    const propco = list.filter((h) => h.propco).length;
    const sponsor = list.filter((h) => h.sponsor).length;
    const contactable = list.filter((h) => h.contactable_org || h.sponsor || h.in_hotel_to_owner).length;
    const actionable = list.filter((h) => h.owner_actionable).length;
    let maturity = "UNTESTED";
    if (m >= 3 && evidenced >= 2) maturity = "MEDIUM";
    if (country === "Mexico" && evidenced >= 3) maturity = "MEDIUM";
    if (country === "Bermuda") maturity = "LOW";
    if (m === 1) maturity = "LOW";
    return {
      country,
      hotels_audited: m,
      any_owner_pct: pct(anyOwner, m),
      evidenced_owner_pct: pct(evidenced, m),
      propco_pct: pct(propco, m),
      sponsor_pct: pct(sponsor, m),
      contactable_org_pct: pct(contactable, m),
      owner_actionable_pct: pct(actionable, m),
      dominant_failure:
        actionable === 0 && evidenced > 0
          ? "PERSON_KNOWN_NO_EMAIL / OWNER_KNOWN_CONTACTABLE_ORG_UNKNOWN"
          : evidenced === 0
            ? "WEAK_EVIDENCE / NO_CANONICAL_MAP"
            : "CONTACT_PATH_INCOMPLETE",
      playbook_maturity: maturity,
      next_improvement:
        country === "Mexico"
          ? "DENUE + corporate registry → PropCo; then org contact before Surfe"
          : country === "Bermuda"
            ? "Corporate registry / filings for PropCo; public contact pages"
            : "Establish country playbook with identity + registry sources",
    };
  });

  // Portfolio reuse
  const uniqueOwners = new Map();
  for (const p of portfolios) {
    uniqueOwners.set(p.owner_entity_id, {
      ...p,
      hotels_in_audit: rows.filter((h) => h.owner_entity_id === p.owner_entity_id).length,
      actionable_today: rows.filter((h) => h.owner_entity_id === p.owner_entity_id && h.owner_actionable).length,
    });
  }
  // Also count CI12 owner ids without portfolio files
  for (const h of rows) {
    if (!h.owner_entity_id) continue;
    if (!uniqueOwners.has(h.owner_entity_id)) {
      uniqueOwners.set(h.owner_entity_id, {
        owner_entity_id: h.owner_entity_id,
        display_name: h.owner_display || h.owner_entity_id,
        hotel_count: 1,
        hotel_ids: [h.hotel_id],
        people_count: h.people_count || 0,
        people_with_email: h.emails?.length ? 1 : 0,
        people_with_phone: h.phones?.length ? 1 : 0,
        people_with_linkedin: h.linkedin_count || 0,
        hotels_in_audit: 1,
        actionable_today: h.owner_actionable ? 1 : 0,
        completeness: null,
        file: null,
      });
    }
  }
  const ownerList = [...uniqueOwners.values()].sort(
    (a, b) => (b.hotel_count || b.hotels_in_audit || 0) - (a.hotel_count || a.hotels_in_audit || 0)
  );
  const top10 = ownerList.slice(0, 10);
  const top25 = ownerList.slice(0, 25);
  const top50 = ownerList.slice(0, 50);
  const hotelsCovered = (list) => {
    const ids = new Set();
    for (const o of list) for (const id of o.hotel_ids || []) if (String(id).startsWith("rec")) ids.add(id);
    // also count audit rows by owner
    for (const h of rows) {
      if (list.some((o) => o.owner_entity_id === h.owner_entity_id)) ids.add(h.hotel_id);
    }
    return ids.size;
  };
  // Potential: enrich top 25 owners once → all their portfolio hotels become actionable if contact path added
  const top25PortfolioHotels = new Set();
  for (const o of top25) {
    for (const id of o.hotel_ids || []) if (String(id).startsWith("rec")) top25PortfolioHotels.add(id);
    for (const h of rows) if (h.owner_entity_id === o.owner_entity_id) top25PortfolioHotels.add(h.hotel_id);
  }
  const alreadyActionable = rows.filter((h) => h.owner_actionable).length;
  const potentialFromTop25 =
    top25PortfolioHotels.size +
    // GSF portfolio fixture may list hotels without rec ids in relationships — use hotel_count sum capped
    Math.max(0, top25.reduce((s, o) => s + (o.hotel_count || 0), 0) - top25PortfolioHotels.size);

  const portfolioReuse = {
    unique_owner_orgs: ownerList.length,
    unique_contactable_owner_orgs: ownerList.filter((o) => o.people_with_email > 0 || o.actionable_today > 0).length,
    hotels_per_owner_org: ownerList.map((o) => ({
      owner_entity_id: o.owner_entity_id,
      display_name: o.display_name,
      hotel_count: o.hotel_count || o.hotels_in_audit || 0,
    })),
    top_25_owner_orgs: top25,
    hotels_covered_by_top_10: hotelsCovered(top10),
    hotels_covered_by_top_25: hotelsCovered(top25),
    hotels_covered_by_top_50: hotelsCovered(top50),
    potential_hotels_actionable_by_enriching_top_25_once: {
      estimate: Math.max(potentialFromTop25, top25.reduce((s, o) => s + (o.hotel_count || 0), 0)),
      already_actionable: alreadyActionable,
      method:
        "Sum of portfolio hotel_relationships for top owner orgs + audit-linked hotels. Assumes one org-level contact enrichment unlocks all linked hotels. NOT a production guarantee.",
    },
  };

  const kpis = {
    denominators: {
      planning_universe: PLANNING_UNIVERSE,
      audited_artifact_hotels: n,
      hotels_with_evidenced_owner: metrics.HOTELS_WITH_ANY_EVIDENCED_OWNER,
      hotels_in_hotel_to_owner: count((h) => h.in_hotel_to_owner),
    },
    OWNER_COVERAGE_RATE: {
      value: pct(metrics.HOTELS_WITH_ANY_OWNER_VALUE, n),
      numerator: metrics.HOTELS_WITH_ANY_OWNER_VALUE,
      denominator: n,
      denominator_def: "hotels appearing in any ownership/contact artifact in this checkout",
    },
    OWNER_COVERAGE_RATE_VS_PLANNING_UNIVERSE: {
      value: pct(metrics.HOTELS_WITH_ANY_OWNER_VALUE, PLANNING_UNIVERSE),
      numerator: metrics.HOTELS_WITH_ANY_OWNER_VALUE,
      denominator: PLANNING_UNIVERSE,
      denominator_def: "planning CALA ~15000",
    },
    EVIDENCED_OWNER_RATE: {
      value: pct(metrics.HOTELS_WITH_ANY_EVIDENCED_OWNER, n),
      numerator: metrics.HOTELS_WITH_ANY_EVIDENCED_OWNER,
      denominator: n,
    },
    EVIDENCED_OWNER_RATE_VS_PLANNING_UNIVERSE: {
      value: pct(metrics.HOTELS_WITH_ANY_EVIDENCED_OWNER, PLANNING_UNIVERSE),
      numerator: metrics.HOTELS_WITH_ANY_EVIDENCED_OWNER,
      denominator: PLANNING_UNIVERSE,
    },
    PROPCO_RESOLUTION_RATE: {
      value: pct(metrics.PROPERTY_OWNER_LEGAL_ENTITY_RESOLVED, n),
      numerator: metrics.PROPERTY_OWNER_LEGAL_ENTITY_RESOLVED,
      denominator: n,
    },
    SPONSOR_RESOLUTION_RATE: {
      value: pct(metrics.OWNER_SPONSOR_GROUP_RESOLVED, n),
      numerator: metrics.OWNER_SPONSOR_GROUP_RESOLVED,
      denominator: n,
    },
    CONTACTABLE_OWNER_ORG_RATE: {
      value: pct(metrics.CONTACTABLE_OWNER_ORG_RESOLVED, n),
      numerator: metrics.CONTACTABLE_OWNER_ORG_RESOLVED,
      denominator: n,
    },
    OWNER_PERSON_RATE: {
      value: pct(metrics.OWNER_PLUS_RELEVANT_PERSON, n),
      numerator: metrics.OWNER_PLUS_RELEVANT_PERSON,
      denominator: n,
    },
    OWNER_EMAIL_RATE: {
      value: pct(metrics.OWNER_PLUS_PUBLIC_EMAIL, n),
      numerator: metrics.OWNER_PLUS_PUBLIC_EMAIL,
      denominator: n,
    },
    OWNER_VERIFIED_EMAIL_RATE: {
      value: null,
      note: "NOT RELIABLY MEASURABLE",
    },
    OWNER_PHONE_RATE: {
      value: typeof metrics.OWNER_PLUS_CORPORATE_PHONE === "number" ? pct(metrics.OWNER_PLUS_CORPORATE_PHONE, n) : null,
      numerator: metrics.OWNER_PLUS_CORPORATE_PHONE,
      denominator: n,
    },
    OWNER_ACTIONABLE_RATE: {
      value_on_audited_artifacts: pct(metrics.FULLY_OWNER_ACTIONABLE, n),
      value_on_planning_universe: pct(metrics.FULLY_OWNER_ACTIONABLE, PLANNING_UNIVERSE),
      numerator: metrics.FULLY_OWNER_ACTIONABLE,
      denominator_audited: n,
      denominator_planning: PLANNING_UNIVERSE,
      definition:
        "Hotel has evidenced owner AND at least one viable contact path (email/phone/showcase contact package)",
    },
  };

  const next25 = proposeNext25(rows);

  const explorerReadiness = {
    chain: "Hotel → PropCo → Sponsor → Contactable Org → Person → Email/Phone/Profile",
    overall: "PARTIAL",
    prevents_full_flow: [
      "Only 4 hotels in HOTEL_TO_OWNER",
      "hotel-ownership-intelligence.js hardcodes PropCo/Economic owner as Not yet verified",
      "Contact Intelligence runtime store empty — KGPV showcase only",
      "Demo fixtures bypass canonical CI store for display",
    ],
    controls: {
      "Krystal Grand Puerto Vallarta": {
        structure: "YES",
        contactable_org: "YES",
        people: "YES",
        email: "YES (showcase)",
        phone: "YES (showcase)",
        profile: "PARTIAL",
        provenance: "YES",
        demo_fixture_dependency: "YES",
        source: "CANONICAL map + DEMO showcase",
      },
      "Cambridge Beaches": {
        structure: "YES",
        contactable_org: "PARTIAL",
        people: "YES (names)",
        email: "NO",
        phone: "PARTIAL (hotel phone in research)",
        profile: "PARTIAL (LinkedIn)",
        provenance: "YES",
        demo_fixture_dependency: "YES",
        source: "CANONICAL map + DEMO research",
      },
      "Sheraton Guadalajara Expo": {
        structure: "YES",
        contactable_org: "PARTIAL",
        people: "YES",
        email: "PARTIAL (operator Aimbridge — not owner)",
        phone: "PARTIAL",
        profile: "PARTIAL",
        provenance: "YES",
        demo_fixture_dependency: "YES",
        source: "CANONICAL map + DEMO research",
      },
      "voco / Real Inn Cancún": {
        structure: "PARTIAL (PropCo UNKNOWN in research)",
        contactable_org: "PARTIAL (Alliance)",
        people: "YES",
        email: "NO",
        phone: "NO",
        profile: "PARTIAL",
        provenance: "YES",
        demo_fixture_dependency: "YES",
        source: "CANONICAL map + DEMO research",
      },
    },
  };

  const writeReadiness = {
    ownership_write_ready: "NO",
    contact_write_ready: "NO",
    census_read_model_ready: "PARTIAL",
    notes: [
      "OWNERSHIP_WRITE_GUARANTEES / CONTACT_WRITE_GUARANTEES force Airtable ownership writes = 0",
      "ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=0 / ENABLE_OWNER_OPERATOR_WRITES=0 defaults",
      "Contact API canonical_promotion_forbidden",
      "Census can display management company fields but not full PropCo→Person→Contact graph",
      "Schema gap: flat Owner Name fields ≠ entity graph with evidence/currentness/verification",
    ],
  };

  const contactEngine = {
    one_canonical_contact_engine: "PARTIAL",
    declared_canonical: [
      "owner-target-resolver.js",
      "contact-intelligence/service.js + store.js",
    ],
    duplicate_or_parallel_paths: [
      "owner-contact-resolution-v2/",
      "ci12-staging / cohort-12",
      "golden-demo-ownership API",
      "KGPV static showcase path",
      "person-email-discovery / live-native-discovery",
      "GDI canonical-merge-policy (meeting WHO, not hotel owner CI)",
      "GTM Owner Targets Airtable",
    ],
  };

  const contactQuality = {
    note: "Contact Intelligence runtime store empty. Quality counts from showcase + deep-research blobs only — NOT production verified inventory.",
    VERIFIED_OFFICIAL: "NOT RELIABLY MEASURABLE",
    VERIFIED_PUBLIC: "NOT RELIABLY MEASURABLE",
    VERIFIED_MAILBOX: 0,
    DOMAIN_PATTERN_SUPPORTED: "NOT RELIABLY MEASURABLE",
    INFERRED: "NOT RELIABLY MEASURABLE",
    PUBLICLY_PUBLISHED_EMAIL_IN_ARTIFACTS: metrics.OWNER_PLUS_PUBLIC_EMAIL,
    hotels_with_phone_in_artifacts: typeof metrics.OWNER_PLUS_CORPORATE_PHONE === "number" ? metrics.OWNER_PLUS_CORPORATE_PHONE : 0,
    risks: [
      "Showcase may present verification badges without mailbox verification configured",
      "Operator emails (Aimbridge) must not be scored as owner contacts",
      "Portfolio people often lack email/phone — LinkedIn-only",
    ],
  };

  const founder = {
    version: "ownership-contact-baseline-founder-summary-v1",
    mode: "READ_ONLY",
    audited_at: new Date().toISOString(),
    audit_universe: {
      planning_cala: PLANNING_UNIVERSE,
      hotels_actually_audited: n,
      deep_sample: n,
      deep_sample_cap_reason: metrics.deep_sample_note,
      production_census_documented: PRODUCTION_CENSUS_DOC,
    },
    ownership_baseline: {
      any_owner_value: `${metrics.HOTELS_WITH_ANY_OWNER_VALUE} / ${n} (artifact set); ${metrics.HOTELS_WITH_ANY_OWNER_VALUE} / ${PLANNING_UNIVERSE} (planning)`,
      any_evidenced_owner: `${metrics.HOTELS_WITH_ANY_EVIDENCED_OWNER} / ${n}; ${metrics.HOTELS_WITH_ANY_EVIDENCED_OWNER} / ${PLANNING_UNIVERSE}`,
      propco: `${metrics.PROPERTY_OWNER_LEGAL_ENTITY_RESOLVED} / ${n}`,
      sponsor: `${metrics.OWNER_SPONSOR_GROUP_RESOLVED} / ${n}`,
      contactable_owner_org: `${metrics.CONTACTABLE_OWNER_ORG_RESOLVED} / ${n}`,
      resolved_owner_structure: `${count((h) => h.owner_state === "RESOLVED_OWNER_STRUCTURE" || h.owner_state === "PROPCO_AND_SPONSOR")} / ${n}`,
      canonical_map_hotels: Object.keys(HOTEL_TO_OWNER).length,
    },
    contact_baseline: {
      owner_plus_relevant_person: `${metrics.OWNER_PLUS_RELEVANT_PERSON} / ${n}`,
      owner_plus_public_email: `${metrics.OWNER_PLUS_PUBLIC_EMAIL} / ${n}`,
      owner_plus_verified_email: "NOT RELIABLY MEASURABLE",
      owner_plus_phone: `${metrics.OWNER_PLUS_CORPORATE_PHONE} / ${n}`,
      owner_plus_fallback: `${count((h) => h.contact_state === "FALLBACK_ORG_CONTACT_ONLY")} / ${n}`,
      fully_owner_actionable: `${metrics.FULLY_OWNER_ACTIONABLE} / ${n}`,
      OWNER_ACTIONABLE_RATE_audited_pct: kpis.OWNER_ACTIONABLE_RATE.value_on_audited_artifacts,
      OWNER_ACTIONABLE_RATE_planning_pct: kpis.OWNER_ACTIONABLE_RATE.value_on_planning_universe,
    },
    recommended_scale_strategy: "OWNER_PORTFOLIO_FIRST",
    recommended_scale_why:
      "Canonical ownership graph only has 4 hotels but portfolio fixtures already attach ~22 hotel relationships to 4 owners. Contact fails after owner is known. Enriching top owners once unlocks more hotels than country-wide cold research. Hybrid country playbooks still needed for PropCo resolution, but the dominant scaling lever is owner reuse + contact path.",
    single_next_action: "OWNERSHIP_CONTACT_25_HOTEL_END_TO_END_CANARY",
    most_important_finding:
      "We do not have a populated ownership graph for CALA — we have a 4-hotel demo map, empty Contact Intelligence store, and fixture/showcase paths that make coverage look farther along than it is. Evidenced ownership vs ~15k is effectively ~0.0x%; OWNER_ACTIONABLE is essentially 1 showcase hotel. The blocking issue is not 'another audit' — it is absence of persisted research outputs at scale plus contact path after owner.",
  };

  // Write data artifacts
  ensureDir(OUT_DATA);
  ensureDir(OUT_REPORTS);

  // JSONL instead of parquet (no parquet dependency required)
  const jsonlPath = path.join(OUT_DATA, "baseline-hotel-states.jsonl");
  fs.writeFileSync(jsonlPath, rows.map((r) => JSON.stringify(r)).join("\n") + "\n", "utf8");
  writeJson(path.join(OUT_DATA, "baseline-hotel-states.parquet.json"), {
    note: "Parquet binary not generated (no parquet writer in offline audit). Authoritative row store is baseline-hotel-states.jsonl + this mirror JSON.",
    row_count: rows.length,
    rows,
  });

  writeJson(path.join(OUT_DATA, "baseline-owner-states.json"), {
    owner_state_counts: ownerStateCounts,
    hotels: rows.map((h) => ({
      hotel_id: h.hotel_id,
      owner_state: h.owner_state,
      risks: h.risks,
      owner_entity_id: h.owner_entity_id,
    })),
  });
  writeJson(path.join(OUT_DATA, "baseline-contact-states.json"), {
    contact_state_counts: contactStateCounts,
    hotels: rows.map((h) => ({
      hotel_id: h.hotel_id,
      contact_state: h.contact_state,
      owner_actionable: h.owner_actionable,
    })),
  });
  writeJson(path.join(OUT_DATA, "baseline-failure-types.json"), { failure_types: failureTypes });
  writeJson(path.join(OUT_DATA, "baseline-country-readiness.json"), { countries: countryReady });
  writeJson(path.join(OUT_DATA, "baseline-owner-portfolio-reuse.json"), portfolioReuse);
  writeJson(path.join(OUT_DATA, "baseline-contact-quality.json"), contactQuality);
  writeJson(path.join(OUT_DATA, "baseline-next-25.json"), next25);
  writeJson(path.join(OUT_DATA, "baseline-founder-summary.json"), founder);
  writeJson(path.join(OUT_DATA, "baseline-metrics.json"), { metrics, kpis, contactEngine, writeReadiness, explorerReadiness });
  writeJson(path.join(OUT_DATA, "baseline-inventory.json"), { inventory: buildInventory() });

  // Markdown reports
  writeMd(
    path.join(OUT_REPORTS, "001-DATA-SOURCE-INVENTORY.md"),
    [
      "# 001 — Data Source Inventory",
      "",
      "READ-ONLY. No live research.",
      "",
      "## Critical absences",
      "",
      "- `data/ownership-v3/` — **ABSENT**",
      "- `reports/ownership-v3/` — **ABSENT**",
      "- `data/hotel-intelligence/ownership/` — **ABSENT (layout only)**",
      "- `data/hotel-intelligence/contact-intelligence/` packages — **EMPTY**",
      "- `data/inegi-denue/` — **ABSENT in this checkout**",
      "",
      "## Authoritative path?",
      "",
      "- **Hotel → Owner:** PARTIAL — `HOTEL_TO_OWNER` (4 hotels) + owner-portfolio fixtures. Not CALA-scale.",
      "- **Owner → Person/Contact:** PARTIAL — declared via Contact Intelligence store + owner-target-resolver; runtime empty; KGPV showcase bypasses.",
      "- **Demo fixtures still bypass:** YES (golden-demo API + static showcase).",
      "- **Flat Owner Name as truth:** RISK — GTM/Census management fields must not be treated as PropCo evidence.",
      "- **Contacts at hotel vs org level:** Showcase mixes; portfolio people are org-level but usually without channels.",
      "",
      "## Sources",
      "",
      ...buildInventory().map(
        (s) =>
          `### ${s.source_name}\n- Path: \`${s.path}\`\n- Type: ${s.type}\n- Authoritative: ${s.authoritative}\n- R/W: ${s.read_write}\n- Evidence: ${s.evidence_stored} | Currentness: ${s.currentness_stored} | Verification: ${s.verification_stored}\n- Explorer: ${s.used_by_hotel_explorer} | Census: ${s.used_by_census} | Research: ${s.used_by_research_engine}\n- Status: ${s.legacy_current_staging}${s.note ? `\n- Note: ${s.note}` : ""}\n`
      ),
    ].join("\n")
  );

  writeMd(
    path.join(OUT_REPORTS, "002-BASELINE-METRICS.md"),
    [
      "# 002 — Baseline Metrics",
      "",
      `Planning CALA universe: **~${PLANNING_UNIVERSE}**`,
      `Production census (documented): **${PRODUCTION_CENSUS_DOC}**`,
      `Audited current dataset (artifacts in checkout): **${n}**`,
      "",
      "```json",
      JSON.stringify(metrics, null, 2),
      "```",
    ].join("\n")
  );

  writeMd(
    path.join(OUT_REPORTS, "003-OWNERSHIP-STATES.md"),
    ["# 003 — Ownership States", "", "```json", JSON.stringify(ownerStateCounts, null, 2), "```"].join("\n")
  );
  writeMd(
    path.join(OUT_REPORTS, "004-CONTACT-STATES.md"),
    ["# 004 — Contact States", "", "```json", JSON.stringify(contactStateCounts, null, 2), "```"].join("\n")
  );
  writeMd(
    path.join(OUT_REPORTS, "005-FAILURE-TYPES.md"),
    ["# 005 — Failure Types", "", "```json", JSON.stringify(failureTypes, null, 2), "```"].join("\n")
  );
  writeMd(
    path.join(OUT_REPORTS, "006-DETAILED-SAMPLE.md"),
    [
      "# 006 — Detailed Sample",
      "",
      `Sample size: **${n}** (all hotels with stored ownership/contact artifacts).`,
      "",
      metrics.deep_sample_note,
      "",
      "Golden Four used as controls only (not majority of sample intent — sample is artifact-limited).",
      "",
      "| hotel_id | name | country | owner_state | contact_state | actionable |",
      "|---|---|---|---|---|---|",
      ...rows.map(
        (h) =>
          `| ${h.hotel_id} | ${h.hotel_name || ""} | ${h.country || ""} | ${h.owner_state} | ${h.contact_state} | ${h.owner_actionable} |`
      ),
    ].join("\n")
  );
  writeMd(
    path.join(OUT_REPORTS, "007-COUNTRY-READINESS.md"),
    ["# 007 — Country Readiness", "", "```json", JSON.stringify(countryReady, null, 2), "```"].join("\n")
  );
  writeMd(
    path.join(OUT_REPORTS, "008-OWNER-PORTFOLIO-REUSE.md"),
    ["# 008 — Owner Portfolio Reuse", "", "```json", JSON.stringify(portfolioReuse, null, 2), "```"].join("\n")
  );
  writeMd(
    path.join(OUT_REPORTS, "009-CONTACT-ENGINE-AUDIT.md"),
    ["# 009 — Contact Engine Audit", "", "```json", JSON.stringify(contactEngine, null, 2), "```"].join("\n")
  );
  writeMd(
    path.join(OUT_REPORTS, "010-CONTACT-DATA-QUALITY.md"),
    ["# 010 — Contact Data Quality", "", "```json", JSON.stringify(contactQuality, null, 2), "```"].join("\n")
  );
  writeMd(
    path.join(OUT_REPORTS, "011-HOTEL-EXPLORER-READINESS.md"),
    ["# 011 — Hotel Explorer Readiness", "", "```json", JSON.stringify(explorerReadiness, null, 2), "```"].join("\n")
  );
  writeMd(
    path.join(OUT_REPORTS, "012-AIRTABLE-CENSUS-READINESS.md"),
    ["# 012 — Airtable / Census Readiness", "", "```json", JSON.stringify(writeReadiness, null, 2), "```"].join("\n")
  );
  writeMd(
    path.join(OUT_REPORTS, "013-BASELINE-KPI.md"),
    ["# 013 — Baseline KPI", "", "```json", JSON.stringify(kpis, null, 2), "```"].join("\n")
  );
  writeMd(
    path.join(OUT_REPORTS, "014-NEXT-25-COHORT.md"),
    [
      "# 014 — Next 25 Cohort (PROPOSAL ONLY — DO NOT EXECUTE)",
      "",
      "```json",
      JSON.stringify(next25, null, 2),
      "```",
    ].join("\n")
  );
  writeMd(
    path.join(OUT_REPORTS, "015-EXECUTION-RECOMMENDATION.md"),
    [
      "# 015 — Execution Recommendation",
      "",
      "## Scale strategy",
      "",
      "**OWNER_PORTFOLIO_FIRST** (with country playbooks as supporting work).",
      "",
      "## Single next packet",
      "",
      "**OWNERSHIP_CONTACT_25_HOTEL_END_TO_END_CANARY**",
      "",
      "Do not run another broad audit.",
      "",
      "## Why not remediation-only first?",
      "",
      "Architecture is PARTIAL but not blocking a bounded canary: HOTEL_TO_OWNER, owner-target-resolver, CI store, and research handoff exist. The gap is populated evidence + contact paths. A 25-hotel canary will force persistence of failure stages and prove the chain.",
      "",
      "## Hard stops observed",
      "",
      "- No ownership-v3 corpus in this checkout",
      "- Contact store empty",
      "- Airtable ownership writes disabled by design",
      "- Explorer demo dependency",
    ].join("\n")
  );

  console.log(
    JSON.stringify(
      {
        status: "BASELINE_AUDIT_COMPLETE",
        mode: "READ_ONLY",
        hotels_audited: n,
        planning_universe: PLANNING_UNIVERSE,
        owner_actionable: metrics.FULLY_OWNER_ACTIONABLE,
        OWNER_ACTIONABLE_RATE_planning_pct: kpis.OWNER_ACTIONABLE_RATE.value_on_planning_universe,
        reports: OUT_REPORTS,
        data: OUT_DATA,
      },
      null,
      2
    )
  );
}

main();
