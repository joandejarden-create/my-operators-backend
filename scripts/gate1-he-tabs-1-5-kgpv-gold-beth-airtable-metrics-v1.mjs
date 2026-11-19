#!/usr/bin/env node
/**
 * Gate 1 revised metrics — KGPV golden-demo UX vs Bethesda Airtable-first reality.
 *
 * Architecture:
 *   KGPV defines the desired experience (UI structure / depth).
 *   HPC + established HI Airtable tables define canonical intelligence.
 *   Do NOT treat KGPV fixtures/CI as verified Airtable.
 *   Do NOT migrate golden-demo fixtures without independent verification.
 *
 *   node scripts/gate1-he-tabs-1-5-kgpv-gold-beth-airtable-metrics-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import {
  resolveHeAuditBaseUrl,
  assertHeLocalRuntimeTarget,
} from "./lib/he-local-runtime-target.mjs";
import { GRAPH_BASE_ID_CANONICAL } from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-contract-v1.js";

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const OUT_JSON = path.join(
  ROOT,
  "reports/hotel-explorer/HE-TABS-1-5-GATE1-KGPV-GOLD-BETH-AIRTABLE-METRICS-V1.json",
);
const OUT_MD = path.join(
  ROOT,
  "reports/hotel-explorer/HE-TABS-1-5-GATE1-FOUNDER-REVIEW-V1.md",
);

const HPC = { id: "tbl9aY5ijiuIzzWam", name: "Hotel Property Census" };
const CENSUS = { id: "tblgj2qEwxjTcg6q0", name: "Hotel Census" };
const HI = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "lib/hotel-intelligence/research-engine-v1/airtable-graph-table-ids.generated.json"),
    "utf8",
  ),
).tableIds;

const KGPV_ID = "recUNycnMwOVFX0hc";
const BETH_ID = "recLuxvwwxID7U2B8";

/**
 * Field catalog: what KGPV gold experience requires (tabs 1–5),
 * plus Bethesda Airtable inventory probes.
 */
const CATALOG = [
  // Tab 1 — Hotel Overview (canonical: HPC)
  { tab: "hotel", field: "name", question: "Property name", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Canonical Property Name", "Property Name"] },
  { tab: "hotel", field: "brand", question: "Brand / affiliation", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Current Brand"] },
  { tab: "hotel", field: "status", question: "Open / pipeline status", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Radar Status", "project_phase"] },
  { tab: "hotel", field: "website", question: "Official website", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Official Property URL"] },
  { tab: "hotel", field: "telephone", question: "Property phone", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Phone"] },
  { tab: "hotel", field: "address1", question: "Street address", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Address", "Address 1"] },
  { tab: "hotel", field: "city", question: "City", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["City"] },
  { tab: "hotel", field: "country", question: "Country", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Country"] },
  { tab: "hotel", field: "hotelHeadline", question: "Headline", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Hotel Headline", "Property Positioning"] },
  { tab: "hotel", field: "hotelDescription", question: "Short description", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Short Property Summary"] },
  { tab: "hotel", field: "parentCompany", question: "Brand parent company", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Parent Company", "Brand Family"] },
  { tab: "hotel", field: "chainScale", question: "Chain scale", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Chain Scale", "Hotel Class / Segment"] },
  { tab: "hotel", field: "censusPropertyType", question: "Property type", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Property Type"] },
  { tab: "hotel", field: "rooms", question: "Rooms / keys", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Rooms / Keys"] },
  { tab: "hotel", field: "totalMeetingSpace", question: "Meeting space", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Total Meeting Space", "Total Meeting Space Sq Ft", "Meeting Space Sq Ft"], allowVerifiedDerived: true },
  { tab: "hotel", field: "operationType", question: "Operation type", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Operation Type", "Management Model"] },
  { tab: "hotel", field: "managementCompany", question: "Operator label", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Operator / Management Company"] },
  { tab: "hotel", field: "market", question: "Market", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Market"] },
  { tab: "hotel", field: "submarket", question: "Submarket", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Submarket"] },
  { tab: "hotel", field: "locationType", question: "Location type", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Location", "Location Type", "Asset Context"] },
  { tab: "hotel", field: "projectPhase", question: "Project phase", goldRequired: false, bethAirtableProbe: "hpc", hpcFields: ["project_phase"] },
  { tab: "hotel", field: "projectedOpenDate", question: "Projected open (pipeline)", goldRequired: false, bethAirtableProbe: "hpc", hpcFields: ["projected_open_date"], naIfOpen: true },
  { tab: "hotel", field: "starRating", question: "Star rating", goldRequired: false, bethAirtableProbe: "hpc", hpcFields: ["Star Rating"] },
  { tab: "hotel", field: "dataConfidence", question: "Data confidence", goldRequired: false, bethAirtableProbe: "hpc", hpcFields: ["Data Confidence Tier", "Data Confidence"] },
  { tab: "hotel", field: "amenitiesDisplay", question: "Amenities", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Amenities - Structured Tags", "Amenities"] },

  // Tab 2 — Ownership (canonical: HI graph)
  { tab: "ownership", field: "executive_summary.text", question: "Ownership narrative summary", goldRequired: true, bethAirtableProbe: "hi_derived" },
  { tab: "ownership", field: "what_remains_uncertain", question: "Uncertainty callouts", goldRequired: true, bethAirtableProbe: "hi_evidence" },
  { tab: "ownership", field: "economic_owner", question: "Economic owner", goldRequired: true, bethAirtableProbe: "hi_rel", relType: "Economic Owner" },
  { tab: "ownership", field: "property_company", question: "Legal PropCo", goldRequired: true, bethAirtableProbe: "hi_rel", relType: "Legal Owner" },
  { tab: "ownership", field: "operator", question: "Operator", goldRequired: true, bethAirtableProbe: "hi_rel", relType: "Operator" },
  { tab: "ownership", field: "control_status", question: "Control / verification posture", goldRequired: true, bethAirtableProbe: "hi_derived" },
  { tab: "ownership", field: "source_coverage", question: "Evidence coverage", goldRequired: true, bethAirtableProbe: "hi_evidence" },
  { tab: "ownership", field: "what_we_know[]", question: "Known facts list", goldRequired: true, bethAirtableProbe: "hi_evidence" },
  { tab: "ownership", field: "control_rows", question: "Role × org control table", goldRequired: true, bethAirtableProbe: "hi_rel_any" },
  { tab: "ownership", field: "ubo_group", question: "UBO / parent group", goldRequired: true, bethAirtableProbe: "hi_org_parent" },
  { tab: "ownership", field: "portfolio_preview", question: "Portfolio preview", goldRequired: true, bethAirtableProbe: "hi_derived" },
  { tab: "ownership", field: "brand_status", question: "Brand relationship", goldRequired: true, bethAirtableProbe: "hi_rel", relType: "Brand" },
  { tab: "ownership", field: "decision_authority.people", question: "Decision authority people", goldRequired: true, bethAirtableProbe: "hi_contacts" },
  { tab: "ownership", field: "qualified_signatories", question: "Qualified signatories", goldRequired: true, bethAirtableProbe: "hi_contacts" },
  { tab: "ownership", field: "corporate_contact_channels", question: "Corporate channels", goldRequired: true, bethAirtableProbe: "hi_channels" },
  { tab: "ownership", field: "asset_agreement_profile", question: "Agreement posture", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Operation Type"] },
  { tab: "ownership", field: "property_history", question: "Ownership history", goldRequired: true, bethAirtableProbe: "hi_evidence" },
  { tab: "ownership", field: "development_pursuit", question: "Pursuit notes", goldRequired: false, bethAirtableProbe: "hi_runs" },
  { tab: "ownership", field: "research_gaps", question: "Research gaps", goldRequired: true, bethAirtableProbe: "hi_runs" },
  { tab: "ownership", field: "sources", question: "Source bibliography", goldRequired: true, bethAirtableProbe: "hi_evidence" },

  // Tab 3 — Contacts
  { tab: "contacts", field: "quality_summary", question: "Contact quality summary", goldRequired: true, bethAirtableProbe: "hi_derived" },
  { tab: "contacts", field: "ownershipSummary", question: "Ownership summary on contacts", goldRequired: true, bethAirtableProbe: "hi_derived" },
  { tab: "contacts", field: "organization_contacts", question: "Org contact routes", goldRequired: true, bethAirtableProbe: "hi_channels" },
  { tab: "contacts", field: "people_groups", question: "People by role", goldRequired: true, bethAirtableProbe: "hi_contacts" },
  { tab: "contacts", field: "emails_phones", question: "Emails / phones", goldRequired: true, bethAirtableProbe: "hi_channels" },
  { tab: "contacts", field: "fallback_paths", question: "Public fallback paths", goldRequired: true, bethAirtableProbe: "hpc", hpcFields: ["Phone", "Official Property URL"] },
  { tab: "contacts", field: "research_unresolved", question: "Unresolved contact research", goldRequired: true, bethAirtableProbe: "hi_runs" },

  // Tab 4 — Organization
  { tab: "organization", field: "display_name", question: "Org display name", goldRequired: true, bethAirtableProbe: "hi_org" },
  { tab: "organization", field: "legal_name", question: "Org legal name", goldRequired: true, bethAirtableProbe: "hi_org" },
  { tab: "organization", field: "ticker", question: "Ticker / registry id", goldRequired: true, bethAirtableProbe: "hi_org" },
  { tab: "organization", field: "portfolio_kpis", question: "Portfolio KPIs", goldRequired: true, bethAirtableProbe: "hi_derived" },
  { tab: "organization", field: "owned_controlled_hotels", question: "Owned hotels list", goldRequired: true, bethAirtableProbe: "hi_derived" },
  { tab: "organization", field: "operated_managed_hotels", question: "Operated hotels list", goldRequired: true, bethAirtableProbe: "hi_derived" },
  { tab: "organization", field: "key_people", question: "Key people", goldRequired: true, bethAirtableProbe: "hi_contacts" },
  { tab: "organization", field: "why_this_matters", question: "Why org matters", goldRequired: true, bethAirtableProbe: "hi_org" },

  // Tab 5 — Relationships
  { tab: "relationships", field: "structure_chart", question: "Structure chart", goldRequired: true, bethAirtableProbe: "hi_derived" },
  { tab: "relationships", field: "relationship_edges", question: "Relationship edges", goldRequired: true, bethAirtableProbe: "hi_rel_any" },
  { tab: "relationships", field: "org_network", question: "Org network", goldRequired: true, bethAirtableProbe: "hi_derived" },
];

function loadEnv() {
  try {
    const dotenv = require("dotenv");
    dotenv.config({ path: "C:/Dev/dealality/my-operators-backend/.env" });
    dotenv.config({ path: path.join(ROOT, ".env") });
  } catch {
    /* optional */
  }
}

function nonempty(v) {
  if (v == null) return false;
  if (typeof v === "number") return Number.isFinite(v) && v !== 0;
  if (typeof v === "boolean") return true;
  if (Array.isArray(v)) return v.length > 0;
  if (typeof v === "object") return Object.keys(v).length > 0;
  const s = String(v).trim();
  return s.length > 0 && s !== "—" && s.toLowerCase() !== "null" && s !== "Unknown" && s !== "Unknown Hotel";
}

function getPath(obj, dotted) {
  if (!obj) return undefined;
  if (!dotted.includes(".")) return obj[dotted];
  return dotted.split(".").reduce((a, k) => (a == null ? undefined : a[k]), obj);
}

async function fetchJson(url) {
  const res = await fetch(url);
  const text = await res.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    json = null;
  }
  return { ok: res.ok, status: res.status, json, bytes: text.length };
}

async function atFind(baseId, tableId, recordId, token) {
  const formula = encodeURIComponent(`RECORD_ID()='${recordId}'`);
  const url = `https://api.airtable.com/v0/${baseId}/${tableId}?filterByFormula=${formula}&maxRecords=1`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => ({}));
  return (json.records || [])[0] || null;
}

function hpcHas(fields, keys) {
  for (const k of keys || []) {
    if (nonempty(fields?.[k])) return { ok: true, field: k, valuePreview: String(fields[k]).slice(0, 80) };
  }
  return { ok: false, field: null, valuePreview: null };
}

function extractUi(field, hotelId, bundle) {
  const h = bundle.brandPresence.json?.hotel || {};
  const o = bundle.ownership.json || {};
  const g = bundle.graph.json || {};
  const c = bundle.contacts.json || {};
  const pkg = c.package || {};
  const org = bundle.org.json || {};
  const report = o.report || {};
  const control = report.ownership_and_control || {};
  const orgFromOwnership = o.organization || {};

  const map = {
    name: h.name,
    brand: h.brand,
    status: h.status || h.radarStatus || h.projectPhase,
    website: h.website,
    telephone: h.telephone || h.phone,
    address1: h.address1 || h.address,
    city: h.city,
    country: h.country,
    hotelHeadline: h.hotelHeadline,
    hotelDescription: h.hotelDescription || h.description,
    parentCompany: h.parentCompany,
    chainScale: h.chainScale,
    censusPropertyType: h.censusPropertyType,
    rooms: h.rooms,
    totalMeetingSpace: h.totalMeetingSpace,
    operationType: h.operationType,
    managementCompany: h.managementCompany,
    market: h.market,
    submarket: h.submarket,
    locationType: h.locationType,
    projectPhase: h.projectPhase,
    projectedOpenDate: h.projectedOpenDate,
    starRating: h.starRating,
    dataConfidence: h.dataConfidence,
    amenitiesDisplay: h.amenitiesDisplay || h.amenities,
    "executive_summary.text": getPath(report, "executive_summary.text") || report.executive_summary,
    what_remains_uncertain:
      report.what_remains_uncertain ||
      report.observations ||
      o.what_remains_uncertain,
    economic_owner:
      control.economic_owner_or_group ||
      control.economic_owner ||
      g.projection?.economic_owner,
    property_company:
      control.legal_property_owner_propco ||
      control.property_company ||
      g.projection?.property_company,
    operator: control.operator || g.projection?.operator,
    control_status:
      control.ownership_structure ||
      control.control_status ||
      g.projection?.control_status,
    source_coverage: report.evidence || report.source_coverage || o.source_coverage,
    "what_we_know[]":
      report.product_truth_set_ref ||
      report.what_we_know ||
      report.observations ||
      o.what_we_know,
    control_rows:
      report.ownership_and_control ||
      report.relationships ||
      g.relationships,
    ubo_group: control.parent_sponsor || report.ubo_group || o.ubo_group,
    portfolio_preview: report.portfolio_preview || org.portfolio_preview || o.portfolio_preview,
    brand_status: control.brand || report.brand_chronology || g.projection?.brand,
    "decision_authority.people":
      report.decision_authority ||
      getPath(report, "decision_authority.people") ||
      o.decision_authority,
    qualified_signatories:
      report.qualified_signatories ||
      report.decision_authority?.qualified_signatories,
    corporate_contact_channels:
      report.corporate_contacts || report.corporate_contact_channels,
    asset_agreement_profile:
      control.lease_lessor || report.asset_agreement_profile,
    property_history: report.property_history,
    development_pursuit:
      report.strategic_development ||
      report.commercial_pursuit ||
      report.development_pursuit,
    research_gaps: report.research_gaps || report.research_status,
    sources: report.evidence || report.sources,
    quality_summary: pkg.quality_summary || c.quality_summary,
    ownershipSummary: pkg.ownershipSummary || c.ownershipSummary,
    organization_contacts: pkg.organization_contact_route || pkg.organization_contacts,
    people_groups: pkg.people || pkg.people_groups,
    emails_phones: [
      ...(pkg.hotel_contact?.channels || []),
      ...(pkg.organization_contact_route?.channels || []),
    ],
    fallback_paths: pkg.fallback_paths,
    research_unresolved: pkg.unresolved_reasons,
    display_name:
      org.organization?.display_name ||
      orgFromOwnership.display_name ||
      orgFromOwnership.name ||
      g.organizations?.[0]?.tradingName,
    legal_name:
      org.organization?.legal_name ||
      orgFromOwnership.legal_name ||
      g.organizations?.[0]?.legalName,
    ticker:
      org.organization?.ticker ||
      orgFromOwnership.ticker ||
      g.organizations?.[0]?.registryId,
    portfolio_kpis: org.portfolio_kpis || orgFromOwnership.portfolio_kpis,
    owned_controlled_hotels:
      org.owned_controlled_hotels || orgFromOwnership.owned_controlled_hotels,
    operated_managed_hotels:
      org.operated_managed_hotels || orgFromOwnership.operated_managed_hotels,
    key_people: org.key_people || report.decision_authority,
    why_this_matters: org.why_this_matters || report.why_this_matters,
    structure_chart:
      report.ownership_chain || report.structure_chart || report.relationships || g.relationships,
    relationship_edges: report.relationships || g.relationships,
    org_network: report.organizations || o.org_network,
  };
  const v = map[field];
  if (Array.isArray(v) && v.length === 0) return null;
  return v;
}

function assessBethAirtable(meta, ctx) {
  const { hpcFields, graph, contactsCount, channelsCount, runsCount, evidenceCount } = ctx;
  const rels = graph?.relationships || [];
  const orgs = graph?.organizations || [];
  const hasRel = (type) =>
    rels.some((r) => String(r.relationshipType) === type || (type === "Operator" && r.relationshipType === "Management Company"));

  switch (meta.bethAirtableProbe) {
    case "hpc": {
      if (meta.naIfOpen && String(hpcFields["Radar Status"] || "") === "Open") {
        return { existsInAirtable: true, detail: "N/A for Open — field not required", airtableClass: "N_A_OPEN_ASSET" };
      }
      const hit = hpcHas(hpcFields, meta.hpcFields);
      return {
        existsInAirtable: hit.ok,
        detail: hit.ok ? `HPC.${hit.field}` : `Missing on HPC (${(meta.hpcFields || []).join("|")})`,
        airtableClass: hit.ok ? "EXISTS_HPC" : "MISSING_HPC",
        airtableField: hit.field,
      };
    }
    case "hi_rel": {
      const ok = hasRel(meta.relType);
      return {
        existsInAirtable: ok,
        detail: ok ? `HI Rel ${meta.relType}` : `No HI Rel type=${meta.relType}`,
        airtableClass: ok ? "EXISTS_HI_REL" : "MISSING_HI_REL",
      };
    }
    case "hi_rel_any":
      return {
        existsInAirtable: rels.length > 0,
        detail: rels.length ? `${rels.length} HI relationships` : "No HI relationships",
        airtableClass: rels.length ? "EXISTS_HI_REL" : "MISSING_HI_REL",
      };
    case "hi_org":
      return {
        existsInAirtable: orgs.length > 0,
        detail: orgs.length ? `HI Organizations (${orgs.map((o) => o.legalName || o.tradingName).join("; ")})` : "No linked HI Organizations",
        airtableClass: orgs.length ? "EXISTS_HI_ORG" : "MISSING_HI_ORG",
      };
    case "hi_org_parent":
      return {
        existsInAirtable: false,
        detail: "Parent/UBO chain not populated on HI Organizations for Bethesda",
        airtableClass: "MISSING_HI_ORG",
      };
    case "hi_contacts":
      return {
        existsInAirtable: contactsCount > 0,
        detail: contactsCount ? `${contactsCount} HI Org Contacts` : "No HI Org Contacts for linked orgs",
        airtableClass: contactsCount ? "EXISTS_HI_CONTACTS" : "MISSING_HI_CONTACTS",
      };
    case "hi_channels":
      return {
        existsInAirtable: channelsCount > 0,
        detail: channelsCount ? `${channelsCount} HI Contact Channels` : "No HI Contact Channels",
        airtableClass: channelsCount ? "EXISTS_HI_CHANNELS" : "MISSING_HI_CHANNELS",
      };
    case "hi_evidence":
      return {
        existsInAirtable: evidenceCount > 0,
        detail: evidenceCount ? `${evidenceCount} HI Graph Evidence` : "No HI Graph Evidence rows",
        airtableClass: evidenceCount ? "EXISTS_HI_EVIDENCE" : "MISSING_HI_EVIDENCE",
      };
    case "hi_runs":
      return {
        existsInAirtable: runsCount > 0,
        detail: runsCount ? `${runsCount} HI Ownership Research Runs` : "No HI Ownership Research Runs",
        airtableClass: runsCount ? "EXISTS_HI_RUNS" : "MISSING_HI_RUNS",
      };
    case "hi_derived":
      return {
        existsInAirtable: rels.length > 0 || orgs.length > 0,
        detail: "Derived read-model — requires underlying HI facts + HE wiring",
        airtableClass: rels.length || orgs.length ? "DERIVABLE_FROM_HI" : "NOT_DERIVABLE_YET",
      };
    default:
      return { existsInAirtable: false, detail: "unspecified probe", airtableClass: "UNKNOWN" };
  }
}

/**
 * Live integration: UI value is actually served from live Airtable for Bethesda.
 * Conservative — only claim LIVE when we can attribute to HPC/HI.
 */
function assessLiveIntegration(meta, uiPresent, air, hotelId, ownershipInCohort, graph, hotelDto) {
  if (hotelId === KGPV_ID) {
    // KGPV UI is golden-demo — never count as live Airtable integration for Gate 1 Airtable-first metrics
    return {
      liveIntegrated: false,
      liveClass: uiPresent ? "GOLDEN_DEMO_UI_ONLY" : "GOLDEN_DEMO_UI_EMPTY",
      note: "KGPV demonstrated experience — not counted as live Airtable→UI integration",
    };
  }

  // Bethesda
  if (!uiPresent) {
    if (air.existsInAirtable && air.airtableClass !== "N_A_OPEN_ASSET" && air.airtableClass !== "DERIVABLE_FROM_HI") {
      return {
        liveIntegrated: false,
        liveClass: "EXISTS_IN_AIRTABLE_NOT_CONNECTED",
        note: "Present in Airtable but not shown (or not wired) in HE",
      };
    }
    if (air.airtableClass === "DERIVABLE_FROM_HI") {
      return {
        liveIntegrated: false,
        liveClass: "EXISTS_IN_AIRTABLE_NOT_CONNECTED",
        note: "Underlying HI facts exist; derived HE view not connected",
      };
    }
    if (air.airtableClass === "N_A_OPEN_ASSET") {
      return { liveIntegrated: true, liveClass: "LIVE_N_A", note: "Correctly not required for Open asset" };
    }
    return {
      liveIntegrated: false,
      liveClass: "GENUINE_GAP",
      note: "Not in Airtable and not in HE — research or steward fill after reuse",
    };
  }

  // UI present on Bethesda
  if (meta.tab === "hotel") {
    if (air.existsInAirtable && air.airtableClass === "EXISTS_HPC") {
      return {
        liveIntegrated: true,
        liveClass: "LIVE_HPC_TO_UI",
        note: "HE Overview shows value that exists on HPC record",
      };
    }
    const fp = hotelDto?.fieldProvenance?.[meta.field];
    if (fp?.class === "VERIFIED_HPC") {
      return {
        liveIntegrated: true,
        liveClass: "LIVE_HPC_TO_UI",
        note: `HE Overview shows HPC-backed value (${fp.hpcField || "alias"})`,
      };
    }
    if (
      meta.allowVerifiedDerived &&
      (fp?.class === "VERIFIED_DERIVED" ||
        hotelDto?.meetingSpaceProvenance?.factKind === "VERIFIED_FACT")
    ) {
      return {
        liveIntegrated: true,
        liveClass: "LIVE_VERIFIED_DERIVED",
        note:
          fp?.note ||
          "HE Overview shows verified derived intelligence (ADP/HI profile + official source URL), not an HPC numeric field",
      };
    }
    // UI shows value but HPC field empty — enrichment / alias / wrong-path projection
    return {
      liveIntegrated: false,
      liveClass: "UI_POPULATED_NOT_FROM_VERIFIED_HPC_FIELD",
      note: "HE shows a value but the primary HPC field(s) are empty — likely enrichment, Brand Family fallback, or Census-path formatter; not verified HPC field integration",
    };
  }

  if (["ownership", "organization", "relationships"].includes(meta.tab)) {
    if (
      air.existsInAirtable &&
      (air.airtableClass === "EXISTS_HI_REL" ||
        air.airtableClass === "EXISTS_HI_ORG" ||
        air.airtableClass === "EXISTS_HI_CONTACTS" ||
        air.airtableClass === "EXISTS_HI_CHANNELS" ||
        air.airtableClass === "EXISTS_HI_EVIDENCE" ||
        air.airtableClass === "EXISTS_HI_RUNS")
    ) {
      return {
        liveIntegrated: true,
        liveClass: "LIVE_HI_GRAPH_TO_UI",
        note: "HE shows live HI graph Airtable data",
      };
    }
    if (air.airtableClass === "DERIVABLE_FROM_HI" && uiPresent) {
      return {
        liveIntegrated: false,
        liveClass: "UI_FROM_PARTIAL_OVERLAY",
        note: "Partial overlay/stub — not full gold-depth derived view from Airtable",
      };
    }
  }

  if (meta.tab === "contacts") {
    if (air.existsInAirtable && meta.field === "fallback_paths" && air.airtableClass === "EXISTS_HPC") {
      return {
        liveIntegrated: false,
        liveClass: "EXISTS_IN_AIRTABLE_NOT_CONNECTED",
        note: "Property phone/URL on HPC can seed fallback_paths but Contacts CI package not wired",
      };
    }
  }

  return {
    liveIntegrated: false,
    liveClass: "UI_POPULATED_UNVERIFIED_SOURCE",
    note: "UI populated; not attributed to verified HPC/HI field",
  };
}

function pct(n, d) {
  return d ? Math.round((n / d) * 1000) / 10 : 0;
}

async function countHiForOrgs(baseId, token, orgRecIds) {
  if (!orgRecIds.length) return { contacts: 0, channels: 0 };
  let contacts = 0;
  let channels = 0;
  // Contacts: filter by Organization link containing org id is hard via formula; list recent and filter client-side (small).
  const contactTable = HI["HI Org Contacts"];
  const channelTable = HI["HI Contact Channels"];
  for (const orgId of orgRecIds.slice(0, 5)) {
    const cFormula = encodeURIComponent(`FIND('${orgId}', ARRAYJOIN({Organization}))`);
    const cUrl = `https://api.airtable.com/v0/${baseId}/${contactTable}?filterByFormula=${cFormula}&maxRecords=20`;
    const cRes = await fetch(cUrl, { headers: { Authorization: `Bearer ${token}` } });
    const cJson = await cRes.json().catch(() => ({}));
    contacts += (cJson.records || []).length;

    const chFormula = encodeURIComponent(`FIND('${orgId}', ARRAYJOIN({Organization}))`);
    const chUrl = `https://api.airtable.com/v0/${baseId}/${channelTable}?filterByFormula=${chFormula}&maxRecords=20`;
    const chRes = await fetch(chUrl, { headers: { Authorization: `Bearer ${token}` } });
    const chJson = await chRes.json().catch(() => ({}));
    channels += (chJson.records || []).length;
  }
  return { contacts, channels };
}

async function main() {
  loadEnv();
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  const baseUrl = resolveHeAuditBaseUrl(process.argv, process.env);
  const runtime = await assertHeLocalRuntimeTarget({ baseUrl });
  if (!runtime.ok) {
    console.error(runtime);
    process.exit(2);
  }
  if (!token || baseId !== GRAPH_BASE_ID_CANONICAL) {
    console.error("Expected AIRTABLE_BASE_ID_ALT=appCCUsuGsE1ifoLk with API key");
    process.exit(2);
  }

  // Identity
  const kgpvCensus = await atFind(baseId, CENSUS.id, KGPV_ID, token);
  const kgpvHpc = await atFind(baseId, HPC.id, KGPV_ID, token);
  const bethCensus = await atFind(baseId, CENSUS.id, BETH_ID, token);
  const bethHpc = await atFind(baseId, HPC.id, BETH_ID, token);

  // Live APIs
  async function pack(id) {
    const brandPresence = await fetchJson(`${baseUrl}/api/brand-presence/hotel/${id}`);
    const ownership = await fetchJson(`${baseUrl}/api/golden-demo/ownership/hotel/${id}`);
    const contacts = await fetchJson(`${baseUrl}/api/contact-intelligence/hotels/${id}?audience=internal`);
    const graph = await fetchJson(`${baseUrl}/api/he-airtable-graph/hotel/${id}`);
    let org = { ok: false, json: null };
    const slug = ownership.json?.organization?.slug || ownership.json?.ownership_group?.slug;
    if (slug) org = await fetchJson(`${baseUrl}/api/golden-demo/ownership/groups/${encodeURIComponent(slug)}`);
    return { brandPresence, ownership, contacts, graph, org };
  }

  const kgpvBundle = await pack(KGPV_ID);
  const bethBundle = await pack(BETH_ID);

  const bethHpcFields = bethHpc?.fields || {};
  const bethGraph = bethBundle.graph.json || {};
  const orgRecIds = (bethGraph.organizations || []).map((o) => o.airtableId).filter(Boolean);
  const hiCounts = await countHiForOrgs(baseId, token, orgRecIds);

  // Evidence / runs for hotel — best-effort formula on HPC Hotel ID text
  async function countByHpcId(tableId) {
    const formula = encodeURIComponent(`{HPC Hotel ID}='${BETH_ID}'`);
    const url = `https://api.airtable.com/v0/${baseId}/${tableId}?filterByFormula=${formula}&maxRecords=50`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = await res.json().catch(() => ({}));
    return (json.records || []).length;
  }
  let evidenceCount = 0;
  let runsCount = 0;
  try {
    evidenceCount = await countByHpcId(HI["HI Graph Evidence"]);
  } catch {
    evidenceCount = 0;
  }
  try {
    runsCount = await countByHpcId(HI["HI Ownership Research Runs"]);
  } catch {
    runsCount = 0;
  }

  const bethAirCtx = {
    hpcFields: bethHpcFields,
    graph: bethGraph,
    contactsCount: hiCounts.contacts,
    channelsCount: hiCounts.channels,
    evidenceCount,
    runsCount,
  };

  const rows = CATALOG.map((meta) => {
    const kgpvUi = extractUi(meta.field, KGPV_ID, kgpvBundle);
    const bethUi = extractUi(meta.field, BETH_ID, bethBundle);
    const kgpvUiPresent = nonempty(kgpvUi);
    const bethUiPresent = nonempty(bethUi);
    const air = assessBethAirtable(meta, bethAirCtx);
    const bethHotel = bethBundle.brandPresence?.json?.hotel || null;
    const kgpvHotel = kgpvBundle.brandPresence?.json?.hotel || null;
    const liveK = assessLiveIntegration(
      meta,
      kgpvUiPresent,
      air,
      KGPV_ID,
      true,
      kgpvBundle.graph.json,
      kgpvHotel,
    );
    const liveB = assessLiveIntegration(
      meta,
      bethUiPresent,
      air,
      BETH_ID,
      false,
      bethGraph,
      bethHotel,
    );

    let gate1Action = "NONE";
    if (liveB.liveClass === "EXISTS_IN_AIRTABLE_NOT_CONNECTED") gate1Action = "INTEGRATION_WIRE";
    else if (liveB.liveClass === "UI_POPULATED_NOT_FROM_VERIFIED_HPC_FIELD") gate1Action = "FIX_PROVENANCE_OR_BACKFILL_HPC";
    else if (liveB.liveClass === "GENUINE_GAP") gate1Action = "RESEARCH_OR_STEWARD_AFTER_REUSE";
    else if (liveB.liveClass === "UI_FROM_PARTIAL_OVERLAY") gate1Action = "INTEGRATION_DEEPEN_FROM_HI";
    else if (
      liveB.liveClass === "LIVE_HPC_TO_UI" ||
      liveB.liveClass === "LIVE_HI_GRAPH_TO_UI" ||
      liveB.liveClass === "LIVE_VERIFIED_DERIVED" ||
      liveB.liveClass === "LIVE_N_A"
    )
      gate1Action = "REUSE_OK";

    return {
      tab: meta.tab,
      field: meta.field,
      question: meta.question,
      goldRequired: meta.goldRequired !== false,
      kgpv: {
        demonstratedUi: kgpvUiPresent,
        liveClass: liveK.liveClass,
        note: "Golden-demo UX reference only — not canonical Airtable proof",
      },
      bethesda: {
        uiPresent: bethUiPresent,
        airtableExists: air.existsInAirtable,
        airtableClass: air.airtableClass,
        airtableDetail: air.detail,
        liveClass: liveB.liveClass,
        liveIntegrated: liveB.liveIntegrated,
        note: liveB.note,
      },
      gate1Action,
    };
  });

  const goldRequired = rows.filter((r) => r.goldRequired);
  const kgpvDemoCoverage = {
    fields: goldRequired.length,
    demonstratedUi: goldRequired.filter((r) => r.kgpv.demonstratedUi).length,
    demonstratedUiPct: pct(
      goldRequired.filter((r) => r.kgpv.demonstratedUi).length,
      goldRequired.length,
    ),
    note: "KGPV UI population = desired experience depth. NOT verified Airtable coverage.",
  };

  const bethExistingAirtable = rows.filter(
    (r) =>
      r.bethesda.airtableExists &&
      !["N_A_OPEN_ASSET"].includes(r.bethesda.airtableClass),
  );
  const bethExistingSourceCoverage = {
    fields: rows.length,
    existsInAirtable: bethExistingAirtable.length,
    existsInAirtablePct: pct(bethExistingAirtable.length, rows.length),
    byClass: {},
  };
  for (const r of rows) {
    const c = r.bethesda.airtableClass;
    bethExistingSourceCoverage.byClass[c] = (bethExistingSourceCoverage.byClass[c] || 0) + 1;
  }

  const bethNotConnected = rows.filter((r) => r.bethesda.liveClass === "EXISTS_IN_AIRTABLE_NOT_CONNECTED");
  const bethLiveIntegrated = rows.filter((r) => r.bethesda.liveIntegrated);
  const bethGenuineGaps = rows.filter((r) => r.bethesda.liveClass === "GENUINE_GAP");
  const bethUiNotFromHpc = rows.filter(
    (r) => r.bethesda.liveClass === "UI_POPULATED_NOT_FROM_VERIFIED_HPC_FIELD",
  );
  const integrationBeforePaid = rows.filter((r) =>
    ["INTEGRATION_WIRE", "FIX_PROVENANCE_OR_BACKFILL_HPC", "INTEGRATION_DEEPEN_FROM_HI"].includes(
      r.gate1Action,
    ),
  );
  const researchAfterReuse = rows.filter((r) => r.gate1Action === "RESEARCH_OR_STEWARD_AFTER_REUSE");

  const metrics = {
    kgpvGoldenDemoFieldCoveragePct: kgpvDemoCoverage.demonstratedUiPct,
    bethesdaExistingVerifiedAirtableCoveragePct: bethExistingSourceCoverage.existsInAirtablePct,
    bethesdaAirtableNotYetConnectedPct: pct(bethNotConnected.length, rows.length),
    bethesdaLiveAirtableToUiIntegrationPct: pct(bethLiveIntegrated.length, rows.length),
    bethesdaGenuineResearchGapsAfterReusePct: pct(bethGenuineGaps.length, rows.length),
    bethesdaUiPopulatedWithoutVerifiedHpcFieldPct: pct(bethUiNotFromHpc.length, rows.length),
    integrationWorkBeforePaidCount: integrationBeforePaid.length,
    researchGapCount: researchAfterReuse.length,
  };

  const integrationWork = [
    {
      id: "fix_brand_presence_source_label",
      priority: "P0",
      summary:
        "Stop mislabeling HPC reads as Hotel Census; resolve owning table via RECORD_ID() and set censusSourceTable correctly.",
    },
    {
      id: "wire_overview_from_hpc_formatter",
      priority: "P0",
      summary:
        "For HPC-owned hotels (Bethesda), use formatHotelPropertyCensusAsBrandPresenceHotel (or equivalent) so Overview provenance is true HPC — and backfill empty HPC fields that UI currently invents via Brand Family / enrichment.",
    },
    {
      id: "wire_org_relationships_tabs_from_hi_graph",
      priority: "P0",
      summary:
        "Connect Organization + Relationships tabs to he-airtable-graph reader so Operator/Brand orgs and edges reach gold-depth UI without fixtures.",
    },
    {
      id: "derive_portfolio_views",
      priority: "P1",
      summary:
        "Build portfolio_kpis / operated_managed_hotels / structure_chart from HI Rel queries once org is linked.",
    },
    {
      id: "contacts_fallback_from_hpc",
      priority: "P1",
      summary:
        "Wire Contacts fallback_paths from HPC Phone + Official Property URL before paid person enrichment.",
    },
    {
      id: "kgpv_identity_unification",
      priority: "P1",
      summary:
        "Unify KGPV onto HPC (or approved link strategy) before any golden-demo → Airtable migration. Fixtures stay UX reference only until independently verified.",
    },
  ];

  const report = {
    contract: "HE_TABS_1_5_GATE1_KGPV_GOLD_BETH_AIRTABLE_METRICS_V1",
    gate: "GATE_1_REVISED_KGPV_GOLDEN_DEMO_CLARIFICATION",
    status: "AWAITING_FOUNDER_APPROVAL",
    auditedAt: new Date().toISOString(),
    principle:
      "KGPV defines the desired experience. HPC and established Airtable tables define canonical intelligence. Do not migrate golden-demo fixtures without independent verification.",
    paidResearch: {
      substantivePaidEnrichment: "BLOCKED_UNTIL_GATE1_APPROVAL",
      spendToDateUsd: 0,
      authorizedCeilingUsd: 25,
    },
    baseUrl,
    runtimeTarget: {
      servingDirectoryBasename: runtime.servingDirectoryBasename,
      servingStream: runtime.servingStream,
    },
    architecture: {
      kgpvRole: "GOLDEN_DEMO_UX_REFERENCE",
      bethesdaRole: "AIRTABLE_FIRST_VALIDATION_PILOT",
      canonicalBase: GRAPH_BASE_ID_CANONICAL,
      hpc: HPC,
      hotelCensus: CENSUS,
      hiTables: HI,
    },
    identity: {
      kgpv: {
        id: KGPV_ID,
        owningTable: kgpvCensus ? CENSUS.name : null,
        owningTableId: kgpvCensus ? CENSUS.id : null,
        onHpc: Boolean(kgpvHpc),
        status: kgpvCensus && !kgpvHpc ? "HOTEL_CENSUS_NOT_HPC" : "CHECK",
      },
      bethesda: {
        id: BETH_ID,
        owningTable: bethHpc ? HPC.name : null,
        owningTableId: bethHpc ? HPC.id : null,
        onHotelCensus: Boolean(bethCensus),
        status: bethHpc && !bethCensus ? "HPC_CANONICAL" : "CHECK",
        hiRelationships: (bethGraph.relationships || []).map((r) => ({
          type: r.relationshipType,
          airtableId: r.airtableId,
        })),
        hiOrganizations: (bethGraph.organizations || []).map((o) => ({
          airtableId: o.airtableId,
          legalName: o.legalName,
          tradingName: o.tradingName,
        })),
        hiOrgContactsCount: hiCounts.contacts,
        hiContactChannelsCount: hiCounts.channels,
        hiEvidenceCount: evidenceCount,
        hiRunsCount: runsCount,
      },
    },
    metrics,
    scorecard: {
      kgpvGoldenDemoFieldCoverage: kgpvDemoCoverage,
      bethesdaExistingVerifiedAirtableCoverage: bethExistingSourceCoverage,
      bethesdaAirtableNotYetConnected: {
        count: bethNotConnected.length,
        pct: pct(bethNotConnected.length, rows.length),
        fields: bethNotConnected.map((r) => r.field),
      },
      bethesdaLiveAirtableToUiIntegration: {
        count: bethLiveIntegrated.length,
        pct: pct(bethLiveIntegrated.length, rows.length),
      },
      bethesdaGenuineResearchGapsAfterReuse: {
        count: bethGenuineGaps.length,
        pct: pct(bethGenuineGaps.length, rows.length),
        fields: bethGenuineGaps.map((r) => r.field),
      },
      requiredIntegrationBeforePaidEnrichment: {
        count: integrationBeforePaid.length,
        fields: integrationBeforePaid.map((r) => ({
          field: r.field,
          tab: r.tab,
          action: r.gate1Action,
          liveClass: r.bethesda.liveClass,
        })),
      },
    },
    integrationWork,
    researchPrioritiesAfterIntegration: [
      {
        priority: 1,
        field: "economic_owner",
        action: "Confirm CURRENT Bethesda economic owner (free SDAT/filings first; paid only if blocked). Do not use 2013 Northview/Oaktree as current claim.",
        estimatedUsd: "8–15",
      },
      {
        priority: 2,
        fields: ["property_company", "ubo_group"],
        action: "After economic owner — PropCo/UBO if still missing.",
        estimatedUsd: "0–5",
      },
      {
        priority: 3,
        fields: ["decision_authority.people", "emails_phones", "people_groups", "key_people"],
        action: "Decision-maker contacts only after owner known; public paths before paid.",
        estimatedUsd: "5–10",
      },
      {
        priority: 4,
        hotel: "KGPV",
        action: "No paid research / no fixture migration until HPC identity unification + independent fact verification.",
        estimatedUsd: "0",
      },
    ],
    fields: rows,
  };

  fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2));

  const md = [];
  md.push("# Gate 1 — Founder Review (Revised: KGPV Golden Demo Clarification)");
  md.push("");
  md.push(`**Status:** AWAITING FOUNDER APPROVAL — substantive paid research **blocked**`);
  md.push(`**Audited:** ${report.auditedAt}`);
  md.push(`**Runtime:** ${baseUrl}`);
  md.push("");
  md.push("## Architecture principle");
  md.push("");
  md.push("**KGPV defines the desired experience. HPC and the appropriate established Airtable tables define the canonical intelligence.**");
  md.push("");
  md.push("- KGPV = golden demonstration of UI structure, field coverage, content depth, and intended end-user experience.");
  md.push("- KGPV fixtures / showcase / local Contact Intelligence are **not** verified canonical Airtable data.");
  md.push("- Bethesda Marriott = Airtable-first research, mapping, integration, and persistence validation pilot.");
  md.push("- Do **not** migrate golden-demo fixtures into production Airtable without independent verification.");
  md.push("");
  md.push("## Identity");
  md.push("");
  md.push("| Hotel | Role | Owning Airtable table |");
  md.push("|---|---|---|");
  md.push(`| KGPV (\`${KGPV_ID}\`) | Golden demo UX | **Hotel Census** (\`${CENSUS.id}\`) — not on HPC |`);
  md.push(`| Bethesda (\`${BETH_ID}\`) | Airtable-first pilot | **Hotel Property Census** (\`${HPC.id}\`) |`);
  md.push("");
  md.push(`Base: \`${GRAPH_BASE_ID_CANONICAL}\` (\`AIRTABLE_BASE_ID_ALT\`).`);
  md.push("");
  md.push("## Gate 1 scorecard (separated metrics)");
  md.push("");
  md.push("| Metric | Value | Meaning |");
  md.push("|---|---:|---|");
  md.push(`| **KGPV golden-demo field coverage** | **${metrics.kgpvGoldenDemoFieldCoveragePct}%** (${kgpvDemoCoverage.demonstratedUi}/${kgpvDemoCoverage.fields} gold-required) | Demonstrated UI depth — **not** Airtable proof |`);
  md.push(`| **Bethesda existing verified Airtable coverage** | **${metrics.bethesdaExistingVerifiedAirtableCoveragePct}%** (${bethExistingSourceCoverage.existsInAirtable}/${rows.length}) | Facts already in HPC/HI (independent of HE wiring) |`);
  md.push(`| **Bethesda Airtable not yet connected to HE** | **${metrics.bethesdaAirtableNotYetConnectedPct}%** (${bethNotConnected.length}) | Exists in Airtable; HE not showing / not wired |`);
  md.push(`| **Bethesda live Airtable→UI integration** | **${metrics.bethesdaLiveAirtableToUiIntegrationPct}%** (${bethLiveIntegrated.length}/${rows.length}) | HE actually serving verified HPC/HI values |`);
  md.push(`| **Genuine research gaps (after reuse)** | **${metrics.bethesdaGenuineResearchGapsAfterReusePct}%** (${bethGenuineGaps.length}) | Missing in Airtable and HE — candidates for research post-integration |`);
  md.push(`| **UI populated without verified HPC field** | **${metrics.bethesdaUiPopulatedWithoutVerifiedHpcFieldPct}%** (${bethUiNotFromHpc.length}) | Overview shows values not present on primary HPC fields (enrichment/fallback) |`);
  md.push(`| **Integration work items before paid** | **${metrics.integrationWorkBeforePaidCount}** fields | Must wire/fix provenance before enrichment spend |`);
  md.push("");
  md.push("## What already exists in Airtable (Bethesda)");
  md.push("");
  md.push(`HI relationships: ${(bethGraph.relationships || []).map((r) => r.relationshipType).join(", ") || "none"}`);
  md.push("");
  md.push(`HI orgs: ${(bethGraph.organizations || []).map((o) => o.legalName || o.tradingName).join("; ") || "none"}`);
  md.push("");
  md.push(`HI Org Contacts: ${hiCounts.contacts} · Channels: ${hiCounts.channels} · Evidence: ${evidenceCount} · Runs: ${runsCount}`);
  md.push("");
  md.push("### Existing Airtable not yet connected");
  md.push("");
  if (!bethNotConnected.length) md.push("_None classified in this pass._");
  for (const r of bethNotConnected) {
    md.push(`- **${r.field}** (${r.tab}) — ${r.bethesda.airtableDetail}`);
  }
  md.push("");
  md.push("### Genuine research gaps (after source reuse)");
  md.push("");
  for (const r of bethGenuineGaps) {
    md.push(`- **${r.field}** (${r.tab}) — ${r.bethesda.note}`);
  }
  md.push("");
  md.push("## Required integration work before paid enrichment");
  md.push("");
  for (const w of integrationWork) {
    md.push(`1. **${w.id}** (${w.priority}) — ${w.summary}`);
  }
  md.push("");
  md.push("## Research priorities (only after integration reuse)");
  md.push("");
  for (const r of report.researchPrioritiesAfterIntegration) {
    md.push(`- P${r.priority}: ${r.action} _(est. $${r.estimatedUsd})_`);
  }
  md.push("");
  md.push("## Field appendix");
  md.push("");
  md.push("| Field | Tab | KGPV demo UI | Bethesda Airtable | Bethesda live→UI | Action |");
  md.push("|---|---|---|---|---|---|");
  for (const r of rows) {
    md.push(
      `| ${r.field} | ${r.tab} | ${r.kgpv.demonstratedUi ? "YES" : ""} | ${r.bethesda.airtableClass} | ${r.bethesda.liveClass} | ${r.gate1Action} |`,
    );
  }
  md.push("");
  md.push("---");
  md.push("");
  md.push(`Machine-readable: \`${path.basename(OUT_JSON)}\``);
  md.push("");
  md.push("**Founder decision:** Approve revised Gate 1 (KGPV=gold UX; Bethesda=Airtable-first), then authorize integration work. Substantive paid research stays blocked until approval.");

  fs.writeFileSync(OUT_MD, md.join("\n"));
  console.log(
    JSON.stringify(
      {
        ok: true,
        outJson: OUT_JSON,
        outMd: OUT_MD,
        metrics,
        integrationWorkBeforePaid: metrics.integrationWorkBeforePaidCount,
        researchGaps: metrics.researchGapCount,
      },
      null,
      2,
    ),
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
