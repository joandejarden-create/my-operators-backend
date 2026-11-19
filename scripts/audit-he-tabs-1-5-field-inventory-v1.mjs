#!/usr/bin/env node
/**
 * HE Tabs 1–5 field inventory + baseline metrics (Airtable-first audit).
 * Read-only. Probes local HE APIs; optional live HI graph via Integration .env.
 *
 *   node scripts/audit-he-tabs-1-5-field-inventory-v1.mjs
 *   node scripts/audit-he-tabs-1-5-field-inventory-v1.mjs --base-url http://127.0.0.1:8080
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function arg(flag, fallback = "") {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] || fallback : fallback;
}

const BASE = arg("--base-url", "http://127.0.0.1:8080").replace(/\/$/, "");
const OUT_JSON = path.join(ROOT, "reports/hotel-explorer/HE-TABS-1-5-FIELD-INVENTORY-V1.json");
const OUT_MD = path.join(ROOT, "reports/hotel-explorer/HE-TABS-1-5-FIELD-INVENTORY-V1.md");

const HOTELS = [
  {
    id: "recUNycnMwOVFX0hc",
    label: "Krystal Grand Puerto Vallarta (golden-demo reference)",
    role: "REFERENCE_AUDIT",
  },
  {
    id: "recLuxvwwxID7U2B8",
    label: "Bethesda Marriott (repeatability / pilot)",
    role: "REPEATABILITY_TARGET",
  },
];

/** Canonical tabs 1–5 of the 11-tab Hotel Explorer overlay. */
const FIELD_CATALOG = [
  // Tab 1 — Hotel Overview (HDP)
  { tab: "hotel", section: "Header", field: "name", entity: "hotel", airtable: "HPC/Hotel Census:name|Property Name", writeDest: "HPC" },
  { tab: "hotel", section: "Header", field: "brand", entity: "hotel", airtable: "HPC:Current Brand|Affiliation", writeDest: "HPC" },
  { tab: "hotel", section: "Header", field: "status", entity: "hotel", airtable: "HPC:project_phase|status", writeDest: "HPC" },
  { tab: "hotel", section: "Header", field: "website", entity: "hotel", airtable: "HPC:Website|Official Property URL", writeDest: "HPC" },
  { tab: "hotel", section: "Header", field: "telephone", entity: "hotel", airtable: "HPC:Phone|Telephone", writeDest: "HPC" },
  { tab: "hotel", section: "Header", field: "address1", entity: "hotel", airtable: "HPC:Address 1", writeDest: "HPC" },
  { tab: "hotel", section: "Header", field: "city", entity: "hotel", airtable: "HPC:city", writeDest: "HPC" },
  { tab: "hotel", section: "Header", field: "country", entity: "hotel", airtable: "HPC:country", writeDest: "HPC" },
  { tab: "hotel", section: "Description", field: "hotelHeadline", entity: "hotel", airtable: "HPC:Hotel Headline / enrichment", writeDest: "HPC_or_ADP_profile" },
  { tab: "hotel", section: "Description", field: "hotelDescription", entity: "hotel", airtable: "HPC:Short Property Summary|description", writeDest: "HPC_or_ADP_profile" },
  { tab: "hotel", section: "Property Facts", field: "parentCompany", entity: "brand_parent", airtable: "HPC:Parent Company", writeDest: "HPC" },
  { tab: "hotel", section: "Property Facts", field: "chainScale", entity: "hotel", airtable: "HPC:Chain Scale", writeDest: "HPC" },
  { tab: "hotel", section: "Property Facts", field: "censusPropertyType", entity: "hotel", airtable: "HPC:Property Type", writeDest: "HPC" },
  { tab: "hotel", section: "Property Facts", field: "rooms", entity: "hotel", airtable: "HPC:Rooms / Keys|rooms", writeDest: "HPC" },
  { tab: "hotel", section: "Property Facts", field: "totalMeetingSpace", entity: "hotel", airtable: "HPC:Total Meeting Space", writeDest: "HPC" },
  { tab: "hotel", section: "Property Facts", field: "operationType", entity: "hotel", airtable: "HPC:Operation Type", writeDest: "HPC" },
  { tab: "hotel", section: "Property Facts", field: "managementCompany", entity: "operator", airtable: "HPC:Operator / Management Company", writeDest: "HPC_projection_only" },
  { tab: "hotel", section: "Property Facts", field: "market", entity: "hotel", airtable: "HPC:Market", writeDest: "HPC" },
  { tab: "hotel", section: "Property Facts", field: "submarket", entity: "hotel", airtable: "HPC:Submarket", writeDest: "HPC" },
  { tab: "hotel", section: "Property Facts", field: "locationType", entity: "hotel", airtable: "HPC:Location", writeDest: "HPC" },
  { tab: "hotel", section: "Property Facts", field: "projectPhase", entity: "hotel", airtable: "HPC:project_phase", writeDest: "HPC" },
  { tab: "hotel", section: "Property Facts", field: "projectedOpenDate", entity: "hotel", airtable: "HPC:projected_open_date", writeDest: "HPC" },
  { tab: "hotel", section: "Property Facts", field: "starRating", entity: "hotel", airtable: "HPC:Star Rating", writeDest: "HPC" },
  { tab: "hotel", section: "Property Facts", field: "dataConfidence", entity: "hotel", airtable: "HPC:Data Confidence", writeDest: "HPC" },
  { tab: "hotel", section: "Amenities", field: "amenitiesDisplay", entity: "hotel", airtable: "HPC amenities + ADP Attributes (verified)", writeDest: "HPC_and_or_ADP_Attributes" },

  // Tab 2 — Ownership
  { tab: "ownership", section: "Executive Summary", field: "executive_summary.text", entity: "ownership_case", airtable: "HI Ownership Research Runs + HI Graph Evidence", writeDest: "HI_graph" },
  { tab: "ownership", section: "Executive Summary", field: "what_remains_uncertain", entity: "ownership_case", airtable: "HI Graph Evidence / research gaps", writeDest: "HI_graph" },
  { tab: "ownership", section: "Meta", field: "economic_owner", entity: "org_relationship", airtable: "HI Hotel–Org Relationships (Economic Owner) → HI Organizations", writeDest: "HI_graph" },
  { tab: "ownership", section: "Meta", field: "property_company", entity: "org_relationship", airtable: "HI Hotel–Org Relationships (Legal Owner) → HI Organizations", writeDest: "HI_graph" },
  { tab: "ownership", section: "Meta", field: "operator", entity: "org_relationship", airtable: "HI Hotel–Org Relationships (Operator) → HI Organizations", writeDest: "HI_graph" },
  { tab: "ownership", section: "Meta", field: "control_status", entity: "ownership_case", airtable: "derived from relationship Verification State", writeDest: "HI_graph" },
  { tab: "ownership", section: "Meta", field: "source_coverage", entity: "evidence", airtable: "HI Graph Evidence count", writeDest: "HI_graph" },
  { tab: "ownership", section: "Insights", field: "what_we_know[]", entity: "ownership_case", airtable: "HI Graph Evidence / run summary", writeDest: "HI_graph" },
  { tab: "ownership", section: "Ownership & Control", field: "control_rows", entity: "org_relationship", airtable: "HI Hotel–Org Relationships by role", writeDest: "HI_graph" },
  { tab: "ownership", section: "UBO", field: "ubo_group", entity: "org", airtable: "HI Organizations (Parent / UBO)", writeDest: "HI_graph" },
  { tab: "ownership", section: "Portfolio Preview", field: "portfolio_preview", entity: "org", airtable: "HI Hotel–Org Relationships for org across HPC", writeDest: "HI_graph" },
  { tab: "ownership", section: "Franchise & Brand", field: "brand_status", entity: "brand_relationship", airtable: "HI Hotel–Org Relationships (Brand) + HPC Affiliation", writeDest: "HI_graph_and_HPC" },
  { tab: "ownership", section: "People", field: "decision_authority.people", entity: "person", airtable: "HI Org Contacts", writeDest: "HI_graph" },
  { tab: "ownership", section: "Signatories", field: "qualified_signatories", entity: "person", airtable: "HI Org Contacts (authority flags)", writeDest: "HI_graph" },
  { tab: "ownership", section: "Channels", field: "corporate_contact_channels", entity: "channel", airtable: "HI Contact Channels", writeDest: "HI_graph" },
  { tab: "ownership", section: "Asset Profile", field: "asset_agreement_profile", entity: "hotel", airtable: "HPC + HI relationships", writeDest: "HPC_and_HI_graph" },
  { tab: "ownership", section: "History", field: "property_history", entity: "hotel", airtable: "HI Graph Evidence (historical)", writeDest: "HI_graph" },
  { tab: "ownership", section: "Pursuit", field: "development_pursuit", entity: "ownership_case", airtable: "HI Ownership Research Runs notes", writeDest: "HI_graph" },
  { tab: "ownership", section: "Gaps", field: "research_gaps", entity: "ownership_case", airtable: "HI Ownership Research Runs", writeDest: "HI_graph" },
  { tab: "ownership", section: "References", field: "sources", entity: "evidence", airtable: "HI Graph Evidence", writeDest: "HI_graph" },

  // Tab 3 — Contacts
  { tab: "contacts", section: "Quality", field: "quality_summary", entity: "contact_package", airtable: "derived from HI Org Contacts + Channels", writeDest: "HI_graph" },
  { tab: "contacts", section: "Ownership Summary", field: "ownershipSummary", entity: "ownership_case", airtable: "HI relationships projection", writeDest: "HI_graph" },
  { tab: "contacts", section: "Orgs", field: "organization_contacts", entity: "org", airtable: "HI Organizations + Channels", writeDest: "HI_graph" },
  { tab: "contacts", section: "People", field: "people_groups", entity: "person", airtable: "HI Org Contacts", writeDest: "HI_graph" },
  { tab: "contacts", section: "Channels", field: "emails_phones", entity: "channel", airtable: "HI Contact Channels", writeDest: "HI_graph" },
  { tab: "contacts", section: "Fallback", field: "fallback_paths", entity: "contact_package", airtable: "HI Contact Channels (public/fallback)", writeDest: "HI_graph" },
  { tab: "contacts", section: "Unresolved", field: "research_unresolved", entity: "contact_package", airtable: "HI Ownership Research Runs", writeDest: "HI_graph" },

  // Tab 4 — Organization
  { tab: "organization", section: "Overview", field: "display_name", entity: "org", airtable: "HI Organizations:Trading/Legal Name", writeDest: "HI_graph" },
  { tab: "organization", section: "Overview", field: "legal_name", entity: "org", airtable: "HI Organizations:Legal Name", writeDest: "HI_graph" },
  { tab: "organization", section: "Overview", field: "ticker", entity: "org", airtable: "HI Organizations:Registry / Identifier", writeDest: "HI_graph" },
  { tab: "organization", section: "KPIs", field: "portfolio_kpis", entity: "org", airtable: "derived from HI Hotel–Org Relationships", writeDest: "derived_read_model" },
  { tab: "organization", section: "Owned", field: "owned_controlled_hotels", entity: "org_relationship", airtable: "HI Hotel–Org Relationships (owner roles)", writeDest: "HI_graph" },
  { tab: "organization", section: "Operated", field: "operated_managed_hotels", entity: "org_relationship", airtable: "HI Hotel–Org Relationships (Operator)", writeDest: "HI_graph" },
  { tab: "organization", section: "People", field: "key_people", entity: "person", airtable: "HI Org Contacts", writeDest: "HI_graph" },
  { tab: "organization", section: "Context", field: "why_this_matters", entity: "org", airtable: "HI Organizations:Notes / Evidence", writeDest: "HI_graph" },

  // Tab 5 — Relationships
  { tab: "relationships", section: "Structure", field: "structure_chart", entity: "org_relationship", airtable: "HI Hotel–Org Relationships graph", writeDest: "HI_graph" },
  { tab: "relationships", section: "Edges", field: "relationship_edges", entity: "org_relationship", airtable: "HI Hotel–Org Relationships", writeDest: "HI_graph" },
  { tab: "relationships", section: "Network", field: "org_network", entity: "org", airtable: "HI Organizations + relationships", writeDest: "HI_graph" },
];

function nonempty(v) {
  if (v == null) return false;
  if (typeof v === "number") return Number.isFinite(v) && !(v === 0 && arguments[1] === "rooms");
  if (typeof v === "boolean") return true;
  if (Array.isArray(v)) return v.length > 0;
  if (typeof v === "object") return Object.keys(v).length > 0;
  const s = String(v).trim();
  return s.length > 0 && s !== "—" && s !== "unknown" && s.toLowerCase() !== "null";
}

function getPath(obj, dotted) {
  if (!obj) return undefined;
  if (!dotted.includes(".")) return obj[dotted];
  return dotted.split(".").reduce((acc, k) => (acc == null ? undefined : acc[k]), obj);
}

async function fetchJson(url) {
  const res = await fetch(url);
  const text = await res.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    json = { _raw: text.slice(0, 200) };
  }
  return { ok: res.ok, status: res.status, json, bytes: text.length };
}

function loadDotenvFromIntegration() {
  try {
    const dotenv = require("dotenv");
    const envPath = path.join("C:/Dev/dealality/my-operators-backend", ".env");
    if (fs.existsSync(envPath)) dotenv.config({ path: envPath });
  } catch {
    /* optional */
  }
}

async function probeHiGraph(hpcId) {
  loadDotenvFromIntegration();
  const key = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_TOKEN || process.env.AIRTABLE_PAT;
  const baseId = "appCCUsuGsE1ifoLk";
  const relTable = "tblGiwN0foXXDESvZ";
  if (!key) {
    return { available: false, reason: "NO_AIRTABLE_CREDENTIALS", relationships: 0 };
  }
  const formula = encodeURIComponent(`{HPC Hotel ID}='${hpcId}'`);
  const url = `https://api.airtable.com/v0/${baseId}/${relTable}?filterByFormula=${formula}&maxRecords=50`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${key}` } });
  if (!res.ok) {
    return { available: true, reason: `HTTP_${res.status}`, relationships: 0 };
  }
  const body = await res.json();
  return {
    available: true,
    reason: "OK",
    relationships: Array.isArray(body.records) ? body.records.length : 0,
    sample: (body.records || []).slice(0, 3).map((r) => ({
      id: r.id,
      type: r.fields?.["Relationship Type"],
      org: r.fields?.["Organization ID"] || r.fields?.["Organization"],
      current: r.fields?.["Current?"],
    })),
  };
}

function assessHotelFields(hotelId, payloads) {
  const { brandPresence, ownership, contacts, org } = payloads;
  const hotel = brandPresence?.json?.hotel || {};
  const report = ownership?.json?.report || {};
  const inCohort = ownership?.json?.in_cohort === true;
  const contactVm = contacts?.json || {};
  const rows = [];

  for (const spec of FIELD_CATALOG) {
    let value;
    let existingSource = "unknown";
    let populated = false;

    if (spec.tab === "hotel") {
      value = hotel[spec.field];
      if (spec.field === "amenitiesDisplay") {
        value = hotel.amenitiesDisplay || hotel.amenities;
      }
      existingSource = brandPresence?.json?.source || "brand-presence/HPC-or-fixture";
      populated = nonempty(value, spec.field);
    } else if (spec.tab === "ownership") {
      existingSource = inCohort ? "golden-demo/fixtures" : "stub/out_of_cohort";
      if (spec.field === "executive_summary.text") value = getPath(report, "executive_summary.text") || report.executive_summary;
      else if (spec.field === "economic_owner") value = getPath(report, "ownership_and_control.economic_owner") || report.economic_owner;
      else if (spec.field === "property_company") value = getPath(report, "ownership_and_control.property_company");
      else if (spec.field === "operator") value = getPath(report, "ownership_and_control.operator");
      else if (spec.field === "decision_authority.people") value = getPath(report, "decision_authority.people");
      else if (spec.field === "sources") value = report.sources || getPath(report, "references");
      else if (spec.field === "what_we_know[]") value = report.what_we_know || getPath(report, "key_strategic_insights");
      else if (spec.field === "control_status") value = getPath(report, "ownership_and_control.control_status") || (inCohort ? "present" : null);
      else if (spec.field === "source_coverage") value = report.sources?.length || getPath(ownership?.json, "deep_research.sources");
      else value = getPath(report, spec.field) || report[spec.field];
      populated = inCohort && nonempty(value);
      if (!inCohort) populated = false;
    } else if (spec.tab === "contacts") {
      const showcase = hotelId === "recUNycnMwOVFX0hc";
      existingSource = showcase ? "showcase JSON / contact-intelligence API" : "contact-intelligence API (sparse)";
      if (spec.field === "quality_summary") value = contactVm.quality || contactVm.qualitySummary || contactVm.summary;
      else if (spec.field === "ownershipSummary") value = contactVm.ownershipSummary || contactVm.ownership_summary;
      else if (spec.field === "people_groups") value = contactVm.people || contactVm.peopleGroups || contactVm.groups;
      else if (spec.field === "organization_contacts") value = contactVm.organizations || contactVm.orgs;
      else if (spec.field === "emails_phones") {
        value = contacts?.bytes > 2000 ? `payload_bytes=${contacts.bytes}` : contactVm.channels;
      } else value = contactVm[spec.field];
      populated = nonempty(value) || (showcase && contacts?.bytes > 5000);
    } else if (spec.tab === "organization") {
      existingSource = org?.ok ? "golden-demo/groups" : "unavailable";
      const o = org?.json?.organization || org?.json?.group || org?.json || {};
      if (spec.field === "display_name") value = o.display_name || o.name || o.legal_name;
      else if (spec.field === "portfolio_kpis") value = o.kpis || o.portfolio_kpis || o.metrics;
      else if (spec.field === "owned_controlled_hotels") value = o.owned || o.owned_hotels || o.portfolio;
      else value = o[spec.field] || getPath(o, spec.field);
      populated = nonempty(value) && inCohort;
    } else if (spec.tab === "relationships") {
      existingSource = inCohort ? "ownership payload edges/fixtures" : "stub";
      const edges =
        getPath(ownership?.json, "deep_research.relationships") ||
        report.relationship_edges ||
        report.relationships?.edges ||
        ownership?.json?.deep_research?.relationship_edges;
      if (spec.field === "relationship_edges" || spec.field === "structure_chart") value = edges || (inCohort ? report.ownership_and_control : null);
      else value = null;
      populated = inCohort && nonempty(value);
    }

    let verification = "NOT_ASSESSED";
    if (!populated) verification = "EMPTY_OR_STUB";
    else if (existingSource.includes("fixture") || existingSource.includes("showcase") || existingSource.includes("golden")) {
      verification = "FIXTURE_POPULATED_NOT_AIRTABLE_CANONICAL";
    } else if (existingSource.includes("HPC") || existingSource.includes("brand-presence")) {
      verification = "HPC_OR_API_PRESENT_VERIFY_FRESHNESS";
    }

    let researchNeeded = "NO";
    let action = "REUSE_AND_VERIFY";
    if (!populated) {
      researchNeeded = "YES";
      action = "RESEARCH_THEN_PERSIST_TO_CANONICAL";
    } else if (verification.startsWith("FIXTURE")) {
      researchNeeded = "VERIFY_THEN_MIGRATE";
      action = "MIGRATE_VERIFIED_FACTS_TO_HI_GRAPH_OR_HPC";
    }

    rows.push({
      hotelId,
      field: spec.field,
      tab: spec.tab,
      section: spec.section,
      entity: spec.entity,
      currentContent: populated ? "POPULATED" : "EMPTY",
      contentPreview: populated
        ? typeof value === "object"
          ? `object/array keys=${Array.isArray(value) ? value.length : Object.keys(value || {}).length}`
          : String(value).slice(0, 120)
        : "",
      existingSource,
      airtableCanonical: spec.airtable,
      writeDestination: spec.writeDest,
      researchNeeded,
      verificationStatus: verification,
      recommendedAction: action,
    });
  }
  return rows;
}

function tabMetrics(rows, tab) {
  const subset = rows.filter((r) => r.tab === tab);
  const applicable = subset.length;
  const populated = subset.filter((r) => r.currentContent === "POPULATED").length;
  const fixtureOnly = subset.filter((r) => r.verificationStatus.startsWith("FIXTURE")).length;
  const empty = subset.filter((r) => r.currentContent === "EMPTY").length;
  return {
    tab,
    applicable,
    populated,
    empty,
    fixtureOnly,
    substantivePct: applicable ? Math.round((populated / applicable) * 1000) / 10 : 0,
    airtableCanonicalPct: applicable
      ? Math.round(((populated - fixtureOnly) / applicable) * 1000) / 10
      : 0,
  };
}

async function main() {
  const hotelReports = [];
  for (const h of HOTELS) {
    const brandPresence = await fetchJson(`${BASE}/api/brand-presence/hotel/${h.id}`);
    const ownership = await fetchJson(`${BASE}/api/golden-demo/ownership/hotel/${h.id}`);
    const contacts = await fetchJson(`${BASE}/api/contact-intelligence/hotels/${h.id}?audience=internal`);
    let org = { ok: false, status: 0, json: null, bytes: 0 };
    const slug =
      ownership.json?.organization?.slug ||
      ownership.json?.organization?.id ||
      ownership.json?.ownership_group?.slug ||
      ownership.json?.report?.organization_slug;
    if (slug) {
      org = await fetchJson(`${BASE}/api/golden-demo/ownership/groups/${encodeURIComponent(slug)}`);
    }
    const graph = await probeHiGraph(h.id);
    const rows = assessHotelFields(h.id, { brandPresence, ownership, contacts, org });
    const tabs = ["hotel", "ownership", "contacts", "organization", "relationships"].map((t) =>
      tabMetrics(rows, t),
    );
    hotelReports.push({
      ...h,
      brandPresenceName: brandPresence.json?.hotel?.name || null,
      brandPresenceBrand: brandPresence.json?.hotel?.brand || null,
      brandPresenceRooms: brandPresence.json?.hotel?.rooms ?? null,
      ownershipInCohort: ownership.json?.in_cohort === true,
      ownershipDataStatus: ownership.json?.data_status || null,
      contactBytes: contacts.bytes,
      hiGraph: graph,
      tabMetrics: tabs,
      overall: {
        applicable: rows.length,
        populated: rows.filter((r) => r.currentContent === "POPULATED").length,
        empty: rows.filter((r) => r.currentContent === "EMPTY").length,
        fixtureOnly: rows.filter((r) => r.verificationStatus.startsWith("FIXTURE")).length,
      },
      fields: rows,
    });
  }

  const report = {
    contract: "HE_TABS_1_5_FIELD_INVENTORY_V1",
    auditedAt: new Date().toISOString(),
    baseUrl: BASE,
    tabModelNote:
      "Hotel Explorer overlay has 11 tabs; primary research scope is tabs 1–5: hotel, ownership, contacts, organization, relationships. Tabs 7–9 (area/demand/access) reuse HPC/Radar — out of research scope.",
    identityResolution: {
      hotelExplorerParam: "recUNycnMwOVFX0hc",
      resolvedName: hotelReports[0]?.brandPresenceName,
      isBethesdaMarriott: false,
      bethesdaCanonicalId: "recLuxvwwxID7U2B8",
    },
    sourceOfTruthHierarchy: [
      "1. Verified HI Airtable graph (orgs/relationships/contacts/channels/evidence) on appCCUsuGsE1ifoLk",
      "2. Hotel Property Census / Hotel Census property attributes",
      "3. ADP verified attributes (amenities/meeting) — display overlay, do not mutate publication gates",
      "4. Brand Explorer verified brand facts (brand entity only)",
      "5. Golden-demo / showcase fixtures — regression only until migrated",
      "6. New research — only for genuine gaps",
    ],
    hotels: hotelReports,
  };

  fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
  fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2));

  const md = [];
  md.push("# HE Tabs 1–5 Field Inventory V1");
  md.push("");
  md.push(`Audited: ${report.auditedAt}`);
  md.push("");
  md.push("## Identity resolution");
  md.push("");
  md.push(`- URL \`hotelExplorer=recUNycnMwOVFX0hc\` → **${report.identityResolution.resolvedName}** (NOT Bethesda Marriott).`);
  md.push(`- Bethesda Marriott canonical HPC id: \`${report.identityResolution.bethesdaCanonicalId}\`.`);
  md.push("");
  md.push("## Tab model");
  md.push("");
  md.push(report.tabModelNote);
  md.push("");
  md.push("## Source-of-truth hierarchy");
  md.push("");
  for (const s of report.sourceOfTruthHierarchy) md.push(`- ${s}`);
  md.push("");

  for (const h of hotelReports) {
    md.push(`## ${h.label}`);
    md.push("");
    md.push(`- HPC id: \`${h.id}\``);
    md.push(`- Brand-presence: **${h.brandPresenceName}** · brand=${h.brandPresenceBrand} · rooms=${h.brandPresenceRooms}`);
    md.push(`- Ownership in_cohort: **${h.ownershipInCohort}** · data_status=${h.ownershipDataStatus}`);
    md.push(`- Contact payload bytes: ${h.contactBytes}`);
    md.push(
      `- HI graph relationships for hotel: **${h.hiGraph.relationships}** (${h.hiGraph.reason}${h.hiGraph.available === false ? ` — ${h.hiGraph.reason}` : ""})`,
    );
    md.push("");
    md.push("| Tab | Applicable | Populated | Empty | Fixture-only | Substantive % | Non-fixture populated % |");
    md.push("|---|---:|---:|---:|---:|---:|---:|");
    for (const t of h.tabMetrics) {
      md.push(
        `| ${t.tab} | ${t.applicable} | ${t.populated} | ${t.empty} | ${t.fixtureOnly} | ${t.substantivePct}% | ${t.airtableCanonicalPct}% |`,
      );
    }
    md.push("");
    md.push("| Field | Tab | Current | Existing source | Airtable canonical | Research needed | Verification | Recommended action |");
    md.push("|---|---|---|---|---|---|---|---|");
    for (const r of h.fields) {
      md.push(
        `| ${r.field} | ${r.tab} | ${r.currentContent}${r.contentPreview ? ` (${r.contentPreview.replace(/\|/g, "/")})` : ""} | ${r.existingSource} | ${r.airtableCanonical} | ${r.researchNeeded} | ${r.verificationStatus} | ${r.recommendedAction} |`,
      );
    }
    md.push("");
  }

  md.push("## Baseline verdict");
  md.push("");
  md.push("- KGPV tabs 2–5 are **fixture-rich** but **not Airtable-canonical** (HI graph relationship count expected 0 until migration).");
  md.push("- Bethesda tabs 2–5 are **out of golden cohort** (ownership stub); tab 1 has HPC/API gaps (rooms/website/phone often empty).");
  md.push("- Do not treat UI population as durable persistence.");
  md.push("- Next: reuse/migrate verified KGPV fixture facts into HI graph; research Bethesda genuine gaps within authorized budget.");
  md.push("");

  fs.writeFileSync(OUT_MD, md.join("\n"));
  console.log(JSON.stringify({ ok: true, outJson: OUT_JSON, outMd: OUT_MD, hotels: hotelReports.map((h) => ({ id: h.id, name: h.brandPresenceName, overall: h.overall, hiGraph: h.hiGraph })) }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
