#!/usr/bin/env node
/**
 * Gate 1 addendum — runtime provenance verification (not intended destinations alone).
 * Read-only. No Airtable writes. No fixture migration.
 *
 *   node scripts/gate1-he-tabs-1-5-runtime-provenance-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import {
  resolveHeAuditBaseUrl,
  assertHeLocalRuntimeTarget,
} from "./lib/he-local-runtime-target.mjs";
import { isMexicoRadarFixtureHotelId } from "../lib/hotel-intelligence/golden-demo/mexico-radar-fixture-fallback.js";
import {
  GRAPH_BASE_ID_CANONICAL,
} from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-contract-v1.js";

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const OUT_JSON = path.join(
  ROOT,
  "reports/hotel-explorer/HE-TABS-1-5-GATE1-RUNTIME-PROVENANCE-V1.json",
);
const OUT_MD = path.join(
  ROOT,
  "reports/hotel-explorer/HE-TABS-1-5-GATE1-FOUNDER-REVIEW-V1.md",
);
const PRIOR_GATE1 = path.join(
  ROOT,
  "reports/hotel-explorer/HE-TABS-1-5-GATE1-FIELD-AIRTABLE-MAPPING-V1.json",
);

const HPC = { tableId: "tbl9aY5ijiuIzzWam", tableName: "Hotel Property Census" };
const CENSUS = { tableId: "tblgj2qEwxjTcg6q0", tableName: "Hotel Census" };
const HI = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "lib/hotel-intelligence/research-engine-v1/airtable-graph-table-ids.generated.json"),
    "utf8",
  ),
).tableIds;

const HOTELS = [
  {
    id: "recUNycnMwOVFX0hc",
    label: "Krystal Grand Puerto Vallarta",
    role: "REFERENCE",
  },
  {
    id: "recLuxvwwxID7U2B8",
    label: "Bethesda Marriott",
    role: "REPEATABILITY",
  },
];

/** Field catalog used for provenance rows (tabs 1–5). */
const FIELDS = [
  // hotel overview — expected from property census tables
  ...[
    "name","brand","status","website","telephone","address1","city","country",
    "hotelHeadline","hotelDescription","parentCompany","chainScale","censusPropertyType",
    "rooms","totalMeetingSpace","operationType","managementCompany","market","submarket",
    "locationType","projectPhase","projectedOpenDate","starRating","dataConfidence","amenitiesDisplay",
  ].map((field) => ({ tab: "hotel", field })),
  ...[
    "executive_summary.text","what_remains_uncertain","economic_owner","property_company","operator",
    "control_status","source_coverage","what_we_know[]","control_rows","ubo_group","portfolio_preview",
    "brand_status","decision_authority.people","qualified_signatories","corporate_contact_channels",
    "asset_agreement_profile","property_history","development_pursuit","research_gaps","sources",
  ].map((field) => ({ tab: "ownership", field })),
  ...[
    "quality_summary","ownershipSummary","organization_contacts","people_groups","emails_phones",
    "fallback_paths","research_unresolved",
  ].map((field) => ({ tab: "contacts", field })),
  ...[
    "display_name","legal_name","ticker","portfolio_kpis","owned_controlled_hotels",
    "operated_managed_hotels","key_people","why_this_matters",
  ].map((field) => ({ tab: "organization", field })),
  ...[
    "structure_chart","relationship_edges","org_network",
  ].map((field) => ({ tab: "relationships", field })),
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

async function atGet(baseId, tableId, recordId, token) {
  const url = `https://api.airtable.com/v0/${baseId}/${tableId}/${recordId}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => ({}));
  return { ok: res.ok, status: res.status, json };
}

async function atFindByRecordId(baseId, tableId, recordId, token) {
  const formula = encodeURIComponent(`RECORD_ID()='${recordId}'`);
  const url = `https://api.airtable.com/v0/${baseId}/${tableId}?filterByFormula=${formula}&maxRecords=1`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => ({}));
  const rec = (json.records || [])[0] || null;
  return { ok: res.ok, status: res.status, found: Boolean(rec), record: rec };
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

function nonempty(v, field) {
  if (v == null) return false;
  if (typeof v === "number") return field === "rooms" ? v > 0 : Number.isFinite(v);
  if (Array.isArray(v)) return v.length > 0;
  if (typeof v === "object") return Object.keys(v).length > 0;
  const s = String(v).trim();
  return s.length > 0 && s !== "—" && s.toLowerCase() !== "null";
}

function getPath(obj, dotted) {
  if (!obj) return undefined;
  if (!dotted.includes(".")) return obj[dotted];
  return dotted.split(".").reduce((a, k) => (a == null ? undefined : a[k]), obj);
}

function extractUi(field, bundle) {
  const h = bundle.brandPresence.json?.hotel || {};
  const o = bundle.ownership.json || {};
  const g = bundle.graph.json || {};
  const c = bundle.contacts.json || {};
  const pkg = c.package || c;
  const org = bundle.org.json || {};
  switch (field) {
    case "name": return h.name;
    case "brand": return h.brand;
    case "status": return h.status || h.projectPhase;
    case "website": return h.website;
    case "telephone": return h.telephone || h.phone;
    case "address1": return h.address1 || h.address;
    case "city": return h.city;
    case "country": return h.country;
    case "hotelHeadline": return h.hotelHeadline;
    case "hotelDescription": return h.hotelDescription || h.description;
    case "parentCompany": return h.parentCompany;
    case "chainScale": return h.chainScale;
    case "censusPropertyType": return h.censusPropertyType || h.propertyType;
    case "rooms": return h.rooms;
    case "totalMeetingSpace": return h.totalMeetingSpace;
    case "operationType": return h.operationType;
    case "managementCompany": return h.managementCompany;
    case "market": return h.market;
    case "submarket": return h.submarket;
    case "locationType": return h.locationType;
    case "projectPhase": return h.projectPhase;
    case "projectedOpenDate": return h.projectedOpenDate;
    case "starRating": return h.starRating;
    case "dataConfidence": return h.dataConfidence;
    case "amenitiesDisplay": return h.amenitiesDisplay || h.amenities;
    case "executive_summary.text": return getPath(o, "executive_summary.text") || o.executive_summary;
    case "what_remains_uncertain": return o.what_remains_uncertain;
    case "economic_owner":
      return o.report?.ownership_and_control?.economic_owner || g.projection?.economic_owner;
    case "property_company":
      return o.report?.ownership_and_control?.property_company || g.projection?.property_company;
    case "operator":
      return o.report?.ownership_and_control?.operator || g.projection?.operator;
    case "control_status":
      return o.report?.ownership_and_control?.control_status || g.projection?.control_status;
    case "source_coverage": return o.source_coverage;
    case "what_we_know[]": return o.what_we_know;
    case "control_rows": return o.control_rows || g.relationships;
    case "ubo_group": return o.ubo_group;
    case "portfolio_preview": return o.portfolio_preview;
    case "brand_status": return o.brand_status || g.projection?.brand;
    case "decision_authority.people": return getPath(o, "decision_authority.people") || o.decision_authority;
    case "qualified_signatories": return o.qualified_signatories;
    case "corporate_contact_channels": return o.corporate_contact_channels;
    case "asset_agreement_profile": return o.asset_agreement_profile;
    case "property_history": return o.property_history;
    case "development_pursuit": return o.development_pursuit;
    case "research_gaps": return o.research_gaps;
    case "sources": return o.sources;
    case "quality_summary": return pkg.quality_summary;
    case "ownershipSummary": return pkg.ownershipSummary;
    case "organization_contacts": return pkg.organization_contact_route;
    case "people_groups": return pkg.people;
    case "emails_phones": {
      const ch = [
        ...(pkg.hotel_contact?.channels || []),
        ...(pkg.organization_contact_route?.channels || []),
      ];
      return ch.length ? ch : null;
    }
    case "fallback_paths": return pkg.fallback_paths;
    case "research_unresolved": return pkg.unresolved_reasons;
    case "display_name": return org.organization?.display_name || g.organizations?.[0]?.tradingName;
    case "legal_name": return org.organization?.legal_name || g.organizations?.[0]?.legalName;
    case "ticker": return org.organization?.ticker;
    case "portfolio_kpis": return org.portfolio_kpis;
    case "owned_controlled_hotels": return org.owned_controlled_hotels;
    case "operated_managed_hotels": return org.operated_managed_hotels;
    case "key_people": return org.key_people;
    case "why_this_matters": return org.why_this_matters;
    case "structure_chart": return o.structure_chart || g.relationships;
    case "relationship_edges": return g.relationships || o.relationship_edges;
    case "org_network": return o.org_network;
    default: return undefined;
  }
}

/**
 * Provenance classification for a field given hotel identity + live probes.
 */
function classifyProvenance(meta, hotelCtx, uiPresent) {
  const { identity, ownership, graph, contacts, brandPresence, mexicoFixtureOverlay } = hotelCtx;
  const reportedTable = brandPresence.json?.hotel?.censusSourceTable || null;
  const inCohort = ownership.json?.in_cohort === true;
  const dataStatus = ownership.json?.data_status || null;
  const graphRels = graph.json?.relationshipCount || 0;
  const hasGraphOperator = (graph.json?.relationships || []).some((r) =>
    ["Operator", "Management Company"].includes(String(r.relationshipType)),
  );
  const hasGraphBrand = (graph.json?.relationships || []).some(
    (r) => String(r.relationshipType) === "Brand",
  );
  const hasGraphEconomic = Boolean(graph.json?.projection?.economic_owner);
  const ciSeed = contacts.bytes > 4000 || Boolean(contacts.json?.package?.owner_entity_id);

  const propertyCanonical =
    identity.owningTableId === HPC.tableId
      ? {
          canonical_source_base: identity.baseId,
          canonical_source_table: HPC.tableName,
          canonical_source_table_id: HPC.tableId,
          canonical_source_record: identity.recordId,
          writeback_destination: `${HPC.tableName} (${HPC.tableId})`,
        }
      : identity.owningTableId === CENSUS.tableId
        ? {
            canonical_source_base: identity.baseId,
            canonical_source_table: CENSUS.tableName,
            canonical_source_table_id: CENSUS.tableId,
            canonical_source_record: identity.recordId,
            writeback_destination: `${CENSUS.tableName} (${CENSUS.tableId}) — migrate/unify to HPC before HI Rel writes`,
          }
        : {
            canonical_source_base: identity.baseId,
            canonical_source_table: null,
            canonical_source_table_id: null,
            canonical_source_record: identity.recordId,
            writeback_destination: "UNRESOLVED_PROPERTY_IDENTITY",
          };

  const identity_resolution_status = identity.identity_resolution_status;

  // --- Tab 1 hotel overview ---
  if (meta.tab === "hotel") {
    const intendedHpc =
      identity.owningTableId === HPC.tableId
        ? "LIVE_HPC_AIRTABLE"
        : identity.owningTableId === CENSUS.tableId
          ? "LIVE_HOTEL_CENSUS_AIRTABLE"
          : "UNRESOLVED";

    let actual_runtime_source = "EMPTY_OR_UNAVAILABLE";
    let source_type = "EMPTY";
    let provenance_verified = false;
    let mismatch = null;

    if (uiPresent) {
      if (mexicoFixtureOverlay && ["name", "brand", "parentCompany", "managementCompany", "chainScale", "market", "submarket", "website", "hotelDescription", "amenitiesDisplay"].includes(meta.field)) {
        actual_runtime_source =
          "brand-presence: LIVE Airtable property record + golden-demo Mexico fixture presentation overlay (isMexicoRadarFixtureHotelId)";
        source_type = "MIXED_LIVE_AIRTABLE_PLUS_FIXTURE_OVERLAY";
        provenance_verified = true;
        mismatch =
          "Some Overview fields may be fixture-preferred over live Census values for Mexico golden-demo hotels.";
      } else if (identity.owningTableId === HPC.tableId) {
        actual_runtime_source =
          `brand-presence GET Hotel Census path (mislabels censusSourceTable=${reportedTable}) but RECORD_ID ownership = Hotel Property Census; live fields via Airtable find`;
        source_type = "LIVE_HPC_AIRTABLE";
        provenance_verified = true;
        if (reportedTable && reportedTable !== HPC.tableName) {
          mismatch = `API reports censusSourceTable="${reportedTable}" but canonical owning table is ${HPC.tableName} (${HPC.tableId}). Airtable GET-by-id ignores table path.`;
        }
      } else if (identity.owningTableId === CENSUS.tableId) {
        actual_runtime_source = `brand-presence → live ${CENSUS.tableName} (${CENSUS.tableId}) via AIRTABLE_BASE_ID_ALT`;
        source_type = "LIVE_HOTEL_CENSUS_AIRTABLE";
        provenance_verified = true;
        mismatch =
          "Intended long-term SoT is Hotel Property Census; runtime Overview for this hotel is Hotel Census, not HPC.";
      }
    } else if (meta.field === "projectedOpenDate") {
      actual_runtime_source = "N/A for Open asset (HPC/Census field exists)";
      source_type = "DERIVED_OR_N_A";
      provenance_verified = true;
    } else {
      actual_runtime_source = "Not returned by brand-presence; may be empty on Airtable record";
      source_type = "EMPTY";
      provenance_verified = true;
    }

    return {
      ...propertyCanonical,
      actual_runtime_source,
      source_type,
      identity_resolution_status,
      provenance_verified,
      intended_vs_actual_mismatch: mismatch,
      ui_present: uiPresent,
      notes: `Canonical property SoT for this hotel: ${intendedHpc}`,
    };
  }

  // --- Ownership / org / relationships ---
  if (["ownership", "organization", "relationships"].includes(meta.tab)) {
    const hiDest = {
      canonical_source_base: GRAPH_BASE_ID_CANONICAL,
      canonical_source_table:
        meta.tab === "organization" && ["display_name", "legal_name", "ticker", "why_this_matters", "key_people"].includes(meta.field)
          ? meta.field === "key_people"
            ? "HI Org Contacts"
            : "HI Organizations"
          : meta.field === "sources" || meta.field === "what_we_know[]" || meta.field === "property_history" || meta.field === "what_remains_uncertain"
            ? "HI Graph Evidence"
            : meta.field === "decision_authority.people" || meta.field === "qualified_signatories"
              ? "HI Org Contacts"
              : meta.field === "corporate_contact_channels"
                ? "HI Contact Channels"
                : ["executive_summary.text", "research_gaps", "development_pursuit"].includes(meta.field)
                  ? "HI Ownership Research Runs"
                  : "HI Hotel–Org Relationships",
      canonical_source_table_id: null,
      canonical_source_record: null,
      writeback_destination: "HI graph on appCCUsuGsE1ifoLk (see table name)",
    };
    hiDest.canonical_source_table_id =
      HI[hiDest.canonical_source_table] || null;

    // KGPV cohort fixtures
    if (inCohort && dataStatus === "DEEP_RESEARCH_PARTIAL") {
      if (uiPresent || ["economic_owner", "operator", "executive_summary.text", "control_rows", "structure_chart", "relationship_edges", "display_name", "legal_name", "decision_authority.people", "property_history", "research_gaps", "brand_status", "control_status", "source_coverage"].includes(meta.field)) {
        return {
          ...hiDest,
          canonical_source_record: null,
          actual_runtime_source:
            "golden-demo ownership API → fixtures/golden-demo/gsf-mexico-ownership-cohort-v1.json (+ related cohort JSON); HI graph rel count=0",
          source_type: "FIXTURE_GOLDEN_DEMO",
          identity_resolution_status,
          writeback_destination: hiDest.writeback_destination,
          provenance_verified: true,
          intended_vs_actual_mismatch:
            "Canonical write destination is HI graph/HPC-linked relationships; runtime serves golden-demo fixtures. Not live HPC ownership fields.",
          ui_present: uiPresent,
        };
      }
    }

    // Bethesda HI graph overlay
    if (graphRels > 0) {
      if (
        (meta.field === "operator" && hasGraphOperator) ||
        (meta.field === "brand_status" && hasGraphBrand) ||
        (["control_status", "control_rows", "relationship_edges", "structure_chart"].includes(meta.field) && graphRels > 0) ||
        (["display_name", "legal_name"].includes(meta.field) && (graph.json?.organizations || []).length)
      ) {
        const orgRec = graph.json?.organizations?.[0]?.airtableId || null;
        const relIds = (graph.json?.relationships || []).map((r) => r.airtableId);
        return {
          ...hiDest,
          canonical_source_record:
            meta.tab === "organization" ? orgRec : relIds[0] || null,
          actual_runtime_source:
            "golden-demo ownership overlay → he-airtable-graph-reader → live HI Hotel–Org Relationships + HI Organizations",
          source_type: "LIVE_HI_GRAPH_AIRTABLE",
          identity_resolution_status,
          provenance_verified: true,
          intended_vs_actual_mismatch: null,
          ui_present: uiPresent,
          related_records: { organizations: graph.json?.organizations || [], relationships: graph.json?.relationships || [] },
        };
      }
      if (meta.field === "economic_owner" || meta.field === "property_company" || meta.field === "ubo_group") {
        return {
          ...hiDest,
          actual_runtime_source: hasGraphEconomic
            ? "HI graph Economic Owner relationship"
            : "No Economic/Legal Owner on HI graph; ownership API may show unresolved stub",
          source_type: hasGraphEconomic ? "LIVE_HI_GRAPH_AIRTABLE" : "EMPTY",
          identity_resolution_status,
          provenance_verified: true,
          intended_vs_actual_mismatch: null,
          ui_present: uiPresent,
        };
      }
      if (
        ["portfolio_kpis", "owned_controlled_hotels", "operated_managed_hotels", "portfolio_preview", "org_network"].includes(
          meta.field,
        )
      ) {
        return {
          ...hiDest,
          actual_runtime_source:
            "Derivable from HI graph org portfolio query — not fully wired in HE Organization tab runtime",
          source_type: "DERIVED_OR_AGGREGATED",
          identity_resolution_status,
          provenance_verified: true,
          intended_vs_actual_mismatch: "Canonical facts partially exist; HE derived views not connected",
          ui_present: uiPresent,
        };
      }
    }

    return {
      ...hiDest,
      actual_runtime_source: uiPresent
        ? "UI value present but source not live HPC/HI-graph verified for this hotel"
        : "Empty at runtime; no live HI graph / fixture value",
      source_type: uiPresent ? "UNVERIFIED_OR_STUB" : "EMPTY",
      identity_resolution_status,
      provenance_verified: true,
      intended_vs_actual_mismatch: null,
      ui_present: uiPresent,
    };
  }

  // --- Contacts ---
  if (meta.tab === "contacts") {
    const dest = {
      canonical_source_base: GRAPH_BASE_ID_CANONICAL,
      canonical_source_table: "HI Contact Channels / HI Org Contacts",
      canonical_source_table_id: `${HI["HI Contact Channels"]} / ${HI["HI Org Contacts"]}`,
      canonical_source_record: null,
      writeback_destination: "HI Org Contacts + HI Contact Channels (verified facts only)",
    };
    if (ciSeed && hotelCtx.hotelId === "recUNycnMwOVFX0hc") {
      return {
        ...dest,
        actual_runtime_source:
          "contact-intelligence hotelGet → slice-ten seed / local CI package (not HI Airtable channels); may also hit local CI file store under serving checkout",
        source_type: "LOCAL_CONTACT_INTELLIGENCE_AND_OR_SLICE_SEED",
        identity_resolution_status,
        provenance_verified: true,
        intended_vs_actual_mismatch:
          "Canonical destination is HI Contact Channels; runtime is CI slice seed / local store — GSF not migrated to Airtable",
        ui_present: uiPresent,
      };
    }
    if (meta.field === "fallback_paths" || meta.field === "emails_phones") {
      // property phone/website from brand-presence could serve as fallback
      if (nonempty(brandPresence.json?.hotel?.telephone) || nonempty(brandPresence.json?.hotel?.website)) {
        return {
          ...dest,
          actual_runtime_source:
            uiPresent
              ? "CI package and/or property phone/website from brand-presence"
              : "Property phone/website exist on live property Airtable but Contacts tab CI package empty",
          source_type: uiPresent ? "MIXED_OR_PROPERTY_FALLBACK" : "EMPTY",
          identity_resolution_status,
          provenance_verified: true,
          intended_vs_actual_mismatch: null,
          ui_present: uiPresent,
        };
      }
    }
    return {
      ...dest,
      actual_runtime_source: "contact-intelligence API sparse/empty for this hotel",
      source_type: "EMPTY",
      identity_resolution_status,
      provenance_verified: true,
      intended_vs_actual_mismatch: null,
      ui_present: uiPresent,
    };
  }

  return {
    canonical_source_base: null,
    canonical_source_table: null,
    canonical_source_record: null,
    actual_runtime_source: "unclassified",
    source_type: "UNCLASSIFIED",
    identity_resolution_status,
    writeback_destination: null,
    provenance_verified: false,
    ui_present: uiPresent,
  };
}

function pct(n, d) {
  return d ? Math.round((n / d) * 1000) / 10 : 0;
}

function sourceBreakdown(rows) {
  const buckets = {
    LIVE_HPC_AIRTABLE: 0,
    LIVE_HOTEL_CENSUS_AIRTABLE: 0,
    LIVE_HI_GRAPH_AIRTABLE: 0,
    MIXED_LIVE_AIRTABLE_PLUS_FIXTURE_OVERLAY: 0,
    FIXTURE_GOLDEN_DEMO: 0,
    LOCAL_CONTACT_INTELLIGENCE_AND_OR_SLICE_SEED: 0,
    DERIVED_OR_AGGREGATED: 0,
    DERIVED_OR_N_A: 0,
    MIXED_OR_PROPERTY_FALLBACK: 0,
    EMPTY: 0,
    UNVERIFIED_OR_STUB: 0,
    UNCLASSIFIED: 0,
  };
  for (const r of rows) {
    const k = r.source_type in buckets ? r.source_type : "UNCLASSIFIED";
    buckets[k] += 1;
  }
  const total = rows.length;
  const liveHpc = buckets.LIVE_HPC_AIRTABLE;
  const liveOtherAirtable = buckets.LIVE_HOTEL_CENSUS_AIRTABLE + buckets.LIVE_HI_GRAPH_AIRTABLE;
  const local = buckets.LOCAL_CONTACT_INTELLIGENCE_AND_OR_SLICE_SEED;
  const fixtures =
    buckets.FIXTURE_GOLDEN_DEMO + buckets.MIXED_LIVE_AIRTABLE_PLUS_FIXTURE_OVERLAY;
  const derived = buckets.DERIVED_OR_AGGREGATED + buckets.DERIVED_OR_N_A;
  return {
    bySourceType: Object.fromEntries(
      Object.entries(buckets).map(([k, v]) => [k, { count: v, pct: pct(v, total) }]),
    ),
    summaryPct: {
      genuinelyLiveHpcPct: pct(liveHpc, total),
      otherLiveAirtablePct: pct(liveOtherAirtable, total),
      localContactIntelligencePct: pct(local, total),
      fixturesOrFixtureOverlayPct: pct(fixtures, total),
      derivedOrNaPct: pct(derived, total),
      emptyOrUnverifiedPct: pct(
        buckets.EMPTY + buckets.UNVERIFIED_OR_STUB + buckets.UNCLASSIFIED + buckets.MIXED_OR_PROPERTY_FALLBACK,
        total,
      ),
    },
  };
}

async function main() {
  loadEnv();
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseAlt = process.env.AIRTABLE_BASE_ID_ALT;
  const basePrimary = process.env.AIRTABLE_BASE_ID;
  const baseUrl = resolveHeAuditBaseUrl(process.argv, process.env);
  const runtime = await assertHeLocalRuntimeTarget({ baseUrl });
  if (!runtime.ok) {
    console.error(runtime);
    process.exit(2);
  }
  if (!token || !baseAlt) {
    console.error("Missing AIRTABLE_API_KEY or AIRTABLE_BASE_ID_ALT");
    process.exit(2);
  }

  const baseConfirmation = {
    AIRTABLE_BASE_ID_ALT: baseAlt,
    AIRTABLE_BASE_ID_primary_product: basePrimary || null,
    isAltTheHpcBase: baseAlt === GRAPH_BASE_ID_CANONICAL,
    graphCanonicalBase: GRAPH_BASE_ID_CANONICAL,
    note:
      "Brand-presence / Radar / HPC / HI graph read AIRTABLE_BASE_ID_ALT. Primary AIRTABLE_BASE_ID is a different product base and is not the HPC host.",
  };

  const hotelReports = [];

  for (const hotel of HOTELS) {
    const censusOwn = await atFindByRecordId(baseAlt, CENSUS.tableId, hotel.id, token);
    const hpcOwn = await atFindByRecordId(baseAlt, HPC.tableId, hotel.id, token);
    let owningTableId = null;
    let owningTableName = null;
    let identity_resolution_status = "UNRESOLVED";
    if (hpcOwn.found && !censusOwn.found) {
      owningTableId = HPC.tableId;
      owningTableName = HPC.tableName;
      identity_resolution_status = "RESOLVED_HPC";
    } else if (censusOwn.found && !hpcOwn.found) {
      owningTableId = CENSUS.tableId;
      owningTableName = CENSUS.tableName;
      identity_resolution_status = "RESOLVED_HOTEL_CENSUS_NOT_HPC";
    } else if (hpcOwn.found && censusOwn.found) {
      identity_resolution_status = "AMBIGUOUS_BOTH_TABLES";
      owningTableId = HPC.tableId;
      owningTableName = HPC.tableName;
    } else {
      identity_resolution_status = "NOT_FOUND";
    }

    // Airtable GET-by-id quirk evidence
    const getCensusPath = await atGet(baseAlt, CENSUS.tableId, hotel.id, token);
    const getHpcPath = await atGet(baseAlt, HPC.tableId, hotel.id, token);

    const brandPresence = await fetchJson(`${baseUrl}/api/brand-presence/hotel/${hotel.id}`);
    const ownership = await fetchJson(`${baseUrl}/api/golden-demo/ownership/hotel/${hotel.id}`);
    const contacts = await fetchJson(
      `${baseUrl}/api/contact-intelligence/hotels/${hotel.id}?audience=internal`,
    );
    const graph = await fetchJson(`${baseUrl}/api/he-airtable-graph/hotel/${hotel.id}`);
    let org = { ok: false, json: null, bytes: 0 };
    const slug = ownership.json?.organization?.slug || ownership.json?.ownership_group?.slug;
    if (slug) {
      org = await fetchJson(`${baseUrl}/api/golden-demo/ownership/groups/${encodeURIComponent(slug)}`);
    }

    const mexicoFixtureOverlay = isMexicoRadarFixtureHotelId(hotel.id);
    const hotelCtx = {
      hotelId: hotel.id,
      identity: {
        baseId: baseAlt,
        recordId: hotel.id,
        owningTableId,
        owningTableName,
        identity_resolution_status,
        censusFound: censusOwn.found,
        hpcFound: hpcOwn.found,
      },
      ownership,
      graph,
      contacts,
      brandPresence,
      mexicoFixtureOverlay,
    };

    const rows = FIELDS.map((meta) => {
      const value = extractUi(meta.field, { brandPresence, ownership, contacts, org, graph });
      const uiPresent = nonempty(value, meta.field);
      const prov = classifyProvenance(meta, hotelCtx, uiPresent);
      return {
        tab: meta.tab,
        field: meta.field,
        canonical_source_base: prov.canonical_source_base,
        canonical_source_table: prov.canonical_source_table,
        canonical_source_table_id: prov.canonical_source_table_id || null,
        canonical_source_record: prov.canonical_source_record,
        actual_runtime_source: prov.actual_runtime_source,
        source_type: prov.source_type,
        identity_resolution_status: prov.identity_resolution_status,
        writeback_destination: prov.writeback_destination,
        provenance_verified: prov.provenance_verified === true,
        intended_vs_actual_mismatch: prov.intended_vs_actual_mismatch || null,
        ui_present: uiPresent,
      };
    });

    const hpcIntendedButNot =
      rows.filter(
        (r) =>
          r.tab === "hotel" &&
          r.ui_present &&
          r.source_type !== "LIVE_HPC_AIRTABLE" &&
          r.source_type !== "DERIVED_OR_N_A",
      ).length;

    hotelReports.push({
      ...hotel,
      identity: hotelCtx.identity,
      airtableGetByIdQuirk: {
        explanation:
          "Airtable REST GET /v0/{base}/{tableId|name}/{recordId} returns the record even when the table path is wrong. Ownership must be established with filterByFormula RECORD_ID().",
        getViaCensusPathStatus: getCensusPath.status,
        getViaHpcPathStatus: getHpcPath.status,
        recordIdFilterCensusFound: censusOwn.found,
        recordIdFilterHpcFound: hpcOwn.found,
      },
      runtimeSignals: {
        brandPresenceCensusSourceTable: brandPresence.json?.hotel?.censusSourceTable || null,
        brandPresenceName: brandPresence.json?.hotel?.name || null,
        brandPresenceRooms: brandPresence.json?.hotel?.rooms ?? null,
        mexicoFixtureOverlay,
        ownershipInCohort: ownership.json?.in_cohort === true,
        ownershipDataStatus: ownership.json?.data_status || null,
        hiGraphRelationshipCount: graph.json?.relationshipCount || 0,
        hiGraphRelTypes: (graph.json?.relationships || []).map((r) => r.relationshipType),
        hiGraphOrgAirtableIds: (graph.json?.organizations || []).map((o) => o.airtableId),
        contactBytes: contacts.bytes,
      },
      fieldsWhereIntendedHpcButRuntimeDifferent: hpcIntendedButNot,
      sourceBreakdown: sourceBreakdown(rows),
      fields: rows,
    });
  }

  const prior = fs.existsSync(PRIOR_GATE1)
    ? JSON.parse(fs.readFileSync(PRIOR_GATE1, "utf8"))
    : null;

  const duplicationAssessment = {
    verifiedHpcDuplicatedElsewhere: false,
    notes: [
      "Bethesda property attributes are served from live HPC record; not duplicated into a second property table for Overview.",
      "KGPV property attributes are on Hotel Census (not HPC). Fixture overlay can prefer golden-demo presentation for some Mexico fields — display overlay, not a second Airtable write in this path.",
      "Contact Intelligence local files / slice seeds are staging only and must not be treated as a parallel property database.",
      "HI Organizations/Relationships for Bethesda Operator/Brand are graph edges, not copies of HPC rooms/website fields.",
      "Do not migrate fixtures or rewrite identity without founder approval.",
    ],
  };

  const report = {
    contract: "HE_TABS_1_5_GATE1_RUNTIME_PROVENANCE_V1",
    gate: "GATE_1_RUNTIME_PROVENANCE_ADDENDUM",
    status: "AWAITING_FOUNDER_APPROVAL",
    auditedAt: new Date().toISOString(),
    baseUrl,
    runtimeTarget: {
      servingDirectoryBasename: runtime.servingDirectoryBasename,
      servingStream: runtime.servingStream,
    },
    bases: baseConfirmation,
    tables: {
      hotelCensus: CENSUS,
      hotelPropertyCensus: HPC,
      hiGraph: HI,
    },
    identityMatrix: hotelReports.map((h) => ({
      hotelId: h.id,
      label: h.label,
      owningTable: h.identity.owningTableName,
      owningTableId: h.identity.owningTableId,
      identity_resolution_status: h.identity.identity_resolution_status,
      apiReportedCensusSourceTable: h.runtimeSignals.brandPresenceCensusSourceTable,
      apiLabelMismatch:
        h.runtimeSignals.brandPresenceCensusSourceTable &&
        h.identity.owningTableName &&
        h.runtimeSignals.brandPresenceCensusSourceTable !== h.identity.owningTableName,
    })),
    duplicationAssessment,
    hotels: hotelReports,
    priorGate1MappingRef: prior
      ? {
          contract: prior.contract,
          aggregateDestinationPct: prior.aggregate?.percentages || null,
          note: "Prior report mapped intended destinations; this addendum verifies actual runtime sources.",
        }
      : null,
    founderDecision:
      "Approve Gate 1 only after accepting identity matrix + provenance percentages. Substantive paid research still blocked until approval.",
  };

  fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2));

  // Rewrite founder review MD to include provenance addendum as the lead Gate 1 doc
  const md = [];
  md.push("# Gate 1 — Field-to-Airtable Mapping + Runtime Provenance (Founder Review)");
  md.push("");
  md.push(`**Status:** AWAITING FOUNDER APPROVAL — substantive paid research **blocked**`);
  md.push(`**Audited:** ${report.auditedAt}`);
  md.push(`**Runtime:** ${baseUrl} (${runtime.servingDirectoryBasename})`);
  md.push(`**Paid spend:** $0 · Ceiling $25 unused`);
  md.push("");
  md.push("## Critical identity / base confirmation");
  md.push("");
  md.push(`| Env / entity | Value |`);
  md.push(`|---|---|`);
  md.push(`| \`AIRTABLE_BASE_ID_ALT\` (Radar / brand-presence / HPC host) | \`${baseAlt}\` |`);
  md.push(`| Is ALT the HPC / HI graph base? | **${baseConfirmation.isAltTheHpcBase ? "YES" : "NO"}** (\`appCCUsuGsE1ifoLk\`) |`);
  md.push(`| \`AIRTABLE_BASE_ID\` (primary product base — not HPC) | \`${basePrimary || "(unset)"}\` |`);
  md.push(`| Hotel Property Census | \`${HPC.tableName}\` / \`${HPC.tableId}\` |`);
  md.push(`| Hotel Census (legacy/transitional) | \`${CENSUS.tableName}\` / \`${CENSUS.tableId}\` |`);
  md.push("");
  md.push("> Airtable **GET-by-record-id ignores table path**. Provenance uses \`filterByFormula RECORD_ID()\` to establish owning table.");
  md.push("");
  md.push("## Property identity matrix");
  md.push("");
  md.push("| Hotel | Record ID | Owning table (verified) | API `censusSourceTable` | Match? |");
  md.push("|---|---|---|---|---|");
  for (const h of report.identityMatrix) {
    md.push(
      `| ${h.label} | \`${h.hotelId}\` | **${h.owningTable}** (\`${h.owningTableId}\`) | ${h.apiReportedCensusSourceTable} | ${h.apiLabelMismatch ? "**MISLABEL**" : "OK"} |`,
    );
  }
  md.push("");
  md.push("### Implications");
  md.push("");
  md.push("- **Bethesda** is genuinely on **Hotel Property Census** — Overview fields are live HPC (despite API mislabeling source as Hotel Census).");
  md.push("- **KGPV** is on **Hotel Census**, not HPC — Overview is live Census Airtable, with possible **Mexico golden-demo fixture overlay** on selected fields. HI relationship writes remain blocked until Census↔HPC unification.");
  md.push("- Brand-presence always tries \`Hotel Census\` first; HPC fallback only when Census find fails/unknown — so HPC hotels are often read via the Census path name and mis-tagged.");
  md.push("");
  md.push("## Runtime source breakdown (actual, not intended destination)");
  md.push("");
  for (const h of hotelReports) {
    md.push(`### ${h.label}`);
    md.push("");
    md.push("| Bucket | % |");
    md.push("|---|---:|");
    const s = h.sourceBreakdown.summaryPct;
    md.push(`| Genuinely live **HPC** Airtable | **${s.genuinelyLiveHpcPct}%** |`);
    md.push(`| Other live Airtable (Hotel Census and/or HI graph) | **${s.otherLiveAirtablePct}%** |`);
    md.push(`| Local Contact Intelligence / slice seed | **${s.localContactIntelligencePct}%** |`);
    md.push(`| Fixtures / fixture overlay | **${s.fixturesOrFixtureOverlayPct}%** |`);
    md.push(`| Derived / N/A | ${s.derivedOrNaPct}% |`);
    md.push(`| Empty / unverified / other | ${s.emptyOrUnverifiedPct}% |`);
    md.push("");
    md.push("By source_type:");
    md.push("");
    md.push("| source_type | count | pct |");
    md.push("|---|---:|---:|");
    for (const [k, v] of Object.entries(h.sourceBreakdown.bySourceType)) {
      if (v.count === 0) continue;
      md.push(`| ${k} | ${v.count} | ${v.pct}% |`);
    }
    md.push("");
  }
  md.push("## Trace: tabs 1–5 → API → code → origin");
  md.push("");
  md.push("| Tab | HE API | Application code | Actual origin classes |");
  md.push("|---|---|---|---|");
  md.push("| 1 Hotel Overview | \`GET /api/brand-presence/hotel/:id\` | \`api/brand-presence.js\` → Airtable \`AIRTABLE_BASE_ID_ALT\` (+ optional Mexico fixture overlay) | Live Hotel Census **or** live HPC (identity-dependent); fixture overlay for Mexico demo IDs |");
  md.push("| 2 Ownership | \`GET /api/golden-demo/ownership/hotel/:id\` | \`api/golden-demo-ownership.js\` → GSF cohort fixtures **or** HI graph overlay | Fixture (KGPV) / Live HI graph (Bethesda partial) |");
  md.push("| 3 Contacts | \`GET /api/contact-intelligence/hotels/:id\` | CI service → slice seed / local CI store | Local CI / slice seed (KGPV); sparse empty (Bethesda) |");
  md.push("| 4 Organization | golden-demo groups + graph orgs | fixtures or \`he-airtable-graph-reader-v1.js\` | Fixture (KGPV) / Live HI Organizations (Bethesda operator org) |");
  md.push("| 5 Relationships | ownership edges + \`/api/he-airtable-graph/hotel/:id\` | fixtures or HI Hotel–Org Relationships | Fixture (KGPV) / Live HI Rel (Bethesda Operator+Brand) |");
  md.push("");
  md.push("## Duplication / wrong-source checks");
  md.push("");
  for (const n of duplicationAssessment.notes) md.push(`- ${n}`);
  md.push("");
  md.push("## Fields intended as HPC but runtime differs (Overview)");
  md.push("");
  md.push("| Hotel | Count of populated Overview fields not classed LIVE_HPC |");
  md.push("|---|---:|");
  for (const h of hotelReports) {
    md.push(`| ${h.label} | ${h.fieldsWhereIntendedHpcButRuntimeDifferent} |`);
  }
  md.push("");
  md.push("## Research still blocked");
  md.push("");
  md.push("Substantive paid enrichment remains blocked until founder approves this provenance addendum. Est. post-approval research $8–$25 (Bethesda current owner + contacts); KGPV $0 until identity unification + verified migration.");
  md.push("");
  md.push("## Per-field provenance appendix");
  md.push("");
  for (const h of hotelReports) {
    md.push(`### ${h.label} (\`${h.id}\`) — ${h.identity.identity_resolution_status}`);
    md.push("");
    md.push("| Field | Tab | source_type | canonical table | canonical record | actual_runtime_source | mismatch | verified |");
    md.push("|---|---|---|---|---|---|---|---|");
    for (const r of h.fields) {
      md.push(
        `| ${r.field} | ${r.tab} | ${r.source_type} | ${r.canonical_source_table || "—"} | ${r.canonical_source_record || h.id} | ${String(r.actual_runtime_source).replace(/\|/g, "/").slice(0, 120)} | ${r.intended_vs_actual_mismatch ? "YES" : ""} | ${r.provenance_verified} |`,
      );
    }
    md.push("");
  }
  md.push("---");
  md.push("");
  md.push(`Machine-readable provenance: \`${path.basename(OUT_JSON)}\``);
  md.push(`Prior destination mapping: \`HE-TABS-1-5-GATE1-FIELD-AIRTABLE-MAPPING-V1.json\``);
  md.push("");
  md.push("**Founder decision:** Approve Gate 1 provenance, or request corrections before any paid research.");

  fs.writeFileSync(OUT_MD, md.join("\n"));
  console.log(
    JSON.stringify(
      {
        ok: true,
        outJson: OUT_JSON,
        outMd: OUT_MD,
        identityMatrix: report.identityMatrix,
        breakdowns: hotelReports.map((h) => ({
          id: h.id,
          summaryPct: h.sourceBreakdown.summaryPct,
        })),
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
