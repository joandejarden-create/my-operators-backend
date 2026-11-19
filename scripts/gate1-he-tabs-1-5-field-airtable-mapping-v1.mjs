#!/usr/bin/env node
/**
 * Gate 1 — HE Tabs 1–5 field → Airtable mapping + reuse/gap founder report.
 * No substantive paid research. Live HE API + HI graph read-only probes only.
 *
 *   node scripts/gate1-he-tabs-1-5-field-airtable-mapping-v1.mjs
 *   node scripts/gate1-he-tabs-1-5-field-airtable-mapping-v1.mjs --base-url http://127.0.0.1:8091
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import {
  resolveHeAuditBaseUrl,
  assertHeLocalRuntimeTarget,
} from "./lib/he-local-runtime-target.mjs";
import {
  GRAPH_BASE_ID_CANONICAL,
  MAP_HI_ORGANIZATION,
  MAP_HI_HOTEL_ORG_RELATIONSHIP,
  MAP_HI_ORG_CONTACT,
  MAP_HI_CONTACT_CHANNEL,
} from "../lib/hotel-intelligence/research-engine-v1/airtable-graph-contract-v1.js";

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

const registry = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "lib/hotel-intelligence/research-engine-v1/airtable-graph-table-ids.generated.json"),
    "utf8",
  ),
);

const BASE_ID = GRAPH_BASE_ID_CANONICAL;
const T = registry.tableIds;
const HPC = { id: registry.hpcTableId, name: registry.hpcTableName };
const CENSUS_LEGACY = { id: "tblgj2qEwxjTcg6q0", name: "Hotel Census (legacy identity surface)" };

const OUT_JSON = path.join(
  ROOT,
  "reports/hotel-explorer/HE-TABS-1-5-GATE1-FIELD-AIRTABLE-MAPPING-V1.json",
);
const OUT_MD = path.join(
  ROOT,
  "reports/hotel-explorer/HE-TABS-1-5-GATE1-FOUNDER-REVIEW-V1.md",
);

/** @typedef {'HPC'|'HI_ORG'|'HI_REL'|'HI_CONTACT'|'HI_CHANNEL'|'HI_RUN'|'HI_EVIDENCE'|'ADP'|'DERIVED'|'COMPOSITE'} DestClass */

/**
 * Complete field catalog with canonical Airtable destinations.
 * destinationStatus: ESTABLISHED | DERIVED_READ_MODEL | COMPOSITE_ESTABLISHED | UNCLEAR
 */
const FIELD_MAP = [
  // Tab 1 — Hotel Overview
  { tab: "hotel", section: "Header", field: "name", question: "What is the property name?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["Property Name", "name"], relationship: "HPC hotel record", destinationStatus: "ESTABLISHED", reuseSources: ["HPC", "Hotel Census"], researchHint: "reuse" },
  { tab: "hotel", section: "Header", field: "brand", question: "What brand/affiliation is displayed?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["Current Brand", "Affiliation"], relationship: "HPC hotel record (+ optional HI Rel Brand)", destinationStatus: "ESTABLISHED", reuseSources: ["HPC", "Brand Explorer"], researchHint: "reuse" },
  { tab: "hotel", section: "Header", field: "status", question: "Open / pipeline / other status?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["project_phase", "status", "Radar Status"], relationship: "HPC hotel record", destinationStatus: "ESTABLISHED", reuseSources: ["HPC"], researchHint: "reuse" },
  { tab: "hotel", section: "Header", field: "website", question: "Official property website?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["Official Property URL", "Website"], relationship: "HPC hotel record", destinationStatus: "ESTABLISHED", reuseSources: ["HPC", "Hotel Census"], researchHint: "reuse" },
  { tab: "hotel", section: "Header", field: "telephone", question: "Property phone?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["Phone", "Telephone"], relationship: "HPC hotel record", destinationStatus: "ESTABLISHED", reuseSources: ["HPC", "Hotel Census"], researchHint: "reuse" },
  { tab: "hotel", section: "Header", field: "address1", question: "Street address?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["Address 1"], relationship: "HPC hotel record", destinationStatus: "ESTABLISHED", reuseSources: ["HPC"], researchHint: "reuse" },
  { tab: "hotel", section: "Header", field: "city", question: "City?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["city", "City"], relationship: "HPC hotel record", destinationStatus: "ESTABLISHED", reuseSources: ["HPC"], researchHint: "reuse" },
  { tab: "hotel", section: "Header", field: "country", question: "Country?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["country", "Country"], relationship: "HPC hotel record", destinationStatus: "ESTABLISHED", reuseSources: ["HPC"], researchHint: "reuse" },
  { tab: "hotel", section: "Description", field: "hotelHeadline", question: "Short owner-facing headline?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["Hotel Headline"], relationship: "HPC enrichment / ADP overlay", destinationStatus: "ESTABLISHED", reuseSources: ["HPC", "ADP"], researchHint: "reuse_or_light_copy" },
  { tab: "hotel", section: "Description", field: "hotelDescription", question: "Short property summary?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["Short Property Summary", "description"], relationship: "HPC hotel record", destinationStatus: "ESTABLISHED", reuseSources: ["HPC", "ADP"], researchHint: "reuse_or_light_copy" },
  { tab: "hotel", section: "Property Facts", field: "parentCompany", question: "Brand parent company?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["Parent Company"], relationship: "HPC hotel record (brand parent, not economic owner)", destinationStatus: "ESTABLISHED", reuseSources: ["HPC", "Brand Explorer"], researchHint: "reuse" },
  { tab: "hotel", section: "Property Facts", field: "chainScale", question: "Chain scale?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["Chain Scale"], relationship: "HPC hotel record", destinationStatus: "ESTABLISHED", reuseSources: ["HPC"], researchHint: "reuse" },
  { tab: "hotel", section: "Property Facts", field: "censusPropertyType", question: "Property type?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["Property Type"], relationship: "HPC hotel record", destinationStatus: "ESTABLISHED", reuseSources: ["HPC"], researchHint: "reuse" },
  { tab: "hotel", section: "Property Facts", field: "rooms", question: "Rooms / keys?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["Rooms / Keys", "rooms"], relationship: "HPC hotel record", destinationStatus: "ESTABLISHED", reuseSources: ["HPC", "Hotel Census"], researchHint: "reuse" },
  { tab: "hotel", section: "Property Facts", field: "totalMeetingSpace", question: "Total meeting space?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["Total Meeting Space"], relationship: "HPC hotel record", destinationStatus: "ESTABLISHED", reuseSources: ["HPC"], researchHint: "reuse" },
  { tab: "hotel", section: "Property Facts", field: "operationType", question: "Franchise / managed / etc.?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["Operation Type"], relationship: "HPC hotel record", destinationStatus: "ESTABLISHED", reuseSources: ["HPC"], researchHint: "reuse" },
  { tab: "hotel", section: "Property Facts", field: "managementCompany", question: "Operator / management company label?", destClass: "COMPOSITE", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["Operator / Management Company"], relationship: `HPC text + HI Rel Operator (${T["HI Hotel–Org Relationships"]}) → HI Organizations`, destinationStatus: "COMPOSITE_ESTABLISHED", reuseSources: ["HPC", "HI_graph"], researchHint: "reuse" },
  { tab: "hotel", section: "Property Facts", field: "market", question: "Dealality market?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["Market"], relationship: "HPC hotel record", destinationStatus: "ESTABLISHED", reuseSources: ["HPC", "Radar"], researchHint: "reuse" },
  { tab: "hotel", section: "Property Facts", field: "submarket", question: "Dealality submarket/corridor?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["Submarket"], relationship: "HPC hotel record", destinationStatus: "ESTABLISHED", reuseSources: ["HPC", "Radar"], researchHint: "reuse" },
  { tab: "hotel", section: "Property Facts", field: "locationType", question: "Location type (resort/suburban/…)?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["Location"], relationship: "HPC hotel record", destinationStatus: "ESTABLISHED", reuseSources: ["HPC"], researchHint: "reuse" },
  { tab: "hotel", section: "Property Facts", field: "projectPhase", question: "Project phase?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["project_phase"], relationship: "HPC hotel record", destinationStatus: "ESTABLISHED", reuseSources: ["HPC"], researchHint: "reuse" },
  { tab: "hotel", section: "Property Facts", field: "projectedOpenDate", question: "Projected open date (if pipeline)?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["projected_open_date"], relationship: "HPC hotel record", destinationStatus: "ESTABLISHED", reuseSources: ["HPC"], researchHint: "reuse_if_pipeline" },
  { tab: "hotel", section: "Property Facts", field: "starRating", question: "Star rating?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["Star Rating"], relationship: "HPC hotel record", destinationStatus: "ESTABLISHED", reuseSources: ["HPC"], researchHint: "reuse_or_optional" },
  { tab: "hotel", section: "Property Facts", field: "dataConfidence", question: "Census data confidence?", destClass: "HPC", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["Data Confidence"], relationship: "HPC hotel record", destinationStatus: "ESTABLISHED", reuseSources: ["HPC"], researchHint: "reuse" },
  { tab: "hotel", section: "Amenities", field: "amenitiesDisplay", question: "Amenities shown in Overview?", destClass: "COMPOSITE", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["HPC amenity fields"], relationship: "HPC amenities + ADP verified Attributes (display overlay; do not mutate ADP publication gates)", destinationStatus: "COMPOSITE_ESTABLISHED", reuseSources: ["HPC", "ADP"], researchHint: "reuse" },

  // Tab 2 — Ownership
  { tab: "ownership", section: "Executive Summary", field: "executive_summary.text", question: "Who owns / controls — narrative summary?", destClass: "HI_RUN", baseId: BASE_ID, tableId: T["HI Ownership Research Runs"], tableName: "HI Ownership Research Runs", fields: ["Summary", "Notes"], relationship: "Run → HPC hotel; evidence in HI Graph Evidence", destinationStatus: "ESTABLISHED", reuseSources: ["HI_graph", "golden_demo_fixtures"], researchHint: "migrate_or_research" },
  { tab: "ownership", section: "Executive Summary", field: "what_remains_uncertain", question: "What ownership facts remain uncertain?", destClass: "HI_EVIDENCE", baseId: BASE_ID, tableId: T["HI Graph Evidence"], tableName: "HI Graph Evidence", fields: ["Notes", "Evidence"], relationship: "Evidence linked to hotel/org/rel", destinationStatus: "ESTABLISHED", reuseSources: ["HI_graph"], researchHint: "research_gaps" },
  { tab: "ownership", section: "Meta", field: "economic_owner", question: "Who is the economic owner?", destClass: "HI_REL", baseId: BASE_ID, tableId: T["HI Hotel–Org Relationships"], tableName: "HI Hotel–Org Relationships", fields: [MAP_HI_HOTEL_ORG_RELATIONSHIP.relationshipType, MAP_HI_ORGANIZATION.legalName, MAP_HI_ORGANIZATION.tradingName], relationship: `RelationshipType=Economic Owner → ${T["HI Organizations"]} via Organization link; HPC Hotel → ${HPC.id}`, destinationStatus: "ESTABLISHED", reuseSources: ["HI_graph", "golden_demo_fixtures"], researchHint: "critical_research_if_missing" },
  { tab: "ownership", section: "Meta", field: "property_company", question: "Legal PropCo / property vehicle?", destClass: "HI_REL", baseId: BASE_ID, tableId: T["HI Hotel–Org Relationships"], tableName: "HI Hotel–Org Relationships", fields: ["Relationship Type=Legal Owner"], relationship: "Legal Owner → HI Organizations", destinationStatus: "ESTABLISHED", reuseSources: ["HI_graph", "golden_demo_fixtures"], researchHint: "research_if_missing" },
  { tab: "ownership", section: "Meta", field: "operator", question: "Who operates/manages the hotel?", destClass: "HI_REL", baseId: BASE_ID, tableId: T["HI Hotel–Org Relationships"], tableName: "HI Hotel–Org Relationships", fields: ["Relationship Type=Operator|Management Company"], relationship: "Operator → HI Organizations (+ HPC Operator text)", destinationStatus: "ESTABLISHED", reuseSources: ["HPC", "HI_graph"], researchHint: "reuse" },
  { tab: "ownership", section: "Meta", field: "control_status", question: "Control / verification posture?", destClass: "DERIVED", baseId: BASE_ID, tableId: T["HI Hotel–Org Relationships"], tableName: "HI Hotel–Org Relationships", fields: [MAP_HI_HOTEL_ORG_RELATIONSHIP.verificationState], relationship: "Derived from relationship Verification State + presence of Economic Owner", destinationStatus: "DERIVED_READ_MODEL", reuseSources: ["HI_graph"], researchHint: "derived" },
  { tab: "ownership", section: "Meta", field: "source_coverage", question: "How many evidence sources support ownership?", destClass: "DERIVED", baseId: BASE_ID, tableId: T["HI Graph Evidence"], tableName: "HI Graph Evidence", fields: ["count of evidence rows"], relationship: "Derived from HI Graph Evidence for hotel", destinationStatus: "DERIVED_READ_MODEL", reuseSources: ["HI_graph"], researchHint: "derived" },
  { tab: "ownership", section: "Insights", field: "what_we_know[]", question: "Key ownership facts we know?", destClass: "HI_EVIDENCE", baseId: BASE_ID, tableId: T["HI Graph Evidence"], tableName: "HI Graph Evidence", fields: ["Evidence", "Notes"], relationship: "Graph evidence + run summary", destinationStatus: "ESTABLISHED", reuseSources: ["HI_graph", "golden_demo_fixtures"], researchHint: "migrate_or_research" },
  { tab: "ownership", section: "Ownership & Control", field: "control_rows", question: "Role × org control table?", destClass: "HI_REL", baseId: BASE_ID, tableId: T["HI Hotel–Org Relationships"], tableName: "HI Hotel–Org Relationships", fields: ["Relationship Type", "Organization", "Verification State"], relationship: "All current hotel–org relationships", destinationStatus: "ESTABLISHED", reuseSources: ["HI_graph"], researchHint: "reuse_graph" },
  { tab: "ownership", section: "UBO", field: "ubo_group", question: "Ultimate beneficial owner / parent group?", destClass: "HI_ORG", baseId: BASE_ID, tableId: T["HI Organizations"], tableName: "HI Organizations", fields: [MAP_HI_ORGANIZATION.parentOrgLink, MAP_HI_ORGANIZATION.parentOrgId], relationship: "Parent Organization chain on HI Organizations", destinationStatus: "ESTABLISHED", reuseSources: ["HI_graph", "golden_demo_fixtures"], researchHint: "research_if_missing" },
  { tab: "ownership", section: "Portfolio Preview", field: "portfolio_preview", question: "Other hotels in same ownership/control?", destClass: "DERIVED", baseId: BASE_ID, tableId: T["HI Hotel–Org Relationships"], tableName: "HI Hotel–Org Relationships", fields: ["same Organization across HPC"], relationship: "Query HI Rel by Organization ID across HPC", destinationStatus: "DERIVED_READ_MODEL", reuseSources: ["HI_graph"], researchHint: "derived_after_org_persist" },
  { tab: "ownership", section: "Franchise & Brand", field: "brand_status", question: "Brand relationship / affiliation status?", destClass: "COMPOSITE", baseId: BASE_ID, tableId: T["HI Hotel–Org Relationships"], tableName: "HI Hotel–Org Relationships", fields: ["Relationship Type=Brand"], relationship: "HI Rel Brand + HPC Affiliation/Current Brand", destinationStatus: "COMPOSITE_ESTABLISHED", reuseSources: ["HPC", "HI_graph", "Brand Explorer"], researchHint: "reuse" },
  { tab: "ownership", section: "People", field: "decision_authority.people", question: "Who has decision authority on ownership side?", destClass: "HI_CONTACT", baseId: BASE_ID, tableId: T["HI Org Contacts"], tableName: "HI Org Contacts", fields: [MAP_HI_ORG_CONTACT.displayName, MAP_HI_ORG_CONTACT.title, MAP_HI_ORG_CONTACT.functionRole], relationship: "HI Org Contacts → HI Organizations", destinationStatus: "ESTABLISHED", reuseSources: ["HI_graph", "CI_showcase", "golden_demo_fixtures"], researchHint: "migrate_verified_only" },
  { tab: "ownership", section: "Signatories", field: "qualified_signatories", question: "Qualified signatories?", destClass: "HI_CONTACT", baseId: BASE_ID, tableId: T["HI Org Contacts"], tableName: "HI Org Contacts", fields: ["authority / notes flags"], relationship: "HI Org Contacts with authority annotation (field may need ensure)", destinationStatus: "ESTABLISHED", reuseSources: ["HI_graph"], researchHint: "optional_schema_flag" },
  { tab: "ownership", section: "Channels", field: "corporate_contact_channels", question: "Corporate email/phone paths?", destClass: "HI_CHANNEL", baseId: BASE_ID, tableId: T["HI Contact Channels"], tableName: "HI Contact Channels", fields: [MAP_HI_CONTACT_CHANNEL.channelKind, MAP_HI_CONTACT_CHANNEL.channelValue], relationship: "Channels → Person or Organization", destinationStatus: "ESTABLISHED", reuseSources: ["HI_graph", "CI_showcase"], researchHint: "migrate_verified_only" },
  { tab: "ownership", section: "Asset Profile", field: "asset_agreement_profile", question: "Franchise / management agreement posture?", destClass: "COMPOSITE", baseId: BASE_ID, tableId: HPC.id, tableName: HPC.name, fields: ["Operation Type"], relationship: "HPC Operation Type + HI Rel Operator/Brand notes", destinationStatus: "COMPOSITE_ESTABLISHED", reuseSources: ["HPC", "HI_graph"], researchHint: "reuse_light" },
  { tab: "ownership", section: "History", field: "property_history", question: "Ownership / transaction history?", destClass: "HI_EVIDENCE", baseId: BASE_ID, tableId: T["HI Graph Evidence"], tableName: "HI Graph Evidence", fields: ["Evidence", "Observed At", "Source URL"], relationship: "Historical evidence rows (not current Economic Owner claim)", destinationStatus: "ESTABLISHED", reuseSources: ["HI_graph", "staged_research"], researchHint: "stage_historical_separately" },
  { tab: "ownership", section: "Pursuit", field: "development_pursuit", question: "Development / pursuit notes?", destClass: "HI_RUN", baseId: BASE_ID, tableId: T["HI Ownership Research Runs"], tableName: "HI Ownership Research Runs", fields: ["Notes"], relationship: "Research run notes", destinationStatus: "ESTABLISHED", reuseSources: ["HI_graph"], researchHint: "optional" },
  { tab: "ownership", section: "Gaps", field: "research_gaps", question: "Open research gaps?", destClass: "HI_RUN", baseId: BASE_ID, tableId: T["HI Ownership Research Runs"], tableName: "HI Ownership Research Runs", fields: ["Notes", "Status"], relationship: "Research run ledger", destinationStatus: "ESTABLISHED", reuseSources: ["HI_graph"], researchHint: "derived_from_gaps" },
  { tab: "ownership", section: "References", field: "sources", question: "Source bibliography?", destClass: "HI_EVIDENCE", baseId: BASE_ID, tableId: T["HI Graph Evidence"], tableName: "HI Graph Evidence", fields: ["Source Name", "Source URL"], relationship: "HI Graph Evidence", destinationStatus: "ESTABLISHED", reuseSources: ["HI_graph"], researchHint: "persist_with_evidence" },

  // Tab 3 — Contacts
  { tab: "contacts", section: "Quality", field: "quality_summary", question: "Contact package quality summary?", destClass: "DERIVED", baseId: BASE_ID, tableId: T["HI Contact Channels"], tableName: "HI Contact Channels", fields: ["derived from channel verification"], relationship: "Derived from HI Org Contacts + Channels", destinationStatus: "DERIVED_READ_MODEL", reuseSources: ["HI_graph", "CI_runtime"], researchHint: "derived" },
  { tab: "contacts", section: "Ownership Summary", field: "ownershipSummary", question: "Ownership summary on Contacts tab?", destClass: "DERIVED", baseId: BASE_ID, tableId: T["HI Hotel–Org Relationships"], tableName: "HI Hotel–Org Relationships", fields: ["projection"], relationship: "Projection of ownership relationships", destinationStatus: "DERIVED_READ_MODEL", reuseSources: ["HI_graph"], researchHint: "derived" },
  { tab: "contacts", section: "Orgs", field: "organization_contacts", question: "Organization-level contact routes?", destClass: "HI_CHANNEL", baseId: BASE_ID, tableId: T["HI Contact Channels"], tableName: "HI Contact Channels", fields: [MAP_HI_CONTACT_CHANNEL.channelValue], relationship: "Org-subject channels + HI Organizations", destinationStatus: "ESTABLISHED", reuseSources: ["CI_showcase", "HI_graph"], researchHint: "migrate_verified_only" },
  { tab: "contacts", section: "People", field: "people_groups", question: "People grouped by role?", destClass: "HI_CONTACT", baseId: BASE_ID, tableId: T["HI Org Contacts"], tableName: "HI Org Contacts", fields: [MAP_HI_ORG_CONTACT.displayName, MAP_HI_ORG_CONTACT.title], relationship: "HI Org Contacts", destinationStatus: "ESTABLISHED", reuseSources: ["CI_showcase", "HI_graph"], researchHint: "migrate_verified_only" },
  { tab: "contacts", section: "Channels", field: "emails_phones", question: "Emails and phones?", destClass: "HI_CHANNEL", baseId: BASE_ID, tableId: T["HI Contact Channels"], tableName: "HI Contact Channels", fields: [MAP_HI_CONTACT_CHANNEL.channelKind, MAP_HI_CONTACT_CHANNEL.channelValue, MAP_HI_CONTACT_CHANNEL.verificationStatus], relationship: "HI Contact Channels", destinationStatus: "ESTABLISHED", reuseSources: ["CI_showcase", "HI_graph"], researchHint: "migrate_verified_only" },
  { tab: "contacts", section: "Fallback", field: "fallback_paths", question: "Public/fallback contact paths?", destClass: "HI_CHANNEL", baseId: BASE_ID, tableId: T["HI Contact Channels"], tableName: "HI Contact Channels", fields: ["Classification", "Access / Publication Restriction"], relationship: "Public/fallback channels", destinationStatus: "ESTABLISHED", reuseSources: ["HPC_phone_website", "CI_showcase"], researchHint: "reuse_property_channels" },
  { tab: "contacts", section: "Unresolved", field: "research_unresolved", question: "Unresolved contact research?", destClass: "HI_RUN", baseId: BASE_ID, tableId: T["HI Ownership Research Runs"], tableName: "HI Ownership Research Runs", fields: ["Notes", "Status"], relationship: "Research run unresolved reasons", destinationStatus: "ESTABLISHED", reuseSources: ["CI_runtime"], researchHint: "derived" },

  // Tab 4 — Organization
  { tab: "organization", section: "Overview", field: "display_name", question: "Organization trading/display name?", destClass: "HI_ORG", baseId: BASE_ID, tableId: T["HI Organizations"], tableName: "HI Organizations", fields: [MAP_HI_ORGANIZATION.tradingName], relationship: "HI Organizations", destinationStatus: "ESTABLISHED", reuseSources: ["HI_graph", "golden_demo_fixtures"], researchHint: "reuse_after_org_persist" },
  { tab: "organization", section: "Overview", field: "legal_name", question: "Organization legal name?", destClass: "HI_ORG", baseId: BASE_ID, tableId: T["HI Organizations"], tableName: "HI Organizations", fields: [MAP_HI_ORGANIZATION.legalName], relationship: "HI Organizations", destinationStatus: "ESTABLISHED", reuseSources: ["HI_graph", "golden_demo_fixtures"], researchHint: "reuse_after_org_persist" },
  { tab: "organization", section: "Overview", field: "ticker", question: "Ticker / registry identifier?", destClass: "HI_ORG", baseId: BASE_ID, tableId: T["HI Organizations"], tableName: "HI Organizations", fields: [MAP_HI_ORGANIZATION.registryId], relationship: "HI Organizations Registry / Identifier", destinationStatus: "ESTABLISHED", reuseSources: ["public_filings", "golden_demo_fixtures"], researchHint: "reuse_or_light_lookup" },
  { tab: "organization", section: "KPIs", field: "portfolio_kpis", question: "Portfolio KPI strip?", destClass: "DERIVED", baseId: BASE_ID, tableId: T["HI Hotel–Org Relationships"], tableName: "HI Hotel–Org Relationships", fields: ["aggregated counts"], relationship: "Derived from HI Rel across portfolio", destinationStatus: "DERIVED_READ_MODEL", reuseSources: ["HI_graph"], researchHint: "derived" },
  { tab: "organization", section: "Owned", field: "owned_controlled_hotels", question: "Hotels owned/controlled by org?", destClass: "DERIVED", baseId: BASE_ID, tableId: T["HI Hotel–Org Relationships"], tableName: "HI Hotel–Org Relationships", fields: ["owner-role relationships"], relationship: "Query by Organization + owner roles", destinationStatus: "DERIVED_READ_MODEL", reuseSources: ["HI_graph"], researchHint: "derived" },
  { tab: "organization", section: "Operated", field: "operated_managed_hotels", question: "Hotels operated/managed by org?", destClass: "DERIVED", baseId: BASE_ID, tableId: T["HI Hotel–Org Relationships"], tableName: "HI Hotel–Org Relationships", fields: ["Operator relationships"], relationship: "Query by Organization + Operator", destinationStatus: "DERIVED_READ_MODEL", reuseSources: ["HI_graph"], researchHint: "derived" },
  { tab: "organization", section: "People", field: "key_people", question: "Key people at organization?", destClass: "HI_CONTACT", baseId: BASE_ID, tableId: T["HI Org Contacts"], tableName: "HI Org Contacts", fields: [MAP_HI_ORG_CONTACT.displayName, MAP_HI_ORG_CONTACT.title], relationship: "HI Org Contacts for org", destinationStatus: "ESTABLISHED", reuseSources: ["HI_graph", "CI_showcase"], researchHint: "migrate_verified_only" },
  { tab: "organization", section: "Context", field: "why_this_matters", question: "Why this org matters for the hotel?", destClass: "HI_ORG", baseId: BASE_ID, tableId: T["HI Organizations"], tableName: "HI Organizations", fields: [MAP_HI_ORGANIZATION.notes, MAP_HI_ORGANIZATION.provenanceSummary], relationship: "HI Organizations Notes / Provenance", destinationStatus: "ESTABLISHED", reuseSources: ["HI_graph"], researchHint: "copy_after_facts" },

  // Tab 5 — Relationships
  { tab: "relationships", section: "Structure", field: "structure_chart", question: "Ownership/control structure chart?", destClass: "DERIVED", baseId: BASE_ID, tableId: T["HI Hotel–Org Relationships"], tableName: "HI Hotel–Org Relationships", fields: ["graph projection"], relationship: "Derived UI from HI Rel + HI Orgs", destinationStatus: "DERIVED_READ_MODEL", reuseSources: ["HI_graph", "golden_demo_fixtures"], researchHint: "derived" },
  { tab: "relationships", section: "Edges", field: "relationship_edges", question: "Relationship edges list?", destClass: "HI_REL", baseId: BASE_ID, tableId: T["HI Hotel–Org Relationships"], tableName: "HI Hotel–Org Relationships", fields: ["Relationship Type", "Organization", "HPC Hotel"], relationship: "Direct HI Rel rows", destinationStatus: "ESTABLISHED", reuseSources: ["HI_graph"], researchHint: "reuse_graph" },
  { tab: "relationships", section: "Network", field: "org_network", question: "Broader org network?", destClass: "DERIVED", baseId: BASE_ID, tableId: T["HI Organizations"], tableName: "HI Organizations", fields: [MAP_HI_ORGANIZATION.parentOrgLink], relationship: "Orgs + parent links + portfolio rels", destinationStatus: "DERIVED_READ_MODEL", reuseSources: ["HI_graph"], researchHint: "derived" },
];

const HOTELS = [
  {
    id: "recUNycnMwOVFX0hc",
    label: "Krystal Grand Puerto Vallarta",
    role: "REFERENCE_AUDIT",
    identityNote: `Hotel Census–native (${CENSUS_LEGACY.id}); HI HPC Hotel link targets ${HPC.id} — relationship writes blocked until identity unification`,
  },
  {
    id: "recLuxvwwxID7U2B8",
    label: "Bethesda Marriott",
    role: "REPEATABILITY_TARGET",
    identityNote: `HPC-linkable (${HPC.id}); Operator+Brand HI rels persisted; Economic Owner unresolved`,
  },
];

function nonempty(v, field) {
  if (v == null) return false;
  if (typeof v === "number") {
    if (field === "rooms" && v === 0) return false;
    return Number.isFinite(v);
  }
  if (typeof v === "boolean") return true;
  if (Array.isArray(v)) return v.length > 0;
  if (typeof v === "object") return Object.keys(v).length > 0;
  const s = String(v).trim();
  return s.length > 0 && s !== "—" && s.toLowerCase() !== "null" && s !== "undefined";
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
    for (const p of [
      path.join("C:/Dev/dealality/my-operators-backend", ".env"),
      path.join(ROOT, ".env"),
    ]) {
      if (fs.existsSync(p)) dotenv.config({ path: p });
    }
  } catch {
    /* optional */
  }
}

function extractUiValue(field, bundle) {
  const { brandPresence, ownership, contacts, org, graph } = bundle;
  const h = brandPresence.json?.hotel || brandPresence.json?.data?.hotel || {};
  const o = ownership.json || {};
  const c = contacts.json || {};
  const pkg = c.package || c;
  const g = graph.json || graph;

  switch (field) {
    case "name":
      return h.name;
    case "brand":
      return h.brand || h.affiliation;
    case "status":
      return h.status || h.projectPhase || h.project_phase;
    case "website":
      return h.website || h.officialPropertyUrl;
    case "telephone":
      return h.telephone || h.phone;
    case "address1":
      return h.address1 || h.address;
    case "city":
      return h.city;
    case "country":
      return h.country;
    case "hotelHeadline":
      return h.hotelHeadline || h.headline;
    case "hotelDescription":
      return h.hotelDescription || h.description;
    case "parentCompany":
      return h.parentCompany;
    case "chainScale":
      return h.chainScale;
    case "censusPropertyType":
      return h.censusPropertyType || h.propertyType;
    case "rooms":
      return h.rooms;
    case "totalMeetingSpace":
      return h.totalMeetingSpace;
    case "operationType":
      return h.operationType;
    case "managementCompany":
      return h.managementCompany || h.operator;
    case "market":
      return h.market;
    case "submarket":
      return h.submarket;
    case "locationType":
      return h.locationType || h.location;
    case "projectPhase":
      return h.projectPhase || h.project_phase;
    case "projectedOpenDate":
      return h.projectedOpenDate || h.projected_open_date;
    case "starRating":
      return h.starRating;
    case "dataConfidence":
      return h.dataConfidence;
    case "amenitiesDisplay":
      return h.amenitiesDisplay || h.amenities;
    case "executive_summary.text":
      return getPath(o, "executive_summary.text") || o.executive_summary || o.summary;
    case "what_remains_uncertain":
      return o.what_remains_uncertain;
    case "economic_owner":
      return (
        o.economic_owner ||
        o.ownership?.economic_owner ||
        g?.projection?.economic_owner?.legalName ||
        g?.projection?.economic_owner?.tradingName
      );
    case "property_company":
      return (
        o.property_company ||
        o.ownership?.property_company ||
        g?.projection?.property_company?.legalName
      );
    case "operator":
      return (
        o.operator ||
        o.ownership?.operator ||
        g?.projection?.operator ||
        (g?.projection?.operator ? g.projection.operator : null)
      );
    case "control_status":
      return o.control_status || g?.projection?.control_status;
    case "source_coverage":
      return o.source_coverage;
    case "what_we_know[]":
      return o.what_we_know || o.insights?.what_we_know;
    case "control_rows":
      return o.control_rows || g?.relationships;
    case "ubo_group":
      return o.ubo_group;
    case "portfolio_preview":
      return o.portfolio_preview;
    case "brand_status":
      return o.brand_status || g?.projection?.brand;
    case "decision_authority.people":
      return getPath(o, "decision_authority.people") || o.decision_authority;
    case "qualified_signatories":
      return o.qualified_signatories;
    case "corporate_contact_channels":
      return o.corporate_contact_channels;
    case "asset_agreement_profile":
      return o.asset_agreement_profile;
    case "property_history":
      return o.property_history;
    case "development_pursuit":
      return o.development_pursuit;
    case "research_gaps":
      return o.research_gaps;
    case "sources":
      return o.sources;
    case "quality_summary":
      return pkg.quality_summary || c.quality_summary;
    case "ownershipSummary":
      return pkg.ownershipSummary || c.ownershipSummary;
    case "organization_contacts":
      return pkg.organization_contact_route || pkg.organization_contacts;
    case "people_groups":
      return pkg.people || pkg.people_groups;
    case "emails_phones": {
      const channels = [
        ...(pkg.hotel_contact?.channels || []),
        ...(pkg.organization_contact_route?.channels || []),
        ...((pkg.people || []).flatMap((p) => p.channels || [])),
      ];
      return channels.length ? channels : null;
    }
    case "fallback_paths":
      return pkg.fallback_paths;
    case "research_unresolved":
      return pkg.unresolved_reasons || pkg.research_unresolved;
    case "display_name":
      return org.json?.organization?.display_name || org.json?.display_name || g?.organizations?.[0]?.tradingName;
    case "legal_name":
      return org.json?.organization?.legal_name || org.json?.legal_name || g?.organizations?.[0]?.legalName;
    case "ticker":
      return org.json?.organization?.ticker || org.json?.ticker;
    case "portfolio_kpis":
      return org.json?.portfolio_kpis || org.json?.kpis;
    case "owned_controlled_hotels":
      return org.json?.owned_controlled_hotels;
    case "operated_managed_hotels":
      return org.json?.operated_managed_hotels;
    case "key_people":
      return org.json?.key_people;
    case "why_this_matters":
      return org.json?.why_this_matters;
    case "structure_chart":
      return o.structure_chart || o.relationships || g?.relationships;
    case "relationship_edges":
      return o.relationship_edges || g?.relationships;
    case "org_network":
      return o.org_network;
    default:
      return undefined;
  }
}

/**
 * Gate 1 intelligence class per field × hotel.
 * POPULATED_FROM_EXISTING — HE shows value from canonical/connected path (HPC/HI/ADP)
 * AVAILABLE_NOT_CONNECTED — intelligence exists (fixtures, CI showcase, staged, HPC) but HE not Airtable-canonical for this field
 * REQUIRES_NEW_RESEARCH — genuinely missing
 */
function classifyField(meta, hotel, uiPresent, bundle) {
  const g = bundle.graph.json || bundle.graph;
  const o = bundle.ownership.json || {};
  const inCohort = o.in_cohort === true;
  const dataStatus = o.data_status || null;
  const relCount = Number(g?.relationshipCount || g?.relationships?.length || 0);
  const hasOperatorRel = (g?.relationships || []).some((r) =>
    ["Operator", "Management Company"].includes(String(r.relationshipType)),
  );
  const hasBrandRel = (g?.relationships || []).some((r) => String(r.relationshipType) === "Brand");
  const hasEconomic = Boolean(g?.projection?.economic_owner);
  const ciBytes = bundle.contacts.bytes || 0;
  const ciRich = ciBytes > 5000;

  // Hotel overview: brand-presence from HPC/Census after alias fix = existing sources
  if (meta.tab === "hotel") {
    if (uiPresent) {
      return {
        gate1Class: "POPULATED_FROM_EXISTING",
        rationale: "Visible via brand-presence from HPC/Hotel Census (canonical property attributes).",
      };
    }
    // Open hotels: projectedOpenDate N/A
    if (meta.field === "projectedOpenDate") {
      return {
        gate1Class: "POPULATED_FROM_EXISTING",
        rationale: "N/A for Open assets — not a research gap; HPC field exists for pipeline hotels.",
        naOpenAsset: true,
      };
    }
    if (["starRating", "dataConfidence", "hotelHeadline", "hotelDescription", "censusPropertyType", "projectPhase"].includes(meta.field)) {
      return {
        gate1Class: "AVAILABLE_NOT_CONNECTED",
        rationale: "HPC field destination established; value may be empty on record or not projected — verify Census/HPC before researching.",
      };
    }
    return {
      gate1Class: "REQUIRES_NEW_RESEARCH",
      rationale: "Not populated in HE and not confirmed present in HPC projection.",
    };
  }

  // Ownership / org / relationships / contacts
  if (hotel.id === "recLuxvwwxID7U2B8") {
    if (meta.field === "operator" || meta.field === "managementCompany" || meta.field === "brand_status") {
      if (hasOperatorRel || hasBrandRel || uiPresent) {
        return {
          gate1Class: "POPULATED_FROM_EXISTING",
          rationale: "HI graph Operator/Brand and/or HPC operator/brand fields already available.",
        };
      }
    }
    if (["control_rows", "relationship_edges", "structure_chart", "control_status"].includes(meta.field) && relCount > 0) {
      return {
        gate1Class: uiPresent || relCount > 0 ? "POPULATED_FROM_EXISTING" : "AVAILABLE_NOT_CONNECTED",
        rationale: `${relCount} HI Hotel–Org Relationships present; UI may still be partial overlay.`,
      };
    }
    if (["display_name", "legal_name"].includes(meta.field) && (g?.organizations || []).length) {
      return {
        gate1Class: uiPresent ? "POPULATED_FROM_EXISTING" : "AVAILABLE_NOT_CONNECTED",
        rationale: "HI Organizations row exists (Marriott International) — wire Organization tab from graph reader.",
      };
    }
    if (meta.field === "economic_owner" || meta.field === "property_company" || meta.field === "ubo_group") {
      if (hasEconomic) {
        return { gate1Class: "POPULATED_FROM_EXISTING", rationale: "Economic owner present on HI graph." };
      }
      return {
        gate1Class: "REQUIRES_NEW_RESEARCH",
        rationale: "No current Economic/Legal Owner on HI graph. Historical 2013 Northview/Oaktree staged — not a current claim. Free SDAT/filings first; paid only if blocked.",
      };
    }
    if (meta.tab === "contacts" || ["decision_authority.people", "qualified_signatories", "corporate_contact_channels", "key_people", "emails_phones", "people_groups", "organization_contacts"].includes(meta.field)) {
      return {
        gate1Class: "REQUIRES_NEW_RESEARCH",
        rationale: "No HI Org Contacts / Channels for Bethesda; property phone/website are fallback only (HPC).",
      };
    }
    if (meta.destinationStatus === "DERIVED_READ_MODEL") {
      if (relCount > 0 && ["portfolio_kpis", "owned_controlled_hotels", "operated_managed_hotels", "portfolio_preview", "org_network"].includes(meta.field)) {
        return {
          gate1Class: "AVAILABLE_NOT_CONNECTED",
          rationale: "Derivable once org portfolio query is wired; Operator org already on graph.",
        };
      }
      return {
        gate1Class: hasEconomic || relCount > 0 ? "AVAILABLE_NOT_CONNECTED" : "REQUIRES_NEW_RESEARCH",
        rationale: "Derived read-model — populate after underlying graph facts exist.",
      };
    }
    if (uiPresent && dataStatus === "AIRTABLE_GRAPH_PARTIAL") {
      return {
        gate1Class: "POPULATED_FROM_EXISTING",
        rationale: "Visible from Airtable graph overlay / HPC.",
      };
    }
    return {
      gate1Class: "REQUIRES_NEW_RESEARCH",
      rationale: "Not in HI graph or HE projection for Bethesda.",
    };
  }

  // KGPV reference
  if (inCohort || ciRich) {
    if (meta.tab === "contacts" || ["decision_authority.people", "corporate_contact_channels", "emails_phones", "people_groups", "organization_contacts", "quality_summary", "ownershipSummary", "fallback_paths", "research_unresolved", "key_people"].includes(meta.field)) {
      return {
        gate1Class: "AVAILABLE_NOT_CONNECTED",
        rationale: "Rich golden-demo / Contact Intelligence showcase intelligence exists; NOT migrated to HI Org Contacts/Channels. GSF→Airtable blocked until fact verification (Gate 1+).",
      };
    }
    if (meta.tab === "ownership" || meta.tab === "organization" || meta.tab === "relationships") {
      if (relCount === 0) {
        return {
          gate1Class: "AVAILABLE_NOT_CONNECTED",
          rationale: "Fixture/golden-demo ownership intelligence exists; HI graph relationships blocked (Census↔HPC identity). Reuse fixtures after identity unification — do not re-research GSF ownership first.",
        };
      }
      return {
        gate1Class: "POPULATED_FROM_EXISTING",
        rationale: "HI graph linked.",
      };
    }
  }

  if (uiPresent) {
    return {
      gate1Class: "AVAILABLE_NOT_CONNECTED",
      rationale: "UI populated but canonical HI/HPC linkage not confirmed.",
    };
  }
  return {
    gate1Class: "REQUIRES_NEW_RESEARCH",
    rationale: "Empty in HE and no confirmed reusable store.",
  };
}

function pct(n, d) {
  if (!d) return 0;
  return Math.round((n / d) * 1000) / 10;
}

async function main() {
  loadDotenvFromIntegration();
  const baseUrl = resolveHeAuditBaseUrl(process.argv, process.env);
  const runtime = await assertHeLocalRuntimeTarget({ baseUrl });
  if (!runtime.ok) {
    console.error("[gate1] REFUSED runtime target", runtime);
    process.exit(2);
  }

  const hotelReports = [];
  for (const hotel of HOTELS) {
    const brandPresence = await fetchJson(`${baseUrl}/api/brand-presence/hotel/${hotel.id}`);
    const ownership = await fetchJson(`${baseUrl}/api/golden-demo/ownership/hotel/${hotel.id}`);
    const contacts = await fetchJson(
      `${baseUrl}/api/contact-intelligence/hotels/${hotel.id}?audience=internal`,
    );
    const graphApi = await fetchJson(`${baseUrl}/api/he-airtable-graph/hotel/${hotel.id}`);
    let org = { ok: false, status: 0, json: null, bytes: 0 };
    const slug =
      ownership.json?.organization?.slug ||
      ownership.json?.organization?.id ||
      ownership.json?.ownership_group?.slug;
    if (slug) {
      org = await fetchJson(`${baseUrl}/api/golden-demo/ownership/groups/${encodeURIComponent(slug)}`);
    }
    const bundle = { brandPresence, ownership, contacts, org, graph: graphApi };

    const rows = FIELD_MAP.map((meta) => {
      const value = extractUiValue(meta.field, bundle);
      const uiPresent = nonempty(value, meta.field);
      const classified = classifyField(meta, hotel, uiPresent, bundle);
      return {
        tab: meta.tab,
        section: meta.section,
        field: meta.field,
        question: meta.question,
        uiPresent,
        gate1Class: classified.gate1Class,
        rationale: classified.rationale,
        destinationStatus: meta.destinationStatus,
        destClass: meta.destClass,
        baseId: meta.baseId,
        tableId: meta.tableId,
        tableName: meta.tableName,
        airtableFields: meta.fields,
        relationship: meta.relationship,
        reuseSources: meta.reuseSources,
        researchHint: meta.researchHint,
        noEstablishedDestination: !["ESTABLISHED", "DERIVED_READ_MODEL", "COMPOSITE_ESTABLISHED"].includes(
          meta.destinationStatus,
        ),
      };
    });

    const counts = {
      total: rows.length,
      populatedFromExisting: rows.filter((r) => r.gate1Class === "POPULATED_FROM_EXISTING").length,
      availableNotConnected: rows.filter((r) => r.gate1Class === "AVAILABLE_NOT_CONNECTED").length,
      requiresNewResearch: rows.filter((r) => r.gate1Class === "REQUIRES_NEW_RESEARCH").length,
      noEstablishedDestination: rows.filter((r) => r.noEstablishedDestination).length,
    };

    hotelReports.push({
      ...hotel,
      brandPresenceName: brandPresence.json?.hotel?.name || null,
      ownershipDataStatus: ownership.json?.data_status || null,
      ownershipInCohort: ownership.json?.in_cohort === true,
      hiGraphRelationshipCount: graphApi.json?.relationshipCount ?? graphApi.json?.relationships?.length ?? 0,
      hiGraphRelTypes: (graphApi.json?.relationships || []).map((r) => r.relationshipType),
      contactBytes: contacts.bytes,
      counts,
      percentages: {
        populatedFromExistingPct: pct(counts.populatedFromExisting, counts.total),
        availableNotConnectedPct: pct(counts.availableNotConnected, counts.total),
        requiresNewResearchPct: pct(counts.requiresNewResearch, counts.total),
        noEstablishedDestinationPct: pct(counts.noEstablishedDestination, counts.total),
      },
      fields: rows,
    });
  }

  const catalogDestinationGaps = FIELD_MAP.filter(
    (m) => !["ESTABLISHED", "DERIVED_READ_MODEL", "COMPOSITE_ESTABLISHED"].includes(m.destinationStatus),
  );

  const schemaImprovements = [
    {
      id: "census_hpc_identity_unification",
      priority: "P0",
      summary:
        "Unify Hotel Census–native hotels (e.g. KGPV recUNycnMwOVFX0hc on tblgj2qEwxjTcg6q0) onto Hotel Property Census (tbl9aY5ijiuIzzWam) or retarget HI HPC Hotel link — currently blocks KGPV HI relationship writes.",
      blocks: ["KGPV ownership/org/relationship Airtable persistence", "GSF org↔hotel graph edges"],
    },
    {
      id: "he_org_tab_from_graph_reader",
      priority: "P1",
      summary:
        "Wire Organization + Relationships tabs to he-airtable-graph reader (display_name/legal_name/edges) so Bethesda Operator org is AVAILABLE→CONNECTED without research.",
      blocks: ["AVAILABLE_NOT_CONNECTED org/relationship fields on Bethesda"],
    },
    {
      id: "signatory_authority_flag",
      priority: "P2",
      summary:
        "Optional HI Org Contacts field/flag for qualified signatory / decision authority (today Notes-only).",
      blocks: ["qualified_signatories structured persistence"],
    },
    {
      id: "contact_migration_gate",
      priority: "P1",
      summary:
        "Verified-fact checklist before GSF showcase → HI Org Contacts / HI Contact Channels migration (no bulk CI runtime import).",
      blocks: ["KGPV contacts Airtable-canonical"],
    },
  ];

  const researchPriorities = [
    {
      priority: 1,
      hotel: "Bethesda Marriott",
      fields: ["economic_owner", "property_company", "ubo_group", "executive_summary.text", "sources", "what_we_know[]"],
      action: "Confirm CURRENT economic owner via free SDAT/county/filings first; paid Parallel only if blocked. Do not promote 2013 Northview/Oaktree as current.",
      estimatedUsd: "8–15",
      paidAllowedAfterGate1: true,
    },
    {
      priority: 2,
      hotel: "Bethesda Marriott",
      fields: ["decision_authority.people", "emails_phones", "people_groups", "key_people"],
      action: "After owner known — native/public contact paths first; paid enrichment only for verified decision-makers within remaining budget.",
      estimatedUsd: "5–10",
      paidAllowedAfterGate1: true,
    },
    {
      priority: 3,
      hotel: "KGPV",
      fields: ["all ownership/contacts fixture fields"],
      action: "NO paid research until Census↔HPC identity fix + verified migration of existing GSF fixture/showcase facts.",
      estimatedUsd: "0",
      paidAllowedAfterGate1: false,
    },
    {
      priority: 4,
      hotel: "Both",
      fields: ["starRating", "hotelHeadline gaps", "ticker"],
      action: "HPC/public reuse; optional light lookup — not Gate 1 blockers.",
      estimatedUsd: "0–2",
      paidAllowedAfterGate1: true,
    },
  ];

  const aggregateFieldSlots = hotelReports.reduce((n, h) => n + h.counts.total, 0);
  const aggregate = {
    fieldSlots: aggregateFieldSlots,
    populatedFromExisting: hotelReports.reduce((n, h) => n + h.counts.populatedFromExisting, 0),
    availableNotConnected: hotelReports.reduce((n, h) => n + h.counts.availableNotConnected, 0),
    requiresNewResearch: hotelReports.reduce((n, h) => n + h.counts.requiresNewResearch, 0),
    noEstablishedDestination: hotelReports.reduce((n, h) => n + h.counts.noEstablishedDestination, 0),
  };
  aggregate.percentages = {
    populatedFromExistingPct: pct(aggregate.populatedFromExisting, aggregate.fieldSlots),
    availableNotConnectedPct: pct(aggregate.availableNotConnected, aggregate.fieldSlots),
    requiresNewResearchPct: pct(aggregate.requiresNewResearch, aggregate.fieldSlots),
    noEstablishedDestinationPct: pct(aggregate.noEstablishedDestination, aggregate.fieldSlots),
  };

  const report = {
    contract: "HE_TABS_1_5_GATE1_FIELD_AIRTABLE_MAPPING_V1",
    gate: "GATE_1_FIELD_TO_AIRTABLE_MAPPING",
    status: "AWAITING_FOUNDER_APPROVAL",
    paidResearch: {
      substantivePaidEnrichment: "BLOCKED_UNTIL_GATE1_APPROVAL",
      spendToDateUsd: 0,
      authorizedCeilingUsd: 25,
      estimatedAdditionalResearchUsd: { low: 8, high: 25, notes: "Bethesda current owner + contacts; KGPV $0 until identity+migrate" },
      limitedMappingAmbiguityCallsUsed: 0,
    },
    principle: "Map first → Reuse existing intelligence → Identify genuine gaps → Research → Verify → Persist to correct Airtable → Display in HE → Validate",
    auditedAt: new Date().toISOString(),
    baseUrl,
    runtimeTarget: {
      servingDirectoryBasename: runtime.servingDirectoryBasename,
      servingStream: runtime.servingStream,
    },
    canonicalGraph: {
      baseId: BASE_ID,
      hpcTable: HPC,
      legacyCensusIdentityRisk: CENSUS_LEGACY,
      hiTables: T,
      forbiddenAdpEvidenceBase: registry.forbiddenBaseId,
    },
    catalog: {
      fieldCount: FIELD_MAP.length,
      tabs: ["hotel", "ownership", "contacts", "organization", "relationships"],
      destinationGaps: catalogDestinationGaps,
      allFieldsHaveEstablishedOrDerivedDestination: catalogDestinationGaps.length === 0,
    },
    aggregate,
    hotels: hotelReports,
    schemaOrIntegrationImprovements: schemaImprovements,
    researchPrioritization: researchPriorities,
    persistencePlan: {
      propertyAttributes: `Write/reuse ${HPC.name} (${HPC.id}) on ${BASE_ID}`,
      ownershipControl: `HI Hotel–Org Relationships (${T["HI Hotel–Org Relationships"]}) with HPC Hotel link + HI Organizations (${T["HI Organizations"]})`,
      people: `HI Org Contacts (${T["HI Org Contacts"]})`,
      channels: `HI Contact Channels (${T["HI Contact Channels"]})`,
      evidenceRuns: `HI Graph Evidence (${T["HI Graph Evidence"]}) + HI Ownership Research Runs (${T["HI Ownership Research Runs"]})`,
      adp: "ADP verified attributes = display overlay only; do not mutate publication gates",
      contactIntelligenceRuntime: "Staging only under HE CONTACT_INTELLIGENCE_DATA_ROOT — not canonical",
      gsfContacts: "Do not migrate to Airtable until facts + field mappings verified (this Gate 1 mapping establishes destinations; fact verification is next after approval)",
    },
  };

  fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
  fs.writeFileSync(OUT_JSON, JSON.stringify(report, null, 2));

  const md = [];
  md.push("# Gate 1 — HE Tabs 1–5 Field-to-Airtable Mapping (Founder Review)");
  md.push("");
  md.push(`**Status:** AWAITING FOUNDER APPROVAL — substantive paid research **blocked**`);
  md.push(`**Audited:** ${report.auditedAt}`);
  md.push(`**Runtime:** ${baseUrl} (${runtime.servingDirectoryBasename} / ${runtime.servingStream})`);
  md.push(`**Paid spend to date:** $0 · **Authorized ceiling:** $25 · **Est. after approval:** $8–$25`);
  md.push("");
  md.push("## Core principle");
  md.push("");
  md.push(report.principle);
  md.push("");
  md.push("## Gate 1 scorecard (required)");
  md.push("");
  md.push("| Metric | Aggregate (63 fields × 2 hotels) | KGPV | Bethesda |");
  md.push("|---|---:|---:|---:|");
  md.push(
    `| Already populated from existing sources | **${aggregate.percentages.populatedFromExistingPct}%** (${aggregate.populatedFromExisting}/${aggregate.fieldSlots}) | ${hotelReports[0].percentages.populatedFromExistingPct}% | ${hotelReports[1].percentages.populatedFromExistingPct}% |`,
  );
  md.push(
    `| Available but not yet connected | **${aggregate.percentages.availableNotConnectedPct}%** (${aggregate.availableNotConnected}) | ${hotelReports[0].percentages.availableNotConnectedPct}% | ${hotelReports[1].percentages.availableNotConnectedPct}% |`,
  );
  md.push(
    `| Requires new research | **${aggregate.percentages.requiresNewResearchPct}%** (${aggregate.requiresNewResearch}) | ${hotelReports[0].percentages.requiresNewResearchPct}% | ${hotelReports[1].percentages.requiresNewResearchPct}% |`,
  );
  md.push(
    `| No established Airtable destination | **${aggregate.percentages.noEstablishedDestinationPct}%** (${aggregate.noEstablishedDestination}) | ${hotelReports[0].percentages.noEstablishedDestinationPct}% | ${hotelReports[1].percentages.noEstablishedDestinationPct}% |`,
  );
  md.push("");
  md.push(`Catalog: **${FIELD_MAP.length}** fields across tabs 1–5. Established/derived/composite destinations: **${FIELD_MAP.length - catalogDestinationGaps.length}/${FIELD_MAP.length}**.`);
  md.push("");
  md.push("## Canonical Airtable architecture");
  md.push("");
  md.push(`- **Base:** \`${BASE_ID}\` (HPC / HI graph — not ADP evidence base \`${registry.forbiddenBaseId}\`)`);
  md.push(`- **Hotel Property Census:** \`${HPC.id}\``);
  md.push(`- **HI Organizations:** \`${T["HI Organizations"]}\``);
  md.push(`- **HI Hotel–Org Relationships:** \`${T["HI Hotel–Org Relationships"]}\` (HPC Hotel + Organization links)`);
  md.push(`- **HI Org Contacts:** \`${T["HI Org Contacts"]}\``);
  md.push(`- **HI Contact Channels:** \`${T["HI Contact Channels"]}\``);
  md.push(`- **HI Ownership Research Runs:** \`${T["HI Ownership Research Runs"]}\``);
  md.push(`- **HI Graph Evidence:** \`${T["HI Graph Evidence"]}\``);
  md.push("");
  md.push("## Existing intelligence vs gaps (executive)");
  md.push("");
  md.push("| Hotel | What already exists | What’s unconnected | Genuine research gaps |");
  md.push("|---|---|---|---|");
  md.push(
    "| **KGPV** | Full golden-demo ownership + GSF contact showcase; HPC/Census property facts | Fixture/CI intelligence **not** on HI graph (Census↔HPC identity blocker) | **Do not re-research** ownership/contacts first — migrate after identity fix + fact verification |",
  );
  md.push(
    "| **Bethesda** | HPC Overview (rooms/website/phone/operator/brand); HI Operator+Brand rels (`" +
      hotelReports[1].hiGraphRelTypes.join(", ") +
      "`) | Org tab / portfolio derived views not fully wired to graph reader | **Current economic owner**, PropCo/UBO, decision-maker contacts |",
  );
  md.push("");
  md.push("## Persistence plan (where new verified research must land)");
  md.push("");
  for (const [k, v] of Object.entries(report.persistencePlan)) {
    md.push(`- **${k}:** ${v}`);
  }
  md.push("");
  md.push("## Proposed schema / integration improvements");
  md.push("");
  for (const s of schemaImprovements) {
    md.push(`1. **${s.id}** (${s.priority}) — ${s.summary}`);
  }
  md.push("");
  md.push("## Research prioritization (only after reuse)");
  md.push("");
  for (const r of researchPriorities) {
    md.push(
      `${r.priority}. **${r.hotel}** — ${r.action} _(est. $${r.estimatedUsd}; paid after Gate 1: ${r.paidAllowedAfterGate1})_`,
    );
  }
  md.push("");
  md.push("## Estimated additional research cost");
  md.push("");
  md.push("| Band | USD | Scope |");
  md.push("|---|---:|---|");
  md.push("| Low | 8 | Bethesda current-owner confirmation if free sources fail |");
  md.push("| Mid | 15 | Owner + 1–2 decision-maker contact verifications |");
  md.push("| Ceiling | 25 | Full Bethesda ownership+contacts within authorized budget |");
  md.push("| KGPV | 0 | Reuse/migrate only until identity unification |");
  md.push("");
  md.push("## Field mapping appendix (catalog)");
  md.push("");
  md.push("| Tab | Field | Question | Table | Airtable fields | Destination |");
  md.push("|---|---|---|---|---|---|");
  for (const m of FIELD_MAP) {
    md.push(
      `| ${m.tab} | ${m.field} | ${m.question.replace(/\|/g, "/")} | ${m.tableName} | ${(m.fields || []).join("; ").replace(/\|/g, "/")} | ${m.destinationStatus} |`,
    );
  }
  md.push("");
  md.push("## Per-hotel field classifications");
  md.push("");
  for (const h of hotelReports) {
    md.push(`### ${h.label} (\`${h.id}\`)`);
    md.push("");
    md.push(`Identity: ${h.identityNote}`);
    md.push("");
    md.push(
      `HI graph rels: **${h.hiGraphRelationshipCount}** (${(h.hiGraphRelTypes || []).join(", ") || "none"}) · ownership status: ${h.ownershipDataStatus} · contact bytes: ${h.contactBytes}`,
    );
    md.push("");
    md.push("| Field | Tab | Gate 1 class | Rationale |");
    md.push("|---|---|---|---|");
    for (const r of h.fields) {
      md.push(`| ${r.field} | ${r.tab} | ${r.gate1Class} | ${String(r.rationale).replace(/\|/g, "/")} |`);
    }
    md.push("");
  }
  md.push("---");
  md.push("");
  md.push("**Founder decision required:** Approve Gate 1 to unlock substantive paid research within the $25 ceiling, or request mapping revisions.");
  md.push("");
  md.push(`Machine-readable: \`${path.basename(OUT_JSON)}\``);

  fs.writeFileSync(OUT_MD, md.join("\n"));
  console.log(
    JSON.stringify(
      {
        ok: true,
        outJson: OUT_JSON,
        outMd: OUT_MD,
        aggregate: report.aggregate.percentages,
        status: report.status,
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
