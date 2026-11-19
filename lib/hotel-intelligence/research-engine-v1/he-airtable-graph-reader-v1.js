/**
 * Airtable-first Hotel Explorer reader for HI graph (tabs 2/4/5 inputs).
 * Traceable to HI Organizations + HI Hotel–Org Relationships — not a competing store.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GRAPH_BASE_ID_CANONICAL,
  HI_GRAPH_TABLES,
  MAP_HI_ORGANIZATION,
  MAP_HI_HOTEL_ORG_RELATIONSHIP,
  assertCanonicalGraphBase,
  resolveGraphBaseId,
} from "./airtable-graph-contract-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function loadRegistry() {
  const p = path.join(__dirname, "airtable-graph-table-ids.generated.json");
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

async function atFetch(baseId, token, tablePath) {
  const url = `https://api.airtable.com/v0/${baseId}/${tablePath}`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json().catch(() => ({}));
  return { res, json };
}

/**
 * Load hotel–org relationships (+ org records) for an HPC hotel id.
 * @returns {Promise<object>}
 */
export async function readHiGraphForHotel(hpcHotelId, opts = {}) {
  const token =
    opts.token || process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || null;
  if (!token) {
    return {
      ok: false,
      error: "NO_AIRTABLE_CREDENTIALS",
      hpcHotelId,
      relationships: [],
      organizations: [],
    };
  }
  const baseId = resolveGraphBaseId(opts.env || process.env);
  const gate = assertCanonicalGraphBase(baseId);
  if (!gate.ok) {
    return { ok: false, error: "BASE_GATE", gate, hpcHotelId, relationships: [], organizations: [] };
  }
  const registry = opts.registry || loadRegistry();
  const relTable = registry.tableIds[HI_GRAPH_TABLES.hotelOrgRelationships];
  const orgTable = registry.tableIds[HI_GRAPH_TABLES.organizations];
  const formula = encodeURIComponent(`{${MAP_HI_HOTEL_ORG_RELATIONSHIP.hpcHotelId}}='${hpcHotelId}'`);
  const { res, json } = await atFetch(baseId, token, `${relTable}?filterByFormula=${formula}&maxRecords=50`);
  if (!res.ok) {
    return {
      ok: false,
      error: "REL_QUERY_FAILED",
      detail: json,
      hpcHotelId,
      relationships: [],
      organizations: [],
    };
  }
  const relationships = (json.records || []).map((r) => ({
    airtableId: r.id,
    relationshipId: r.fields[MAP_HI_HOTEL_ORG_RELATIONSHIP.relationshipId],
    hpcHotelId: r.fields[MAP_HI_HOTEL_ORG_RELATIONSHIP.hpcHotelId],
    organizationId: r.fields[MAP_HI_HOTEL_ORG_RELATIONSHIP.organizationId],
    organizationLink: r.fields[MAP_HI_HOTEL_ORG_RELATIONSHIP.organizationLink],
    relationshipType: r.fields[MAP_HI_HOTEL_ORG_RELATIONSHIP.relationshipType],
    current: r.fields[MAP_HI_HOTEL_ORG_RELATIONSHIP.current],
    confidence: r.fields[MAP_HI_HOTEL_ORG_RELATIONSHIP.confidence],
    verificationState: r.fields[MAP_HI_HOTEL_ORG_RELATIONSHIP.verificationState],
    publicationState: r.fields[MAP_HI_HOTEL_ORG_RELATIONSHIP.publicationState],
    notes: r.fields[MAP_HI_HOTEL_ORG_RELATIONSHIP.notes],
  }));

  const orgRecIds = [
    ...new Set(
      relationships.flatMap((rel) =>
        Array.isArray(rel.organizationLink) ? rel.organizationLink : [],
      ),
    ),
  ];
  const organizations = [];
  for (const orgRecId of orgRecIds) {
    const { res: oRes, json: oJson } = await atFetch(baseId, token, `${orgTable}/${orgRecId}`);
    if (!oRes.ok) continue;
    const f = oJson.fields || {};
    organizations.push({
      airtableId: oJson.id,
      orgId: f[MAP_HI_ORGANIZATION.orgId],
      legalName: f[MAP_HI_ORGANIZATION.legalName],
      tradingName: f[MAP_HI_ORGANIZATION.tradingName],
      organizationType: f[MAP_HI_ORGANIZATION.organizationType],
      jurisdiction: f[MAP_HI_ORGANIZATION.jurisdiction],
      officialWebsite: f[MAP_HI_ORGANIZATION.officialWebsite],
      confidence: f[MAP_HI_ORGANIZATION.confidence],
      verificationState: f[MAP_HI_ORGANIZATION.verificationState],
      publicationState: f[MAP_HI_ORGANIZATION.publicationState],
    });
  }

  const byType = (type) =>
    relationships.find((r) => String(r.relationshipType) === type && r.current !== false) ||
    relationships.find((r) => String(r.relationshipType) === type) ||
    null;

  const orgById = (id) => organizations.find((o) => o.orgId === id || o.airtableId === id) || null;
  const orgForRel = (rel) => {
    if (!rel) return null;
    const linkId = Array.isArray(rel.organizationLink) ? rel.organizationLink[0] : null;
    return organizations.find((o) => o.airtableId === linkId) || orgById(rel.organizationId);
  };

  const economic = byType("Economic Owner");
  const legal = byType("Legal Owner");
  const operator = byType("Operator") || byType("Management Company");
  const brand = byType("Brand");

  const primaryOrg =
    orgForRel(economic) || orgForRel(operator) || orgForRel(brand) || organizations[0] || null;

  let portfolio = [];
  if (primaryOrg?.airtableId && opts.includePortfolio !== false) {
    try {
      portfolio = await listPortfolioForOrganization(primaryOrg.airtableId, {
        token,
        baseId,
        registry,
        focusHotelId: hpcHotelId,
        orgCanonicalId: primaryOrg.orgId || null,
        limit: opts.portfolioLimit || 40,
      });
    } catch (_err) {
      portfolio = [];
    }
  }

  return {
    ok: true,
    contract: "HE_AIRTABLE_GRAPH_READER_V1",
    baseId: GRAPH_BASE_ID_CANONICAL,
    hpcHotelId,
    relationshipCount: relationships.length,
    relationships,
    organizations,
    primaryOrganization: primaryOrg,
    portfolio,
    projection: {
      economic_owner: orgForRel(economic),
      property_company: orgForRel(legal),
      operator: orgForRel(operator),
      brand: orgForRel(brand),
      control_status:
        relationships.length === 0
          ? "NO_GRAPH_RELATIONSHIPS"
          : economic
            ? "ECONOMIC_OWNER_PRESENT"
            : operator
              ? "OPERATOR_PRESENT_OWNER_UNRESOLVED"
              : "RELATIONSHIPS_PRESENT",
      source: "HI_AIRTABLE_GRAPH",
    },
  };
}

/**
 * Hotels related to an HI Organization via HI Hotel–Org Relationships.
 * Read-only. Does not invent ownership.
 */
export async function listPortfolioForOrganization(orgAirtableId, opts = {}) {
  const token =
    opts.token || process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || null;
  if (!token || !orgAirtableId) return [];
  const baseId = opts.baseId || resolveGraphBaseId(opts.env || process.env);
  const gate = assertCanonicalGraphBase(baseId);
  if (!gate.ok) return [];
  const registry = opts.registry || loadRegistry();
  const relTable = registry.tableIds[HI_GRAPH_TABLES.hotelOrgRelationships];
  const orgRecId = String(orgAirtableId).replace(/'/g, "\\'");
  const orgCanonicalId = String(opts.orgCanonicalId || "").replace(/'/g, "\\'");
  // Prefer text Organization ID when provided; also try linked-record FIND.
  const formula = encodeURIComponent(
    orgCanonicalId
      ? `{${MAP_HI_HOTEL_ORG_RELATIONSHIP.organizationId}}='${orgCanonicalId}'`
      : `FIND('${orgRecId}', ARRAYJOIN({${MAP_HI_HOTEL_ORG_RELATIONSHIP.organizationLink}}))`,
  );
  const max = Math.min(Number(opts.limit) || 40, 100);
  let { res, json } = await atFetch(
    baseId,
    token,
    `${relTable}?filterByFormula=${formula}&maxRecords=${max}`,
  );
  // Fallback: if linked FIND returns empty, try Organization ID text via org lookup.
  if (res.ok && !(json.records || []).length && !orgCanonicalId) {
    const orgTable = registry.tableIds[HI_GRAPH_TABLES.organizations];
    const orgGet = await atFetch(baseId, token, `${orgTable}/${orgRecId}`);
    const orgIdText = orgGet.json?.fields?.[MAP_HI_ORGANIZATION.orgId];
    if (orgIdText) {
      const formula2 = encodeURIComponent(
        `{${MAP_HI_HOTEL_ORG_RELATIONSHIP.organizationId}}='${String(orgIdText).replace(/'/g, "\\'")}'`,
      );
      ({ res, json } = await atFetch(
        baseId,
        token,
        `${relTable}?filterByFormula=${formula2}&maxRecords=${max}`,
      ));
    }
  }
  if (!res.ok) return [];

  const focus = opts.focusHotelId || null;
  return (json.records || []).map((r) => {
    const f = r.fields || {};
    const hpcId = f[MAP_HI_HOTEL_ORG_RELATIONSHIP.hpcHotelId] || null;
    const relType = f[MAP_HI_HOTEL_ORG_RELATIONSHIP.relationshipType] || "RELATED";
    return {
      airtable_record_id: hpcId,
      relationship_airtable_id: r.id,
      relationship_type: relType,
      verification_state: f[MAP_HI_HOTEL_ORG_RELATIONSHIP.verificationState] || null,
      confidence: f[MAP_HI_HOTEL_ORG_RELATIONSHIP.confidence] || null,
      focus_hotel: Boolean(focus && hpcId === focus),
      economic_owner_verified: String(relType) === "Economic Owner",
      source: "HI_AIRTABLE_GRAPH",
    };
  });
}

export default { readHiGraphForHotel, listPortfolioForOrganization };
