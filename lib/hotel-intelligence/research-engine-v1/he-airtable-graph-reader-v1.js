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
  MAP_HI_GRAPH_EVIDENCE,
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
  const evidenceTable = registry.tableIds[HI_GRAPH_TABLES.graphEvidence];
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
      evidence: [],
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

  let evidence = [];
  if (evidenceTable && opts.includeEvidence !== false) {
    try {
      evidence = await listEvidenceForHotel(hpcHotelId, {
        token,
        baseId,
        registry,
        limit: opts.evidenceLimit || 100,
      });
    } catch (_err) {
      evidence = [];
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
    evidence,
    evidenceCount: evidence.length,
    sourcesForUi: projectEvidenceToSourcesCitations(evidence),
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
 * Read HI Graph Evidence rows for an HPC hotel (Sources tab SoT).
 */
export async function listEvidenceForHotel(hpcHotelId, opts = {}) {
  const token =
    opts.token || process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || null;
  if (!token || !hpcHotelId) return [];
  const baseId = opts.baseId || resolveGraphBaseId(opts.env || process.env);
  const gate = assertCanonicalGraphBase(baseId);
  if (!gate.ok) return [];
  const registry = opts.registry || loadRegistry();
  const evidenceTable = registry.tableIds[HI_GRAPH_TABLES.graphEvidence];
  if (!evidenceTable) return [];
  const formula = encodeURIComponent(
    `{${MAP_HI_GRAPH_EVIDENCE.hpcHotelId}}='${String(hpcHotelId).replace(/'/g, "\\'")}'`,
  );
  const max = Math.min(Number(opts.limit) || 100, 200);
  const { res, json } = await atFetch(
    baseId,
    token,
    `${evidenceTable}?filterByFormula=${formula}&maxRecords=${max}`,
  );
  if (!res.ok) return [];
  return (json.records || []).map((r) => {
    const f = r.fields || {};
    return {
      airtableId: r.id,
      evidenceId: f[MAP_HI_GRAPH_EVIDENCE.evidenceId] || null,
      hpcHotelId: f[MAP_HI_GRAPH_EVIDENCE.hpcHotelId] || hpcHotelId,
      entityType: f[MAP_HI_GRAPH_EVIDENCE.entityType] || null,
      entityRecordId: f[MAP_HI_GRAPH_EVIDENCE.entityRecordId] || null,
      fieldName: f[MAP_HI_GRAPH_EVIDENCE.fieldName] || null,
      valueObserved: f[MAP_HI_GRAPH_EVIDENCE.valueObserved] || null,
      sourceName: f[MAP_HI_GRAPH_EVIDENCE.sourceName] || null,
      sourceType: f[MAP_HI_GRAPH_EVIDENCE.sourceType] || null,
      sourceUrl: f[MAP_HI_GRAPH_EVIDENCE.sourceUrl] || null,
      sourcePublishedDate: f[MAP_HI_GRAPH_EVIDENCE.sourcePublishedDate] || null,
      retrievedAt: f[MAP_HI_GRAPH_EVIDENCE.retrievedAt] || null,
      evidenceSnippet: f[MAP_HI_GRAPH_EVIDENCE.evidenceSnippet] || null,
      confidence: f[MAP_HI_GRAPH_EVIDENCE.confidence] || null,
      evidenceStrength: f[MAP_HI_GRAPH_EVIDENCE.evidenceStrength] || null,
      current: f[MAP_HI_GRAPH_EVIDENCE.current],
      notes: f[MAP_HI_GRAPH_EVIDENCE.notes] || null,
      source: "HI_GRAPH_EVIDENCE",
    };
  });
}

/**
 * Map HI Graph Evidence → Hotel Explorer Sources tab citation shape
 * (consumed by public/js/hotel-intelligence-workspace.js buildCitations).
 * One citation per unique source URL (canonical evidence document).
 */
export function projectEvidenceToSourcesCitations(evidenceRows = []) {
  const byUrl = new Map();
  for (const row of evidenceRows || []) {
    const url = String(row.sourceUrl || "").trim();
    if (!url) continue;
    const key = url.toLowerCase().replace(/\/+$/, "");
    const claimParts = [];
    if (row.fieldName) claimParts.push(String(row.fieldName));
    if (row.valueObserved) claimParts.push(String(row.valueObserved));
    const claim = claimParts.join(": ") || row.evidenceSnippet || row.sourceName || "Evidence";
    const existing = byUrl.get(key);
    if (!existing) {
      byUrl.set(key, {
        title: row.sourceName || row.sourceUrl || "Source",
        url: row.sourceUrl,
        provider: row.sourceType || "hi_graph_evidence",
        observed_date: row.sourcePublishedDate || row.retrievedAt || null,
        evidence_excerpt: row.evidenceSnippet || row.notes || "",
        claim,
        claims: [claim],
        proves: row.evidenceSnippet || claim,
        does_not:
          row.current === false
            ? "Historical / contextual only — does not by itself establish current ownership."
            : row.notes && /ubo|unresolved|limitation/i.test(String(row.notes))
              ? String(row.notes)
              : "Does not independently prove ultimate beneficial ownership unless explicitly stated.",
        supports: row.current === false ? "history" : "ownership",
        source_type: row.sourceType || "Research Evidence",
        current: row.current !== false,
        airtable_evidence_id: row.airtableId || null,
        evidence_id: row.evidenceId || null,
        confidence: row.confidence || null,
        source: "HI_GRAPH_EVIDENCE",
      });
    } else {
      if (!existing.claims.includes(claim)) existing.claims.push(claim);
      existing.claim = existing.claims.join(" | ");
      existing.proves = existing.claims.join(" | ");
      if (row.current === false) {
        existing.current = false;
        existing.supports = "history";
        existing.does_not =
          "Historical / contextual only — does not by itself establish current ownership.";
      }
    }
  }
  return [...byUrl.values()];
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

export default {
  readHiGraphForHotel,
  listPortfolioForOrganization,
  listEvidenceForHotel,
  projectEvidenceToSourcesCitations,
};
