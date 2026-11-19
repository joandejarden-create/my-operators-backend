/**
 * Golden Demo Ownership Intelligence API (controlled Mexico / GSF cohort).
 * Read-only. No CoStar. No Census writes.
 */

import {
  buildHotelOwnershipReport,
  buildOwnershipGroupProfile,
  buildNeighborsPayload,
  listGoldenDemoCohortIndex,
  GSF_SLUG,
} from "../lib/hotel-intelligence/ownership/golden-demo/gsf-cohort.js";

function sendError(res, status, error, details = {}) {
  return res.status(status).json({ success: false, error, ...details });
}

export async function getGoldenDemoOwnershipIndex(req, res) {
  try {
    return res.json({ success: true, ...listGoldenDemoCohortIndex() });
  } catch (err) {
    console.error("[golden-demo-ownership] index", err);
    return sendError(res, 500, "golden_demo_index_failed");
  }
}

export async function getGoldenDemoHotelOwnership(req, res) {
  try {
    const recordId = String(req.params.recordId || "").trim();
    if (!recordId.startsWith("rec")) {
      return sendError(res, 400, "invalid_airtable_record_id");
    }
    const payload = buildHotelOwnershipReport(recordId);

    // Airtable-first overlay from HI Organizations + HI Hotel–Org Relationships.
    // For out-of-cohort hotels (e.g. Bethesda): graph is the canonical HE source.
    // For golden-demo cohort (KGPV): fixtures remain UX reference; graph attached when present
    // but fixtures are not treated as verified Airtable (Gate 1).
    let airtableGraph = null;
    try {
      const { readHiGraphForHotel } = await import(
        "../lib/hotel-intelligence/research-engine-v1/he-airtable-graph-reader-v1.js"
      );
      airtableGraph = await readHiGraphForHotel(recordId, { includePortfolio: true });
    } catch (graphErr) {
      console.warn("[golden-demo-ownership] hi-graph overlay skipped", graphErr?.message || graphErr);
    }

    if (airtableGraph?.ok && airtableGraph.relationshipCount > 0) {
      const proj = airtableGraph.projection || {};
      const report = payload.report || {};
      const control = report.ownership_and_control || {};
      const nameOf = (org) => org?.tradingName || org?.legalName || null;
      const primaryOrg = airtableGraph.primaryOrganization || proj.operator || proj.brand || null;
      const portfolioByHotel = new Map();
      for (const row of airtableGraph.portfolio || []) {
        const hid = row.airtable_record_id;
        if (!hid) continue;
        const existing = portfolioByHotel.get(hid);
        const rel = row.relationship_type;
        if (!existing) {
          portfolioByHotel.set(hid, {
            hotel: hid === recordId ? payload.hotel?.name || hid : hid,
            name: hid === recordId ? payload.hotel?.name || hid : hid,
            airtable_record_id: hid,
            relationship: rel,
            relationship_type: rel,
            confidence: row.confidence || row.verification_state || "MEDIUM",
            economic_owner_verified: row.economic_owner_verified === true,
            focus_hotel: hid === recordId || row.focus_hotel === true,
            source: "HI_AIRTABLE_GRAPH",
            relationships: [rel],
          });
        } else {
          existing.relationships.push(rel);
          if (row.economic_owner_verified) existing.economic_owner_verified = true;
          // Prefer Operator label when both Brand+Operator exist for same hotel.
          if (/Operator|Management/i.test(String(rel))) {
            existing.relationship = rel;
            existing.relationship_type = rel;
          }
        }
      }
      const portfolioHotels = [...portfolioByHotel.values()];

      payload.airtable_graph = {
        source: "HI_AIRTABLE_GRAPH",
        baseId: airtableGraph.baseId,
        relationshipCount: airtableGraph.relationshipCount,
        control_status: proj.control_status,
        relationships: airtableGraph.relationships,
        organizations: airtableGraph.organizations,
        primaryOrganization: primaryOrg,
        portfolioCount: portfolioHotels.length,
        provenance: {
          class: "LIVE_HI_GRAPH_AIRTABLE",
          verified: true,
          note: "HI Organizations + HI Hotel–Org Relationships on appCCUsuGsE1ifoLk",
        },
      };

      if (!payload.in_cohort) {
        payload.data_status = "AIRTABLE_GRAPH_PARTIAL";
        if (primaryOrg) {
          payload.organization = {
            id: primaryOrg.orgId || primaryOrg.airtableId,
            airtable_id: primaryOrg.airtableId,
            display_name: nameOf(primaryOrg),
            legal_name: primaryOrg.legalName || nameOf(primaryOrg),
            website: primaryOrg.officialWebsite || null,
            jurisdiction: primaryOrg.jurisdiction || null,
            verification_state: primaryOrg.verificationState || null,
            source: "HI_AIRTABLE_GRAPH",
          };
          payload.ownership_group = payload.organization;
        }
        payload.report = {
          ...report,
          executive_summary: {
            text:
              report.executive_summary?.text ||
              `Airtable HI graph relationships available for this hotel (${airtableGraph.relationshipCount}). ` +
                `Operator/brand may be present; economic ownership remains unresolved unless a Validated Economic Owner relationship exists.`,
          },
          ownership_and_control: {
            ...control,
            economic_owner: nameOf(proj.economic_owner)
              ? {
                  name: nameOf(proj.economic_owner),
                  known: true,
                  verification_bucket: proj.economic_owner.confidence || "MEDIUM",
                  source: "HI_AIRTABLE_GRAPH",
                }
              : control.economic_owner || {
                  name: null,
                  known: false,
                  note: "ECONOMIC_OWNER_UNRESOLVED",
                },
            property_company: nameOf(proj.property_company)
              ? {
                  name: nameOf(proj.property_company),
                  known: true,
                  source: "HI_AIRTABLE_GRAPH",
                }
              : control.property_company || null,
            operator: nameOf(proj.operator)
              ? {
                  name: nameOf(proj.operator),
                  known: true,
                  verification_bucket: "HIGH",
                  relationship_type: "OPERATED_BY",
                  source: "HI_AIRTABLE_GRAPH",
                }
              : control.operator || null,
            brand: nameOf(proj.brand)
              ? {
                  name: nameOf(proj.brand),
                  known: true,
                  source: "HI_AIRTABLE_GRAPH",
                }
              : control.brand || null,
            control_status: proj.control_status,
          },
          relationships: airtableGraph.relationships.map((rel) => ({
            relationship_type: rel.relationshipType,
            organization_id: rel.organizationId,
            airtable_id: rel.airtableId,
            verification_state: rel.verificationState,
            confidence: rel.confidence,
            source: "HI_AIRTABLE_GRAPH",
          })),
          organization_and_portfolio: {
            hotels: portfolioHotels,
            source: "HI_AIRTABLE_GRAPH",
            kpis: {
              total_relationships: portfolioHotels.length,
              operated_or_managed: portfolioHotels.filter((h) =>
                /Operator|Management/i.test(String(h.relationship_type || "")),
              ).length,
              owned_or_controlled_verified: portfolioHotels.filter(
                (h) => h.economic_owner_verified,
              ).length,
            },
          },
          research_status: {
            ...(report.research_status || {}),
            airtable_graph: true,
            economic_owner_resolved: Boolean(proj.economic_owner),
          },
        };
      }
    }

    // HPC property contact fallback for Contacts chapter (read-only; no CI write).
    // Also resolve canonical hotel display name for portfolio rows (out-of-cohort).
    try {
      const { resolvePropertyCensusOwnership } = await import(
        "../lib/hotel-intelligence/property-census-identity-v1.js"
      );
      // Reuse brand-presence base via env — ownership route already has Airtable creds in process.
      const Airtable = (await import("airtable")).default;
      const key = process.env.AIRTABLE_API_KEY;
      const baseId = process.env.AIRTABLE_BASE_ID_ALT;
      if (key && baseId && !payload.in_cohort) {
        const atBase = new Airtable({ apiKey: key }).base(baseId);
        const identity = await resolvePropertyCensusOwnership(atBase, recordId);
        const fields = identity.record?.fields || {};
        const hotelName =
          fields.Name ||
          fields["Hotel Name"] ||
          fields["Property Name"] ||
          null;
        if (hotelName) {
          payload.hotel = {
            ...(payload.hotel || {}),
            id: recordId,
            airtable_record_id: recordId,
            name: hotelName,
            source_table: identity.owningTable || null,
            owning_table_kind: identity.owningTableKind || null,
          };
          const portfolioHotels =
            payload.report?.organization_and_portfolio?.hotels || [];
          for (const row of portfolioHotels) {
            if (
              row &&
              (row.airtable_record_id === recordId || row.focus_hotel) &&
              (!row.name || row.name === recordId || row.hotel === recordId)
            ) {
              row.name = hotelName;
              row.hotel = hotelName;
            }
          }
        }
        const phone =
          fields.Phone || fields.Telephone || fields.phone || null;
        const website =
          fields["Official Property URL"] || fields.Website || fields.website || null;
        if (phone || website) {
          const report = payload.report || {};
          payload.report = {
            ...report,
            corporate_contacts: {
              ...(report.corporate_contacts || {}),
              property_phone: phone || report.corporate_contacts?.property_phone || null,
              website: website || report.corporate_contacts?.website || null,
              property_fallback_source: identity.owningTable || "Hotel Property Census",
              source: "HPC_PROPERTY_FALLBACK",
            },
          };
        }
      }
    } catch (contactErr) {
      console.warn(
        "[golden-demo-ownership] HPC contact fallback skipped",
        contactErr?.message || contactErr,
      );
    }

    return res.json({ success: true, ...payload });
  } catch (err) {
    console.error("[golden-demo-ownership] hotel", err);
    return sendError(res, 500, "golden_demo_hotel_failed");
  }
}

export async function getGoldenDemoOwnershipGroup(req, res) {
  try {
    const slug = String(req.params.slug || GSF_SLUG).trim();
    const payload = buildOwnershipGroupProfile(slug);
    if (!payload.ok) return sendError(res, 404, payload.error || "group_not_found");
    return res.json({ success: true, ...payload });
  } catch (err) {
    console.error("[golden-demo-ownership] group", err);
    return sendError(res, 500, "golden_demo_group_failed");
  }
}

/** Focus Graph neighbors — Packet 2 query contract. */
export async function getGoldenDemoOwnershipNeighbors(req, res) {
  try {
    const q = req.query || {};
    const nodeId = String(q.node_id || q.nodeId || q.entity_id || "").trim();
    const nodeType = String(q.node_type || q.nodeType || "ownership_group").trim();
    const hops = Number(q.hops || q.depth || 1);
    if (!nodeId) return sendError(res, 400, "node_id_required");

    const csv = (v) =>
      String(v || "")
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean);

    const payload = buildNeighborsPayload({
      nodeId,
      nodeType,
      hops,
      relationshipTypes: csv(q.relationship_types || q.relationshipTypes),
      relationshipCategories: csv(q.relationship_categories || q.relationshipCategories || q.category),
      currentOnly: q.current_only !== "0" && q.currentOnly !== "0" && q.current_only !== "false",
      minimumConfidence: q.minimum_confidence || q.minimumConfidence || "PROBABLE",
      includeProbable: q.include_probable !== "0" && q.includeProbable !== "0" && q.include_probable !== "false",
      nodeTypes: csv(q.node_types || q.nodeTypes),
      limit: q.limit != null ? Number(q.limit) : undefined,
      expandAll: q.expand_all === "1" || q.expandAll === "1" || q.expand_all === "true",
    });
    if (payload.ok === false) return sendError(res, 404, payload.error || "node_not_found");
    return res.json({ success: true, ...payload });
  } catch (err) {
    console.error("[golden-demo-ownership] neighbors", err);
    return sendError(res, 500, "golden_demo_neighbors_failed");
  }
}
