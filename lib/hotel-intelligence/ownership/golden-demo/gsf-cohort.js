/**
 * Golden Demo — GSF Mexico hotel-relationship cohort (controlled, product-safe).
 * First-party public portfolio + Radar Hotel Census Management Company.
 * No CoStar / GTM licensed fields.
 *
 * Hard rule: OPERATED_BY / management does NOT imply OWNED_BY / PropCo / economic ownership.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { applyRoomsResolutionToPortfolioHotel } from "../../property-fundamentals/index.js";
import {
  isCambridgeHotelId,
  buildCambridgeHotelOwnershipReport,
  buildDovetailOwnershipGroupProfile,
  loadDovetailGoldenDemoCohort,
  DOVETAIL_SLUG,
  CAMBRIDGE_AIRTABLE_ID,
} from "./cambridge-cohort.js";

export const GOLDEN_DEMO_GSF_VERSION = "golden-demo-gsf-cohort-v3";
export const GOLDEN_DEMO_GRAPH_VERSION = "golden-demo-focus-graph-v2";
export const GSF_SLUG = "grupo-hotelero-santa-fe";
export const KGPV_DEEP_RESEARCH_VERSION = "kgpv-deep-research-v2";
export const TEMPORAL_BRAND_STATUSES = Object.freeze([
  "CURRENT",
  "FORMER",
  "ANNOUNCED",
  "PLANNED",
  "CONTESTED",
  "CANCELLED",
  "UNKNOWN",
]);

const REL_CATEGORY = Object.freeze({
  OWNED_BY: "ownership",
  CONTROLLED_BY: "ownership",
  SPONSORED_BY: "ownership",
  OPERATED_BY: "operator",
  ASSET_MANAGED_BY: "operator",
  DEVELOPED_BY: "other",
  BRANDED_BY: "brand",
  LEASED_FROM: "other",
  JV_WITH: "other",
  UNKNOWN: "other",
});

function edgeCategory(relationshipType) {
  return REL_CATEGORY[relationshipType] || "other";
}

function confidenceRank(bucket) {
  const b = String(bucket || "").toUpperCase();
  if (b === "VERIFIED") return 3;
  if (b === "HIGH") return 2;
  if (b === "PROBABLE") return 1;
  return 0;
}

function passesConfidence(bucket, minimumConfidence, includeProbable) {
  const b = String(bucket || "UNKNOWN").toUpperCase();
  const min = String(minimumConfidence || "PROBABLE").toUpperCase();
  if (b === "PROBABLE" && includeProbable === false) return false;
  return confidenceRank(b) >= confidenceRank(min === "PROBABLE" && includeProbable === false ? "HIGH" : min);
}

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE = path.resolve(
  __dirname,
  "../../../../fixtures/golden-demo/gsf-mexico-ownership-cohort-v1.json"
);
const KGPV_DEEP_RESEARCH = path.resolve(
  __dirname,
  "../../../../fixtures/golden-demo/krystal-grand-pv-deep-research-v1.json"
);

let cached = null;
let cachedDeepResearch = null;

export function loadGsfGoldenDemoCohort() {
  if (cached) return cached;
  const raw = JSON.parse(fs.readFileSync(FIXTURE, "utf8"));
  cached = raw;
  return raw;
}

export function loadKgpvDeepResearch() {
  if (cachedDeepResearch) return cachedDeepResearch;
  if (!fs.existsSync(KGPV_DEEP_RESEARCH)) return null;
  cachedDeepResearch = JSON.parse(fs.readFileSync(KGPV_DEEP_RESEARCH, "utf8"));
  return cachedDeepResearch;
}

export function clearGsfCohortCache() {
  cached = null;
  cachedDeepResearch = null;
}

function orgOf(cohort) {
  return cohort.organization || cohort.owner;
}

function verificationBucket(status) {
  const s = String(status || "").toLowerCase();
  if (s === "verified") return "VERIFIED";
  if (s === "high") return "HIGH";
  if (s === "probable" || s === "needs_review") return "PROBABLE";
  return "UNKNOWN";
}

/** Resolve hotel by Radar Hotel Census id, Property Census id, or DHL hotel_id. */
export function findCohortHotel(cohort, id) {
  const key = String(id || "").trim();
  if (!key) return null;
  return (
    (cohort.hotels || []).find(
      (h) =>
        h.airtable_record_id === key ||
        h.property_census_record_id === key ||
        h.hotel_id === key
    ) || null
  );
}

function brandMeta(hotel, deep = null) {
  if (deep && deep.brand_resolution) {
    return {
      brand: deep.brand_resolution.current_trading_brand || hotel.brand,
      status: deep.brand_resolution.current_status || "CURRENT",
      note: deep.brand_resolution.resolution || hotel.identity_notes || null,
    };
  }
  const affiliation = String(hotel.affiliation_display || hotel.brand || "").trim();
  const notes = hotel.identity_notes || null;
  if (affiliation && /breathless/i.test(affiliation) && /krystal/i.test(hotel.name || "")) {
    return {
      brand: affiliation,
      status: "CONTESTED",
      note:
        notes ||
        "Census Affiliation is Breathless while trading name remains Krystal Grand; public GSF/Krystal sites still market Krystal Grand. Treat as contested/transitional — not auto-verified current brand.",
    };
  }
  if (affiliation && !/^independent$/i.test(affiliation)) {
    return { brand: affiliation, status: "LIVE", note: null };
  }
  const n = String(hotel.name || "");
  if (/krystal/i.test(n)) {
    return {
      brand: "Krystal",
      status: "PREVIEW",
      note: "Name-derived display brand when Census Affiliation is Independent/empty.",
    };
  }
  if (/hyatt/i.test(n)) {
    return {
      brand: "Hyatt",
      status: "PREVIEW",
      note: "Name-derived display brand when Census Affiliation is Independent/empty.",
    };
  }
  if (/mahekal/i.test(n)) {
    return {
      brand: "Mahekal",
      status: "PREVIEW",
      note: "Name-derived display brand when Census Affiliation is Independent/empty.",
    };
  }
  return { brand: null, status: "UNKNOWN", note: null };
}

function buildEvidencePack(cohort, hotel, claim) {
  const src = cohort.source || {};
  const org = orgOf(cohort);
  const rel = hotel.relationship_type || "OPERATED_BY";
  return {
    claim,
    relationship: {
      subject: hotel.name,
      subject_hotel_id: hotel.hotel_id,
      relationship_type: rel,
      object: org.display_name,
      object_entity_id: org.entity_id,
    },
    verification_bucket: verificationBucket(hotel.verification_status),
    source_count: 1,
    sources: [
      {
        provider: src.provider,
        title: src.title,
        url: src.url,
        published_date: src.published_date,
        observed_date: src.observed_date,
        evidence_excerpt:
          `${hotel.name}: Radar Hotel Census Management Company = ${hotel.management_company || org.display_name}; also listed on GSF first-party hotel pages.`,
        authority: src.authority,
        confidence: src.confidence || "high_for_management_relationship",
      },
    ],
    what_it_proves:
      "Supports a current hotel → OPERATED_BY / management relationship with Grupo Hotelero Santa Fe (Census Management Company + first-party GSF hotel listing).",
    what_it_does_not_prove:
      "Does not establish OWNED_BY, CONTROLLED_BY, SPONSORED_BY, economic ownership, PropCo title, or beneficial ownership. Operator ≠ owner.",
  };
}

function relationshipSummary(cohort) {
  const hotels = cohort.hotels || [];
  const operated = hotels.filter(
    (h) =>
      h.relationship_type === "OPERATED_BY" ||
      h.secondary_relationship_type === "OPERATED_BY" ||
      h.relationship_type === "ASSET_MANAGED_BY"
  ).length;
  const owned = hotels.filter((h) =>
    ["OWNED_BY", "CONTROLLED_BY"].includes(h.relationship_type)
  ).length;
  const sponsored = hotels.filter((h) => h.relationship_type === "SPONSORED_BY").length;
  const other = Math.max(0, hotels.length - Math.max(operated, owned) - sponsored);
  const ownershipUnknown = hotels.filter((h) => h.economic_owner_verified !== true).length;
  return {
    total_relationships: hotels.length,
    owned_or_controlled_verified: owned,
    sponsored: sponsored,
    operated_or_managed: operated,
    other,
    ownership_unknown: ownershipUnknown,
  };
}

/**
 * Focus Graph neighbors — layout-agnostic nodes/edges + evidence metadata.
 */
export function buildNeighborsPayload(opts = {}) {
  const cohort = loadGsfGoldenDemoCohort();
  const org = orgOf(cohort);
  const nodeId = String(opts.nodeId || opts.entity_id || "").trim();
  let nodeType = String(opts.nodeType || opts.node_type || "organization").trim();
  if (nodeType === "ownership_group") nodeType = "organization";
  const hopsN = Math.max(1, Math.min(2, Number(opts.hops ?? opts.depth ?? 1) || 1));
  const currentOnly = opts.currentOnly !== false && opts.current_only !== false;
  const includeProbable = opts.includeProbable !== false && opts.include_probable !== false;
  const minimumConfidence = opts.minimumConfidence || opts.minimum_confidence || "PROBABLE";
  const relTypeFilter = new Set(
    []
      .concat(opts.relationshipTypes || opts.relationship_types || [])
      .map(String)
      .filter(Boolean)
  );
  const categoryFilter = new Set(
    []
      .concat(opts.relationshipCategories || opts.relationship_categories || [])
      .map((c) => String(c).toLowerCase())
      .filter(Boolean)
  );
  const nodeTypeFilter = new Set(
    []
      .concat(opts.nodeTypes || opts.node_types || [])
      .map(String)
      .filter(Boolean)
  );
  const expandAll = opts.expandAll === true || opts.expand_all === true;
  const defaultLimit = nodeType === "hotel" ? 50 : 6;
  const limit = expandAll
    ? 500
    : Math.max(1, Number(opts.limit != null ? opts.limit : defaultLimit) || defaultLimit);

  function allowEdge(edge) {
    if (currentOnly && edge.is_current === false) return false;
    if (!passesConfidence(edge.verification_bucket, minimumConfidence, includeProbable)) return false;
    if (relTypeFilter.size && !relTypeFilter.has(edge.relationship_type)) return false;
    if (categoryFilter.size && !categoryFilter.has(edge.category)) return false;
    return true;
  }

  function makeOrgHotelEdge(hotel) {
    const rel = hotel.relationship_type || "OPERATED_BY";
    const evidence = buildEvidencePack(
      cohort,
      hotel,
      `${hotel.name} → ${rel} → ${org.display_name}`
    );
    const bucket = verificationBucket(hotel.verification_status);
    const primary = {
      relationship_id: `rel_${hotel.hotel_id}_${org.entity_id}_${rel}`,
      from: hotel.hotel_id,
      to: org.entity_id,
      relationship_type: rel,
      relationship_label: rel.replace(/_/g, " "),
      category: edgeCategory(rel),
      verification_status: hotel.verification_status,
      verification_bucket: bucket,
      is_current: hotel.is_current !== false,
      temporal_status: hotel.is_current === false ? "historical" : "current",
      evidence_count: evidence.source_count,
      observed_date: cohort.source?.observed_date || null,
      evidence,
      data_status: cohort.data_status,
      does_not_imply_ownership: rel === "OPERATED_BY" || rel === "ASSET_MANAGED_BY",
    };
    const edges = [primary];
    if (hotel.secondary_relationship_type && hotel.secondary_relationship_type !== rel) {
      const sec = hotel.secondary_relationship_type;
      edges.push({
        ...primary,
        relationship_id: `rel_${hotel.hotel_id}_${org.entity_id}_${sec}`,
        relationship_type: sec,
        relationship_label: sec.replace(/_/g, " "),
        category: edgeCategory(sec),
        does_not_imply_ownership: sec === "OPERATED_BY" || sec === "ASSET_MANAGED_BY",
        evidence: {
          ...evidence,
          claim: `${hotel.name} → ${sec} → ${org.display_name}`,
          relationship: { ...evidence.relationship, relationship_type: sec },
        },
      });
    }
    return edges;
  }

  function makeBrandEdge(hotel) {
    const b = brandMeta(hotel);
    if (!b.brand) return null;
    const brandId = `brand_preview_${b.brand.toLowerCase().replace(/\s+/g, "_").replace(/&/g, "and")}`;
    return {
      relationship_id: `rel_${hotel.hotel_id}_${brandId}_BRANDED_BY`,
      from: hotel.hotel_id,
      to: brandId,
      relationship_type: "BRANDED_BY",
      relationship_label: "BRANDED BY",
      category: "brand",
      verification_status: b.status === "LIVE" ? "verified" : "preview",
      verification_bucket: b.status === "LIVE" ? "HIGH" : "PROBABLE",
      is_current: b.status !== "CONTESTED",
      temporal_status: b.status === "CONTESTED" ? "contested" : "current",
      evidence_count: 0,
      observed_date: null,
      evidence: {
        claim: `${hotel.name} → BRANDED_BY → ${b.brand} (${b.status})`,
        relationship: {
          subject: hotel.name,
          relationship_type: "BRANDED_BY",
          object: b.brand,
        },
        verification_bucket: b.status === "LIVE" ? "HIGH" : "PROBABLE",
        source_count: 0,
        sources: [],
        what_it_proves:
          b.status === "CONTESTED"
            ? "Surfaces a contested Census affiliation for diligence — not a verified operating brand claim."
            : b.status === "LIVE"
              ? "Census Affiliation / brand field for display."
              : "Display-only brand label when Census Affiliation is Independent/empty.",
        what_it_does_not_prove:
          "Does not by itself prove franchise contract terms, reflag completion, or ownership.",
        data_status: b.status === "LIVE" ? cohort.data_status : "PREVIEW",
      },
      data_status: b.status === "LIVE" ? cohort.data_status : "PREVIEW",
      brand_meta: b,
      brand_id: brandId,
    };
  }

  function hotelNode(hotel) {
    const b = brandMeta(hotel);
    return {
      id: hotel.hotel_id,
      type: "hotel",
      label: hotel.name,
      airtable_record_id: hotel.airtable_record_id,
      city: hotel.city || null,
      market: hotel.market || null,
      rooms: hotel.rooms ?? null,
      chain_scale: hotel.chain_scale || null,
      brand: b.brand,
      brand_status: b.status,
      operator: hotel.operator || hotel.management_company || org.display_name,
      economic_owner: null,
      economic_owner_status: "NOT_YET_VERIFIED",
      organization_id: org.entity_id,
      organization_slug: org.slug,
      organization_label: org.display_name,
      // backward-compat aliases (UI may still read these — values are organization, not owner)
      ownership_group: null,
      ownership_group_slug: org.slug,
      ownership_group_id: org.entity_id,
      verification_bucket: verificationBucket(hotel.verification_status),
      primary_relationship_to_org: hotel.relationship_type || "OPERATED_BY",
    };
  }

  function organizationNode() {
    return {
      id: org.entity_id,
      type: "organization",
      // legacy alias for older clients
      legacy_type: "ownership_group",
      roles: org.organization_roles || ["operator", "manager"],
      not_asserted_as: org.not_asserted_as || ["verified_economic_owner", "propco"],
      label: org.display_name,
      legal_name: org.legal_name,
      slug: org.slug,
      website: org.website,
      known_hotel_relationship_count: (cohort.hotels || []).length,
      entity_type: org.entity_type,
    };
  }

  const nodesById = new Map();
  const edges = [];
  let truncated = false;
  let totalHotelRelationships = 0;

  function addNode(n) {
    if (!n?.id) return;
    if (nodeTypeFilter.size && !nodeTypeFilter.has(n.type) && n.id !== nodeId) return;
    if (!nodesById.has(n.id)) nodesById.set(n.id, n);
  }

  function addEdge(e) {
    if (Array.isArray(e)) {
      let any = false;
      for (const edge of e) {
        if (addEdge(edge)) any = true;
      }
      return any;
    }
    if (!e || !allowEdge(e)) return false;
    edges.push(e);
    return true;
  }

  if (nodeType === "hotel" || nodeType === "brand") {
    if (nodeType === "brand") {
      const brandKey = nodeId.replace(/^brand_preview_/, "").replace(/_/g, " ").replace(/and/g, "&");
      const matched = (cohort.hotels || []).filter((h) => {
        const b = brandMeta(h);
        return String(b.brand || "").toLowerCase() === brandKey.toLowerCase();
      });
      if (!matched.length) {
        return { ok: false, error: "node_not_found", version: GOLDEN_DEMO_GRAPH_VERSION };
      }
      addNode({
        id: nodeId.startsWith("brand_preview_")
          ? nodeId
          : `brand_preview_${brandKey.toLowerCase().replace(/\s+/g, "_")}`,
        type: "brand",
        label: brandMeta(matched[0]).brand,
        data_status: "PREVIEW",
      });
      addNode(organizationNode());
      for (const h of matched) {
        addNode(hotelNode(h));
        addEdge(makeOrgHotelEdge(h));
        const be = makeBrandEdge(h);
        if (be && addEdge(be)) {
          addNode({
            id: be.brand_id,
            type: "brand",
            label: be.brand_meta.brand,
            data_status: be.data_status,
          });
        }
      }
    } else {
      const hotel = findCohortHotel(cohort, nodeId);
      if (!hotel) {
        return {
          ok: true,
          empty: true,
          version: GOLDEN_DEMO_GRAPH_VERSION,
          data_status: "UNKNOWN",
          center: { id: nodeId, type: "hotel", label: "Unknown hotel" },
          hops: hopsN,
          nodes: [],
          edges: [],
          message: "No hotel-relationship graph edges in the controlled demo cohort for this hotel.",
        };
      }
      addNode(hotelNode(hotel));
      addNode(organizationNode());
      addEdge(makeOrgHotelEdge(hotel));
      const be = makeBrandEdge(hotel);
      if (be && addEdge(be)) {
        addNode({
          id: be.brand_id,
          type: "brand",
          label: be.brand_meta.brand,
          data_status: be.data_status,
        });
      }
      if (hopsN >= 2) {
        const siblings = (cohort.hotels || []).filter((h) => h.hotel_id !== hotel.hotel_id);
        let shown = 0;
        for (const s of siblings) {
          if (shown >= limit) {
            truncated = true;
            break;
          }
          addNode(hotelNode(s));
          if (addEdge(makeOrgHotelEdge(s))) shown += 1;
        }
        totalHotelRelationships = siblings.length;
      }
    }
  } else {
    const isOrg =
      nodeId === org.entity_id ||
      nodeId === org.slug ||
      nodeId === GSF_SLUG ||
      !nodeId;
    if (!isOrg && nodeId) {
      return { ok: false, error: "node_not_found", version: GOLDEN_DEMO_GRAPH_VERSION };
    }
    addNode(organizationNode());
    const hotels = cohort.hotels || [];
    totalHotelRelationships = hotels.length;
    let shown = 0;
    for (const h of hotels) {
      if (shown >= limit) {
        truncated = true;
        break;
      }
      addNode(hotelNode(h));
      if (addEdge(makeOrgHotelEdge(h))) {
        shown += 1;
        const be = makeBrandEdge(h);
        if (be && addEdge(be)) {
          addNode({
            id: be.brand_id,
            type: "brand",
            label: be.brand_meta.brand,
            data_status: be.data_status,
          });
        }
      }
    }
  }

  const cleanEdges = edges.map(({ brand_meta, brand_id, ...rest }) => rest);
  const centerHotel = findCohortHotel(cohort, nodeId);

  return {
    ok: true,
    empty: cleanEdges.length === 0,
    version: GOLDEN_DEMO_GRAPH_VERSION,
    data_status: cohort.data_status,
    costar_firewall: "enforced",
    semantics_note:
      "Edge labels are authoritative. OPERATED_BY must not be read as OWNED_BY.",
    center: {
      id:
        nodeType === "hotel"
          ? centerHotel?.hotel_id || nodeId
          : org.entity_id,
      type: nodeType === "brand" ? "brand" : nodeType === "hotel" ? "hotel" : "organization",
      label:
        nodeType === "hotel"
          ? centerHotel?.name || nodeId
          : org.display_name,
    },
    hops: hopsN,
    nodes: [...nodesById.values()],
    edges: cleanEdges,
    pagination: {
      truncated,
      limit,
      expand_all: expandAll,
      total_hotel_relationships: totalHotelRelationships,
      shown_hotel_relationships: cleanEdges.filter((e) =>
        ["OPERATED_BY", "OWNED_BY", "SPONSORED_BY", "CONTROLLED_BY", "ASSET_MANAGED_BY"].includes(
          e.relationship_type
        )
      ).length,
    },
    query: {
      entity_id: nodeId || org.entity_id,
      depth: hopsN,
      relationship_types: [...relTypeFilter],
      relationship_categories: [...categoryFilter],
      current_only: currentOnly,
      minimum_confidence: minimumConfidence,
      include_probable: includeProbable,
      node_types: [...nodeTypeFilter],
    },
    filters_supported: [
      "relationship_types",
      "relationship_categories",
      "current_only",
      "minimum_confidence",
      "include_probable",
      "node_types",
      "limit",
      "expand_all",
      "depth",
    ],
    opportunity_query_ready: true,
    opportunity_query_note:
      "Filter shapes support future questions without layout coupling. Full Opportunity Graph not implemented.",
  };
}

export function buildHotelOwnershipReport(airtableRecordId) {
  if (isCambridgeHotelId(airtableRecordId)) {
    return buildCambridgeHotelOwnershipReport(airtableRecordId);
  }

  const cohort = loadGsfGoldenDemoCohort();
  const org = orgOf(cohort);
  const hotel = findCohortHotel(cohort, airtableRecordId);
  if (!hotel) {
    return {
      ok: true,
      in_cohort: false,
      intelligence_case: "C",
      data_status: "UNKNOWN",
      organization: null,
      ownership_group: null,
      report: {
        case: "C",
        executive_summary: {
          text:
            "Ownership Intelligence has no controlled-demo relationship for this hotel yet. Economic owner is not verified. No fabricated ownership is shown.",
          verification_bucket: "UNKNOWN",
          source_count: 0,
          data_status: "UNKNOWN",
        },
        ownership_and_control: {
          economic_owner_or_group: {
            name: null,
            known: false,
            status: "NOT_YET_VERIFIED",
            note: "Owner: Not yet verified",
          },
          legal_property_owner_propco: {
            name: null,
            known: false,
            status: "UNKNOWN",
            note: "PropCo / legal title unknown.",
          },
          operator: { name: null, known: false, status: "UNKNOWN" },
          brand: { name: null, known: false, status: "UNKNOWN" },
        },
        research_status: {
          message: "Research status — evidence gaps; available contextual intelligence only.",
        },
      },
    };
  }

  const deep =
    hotel.airtable_record_id === "recUNycnMwOVFX0hc" || hotel.hotel_id === "dhl_06G6AB2228339A12S2EZ7PZF5E"
      ? loadKgpvDeepResearch()
      : null;
  const brandDisplay = brandMeta(hotel, deep);
  const owned = hotel.economic_owner_verified === true || (deep && deep.ownership_chain);
  const evidence = deep
    ? {
        claim: `${hotel.name} → OWNED_BY + OPERATED_BY → ${org.display_name}`,
        relationship: {
          subject: hotel.name,
          subject_hotel_id: hotel.hotel_id,
          relationship_type: hotel.relationship_type || "OWNED_BY",
          object: org.display_name,
          object_entity_id: org.entity_id,
        },
        verification_bucket: "HIGH",
        source_count: (deep.sources || []).length,
        sources: (deep.sources || []).map((s) => ({
          provider: s.provider,
          title: s.title,
          url: s.url,
          published_date: null,
          observed_date: s.observed_date || deep.observed_date,
          evidence_excerpt: s.note || s.title,
          authority: s.authority,
          confidence: "high",
        })),
        what_it_proves:
          "Primary GSF securities disclosures support GSF ownership and operation of the former Hilton Puerto Vallarta asset now trading as Krystal Grand Puerto Vallarta (451 rooms / Hacienda).",
        what_it_does_not_prove:
          "Does not prove natural-person UBO beyond disclosed principals; does not prove Breathless is a completed reflag of this hotel; does not collapse adjacent Chartwell-owned Krystal Resort PV into this asset.",
      }
    : buildEvidencePack(
        cohort,
        hotel,
        `${hotel.name} → ${hotel.relationship_type || "OPERATED_BY"} → ${org.display_name}`
      );
  const bucket = verificationBucket(hotel.verification_status);
  const operatorName = hotel.operator || hotel.management_company || org.display_name;
  const intelligenceCase = owned ? "A" : "B";

  const orgPayload = {
    slug: org.slug,
    entity_id: org.entity_id,
    display_name: org.display_name,
    legal_name: org.legal_name,
    website: org.website,
    known_hotel_count: (cohort.hotels || []).length,
    roles: owned
      ? ["owner", "operator", "manager", "brand"]
      : org.organization_roles || ["operator", "manager"],
    not_asserted_as: owned
      ? ["natural_person_ubo"]
      : org.not_asserted_as || ["verified_economic_owner", "propco"],
    ...(deep && deep.corporate_contacts
      ? {
          headquarters: deep.corporate_contacts.headquarters,
          phone: deep.corporate_contacts.corporate_phone,
          email: deep.corporate_contacts.business_email,
        }
      : {}),
  };

  const propco =
    deep &&
    (deep.ownership_chain || []).find((n) => n.role === "propco");
  const econ =
    deep &&
    (deep.ownership_chain || []).find((n) => n.role === "economic_owner");

  return {
    ok: true,
    in_cohort: true,
    intelligence_case: intelligenceCase,
    version: GOLDEN_DEMO_GSF_VERSION,
    deep_research_version: deep ? deep.version : null,
    data_status: deep ? "DEEP_RESEARCH_PARTIAL" : cohort.data_status,
    costar_firewall: "enforced",
    hotel: {
      ...hotel,
      brand_display: brandDisplay.brand,
      brand_display_status: brandDisplay.status,
      brand_display_note: brandDisplay.note,
    },
    organization: orgPayload,
    ownership_group: {
      ...orgPayload,
      framing: owned
        ? "organization_owner_operator"
        : "organization_operator_not_verified_owner",
    },
    deep_research: deep || null,
    report: {
      case: intelligenceCase,
      case_label: owned
        ? "Strong ownership intelligence — owned and operated"
        : "Partial intelligence — operator known; ownership unresolved",
      executive_summary: {
        text: owned
          ? `Economic Owner: ${econ?.name || org.display_name} (HIGH). ` +
            `Legal PropCo / property vehicle: ${propco?.name || "IHVSF (HIGH)"}. ` +
            `Operator / Management: ${operatorName} (HIGH) — owned-and-operated. ` +
            `Brand: ${brandDisplay.brand} (${brandDisplay.status}). ` +
            `Identity note: this is the former Hilton Puerto Vallarta (opened 2012; Hacienda 2018; Altitude→Grand from 2022), distinct from adjacent Chartwell-owned Krystal Resort Puerto Vallarta. ` +
            `Breathless Puerto Vallarta remains an ANNOUNCED 2025 Hyatt+GSF project — not treated as completed reflag of this hotel.`
          : `Economic Owner: Not yet verified. Legal PropCo: Unknown. ` +
            `Operator / Management Company: ${operatorName} (${bucket}) via ${hotel.relationship_type || "OPERATED_BY"}. ` +
            (brandDisplay.brand
              ? `Brand: ${brandDisplay.brand} (${brandDisplay.status}). `
              : "") +
            `Dealality distinguishes the company that operates a hotel from the entity that economically owns it. ` +
            `When ownership is unverified, we say so instead of treating the operator as the owner.`,
        verification_bucket: bucket,
        source_count: evidence.source_count,
        data_status: deep ? "DEEP_RESEARCH_PARTIAL" : cohort.data_status,
        what_we_know: owned
          ? [
              `${econ?.name || org.display_name} is economic owner and operator (owned-and-operated)`,
              `Property held through ${propco?.name || "a dedicated property company"}`,
              `Current brand display: ${brandDisplay.brand} (${brandDisplay.status})`,
              "Former brand identities and announced brand projects are tracked separately",
            ]
          : [
              `${operatorName} is supported as operator / management company`,
              "Economic owner is not yet verified from product-safe evidence",
            ],
        what_remains_uncertain: owned
          ? [
              "Natural-person beneficial ownership",
              "Legal / franchise signing authority",
              "Completed vs announced brand conversion status where applicable",
            ]
          : ["Economic owner", "Property-holding company", "Signing authority"],
        why_this_hotel_matters: owned
          ? "Listed-company owner-operator archetype with PropCo filings and brand chronology."
          : "Operator-known / ownership-unresolved diligence pattern.",
      },
      ownership_and_control: {
        economic_owner_or_group: owned
          ? {
              name: econ?.name || org.display_name,
              known: true,
              status: "HIGH",
              note: econ?.note || "GSF is economic owner of this hotel per securities disclosures.",
            }
          : {
              name: null,
              known: false,
              status: "NOT_YET_VERIFIED",
              note: "Owner: Not yet verified — Management Company is not treated as economic owner.",
            },
        legal_property_owner_propco: owned
          ? {
              name: propco?.name || null,
              known: Boolean(propco?.name),
              status: propco?.confidence || "HIGH",
              note: propco?.note || null,
            }
          : {
              name: null,
              known: false,
              status: "UNKNOWN",
              note: "PropCo / legal title not yet verified from public evidence.",
            },
        registered_operating_company: {
          name: owned ? org.legal_name : null,
          known: owned,
          status: owned ? "HIGH" : "UNKNOWN",
        },
        parent_sponsor: {
          name: owned ? org.display_name : null,
          known: owned,
          status: owned ? "HIGH" : "UNKNOWN",
          note: owned
            ? "Listed public company; Chartwell is related major shareholder / former co-owner."
            : null,
        },
        operator: {
          name: operatorName,
          known: true,
          verification_bucket: bucket,
          relationship_type: hotel.secondary_relationship_type || hotel.relationship_type || "OPERATED_BY",
          note: owned
            ? "Owned-and-operated by GSF."
            : "Supported by Radar Hotel Census Management Company and GSF first-party hotel listing.",
        },
        brand: {
          name: brandDisplay.brand,
          known: Boolean(brandDisplay.brand),
          status: brandDisplay.status,
          note: brandDisplay.note,
        },
        developer: {
          name: owned ? "Grupo Hotelero Santa Fe with Grupo Chartwell (development JV history)" : null,
          known: owned,
          status: owned ? "HIGH" : "UNKNOWN",
        },
        ownership_structure: {
          structure: owned ? "Public-company owned hotel via property subsidiary (IHVSF)" : null,
          known: owned,
          status: owned ? "HIGH" : "UNKNOWN",
          note: owned
            ? "Former Chartwell co-ownership ended 2014 when GSF acquired remaining 50%."
            : "Ownership structure not asserted from operator evidence alone.",
        },
        lease_lessor: {
          name: deep
            ? "Promotora Turística Mexicana, S.A. de C.V. (adjacent event-salon parcels / related party)"
            : null,
          known: Boolean(deep),
          status: deep ? "HIGH" : "UNKNOWN",
          note: deep
            ? "Lease of specific adjacent parcels to IHVSF — not the full hotel freehold lessor."
            : null,
        },
      },
      ownership_chain: deep?.ownership_chain || null,
      decision_authority: deep
        ? {
            status: "PARTIAL",
            message:
              "Named people verified for title/organization; decision and legal signing authority remain independent claims — Authority Not Verified unless explicit evidence",
            people: (deep.people || []).map((p) => ({
              name: p.name,
              title: p.title,
              organization: p.organization,
              role_category: p.decision_authority_category || p.role_category || p.category,
              category: p.decision_authority_category || p.role_category || p.category,
              decision_authority_category: p.decision_authority_category || p.role_category || p.category,
              strategic_relevance: p.strategic_relevance || p.relationship_to_hotel,
              relationship_to_hotel: p.relationship_to_hotel,
              person_verified: p.person_verified !== false,
              title_verified: p.title_verified !== false,
              organization_verified: p.organization_verified !== false,
              decision_authority: p.decision_authority || "Authority Not Verified",
              legal_signing_authority: p.legal_signing_authority || "Authority Not Verified",
              contact_verified: Boolean(p.contact_verified),
              contact: p.contact || null,
              confidence: p.confidence,
              authority_note: p.authority_note,
              sources: p.sources || [],
              professional_profile: p.professional_profile || null,
              professional_profile_url: p.professional_profile_url || (p.professional_profile && p.professional_profile.url) || "",
              professional_profile_type:
                p.professional_profile_type || (p.professional_profile && p.professional_profile.type) || "",
              professional_profile_verified:
                p.professional_profile_verified === true ||
                (p.professional_profile && p.professional_profile.verified === true),
            })),
            roles: (deep.people || []).map((p) => ({
              role: p.role_category || p.category,
              name: p.name,
              title: p.title,
              organization: p.organization,
              status: p.confidence,
              decision_authority: p.decision_authority || "Authority Not Verified",
              legal_signing_authority: p.legal_signing_authority || "Authority Not Verified",
              note: p.authority_note,
            })),
          }
        : {
            status: "NOT_YET_VERIFIED",
            message: "Decision authority not yet verified",
            roles: [
              { role: "Principal / Beneficial Owner", status: "NOT_YET_VERIFIED" },
              { role: "Franchise Decision Authority", status: "NOT_YET_VERIFIED" },
              { role: "Legal Signatory", status: "NOT_YET_VERIFIED" },
              { role: "Development Decision Maker", status: "NOT_YET_VERIFIED" },
              { role: "Asset Management Decision Maker", status: "NOT_YET_VERIFIED" },
              { role: "Property-Level Influencer", status: "NOT_YET_VERIFIED" },
            ],
          },
      evidence,
      brand_chronology: deep?.brand_chronology || null,
      brand_resolution: deep
        ? {
            ...deep.brand_resolution,
            census_claim_preserved: deep.census_claim_preserved || null,
            temporal_statuses_supported: TEMPORAL_BRAND_STATUSES,
          }
        : null,
      organizations: deep?.organizations || null,
      relationships: deep?.relationships || null,
      corporate_contacts: deep?.corporate_contacts || null,
      research_gaps: deep?.research_gaps || null,
      commercial_pursuit: deep?.commercial_pursuit || null,
      product_truth_set_ref: deep?.product_truth_set_ref || null,
      observations: [
        {
          type: owned ? "owned_and_operated" : "management_company",
          text: owned
            ? `GSF securities disclosures support ownership and operation of ${hotel.name} (former Hilton Puerto Vallarta).`
            : `Radar Hotel Census lists Management Company = ${operatorName}.`,
          data_status: deep ? "DEEP_RESEARCH_PARTIAL" : cohort.data_status,
          promoted_to_edge: true,
        },
        {
          type: "identity_disambiguation",
          text:
            deep?.identity_disambiguation?.note ||
            "Distinguish Krystal Grand PV from adjacent Chartwell-owned Krystal Resort PV when present.",
          data_status: deep ? "DEEP_RESEARCH_PARTIAL" : cohort.data_status,
          promoted_to_edge: false,
        },
        ...(brandDisplay.status === "CONTESTED"
          ? [
              {
                type: "contested_brand_affiliation",
                text: brandDisplay.note,
                data_status: cohort.data_status,
                promoted_to_edge: false,
              },
            ]
          : []),
      ],
      property_history: deep?.property_history || (brandDisplay.status === "CONTESTED"
        ? [
            {
              theme: "brand_history_candidate",
              text:
                "Canonical trading name remains Krystal Grand Puerto Vallarta while Census Affiliation shows Breathless (Hyatt). Strong candidate for Property History: current display name vs historical alias vs announced reflag / new build.",
            },
          ]
        : []),
      strategic_development: {
        portfolio_leverage: {
          signal: true,
          text: deep?.commercial_pursuit?.portfolio_leverage ||
            `${org.display_name} is linked to ${(cohort.hotels || []).length} Census-matched hotel relationships across multiple Mexican markets.`,
        },
        brand_exposure: {
          signal: true,
          text: owned
            ? "Current Krystal Grand branding on an owned Pacific resort; separate Hyatt Breathless PV collaboration announced for 2025."
            : "Relationships include Krystal-named assets, Hyatt Regency, Mahekal, and a contested Breathless Census affiliation on Krystal Grand Puerto Vallarta.",
        },
        brand_whitespace: {
          signal: false,
          text: "Whitespace scoring not assigned for this controlled demo.",
        },
        operator_exposure: {
          signal: true,
          text: owned
            ? "GSF is evidenced as both economic owner and operator for this hotel."
            : "GSF is evidenced as operator / management company — not as verified economic owner for these assets.",
        },
        independent_assets: {
          signal: true,
          text: "Mahekal Beach Resort appears alongside Krystal/Hyatt assets under GSF management.",
        },
        conversion_development_signal: {
          signal: true,
          text: deep?.commercial_pursuit?.timing_signals ||
            "Hyatt announced Breathless Puerto Vallarta with GSF; do not auto-upgrade brand without site confirmation.",
        },
        multi_asset_potential: {
          signal: true,
          text: "Adjacent Chartwell-owned Krystal Resort PV under GSF management creates same-node portfolio leverage without merging ownership.",
        },
      },
      why_this_matters: {
        text:
          deep?.commercial_pursuit?.why_matters ||
          `Dealality models real commercial relationships: operator, brand, and owner are separate. ` +
            `${org.display_name} is a multi-market Mexico operator/manager in this demo. ` +
            `When ownership is unverified, the product says so — that accuracy is the differentiator.`,
      },
      deep_research: {
        available: Boolean(deep),
        button_label: "Deep Research",
        status: deep ? "LOADED_PARTIAL" : "COMING_SOON",
        note: deep
          ? "Primary filings enrichment loaded — Webhound session may still add residual people/UBO evidence."
          : "Dossier shell reserved.",
        webhound_session_id: deep?.webhound_session_id || null,
      },
    },
  };
}

export function buildOwnershipGroupProfile(slug) {
  if (String(slug || "").trim() === DOVETAIL_SLUG) {
    return buildDovetailOwnershipGroupProfile(slug);
  }

  const cohort = loadGsfGoldenDemoCohort();
  const org = orgOf(cohort);
  if (String(slug || "") !== org.slug && String(slug || "") !== GSF_SLUG) {
    return { ok: false, error: "group_not_found" };
  }

  const summary = relationshipSummary(cohort);
  const hotels = (cohort.hotels || []).map((h) => {
    const b = brandMeta(h);
    // Global fundamentals resolver: positive Census/cohort rooms win; 0/null do not block fallback.
    const resolved = applyRoomsResolutionToPortfolioHotel({
      ...h,
      name: h.name,
      rooms: h.rooms,
      census_rooms: h.census_rooms ?? (h.rooms != null && Number(h.rooms) > 0 ? h.rooms : null),
    });
    return {
      ...h,
      ...resolved,
      brand_display: b.brand,
      brand_display_status: b.status,
      market_display: h.market || (h.city ? `${h.city} (city)` : "Unknown market"),
      verification_bucket: verificationBucket(h.verification_status),
      rooms_display: resolved.rooms_display ?? resolved.rooms ?? null,
      rooms_status: resolved.rooms_status || (resolved.rooms != null ? "LIVE" : "UNKNOWN"),
      economic_owner_status: h.economic_owner_verified ? "VERIFIED" : "NOT_YET_VERIFIED",
    };
  });

  const byBucket = { VERIFIED: 0, HIGH: 0, PROBABLE: 0, UNKNOWN: 0 };
  for (const h of hotels) byBucket[h.verification_bucket] = (byBucket[h.verification_bucket] || 0) + 1;

  const brandExposure = {};
  for (const h of hotels) {
    const key = h.brand_display || "Unmapped / Independent";
    if (!brandExposure[key]) {
      brandExposure[key] = {
        brand: key,
        hotels: 0,
        known_rooms: 0,
        data_status: h.brand_display_status,
      };
    }
    brandExposure[key].hotels += 1;
    if (h.rooms != null) brandExposure[key].known_rooms += Number(h.rooms) || 0;
  }

  const markets = [...new Set(hotels.map((h) => h.market_display).filter(Boolean))].sort();
  const knownRooms = hotels.reduce((s, h) => s + (h.rooms != null ? Number(h.rooms) || 0 : 0), 0);
  const framing = cohort.portfolio_framing || {};

  return {
    ok: true,
    version: GOLDEN_DEMO_GSF_VERSION,
    data_status: cohort.data_status,
    costar_firewall: "enforced",
    page_framing: {
      title: framing.title || "Known GSF Hotel Relationships",
      kicker: "Organization profile",
      do_not_say: framing.do_not_say || "Hotels owned by Grupo Hotelero Santa Fe",
      summary,
    },
    group: {
      ...org,
      display_name: org.display_name,
      headquarters: null,
      headquarters_status: "UNKNOWN",
      known_current_hotel_count: hotels.length,
      known_hotel_relationships: hotels.length,
      known_rooms: knownRooms || null,
      known_rooms_status: knownRooms ? "LIVE" : "UNKNOWN",
      markets,
      evidence_status: "HIGH",
      evidence_summary: `${summary.total_relationships} hotel relationships — ${summary.operated_or_managed} operated/managed — ${summary.owned_or_controlled_verified} owned/controlled verified — ${summary.ownership_unknown} ownership unknown`,
      verification_breakdown: byBucket,
      relationship_summary: summary,
    },
    portfolio: hotels,
    brand_exposure: Object.values(brandExposure).sort((a, b) => b.hotels - a.hotels),
    operator_exposure: [
      {
        operator: org.display_name,
        hotels: hotels.length,
        known_rooms: knownRooms || null,
        relationship_type: "OPERATED_BY",
        data_status: "CONTROLLED_DEMO",
      },
    ],
    ownership_exposure: [
      {
        note: "No product-safe OWNED_BY / CONTROLLED_BY edges certified in this controlled demo cohort.",
        hotels: 0,
      },
    ],
    portfolio_opportunity: {
      known_hotels: hotels.length,
      known_rooms: knownRooms || null,
      markets: markets.length,
      independent_or_unmapped: brandExposure["Unmapped / Independent"]?.hotels || 0,
      multi_property_leverage: true,
      scores_assigned: false,
      note: "Transparent relationship signals only — operator leverage ≠ ownership.",
    },
    why_this_matters: {
      text:
        `${org.display_name} is modeled as a multi-market Mexico operator/manager with known hotel relationships. ` +
        `Dealality preserves operator vs owner vs brand distinctions instead of collapsing management into ownership.`,
    },
    graph: buildNeighborsPayload({
      nodeId: org.entity_id,
      nodeType: "organization",
      hops: 1,
    }),
  };
}

export function listGoldenDemoCohortIndex() {
  const cohort = loadGsfGoldenDemoCohort();
  const org = orgOf(cohort);
  const summary = relationshipSummary(cohort);
  const hotels = (cohort.hotels || []).map((h) => {
    const id = h.airtable_record_id;
    const hasOwner = h.economic_owner_verified === true || h.relationship_type === "OWNED_BY";
    return {
      airtable_record_id: id,
      hotel_id: h.hotel_id || null,
      name: h.name,
      city: h.city,
      market: h.market || null,
      brand: h.affiliation_display || h.brand || null,
      relationship_type: h.relationship_type || null,
      economic_owner_verified: hasOwner,
      review_url: id
        ? `/app#/opportunity-radar?demo=gsf-pv&country=Mexico&hotel=${encodeURIComponent(id)}`
        : null,
    };
  });

  const dovetail = loadDovetailGoldenDemoCohort();
  const cambridgeHotels = (dovetail.hotels || [])
    .filter((h) => h.airtable_record_id)
    .map((h) => ({
      airtable_record_id: h.airtable_record_id,
      hotel_id: h.hotel_id || null,
      name: h.name,
      city: h.city,
      market: h.market || null,
      brand: h.affiliation_display || h.brand || null,
      relationship_type: h.relationship_type || null,
      economic_owner_verified: h.economic_owner_verified === true,
      golden_demo: "GOLDEN_DEMO_2",
      review_url: `/app#/opportunity-radar?demo=cambridge-beaches&country=Bermuda&hotel=${encodeURIComponent(
        h.airtable_record_id
      )}`,
    }));

  return {
    ok: true,
    version: GOLDEN_DEMO_GSF_VERSION,
    data_status: cohort.data_status,
    groups: [org.slug, DOVETAIL_SLUG],
    hotel_airtable_ids: [...hotels, ...cambridgeHotels]
      .map((h) => h.airtable_record_id)
      .filter(Boolean),
    hotels: [...hotels, ...cambridgeHotels],
    hotels_with_owner_information: [...hotels, ...cambridgeHotels].filter(
      (h) => h.economic_owner_verified
    ),
    relationship_summary: summary,
    default_demo: {
      country: "Mexico",
      market: "Puerto Vallarta / Riviera Nayarit",
      highlight_hotel_airtable_id:
        hotels.find((h) => /Puerto Vallarta/i.test(String(h.city || "")))?.airtable_record_id ||
        null,
      founder_review_url:
        "/app#/opportunity-radar?demo=gsf-pv&country=Mexico&hotel=recUNycnMwOVFX0hc",
      cambridge_demo_url: `/app#/opportunity-radar?demo=cambridge-beaches&country=Bermuda&hotel=${CAMBRIDGE_AIRTABLE_ID}`,
      owner_review_index_url: "/app#/ownership-review",
    },
  };
}

/** Semantic audit rows for Packet 2.1 report. */
export function buildGsfRelationshipSemanticAudit() {
  const cohort = loadGsfGoldenDemoCohort();
  const org = orgOf(cohort);
  const src = cohort.source || {};
  return (cohort.hotels || []).map((h) => {
    const b = brandMeta(h);
    return {
      dhl_hotel_id: h.hotel_id,
      radar_airtable_record_id: h.airtable_record_id,
      property_census_record_id: h.property_census_record_id || null,
      hotel: h.name,
      city: h.city,
      brand: b.brand,
      brand_status: b.status,
      operator: h.operator || h.management_company || org.display_name,
      gsf_relationship: org.display_name,
      exact_relationship_type: h.relationship_type || "OPERATED_BY",
      evidence_source: src.title || src.provider,
      evidence_url: src.url,
      confidence: verificationBucket(h.verification_status),
      current_or_historical: h.is_current === false ? "historical" : "current",
      what_evidence_proves:
        "Hotel is managed/operated in relationship with GSF (Census Management Company + first-party GSF listing).",
      what_evidence_does_not_prove:
        "Economic ownership, PropCo title, CONTROLLED_BY, SPONSORED_BY, or that operator equals owner.",
      economic_owner_verified: h.economic_owner_verified === true,
    };
  });
}
