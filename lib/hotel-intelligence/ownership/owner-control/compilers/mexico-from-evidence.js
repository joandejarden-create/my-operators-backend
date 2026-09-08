/**
 * Packet 2.8B-2 — Mexico Explorer owner paths (Sheraton HNF / voco Alliance).
 * Operator ≠ owner. No Webhound.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createEmptyOwnerControlGraph,
  upsertEdge,
  upsertNode,
} from "../control-graph.js";
import { buildOwnerPortfolioProfile } from "../portfolio-factory.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../../../../");

export const SHERATON_GDL_ID = "recsYJb2R1jarPpK3";
export const VOCO_CANCUN_ID = "recTYaiA4S6fR6ixx";
export const HNF_ENTITY_ID = "ent_inmobiliaria-hnf";
export const ALLIANCE_ENTITY_ID = "ent_alliance-hotel-management";
export const AIMBRIDGE_ENTITY_ID = "ent_aimbridge-latam";

function readJson(rel) {
  try {
    return JSON.parse(fs.readFileSync(path.join(ROOT, rel), "utf8"));
  } catch {
    return null;
  }
}

function compileOneMexicoOwner({
  ownerId,
  ownerName,
  legalName,
  hotelId,
  hotelName,
  deepPath,
  operatorId,
  operatorName,
  brand,
  rooms,
  market,
  country,
  slug,
}) {
  const deep = readJson(deepPath);
  const graph = createEmptyOwnerControlGraph(ownerId, {
    source: "mexico_explorer_deep_research_compile",
    webhound_runs: 0,
    external_spend_usd: 0,
  });

  upsertNode(graph, {
    entity_id: ownerId,
    display_name: ownerName,
    legal_name: legalName || ownerName,
    entity_type: "COMPANY",
    association_level: "CORE_CONTROL",
    confidence: "HIGH",
    evidence_ids: ["src_mexico_deep"],
  });
  upsertNode(graph, {
    entity_id: operatorId,
    display_name: operatorName,
    entity_type: "OPERATING_COMPANY",
    association_level: "OPERATING_ENTITY",
    confidence: "HIGH",
    evidence_ids: ["src_mexico_deep"],
  });
  upsertNode(graph, {
    entity_id: "hotel:" + hotelId,
    display_name: hotelName,
    entity_type: "HOTEL",
    confidence: "HIGH",
    evidence_ids: ["src_mexico_deep"],
  });

  upsertEdge(graph, {
    from_entity_id: "hotel:" + hotelId,
    to_entity_id: ownerId,
    relationship_type: "OWNED_BY",
    association_level: "CORE_CONTROL",
    confidence: "HIGH",
    note: "Legal / economic owner path from Mexico Explorer Full HI compile.",
    evidence_ids: ["src_mexico_deep"],
  });
  upsertEdge(graph, {
    from_entity_id: "hotel:" + hotelId,
    to_entity_id: operatorId,
    relationship_type: "OPERATED_BY",
    association_level: "OPERATING_ENTITY",
    confidence: "HIGH",
    note: "Operator is separate from owner.",
    evidence_ids: ["src_mexico_deep"],
  });

  const assets = deep?.portfolio_notes?.assets || [];
  const hotel_relationships = [
    {
      hotel_id: null,
      airtable_record_id: hotelId,
      hotel_name: hotelName,
      relationship_type: "OWNED_BY",
      secondary_relationship_type: "OPERATED_BY",
      economic_owner_verified: true,
      temporal_status: "CURRENT",
      brand,
      operator: operatorName,
      market,
      country,
      rooms,
      confidence: "HIGH",
      focus_hotel: true,
      source_ids: ["src_mexico_deep"],
      attribution_path: [
        {
          step: 1,
          relationship_type: "OWNED_BY",
          to_entity_id: ownerId,
          to_display_name: ownerName,
        },
      ],
      note: "Owner ≠ operator enforced.",
    },
  ];

  for (const a of assets) {
    if (!a?.name || /sheraton guadalajara|voco|real inn/i.test(a.name)) continue;
    const rels = Array.isArray(a.relationships) ? a.relationships.join(" ") : String(a.note || "");
    const owned = /own|propco|control/i.test(rels);
    hotel_relationships.push({
      hotel_id: null,
      airtable_record_id: null,
      hotel_name: a.name,
      relationship_type: owned ? "OWNED_BY" : "RELATED",
      economic_owner_verified: owned,
      temporal_status: a.status === "Historical" ? "HISTORICAL" : "CURRENT",
      brand: a.brand || null,
      operator: a.operator || null,
      market: a.market || null,
      country: country,
      rooms: a.rooms ?? null,
      confidence: a.confidence || "PROBABLE",
      focus_hotel: false,
      census_match_status: "UNRESOLVED_HOTEL_CANDIDATE",
      source_ids: ["src_mexico_deep"],
      attribution_path: [
        {
          step: 1,
          relationship_type: owned ? "OWNED_BY" : "RELATED",
          to_entity_id: ownerId,
          to_display_name: ownerName,
        },
      ],
      note: a.note || null,
    });
  }

  const profile = buildOwnerPortfolioProfile({
    graph,
    hotel_relationships,
    people: (deep?.people || []).slice(0, 8),
    sources: [{ source_id: "src_mexico_deep", title: deepPath, authority: "dealality_compile" }],
    open_questions: deep?.research_gaps || [],
    meta: {
      slug,
      pubco: false,
      disclosure_quality: "MEDIUM",
      last_researched_at: deep?.observed_date || null,
      known_gaps: ["Broader owner portfolio beyond focus hotel still partial"],
      archetype: "PRIVATE_LEGAL_OWNER_THIRD_PARTY_OPERATOR",
      webhound_runs: 0,
      external_spend_usd: 0,
    },
  });

  return { graph, profile };
}

export function compileSheratonHnfOwnerPortfolioFromEvidence() {
  return compileOneMexicoOwner({
    ownerId: HNF_ENTITY_ID,
    ownerName: "Inmobiliaria HNF",
    legalName: "Inmobiliaria HNF",
    hotelId: SHERATON_GDL_ID,
    hotelName: "Sheraton Guadalajara Expo",
    deepPath: "fixtures/golden-demo/sheraton-gdl-expo-deep-research-v1.json",
    operatorId: AIMBRIDGE_ENTITY_ID,
    operatorName: "Aimbridge LATAM",
    brand: "Sheraton Hotel",
    rooms: 216,
    market: "Pacific Central",
    country: "Mexico",
    slug: "inmobiliaria-hnf",
  });
}

export function compileVocoAllianceOwnerPortfolioFromEvidence() {
  // Economic/legal owner path for voco — Alliance / Camino Real lineage from deep research.
  // Aimbridge remains operator only.
  return compileOneMexicoOwner({
    ownerId: ALLIANCE_ENTITY_ID,
    ownerName: "Alliance Hotel Management / owner path",
    legalName: "Alliance Hotel Management",
    hotelId: VOCO_CANCUN_ID,
    hotelName: "voco Cancún Zona Hotelera",
    deepPath: "fixtures/golden-demo/real-inn-cancun-deep-research-v1.json",
    operatorId: AIMBRIDGE_ENTITY_ID,
    operatorName: "Aimbridge LATAM",
    brand: "voco",
    rooms: 160,
    market: "Cancun",
    country: "Mexico",
    slug: "alliance-hotel-management",
  });
}
