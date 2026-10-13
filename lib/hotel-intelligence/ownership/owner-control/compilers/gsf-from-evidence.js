/**
 * Packet 2.8B-2 — Compile GSF Owner Control Graph + portfolio from existing evidence.
 * NO Webhound. Uses deep research + golden cohort + dossier facts only.
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
import { slugify } from "../owner-anchor.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../../../../");

export const GSF_OWNER_ENTITY_ID = "dle_06G6AB1VK0BCCD94DNN7W8DRWZ";
export const GSF_SLUG = "grupo-hotelero-santa-fe";
export const KGPV_AIRTABLE_ID = "recUNycnMwOVFX0hc";
export const IHVSF_ENTITY_ID = "ent_ihvsf_propco_vallarta";
export const CHARTWELL_ENTITY_ID = "ent_grupo-chartwell";
export const KRYSTAL_RESORT_PV_CANDIDATE_ID = "candidate_krystal_resort_pv";

function readJson(rel) {
  const p = path.join(ROOT, rel);
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

/**
 * Build GSF owner profile from frozen evidence fixtures (not live research).
 */
export function compileGsfOwnerPortfolioFromEvidence() {
  const cohort = readJson("fixtures/golden-demo/gsf-mexico-ownership-cohort-v1.json");
  const deep = readJson("fixtures/golden-demo/krystal-grand-pv-deep-research-v1.json");

  const graph = createEmptyOwnerControlGraph(GSF_OWNER_ENTITY_ID, {
    source: "gsf_deep_research_plus_cohort_compile",
    webhound_runs: 0,
    external_spend_usd: 0,
  });

  upsertNode(graph, {
    entity_id: GSF_OWNER_ENTITY_ID,
    display_name: "Grupo Hotelero Santa Fe",
    legal_name: "Grupo Hotelero Santa Fe, S.A.B. de C.V.",
    entity_type: "COMPANY",
    association_level: "CORE_CONTROL",
    aliases: ["GSF", "HOTEL", "Grupo Hotelero Santa Fe SAB de CV"],
    jurisdiction: "Mexico",
    confidence: "HIGH",
    evidence_ids: ["src_gsf_securities", "src_gsf_site"],
  });

  upsertNode(graph, {
    entity_id: IHVSF_ENTITY_ID,
    display_name: "Inmobiliaria en Hotelería Vallarta Santa Fe",
    legal_name: "Inmobiliaria en Hotelería Vallarta Santa Fe, S. de R.L. de C.V.",
    entity_type: "PROPCO",
    association_level: "PROPERTY_VEHICLE",
    jurisdiction: "Mexico",
    confidence: "HIGH",
    evidence_ids: ["src_kgpv_ownership_chain"],
  });

  upsertNode(graph, {
    entity_id: CHARTWELL_ENTITY_ID,
    display_name: "Grupo Chartwell",
    legal_name: "Grupo Chartwell",
    entity_type: "COMPANY",
    association_level: "JV_PARTNER",
    aliases: ["Chartwell"],
    jurisdiction: "Mexico",
    confidence: "HIGH",
    evidence_ids: ["src_kgpv_ownership_chain"],
  });

  upsertNode(graph, {
    entity_id: "ent_carlos-ancira",
    display_name: "Carlos Gerardo Ancira Elizondo",
    entity_type: "INDIVIDUAL",
    association_level: "SHAREHOLDER_INFLUENCE",
    confidence: "PROBABLE",
    evidence_ids: ["src_kgpv_people"],
  });

  // PropCo controlled by GSF
  upsertEdge(graph, {
    from_entity_id: IHVSF_ENTITY_ID,
    to_entity_id: GSF_OWNER_ENTITY_ID,
    relationship_type: "CONTROLLED_BY",
    association_level: "PROPERTY_VEHICLE",
    confidence: "HIGH",
    note: "IHVSF is GSF subsidiary / property vehicle for Krystal Grand PV thread.",
    evidence_ids: ["src_kgpv_ownership_chain"],
  });
  upsertEdge(graph, {
    from_entity_id: GSF_OWNER_ENTITY_ID,
    to_entity_id: IHVSF_ENTITY_ID,
    relationship_type: "PARENT_OF",
    association_level: "CONTROLLED_ENTITY",
    confidence: "HIGH",
    note: "GSF parent of IHVSF PropCo.",
    evidence_ids: ["src_kgpv_ownership_chain"],
  });

  // Chartwell: former co-owner + current JV-adjacent / shareholder influence — NOT control fan-out
  upsertEdge(graph, {
    from_entity_id: CHARTWELL_ENTITY_ID,
    to_entity_id: GSF_OWNER_ENTITY_ID,
    relationship_type: "RELATED",
    association_level: "SHAREHOLDER_INFLUENCE",
    temporal_status: "CURRENT",
    confidence: "PROBABLE",
    note: "Chartwell among principales accionistas; Ancira is GSF Board President. Not PropCo ownership of KGPV.",
    evidence_ids: ["src_kgpv_ownership_chain"],
  });
  upsertEdge(graph, {
    from_entity_id: "hotel:" + KGPV_AIRTABLE_ID,
    to_entity_id: CHARTWELL_ENTITY_ID,
    relationship_type: "OWNED_BY",
    association_level: "HISTORICAL_ASSOCIATION",
    temporal_status: "FORMER",
    confidence: "HIGH",
    note: "Chartwell formerly co-owned Hilton PV; sold remaining 50% to GSF in 2014.",
    evidence_ids: ["src_kgpv_ownership_chain"],
  });

  // Hotel nodes for focus + managed contrast
  upsertNode(graph, {
    entity_id: "hotel:" + KGPV_AIRTABLE_ID,
    display_name: "Krystal Grand Puerto Vallarta",
    entity_type: "HOTEL",
    association_level: "CORE_CONTROL",
    confidence: "HIGH",
    evidence_ids: ["src_kgpv_deep"],
  });
  upsertNode(graph, {
    entity_id: KRYSTAL_RESORT_PV_CANDIDATE_ID,
    display_name: "Krystal Resort Puerto Vallarta",
    entity_type: "HOTEL",
    association_level: "OPERATING_ENTITY",
    confidence: "HIGH",
    evidence_ids: ["src_kgpv_adjacent"],
  });

  upsertEdge(graph, {
    from_entity_id: "hotel:" + KGPV_AIRTABLE_ID,
    to_entity_id: IHVSF_ENTITY_ID,
    relationship_type: "OWNED_BY",
    association_level: "PROPERTY_VEHICLE",
    confidence: "HIGH",
    control_percentage_class: "FULL_100",
    note: "KGPV owned via IHVSF PropCo vehicle.",
    evidence_ids: ["src_kgpv_ownership_chain"],
  });
  upsertEdge(graph, {
    from_entity_id: "hotel:" + KGPV_AIRTABLE_ID,
    to_entity_id: GSF_OWNER_ENTITY_ID,
    relationship_type: "OWNED_BY",
    association_level: "CORE_CONTROL",
    confidence: "HIGH",
    control_percentage_class: "FULL_100",
    note: "Economic owner GSF consolidates KGPV as owned asset.",
    evidence_ids: ["src_kgpv_ownership_chain", "src_gsf_securities"],
  });
  upsertEdge(graph, {
    from_entity_id: "hotel:" + KGPV_AIRTABLE_ID,
    to_entity_id: GSF_OWNER_ENTITY_ID,
    relationship_type: "OPERATED_BY",
    association_level: "OPERATING_ENTITY",
    confidence: "HIGH",
    note: "GSF self-operates KGPV.",
    evidence_ids: ["src_kgpv_deep"],
  });

  // Critical negative: Resort is Chartwell-owned, GSF-managed
  upsertEdge(graph, {
    from_entity_id: KRYSTAL_RESORT_PV_CANDIDATE_ID,
    to_entity_id: CHARTWELL_ENTITY_ID,
    relationship_type: "OWNED_BY",
    association_level: "JV_PARTNER",
    confidence: "HIGH",
    note: "Krystal Resort PV owned by Grupo Chartwell — distinct from KGPV.",
    evidence_ids: ["src_kgpv_adjacent"],
  });
  upsertEdge(graph, {
    from_entity_id: KRYSTAL_RESORT_PV_CANDIDATE_ID,
    to_entity_id: GSF_OWNER_ENTITY_ID,
    relationship_type: "OPERATED_BY",
    association_level: "OPERATING_ENTITY",
    confidence: "HIGH",
    note: "GSF manages Chartwell-owned Krystal Resort PV. Must NOT attribute as GSF-owned.",
    evidence_ids: ["src_kgpv_adjacent"],
  });

  const sources = [
    {
      source_id: "src_kgpv_deep",
      title: "KGPV deep research compile",
      authority: "dealality_compile",
    },
    {
      source_id: "src_kgpv_ownership_chain",
      title: "KGPV ownership chain (GSF securities / PropCo)",
      authority: "corporate_filings",
    },
    {
      source_id: "src_gsf_securities",
      title: "GSF public-company disclosures",
      authority: "corporate_filings",
    },
    {
      source_id: "src_gsf_site",
      title: "gsf-hotels.com",
      authority: "first_party",
    },
    {
      source_id: "src_kgpv_adjacent",
      title: "Adjacent Chartwell Krystal Resort PV separation",
      authority: "corporate_filings",
    },
    {
      source_id: "src_kgpv_people",
      title: "GSF people / Chartwell shareholder notes",
      authority: "corporate_filings",
    },
  ];

  const hotel_relationships = [];

  // KGPV owned+operated
  hotel_relationships.push({
    hotel_id: "dhl_06G6AB2228339A12S2EZ7PZF5E",
    airtable_record_id: KGPV_AIRTABLE_ID,
    hotel_name: "Krystal Grand Puerto Vallarta",
    relationship_type: "OWNED_BY",
    secondary_relationship_type: "OPERATED_BY",
    economic_owner_verified: true,
    temporal_status: "CURRENT",
    brand: "Krystal Grand",
    operator: "Grupo Hotelero Santa Fe",
    market: "Puerto Vallarta / Riviera Nayarit",
    country: "Mexico",
    rooms: 451,
    confidence: "HIGH",
    focus_hotel: true,
    source_ids: ["src_kgpv_ownership_chain", "src_gsf_securities"],
    attribution_path: [
      {
        step: 1,
        relationship_type: "OWNED_BY",
        to_entity_id: IHVSF_ENTITY_ID,
        to_display_name: "IHVSF (PropCo)",
      },
      {
        step: 2,
        relationship_type: "CONTROLLED_BY",
        to_entity_id: GSF_OWNER_ENTITY_ID,
        to_display_name: "Grupo Hotelero Santa Fe",
      },
    ],
    note: "GSF-owned and self-operated.",
  });

  // Cohort hotels: only KGPV verified owned; others operated/managed pending ownership verify
  for (const h of cohort.hotels || []) {
    if (h.airtable_record_id === KGPV_AIRTABLE_ID) continue;
    hotel_relationships.push({
      hotel_id: h.hotel_id || null,
      airtable_record_id: h.airtable_record_id || null,
      hotel_name: h.canonical_trading_name || h.name,
      relationship_type: h.relationship_type || "OPERATED_BY",
      economic_owner_verified: Boolean(h.economic_owner_verified),
      temporal_status: h.is_current === false ? "HISTORICAL" : "CURRENT",
      brand: h.affiliation_display || h.brand || null,
      operator: h.operator || "Grupo Hotelero Santa Fe",
      market: h.market || h.city || null,
      country: h.country || "Mexico",
      rooms: h.rooms ?? null,
      confidence: h.verification_status || "PROBABLE",
      focus_hotel: false,
      source_ids: ["src_gsf_site"],
      attribution_path:
        h.economic_owner_verified || h.relationship_type === "OWNED_BY"
          ? [
              {
                step: 1,
                relationship_type: "OWNED_BY",
                to_entity_id: GSF_OWNER_ENTITY_ID,
                to_display_name: "Grupo Hotelero Santa Fe",
              },
            ]
          : [
              {
                step: 1,
                relationship_type: "OPERATED_BY",
                to_entity_id: GSF_OWNER_ENTITY_ID,
                to_display_name: "Grupo Hotelero Santa Fe",
              },
            ],
      note: h.identity_notes || h.relationship_semantics || null,
    });
  }

  // Critical managed-not-owned row
  hotel_relationships.push({
    hotel_id: null,
    airtable_record_id: null,
    hotel_name: "Krystal Resort Puerto Vallarta",
    relationship_type: "OPERATED_BY",
    economic_owner_verified: false,
    temporal_status: "CURRENT",
    brand: "Krystal",
    operator: "Grupo Hotelero Santa Fe",
    market: "Puerto Vallarta / Riviera Nayarit",
    country: "Mexico",
    rooms: 530,
    confidence: "HIGH",
    focus_hotel: false,
    census_match_status: "UNRESOLVED_HOTEL_CANDIDATE",
    source_ids: ["src_kgpv_adjacent"],
    attribution_path: [
      {
        step: 1,
        relationship_type: "OPERATED_BY",
        to_entity_id: GSF_OWNER_ENTITY_ID,
        to_display_name: "Grupo Hotelero Santa Fe",
      },
    ],
    note: "Chartwell-owned; GSF-managed. GSF_MANAGED_NOT_OWNED.",
    why_in_portfolio: "OPERATED_BY GSF — owner is Grupo Chartwell (not GSF-owned)",
  });

  const people = (deep.people || []).slice(0, 12).map((p) => ({
    name: p.name,
    title: p.title,
    organization: p.organization,
    role_category: p.role_category || p.category || null,
    decision_authority: p.decision_authority || "Authority Not Verified",
    confidence: p.confidence || "PROBABLE",
  }));

  const open_questions = [
    "Which additional Krystal Grand / Urban assets are consolidated as GSF-owned vs third-party managed?",
    "Natural-person UBO beyond disclosed public-company / Chartwell principals",
    "Census match for Krystal Resort Puerto Vallarta",
  ];

  const profile = buildOwnerPortfolioProfile({
    graph,
    hotel_relationships,
    people,
    sources,
    open_questions,
    meta: {
      slug: GSF_SLUG,
      pubco: true,
      disclosure_quality: "HIGH",
      last_researched_at: deep.observed_date || "2026-09-04",
      known_gaps: [
        "Full consolidated owned-hotel list beyond KGPV not fully certified in this packet",
        "Krystal Resort PV census identity unresolved",
      ],
      archetype: "PUBLIC_COMPANY_OWNER_OPERATOR",
      webhound_runs: 0,
      external_spend_usd: 0,
    },
  });

  return { graph, profile };
}
