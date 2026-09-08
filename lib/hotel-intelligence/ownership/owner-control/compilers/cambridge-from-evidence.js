/**
 * Packet 2.8B-2 — Cambridge / Dovetail owner path compile (private sponsor archetype).
 * Preserves economic sponsor vs legal owner distinctions. No Webhound.
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
const ROOT = path.resolve(__dirname, "../../../../..");

export const CAMBRIDGE_AIRTABLE_ID = "recIwaP1etgx2g9nA";
export const DOVETAIL_ENTITY_ID = "ent_dovetail-hospitality";

function readJson(rel) {
  try {
    return JSON.parse(fs.readFileSync(path.join(ROOT, rel), "utf8"));
  } catch {
    return null;
  }
}

export function compileCambridgeOwnerPortfolioFromEvidence() {
  const deep = readJson("fixtures/golden-demo/cambridge-beaches-deep-research-v1.json");
  const cohort = readJson("fixtures/golden-demo/dovetail-hospitality-cohort-v1.json");

  const ownerName =
    deep?.ownership_and_control?.economic_owner_or_group?.name ||
    cohort?.organization?.display_name ||
    "Dovetail Hospitality";
  const ownerId = cohort?.organization?.entity_id || DOVETAIL_ENTITY_ID;
  const ownerRoleHint = String(
    deep?.ownership_and_control?.economic_owner_or_group?.note ||
      deep?.commercial_pursuit?.who_controls_relationship ||
      ""
  );

  const graph = createEmptyOwnerControlGraph(ownerId, {
    source: "cambridge_deep_research_compile",
    webhound_runs: 0,
    external_spend_usd: 0,
    archetype: "PRIVATE_SPONSOR_ACQUIRER",
  });

  upsertNode(graph, {
    entity_id: ownerId,
    display_name: ownerName,
    legal_name: cohort?.organization?.legal_name || ownerName,
    entity_type: "COMPANY",
    association_level: "CORE_CONTROL",
    confidence: "PROBABLE",
    evidence_ids: ["src_cambridge_deep"],
  });

  upsertNode(graph, {
    entity_id: "hotel:" + CAMBRIDGE_AIRTABLE_ID,
    display_name: "Cambridge Beaches Resort & Spa",
    entity_type: "HOTEL",
    confidence: "HIGH",
    evidence_ids: ["src_cambridge_deep"],
  });

  // Do not force deed-level OWNED_BY if evidence is sponsor/acquirer only
  const forceOwned = /deed|title|propco|100%\s*own/i.test(ownerRoleHint);
  upsertEdge(graph, {
    from_entity_id: "hotel:" + CAMBRIDGE_AIRTABLE_ID,
    to_entity_id: ownerId,
    relationship_type: forceOwned ? "OWNED_BY" : "SPONSORED_BY",
    association_level: forceOwned ? "CORE_CONTROL" : "SPONSORED_ENTITY",
    confidence: forceOwned ? "HIGH" : "PROBABLE",
    note: forceOwned
      ? "Legal/economic ownership supported."
      : "Economic sponsor / acquirer path — not collapsed to deed-level OWNED_BY without title evidence.",
    evidence_ids: ["src_cambridge_deep"],
  });

  const hotel_relationships = [
    {
      hotel_id: null,
      airtable_record_id: CAMBRIDGE_AIRTABLE_ID,
      hotel_name: "Cambridge Beaches Resort & Spa",
      relationship_type: forceOwned ? "OWNED_BY" : "SPONSORED_BY",
      economic_owner_verified: forceOwned,
      temporal_status: "CURRENT",
      brand: "Independent",
      operator: deep?.ownership_and_control?.operator?.name || null,
      market: "Bermuda",
      country: "Bermuda",
      rooms: 86,
      confidence: forceOwned ? "HIGH" : "PROBABLE",
      focus_hotel: true,
      source_ids: ["src_cambridge_deep"],
      attribution_path: [
        {
          step: 1,
          relationship_type: forceOwned ? "OWNED_BY" : "SPONSORED_BY",
          to_entity_id: ownerId,
          to_display_name: ownerName,
        },
      ],
      note: "Preserve sponsor vs legal owner distinction.",
    },
  ];

  // Cohort sister assets if present — classify carefully
  for (const h of cohort?.hotels || []) {
    if (/cambridge/i.test(String(h.name || ""))) continue;
    hotel_relationships.push({
      hotel_id: h.hotel_id || null,
      airtable_record_id: h.airtable_record_id || null,
      hotel_name: h.name,
      relationship_type: h.relationship_type || "RELATED",
      economic_owner_verified: Boolean(h.economic_owner_verified),
      temporal_status: h.is_current === false ? "HISTORICAL" : "CURRENT",
      brand: h.brand || null,
      operator: h.operator || null,
      market: h.market || h.city || null,
      country: h.country || "Bermuda",
      rooms: h.rooms ?? null,
      confidence: h.verification_status || "PROBABLE",
      focus_hotel: false,
      source_ids: ["src_dovetail_cohort"],
      attribution_path: [
        {
          step: 1,
          relationship_type: h.relationship_type || "RELATED",
          to_entity_id: ownerId,
          to_display_name: ownerName,
        },
      ],
      note: h.note || null,
    });
  }

  const profile = buildOwnerPortfolioProfile({
    graph,
    hotel_relationships,
    people: (deep?.people || []).slice(0, 8),
    sources: [
      { source_id: "src_cambridge_deep", title: "Cambridge deep research", authority: "dealality_compile" },
      { source_id: "src_dovetail_cohort", title: "Dovetail cohort fixture", authority: "controlled_demo" },
    ],
    open_questions: deep?.research_gaps || [
      "Deed-level PropCo identity vs economic sponsor",
    ],
    meta: {
      slug: "dovetail-hospitality",
      pubco: false,
      disclosure_quality: "MEDIUM",
      last_researched_at: deep?.observed_date || null,
      known_gaps: ["Legal owner vs sponsor precision"],
      archetype: "PRIVATE_SPONSOR_ACQUIRER",
      webhound_runs: 0,
      external_spend_usd: 0,
    },
  });

  return { graph, profile, owner_role: forceOwned ? "ECONOMIC_OWNER" : "SPONSOR" };
}
