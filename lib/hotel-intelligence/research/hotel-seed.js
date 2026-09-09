/**
 * Packet 2.6C-R2 — hotel identity seed for provider prompts.
 */

import { KGPV_PROPERTY_IDENTITY } from "./archive-integrity.js";
import { KGPV_HOTEL_ID } from "./backfill-kgpv.js";

const SEEDS = {
  [KGPV_HOTEL_ID]: {
    hotel_id: KGPV_HOTEL_ID,
    hotel_name: KGPV_PROPERTY_IDENTITY.canonical_name,
    address: "Puerto Vallarta, Jalisco, Mexico",
    market: "Puerto Vallarta",
    country: "Mexico",
    rooms: 451,
    chain_scale: "Luxury",
    current_brand: "Krystal Grand",
    parent_company: "Grupo Hotelero Santa Fe",
    operator: "Grupo Hotelero Santa Fe",
    owner_operator: true,
    former_names: [],
    announced_names: ["Breathless Puerto Vallarta (announced conversion — completion unverified)"],
    identity_collisions: [
      {
        wrong_name: KGPV_PROPERTY_IDENTITY.collision_name,
        note: KGPV_PROPERTY_IDENTITY.note,
      },
    ],
    known_facts_hints: [
      "GSF is owner-operator (self-operated) — do not invent a third-party management company dispute without evidence.",
      "Full Hotel Intelligence Investigation already completed historically — this is a follow-up addendum investigation.",
    ],
  },
  recIwaP1etgx2g9nA: {
    hotel_id: "recIwaP1etgx2g9nA",
    hotel_name: "Cambridge Beaches Resort & Spa",
    address: "30 Kings Point Road, Somerset, Sandys Parish, Bermuda",
    market: "Bermuda West End / Sandys",
    country: "Bermuda",
    rooms: 86,
    chain_scale: "Independent Luxury",
    current_brand: "Independent — Cambridge Beaches",
    parent_company: "Dovetail + Co",
    operator: null,
    owner_operator: null,
    former_names: [],
    announced_names: [],
    identity_collisions: [
      {
        wrong_name: "Beaches Resorts / Sandals Beaches",
        note: "Property name contains Beaches but is NOT the Beaches Resorts brand.",
      },
      {
        wrong_name: "Cambridge MA hotels",
        note: "Geography is Bermuda Sandys / Somerset.",
      },
    ],
    known_facts_hints: [
      "PropCo/developer: Cambridge Beaches Holdings Limited (Tourism Investment Order 2022).",
      "Economic sponsor: Dovetail + Co (2021 acquisition from Frascati Hotel Company).",
      "CURRENT operator is CONTESTED (Dovetail stewardship vs Benchmark/Pyramid) — do not auto-promote 2021 Benchmark announcement as current.",
      "Principals Phil Hospod / Karla Bruning are sponsor-path evidence — not automatic deed owners.",
      "Acreage conflict 20 vs 23 — disclose; prefer Dovetail 23-acre with conflict note.",
    ],
  },
};

export function buildHotelIdentitySeed(input = {}) {
  const hotelId = String(input.hotel_id || input.hotelId || "").trim();
  const base = SEEDS[hotelId] ? { ...SEEDS[hotelId] } : null;
  const seed = {
    hotel_id: hotelId,
    hotel_name: input.hotel_name || base?.hotel_name || null,
    address: input.address || base?.address || null,
    market: input.market || base?.market || null,
    country: input.country || base?.country || null,
    rooms: input.rooms ?? base?.rooms ?? null,
    chain_scale: input.chain_scale || base?.chain_scale || null,
    current_brand: input.current_brand || base?.current_brand || null,
    parent_company: input.parent_company || base?.parent_company || null,
    operator: input.operator || base?.operator || null,
    owner_operator: input.owner_operator ?? base?.owner_operator ?? null,
    former_names: input.former_names || base?.former_names || [],
    announced_names: input.announced_names || base?.announced_names || [],
    identity_collisions: input.identity_collisions || base?.identity_collisions || [],
    known_facts_hints: input.known_facts_hints || base?.known_facts_hints || [],
  };
  if (hotelId === KGPV_HOTEL_ID) {
    seed.kgpv_identity_guard = {
      must_equal: KGPV_PROPERTY_IDENTITY.canonical_name,
      must_not_equal: KGPV_PROPERTY_IDENTITY.collision_name,
      note: KGPV_PROPERTY_IDENTITY.note,
    };
  }
  return seed;
}

/**
 * Build structured Webhound prompt from template + seed + known facts/gaps.
 */
export function buildWebhoundResearchPrompt({
  hotel_seed,
  template,
  known_facts,
  known_gaps,
  budget_usd,
} = {}) {
  const seed = hotel_seed || {};
  const t = template || {};
  const lines = [];
  lines.push(`# Dealality Deep Research — ${t.display_name || t.template_id}`);
  lines.push("");
  lines.push("You are conducting hotel intelligence research for Dealality.");
  lines.push("Return source-backed findings. Do not invent facts.");
  lines.push("Do not treat this output as canonical product truth — Dealality will normalize separately.");
  lines.push("");
  lines.push("## Hotel identity (authoritative)");
  lines.push(`- Canonical hotel ID: ${seed.hotel_id}`);
  lines.push(`- Current name: ${seed.hotel_name}`);
  if (seed.address) lines.push(`- Address / location: ${seed.address}`);
  if (seed.market) lines.push(`- Market: ${seed.market}`);
  if (seed.current_brand) lines.push(`- Current brand: ${seed.current_brand}`);
  if (seed.parent_company) lines.push(`- Parent / owner: ${seed.parent_company}`);
  if (seed.operator) lines.push(`- Operator: ${seed.operator}`);
  if (seed.owner_operator) lines.push("- Operating model: owner-operated (self-operated)");
  if (seed.former_names?.length) lines.push(`- Former names: ${seed.former_names.join("; ")}`);
  if (seed.announced_names?.length) {
    lines.push(`- Announced / contemplated names: ${seed.announced_names.join("; ")}`);
  }
  if (seed.kgpv_identity_guard) {
    lines.push("");
    lines.push("## HARD PROPERTY IDENTITY GUARD");
    lines.push(
      `${seed.kgpv_identity_guard.must_equal} ≠ ${seed.kgpv_identity_guard.must_not_equal}`
    );
    lines.push(seed.kgpv_identity_guard.note);
    lines.push("Do not mix evidence between these two hotels.");
  }
  for (const c of seed.identity_collisions || []) {
    lines.push(`- Collision: do not use "${c.wrong_name}" — ${c.note || ""}`);
  }
  lines.push("");
  lines.push("## Research template");
  lines.push(`- Template ID: ${t.template_id}`);
  lines.push(`- Version: ${t.version}`);
  lines.push(`- Customer question: ${t.customer_question}`);
  lines.push(`- Research objective: ${t.research_objective}`);
  if (t.research_lanes?.length) lines.push(`- Lanes: ${t.research_lanes.join(", ")}`);
  if (t.scope_groups?.length) {
    lines.push("- Scope groups:");
    for (const g of t.scope_groups) {
      lines.push(`  - ${g.label}: ${(g.items || []).join(", ")}`);
    }
  }
  if (t.report_sections?.length) {
    lines.push(`- Required report sections: ${t.report_sections.join(", ")}`);
  }
  if (t.negative_screens?.length) {
    lines.push(`- Negative screens: ${t.negative_screens.join(", ")}`);
  }
  if (t.validation_rules?.length) {
    lines.push(`- Validation rules: ${t.validation_rules.join(", ")}`);
  }
  if (t.preferred_source_classes?.length) {
    lines.push(`- Preferred source classes: ${t.preferred_source_classes.join(", ")}`);
  }
  lines.push(`- Maximum provider budget: $${Number(budget_usd).toFixed(2)} USD (HARD CAP — do not exceed).`);
  lines.push("");
  lines.push("## Known facts (may already be established)");
  const facts = known_facts || seed.known_facts_hints || [];
  if (Array.isArray(facts) && facts.length) {
    for (const f of facts) lines.push(`- ${typeof f === "string" ? f : JSON.stringify(f)}`);
  } else {
    lines.push("- (none provided)");
  }
  lines.push("");
  lines.push("## Known gaps / open questions");
  const gaps = known_gaps || [];
  if (Array.isArray(gaps) && gaps.length) {
    for (const g of gaps) {
      lines.push(`- ${typeof g === "string" ? g : g.title || g.question || JSON.stringify(g)}`);
    }
  } else {
    lines.push("- Investigate according to the template lanes and produce open questions where unresolved.");
  }
  if (t.template_id === "CHANGE_OPPORTUNITY") {
    lines.push("");
    lines.push("## Change & Opportunity special requirements");
    lines.push("Cover WHY NOW, physical asset/product risk, product investment/deferred capex,");
    lines.push("operating quality, management/operator stability, deal risk flags, and what to verify next.");
    lines.push("Keep physical condition, operating quality, and operator-change risk conceptually separate.");
    lines.push("Single guest reviews are NOT findings — require consistent themes (REPEATED/PERSISTENT/MULTI-SOURCE).");
    lines.push("No invented dollar capex estimates. No fake 0–100 risk scores.");
  }
  lines.push("");
  lines.push("## Output expectations");
  lines.push("Produce a substantive cited research report with:");
  lines.push("- Executive answer");
  lines.push("- Key findings with sources and dates");
  lines.push("- Template-specific investigation sections");
  lines.push("- Open questions / research gaps");
  lines.push("- Source list with URLs where available");
  return lines.join("\n");
}

export function buildWebhoundOutputInstructions(template) {
  const t = template || {};
  return [
    "Structure the final report so Dealality can normalize it.",
    "Prefer explicit section headings matching the investigation template.",
    "Cite sources inline. Distinguish verified facts from unresolved questions.",
    "Do not claim Dealality product canonical status.",
    t.template_id === "CHANGE_OPPORTUNITY"
      ? "Include sections: Executive Answer (Why Now / What Could Derail / What Looks Stable / What Needs Verification), Opportunity Thesis, Asset & Product Risk, Product Investment / Capex, Operating Quality & Management Risk, Operator / Management Stability, Deal Risk Flags, What to Verify Next, Open Questions, Sources."
      : "Include Executive Answer, Key Findings, Investigation detail, Open Questions, Sources.",
  ].join(" ");
}
