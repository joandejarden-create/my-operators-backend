/**
 * Map existing KGPV deep-research fixture → normalized dossier.
 * Preserves narrative; no aggressive rewrite. Does not run Webhound.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createEmptyDossier,
  refreshDossierCounts,
  DOSSIER_SECTION_TITLES,
  DOSSIER_TITLE,
} from "../schema.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEEP_RESEARCH_PATH = path.resolve(
  __dirname,
  "../../../../fixtures/golden-demo/krystal-grand-pv-deep-research-v1.json"
);

function loadDeepResearch(optionalPath) {
  const p = optionalPath || DEEP_RESEARCH_PATH;
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function section(id, blocks) {
  const meaningful = (blocks || []).filter((b) => {
    if (!b) return false;
    if (b.type === "paragraphs" && (!b.paragraphs || !b.paragraphs.length)) return false;
    if (b.type === "table" && (!b.rows || !b.rows.length)) return false;
    if (b.type === "list" && (!b.items || !b.items.length)) return false;
    if (b.type === "trace" && (!b.nodes || !b.nodes.length)) return false;
    return true;
  });
  if (!meaningful.length) return null;
  return {
    id,
    title: DOSSIER_SECTION_TITLES[id] || id,
    blocks: meaningful,
  };
}

function mapPeopleCategory(role) {
  const r = String(role || "").toLowerCase();
  if (/beneficial|principal|owner/.test(r)) return "Ownership / Principal";
  if (/development/.test(r)) return "Development / Growth";
  if (/property leadership|director general/.test(r)) return "Property Leadership";
  if (/asset/.test(r)) return "Asset Management";
  if (/legal|governance/.test(r)) return "Legal / Governance";
  if (/brand|franchise/.test(r)) return "Brand / Franchise";
  if (/commercial|sales|ir|investor/.test(r)) return "Commercial Leadership";
  if (/operator|operations/.test(r)) return "Operations";
  if (/corporate|ceo|cfo|chairman|vice/.test(r)) return "Corporate Leadership";
  return "Other Relevant";
}

function findingStatusFromConfidence(confidence, verified) {
  const c = String(confidence || "").toUpperCase();
  if (verified === true || c === "VERIFIED" || c === "HIGH") {
    return verified === true || c === "VERIFIED" ? "VERIFIED_INTELLIGENCE" : "RESEARCH_FINDING";
  }
  if (c === "NOT_VERIFIED" || c === "UNKNOWN") return "UNRESOLVED";
  return "RESEARCH_FINDING";
}

/**
 * Build claim candidates for Packet 2.5A handoff (no auto-accept).
 */
export function buildClaimCandidatesFromDeep(deep) {
  const candidates = [];
  const chain = deep.ownership_chain || [];
  for (const node of chain) {
    if (!node?.name || node.role === "hotel" || node.role === "ubo") continue;
    candidates.push({
      candidate_id: `dossier_claim_${node.role}_${String(node.name).slice(0, 24)}`,
      claim_type_hint:
        node.role === "propco"
          ? "LEGAL_PROPERTY_OWNER"
          : node.role === "economic_owner"
            ? "ECONOMIC_OWNER"
            : node.role === "former_co_owner"
              ? "HISTORICAL_OWNER"
              : "OWNERSHIP_RELATION",
      subject: deep.identity_disambiguation?.focus_hotel || "Krystal Grand Puerto Vallarta",
      object: node.name,
      status: "CANDIDATE",
      confidence: node.confidence || null,
      auto_promote: false,
      note: node.note || null,
      source_refs: ["kgpv_deep_research_v1"],
    });
  }
  return candidates;
}

/**
 * @param {object} [deep] deep-research JSON
 * @param {object} [options]
 */
export function adaptKgpvDeepResearchToDossier(deep, options = {}) {
  if (!deep) deep = loadDeepResearch(options.deepResearchPath);
  if (!deep) {
    throw new Error("KGPV deep research fixture not found");
  }

  const hotelName =
    deep.identity_disambiguation?.focus_hotel?.split("(")[0]?.trim() ||
    "Krystal Grand Puerto Vallarta";
  const sources = (deep.sources || []).map((s, i) => ({
    id: s.id || `src_${i + 1}`,
    number: i + 1,
    title: s.title || s.id || "Source",
    publisher: s.publisher || s.provider || null,
    date: s.date || s.observed_date || null,
    source_type: s.type || s.source_type || null,
    url: s.url || null,
    claims_supported: s.claims_supported || s.supports || [],
  }));

  const sourceById = Object.fromEntries(sources.map((s) => [s.id, s]));

  const keyFindings = [
    {
      id: "kf_gsf_controls",
      headline: "GSF controls the property",
      explanation:
        "Grupo Hotelero Santa Fe is supported as both economic owner and operator of Krystal Grand Puerto Vallarta from securities disclosures and first-party portfolio evidence.",
      status: "VERIFIED_INTELLIGENCE",
      citation_ids: ["gsf_annual_report_2025"],
    },
    {
      id: "kf_ihvsf_propco",
      headline: "IHVSF is the identified property vehicle",
      explanation:
        "Inmobiliaria en Hotelería Vallarta Santa Fe (IHVSF) is identified as the PropCo / property vehicle with HIGH securities-disclosure support; not treated as a deed scan substitute.",
      status: "RESEARCH_FINDING",
      citation_ids: ["gsf_annual_report_2025"],
    },
    {
      id: "kf_chartwell_historical",
      headline: "Chartwell's ownership involvement is historical",
      explanation:
        "Grupo Chartwell co-developed and co-owned the hotel until GSF acquired the remaining 50% in September 2014. Chartwell remains relevant as adjacent-asset owner and GSF shareholder context.",
      status: "VERIFIED_INTELLIGENCE",
      citation_ids: ["gsf_annual_report_2025"],
    },
    {
      id: "kf_identity_split",
      headline: "Krystal Grand and Krystal Resort are separate properties",
      explanation:
        deep.identity_disambiguation?.note ||
        "Krystal Grand Puerto Vallarta (GSF-owned) must not be conflated with adjacent Chartwell-owned Krystal Resort Puerto Vallarta (GSF-managed).",
      status: "VERIFIED_INTELLIGENCE",
      citation_ids: ["gsf_annual_report_2025", "gsf_expansion_2016_third_party"],
    },
    {
      id: "kf_breathless_announced",
      headline: "Breathless is announced, not the current affiliation",
      explanation:
        deep.brand_resolution?.resolution ||
        "CURRENT trading brand is Krystal Grand. Breathless Puerto Vallarta is an ANNOUNCED Hyatt + GSF project and must not be treated as completed reflag without confirmation.",
      status: "RESEARCH_FINDING",
      citation_ids: ["hyatt_newsroom_2024_02_14", "krystal_grand_official_site"],
    },
  ];

  const pursuit = deep.commercial_pursuit || {};
  const execParas = [
    `This Full Hotel Intelligence Investigation reviews ownership, corporate structure, brand and operator history, people, portfolio context, and development implications for ${hotelName}. Research draws on existing Dealality deep-research artifacts and public filings already captured for the golden demo — no new paid research was run for this packet.`,
    deep.brand_resolution?.resolution
      ? `Primary ownership and control conclusions: ${
          (deep.ownership_chain || []).find((n) => n.role === "economic_owner")?.name ||
          "Grupo Hotelero Santa Fe"
        } is supported as economic owner and operator. ${
          (deep.ownership_chain || []).find((n) => n.role === "propco")?.name ||
          "IHVSF"
        } is identified as the property vehicle. ${deep.brand_resolution.resolution}`
      : null,
    deep.identity_disambiguation
      ? `Property identity is material to diligence: ${deep.identity_disambiguation.focus_hotel} is distinct from ${deep.identity_disambiguation.adjacent_distinct_hotel}. ${deep.identity_disambiguation.note}`
      : null,
    pursuit.why_matters ||
      pursuit.summary ||
      "Commercial implications center on owner-operator control inside a listed Mexican hotel company with multi-property leverage in Puerto Vallarta and an announced Hyatt collaboration that must be kept temporally separate from current branding.",
    (deep.research_gaps || []).length
      ? `Material open questions remain — including natural-person UBO beyond disclosed principals, legal/franchise signatory parties, and whether the announced Breathless project converts this asset, adjacent land, or a separate development. Unresolved items are listed explicitly rather than inferred.`
      : null,
  ].filter(Boolean);

  const chain = deep.ownership_chain || [];
  const people = (deep.people || []).map((p, i) => ({
    id: `person_${i + 1}`,
    name: p.name,
    title: p.title || null,
    organization: p.organization || null,
    group: mapPeopleCategory(p.role_category || p.category),
    role_category: p.role_category || p.category || null,
    relationship_to_hotel: p.relationship_to_hotel || null,
    strategic_relevance: p.strategic_relevance || null,
    decision_authority: p.decision_authority || "Authority Not Verified",
    legal_signing_authority: p.legal_signing_authority || "Authority Not Verified",
    contact_verified: Boolean(p.contact_verified),
    contact: p.contact || null,
    confidence: p.confidence || null,
    status: findingStatusFromConfidence(p.confidence, p.person_verified && p.title_verified),
    citation_ids: p.sources || [],
    authority_note: p.authority_note || null,
  }));

  const events = (deep.property_history || []).map((e, i) => ({
    id: `evt_${i + 1}`,
    date: e.date || null,
    event: e.event || e.label || null,
    parties: e.parties || null,
    change: e.change || null,
    confidence: e.confidence || null,
    citation_ids: e.source_ids || [],
  }));

  const entities = (deep.organizations || []).map((o, i) => ({
    id: o.slug || `ent_${i + 1}`,
    name: o.name,
    slug: o.slug || null,
    relationships: o.relationships || [],
    note: o.note || null,
    ticker: o.ticker || null,
    website: o.website || null,
    hq: o.hq || null,
    canonical: Boolean(o.slug === "grupo-hotelero-santa-fe"),
  }));

  const relationships = (deep.relationships || []).map((r, i) => ({
    id: `rel_${i + 1}`,
    from: r.from,
    type: r.type,
    to: r.to,
    confidence: r.confidence || null,
    note: r.note || null,
    temporal:
      /PREVIOUSLY|FORMER|HISTORICAL/i.test(String(r.type || "") + String(r.note || ""))
        ? "historical"
        : "current",
  }));

  const openQuestions = (deep.research_gaps || []).map((q, i) => ({
    id: `oq_${i + 1}`,
    question: typeof q === "string" ? q : q.question || String(q),
    why_it_matters:
      (typeof q === "object" && q.why_it_matters) ||
      "Affects outreach confidence, legal diligence, or brand/operator decision framing.",
    what_research_established:
      (typeof q === "object" && q.what_research_established) ||
      "Public filings and first-party materials establish corporate ownership/operator context but not this specific resolution.",
    what_remains_unknown:
      (typeof q === "object" && q.what_remains_unknown) ||
      (typeof q === "string" ? q : "Not established from the existing research corpus."),
    future_hook: "RESEARCH_THIS_QUESTION",
    status: "UNRESOLVED",
  }));

  const sections = [
    section("property_identity", [
      {
        type: "paragraphs",
        paragraphs: [
          deep.identity_disambiguation?.focus_hotel
            ? `Focus hotel: ${deep.identity_disambiguation.focus_hotel}.`
            : null,
          deep.identity_disambiguation?.adjacent_distinct_hotel
            ? `Adjacent distinct hotel: ${deep.identity_disambiguation.adjacent_distinct_hotel}.`
            : null,
          deep.identity_disambiguation?.note || null,
          deep.census_claim_preserved
            ? `Census Affiliation preserved as evidence only: ${deep.census_claim_preserved.value} (${deep.census_claim_preserved.treatment}). Product override: ${deep.census_claim_preserved.product_override}.`
            : null,
        ].filter(Boolean),
      },
      {
        type: "table",
        headers: ["Status", "Brand", "Date / Label", "Confidence"],
        rows: (deep.brand_chronology || []).map((b) => [
          b.status || "—",
          b.brand || "—",
          `${b.date || "—"} · ${b.label || ""}`.trim(),
          b.confidence || "—",
        ]),
      },
    ]),
    section("ownership_corporate_structure", [
      {
        type: "table",
        headers: ["Role", "Entity", "Status", "Notes"],
        rows: chain
          .filter((n) => n.role !== "hotel")
          .map((n) => [
            String(n.role || "").replace(/_/g, " "),
            n.name || "Not verified",
            n.status || n.confidence || "—",
            n.note || "—",
          ]),
      },
    ]),
    section("corporate_structure_trace", [
      {
        type: "trace",
        nodes: chain
          .filter((n) => n.name || n.role === "ubo")
          .map((n) => ({
            role: n.role,
            name: n.name || "Natural-person UBO not verified",
            status: n.status || n.confidence,
            note: n.note || null,
          })),
      },
      {
        type: "paragraphs",
        paragraphs: [
          "Control flows from the hotel through the PropCo vehicle to GSF as economic owner / public company, with Chartwell as historical co-owner and related shareholder context — not as current PropCo for this hotel.",
        ],
      },
    ]),
    section("ownership_history_transactions", [
      {
        type: "table",
        headers: ["Date", "Event", "Evidence"],
        rows: (deep.property_history || []).map((e) => [
          e.date || "—",
          e.event || "—",
          (e.source_ids || []).join(", ") || e.confidence || "—",
        ]),
      },
    ]),
    section("brand_operator_history", [
      {
        type: "paragraphs",
        paragraphs: [
          deep.brand_resolution
            ? `CURRENT: ${deep.brand_resolution.current_trading_brand} (${deep.brand_resolution.current_status}). Census Affiliation ${deep.brand_resolution.census_affiliation} treated as ${deep.brand_resolution.census_status}.`
            : null,
          deep.brand_resolution?.resolution || null,
          "Operator: Grupo Hotelero Santa Fe — owned-and-operated for this hotel. Adjacent Krystal Resort PV remains Chartwell-owned and GSF-managed.",
        ].filter(Boolean),
      },
      {
        type: "table",
        headers: ["Temporal status", "Brand", "Label", "Confidence"],
        rows: (deep.brand_chronology || []).map((b) => [
          b.status || "—",
          b.brand || "—",
          b.label || "—",
          b.confidence || "—",
        ]),
      },
    ]),
    section("organization_portfolio", [
      {
        type: "paragraphs",
        paragraphs: (deep.organizations || [])
          .filter((o) => o.slug === "grupo-hotelero-santa-fe")
          .map(
            (o) =>
              `${o.name}${o.ticker ? ` (${o.ticker})` : ""}. HQ: ${o.hq || "—"}. Website: ${o.website || "—"}.`
          ),
      },
      {
        type: "list",
        items: (deep.organizations || []).map(
          (o) =>
            `${o.name}${o.note ? ` — ${o.note}` : ""}${
              o.relationships?.length ? ` [${o.relationships.join(", ")}]` : ""
            }`
        ),
      },
    ]),
    section("people_decision_makers", [
      {
        type: "table",
        headers: ["Name", "Title", "Group", "Decision authority", "Signing authority"],
        rows: people.map((p) => [
          p.name,
          p.title || "—",
          p.group,
          p.decision_authority,
          p.legal_signing_authority,
        ]),
      },
      {
        type: "paragraphs",
        paragraphs: [
          "Titles are not treated as legal or franchise signing authority. Authority fields remain Not Verified unless independently established.",
        ],
      },
    ]),
    section("asset_capital_development", [
      {
        type: "paragraphs",
        paragraphs: [
          "Public filings support development from ~2011, 2012 Hilton opening, 2014 full GSF ownership, 2018 Hacienda expansion to ~451 rooms, and ongoing renovation phasing reported by distribution partners.",
          deep.corporate_contacts?.property_website
            ? `Property website: ${deep.corporate_contacts.property_website}.`
            : null,
        ].filter(Boolean),
      },
    ]),
    section("development_pursuit_implications", [
      {
        type: "paragraphs",
        paragraphs: [
          pursuit.who_controls_relationship || null,
          pursuit.who_influences_brand_operator || null,
          pursuit.approach_organization || null,
          pursuit.portfolio_leverage ||
            "GSF owns this hotel and manages adjacent Chartwell-owned Krystal Resort PV — multi-asset leverage without collapsing ownership.",
          pursuit.timing_signals ||
            "Recent Altitude→Grand rebrand; announced Breathless PV 2025 with GSF; ongoing renovations.",
          pursuit.missing_before_outreach ||
            "Confirm property GM tenure; confirm Breathless project site vs this hotel before brand outreach framing.",
          pursuit.relationship_path || null,
        ].filter(Boolean),
      },
    ]),
    section("unresolved_questions", [
      {
        type: "table",
        headers: ["Question", "Why it matters", "What remains unknown"],
        rows: openQuestions.map((q) => [q.question, q.why_it_matters, q.what_remains_unknown]),
      },
    ]),
    section("sources_evidence", [
      {
        type: "table",
        headers: ["#", "Title", "Publisher", "Date", "Type"],
        rows: sources.map((s) => [
          String(s.number),
          s.title,
          s.publisher || "—",
          s.date || "—",
          s.source_type || "—",
        ]),
      },
    ]),
    section("research_notes", [
      {
        type: "paragraphs",
        paragraphs: [
          `Research status on source artifact: ${deep.research_status || "—"}.`,
          deep.webhound_session_id
            ? "Provider session metadata is retained for internal methodology only and is not product branding."
            : null,
          "Limitations: natural-person UBO, deed-level signatories, and Breathless site specificity remain open. No chain-of-thought is stored.",
        ].filter(Boolean),
      },
    ]),
  ].filter(Boolean);

  const dossier = createEmptyDossier({
    dossier_id: options.dossierId || "dossier_kgpv_full_hi_v1",
    hotel_id: deep.hotel_id || "dhl_06G6AB2228339A12S2EZ7PZF5E",
    hotel_airtable_record_id: deep.hotel_airtable_record_id || "recUNycnMwOVFX0hc",
    hotel_name: hotelName,
    title: DOSSIER_TITLE,
    status:
      String(deep.research_status || "").includes("PARTIAL") ? "PARTIAL" : "COMPLETED",
    research_provider: "dealality_normalized",
    research_run_id: deep.version || "kgpv-deep-research-v3",
    version: 1,
    created_at: deep.observed_date ? `${deep.observed_date}T12:00:00.000Z` : undefined,
    started_at: deep.observed_date ? `${deep.observed_date}T12:00:00.000Z` : undefined,
    completed_at: deep.observed_date ? `${deep.observed_date}T18:00:00.000Z` : undefined,
    research_cost_usd: deep.webhound_budget_usd != null ? Number(deep.webhound_budget_usd) : null,
    executive_summary: { paragraphs: execParas },
    key_findings: keyFindings,
    sections,
    findings: keyFindings,
    entities,
    relationships,
    people,
    events,
    open_questions: openQuestions,
    sources,
    research_methods: [
      "Securities / annual report review",
      "First-party brand and hotel site identity checks",
      "Adjacent-asset collision guard",
      "People title verification without authority inference",
    ],
    methodology_summary:
      "Normalized from existing KGPV deep-research artifact. Substantive findings preserved; formatting and Dealality terminology applied. No second-pass AI rewrite.",
    raw_artifact_reference: {
      kind: "fixture",
      path: "fixtures/golden-demo/krystal-grand-pv-deep-research-v1.json",
      provider_internal: deep.webhound_session_id ? "webhound_session_retained_internal" : null,
      immutable: true,
    },
    mapping_notes: [
      "Webhound proprietary chat/transcript structure is not exposed in the dossier UI.",
      "Source list may be thinner than raw provider export when the fixture omits full bibliography rows.",
      "Finding status VERIFIED_INTELLIGENCE is used only where golden-demo / product truth already treats the fact as validated; otherwise RESEARCH_FINDING.",
      "claim_handoff.auto_promote is always false.",
    ],
    claim_handoff: {
      auto_promote: false,
      candidates: buildClaimCandidatesFromDeep(deep),
    },
  });

  // Attach citation numbers onto key findings where possible
  for (const kf of dossier.key_findings) {
    kf.citations = (kf.citation_ids || [])
      .map((id) => sourceById[id])
      .filter(Boolean)
      .map((s) => s.number);
  }

  return refreshDossierCounts(dossier);
}

/**
 * Thin native research-dossier adapter — fills methodology when present.
 * Primary narrative still comes from deep-research fixture for KGPV demo.
 */
export function adaptNativeResearchDossierMeta(nativeDossier) {
  if (!nativeDossier || typeof nativeDossier !== "object") return null;
  return {
    research_run_id: nativeDossier.run_id || null,
    started_at: nativeDossier.started_at || null,
    completed_at: nativeDossier.finished_at || null,
    research_cost_usd: null,
    duration_ms: nativeDossier.duration_ms ?? null,
    playbooks: nativeDossier.playbooks_invoked || null,
    hotel_airtable_record_id: nativeDossier.starting_seed?.airtable_record_id || null,
    hotel_id: nativeDossier.starting_seed?.hotel_id || null,
    raw_artifact_reference: {
      kind: "native_research_dossier",
      version: nativeDossier.version || null,
      immutable: true,
    },
  };
}

export function loadAndAdaptKgpvDossier(options = {}) {
  return adaptKgpvDeepResearchToDossier(null, options);
}
