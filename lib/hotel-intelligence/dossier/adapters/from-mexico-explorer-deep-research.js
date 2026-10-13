/**
 * Mexico Explorer Full HI dossiers — deep-research fixtures → customer-safe dossier.
 * No paid Webhound rerun. auto_promote=false. Operator ≠ owner.
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
import { enrichReportForPublishing } from "../report-hotel-identity.js";
import {
  SHERATON_GDL_AIRTABLE_ID,
  REAL_INN_CANCUN_AIRTABLE_ID,
} from "../../ownership/golden-demo/mexico-explorer-demo-cohort.js";
import { enforceClientSafeCustomerSurfaces } from "../client-safe/validate-client-safe.js";
import { customerOwnershipRoleLabel } from "../client-safe/relationship-labels.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../../..");

export const SHERATON_DOSSIER_ID = "dossier_sheraton_gdl_expo_full_hi_v1";
export const REAL_INN_DOSSIER_ID = "dossier_real_inn_cancun_full_hi_v1";

export const MEXICO_EXPLORER_DOSSIER_META = Object.freeze({
  [SHERATON_GDL_AIRTABLE_ID]: {
    dossier_id: SHERATON_DOSSIER_ID,
    fixture_file: "sheraton-gdl-expo-full-hotel-intelligence-investigation-v1.json",
    deep_path: "fixtures/golden-demo/sheraton-gdl-expo-deep-research-v1.json",
    hotel_name: "Sheraton Guadalajara Expo",
    request_id: "req_sheraton_gdl_full_hi_run1",
    run_id: "run_sheraton_gdl_full_hi_run1",
    raw_artifact_id: "art_sheraton_gdl_webhound_e4a61251",
    normalized_research_id: "norm_sheraton_gdl_full_hi_v1",
  },
  [REAL_INN_CANCUN_AIRTABLE_ID]: {
    dossier_id: REAL_INN_DOSSIER_ID,
    fixture_file: "real-inn-cancun-full-hotel-intelligence-investigation-v1.json",
    deep_path: "fixtures/golden-demo/real-inn-cancun-deep-research-v1.json",
    hotel_name: "voco Cancún Zona Hotelera",
    request_id: "req_real_inn_cancun_full_hi_run1",
    run_id: "run_real_inn_cancun_full_hi_run1",
    raw_artifact_id: "art_real_inn_cancun_webhound_799762c4",
    normalized_research_id: "norm_real_inn_cancun_full_hi_v1",
  },
});

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
  if (/property leadership|director general|\bgm\b/.test(r)) return "Property Leadership";
  if (/asset/.test(r)) return "Asset Management";
  if (/legal|governance/.test(r)) return "Legal / Governance";
  if (/brand|franchise/.test(r)) return "Brand / Franchise";
  if (/commercial|sales|ir|investor/.test(r)) return "Commercial Leadership";
  if (/operator|operations/.test(r)) return "Operations";
  if (/corporate|ceo|cfo|chairman|vice|president/.test(r)) return "Corporate Leadership";
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

function chainName(deep, role) {
  return (deep.ownership_chain || []).find((n) => n.role === role)?.name || null;
}

function chainNote(deep, role) {
  return (deep.ownership_chain || []).find((n) => n.role === role)?.note || null;
}

/**
 * Open questions must use recommend.js keyword cues so Research Center
 * shows Recommended Follow-up cards (brand/operator, deed/authority, capital, etc.).
 */
function buildOpenQuestions(deep, hotelKey) {
  const curated =
    hotelKey === SHERATON_GDL_AIRTABLE_ID
      ? [
          {
            id: "oq_sg_deed_title",
            question:
              "What does the Jalisco Registro Público deed / title extract show for Av. Mariano Otero 1510 (holder, folio, mortgages)?",
            why_it_matters:
              "Confirms PropCo title posture and any registered capital encumbrances before ownership outreach.",
          },
          {
            id: "oq_sg_signing_authority",
            question:
              "Who are the current legal/notarial deed signatories and decision authority for Inmobiliaria HNF after Patricia Olivia Newton Frausto's death?",
            why_it_matters:
              "Titles and historical chair evidence are not substitutes for current signing authority.",
          },
          {
            id: "oq_sg_franchise",
            question:
              "Which entity is the Marriott/Sheraton franchise agreement counterparty for Guadalajara Expo?",
            why_it_matters:
              "Separates PropCo ownership from brand-contract economics and reflag constraints.",
          },
          {
            id: "oq_sg_operator_agreement",
            question:
              "What is the Aimbridge management / operator agreement effective date and CAPITALI exit date?",
            why_it_matters:
              "Locks the operator transition timeline beyond GM LinkedIn timing clues.",
          },
          {
            id: "oq_sg_portfolio",
            question:
              "Are Aloft Guadalajara Country Club and Delta Hotels Puebla confirmed as HNF-controlled sibling portfolio hotels under the same ownership group?",
            why_it_matters:
              "Supports multi-asset owner leverage without collapsing operator identity.",
          },
        ]
      : [
          {
            id: "oq_ri_voco_conversion",
            question:
              "What is the current voco Cancún conversion / opening status versus the announced/pre-opening posture?",
            why_it_matters:
              "Distinguishes marketed IHG brand from live guest operations and Real Inn wind-down.",
          },
          {
            id: "oq_ri_propco_deed",
            question:
              "What is the exact Mexican PropCo / title / deed vehicle (or fideicomiso) for the Cancún Hotel Zone asset?",
            why_it_matters:
              "Alliance is publicly named as portfolio owner; deed-level vehicle remains unresolved.",
          },
          {
            id: "oq_ri_decision_authority",
            question:
              "What is the legal bridge and decision / signing authority path between Miguel & Carlos Justo and Alliance Hotel Management?",
            why_it_matters:
              "Press buyer names are not registry-verified UBO or deed signatories.",
          },
          {
            id: "oq_ri_capital",
            question:
              "What mortgage, lien, or financing package is registered against the Cancún asset or its holding vehicle?",
            why_it_matters:
              "Capital structure affects refinance, sale, and reflag execution risk.",
          },
          {
            id: "oq_ri_brand_operator",
            question:
              "What are the hotel-specific Aimbridge management agreement and IHG franchise / brand agreement terms?",
            why_it_matters:
              "Operator and brand contracts are announced at portfolio level, not yet obtained hotel-by-hotel.",
          },
          {
            id: "oq_ri_gm_contact",
            question:
              "Who is the current property GM / on-site contact path for ownership or operator outreach?",
            why_it_matters:
              "No verified property GM contact was established in the Full HI corpus.",
          },
        ];

  const fromGaps = (deep.research_gaps || []).map((q, i) => {
    const text = typeof q === "string" ? q : q.question || String(q);
    return {
      id: `oq_gap_${i + 1}`,
      question: text,
      why_it_matters:
        (typeof q === "object" && q.why_it_matters) ||
        "Affects outreach confidence, legal diligence, or brand/operator decision framing.",
      what_research_established:
        "Public Full HI corpus established corporate, brand, and operator context but not this specific resolution.",
      what_remains_unknown: text,
      future_hook: "RESEARCH_THIS_QUESTION",
      status: "OPEN",
      priority: "MEDIUM",
    };
  });

  const primary = curated.map((q) => ({
    ...q,
    what_research_established:
      "Full Hotel Intelligence Investigation established public ownership/operator/brand context but left this unresolved.",
    what_remains_unknown: q.question,
    future_hook: "RESEARCH_THIS_QUESTION",
    status: "OPEN",
    priority: "HIGH",
  }));

  // Prefer curated (matchable) first; keep unique gap text that isn't already covered.
  const seen = new Set(primary.map((q) => q.question.toLowerCase().slice(0, 80)));
  for (const g of fromGaps) {
    const key = g.question.toLowerCase().slice(0, 80);
    if (seen.has(key)) continue;
    seen.add(key);
    primary.push(g);
  }
  return primary;
}

function buildKeyFindings(deep, hotelKey) {
  const propco = chainName(deep, "propco");
  const econ = chainName(deep, "economic_owner") || chainName(deep, "sponsor_principals");
  const op = deep.operator_resolution?.current_operator?.name || "Aimbridge LATAM";
  const brand = deep.brand_resolution?.current_trading_brand || deep.brand_resolution?.current_brand;

  if (hotelKey === SHERATON_GDL_AIRTABLE_ID) {
    return [
      {
        id: "kf_sg_propco",
        headline: "Inmobiliaria HNF is the strongest public PropCo reading",
        explanation:
          propco && chainNote(deep, "propco")
            ? `${propco}. ${chainNote(deep, "propco")}`
            : "PROFECO / project-era evidence supports Inmobiliaria HNF as the property vehicle; deed folio not independently extracted.",
        status: "RESEARCH_FINDING",
      },
      {
        id: "kf_sg_operator",
        headline: "Aimbridge LATAM currently operates the hotel",
        explanation:
          deep.operator_resolution?.current_operator?.note ||
          `${op} is listed on Aimbridge LATAM property pages and portfolio materials.`,
        status: "VERIFIED_INTELLIGENCE",
      },
      {
        id: "kf_sg_brand",
        headline: "CURRENT trading brand is Sheraton / Marriott",
        explanation: deep.brand_resolution?.resolution || `${brand} is the current Marriott flag (GDLSE).`,
        status: "VERIFIED_INTELLIGENCE",
      },
      {
        id: "kf_sg_owner_sphere",
        headline: "Economic ownership sits in the HNF / Newton family sphere (PROBABLE)",
        explanation:
          chainNote(deep, "economic_owner") ||
          "Patricia Olivia Newton Frausto is evidenced historically; current UBO / member register not obtained.",
        status: "RESEARCH_FINDING",
      },
      {
        id: "kf_sg_history",
        headline: "HS HOTSSON → interim HNF identity → Sheraton conversion path is evidenced",
        explanation:
          deep.property_profile?.physical_history ||
          "Opened as HS HOTSSON (2022), interim Gran Hotel Expo by HNF (PROBABLE), rebranded Sheraton in 2024.",
        status: "RESEARCH_FINDING",
      },
      {
        id: "kf_sg_separation",
        headline: "Owner, operator, and brand are separate roles",
        explanation:
          "PropCo/HNF ownership evidence, Aimbridge LATAM operations, and Marriott/Sheraton branding must not be collapsed into a single party.",
        status: "VERIFIED_INTELLIGENCE",
      },
    ];
  }

  return [
    {
      id: "kf_ri_owner",
      headline: "Alliance Hotel Management is the publicly named package owner",
      explanation:
        chainNote(deep, "economic_owner") ||
        `${econ || "Alliance Hotel Management"} is named across IHG/trade coverage for the six-hotel Mexico package including Cancún.`,
      status: "RESEARCH_FINDING",
    },
    {
      id: "kf_ri_propco_gap",
      headline: "Mexican PropCo / title vehicle remains UNKNOWN",
      explanation:
        chainNote(deep, "propco") ||
        "Quintana Roo registry / fideicomiso extract was not obtained in this Full HI pass.",
      status: "UNRESOLVED",
    },
    {
      id: "kf_ri_operator",
      headline: "Aimbridge LATAM is the selected operator for the conversion portfolio",
      explanation:
        deep.operator_resolution?.current_operator?.note ||
        `${op} was selected to manage the six-hotel Alliance/IHG Mexico portfolio; Cancún in Phase 1.`,
      status: "VERIFIED_INTELLIGENCE",
    },
    {
      id: "kf_ri_brand",
      headline: "voco is announced/pre-opening; Real Inn is FORMER",
      explanation: deep.brand_resolution?.resolution || String(brand || ""),
      status: "RESEARCH_FINDING",
    },
    {
      id: "kf_ri_justo",
      headline: "Justo brothers are PROBABLE economic buyers — legal bridge unresolved",
      explanation:
        chainNote(deep, "sponsor_principals") ||
        "Mexican press names Miguel and Carlos Justo as buyers; Alliance legal bridge UNKNOWN.",
      status: "RESEARCH_FINDING",
    },
    {
      id: "kf_ri_separation",
      headline: "Alliance owns the package; Aimbridge operates; IHG brands",
      explanation:
        "Do not treat Alliance as the on-property operator. Aimbridge LATAM manages; IHG/voco is the brand path.",
      status: "VERIFIED_INTELLIGENCE",
    },
  ];
}

export function buildClaimCandidatesFromMexicoDeep(deep) {
  const candidates = [];
  for (const node of deep.ownership_chain || []) {
    if (!node?.name || node.role === "hotel" || node.role === "ubo") continue;
    candidates.push({
      candidate_id: `dossier_claim_${node.role}_${String(node.name).slice(0, 24)}`,
      claim_type_hint:
        node.role === "propco"
          ? "LEGAL_PROPERTY_OWNER"
          : node.role === "economic_owner"
            ? "ECONOMIC_OWNER"
            : "OWNERSHIP_RELATION",
      subject: deep.property_profile?.canonical_name || deep.identity_disambiguation?.focus_hotel,
      object: node.name,
      status: "CANDIDATE",
      confidence: node.confidence || null,
      auto_promote: false,
      note: node.note || null,
      source_refs: [deep.version || "mexico_explorer_deep_research"],
    });
  }
  return candidates;
}

function expandMexicoExplorerSources(deep) {
  const rows = [];
  const seen = new Set();
  function push(s) {
    const url = String(s?.url || "").trim();
    const key = url || `${s?.title || ""}|${s?.publisher || s?.provider || ""}|${s?.id || ""}`;
    if (!key || seen.has(key)) return;
    seen.add(key);
    rows.push({
      id: s.id || `src_${rows.length + 1}`,
      number: rows.length + 1,
      title: s.title || s.id || "Source",
      publisher: s.publisher || s.provider || null,
      date: s.date || s.observed_date || deep.observed_date || null,
      source_type: s.type || s.source_type || s.authority || null,
      url: url || null,
      claims_supported: s.claims_supported || s.supports || [],
    });
  }
  for (const s of deep.sources || []) push(s);
  for (const p of deep.people || []) {
    const url = p.linkedin_url || p.professional_profile_url || p.professional_profile?.url;
    if (url) {
      push({
        title: `Professional profile — ${p.name}`,
        provider: "linkedin",
        authority: "professional_profile",
        url,
        observed_date: deep.observed_date,
      });
    }
  }
  if (deep.corporate_contacts?.property_website) {
    push({
      title: "Property website (corporate contacts)",
      provider: "property",
      authority: "first_party",
      url: deep.corporate_contacts.property_website,
      observed_date: deep.observed_date,
    });
  }
  if (deep.corporate_contacts?.operator_website) {
    push({
      title: "Operator website (corporate contacts)",
      provider: "operator",
      authority: "first_party_operator",
      url: deep.corporate_contacts.operator_website,
      observed_date: deep.observed_date,
    });
  }
  // Provider session URLs are internal — never customer bibliography rows.
  if (deep.webhound_url) {
    /* retained on deep research only; not exposed in customer sources */
  }
  push({
    title: "Compiled investigation notes (ownership / operator / brand package)",
    provider: "dealality",
    authority: "compiled_research",
    id: `src_compiled_${deep.version || "mexico"}`,
    observed_date: deep.observed_date,
  });
  return rows.map((r, i) => ({ ...r, number: i + 1, id: r.id || `src_${i + 1}` }));
}

function buildMexicoFullHiSections(deep, hotelKey, openQuestions, sources, people) {
  const chain = deep.ownership_chain || [];
  const op = deep.operator_resolution || {};
  const profile = deep.property_profile || {};
  const pursuit = deep.commercial_pursuit || {};
  const contacts = deep.corporate_contacts || {};
  const brand = deep.brand_resolution || {};

  const identityParas = [
    `This chapter establishes the focus hotel for the Full Hotel Intelligence Investigation and guards against adjacent-asset confusion in the same market corridor.`,
    deep.identity_disambiguation?.focus_hotel
      ? `Focus hotel: ${deep.identity_disambiguation.focus_hotel}.`
      : `Focus hotel: ${profile.canonical_name || "subject hotel"}.`,
    deep.identity_disambiguation?.note || null,
    profile.address
      ? `Published address evidence supports ${profile.address}${profile.address_note ? ` (${profile.address_note})` : ""}.`
      : null,
    profile.rooms_suites != null
      ? `Room inventory is assessed at ${profile.rooms_suites} rooms/suites with ${profile.rooms_confidence || "stated"} confidence (${profile.rooms_basis || "public listings"}).`
      : null,
    profile.hotel_type || profile.positioning
      ? `Product posture: ${[profile.hotel_type, profile.positioning].filter(Boolean).join(" — ")}.`
      : null,
    profile.physical_history ? `Physical / identity history summary: ${profile.physical_history}` : null,
    profile.website ? `Primary public web channel: ${profile.website}.` : null,
    contacts.property_phone || contacts.property_email
      ? `Property contact channels evidenced in the compile: telephone ${contacts.property_phone || "not established"}; email ${contacts.property_email || "not established"}.`
      : null,
    `Prior recorded affiliation is preserved as evidence context only and is not silently overwritten when research shows a FORMER or ANNOUNCED brand posture.`,
  ].filter(Boolean);

  const ownershipParas = [
    `Ownership diligence separates the hotel asset, PropCo / title vehicle, economic owner or package owner, sponsor principals, and natural-person UBO. Operator and brand are not treated as ownership.`,
    ...chain
      .filter((n) => n.role !== "hotel")
      .map(
        (n) =>
          `${String(n.role || "").replace(/_/g, " ").toUpperCase()}: ${n.name || "Not verified"} — status ${n.status || n.confidence || "—"}.${n.note ? ` ${n.note}` : ""}`
      ),
    hotelKey === SHERATON_GDL_AIRTABLE_ID
      ? `For Sheraton Guadalajara Expo, the strongest public PropCo reading is Inmobiliaria HNF, S. de R.L. de C.V. (RFC IHN120620UZ3), supported by project-era investor references and PROFECO RPCA adhesion contract 5805-2025 for event-hall rental contracting tied to the property. Deed folio confirmation for Av. Mariano Otero 1510 remains an open registry item.`
      : `For Real Inn Cancun / voco Cancún Zona Hotelera, Alliance Hotel Management is the publicly named package owner across IHG and trade coverage, while the Mexican title-holding PropCo / fideicomiso remains UNKNOWN from Quintana Roo registry extracts in this pass.`,
    `Natural-person UBO is not asserted. Probable sponsor or buyer principals are labeled PROBABLE where press names exist without registry confirmation.`,
    contacts.corporate_phone || contacts.business_email
      ? `Corporate / business contact path on the ownership compile: ${contacts.corporate_phone || "—"}; ${contacts.business_email || "—"}. Where Aimbridge emails appear on HNF PROFECO filings, they are disclosed as operator-operated contact paths on a PropCo instrument — not as proof that Aimbridge is the owner.`
      : null,
  ].filter(Boolean);

  const operatorParas = [
    `Operator diligence distinguishes current third-party management from former operators and from brand licensors.`,
    op.current_operator
      ? `CURRENT operator: ${op.current_operator.name} (${op.current_operator.status || "CURRENT"}; confidence ${op.current_operator.confidence || "—"}). ${op.current_operator.note || ""}`
      : "CURRENT operator was not established in the deep-research artifact.",
    op.former_operator
      ? `FORMER operator: ${op.former_operator.name} (${op.former_operator.status || "FORMER"}; confidence ${op.former_operator.confidence || "—"}). ${op.former_operator.note || ""}`
      : null,
    `Aimbridge LATAM is a third-party management platform. Its presence on property pages and portfolio PDFs supports operated / managed relationships and does not convert Aimbridge into the economic owner or property company.`,
    hotelKey === SHERATON_GDL_AIRTABLE_ID
      ? `Sheraton-specific timing clue: General Manager Julieta Fregoso's professional profile shifts from HS HOTSSON GM through April 2024 to Sheraton Guadalajara Expo GM at Aimbridge LATAM from April 2024 — a strong operator-transition signal, not a substitute for the management agreement instrument.`
      : `Cancún-specific operator context: trade coverage states Aimbridge LATAM was selected to manage the six-hotel Alliance/IHG Mexico portfolio with Cancún in Phase 1. Hotel-specific HMA economics and effective dates remain unresolved.`,
    contacts.operator_website
      ? `Operator corporate surface used for diligence navigation: ${contacts.operator_website}.`
      : null,
  ].filter(Boolean);

  const brandParas = [
    `Brand diligence keeps CURRENT, FORMER, and ANNOUNCED statuses temporally separate. Announced reflags are never treated as completed affiliations without completion evidence.`,
    brand.resolution || null,
    brand.current_trading_brand
      ? `CURRENT marketed brand: ${brand.current_trading_brand} (${brand.current_status || "—"}) under ${brand.brand_family || "stated brand family"}.`
      : null,
    brand.census_claim_preserved
      ? `Prior recorded affiliation context: Affiliation ${brand.census_claim_preserved.affiliation || "—"} / Parent ${brand.census_claim_preserved.parent_company || "—"}. ${brand.census_claim_preserved.note || ""}`
      : null,
    hotelKey === SHERATON_GDL_AIRTABLE_ID
      ? `Sheraton / Marriott franchise posture is PROBABLE and consistent with recorded franchise labeling, but the franchise agreement counterparty entity was not obtained. Marriott property code GDLSE anchors first-party brand identity.`
      : `voco under IHG is the current brand. The marketed property name is voco Cancún Zona Hotelera (IHG hoteldetail). Former trading name Real Inn Cancun is retained as FORMER identity only.`,
    `Brand chronology rows below are evidence labels only — they do not invent missing contracts.`,
  ].filter(Boolean);

  const historyParas = [
    `Property history reconstructs development, opening, interim identities, operator handoffs, brand conversions, and material F&B or contracting events with explicit confidence labels.`,
    profile.physical_history || null,
    `Each timeline row below is drawn from the normalized deep-research property_history corpus. Where confidence is PROBABLE, the event is retained with disclosure rather than dropped or upgraded.`,
    hotelKey === SHERATON_GDL_AIRTABLE_ID
      ? `Material ownership-side events include Inmobiliaria HNF incorporation (2012), the 2019 project launch, Patricia Olivia Newton Frausto's death (2022), HS HOTSSON opening (Nov 2022), the 2024 Marriott/Aimbridge conversion package, and 2025 PROFECO contracting plus MATRIA inauguration.`
      : `Material commercial events include the reported May 2025 sale of six Real Inn hotels, the September 2025 IHG voco signing with Alliance, November 2025 Aimbridge management selection, and 2026 opening-target revisions while the IHG listing remains in announced/pre-opening posture.`,
  ].filter(Boolean);

  const portfolioParas = [
    `Organization & portfolio diligence maps related assets without collapsing operator portfolios into owner portfolios.`,
    deep.portfolio_notes?.organization
      ? `Organization framing: ${deep.portfolio_notes.organization}.`
      : null,
    deep.portfolio_notes?.framing || null,
    hotelKey === SHERATON_GDL_AIRTABLE_ID
      ? `HNF-linked sibling context includes Aloft Guadalajara Country Club and Gran Hotel de Puebla by HNF / Delta Hotels Puebla Mexico inside the January 2024 Marriott/Aimbridge three-hotel conversion package. Sibling listing supports multi-asset leverage hypotheses but does not, by itself, prove identical title vehicles for each asset.`
      : `Alliance's six-hotel Mexico package is the relevant owner-side portfolio frame. Aimbridge LATAM's wider managed portfolio must not be misread as Alliance-owned inventory. Cancún remains one Phase 1 conversion asset inside that package.`,
    ...(deep.organizations || []).map(
      (o) =>
        `${o.name}${o.slug ? ` (${o.slug})` : ""} — ${(o.relationships || []).join(", ") || "relationship stated in corpus"}${o.note ? `. ${o.note}` : ""}`
    ),
  ].filter(Boolean);

  const peopleParas = [
    `People diligence records evidenced titles and professional profiles without promoting titles into legal signing authority, franchise authority, or deed UBO.`,
    `Authority fields default to Not Verified unless independently established. LinkedIn verification status is disclosed per person.`,
    ...people.map((p) => {
      const li = p.linkedin_url ? ` Profile: ${p.linkedin_url}.` : "";
      return `${p.name} — ${p.title || "title not stated"} at ${p.organization || "—"}. Category: ${p.group}. Decision authority: ${p.decision_authority}. Signing authority: ${p.legal_signing_authority}.${p.relationship_to_hotel ? ` Relationship: ${p.relationship_to_hotel}.` : ""}${li}`;
    }),
    hotelKey === SHERATON_GDL_AIRTABLE_ID
      ? `Ownership-side succession after Patricia Olivia Newton Frausto remains unresolved at the member-register / apoderado level. Family presence at MATRIA launch is a continuity signal, not registry proof.`
      : `Justo brothers appear as PROBABLE buyers in Mexican press; Alliance executives and Aimbridge/IHG development contacts provide operator and brand paths. Property GM contact was NOT_FOUND in this Full HI pass.`,
  ].filter(Boolean);

  const capitalParas = [
    `Transactions, capital, and corporate-event diligence asks what changed hands, what financing or charges exist, and what remains unverified.`,
    hotelKey === SHERATON_GDL_AIRTABLE_ID
      ? `No public sale, refinance, mortgage, or lien instrument for Av. Mariano Otero 1510 was extracted in this pass. Negative web results are treated as NOT VERIFIED / no evidence found — not proof that no encumbrance exists. PROFECO contracting confirms active HNF commercial use of hotel event space but is not a title extract.`
      : `Reported May 2025 transfer of six Real Inn hotels (including Cancún) is retained as press-supported transaction context. Mortgage / lien / financing package on the Cancún asset and the exact Mexican holding vehicle remain open. Fideicomiso versus other structures is unresolved.`,
    contacts.notes || null,
    `Capital follow-up recommendations should target registry charges, facility documents, and deed extracts rather than inferring leverage from operator announcements.`,
  ].filter(Boolean);

  const commercialParas = [
    `Commercial and pursuit intelligence translates verified structure into outreach sequencing without inventing decision makers.`,
    pursuit.why_matters || null,
    pursuit.approach_organization || null,
    pursuit.missing_before_outreach || null,
    hotelKey === SHERATON_GDL_AIRTABLE_ID
      ? `Practical path: property GM and Aimbridge LATAM for operator-side conversations; HNF / Newton ownership-side outreach only after current apoderado and deed confirmation. Do not pitch Marriott as owner or Aimbridge as PropCo.`
      : `Practical path: Alliance owner-side plus Aimbridge LATAM operator-side plus IHG development contacts for brand conversion context. Confirm PropCo and Justo↔Alliance legal bridge before deed-level ownership outreach. Do not treat Alliance as the on-property operator.`,
    `Open questions in the next chapter are the explicit verify-next queue for recommended follow-up investigations.`,
  ].filter(Boolean);

  const openParas = [
    `Open questions are unresolved diligence items retained after the Full Hotel Intelligence Investigation. They drive recommended follow-up investigations and must not be silently closed.`,
    `Each question below includes why it matters and what remains unknown. Later research updates may resolve individual items without rerunning the full investigation.`,
    ...openQuestions.map(
      (q, i) =>
        `${i + 1}. ${q.question} — Why it matters: ${q.why_it_matters || "—"}. Remains unknown: ${q.what_remains_unknown || q.question}`
    ),
  ];

  const sourceParas = [
    `Evidence quality draws on first-party brand and operator sites, government / consumer-protection filings where available, trade press, professional profiles, and the retained Full Hotel Intelligence research corpus.`,
    `Source rows below are the bibliography for this investigation.`,
    `This investigation cites ${sources.length} sources. Findings are research-grade and are not auto-applied to master property records.`,
    null,
  ].filter(Boolean);

  return [
    section("property_identity", [
      { type: "paragraphs", paragraphs: identityParas },
      {
        type: "table",
        headers: ["Status", "Brand", "Date / Label"],
        rows: (deep.brand_chronology || []).map((b) => [
          b.status || "—",
          b.brand || "—",
          `${b.date || "—"} · ${b.label || ""}`.trim(),
        ]),
      },
    ]),
    section("ownership_chain_propco", [
      { type: "paragraphs", paragraphs: ownershipParas },
      {
        type: "trace",
        nodes: chain.map((n) => ({
          role: n.role,
          role_label: customerOwnershipRoleLabel(n.role),
          name: n.name || (n.role === "propco" || n.role === "ubo" ? "Not verified" : "—"),
          status: n.status || n.confidence,
          note: n.note || null,
        })),
      },
      {
        type: "table",
        headers: ["Role", "Entity", "Status", "Notes"],
        rows: chain
          .filter((n) => n.role !== "hotel")
          .map((n) => [
            customerOwnershipRoleLabel(n.role),
            n.name || "Not verified",
            n.status || n.confidence || "—",
            n.note || "—",
          ]),
      },
    ]),
    section("operator_management", [
      { type: "paragraphs", paragraphs: operatorParas },
      {
        type: "table",
        headers: ["Temporal", "Operator", "Confidence", "Notes"],
        rows: [
          op.current_operator
            ? [
                "CURRENT",
                op.current_operator.name,
                op.current_operator.confidence || "—",
                op.current_operator.note || "—",
              ]
            : null,
          op.former_operator
            ? [
                "FORMER",
                op.former_operator.name,
                op.former_operator.confidence || "—",
                op.former_operator.note || "—",
              ]
            : null,
        ].filter(Boolean),
      },
    ]),
    section("brand_reflag", [
      { type: "paragraphs", paragraphs: brandParas },
      {
        type: "table",
        headers: ["Temporal status", "Brand", "Label"],
        rows: (deep.brand_chronology || []).map((b) => [
          b.status || "—",
          b.brand || "—",
          b.label || "—",
        ]),
      },
    ]),
    section("property_history", [
      { type: "paragraphs", paragraphs: historyParas },
      {
        type: "table",
        headers: ["Date", "Event", "Confidence"],
        rows: (deep.property_history || []).map((e) => [
          e.date || "—",
          e.event || "—",
          e.confidence || e.status || "—",
        ]),
      },
    ]),
    section("organization_portfolio", [
      { type: "paragraphs", paragraphs: portfolioParas },
      {
        type: "list",
        items: (deep.portfolio_notes?.assets || []).map(
          (a) =>
            `${a.name}${a.brand ? ` · ${a.brand}` : ""}${a.market ? ` · ${a.market}` : ""}${
              a.rooms != null ? ` · ${a.rooms} rooms` : ""
            }${a.status ? ` · ${a.status}` : ""}`
        ),
      },
    ]),
    section("people_decision_authority", [
      { type: "paragraphs", paragraphs: peopleParas },
      {
        type: "table",
        table_kind: "people",
        headers: [
          "Name",
          "Title",
          "Organization",
          "Role / Relevance",
          "Evidence",
          "Public Contact",
        ],
        rows: people.map((p) => [
          p.name,
          p.title || "—",
          p.organization || "—",
          p.relationship_to_hotel || p.strategic_relevance || p.group || "—",
          p.authority_note ||
            `Decision authority: ${p.decision_authority}. Signing authority: ${p.legal_signing_authority}.`,
          // LinkedIn lives here so the dossier card extractor can promote it to Professional Profile;
          // email/phone (when present) remain as Public Contact after strip.
          [p.professional_profile_url, p.contact].filter(Boolean).join(" · ") || "—",
        ]),
      },
    ]),
    section("transactions_capital", [
      { type: "paragraphs", paragraphs: capitalParas },
      {
        type: "list",
        items: (deep.research_gaps || [])
          .filter((g) => /mortgage|lien|financ|capital|deed|title|propco|fideicomiso/i.test(String(g)))
          .map((g) => (typeof g === "string" ? g : g.question || String(g))),
      },
    ]),
    section("commercial_pursuit", [{ type: "paragraphs", paragraphs: commercialParas }]),
    section("open_questions", [
      { type: "paragraphs", paragraphs: openParas },
      {
        type: "table",
        headers: ["Question", "Why it matters", "What remains unknown"],
        rows: openQuestions.map((q) => [q.question, q.why_it_matters, q.what_remains_unknown]),
      },
    ]),
    section("sources_evidence", [
      { type: "paragraphs", paragraphs: sourceParas },
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
  ].filter(Boolean);
}

/**
 * @param {object} deep Mexico Explorer deep-research JSON
 * @param {object} [options]
 */
export function adaptMexicoExplorerDeepResearchToDossier(deep, options = {}) {
  if (!deep || typeof deep !== "object") {
    throw new Error("mexico_explorer_deep_research_required");
  }
  const hotelKey = String(deep.hotel_airtable_record_id || options.hotelAirtableId || "").trim();
  const meta = MEXICO_EXPLORER_DOSSIER_META[hotelKey];
  if (!meta && !options.dossierId) {
    throw new Error(`unsupported_mexico_explorer_hotel:${hotelKey || "missing"}`);
  }

  const hotelName =
    options.hotelName ||
    meta?.hotel_name ||
    deep.property_profile?.canonical_name ||
    deep.identity_disambiguation?.focus_hotel ||
    "Mexico Explorer hotel";

  const sources = expandMexicoExplorerSources(deep);

  const openQuestions = buildOpenQuestions(deep, hotelKey);
  const keyFindings = buildKeyFindings(deep, hotelKey).map((kf) => ({
    ...kf,
    body: kf.explanation,
    citations: [],
  }));

  const people = (deep.people || []).map((p, i) => {
    const url =
      p.linkedin_url ||
      p.professional_profile_url ||
      (p.professional_profile && p.professional_profile.url) ||
      null;
    const verified =
      p.professional_profile_verified === true ||
      p.professional_profile_status === "VERIFIED" ||
      (p.professional_profile && p.professional_profile.verified === true);
    const safeUrl =
      verified && url && /linkedin\.com\/in\//i.test(String(url)) ? String(url).trim() : null;
    return {
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
      linkedin_url: safeUrl,
      professional_profile_url: safeUrl,
      professional_profile_type: safeUrl ? "LINKEDIN" : null,
      professional_profile_verified: Boolean(safeUrl),
      professional_profile_status: safeUrl
        ? "VERIFIED"
        : p.professional_profile_status || (url ? "UNVERIFIED" : "NOT_FOUND"),
      professional_profiles: safeUrl
        ? [{ type: "LINKEDIN", url: safeUrl, verified: true }]
        : [],
      professional_profile: safeUrl
        ? {
            type: "LINKEDIN",
            url: safeUrl,
            verified: true,
            status: "VERIFIED",
          }
        : p.professional_profile || null,
    };
  });

  const events = (deep.property_history || []).map((e, i) => ({
    id: `evt_${i + 1}`,
    date: e.date || null,
    event: e.event || e.label || null,
    confidence: e.confidence || null,
    citation_ids: e.source_ids || [],
  }));

  const entities = (deep.organizations || []).map((o, i) => ({
    id: o.slug || `ent_${i + 1}`,
    name: o.name,
    slug: o.slug || null,
    relationships: o.relationships || [],
    note: o.note || null,
    website: o.website || null,
    hq: o.hq || null,
  }));

  const relationships = (deep.relationships || []).map((r, i) => ({
    id: `rel_${i + 1}`,
    from: r.from || r.subject,
    type: r.type || r.relationship_type,
    to: r.to || r.object,
    confidence: r.confidence || null,
    note: r.note || null,
    temporal: r.temporal || (/FORMER|PREVIOUSLY|HISTORICAL/i.test(String(r.type || "")) ? "historical" : "current"),
  }));

  const pursuit = deep.commercial_pursuit || {};
  const op = deep.operator_resolution || {};

  const execParas = [
    `This Full Hotel Intelligence Investigation reviews ownership, corporate structure, brand and operator relationships, people, portfolio context, and development implications for ${hotelName}. Evidence is compiled from the completed Full Hotel Intelligence Investigation and supporting public sources.`,
    deep.brand_resolution?.resolution
      ? `Brand resolution: ${deep.brand_resolution.resolution}`
      : null,
    op.current_operator
      ? `Current operator reading: ${op.current_operator.name} (${op.current_operator.status || "CURRENT"}; ${op.current_operator.confidence || "—"}).${
          op.former_operator?.name
            ? ` Former operator: ${op.former_operator.name}.`
            : ""
        }`
      : null,
    (() => {
      const propco = chainName(deep, "propco");
      const econ = chainName(deep, "economic_owner");
      const parts = [];
      if (propco) parts.push(`Property company reading: ${propco}`);
      else parts.push("Property company / title vehicle: not verified from reviewed public sources");
      if (econ) parts.push(`Economic / package owner reading: ${econ}`);
      return parts.join(". ") + ".";
    })(),
    pursuit.why_matters || null,
    openQuestions.length
      ? `Material open questions remain (${openQuestions.length}) — including deed/title, decision authority, brand/operator contracts, and capital structure where not established. Unresolved items are listed explicitly rather than inferred.`
      : null,
    `Methodology: owner ≠ operator ≠ brand. Titles are not signing authority. Findings are research-grade and are not auto-applied to master property records. This dossier is the primary Full Hotel Intelligence Investigation for the hotel.`,
  ].filter(Boolean);

  const sections = buildMexicoFullHiSections(deep, hotelKey, openQuestions, sources, people);

  const completedAt = deep.observed_date
    ? `${deep.observed_date}T18:00:00.000Z`
    : "2026-09-08T18:00:00.000Z";
  const startedAt = deep.observed_date
    ? `${deep.observed_date}T12:00:00.000Z`
    : "2026-09-08T12:00:00.000Z";

  const dossier = createEmptyDossier({
    dossier_id: options.dossierId || meta.dossier_id,
    hotel_id: deep.hotel_id || hotelKey,
    hotel_airtable_record_id: hotelKey,
    hotel_name: hotelName,
    title: DOSSIER_TITLE,
    status: "COMPLETED_WITH_OPEN_QUESTIONS",
    research_provider: "WEBHOUND",
    research_run_id: deep.webhound_session_id || deep.version || null,
    version: 2,
    created_at: startedAt,
    started_at: startedAt,
    completed_at: completedAt,
    research_cost_usd: deep.webhound_budget_usd != null ? Number(deep.webhound_budget_usd) : 5,
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
      "Full Hotel Intelligence Investigation",
      "Compiled ownership, operator, brand, people, and portfolio evidence review",
      "People title verification without authority inference",
      "Owner ≠ operator ≠ brand separation checks",
      "Required institutional investigation chapters",
    ],
    methodology_summary:
      "Compiled from the completed Full Hotel Intelligence Investigation into the standard investigation chapters. Substantive findings preserved; no second-pass invention. Findings remain research-grade until independently reviewed.",
    raw_artifact_reference: {
      kind: "fixture",
      path: meta?.deep_path || options.deepPath || null,
      provider_internal: deep.webhound_session_id ? "webhound_session_retained_internal" : null,
      immutable: true,
    },
    mapping_notes: [
      "Ownership deep-research fixtures alone do not populate Research Center — this Full HI dossier + archive backfill does.",
      "Open questions are phrased for deterministic follow-up recommendation matching.",
      "claim_handoff.auto_promote is always false.",
      "Section ids follow FULL_HI_REQUIRED_CHAPTER_IDS for live integrity.",
    ],
    claim_handoff: {
      auto_promote: false,
      candidates: buildClaimCandidatesFromMexicoDeep(deep),
    },
  });

  refreshDossierCounts(dossier);
  const words = String(
    JSON.stringify({
      executive_summary: dossier.executive_summary,
      key_findings: dossier.key_findings,
      sections: dossier.sections,
    })
  )
    .replace(/[{}\[\]",:]/g, " ")
    .split(/\s+/)
    .filter(Boolean).length;
  dossier.substantive_word_count = words;

  // Cover / report geography — required for customer-facing location line.
  const profile = deep.property_profile || {};
  dossier.property_profile = profile;
  dossier.city = profile.city || null;
  dossier.country = profile.country || null;
  dossier.hotel_location = {
    city: profile.city || null,
    locality: profile.submarket || null,
    state_region: profile.state || profile.state_region || null,
    country: profile.country || null,
    label: null,
  };

  // Customer surfaces only — strip Dealality infrastructure language before fixture write / serve.
  return enrichReportForPublishing(enforceClientSafeCustomerSurfaces(dossier));
}

export function loadMexicoExplorerDeepResearchFixture(hotelAirtableId) {
  const meta = MEXICO_EXPLORER_DOSSIER_META[String(hotelAirtableId || "").trim()];
  if (!meta) return null;
  const p = path.join(ROOT, meta.deep_path);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function loadAndAdaptMexicoExplorerDossier(hotelAirtableId) {
  const deep = loadMexicoExplorerDeepResearchFixture(hotelAirtableId);
  if (!deep) throw new Error(`mexico_explorer_deep_missing:${hotelAirtableId}`);
  return adaptMexicoExplorerDeepResearchToDossier(deep, { hotelAirtableId });
}

export function writeMexicoExplorerDossierFixtures({ root = ROOT } = {}) {
  const outDir = path.join(root, "fixtures/hotel-intelligence/dossier");
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
  const written = [];
  for (const hotelId of Object.keys(MEXICO_EXPLORER_DOSSIER_META)) {
    const meta = MEXICO_EXPLORER_DOSSIER_META[hotelId];
    const dossier = loadAndAdaptMexicoExplorerDossier(hotelId);
    const outPath = path.join(outDir, meta.fixture_file);
    fs.writeFileSync(outPath, JSON.stringify(dossier, null, 2) + "\n");
    written.push({ hotelId, dossier_id: dossier.dossier_id, path: outPath });
  }
  return written;
}
