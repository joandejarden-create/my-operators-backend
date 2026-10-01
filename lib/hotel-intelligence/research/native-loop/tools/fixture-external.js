/**
 * Offline / fixture external responses for Iteration 2 Golden Five.
 * Used when live Parallel/SerpAPI are unavailable or external_mode=fixture.
 * Precision-first: do not invent HIGH ownership for AVA; improve contact/Cambridge graph.
 */

export const FIXTURE_EXTERNAL_VERSION = "native-fixture-external-v1";

const BY_CASE = {
  "OWN-AVA-CANCUN": {
    search_hits: [
      {
        title: "AVA Resort Cancun — property listing (weak directory)",
        url: "https://example.invalid/ava-cancun-directory",
        source_type: "aggregator",
        authority_tier: "weak",
        snippet:
          "AVA Resort Cancun listed under various management brands; ownership entity not independently confirmed in this source.",
      },
      {
        title: "KTRC mention without deed corroboration",
        url: "https://example.invalid/ktrc-ava-mention",
        source_type: "press",
        authority_tier: "supporting",
        snippet:
          "CORPORACION INMOBILIARIA KTRC appears in secondary commentary linked to AVA Cancun; no transaction announcement or registry extract in this pass.",
      },
    ],
    parallel: {
      notes:
        "Parallel fixture: identity lock held for AVA Resort Cancun. KTRC remains a single-source candidate without independent corroboration. Return UNRESOLVED ownership.",
      claims: [
        {
          relationship: "OWNED_BY",
          object: "CORPORACION INMOBILIARIA KTRC",
          status: "UNRESOLVED",
          confidence: null,
          unresolved: true,
          field: "economic_owner",
          excerpt:
            "Candidate owner KTRC not independently verified via deed, issuer filing, or transaction announcement.",
          url: "https://example.invalid/ktrc-ava-mention",
          authority_tier: "supporting",
        },
      ],
      contradictions: [],
      material_improvement: "added_corroboration_attempt_still_unresolved",
    },
  },
  "OWN-KGPV": {
    search_hits: [
      {
        title: "GSF issuer disclosure — Krystal Grand Puerto Vallarta",
        url: "https://example.invalid/gsf-kgpv-issuer",
        source_type: "securities_filing",
        authority_tier: "high",
        snippet:
          "Hoteles propios incluye Krystal Grand Puerto Vallarta; entidad propietaria del inmueble Inmobiliaria en Hotelería Vallarta Santa Fe; operated by Grupo Hotelero Santa Fe.",
      },
    ],
    parallel: {
      notes: "Parallel fixture corroborates GSF ownership + IHVSF PropCo for exact KGPV identity.",
      claims: [
        {
          relationship: "OWNED_BY",
          object: "Grupo Hotelero Santa Fe",
          status: "VERIFIED",
          confidence: "HIGH",
          field: "economic_owner",
          excerpt: "Issuer disclosure lists KGPV among owned hotels of Grupo Hotelero Santa Fe.",
          url: "https://example.invalid/gsf-kgpv-issuer",
          authority_tier: "high",
        },
      ],
      contradictions: [],
      material_improvement: "independent_corroboration",
    },
  },
  "PORT-GSF": {
    search_hits: [
      {
        title: "Grupo Hotelero Santa Fe portfolio overview",
        url: "https://example.invalid/gsf-portfolio",
        source_type: "company_investor",
        authority_tier: "high",
        snippet: "GSF discloses owned hotel portfolio including Krystal Grand properties in Mexico.",
      },
    ],
    parallel: {
      notes: "Portfolio corroboration for GSF owned set.",
      claims: [
        {
          relationship: "HAS_PORTFOLIO",
          object: "Grupo Hotelero Santa Fe",
          status: "PROBABLE",
          confidence: "MEDIUM",
          field: "portfolio",
          excerpt: "Company materials describe multi-property owned hotel portfolio.",
          url: "https://example.invalid/gsf-portfolio",
          authority_tier: "high",
        },
      ],
      material_improvement: "portfolio_source_diversity",
    },
  },
  "CT-PHIL-HOSPOD": {
    search_hits: [
      {
        title: "Dovetail + Co — leadership",
        url: "https://www.dovetailandco.com",
        source_type: "company_official",
        authority_tier: "high",
        snippet: "Phil Hospod, Founder & CEO of Dovetail + Co.",
      },
      {
        title: "Dovetail domain",
        url: "https://www.dovetailandco.com/about",
        source_type: "company_official",
        authority_tier: "high",
        snippet: "Company domain dovetailandco.com; no public mailbox published on this page.",
      },
    ],
    parallel: {
      notes:
        "Contact fixture: person + role + domain evidenced. Email not independently published — leave email UNVERIFIED (do not invent).",
      claims: [
        {
          relationship: "PERSON_AFFILIATED_WITH",
          subject: "Phil Hospod",
          object: "Dovetail + Co",
          status: "PROBABLE",
          confidence: "MEDIUM",
          field: "person",
          excerpt: "Phil Hospod listed as Founder & CEO of Dovetail + Co on first-party site.",
          url: "https://www.dovetailandco.com",
          authority_tier: "high",
        },
        {
          relationship: "HAS_ROLE",
          subject: "Phil Hospod",
          object: "Founder & CEO",
          status: "PROBABLE",
          confidence: "MEDIUM",
          field: "role",
          excerpt: "Role Founder & CEO on first-party leadership page.",
          url: "https://www.dovetailandco.com",
          authority_tier: "high",
        },
        {
          relationship: "HAS_DOMAIN",
          subject: "Dovetail + Co",
          object: "dovetailandco.com",
          status: "VERIFIED",
          confidence: "HIGH",
          field: "domain",
          excerpt: "Official company domain resolved from first-party site.",
          url: "https://www.dovetailandco.com",
          authority_tier: "high",
        },
        {
          relationship: "HAS_EMAIL",
          subject: "Phil Hospod",
          object: null,
          status: "UNRESOLVED",
          unresolved: true,
          unknown_code: "UNVERIFIED_CONTACT",
          field: "email",
          excerpt: "No independently published business email on inspected first-party pages.",
          authority_tier: "supporting",
        },
      ],
      material_improvement: "person_role_domain_without_invented_email",
    },
  },
  "MULTI-CAMBRIDGE-OP-DISPUTE": {
    search_hits: [
      {
        title: "2021 announcement — Pyramid / Benchmark association",
        url: "https://example.invalid/cambridge-2021-announcement",
        source_type: "press",
        authority_tier: "strong_secondary",
        snippet:
          "2021 announcement associated Cambridge Beaches with Pyramid Global / Benchmark Hospitality management narrative.",
      },
      {
        title: "Dovetail + Co portfolio — Cambridge Beaches",
        url: "https://www.dovetailandco.com",
        source_type: "company_official",
        authority_tier: "high",
        snippet:
          "Dovetail + Co first-party materials present Cambridge Beaches within stewarded/self-operated posture; residual third-party listings may persist.",
      },
      {
        title: "Residual Pyramid listing (stale risk)",
        url: "https://example.invalid/pyramid-residual-listing",
        source_type: "aggregator",
        authority_tier: "weak",
        snippet:
          "Residual directory still lists Pyramid/Benchmark association without termination proof — treat as residual uncertainty, not current HIGH operator.",
      },
    ],
    parallel: {
      notes:
        "Operator dispute chain: historical announced operator vs current first-party self-op. Ownership remains Dovetail; operator residual unresolved at HIGH.",
      claims: [
        {
          relationship: "OWNED_BY",
          object: "Dovetail + Co",
          status: "VERIFIED",
          confidence: "HIGH",
          field: "economic_owner",
          excerpt: "First-party / prior native evidence supports Dovetail ownership of Cambridge Beaches.",
          url: "https://www.dovetailandco.com",
          authority_tier: "high",
        },
        {
          relationship: "OPERATED_BY",
          object: "Dovetail + Co",
          status: "PROBABLE",
          confidence: "MEDIUM",
          field: "operator",
          excerpt: "Current assessment: probable Dovetail self-op with residual listing uncertainty.",
          url: "https://www.dovetailandco.com",
          authority_tier: "high",
        },
        {
          relationship: "FORMERLY_OPERATED_BY",
          object: "Benchmark / Pyramid Global Hospitality",
          status: "ANNOUNCED",
          confidence: "MEDIUM",
          field: "announced_operator_2021",
          excerpt: "2021 announcement associated Benchmark/Pyramid; not proof of current operator.",
          url: "https://example.invalid/cambridge-2021-announcement",
          authority_tier: "strong_secondary",
        },
      ],
      contradictions: [
        {
          summary: "OPERATED_BY conflict: Dovetail + Co vs Benchmark / Pyramid Global Hospitality",
          hypothesis: "temporal_difference",
          research_query:
            "Determine whether Cambridge Beaches management transferred from Benchmark/Pyramid to Dovetail self-op and whether residual listings are stale",
        },
      ],
      material_improvement: "operator_dispute_chain_synthesized",
    },
  },
};

export function getFixtureBundle(caseId) {
  return BY_CASE[caseId] || null;
}

export function resolveExternalMode(state) {
  const raw = String(
    state.bounds?.external_mode || process.env.HI_NATIVE_EXTERNAL_MODE || "auto"
  ).toLowerCase();
  if (raw === "off" || raw === "live" || raw === "fixture") return raw;
  // auto: live if authorized+keys, else fixture when case fixture exists, else skip
  return "auto";
}
