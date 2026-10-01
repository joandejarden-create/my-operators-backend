import { PLAYBOOK_ID } from "./constants.js";
import { resolveOwnershipResearchStrategy } from "../ownership-research-strategy/index.js";

function hotelLabel(hotel = {}) {
  return (
    hotel.hotel_name ||
    hotel.official_name ||
    hotel.property_name ||
    hotel.name ||
    "hotel"
  );
}

function cityCountry(hotel = {}) {
  const city = hotel.city || "";
  const country = hotel.country || "";
  return [city, country].filter(Boolean).join(" ");
}

/**
 * Build query goals for a playbook. Vary objective / source framing / temporal focus —
 * not merely synonymous wording of the same Context.dev search.
 *
 * @returns {{ query: string, researchGoal: string, evidence_gap: string, playbook_id: string, ladder_rung?: string }[]}
 */
export function buildPlaybookGoals(playbookId, hotel = {}, state = {}) {
  const name = hotelLabel(hotel);
  const loc = cityCountry(hotel);
  const address = hotel.address || hotel.street_address || "";
  const id = playbookId;

  // V2.2 evidence-gap: prefer narrowly attributed objectives when present
  const gap = state.evidence_gap_objective || null;
  if (gap?.missing_claim && gap?.query_templates?.length) {
    return gap.query_templates.map((q) => ({
      query: typeof q === "string" ? q : q.query,
      researchGoal: gap.why_evidence_changes_verification || gap.missing_claim,
      evidence_gap: gap.missing_claim_code || "evidence_gap_targeted",
      playbook_id: id,
      candidate_id: gap.candidate_id || null,
      missing_claim: gap.missing_claim,
      max_expected_value: gap.max_expected_value || null,
      evidence_gap_attributed: true,
    }));
  }

  if (id === PLAYBOOK_ID.GENERAL_OWNERSHIP_DISCOVERY) {
    return [
      {
        query: `"${name}" ${loc} (proprietário OR "de propriedade" OR ownership OR "owned by" OR investidor OR sponsor)`.trim(),
        researchGoal: "Generate plausible current owner/economic-sponsor candidates",
        evidence_gap: "missing_owner_candidate",
        playbook_id: id,
        ladder_rung: "OWNER_CANDIDATE_DISCOVERY",
      },
      {
        query: `"${name}" ${loc} (portfolio OR "hotel portfolio" OR "real estate" OR holding OR controladora)`.trim(),
        researchGoal: "Find portfolio/holding clues for economic sponsor",
        evidence_gap: "missing_owner_candidate",
        playbook_id: id,
      },
    ];
  }

  if (id === PLAYBOOK_ID.TRANSACTION_CURRENT_OWNER) {
    return [
      {
        query: `"${name}" ${loc} (adquiriu OR aquisição OR comprou OR venda OR sold OR acquired OR disposition OR "change of ownership")`.trim(),
        researchGoal: "Determine who owns/economically sponsors the property NOW via transactions",
        evidence_gap: "missing_current_owner",
        playbook_id: id,
        ladder_rung: "TRANSACTION_CURRENTNESS",
        missing_claim: "PROPERTY_OWNER_LINKAGE",
      },
      {
        query: `"${name}" (brand conversion OR rebrand OR "now a" OR "formerly" OR "antes" OR conversão)`.trim(),
        researchGoal: "Brand/operator change as currentness signal",
        evidence_gap: "missing_current_owner",
        playbook_id: id,
      },
    ];
  }

  if (id === PLAYBOOK_ID.CORPORATE_ENTITY_RELATIONSHIP) {
    const strategy = resolveOwnershipResearchStrategy(hotel);
    // Brazil: reuse V1.1 legal-entity strategy — do not rebuild CNPJ ladder here.
    if (strategy && typeof strategy.buildLegalEntityQueries === "function") {
      const qs = strategy.buildLegalEntityQueries(hotel) || [];
      const goals = qs.slice(0, 8).map((query) => ({
        query,
        researchGoal: "Exact-property legal entity via Brazil V1.1 / corporate path",
        evidence_gap: "exact_property_legal_entity",
        playbook_id: id,
        ladder_rung: "LEGAL_ENTITY_DISCOVERY",
        brazil_v1_1_reused: true,
        missing_claim: "TARGET_PROPERTY_LEGAL_ENTITY",
      }));
      if (address) {
        goals.unshift({
          query: `"${address}" ${loc} (CNPJ OR "razão social" OR LTDA OR proprietário)`.trim(),
          researchGoal: "Exact-address CNPJ / legal entity for target property only",
          evidence_gap: "exact_property_legal_entity",
          playbook_id: id,
          missing_claim: "TARGET_PROPERTY_LEGAL_ENTITY",
        });
      }
      if (goals.length) return goals;
    }
    return [
      {
        query: `"${name}" ${loc} ("razão social" OR CNPJ OR "registered office" OR subsidiary OR parent OR "joint venture")`.trim(),
        researchGoal: "Corporate entity relationship research",
        evidence_gap: "missing_legal_entity",
        playbook_id: id,
        missing_claim: "TARGET_PROPERTY_LEGAL_ENTITY",
      },
      address
        ? {
            query: `"${address}" (CNPJ OR "razão social" OR owner OR proprietário)`,
            researchGoal: "Address-tied corporate entity",
            evidence_gap: "exact_property_legal_entity",
            playbook_id: id,
            missing_claim: "TARGET_PROPERTY_LEGAL_ENTITY",
          }
        : null,
    ].filter(Boolean);
  }

  if (id === PLAYBOOK_ID.FINANCING_PUBLIC_RECORD) {
    const fundHint = state.fii_ticker || state.fund_name || "";
    return [
      {
        query: `"${name}" ${loc} ${address ? `"${address}"` : ""} (FII OR "fundo imobiliário" OR REIT OR CVM OR portfolio OR "ativo" OR imóvel) ${fundHint}`.trim(),
        researchGoal: "FII/fund documented ownership or economic interest in THIS property",
        evidence_gap: "fund_property_linkage",
        playbook_id: id,
        ladder_rung: "FII_INVESTMENT",
        missing_claim: "FII_PROPERTY_LINKAGE",
      },
      {
        query: `${fundHint ? `"${fundHint}"` : `"${name}"`} (CVM OR "informe" OR portfolio OR "aquisição" OR "imóvel hoteleiro") ${loc}`.trim(),
        researchGoal: "Fund regulatory/portfolio disclosure linking asset to hotel",
        evidence_gap: "fund_property_linkage",
        playbook_id: id,
        brazil_fii_path: true,
        missing_claim: "FII_PROPERTY_LINKAGE",
      },
    ];
  }

  return [];
}

export function describePlaybook(playbookId) {
  return {
    playbook_id: playbookId,
    varies: {
      [PLAYBOOK_ID.GENERAL_OWNERSHIP_DISCOVERY]:
        "objective=candidate recall; sources=press/owner sites/graph; temporal=current",
      [PLAYBOOK_ID.TRANSACTION_CURRENT_OWNER]:
        "objective=currentness; sources=acquisition/disposition; temporal=transaction window",
      [PLAYBOOK_ID.CORPORATE_ENTITY_RELATIONSHIP]:
        "objective=entity graph; sources=registry/legal pages; reuses Brazil V1.1",
      [PLAYBOOK_ID.FINANCING_PUBLIC_RECORD]:
        "objective=fund/lender; sources=FII/CVM/financing; temporal=disclosure",
    }[playbookId],
  };
}
