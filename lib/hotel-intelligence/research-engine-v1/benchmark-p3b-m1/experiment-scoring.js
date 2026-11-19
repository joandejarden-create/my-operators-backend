/**
 * P3B-M1 three-hotel ownership research scoring + comparable method report.
 * Uses HE_INTEL_001_OWNERSHIP_RESEARCH_OBJECTIVE_V1 classifier only.
 * Valid URL alone ≠ ownership verification.
 */

import {
  OWNERSHIP_RESEARCH_OBJECTIVE_ID,
  OWNERSHIP_RESEARCH_OBJECTIVE_VERSION,
  classifyOwnershipResearchSuccess,
  OWNERSHIP_SUCCESS_TIER,
} from "../ownership-research-objective.js";
import { isSafeHttpUrl, normalizeAndFilterNewIntelligence } from "./evidence.js";
import { P3B_M1_HARD_CAP_USD, P3B_M1_PER_HOTEL_CAP_USD } from "./contract.js";

export const EXPERIMENT_SCORING_VERSION = "he-intel-p3b-m1-experiment-scoring-v1";

/**
 * Map raw provider facts into classifier input. Conservative: nothing is
 * verified unless explicit verified + evidenceUrl flags are present.
 */
export function factsToClassifierInput(facts = [], opts = {}) {
  const list = Array.isArray(facts) ? facts : [];
  const filtered = normalizeAndFilterNewIntelligence(list, {
    seedPropertyNames: opts.seedPropertyNames || [],
  });

  let ultimateOwner = null;
  let legalTitleholder = null;
  let ownershipPerson = null;
  let contact = null;
  let structureChain = null;
  const unsupported = [];
  const gaps = [];

  // Classifier uses raw fact verification flags — not mere accepted envelopes /
  // URL presence. Seed identity facts are skipped.
  for (const f of list) {
    if (f?.isSeedFact === true) continue;
    const urlOk = isSafeHttpUrl(f.url);
    const claim = String(f.claimType || "").toUpperCase();
    // URL alone never verifies ownership
    const verified = f.verified === true && urlOk && f.unsupported !== true;

    if (claim === "ULTIMATE_OWNER" || claim === "CONTROLLING_INVESTOR" || claim === "ECONOMIC_SPONSOR") {
      if (!ultimateOwner) {
        ultimateOwner = {
          name: f.value || null,
          verified,
          evidenceUrl: urlOk ? f.url : null,
          linkedToHpc: f.linkedToHpc === true,
          inferred: f.inferred === true,
          unsupported: f.unsupported === true || !verified,
        };
      }
    } else if (claim === "LEGAL_TITLEHOLDER" || claim === "OWNER_ENTITY") {
      if (!legalTitleholder) {
        legalTitleholder = {
          name: f.value || null,
          verified,
          evidenceUrl: urlOk ? f.url : null,
          linkedToHpc: f.linkedToHpc === true,
          inferred: f.inferred === true,
          unsupported: !verified,
        };
      }
    } else if (claim === "OWNERSHIP_DECISION_MAKER" || claim === "OWNERSHIP_REPRESENTATIVE") {
      if (!ownershipPerson) {
        ownershipPerson = {
          name: f.value || null,
          verified,
          evidenceUrl: urlOk ? f.url : null,
          roleRelevant: f.roleRelevant === true,
          connectedToOwnerOrRepresentative: f.connectedToOwnerOrRepresentative === true,
          unrelatedHotelSales: f.unrelatedHotelSales === true,
          genericOperatorContact: f.genericOperatorContact === true,
        };
      }
    } else if (claim === "ALTERNATIVE_CONTACT_PATHWAY") {
      if (!contact) {
        contact = {
          directVerified: f.directVerified === true && verified,
          indirectInstitutionalPathway: f.indirectInstitutionalPathway !== false,
          inferred: f.inferred === true,
          evidenceUrl: urlOk ? f.url : null,
          connectedToPersonOrOwner: f.connectedToPersonOrOwner === true,
        };
      }
    } else if (claim === "OWNERSHIP_STRUCTURE_EDGE") {
      if (!structureChain) {
        structureChain = {
          verified: false, // content retrieved ≠ structure verified
          evidenceUrl: urlOk ? f.url : null,
          relevant: true,
          note: "Source candidate only until adjudicated",
        };
      }
    }

    if (f.unsupported === true || (urlOk && f.verified !== true && claim.includes("OWNER"))) {
      unsupported.push({ claimType: claim, value: f.value, url: f.url || null });
    }
  }

  if (!ultimateOwner) gaps.push("ULTIMATE_OWNER_NOT_IDENTIFIED");
  if (!legalTitleholder) gaps.push("LEGAL_ENTITY_CHAIN_INCOMPLETE");
  if (!ownershipPerson) gaps.push("NAMED_OWNERSHIP_CONTACTS_MISSING");
  if (!contact) gaps.push("VERIFIED_CONTACT_CHANNELS_MISSING");

  return {
    classifierInput: {
      ultimateOwner,
      legalTitleholder,
      ownershipPerson,
      contact,
      structureChain,
      stagedOnly: opts.stagedOnly !== false,
      ultimateOwnerUnresolved: !ultimateOwner || ultimateOwner.verified !== true,
    },
    filtered,
    unsupported,
    gaps,
  };
}

export function scoreHotelMethodResult({
  hotel,
  method,
  facts = [],
  cost = {},
  sources = [],
} = {}) {
  const mapped = factsToClassifierInput(facts, {
    seedPropertyNames: hotel?.name ? [hotel.name] : [],
  });
  const outcome = classifyOwnershipResearchSuccess(mapped.classifierInput);
  const acceptedSources = (sources.length
    ? sources
    : (facts || []).map((f) => f.url).filter(Boolean)
  ).filter((u) => isSafeHttpUrl(u));

  const usefulOwnershipFindings = [
    outcome.tier !== OWNERSHIP_SUCCESS_TIER.UNRESOLVED ? 1 : 0,
  ].reduce((a, b) => a + b, 0);
  // Count actionable contact only when classifier indicates contact path
  const actionableContact =
    outcome.tier === OWNERSHIP_SUCCESS_TIER.FULL_ACTIONABLE ||
    outcome.tier === OWNERSHIP_SUCCESS_TIER.PARTIAL_REPRESENTATIVE_PATHWAY
      ? 1
      : 0;

  const totalCost = Number(cost.total_usd || 0) || 0;

  return {
    hotel: {
      hpcId: hotel?.hpcId || null,
      name: hotel?.name || null,
    },
    method,
    objective: {
      id: OWNERSHIP_RESEARCH_OBJECTIVE_ID,
      version: OWNERSHIP_RESEARCH_OBJECTIVE_VERSION,
    },
    questions: {
      ultimate_owner_identified:
        Boolean(mapped.classifierInput.ultimateOwner?.name) &&
        mapped.classifierInput.ultimateOwner?.verified === true,
      ownership_verified: outcome.tier === OWNERSHIP_SUCCESS_TIER.FULL_ACTIONABLE ||
        outcome.tier === OWNERSHIP_SUCCESS_TIER.PARTIAL_OWNER_WITHOUT_CONTACT,
      named_relevant_ownership_contacts_found:
        Boolean(mapped.classifierInput.ownershipPerson?.name) &&
        mapped.classifierInput.ownershipPerson?.verified === true,
      direct_verified_contact_found:
        mapped.classifierInput.contact?.directVerified === true,
      credible_indirect_pathways_found:
        mapped.classifierInput.contact?.indirectInstitutionalPathway === true &&
        mapped.classifierInput.contact?.inferred !== true,
      unsupported_or_incorrect_claims: mapped.unsupported,
      research_gaps_remaining: mapped.gaps,
      sources_reviewed_and_accepted: acceptedSources,
      // Valid URL alone is NOT independent verification
      url_alone_not_verification: true,
    },
    classifier: outcome,
    cost: {
      provider_usd: cost.provider_usd ?? null,
      model_token_usd: cost.model_token_usd ?? null,
      orchestration_usd: cost.orchestration_usd ?? 0,
      total_usd: totalCost,
      cost_per_verified_useful_ownership_finding:
        usefulOwnershipFindings > 0
          ? Number((totalCost / usefulOwnershipFindings).toFixed(6))
          : null,
      cost_per_actionable_ownership_contact:
        actionableContact > 0
          ? Number((totalCost / actionableContact).toFixed(6))
          : null,
    },
    scoring_version: EXPERIMENT_SCORING_VERSION,
  };
}

/**
 * Build empty comparable matrix for three hotels × methods (pre-execution).
 */
export function buildComparableReportSkeleton(hotels = [], methods = []) {
  const rows = [];
  for (const hotel of hotels) {
    for (const method of methods) {
      rows.push(
        scoreHotelMethodResult({
          hotel,
          method,
          facts: [],
          cost: { total_usd: 0 },
          sources: [],
        })
      );
    }
  }
  return {
    version: EXPERIMENT_SCORING_VERSION,
    objective_id: OWNERSHIP_RESEARCH_OBJECTIVE_ID,
    objective_version: OWNERSHIP_RESEARCH_OBJECTIVE_VERSION,
    hard_cap_usd: P3B_M1_HARD_CAP_USD,
    per_hotel_cap_usd: P3B_M1_PER_HOTEL_CAP_USD,
    paid_execution_authorized: false,
    rows,
    recommendation: {
      develop_further: null,
      rationale:
        "Pending live paid execution (not authorized). Offline activation validates scoring + wiring only.",
    },
  };
}

/**
 * Aggregate method recommendation from scored rows (when live results exist).
 */
export function recommendResearchApproach(rows = []) {
  const byMethod = new Map();
  for (const row of rows) {
    const m = row.method;
    if (!byMethod.has(m)) {
      byMethod.set(m, {
        method: m,
        full: 0,
        partial: 0,
        unresolved: 0,
        totalCost: 0,
        actionableContacts: 0,
      });
    }
    const agg = byMethod.get(m);
    const tier = row.classifier?.tier;
    if (tier === OWNERSHIP_SUCCESS_TIER.FULL_ACTIONABLE) agg.full += 1;
    else if (tier && tier !== OWNERSHIP_SUCCESS_TIER.UNRESOLVED) agg.partial += 1;
    else agg.unresolved += 1;
    agg.totalCost += Number(row.cost?.total_usd || 0);
    if (row.questions?.direct_verified_contact_found || row.questions?.credible_indirect_pathways_found) {
      agg.actionableContacts += 1;
    }
  }
  const ranked = [...byMethod.values()].sort((a, b) => {
    if (b.full !== a.full) return b.full - a.full;
    if (b.actionableContacts !== a.actionableContacts) return b.actionableContacts - a.actionableContacts;
    if (b.partial !== a.partial) return b.partial - a.partial;
    return a.totalCost - b.totalCost;
  });
  const top = ranked[0] || null;
  return {
    develop_further: top?.method || null,
    ranked,
    rationale: top
      ? `Prefer ${top.method} for further development based on FULL_ACTIONABLE=${top.full}, actionable contacts=${top.actionableContacts}, partial=${top.partial}, cost=${top.totalCost}`
      : "No method rows to rank",
  };
}
