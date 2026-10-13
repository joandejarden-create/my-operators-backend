/**
 * Lane-aware research planner — Iteration 1.
 * Produces explicit questions; updates after each iteration from gaps/contradictions.
 */

export const PLANNER_VERSION = "native-planner-v1";

const OWNERSHIP_BASE = [
  { id: "q_identity", question: "Confirm exact hotel identity (name, city, country, aliases).", priority: 100, field: "identity" },
  { id: "q_address", question: "Confirm address / property identity anchors.", priority: 95, field: "address" },
  { id: "q_official", question: "Find official hotel / brand first-party references.", priority: 90, field: "first_party" },
  { id: "q_owner", question: "Find ownership / company references for the exact hotel.", priority: 88, field: "economic_owner" },
  { id: "q_txn", question: "Search transaction / acquisition / sale history.", priority: 80, field: "transaction" },
  { id: "q_propco", question: "Identify property/LLC (PropCo) vs economic owner.", priority: 85, field: "propco" },
  { id: "q_operator", question: "Distinguish current operator from owner and brand.", priority: 84, field: "operator" },
  { id: "q_mgmt", question: "Distinguish management company from brand affiliation.", priority: 75, field: "management" },
  { id: "q_ultimate", question: "Identify ultimate / parent owner if evidenced.", priority: 70, field: "ultimate_owner" },
  { id: "q_verify", question: "Independently verify strongest ownership hypothesis.", priority: 92, field: "verification" },
  { id: "q_alts", question: "Preserve unresolved ownership alternatives if evidence conflicts.", priority: 65, field: "alternatives" },
];

const PORTFOLIO_BASE = [
  { id: "q_id_org", question: "Confirm company / owner entity identity and aliases.", priority: 100, field: "identity" },
  { id: "q_portfolio", question: "Enumerate hotels in the owner portfolio with evidence.", priority: 90, field: "portfolio" },
  { id: "q_subs", question: "Identify subsidiaries / SPEs holding properties.", priority: 85, field: "subsidiaries" },
  { id: "q_op_mix", question: "Separate owned vs operated-for-third-party relationships.", priority: 80, field: "operating_mix" },
];

const CONTACT_BASE = [
  { id: "q_person", question: "Identify decision-maker person with role at the target company.", priority: 95, field: "person" },
  { id: "q_role", question: "Verify current role and company affiliation.", priority: 90, field: "role" },
  { id: "q_domain", question: "Resolve company domain for contact paths.", priority: 85, field: "domain" },
  { id: "q_email", question: "Discover business email only with evidence (never invent).", priority: 70, field: "email" },
  { id: "q_phone", question: "Discover HQ vs property phone with attribution.", priority: 65, field: "phone" },
];

const FACTS_BASE = [
  { id: "q_identity", question: "Confirm hotel identity.", priority: 100, field: "identity" },
  { id: "q_fact", question: "Resolve requested hotel/company facts with first-party preference.", priority: 90, field: "facts" },
  { id: "q_conflict", question: "Preserve conflicting numeric/fact values if sources disagree.", priority: 85, field: "conflicts" },
];

const MULTI_BASE = [
  ...OWNERSHIP_BASE.slice(0, 8),
  { id: "q_dispute", question: "Map conflicting operator/owner narratives chronologically.", priority: 93, field: "dispute" },
  { id: "q_residual", question: "Test whether residual portfolio listings imply active management.", priority: 88, field: "residual" },
];

export function resolveLane(input = {}) {
  const cat = String(input.category || input.lane || "").toUpperCase();
  if (cat.includes("PORTFOLIO") || cat === "OWNER_PORTFOLIO") return "OWNER_PORTFOLIO";
  if (cat.includes("CONTACT")) return "CONTACT_INTELLIGENCE";
  if (cat.includes("FACT") || cat === "HOTEL_FACTS") return "HOTEL_FACTS";
  if (cat.includes("MULTI") || cat === "OPEN_RESEARCH") return "OPEN_RESEARCH";
  return "HOTEL_OWNERSHIP";
}

function basePlan(lane) {
  if (lane === "OWNER_PORTFOLIO") return PORTFOLIO_BASE;
  if (lane === "CONTACT_INTELLIGENCE") return CONTACT_BASE;
  if (lane === "HOTEL_FACTS") return FACTS_BASE;
  if (lane === "OPEN_RESEARCH") return MULTI_BASE;
  return OWNERSHIP_BASE;
}

export function createInitialPlan(state) {
  const lane = state.lane || resolveLane(state.objective);
  const plan = basePlan(lane).map((q) => ({
    ...q,
    status: "OPEN",
    answered_by_claim_ids: [],
  }));
  // Boost requested fields
  for (const f of state.objective?.requested_fields || []) {
    const hit = plan.find((p) => p.field === f || String(f).includes(p.field));
    if (hit) hit.priority += 5;
  }
  plan.sort((a, b) => b.priority - a.priority);
  state.research_plan = plan;
  state.lane = lane;
  return plan;
}

export function refreshPlanFromState(state) {
  for (const step of state.research_plan) {
    const related = state.claims.filter(
      (c) =>
        c.field === step.field ||
        (step.field === "economic_owner" && c.relationship === "OWNED_BY") ||
        (step.field === "propco" && c.relationship === "PROPCO") ||
        (step.field === "operator" && /OPERATED_BY|OPERATOR/.test(c.relationship || ""))
    );
    const verified = related.filter((c) => c.status === "VERIFIED" || c.status === "HIGH");
    if (verified.length) {
      step.status = "ANSWERED";
      step.answered_by_claim_ids = verified.map((c) => c.claim_id);
    } else if (related.some((c) => c.status === "UNRESOLVED" || c.status === "CONTESTED")) {
      step.status = "CONTESTED";
    } else if (related.length) {
      step.status = "PARTIAL";
      step.answered_by_claim_ids = related.map((c) => c.claim_id);
    }
  }
  for (const c of state.contradictions.filter((x) => x.status === "OPEN")) {
    const id = `q_contra_${c.contradiction_id}`;
    if (!state.research_plan.some((p) => p.id === id)) {
      state.research_plan.push({
        id,
        question: `Resolve contradiction: ${c.summary}`,
        priority: 99,
        field: "contradiction",
        status: "OPEN",
        answered_by_claim_ids: [],
      });
    }
  }
  state.research_plan.sort((a, b) => {
    const openBoost = (x) => (x.status === "OPEN" || x.status === "CONTESTED" ? 0 : 50);
    return b.priority - openBoost(b) - (a.priority - openBoost(a));
  });
  return state.research_plan;
}

export function nextOpenPlanStep(state) {
  return (
    state.research_plan.find((p) => p.status === "OPEN" || p.status === "CONTESTED") ||
    state.research_plan.find((p) => p.status === "PARTIAL") ||
    null
  );
}
