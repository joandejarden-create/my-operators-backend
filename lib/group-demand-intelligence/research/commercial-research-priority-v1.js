/**
 * Maps commercial drawer priorities to adaptive research gap ranking.
 * Foundational identity/chronology eligibility remains mandatory (P0).
 */
export const COMMERCIAL_RESEARCH_PRIORITY_V1 =
  "gdi-commercial-research-priority-v1";

/** Four Stage F priority drawer sections (customer-facing commercial value). */
export const FOUR_PRIORITY_DRAWER_SECTIONS = Object.freeze([
  "potential_business",
  "why_this_hotel",
  "who_to_contact",
  "recommended_next_step",
]);

const FOUNDATIONAL_RE =
  /legal name|company identity|entity|eligibility|chronology|activity date|office address|registration|confirm (the )?company|official website/i;

const DEMAND_RE =
  /hotel.?demand|room.?night|meeting|lodging|travel.?pattern|visitor|offsite|workshop|training|catering|demand credibility/i;

const PROPERTY_RE =
  /why this hotel|property.?fit|competit|corridor|facility|chapultepec|santa fe|polanco|preferred.?hotel|commute/i;

const CONTACT_RE =
  /contact|buyer|people|workplace|facilities|linkedin|decision.?maker|secondary contact|outreach|who to contact/i;

const NEXT_STEP_RE =
  /qualification|next step|sourcing|travel.?management|tmc|procurement|when to approach/i;

/**
 * Rank an unresolved commercial question for research budget allocation.
 * Higher score = more commercially valuable once foundational checks pass.
 */
export function scoreCommercialResearchQuestion(question = "", opts = {}) {
  const q = String(question || "");
  let score = 0;
  let bucket = "OTHER";
  let foundational = false;

  if (FOUNDATIONAL_RE.test(q) || opts.foundational === true) {
    foundational = true;
    score += 100;
    bucket = "FOUNDATIONAL";
  }
  if (DEMAND_RE.test(q)) {
    score += 40;
    bucket = bucket === "FOUNDATIONAL" ? bucket : "DEMAND_CREDIBILITY";
  }
  if (PROPERTY_RE.test(q)) {
    score += 35;
    bucket = bucket === "FOUNDATIONAL" ? bucket : "PROPERTY_COMPETITIVENESS";
  }
  if (CONTACT_RE.test(q)) {
    score += 38;
    bucket = bucket === "FOUNDATIONAL" ? bucket : "CONTACT_ACCESSIBILITY";
  }
  if (NEXT_STEP_RE.test(q)) {
    score += 30;
    bucket = bucket === "FOUNDATIONAL" ? bucket : "NEXT_SALES_DECISION";
  }

  return {
    version: COMMERCIAL_RESEARCH_PRIORITY_V1,
    score,
    bucket,
    foundational,
    mapsToPrioritySections: foundational
      ? ["opportunity_overview", "what_is_happening", "facts_and_uncertainties"]
      : FOUR_PRIORITY_DRAWER_SECTIONS.filter((id) => {
          if (id === "potential_business") return DEMAND_RE.test(q);
          if (id === "why_this_hotel") return PROPERTY_RE.test(q);
          if (id === "who_to_contact") return CONTACT_RE.test(q);
          if (id === "recommended_next_step") return NEXT_STEP_RE.test(q) || CONTACT_RE.test(q);
          return false;
        }),
  };
}

/**
 * Pick the highest-value unresolved task, never skipping foundational gaps.
 */
export function prioritizeUnresolvedResearchTasks(tasks = []) {
  const list = (tasks || [])
    .map((t, i) => {
      const question = String(t?.question || t || "");
      const scored = scoreCommercialResearchQuestion(question, {
        foundational: t?.priority === "P0" || t?.mandatory === true,
      });
      return { task: t, index: i, question, ...scored };
    })
    .filter((x) => x.question);

  const foundational = list.filter((x) => x.foundational);
  if (foundational.length) {
    foundational.sort((a, b) => b.score - a.score);
    return foundational[0];
  }
  list.sort((a, b) => b.score - a.score || a.index - b.index);
  return list[0] || null;
}
