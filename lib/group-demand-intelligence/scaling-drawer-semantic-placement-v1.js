/**
 * Scaling drawer semantic field-placement contract V1
 *
 * Correct headings with wrong content placement must FAIL.
 * Structure-only audits are insufficient for Phase 2 commercial certification.
 */

import {
  SCALING_CANONICAL_SECTION_IDS,
  buildScalingDrawerNormalizedModel,
  extractScalingSectionIdsFromHtml,
} from "./scaling-drawer-normalized-model-v1.js";
import {
  hasAssociationProcurementBoilerplate,
  hasAssociationDemandBoilerplate,
} from "./scaling-drawer-presentation-contract-v1.js";

export const SCALING_SEMANTIC_PLACEMENT_CONTRACT_V1 =
  "gdi-scaling-drawer-semantic-placement-v1";

const ASSOC_LEAK =
  /association staff\s*vs\s*PCO|host\/overflow|Delegate lodging|Scientific programme|Event edition|PCO or housing|Peak rooms:|Attendance:/i;

/**
 * Patterns that belong in specific sections; presence in the wrong section is a FAIL.
 */
export const SEMANTIC_MISPLACEMENT_RULES = Object.freeze([
  {
    id: "assoc_procurement_in_corporate_action",
    sectionIds: ["recommended_next_step", "why_act_now", "potential_business"],
    test: (text) => ASSOC_LEAK.test(text) || hasAssociationProcurementBoilerplate(text),
    message:
      "Association procurement / housing language must not appear in Scaling corporate sales sections",
  },
  {
    id: "publisher_as_company_in_what",
    sectionIds: ["what_is_happening"],
    test: (text) =>
      /mynewsdesk\.com\/about\/contact|treat Mynewsdesk as the commercial target/i.test(
        text,
      ),
    message: "Press-publisher contact pages must not be framed as the company activity target",
  },
  {
    id: "timing_only_in_potential",
    sectionIds: ["potential_business"],
    // Pure timing urgency without demand mechanism is misplaced if it is the whole body
    test: (text) =>
      /^(act now|time.?critical|window closing)/i.test(String(text || "").trim()) &&
      !/guestroom|meeting|lodging|travel|visit|training|catering/i.test(text),
    message: "Why-act-now timing copy must not be the sole Potential business body",
  },
  {
    id: "named_buyer_in_evidence_only",
    sectionIds: ["supporting_evidence"],
    test: (text, ctx) =>
      Boolean(ctx?.namedBuyer) &&
      new RegExp(
        `Contact\\s+${escapeRe(ctx.namedBuyer)}|Email\\s+${escapeRe(ctx.namedBuyer)}`,
        "i",
      ).test(text),
    message: "Named buyer contact details must live in Who to contact, not Supporting evidence",
  },
  {
    id: "hotel_demand_in_facts_as_certainty",
    sectionIds: ["facts_and_uncertainties"],
    test: (text) => {
      const claimsCertainty =
        /\b(confirmed room block|signed RFP|preferred[\s-]?hotel agreement is in place)\b/i.test(
          text,
        );
      const hedges =
        /\b(not (yet )?(confirmed|verified)|no confirmed|hypothesis|unverified|not established)\b/i.test(
          text,
        );
      return claimsCertainty && !hedges;
    },
    message:
      "Unverified hotel demand must not appear as established fact without confirmation language",
  },
]);

function escapeRe(s) {
  return String(s || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Extract per-section body text from rendered Scaling HTML.
 */
export function extractScalingSectionBodiesFromHtml(html) {
  const out = {};
  const re =
    /<section[^>]*data-section-id=["']([a-z_]+)["'][^>]*>([\s\S]*?)<\/section>/gi;
  let m;
  while ((m = re.exec(String(html || "")))) {
    const id = m[1];
    const body = String(m[2] || "")
      .replace(/<script[\s\S]*?<\/script>/gi, "")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    out[id] = (out[id] ? out[id] + " " : "") + body;
  }
  return out;
}

/**
 * Model-level placement checks (before HTML).
 */
export function auditScalingSemanticPlacementFromModel(model, opportunity = {}) {
  const discrepancies = [];
  if (!model?.sections) {
    return {
      ok: false,
      contract: SCALING_SEMANTIC_PLACEMENT_CONTRACT_V1,
      discrepancies: [
        { severity: "P0", id: "missing_model", message: "scalingDrawerModel missing" },
      ],
    };
  }

  const namedBuyer =
    opportunity.primaryContact?.name ||
    opportunity.contactName ||
    model.sections.who_to_contact?.primaryName ||
    null;

  const factsOnly = (model.sections.facts_and_uncertainties?.facts || [])
    .map(text)
    .filter(Boolean)
    .join(" ");
  const sectionText = {
    what_is_happening: text(model.sections.what_is_happening?.body),
    potential_business: text(model.sections.potential_business?.body),
    why_this_hotel: [
      text(model.sections.why_this_hotel?.body),
      ...(model.sections.why_this_hotel?.advantages || []),
      ...(model.sections.why_this_hotel?.limitations || []),
    ].join(" "),
    why_act_now: text(model.sections.why_act_now?.body),
    // Certainty claims are audited against facts only — uncertainties may hedge honestly.
    facts_and_uncertainties: factsOnly,
    who_to_contact: JSON.stringify(model.sections.who_to_contact || {}),
    recommended_next_step: text(model.sections.recommended_next_step?.body),
    supporting_evidence: JSON.stringify(model.sections.supporting_evidence || {}),
  };

  for (const rule of SEMANTIC_MISPLACEMENT_RULES) {
    for (const sid of rule.sectionIds) {
      const blob = sectionText[sid] || "";
      if (!blob) continue;
      if (rule.test(blob, { namedBuyer, opportunity, model })) {
        discrepancies.push({
          severity: "P0",
          id: rule.id,
          sectionId: sid,
          message: rule.message,
        });
      }
    }
  }

  // Required commercial fields present as non-empty (honest gap copy OK)
  for (const sid of [
    "what_is_happening",
    "potential_business",
    "why_this_hotel",
    "why_act_now",
    "recommended_next_step",
  ]) {
    const body =
      sid === "why_this_hotel"
        ? sectionText.why_this_hotel
        : text(model.sections[sid]?.body);
    if (!body || body === "—") {
      discrepancies.push({
        severity: "P0",
        id: `empty_${sid}`,
        sectionId: sid,
        message: `Section ${sid} body is empty`,
      });
    }
  }

  // Potential business should mention lodging/meeting/travel demand mechanism language
  // OR honest hypothesis — reject association-only demand
  const pot = sectionText.potential_business;
  if (hasAssociationDemandBoilerplate(pot) || ASSOC_LEAK.test(pot)) {
    discrepancies.push({
      severity: "P0",
      id: "assoc_demand_in_potential",
      sectionId: "potential_business",
      message: "Potential business must not use Association Events demand boilerplate",
    });
  }

  // Facts vs uncertainties: empty-string facts list items already cleaned in model;
  // reject if uncertainties array contains blank after clean (caller responsibility)

  return {
    ok: discrepancies.length === 0,
    contract: SCALING_SEMANTIC_PLACEMENT_CONTRACT_V1,
    discrepancies,
    sectionIds: model.sectionIds || [...SCALING_CANONICAL_SECTION_IDS],
  };
}

/**
 * HTML-level placement audit after render.
 */
export function auditScalingSemanticPlacementFromHtml(html, opportunity = {}) {
  const bodies = extractScalingSectionBodiesFromHtml(html);
  const discrepancies = [];
  const namedBuyer =
    opportunity.primaryContact?.name || opportunity.contactName || null;

  for (const rule of SEMANTIC_MISPLACEMENT_RULES) {
    for (const sid of rule.sectionIds) {
      const blob = bodies[sid] || "";
      if (!blob) continue;
      if (rule.test(blob, { namedBuyer, opportunity })) {
        discrepancies.push({
          severity: "P0",
          id: rule.id,
          sectionId: sid,
          message: rule.message,
        });
      }
    }
  }

  // Association leak anywhere in Scaling canonical drawer
  if (ASSOC_LEAK.test(String(html || ""))) {
    discrepancies.push({
      severity: "P0",
      id: "assoc_leak_anywhere",
      message: "Association-specific narrative leaked into Scaling drawer HTML",
    });
  }

  const ids = [];
  for (const id of extractScalingSectionIdsFromHtml(html)) {
    if (SCALING_CANONICAL_SECTION_IDS.includes(id) && !ids.includes(id)) ids.push(id);
  }

  return {
    ok: discrepancies.length === 0,
    contract: SCALING_SEMANTIC_PLACEMENT_CONTRACT_V1,
    discrepancies,
    sectionBodies: bodies,
    sectionIds: ids,
  };
}

/**
 * Convenience: build model from opportunity + audit.
 */
export function auditOpportunitySemanticPlacement(opportunity, opts = {}) {
  const model =
    opportunity.scalingDrawerModel ||
    buildScalingDrawerNormalizedModel(opportunity, opts);
  return auditScalingSemanticPlacementFromModel(model, opportunity);
}
