/**
 * Shared customer-facing content remediation for GDI publication quality.
 *
 * Corrects repetitive / malformed / contradictory presentation without inventing
 * research facts. Uncertainty is preserved in clear customer language.
 *
 * Applied in the universal presentation pipeline so local preview and package
 * builds share one remediation path.
 */

import {
  remediateDuplicateOrgAssociationSentence,
  stripEmptyParentheses,
  dedupeTileClassificationSourcing,
} from "./gdi-publication-quality-gate-v1.js";
import {
  CUSTOMER_SALES_ACTION,
  CUSTOMER_SALES_ACTION_LABEL,
  resolveCustomerSalesAction,
} from "../customer-sales-action-resolver-v1.js";

export const GDI_CUSTOMER_FACING_CONTENT_REMEDIATION_V1 =
  "gdi-customer-facing-content-remediation-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/** Boilerplate that duplicates the sourcing badge. */
const SOURCING_UNCONFIRMED_BOILERPLATE =
  /\s*(?:,|\.|—|-)?\s*with hotel sourcing unconfirmed\s*[—–-]\s*a plausible group-lodging conversation for [^.,;]+(?:,)?\s*not proof of an open room block or RFP\.?/gi;

const SOURCING_UNCONFIRMED_SHORT =
  /\s+with hotel sourcing unconfirmed(?=\s*[—–.,]|$)/gi;

/**
 * Replace internal placeholder tokens with explicit uncertainty language.
 * Does not invent dates, venues, or demand claims.
 */
export function rewritePlaceholderUncertainty(value) {
  let s = text(value);
  if (!s) return s;
  s = s.replace(/\bdates?\s+TBD\b/gi, "dates not yet announced");
  s = s.replace(/\bdate\s+TBD\b/gi, "date not yet announced");
  s = s.replace(/\bdestination\s+TBD\b/gi, "destination not yet announced");
  s = s.replace(/\bhotel\s+TBD\b/gi, "hotel not yet announced");
  s = s.replace(/\bvenue\s+TBD\b/gi, "venue not yet announced");
  s = s.replace(/\blodging\s+TBA\b/gi, "lodging not yet announced");
  s = s.replace(/\bhotel\s+block\s+TBA\b/gi, "hotel block not yet announced");
  s = s.replace(/\bTBA\b/g, "not yet announced");
  s = s.replace(/\bTBD\b/g, "not yet announced");
  s = s.replace(/\bTODO\b/g, "not yet confirmed");
  s = s.replace(/\bFIXME\b/gi, "not yet confirmed");
  s = s.replace(/\bPLACEHOLDER\b/gi, "not yet confirmed");
  s = s.replace(/\blorem ipsum\b/gi, "");
  return text(s);
}

export function stripDuplicatedSourcingBoilerplate(value) {
  let s = text(value);
  if (!s) return s;
  s = s.replace(SOURCING_UNCONFIRMED_BOILERPLATE, "");
  s = s.replace(SOURCING_UNCONFIRMED_SHORT, "");
  // Collapse leftover "with not proof" fragments if any
  s = s.replace(/\s*—\s*not proof of an open room block or RFP\.?/gi, "");
  return text(s).replace(/\s([.,])/g, "$1");
}

/**
 * Soften unsupported "confirmed room demand" claims when target sourcing
 * is unconfirmed. Preserves explicit negations ("not confirmed").
 */
export function softenUnsupportedConfirmedRoomClaims(value, sourcingLabel) {
  const s = text(value);
  const sourcing = text(sourcingLabel);
  if (!s) return s;
  const sourcingUnconfirmed =
    /not established|unconfirmed|not confirmed|sourcing unconfirmed/i.test(
      sourcing,
    );
  if (!sourcingUnconfirmed) return s;
  // Do not rewrite accurate negations.
  if (/not confirmed room demand|no confirmed room/i.test(s)) return s;
  let out = s;
  out = out.replace(
    /\bconfirmed room demand\b/gi,
    "hypothesized room demand (unconfirmed)",
  );
  out = out.replace(
    /\bconfirmed (\d+)\s*rooms\b/gi,
    "reported $1 rooms (unverified for this hotel)",
  );
  out = out.replace(
    /\bguaranteed room block at (?:this|the target) hotel\b/gi,
    "room block at this hotel (not verified)",
  );
  return text(out);
}

function looksLikeClosedMonitorGuidance(action) {
  return /do not pursue as an open|sourcing (?:for this edition )?appears closed|do not aggressively sell|keep a light relationship|official hotel sourcing.*closed|fully placed|do not pursue this cycle/i.test(
    text(action),
  );
}

function looksLikeDefensiveQualifyGuidance(action) {
  return /monitor for sourcing|do not claim open rfp|lodging demand path not established/i.test(
    text(action),
  );
}

/**
 * Align sales-action pill with next-step guidance.
 * - Closed / do-not-pursue → Monitor
 * - Defensive "do not claim open RFP" on lodging demand → Qualify Opportunity
 * Never leave Win Room Block / Build Account conflicting with those stances.
 */
export function alignSalesActionWithGuidance(opp = {}) {
  const next = text(
    opp.recommendedNextStep || opp.recommendedAction || opp.suggestedAction,
  );
  const currentKey = String(opp.salesActionKey || "").toUpperCase();
  const pursuitConflict =
    currentKey === CUSTOMER_SALES_ACTION.WIN_ROOM_BLOCK ||
    currentKey === CUSTOMER_SALES_ACTION.CONTACT_BUYER ||
    currentKey === CUSTOMER_SALES_ACTION.BUILD_ACCOUNT ||
    !currentKey;

  if (looksLikeClosedMonitorGuidance(next)) {
    if (currentKey === CUSTOMER_SALES_ACTION.MONITOR) return opp;
    return {
      ...opp,
      salesActionKey: CUSTOMER_SALES_ACTION.MONITOR,
      salesActionPillLabel:
        CUSTOMER_SALES_ACTION_LABEL[CUSTOMER_SALES_ACTION.MONITOR],
      salesActionReason:
        "Aligned to closed / do-not-pursue commercial guidance",
      showSalesActionPill: true,
    };
  }

  const qualifyNext =
    /qualify whether|confirm whether|ask .*whether/i.test(next)
      ? next
      : "Qualify whether a target-hotel or overflow lodging path is open for this edition. Do not claim an open RFP or confirmed overflow without evidence.";

  if (
    looksLikeDefensiveQualifyGuidance(next) &&
    (pursuitConflict || currentKey === CUSTOMER_SALES_ACTION.QUALIFY_OPPORTUNITY)
  ) {
    return {
      ...opp,
      salesActionKey: CUSTOMER_SALES_ACTION.QUALIFY_OPPORTUNITY,
      salesActionPillLabel:
        CUSTOMER_SALES_ACTION_LABEL[CUSTOMER_SALES_ACTION.QUALIFY_OPPORTUNITY],
      salesActionReason:
        "Credible lodging demand with unconfirmed sourcing — Qualify, do not Win Room Block",
      recommendedNextStep: qualifyNext,
      recommendedAction: qualifyNext,
      showSalesActionPill: true,
    };
  }
  return opp;
}

function remediateField(value, org, sourcingLabel) {
  let s = text(value);
  if (!s) return s;
  s = stripEmptyParentheses(s);
  s = rewritePlaceholderUncertainty(s);
  s = stripDuplicatedSourcingBoilerplate(s);
  if (org) s = remediateDuplicateOrgAssociationSentence(s, org);
  s = softenUnsupportedConfirmedRoomClaims(s, sourcingLabel);
  return s;
}

/**
 * Apply full customer-facing content remediation to a DTO or opportunity.
 */
export function applyCustomerFacingContentRemediation(opportunity = {}) {
  const reasons = [];
  let next = { ...opportunity };
  const org = text(next.organizationName || next.displayCompanyName);
  const sourcing = text(
    next.hotelSourcingStatusLabel || next.targetHotelSourcingStatusLabel,
  );

  const beforeWhat = text(next.whatIsHappening);
  for (const field of [
    "whatIsHappening",
    "summary",
    "whyThisHotel",
    "whyActNow",
    "potentialHotelBusiness",
    "potentialBusiness",
    "recommendedNextStep",
    "recommendedAction",
    "suggestedAction",
    "cardWhyNowLine",
    "displayTitle",
    "title",
  ]) {
    if (typeof next[field] !== "string" || !next[field]) continue;
    const prev = next[field];
    next[field] = remediateField(prev, org, sourcing);
    if (prev !== next[field]) reasons.push(`${field}:remediated`);
  }

  if (beforeWhat && beforeWhat !== text(next.whatIsHappening)) {
    reasons.push("whatIsHappening:org_or_sourcing_boilerplate");
  }

  if (next.tileClassificationLine || sourcing) {
    const prevTile = text(next.tileClassificationLine);
    next.tileClassificationLine = dedupeTileClassificationSourcing(
      next.tileClassificationLine,
      sourcing,
    );
    if (prevTile !== text(next.tileClassificationLine)) {
      reasons.push("tileClassificationLine:sourcing_deduped");
    }
  }

  // Sentence capitalization after period for customer polish
  for (const field of [
    "recommendedNextStep",
    "recommendedAction",
    "suggestedAction",
    "whatIsHappening",
    "whyActNow",
    "whyThisHotel",
  ]) {
    if (typeof next[field] === "string" && next[field]) {
      next[field] = text(next[field]).replace(
        /\.\s+([a-z])/g,
        (_, c) => `. ${c.toUpperCase()}`,
      );
    }
  }

  const beforeAction = text(next.salesActionPillLabel);
  // Prefer shared resolver (Qualify vs Monitor nuance), then align guidance text.
  if (next.publishedPresentationLocked !== true) {
    const resolved = resolveCustomerSalesAction({
      ...next,
      recommendedAction: next.recommendedNextStep || next.recommendedAction,
    });
    if (resolved?.key && CUSTOMER_SALES_ACTION_LABEL[resolved.key]) {
      next.salesActionKey = resolved.key;
      next.salesActionPillLabel = resolved.label;
      next.salesActionReason = resolved.reason;
    }
  }
  next = alignSalesActionWithGuidance(next);
  if (beforeAction && beforeAction !== text(next.salesActionPillLabel)) {
    reasons.push("salesAction:aligned_to_guidance");
  }

  if (!reasons.length) return opportunity;
  return {
    ...next,
    customerFacingContentRemediation: {
      version: GDI_CUSTOMER_FACING_CONTENT_REMEDIATION_V1,
      reasons,
    },
  };
}
