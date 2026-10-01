/**
 * Classify owner-portfolio claims (P1.7 Strategy B).
 * A portfolio page may list owned, managed, developed, invested, or historical assets.
 * Do not overload OWNED_BY.
 */

export const PORTFOLIO_CLASSIFY_VERSION = "ownership-portfolio-classify-v1";

export const PORTFOLIO_TEMPORAL_STATUSES = Object.freeze([
  "current",
  "sold",
  "acquired",
  "under_development",
  "planned",
  "unknown",
]);

const OPERATED = /\b(operat(?:e|es|ed|ing|or|ion)?s?|manag(?:e|es|ed|ing|ement)|administr[ao]|opera(?:do|dora)|gesti[oó]n)\b/i;
const DEVELOPED = /\b(develop(?:ed|er|ment)|desarroll(?:o|ado|ador)|promotor)\b/i;
const SPONSORED = /\b(sponsor(?:ed|s)?|backed by|portfolio company)\b/i;
const INVESTED = /\b(invest(?:ment|ed)|inversi[oó]n|participaci[oó]n|stake)\b/i;
const OWNED = /\b(own(?:s|ed|ership)|propietari[oa]|activos propios|owned assets|portfolio of owned)\b/i;
const SOLD = /\b(sold|divest(?:ed|iture)|disposed|enajenad|vendid[oa]|no longer owns)\b/i;
const PIPELINE = /\b(pipeline|under construction|under development|en desarrollo|planned|pre-opening)\b/i;
const LEASED = /\b(leas(?:e|ed|ing)|arrend(?:ado|amiento)|sale[- ]and[- ]leaseback)\b/i;
const JV = /\b(joint venture|\bjv\b|asociaci[oó]n|co-invest)\b/i;

/**
 * @param {string} pageText
 * @param {string} [assetContext]
 * @param {string} [pagePrior] — owner-level prior from first-party type (reit → OWNED_BY)
 */
export function classifyPortfolioRelationship(pageText, assetContext = "", pagePrior = null) {
  const blob = `${pageText || ""}\n${assetContext || ""}`;
  const reasons = [];

  let temporal = "current";
  if (SOLD.test(blob)) {
    temporal = "sold";
    reasons.push("disposition_language");
  } else if (PIPELINE.test(blob)) {
    temporal = /planned|pre-opening/i.test(blob) ? "planned" : "under_development";
    reasons.push("pipeline_language");
  } else if (/\bacquir(?:ed|es)|adquirid/i.test(blob)) {
    temporal = "acquired";
    reasons.push("acquisition_language");
  }

  let relationshipType = null;
  let observationOnly = false;

  if (LEASED.test(blob) && !OWNED.test(blob)) {
    relationshipType = "LEASED_FROM";
    reasons.push("lease_language");
    observationOnly = true;
  } else if (JV.test(blob) && !OWNED.test(blob)) {
    relationshipType = "JV_WITH";
    reasons.push("jv_language");
  } else if (OPERATED.test(blob) && !OWNED.test(blob)) {
    relationshipType = "OPERATED_BY";
    reasons.push("operator_language");
  } else if (DEVELOPED.test(blob) && !OWNED.test(blob) && !SPONSORED.test(blob)) {
    relationshipType = "DEVELOPED_BY";
    reasons.push("developer_language");
  } else if (INVESTED.test(blob) && !OWNED.test(blob) && !SPONSORED.test(blob)) {
    relationshipType = "INVESTED_IN_BY";
    reasons.push("investment_language_not_control");
  } else if (SPONSORED.test(blob) && !OWNED.test(blob)) {
    relationshipType = "SPONSORED_BY";
    reasons.push("sponsor_language");
  } else if (OWNED.test(blob)) {
    relationshipType = "OWNED_BY";
    reasons.push("owned_language");
  } else if (pagePrior && ["OWNED_BY", "SPONSORED_BY", "OPERATED_BY", "DEVELOPED_BY"].includes(pagePrior)) {
    relationshipType = pagePrior;
    reasons.push(`page_prior:${pagePrior}`);
  } else {
    relationshipType = "SPONSORED_BY";
    reasons.push("unspecified_portfolio_listing");
    observationOnly = true;
  }

  if (temporal === "sold") {
    observationOnly = true;
    reasons.push("historical_not_current_edge");
  }

  return {
    relationship_type: relationshipType,
    temporal_status: temporal,
    is_current: temporal === "current" || temporal === "acquired",
    observation_only: observationOnly,
    reasons,
  };
}
