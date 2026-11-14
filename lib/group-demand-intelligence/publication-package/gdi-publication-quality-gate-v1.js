/**
 * GDI publication presentation-quality gate V1.
 *
 * Separate from parity: two identical poor-quality reports must not certify.
 * Prefer correcting generation; this gate detects customer-facing defects.
 *
 * Also enforces commercial-expiration gate on customer-facing inventories
 * (expired one-time / duplicates must not certify as active pursuit).
 */

import {
  RESEARCH_WATCH_DISPOSITIONS,
  classifyCommercialTemporalStatus,
} from "../commercial-expiration-gate-v1.js";

export const GDI_PUBLICATION_QUALITY_GATE_V1 =
  "gdi-publication-quality-gate-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function lower(v) {
  return text(v).toLowerCase();
}

function paragraphs(blob) {
  return text(blob)
    .split(/\n{2,}|(?<=\.)\s{2,}/)
    .map((p) => text(p))
    .filter(Boolean);
}

function hasEmptyParens(s) {
  return /\(\s*\)/.test(String(s || ""));
}

function hasEmptyPunctWrappers(s) {
  return /\[\s*\]|\{\s*\}|—\s*—|\.\s*\./.test(String(s || ""));
}

function looksLikeInternalId(s) {
  return /\b(rec[A-Za-z0-9]{10,}|gdi_[a-z0-9_]{8,}|QUALIFY_OPPORTUNITY|SALES_READY|OVERFLOW_HOUSING)\b/.test(
    String(s || ""),
  );
}

function repeatedPhrase(blob, phrase) {
  if (!phrase || phrase.length < 8) return false;
  const re = new RegExp(
    phrase.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"),
    "gi",
  );
  const matches = String(blob || "").match(re);
  return (matches || []).length >= 2;
}

function duplicateOrgAssociation(whatIsHappening, org, title) {
  const w = text(whatIsHappening);
  const o = text(org);
  const t = text(title);
  if (!w || !o) return false;
  // "Org is associated with Org"
  if (
    lower(w).includes(`${lower(o)} is associated with ${lower(o)}`) ||
    lower(w).includes(`${lower(o)} is tied to ${lower(o)}`)
  ) {
    return true;
  }
  if (t && lower(o) === lower(t) && /is associated with/i.test(w)) {
    const after = w.split(/is associated with/i)[1] || "";
    if (lower(after).includes(lower(o))) return true;
  }
  return false;
}

/**
 * Scan one opportunity's customer-facing fields for quality defects.
 */
export function auditOpportunityPresentationQuality(opp = {}) {
  const findings = [];
  const id = text(opp.id) || "(missing-id)";
  const org = opp.organizationName || opp.displayCompanyName || "";
  const title = opp.displayTitle || opp.title || "";
  const sourcing = text(opp.hotelSourcingStatusLabel);
  const tile = text(opp.tileClassificationLine);
  const fields = {
    whatIsHappening: opp.whatIsHappening,
    potentialHotelBusiness: opp.potentialHotelBusiness || opp.potentialBusiness,
    whyThisHotel: opp.whyThisHotel,
    whyActNow: opp.whyActNow,
    recommendedNextStep: opp.recommendedNextStep || opp.recommendedAction,
    summary: opp.summary,
    cardWhyNowLine: opp.cardWhyNowLine,
    tileClassificationLine: tile,
    hotelSourcingStatusLabel: sourcing,
    displayTitle: title,
  };

  if (duplicateOrgAssociation(fields.whatIsHappening, org, title)) {
    findings.push({
      code: "DUPLICATE_ORGANIZATION_NAME",
      opportunityId: id,
      field: "whatIsHappening",
      detail: "Organization name repeated in association sentence",
    });
  }

  if (sourcing && tile && lower(tile).includes(lower(sourcing))) {
    // Tile line already embeds sourcing while badge also shows it — customer sees twice.
    findings.push({
      code: "REPEATED_SOURCING_STATUS",
      opportunityId: id,
      field: "tileClassificationLine|hotelSourcingStatusLabel",
      detail: "Sourcing status appears in both tile line and sourcing label",
    });
  }

  // Badge owns hotelSourcingStatusLabel once — count repeats only in narrative fields.
  const narrativeJoined = Object.entries(fields)
    .filter(([k]) => k !== "hotelSourcingStatusLabel")
    .map(([, v]) => text(v))
    .join(" · ");
  if (
    sourcing &&
    (repeatedPhrase(narrativeJoined, sourcing) ||
      lower(narrativeJoined).includes(lower(sourcing)))
  ) {
    findings.push({
      code: "REPEATED_SOURCING_STATUS",
      opportunityId: id,
      field: "customer_text_blob",
      detail: "Identical sourcing status repeated in customer text",
    });
  }

  for (const [field, value] of Object.entries(fields)) {
    const s = text(value);
    if (!s) continue;
    if (hasEmptyParens(s)) {
      findings.push({
        code: "EMPTY_PARENTHESES",
        opportunityId: id,
        field,
        detail: "Empty parentheses in customer-facing text",
      });
    }
    if (hasEmptyPunctWrappers(s)) {
      findings.push({
        code: "EMPTY_PUNCTUATION_WRAPPER",
        opportunityId: id,
        field,
        detail: "Empty punctuation wrapper in customer-facing text",
      });
    }
    if (/TODO|TBD|PLACEHOLDER|lorem ipsum|FIXME/i.test(s)) {
      findings.push({
        code: "PLACEHOLDER_TEXT",
        opportunityId: id,
        field,
        detail: "Placeholder text in customer-facing content",
      });
    }
    if (looksLikeInternalId(s) && field !== "id") {
      // Soft: only flag when the whole field is an enum-like token
      if (/^[A-Z][A-Z0-9_]{5,}$/.test(s) || /^rec[A-Za-z0-9]{10,}$/.test(s)) {
        findings.push({
          code: "INTERNAL_ID_OR_ENUM",
          opportunityId: id,
          field,
          detail: "Internal id/enum leaked to customer surface",
        });
      }
    }
  }

  const paras = paragraphs(
    [fields.whatIsHappening, fields.whyThisHotel, fields.whyActNow]
      .map(text)
      .filter(Boolean)
      .join("\n\n"),
  );
  const seen = new Set();
  for (const p of paras) {
    const k = lower(p);
    if (k.length < 40) continue;
    if (seen.has(k)) {
      findings.push({
        code: "REPEATED_PARAGRAPH",
        opportunityId: id,
        field: "narrative",
        detail: "Identical paragraph repeated across sections",
      });
    }
    seen.add(k);
  }

  const action = text(fields.recommendedNextStep);
  const pill = text(opp.salesActionPillLabel);
  // Win Room Block / Build Account / Contact Buyer must not sit beside
  // closed-path or defensive "do not claim open RFP" guidance.
  // Qualify Opportunity + "do not claim open RFP" is coherent.
  if (
    action &&
    pill &&
    /win room block|build account|contact buyer/i.test(pill) &&
    /do not pursue|sourcing appears closed|monitor for sourcing|do not claim open rfp|fully placed/i.test(
      action,
    )
  ) {
    findings.push({
      code: "CONFLICTING_COMMERCIAL_ACTIONS",
      opportunityId: id,
      field: "salesActionPillLabel|recommendedNextStep",
      detail: "Sales-action pill conflicts with recommended next step",
    });
  }
  if (
    action &&
    pill &&
    /monitor/i.test(pill) &&
    /win the (primary )?block|request (an )?rfp|pursue as host/i.test(action) &&
    !/do not|without evidence|unconfirmed/i.test(action)
  ) {
    findings.push({
      code: "CONFLICTING_COMMERCIAL_ACTIONS",
      opportunityId: id,
      field: "salesActionPillLabel|recommendedNextStep",
      detail: "Monitor pill conflicts with active pursuit next-step language",
    });
  }

  // Require affirmative confirmed-demand claims — ignore accurate negations
  // like "not confirmed room demand" / "no confirmed room block".
  const joinedForDemand = `${narrativeJoined} · ${sourcing}`;
  const affirmativeConfirmedDemand =
    /(?<!not\s)(?<!no\s)confirmed room demand|(?<!not\s)confirmed \d+\s*rooms|guaranteed room block at (?:this|the target) hotel/i.test(
      joinedForDemand,
    ) && !/not confirmed room demand|no confirmed room(?:\s+block)?/i.test(joinedForDemand);
  if (
    affirmativeConfirmedDemand &&
    /not established|unconfirmed|not confirmed/i.test(sourcing)
  ) {
    findings.push({
      code: "UNSUPPORTED_CONFIRMED_ROOM_DEMAND",
      opportunityId: id,
      field: "narrative|hotelSourcingStatusLabel",
      detail: "Confirmed room-demand claim while sourcing unconfirmed",
    });
  }

  return findings;
}

/**
 * Run quality gate across a publication package or opportunity list.
 * @param {{ opportunities?: object[], failOnCodes?: string[] }} input
 */
export function runPublicationQualityGate(input = {}) {
  const opportunities = Array.isArray(input.opportunities)
    ? input.opportunities
    : [];
  const findings = [];
  const referenceDate =
    text(input.referenceDate) || new Date().toISOString().slice(0, 10);
  for (const o of opportunities) {
    findings.push(...auditOpportunityPresentationQuality(o));
    if (input.enforceCommercialExpirationGate === false) continue;
    const temporal =
      o.commercialTemporalDisposition ||
      o.commercialExpirationGateV1?.disposition ||
      classifyCommercialTemporalStatus(o, {
        referenceDate,
        dispositionOverride: o.commercialTemporalDisposition || null,
        duplicateOf: o.duplicateOf || o.duplicateEventOf || null,
      }).disposition;
    if (RESEARCH_WATCH_DISPOSITIONS.includes(temporal)) {
      findings.push({
        code: "COMMERCIAL_EXPIRATION_GATE_BLOCK",
        opportunityId: text(o.id) || "(missing-id)",
        field: "commercialTemporalDisposition",
        detail: `Customer-facing inventory includes research/watch disposition ${temporal}`,
      });
    }
  }

  // Deduplicate identical codes per opportunity+field
  const uniq = [];
  const seen = new Set();
  for (const f of findings) {
    const k = `${f.opportunityId}|${f.code}|${f.field}`;
    if (seen.has(k)) continue;
    seen.add(k);
    uniq.push(f);
  }

  const failOnCodes = Array.isArray(input.failOnCodes)
    ? input.failOnCodes
    : [
        "DUPLICATE_ORGANIZATION_NAME",
        "REPEATED_SOURCING_STATUS",
        "EMPTY_PARENTHESES",
        "EMPTY_PUNCTUATION_WRAPPER",
        "PLACEHOLDER_TEXT",
        "INTERNAL_ID_OR_ENUM",
        "REPEATED_PARAGRAPH",
        "CONFLICTING_COMMERCIAL_ACTIONS",
        "UNSUPPORTED_CONFIRMED_ROOM_DEMAND",
        "COMMERCIAL_EXPIRATION_GATE_BLOCK",
      ];
  const blocking = uniq.filter((f) => failOnCodes.includes(f.code));

  return {
    version: GDI_PUBLICATION_QUALITY_GATE_V1,
    ok: blocking.length === 0,
    findingCount: uniq.length,
    blockingCount: blocking.length,
    findings: uniq,
    blocking,
  };
}

/**
 * Light remediation helpers for generation-time fixes (not comparison masking).
 */
export function remediateDuplicateOrgAssociationSentence(sentence, org) {
  const s = text(sentence);
  const o = text(org);
  if (!s || !o) return s;
  const esc = o.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  let out = s.replace(
    new RegExp(
      `^${esc}\\s+is associated with\\s+${esc}(?:\\s*[—\\-]\\s*)?`,
      "i",
    ),
    `${o} — `,
  );
  out = out.replace(
    new RegExp(`^${esc}\\s+is tied to\\s+${esc}(?:\\s*[—\\-]\\s*)?`, "i"),
    `${o} — `,
  );
  return text(out);
}

export function stripEmptyParentheses(s) {
  return text(String(s || "").replace(/\(\s*\)/g, ""));
}

/**
 * Remove duplicate sourcing segment from tileClassificationLine when it
 * already equals hotelSourcingStatusLabel (badge owns that fact once).
 */
export function dedupeTileClassificationSourcing(tileLine, sourcingLabel) {
  const tile = text(tileLine);
  const sourcing = text(sourcingLabel);
  if (!tile || !sourcing) return tile;
  if (!lower(tile).includes(lower(sourcing))) return tile;
  const parts = tile.split(/\s*·\s*/).map(text).filter(Boolean);
  const filtered = parts.filter((p) => lower(p) !== lower(sourcing));
  return filtered.join(" · ") || tile;
}
