/**
 * Classify renewal / amendment / termination outcomes from later-document evidence.
 * Unexercised options are NEVER treated as executed renewals.
 * Same brand retention alone does NOT prove franchise renewal.
 */

import { RENEWAL_STATUS_CLASS } from "./contract-terms-schema-proposal-v4.js";

export const CONTRACT_TERMS_RENEWAL_CLASSIFIER_V1 = "contract-terms-renewal-classifier-v1";

function hasExplicitRenewalLanguage(text) {
  return /\b(?:renewed|renewal\s+(?:of|executed)|extended\s+the\s+(?:franchise|management|license)|amendment\s+(?:to\s+)?(?:the\s+)?(?:franchise|management))\b/i.test(
    String(text || ""),
  );
}

function hasTerminationLanguage(text) {
  return /\b(?:terminat(?:ed|ion)|disaffiliat(?:ed|ion)|deflag|reflag(?:ged)?|ceased\s+(?:to\s+)?operat)\b/i.test(
    String(text || ""),
  );
}

function hasAutomaticRenewal(text) {
  return /\bautomatic(?:ally)?\s+renew/i.test(String(text || ""));
}

function hasOptionAvailable(text) {
  return /\brenewal\s+options?\b/i.test(String(text || "")) || /\boption\s+to\s+renew\b/i.test(String(text || ""));
}

/**
 * @param {object} args
 * @param {string} args.originalText
 * @param {string} [args.laterText]
 * @param {boolean} [args.sameBrandObserved]
 * @param {boolean} [args.expirationStillCited]
 * @param {Date|string|null} [args.scheduledExpiration]
 * @param {Date|string|null} [args.asOf]
 */
export function classifyRenewalOutcome(args = {}) {
  const original = String(args.originalText || "");
  const later = String(args.laterText || "");
  const combined = `${original}\n${later}`;
  const asOf = args.asOf ? new Date(args.asOf) : new Date();
  const exp = args.scheduledExpiration ? new Date(args.scheduledExpiration) : null;
  const pastExpiration = exp && !Number.isNaN(exp.getTime()) && exp.getTime() < asOf.getTime();

  const reasons = [];
  let status = RENEWAL_STATUS_CLASS.STATUS_UNCONFIRMED;

  if (hasTerminationLanguage(later) || (pastExpiration && hasTerminationLanguage(combined))) {
    if (/\breflag|disaffiliat|replaced\b/i.test(later)) {
      status = RENEWAL_STATUS_CLASS.REPLACED_OR_REFLAGGED_VERIFIED;
      reasons.push("Later document describes reflag/disaffiliation/replacement");
    } else {
      status = RENEWAL_STATUS_CLASS.TERMINATED_VERIFIED;
      reasons.push("Later document describes termination");
    }
  } else if (/\brenewed\b|\brenewal\s+executed\b/i.test(later) && hasExplicitRenewalLanguage(later)) {
    status = RENEWAL_STATUS_CLASS.RENEWAL_EXECUTED_VERIFIED;
    reasons.push("Later document explicitly states renewal executed");
  } else if (/\bextended\b.*\b(?:franchise|management|license|term)\b/i.test(later)) {
    status = RENEWAL_STATUS_CLASS.EXTENSION_EXECUTED_VERIFIED;
    reasons.push("Later document explicitly states extension");
  } else if (hasAutomaticRenewal(original) || hasAutomaticRenewal(later)) {
    status = RENEWAL_STATUS_CLASS.AUTOMATIC_RENEWAL_CONDITIONAL;
    reasons.push("Automatic renewal language found; execution not independently verified");
  } else if (hasOptionAvailable(original)) {
    status = RENEWAL_STATUS_CLASS.RENEWAL_OPTION_AVAILABLE;
    reasons.push("Renewal option disclosed; not treated as executed");
  } else if (args.expirationStillCited && !pastExpiration) {
    status = RENEWAL_STATUS_CLASS.CURRENT_TERMS_CORROBORATED;
    reasons.push("Expiration still cited and not past; no contrary later evidence");
  } else if (args.sameBrandObserved && pastExpiration) {
    status = RENEWAL_STATUS_CLASS.STATUS_UNCONFIRMED;
    reasons.push(
      "Same brand observed after scheduled expiration is NOT proof of franchise renewal — STATUS_UNCONFIRMED",
    );
  } else if (pastExpiration && !later) {
    status = RENEWAL_STATUS_CLASS.STATUS_UNCONFIRMED;
    reasons.push("Scheduled expiration passed without later renewal/termination evidence");
  }

  return {
    version: CONTRACT_TERMS_RENEWAL_CLASSIFIER_V1,
    renewalStatusClass: status,
    reasons,
    rules: {
      unexercisedOptionNotExecuted: true,
      sameBrandNotProofOfRenewal: true,
      absenceOfTerminationNotProofOfRenewal: true,
    },
  };
}

export default { classifyRenewalOutcome, RENEWAL_STATUS_CLASS };
