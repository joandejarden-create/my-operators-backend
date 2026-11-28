/**
 * Findymail reuse policy V1 — check canonical Airtable + ledger before paid lookup.
 * Never classify an unexecuted Findymail stage as successful.
 */

import {
  assessFindymailEmailEligibility,
  normalizePersonKey,
} from "../group-demand-intelligence/workflow/gdi-findymail-stage4-enrichment-v1.js";
import {
  describeFindymailKeyPresence,
  personAlreadyEnrichedInLedger,
  readFindymailUsageLedger,
} from "../group-demand-intelligence/providers/gdi-findymail-client-v1.js";

export const FINDYMAIL_REUSE_POLICY_V1 = "findymail-reuse-policy-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * Decide whether a paid Findymail lookup is allowed for a person.
 * @param {object} person — shared resolver person or probe
 * @param {object} [opts]
 */
export function planFindymailLookup(person = {}, opts = {}) {
  const name = text(person.displayName || person.name || person.fullName);
  const org = text(person.organizationName || person.organization);
  const existingEmail = text(person.email);
  const personKey = normalizePersonKey(name, org);
  const keyPresence = describeFindymailKeyPresence();

  if (existingEmail && /@/.test(existingEmail)) {
    return {
      version: FINDYMAIL_REUSE_POLICY_V1,
      action: "REUSE_CANONICAL",
      executePaidLookup: false,
      reason: "CANONICAL_EMAIL_ALREADY_PRESENT",
      existingEmail,
      personKey,
      gate: "CONTACT_NO_DUPLICATE_PAID_ENRICHMENT",
      status: "PASS",
      executionClaimAllowed: false,
    };
  }

  const ledger = Array.isArray(opts.ledger)
    ? opts.ledger
    : readFindymailUsageLedger({
        ledgerPath: opts.ledgerPath,
        repoRoot: opts.repoRoot,
      });
  if (personAlreadyEnrichedInLedger(personKey, ledger)) {
    return {
      version: FINDYMAIL_REUSE_POLICY_V1,
      action: "REUSE_LEDGER_OR_BLOCK_DUPLICATE",
      executePaidLookup: false,
      reason: "PRIOR_FINDYMAIL_LOOKUP_RECORDED",
      personKey,
      gate: "CONTACT_NO_DUPLICATE_PAID_ENRICHMENT",
      status: "PASS",
      executionClaimAllowed: false,
    };
  }

  if (!keyPresence.present) {
    return {
      version: FINDYMAIL_REUSE_POLICY_V1,
      action: "BLOCK",
      executePaidLookup: false,
      reason: "FINDYMAIL_CREDENTIALS_MISSING",
      personKey,
      status: "BLOCKED",
      executionClaimAllowed: false,
    };
  }

  if (!opts.authorizationId) {
    return {
      version: FINDYMAIL_REUSE_POLICY_V1,
      action: "BLOCK",
      executePaidLookup: false,
      reason: "SPENDING_AUTHORIZATION_MISSING",
      personKey,
      status: "BLOCKED",
      executionClaimAllowed: false,
    };
  }

  const eligibility = assessFindymailEmailEligibility(
    {
      name,
      fullName: name,
      organization: org,
      email: existingEmail || null,
      companyDomain: person.employerDomain || opts.domain || null,
    },
    { organizationName: org },
    { ledger },
  );

  if (!eligibility?.eligible) {
    return {
      version: FINDYMAIL_REUSE_POLICY_V1,
      action: "SKIP",
      executePaidLookup: false,
      reason: eligibility?.reason || "NOT_ELIGIBLE",
      personKey,
      status: "SKIPPED",
      executionClaimAllowed: false,
    };
  }

  return {
    version: FINDYMAIL_REUSE_POLICY_V1,
    action: "EXECUTE_IF_AUTHORIZED",
    executePaidLookup: opts.allowPaid === true,
    reason:
      opts.allowPaid === true
        ? "UNRESOLVED_ELIGIBLE_PERSON"
        : "ELIGIBLE_BUT_PAID_NOT_ENABLED",
    personKey,
    authorizationId: opts.authorizationId,
    status: opts.allowPaid === true ? "READY" : "BLOCKED",
    /** Unexecuted must never be reported as successful. */
    executionClaimAllowed: false,
  };
}

export default {
  FINDYMAIL_REUSE_POLICY_V1,
  planFindymailLookup,
};
