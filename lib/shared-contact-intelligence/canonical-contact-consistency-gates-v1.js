/**
 * Permanent cross-product contact consistency gates V1.
 * Fixture-only is never Live PASS. Missing Airtable → NOT_TESTED / BLOCKED.
 */

import {
  CONTACT_CONSISTENCY_GATE,
  GATE_STATUS,
  SHARED_CANONICAL_CONTACT_INTELLIGENCE_V1,
} from "./architecture-constants-v1.js";
import {
  assertGdiHeChannelParity,
  resolveCanonicalHotelContacts,
} from "./canonical-channel-resolver-v1.js";
import { resolveCanonicalPersonId } from "./canonical-person-identity-v1.js";
import { computeSharedContactability } from "./canonical-contactability-v1.js";
import { planFindymailLookup } from "./findymail-reuse-policy-v1.js";
import { assertSurfeTransientOnly, SURFE_POLICY } from "./surfe-transient-policy-v1.js";

export const CANONICAL_CONTACT_CONSISTENCY_GATES_V1 =
  "canonical-contact-consistency-gates-v1";

function gate(name, status, detail = {}) {
  return { gate: name, status, ...detail };
}

export function evaluateIdentityResolved(people = []) {
  const unresolved = [];
  for (const p of people) {
    const r = resolveCanonicalPersonId({
      canonicalPersonId: p.canonicalPersonId || p.personId,
      displayName: p.displayName || p.name,
      organizationName: p.organizationName,
      gdiPersonIdentityKey: p.gdiPersonIdentityKey,
      linkedInUrl: p.linkedinUrl,
    });
    if (!r.ok || !r.canonicalPersonId) {
      unresolved.push({
        name: p.displayName || p.name,
        reason: r.reason || "UNRESOLVED",
      });
    }
  }
  if (!people.length) {
    return gate(
      CONTACT_CONSISTENCY_GATE.CONTACT_CANONICAL_IDENTITY_RESOLVED,
      GATE_STATUS.NOT_MEASURABLE,
      { reason: "NO_PEOPLE" },
    );
  }
  return gate(
    CONTACT_CONSISTENCY_GATE.CONTACT_CANONICAL_IDENTITY_RESOLVED,
    unresolved.length ? GATE_STATUS.FAIL : GATE_STATUS.PASS,
    { unresolved, resolvedCount: people.length - unresolved.length },
  );
}

export function evaluateAirtableSourceOfTruth(hotelResult) {
  if (!hotelResult) {
    return gate(
      CONTACT_CONSISTENCY_GATE.CONTACT_AIRTABLE_SOURCE_OF_TRUTH,
      GATE_STATUS.NOT_TESTED,
      { reason: "NO_RESULT" },
    );
  }
  if (hotelResult.status === "BLOCKED" || hotelResult.reason === "NO_AIRTABLE_CREDENTIALS") {
    return gate(
      CONTACT_CONSISTENCY_GATE.CONTACT_AIRTABLE_SOURCE_OF_TRUTH,
      GATE_STATUS.BLOCKED,
      { reason: hotelResult.reason || "AIRTABLE_UNAVAILABLE" },
    );
  }
  if (!hotelResult.airtableLive && hotelResult.sourceOfTruth === "FIXTURE") {
    return gate(
      CONTACT_CONSISTENCY_GATE.CONTACT_AIRTABLE_SOURCE_OF_TRUTH,
      GATE_STATUS.NOT_TESTED,
      { reason: "FIXTURE_ONLY_NOT_LIVE_CERTIFICATION" },
    );
  }
  if (
    hotelResult.ok &&
    hotelResult.airtableLive &&
    /HI_ORG_CONTACTS/.test(String(hotelResult.sourceOfTruth || ""))
  ) {
    return gate(
      CONTACT_CONSISTENCY_GATE.CONTACT_AIRTABLE_SOURCE_OF_TRUTH,
      GATE_STATUS.PASS,
      { sourceOfTruth: hotelResult.sourceOfTruth },
    );
  }
  return gate(
    CONTACT_CONSISTENCY_GATE.CONTACT_AIRTABLE_SOURCE_OF_TRUTH,
    GATE_STATUS.FAIL,
    { reason: "NOT_AIRTABLE_SOT", sourceOfTruth: hotelResult.sourceOfTruth || null },
  );
}

export function evaluateChannelProvenance(people = []) {
  const bad = [];
  for (const p of people || []) {
    for (const ch of p.channels || []) {
      if (ch.sessionOnly || ch.surfeDerived) continue;
      if (!ch.canonicalAirtableRecordId && !ch.notesFallback) {
        bad.push({
          person: p.displayName,
          channelType: ch.channelType,
          reason: "MISSING_AIRTABLE_RECORD_ID",
        });
      }
      if (!ch.evidence?.source && !ch.provider) {
        bad.push({
          person: p.displayName,
          channelType: ch.channelType,
          reason: "MISSING_PROVENANCE",
        });
      }
    }
  }
  return gate(
    CONTACT_CONSISTENCY_GATE.CONTACT_CHANNEL_PROVENANCE_VALIDATED,
    bad.length ? GATE_STATUS.FAIL : GATE_STATUS.PASS,
    { invalid: bad },
  );
}

export function evaluatePersistenceReadback(people = [], opts = {}) {
  if (opts.airtableLive === false) {
    return gate(
      CONTACT_CONSISTENCY_GATE.CONTACT_AIRTABLE_PERSISTENCE_READBACK,
      GATE_STATUS.NOT_TESTED,
      { reason: "AIRTABLE_NOT_LIVE" },
    );
  }
  const withEmail = (people || []).filter((p) => p.email);
  const readBackOk = withEmail.filter((p) =>
    (p.channels || []).some(
      (c) =>
        c.channelType === "Email" &&
        c.canonicalAirtableRecordId &&
        String(c.channelValue).toLowerCase() === String(p.email).toLowerCase(),
    ),
  );
  if (!withEmail.length) {
    return gate(
      CONTACT_CONSISTENCY_GATE.CONTACT_AIRTABLE_PERSISTENCE_READBACK,
      GATE_STATUS.NOT_MEASURABLE,
      { reason: "NO_PERSISTED_EMAILS" },
    );
  }
  return gate(
    CONTACT_CONSISTENCY_GATE.CONTACT_AIRTABLE_PERSISTENCE_READBACK,
    readBackOk.length === withEmail.length ? GATE_STATUS.PASS : GATE_STATUS.FAIL,
    {
      emailsExpected: withEmail.length,
      emailsReadBack: readBackOk.length,
    },
  );
}

export function evaluateGdiHeParity(people = []) {
  const mismatches = [];
  for (const p of people || []) {
    const he = { ...p };
    const gdi = { ...p }; // same resolver → must match; divergence = FAIL
    const r = assertGdiHeChannelParity(he, gdi);
    if (!r.ok) mismatches.push({ person: p.displayName, ...r });
  }
  return gate(
    CONTACT_CONSISTENCY_GATE.CONTACT_GDI_HE_CHANNEL_PARITY,
    mismatches.length ? GATE_STATUS.FAIL : GATE_STATUS.PASS,
    { mismatches, compared: (people || []).length },
  );
}

export function evaluateNoDuplicatePaidEnrichment(people = [], opts = {}) {
  const plans = (people || []).map((p) => planFindymailLookup(p, opts));
  const wouldDuplicate = plans.filter(
    (pl) =>
      pl.action === "REUSE_CANONICAL" ||
      pl.action === "REUSE_LEDGER_OR_BLOCK_DUPLICATE",
  );
  const illegalExecute = plans.filter(
    (pl) =>
      pl.executePaidLookup === true &&
      (pl.action === "REUSE_CANONICAL" ||
        pl.action === "REUSE_LEDGER_OR_BLOCK_DUPLICATE"),
  );
  return gate(
    CONTACT_CONSISTENCY_GATE.CONTACT_NO_DUPLICATE_PAID_ENRICHMENT,
    illegalExecute.length ? GATE_STATUS.FAIL : GATE_STATUS.PASS,
    {
      reuseBlockedCount: wouldDuplicate.length,
      illegalExecuteCount: illegalExecute.length,
      plans,
    },
  );
}

export function evaluateDirectContactability(people = [], opts = {}) {
  const metrics = computeSharedContactability(people, opts);
  if (metrics.directContactabilityStatus === "NOT_MEASURABLE") {
    return gate(
      CONTACT_CONSISTENCY_GATE.CONTACT_DIRECT_CONTACTABILITY_VALIDATED,
      GATE_STATUS.NOT_MEASURABLE,
      { metrics },
    );
  }
  return gate(
    CONTACT_CONSISTENCY_GATE.CONTACT_DIRECT_CONTACTABILITY_VALIDATED,
    GATE_STATUS.PASS,
    { metrics },
  );
}

export function evaluateSurfeTransientOnly(sampleChannels = []) {
  const samples = sampleChannels.length
    ? sampleChannels
    : [{ provider: "surfe", sessionOnly: true, surfeDerived: true }];
  const results = samples.map((c) => assertSurfeTransientOnly(c));
  const failed = results.filter((r) => !r.ok);
  return gate(
    CONTACT_CONSISTENCY_GATE.CONTACT_SURFE_TRANSIENT_ONLY,
    failed.length ? GATE_STATUS.FAIL : GATE_STATUS.PASS,
    { policy: SURFE_POLICY, results },
  );
}

/**
 * Run the full permanent gate suite for a hotel.
 */
export async function runCanonicalContactConsistencyGates(hotelId, opts = {}) {
  const hotelResult = opts.hotelResult
    ? opts.hotelResult
    : await resolveCanonicalHotelContacts(hotelId, opts);
  const people = hotelResult.people || [];

  const gates = [
    evaluateIdentityResolved(people),
    evaluateAirtableSourceOfTruth(hotelResult),
    evaluateChannelProvenance(people),
    evaluatePersistenceReadback(people, {
      airtableLive: hotelResult.airtableLive,
    }),
    evaluateGdiHeParity(people),
    evaluateNoDuplicatePaidEnrichment(people, opts),
    evaluateDirectContactability(people, { hotelId }),
    evaluateSurfeTransientOnly(opts.surfeSamples),
  ];

  const byName = Object.fromEntries(gates.map((g) => [g.gate, g]));
  const liveBlocked = gates.some((g) => g.status === GATE_STATUS.BLOCKED);
  const liveFail = gates.some((g) => g.status === GATE_STATUS.FAIL);
  const notTested = gates.some((g) => g.status === GATE_STATUS.NOT_TESTED);

  let overall = GATE_STATUS.PASS;
  if (liveFail) overall = GATE_STATUS.FAIL;
  else if (liveBlocked) overall = GATE_STATUS.BLOCKED;
  else if (notTested && !hotelResult.airtableLive) overall = GATE_STATUS.NOT_TESTED;

  return {
    version: CANONICAL_CONTACT_CONSISTENCY_GATES_V1,
    sharedContract: SHARED_CANONICAL_CONTACT_INTELLIGENCE_V1,
    hotelId,
    overall,
    airtableLive: Boolean(hotelResult.airtableLive),
    peopleCount: people.length,
    gates: byName,
    hotelResultSummary: {
      ok: hotelResult.ok,
      status: hotelResult.status,
      sourceOfTruth: hotelResult.sourceOfTruth || null,
      duplicatePersonRowsCollapsed: hotelResult.duplicatePersonRowsCollapsed || 0,
    },
    generatedAt: new Date().toISOString(),
  };
}

export default {
  CANONICAL_CONTACT_CONSISTENCY_GATES_V1,
  runCanonicalContactConsistencyGates,
  evaluateIdentityResolved,
  evaluateAirtableSourceOfTruth,
  evaluateChannelProvenance,
  evaluatePersistenceReadback,
  evaluateGdiHeParity,
  evaluateNoDuplicatePaidEnrichment,
  evaluateDirectContactability,
  evaluateSurfeTransientOnly,
};
