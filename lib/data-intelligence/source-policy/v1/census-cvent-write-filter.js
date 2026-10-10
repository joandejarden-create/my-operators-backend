/**
 * Strip Cvent venue/hotel canonical census writes; retain discovery steward notes.
 * Used by Choice + LATAM Cvent matchers (source-policy-v1 follow-up).
 */

import {
  SOURCE_POLICY_VERSION,
  SourceContentDomain,
} from "./source-roles.js";
import { canPersistAsCanonical } from "./gates.js";
import { createDiscoveryResearchCandidate } from "./gates.js";

/** Census fields that must not be written from Cvent venue/hotel alone. */
export const CVENT_VENUE_CENSUS_BLOCKED_WRITE_FIELDS = Object.freeze([
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Rooms Source Type",
  "Rooms Notes",
  "Address",
  "Address Confidence",
  "Address Source URL",
  "Phone",
  "Hotel Description - Source Text",
  "Amenities - Source Text",
  "Official Property URL",
  "Current Brand",
  "Brand Family",
  "Property Type",
  "Asset Context",
  "Meeting Space Flag",
  "Canonical Property Name",
]);

/**
 * @param {Record<string, unknown>} patch
 * @param {{ sourceUrl?: string, venue?: object, allowIdentityFields?: boolean }} [opts]
 */
export function filterCventVenueCensusPatch(patch = {}, opts = {}) {
  const sourceUrl = opts.sourceUrl || "";
  const out = { ...patch };
  const blocked = [];
  const candidates = [];

  for (const field of CVENT_VENUE_CENSUS_BLOCKED_WRITE_FIELDS) {
    if (!Object.prototype.hasOwnProperty.call(out, field)) continue;
    const gate = canPersistAsCanonical({
      url: sourceUrl,
      contentDomain: SourceContentDomain.CVENT_VENUE_HOTEL,
      field,
      candidateValue: out[field],
    });
    if (!gate.ok) {
      candidates.push(
        createDiscoveryResearchCandidate(
          {
            url: sourceUrl,
            contentDomain: SourceContentDomain.CVENT_VENUE_HOTEL,
            field,
            candidateValue: out[field],
          },
          {
            notes: `Blocked census write from Cvent venue (${SOURCE_POLICY_VERSION})`,
          }
        )
      );
      blocked.push(field);
      delete out[field];
    }
  }

  // Drop orphan Last Reviewed Date if nothing else remains except notes
  const remainingKeys = Object.keys(out).filter(
    (k) => k !== "Last Reviewed Date" && k !== "Notes for Steward"
  );
  if (!remainingKeys.length) {
    delete out["Last Reviewed Date"];
  }

  let stewardNote = null;
  if (candidates.length) {
    const bits = candidates
      .slice(0, 12)
      .map((c) => `${c.field}=${JSON.stringify(c.candidateValue)}`)
      .join("; ");
    stewardNote = `cvent_discovery_only ${SOURCE_POLICY_VERSION}: ${bits}`.slice(
      0,
      4000
    );
  }

  return {
    patch: out,
    blockedFields: blocked,
    discoveryCandidates: candidates,
    stewardDiscoveryNote: stewardNote,
    sourcePolicyVersion: SOURCE_POLICY_VERSION,
  };
}

/**
 * Append discovery steward note without overwriting unrelated steward content.
 */
export function appendCventDiscoveryStewardNote(existingNote, discoveryNote) {
  const prev = String(existingNote || "").trim();
  const add = String(discoveryNote || "").trim();
  if (!add) return prev || null;
  if (prev.includes("cvent_discovery_only")) {
    return prev;
  }
  return [prev, add].filter(Boolean).join("\n").slice(0, 90000);
}
