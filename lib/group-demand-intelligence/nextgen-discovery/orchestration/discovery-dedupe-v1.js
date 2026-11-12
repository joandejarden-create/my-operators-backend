/**
 * Pre-deep-research dedupe against hotel inventory, ecosystem graph,
 * prior hunter discoveries, Apify/Exa research keys, and rejected/monitored sets.
 */

import { createHash } from "node:crypto";
import { opportunityDedupKey } from "../organization-demand-ecosystem-v1.js";

export const DISCOVERY_DEDUPE_V1 = "gdi-nextgen-discovery-dedupe-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim().toLowerCase();
}

function hash(s) {
  return createHash("sha256").update(String(s || "")).digest("hex").slice(0, 16);
}

export function discoveryIdentityKey(row = {}) {
  const org = text(row.organizationName);
  const act = text(row.activityTitle || row.demandTrigger);
  const date = text(row.activityDate).slice(0, 10);
  return `disc_${hash([org, act, date].join("|"))}`;
}

export function buildDedupeKeySets({
  existingHotelOpportunities = [],
  ecosystemGraph = null,
  priorHunterDiscoveries = [],
  priorApifyExaKeys = [],
  rejectedOrMonitored = [],
  existingKeys = [],
} = {}) {
  const keys = new Set(
    (existingKeys || []).map((k) => String(k || "").trim()).filter(Boolean)
  );

  for (const o of existingHotelOpportunities || []) {
    const id = String(o.id || o.opportunityId || "").trim();
    if (id) keys.add(id);
    keys.add(
      opportunityDedupKey({
        organizationName: o.organizationName || o.title,
        activityTitle: o.activityTitle || o.title,
        activityDate: o.eventStartDate || o.activityDate,
      })
    );
    keys.add(
      discoveryIdentityKey({
        organizationName: o.organizationName || o.organizingEntity,
        activityTitle: o.activityTitle || o.title,
        activityDate: o.eventStartDate || o.activityDate,
      })
    );
  }

  if (ecosystemGraph?.nodes) {
    for (const n of Object.values(ecosystemGraph.nodes)) {
      if (n?.id) keys.add(String(n.id));
      if (n?.dedupKey) keys.add(String(n.dedupKey));
      if (n?.organizationName || n?.activityTitle) {
        keys.add(
          discoveryIdentityKey({
            organizationName: n.organizationName || n.name,
            activityTitle: n.activityTitle || n.name,
            activityDate: n.activityDate,
          })
        );
      }
    }
  }

  for (const d of priorHunterDiscoveries || []) {
    keys.add(discoveryIdentityKey(d));
    if (d.dedupKey) keys.add(String(d.dedupKey));
  }

  for (const k of priorApifyExaKeys || []) {
    if (k) keys.add(String(k));
  }

  const rejected = new Set();
  for (const r of rejectedOrMonitored || []) {
    const k = discoveryIdentityKey(r);
    rejected.add(k);
    keys.add(k);
  }

  return { keys, rejectedKeys: rejected };
}

/**
 * Partition normalized discoveries into incremental vs duplicate/rejected.
 */
export function dedupeNormalizedDiscoveries(discoveries = [], keySets = {}) {
  const { keys, rejectedKeys } = {
    keys: keySets.keys || new Set(),
    rejectedKeys: keySets.rejectedKeys || new Set(),
  };
  const incremental = [];
  const duplicates = [];
  const previouslyRejected = [];
  const seen = new Set();

  for (const row of discoveries || []) {
    const idKey = discoveryIdentityKey(row);
    const oppKey = opportunityDedupKey({
      organizationName: row.organizationName,
      activityTitle: row.activityTitle,
      activityDate: row.activityDate,
    });
    const matchKey =
      [...keys].find((k) => k === idKey || k === oppKey || k === row.id) || null;

    if (rejectedKeys.has(idKey) || row.previouslyRejected === true) {
      previouslyRejected.push({ ...row, dedupeKey: idKey, dedupeStatus: "PREVIOUSLY_REJECTED" });
      continue;
    }
    if (seen.has(idKey) || (matchKey && keys.has(matchKey))) {
      duplicates.push({
        ...row,
        dedupeKey: idKey,
        dedupeStatus: "DUPLICATE",
        matchedExistingKey: matchKey || idKey,
      });
      continue;
    }
    seen.add(idKey);
    keys.add(idKey);
    keys.add(oppKey);
    incremental.push({
      ...row,
      dedupeKey: idKey,
      dedupeStatus: "INCREMENTAL",
    });
  }

  return {
    schema: DISCOVERY_DEDUPE_V1,
    incremental,
    duplicates,
    previouslyRejected,
    summary: {
      input: (discoveries || []).length,
      incremental: incremental.length,
      duplicates: duplicates.length,
      previouslyRejected: previouslyRejected.length,
    },
  };
}
