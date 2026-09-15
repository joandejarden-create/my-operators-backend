#!/usr/bin/env node
/**
 * Freeze GDI contact reachability cohort from official person-discovery enrichment queue.
 * No Surfe calls. Hotel metadata is DATA only (not core logic).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildReachabilityCohortFromQueue,
  inferDomainFromOfficialSourceUrl,
} from "../lib/group-demand-intelligence/contact-coverage.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, "..");

const discoveryPath = path.join(
  root,
  "data/group-demand-intelligence/bethesda-official-person-discovery.json"
);
const outPath = path.join(
  root,
  "data/group-demand-intelligence/evals/bethesda-contact-reachability-cohort-v1.json"
);

const discovery = JSON.parse(fs.readFileSync(discoveryPath, "utf8"));
const queue = discovery.enrichmentQueue || [];

/** Attach official sourceUrl / domain from discovery rows when queue omits them (reusable join). */
function enrichQueueFromDiscovery(queueRows, discoveryDoc) {
  const byOpp = new Map();
  for (const row of discoveryDoc.opportunities || []) {
    byOpp.set(row.opportunityId || row.id, row);
  }
  return (queueRows || []).map((q) => {
    const row = byOpp.get(q.opportunityId);
    const primary = row?.primaryCandidate || null;
    const backups = row?.backupCandidates || [];
    const namedMatch =
      [primary, ...backups].find(
        (c) =>
          c?.name &&
          String(c.name).toLowerCase().trim() === String(q.name || "").toLowerCase().trim()
      ) || null;
    const sourceUrl =
      q.sourceUrl ||
      namedMatch?.sourceUrl ||
      primary?.sourceUrl ||
      namedMatch?.sources?.[0]?.url ||
      primary?.sources?.[0]?.url ||
      null;
    const domain =
      q.domain ||
      (q.email && String(q.email).includes("@")
        ? String(q.email).split("@")[1].toLowerCase()
        : null) ||
      inferDomainFromOfficialSourceUrl(sourceUrl);
    return { ...q, sourceUrl, domain };
  });
}

const enrichedQueue = enrichQueueFromDiscovery(queue, discovery);

const cohort = buildReachabilityCohortFromQueue(enrichedQueue, {
  version: "gdi_contact_reachability_cohort_v1",
  hotelId: discovery.hotelId || null,
  hotelName: "Bethesda Marriott",
  purpose:
    "Bounded reachability enrichment — frozen named WHO from official-source discovery. Surfe HOW only.",
});

fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, JSON.stringify(cohort, null, 2));

console.log(
  JSON.stringify(
    {
      outPath,
      subjects: cohort.subjects.length,
      emailRequests: cohort.subjects.filter((s) => s.requestEmail).length,
      mobileRequests: cohort.subjects.filter((s) => s.requestMobile).length,
      domains: cohort.subjects.map((s) => ({
        name: s.full_name,
        domain: s.enrichmentDomain,
        need: s.reachabilityNeed,
      })),
      ids: cohort.subjects.map((s) => s.full_name),
    },
    null,
    2
  )
);
