/**
 * PE V1.6 — enrich Woman's Club detail and emit founder validation snapshot.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { enrichPrivateEventOpportunityDetail } from "../lib/group-demand-intelligence/private-events/customer-detail-enrichment.js";
import { sanitizeOpportunityForShare } from "../lib/group-demand-intelligence/share/gdi-signed-share-capability-v1.js";

const HOTEL = "recLuxvwwxID7U2B8";
const OPP = "gdi_pe_781f12393f8117e7";
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "../reports/group-demand-intelligence/private-events-v1-6"
);

const doc = await loadOpportunitiesCanonical(HOTEL);
const raw = (doc.opportunities || []).find((o) => o.id === OPP);
if (!raw) {
  console.error("OPPORTUNITY_NOT_FOUND");
  process.exit(1);
}

const before = {
  sources: (raw.sources || []).length,
  sourceNames: (raw.sources || []).map((s) => s.name || s.title || null),
  contact: raw.primaryContact || null,
  commercialContactPath: raw.commercialContactPath || null,
  evidenceConfidence: raw.evidenceConfidence,
  knownVerified: raw.knownVsEstimated?.verified?.length ?? 0,
  address: raw.venueAddress || null,
  distance: raw.distanceMiles ?? null,
};

const { opportunity: enriched, peEnriched, hasOfficialSource, sourceCount } =
  await enrichPrivateEventOpportunityDetail(raw);
const share = sanitizeOpportunityForShare(enriched);

const after = {
  peEnriched,
  hasOfficialSource,
  sourceCount,
  sources: (enriched.sources || []).map((s) => ({
    name: s.name,
    url: s.url,
    supports: s.supportsFact,
  })),
  venueName: enriched.venueName,
  venueAddress: [enriched.venueAddress, enriched.venueCity, enriched.venueRegion]
    .filter(Boolean)
    .join(", "),
  distanceMiles: enriched.distanceMiles,
  driveTimeMinutes: enriched.driveTimeMinutes,
  lodging: enriched.onSiteLodgingStatusLabel,
  activity: enriched.eventActivityEvidenceStatusLabel,
  partner: enriched.partnerStatusLabel,
  commercialPath: enriched.commercialContactPathLabel,
  contactName: enriched.primaryContact?.name,
  contactOfficialUrl: enriched.contactOfficialUrl,
  evidenceConfidence: enriched.evidenceConfidence,
  evidenceExplanation: enriched.evidenceConfidenceExplanation,
  verified: enriched.knownVsEstimated?.verified?.length ?? 0,
  estimated: enriched.knownVsEstimated?.estimated?.length ?? 0,
  inferred: enriched.knownVsEstimated?.inferred?.length ?? 0,
  officialSources: (enriched.officialSourceUrls || []).length,
  summaryWhat: enriched.summaryWhat,
  thesis: enriched.summaryWhyMatters,
  action: enriched.recommendedAction,
  eventDateDisplay: enriched.eventDateDisplay,
  detailLayout: enriched.detailLayout,
  shareSourceCount: (share.sources || []).filter((s) => s.url).length,
  shareHasContact: Boolean(share.primaryContact?.name),
  shareHasVenue: Boolean(share.venueName && share.venueAddress),
  shareHasFit: Boolean(share.lodgingCatchmentFit && share.productFit),
};

const missing = [];
const required = [
  "venueName",
  "venueAddress",
  "distanceMiles",
  "onSiteLodgingStatus",
  "eventActivityEvidenceStatus",
  "partnerStatus",
  "commercialContactPath",
  "lodgingCatchmentFit",
  "productFit",
  "partnershipPotential",
  "sources",
];
for (const k of required) {
  const v = enriched[k];
  if (v == null || v === "" || (Array.isArray(v) && v.length === 0)) missing.push(k);
}

fs.mkdirSync(OUT, { recursive: true });
const report = { before, after, missing, generatedAt: new Date().toISOString() };
fs.writeFileSync(path.join(OUT, "WOMANS_CLUB_DETAIL_SNAPSHOT.json"), JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));

if (!hasOfficialSource || sourceCount < 1) {
  console.error("STOP: TRUE opportunity missing clickable sources");
  process.exit(2);
}
