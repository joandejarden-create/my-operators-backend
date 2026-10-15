#!/usr/bin/env node
/**
 * Post-deploy verify for use-case qualification integrity release.
 * Fresh Airtable pull + contract visibility for the nine authorized IDs.
 * Optional: --base-url http://localhost:8080 or production URL with --share-token.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import {
  evaluateProspectPublicationEligibility,
} from "../lib/group-demand-intelligence/prospect-publication-eligibility-v1.js";
import { isReviewedProspectCustomerVisible } from "../lib/group-demand-intelligence/reviewed-prospect-publication-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-use-case-qualification-integrity/release/postdeploy-verify.json"
);

const NINE = [
  { id: "gdi_pe_295cfef5822e63c3", hotelId: "recnPB3Q02NO8Ma1o", org: "Sparks Gallery" },
  { id: "gdi_pe_879047c3211d2e82", hotelId: "recnPB3Q02NO8Ma1o", org: "City Cruises San Diego" },
  { id: "gdi_pe_5ce20a54b903e243", hotelId: "recnPB3Q02NO8Ma1o", org: "University Club San Diego" },
  { id: "gdi_pe_ca47fef6aa622566", hotelId: "recnPB3Q02NO8Ma1o", org: "deck655" },
  { id: "gdi_pe_5f72b9d7665314b5", hotelId: "recnPB3Q02NO8Ma1o", org: "Flagship Cruises & Events" },
  { id: "gdi_pe_4b9b49bae4136428", hotelId: "recnPB3Q02NO8Ma1o", org: "Nolita House" },
  { id: "gdi_pe_a7a2fda2a474aa0b", hotelId: "recnPB3Q02NO8Ma1o", org: "The House Next Door" },
  { id: "gdi_sc_0fe98055e8df9f75", hotelId: "rec0eTEnCqlsGJJt9", org: "Wisedocs" },
  { id: "gdi_sc_188e25b6b3b56637", hotelId: "recnPB3Q02NO8Ma1o", org: "The Shipyard" },
];

const args = process.argv.slice(2);
function argVal(name) {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : null;
}
const baseUrl = (argVal("--base-url") || "").replace(/\/$/, "");
const shareToken = argVal("--share-token");

async function fetchList(hotelId) {
  if (!baseUrl) return null;
  const headers = { Accept: "application/json" };
  let url = `${baseUrl}/api/group-demand-intelligence/hotels/${hotelId}/opportunities`;
  if (shareToken) {
    url = `${baseUrl}/api/group-demand-intelligence/share/hotels/${hotelId}/opportunities`;
    headers["x-gdi-share-capability"] = shareToken;
  }
  const res = await fetch(url, { headers });
  const text = await res.text();
  let body = null;
  try {
    body = JSON.parse(text);
  } catch {
    body = { raw: text.slice(0, 400) };
  }
  return { status: res.status, body };
}

function pickListIds(body) {
  const rows =
    body?.opportunities ||
    body?.items ||
    body?.data?.opportunities ||
    (Array.isArray(body) ? body : []);
  return new Set(
    (rows || []).map((r) => r.id || r.opportunityId).filter(Boolean)
  );
}

const byHotel = new Map();
for (const row of NINE) {
  if (!byHotel.has(row.hotelId)) byHotel.set(row.hotelId, []);
  byHotel.get(row.hotelId).push(row);
}

const hotelSnapshots = {};
const rows = [];

for (const [hotelId, targets] of byHotel) {
  const loaded = await loadOpportunitiesCanonical(hotelId, { forceRefresh: true });
  const opps = loaded.opportunities || loaded || [];
  const list = Array.isArray(opps) ? opps : [];
  const facing = filterCustomerFacingOpportunities(list, { hotelId });
  hotelSnapshots[hotelId] = {
    persistence: loaded.persistence || null,
    total: list.length,
    facingFilterCount: facing.length,
    reviewedVisibleCount: list.filter((o) =>
      isReviewedProspectCustomerVisible(o, { hotelId })
    ).length,
    facingIds: facing.map((o) => o.id),
  };

  let http = null;
  if (baseUrl) {
    http = await fetchList(hotelId);
  }

  for (const t of targets) {
    const opp = list.find((o) => o.id === t.id) || null;
    const elig = opp ? evaluateProspectPublicationEligibility(opp) : null;
    const reviewed = opp
      ? isReviewedProspectCustomerVisible(opp, { hotelId })
      : false;
    const filterOk = facing.some((o) => o.id === t.id);
    const httpIds = http?.body ? pickListIds(http.body) : null;
    rows.push({
      ...t,
      found: Boolean(opp),
      basis: opp?.prospectPublicationBasis || null,
      facing: opp?.prospectHotelFacing === true,
      customerVisible: opp?.customerVisible === true,
      elig: elig?.eligibility || null,
      hotelFacingElig: elig?.hotelFacing === true,
      suitabilityFailed: elig?.suitability?.failed || elig?.failed || [],
      useCase: elig?.useCase || null,
      reviewed,
      filterOk,
      publicationEventCount: Array.isArray(opp?.publicationEvents)
        ? opp.publicationEvents.length
        : 0,
      contactLabel:
        opp?.primaryContact?.name ||
        opp?.contactPackage?.person ||
        opp?.organizationName ||
        null,
      feedbackPreserved: opp?.customerFeedback != null || opp?.feedback != null,
      httpStatus: http?.status ?? null,
      httpListContains: httpIds ? httpIds.has(t.id) : null,
      disposition:
        filterOk && reviewed && opp?.prospectPublicationBasis === "REVIEWED_PACKAGE_V1"
          ? t.id.startsWith("gdi_sc_")
            ? "BASIS_REPAIRED_VISIBLE"
            : "RESTORED_VISIBLE"
          : "NOT_VISIBLE_OR_INCOMPLETE",
    });
  }
}

const report = {
  generatedAt: new Date().toISOString(),
  releaseShaExpected: "b571d945ed5ae8df58c65d53a3fcffd91a48950b",
  baseUrl: baseUrl || null,
  hotelSnapshots,
  rows,
  summary: {
    allNineFound: rows.every((r) => r.found),
    allFilterVisible: rows.every((r) => r.filterOk),
    allReviewedVisible: rows.every((r) => r.reviewed),
    allHaveBasis: rows.every((r) => r.basis === "REVIEWED_PACKAGE_V1"),
    visibleIds: rows.filter((r) => r.filterOk).map((r) => r.id),
    exceptions: rows.filter((r) => !r.filterOk || !r.reviewed || r.basis !== "REVIEWED_PACKAGE_V1"),
  },
};

fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(report, null, 2));
console.log(JSON.stringify(report.summary, null, 2));
console.log(`wrote ${OUT}`);
if (!report.summary.allNineFound || !report.summary.allFilterVisible) {
  process.exitCode = 1;
}
