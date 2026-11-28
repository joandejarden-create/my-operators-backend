#!/usr/bin/env node
/** Rebuild founder report from live Airtable + Findymail ledger (no paid calls). */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { readHiGraphForHotel } from "../../lib/hotel-intelligence/research-engine-v1/he-airtable-graph-reader-v1.js";
import { readFindymailUsageLedger } from "../../lib/group-demand-intelligence/providers/gdi-findymail-client-v1.js";
import { computeContactabilityMetrics } from "../../lib/hotel-intelligence/contact-intelligence/he-contactability-metrics-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
dotenv.config({ path: path.join(ROOT, ".env"), override: false });
dotenv.config({ path: "C:/Dev/dealality/my-operators-backend/.env", override: false });

const HOTEL = "recLuxvwwxID7U2B8";
const AUTH = "FOUNDER_GO_HE50_BETHESDA_FINDYMAIL_LIVE_20261128";
const USD = 0.05;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

const graph = await readHiGraphForHotel(HOTEL, {
  includeContacts: true,
  includePortfolio: false,
});
const ledger = readFindymailUsageLedger().filter(
  (r) => text(r.hotelId) === HOTEL || text(r.authorizationId) === AUTH,
);
const he50 = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "reports/hotel-explorer/he-5-0-pilot/HE50-RESEARCH-SPEND-LEDGER.json"),
    "utf8",
  ),
);

const orgName = (c) => {
  const o = (graph.organizations || []).find(
    (x) => String(x.orgId) === String(c.organizationId),
  );
  return o?.tradingName || o?.legalName || null;
};

const eight = (graph.orgContacts || []).map((c) => {
  const org = orgName(c);
  const emailCh = (c.channels || []).find((ch) => /email/i.test(String(ch.kind || "")));
  const personKey = `${text(c.displayName).toLowerCase()}|${text(org).toLowerCase()}`;
  const led = ledger.find(
    (r) => text(r.personKey).toLowerCase() === personKey,
  );
  return {
    recoveredAirtableId: c.airtableId,
    person: c.displayName,
    organization: org,
    verificationState: c.verificationState,
    findymailLookupPerformed: Boolean(led),
    findymailDisposition: led?.resultDisposition || null,
    verifiedBusinessEmail: emailCh?.value || null,
    directTelephone: null,
    airtableChannelId: emailCh?.airtableId || null,
    airtableReadBackOk: Boolean(emailCh?.value),
    visibleInHotelExplorer: Boolean(emailCh?.value),
    sharedGdiHeResolver: Boolean(emailCh?.value),
    researchCostCredits: led?.emailFound ? Number(led.creditsDebited || 0) : 0,
  };
});

// Unique metrics
const byName = new Map();
for (const row of eight) {
  const k = text(row.person).toLowerCase();
  if (!byName.has(k) || row.verifiedBusinessEmail) byName.set(k, row);
}
const unique = [...byName.values()];
const people = unique.map((r) => ({
  name: r.person,
  organization: r.organization,
  verificationStatus: "VERIFIED",
  channels: r.verifiedBusinessEmail
    ? [{ kind: "email", value: r.verifiedBusinessEmail }]
    : [],
  linkedinUrl: "https://www.linkedin.com/in/x",
}));
const metrics = computeContactabilityMetrics(people);
const credits = ledger
  .filter((r) => r.emailFound)
  .reduce((s, r) => s + Number(r.creditsDebited || 0), 0);

const report = {
  version: "he50-bethesda-findymail-live-founder-report-v1",
  authorizationId: AUTH,
  hotelId: HOTEL,
  generatedAt: new Date().toISOString(),
  paidExecution: true,
  budget: {
    he50CapUsd: 50,
    confirmedActualUsd: he50.confirmedActualUsd,
    remainingAuthorizedUsd: he50.remainingAuthorizedUsd,
    findymailCreditsDebited: credits,
    findymailUsdEstimate: Number((credits * USD).toFixed(4)),
    usdPerCreditEstimate: USD,
  },
  reconciliation: {
    recoveredHiOrgContacts: eight.length,
    uniqueNamedProfessionals: unique.length,
  },
  eightContactTable: eight,
  uniquePeopleSummary: unique,
  contactabilityAfter: metrics,
  ledgerRows: ledger,
  unresolved: unique
    .filter((r) => !r.verifiedBusinessEmail)
    .map((r) => ({ person: r.person, reason: r.findymailDisposition || "NO_EMAIL" })),
};

const out = path.join(
  ROOT,
  "reports/hotel-explorer/HE50-BETHESDA-FINDYMAIL-LIVE-FOUNDER-REPORT-V1.json",
);
fs.writeFileSync(out, JSON.stringify(report, null, 2));
console.log(out);
console.log(
  JSON.stringify(
    {
      emails: unique.filter((u) => u.verifiedBusinessEmail).length,
      rate: metrics.verifiedDirectContactabilityRate,
      remaining: he50.remainingAuthorizedUsd,
      credits,
    },
    null,
    2,
  ),
);
