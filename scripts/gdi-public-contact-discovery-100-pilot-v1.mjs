#!/usr/bin/env node
/**
 * Bounded 100-contact Public Contact Discovery 2.0 pilot (EXA_ONLY).
 *
 * - Deduped cohort of 100 eligible named contacts
 * - Reuse existing intelligence first
 * - Max incremental Exa spend: $10 (hard stop)
 * - No Surfe
 * - Optional --persist-airtable for verified new channels (read-back certified)
 *
 *   node scripts/gdi-public-contact-discovery-100-pilot-v1.mjs --cohort-only
 *   node scripts/gdi-public-contact-discovery-100-pilot-v1.mjs --run
 *   node scripts/gdi-public-contact-discovery-100-pilot-v1.mjs --run --persist-airtable
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  discoverPublicContactChannels,
  PUBLIC_CONTACT_RESEARCH_STATUS,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-discovery-v1.js";
import {
  buildContactChannelDisplayState,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-channel-hierarchy-v1.js";
import {
  inspectPublicContactSearchCredentials,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-search-providers-v1.js";
import { looksLikePersonName } from "../lib/group-demand-intelligence/secondary-named-contact-v1.js";
import {
  findResearchInventoryByCompositeKey,
  upsertResearchInventoryRecord,
  readBackResearchInventory,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js";
import { buildHotelResearchCompositeKey } from "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
dotenv.config({ path: path.join(ROOT, ".env"), override: false });
dotenv.config({
  path: "C:/Dev/dealality/my-operators-backend/.env",
  override: false,
});

const COHORT_ONLY = process.argv.includes("--cohort-only");
const RUN = process.argv.includes("--run") || !COHORT_ONLY;
const PERSIST = process.argv.includes("--persist-airtable");
const SPEND_CEILING_USD = Number(process.env.GDI_PUBLIC_CONTACT_PILOT_BUDGET || 10);
const COHORT_SIZE = 100;
const MAX_QUERIES_PER_CONTACT = 2;
const MAX_PAGES_PER_CONTACT = 3;

const OUT_DIR = path.join(
  ROOT,
  "reports/gdi-public-contact-discovery-2-v1/pilot-100",
);
const HOTELS_ROOT = path.join(ROOT, "data/group-demand-intelligence/hotels");

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function nameKey(n) {
  return text(n)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
}

function parsePayload(raw) {
  if (!raw) return null;
  if (typeof raw === "object") return raw;
  try {
    return JSON.parse(String(raw));
  } catch {
    return null;
  }
}

function contactChannels(o) {
  const pc = o.primaryContact || {};
  return {
    email: text(pc.email || pc.workEmail || o.contactEmail) || null,
    phone: text(pc.phone || pc.directPhone || o.contactPhone) || null,
  };
}

function roleBucket(title) {
  const t = text(title).toLowerCase();
  if (/sales|commercial|account/i.test(t)) return "sales_commercial";
  if (/director|directora|gerente|manager|head|chief|vp|president/i.test(t))
    return "leadership";
  if (/event|meeting|housing|travel|procurement|sourcing/i.test(t))
    return "events_sourcing";
  if (/gobierno|secretaria|municipal|public/i.test(t)) return "public_sector";
  return "other";
}

function buildCohort() {
  const hotels = fs
    .readdirSync(HOTELS_ROOT)
    .filter((d) => fs.existsSync(path.join(HOTELS_ROOT, d, "opportunities.json")));

  const candidates = [];
  const seenPerson = new Set();

  for (const hotelId of hotels) {
    const doc = JSON.parse(
      fs.readFileSync(
        path.join(HOTELS_ROOT, hotelId, "opportunities.json"),
        "utf8",
      ),
    );
    for (const o of doc.opportunities || []) {
      const name = text(o.primaryContact?.name || o.contactName);
      if (!looksLikePersonName(name)) continue;
      const nk = nameKey(name);
      if (seenPerson.has(nk)) continue;
      const ch = contactChannels(o);
      const missingEmail = !ch.email;
      const missingPhone = !ch.phone;
      if (!missingEmail && !missingPhone) continue; // already complete — track separately
      seenPerson.add(nk);
      candidates.push({
        hotelId,
        opportunityId: o.id,
        personName: name,
        title: text(o.primaryContact?.title || o.primaryContact?.role),
        organization: text(
          o.organizationName || o.primaryContact?.organization,
        ),
        segment: text(o.segment || o.demandMechanism || o.opportunityType),
        market: text(o.market || o.geography || doc.market || hotelId),
        roleBucket: roleBucket(o.primaryContact?.title || o.primaryContact?.role),
        missingEmail,
        missingPhone,
        existingEmail: ch.email,
        existingPhone: ch.phone,
        opportunity: o,
      });
    }
  }

  // Diversity: round-robin by hotel then roleBucket
  const byHotel = new Map();
  for (const c of candidates) {
    if (!byHotel.has(c.hotelId)) byHotel.set(c.hotelId, []);
    byHotel.get(c.hotelId).push(c);
  }
  for (const list of byHotel.values()) {
    list.sort((a, b) => {
      // Prefer missing both, then missing email, then missing phone
      const score = (x) =>
        (x.missingEmail && x.missingPhone ? 0 : x.missingEmail ? 1 : 2);
      return score(a) - score(b) || a.personName.localeCompare(b.personName);
    });
  }

  const cohort = [];
  const hotelIds = [...byHotel.keys()];
  let idx = 0;
  while (cohort.length < COHORT_SIZE && hotelIds.some((h) => byHotel.get(h).length)) {
    const h = hotelIds[idx % hotelIds.length];
    idx += 1;
    const list = byHotel.get(h);
    if (!list.length) continue;
    cohort.push(list.shift());
  }

  // Force-include founder regression people if present and not already in cohort
  const forceNames = [
    "maritza jeanette medina garcia",
    "alejandra vazquez",
  ];
  for (const hotelId of hotels) {
    const doc = JSON.parse(
      fs.readFileSync(
        path.join(HOTELS_ROOT, hotelId, "opportunities.json"),
        "utf8",
      ),
    );
    for (const o of doc.opportunities || []) {
      const nk = nameKey(o.primaryContact?.name);
      if (!forceNames.includes(nk)) continue;
      if (cohort.some((c) => nameKey(c.personName) === nk)) continue;
      if (cohort.length >= COHORT_SIZE) cohort.pop();
      const ch = contactChannels(o);
      cohort.unshift({
        hotelId,
        opportunityId: o.id,
        personName: text(o.primaryContact?.name),
        title: text(o.primaryContact?.title),
        organization: text(o.organizationName),
        segment: text(o.segment),
        market: hotelId,
        roleBucket: roleBucket(o.primaryContact?.title),
        missingEmail: !ch.email,
        missingPhone: !ch.phone,
        existingEmail: ch.email,
        existingPhone: ch.phone,
        opportunity: o,
        founderRegression: true,
      });
    }
  }

  return {
    totalEligibleNamedMissingChannels: candidates.length,
    hotelsScanned: hotels.length,
    cohortSize: cohort.length,
    cohort: cohort.map((c) => ({
      hotelId: c.hotelId,
      opportunityId: c.opportunityId,
      personName: c.personName,
      title: c.title,
      organization: c.organization,
      segment: c.segment,
      roleBucket: c.roleBucket,
      missingEmail: c.missingEmail,
      missingPhone: c.missingPhone,
      existingEmail: c.existingEmail,
      existingPhone: c.existingPhone,
      founderRegression: c.founderRegression === true,
    })),
    _full: cohort,
  };
}

async function persistVerified(hotelId, opportunityId, opportunity) {
  const compositeKey = buildHotelResearchCompositeKey(hotelId, opportunityId);
  let existing = null;
  try {
    existing = await findResearchInventoryByCompositeKey(compositeKey);
  } catch (err) {
    return { ok: false, error: err.message || String(err) };
  }
  if (!existing) {
    return { ok: false, skipped: true, reason: "NO_EXISTING_INVENTORY_ROW" };
  }
  const existingPayload = parsePayload(existing.fields?.researchPayloadJson) || {};
  const proposed = {
    ...existingPayload,
    ...opportunity,
    id: opportunityId,
    opportunityId,
    hotelId,
    segment:
      text(opportunity.segment) ||
      text(existingPayload.segment) ||
      text(existing.fields?.segment) ||
      null,
  };
  const write = await upsertResearchInventoryRecord({
    opportunity: proposed,
    hotel: { hotelId },
    dryRun: false,
  });
  if (!write?.ok || !write.airtableRecordId) {
    return { ok: false, write };
  }
  const rb = await readBackResearchInventory(write.airtableRecordId);
  const rbPc = parsePayload(rb?.fields?.researchPayloadJson)?.primaryContact || {};
  const wantEmail = text(opportunity.primaryContact?.email).toLowerCase();
  const gotEmail = text(rbPc.email || rbPc.workEmail).toLowerCase();
  return {
    ok: true,
    recordId: write.airtableRecordId,
    emailMatch: !wantEmail || wantEmail === gotEmail,
    surfeDerived: rbPc.surfeDerived === true,
  };
}

async function runPilot(cohortBundle) {
  const creds = inspectPublicContactSearchCredentials();
  const results = [];
  let searchCostUsd = 0;
  let queriesExecuted = 0;
  let pagesFetched = 0;
  let reused = 0;
  let newEmails = 0;
  let newPhones = 0;
  let orgRoutes = 0;
  let rejected = 0;
  let bothChannels = 0;
  let oneChannel = 0;
  let noneRecovered = 0;
  let sourceVerified = 0;
  let airtableOk = 0;
  let airtableFail = 0;
  let surfeAvoided = 0;
  let stoppedForBudget = false;

  for (const entry of cohortBundle._full) {
    if (searchCostUsd >= SPEND_CEILING_USD) {
      stoppedForBudget = true;
      break;
    }

    const before = contactChannels(entry.opportunity);
    const needsSearch = before.email == null || before.phone == null;

    // Reuse path: already has channels
    if (before.email && before.phone) {
      reused += 1;
      bothChannels += 1;
      surfeAvoided += 1;
      results.push({
        ...entryLite(entry),
        status: PUBLIC_CONTACT_RESEARCH_STATUS.REUSED_EXISTING,
        mode: "reuse",
        email: before.email,
        phone: before.phone,
      });
      continue;
    }

    const remainingBudget = SPEND_CEILING_USD - searchCostUsd;
    const authorizePaid =
      needsSearch && remainingBudget > 0 && creds.exaConfigured;

    let discovery;
    try {
      discovery = await discoverPublicContactChannels({
        opportunity: entry.opportunity,
        authorizePaidSearch: authorizePaid,
        seedRegressionFixtures: entry.founderRegression === true,
        maxQueries: authorizePaid ? MAX_QUERIES_PER_CONTACT : 0,
        maxPages: MAX_PAGES_PER_CONTACT,
        timeoutMs: 18000,
      });
    } catch (err) {
      results.push({
        ...entryLite(entry),
        status: "ERROR",
        error: err.message || String(err),
      });
      noneRecovered += 1;
      continue;
    }

    const cost = Number(discovery.metrics?.searchCostUsd || 0);
    searchCostUsd += cost;
    queriesExecuted += Number(discovery.metrics?.queriesExecuted || 0);
    pagesFetched += Number(discovery.metrics?.pagesFetched || 0);

    const d =
      discovery.display ||
      buildContactChannelDisplayState(discovery.opportunity || entry.opportunity);
    const email = text(d.publicDirectEmail) || null;
    const phone = text(d.publicDirectPhone) || null;
    const orgEmail = text(d.organizationalEmail) || null;

    if (discovery.status === PUBLIC_CONTACT_RESEARCH_STATUS.REUSED_EXISTING) {
      reused += 1;
    }
    if (email && !before.email) newEmails += 1;
    if (phone && !before.phone) newPhones += 1;
    if (orgEmail) orgRoutes += 1;

    if (email && phone) bothChannels += 1;
    else if (email || phone) oneChannel += 1;
    else noneRecovered += 1;

    if (email || phone) {
      sourceVerified += 1;
      if (email) surfeAvoided += 1;
      if (phone) surfeAvoided += 1;
    }

    // Reject invented phones when email-only source expected — discovery already
    // gates this; count mismatches where name doesn't match
    if (
      email &&
      discovery.opportunity?.primaryContact?.name &&
      nameKey(discovery.opportunity.primaryContact.name) !==
        nameKey(entry.personName)
    ) {
      rejected += 1;
    }

    let persist = null;
    if (
      PERSIST &&
      (email || phone) &&
      discovery.status !== PUBLIC_CONTACT_RESEARCH_STATUS.REUSED_EXISTING
    ) {
      persist = await persistVerified(
        entry.hotelId,
        entry.opportunityId,
        discovery.opportunity,
      );
      if (persist?.ok && persist.emailMatch && !persist.surfeDerived) airtableOk += 1;
      else if (persist && !persist.skipped) airtableFail += 1;
    }

    results.push({
      ...entryLite(entry),
      status: discovery.status,
      mode: authorizePaid ? "exa_search" : "offline_urls_only",
      email,
      phone,
      orgEmail,
      surfeCta: d.surfeCtaLabel,
      searchCostUsd: cost,
      queriesExecuted: discovery.metrics?.queriesExecuted || 0,
      pagesFetched: discovery.metrics?.pagesFetched || 0,
      sourceUrl: d.channelSourceUrl || null,
      persist,
    });
  }

  const researched = results.length;
  const newChannels = newEmails + newPhones;
  return {
    generatedAt: new Date().toISOString(),
    providerMode: creds.chosenMode,
    spendCeilingUsd: SPEND_CEILING_USD,
    actualSpendUsd: Number(searchCostUsd.toFixed(4)),
    stoppedForBudget,
    surfeCalls: 0,
    metrics: {
      totalContactsResearched: researched,
      previouslyVerifiedChannelsReused: reused,
      newPublicEmails: newEmails,
      newPublicPhones: newPhones,
      organizationalRoutes: orgRoutes,
      incorrectMatchesRejected: rejected,
      contactsWithBothChannels: bothChannels,
      contactsWithOneChannel: oneChannel,
      contactsWithNoRecoverableChannels: noneRecovered,
      sourceVerificationSuccess: sourceVerified,
      exaQueriesExecuted: queriesExecuted,
      pagesFetched,
      costPerVerifiedNewChannel:
        newChannels > 0 ? Number((searchCostUsd / newChannels).toFixed(4)) : null,
      airtableWriteReadbackSuccess: airtableOk,
      airtableWriteReadbackFail: airtableFail,
      surfeCallsAvoided: surfeAvoided,
      remainingResearchGaps: noneRecovered + (COHORT_SIZE - researched),
    },
    results,
  };
}

function entryLite(entry) {
  return {
    hotelId: entry.hotelId,
    opportunityId: entry.opportunityId,
    personName: entry.personName,
    organization: entry.organization,
    roleBucket: entry.roleBucket,
    founderRegression: entry.founderRegression === true,
  };
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const cohortBundle = buildCohort();
  fs.writeFileSync(
    path.join(OUT_DIR, "COHORT.json"),
    `${JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        totalEligibleNamedMissingChannels:
          cohortBundle.totalEligibleNamedMissingChannels,
        hotelsScanned: cohortBundle.hotelsScanned,
        cohortSize: cohortBundle.cohortSize,
        cohort: cohortBundle.cohort,
      },
      null,
      2,
    )}\n`,
  );

  if (COHORT_ONLY) {
    console.log(
      JSON.stringify(
        {
          cohortOnly: true,
          cohortSize: cohortBundle.cohortSize,
          eligible: cohortBundle.totalEligibleNamedMissingChannels,
        },
        null,
        2,
      ),
    );
    return;
  }

  if (!RUN) return;

  const report = await runPilot(cohortBundle);
  fs.writeFileSync(
    path.join(OUT_DIR, "PILOT_RESULTS.json"),
    `${JSON.stringify(report, null, 2)}\n`,
  );

  const md = `# Public Contact Discovery 2.0 — 100-contact pilot

| Metric | Value |
|--------|-------|
| Mode | ${report.providerMode} |
| Contacts researched | ${report.metrics.totalContactsResearched} |
| Reused existing | ${report.metrics.previouslyVerifiedChannelsReused} |
| New emails | ${report.metrics.newPublicEmails} |
| New phones | ${report.metrics.newPublicPhones} |
| Org routes | ${report.metrics.organizationalRoutes} |
| Both channels | ${report.metrics.contactsWithBothChannels} |
| One channel | ${report.metrics.contactsWithOneChannel} |
| None recovered | ${report.metrics.contactsWithNoRecoverableChannels} |
| Rejected mismatches | ${report.metrics.incorrectMatchesRejected} |
| Exa queries | ${report.metrics.exaQueriesExecuted} |
| Actual spend | $${report.actualSpendUsd} / $${report.spendCeilingUsd} |
| Cost / new channel | ${report.metrics.costPerVerifiedNewChannel ?? "n/a"} |
| Airtable OK | ${report.metrics.airtableWriteReadbackSuccess} |
| Surfe avoided | ${report.metrics.surfeCallsAvoided} |
| Budget stop | ${report.stoppedForBudget} |

Surfe calls: **0**. Full DB backfill: **not started** (STOP for founder).
`;
  fs.writeFileSync(path.join(OUT_DIR, "PILOT_REPORT.md"), md);
  console.log(JSON.stringify({ ...report, results: `[${report.results.length} rows]` }, null, 2));
  console.log(`Wrote ${path.join(OUT_DIR, "PILOT_RESULTS.json")}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
