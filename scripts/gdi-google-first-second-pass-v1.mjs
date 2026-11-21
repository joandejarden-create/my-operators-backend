#!/usr/bin/env node
/**
 * Second pass over the original Public Contact Discovery 2.0 100-contact cohort.
 *
 * Order: saved → Google Gemini grounding → Exa → verify → Airtable persist
 * No Surfe. Hard spend ceiling $10 (Google estimate + Exa actual).
 *
 *   node scripts/gdi-google-first-second-pass-v1.mjs --plan
 *   node scripts/gdi-google-first-second-pass-v1.mjs --run
 *   node scripts/gdi-google-first-second-pass-v1.mjs --run --persist-airtable
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
import {
  inspectGoogleGroundedContactCredentials,
} from "../lib/group-demand-intelligence/presentation/gdi-google-grounded-contact-discovery-v1.js";
import {
  collectExistingContactCandidates,
  defaultEvidenceForKnownFixtures,
  deriveContactLevelChanges,
} from "../lib/group-demand-intelligence/presentation/gdi-commercial-contact-intelligence-v1.js";
import {
  selectBestCommercialContacts,
  compareBaselineToEnhanced,
  projectSelectionOntoOpportunity,
} from "../lib/group-demand-intelligence/presentation/gdi-commercial-contact-selection-v1.js";
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

const PLAN_ONLY = process.argv.includes("--plan");
const RUN = process.argv.includes("--run");
const PERSIST = process.argv.includes("--persist-airtable");
const RESUME = process.argv.includes("--resume");
const LIMIT = Number(
  process.argv.find((a) => a.startsWith("--limit="))?.split("=")[1] || 0,
);
const SPEND_CEILING_USD = Number(
  process.env.GDI_GOOGLE_FIRST_SECOND_PASS_BUDGET || 10,
);
/** Conservative charge toward the cap when Gemini pricing is COST_UNKNOWN. */
const GOOGLE_EST_USD_PER_CALL = Number(
  process.env.GDI_GOOGLE_CONTACT_EST_USD_PER_CALL || 0.03,
);
const MAX_QUERIES = Number(process.env.GDI_SECOND_PASS_MAX_QUERIES || 3);
const MAX_PAGES = Number(process.env.GDI_SECOND_PASS_MAX_PAGES || 3);

const BASELINE_DIR = path.join(
  ROOT,
  "reports/gdi-public-contact-discovery-2-v1/pilot-100",
);
const OUT_DIR = path.join(ROOT, "reports/gdi-google-first-second-pass-v1");
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

function isGenericMailbox(email) {
  const local = text(email).split("@")[0].toLowerCase();
  return /^(info|contact|sales|hello|admin|office|reception|support|enquiries|inquiries|mail|general|prensa|comunicacion)$/i.test(
    local,
  );
}

function channelSnapshot(row) {
  return {
    email: text(row.email) || null,
    phone: text(row.phone) || null,
    namedEmail:
      text(row.email) && !isGenericMailbox(row.email) ? text(row.email) : null,
  };
}

function loadOpportunity(hotelId, opportunityId) {
  const p = path.join(HOTELS_ROOT, hotelId, "opportunities.json");
  if (!fs.existsSync(p)) return null;
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  return (doc.opportunities || []).find((o) => o.id === opportunityId) || null;
}

function classifyPriority(baselineRow, liveOpp) {
  const base = channelSnapshot(baselineRow);
  const livePc = liveOpp?.primaryContact || {};
  const liveEmail =
    text(livePc.email || livePc.workEmail || liveOpp?.contactEmail) ||
    base.email;
  const livePhone =
    text(livePc.phone || livePc.directPhone || liveOpp?.contactPhone) ||
    base.phone;
  const hasEmail = Boolean(liveEmail);
  const hasPhone = Boolean(livePhone);
  if (hasEmail && hasPhone) return { tier: "COMPLETE", liveEmail, livePhone };
  if (!hasEmail && !hasPhone) return { tier: "NONE", liveEmail, livePhone };
  return { tier: "ONE", liveEmail, livePhone };
}

function buildWorkQueue() {
  const baseline = JSON.parse(
    fs.readFileSync(path.join(BASELINE_DIR, "PILOT_RESULTS.json"), "utf8"),
  );
  const cohort = JSON.parse(
    fs.readFileSync(path.join(BASELINE_DIR, "COHORT.json"), "utf8"),
  );
  const byKey = new Map(
    (baseline.results || []).map((r) => [
      `${r.hotelId}::${r.opportunityId}`,
      r,
    ]),
  );

  const queue = [];
  for (const c of cohort.cohort || []) {
    const key = `${c.hotelId}::${c.opportunityId}`;
    const baselineRow = byKey.get(key) || {
      hotelId: c.hotelId,
      opportunityId: c.opportunityId,
      personName: c.personName,
      email: c.existingEmail,
      phone: c.existingPhone,
    };
    const opportunity = loadOpportunity(c.hotelId, c.opportunityId);
    if (!opportunity) {
      queue.push({
        ...c,
        baselineRow,
        opportunity: null,
        skip: true,
        skipReason: "OPPORTUNITY_NOT_FOUND_LOCALLY",
        priority: 99,
      });
      continue;
    }
    // Ensure person name on opportunity
    if (!text(opportunity.primaryContact?.name) && c.personName) {
      opportunity.primaryContact = {
        ...(opportunity.primaryContact || {}),
        name: c.personName,
        title: c.title || opportunity.primaryContact?.title,
      };
    }
    const pri = classifyPriority(baselineRow, opportunity);
    let priority = 99;
    if (pri.tier === "NONE") priority = 1;
    else if (pri.tier === "ONE") priority = 2;
    else priority = 3; // COMPLETE — skip research unless founderRegression freshness

    const skipComplete =
      pri.tier === "COMPLETE" && c.founderRegression !== true;

    queue.push({
      hotelId: c.hotelId,
      opportunityId: c.opportunityId,
      personName: c.personName || opportunity.primaryContact?.name,
      title: c.title,
      organization: c.organization,
      roleBucket: c.roleBucket,
      founderRegression: c.founderRegression === true,
      baselineRow,
      opportunity,
      tier: pri.tier,
      baselineEmail: pri.liveEmail,
      baselinePhone: pri.livePhone,
      priority,
      skip: skipComplete,
      skipReason: skipComplete ? "ALREADY_BOTH_CHANNELS" : null,
    });
  }

  queue.sort(
    (a, b) =>
      a.priority - b.priority ||
      text(a.personName).localeCompare(text(b.personName)),
  );
  return { baseline, cohort, queue };
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
  const existingPc = existingPayload.primaryContact || {};
  const nextPc = opportunity.primaryContact || {};
  // Do not overwrite stronger existing channels with weaker blanks.
  const mergedPc = {
    ...existingPc,
    ...nextPc,
    email: text(nextPc.email || existingPc.email) || null,
    workEmail: text(nextPc.workEmail || nextPc.email || existingPc.workEmail) || null,
    phone: text(nextPc.phone || existingPc.phone) || null,
    directPhone: text(nextPc.directPhone || nextPc.phone || existingPc.directPhone) || null,
    phoneExtensions:
      Array.isArray(nextPc.phoneExtensions) && nextPc.phoneExtensions.length
        ? nextPc.phoneExtensions
        : existingPc.phoneExtensions || [],
  };
  const proposed = {
    ...existingPayload,
    ...opportunity,
    primaryContact: mergedPc,
    id: opportunityId,
    opportunityId,
    hotelId,
    publicContactSecondPass: {
      version: "gdi-google-first-second-pass-v1",
      at: new Date().toISOString(),
      providers: ["google_gemini", "exa"],
    },
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
  const wantEmail = text(mergedPc.email).toLowerCase();
  const gotEmail = text(rbPc.email || rbPc.workEmail).toLowerCase();
  const wantPhoneDigits = text(mergedPc.phone).replace(/\D/g, "");
  const gotPhoneDigits = text(rbPc.phone || rbPc.directPhone).replace(/\D/g, "");
  return {
    ok: true,
    recordId: write.airtableRecordId,
    emailMatch: !wantEmail || wantEmail === gotEmail,
    phoneMatch:
      !wantPhoneDigits ||
      gotPhoneDigits === wantPhoneDigits ||
      gotPhoneDigits.endsWith(wantPhoneDigits) ||
      wantPhoneDigits.endsWith(gotPhoneDigits),
    extensionsMatch:
      !mergedPc.phoneExtensions?.length ||
      (Array.isArray(rbPc.phoneExtensions) &&
        mergedPc.phoneExtensions.every((e) =>
          rbPc.phoneExtensions.map(String).includes(String(e)),
        )),
    surfeDerived: rbPc.surfeDerived === true,
  };
}

function attributeRecovery(discovery, before, afterNamedEmail, afterPhone) {
  const m = discovery?.metrics || {};
  const googleOk = m.googleGroundedOk === true;
  const exaUsed = Number(m.exaQueriesExecuted || 0) > 0;
  const googleSkippedExa = m.exaSkippedBecauseGoogleSufficient === true;
  let emailAttr = null;
  let phoneAttr = null;
  if (afterNamedEmail && !before.namedEmail) {
    emailAttr = googleOk && (googleSkippedExa || !exaUsed) ? "google" : googleOk && exaUsed ? "google_then_exa" : "exa";
  }
  if (afterPhone && !before.phone) {
    phoneAttr = googleOk && (googleSkippedExa || !exaUsed) ? "google" : googleOk && exaUsed ? "google_then_exa" : "exa";
  }
  return { emailAttr, phoneAttr, googleOk, exaUsed, googleSkippedExa };
}

async function runSecondPass(plan) {
  const creds = inspectPublicContactSearchCredentials();
  const gCreds = inspectGoogleGroundedContactCredentials();
  const results = [];
  let billedUsd = 0;
  let googleActualUsd = 0;
  let googleEstUsd = 0;
  let exaUsd = 0;
  let googleCalls = 0;
  let exaQueries = 0;
  let stoppedForBudget = false;

  const metrics = {
    planned: plan.queue.filter((q) => !q.skip).length,
    skippedComplete: plan.queue.filter((q) => q.skipReason === "ALREADY_BOTH_CHANNELS").length,
    researched: 0,
    reused: 0,
    newNamedEmails: 0,
    newNamedPhones: 0,
    newExtensions: 0,
    newOrgRoutes: 0,
    newVerifiedLinkedIn: 0,
    emailsFromGoogle: 0,
    emailsFromExa: 0,
    emailsFromGoogleThenExa: 0,
    phonesFromGoogle: 0,
    phonesFromExa: 0,
    phonesFromGoogleThenExa: 0,
    noneToOne: 0,
    oneToBoth: 0,
    stillNone: 0,
    stillOne: 0,
    bothAfter: 0,
    betterEntryPoints: 0,
    newDirectBuyers: 0,
    retainedOriginal: 0,
    incorrectRejected: 0,
    airtableOk: 0,
    airtableFail: 0,
    airtableSkippedNoRow: 0,
    surfeAvoided: 0,
  };

  const progressPath = path.join(OUT_DIR, "PROGRESS.json");
  const doneKeys = new Set();
  if (RESUME && fs.existsSync(progressPath)) {
    try {
      const prev = JSON.parse(fs.readFileSync(progressPath, "utf8"));
      for (const r of prev.results || []) {
        doneKeys.add(`${r.hotelId}::${r.opportunityId}`);
        results.push(r);
      }
      billedUsd = Number(prev.billedUsd || 0);
      googleActualUsd = Number(prev.googleActualUsd || 0);
      googleEstUsd = Number(prev.googleEstimatedUsd || 0);
      exaUsd = Number(prev.exaActualUsd || 0);
      googleCalls = Number(prev.googleCalls || 0);
      exaQueries = Number(prev.exaQueries || 0);
      if (prev.metrics && typeof prev.metrics === "object") {
        Object.assign(metrics, prev.metrics);
      }
      console.error(
        `[resume] loaded ${doneKeys.size} rows, billed=$${billedUsd.toFixed(4)}`,
      );
    } catch (err) {
      console.error(`[resume] ignore corrupt progress: ${err.message}`);
    }
  }

  const work = plan.queue.filter((q) => !q.skip);
  const limited = LIMIT > 0 ? work.slice(0, LIMIT) : work;

  function checkpoint() {
    fs.writeFileSync(
      progressPath,
      `${JSON.stringify(
        {
          updatedAt: new Date().toISOString(),
          billedUsd,
          googleActualUsd,
          googleEstimatedUsd: googleEstUsd,
          exaActualUsd: exaUsd,
          googleCalls,
          exaQueries,
          metrics,
          results,
        },
        null,
        2,
      )}\n`,
    );
  }

  for (const entry of limited) {
    const key = `${entry.hotelId}::${entry.opportunityId}`;
    if (doneKeys.has(key)) continue;

    if (billedUsd >= SPEND_CEILING_USD) {
      stoppedForBudget = true;
      break;
    }

    const before = {
      email: entry.baselineEmail,
      phone: entry.baselinePhone,
      namedEmail:
        entry.baselineEmail && !isGenericMailbox(entry.baselineEmail)
          ? entry.baselineEmail
          : null,
    };

    if (before.namedEmail && before.phone) {
      metrics.reused += 1;
      metrics.bothAfter += 1;
      metrics.surfeAvoided += 1;
      results.push({
        hotelId: entry.hotelId,
        opportunityId: entry.opportunityId,
        personName: entry.personName,
        tierBefore: entry.tier,
        mode: "reuse_complete",
        email: before.namedEmail,
        phone: before.phone,
      });
      continue;
    }

    const remaining = SPEND_CEILING_USD - billedUsd;
    const authorizePaid =
      remaining > 0 &&
      (gCreds.geminiConfigured || creds.exaConfigured);

    let discovery;
    try {
      discovery = await discoverPublicContactChannels({
        opportunity: entry.opportunity,
        authorizePaidSearch: authorizePaid,
        seedRegressionFixtures: entry.founderRegression === true,
        maxQueries: authorizePaid ? MAX_QUERIES : 0,
        maxPages: MAX_PAGES,
        timeoutMs: 20000,
        googleTimeoutMs: 55000,
      });
    } catch (err) {
      metrics.researched += 1;
      metrics.stillNone += entry.tier === "NONE" ? 1 : 0;
      results.push({
        hotelId: entry.hotelId,
        opportunityId: entry.opportunityId,
        personName: entry.personName,
        tierBefore: entry.tier,
        status: "ERROR",
        error: String(err?.message || err).slice(0, 200),
      });
      doneKeys.add(key);
      checkpoint();
      console.error(`[error] ${entry.personName}: ${err.message || err}`);
      continue;
    }

    metrics.researched += 1;

    const gCostActual = Number(discovery.metrics?.googleCostUsd || 0);
    const eCost = Number(discovery.metrics?.exaCostUsd || 0);
    const gAttempted = discovery.metrics?.googleGroundedAttempted === true;
    let gEst = 0;
    if (gAttempted) {
      googleCalls += 1;
      if (gCostActual > 0) {
        googleActualUsd += gCostActual;
        billedUsd += gCostActual;
      } else {
        gEst = GOOGLE_EST_USD_PER_CALL;
        googleEstUsd += gEst;
        billedUsd += gEst;
      }
    }
    exaUsd += eCost;
    billedUsd += eCost;
    exaQueries += Number(discovery.metrics?.exaQueriesExecuted || 0);

    let oppOut = discovery.opportunity || entry.opportunity;

    // Offline commercial relevance (no extra paid ecosystem search).
    const baselinePrimary = { ...(entry.opportunity.primaryContact || {}) };
    const candidates = collectExistingContactCandidates(oppOut);
    for (const c of candidates) {
      c.commercialEvidence =
        c.commercialEvidence || defaultEvidenceForKnownFixtures(c, oppOut);
    }
    const selection = selectBestCommercialContacts(candidates, {
      organizationName: oppOut.organizationName || entry.organization,
    });
    const projected = projectSelectionOntoOpportunity(oppOut, selection);
    const comparison = compareBaselineToEnhanced(baselinePrimary, selection);
    oppOut = projected.opportunity;
    const changes = deriveContactLevelChanges(
      baselinePrimary,
      oppOut,
      selection,
      comparison,
    );
    const upgradeType = text(comparison?.upgradeType);
    if (/BETTER|NEW_PRIMARY|CONTACT_CHANGED/i.test(upgradeType)) {
      metrics.betterEntryPoints += 1;
    } else {
      metrics.retainedOriginal += 1;
    }
    if (/BUYER/i.test(upgradeType)) {
      metrics.newDirectBuyers += 1;
    } else if (
      changes.some((ch) =>
        /NEW_BUYER|BETTER_BUYER/i.test(String(ch?.type || ch)),
      )
    ) {
      metrics.newDirectBuyers += 1;
    }

    const display = buildContactChannelDisplayState(oppOut);
    const emailRaw = text(display.publicDirectEmail) || null;
    const namedEmail =
      emailRaw && !isGenericMailbox(emailRaw) ? emailRaw : null;
    const phone = text(display.publicDirectPhone) || null;
    const orgEmail = text(display.organizationalEmail) || null;
    const extensions = Array.isArray(display.phoneExtensions)
      ? display.phoneExtensions
      : [];
    const liStatus = text(oppOut.primaryContact?.liProfileMatchStatus);
    const liUrl =
      liStatus === "VERIFIED_LINKEDIN_PROFILE"
        ? text(oppOut.primaryContact?.linkedinUrl)
        : null;

    const attr = attributeRecovery(discovery, before, namedEmail, phone);

    if (namedEmail && !before.namedEmail) {
      metrics.newNamedEmails += 1;
      if (attr.emailAttr === "google") metrics.emailsFromGoogle += 1;
      else if (attr.emailAttr === "exa") metrics.emailsFromExa += 1;
      else if (attr.emailAttr === "google_then_exa") {
        metrics.emailsFromGoogleThenExa += 1;
      }
    }
    if (phone && !before.phone) {
      metrics.newNamedPhones += 1;
      if (attr.phoneAttr === "google") metrics.phonesFromGoogle += 1;
      else if (attr.phoneAttr === "exa") metrics.phonesFromExa += 1;
      else if (attr.phoneAttr === "google_then_exa") {
        metrics.phonesFromGoogleThenExa += 1;
      }
    }
    if (extensions.length && !(entry.opportunity.primaryContact?.phoneExtensions || []).length) {
      metrics.newExtensions += 1;
    }
    if (orgEmail) metrics.newOrgRoutes += 1;
    if (liUrl) metrics.newVerifiedLinkedIn += 1;

    const afterBoth = Boolean(namedEmail && phone);
    const afterOne = Boolean((namedEmail || phone) && !afterBoth);
    const afterNone = !namedEmail && !phone;

    if (entry.tier === "NONE" && afterOne) metrics.noneToOne += 1;
    if (entry.tier === "ONE" && afterBoth) metrics.oneToBoth += 1;
    if (entry.tier === "NONE" && afterBoth) {
      metrics.noneToOne += 1; // progressed through one conceptually
      metrics.oneToBoth += 1;
    }
    if (afterBoth) metrics.bothAfter += 1;
    else if (afterOne) metrics.stillOne += 1;
    else metrics.stillNone += 1;

    if (namedEmail || phone) metrics.surfeAvoided += 1;

    if (
      namedEmail &&
      oppOut.primaryContact?.name &&
      nameKey(oppOut.primaryContact.name) !== nameKey(entry.personName)
    ) {
      metrics.incorrectRejected += 1;
    }

    let persist = null;
    if (
      PERSIST &&
      (namedEmail || phone || liUrl || extensions.length) &&
      discovery.status !== PUBLIC_CONTACT_RESEARCH_STATUS.REUSED_EXISTING
    ) {
      persist = await persistVerified(
        entry.hotelId,
        entry.opportunityId,
        oppOut,
      );
      if (persist?.ok && persist.emailMatch !== false && !persist.surfeDerived) {
        metrics.airtableOk += 1;
      } else if (persist?.skipped) {
        metrics.airtableSkippedNoRow += 1;
      } else if (persist) {
        metrics.airtableFail += 1;
      }
    }

    const row = {
      hotelId: entry.hotelId,
      opportunityId: entry.opportunityId,
      personName: entry.personName,
      organization: entry.organization,
      tierBefore: entry.tier,
      status: discovery.status,
      mode: authorizePaid
        ? gCreds.geminiConfigured
          ? "google_first_then_exa"
          : "exa_only_fallback"
        : "offline_only",
      email: namedEmail,
      genericEmailExcluded: emailRaw && !namedEmail ? emailRaw : null,
      phone,
      phoneExtensions: extensions,
      orgEmail,
      linkedInUrl: liUrl,
      linkedInStatus: liStatus || null,
      attribution: attr,
      google: {
        attempted: gAttempted,
        ok: discovery.metrics?.googleGroundedOk === true,
        estUsd: gEst,
        actualUsd: gCostActual,
        webQueries: discovery.metrics?.googleWebSearchQueries || [],
      },
      exa: {
        queries: discovery.metrics?.exaQueriesExecuted || 0,
        costUsd: eCost,
        skipped: discovery.metrics?.exaSkippedBecauseGoogleSufficient === true,
        reason: discovery.metrics?.exaFallbackReason || null,
      },
      commercial: {
        upgradeType: comparison?.upgradeType || null,
        betterEntryPoint: /BETTER|NEW_PRIMARY|CONTACT_CHANGED/i.test(
          text(comparison?.upgradeType),
        ),
        retained: /RETAINED/i.test(text(comparison?.upgradeType)),
        changeTypes: (changes || []).map((c) => c.type || c).slice(0, 6),
      },
      sourceUrl:
        display.channelSourceUrl ||
        oppOut.primaryContact?.channelSourceUrl ||
        null,
      persist,
      billedUsdRunning: Number(billedUsd.toFixed(4)),
    };
    results.push(row);
    doneKeys.add(key);
    checkpoint();
    console.error(
      `[${results.length}/${limited.length}] ${entry.personName} · ${discovery.status || "?"} · email=${namedEmail || "—"} phone=${phone || "—"} · billed=$${billedUsd.toFixed(3)}`,
    );
  }

  return {
    generatedAt: new Date().toISOString(),
    providerMode: creds.chosenMode,
    googleReady: gCreds.geminiConfigured,
    googleEnvVar: gCreds.envVarPreferred,
    spendCeilingUsd: SPEND_CEILING_USD,
    googleEstUsdPerCall: GOOGLE_EST_USD_PER_CALL,
    actualSpend: {
      billedTowardCapUsd: Number(billedUsd.toFixed(4)),
      googleActualUsd: Number(googleActualUsd.toFixed(4)),
      googleEstimatedUsd: Number(googleEstUsd.toFixed(4)),
      exaActualUsd: Number(exaUsd.toFixed(4)),
      googleCalls,
      exaQueries,
      note:
        "Gemini amountUsd often COST_UNKNOWN; estimated USD used for cap only. Exa is provider-reported.",
    },
    stoppedForBudget,
    surfeCalls: 0,
    baseline: {
      both: plan.baseline.metrics?.contactsWithBothChannels,
      one: plan.baseline.metrics?.contactsWithOneChannel,
      none: plan.baseline.metrics?.contactsWithNoRecoverableChannels,
      newEmails: plan.baseline.metrics?.newPublicEmails,
      newPhones: plan.baseline.metrics?.newPublicPhones,
      spend: plan.baseline.actualSpendUsd,
      mode: plan.baseline.providerMode,
    },
    metrics,
    results,
  };
}

async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const plan = buildWorkQueue();
  const planSummary = {
    generatedAt: new Date().toISOString(),
    spendCeilingUsd: SPEND_CEILING_USD,
    googleEstUsdPerCall: GOOGLE_EST_USD_PER_CALL,
    credentials: inspectPublicContactSearchCredentials(),
    google: inspectGoogleGroundedContactCredentials(),
    counts: {
      total: plan.queue.length,
      none: plan.queue.filter((q) => q.tier === "NONE" && !q.skip).length,
      one: plan.queue.filter((q) => q.tier === "ONE" && !q.skip).length,
      completeSkipped: plan.queue.filter(
        (q) => q.skipReason === "ALREADY_BOTH_CHANNELS",
      ).length,
      missingLocalOpp: plan.queue.filter(
        (q) => q.skipReason === "OPPORTUNITY_NOT_FOUND_LOCALLY",
      ).length,
      toResearch: plan.queue.filter((q) => !q.skip).length,
    },
    priorityHead: plan.queue
      .filter((q) => !q.skip)
      .slice(0, 15)
      .map((q) => ({
        tier: q.tier,
        personName: q.personName,
        organization: q.organization,
        opportunityId: q.opportunityId,
      })),
  };
  fs.writeFileSync(
    path.join(OUT_DIR, "PLAN.json"),
    `${JSON.stringify(planSummary, null, 2)}\n`,
  );

  if (PLAN_ONLY || !RUN) {
    console.log(JSON.stringify(planSummary, null, 2));
    return;
  }

  if (!planSummary.google.geminiConfigured) {
    console.warn(
      "WARNING: GEMINI_NOT_CONFIGURED — second pass will continue Exa-only; Google-first coverage NOT claimed.",
    );
  }

  const report = await runSecondPass(plan);
  fs.writeFileSync(
    path.join(OUT_DIR, "SECOND_PASS_RESULTS.json"),
    `${JSON.stringify(report, null, 2)}\n`,
  );

  const md = `# GDI — Google-first second pass (original 100 cohort)

| Metric | Baseline (EXA_ONLY) | Second pass |
|--------|---------------------|-------------|
| Mode | ${report.baseline.mode} | ${report.providerMode} |
| Google ready | n/a | ${report.googleReady} |
| Both / one / none | ${report.baseline.both} / ${report.baseline.one} / ${report.baseline.none} | both+ ${report.metrics.bothAfter} · still one ${report.metrics.stillOne} · still none ${report.metrics.stillNone} |
| New named emails | ${report.baseline.newEmails} (pass1) | **+${report.metrics.newNamedEmails}** this pass |
| New named phones | ${report.baseline.newPhones} (pass1) | **+${report.metrics.newNamedPhones}** this pass |
| New extensions | — | ${report.metrics.newExtensions} |
| Verified LinkedIn | — | ${report.metrics.newVerifiedLinkedIn} |
| Org routes | — | ${report.metrics.newOrgRoutes} |
| None→one / one→both | — | ${report.metrics.noneToOne} / ${report.metrics.oneToBoth} |
| Emails Google / Exa / both | — | ${report.metrics.emailsFromGoogle} / ${report.metrics.emailsFromExa} / ${report.metrics.emailsFromGoogleThenExa} |
| Phones Google / Exa / both | — | ${report.metrics.phonesFromGoogle} / ${report.metrics.phonesFromExa} / ${report.metrics.phonesFromGoogleThenExa} |
| Better entry points | — | ${report.metrics.betterEntryPoints} |
| New direct buyers | — | ${report.metrics.newDirectBuyers} |
| Airtable OK / fail / no-row | — | ${report.metrics.airtableOk} / ${report.metrics.airtableFail} / ${report.metrics.airtableSkippedNoRow} |
| Spend (billed to cap) | $${report.baseline.spend} | **$${report.actualSpend.billedTowardCapUsd} / $${report.spendCeilingUsd}** |
| Google actual / est | — | $${report.actualSpend.googleActualUsd} / $${report.actualSpend.googleEstimatedUsd} |
| Exa actual | — | $${report.actualSpend.exaActualUsd} |
| Surfe | 0 | **0** |
| Budget stop | — | ${report.stoppedForBudget} |

Surfe calls: **0**. Cohort not expanded. Customer STAGED→CURRENT: **not requested**.
`;
  fs.writeFileSync(path.join(OUT_DIR, "FOUNDER_REPORT.md"), md);
  console.log(
    JSON.stringify(
      {
        ...report,
        results: `[${report.results.length} rows]`,
        wrote: path.join(OUT_DIR, "SECOND_PASS_RESULTS.json"),
      },
      null,
      2,
    ),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
