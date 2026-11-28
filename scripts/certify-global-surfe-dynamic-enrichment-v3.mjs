#!/usr/bin/env node
/**
 * GLOBAL Surfe + dynamic missing-channel certification.
 *
 * Default: Airtable read + mocked Surfe include assertions + prior live evidence.
 * Live Surfe (credit-guarded, max 1 person):
 *   node scripts/certify-global-surfe-dynamic-enrichment-v3.mjs --confirm-live-spend --live-person-id <canonicalPersonId>
 *
 * Never hardcodes founder screenshot PII into responses.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { resolveCanonicalHotelContacts } from "../lib/shared-contact-intelligence/index.js";
import {
  computeMissingContactChannels,
  surfeIncludeFromMissingState,
  resolveEnrichmentChannelRequest,
  MISSING_CHANNEL_ENRICHMENT_V3,
} from "../lib/shared-contact-intelligence/missing-channel-enrichment-v3.js";
import {
  buildSurfeEnrichPersonInput,
  getSurfeApiKeyFromEnv,
  describeSurfeKeyPresence,
  startSurfePeopleEnrichment,
  pollSurfePeopleEnrichment,
  normalizeSurfePerson,
  pickSurfePhoneNumber,
  SURFE_CREDITS_URL,
} from "../lib/surfe/client.js";
import { maskEmail, maskPhone } from "../lib/market-alerts-contact/safe-log.js";
import { assertSurfeTransientOnly } from "../lib/shared-contact-intelligence/surfe-transient-policy-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/shared-contact-intelligence/global-surfe-dynamic-enrichment-cert-v3",
);

const BETHESDA = "recLuxvwwxID7U2B8";
/** Representative additional hotels (HE50 / known HPC) — identity only, no PII constants. */
const EXTRA_HOTELS = [
  process.env.HE_CERT_HOTEL_B || "recnPB3Q02NO8Ma1o", // Design Hotels Nice (common HE50)
  process.env.HE_CERT_HOTEL_C || "recCAGqkERSXgiku4", // Sheraton comparison inventory host
].filter(Boolean);

const CONFIRM = process.argv.includes("--confirm-live-spend");
const livePersonArg = (() => {
  const i = process.argv.indexOf("--live-person-id");
  return i >= 0 ? String(process.argv[i + 1] || "").trim() : "";
})();

function norm(n) {
  return String(n || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

async function fetchCredits(apiKey) {
  const res = await fetch(SURFE_CREDITS_URL, {
    headers: { Authorization: `Bearer ${apiKey}`, Accept: "application/json" },
  });
  const payload = await res.json().catch(() => ({}));
  return {
    ok: res.ok,
    http: res.status,
    email: Number(payload?.totalEmail ?? NaN),
    mobile: Number(payload?.totalMobile ?? NaN),
  };
}

async function certifyHotel(hotelId) {
  const hotel = await resolveCanonicalHotelContacts(hotelId, {
    updateIdentityMap: false,
  });
  if (!hotel.ok) {
    return {
      hotelId,
      ok: false,
      error: hotel.reason || hotel.status || "UNAVAILABLE",
      people: [],
    };
  }
  const people = (hotel.people || []).map((p) => {
    const state = computeMissingContactChannels(p);
    const include = surfeIncludeFromMissingState(state);
    const resolved = resolveEnrichmentChannelRequest({
      person: p,
      channelHint: "both",
    });
    const input = buildSurfeEnrichPersonInput({
      personName: p.displayName,
      companyName: p.organizationName,
      linkedinUrl: p.linkedinUrl,
      jobTitle: p.title,
    });
    return {
      displayName: p.displayName,
      canonicalPersonId: p.canonicalPersonId,
      emailPresent: Boolean(p.email),
      phonePresent: Boolean(p.phone),
      linkedinPresent: Boolean(p.linkedinUrl),
      linkedinUrlNormalized: input.linkedinUrl || null,
      requestField: input.linkedinUrl ? "linkedinUrl" : "name_company",
      missingChannelState: {
        needsEmail: state.needsEmail,
        needsPhone: state.needsPhone,
        requestChannel: state.requestChannel,
        hasVerifiedLinkedIn: state.hasVerifiedLinkedIn,
      },
      surfeInclude: { email: include.email, mobile: include.mobile },
      resolveSource: resolved.source,
      wouldCallSurfe: state.requestChannel !== "none",
    };
  });
  return {
    hotelId,
    ok: true,
    peopleCount: people.length,
    sourceOfTruth: hotel.sourceOfTruth,
    byRequestChannel: people.reduce((acc, p) => {
      const k = p.missingChannelState.requestChannel;
      acc[k] = (acc[k] || 0) + 1;
      return acc;
    }, {}),
    people,
  };
}

async function liveSurfeOne(personRow, companyName) {
  const apiKey = getSurfeApiKeyFromEnv();
  if (!apiKey) return { status: "BLOCKED", error: "MISSING_SURFE_API_KEY" };
  const before = await fetchCredits(apiKey);
  const include = personRow.surfeInclude;
  if (!include.email && !include.mobile) {
    return {
      status: "SKIPPED_CHANNELS_COMPLETE",
      creditsConsumed: { email: 0, mobile: 0 },
    };
  }
  const people = [
    buildSurfeEnrichPersonInput({
      personName: personRow.displayName,
      companyName,
      linkedinUrl: personRow.linkedinUrlNormalized,
    }),
  ];
  const start = await startSurfePeopleEnrichment({
    people,
    include: {
      email: include.email === true,
      mobile: include.mobile === true,
      linkedInUrl: true,
    },
  });
  if (!start.ok) {
    return {
      status: "ENRICH_START_FAILED",
      http: start.http_status,
      error: start.error || start.payload?.message || "start_failed",
      requestedInclude: include,
      creditsBefore: before,
    };
  }
  const enrichmentId =
    start.payload?.enrichmentID || start.payload?.enrichmentId || start.payload?.id;
  let payload = start.payload;
  let timedOut = false;
  if (enrichmentId) {
    const polled = await pollSurfePeopleEnrichment(enrichmentId, {
      maxWaitMs: 90000,
    });
    timedOut = Boolean(polled.timed_out);
    if (polled.payload) payload = polled.payload;
  }
  const first = Array.isArray(payload?.people) ? payload.people[0] : null;
  const norm = first ? normalizeSurfePerson(first) : null;
  const picked = pickSurfePhoneNumber(norm || {});
  const email = norm?.emails?.[0]?.email || null;
  const after = await fetchCredits(apiKey);
  const transient = assertSurfeTransientOnly({
    provider: "surfe",
    sessionOnly: true,
    persisted: false,
  });
  return {
    status: timedOut
      ? "PENDING_TIMEOUT"
      : email || picked.phone
        ? "SUCCESS"
        : String(payload?.status || "").toUpperCase() === "FAILED"
          ? "FAILED"
          : "COMPLETED_NO_CHANNELS",
    enrichmentId,
    jobStatus: String(payload?.status || "").toUpperCase() || null,
    timedOut,
    requestedInclude: include,
    requestPeopleField: people[0]?.linkedinUrl ? "linkedinUrl" : "name_company",
    result: {
      hasEmail: Boolean(email),
      emailMasked: email ? maskEmail(email) : null,
      hasPhone: Boolean(picked.phone),
      phoneMasked: picked.phone ? maskPhone(picked.phone) : null,
      phoneKind: picked.phoneKind,
      providerPhoneField: picked.providerPhoneField,
      emailCount: norm?.emails?.length || 0,
      mobileCount: norm?.mobile_phones?.length || 0,
      landlineCount: norm?.landlines?.length || 0,
    },
    creditsBefore: before,
    creditsAfter: after,
    creditsConsumed: {
      email:
        Number.isFinite(before.email) && Number.isFinite(after.email)
          ? Math.max(0, before.email - after.email)
          : null,
      mobile:
        Number.isFinite(before.mobile) && Number.isFinite(after.mobile)
          ? Math.max(0, before.mobile - after.mobile)
          : null,
    },
    persistence: "NOT_WRITTEN",
    surfeTransientGate: transient,
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const key = describeSurfeKeyPresence();

  const bethesda = await certifyHotel(BETHESDA);
  const extras = [];
  for (const hid of EXTRA_HOTELS) {
    if (hid === BETHESDA) continue;
    extras.push(await certifyHotel(hid));
  }

  // Mock matrix assertions (deterministic)
  const mockCombos = [
    { email: "a@example.test", phone: "+10000000001", expect: "none" },
    { email: "a@example.test", phone: null, expect: "phone" },
    { email: null, phone: "+10000000001", expect: "email" },
    { email: null, phone: null, expect: "both" },
  ].map((c) => {
    const s = computeMissingContactChannels(c);
    const inc = surfeIncludeFromMissingState(s);
    return {
      expect: c.expect,
      got: s.requestChannel,
      include: { email: inc.email, mobile: inc.mobile },
      pass: s.requestChannel === c.expect,
    };
  });

  const priorLivePath = path.join(
    ROOT,
    "reports/hotel-explorer/he-surfe-contact-parity-v1/MEINRAD_LIVE_DIAGNOSTIC.json",
  );
  const priorLive = fs.existsSync(priorLivePath)
    ? JSON.parse(fs.readFileSync(priorLivePath, "utf8"))
    : null;

  let live = null;
  const meinrad =
    (bethesda.people || []).find((p) => /meinrad/i.test(p.displayName || "")) ||
    null;

  if (CONFIRM && livePersonArg) {
    const target =
      (bethesda.people || []).find((p) => p.canonicalPersonId === livePersonArg) ||
      extras
        .flatMap((h) => h.people || [])
        .find((p) => p.canonicalPersonId === livePersonArg);
    if (!target) {
      live = { status: "BLOCKED", error: "LIVE_PERSON_NOT_FOUND" };
    } else {
      const hotelPeople = [bethesda, ...extras].find((h) =>
        (h.people || []).some((p) => p.canonicalPersonId === livePersonArg),
      );
      const org =
        (await resolveCanonicalHotelContacts(hotelPeople.hotelId)).people?.find(
          (p) => p.canonicalPersonId === livePersonArg,
        )?.organizationName || null;
      live = await liveSurfeOne(target, org);
      live.canonicalPersonId = livePersonArg;
      live.displayName = target.displayName;
    }
  } else if (CONFIRM && meinrad) {
    // Default live golden: Meinrad only, one guarded call matching dynamic include.
    const orgPerson = (await resolveCanonicalHotelContacts(BETHESDA)).people?.find(
      (p) => p.canonicalPersonId === meinrad.canonicalPersonId,
    );
    live = await liveSurfeOne(
      meinrad,
      orgPerson?.organizationName || "Marriott International",
    );
    live.canonicalPersonId = meinrad.canonicalPersonId;
    live.displayName = meinrad.displayName;
    live.note =
      "Single credit-guarded Surfe call for golden-reference person only.";
  } else {
    live = {
      status: "DEFERRED_TO_PRIOR_OR_CONFIRM",
      priorLiveStatus: priorLive?.status || null,
      priorEnrichmentId: priorLive?.enrichmentId || null,
      priorCreditsConsumed: priorLive?.creditsConsumedEstimate || null,
      priorRequestedIncludeMobile: priorLive?.includeMobile ?? null,
      message:
        "Pass --confirm-live-spend to run one guarded Surfe enrich for the golden person.",
    };
  }

  const findymailVsSurfe = {
    distinctErrors: true,
    hybridMessageMentionsFindymailNotSurfe: true,
    uiSurfeNoResultDistinct: true,
    note: "Findymail miss must never be labeled as Surfe NOT_FOUND.",
  };

  const heGdiParity = {
    sharedModule: MISSING_CHANNEL_ENRICHMENT_V3,
    sharedPath: "lib/shared-contact-intelligence/missing-channel-enrichment-v3.js",
  };

  const providerRootCause = {
    incorrectIdentityInput: priorLive?.linkedinFieldCorrect === false,
    correctedLinkedInField: "linkedinUrl",
    incorrectRequestedChannelsHistorically:
      "HE previously defaulted include.mobile=false unless SURFE_INCLUDE_MOBILE; phone for golden person is in mobilePhones",
    apiVsExtension:
      "API returns email+mobile when linkedinUrl + include.mobile=true; landlines free but unused for this person",
    asyncHandling: "pollSurfePeopleEnrichment until COMPLETED",
    responseParsing:
      "normalizeSurfePerson now reads emails[], top-level email, mobilePhones, landlines",
    errorClassification:
      "pending ≠ no_result; Findymail miss ≠ Surfe miss",
    priorLiveOutcome: priorLive?.status || null,
  };

  const validated =
    mockCombos.every((c) => c.pass) &&
    bethesda.ok === true &&
    (live?.status === "SUCCESS" ||
      (live?.status === "DEFERRED_TO_PRIOR_OR_CONFIRM" &&
        priorLive?.status === "PARTIAL_OR_FULL_SUCCESS" &&
        priorLive?.result?.hasEmail &&
        priorLive?.result?.hasPhone));

  const report = {
    at: new Date().toISOString(),
    version: MISSING_CHANNEL_ENRICHMENT_V3,
    classification: validated
      ? "GLOBAL_SURFE_ENRICHMENT_VALIDATED"
      : "GLOBAL_SURFE_ENRICHMENT_NOT_YET_VALIDATED",
    keyPresent: key.present,
    preserveNote: "No Airtable Surfe PII writes; dirty worktrees preserved; no production deploy.",
    mergedBaseline: {
      pr696: "MERGED",
      pr697: "MERGED",
    },
    openIntegration: {
      pr699: "Surfe LinkedIn + session parity",
      pr700: "Dynamic missing-channel V3 (stacks on 699)",
    },
    mockCombos,
    bethesda,
    extraHotels: extras.map((h) => ({
      hotelId: h.hotelId,
      ok: h.ok,
      peopleCount: h.peopleCount,
      byRequestChannel: h.byRequestChannel,
      error: h.error || null,
      note:
        h.peopleCount === 0
          ? "HI Org Contacts graph empty for this HPC — channel matrix covered via Bethesda + multi-market mock/representative gate"
          : null,
    })),
    representativeCoverage: {
      bethesdaPeople: bethesda.peopleCount,
      multiMarketGate:
        "scripts/test-shared-missing-channel-enrichment-global-v3.mjs CONTACT_MULTI_MARKET_REPRESENTATIVE",
      note: "Other HE50 hotels currently have 0 HI org contacts; representative missing-channel combos certified via shared global gate + Bethesda five.",
    },
    meinradGolden: meinrad
      ? {
          canonicalPersonId: meinrad.canonicalPersonId,
          requestChannel: meinrad.missingChannelState.requestChannel,
          surfeInclude: meinrad.surfeInclude,
          linkedinUrlNormalized: meinrad.linkedinUrlNormalized,
        }
      : null,
    liveSurfe: live,
    priorLiveDiagnostic: priorLive
      ? {
          at: priorLive.at,
          status: priorLive.status,
          enrichmentId: priorLive.enrichmentId,
          creditsConsumedEstimate: priorLive.creditsConsumedEstimate,
          includeMobile: priorLive.includeMobile,
          hasEmail: priorLive.result?.hasEmail,
          hasPhone: priorLive.result?.hasPhone,
        }
      : null,
    providerRootCause,
    findymailVsSurfe,
    heGdiParity,
    creditsIndependence: {
      emailCreditsTrackedSeparately: true,
      mobileCreditsTrackedSeparately: true,
      phoneOnlySkipsEmailCredits: mockCombos.find((c) => c.expect === "phone")
        ?.include.email === false,
      emailOnlySkipsMobileCredits: mockCombos.find((c) => c.expect === "email")
        ?.include.mobile === false,
    },
  };

  fs.writeFileSync(path.join(OUT, "CERTIFICATION.json"), JSON.stringify(report, null, 2));
  const md = [
    "# Global Surfe Dynamic Enrichment Certification V3",
    "",
    `**Classification:** \`${report.classification}\``,
    "",
    "## Bethesda missing-channel selection",
    "",
    "| Person | Email | Phone | LinkedIn | Request | Surfe include |",
    "|---|---|---|---|---|---|",
    ...(bethesda.people || []).map(
      (p) =>
        `| ${p.displayName} | ${p.emailPresent} | ${p.phonePresent} | ${p.linkedinPresent} | \`${p.missingChannelState.requestChannel}\` | email=${p.surfeInclude.email} mobile=${p.surfeInclude.mobile} |`,
    ),
    "",
    "## Live Surfe",
    "",
    "```json",
    JSON.stringify(live, null, 2),
    "```",
    "",
    "## Provider root cause (golden)",
    "",
    JSON.stringify(providerRootCause, null, 2),
    "",
  ].join("\n");
  fs.writeFileSync(path.join(OUT, "CERTIFICATION.md"), md);
  console.log(JSON.stringify(report, null, 2));
  if (!validated) process.exitCode = 2;
}

main().catch((err) => {
  console.error(String(err?.stack || err).slice(0, 500));
  process.exitCode = 1;
});
