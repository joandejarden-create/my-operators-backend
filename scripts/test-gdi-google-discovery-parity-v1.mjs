#!/usr/bin/env node
/**
 * Offline + optional live Google discovery parity diagnostics.
 *
 * Offline: prompts, candidate-on-source verification, wrapper URL ranking.
 * Live (--live): bounded Gemini grounding for founder cases (Islas/Maritza/Alejandra
 * + sample unresolved). Separates GOOGLE_CONSUMER_OBSERVED vs GEMINI vs VERIFIED.
 *
 *   node scripts/test-gdi-google-discovery-parity-v1.mjs
 *   node scripts/test-gdi-google-discovery-parity-v1.mjs --live
 */
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import dotenv from "dotenv";
import {
  buildGoogleGroundedContactPrompt,
  buildGoogleGroundedFollowUpPrompt,
  verifyGoogleCandidateOnHtml,
  isGoogleGroundingWrapperUrl,
  classifyLinkedInProfileMatch,
  LINKEDIN_PROFILE_MATCH,
  runGoogleGroundedContactDiscovery,
} from "../lib/group-demand-intelligence/presentation/gdi-google-grounded-contact-discovery-v1.js";
import {
  discoverPublicContactChannels,
  scorePublicContactCandidateUrl,
  GDI_PUBLIC_CONTACT_FAILURE_STAGE,
  ISLAS_PUBLIC_CONTACT_FIXTURE_V1 as ISLAS,
  MARITZA_PUBLIC_CONTACT_FIXTURE_V1 as MARITZA,
  ALEJANDRA_PUBLIC_CONTACT_FIXTURE_V1 as ALEJ,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-discovery-v1.js";
import { recoverPublicContactChannels } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-channel-hierarchy-v1.js";

dotenv.config({ path: join(process.cwd(), ".env"), override: false });
dotenv.config({
  path: "C:/Dev/dealality/my-operators-backend/.env",
  override: false,
});

const LIVE = process.argv.includes("--live");
const OUT = join(process.cwd(), "reports/gdi-google-discovery-parity-v1");
mkdirSync(OUT, { recursive: true });

// --- Offline: natural-language prompt ---
const prompt = buildGoogleGroundedContactPrompt(
  { name: ISLAS.personName, title: ISLAS.title },
  { name: ISLAS.organization, domain: "hcg.gob.mx" },
);
assert.match(prompt.text, /Find the publicly listed professional contact information/i);
assert.match(prompt.text, /ORIGINAL source URLs/i);
assert.match(prompt.text, /hcg\.gob\.mx/i);
assert.match(prompt.text, /Do not infer|invent telephone|Do not invent/i);

const follow = buildGoogleGroundedFollowUpPrompt(
  { name: ISLAS.personName },
  { name: ISLAS.organization, domain: "hcg.gob.mx" },
  { candidates: { email: null, phone: null }, sourceUrls: [] },
);
assert.match(follow.text, /Still missing/i);
assert.match(follow.text, /site:hcg\.gob\.mx/i);

// --- Offline: wrapper ranking ---
assert.ok(
  scorePublicContactCandidateUrl(
    "https://portal.hcg.gob.mx/hcg/directorioind/x",
  ) >
    scorePublicContactCandidateUrl(
      "https://vertexaisearch.cloud.google.com/grounding-api-redirect/x",
    ),
);
assert.equal(
  isGoogleGroundingWrapperUrl(
    "https://vertexaisearch.cloud.google.com/grounding-api-redirect/abc",
  ),
  true,
);

// --- Offline: candidate-on-source verification ---
const html = `
  <h1>Lic. Francisco Javier Islas Godoy</h1>
  <p>Coordinador General de Adquisiciones</p>
  <p>Email: fislas@hcg.gob.mx</p>
  <p>Tel: 33 3883 4400 Ext. 51621</p>
`;
const hit = verifyGoogleCandidateOnHtml(html, ISLAS.personName, {
  email: "fislas@hcg.gob.mx",
  phone: "+52 33 3883 4400",
  phoneExtensions: ["51621"],
});
assert.equal(hit.email, "fislas@hcg.gob.mx");
assert.ok(hit.phone);
assert.ok(hit.phoneExtensions.includes("51621"));

const miss = verifyGoogleCandidateOnHtml(html, "Someone Else Entirely", {
  email: "fislas@hcg.gob.mx",
});
assert.equal(miss.email, null);
assert.ok(miss.reasons.includes("PERSON_MATCH_FAILED"));

// --- Offline: recover with Google candidate hints (Islas fixture HTML) ---
const hcgPath = join(
  process.cwd(),
  "reports/gdi-public-contact-search-before-surfe-v1/hcg-islas-directory.html",
);
assert.ok(existsSync(hcgPath));
const hcgHtml = readFileSync(hcgPath, "utf8");
const recovered = await recoverPublicContactChannels(
  {
    id: "disc_6ea16462392a",
    primaryContact: { name: ISLAS.personName, title: ISLAS.title },
    organizationName: ISLAS.organization,
    sources: [{ url: ISLAS.expectedSearchHitUrl }],
  },
  {
    maxUrls: 1,
    skipDns: true,
    googleCandidateHints: {
      email: "fislas@hcg.gob.mx",
      phone: "+52 33 3883 4400",
      phoneExtensions: ["51621"],
    },
    fetchImpl: async (url) => ({
      ok: true,
      status: 200,
      text: async () => hcgHtml,
      arrayBuffer: async () => Buffer.from(hcgHtml),
      headers: new Map([["content-type", "text/html"]]),
      url,
    }),
  },
);
assert.equal(String(recovered.found?.email || "").toLowerCase(), ISLAS.expectedEmail);

// --- Permanent regression: Google candidate must beat lookalike alternate email ---
// A page that also lists francisco.islas@jalisco.gob.mx must NOT override Google's
// correctly identified fislas@hcg.gob.mx when that candidate is verified on-source.
const lookalikeHtml = `
  <h1>Lic. Francisco Javier Islas Godoy</h1>
  <p>Coordinador General de Adquisiciones — Hospital Civil de Guadalajara</p>
  <p>Also listed elsewhere: francisco.islas@jalisco.gob.mx</p>
  <p>Official: fislas@hcg.gob.mx · Tel 33 3883 4400 Ext. 51621</p>
`;
const lookalikeRecovered = await recoverPublicContactChannels(
  {
    id: "disc_6ea16462392a",
    primaryContact: { name: ISLAS.personName, title: ISLAS.title },
    organizationName: ISLAS.organization,
    sources: [{ url: "https://portal.hcg.gob.mx/hcg/directorioind/lookalike-regression" }],
  },
  {
    maxUrls: 1,
    skipDns: true,
    googleCandidateHints: {
      email: "fislas@hcg.gob.mx",
      phone: "+52 33 3883 4400",
      phoneExtensions: ["51621"],
    },
    fetchImpl: async (url) => ({
      ok: true,
      status: 200,
      text: async () => lookalikeHtml,
      arrayBuffer: async () => Buffer.from(lookalikeHtml),
      headers: new Map([["content-type", "text/html"]]),
      url,
    }),
  },
);
assert.equal(
  String(lookalikeRecovered.found?.email || "").toLowerCase(),
  "fislas@hcg.gob.mx",
  "lookalike alternate must not override Google candidate",
);
assert.notEqual(
  String(lookalikeRecovered.found?.email || "").toLowerCase(),
  "francisco.islas@jalisco.gob.mx",
);

const li = classifyLinkedInProfileMatch({
  url: "https://www.linkedin.com/in/alejandra-vazquez-igeco",
  personName: "Alejandra Vázquez",
  organization: "IGECO",
  evidenceText: "Sales Manager IGECO Mexico",
});
assert.equal(li.status, LINKEDIN_PROFILE_MATCH.VERIFIED_LINKEDIN_PROFILE);

const offlineReport = {
  ok: true,
  failureStagesCatalog: Object.keys(GDI_PUBLIC_CONTACT_FAILURE_STAGE),
  promptNaturalLanguage: true,
  candidateOnSourceVerify: true,
  wrapperDeprioritized: true,
  lookalikePersonChannelAssociationBlocked: true,
};

writeFileSync(join(OUT, "OFFLINE_PARITY.json"), JSON.stringify(offlineReport, null, 2));
console.log("PASS offline google discovery parity");

if (!LIVE) {
  process.exit(0);
}

// --- Live benchmark (bounded) ---
const CASES = [
  {
    id: "islas",
    personName: ISLAS.personName,
    title: ISLAS.title,
    organization: ISLAS.organization,
    domain: "hcg.gob.mx",
    opportunityId: "disc_6ea16462392a",
    consumerObserved: {
      email: "fislas@hcg.gob.mx",
      phone: "+52 33 3883 4400",
      extensions: ["51621"],
      sourceUrl: ISLAS.expectedSearchHitUrl,
      note: "Founder Google AI / official HCG directorioind",
    },
    expectedEmail: ISLAS.expectedEmail,
    expectedExt: "51621",
  },
  {
    id: "maritza",
    personName: MARITZA.personName,
    title: MARITZA.title,
    organization: MARITZA.organization,
    domain: "jalisco.gob.mx",
    opportunityId: "disc_b4661c17bd92",
    consumerObserved: {
      email: "maritza.medina@jalisco.gob.mx",
      phone: "+52 33 3819 2333",
      extensions: ["42333", "47542"],
      note: "Founder Google / Jalisco directory",
    },
    expectedEmail: MARITZA.expectedEmail,
  },
  {
    id: "alejandra",
    personName: ALEJ.personName,
    title: ALEJ.title,
    organization: ALEJ.organization,
    domain: "igeco.mx",
    opportunityId: "paid3_0a6b19883953",
    consumerObserved: {
      email: "alejandra.vazquez@igeco.mx",
      phone: null,
      linkedIn: "candidate if identity supported",
      note: "Founder Google / Ecomondo contact page",
    },
    expectedEmail: ALEJ.expectedEmail,
  },
];

const liveRows = [];
let spendEst = 0;
for (const c of CASES) {
  // Single live path: discovery runs Google-first (+ follow-up) then Exa if needed.
  const disc = await discoverPublicContactChannels({
    opportunity: {
      id: c.opportunityId,
      primaryContact: { name: c.personName, title: c.title },
      organizationName: c.organization,
      organizationDomain: c.domain,
    },
    authorizePaidSearch: true,
    seedRegressionFixtures: false,
    maxQueries: 3,
    maxPages: 4,
    timeoutMs: 20000,
    googleTimeoutMs: 60000,
    googleAllowFollowUp: true,
    googleResolveRedirects: true,
  });
  spendEst += Number(disc.metrics?.searchCostUsd || 0);
  if (!(disc.metrics?.googleCostUsd > 0) && disc.metrics?.googleGroundedAttempted) {
    spendEst += 0.03 * Number(disc.metrics?.googleGeminiCalls || 1);
  }

  const grounded = disc.googleDiscovery || {};
  const email = disc.found?.email || disc.opportunity?.primaryContact?.email || null;
  const phone = disc.found?.phone || disc.opportunity?.primaryContact?.phone || null;
  const exts = disc.opportunity?.primaryContact?.phoneExtensions || [];
  const liUrl =
    disc.opportunity?.primaryContact?.linkedinUrl ||
    disc.linkedInDiscovery?.url ||
    null;

  const geminiEmail =
    grounded.candidates?.email || disc.metrics?.googleCandidates?.email || null;
  const geminiPhone =
    grounded.candidates?.phone || disc.metrics?.googleCandidates?.phone || null;
  const geminiUrls = grounded.sourceUrls || [];

  let failureStage = disc.metrics?.failureStage || null;
  if (
    c.consumerObserved.email &&
    geminiEmail &&
    String(geminiEmail).toLowerCase() === String(c.consumerObserved.email).toLowerCase() &&
    !email
  ) {
    failureStage =
      failureStage || GDI_PUBLIC_CONTACT_FAILURE_STAGE.SOURCE_EXTRACTION_FAILED;
  } else if (c.consumerObserved.email && !geminiEmail && !email) {
    failureStage =
      failureStage ||
      GDI_PUBLIC_CONTACT_FAILURE_STAGE.GOOGLE_DID_NOT_DISCOVER_SOURCE;
  }

  liveRows.push({
    caseId: c.id,
    personName: c.personName,
    organization: c.organization,
    opportunityId: c.opportunityId,
    GOOGLE_CONSUMER_OBSERVED: c.consumerObserved,
    GEMINI_GROUNDING_DISCOVERED: {
      email: geminiEmail,
      phone: geminiPhone,
      extensions: grounded.candidates?.phoneExtensions || [],
      linkedIn: grounded.linkedIn?.url || null,
      linkedInStatus: grounded.linkedIn?.status || null,
      sourceUrls: geminiUrls.slice(0, 8),
      webSearchQueries: grounded.webSearchQueries || [],
      followUpUsed: grounded.followUpUsed === true,
      redirectMap: (grounded.redirectMap || []).slice(0, 6),
      textPreview: grounded.textPreview || null,
    },
    EXA_USED: {
      queries: disc.metrics?.exaQueriesExecuted || 0,
      costUsd: disc.metrics?.exaCostUsd || 0,
      skipped: disc.metrics?.exaSkippedBecauseGoogleSufficient === true,
    },
    ORIGINAL_SOURCE_VERIFIED: Boolean(email || phone),
    GDI_CONTACT_RECOVERED: {
      email,
      phone,
      extensions: exts,
      linkedIn: liUrl,
      status: disc.status,
      sourceUrl:
        disc.opportunity?.primaryContact?.channelSourceUrl ||
        disc.found?.sourceUrl ||
        null,
    },
    AIRTABLE_PERSISTED: "NOT_RUN_IN_THIS_GATE",
    DRAWER_VISIBLE: "NOT_ASSESSED_IN_THIS_GATE",
    FAILURE_STAGE: failureStage,
    MATCH: {
      email:
        c.expectedEmail &&
        String(email || "").toLowerCase() === String(c.expectedEmail).toLowerCase(),
      extension: c.expectedExt
        ? exts.map(String).includes(String(c.expectedExt))
        : null,
    },
  });
  console.error(
    `[live] ${c.id}: geminiEmail=${geminiEmail || "—"} verifiedEmail=${email || "—"} stage=${failureStage || "NONE"}`,
  );
}

const liveReport = {
  generatedAt: new Date().toISOString(),
  parityClaimVsConsumerAiMode: "NOT_CLAIMED",
  spendEstimateUsd: Number(spendEst.toFixed(4)),
  cases: liveRows,
  summary: {
    consumerEmailsKnown: liveRows.filter((r) => r.GOOGLE_CONSUMER_OBSERVED.email)
      .length,
    geminiDiscoveredMatchingEmail: liveRows.filter(
      (r) =>
        r.GOOGLE_CONSUMER_OBSERVED.email &&
        r.GEMINI_GROUNDING_DISCOVERED.email &&
        String(r.GEMINI_GROUNDING_DISCOVERED.email).toLowerCase() ===
          String(r.GOOGLE_CONSUMER_OBSERVED.email).toLowerCase(),
    ).length,
    gdiVerifiedMatchingEmail: liveRows.filter((r) => r.MATCH.email).length,
    failureStages: liveRows.map((r) => ({
      id: r.caseId,
      stage: r.FAILURE_STAGE,
    })),
  },
};

writeFileSync(join(OUT, "LIVE_PARITY.json"), JSON.stringify(liveReport, null, 2));

const md = `# Google discovery parity benchmark

Parity with consumer Google AI Mode: **NOT CLAIMED**

| Case | Consumer email | Gemini email | GDI verified email | Failure stage |
|------|----------------|--------------|--------------------|---------------|
${liveRows
  .map(
    (r) =>
      `| ${r.caseId} | ${r.GOOGLE_CONSUMER_OBSERVED.email || "—"} | ${r.GEMINI_GROUNDING_DISCOVERED.email || "—"} | ${r.GDI_CONTACT_RECOVERED.email || "—"} | ${r.FAILURE_STAGE || "NONE"} |`,
  )
  .join("\n")}

Spend est: $${liveReport.spendEstimateUsd}
`;
writeFileSync(join(OUT, "FOUNDER_PARITY_TABLE.md"), md);
console.log(JSON.stringify(liveReport.summary, null, 2));
console.log(`Wrote ${join(OUT, "LIVE_PARITY.json")}`);
