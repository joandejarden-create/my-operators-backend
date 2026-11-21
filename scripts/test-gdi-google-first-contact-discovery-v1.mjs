#!/usr/bin/env node
/**
 * Offline gate: Google Gemini grounding FIRST, Exa SECOND, Surfe LAST.
 * Proves provider order, LinkedIn classification, and Exa skip when Google
 * already seeded an authoritative directory URL that verifies channels.
 */
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import {
  classifyLinkedInProfileMatch,
  parseGoogleGroundedContactCandidates,
  buildGoogleGroundedContactPrompt,
  LINKEDIN_PROFILE_MATCH,
  inspectGoogleGroundedContactCredentials,
} from "../lib/group-demand-intelligence/presentation/gdi-google-grounded-contact-discovery-v1.js";
import {
  discoverPublicContactChannels,
  buildPublicPersonContactQueryPlan,
  ISLAS_PUBLIC_CONTACT_FIXTURE_V1 as ISLAS,
  ALEJANDRA_PUBLIC_CONTACT_FIXTURE_V1 as ALEJ,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-discovery-v1.js";
import { inspectPublicContactSearchCredentials } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-search-providers-v1.js";
import { resolveMissingContactChannels } from "../lib/group-demand-intelligence/presentation/gdi-missing-channel-resolver-v1.js";

const OUT = join(process.cwd(), "reports/gdi-google-first-contact-discovery-v1");
mkdirSync(OUT, { recursive: true });

// --- LinkedIn classification ---
const verifiedLi = classifyLinkedInProfileMatch({
  url: "https://www.linkedin.com/in/alejandra-vazquez-igeco",
  personName: "Alejandra Vázquez",
  organization: "IGECO México",
  title: "Sales Manager",
  evidenceText: "Alejandra Vazquez Sales Manager at IGECO Mexico Ecomondo",
});
assert.equal(verifiedLi.status, LINKEDIN_PROFILE_MATCH.VERIFIED_LINKEDIN_PROFILE);

const uncertainLi = classifyLinkedInProfileMatch({
  url: "https://www.linkedin.com/in/random-person-12345",
  personName: "Alejandra Vázquez",
  organization: "IGECO",
  evidenceText: "unrelated profile",
});
assert.ok(
  [
    LINKEDIN_PROFILE_MATCH.PROFILE_MATCH_UNCERTAIN,
    LINKEDIN_PROFILE_MATCH.NO_PROFILE_FOUND,
    LINKEDIN_PROFILE_MATCH.CANDIDATE_LINKEDIN_PROFILE,
  ].includes(uncertainLi.status),
);

const noLi = classifyLinkedInProfileMatch({ url: null, personName: "X" });
assert.equal(noLi.status, LINKEDIN_PROFILE_MATCH.NO_PROFILE_FOUND);

// --- Prompt + candidate parse (leads only) ---
const prompt = buildGoogleGroundedContactPrompt(
  { name: ISLAS.personName, title: ISLAS.title },
  { name: ISLAS.organization, domain: "hcg.gob.mx" },
);
assert.ok(/Francisco Javier Islas/i.test(prompt.text));
assert.ok(/LinkedIn/i.test(prompt.text));

const parsed = parseGoogleGroundedContactCandidates(
  `EMAIL: fislas@hcg.gob.mx
PHONE: +52 33 3883 4400
EXTENSIONS: 51621
LINKEDIN: https://www.linkedin.com/in/not-verified
SOURCES: https://portal.hcg.gob.mx/hcg/directorioind/lic-francisco%20%20javier%20-godoy%20-islas`,
  {
    searchResults: [
      {
        url: "https://portal.hcg.gob.mx/hcg/directorioind/lic-francisco%20%20javier%20-godoy%20-islas",
      },
    ],
    citations: [],
    searchMetadata: { webSearchQueries: ["Francisco Islas HCG"] },
  },
);
assert.equal(parsed.candidateEmail, "fislas@hcg.gob.mx");
assert.ok(parsed.sourceUrls.some((u) => /directorioind/i.test(u)));

// --- Credential mode prefers Google when present ---
const mode = inspectPublicContactSearchCredentials({
  GEMINI_API_KEY: "x",
  EXA_API_KEY: "y",
});
assert.equal(mode.chosenMode, "GOOGLE_GEMINI_THEN_EXA");
assert.deepEqual(mode.providerOrder[0], "gemini_google_search");

assert.equal(
  inspectGoogleGroundedContactCredentials({}).geminiConfigured,
  false,
);

// --- Query plan includes LinkedIn strategies ---
const plan = buildPublicPersonContactQueryPlan(
  { name: ALEJ.personName, title: ALEJ.title, organization: "IGECO" },
  { name: "IGECO" },
);
assert.ok(plan.queries.some((q) => /LinkedIn|linkedin\.com\/in/i.test(q.query)));

// --- Google-first discovery: mock Gemini yields HCG URL; Exa must not run ---
const hcgHtmlPath = join(
  process.cwd(),
  "reports/gdi-public-contact-search-before-surfe-v1/hcg-islas-directory.html",
);
assert.ok(existsSync(hcgHtmlPath), "Islas HTML fixture required");
const hcgHtml = readFileSync(hcgHtmlPath, "utf8");

let geminiCalls = 0;
let exaCalls = 0;
const mockFetch = async (url, init = {}) => {
  const u = String(url);
  // Gemini generateContent
  if (/generativelanguage\.googleapis\.com/i.test(u)) {
    geminiCalls += 1;
    return {
      ok: true,
      status: 200,
      json: async () => ({
        candidates: [
          {
            content: {
              parts: [
                {
                  text: `EMAIL: fislas@hcg.gob.mx
PHONE: +52 33 3883 4400
EXTENSIONS: 51621
SOURCES: ${ISLAS.expectedSearchHitUrl}`,
                },
              ],
            },
            groundingMetadata: {
              webSearchQueries: ["Francisco Javier Islas Godoy Hospital Civil"],
              groundingChunks: [
                { web: { uri: ISLAS.expectedSearchHitUrl, title: "Directorio HCG" } },
              ],
              groundingSupports: [],
            },
          },
        ],
        usageMetadata: {
          promptTokenCount: 200,
          candidatesTokenCount: 80,
          totalTokenCount: 280,
        },
      }),
      text: async () => "",
    };
  }
  // Exa
  if (/api\.exa\.ai/i.test(u)) {
    exaCalls += 1;
    return {
      ok: true,
      status: 200,
      json: async () => ({ results: [], costDollars: { total: 0.01 } }),
      text: async () => "",
    };
  }
  // Official directory
  if (/directorioind/i.test(u)) {
    return {
      ok: true,
      status: 200,
      text: async () => hcgHtml,
      arrayBuffer: async () => Buffer.from(hcgHtml),
      headers: new Map([["content-type", "text/html"]]),
      url: u,
    };
  }
  return { ok: false, status: 404, text: async () => "", json: async () => ({}), url: u };
};

const disc = await discoverPublicContactChannels({
  opportunity: {
    id: "disc_6ea16462392a",
    primaryContact: { name: ISLAS.personName, title: ISLAS.title },
    organizationName: ISLAS.organization,
  },
  authorizePaidSearch: true,
  seedRegressionFixtures: false,
  skipDns: true,
  env: { GEMINI_API_KEY: "test-key", EXA_API_KEY: "test-exa" },
  fetchImpl: mockFetch,
  maxQueries: 4,
  maxPages: 2,
});

assert.equal(geminiCalls, 1, "exactly one Google grounded call");
assert.equal(exaCalls, 0, "Exa skipped when Google URL verified channels");
assert.equal(disc.metrics.googleGroundedAttempted, true);
assert.equal(disc.metrics.exaSkippedBecauseGoogleSufficient, true);
assert.equal(String(disc.found?.email || "").toLowerCase(), ISLAS.expectedEmail);
assert.ok((disc.opportunity?.primaryContact?.phoneExtensions || []).includes("51621"));
assert.ok(disc.metrics.providerOrder[1] === "google_gemini");

// --- Resolver still Surfe-last ---
let surfeCalls = 0;
const resolved = await resolveMissingContactChannels({
  channel: "both",
  opportunity: {
    id: "disc_6ea16462392a",
    primaryContact: { name: ISLAS.personName, title: ISLAS.title },
    organizationName: ISLAS.organization,
  },
  contact: { name: ISLAS.personName, title: ISLAS.title },
  organization: { name: ISLAS.organization },
  authorizePaidPublicSearch: true,
  authorizeLiveSurfe: true,
  surfeProvider: {
    isMock: true,
    async reveal() {
      surfeCalls += 1;
      return { ok: true, miss: true, contactDetails: null };
    },
  },
  seedRegressionFixtures: false,
  skipDns: true,
  env: { GEMINI_API_KEY: "test-key", EXA_API_KEY: "test-exa" },
  fetchImpl: mockFetch,
});
assert.equal(resolved.ok, true);
assert.equal(surfeCalls, 0, "Surfe not called after Google-verified channels");

const report = {
  ok: true,
  googleFirst: true,
  exaSecond: true,
  surfeLast: true,
  geminiCalls,
  exaCalls,
  surfeCalls,
  islasEmail: disc.found?.email || null,
  islasExtensions: disc.opportunity?.primaryContact?.phoneExtensions || [],
  linkedInVerifiedExample: verifiedLi.status,
};
writeFileSync(join(OUT, "OFFLINE_GATE.json"), JSON.stringify(report, null, 2));
console.log("PASS test-gdi-google-first-contact-discovery-v1");
console.log(JSON.stringify(report, null, 2));
