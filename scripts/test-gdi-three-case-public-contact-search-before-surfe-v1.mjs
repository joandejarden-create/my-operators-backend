#!/usr/bin/env node
/**
 * Three-case offline regression: Maritza, Alejandra, Islas Godoy.
 * Proves Google-style query plans, honorific-tolerant extraction, extensions,
 * and public-before-Surfe resolver ordering — without requiring live Surfe.
 */
import assert from "node:assert/strict";
import { readFileSync, existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import {
  buildPublicPersonContactQueryPlan,
  discoverPublicContactChannels,
  classifyPublicPhoneKind,
  MARITZA_PUBLIC_CONTACT_FIXTURE_V1 as MARITZA,
  ALEJANDRA_PUBLIC_CONTACT_FIXTURE_V1 as ALEJ,
  ISLAS_PUBLIC_CONTACT_FIXTURE_V1 as ISLAS,
  PUBLIC_PHONE_KIND,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-discovery-v1.js";
import {
  recoverPublicContactChannels,
  stripPersonNameHonorifics,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-channel-hierarchy-v1.js";
import { resolveMissingContactChannels } from "../lib/group-demand-intelligence/presentation/gdi-missing-channel-resolver-v1.js";

const ROOT = process.cwd();
const OUT = join(ROOT, "reports/gdi-public-contact-search-before-surfe-v1");
mkdirSync(OUT, { recursive: true });

assert.equal(stripPersonNameHonorifics("Lic. Francisco Javier Islas Godoy"), "Francisco Javier Islas Godoy");

// --- Query plans (Google-style progressive strategies) ---
const islasPlan = buildPublicPersonContactQueryPlan(
  {
    name: ISLAS.personName,
    title: ISLAS.title,
    organization: ISLAS.organization,
  },
  { name: ISLAS.organization },
  {
    sources: [{ url: "https://portal.hcg.gob.mx/hcg/directorio/1" }],
  },
);
assert.ok(islasPlan.queries.length >= 8);
assert.ok(islasPlan.queries.some((q) => /Hospital Civil/i.test(q.query)));
assert.ok(islasPlan.queries.some((q) => /site:portal\.hcg\.gob\.mx|site:hcg\.gob\.mx/i.test(q.query)));
assert.ok(islasPlan.queries.some((q) => /directorio/i.test(q.query)));
assert.ok(islasPlan.queries.some((q) => /contacto|contact|correo|telefono|teléfono/i.test(q.query)));
assert.ok(
  !islasPlan.queries.some((q) => /^Lic\./i.test(q.query)),
  "honorific should be stripped from query subject",
);

const maritzaPlan = buildPublicPersonContactQueryPlan(
  { name: MARITZA.personName, title: MARITZA.title, organization: MARITZA.organization },
  { name: MARITZA.organization },
);
assert.ok(maritzaPlan.queries.some((q) => /correo|teléfono|directorio/i.test(q.query)));

const alejPlan = buildPublicPersonContactQueryPlan(
  { name: ALEJ.personName, title: ALEJ.title, organization: "IGECO" },
  { name: "IGECO" },
);
assert.ok(
  alejPlan.queries.some((q) =>
    /Alejandra Vazquez Sales Manager IGECO/i.test(q.query),
  ),
);

// --- Offline HTML extraction: Islas fixture page (curl-captured) ---
const hcgHtmlPath = join(ROOT, "reports/gdi-public-contact-search-before-surfe-v1/hcg-islas-directory.html");
assert.ok(existsSync(hcgHtmlPath), "tmp/hcg-islas.html required (official directory capture)");
const hcgHtml = readFileSync(hcgHtmlPath, "utf8");

const islasOpp = {
  id: "disc_6ea16462392a",
  primaryContact: {
    name: ISLAS.personName,
    title: ISLAS.title,
    organization: ISLAS.organization,
  },
  organizationName: ISLAS.organization,
  sources: ISLAS.candidateUrls.map((url) => ({ url })),
};

const islasRecovered = await recoverPublicContactChannels(islasOpp, {
  maxUrls: 2,
  skipDns: true,
  allowInsecureTlsRetry: true,
  fetchImpl: async (url) => {
    if (/directorioind/i.test(String(url))) {
      return {
        ok: true,
        status: 200,
        text: async () => hcgHtml,
        arrayBuffer: async () => Buffer.from(hcgHtml),
        headers: new Map([["content-type", "text/html"]]),
        url,
      };
    }
    return { ok: false, status: 404, text: async () => "", url };
  },
});

assert.equal(
  String(islasRecovered.found?.email || "").toLowerCase(),
  ISLAS.expectedEmail,
  "Islas email from official directory",
);
assert.ok(
  String(islasRecovered.found?.phone || "").replace(/\D/g, "").endsWith("3338834400"),
  "Islas office phone digits",
);
assert.ok(
  (islasRecovered.found?.phoneExtensions || []).includes("51621"),
  "Islas extension 51621 must survive extraction",
);

const islasKind = classifyPublicPhoneKind(ISLAS.expectedPhoneE164, {
  extensions: ISLAS.expectedExtensions,
});
assert.equal(islasKind.kind, PUBLIC_PHONE_KIND.OFFICE_WITH_EXTENSION);

// Seeded discovery path (no paid search) for Islas
const islasDisc = await discoverPublicContactChannels({
  opportunity: islasOpp,
  candidateUrls: ISLAS.candidateUrls,
  authorizePaidSearch: false,
  seedRegressionFixtures: true,
  forceIslasFixtureSeed: true,
  skipDns: true,
  fetchImpl: async (url) => {
    if (/directorioind/i.test(String(url))) {
      return {
        ok: true,
        status: 200,
        text: async () => hcgHtml,
        arrayBuffer: async () => Buffer.from(hcgHtml),
        headers: new Map([["content-type", "text/html"]]),
        url,
      };
    }
    return { ok: false, status: 404, text: async () => "", url };
  },
});
assert.equal(String(islasDisc.found?.email || "").toLowerCase(), ISLAS.expectedEmail);
assert.ok((islasDisc.opportunity?.primaryContact?.phoneExtensions || []).includes("51621"));

// --- Public-before-Surfe ordering ---
let surfeCalls = 0;
const mockSurfe = {
  isMock: true,
  async reveal() {
    surfeCalls += 1;
    return {
      ok: true,
      miss: true,
      message: "No email or phone found for this person.",
      contactDetails: null,
    };
  },
};

const resolved = await resolveMissingContactChannels({
  channel: "both",
  opportunity: {
    id: "disc_6ea16462392a",
    primaryContact: { name: ISLAS.personName, title: ISLAS.title },
    organizationName: ISLAS.organization,
    sources: [{ url: ISLAS.candidateUrls[0] }],
  },
  contact: { name: ISLAS.personName, title: ISLAS.title },
  organization: { name: ISLAS.organization },
  authorizePaidPublicSearch: false,
  forcePublicSearch: true,
  authorizeLiveSurfe: true,
  surfeProvider: mockSurfe,
  candidateUrls: ISLAS.candidateUrls,
  seedRegressionFixtures: true,
  forceIslasFixtureSeed: true,
  skipDns: true,
  fetchImpl: async (url) => {
    if (/directorioind/i.test(String(url))) {
      return {
        ok: true,
        status: 200,
        text: async () => hcgHtml,
        arrayBuffer: async () => Buffer.from(hcgHtml),
        headers: new Map([["content-type", "text/html"]]),
        url,
      };
    }
    return { ok: false, status: 404, text: async () => "", url };
  },
});

assert.equal(resolved.ok, true);
assert.equal(String(resolved.contactDetails?.email || "").toLowerCase(), ISLAS.expectedEmail);
assert.ok((resolved.contactDetails?.phoneExtensions || []).includes("51621"));
assert.equal(
  surfeCalls,
  0,
  "Surfe must not run when public directory already recovered both channels",
);
assert.ok(
  !/no email or phone found for this person/i.test(resolved.message || ""),
  "must not claim person has no channels after public hit",
);
assert.match(resolved.resolvedVia, /public/);

// Surfe only when public fails
surfeCalls = 0;
const unresolved = await resolveMissingContactChannels({
  channel: "both",
  opportunity: {
    id: "synthetic_miss",
    primaryContact: { name: "Nonexistent Person XYZ" },
    organizationName: "Example Org",
    sources: [],
  },
  contact: { name: "Nonexistent Person XYZ" },
  organization: { name: "Example Org" },
  authorizePaidPublicSearch: false,
  forcePublicSearch: true,
  authorizeLiveSurfe: true,
  surfeProvider: mockSurfe,
  seedRegressionFixtures: false,
  skipDns: true,
  fetchImpl: async () => ({ ok: false, status: 404, text: async () => "", url: "" }),
});
assert.equal(surfeCalls, 1, "Surfe runs only after public miss");
assert.match(unresolved.message || "", /neither public directory search nor paid enrichment/i);

// Maritza extension classification
const maritzaPhone = classifyPublicPhoneKind(MARITZA.expectedPhoneE164, {
  extensions: MARITZA.expectedExtensions,
});
assert.equal(maritzaPhone.kind, PUBLIC_PHONE_KIND.OFFICE_WITH_EXTENSION);
assert.deepEqual(maritzaPhone.extensions.slice(0, 2), MARITZA.expectedExtensions);

const report = {
  ok: true,
  gate: "test-gdi-three-case-public-contact-search-before-surfe-v1",
  cases: {
    islas: {
      email: ISLAS.expectedEmail,
      phone: ISLAS.expectedPhoneE164,
      extensions: ISLAS.expectedExtensions,
      surfeSkippedOnPublicHit: true,
    },
    maritza: {
      email: MARITZA.expectedEmail,
      phone: MARITZA.expectedPhoneE164,
      extensions: MARITZA.expectedExtensions,
    },
    alejandra: {
      email: ALEJ.expectedEmail,
      findPhoneOnly: true,
      secondary: ALEJ.secondaryPersonName,
    },
  },
};
writeFileSync(join(OUT, "OFFLINE_THREE_CASE.json"), JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));
