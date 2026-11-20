#!/usr/bin/env node
/**
 * Two-case Public Contact Discovery 2.0 certification (Maritza + Alejandra).
 *
 * Modes:
 *   --offline-seed     fixture URLs only (no paid search)
 *   --exa-search       authorize Exa search for Alejandra founder query
 *   --write-local      persist channels onto local opportunities.json
 *   --airtable-dry-run proposal only
 *
 * Never prints API keys. Never calls Surfe.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";
import dotenv from "dotenv";
import {
  discoverPublicContactChannels,
  MARITZA_PUBLIC_CONTACT_FIXTURE_V1 as MARITZA,
  ALEJANDRA_PUBLIC_CONTACT_FIXTURE_V1 as ALEJANDRA,
  PUBLIC_CONTACT_RESEARCH_STATUS,
  buildPublicPersonContactQueryPlan,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-discovery-v1.js";
import {
  createPublicContactSearchProvider,
  inspectPublicContactSearchCredentials,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-search-providers-v1.js";
import { buildContactChannelDisplayState } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-channel-hierarchy-v1.js";
import { proposeAirtablePublicContactUpsert } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-channel-hierarchy-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
dotenv.config({ path: path.join(ROOT, ".env"), override: false });
dotenv.config({
  path: "C:/Dev/dealality/my-operators-backend/.env",
  override: false,
});

const WRITE_LOCAL = process.argv.includes("--write-local");
const AIRTABLE_DRY = process.argv.includes("--airtable-dry-run");
const EXA_SEARCH = process.argv.includes("--exa-search");
const OFFLINE_SEED =
  process.argv.includes("--offline-seed") || !EXA_SEARCH;

const OUT_DIR = path.join(
  ROOT,
  "reports",
  "gdi-public-contact-discovery-2-v1",
);
const HOTEL_ID = "recCAGqkERSXgiku4";
const OPP_PATH = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL_ID,
  "opportunities.json",
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function phoneDigits(p) {
  return text(p).replace(/\D/g, "");
}
function loadDoc() {
  return JSON.parse(fs.readFileSync(OPP_PATH, "utf8"));
}

const creds = inspectPublicContactSearchCredentials();
const report = {
  suite: "gdi-two-case-public-contact-discovery-cert-v1",
  credentials: creds,
  chosenMode: creds.chosenMode,
  surfeCalls: 0,
  paidSearchAuthorized: EXA_SEARCH,
  offlineSeed: OFFLINE_SEED,
  cases: {},
  airtable: null,
  searchCostUsd: 0,
};

assert.ok(
  creds.exaConfigured || OFFLINE_SEED,
  "Exa required for --exa-search; offline-seed allowed without spend",
);

// --- Maritza (seed / reuse) ---
{
  const doc = loadDoc();
  const row = (doc.opportunities || []).find((o) => o.id === "disc_b4661c17bd92");
  assert.ok(row, "Maritza opportunity missing");
  const discovery = await discoverPublicContactChannels({
    opportunity: {
      ...row,
      primaryContact: {
        ...(row.primaryContact || {}),
        // Allow re-verify; keep email if present for reuse path
      },
    },
    candidateUrls: [...MARITZA.candidateUrls],
    authorizePaidSearch: false,
    seedRegressionFixtures: true,
    maxPages: 4,
    timeoutMs: 20000,
  });
  const d = discovery.display || buildContactChannelDisplayState(discovery.opportunity);
  const emailOk =
    text(d.publicDirectEmail).toLowerCase() === MARITZA.expectedEmail.toLowerCase();
  const phoneOk =
    phoneDigits(d.publicDirectPhone).endsWith("3338192333") ||
    phoneDigits(d.publicDirectPhone).endsWith("338192333");
  assert.ok(emailOk, `Maritza email fail: ${d.publicDirectEmail}`);
  assert.ok(phoneOk, `Maritza phone fail: ${d.publicDirectPhone}`);
  assert.equal(d.surfeCtaLabel, null);
  report.cases.maritza = {
    status: discovery.status,
    email: d.publicDirectEmail,
    phone: d.publicDirectPhone,
    extensions: discovery.opportunity?.primaryContact?.phoneExtensions || [],
    surfeCta: d.surfeCtaLabel,
    emailOk,
    phoneOk,
  };
}

// --- Alejandra ---
{
  const doc = loadDoc();
  const row = (doc.opportunities || []).find((o) => o.id === "paid3_0a6b19883953");
  assert.ok(row, "Ecomondo / Alejandra opportunity missing");

  // Strip channels so recovery is forced
  const stripped = {
    ...row,
    contactEmail: null,
    contactPhone: null,
    primaryContact: {
      ...(row.primaryContact || {}),
      name: ALEJANDRA.personName,
      title: ALEJANDRA.title,
      organization: "IGECO",
      email: null,
      phone: null,
      workEmail: null,
      directPhone: null,
    },
    secondaryContact: null,
  };

  const plan = buildPublicPersonContactQueryPlan(
    {
      name: ALEJANDRA.personName,
      title: ALEJANDRA.title,
      organization: "IGECO",
    },
    { name: "IGECO" },
  );
  assert.ok(
    plan.queries.some((q) =>
      /Alejandra Vazquez Sales Manager IGECO/i.test(q.query),
    ),
    "founder-style name+title+org query required",
  );

  let searchHit = null;
  if (EXA_SEARCH) {
    const provider = createPublicContactSearchProvider({ numResults: 5 });
    const searchQueries = [
      ALEJANDRA.founderSearchQuery,
      ALEJANDRA.founderSearchQueryWithContact,
      "Alejandra Vazquez site:ecomondomexico.com.mx",
    ];
    const attempts = [];
    let hit = false;
    let hitUrls = [];
    let providerUsed = null;
    for (const q of searchQueries) {
      const res = await provider.search(q);
      report.searchCostUsd += Number(res.costUsd || 0);
      providerUsed = res.provider || providerUsed;
      const urls = res.urls || [];
      const ok = urls.some((u) =>
        /ecomondomexico\.com\.mx\/(en\/)?contact/i.test(String(u)),
      );
      attempts.push({
        query: q,
        hit: ok,
        resultCount: urls.length,
        costUsd: res.costUsd || 0,
      });
      if (ok) {
        hit = true;
        hitUrls = urls;
        break;
      }
    }
    searchHit = {
      provider: providerUsed,
      hitContactPage: hit,
      attempts,
      topUrls: hitUrls.slice(0, 5),
    };
    assert.equal(
      searchHit.hitContactPage,
      true,
      "Exa search strategies must surface ecomondomexico.com.mx contact page",
    );
  }

  const discovery = await discoverPublicContactChannels({
    opportunity: stripped,
    candidateUrls: OFFLINE_SEED ? [...ALEJANDRA.candidateUrls] : [],
    authorizePaidSearch: EXA_SEARCH,
    seedRegressionFixtures: OFFLINE_SEED,
    forceAlejandraFixtureSeed: OFFLINE_SEED,
    maxQueries: EXA_SEARCH ? 3 : 0,
    maxPages: 4,
    timeoutMs: 20000,
  });
  report.searchCostUsd += Number(discovery.metrics?.searchCostUsd || 0);

  const d =
    discovery.display ||
    buildContactChannelDisplayState(discovery.opportunity);
  const emailOk =
    text(d.publicDirectEmail).toLowerCase() ===
    ALEJANDRA.expectedEmail.toLowerCase();
  assert.equal(emailOk, true, `Alejandra email: ${d.publicDirectEmail}`);
  assert.equal(
    d.publicDirectPhone,
    null,
    "Must not invent a phone for Alejandra",
  );
  assert.equal(d.surfeCtaLabel, "Find Phone");

  const secondary = discovery.opportunity?.secondaryContact || {};
  const peers = discovery.opportunity?.discoveredPeerContacts || [];
  const paulina =
    peers.find((p) => /paulina\s+s[aá]nchez/i.test(p.name || "")) ||
    (/paulina/i.test(secondary.name || "") ? secondary : null);
  const paulinaEmail = text(
    paulina?.email || paulina?.workEmail || "",
  ).toLowerCase();
  assert.ok(paulina, "Paulina Sánchez must be discovered as peer/secondary");
  assert.equal(
    paulinaEmail,
    ALEJANDRA.secondaryExpectedEmail.toLowerCase(),
  );

  report.cases.alejandra = {
    status: discovery.status,
    email: d.publicDirectEmail,
    phone: d.publicDirectPhone,
    surfeCta: d.surfeCtaLabel,
    emailOk,
    phoneInvented: Boolean(d.publicDirectPhone),
    searchHit,
    secondary: {
      name: paulina?.name || null,
      email: paulinaEmail || null,
    },
    metrics: discovery.metrics || null,
  };

  if (AIRTABLE_DRY) {
    report.airtable = {
      mode: "dry-run",
      alejandra: proposeAirtablePublicContactUpsert(discovery.opportunity, d),
      note: "Dry-run only — Surfe-derived never proposed",
    };
  }

  if (WRITE_LOCAL) {
    const next = loadDoc();
    const idx = next.opportunities.findIndex((o) => o.id === row.id);
    assert.ok(idx >= 0);
    next.opportunities[idx] = discovery.opportunity;
    // Also refresh Maritza rows if discovery changed them earlier in other runs
    next.updatedAt = new Date().toISOString();
    fs.writeFileSync(OPP_PATH, `${JSON.stringify(next, null, 2)}\n`, "utf8");
    report.localWrite = { path: OPP_PATH, opportunityId: row.id };
  }
}

fs.mkdirSync(OUT_DIR, { recursive: true });
report.passed = true;
report.surfeLookupsAvoided =
  (report.cases.maritza?.surfeCta == null ? 1 : 0) +
  (report.cases.alejandra?.emailOk ? 1 : 0);
fs.writeFileSync(
  path.join(OUT_DIR, "TWO_CASE_CERT.json"),
  `${JSON.stringify(report, null, 2)}\n`,
  "utf8",
);
fs.writeFileSync(
  path.join(OUT_DIR, "TWO_CASE_GATE.md"),
  `# Public Contact Discovery 2.0 — Two-case gate

## Provider mode
- Chosen: **${report.chosenMode}**
- Exa configured: ${creds.exaConfigured}
- Serper configured: ${creds.serperConfigured}
- Search cost (this cert): **$${Number(report.searchCostUsd || 0).toFixed(4)}**
- Surfe calls: **0**

## Maritza
- Email: \`${report.cases.maritza.email}\`
- Phone: \`${report.cases.maritza.phone}\`
- Surfe CTA: none

## Alejandra Vázquez / Ecomondo
- Email: \`${report.cases.alejandra.email}\`
- Phone: none (not invented)
- Surfe CTA: Find Phone
- Secondary: ${report.cases.alejandra.secondary.name} <${report.cases.alejandra.secondary.email}>
- Search hit contact page: ${report.cases.alejandra.searchHit?.hitContactPage ?? "n/a (offline-seed)"}

## STOP
Founder review required before database-wide backfill.
`,
  "utf8",
);

console.log(JSON.stringify(report, null, 2));
