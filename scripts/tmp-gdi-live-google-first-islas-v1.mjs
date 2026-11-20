#!/usr/bin/env node
/**
 * Bounded live certification: Google-first Islas discovery (1 Gemini call).
 * Loads GEMINI_API_KEY from Integration .env without printing secrets.
 * Does NOT call Surfe. Does NOT apply Airtable.
 *
 * Usage: node scripts/tmp-gdi-live-google-first-islas-v1.mjs
 * Optional: GDI_LIVE_GOOGLE_THREE_CASE=1 for Maritza + Alejandra too (max 3 Gemini).
 */
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "node:fs";
import { join } from "node:path";
import { discoverPublicContactChannels, ISLAS_PUBLIC_CONTACT_FIXTURE_V1 as ISLAS, MARITZA_PUBLIC_CONTACT_FIXTURE_V1 as MARITZA, ALEJANDRA_PUBLIC_CONTACT_FIXTURE_V1 as ALEJ } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-discovery-v1.js";
import { inspectPublicContactSearchCredentials } from "../lib/group-demand-intelligence/presentation/gdi-public-contact-search-providers-v1.js";
import { inspectGoogleGroundedContactCredentials } from "../lib/group-demand-intelligence/presentation/gdi-google-grounded-contact-discovery-v1.js";

function loadEnvFile(path) {
  if (!existsSync(path)) return {};
  const out = {};
  for (const line of readFileSync(path, "utf8").split(/\r?\n/)) {
    if (!line || line.trim().startsWith("#")) continue;
    const i = line.indexOf("=");
    if (i < 1) continue;
    const k = line.slice(0, i).trim();
    let v = line.slice(i + 1).trim();
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
      v = v.slice(1, -1);
    }
    out[k] = v;
  }
  return out;
}

const envPath =
  process.env.GDI_ENV_FILE ||
  "C:/Dev/dealality/my-operators-backend/.env";
const fileEnv = loadEnvFile(envPath);
const env = { ...process.env, ...fileEnv };

const OUT = join(process.cwd(), "reports/gdi-google-first-contact-discovery-v1");
mkdirSync(OUT, { recursive: true });

const creds = inspectPublicContactSearchCredentials(env);
const gCreds = inspectGoogleGroundedContactCredentials(env);
console.log(
  JSON.stringify(
    {
      envFilePresent: existsSync(envPath),
      googleReady: gCreds.geminiConfigured,
      mode: creds.chosenMode,
      providerOrder: creds.providerOrder,
      // never print key values
      geminiEnvVar: gCreds.envVarPreferred,
      exaConfigured: creds.exaConfigured,
    },
    null,
    2,
  ),
);

if (!gCreds.geminiConfigured) {
  console.error("GEMINI_NOT_CONFIGURED — Exa-only fallback would apply in production");
  process.exit(2);
}

async function runCase(label, fixture, extra = {}) {
  const started = Date.now();
  const result = await discoverPublicContactChannels({
    opportunity: {
      id: fixture.opportunityIds?.[0] || `live_${label}`,
      primaryContact: {
        name: fixture.personName,
        title: fixture.title,
        organization: fixture.organization,
      },
      organizationName: fixture.organization,
      organizationDomain: extra.domain || null,
      sources: extra.seedSources || [],
    },
    authorizePaidSearch: true,
    seedRegressionFixtures: false,
    skipExaFallback: extra.skipExa === true,
    maxQueries: Number(extra.maxQueries || 4),
    maxPages: 3,
    timeoutMs: 20000,
    googleTimeoutMs: 60000,
    env,
    allowInsecureTlsRetry: true,
  });
  return {
    label,
    ok: result.ok,
    status: result.status,
    email: result.found?.email || result.opportunity?.primaryContact?.email || null,
    phone: result.found?.phone || result.opportunity?.primaryContact?.phone || null,
    phoneExtensions:
      result.opportunity?.primaryContact?.phoneExtensions ||
      result.found?.phoneExtensions ||
      [],
    linkedInStatus: result.linkedInDiscovery?.status || result.opportunity?.primaryContact?.linkedInMatchStatus || null,
    linkedInUrl:
      result.opportunity?.primaryContact?.linkedinUrl ||
      result.linkedInDiscovery?.url ||
      null,
    metrics: result.metrics,
    google: result.googleDiscovery
      ? {
          ok: result.googleDiscovery.ok,
          reason: result.googleDiscovery.reason,
          groundingUsed: result.googleDiscovery.groundingUsed,
          model: result.googleDiscovery.model,
          sourceUrlCount: result.googleDiscovery.sourceUrls?.length || 0,
          costUsd: result.googleDiscovery.costUsd,
          persistenceAuthority: result.googleDiscovery.persistenceAuthority,
        }
      : null,
    elapsedMs: Date.now() - started,
  };
}

const cases = [];
cases.push(
  await runCase("islas", ISLAS, {
    domain: "hcg.gob.mx",
    // Prefer Google discovery; allow Exa if Google incomplete
    skipExa: false,
    maxQueries: 3,
  }),
);

if (process.env.GDI_LIVE_GOOGLE_THREE_CASE === "1") {
  cases.push(
    await runCase("maritza", MARITZA, {
      domain: "jalisco.gob.mx",
      maxQueries: 3,
    }),
  );
  cases.push(
    await runCase("alejandra", ALEJ, {
      domain: "igeco.mx",
      maxQueries: 3,
    }),
  );
}

const report = {
  capturedAt: new Date().toISOString(),
  mode: creds.chosenMode,
  providerOrder: ["saved", "google_gemini", "exa", "source_verify", "surfe_last"],
  cases,
  totals: {
    googleCostUsd: cases.reduce((s, c) => s + Number(c.metrics?.googleCostUsd || 0), 0),
    exaCostUsd: cases.reduce((s, c) => s + Number(c.metrics?.exaCostUsd || 0), 0),
    exaQueries: cases.reduce((s, c) => s + Number(c.metrics?.exaQueriesExecuted || 0), 0),
    googleOk: cases.filter((c) => c.google?.ok).length,
    emails: cases.filter((c) => c.email).length,
    phones: cases.filter((c) => c.phone).length,
  },
  parityWithConsumerGoogleAiMode: "NOT_CLAIMED — official Gemini grounding only",
};

writeFileSync(join(OUT, "LIVE_GOOGLE_FIRST.json"), JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));

const islas = cases[0];
const pass =
  String(islas.email || "").toLowerCase() === ISLAS.expectedEmail &&
  (islas.phoneExtensions || []).includes("51621");
process.exit(pass ? 0 : 1);
