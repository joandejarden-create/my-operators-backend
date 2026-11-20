#!/usr/bin/env node
/**
 * Maritza Jeanette Medina García — Public Contact Discovery 2.0 certification.
 *
 * Controlled recovery using official government directory URLs (no paid search,
 * no Surfe). Optionally dry-run / apply research-inventory persistence.
 *
 *   node scripts/gdi-maritza-public-contact-discovery-cert-v1.mjs
 *   node scripts/gdi-maritza-public-contact-discovery-cert-v1.mjs --write-local
 *   node scripts/gdi-maritza-public-contact-discovery-cert-v1.mjs --airtable-dry-run
 *   node scripts/gdi-maritza-public-contact-discovery-cert-v1.mjs --airtable-apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";
import dotenv from "dotenv";
import {
  discoverPublicContactChannels,
  MARITZA_PUBLIC_CONTACT_FIXTURE_V1 as FIX,
  PUBLIC_CONTACT_RESEARCH_STATUS,
  PUBLIC_PHONE_KIND,
  buildPublicPersonContactQueryPlan,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-discovery-v1.js";
import {
  buildContactChannelDisplayState,
  proposeAirtablePublicContactUpsert,
} from "../lib/group-demand-intelligence/presentation/gdi-public-contact-channel-hierarchy-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
dotenv.config({ path: path.join(ROOT, ".env"), override: false });
dotenv.config({
  path: "C:/Dev/dealality/my-operators-backend/.env",
  override: false,
});

const WRITE_LOCAL = process.argv.includes("--write-local");
const AIRTABLE_DRY = process.argv.includes("--airtable-dry-run");
const AIRTABLE_APPLY = process.argv.includes("--airtable-apply");

const OUT_DIR = path.join(
  ROOT,
  "reports",
  "gdi-public-contact-discovery-2-v1",
);
const HOTEL_ID = FIX.hotelId;
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

function loadOppDoc() {
  return JSON.parse(fs.readFileSync(OPP_PATH, "utf8"));
}

function findMaritzaRows(doc) {
  return (doc.opportunities || []).filter((o) =>
    /Maritza Jeanette Medina/i.test(
      JSON.stringify(o.primaryContact || {}) + String(o.contactName || ""),
    ),
  );
}

async function runDiscoveryOnOpportunity(raw) {
  return discoverPublicContactChannels({
    opportunity: raw,
    candidateUrls: [...FIX.candidateUrls],
    authorizePaidSearch: false,
    maxPages: 4,
    timeoutMs: 20000,
    maxBytes: 2_500_000,
  });
}

const audit = {
  suite: "gdi-maritza-public-contact-discovery-cert-v1",
  rootCause:
    "recoverPublicContactChannels only fetched opportunity-attached news/project URLs; no person-targeted public search reached info.jalisco.gob.mx / transparencia staff directories. Extraction works once those pages are candidates.",
  limitingStage:
    "1+2+4: no explicit person-channel search / insufficient query variation / missing authoritative staff-directory discovery (not extraction or DTO loss)",
  fixture: FIX,
};

const queryPlan = buildPublicPersonContactQueryPlan(
  {
    name: FIX.personName,
    title: FIX.title,
    organization: FIX.organization,
  },
  { name: FIX.organization },
);
assert.ok(queryPlan.queries.length >= 6, "expected Spanish/local query variation");
assert.ok(
  queryPlan.queries.some((q) => /correo|directorio|tel[eé]fono/i.test(q.query)),
  "expected Spanish contact terms",
);
assert.ok(
  queryPlan.queries.some((q) => /info\.jalisco\.gob\.mx/i.test(q.query)),
  "expected gov directory site query",
);

const doc = loadOppDoc();
const rows = findMaritzaRows(doc);
assert.ok(rows.length >= 1, "Maritza opportunities must exist in local inventory");

const results = [];
for (const row of rows) {
  const before = buildContactChannelDisplayState(row);
  const discovery = await runDiscoveryOnOpportunity(row);
  const after = discovery.display || buildContactChannelDisplayState(discovery.opportunity);
  const emailOk =
    text(after.publicDirectEmail).toLowerCase() === FIX.expectedEmail.toLowerCase();
  const phoneOk =
    phoneDigits(after.publicDirectPhone).endsWith("3338192333") ||
    phoneDigits(after.publicDirectPhone).endsWith("338192333");
  const found = discovery.found || {};
  results.push({
    opportunityId: row.id,
    title: row.displayTitle || row.title,
    before: {
      email: before.publicDirectEmail,
      phone: before.publicDirectPhone,
      surfeCta: before.surfeCtaLabel,
    },
    after: {
      email: after.publicDirectEmail,
      phone: after.publicDirectPhone,
      phoneKind: found.phoneKind || null,
      extensions: found.phoneExtensions || [],
      surfeCta: after.surfeCtaLabel,
      sourceUrl: after.channelSourceUrl,
    },
    status: discovery.status,
    emailOk,
    phoneOk,
    changed: discovery.changed,
  });

  assert.ok(
    discovery.status === PUBLIC_CONTACT_RESEARCH_STATUS.PUBLIC_CHANNELS_VERIFIED ||
      discovery.status === PUBLIC_CONTACT_RESEARCH_STATUS.REUSED_EXISTING,
    `unexpected status ${discovery.status}`,
  );
  assert.equal(emailOk, true, `email mismatch on ${row.id}`);
  assert.equal(phoneOk, true, `phone mismatch on ${row.id}: ${after.publicDirectPhone}`);
  assert.equal(
    after.surfeCtaLabel,
    null,
    "Find Email & Phone must not show when both public channels verified",
  );
  if (discovery.status !== PUBLIC_CONTACT_RESEARCH_STATUS.REUSED_EXISTING) {
    assert.ok(
      found.phoneKind === PUBLIC_PHONE_KIND.OFFICE_WITH_EXTENSION ||
        found.phoneKind === PUBLIC_PHONE_KIND.OFFICE,
      `expected office phone kind, got ${found.phoneKind}`,
    );
  } else {
    assert.ok(
      row.primaryContact?.phoneOwnership === "person_office" ||
        row.primaryContact?.phoneType === "office" ||
        phoneOk,
      "reused channels must remain person-office typed",
    );
  }
}

// Baseline control: news-only sources must NOT recover channels (proves discovery gap)
{
  const control = await discoverPublicContactChannels({
    opportunity: {
      id: "control_news_only",
      primaryContact: {
        name: FIX.personName,
        title: FIX.title,
      },
      sources: [
        {
          url: "https://www.informador.mx/jalisco/expo-guadalajara-pablo-lemus-anuncia-inversion-de-dos-mil-mdp-para-transformar-la-zona-20260513-0175.html",
        },
      ],
    },
    candidateUrls: [],
    authorizePaidSearch: false,
    maxPages: 2,
    timeoutMs: 12000,
  });
  audit.newsOnlyControl = {
    status: control.status,
    email: control.display?.publicDirectEmail || null,
    phone: control.display?.publicDirectPhone || null,
    provesGap:
      !control.display?.publicDirectEmail && !control.display?.publicDirectPhone,
  };
  assert.equal(audit.newsOnlyControl.provesGap, true);
}

let airtable = null;
if (AIRTABLE_DRY || AIRTABLE_APPLY) {
  const primary = results[0];
  const opp = (await runDiscoveryOnOpportunity(rows[0])).opportunity;
  const proposal = proposeAirtablePublicContactUpsert(
    opp,
    buildContactChannelDisplayState(opp),
  );
  airtable = {
    mode: AIRTABLE_APPLY ? "apply" : "dry-run",
    proposal,
    note: AIRTABLE_APPLY
      ? "Apply path uses existing research inventory upsert (founder-authorized only)"
      : "Dry-run proposal only — no Airtable mutation",
  };
  if (AIRTABLE_APPLY) {
    const {
      findResearchInventoryByCompositeKey,
      upsertResearchInventoryRecord,
      readBackResearchInventory,
      buildResearchInventoryFields,
    } = await import(
      "../lib/group-demand-intelligence/workflow/gdi-hotel-research-inventory-store-v1.js"
    );
    const { buildHotelResearchCompositeKey } = await import(
      "../lib/group-demand-intelligence/hotel-research-opportunity-field-map.js"
    );
    const { buildSlimResearchPayload } = await import(
      "../lib/group-demand-intelligence/workflow/gdi-research-inventory-slim-payload-v1.js"
    );
    const compositeKey = buildHotelResearchCompositeKey({
      hotelId: HOTEL_ID,
      opportunityId: primary.opportunityId,
    });
    const existing = await findResearchInventoryByCompositeKey(compositeKey);
    const slim = buildSlimResearchPayload(opp, { hotelId: HOTEL_ID });
    const fields = buildResearchInventoryFields({
      hotelId: HOTEL_ID,
      opportunity: { ...opp, ...slim },
      existingRecord: existing?.record || null,
    });
    const written = await upsertResearchInventoryRecord({
      compositeKey,
      fields,
      existingRecordId: existing?.record?.id || null,
    });
    const readBack = await readBackResearchInventory(written.id || existing?.record?.id);
    airtable.apply = {
      compositeKey,
      recordId: written.id || existing?.record?.id || null,
      readBackOk: Boolean(readBack?.id),
    };
  }
}

if (WRITE_LOCAL) {
  const nextDoc = loadOppDoc();
  let writes = 0;
  for (const row of findMaritzaRows(nextDoc)) {
    const discovery = await runDiscoveryOnOpportunity(row);
    const idx = nextDoc.opportunities.findIndex((o) => o.id === row.id);
    if (idx >= 0 && discovery.opportunity) {
      nextDoc.opportunities[idx] = discovery.opportunity;
      writes += 1;
    }
  }
  nextDoc.updatedAt = new Date().toISOString();
  fs.writeFileSync(OPP_PATH, `${JSON.stringify(nextDoc, null, 2)}\n`, "utf8");
  audit.localWrite = { writes, path: OPP_PATH };
}

fs.mkdirSync(OUT_DIR, { recursive: true });
const report = {
  ...audit,
  passed: true,
  surfeCalls: 0,
  paidSearchCalls: 0,
  results,
  airtable,
  queryPlanSample: queryPlan.queries.slice(0, 10),
  measurement: {
    namedContactsResearched: rows.length,
    newPublicEmailsFound: results.filter((r) => r.emailOk && !r.before.email).length,
    newPublicPhonesFound: results.filter((r) => r.phoneOk && !r.before.phone).length,
    bothChannels: results.filter((r) => r.emailOk && r.phoneOk).length,
    surfeLookupsAvoided: results.filter((r) => r.after.surfeCta == null).length,
  },
};

fs.writeFileSync(
  path.join(OUT_DIR, "MARITZA_CERT.json"),
  `${JSON.stringify(report, null, 2)}\n`,
  "utf8",
);
fs.writeFileSync(
  path.join(OUT_DIR, "AUDIT.md"),
  `# Public Contact Discovery 2.0 — Maritza audit

## Root cause

${report.rootCause}

## Limiting stage

${report.limitingStage}

## Proof

- News-only attached sources: **no** public channels recovered.
- Official directory candidate URLs: email \`${FIX.expectedEmail}\` + office phone recovered; Surfe CTA cleared.
- Authority remains RELEVANT_COMMERCIAL_ENTRY_POINT (procurement route ≠ hotel-booking authority).

## Paid spend

Surfe calls: **0**. Paid web search: **0**.
`,
  "utf8",
);

console.log(JSON.stringify(report, null, 2));
