#!/usr/bin/env node
/**
 * Hydrate remaining attribution-safe Findymail emails into Wyndham STAGED
 * (primary or secondary match only — never cross-person).
 *
 *   node scripts/gdi-findymail-hydrate-remaining-v1.mjs --dry-run
 *   node scripts/gdi-findymail-hydrate-remaining-v1.mjs --apply --auth FOUNDER_GO_GDI_FINDYMAIL_STAGE4_PERMANENT_E2E_20261127
 *   node scripts/gdi-findymail-hydrate-remaining-v1.mjs --apply --persist-airtable --auth FOUNDER_GO_GDI_FINDYMAIL_STAGE4_PERMANENT_E2E_20261127
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/gdi-publication-package-store-v1.js";
import { fingerprintPublicationContent } from "../lib/group-demand-intelligence/publication-package/gdi-customer-publication-package-v1.js";
import { emailLikelyMatchesPerson } from "../lib/group-demand-intelligence/workflow/gdi-findymail-stage4-enrichment-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recnPB3Q02NO8Ma1o";
const AUTH = "FOUNDER_GO_GDI_FINDYMAIL_STAGE4_PERMANENT_E2E_20261127";
const STORE_ROOT = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages",
);
const LIVE = path.join(
  ROOT,
  "reports/gdi-canonical-full-e2e-gold-standard-certification-v1/17-FINDYMAIL_REMAINING_LIVE.json",
);

function flag(n) {
  return process.argv.includes(n);
}
function arg(n, fb = null) {
  const i = process.argv.indexOf(n);
  return i >= 0 ? process.argv[i + 1] || fb : fb;
}
function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function loadEnv() {
  for (const envPath of [
    path.join(ROOT, ".env"),
    "C:/Dev/dealality/my-operators-backend/.env",
  ]) {
    if (!fs.existsSync(envPath)) continue;
    for (const line of fs.readFileSync(envPath, "utf8").split(/\r?\n/)) {
      const m = line.match(
        /^(FINDYMAIL_API_KEY|AIRTABLE_API_KEY|AIRTABLE_PAT|AIRTABLE_BASE_ID|AIRTABLE_GDI_BASE_ID)=(.*)$/,
      );
      if (!m) continue;
      if (!process.env[m[1]]) {
        process.env[m[1]] = m[2].replace(/^["']|["']$/g, "");
      }
    }
  }
}

function applyEmail(person, email, domain) {
  return {
    ...person,
    email,
    emailOwnership: "person_specific",
    emailVerificationStatus: "FINDYMAIL_PROVIDER_RETURNED",
    emailCustomerVerified: false,
    channels: [
      ...(person.channels || []),
      {
        channelType: "PROFESSIONAL_EMAIL",
        value: email,
        source: "FINDYMAIL",
        verificationStatus: "UNVERIFIED",
        verificationDate: new Date().toISOString(),
        personDirect: true,
        researchProvenance: { provider: "FINDYMAIL", domain },
      },
    ],
  };
}

loadEnv();
const store = createGdiPublicationPackageStore({ rootDir: STORE_ROOT });
const pointer = store.readPointer(HOTEL_ID, "STAGED");
const sourceVersion = pointer.publicationVersion || pointer.publicationId;
const loaded = store.loadPackage(HOTEL_ID, sourceVersion);
if (!loaded.ok) {
  console.error(loaded);
  process.exit(1);
}
const src = loaded.package;
const live = JSON.parse(fs.readFileSync(LIVE, "utf8"));
const found = (live.selected || []).filter(
  (r) =>
    r.outcome === "FOUND_VERIFICATION_REQUIRED" &&
    r.email &&
    r.attributionOk === true,
);

const byOpp = new Map();
for (const r of found) {
  const name = text(String(r.personKey || "").split("|")[0]);
  if (!emailLikelyMatchesPerson(r.email, name)) continue;
  byOpp.set(r.opportunityId, { ...r, name });
}

const changes = [];
const nextOpps = (src.opportunities || []).map((o) => {
  const hit = byOpp.get(o.id || o.opportunityId);
  if (!hit) return o;
  const primary = o.primaryContact || {};
  const secondary = o.secondaryContact || null;
  const pName = text(primary.name || primary.fullName).toLowerCase();
  const sName = text(secondary?.name || secondary?.fullName).toLowerCase();
  const target = hit.name.toLowerCase();

  if (pName === target && !text(primary.email)) {
    changes.push({
      opportunityId: o.id,
      role: "PRIMARY",
      name: hit.name,
      email: hit.email,
    });
    return {
      ...o,
      primaryContact: applyEmail(primary, hit.email, hit.domain),
      contactResearchStage: {
        ...(o.contactResearchStage || {}),
        findymailUsed: true,
        findymailPermanentComponent: true,
        findymailHydratedAt: new Date().toISOString(),
      },
      tileContactPreview: {
        ...(o.tileContactPreview || {}),
        email: hit.email,
      },
    };
  }
  if (secondary && sName === target && !text(secondary.email)) {
    changes.push({
      opportunityId: o.id,
      role: "SECONDARY",
      name: hit.name,
      email: hit.email,
    });
    const nextSec = applyEmail(secondary, hit.email, hit.domain);
    const alts = Array.isArray(o.alternativeEntryPoints)
      ? o.alternativeEntryPoints.map((a) =>
          text(a.name).toLowerCase() === target
            ? { ...a, email: hit.email }
            : a,
        )
      : o.alternativeEntryPoints;
    return {
      ...o,
      secondaryContact: nextSec,
      alternativeEntryPoints: alts,
      contactResearchStage: {
        ...(o.contactResearchStage || {}),
        findymailUsed: true,
        findymailPermanentComponent: true,
        findymailHydratedAt: new Date().toISOString(),
      },
    };
  }
  return o;
});

const shortFp = crypto
  .createHash("sha256")
  .update(JSON.stringify(changes))
  .digest("hex")
  .slice(0, 8);
const publicationVersion = `gdi_hotel_wide_fqo_${HOTEL_ID}_20261127_${shortFp}`;
const draft = {
  ...src,
  publicationVersion,
  publicationId: publicationVersion,
  createdAt: new Date().toISOString(),
  opportunities: nextOpps,
  opportunityCount: nextOpps.length,
  inventoryIds: nextOpps.map((o) => o.opportunityId || o.id),
  sortOrder: nextOpps.map((o) => o.opportunityId || o.id),
  gates: {
    ...(src.gates || {}),
    findymailRemainingHydration: {
      contract: "gdi-findymail-hydrate-remaining-v1",
      sourceVersion,
      hydrated: changes.length,
      creditsDebitedEstimate: live.creditsDebitedEstimate,
      authorizationId: AUTH,
    },
  },
};
const fp = fingerprintPublicationContent(draft);
const nextPkg = { ...draft, contentFingerprintSha256: fp.contentFingerprintSha256 };

const report = {
  dryRun: !flag("--apply"),
  sourceVersion,
  publicationVersion,
  hydrated: changes.length,
  primaryHydrated: changes.filter((c) => c.role === "PRIMARY").length,
  secondaryHydrated: changes.filter((c) => c.role === "SECONDARY").length,
  changes,
};
console.log(JSON.stringify(report, null, 2));

if (!flag("--apply")) process.exit(0);
if (arg("--auth") !== AUTH) {
  console.error(`--apply requires --auth ${AUTH}`);
  process.exit(2);
}

const staged = store.stageForProduction(nextPkg);
if (!staged.ok) {
  console.error(staged);
  process.exit(1);
}

let airtable = null;
if (flag("--persist-airtable")) {
  const { persistFindymailContactsToAirtable } = await import(
    pathToFileURL(
      path.join(
        ROOT,
        "lib/group-demand-intelligence/persistence/gdi-findymail-airtable-contact-persist-v1.js",
      ),
    ).href
  );
  const patches = changes
    .filter((c) => c.role === "PRIMARY")
    .map((c) => {
      const opp = nextOpps.find((o) => o.id === c.opportunityId);
      return {
        opportunity: { ...opp, hotelId: HOTEL_ID },
        enrichment: {
          person: opp.primaryContact,
          channel: { value: c.email, source: "FINDYMAIL" },
        },
      };
    });
  airtable = await persistFindymailContactsToAirtable(patches, {
    dryRun: false,
    apply: true,
    hotelId: HOTEL_ID,
    runId: `findymail_remaining_${Date.now()}`,
  });
}

fs.writeFileSync(
  path.join(
    ROOT,
    "reports/gdi-canonical-full-e2e-gold-standard-certification-v1/21-FINDYMAIL_REMAINING_HYDRATION.json",
  ),
  `${JSON.stringify(
    {
      ...report,
      airtableWritten: airtable?.written ?? null,
      stagedPointer: store.readPointer(HOTEL_ID, "STAGED"),
    },
    null,
    2,
  )}\n`,
);

console.log(
  JSON.stringify(
    {
      ok: true,
      publicationVersion,
      airtableWritten: airtable?.written ?? null,
      stagedPointer: store.readPointer(HOTEL_ID, "STAGED"),
    },
    null,
    2,
  ),
);
