#!/usr/bin/env node
/**
 * Hydrate attribution-safe Findymail emails into Wyndham STAGED via publication store.
 *
 *   node scripts/gdi-findymail-wyndham-hydrate-staged-v1.mjs --dry-run
 *   node scripts/gdi-findymail-wyndham-hydrate-staged-v1.mjs --apply --auth FOUNDER_GO_GDI_FINDYMAIL_STAGE4_PERMANENT_E2E_20261127
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
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

const store = createGdiPublicationPackageStore({ rootDir: STORE_ROOT });
const stagedPointer = store.readPointer(HOTEL_ID, "STAGED");
const sourceVersion =
  stagedPointer?.publicationVersion ||
  stagedPointer?.publicationId ||
  "gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_46de4104";
const loaded = store.loadPackage(HOTEL_ID, sourceVersion);
if (!loaded.ok) {
  console.error(loaded);
  process.exit(1);
}
const src = loaded.package;

const found = [];
const seen = new Set();
const reportDir = path.join(
  ROOT,
  "reports/gdi-canonical-full-e2e-gold-standard-certification-v1",
);
for (const f of fs.readdirSync(reportDir)) {
  if (!/^08-FINDYMAIL_EXECUTION.*\.json$/.test(f)) continue;
  const execution = JSON.parse(fs.readFileSync(path.join(reportDir, f), "utf8"));
  for (const r of execution.selected || []) {
    if (r.outcome !== "FOUND_VERIFICATION_REQUIRED" || !r.email) continue;
    const name = text(String(r.personKey || "").split("|")[0]);
    if (!emailLikelyMatchesPerson(r.email, name)) continue;
    const key = name.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    found.push({ name, email: r.email, domain: r.domain, personKey: r.personKey });
  }
}
const byName = new Map(found.map((r) => [r.name.toLowerCase(), r]));

let hydrated = 0;
const changes = [];
const hydratedOpps = (src.opportunities || []).map((o) => {
  const c = o.primaryContact || {};
  const hit = byName.get(text(c.name || c.fullName).toLowerCase());
  if (!hit || text(c.email)) return o;
  hydrated += 1;
  changes.push({
    opportunityId: o.id || o.opportunityId,
    name: c.name || c.fullName,
    email: hit.email,
  });
  return {
    ...o,
    primaryContact: {
      ...c,
      email: hit.email,
      emailOwnership: "person_specific",
      emailVerificationStatus: "FINDYMAIL_PROVIDER_RETURNED",
      emailCustomerVerified: false,
      channels: [
        ...(c.channels || []),
        {
          channelType: "PROFESSIONAL_EMAIL",
          value: hit.email,
          source: "FINDYMAIL",
          verificationStatus: "UNVERIFIED",
          verificationDate: new Date().toISOString(),
          personDirect: true,
          researchProvenance: { provider: "FINDYMAIL", domain: hit.domain },
        },
      ],
    },
    contactResearchStage: {
      ...(o.contactResearchStage || {}),
      findymailUsed: true,
      findymailPermanentComponent: true,
      findymailHydratedAt: new Date().toISOString(),
    },
  };
});

const shortFp = crypto
  .createHash("sha256")
  .update(JSON.stringify(changes))
  .digest("hex")
  .slice(0, 8);
const publicationVersion = `gdi_hotel_wide_fqo_${HOTEL_ID}_20261127_${shortFp}`;
const nextPkgDraft = {
  ...src,
  publicationVersion,
  publicationId: publicationVersion,
  createdAt: new Date().toISOString(),
  opportunities: hydratedOpps,
  opportunityCount: hydratedOpps.length,
  inventoryIds: hydratedOpps.map((o) => o.opportunityId || o.id),
  sortOrder: hydratedOpps.map((o) => o.opportunityId || o.id),
  gates: {
    ...(src.gates || {}),
    findymailStage4Hydration: {
      contract: "gdi-findymail-wyndham-hydrate-staged-v1",
      sourceVersion,
      hydrated,
      authorizationId: AUTH,
    },
  },
};
const fpBody = fingerprintPublicationContent(nextPkgDraft);
const nextPkg = {
  ...nextPkgDraft,
  contentFingerprintSha256: fpBody.contentFingerprintSha256,
};

const report = {
  dryRun: !flag("--apply"),
  sourceVersion,
  publicationVersion,
  hydrated,
  changes,
  foundCandidates: found.length,
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
fs.writeFileSync(
  path.join(reportDir, "09-STAGED_FINDYMAIL_HYDRATION.json"),
  `${JSON.stringify({ ...report, stagedPointer: store.readPointer(HOTEL_ID, "STAGED") }, null, 2)}\n`,
);
console.log(
  JSON.stringify(
    {
      ok: true,
      publicationVersion,
      stagedPointer: store.readPointer(HOTEL_ID, "STAGED"),
    },
    null,
    2,
  ),
);
