#!/usr/bin/env node
/**
 * Hotel-agnostic Findymail -> Airtable contact persistence (wraps
 * persistFindymailContactsToAirtable). Dry-run by default. No hotel defaults.
 *
 * Input: Stage 4 Live contact report (04-contact-research.json) of a prior
 * Full E2E V3 Live run, or --opportunities-file. Only opportunities whose primary
 * contact carries a FINDYMAIL-sourced email channel are eligible (Surfe and
 * public-source emails are never persisted by this path).
 *
 * Apply requires ALL of:
 *   --apply
 *   (--authorize-research-persistence + --exact-write-set-authorization <text>)
 *     OR (--authorize-findymail-persist + --authorize-airtable-production-writes)
 *   env GDI_FINDYMAIL_PERSIST_PRODUCTION_WRITES=1
 * Paid research / Findymail spend authorization never unlocks Airtable writes.
 * This script never calls Findymail (no spend).
 *
 * Exit: 0 ok · 1 runtime failure · 2 usage / missing authorization (fail closed)
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { persistFindymailContactsToAirtable } from "../../lib/group-demand-intelligence/persistence/gdi-findymail-airtable-contact-persist-v1.js";
import {
  assertFindymailPersistAuthorized,
  buildFindymailPersistPatchesFromContactResults,
  resolveFindymailPersistAuthorization,
} from "../../lib/group-demand-intelligence/persistence/gdi-findymail-persist-authorization-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "../..");

function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  if (i >= 0 && process.argv[i + 1]) return process.argv[i + 1];
  return fallback;
}
function flag(name) {
  return process.argv.includes(name);
}

function usage() {
  console.log(`Usage:
  node scripts/gdi/run-gdi-findymail-airtable-persist-v1.mjs --hotel-id <rec...> [options]

Default is DRY-RUN (no Airtable writes, no provider calls).

Options:
  --contact-report <path>      Stage 4 Live report (default: reports/gdi-canonical-full-e2e-v3/<hotel>/04-contact-research.json)
  --opportunities-file <path>  Alternative: JSON array / { opportunities: [] }
  --out-dir <path>             Where to write findymail-airtable-persist.json (default: report dir)
  --apply                      Perform Airtable writes (needs authorization below + env gate)
  --authorize-research-persistence --exact-write-set-authorization <text>
  --authorize-findymail-persist --authorize-airtable-production-writes
`);
}

function readJson(p) {
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

async function main() {
  const hotelId = arg("--hotel-id");
  if (!hotelId) {
    usage();
    process.exit(2);
  }
  const apply = flag("--apply");
  const authOpts = {
    authorizeResearchPersistence: flag("--authorize-research-persistence"),
    exactWriteSetAuthorization: arg("--exact-write-set-authorization") || undefined,
    authorizeFindymailPersist: flag("--authorize-findymail-persist"),
    authorizeAirtableProductionWrites: flag("--authorize-airtable-production-writes"),
  };

  if (apply) {
    try {
      assertFindymailPersistAuthorized(authOpts, { apply: true });
    } catch (err) {
      console.error(`Findymail persist apply refused (${err.code}): ${err.message}`);
      process.exit(2);
    }
  }

  const defaultDir = path.join(root, "reports/gdi-canonical-full-e2e-v3", hotelId);
  let rows = [];
  let source = null;
  if (arg("--opportunities-file")) {
    source = path.resolve(arg("--opportunities-file"));
    const raw = readJson(source);
    rows = Array.isArray(raw) ? raw : raw?.opportunities || [];
  } else {
    source = path.resolve(
      arg("--contact-report") || path.join(defaultDir, "04-contact-research.json"),
    );
    const raw = readJson(source);
    if (!raw) {
      console.error(`Contact report not found/unreadable: ${source}`);
      process.exit(2);
    }
    if (raw.mode !== "Live") {
      console.error("Contact report must be a Live Stage 4 report (mode=Live)");
      process.exit(2);
    }
    rows = raw.results || [];
  }
  rows = rows.map((r) => {
    const opp = r?.opportunity || r;
    return { opportunity: { ...opp, hotelId: opp?.hotelId || hotelId } };
  });

  const { patches, skipped } = buildFindymailPersistPatchesFromContactResults(rows);
  const result = await persistFindymailContactsToAirtable(patches, {
    hotelId,
    apply,
    dryRun: !apply,
    runId: `findymail_persist_${hotelId}_${Date.now().toString(36)}`,
  });

  const outDir = path.resolve(arg("--out-dir") || path.dirname(source));
  fs.mkdirSync(outDir, { recursive: true });
  const report = {
    hotelId,
    source,
    dryRun: result.dryRun !== false,
    applied: result.dryRun === false,
    candidateCount: patches.length,
    skipped,
    wouldWrite: result.wouldWrite ?? null,
    written: result.written ?? 0,
    authorization: resolveFindymailPersistAuthorization(authOpts),
    at: new Date().toISOString(),
  };
  const outFile = path.join(outDir, "findymail-airtable-persist.json");
  fs.writeFileSync(outFile, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ ok: true, outFile, ...report }, null, 2));
  process.exit(0);
}

main().catch((err) => {
  console.error(err?.stack || err);
  process.exit(1);
});
