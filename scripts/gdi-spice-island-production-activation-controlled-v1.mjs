#!/usr/bin/env node
/**
 * Controlled Spice Island production activation workflow.
 *
 * Default: prepare + verify only (NO merge/deploy/backup-write/publish).
 * Live steps require BOTH:
 *   --authorize-production-release
 *   --authorization-phrase=FOUNDER_GO_GDI_SPICE_PRODUCTION_ACTIVATION_<SHA8>
 *
 * Does not modify ADP, rotate share tokens, or rewrite peer hotels.
 */
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import {
  SPICE_ISLAND_HOTEL_ID,
  SPICE_ISLAND_CERTIFIED_SEVEN_IDS,
  resolveApprovedPublicationOpportunities,
} from "../lib/group-demand-intelligence/durable-publication/gdi-approved-publication-opportunity-source-v1.js";
import { publishHotelInventoryOneClick } from "../lib/group-demand-intelligence/durable-publication/gdi-one-click-hotel-publish-v1.js";
import { buildCanonicalPresentationObject } from "../lib/group-demand-intelligence/contracts/gdi-canonical-opportunity-contract-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-one-click-publication-architecture-v1/production-activation-controlled-v1.json",
);

function arg(name, fallback = null) {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  if (hit) return hit.slice(name.length + 3);
  return process.argv.includes(`--${name}`) ? true : fallback;
}

function runNode(script) {
  const r = spawnSync(process.execPath, [script], {
    cwd: ROOT,
    encoding: "utf8",
    env: process.env,
  });
  return {
    script,
    ok: r.status === 0,
    status: r.status,
    tail: String(r.stdout || r.stderr || "")
      .split(/\r?\n/)
      .slice(-12)
      .join("\n"),
  };
}

function sha8() {
  try {
    const r = spawnSync("git", ["rev-parse", "HEAD"], {
      cwd: ROOT,
      encoding: "utf8",
    });
    return String(r.stdout || "").trim().slice(0, 8);
  } catch {
    return "UNKNOWN";
  }
}

const head = sha8();
const wantPhrase = `FOUNDER_GO_GDI_SPICE_PRODUCTION_ACTIVATION_${head}`;
const authorize = arg("authorize-production-release") === true;
const phrase = String(arg("authorization-phrase", "") || "");
const authorized = authorize && phrase === wantPhrase;

const tests = [
  "scripts/test-gdi-hangers-universal-presentation-parity-v1.mjs",
  "scripts/test-gdi-universal-presentation-publication-parity-v1.mjs",
  "scripts/test-gdi-one-click-hotel-publish-v1.mjs",
  "scripts/test-gdi-one-click-publish-auth-v1.mjs",
  "scripts/gdi-one-click-publish-spice-island-dry-run-v1.mjs",
  "scripts/test-gdi-publication-full-content-contract-v1.mjs",
  "scripts/gdi-universal-multi-hotel-presentation-regression-v1.mjs",
].map(runNode);

const resolved = resolveApprovedPublicationOpportunities({
  hotelId: SPICE_ISLAND_HOTEL_ID,
  opportunityIds: [...SPICE_ISLAND_CERTIFIED_SEVEN_IDS],
});
const opps = resolved.opportunities || [];
const hangers = opps.find((o) => o.id === "gdi_s4912_b6d9b1929c05");
const canonical = hangers
  ? buildCanonicalPresentationObject(hangers, {
      hotelId: SPICE_ISLAND_HOTEL_ID,
      lock: true,
    })
  : null;

const dry = publishHotelInventoryOneClick({
  hotelId: SPICE_ISLAND_HOTEL_ID,
  opportunities: opps,
  opportunityIds: [...SPICE_ISLAND_CERTIFIED_SEVEN_IDS],
  dryRun: true,
});

const classCounts = opps.reduce((a, o) => {
  const c = o.commercialInventoryClass || o.commercialClassification || "OTHER";
  a[c] = (a[c] || 0) + 1;
  return a;
}, {});

const aventa = opps.some((o) =>
  /aventa|carlisle/i.test(JSON.stringify(o)),
);

const testsOk = tests.every((t) => t.ok);
const inventoryOk =
  opps.length === 7 &&
  classCounts.SALES_READY === 1 &&
  classCounts.ACCOUNT_DEVELOPMENT === 6 &&
  !aventa &&
  dry.ok === true;

const report = {
  ok: testsOk && inventoryOk,
  status: authorized
    ? "AUTHORIZED_BUT_LIVE_STEPS_REQUIRE_OPERATOR_EXECUTION"
    : "PREPARED_AWAITING_FOUNDER_AUTHORIZATION",
  headSha8: head,
  requiredAuthorizationPhrase: wantPhrase,
  authorized,
  hotelId: SPICE_ISLAND_HOTEL_ID,
  opportunityIds: SPICE_ISLAND_CERTIFIED_SEVEN_IDS,
  classCounts,
  aventaExcluded: !aventa,
  hangers: canonical
    ? {
        title: canonical.identity.canonicalTitle,
        primary: canonical.drawer.primaryContact?.name,
        secondary: canonical.drawer.secondaryContacts?.[0]?.name,
        facts: (canonical.drawer.establishedFacts || []).length,
        uncertainties: (canonical.drawer.uncertainties || []).length,
        qq: (canonical.drawer.qualificationQuestions || []).length,
        evidence: (canonical.drawer.supportingEvidence || []).length,
      }
    : null,
  dryRun: {
    ok: dry.ok === true,
    opportunityCount: dry.plan?.opportunityCount || null,
    code: dry.code || null,
  },
  tests,
  liveSteps: {
    backupProductionRegistry: authorized ? "READY_TO_EXECUTE" : "BLOCKED",
    deployRailwayExactSha: authorized ? "READY_TO_EXECUTE" : "BLOCKED",
    publishSpiceSeven: authorized ? "READY_TO_EXECUTE" : "BLOCKED",
    verifyExternalLink: authorized ? "READY_TO_EXECUTE" : "BLOCKED",
    note:
      "This script never writes production volumes, merges PRs, or deploys Railway by itself. Live execution is operator-gated after founder phrase match.",
  },
  contentHashSeed: createHash("sha256")
    .update(JSON.stringify(SPICE_ISLAND_CERTIFIED_SEVEN_IDS))
    .digest("hex")
    .slice(0, 16),
};

fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));
process.exit(report.ok ? 0 : 1);
