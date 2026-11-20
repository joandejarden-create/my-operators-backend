#!/usr/bin/env node
/**
 * Bethesda Marriott — controlled HI Org Contacts apply under HE50 standing auth.
 *
 *   node scripts/he50-beth-hi-org-contacts-controlled-apply-v1.mjs --dry-run
 *   node scripts/he50-beth-hi-org-contacts-controlled-apply-v1.mjs --apply
 *
 * No Surfe. No Hotel Census. No Economic/Legal Owner edge creation.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import { buildBethHiOrgContactsMutationPackage } from "../lib/hotel-intelligence/he-5-0-full-e2e/hi-org-contacts-beth-package-v1.js";
import { runHe50ControlledApply } from "../lib/hotel-intelligence/he-5-0-full-e2e/controlled-apply-executor-v1.js";
import { classifyPilotMutationPackage } from "../lib/hotel-intelligence/he-5-0-full-e2e/conditional-approval-v1.js";
import { HE50_STANDING_APPLY_GATE } from "../lib/hotel-intelligence/he-5-0-full-e2e/standing-authorization-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p });
    break;
  }
}

const APPLY = process.argv.includes("--apply");
const OUT_DIR = path.join(
  ROOT,
  "reports/hotel-explorer/he50-hi-org-contacts",
);

async function main() {
  if (!HE50_STANDING_APPLY_GATE.hiOrgContactsAuthorized) {
    throw new Error("HI Org Contacts standing authorization not enabled");
  }
  const pack = buildBethHiOrgContactsMutationPackage();
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(
    path.join(OUT_DIR, "BETH-MUTATION-PACKAGE-V1.json"),
    JSON.stringify(pack, null, 2),
  );

  const classified = classifyPilotMutationPackage(pack.mutations);
  fs.writeFileSync(
    path.join(OUT_DIR, "BETH-CLASSIFICATION-V1.json"),
    JSON.stringify(classified, null, 2),
  );

  const report = await runHe50ControlledApply({
    apply: APPLY,
    mutations: pack.mutations,
    outDir: OUT_DIR,
  });

  const summary = {
    mode: APPLY ? "APPLY" : "DRY_RUN",
    hotelId: pack.hotelId,
    ownershipEdgesCreated: false,
    classification: classified.summary,
    reportStatus: report.reportStatus,
    counts: report.counts,
    applied: (report.results || [])
      .filter((r) => r.applied)
      .map((r) => r.mutationId),
    quarantined: (report.results || [])
      .filter((r) => r.quarantineReason || r.failed)
      .map((r) => ({
        id: r.mutationId,
        reason: r.quarantineReason,
        failed: r.failed,
      })),
    org: pack.org,
    people: pack.people.map((p) => ({
      name: p.displayName,
      title: p.title,
      functionRole: p.functionRole,
      propertyResponsibility: p.propertyResponsibility,
      personId: p.personId,
      linkedIn: p.linkedIn,
    })),
  };

  const summaryName = APPLY
    ? "BETH-HI-ORG-CONTACTS-APPLY-SUMMARY-V1.json"
    : "BETH-HI-ORG-CONTACTS-DRY-RUN-SUMMARY-V1.json";
  fs.writeFileSync(path.join(OUT_DIR, summaryName), JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
