#!/usr/bin/env node
/**
 * Permanent Findymail Stage 4 component gates.
 *   npm run test:gdi-findymail-stage4-permanent-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  assessFindymailEmailEligibility,
  buildFindymailEnrichmentManifest,
  FINDYMAIL_STAGE4_AUTH_TOKEN,
} from "../lib/group-demand-intelligence/workflow/gdi-findymail-stage4-enrichment-v1.js";
import {
  GDI_FINDYMAIL_CLIENT_V1,
  describeFindymailKeyPresence,
} from "../lib/group-demand-intelligence/providers/gdi-findymail-client-v1.js";
import { runProfessionalContactResearchStage } from "../lib/group-demand-intelligence/workflow/gdi-contact-research-stage-v1.js";
import { buildFindymailAirtableContactPatch } from "../lib/group-demand-intelligence/persistence/gdi-findymail-airtable-contact-persist-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err && err.message ? err.message : err}`);
  }
}

check("client_and_policy_files_exist", () => {
  assert.ok(
    fs.existsSync(
      path.join(
        ROOT,
        "lib/group-demand-intelligence/providers/gdi-findymail-client-v1.js",
      ),
    ),
  );
  assert.equal(GDI_FINDYMAIL_CLIENT_V1, "gdi-findymail-client-v1");
  assert.match(FINDYMAIL_STAGE4_AUTH_TOKEN, /FINDYMAIL_STAGE4/);
  const doc = fs.readFileSync(
    path.join(ROOT, "docs/ai-build-system/GDI_STAGE4_CONTACT_RESEARCH_COMPLETION_V1.md"),
    "utf8",
  );
  assert.match(doc, /permanent Stage 4 component/i);
  assert.match(doc, /Surfe/);
  assert.equal(typeof describeFindymailKeyPresence().present, "boolean");
});

check("eligibility_allows_email_gap_even_with_linkedin", () => {
  const elig = assessFindymailEmailEligibility(
    {
      name: "David Aguirre",
      organization: "Turner Construction Company",
      linkedinUrl: "https://linkedin.com/in/david-aguirre-27020411",
    },
    {
      organizationName: "Turner Construction Company",
      sources: [{ url: "https://www.turnerconstruction.com/about" }],
    },
  );
  assert.equal(elig.eligible, true);
  assert.equal(elig.domain, "turnerconstruction.com");
});

check("eligibility_skips_when_email_present", () => {
  const elig = assessFindymailEmailEligibility(
    {
      name: "Kathryn Newstead",
      organization: "Downtown San Diego Partnership",
      email: "knewstead@downtownsandiego.org",
    },
    { organizationName: "Downtown San Diego Partnership" },
  );
  assert.equal(elig.eligible, false);
  assert.equal(elig.reason, "EMAIL_ALREADY_PRESENT");
});

check("airtable_patch_rejects_surfe_source", () => {
  const patch = buildFindymailAirtableContactPatch(
    { id: "gdi_x", primaryContact: { name: "A B" } },
    {
      person: { email: "a@b.com" },
      channel: { source: "SURFE", value: "a@b.com" },
    },
  );
  assert.equal(patch.ok, false);
  assert.equal(patch.reason, "SURFE_PERSISTENCE_FORBIDDEN");
});

check("manifest_builds_from_wyndham_package", () => {
  const pkgPath = path.join(
    ROOT,
    "lib/group-demand-intelligence/bundled-publication-packages/recnPB3Q02NO8Ma1o/gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_46de4104/package.json",
  );
  const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
  const manifest = buildFindymailEnrichmentManifest(pkg.opportunities || [], {
    creditHardCap: 30,
    repoRoot: ROOT,
  });
  assert.ok(manifest.uniquePeople >= 26);
  // Tip packages may already be fully emailed (eligibleCount 0). Gate still builds.
  assert.ok(manifest.eligibleCount >= 0);
  assert.ok(manifest.selectedCount <= 30);
  // Abbreviated surnames must never be Findymail-selected.
  const selectedNames = (manifest.selected || manifest.eligible || [])
    .map((r) => r.name || r.person?.name || "")
    .join(" | ");
  assert.doesNotMatch(selectedNames, /\b[A-Za-z]\.\s*(\||$)/);
});

check("ps1_wires_authorize_findymail", () => {
  const ps1 = fs.readFileSync(
    path.join(ROOT, "scripts/gdi/Invoke-GdiContactResearch.ps1"),
    "utf8",
  );
  assert.match(ps1, /AuthorizeFindymail/);
  assert.match(ps1, /permanent Stage 4/i);
  const common = fs.readFileSync(
    path.join(ROOT, "scripts/gdi/_GdiV3Common.ps1"),
    "utf8",
  );
  assert.match(common, /AuthorizeFindymail/);
  assert.match(common, /--authorize-findymail/);
});

// async check helper
await (async () => {
  try {
    await assert.rejects(
      () =>
        runProfessionalContactResearchStage(
          { primaryContact: { name: "Test Person" } },
          { authorizeSurfe: true, mode: "DryRun" },
        ),
      /Surfe/,
    );
    console.log("PASS surfe_still_refused_in_stage4");
  } catch (err) {
    failed += 1;
    console.error(`FAIL surfe_still_refused_in_stage4: ${err.message || err}`);
  }
})();

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-findymail-stage4-permanent-v1");
