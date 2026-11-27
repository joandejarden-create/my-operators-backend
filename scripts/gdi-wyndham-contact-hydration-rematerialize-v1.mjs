#!/usr/bin/env node
/**
 * Wyndham locked-26 STAGED contact hydration rematerialize.
 *
 *   node scripts/gdi-wyndham-contact-hydration-rematerialize-v1.mjs --dry-run
 *   node scripts/gdi-wyndham-contact-hydration-rematerialize-v1.mjs --apply \
 *     --auth "FOUNDER_GO AUTHORIZE_WYNDHAM_STAGED_CONTACT_HYDRATION_RECOVERY"
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  rematerializeWyndhamStagedContactHydration,
  WYNDHAM_CONTACT_HYDRATION_AUTH,
  GDI_WYNDHAM_CONTACT_HYDRATION_REMATERIALIZE_V1,
} from "../lib/group-demand-intelligence/publication-package/gdi-wyndham-contact-hydration-rematerialize-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-contact-research-hydration-v1");
fs.mkdirSync(OUT, { recursive: true });

const args = process.argv.slice(2);
const apply = args.includes("--apply");
const authIdx = args.indexOf("--auth");
const auth = authIdx >= 0 ? String(args[authIdx + 1] || "").trim() : "";

const result = rematerializeWyndhamStagedContactHydration({
  dryRun: !apply,
  apply,
  auth: apply ? auth : WYNDHAM_CONTACT_HYDRATION_AUTH,
});

fs.writeFileSync(
  path.join(OUT, "01-REMATERIALIZE_RESULT.json"),
  `${JSON.stringify(result, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "02-BEFORE_COVERAGE.json"),
  `${JSON.stringify(result.beforeAudit || {}, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "03-AFTER_COVERAGE.json"),
  `${JSON.stringify(result.afterAudit || {}, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "04-CONTACT_AUDIT_ROWS.json"),
  `${JSON.stringify(result.afterAudit?.rows || [], null, 2)}\n`,
);

const md = `# GDI Wyndham Contact Hydration

**Contract:** \`${GDI_WYNDHAM_CONTACT_HYDRATION_REMATERIALIZE_V1}\`
**Mode:** ${apply ? "APPLY" : "DRY_RUN"}
**Publication version:** ${result.publicationVersion || "n/a"}
**Activated:** ${result.activated === true}

## Coverage

| Metric | Before | After |
|---|---:|---:|
| Opportunities | ${result.beforeAudit?.totalOpportunities ?? 0} | ${result.afterAudit?.totalOpportunities ?? 0} |
| Named contacts | ${result.beforeAudit?.namedContacts ?? 0} | ${result.afterAudit?.namedContacts ?? 0} |
| Verified titles | ${result.beforeAudit?.verifiedTitles ?? 0} | ${result.afterAudit?.verifiedTitles ?? 0} |
| Verified email | ${result.beforeAudit?.verifiedEmail ?? 0} | ${result.afterAudit?.verifiedEmail ?? 0} |
| Verified telephone | ${result.beforeAudit?.verifiedTelephone ?? 0} | ${result.afterAudit?.verifiedTelephone ?? 0} |
| Personal LinkedIn | ${result.beforeAudit?.verifiedPersonalLinkedIn ?? 0} | ${result.afterAudit?.verifiedPersonalLinkedIn ?? 0} |
| Email or telephone | ${result.beforeAudit?.emailOrTelephone ?? 0} | ${result.afterAudit?.emailOrTelephone ?? 0} |
| Usable outreach route | ${result.beforeAudit?.usableOutreachRoute ?? 0} | ${result.afterAudit?.usableOutreachRoute ?? 0} |

## Notes

- LinkedIn mapped from \`sourceUrl\` → \`linkedinUrl\` (existing evidence; no paid research).
- Email/phone counts unchanged unless settled historical person channels are recovered.
- Stage 4 channel status annotated on every primary contact.
`;
fs.writeFileSync(path.join(OUT, "FOUNDER_CONTACT_HYDRATION.md"), md);

if (!result.ok) {
  console.error(JSON.stringify(result, null, 2));
  process.exit(10);
}
console.log(
  JSON.stringify(
    {
      ok: true,
      apply,
      activated: result.activated === true,
      publicationVersion: result.publicationVersion,
      outDir: OUT,
    },
    null,
    2,
  ),
);
