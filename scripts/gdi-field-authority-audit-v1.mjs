#!/usr/bin/env node
/**
 * Field-authority audit across Bethesda 67:
 * frozen local master ↔ V2 package ↔ (optional) live APIs / DOM.
 * Detects wrong-field substitutions without mutating publication content.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_FIELD_AUTHORITY_CONTRACT_V1,
  GDI_FIELD_AUTHORITY_MATRIX_V1,
  detectContactSubtitleSubstitution,
  resolveAuthoritativeCardContactSubtitle,
} from "../lib/group-demand-intelligence/presentation/gdi-field-authority-contract-v1.js";
import { attachPackageCustomerPresentation } from "../lib/group-demand-intelligence/publication-package/gdi-package-customer-presentation-attach-v1.js";
import { verifyPublicationPackageIntegrity } from "../lib/group-demand-intelligence/publication-package/index.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-field-authority-audit-v1");
const HOTEL = "recLuxvwwxID7U2B8";
const APPROVED_FP =
  "df8da65a172727e627a71c3a31bead5f3f70ee95b03b7245f7a0ea34bbb33c66";
const SNAP = path.join(
  ROOT,
  "reports/gdi-local-master-publication-v1/local-master-snapshot/local-live-opportunities.json",
);
const PKG = path.join(
  ROOT,
  "lib/group-demand-intelligence/bundled-publication-packages/recLuxvwwxID7U2B8/gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2/package.json",
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}
function action(o = {}) {
  return text(
    o.tileBadgePlan?.salesActionPillLabel ||
      o.salesActionPillLabel ||
      o.salesActivationLabel,
  );
}
function icons(o = {}) {
  return JSON.stringify(o.revenueStreamIcons || null);
}
function summary(o = {}) {
  return text(o.whatIsHappening || o.summaryWhat || o.customerSummary || o.summary);
}
function title(o = {}) {
  return text(o.displayTitle || o.title);
}

fs.mkdirSync(OUT, { recursive: true });

const frozen = JSON.parse(fs.readFileSync(SNAP, "utf8"));
const pkg = JSON.parse(fs.readFileSync(PKG, "utf8"));
const integrity = verifyPublicationPackageIntegrity(pkg);
const attached = attachPackageCustomerPresentation(pkg.opportunities, {
  hotelId: HOTEL,
  publicationVersion: pkg.publicationVersion,
  contentFingerprintSha256: pkg.contentFingerprintSha256,
});

const differences = [];
const contactIntent = [];

for (const f of frozen.opportunities || []) {
  const id = f.id;
  const p = (pkg.opportunities || []).find((x) => x.id === id);
  const a = (attached || []).find((x) => x.id === id);
  const resolved = resolveAuthoritativeCardContactSubtitle(f);
  const subCheck = detectContactSubtitleSubstitution(
    f,
    a?.canonicalCustomerPresentation?.contact?.role ||
      a?.tileContactPreview?.displayRole,
  );

  const row = {
    id,
    title: title(f),
    contact: {
      localDisplayRole: text(f.tileContactPreview?.displayRole),
      localOpportunityRole: text(f.tileContactPreview?.opportunityRole),
      localDisplayRoleKind: text(f.tileContactPreview?.displayRoleKind),
      authoritativeSubtitle: resolved.subtitle,
      authoritativeSource: resolved.source,
      packageDisplayRole: text(p?.tileContactPreview?.displayRole),
      packageOpportunityRole: text(p?.tileContactPreview?.opportunityRole),
      attachedCardSubtitle: text(
        a?.canonicalCustomerPresentation?.contact?.cardContactSubtitle ||
          a?.cardContactSubtitle,
      ),
      substitutionOk: subCheck.ok,
      substitutionIssues: subCheck.issues,
    },
  };
  contactIntent.push(row);

  if (
    row.contact.localDisplayRole &&
    row.contact.localOpportunityRole &&
    row.contact.localDisplayRole !== row.contact.localOpportunityRole &&
    row.contact.attachedCardSubtitle === row.contact.localOpportunityRole
  ) {
    differences.push({
      differenceId: `CONTACT_SUBTITLE_USES_OPPORTUNITY_ROLE__${id}`,
      opportunityId: id,
      field: "cardContactSubtitle",
      severity: "MATERIAL",
      localMasterValue: row.contact.localDisplayRole,
      wrongValue: row.contact.localOpportunityRole,
      rootCause: "opportunityRole substituted for displayRole",
    });
  }

  if (p && title(f) !== title(p)) {
    differences.push({
      differenceId: `TITLE_PACKAGE_DRIFT__${id}`,
      opportunityId: id,
      field: "displayTitle",
      severity: "MATERIAL",
      localMasterValue: title(f),
      packageValue: title(p),
    });
  }
  if (p && action(f) !== action(p)) {
    differences.push({
      differenceId: `ACTION_PACKAGE_DRIFT__${id}`,
      opportunityId: id,
      field: "commercialAction",
      severity: "MATERIAL",
      localMasterValue: action(f),
      packageValue: action(p),
    });
  }
  if (p && icons(f) !== icons(p)) {
    differences.push({
      differenceId: `ICONS_PACKAGE_DRIFT__${id}`,
      opportunityId: id,
      field: "revenueStreamIcons",
      severity: "MATERIAL",
      localMasterValue: icons(f),
      packageValue: icons(p),
    });
  }
  // Attached must preserve displayRole as card subtitle when kinds differ
  if (
    row.contact.localDisplayRole &&
    row.contact.attachedCardSubtitle &&
    row.contact.attachedCardSubtitle !== row.contact.localDisplayRole &&
    row.contact.localDisplayRoleKind === "professional_title"
  ) {
    differences.push({
      differenceId: `ATTACH_DROPPED_DISPLAY_ROLE__${id}`,
      opportunityId: id,
      field: "cardContactSubtitle",
      severity: "MATERIAL",
      localMasterValue: row.contact.localDisplayRole,
      attachedValue: row.contact.attachedCardSubtitle,
    });
  }
}

const lorna = contactIntent.find(
  (r) => r.id === "gdi_opp_regulatory_information_conference_20260930",
);

const report = {
  schema: "gdi-field-authority-audit-v1",
  contractVersion: GDI_FIELD_AUTHORITY_CONTRACT_V1,
  generatedAt: new Date().toISOString(),
  hotelId: HOTEL,
  approvedFingerprint: APPROVED_FP,
  packageFingerprint: pkg.contentFingerprintSha256 || null,
  fingerprintMatch: pkg.contentFingerprintSha256 === APPROVED_FP,
  integrityOk: integrity?.ok !== false,
  inventory: (frozen.opportunities || []).length,
  matrixFieldCount: GDI_FIELD_AUTHORITY_MATRIX_V1.length,
  differences,
  materialCount: differences.filter((d) => d.severity === "MATERIAL").length,
  lornaFixture: {
    id: lorna?.id,
    expectedSubtitle: "RIC Conference Contact",
    opportunityRolePresent: "Group Travel Coordinator",
    authoritative: lorna?.contact,
    pass:
      lorna?.contact?.authoritativeSubtitle === "RIC Conference Contact" &&
      lorna?.contact?.attachedCardSubtitle === "RIC Conference Contact" &&
      lorna?.contact?.substitutionOk === true,
  },
  originalIntentSummary: {
    contactTilePriority: [
      "1) professionalTitle / activity-specific displayRole",
      "2) opportunityRole enum (only if no title)",
      "3) Role Not Yet Verified",
    ],
    displayRoleVsOpportunityRole:
      "displayRole is the customer subtitle; opportunityRole is commercial classification and must not replace displayRole when both exist",
    localMasterAuthority:
      "Authenticated local GDI / freeze snapshot is presentation reference for publication",
  },
};

fs.writeFileSync(
  path.join(OUT, "FIELD_AUTHORITY_MATRIX.json"),
  `${JSON.stringify(
    {
      version: GDI_FIELD_AUTHORITY_CONTRACT_V1,
      fields: GDI_FIELD_AUTHORITY_MATRIX_V1,
    },
    null,
    2,
  )}\n`,
);
fs.writeFileSync(
  path.join(OUT, "67-CONTACT-FIELD-INTENT.json"),
  `${JSON.stringify({ rows: contactIntent }, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "FIELD_SUBSTITUTION_REGISTER.json"),
  `${JSON.stringify({ differences, materialCount: report.materialCount }, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(OUT, "AUDIT_SUMMARY.json"),
  `${JSON.stringify(report, null, 2)}\n`,
);

const md = `# GDI Field Authority Audit V1

## Original local presentation intent (contacts)

Tile footer shows **one** name + **one** subtitle, in order:

1. Verified professional / activity-specific title → \`displayRole\` (\`displayRoleKind=professional_title\`)
2. Else opportunity-specific role enum → \`opportunityRole\` (Event Organizer, Group Travel Coordinator, …)
3. Else \`Role Not Yet Verified\`

\`buyerEligibility\` / contact relevance are **not** tile subtitles.

**Lorna Kipfer fixture:** \`displayRole=RIC Conference Contact\`, \`opportunityRole=Group Travel Coordinator\`. Card must show RIC Conference Contact.

## Package fingerprint

- Approved: \`${APPROVED_FP}\`
- Package: \`${pkg.contentFingerprintSha256}\`
- Match: ${report.fingerprintMatch}

## Audit results

- Opportunities: ${report.inventory}
- Material substitution / drift findings: **${report.materialCount}**
- Lorna fixture: ${report.lornaFixture.pass ? "PASS" : "FAIL"}

## Matrix

See \`FIELD_AUTHORITY_MATRIX.json\` (${GDI_FIELD_AUTHORITY_MATRIX_V1.length} fields).

## Publication content

No fingerprint change in this audit. Mapper enforces authority at read/attach time only.
`;
fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), md);

console.log(
  JSON.stringify(
    {
      inventory: report.inventory,
      materialCount: report.materialCount,
      fingerprintMatch: report.fingerprintMatch,
      lornaPass: report.lornaFixture.pass,
      sampleDiffs: differences.slice(0, 8),
    },
    null,
    2,
  ),
);

if (!report.fingerprintMatch || !report.lornaFixture.pass) process.exit(2);
if (report.materialCount > 0) process.exit(2);
