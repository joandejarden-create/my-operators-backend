#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createGdiPublicationPackageStore,
  buildCustomerPublicationPackage,
  verifyPublicationPackageIntegrity,
} from "../lib/group-demand-intelligence/publication-package/index.js";
import { GDI_PUBLICATION_LIFECYCLE_STATE } from "../lib/group-demand-intelligence/publication-package/gdi-publication-lifecycle-v1.js";
import { GDI_FULL_E2E_HOTEL_TEMPLATE_V1 } from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-sheraton-paid-full-e2e-discovery-v3");
const HOTEL_ID = "recCAGqkERSXgiku4";
const HOTEL_NAME = "Sheraton Guadalajara Expo";
const V2_VERSION =
  "gdi_pub_recCAGqkERSXgiku4_20261113_commercial_e2e_remediation_v2";
const V2_FP =
  "6f9c0416be28c2409cf9fcccfb78cc45dd3a5178b27d4e0009f2622a21efd092";

function seriesKey(o) {
  const t = String(o.displayTitle || o.title || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
  if (/ecomondo|re\+?\s*mexico|re mexico/.test(t)) return "re_ecomondo_mexico_2027";
  if (/sicon|semana internacional de la construccion/.test(t)) return "sicon_mexico_2027";
  if (/expo nacional ferretera/.test(t)) return "ferretera_2027";
  if (/hospitality expo|hostelco/.test(t)) return "mx_hospitality_hostelco_2027";
  if (/euroguss/.test(t)) return "euroguss_mexico_2027";
  if (/itb americas/.test(t)) return "itb_americas_2027";
  if (/neumolog/.test(t)) return "neumologia_2027";
  if (/anpact|expo transporte/.test(t)) return "anpact_2027";
  if (/hyrox/.test(t)) return "hyrox_gdl_2027";
  if (/ciics|investigacion cualitativa/.test(t)) return "ciics_2027";
  return t.slice(0, 48);
}

const store = createGdiPublicationPackageStore({
  root: path.join(ROOT, "lib/group-demand-intelligence/bundled-publication-packages"),
});
const staged = store.loadPackage(
  HOTEL_ID,
  "gdi_pub_recCAGqkERSXgiku4_20261114_paid_full_e2e_discovery_v3",
);
const ops = staged.package.opportunities;
const netNew = ops.filter((o) => String(o.id).startsWith("paid3_"));
const retained = ops.filter((o) => !String(o.id).startsWith("paid3_"));
const kept = [];
const dropped = [];
const seen = new Set();
for (const o of netNew) {
  const k = seriesKey(o);
  if (seen.has(k)) {
    dropped.push({ id: o.id, title: o.title, series: k });
    continue;
  }
  seen.add(k);
  kept.push(o);
}
const customer = [...kept, ...retained];
const ledger = JSON.parse(
  fs.readFileSync(path.join(OUT, "paid-budget-ledger.json"), "utf8"),
);
const publicationVersion =
  "gdi_pub_recCAGqkERSXgiku4_20261114_paid_full_e2e_discovery_v3";
const pkg = buildCustomerPublicationPackage({
  hotelId: HOTEL_ID,
  hotelName: HOTEL_NAME,
  opportunities: customer,
  publicationVersion,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  approvalStatus: "PENDING_FOUNDER_APPROVAL",
  notes: {
    founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
    paidDiscovery: "gdi-sheraton-paid-full-e2e-discovery-v3",
    dedupedSeries: true,
    spentUsd: ledger.spentUsd,
    netNewQualified: kept.length,
    preservesV2: { version: V2_VERSION, fingerprint: V2_FP },
    templateId: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
  },
});
const integrity = verifyPublicationPackageIntegrity(pkg);
if (!integrity.ok) {
  console.error(integrity.fails);
  process.exit(2);
}
store.writePackage(pkg);
store.writePointer(HOTEL_ID, "STAGED", {
  publicationId: pkg.publicationId,
  publicationVersion: pkg.publicationVersion,
  contentFingerprintSha256: pkg.contentFingerprintSha256,
  lifecycleState: GDI_PUBLICATION_LIFECYCLE_STATE.FOUNDER_REVIEWED,
  founderReviewLabel: "STAGED_FOR_FOUNDER_REVIEW",
  supersedes: {
    publicationVersion: V2_VERSION,
    contentFingerprintSha256: V2_FP,
  },
});

fs.writeFileSync(
  path.join(OUT, "06-INCREMENTAL_QUALIFIED_OPPORTUNITIES.json"),
  JSON.stringify(
    {
      target: 15,
      netNewQualifiedCount: kept.length,
      netNewQualified: kept.map((o) => ({
        id: o.id,
        title: o.title,
        org: o.organizationName,
        contact: o.primaryContact?.name,
        role: o.primaryContact?.title,
        when: o.datePillDisplayLabel,
        segment: o.researchSegment,
      })),
      duplicatesCollapsed: dropped,
      targetMet: kept.length >= 15,
    },
    null,
    2,
  ) + "\n",
);
fs.writeFileSync(
  path.join(OUT, "13-NEW_STAGED_PUBLICATION.json"),
  JSON.stringify(
    {
      status: "STAGED_FOR_FOUNDER_REVIEW",
      publicationVersion,
      fingerprint: pkg.contentFingerprintSha256,
      opportunityCount: customer.length,
      netNewQualified: kept.length,
      retained: retained.length,
      inventoryCounts: pkg.inventoryCounts,
      spentUsd: ledger.spentUsd,
      remainingUsd: Number((40 - ledger.spentUsd).toFixed(2)),
    },
    null,
    2,
  ) + "\n",
);

const top = kept.slice(0, 15);
const lines = [
  "# Top sales prospects — Sheraton Guadalajara Expo (Paid Discovery V3)",
  "",
  `Reference: 2026-11-14. **Net-new qualified (deduped): ${kept.length}** (target 15). Spent $${ledger.spentUsd} of $40.`,
  "",
];
top.forEach((o, i) => {
  lines.push(`## ${i + 1}. ${o.displayTitle}`);
  lines.push(`- **Organization:** ${o.organizationName}`);
  lines.push(`- **Demand type:** ${o.opportunityTypeLabel || o.researchSegment}`);
  lines.push(`- **Window:** ${o.datePillDisplayLabel}`);
  lines.push(`- **Hotel business:** ${String(o.potentialHotelBusiness || "").slice(0, 300)}`);
  lines.push(`- **Why Sheraton:** ${String(o.whyThisHotel || "").slice(0, 240)}`);
  lines.push(
    `- **Contact:** ${o.primaryContact?.name || "n/a"}${o.primaryContact?.title ? ` — ${o.primaryContact.title}` : ""} (RELEVANT_COMMERCIAL_ENTRY_POINT)`,
  );
  lines.push(`- **Sourcing:** ${o.hotelSourcingStatusLabel}`);
  lines.push(`- **Evidence class:** ${o.demandEvidenceClass}`);
  lines.push(`- **Next step:** ${String(o.recommendedNextStep || "").slice(0, 260)}`);
  lines.push(
    "- **Validation question:** Is hotel sourcing open for additional properties, and is this contact the correct lodging entry point?",
  );
  lines.push("");
});
fs.writeFileSync(path.join(OUT, "11-TOP_15_SALES_PROSPECTS.md"), lines.join("\n"));
fs.writeFileSync(
  path.join(OUT, "FOUNDER_REPORT.md"),
  `# Sheraton Guadalajara Expo — Paid Full E2E Discovery V3

**Hard ceiling:** $40 · **Spent:** $${ledger.spentUsd} · **Remaining:** $${Number((40 - ledger.spentUsd).toFixed(2))}  
**New staged fingerprint:** \`${pkg.contentFingerprintSha256}\`  
**Preserves V2:** \`${V2_FP}\`

## Result

| Class | Count |
|-------|------:|
| **NET-NEW QUALIFIED (deduped)** | **${kept.length}** |
| Target | 15 |
| Target met | ${kept.length >= 15 ? "**YES**" : "NO"} |
| Series duplicates collapsed | ${dropped.length} |
| Existing retained | ${retained.length} |
| Customer-visible total | ${customer.length} |
| Sales Ready | 0 |
| Account Development | ${customer.length} |

Cost per net-new qualified: **$${(ledger.spentUsd / Math.max(kept.length, 1)).toFixed(2)}**

## Preview
\`http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=${HOTEL_ID}&preview=staged\`

Surfe not used. Airtable dry-run only. No CUSTOMER_PUBLISHED.
`,
);

console.log(
  JSON.stringify(
    {
      netNew: kept.length,
      dropped: dropped.length,
      total: customer.length,
      fp: pkg.contentFingerprintSha256,
      targetMet: kept.length >= 15,
    },
    null,
    2,
  ),
);
