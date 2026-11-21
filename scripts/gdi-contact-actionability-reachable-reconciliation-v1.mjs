/**
 * Reconcile leakage-audit VERIFIED_REACHABLE (31) vs actionability REACHABLE.
 * Read-only. Documents commerciallyRelevant requirement as the intentional delta.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/gdi-publication-package-store-v1.js";
import { createGdiPublishedInventoryRegistry } from "../lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js";
import { classifyContactActionability } from "../lib/group-demand-intelligence/presentation/gdi-contact-actionability-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports",
  "gdi-contact-actionability-implementation-v1",
);

function loadRows() {
  const store = createGdiPublicationPackageStore();
  const registry = createGdiPublishedInventoryRegistry();
  const rows = [];
  for (const hotelId of [
    "recLuxvwwxID7U2B8",
    "recH9h3McgMNFQEla",
    "recrPQcZg7SFARRb2",
  ]) {
    const loaded = store.loadCustomerVisiblePackage(hotelId);
    if (!loaded.ok) continue;
    for (const opp of loaded.package.opportunities || []) {
      rows.push({
        hotelId,
        hotelName: loaded.package.hotelName,
        opp,
        a: classifyContactActionability(opp),
      });
    }
  }
  try {
    for (const r of registry.listAllHotelsPublished({ includeRevoked: false }) || []) {
      if (rows.some((x) => x.hotelId === r.hotelId && (x.opp.opportunityId || x.opp.id) === r.opportunityId)) {
        continue;
      }
      const snap = registry.loadSnapshot(r.hotelId, r.opportunityId, r.publishedVersion);
      const nested = snap?.snapshot || {};
      const opp = {
        ...nested,
        opportunityId: r.opportunityId || nested.id,
        primaryContact: nested.primaryContact || {
          name: nested.contactName,
          email: nested.contactEmail,
          phone: nested.contactPhone,
          title: nested.contactRole,
        },
      };
      rows.push({
        hotelId: r.hotelId,
        hotelName: "Spice Island Beach Resort",
        opp,
        a: classifyContactActionability(opp),
      });
    }
  } catch {
    /* optional */
  }
  return rows;
}

const rows = loadRows();
let softReachable = 0;
let strictReachable = 0;
let salesReady = 0;
const softOnly = [];

for (const row of rows) {
  const d = row.a.diagnostics || {};
  if (row.a.contactActionabilityTier === "SALES_READY") salesReady += 1;
  if (row.a.contactActionabilityTier === "REACHABLE") strictReachable += 1;
  // Soft = verified email OR named+phone (leakage VERIFIED_REACHABLE style)
  if (d.verifiedEmailOrNamedPhoneSoft) softReachable += 1;
  if (
    d.namedPhoneAny &&
    row.a.contactActionabilityTier !== "REACHABLE" &&
    row.a.contactActionabilityTier !== "SALES_READY"
  ) {
    softOnly.push({
      hotelId: row.hotelId,
      opportunityId: row.opp.opportunityId || row.opp.id,
      title: row.opp.title || row.opp.name,
      tier: row.a.contactActionabilityTier,
      buyerVsEntry: row.a.buyerVsEntry,
      commerciallyRelevant: row.a.flags?.hasCommerciallyRelevant,
      reason:
        "Named phone present but commercial relevance not established → CONTACT_RESEARCH_REQUIRED under V1 contract",
    });
  }
}

const report = {
  schema: "gdi-contact-actionability-reachable-reconciliation-v1",
  generatedAt: new Date().toISOString(),
  baselineVisible: rows.length,
  leakageAuditVerifiedReachable: 31,
  softNamedPhoneOrVerifiedEmail: softReachable,
  actionabilitySalesReady: salesReady,
  actionabilityReachable: strictReachable,
  // Soft reachable excluding those already SALES_READY approximates leakage's 31
  softReachableExcludingSalesReady: softReachable - salesReady,
  deltaExplanation:
    "Leakage VERIFIED_REACHABLE counted named+phone without requiring classifyContactEligibility commercial relevance. Actionability REACHABLE requires commercially relevant named person (DIRECT_BUYER / ENTRY_POINT / remit-evidenced research candidate). The ~16 gap is intentional under the founder-approved contract.",
  softOnlySample: softOnly.slice(0, 20),
  softOnlyCount: softOnly.length,
  contractDecision:
    "KEEP commerciallyRelevant requirement for REACHABLE. Document soft diagnostic for transparency; do not inflate REACHABLE with non-relevant named phones.",
};

fs.mkdirSync(OUT, { recursive: true });
fs.writeFileSync(
  path.join(OUT, "REACHABLE_RECONCILIATION.json"),
  `${JSON.stringify(report, null, 2)}\n`,
  "utf8",
);
console.log(JSON.stringify(report, null, 2));
