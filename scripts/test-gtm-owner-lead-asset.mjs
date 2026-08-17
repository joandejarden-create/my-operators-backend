import assert from "node:assert/strict";
import { pickLeadProperty, MANUAL_LEAD_ASSET } from "../lib/gtm-owner-target/owner-lead-asset.js";
import {
  buildP0V1rEmailEnrichment,
  P0_V1R_EMAIL_SPECS,
} from "../lib/gtm-owner-target/adapters/p0-v1r-email-research.js";
import { validateRegistryEnrichmentRecord } from "../lib/gtm-owner-target/registry-contact-verification.js";
import { buildOwnerPortfolioAudit } from "../lib/gtm-owner-target/owner-portfolio-audit.js";

const props = [
  {
    buildingName: "ibis Mexico Tlalnepantla",
    brandAffiliation: "ibis",
    city: "Tlalnepantla",
    country: "Mexico",
    hotelOperator: "Accor North America",
  },
  {
    buildingName: "Sunscape Dorado Pacifico Ixtapa",
    brandAffiliation: "Sunscape Resorts & Spas",
    city: "Ixtapa",
    country: "Mexico",
    hotelOperator: "Apple Leisure Group",
  },
];

const lead = pickLeadProperty(props, "Park Mizgal, S.C.");
assert.ok(lead?.buildingName.includes("Sunscape"), "Park Mizgal lead should prefer Sunscape");

const audit = buildOwnerPortfolioAudit(
  { ownerName: "Park Mizgal, S.C.", icpSegment: "regional_operator", outreachReady: true },
  props.map((p) => ({ ...p, trueOwner: "Park Mizgal, S.C." }))
);
assert.ok(audit.leadPitchAsset.includes("Sunscape"), "portfolio audit lead asset should be Sunscape");

for (const spec of P0_V1R_EMAIL_SPECS) {
  const enrichment = buildP0V1rEmailEnrichment(spec);
  const validation = validateRegistryEnrichmentRecord(enrichment);
  assert.equal(enrichment.contact.verificationTier, "V1R", `${spec.slug} should be V1R`);
  assert.ok(enrichment.contact.email, `${spec.slug} should have email`);
  assert.equal(validation.ok, true, `${spec.slug}: ${validation.failures.join("; ")}`);
}

console.log(
  `owner-lead-asset + p0-v1r tests OK (${P0_V1R_EMAIL_SPECS.length} V1R specs, Park Mizgal override active)`
);
