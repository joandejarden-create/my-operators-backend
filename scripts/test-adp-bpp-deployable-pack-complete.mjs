#!/usr/bin/env node
/**
 * npm run test:adp-bpp-deployable-pack-complete
 */
import {
  auditBppDeployablePackCompleteness,
  ADP_BPP_DEPLOYABLE_PACK_COMPLETE,
  ADP_NEW_PROPERTY_BPP_END_TO_END_PUBLICATION_GATE,
  assertNewPropertyBppEndToEndPublication,
} from "../lib/ai-demand-positioning/brand-portfolio/adp-bpp-deployable-pack-completeness-v1.js";
import { BPP_CUSTOMER_STATE } from "../lib/ai-demand-positioning/brand-portfolio/adp-bpp-customer-state-v1.js";
import { resolveBrandPortfolioPosition } from "../api/ai-demand-positioning.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { existsSync } from "fs";
import { join } from "path";

const audit = auditBppDeployablePackCompleteness();
console.log(JSON.stringify(audit, null, 2));

const casasProfile = loadPropertyProfile("adp_casas_del_xvi");
const casasResolved = resolveBrandPortfolioPosition("adp_casas_del_xvi", casasProfile, { query: {} });
const casasPub = existsSync(
  join(process.cwd(), "data/ai-demand-positioning/published/adp_casas_del_xvi/manifest.json")
);
const e2e = assertNewPropertyBppEndToEndPublication({
  propertyId: "adp_casas_del_xvi",
  publishedSnapshotExists: casasPub,
  bppAnalyticalState: BPP_CUSTOMER_STATE.BPP_READY_POPULATED_RANK_ONLY,
  bppPackEntryExists: true,
  resolverState:
    casasResolved?.bppCustomerState ||
    (casasResolved?.status === "READY"
      ? BPP_CUSTOMER_STATE.BPP_READY_POPULATED_RANK_ONLY
      : casasResolved?.status),
  rendererState:
    casasResolved?.status === "READY"
      ? BPP_CUSTOMER_STATE.BPP_READY_POPULATED_RANK_ONLY
      : casasResolved?.status,
});
console.log(JSON.stringify({ gate: ADP_NEW_PROPERTY_BPP_END_TO_END_PUBLICATION_GATE, ...e2e }, null, 2));

if (!audit.pass || !e2e.pass) {
  console.error(`FAIL ${ADP_BPP_DEPLOYABLE_PACK_COMPLETE}`);
  process.exit(1);
}
console.log(JSON.stringify({ ok: true, gate: ADP_BPP_DEPLOYABLE_PACK_COMPLETE }, null, 2));
