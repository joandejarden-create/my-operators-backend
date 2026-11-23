import fs from "node:fs";
import { loadDealOpportunities } from "../lib/hotel-intelligence/deal-opportunities/hif-deal-opportunities-reader-v1.js";
import {
  hasAffirmativeOpenProcessEvidence,
  isExpiryOnlyWithoutOpenProcess,
  assessCommercialEligibility,
} from "../lib/hotel-intelligence/deal-opportunities/hif-deal-opportunities-commercial-eligibility-v1.js";

function loadEnv(p) {
  if (!fs.existsSync(p)) return;
  for (const line of fs.readFileSync(p, "utf8").split(/\r?\n/)) {
    const m = line.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
    if (!m || process.env[m[1]]) continue;
    let v = m[2];
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
      v = v.slice(1, -1);
    }
    process.env[m[1]] = v;
  }
}
loadEnv("C:/Dev/dealality/my-operators-backend/.env");

const data = await loadDealOpportunities();
const q = data.opportunities.filter((o) => o.group === "qualified");
console.log("qualifiedCount", q.length);
for (const o of q) {
  console.log("---", o.id, o.name);
  console.log("reason", o.classifyReason, "details", o.eligibilityDetails);
  console.log("why", String(o.why || "").slice(0, 280));
  console.log(
    "affirmative",
    hasAffirmativeOpenProcessEvidence(o.why || ""),
    "expiryOnly",
    isExpiryOnlyWithoutOpenProcess(o.why || ""),
  );
}
const check = [
  "find_cb2b_courtyard_denver_bct_2026",
  "find_ds_eci_v1_margaritaville_bh_kw_mct_early_2026",
  "find_ds_eci_v1_margaritaville_hollywood_mct_early_2026",
  "find_ds_opew_v2_bidwell_portland_bct_2026",
  "find_ds_opew_v2_ri_holtsville_bct_2026",
  "find_ds_cm_v1_hilton_spark_br_early_2026",
  "find_ds_cm_v1_justo_real_inn_early_2025",
];
for (const id of check) {
  const x = data.opportunities.find((r) => r.id === id);
  console.log("CHECK", id, "=>", x?.group, x?.classifyReason);
}

// Raw finding fields for Courtyard + Hollywood (via re-assess with reconstructed finding)
const { mapFinding } = {};
void mapFinding;
const denver = data.opportunities.find((r) => r.id === "find_cb2b_courtyard_denver_bct_2026");
const holly = data.opportunities.find((r) => r.id === "find_ds_eci_v1_margaritaville_hollywood_mct_early_2026");
console.log("DENVER_WHY_FULL", denver?.why);
console.log("DENVER_FACTS", denver?.facts);
console.log("HOLLY_WHY_FULL", holly?.why);
console.log(
  "HOLLY_assess_why_only",
  assessCommercialEligibility({
    finding: {
      findingId: holly?.id,
      fieldName: holly?.fieldName,
      valueObserved: holly?.why,
      notes: "",
    },
    inventory: { hotelName: holly?.name },
  }),
);
