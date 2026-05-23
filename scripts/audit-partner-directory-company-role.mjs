/**
 * Audit /api/partner-directory companyRole values vs filter.
 */
import "../load-env.js";
import { companyRoleFromEcosystemField } from "../lib/company-role-normalize.js";

const port = process.env.PORT || 3000;
const base = process.env.PARTNER_DIRECTORY_AUDIT_URL || `http://127.0.0.1:${port}`;

const res = await fetch(`${base}/api/partner-directory`, { cache: "no-store" });
if (!res.ok) {
  console.error("Fetch failed", res.status, await res.text());
  process.exit(1);
}
const { companies = [] } = await res.json();

const byRole = new Map();
for (const c of companies) {
  const role = (c.companyRole || "").trim() || "(empty)";
  byRole.set(role, (byRole.get(role) || 0) + 1);
}

console.log("API companies:", companies.length);
console.log("By companyRole:");
for (const [role, count] of [...byRole.entries()].sort((a, b) => b[1] - a[1])) {
  console.log(`  ${role}: ${count}`);
}

const both = companies.filter((c) => c.companyRole === "Both");
console.log("\nBoth (filter match):", both.length);
console.log(both.slice(0, 8).map((c) => c.name).join(", "));
