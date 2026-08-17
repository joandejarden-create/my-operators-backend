/**
 * Print Google search URLs for brand/operator reference material discovery.
 *
 *   npm run partner-reference:search -- --brand "Fairfield" --parent marriott
 *   npm run partner-reference:search -- --operator "Arbor Lodging" --domain arborlodging.com
 */
import {
  buildGoogleSearchUrls,
  GENERIC_OPERATOR_SEARCH_PATTERNS,
  DEVELOPMENT_PORTALS,
} from "../api/lib/partner-development-portal-registry.js";

function arg(name) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : null;
}

const brand = arg("--brand");
const parent = arg("--parent");
const operator = arg("--operator");
const domain = arg("--domain");

if (brand) {
  console.log(`\nBrand: ${brand}${parent ? ` (parent: ${parent})` : ""}\n`);
  for (const row of buildGoogleSearchUrls(brand, parent)) {
    console.log(row.query);
    console.log(row.url);
    console.log("");
  }
} else if (operator) {
  const patterns = GENERIC_OPERATOR_SEARCH_PATTERNS.map((p) =>
    p.replace(/\[operator\]/gi, operator).replace(/\[domain\]/gi, domain || "example.com")
  );
  console.log(`\nOperator: ${operator}\n`);
  for (const q of patterns) {
    console.log(q);
    console.log(`https://www.google.com/search?q=${encodeURIComponent(q)}`);
    console.log("");
  }
} else {
  console.log("Development portals:\n");
  for (const p of DEVELOPMENT_PORTALS) {
    console.log(`- ${p.parentCompany}: ${p.developmentPortal}`);
  }
  console.log('\nUsage: --brand "Fairfield" [--parent marriott]  OR  --operator "Arbor Lodging" [--domain arborlodging.com]');
}
