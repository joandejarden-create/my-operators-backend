/**
 * Audit Demand Generator modules for hotel/city/org hardcodes in production logic.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const FORBIDDEN = Object.freeze({
  HOTEL: [/bethesda\s*marriott/i, /renaissance\s*new\s*york/i, /cambridge\s*beaches/i],
  CITY: [], // city strings in seed fixtures OK; production modules use profile.label
  ORGANIZATION: [],
  PROGRAM: [],
  DOMAIN: [],
  PERSON: [/jessica\.mitchell/i, /jamie\s*mccormick/i],
});

/** Production logic files only (exclude fixtures, canary seeds, reports). */
const PRODUCTION_FILES = [
  "constants.js",
  "entities.js",
  "fit-relevance.js",
  "qualify-promote.js",
  "discovery.js",
  "airtable-field-map.js",
  "airtable-client.js",
  "airtable-stores.js",
  "lodging-evidence.js",
  "index.js",
  "hotel-specific-audit.js",
];

export function auditDemandGeneratorHotelSpecificLogic(dir = __dirname) {
  const findings = [];
  for (const file of PRODUCTION_FILES) {
    const full = path.join(dir, file);
    if (!fs.existsSync(full)) continue;
    const text = fs.readFileSync(full, "utf8");
    for (const [category, patterns] of Object.entries(FORBIDDEN)) {
      for (const re of patterns) {
        if (re.test(text)) {
          findings.push({ file, category, pattern: String(re) });
        }
      }
    }
    // Explicit hotel census IDs must not appear in production logic
    if (/recLuxvwwxID7U2B8|recG66DQJKP2c0UNh|recIwaP1etgx2g9nA/.test(text)) {
      if (file !== "hotel-specific-audit.js") {
        findings.push({
          file,
          category: "HOTEL",
          pattern: "hardcoded_census_hotel_id",
        });
      }
    }
  }
  return {
    ok: findings.length === 0,
    findings,
    categories: {
      HOTEL: findings.some((f) => f.category === "HOTEL") ? "YES" : "NO",
      CITY: findings.some((f) => f.category === "CITY") ? "YES" : "NO",
      ORGANIZATION: findings.some((f) => f.category === "ORGANIZATION")
        ? "YES"
        : "NO",
      PROGRAM: findings.some((f) => f.category === "PROGRAM") ? "YES" : "NO",
      DOMAIN: findings.some((f) => f.category === "DOMAIN") ? "YES" : "NO",
      PERSON: findings.some((f) => f.category === "PERSON") ? "YES" : "NO",
    },
  };
}
