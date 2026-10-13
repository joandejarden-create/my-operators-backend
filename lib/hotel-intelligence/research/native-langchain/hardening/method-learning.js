/**
 * Packet 2.8C-2 — Offline method learning from historical Webhound artifacts.
 * NEVER injects gold answers / hotel-specific expected conclusions into live research.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../../../../");

export const METHOD_LEARNING_VERSION = "method-learning-offline-v1";

/**
 * General transferable methods — no hotel-specific gold conclusions.
 */
export const GENERAL_METHODS = Object.freeze([
  {
    id: "mx_pubco_issuer_annual_report",
    archetype: "MX_PUBLIC_COMPANY_OWNERSHIP",
    method:
      "Mexican public-company hotel ownership often requires searching issuer annual reports and subsidiary/portfolio sections before generic web search.",
    transfers_to: ["kgpv", "other_mx_pubco"],
  },
  {
    id: "mx_hecho_relevante_transactions",
    archetype: "MX_PUBLIC_COMPANY_TRANSACTIONS",
    method:
      "Material hotel acquisitions/dispositions for MX issuers often appear as BMV hechos relevantes or IR press before secondary trade press.",
    transfers_to: ["mx_pubco"],
  },
  {
    id: "bm_tourism_order_development",
    archetype: "BM_PRIVATE_RESORT_DEVELOPMENT",
    method:
      "Bermuda resort renovations/expansions often surface via Tourism Authority orders and local reputable press; deed PropCo may remain access-limited.",
    transfers_to: ["cambridge", "bm_private"],
  },
  {
    id: "brand_conversion_first_party",
    archetype: "BRAND_REFLAG",
    method:
      "Brand conversion chronology should prioritize brand/company announcements and hotel first-party pages; secondary press is a discovery lead to those primaries.",
    transfers_to: ["voco_cancun", "sheraton_gdl", "kgpv"],
  },
  {
    id: "operator_vs_owner_separation",
    archetype: "OPERATOR_PLATFORM",
    method:
      "Operator announcements must not be treated as ownership; seek separate owner-package naming and PropCo/registry evidence.",
    transfers_to: ["sheraton_gdl", "voco_cancun", "kgpv"],
  },
  {
    id: "person_requires_in_profile",
    archetype: "PEOPLE",
    method:
      "Person verification requires a person-level professional profile (/in/) or first-party bio with org/role match — company pages and search directories are insufficient.",
    transfers_to: ["all"],
  },
]);

/**
 * Scan historical deep-research fixtures for SOURCE FAMILIES / DOC CLASSES only.
 * Strips hotel-specific gold conclusions.
 */
export function learnMethodsFromHistoricalFixtures({ fixturePaths = [] } = {}) {
  const discovered = [];
  for (const rel of fixturePaths) {
    const abs = path.isAbsolute(rel) ? rel : path.join(ROOT, rel);
    if (!fs.existsSync(abs)) continue;
    let raw = "";
    try {
      raw = fs.readFileSync(abs, "utf8");
    } catch {
      continue;
    }
    const families = [];
    if (/reporte anual|bmv|cnbv|hechos relevantes/i.test(raw)) families.push("mx_issuer_filing");
    if (/tourism order|bermuda tourism/i.test(raw)) families.push("bm_tourism_authority");
    if (/linkedin\.com\/in\//i.test(raw)) families.push("person_linkedin_in");
    if (/acquisition|acquired|sale of|compr[oó]/i.test(raw)) families.push("transaction_press_or_filing");
    if (/annual report|\.pdf/i.test(raw)) families.push("pdf_document_class");
    discovered.push({
      fixture: rel,
      source_families_observed: [...new Set(families)],
      // Explicitly omit any expected ownership/brand conclusions.
      gold_answers_excluded: true,
    });
  }

  return {
    version: METHOD_LEARNING_VERSION,
    mode: "TRAINING_METHOD_ANALYSIS",
    gold_leakage_prohibited: true,
    general_methods: GENERAL_METHODS,
    fixture_scans: discovered,
    live_research_may_use: GENERAL_METHODS.map((m) => m.method),
    live_research_must_not_use: [
      "exact expected gold answers",
      "hotel-specific document URLs solely because they prove a gold field",
      "GoldenHotelReference values as research context",
    ],
  };
}

export function methodsSafeForLivePrompt(jurisdiction = "", domain = "") {
  return GENERAL_METHODS.filter((m) => {
    if (/MX/i.test(jurisdiction) && /mx_/i.test(m.id)) return true;
    if (/BM|BERMUDA/i.test(jurisdiction) && /bm_/i.test(m.id)) return true;
    if (/BRAND/i.test(domain) && m.id.includes("brand")) return true;
    if (/PEOPLE/i.test(domain) && m.id.includes("person")) return true;
    if (/OPERATOR|OWNER/i.test(domain) && m.id.includes("operator_vs_owner")) return true;
    return m.transfers_to.includes("all");
  }).map((m) => m.method);
}
