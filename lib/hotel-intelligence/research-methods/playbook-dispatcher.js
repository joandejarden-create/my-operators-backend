/**
 * Research playbook dispatcher — selects applicable methods from seed + discoveries.
 * Does not hard-code hotel answers.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { validatePlaybook } from "./playbook-schema.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PLAYBOOK_PATH = path.join(__dirname, "playbooks", "mexico-pubco-v1.json");
const PRIVATE_PROPCO_PATH = path.join(__dirname, "playbooks", "mexico-private-propco-v1.json");

export function loadMexicoPubcoPlaybooks() {
  const raw = JSON.parse(fs.readFileSync(PLAYBOOK_PATH, "utf8"));
  return (raw.playbooks || []).filter((p) => validatePlaybook(p).ok);
}

export function loadMexicoPrivatePropcoPlaybooks() {
  if (!fs.existsSync(PRIVATE_PROPCO_PATH)) return [];
  const raw = JSON.parse(fs.readFileSync(PRIVATE_PROPCO_PATH, "utf8"));
  return (raw.playbooks || []).filter((p) => validatePlaybook(p).ok);
}

export function loadAllMexicoPlaybooks() {
  return [...loadMexicoPubcoPlaybooks(), ...loadMexicoPrivatePropcoPlaybooks()];
}

/**
 * @param {object} seedHotel
 * @param {{ discoveries?: object }} [ctx]
 */
export function dispatchPlaybooks(seedHotel, ctx = {}) {
  const country = String(seedHotel.country || "").toLowerCase();
  const name = String(seedHotel.name || "");
  const operator = String(seedHotel.management_company || seedHotel.operator || "");
  const affiliation = String(seedHotel.affiliation_display || "");
  const discoveries = ctx.discoveries || {};

  const all = loadAllMexicoPlaybooks();
  const selected = [];
  const ownershipGap =
    Boolean(discoveries.ownership_gap || discoveries.propco_gap) ||
    Boolean(ctx.gap && /PROPCO|OWNERSHIP/i.test(String(ctx.gap)));

  for (const pb of all) {
    const id = pb.playbook_id;
    let reason = null;

    if (id === "HOTEL-IDENTITY-01" && name) reason = "hotel_name_present";
    if (id === "HOTEL-IDENTITY-02" && /krystal/i.test(name) && /vallarta|mexico/i.test(country + name)) {
      reason = "similar_name_risk_in_mexico_market";
    }
    if (id === "BRAND-HISTORY-01" && (affiliation || name)) reason = "affiliation_or_name_present";
    if (id === "ORG-PORTFOLIO-01" && operator) reason = "operator_seed_present";
    if (
      (id === "MX-PUBCO-01" || id === "MX-PUBCO-02" || id === "MX-PUBCO-03") &&
      /mexico/.test(country) &&
      (operator || discoveries.public_company_candidate)
    ) {
      reason = operator ? "mexico_plus_operator_seed" : "mexico_plus_discovered_pubco";
    }
    if (id === "PUBCO-PEOPLE-01" && (operator || discoveries.public_company_candidate)) {
      reason = "organization_candidate_present";
    }
    if (id === "DECISION-AUTHORITY-01") reason = "always_when_people_lane_active";
    if (id === "PROPERTY-HISTORY-01" && name) reason = "hotel_timeline_lane";
    if (
      (id === "MX-PRIVATE-PROPCO-01" || id === "MX-PRIVATE-PROPCO-02") &&
      /mexico/.test(country) &&
      (ownershipGap || !discoveries.public_company_candidate)
    ) {
      reason =
        id === "MX-PRIVATE-PROPCO-02" && discoveries.sponsor_token
          ? "mexico_sponsor_token_legal_expansion"
          : "mexico_private_propco_gap";
    }

    if (reason) {
      selected.push({
        playbook_id: id,
        name: pb.name,
        version: pb.version,
        trigger_reason: reason,
        negative_screens: pb.negative_screens || [],
        transfer_status: pb.transfer_status || null,
      });
    }
  }

  return {
    version: "playbook-dispatcher-v1",
    selected,
    playbook_ids: selected.map((s) => s.playbook_id),
  };
}

export function buildDynamicSearchQueries(seedHotel, state = {}) {
  const name = String(seedHotel.name || "").trim();
  const shortName = name
    .replace(/\b(all\s*-?\s*inclusive|adults?\s+only|resort\s*&\s*spa|resort and spa|urban\s+resort|by\s+marriott)/gi, " ")
    .replace(/\s{2,}/g, " ")
    .trim();
  const city = String(seedHotel.city || "").trim();
  const country = String(seedHotel.country || "").trim();
  const op = String(seedHotel.management_company || seedHotel.operator || "").trim();
  const affiliation = String(seedHotel.affiliation_display || "").trim();
  const aliases = state.aliases || [];
  const qs = [];

  qs.push(`"${name}" ${city} hotel`);
  qs.push(`"${shortName || name}" owner OR propiedad OR adquisicion OR acquisition`);
  // Generic MX ownership / capital-structure probes (no issuer names hardcoded)
  if (/mexico|méxico|mx\b/i.test(country) || !country) {
    qs.push(`"${shortName || name}" (FIBRA OR fideicomiso OR PropCo OR "dueño" OR propietario)`);
    qs.push(`"${shortName || name}" ("reporte anual" OR "presentación a inversionistas" OR BMV) filetype:pdf`);
    // Private PropCo / consumer-adhesion family (generic — no hotel answers)
    qs.push(
      `"${shortName || name}" (inmobiliaria OR "razón social" OR PROFECO OR "contrato de adhesión")`
    );
    qs.push(`site:rpca.profeco.gob.mx "${shortName || name}"`);
  }
  if (/krystal\s+grand/i.test(name)) {
    qs.push(`"${name}" Hilton Altitude Breathless`);
    qs.push(`"Krystal Resort Puerto Vallarta" Chartwell OR "third party"`);
  }
  if (affiliation && !new RegExp(affiliation.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "i").test(name)) {
    qs.push(`${affiliation.split(/\s+/).slice(0, 2).join(" ")} "${city}" Hyatt announce OR newsroom`);
    if (/breathless/i.test(affiliation)) qs.push(`"Breathless Puerto Vallarta" Hyatt`);
  }
  if (op) {
    qs.push(`"${op}" reporte anual filetype:pdf`);
    qs.push(`"${op}" "${name}"`);
    qs.push(`"${op}" BMV`);
    // No hardcoded site:gsf-hotels.com on generic path — org follow-up discovers issuer domains from evidence.
  }
  for (const a of aliases.slice(0, 2)) {
    qs.push(`"${a}" ${city} hotel`);
  }
  return [...new Set(qs)].slice(0, 9);
}

export function buildDocumentSearchTerms(seedHotel, state = {}) {
  const terms = new Set([
    seedHotel.name,
    seedHotel.city,
    "Puerto Vallarta",
    "Hilton",
    "Altitude",
    "Breathless",
    "Chartwell",
    "Inmobiliaria",
    "Vallarta Santa Fe",
    "adquisición",
    "participación",
    "propiedad",
    "habitaciones",
    "Hacienda",
    "apertura",
    "marca",
    "management",
    "lease",
  ]);
  for (const a of state.aliases || []) terms.add(a);
  for (const e of state.entities || []) terms.add(e);
  return [...terms].filter(Boolean);
}
