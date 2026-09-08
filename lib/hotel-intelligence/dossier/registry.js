/**
 * Packet 2.6B dossier registry — filesystem fixtures only (no live research).
 * Only validated FULL_HOTEL_INTELLIGENCE_INVESTIGATION dossiers are served.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { toLibraryCard, validateDossier } from "./schema.js";
import { loadAndAdaptKgpvModulesDossier } from "./adapters/from-webhound-kgpv-modules.js";
import { DOSSIER_TYPE, DOSSIER_TYPE_RESEARCH_ADDENDUM } from "./statuses.js";
import { listPersistedResearchAddendums, getPersistedResearchAddendum } from "../research/addendum-store.js";
import { enrichReportForPublishing } from "./report-hotel-identity.js";
import { enforceClientSafeCustomerSurfaces } from "./client-safe/validate-client-safe.js";
import { bindFindingsList } from "./finding-binding.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE_DIR = path.resolve(__dirname, "../../../fixtures/hotel-intelligence/dossier");
const KGPV_FIXTURE = path.join(FIXTURE_DIR, "kgpv-full-hotel-intelligence-investigation-v1.json");

/** Customer-facing dossiers: identity enrich + finding_id binding + client-safe strip. */
function toCustomerFacingDossier(raw) {
  if (!raw) return null;
  const enriched = enrichReportForPublishing(raw);
  if (Array.isArray(enriched.key_findings) && enriched.key_findings.length) {
    enriched.key_findings = bindFindingsList(enriched.key_findings);
  }
  return enforceClientSafeCustomerSurfaces(enriched);
}

let cache = null;

function ensureKgpvFixture() {
  if (!fs.existsSync(FIXTURE_DIR)) fs.mkdirSync(FIXTURE_DIR, { recursive: true });
  if (!fs.existsSync(KGPV_FIXTURE)) {
    const dossier = loadAndAdaptKgpvModulesDossier();
    fs.writeFileSync(KGPV_FIXTURE, JSON.stringify(dossier, null, 2));
  }
}

function isRenderableDossier(raw) {
  if (!raw || typeof raw !== "object") return false;
  // Exclude contracts / page fingerprints / non-dossier JSON companions.
  if (raw.architecture && !raw.dossier_type) return false;
  if (String(raw.version || "").includes("golden-page-contract")) return false;
  // Packet 2.7-R2: hide superseded / pre-release invalid artifacts from customer serve.
  if (raw.customer_visible === false) return false;
  if (/SUPERSEDED/i.test(String(raw.status || ""))) return false;
  if (/SUPERSEDED/i.test(String(raw.investigation_status || ""))) return false;
  const v = validateDossier(raw);
  if (!v.ok) return false;
  if (!raw.hotel_name) return false;
  if (!Array.isArray(raw.sections) || raw.sections.length < 1) return false;
  const execParas = raw.executive_summary && raw.executive_summary.paragraphs;
  if (!Array.isArray(execParas) || execParas.length < 1) return false;
  if (raw.dossier_type === DOSSIER_TYPE_RESEARCH_ADDENDUM) {
    return raw.sections.length >= 2;
  }
  if (raw.dossier_type === DOSSIER_TYPE && raw.sections.length < 5) return false;
  return true;
}

function loadAll() {
  if (cache) return cache;
  ensureKgpvFixture();
  const files = fs
    .readdirSync(FIXTURE_DIR)
    .filter((f) => f.endsWith(".json") && !f.includes("contract"));
  const list = [];
  for (const file of files) {
    try {
      const raw = JSON.parse(fs.readFileSync(path.join(FIXTURE_DIR, file), "utf8"));
      if (!isRenderableDossier(raw)) {
        console.warn("[dossier-registry] skip non-renderable", file);
        continue;
      }
      list.push(raw);
    } catch (err) {
      console.warn("[dossier-registry] skip", file, err.message);
    }
  }
  cache = list;
  return cache;
}

export function clearDossierRegistryCache() {
  cache = null;
}

export function listDossiersForHotel({ hotelId, airtableRecordId } = {}) {
  const all = loadAll();
  const hid = String(hotelId || "").trim();
  const rid = String(airtableRecordId || "").trim();
  const fixtureCards = all
    .filter((d) => {
      if (rid && d.hotel_airtable_record_id === rid) return true;
      if (hid && (d.hotel_id === hid || d.hotel_airtable_record_id === hid)) return true;
      // KGPV fixture uses hotel_id "kgpv"
      if (rid === "recUNycnMwOVFX0hc" && d.hotel_airtable_record_id === "recUNycnMwOVFX0hc") {
        return true;
      }
      return false;
    })
    .map((d) => toLibraryCard(toCustomerFacingDossier(d)));
  const addendumCards = listPersistedResearchAddendums({ hotelId: hid || rid }).map((d) =>
    toLibraryCard(toCustomerFacingDossier(d))
  );
  return [...fixtureCards, ...addendumCards].sort((a, b) =>
    String(b.completed_at || "").localeCompare(String(a.completed_at || ""))
  );
}

export function getDossierById(dossierId) {
  const id = String(dossierId || "").trim();
  if (!id) return null;
  const persisted = getPersistedResearchAddendum(id);
  if (persisted) return toCustomerFacingDossier(persisted);
  const all = loadAll();
  // Prefer richest match if duplicates ever appear.
  const matches = all.filter((d) => d.dossier_id === id);
  if (!matches.length) return null;
  matches.sort(
    (a, b) =>
      Number(b.substantive_word_count || 0) - Number(a.substantive_word_count || 0) ||
      Number((b.sections || []).length) - Number((a.sections || []).length)
  );
  return toCustomerFacingDossier(matches[0]);
}

export function getDossierForHotelRecord(airtableRecordId) {
  const cards = listDossiersForHotel({ airtableRecordId });
  if (!cards.length) return null;
  // Prefer Full Hotel Intelligence Investigation over newer Research Addenda.
  const full = cards.find(
    (c) =>
      c.dossier_type === DOSSIER_TYPE ||
      String(c.dossier_id || "").includes("full_hi") ||
      /Full Hotel Intelligence/i.test(c.title || "")
  );
  return getDossierById((full || cards[0]).dossier_id);
}

export function rebuildKgpvDossierFixture() {
  clearDossierRegistryCache();
  const dossier = loadAndAdaptKgpvModulesDossier();
  const v = validateDossier(dossier);
  if (!v.ok) {
    throw new Error(`KGPV dossier invalid: ${v.errors.join(", ")}`);
  }
  if (!fs.existsSync(FIXTURE_DIR)) fs.mkdirSync(FIXTURE_DIR, { recursive: true });
  fs.writeFileSync(KGPV_FIXTURE, JSON.stringify(dossier, null, 2));
  clearDossierRegistryCache();
  return dossier;
}

export { KGPV_FIXTURE, FIXTURE_DIR, isRenderableDossier };
