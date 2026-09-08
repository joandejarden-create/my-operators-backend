/**
 * Packet 2.6C-R2 — persist Research Addendum dossiers under research data root.
 * Separated from fixture registry so historical Full Investigation fixtures stay frozen.
 */

import fs from "node:fs";
import path from "node:path";
import { resolveDataRoot, ensureDir, readJsonFile, writeJsonFile } from "../local-store.js";
import { validateDossier, refreshDossierCounts } from "../dossier/schema.js";
import { validateIntelligenceReport } from "../dossier/report-contracts.js";
import { DOSSIER_TYPE_RESEARCH_ADDENDUM } from "../dossier/statuses.js";

function addendaRoot(env = process.env) {
  const root = path.join(resolveDataRoot(env), "research", "addenda");
  ensureDir(root);
  return root;
}

function indexPath(env) {
  return path.join(addendaRoot(env), "index.json");
}

function dossierPath(dossierId, env) {
  const safe = String(dossierId || "").replace(/[^a-zA-Z0-9_-]/g, "_");
  return path.join(addendaRoot(env), `${safe}.json`);
}

function readIndex(env) {
  return readJsonFile(indexPath(env), {
    version: "hi-research-addenda-index-v1",
    dossiers: [],
    updated_at: null,
  });
}

export function persistResearchAddendum(dossier, env = process.env) {
  if (!dossier || !dossier.dossier_id) {
    const err = new Error("addendum_dossier_id_required");
    err.code = "addendum_dossier_id_required";
    throw err;
  }
  dossier.dossier_type = DOSSIER_TYPE_RESEARCH_ADDENDUM;
  refreshDossierCounts(dossier);
  const shape = validateDossier(dossier);
  const contract = validateIntelligenceReport(dossier, { template_id: dossier.template_id });
  const errors = [...(shape.errors || []), ...(contract.ok ? [] : contract.errors || [])];
  // Shape already checked type; contract is authoritative for addendum content.
  const filtered = errors.filter((e) => e !== "dossier_type_invalid");
  if (filtered.length) {
    const err = new Error(`addendum_invalid:${filtered.join(",")}`);
    err.code = "addendum_invalid";
    err.errors = filtered;
    throw err;
  }
  writeJsonFile(dossierPath(dossier.dossier_id, env), dossier);
  const idx = readIndex(env);
  const card = {
    dossier_id: dossier.dossier_id,
    hotel_id: dossier.hotel_id,
    hotel_airtable_record_id: dossier.hotel_airtable_record_id,
    hotel_name: dossier.hotel_name,
    title: dossier.title,
    template_id: dossier.template_id || null,
    research_request_id: dossier.research_request_id || null,
    research_run_id: dossier.research_run_id || null,
    parent_report_id: dossier.parent_report_id || null,
    completed_at: dossier.completed_at,
    source_count: dossier.source_count,
    finding_count: dossier.finding_count,
    open_question_count: dossier.open_question_count,
  };
  idx.dossiers = (idx.dossiers || []).filter((d) => d.dossier_id !== dossier.dossier_id);
  idx.dossiers.push(card);
  idx.updated_at = new Date().toISOString();
  writeJsonFile(indexPath(env), idx);
  return dossier;
}

export function getPersistedResearchAddendum(dossierId, env = process.env) {
  const id = String(dossierId || "").trim();
  if (!id) return null;
  const file = dossierPath(id, env);
  if (!fs.existsSync(file)) return null;
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return null;
  }
}

export function listPersistedResearchAddendums({ hotelId } = {}, env = process.env) {
  const hid = String(hotelId || "").trim();
  const idx = readIndex(env);
  const cards = (idx.dossiers || []).filter((d) => {
    if (!hid) return true;
    return d.hotel_id === hid || d.hotel_airtable_record_id === hid;
  });
  const out = [];
  for (const c of cards) {
    const full = getPersistedResearchAddendum(c.dossier_id, env);
    if (full) out.push(full);
  }
  return out;
}
