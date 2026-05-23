/**
 * Weekly KPI snapshots for brand and owner deal workspaces.
 * Storage: Airtable table when AIRTABLE_TABLE_WORKSPACE_KPI_SNAPSHOTS is set; else file-backed JSON.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_FILE = path.join(__dirname, "..", "data", "brand-workspace-kpi-history.json");
const MAX_WEEKS_PER_SCOPE = 16;

const KPI_TABLE =
  process.env.AIRTABLE_TABLE_WORKSPACE_KPI_SNAPSHOTS || "Workspace KPI Snapshots";
const FIELD_SCOPE = process.env.AIRTABLE_KPI_FIELD_SCOPE_KEY || "Scope Key";
const FIELD_PERSONA = process.env.AIRTABLE_KPI_FIELD_PERSONA || "Persona";
const FIELD_WEEK = process.env.AIRTABLE_KPI_FIELD_WEEK_KEY || "Week Key";
const FIELD_SNAPSHOT = process.env.AIRTABLE_KPI_FIELD_SNAPSHOT_JSON || "Snapshot JSON";
const FIELD_SAVED_AT = process.env.AIRTABLE_KPI_FIELD_SAVED_AT || "Saved At";

function useAirtableStore() {
  return Boolean(process.env.AIRTABLE_API_KEY && process.env.AIRTABLE_BASE_ID && KPI_TABLE);
}

function getAirtableBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) throw new Error("Airtable not configured");
  return new Airtable({ apiKey }).base(baseId);
}

function readFileStore() {
  try {
    if (!fs.existsSync(DATA_FILE)) return {};
    const raw = fs.readFileSync(DATA_FILE, "utf8");
    const data = JSON.parse(raw);
    return data && typeof data === "object" ? data : {};
  } catch {
    return {};
  }
}

function writeFileStore(store) {
  fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
  fs.writeFileSync(DATA_FILE, JSON.stringify(store, null, 0), "utf8");
}

function pruneScopeWeeks(scopeObj) {
  if (!scopeObj || typeof scopeObj !== "object") return;
  const keys = Object.keys(scopeObj).sort();
  if (keys.length <= MAX_WEEKS_PER_SCOPE) return;
  keys.slice(0, keys.length - MAX_WEEKS_PER_SCOPE).forEach((k) => {
    delete scopeObj[k];
  });
}

function parsePersonaFromScope(scopeKey) {
  const parts = String(scopeKey || "").split("|");
  if (parts[0] === "v2" && (parts[1] === "owner" || parts[1] === "brand")) return parts[1];
  return "brand";
}

function escapeFormulaString(s) {
  return String(s || "").replace(/\\/g, "\\\\").replace(/'/g, "\\'");
}

async function loadWeeksFromAirtable(scopeKey) {
  const base = getAirtableBase();
  const formula = `{${FIELD_SCOPE}} = '${escapeFormulaString(scopeKey)}'`;
  const records = await base(KPI_TABLE)
    .select({ filterByFormula: formula, pageSize: 100 })
    .all();
  const weeks = {};
  for (const rec of records) {
    const f = rec.fields || {};
    const wk = String(f[FIELD_WEEK] || "").trim();
    if (!wk) continue;
    let snap = null;
    try {
      const raw = f[FIELD_SNAPSHOT];
      snap = typeof raw === "string" ? JSON.parse(raw) : raw;
    } catch {
      snap = null;
    }
    if (snap && typeof snap === "object") {
      weeks[wk] = {
        ...snap,
        savedAt: f[FIELD_SAVED_AT] || snap.savedAt || null,
        _recordId: rec.id,
      };
    }
  }
  return weeks;
}

async function upsertWeekInAirtable(scopeKey, weekKey, snapshot) {
  const base = getAirtableBase();
  const persona = snapshot.persona || parsePersonaFromScope(scopeKey);
  const formula = `AND({${FIELD_SCOPE}} = '${escapeFormulaString(scopeKey)}', {${FIELD_WEEK}} = '${escapeFormulaString(weekKey)}')`;
  const existing = await base(KPI_TABLE)
    .select({ filterByFormula: formula, maxRecords: 1 })
    .firstPage();
  const savedAt = new Date().toISOString();
  const payload = {
    [FIELD_SCOPE]: scopeKey,
    [FIELD_PERSONA]: persona,
    [FIELD_WEEK]: weekKey,
    [FIELD_SNAPSHOT]: JSON.stringify({ ...snapshot, savedAt }),
    [FIELD_SAVED_AT]: savedAt,
  };
  if (existing.length > 0) {
    await base(KPI_TABLE).update(existing[0].id, payload);
  } else {
    await base(KPI_TABLE).create(payload);
  }
}

/**
 * GET /api/brand-workspace/kpi-history?scopeKey=...
 */
export async function getBrandWorkspaceKpiHistory(req, res) {
  try {
    const scopeKey = String(req.query.scopeKey || "default").slice(0, 512);
    let weeks = {};
    let storage = "file";

    if (useAirtableStore()) {
      try {
        weeks = await loadWeeksFromAirtable(scopeKey);
        storage = "airtable";
      } catch (err) {
        console.warn("[kpi-history] Airtable read failed, using file store:", err.message);
        const store = readFileStore();
        weeks = store[scopeKey] && typeof store[scopeKey] === "object" ? store[scopeKey] : {};
        storage = "file-fallback";
      }
    } else {
      const store = readFileStore();
      weeks = store[scopeKey] && typeof store[scopeKey] === "object" ? store[scopeKey] : {};
    }

    res.json({ success: true, scopeKey, weeks, storage });
  } catch (err) {
    console.error("getBrandWorkspaceKpiHistory:", err);
    res.status(500).json({ success: false, error: err.message || "Internal error" });
  }
}

/**
 * POST /api/brand-workspace/kpi-history
 * Body: { scopeKey, weekKey, snapshot }
 */
export async function postBrandWorkspaceKpiSnapshot(req, res) {
  try {
    const scopeKey = String(req.body?.scopeKey || "").slice(0, 512);
    const weekKey = String(req.body?.weekKey || "").slice(0, 32);
    const snapshot = req.body?.snapshot;
    if (!scopeKey || !weekKey || !snapshot || typeof snapshot !== "object") {
      return res.status(400).json({
        success: false,
        error: "scopeKey, weekKey, and snapshot object are required",
      });
    }
    const toSave = {
      ...snapshot,
      savedAt: new Date().toISOString(),
    };

    let storage = "file";
    if (useAirtableStore()) {
      try {
        await upsertWeekInAirtable(scopeKey, weekKey, toSave);
        storage = "airtable";
      } catch (err) {
        console.warn("[kpi-history] Airtable write failed, using file store:", err.message);
        const store = readFileStore();
        if (!store[scopeKey]) store[scopeKey] = {};
        store[scopeKey][weekKey] = toSave;
        pruneScopeWeeks(store[scopeKey]);
        writeFileStore(store);
        storage = "file-fallback";
      }
    } else {
      const store = readFileStore();
      if (!store[scopeKey]) store[scopeKey] = {};
      store[scopeKey][weekKey] = toSave;
      pruneScopeWeeks(store[scopeKey]);
      writeFileStore(store);
    }

    res.json({ success: true, storage });
  } catch (err) {
    console.error("postBrandWorkspaceKpiSnapshot:", err);
    res.status(500).json({ success: false, error: err.message || "Internal error" });
  }
}
