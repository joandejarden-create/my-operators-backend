/**
 * Persists weekly KPI snapshots for My Brand Deals (per filter scope) so WoW trends
 * survive refresh and work across browsers. File-backed; not Airtable.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_FILE = path.join(__dirname, "..", "data", "brand-workspace-kpi-history.json");
const MAX_WEEKS_PER_SCOPE = 16;

function readStore() {
  try {
    if (!fs.existsSync(DATA_FILE)) return {};
    const raw = fs.readFileSync(DATA_FILE, "utf8");
    const data = JSON.parse(raw);
    return data && typeof data === "object" ? data : {};
  } catch {
    return {};
  }
}

function writeStore(store) {
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

/**
 * GET /api/brand-workspace/kpi-history?scopeKey=...
 */
export function getBrandWorkspaceKpiHistory(req, res) {
  try {
    const scopeKey = String(req.query.scopeKey || "default").slice(0, 512);
    const store = readStore();
    const weeks = store[scopeKey] && typeof store[scopeKey] === "object" ? store[scopeKey] : {};
    res.json({ success: true, scopeKey, weeks });
  } catch (err) {
    console.error("getBrandWorkspaceKpiHistory:", err);
    res.status(500).json({ success: false, error: err.message || "Internal error" });
  }
}

/**
 * POST /api/brand-workspace/kpi-history
 * Body: { scopeKey, weekKey, snapshot }
 */
export function postBrandWorkspaceKpiSnapshot(req, res) {
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
    const store = readStore();
    if (!store[scopeKey]) store[scopeKey] = {};
    store[scopeKey][weekKey] = {
      ...snapshot,
      savedAt: new Date().toISOString(),
    };
    pruneScopeWeeks(store[scopeKey]);
    writeStore(store);
    res.json({ success: true });
  } catch (err) {
    console.error("postBrandWorkspaceKpiSnapshot:", err);
    res.status(500).json({ success: false, error: err.message || "Internal error" });
  }
}
