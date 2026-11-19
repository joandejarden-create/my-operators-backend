/**
 * Brand Explorer — field content roles (numeric vs narrative).
 * Prevents numeric KPI validators from erasing valid commercial summaries.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const BE_FIELD_CONTENT_ROLES_V1 = "BE_FIELD_CONTENT_ROLES_V1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const CONTRACT_REL = "configs/brand-explorer-field-content-roles-contract-v1.json";

const NUMERIC_ROLES = new Set([
  "number",
  "percentage",
  "percent",
  "amount",
  "ratio",
  "currency",
]);

const COMMERCIAL_SUMMARY_SLOTS = Object.freeze([
  "commercial.kpi.channels",
  "commercial.kpi.campaigns",
  "commercial.kpi.b2b",
  "commercial.kpi.lens",
]);

let _contract;

export function loadFieldContentRolesContract() {
  if (_contract) return _contract;
  const raw = fs.readFileSync(path.join(ROOT, CONTRACT_REL), "utf8");
  _contract = JSON.parse(raw);
  return _contract;
}

export function isNumericContentRole(role) {
  const r = String(role || "number").trim().toLowerCase();
  return NUMERIC_ROLES.has(r);
}

export function resolveContentRoleForSlot(slotKey, fallbackRole = "text") {
  const c = loadFieldContentRolesContract();
  const key = String(slotKey || "").trim();
  const entry = c.slotRoles && c.slotRoles[key];
  if (entry && entry.role) return String(entry.role);
  if (COMMERCIAL_SUMMARY_SLOTS.includes(key)) return "short_commercial_summary";
  if (/\.kpi\./i.test(key)) return fallbackRole === "text" ? "number" : fallbackRole;
  return fallbackRole;
}

export function canonicalLabelForCommercialSummarySlot(slotKey) {
  const c = loadFieldContentRolesContract();
  const entry = c.slotRoles && c.slotRoles[String(slotKey || "").trim()];
  return entry && entry.canonicalLabel ? String(entry.canonicalLabel) : null;
}

export function isCommercialSummarySlot(slotKey) {
  return COMMERCIAL_SUMMARY_SLOTS.includes(String(slotKey || "").trim());
}

export function isInternalResearchInstructionText(value) {
  const t = String(value || "").trim();
  if (!t) return false;
  return /diligence:|KPI lens:|request (current|negotiated|channel)|confirm definitions|do not invent|do not rely on generated|KPI figures are not disclosed|Focus this card|RESEARCH_REQUIRED|owner diligence:|obtain brand\.com|ask which brand campaigns|define the primary KPI stack|account concentration/i.test(
    t,
  );
}

/**
 * Compact-numeric detector for genuinely numeric KPI roles only.
 * Must NOT be used to reject short commercial summaries.
 */
export function looksLikeDiligenceOrParagraphForNumericKpi(value) {
  const t = String(value || "").trim();
  if (!t) return false;
  if (isInternalResearchInstructionText(t)) return true;
  if (/diligence:|request current|confirm definitions|Use disclosure\/scorecards|from .+ development/i.test(t)) {
    return true;
  }
  if (t.length > 64 || t.split(/\s+/).length > 10) return true;
  return false;
}

export function shouldApplyNumericKpiUnavailableGate(role, value) {
  if (!isNumericContentRole(role)) return false;
  return looksLikeDiligenceOrParagraphForNumericKpi(value);
}

export function commercialSummarySlotKeys() {
  return [...COMMERCIAL_SUMMARY_SLOTS];
}
