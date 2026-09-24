/**
 * Shared Airtable client for Demand Generator graph (canonical GDI base).
 */

import Airtable from "airtable";
import {
  getGdiOpportunitiesAirtableBaseId,
  assertNotLegacyMvpCanonicalBase,
} from "../../decision-outcomes/airtable-base.js";
import { airtableRequestTimeoutMs } from "../../http/with-timeout.js";
import { DG_SCHEMA_VERSION } from "./constants.js";

export function getDgAirtableToken() {
  return process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
}

export function getDgAirtableBaseId() {
  return getGdiOpportunitiesAirtableBaseId();
}

export function isDgAirtableConfigured() {
  return Boolean(getDgAirtableToken() && getDgAirtableBaseId());
}

export function getDgBase() {
  const apiKey = getDgAirtableToken();
  const baseId = getDgAirtableBaseId();
  if (!apiKey || !baseId) {
    const err = new Error("gdi_demand_generators_airtable_not_configured");
    err.code = "gdi_demand_generators_airtable_not_configured";
    throw err;
  }
  assertNotLegacyMvpCanonicalBase(baseId, { surface: "gdi_demand_generators" });
  return new Airtable({
    apiKey,
    requestTimeout: airtableRequestTimeoutMs(),
  }).base(baseId);
}

export function omitEmpty(fields) {
  const out = {};
  for (const [k, v] of Object.entries(fields)) {
    if (v === undefined || v === null || v === "") continue;
    out[k] = v;
  }
  return out;
}

export function asSelect(value, allowed) {
  const v = value == null ? "" : String(value).trim();
  if (!v) return undefined;
  if (Array.isArray(allowed) && allowed.length && !allowed.includes(v)) {
    return undefined;
  }
  return v;
}

export function joinLines(arr) {
  if (!Array.isArray(arr) || !arr.length) return undefined;
  return arr.map((x) => String(x || "").trim()).filter(Boolean).join("\n");
}

export function jsonField(obj) {
  if (obj == null) return undefined;
  try {
    return JSON.stringify(obj);
  } catch {
    return undefined;
  }
}

export { DG_SCHEMA_VERSION };
