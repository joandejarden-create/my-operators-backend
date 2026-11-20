/**
 * Live Surfe gate for GDI contact reveal — credit check + in-flight dedupe.
 * Never logs or stores returned PII.
 */
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getSurfeCredits } from "../surfe/client.js";
import { surfeIncludeForChannel } from "./gdi-surfe-contact-provider-v1.js";

export const GDI_SURFE_LIVE_GATE_V1 = "gdi-surfe-live-gate-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const inFlight = new Set();

function accountingPath(env = process.env) {
  if (env.GDI_SURFE_ACCOUNTING_PATH) return path.resolve(env.GDI_SURFE_ACCOUNTING_PATH);
  return path.join(ROOT, "data/group-demand-intelligence/surfe-usage/gdi-live-accounting-v1.json");
}

export function contactRevealIdentityKey({ hotelId, opportunityId, contactName, channel }) {
  return createHash("sha256")
    .update(
      [
        String(hotelId || ""),
        String(opportunityId || ""),
        String(contactName || "").toLowerCase().trim(),
        String(channel || "both"),
      ].join("|"),
    )
    .digest("hex")
    .slice(0, 24);
}

export async function preflightSurfeCredits(channel = "both") {
  const include = surfeIncludeForChannel(channel);
  const credits = await getSurfeCredits();
  if (!credits.ok) {
    return {
      ok: false,
      error: "SURFE_CREDITS_UNAVAILABLE",
      message: "Could not read Surfe credit balance.",
      credits: null,
      include,
    };
  }
  const email = Number(credits.payload?.totalEmail ?? 0);
  const mobile = Number(credits.payload?.totalMobile ?? 0);
  if (include.email && email < 1) {
    return {
      ok: false,
      error: "INSUFFICIENT_CREDITS",
      message: "Insufficient Surfe email credits for this lookup.",
      credits: { totalEmail: email, totalMobile: mobile },
      include,
    };
  }
  if (include.mobile && mobile < 1) {
    return {
      ok: false,
      error: "INSUFFICIENT_CREDITS",
      message: "Insufficient Surfe mobile credits for this lookup.",
      credits: { totalEmail: email, totalMobile: mobile },
      include,
    };
  }
  return {
    ok: true,
    credits: { totalEmail: email, totalMobile: mobile },
    include,
  };
}

export function beginSurfeInFlight(key) {
  if (inFlight.has(key)) {
    return {
      ok: false,
      error: "LOOKUP_ALREADY_IN_PROGRESS",
      message: "A contact lookup for this person is already in progress.",
    };
  }
  inFlight.add(key);
  return { ok: true };
}

export function endSurfeInFlight(key) {
  inFlight.delete(key);
}

/**
 * Append non-PII accounting row (enrichment id, channel, credit deltas). Never store email/phone.
 */
export function appendSurfeLiveAccounting(entry = {}, env = process.env) {
  const file = accountingPath(env);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  let doc = { version: GDI_SURFE_LIVE_GATE_V1, entries: [] };
  if (fs.existsSync(file)) {
    try {
      doc = JSON.parse(fs.readFileSync(file, "utf8"));
    } catch {
      /* keep empty */
    }
  }
  const safe = {
    at: new Date().toISOString(),
    hotelId: entry.hotelId || null,
    opportunityId: entry.opportunityId || null,
    identityKey: entry.identityKey || null,
    channel: entry.channel || null,
    enrichmentId: entry.enrichmentId || null,
    providerHttpStatus: entry.providerHttpStatus ?? null,
    miss: entry.miss === true,
    ok: entry.ok === true,
    error: entry.error || null,
    creditsBefore: entry.creditsBefore || null,
    creditsAfter: entry.creditsAfter || null,
    // Explicit: no PII fields allowed
    emailReturned: entry.emailReturned === true,
    phoneReturned: entry.phoneReturned === true,
  };
  doc.entries = Array.isArray(doc.entries) ? doc.entries : [];
  doc.entries.push(safe);
  doc.updatedAt = safe.at;
  fs.writeFileSync(file, `${JSON.stringify(doc, null, 2)}\n`);
  return { path: file, entry: safe };
}
