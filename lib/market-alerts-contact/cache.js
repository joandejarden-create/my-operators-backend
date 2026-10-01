/**
 * Local JSON for Market Alerts — stakeholder workflow state ONLY.
 * Never persists Surfe-derived email / phone / LinkedIn / provider IDs / payloads.
 * CONTACT_CACHE_DAYS must not be used for Surfe contact detail caching.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { assertContactPersistenceWritable } from "./persistence.js";
import { stripSurfeContactDetails } from "./stakeholder-schema.js";
import { logContactOp } from "./safe-log.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const DEFAULT_DIR = path.join(ROOT, "data", "market-alerts", "contact-enrichment");

function cacheDir() {
  return process.env.MARKET_ALERTS_CONTACT_CACHE_DIR
    ? path.resolve(process.env.MARKET_ALERTS_CONTACT_CACHE_DIR)
    : DEFAULT_DIR;
}

function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

function alertContactsPath(alertId) {
  const safe = String(alertId || "unknown").replace(/[^a-zA-Z0-9_-]/g, "_");
  return path.join(cacheDir(), `alert-${safe}.json`);
}

function personCachePath(key) {
  const safe = String(key || "unknown").replace(/[^a-zA-Z0-9_-]/g, "_");
  return path.join(cacheDir(), `person-${safe}.json`);
}

function canWrite() {
  return assertContactPersistenceWritable().writable;
}

/** Safe identity-only person key — no provider IDs. */
function personKey({ personName, companyName } = {}) {
  const n = String(personName || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "");
  const c = String(companyName || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "");
  return `p:${n}__c:${c}`;
}

function sanitizeAlertPayload(payload = {}) {
  const cleaned = stripSurfeContactDetails(payload);
  // Drop successful contact detail caches entirely
  if (Array.isArray(cleaned.contacts)) {
    cleaned.contacts = cleaned.contacts.map((c) =>
      stripSurfeContactDetails({
        personName: c.personName,
        jobTitle: c.jobTitle,
        companyName: c.companyName || c.company,
        stakeholderRole: c.stakeholderRole || c.stakeholderClass,
        stakeholderClassification: c.stakeholderClassification || c.stakeholderClass,
        reasonSelected: c.reasonSelected || c.whyRelevant,
        matchConfidence: c.matchConfidence || c.confidence,
        articleDerived: c.articleDerived,
        quoted: c.quoted,
        enrichmentStatus: c.enrichmentStatus,
        identificationStatus: c.identificationStatus,
        contactLookupStatus: c.contactLookupStatus,
        stakeholderId: c.stakeholderId,
      })
    );
  }
  return cleaned;
}

export function readAlertContactCache(alertId) {
  try {
    const p = alertContactsPath(alertId);
    if (!fs.existsSync(p)) return null;
    return sanitizeAlertPayload(JSON.parse(fs.readFileSync(p, "utf8")));
  } catch {
    return null;
  }
}

export function writeAlertContactCache(alertId, payload) {
  if (!canWrite()) return { blocked: true, reason: "CONTACT_PERSISTENCE_BLOCKED", alertId };
  ensureDir(cacheDir());
  const p = alertContactsPath(alertId);
  const next = {
    ...sanitizeAlertPayload(payload),
    alertId,
    persistenceMode: "local_dev",
    durable: false,
    surfeContactDetailsForbidden: true,
    updatedAt: new Date().toISOString(),
  };
  fs.writeFileSync(p, JSON.stringify(next, null, 2));
  return next;
}

/**
 * @deprecated Do not cache Surfe contact details. Returns null always for PII safety.
 * Kept for call-site compatibility; never returns email/phone/linkedin.
 */
export function readPersonContactCache({ personName, companyName } = {}) {
  try {
    const key = personKey({ personName, companyName });
    const p = personCachePath(key);
    if (!fs.existsSync(p)) return null;
    const data = sanitizeAlertPayload(JSON.parse(fs.readFileSync(p, "utf8")));
    // Identity-only reuse — never treat as verified contact details
    if (data.email || data.phone || data.linkedinUrl) {
      return {
        personName: data.personName || personName,
        jobTitle: data.jobTitle || null,
        companyName: data.companyName || companyName,
        cacheKey: key,
        cacheHit: false,
        contactDetailsStripped: true,
      };
    }
    return {
      personName: data.personName || personName,
      jobTitle: data.jobTitle || null,
      companyName: data.companyName || companyName,
      stakeholderRole: data.stakeholderRole || null,
      reasonSelected: data.reasonSelected || null,
      matchConfidence: data.matchConfidence || null,
      cacheKey: key,
      cacheHit: true,
      identityOnly: true,
    };
  } catch {
    return null;
  }
}

/**
 * Persist identity-only — strips Surfe PII before write.
 */
export function writePersonContactCache(contact) {
  if (!canWrite()) return { blocked: true, reason: "CONTACT_PERSISTENCE_BLOCKED" };
  ensureDir(cacheDir());
  const key = personKey(contact);
  const p = personCachePath(key);
  const next = stripSurfeContactDetails({
    personName: contact.personName || null,
    jobTitle: contact.jobTitle || null,
    companyName: contact.companyName || contact.company || null,
    stakeholderRole: contact.stakeholderRole || contact.stakeholderClass || null,
    reasonSelected: contact.reasonSelected || contact.whyRelevant || null,
    matchConfidence: contact.matchConfidence || contact.confidence || null,
    articleDerived: contact.articleDerived === true,
    quoted: contact.quoted === true,
    cacheKey: key,
    persistenceMode: "local_dev",
    durable: false,
    identityOnly: true,
    surfeContactDetailsForbidden: true,
    updatedAt: new Date().toISOString(),
  });
  fs.writeFileSync(p, JSON.stringify(next, null, 2));
  return next;
}

/** Pending enrichment jobs — workflow status only; never store Surfe people/PII. */
export function readPendingEnrichment(enrichmentId) {
  try {
    const safe = String(enrichmentId || "").replace(/[^a-zA-Z0-9_-]/g, "_");
    const p = path.join(cacheDir(), `pending-${safe}.json`);
    if (!fs.existsSync(p)) return null;
    return stripSurfeContactDetails(JSON.parse(fs.readFileSync(p, "utf8")));
  } catch {
    return null;
  }
}

export function writePendingEnrichment(enrichmentId, payload) {
  if (!canWrite()) return { blocked: true, reason: "CONTACT_PERSISTENCE_BLOCKED", enrichmentId };
  ensureDir(cacheDir());
  const safe = String(enrichmentId || "").replace(/[^a-zA-Z0-9_-]/g, "_");
  const p = path.join(cacheDir(), `pending-${safe}.json`);
  const next = stripSurfeContactDetails({
    enrichmentId,
    alertId: payload.alertId || null,
    status: payload.status || "PENDING",
    company: payload.company || null,
    personName: payload.personName || null,
    persistenceMode: "local_dev",
    durable: false,
    surfeContactDetailsForbidden: true,
    updatedAt: new Date().toISOString(),
  });
  fs.writeFileSync(p, JSON.stringify(next, null, 2));
  return next;
}

export function markPendingEnrichmentDone(enrichmentId, _result) {
  const existing = readPendingEnrichment(enrichmentId) || {};
  // Never store result people / PII — count only
  return writePendingEnrichment(enrichmentId, {
    ...existing,
    status: "COMPLETED",
    resultSummary: {
      completedAt: new Date().toISOString(),
      peopleCountIgnored: true,
      note: "Surfe contact details are ephemeral and were not persisted",
    },
  });
}

/**
 * Purge Surfe PII from all local cache files. Returns cleanup report.
 */
export function purgeSurfePiiFromLocalCache() {
  const dir = cacheDir();
  const report = {
    dir,
    scanned: 0,
    rewritten: 0,
    deletedPersonCachesWithPii: 0,
    fieldsRemoved: [],
    files: [],
  };
  if (!fs.existsSync(dir)) return report;

  const bannedKeys = ["email", "phone", "mobile", "linkedinUrl", "linkedInUrl", "providerPersonId", "emails", "phones"];

  for (const name of fs.readdirSync(dir)) {
    if (!name.endsWith(".json")) continue;
    const full = path.join(dir, name);
    report.scanned += 1;
    let raw;
    try {
      raw = JSON.parse(fs.readFileSync(full, "utf8"));
    } catch {
      continue;
    }

    const before = JSON.stringify(raw);
    const hasPii =
      bannedKeys.some((k) => JSON.stringify(raw).toLowerCase().includes(`"${k.toLowerCase()}"`)) ||
      /@[a-z0-9.-]+\.[a-z]{2,}/i.test(before);

    if (name.startsWith("person-") && hasPii) {
      const cleaned = stripSurfeContactDetails({
        personName: raw.personName || null,
        jobTitle: raw.jobTitle || null,
        companyName: raw.companyName || null,
        stakeholderRole: raw.stakeholderRole || null,
        reasonSelected: raw.reasonSelected || null,
        matchConfidence: raw.matchConfidence || null,
        identityOnly: true,
        surfeContactDetailsForbidden: true,
        purgedAt: new Date().toISOString(),
      });
      fs.writeFileSync(full, JSON.stringify(cleaned, null, 2));
      report.rewritten += 1;
      report.deletedPersonCachesWithPii += 1;
      report.files.push({ file: name, action: "stripped_pii" });
      for (const k of bannedKeys) {
        if (Object.prototype.hasOwnProperty.call(raw, k) && raw[k]) {
          report.fieldsRemoved.push(`${name}:${k}`);
        }
      }
      continue;
    }

    if (hasPii) {
      const cleaned = sanitizeAlertPayload(raw);
      cleaned.purgedAt = new Date().toISOString();
      cleaned.surfeContactDetailsForbidden = true;
      fs.writeFileSync(full, JSON.stringify(cleaned, null, 2));
      report.rewritten += 1;
      report.files.push({ file: name, action: "stripped_pii" });
    }
  }

  logContactOp("local_cache_pii_purge", {
    scanned: report.scanned,
    rewritten: report.rewritten,
  });
  return report;
}

export function getContactCacheDir() {
  return cacheDir();
}
