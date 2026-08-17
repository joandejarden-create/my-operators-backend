/**
 * Person / LinkedIn identity helpers for Acquisition Intelligence.
 */

import { normalizeOwnerKey } from "../gtm-owner-target/normalize.js";

/**
 * Normalize a LinkedIn profile URL for identity matching.
 * Collapses www, trailing slash, query/hash, and common path variants.
 * @param {string} raw
 * @returns {string} empty if not a usable LinkedIn profile URL
 */
export function normalizeLinkedInProfileUrl(raw) {
  const s = String(raw || "").trim();
  if (!s) return "";

  let urlText = s;
  if (!/^https?:\/\//i.test(urlText)) {
    urlText = `https://${urlText}`;
  }

  let parsed;
  try {
    parsed = new URL(urlText);
  } catch {
    return "";
  }

  const host = parsed.hostname.replace(/^www\./i, "").toLowerCase();
  if (host !== "linkedin.com" && !host.endsWith(".linkedin.com")) {
    return "";
  }

  // Keep /in/{slug} only — drop locale prefixes like /mwlite/, /pub/
  let path = parsed.pathname.replace(/\/+$/, "").toLowerCase();
  const inMatch = path.match(/\/in\/([^/]+)/);
  if (!inMatch) return "";

  const slug = decodeURIComponent(inMatch[1]).replace(/\/+$/, "");
  if (!slug) return "";

  return `https://www.linkedin.com/in/${slug}`;
}

/**
 * @param {string} firstName
 * @param {string} lastName
 */
export function formatPersonDisplayName(firstName, lastName) {
  return [String(firstName || "").trim(), String(lastName || "").trim()]
    .filter(Boolean)
    .join(" ")
    .trim();
}

/**
 * Fallback identity when LinkedIn URL is missing.
 * @param {string} firstName
 * @param {string} lastName
 * @param {string} company
 */
export function buildNameCompanyIdentityKey(firstName, lastName, company) {
  const name = normalizeOwnerKey(formatPersonDisplayName(firstName, lastName));
  const co = normalizeOwnerKey(company);
  if (!name) return "";
  return `nc:${name}|${co || "_"}`;
}

/**
 * Prefer LinkedIn URL; else name+company.
 * @param {{ linkedInUrl?: string, firstName?: string, lastName?: string, company?: string }} row
 */
export function buildPersonIdentityKey(row) {
  const linkedIn = normalizeLinkedInProfileUrl(row.linkedInUrl || "");
  if (linkedIn) return `li:${linkedIn}`;
  return buildNameCompanyIdentityKey(row.firstName, row.lastName, row.company);
}

/**
 * User-scoped relationship dedupe key.
 * @param {string} sourceUserId
 * @param {{ linkedInUrl?: string, firstName?: string, lastName?: string, company?: string }} row
 */
export function buildRelationshipDedupeKey(sourceUserId, row) {
  const uid = String(sourceUserId || "").trim();
  if (!uid) return "";
  const personKey = buildPersonIdentityKey(row);
  if (!personKey) return "";
  return `${uid}|${personKey}`;
}

/**
 * Contact-table dedupe key aligned with LinkedIn-first identity.
 * Distinct from CoStar email-first keys when LinkedIn URL is present.
 */
export function buildAcquisitionContactDedupeKey(row) {
  const linkedIn = normalizeLinkedInProfileUrl(row.linkedInUrl || "");
  if (linkedIn) return `li:${linkedIn}`;

  const email = String(row.email || "")
    .trim()
    .toLowerCase();
  if (email) return `email:${email}`;

  return buildNameCompanyIdentityKey(row.firstName, row.lastName, row.company);
}
