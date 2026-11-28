/**
 * Canonical Person Identity V1.
 *
 * Stable Person ID = HI Org Contacts "Person ID".
 * GDI personIdentityKey maps reversibly onto HI Person ID — never merge on name alone.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { personIdentityKey as gdiPersonIdentityKey } from "../group-demand-intelligence/workflow/contact-field-provenance-v1.js";

export const CANONICAL_PERSON_IDENTITY_V1 = "canonical-person-identity-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_MAP_PATH = path.join(
  __dirname,
  "../../data/shared-contact-intelligence/gdi-hi-person-id-map-v1.json",
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function normName(v) {
  return text(v).toLowerCase();
}

/**
 * Build a reversible mapping entry (non-destructive).
 */
export function buildPersonIdentityLink({
  hiPersonId,
  hiAirtableId = null,
  displayName,
  organizationName = null,
  organizationId = null,
  gdiPersonIdentityKey: gdiKey = null,
  linkedInUrl = null,
  evidence = [],
} = {}) {
  const name = text(displayName);
  const org = text(organizationName);
  const resolvedGdiKey =
    gdiKey ||
    (name
      ? gdiPersonIdentityKey({ name, organization: org || undefined })
      : null);
  return {
    version: CANONICAL_PERSON_IDENTITY_V1,
    canonicalPersonId: text(hiPersonId) || null,
    hiAirtableId: hiAirtableId || null,
    displayName: name || null,
    organizationName: org || null,
    organizationId: organizationId || null,
    gdiPersonIdentityKey: resolvedGdiKey,
    linkedInUrl: linkedInUrl || null,
    evidence: Array.isArray(evidence) ? evidence : [],
    mergePolicy: "NO_NAME_ONLY_MERGE",
    reversible: true,
  };
}

/**
 * Score identity evidence for matching — never treat similar names as sufficient.
 */
export function scoreIdentityEvidence(candidate = {}, probe = {}) {
  let score = 0;
  const reasons = [];
  if (
    candidate.canonicalPersonId &&
    probe.canonicalPersonId &&
    String(candidate.canonicalPersonId) === String(probe.canonicalPersonId)
  ) {
    score += 100;
    reasons.push("CANONICAL_PERSON_ID");
  }
  if (
    candidate.hiAirtableId &&
    probe.hiAirtableId &&
    String(candidate.hiAirtableId) === String(probe.hiAirtableId)
  ) {
    score += 80;
    reasons.push("HI_AIRTABLE_ID");
  }
  if (
    candidate.gdiPersonIdentityKey &&
    probe.gdiPersonIdentityKey &&
    String(candidate.gdiPersonIdentityKey) === String(probe.gdiPersonIdentityKey)
  ) {
    score += 60;
    reasons.push("GDI_PERSON_IDENTITY_KEY");
  }
  const cLi = text(candidate.linkedInUrl).toLowerCase().replace(/\/$/, "");
  const pLi = text(probe.linkedInUrl).toLowerCase().replace(/\/$/, "");
  if (cLi && pLi && cLi === pLi && /linkedin\.com\/in\//i.test(cLi)) {
    score += 70;
    reasons.push("LINKEDIN_URL");
  }
  const sameName = normName(candidate.displayName) === normName(probe.displayName);
  const sameOrg =
    normName(candidate.organizationName) &&
    normName(candidate.organizationName) === normName(probe.organizationName);
  if (sameName && sameOrg) {
    score += 40;
    reasons.push("NAME_PLUS_ORG");
  } else if (sameName && !sameOrg) {
    // Explicitly insufficient alone
    reasons.push("NAME_ONLY_INSUFFICIENT");
  }
  if (
    probe.employerDomain &&
    candidate.employerDomain &&
    String(probe.employerDomain).toLowerCase() ===
      String(candidate.employerDomain).toLowerCase() &&
    sameName
  ) {
    score += 25;
    reasons.push("NAME_PLUS_EMPLOYER_DOMAIN");
  }
  return { score, reasons, match: score >= 60 };
}

export function loadPersonIdMap(mapPath = DEFAULT_MAP_PATH) {
  try {
    if (!fs.existsSync(mapPath)) {
      return {
        version: CANONICAL_PERSON_IDENTITY_V1,
        path: mapPath,
        entries: [],
        byCanonicalPersonId: {},
        byGdiPersonIdentityKey: {},
      };
    }
    const raw = JSON.parse(fs.readFileSync(mapPath, "utf8"));
    const entries = Array.isArray(raw.entries) ? raw.entries : [];
    const byCanonicalPersonId = {};
    const byGdiPersonIdentityKey = {};
    for (const e of entries) {
      if (e.canonicalPersonId) byCanonicalPersonId[e.canonicalPersonId] = e;
      if (e.gdiPersonIdentityKey) byGdiPersonIdentityKey[e.gdiPersonIdentityKey] = e;
    }
    return {
      version: CANONICAL_PERSON_IDENTITY_V1,
      path: mapPath,
      entries,
      byCanonicalPersonId,
      byGdiPersonIdentityKey,
      generatedAt: raw.generatedAt || null,
    };
  } catch (err) {
    return {
      version: CANONICAL_PERSON_IDENTITY_V1,
      path: mapPath,
      entries: [],
      byCanonicalPersonId: {},
      byGdiPersonIdentityKey: {},
      error: String(err?.message || err),
    };
  }
}

/**
 * Upsert a reversible mapping entry without deleting history.
 */
export function upsertPersonIdMapEntry(entry, opts = {}) {
  const mapPath = opts.mapPath || DEFAULT_MAP_PATH;
  const existing = loadPersonIdMap(mapPath);
  const next = buildPersonIdentityLink(entry);
  if (!next.canonicalPersonId && !next.gdiPersonIdentityKey) {
    return { ok: false, error: "IDENTITY_INCOMPLETE", entry: next };
  }
  const entries = [...existing.entries];
  const idx = entries.findIndex(
    (e) =>
      (next.canonicalPersonId && e.canonicalPersonId === next.canonicalPersonId) ||
      (next.gdiPersonIdentityKey &&
        e.gdiPersonIdentityKey === next.gdiPersonIdentityKey),
  );
  if (idx >= 0) {
    entries[idx] = {
      ...entries[idx],
      ...next,
      prior: {
        canonicalPersonId: entries[idx].canonicalPersonId,
        gdiPersonIdentityKey: entries[idx].gdiPersonIdentityKey,
        updatedAt: entries[idx].updatedAt || null,
      },
      updatedAt: new Date().toISOString(),
    };
  } else {
    entries.push({ ...next, createdAt: new Date().toISOString() });
  }
  const payload = {
    version: CANONICAL_PERSON_IDENTITY_V1,
    generatedAt: new Date().toISOString(),
    policy: "REVERSIBLE_NON_DESTRUCTIVE",
    entries,
  };
  if (opts.write !== false) {
    fs.mkdirSync(path.dirname(mapPath), { recursive: true });
    fs.writeFileSync(mapPath, JSON.stringify(payload, null, 2) + "\n", "utf8");
  }
  return { ok: true, entry: next, path: mapPath, count: entries.length };
}

/**
 * Prefer HI Person ID; fall back to mapped GDI key; never invent from name alone.
 */
export function resolveCanonicalPersonId(probe = {}, opts = {}) {
  const map = opts.map || loadPersonIdMap(opts.mapPath);
  if (probe.canonicalPersonId || probe.personId || probe.hiPersonId) {
    return {
      ok: true,
      canonicalPersonId: String(
        probe.canonicalPersonId || probe.personId || probe.hiPersonId,
      ),
      method: "DIRECT_PERSON_ID",
    };
  }
  const gdiKey =
    probe.gdiPersonIdentityKey ||
    (probe.displayName || probe.name
      ? gdiPersonIdentityKey({
          name: probe.displayName || probe.name,
          organization: probe.organizationName || probe.organization,
          domain: probe.employerDomain || probe.domain,
        })
      : null);
  if (gdiKey && map.byGdiPersonIdentityKey[gdiKey]?.canonicalPersonId) {
    return {
      ok: true,
      canonicalPersonId: map.byGdiPersonIdentityKey[gdiKey].canonicalPersonId,
      method: "GDI_MAP",
      gdiPersonIdentityKey: gdiKey,
    };
  }
  if (Array.isArray(opts.candidates) && opts.candidates.length) {
    let best = null;
    for (const c of opts.candidates) {
      const scored = scoreIdentityEvidence(c, {
        ...probe,
        gdiPersonIdentityKey: gdiKey,
      });
      if (scored.match && (!best || scored.score > best.score)) {
        best = { ...scored, candidate: c };
      }
    }
    if (best?.candidate?.canonicalPersonId) {
      return {
        ok: true,
        canonicalPersonId: best.candidate.canonicalPersonId,
        method: "EVIDENCE_MATCH",
        score: best.score,
        reasons: best.reasons,
      };
    }
  }
  return {
    ok: false,
    canonicalPersonId: null,
    method: "UNRESOLVED",
    gdiPersonIdentityKey: gdiKey,
    reason: "INSUFFICIENT_IDENTITY_EVIDENCE",
  };
}

export function fingerprintPersonNameOrg(name, organization) {
  return createHash("sha256")
    .update(`${normName(name)}|${normName(organization)}`)
    .digest("hex")
    .slice(0, 16);
}

export { gdiPersonIdentityKey, DEFAULT_MAP_PATH };

export default {
  CANONICAL_PERSON_IDENTITY_V1,
  buildPersonIdentityLink,
  scoreIdentityEvidence,
  loadPersonIdMap,
  upsertPersonIdMapEntry,
  resolveCanonicalPersonId,
  fingerprintPersonNameOrg,
  gdiPersonIdentityKey,
  DEFAULT_MAP_PATH,
};
