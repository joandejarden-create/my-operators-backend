/**
 * Association / Events evidence ledger V1.
 *
 * Preserves verified claims and explicit rejections so downstream extraction
 * cannot silently recontaminate opportunities (LARIM ← Santa Fe regression).
 *
 * New authoritative evidence can supersede protected claims only with
 * explicit strongerEvidence + reason + provenance.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { isAssociationEventsValidPersonName } from "./association-events-buyer-integrity-v1.js";

export const ASSOCIATION_EVENTS_EVIDENCE_LEDGER_V1 =
  "gdi-association-events-evidence-ledger-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function normEmail(e) {
  return text(e).toLowerCase();
}

function normName(n) {
  return text(n).toLowerCase();
}

/** Canonical protected / rejected seeds for known Mexico campaign cases. */
export const ASSOCIATION_EVENTS_PROTECTED_SEEDS = Object.freeze({
  gdi_e2d_b55abdf7a9488331: {
    opportunityId: "gdi_e2d_b55abdf7a9488331",
    hotelId: "recIEcSWuhSCoNLFN",
    acceptedClaims: {
      eventName: "LARIM 2026 — XVIII Reunión Regional Latinoamericana de la UAI",
      organizationName: "IAU LARIM 2026 (UNAM Local Organizing Committee)",
      venueName: "Cancún Center",
      eventStartDate: "2026-11-30",
      eventEndDate: "2026-12-04",
      contactName: "Vladimir Avila-Reese",
      contactRole: "SOC Co-chair, IA-UNAM",
      contactEmail: "avila@astro.unam.mx",
      organizationContactEmail: "larim2026@astro.unam.mx",
      namedStakeholders: [
        {
          name: "Vladimir Avila-Reese",
          title: "SOC Co-chair",
          email: "avila@astro.unam.mx",
        },
        {
          name: "Rosa A. González-Lópezlira",
          title: "LOC Co-chair",
          email: "r.gonzalez@irya.unam.mx",
        },
      ],
      demandMechanism: "OFFSITE_OVERFLOW_LODGING",
      externalRoomBlocksAvailable: true,
      hostHotelContracted: false,
    },
    acceptedProvenance: {
      reason: "Founder-validated LARIM identity from autonomous long-form package",
      sources: [
        "https://www.iau.org/IAU/Iau/Science/Scientific-Meetings/IAUM2026/LARIM2026.aspx",
        "reports/gdi-association-events-autonomous-long-form-research-v1/",
      ],
      acceptedAt: "2026-10-27T00:00:00.000Z",
    },
    rejectedClaims: [
      {
        claimType: "contactName",
        value: "Santa Fe Business Forum",
        reason: "Not a person; unrelated Chilean tourism forum identity",
      },
      {
        claimType: "contactEmail",
        value: "sara.errazuriz@uai.cl",
        reason: "Unrelated UAI Chile contact; not LARIM lodging buyer",
      },
      {
        claimType: "organizationName",
        value: "Santa Fe Business Forum",
        reason: "Wrong entity; rejected in prior commercial QA",
      },
      {
        claimType: "namedStakeholder",
        value: "Santa Fe Business Forum",
        reason: "Org/forum label, not LARIM buyer",
      },
    ],
  },
  gdi_e2d_583f9193e013df37: {
    opportunityId: "gdi_e2d_583f9193e013df37",
    hotelId: "recIEcSWuhSCoNLFN",
    acceptedClaims: {
      eventName: "Longevity Congress / VIVAVITA Future Human Cancún 2027",
      organizationName: "VIVAVITA / Apex Business Group Worldwide S.L.",
      eventStartDate: "2027-02-11",
      eventEndDate: "2027-02-14",
      organizationContactEmail: "info@vivavitaexpos.com",
      hostHotelContracted: false,
    },
    acceptedProvenance: {
      reason: "Verified dates/org from vivavitaexpos; named person still unresolved",
      sources: ["https://vivavitaexpos.com/longevity-congress/cancun-2027/"],
      acceptedAt: "2026-10-27T00:00:00.000Z",
    },
    rejectedClaims: [
      {
        claimType: "contactName",
        value: "Business Expos",
        reason: "Not a person name",
      },
      {
        claimType: "contactName",
        value: "Peter Hughes",
        reason: "Different Apex Group Ltd entity — not Apex Business Group Worldwide S.L.",
      },
    ],
  },
});

export function evidenceLedgerPath(hotelId) {
  const slug =
    hotelId === "recterKmNPdntCm0v"
      ? "chapultepec"
      : hotelId === "recIEcSWuhSCoNLFN"
        ? "condesa"
        : text(hotelId).replace(/[^\w.-]+/g, "_").slice(0, 40);
  return path.join(
    ROOT,
    "reports/gdi-association-events-campaigns",
    slug,
    "evidence-ledger.json",
  );
}

export function loadEvidenceLedger(hotelId) {
  const p = evidenceLedgerPath(hotelId);
  if (!fs.existsSync(p)) {
    return {
      version: ASSOCIATION_EVENTS_EVIDENCE_LEDGER_V1,
      hotelId,
      opportunities: {},
      revisions: [],
      updatedAt: null,
    };
  }
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return {
      version: ASSOCIATION_EVENTS_EVIDENCE_LEDGER_V1,
      hotelId,
      opportunities: {},
      revisions: [],
      updatedAt: null,
    };
  }
}

export function saveEvidenceLedger(ledger) {
  const p = evidenceLedgerPath(ledger.hotelId);
  fs.mkdirSync(path.dirname(p), { recursive: true });
  ledger.updatedAt = new Date().toISOString();
  fs.writeFileSync(p, `${JSON.stringify(ledger, null, 2)}\n`, "utf8");
  return p;
}

export function ensureProtectedSeeds(hotelId) {
  const ledger = loadEvidenceLedger(hotelId);
  ledger.opportunities = ledger.opportunities || {};
  for (const seed of Object.values(ASSOCIATION_EVENTS_PROTECTED_SEEDS)) {
    if (seed.hotelId !== hotelId) continue;
    const existing = ledger.opportunities[seed.opportunityId] || {
      opportunityId: seed.opportunityId,
      acceptedClaims: {},
      acceptedProvenance: {},
      rejectedClaims: [],
    };
    existing.acceptedClaims = {
      ...seed.acceptedClaims,
      ...existing.acceptedClaims,
    };
    existing.acceptedProvenance = existing.acceptedProvenance?.reason
      ? existing.acceptedProvenance
      : seed.acceptedProvenance;
    const rejKey = new Set(
      (existing.rejectedClaims || []).map(
        (r) => `${r.claimType}::${normName(r.value)}`,
      ),
    );
    for (const r of seed.rejectedClaims || []) {
      const k = `${r.claimType}::${normName(r.value)}`;
      if (!rejKey.has(k)) {
        existing.rejectedClaims = [...(existing.rejectedClaims || []), r];
        rejKey.add(k);
      }
    }
    ledger.opportunities[seed.opportunityId] = existing;
  }
  saveEvidenceLedger(ledger);
  return ledger;
}

function isRejectedValue(entry, claimType, value) {
  if (!value) return false;
  const v = claimType.includes("email") ? normEmail(value) : normName(value);
  return (entry?.rejectedClaims || []).some((r) => {
    if (r.claimType !== claimType && !(claimType === "namedStakeholder" && r.claimType === "contactName")) {
      // also match contactName rejection against stakeholder names
      if (!(claimType === "namedStakeholder" && r.claimType === "contactName")) {
        if (r.claimType !== claimType) return false;
      }
    }
    const rv = r.claimType.includes("email") ? normEmail(r.value) : normName(r.value);
    return rv && (rv === v || v.includes(rv) || rv.includes(v));
  });
}

/**
 * Merge incoming research into prior opportunity while protecting accepted claims
 * and blocking rejected contamination.
 *
 * @param {object} prior
 * @param {object} incoming
 * @param {object} [opts]
 * @param {object} [opts.ledgerEntry]
 * @param {boolean} [opts.strongerEvidence] — allow supersede of protected fields
 * @param {string} [opts.revisionReason]
 * @param {string[]} [opts.revisionSources]
 */
export function mergeAssociationEventsEvidence(prior = {}, incoming = {}, opts = {}) {
  const entry = opts.ledgerEntry || null;
  const accepted = entry?.acceptedClaims || {};
  const out = { ...prior, ...incoming, id: prior.id || incoming.id };

  // Restore / enforce accepted claims unless strongerEvidence supersede
  const protectedFields = [
    "eventName",
    "organizationName",
    "venueName",
    "eventStartDate",
    "eventEndDate",
    "contactName",
    "contactRole",
    "contactEmail",
    "organizationContactEmail",
    "demandMechanism",
    "externalRoomBlocksAvailable",
    "hostHotelContracted",
  ];

  const blocked = [];
  const restored = [];

  for (const field of protectedFields) {
    if (accepted[field] === undefined || accepted[field] === null) continue;
    const incomingVal = incoming[field];
    const acceptedVal = accepted[field];
    if (incomingVal == null || incomingVal === "") {
      out[field] = acceptedVal;
      restored.push(field);
      continue;
    }
    // Block rejected values
    if (isRejectedValue(entry, field, incomingVal)) {
      out[field] = acceptedVal;
      blocked.push({ field, value: incomingVal, reason: "rejected_claim" });
      continue;
    }
    // Protect accepted person/email/event identity from weaker scrapes
    const isIdentity =
      field === "contactName" ||
      field === "contactRole" ||
      field === "contactEmail" ||
      field === "organizationName" ||
      field === "eventName" ||
      field === "venueName" ||
      field === "eventStartDate" ||
      field === "eventEndDate";
    if (isIdentity && text(incomingVal) !== text(acceptedVal)) {
      if (opts.strongerEvidence === true && opts.revisionReason) {
        out[field] = incomingVal;
        restored.push(`superseded:${field}`);
      } else {
        out[field] = acceptedVal;
        blocked.push({
          field,
          value: incomingVal,
          reason: "protected_accepted_claim_without_stronger_evidence",
        });
      }
    }
  }

  // Named stakeholders: drop rejected; ensure accepted present
  let stakeholders = [
    ...(Array.isArray(out.namedStakeholders) ? out.namedStakeholders : []),
  ];
  stakeholders = stakeholders.filter((s) => {
    const n = s?.name;
    if (!n) return false;
    if (isRejectedValue(entry, "namedStakeholder", n)) return false;
    if (isRejectedValue(entry, "contactName", n)) return false;
    if (!isAssociationEventsValidPersonName(n)) return false;
    return true;
  });
  for (const s of accepted.namedStakeholders || []) {
    if (!stakeholders.some((x) => normName(x.name) === normName(s.name))) {
      stakeholders.unshift(s);
    }
  }
  out.namedStakeholders = stakeholders;

  // If contactName invalid or rejected → restore accepted
  if (
    !isAssociationEventsValidPersonName(out.contactName) ||
    isRejectedValue(entry, "contactName", out.contactName)
  ) {
    if (accepted.contactName) {
      out.contactName = accepted.contactName;
      out.contactRole = accepted.contactRole || out.contactRole;
      restored.push("contactName");
    } else {
      out.contactName = null;
      out.contactRole = out.contactRole || null;
    }
  }

  // Rejected emails
  if (isRejectedValue(entry, "contactEmail", out.contactEmail)) {
    out.contactEmail = accepted.contactEmail || null;
    blocked.push({
      field: "contactEmail",
      value: incoming.contactEmail,
      reason: "rejected_claim",
    });
  }

  out.associationEventsEvidenceMerge = {
    version: ASSOCIATION_EVENTS_EVIDENCE_LEDGER_V1,
    blocked,
    restored,
    strongerEvidence: opts.strongerEvidence === true,
    revisionReason: opts.revisionReason || null,
  };

  return out;
}

/**
 * Apply ledger merge for a hotel opportunity (loads ledger entry by id).
 */
export function applyEvidenceLedgerToOpportunity(hotelId, opportunity = {}, opts = {}) {
  const ledger = ensureProtectedSeeds(hotelId);
  const entry = ledger.opportunities?.[opportunity.id] || null;
  const prior = entry?.acceptedClaims
    ? { id: opportunity.id, ...entry.acceptedClaims }
    : { id: opportunity.id };
  return mergeAssociationEventsEvidence(prior, opportunity, {
    ...opts,
    ledgerEntry: entry,
  });
}

/**
 * Record a rejected claim discovered during research.
 */
export function recordRejectedClaim(hotelId, opportunityId, claim) {
  const ledger = loadEvidenceLedger(hotelId);
  ledger.opportunities = ledger.opportunities || {};
  const entry = ledger.opportunities[opportunityId] || {
    opportunityId,
    acceptedClaims: {},
    rejectedClaims: [],
  };
  entry.rejectedClaims = entry.rejectedClaims || [];
  const k = `${claim.claimType}::${normName(claim.value)}`;
  if (
    !entry.rejectedClaims.some(
      (r) => `${r.claimType}::${normName(r.value)}` === k,
    )
  ) {
    entry.rejectedClaims.push({
      ...claim,
      rejectedAt: new Date().toISOString(),
    });
  }
  ledger.opportunities[opportunityId] = entry;
  saveEvidenceLedger(ledger);
  return entry;
}
