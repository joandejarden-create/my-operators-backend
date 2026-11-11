/**
 * Waterstone contact maturation overlay V1 — zero paid, local preview only.
 *
 * Recovers named contacts / org routes from existing evidence into customer
 * presentation without Airtable writes or invented people.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { looksLikePersonName } from "./secondary-named-contact-v1.js";

export const WATERSTONE_CONTACT_MATURATION_OVERLAY_V1 =
  "gdi-waterstone-contact-maturation-overlay-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
export const WATERSTONE_CONTACT_MATURATION_OVERLAY_PATH = path.join(
  ROOT,
  "data/group-demand-intelligence/local-shadow/waterstone-contact-maturation-overlay-v1.json",
);

const WATERSTONE_HOTEL_ID = "recgMYovrrZDJMqzX";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function loadOverlayDoc() {
  if (!fs.existsSync(WATERSTONE_CONTACT_MATURATION_OVERLAY_PATH)) return null;
  try {
    return JSON.parse(
      fs.readFileSync(WATERSTONE_CONTACT_MATURATION_OVERLAY_PATH, "utf8"),
    );
  } catch {
    return null;
  }
}

/**
 * Merge one maturation row onto a store opportunity.
 * Never invents names. Never overwrites an existing verified named primary.
 */
export function applyContactMaturationRow(opportunity = {}, row = {}) {
  if (!row?.contactMaturationV1) return opportunity;
  const o = { ...opportunity };
  const existingName =
    text(o.primaryContact?.name) || text(o.contactName) || "";
  const hasNamedPrimary =
    existingName && looksLikePersonName(existingName);

  if (row.contactOfficialUrl && !o.contactOfficialUrl) {
    o.contactOfficialUrl = row.contactOfficialUrl;
  }
  if (row.companyPhone && !o.companyPhone) {
    o.companyPhone = row.companyPhone;
  }
  if (row.organizationContactEmail && !o.organizationContactEmail) {
    o.organizationContactEmail = row.organizationContactEmail;
  }
  if (row.contactResearchGap) {
    o.contactResearchGap = row.contactResearchGap;
    o.unresolvedContactResearchQuestions = [
      ...(Array.isArray(o.unresolvedContactResearchQuestions)
        ? o.unresolvedContactResearchQuestions
        : []),
      row.contactResearchGap,
    ].filter((q, i, a) => a.indexOf(q) === i);
  }

  const incoming = row.primaryContact && typeof row.primaryContact === "object"
    ? { ...row.primaryContact }
    : null;

  if (incoming && looksLikePersonName(incoming.name) && !hasNamedPrimary) {
    o.primaryContact = {
      ...(o.primaryContact && typeof o.primaryContact === "object"
        ? o.primaryContact
        : {}),
      ...incoming,
      name: text(incoming.name),
    };
    o.contactName = text(incoming.name);
    o.contactRole = text(incoming.title) || o.contactRole || null;
    o.contactPhone = text(incoming.phone) || o.contactPhone || null;
    o.contactQuality = "NAMED_RELEVANT_INFLUENCER";
    // Drop stale "no named contact" uncertainty lines that contradict recovery.
    const scrubNoNamed = (arr) =>
      (Array.isArray(arr) ? arr : [])
        .map((x) => text(x))
        .filter(
          (x) =>
            x &&
            !/no verified named primary contact|buyer not yet identified/i.test(
              x,
            ),
        );
    if (Array.isArray(o.uncertainties)) {
      o.uncertainties = scrubNoNamed(o.uncertainties);
    }
    if (Array.isArray(o.establishedFactsAndUncertainties?.uncertainties)) {
      o.establishedFactsAndUncertainties = {
        ...o.establishedFactsAndUncertainties,
        uncertainties: scrubNoNamed(
          o.establishedFactsAndUncertainties.uncertainties,
        ),
      };
    }
    const secondary =
      row.secondaryContact && typeof row.secondaryContact === "object"
        ? row.secondaryContact
        : null;
    if (
      secondary &&
      looksLikePersonName(secondary.name) &&
      !(
        text(o.secondaryContact?.name) &&
        looksLikePersonName(o.secondaryContact.name)
      )
    ) {
      o.secondaryContact = { ...secondary, name: text(secondary.name) };
    }
  } else if (incoming && !hasNamedPrimary) {
    // Org-route shell — clear functional fake names
    const prior = o.primaryContact && typeof o.primaryContact === "object"
      ? { ...o.primaryContact }
      : {};
    if (prior.name && !looksLikePersonName(prior.name)) {
      prior.name = null;
      prior.title = null;
    }
    o.primaryContact = {
      ...prior,
      ...incoming,
      name: null,
      title: incoming.title || null,
      organization:
        text(incoming.organization) ||
        text(prior.organization) ||
        text(o.organizationName) ||
        null,
      officialContactUrl:
        text(incoming.officialContactUrl) ||
        text(row.contactOfficialUrl) ||
        text(prior.officialContactUrl) ||
        null,
      companyPhone:
        text(incoming.companyPhone) ||
        text(row.companyPhone) ||
        text(prior.companyPhone) ||
        null,
      contactKind: "ORGANIZATION",
    };
  }

  o.contactMaturationOverlay = {
    version: WATERSTONE_CONTACT_MATURATION_OVERLAY_V1,
    priorityBand: row.priorityBand || null,
    recoveryClass: row.recoveryClass || null,
    provenance: row.provenance || null,
    appliedAt: new Date().toISOString().slice(0, 10),
  };
  o.localShadowPortfolio = true;
  return o;
}

/**
 * Apply Waterstone contact maturation overlay across a merged opportunity list.
 */
export function applyWaterstoneContactMaturationOverlay(
  opportunities = [],
  hotelId = null,
  { env = process.env } = {},
) {
  if (String(hotelId) !== WATERSTONE_HOTEL_ID) return opportunities;
  if (String(env.GDI_WATERSTONE_CONTACT_MATURATION || "1").trim() === "0") {
    return opportunities;
  }
  const doc = loadOverlayDoc();
  if (!doc?.opportunities?.length) return opportunities;
  const byId = new Map(
    doc.opportunities.map((r) => [String(r.id), r]),
  );
  return opportunities.map((o) => {
    const row = byId.get(String(o.id));
    if (!row) return o;
    return applyContactMaturationRow(o, row);
  });
}
