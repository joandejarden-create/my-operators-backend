/**
 * P0 V1R email sprint — upgrade high-confidence outreach-ready owners from V2 LinkedIn to named email.
 * V1R = named person on entity domain + proof URL.
 */
import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { buildEnrichmentFromSeedContact } from "./mx-corporate-web-first.js";
import { MX_CORPORATE_WEB_SEEDS } from "./mx-corporate-web-seeds.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..", "..", "..");
const P0_AUDIT_JSON = join(ROOT, "reports", "gtm-owner-portfolio-audit.json");

/** @typedef {{ slug: string, contactKey?: string, ownerTargetId?: string, email?: string, verificationUrl?: string, verificationSource?: string, enrichedBy?: string }} P0V1rSpec */

/** High-confidence P0 owners — named email found on entity domain with proof. */
/** @type {P0V1rSpec[]} */
export const P0_V1R_EMAIL_SPECS = [
  { slug: "american-hotels-group", contactKey: "primary" },
  { slug: "grupo-posadas", contactKey: "primary" },
  { slug: "norte-19", contactKey: "primary" },
  { slug: "landstar-hotels", contactKey: "primary" },
  { slug: "hoteles-costa-del-sol", contactKey: "primary" },
  { slug: "collective-hospitality", contactKey: "primary" },
  { slug: "grupo-hotelero-santa-fe", contactKey: "cfo_bridge" },
];

/** Owners researched for V1R but blocked — CoStar/inferred email without proof URL. */
export const P0_V1R_BLOCKED_NOTES = [
  "GHL Hoteles — jorge.londono@ghlholding.com is CoStar-only; Andrés Fajardo is current CEO (V2).",
  "Grupo Marta — amonge@grupomarta.com is CoStar-only; grupomarta.com lists no named emails.",
  "Velas, Oasis, Excellence, Brookfield, Grupo Brisas, Atlantica — V2 LinkedIn; no entity-domain email on proof page.",
];

function loadOwnerTargetIdFromAudit(ownerName) {
  try {
    const data = JSON.parse(readFileSync(P0_AUDIT_JSON, "utf8"));
    const item = (data.items || []).find((i) => {
      const a = normalizeLoose(i.ownerName);
      const b = normalizeLoose(ownerName);
      return a === b || a.includes(b) || b.includes(a);
    });
    return item?.ownerTargetId || null;
  } catch {
    return null;
  }
}

function normalizeLoose(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/**
 * @param {P0V1rSpec} spec
 */
export function buildP0V1rEmailEnrichment(spec) {
  const seed = MX_CORPORATE_WEB_SEEDS.find((s) => s.slug === spec.slug);
  if (!seed) throw new Error(`Unknown V1R seed slug: ${spec.slug}`);

  const ownerTargetId =
    spec.ownerTargetId || loadOwnerTargetIdFromAudit(seed.ownerNameMatch[0]);

  const base = buildEnrichmentFromSeedContact(seed, {
    ownerTargetId,
    contactKey: spec.contactKey || "primary",
    enrichedBy: spec.enrichedBy || "p0_v1r_email_sprint_2026-07",
  });

  if (spec.email) {
    base.contact.email = spec.email;
    base.contact.verificationTier = "V1R";
    if (spec.verificationSource) base.contact.verificationSource = spec.verificationSource;
    if (spec.verificationUrl) {
      base.contact.verificationUrl = spec.verificationUrl;
      base.registry.verificationUrl = spec.verificationUrl;
      base.registry.lookupNotes = [
        `P0 V1R email sprint — proof: ${spec.verificationUrl}`,
        ...(seed.researchNotes || []),
      ].join("\n");
    }
  }

  if (base.contact.verificationTier === "V1R" && base.contact.email) {
    base.enrichedBy = spec.enrichedBy || "p0_v1r_email_sprint_2026-07";
  }

  return base;
}

/**
 * @returns {object[]}
 */
export function buildAllP0V1rEmailEnrichments() {
  return P0_V1R_EMAIL_SPECS.map((spec) => buildP0V1rEmailEnrichment(spec));
}
