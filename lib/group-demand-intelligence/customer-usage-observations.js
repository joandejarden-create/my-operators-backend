/**
 * Hotel-level GDI customer usage / outreach observations.
 * Not per-opportunity actions — used when hotel reports aggregate outreach
 * without identifying exactly which opportunities were contacted.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(
  __dirname,
  "../../data/group-demand-intelligence/hotels"
);

function hotelDir(hotelId) {
  return path.join(ROOT, String(hotelId || "").trim());
}

function observationsPath(hotelId) {
  return path.join(hotelDir(hotelId), "customer-usage-observations.json");
}

function observationId({ hotelId, observationDate, kind, evidenceKey }) {
  const raw = [
    String(hotelId || "").trim(),
    String(observationDate || "").slice(0, 10),
    String(kind || "").trim(),
    String(evidenceKey || "").trim(),
  ].join("|");
  return `gdi_usage_${createHash("sha256").update(raw).digest("hex").slice(0, 16)}`;
}

export function loadCustomerUsageObservations(hotelId) {
  const p = observationsPath(hotelId);
  if (!fs.existsSync(p)) {
    return { hotelId, schemaVersion: 1, observations: [] };
  }
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return { hotelId, schemaVersion: 1, observations: [] };
  }
}

/**
 * Idempotent upsert by deterministic observationId.
 * @returns {{ observation, created: boolean, doc }}
 */
export function upsertCustomerUsageObservation(hotelId, input = {}) {
  const hotel = String(hotelId || "").trim();
  if (!hotel) throw new Error("hotelId_required");

  const observationDate = String(input.observationDate || "").slice(0, 10);
  const kind = String(input.kind || "OUTREACH_SUMMARY").trim();
  const evidenceKey = String(input.evidenceKey || "").trim();
  const id =
    input.observationId ||
    observationId({ hotelId: hotel, observationDate, kind, evidenceKey });

  const doc = loadCustomerUsageObservations(hotel);
  const existing = (doc.observations || []).find((o) => o.observationId === id);
  if (existing) {
    return { observation: existing, created: false, doc };
  }

  const observation = {
    observationId: id,
    hotelId: hotel,
    kind,
    observationDate,
    opportunitiesSurfaced:
      input.opportunitiesSurfaced != null
        ? Number(input.opportunitiesSurfaced)
        : null,
    opportunitiesContacted:
      input.opportunitiesContacted != null
        ? Number(input.opportunitiesContacted)
        : null,
    sourceActor: input.sourceActor || null,
    sourceOrganization: input.sourceOrganization || null,
    sourceType: input.sourceType || "CUSTOMER_FIRST_PARTY_EMAIL",
    evidenceSummary: input.evidenceSummary || null,
    note: input.note || null,
    createdAt: new Date().toISOString(),
    schemaVersion: 1,
  };

  doc.hotelId = hotel;
  doc.schemaVersion = 1;
  doc.observations = [...(doc.observations || []), observation];
  doc.updatedAt = new Date().toISOString();

  fs.mkdirSync(hotelDir(hotel), { recursive: true });
  fs.writeFileSync(observationsPath(hotel), JSON.stringify(doc, null, 2), "utf8");

  return { observation, created: true, doc };
}
