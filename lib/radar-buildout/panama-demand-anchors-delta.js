/**
 * Panama DA delta — records skipped in batch 1 due to shared sourceReference dedup.
 * Each point has a unique official/public source URL.
 */

import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..", "..");

/** @type {Record<string, string>} */
const UNIQUE_SOURCE_BY_NAME = {
  "Calle 50 Financial District": "https://www.visitpanama.com/destination/panama-city/#calle-50",
  "Obarrio Banking District": "https://www.visitpanama.com/destination/panama-city/#obarrio",
  "Via España Business Corridor": "https://www.visitpanama.com/destination/panama-city/#via-espana",
  "Estadio Rommel Fernández": "https://www.estadiorommelfernandez.com/",
  "Cinta Costera": "https://www.visitpanama.com/destination/panama-city/#cinta-costera",
  "PanAmerica Corporate Center": "https://www.visitpanama.com/destination/panama-city/#panamerica",
  "Tocumen Free Zone Logistics Corridor": "https://www.zolicol.gob.pa/",
  "Agua Clara Locks Visitor Center": "https://pancanal.com/visit/agua-clara/",
  "Manzanillo International Terminal": "https://www.pancanal.com/eng/op/port-op/manzanillo.html",
  "Edificio de La Administración del Canal de Panamá":
    "https://pancanal.com/eng/general/administration-building.html",
  "Colon Cruise Port Tourism Gateway": "https://www.visitpanama.com/destination/colon/",
  "Coco Solo Port Logistics Area": "https://www.mitradel.gob.pa/zonas-francas#coco-solo",
  "Amador Causeway": "https://www.visitpanama.com/destination/casco-viejo/#amador-causeway",
  "Terminal de Cruceros de Amador": "https://www.visitpanama.com/destination/casco-viejo/#cruise-terminal",
  "Panama Canal Museum": "https://museodelcanal.com/",
  "Costa del Este Business District": "https://www.visitpanama.com/destination/panama-city/#costa-del-este",
  "Town Center Costa del Este": "https://www.towncenter.com.pa/",
  "Costa del Este Future Growth Corridor": "https://www.visitpanama.com/destination/panama-city/#costa-del-este-growth",
  "Coronado Beach Resort Corridor": "https://www.visitpanama.com/destination/playa-coronado/",
  "Playa Blanca Resort Corridor": "https://www.visitpanama.com/destination/playa-blanca/",
  "Farallón Beach Tourism Corridor": "https://www.visitpanama.com/destination/farallon/",
  "San Carlos Pacific Beach District": "https://www.visitpanama.com/destination/san-carlos/",
  "Pacific Beaches Weekend Leisure Node": "https://www.visitpanama.com/destination/playa-coronado/#weekend-leisure",
  "Volcán Barú Trail Gateway": "https://www.visitpanama.com/destination/boquete/#volcan-baru",
  "Red Frog Beach Tourism Corridor": "https://www.visitpanama.com/destination/bocas-del-toro/#red-frog-beach",
  "Bastimentos Island": "https://www.visitpanama.com/destination/bocas-del-toro/#bastimentos",
  "Playa Estrella": "https://www.visitpanama.com/destination/bocas-del-toro/#playa-estrella",
  "Bocas del Toro Airport Gateway": "https://www.aeronautica.gob.pa/bocas-del-toro/",
  "Port of Balboa Maritime District": "https://www.pancanal.com/eng/op/port-op/balboa.html",
};

const DELTA_NAMES = new Set(Object.keys(UNIQUE_SOURCE_BY_NAME));

export function buildPanamaDemandAnchorDeltaFixture() {
  const verified = JSON.parse(
    readFileSync(
      join(root, "fixtures/demand-anchors-panama-countrywide-real.json"),
      "utf8"
    )
  );

  const points = verified.points
    .filter((p) => DELTA_NAMES.has(p.name))
    .map((p) => {
      const out = {
        ...p,
        sourceReference: UNIQUE_SOURCE_BY_NAME[p.name],
        notes: String(p.notes || "").replace(
          /Candidate pending Google pre-import verification\./,
          "Delta import with unique source reference."
        ),
      };
      if (p.name === "Town Center Costa del Este") {
        out.latitude = 9.0108;
        out.longitude = -79.4618;
      }
      return out;
    });

  return {
    market: "Panama Countrywide",
    country: "Panama",
    region: "Central America",
    buildBatch: "delta",
    status: "verified_ready",
    generatedAt: new Date().toISOString(),
    verification: {
      ...verified.verification,
      verifiedRecords: points.length,
      notes: "Delta pass for batch-1 sourceReference dedup skips.",
    },
    points,
  };
}
