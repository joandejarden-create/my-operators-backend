#!/usr/bin/env node
/**
 * Regression gate: HPC_OPEN_PIPELINE_COORDINATE_COMPLETENESS_GATE_V1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  HPC_COORDINATE_COMPLETENESS_GATE_VERSION,
  validateHpcCoordinates,
  evaluateOpenPipelineActivation,
  buildCoordinateCompletenessDiagnostics,
  buildProposedCoordinatePatch,
  detectCityCenterProximity,
} from "../lib/hotel-census/hpc-coordinate-completeness-gate-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

assert.equal(
  HPC_COORDINATE_COMPLETENESS_GATE_VERSION,
  "HPC_OPEN_PIPELINE_COORDINATE_COMPLETENESS_GATE_V1"
);

// Missing
{
  const v = validateHpcCoordinates({
    lat: null,
    lng: null,
    address: "249 Queens Quay West",
    city: "Toronto",
    country: "Canada",
  });
  assert.equal(v.hardFail, true);
  assert.ok(v.hardIssues.includes("MISSING_LATITUDE_OR_LONGITUDE"));
  assert.equal(v.mapEligible, false);
}

// 0,0
{
  const v = validateHpcCoordinates({
    lat: 0,
    lng: 0,
    address: "249 Queens Quay West",
    city: "Toronto",
    country: "Canada",
  });
  assert.ok(v.hardIssues.includes("ZERO_ZERO_COORDINATES"));
}

// Out of range
{
  const v = validateHpcCoordinates({
    lat: 120,
    lng: -79,
    address: "x",
    city: "Toronto",
    country: "Canada",
  });
  assert.ok(v.hardIssues.includes("OUT_OF_RANGE"));
}

// Country bounds mismatch
{
  const v = validateHpcCoordinates({
    lat: -33.86,
    lng: 151.2,
    address: "249 Queens Quay West",
    city: "Toronto",
    country: "Canada",
  });
  assert.ok(v.hardIssues.includes("COUNTRY_BOUNDS_MISMATCH"));
}

// Confirmed city-center fallback → hard
{
  const v = validateHpcCoordinates({
    lat: 43.6532,
    lng: -79.3832,
    address: "Toronto",
    city: "Toronto",
    country: "Canada",
    confirmedCityCenterFallback: true,
  });
  assert.ok(v.hardIssues.includes("CONFIRMED_CITY_CENTER_FALLBACK"));
}

// Near city center but property-level → soft only (do not hard-block)
{
  const v = validateHpcCoordinates({
    lat: 43.6532,
    lng: -79.3832,
    address: "100 Queen St W",
    city: "Toronto",
    country: "Canada",
    confirmedCityCenterFallback: false,
  });
  assert.equal(v.hardFail, false);
  assert.ok(v.softIssues.includes("POTENTIAL_CITY_CENTER_UNCONFIRMED"));
}

// Accurate harbourfront pin near but not city center → pass
{
  const v = validateHpcCoordinates({
    lat: 43.6387,
    lng: -79.3855,
    address: "249 Queens Quay West",
    city: "Toronto",
    country: "Canada",
  });
  assert.equal(v.ok, true);
  assert.equal(v.hardFail, false);
  assert.equal(v.mapEligible, true);
  assert.ok(!v.issues.includes("POTENTIAL_CITY_CENTER_UNCONFIRMED"));
}

// Address unrelated → hard
{
  const v = validateHpcCoordinates({
    lat: 40.42,
    lng: -3.7,
    address: "249 Queens Quay West",
    city: "Toronto",
    country: "Canada",
    addressUnrelated: true,
  });
  assert.ok(v.hardIssues.includes("ADDRESS_UNRELATED_COORDINATES"));
}

// Activation blocked for new Open without coords; status preserved on existing exception
{
  const blocked = evaluateOpenPipelineActivation(
    {
      "Radar Status": "Hidden",
      Latitude: null,
      Longitude: null,
      Address: "Calle de la Paz 11",
      City: "Madrid",
      Country: "Spain",
    },
    { targetStatus: "Open", isNewActivation: true }
  );
  assert.equal(blocked.activationBlocked, true);
  assert.equal(blocked.preserveStatus, true);

  const existing = evaluateOpenPipelineActivation({
    "Radar Status": "Open",
    Latitude: null,
    Longitude: null,
    Address: "249 Queens Quay West",
    City: "Toronto",
    Country: "Canada",
  });
  assert.equal(existing.existingExceptionFlag, true);
  assert.equal(existing.preserveStatus, true);
  assert.equal(existing.status, "Open");
}

// Hidden without coords — not an Open/Pipeline activation target
{
  const h = evaluateOpenPipelineActivation({
    "Radar Status": "Hidden",
    Latitude: null,
    Longitude: null,
  });
  assert.equal(h.targetingOpenPipeline, false);
  assert.equal(h.activationBlocked, false);
}

// Diagnostics batch
{
  const diag = buildCoordinateCompletenessDiagnostics([
    {
      id: "recH9h3McgMNFQEla",
      fields: {
        "Property Name": "Radisson Blu Toronto Downtown",
        "Radar Status": "Open",
        Address: "249 Queens Quay West",
        City: "Toronto",
        Country: "Canada",
      },
    },
    {
      id: "recClean000000001",
      fields: {
        "Property Name": "Clean Hotel",
        "Radar Status": "Open",
        Address: "1 Main St",
        City: "Bethesda",
        Country: "United States",
        Latitude: 39.016519,
        Longitude: -77.103463,
      },
    },
    {
      id: "recHidden00000001",
      fields: {
        "Property Name": "Hidden",
        "Radar Status": "Hidden",
        Latitude: null,
        Longitude: null,
      },
    },
  ]);
  assert.equal(diag.hardExceptionCount, 1);
  assert.equal(diag.missingCoordinates[0].id, "recH9h3McgMNFQEla");
  assert.ok(!diag.hardExceptions.some((e) => e.id === "recHidden00000001"));
}

// Proposed patch builder
{
  const patch = buildProposedCoordinatePatch({
    before: { Latitude: null, Longitude: null },
    geocode: {
      lat: 40.4155,
      lng: -3.7072,
      provider: "mapbox",
      method: "mapbox_permanent_geocode",
      confidence: "High",
      formatted_address: "Calle de la Paz 11, 28012 Madrid, Spain",
      query: "UMusic Hotel Madrid, Calle de la Paz 11, Madrid, Spain",
      permanent: true,
      relevance: 0.95,
    },
    verifiedAt: "2026-11-18",
  });
  assert.equal(patch.airtableWrite, false);
  assert.equal(patch.after.Latitude, 40.4155);
  assert.equal(patch.after["Coordinate Confidence"], "High");
  assert.equal(patch.after["Geocode Provider"], "Mapbox");
  assert.equal(patch.after["Geocode Reviewed Date"], "2026-11-18");
}

// Proximity helper: near center soft; confirmed hard path separate
{
  const soft = detectCityCenterProximity({
    lat: 40.4168,
    lng: -3.7038,
    city: "Madrid",
    country: "Spain",
  });
  assert.equal(soft.soft, true);
  assert.equal(soft.confirmed, false);
}

// Contract files present
assert.ok(
  fs.existsSync(
    path.join(ROOT, "lib/hotel-census/hpc-coordinate-completeness-gate-v1.js")
  )
);

console.log(
  JSON.stringify({
    ok: true,
    gate: "test:hpc-coordinate-completeness-gate-v1",
    contract: HPC_COORDINATE_COMPLETENESS_GATE_VERSION,
  })
);
