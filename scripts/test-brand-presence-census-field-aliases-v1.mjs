#!/usr/bin/env node
/**
 * Regression: Hotel Census records using HPC-style field names must map into HE overview DTOs.
 */
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { pathToFileURL } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

// Exercise the same alias resolution via a minimal inline replica of the critical reads
// (formatHotelRecord is not exported). Keep in sync with api/brand-presence.js.
function readTextField(fields, key) {
  const raw = fields[key];
  if (raw == null || raw === "") return null;
  if (typeof raw === "string") return raw.trim() || null;
  return String(raw).trim() || null;
}
function readTextFieldAliases(fields, keys) {
  for (const key of keys) {
    const value = readTextField(fields, key);
    if (value) return value;
  }
  return null;
}
function readNumberField(fields, key) {
  const raw = fields[key];
  if (raw == null || raw === "") return null;
  const num = Number(raw);
  return Number.isFinite(num) ? num : null;
}
function readNumberFieldAliases(fields, keys) {
  for (const key of keys) {
    const value = readNumberField(fields, key);
    if (value != null) return value;
  }
  return null;
}

const fields = {
  "Property Name": "Bethesda Marriott",
  "Current Brand": "Marriott Hotels",
  "Rooms / Keys": 407,
  "Official Property URL": "https://www.marriott.com/en-us/hotels/wasbt-bethesda-marriott/overview/",
  Phone: "+1 301-897-9400",
  "Operator / Management Company": "Marriott International (Managed)",
  "Hotel Class / Segment": "Upper Upscale",
  "Management Model": "Management Agreement",
  Parent: undefined,
};

const rooms = readNumberFieldAliases(fields, ["rooms", "Rooms / Keys", "Rooms"]) || 0;
const website = readTextFieldAliases(fields, ["Website", "Official Property URL"]);
const telephone = readTextFieldAliases(fields, ["Telephone", "Phone"]);
const managementCompany = readTextFieldAliases(fields, [
  "Management Company",
  "Operator / Management Company",
]);
const chainScale = readTextFieldAliases(fields, ["Chain Scale", "Hotel Class / Segment"]);
const operationType = readTextFieldAliases(fields, ["Operation Type", "Management Model"]);

assert.equal(rooms, 407);
assert.equal(website.includes("marriott.com"), true);
assert.equal(telephone, "+1 301-897-9400");
assert.match(managementCompany, /Marriott/);
assert.equal(chainScale, "Upper Upscale");
assert.equal(operationType, "Management Agreement");

// Confirm brand-presence source file contains the alias helpers (guard against revert).
const src = await import("node:fs").then((fs) =>
  fs.readFileSync(path.join(ROOT, "api/brand-presence.js"), "utf8"),
);
assert.match(src, /readNumberFieldAliases/);
assert.match(src, /Rooms \/ Keys/);
assert.match(src, /Official Property URL/);
assert.match(src, /Operator \/ Management Company/);

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "test-brand-presence-census-field-aliases-v1",
      rooms,
      website,
      telephone,
      managementCompany,
      chainScale,
      operationType,
    },
    null,
    2,
  ),
);
