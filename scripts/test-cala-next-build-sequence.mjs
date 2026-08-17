#!/usr/bin/env node
/**
 * Post-Colombia CALA build sequence validation.
 */
import { readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { getCountryConfig } from "../lib/radar-buildout/country-configs.js";
import {
  POST_COLOMBIA_BUILD_SEQUENCE,
  listPostColombiaSequenceCountries,
} from "../lib/radar-buildout/post-colombia-build-sequence.js";
import { BUILD_STRATEGY_TYPES } from "../lib/radar-buildout/country-build-strategies.js";
import { buildCountryPlanPayload } from "../lib/radar-buildout/build-plan-generator.js";
import { buildRadarBuildPlanAirtableFields } from "../lib/radar-buildout/airtable-radar-build-plans-io.js";
import { RADAR_BUILD_PLANS_FIELDS as F } from "../lib/radar-buildout/airtable-radar-build-plans-fields.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

let failed = 0;
function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else console.log("ok:", msg);
}

const sequence = listPostColombiaSequenceCountries();
assert(sequence.length === 38, "post-Colombia sequence has 38 countries");
assert(sequence[0].country === "Puerto Rico" && sequence[0].recommendedBuildSequence === 1, "PR is sequence 1");
assert(sequence[2].country === "Colombia" && sequence[2].recommendedBuildSequence === 3, "Colombia is sequence 3");
assert(sequence[3].country === "Mexico" && sequence[3].recommendedBuildSequence === 4, "Mexico is sequence 4");

const mexico = getCountryConfig("Mexico");
assert(mexico.nextBuildMarket === "Completed / Tier 1 Markets", "Mexico Tier 1 markets complete");
assert(/Cancún|Tier-1|CDMX|markets complete/i.test(mexico.buildApproachNotes || mexico.notes || ""), "Mexico notes document market builds");
assert(mexico.marketSubmarkets?.["Cancún / Riviera Maya"]?.length >= 9, "Mexico Cancún submarkets defined");

const brazil = getCountryConfig("Brazil");
assert(/deferred/i.test(brazil.buildApproachNotes), "Brazil notes say deferred");
assert(brazil.nextBuildMarket.includes("São Paulo"), "Brazil next build market SP/Rio");
assert(brazil.recommendedBuildSequence === 16, "Brazil sequence 16 after Caribbean islands");

const bahamas = getCountryConfig("Bahamas");
assert(bahamas.buildStrategy === BUILD_STRATEGY_TYPES.ISLAND_COUNTRYWIDE, "Bahamas island countrywide");
assert(bahamas.submarkets.includes("Paradise Island"), "Bahamas has Paradise Island submarket");
assert(bahamas.submarkets.includes("Cable Beach / Baha Mar"), "Bahamas has Cable Beach / Baha Mar");
assert(/Completed|Deal Ready/i.test(bahamas.nextBuildMarket), "Bahamas next build market marked complete");

const panama = getCountryConfig("Panama");
assert(panama.buildStrategy === BUILD_STRATEGY_TYPES.CORRIDOR_BASED, "Panama corridor-based");
assert(panama.submarkets.includes("Costa del Este"), "Panama has Costa del Este submarket");
assert(/Completed|Deal Ready/i.test(panama.nextBuildMarket), "Panama next build market marked complete");

const costaRica = getCountryConfig("Costa Rica");
assert(costaRica.buildStrategy === BUILD_STRATEGY_TYPES.CORRIDOR_BASED, "Costa Rica corridor-based");
assert(costaRica.submarkets.includes("Monteverde"), "Costa Rica has Monteverde submarket");

const fixturePaths = [
  "fixtures/demand-anchors-mexico-cancun-riviera-maya-candidates.json",
  "fixtures/demand-anchors-panama-countrywide-candidates.json",
  "fixtures/demand-anchors-costa-rica-countrywide-candidates.json",
];

for (const rel of fixturePaths) {
  const abs = join(root, rel);
  assert(existsSync(abs), `fixture exists: ${rel}`);
  const fixture = JSON.parse(readFileSync(abs, "utf8"));
  assert(Array.isArray(fixture.points), `${rel} has points array`);
  assert(fixture.governanceRequired === true, `${rel} governanceRequired`);
  assert(fixture.googlePreImportVerificationRecommended === true, `${rel} google verification recommended`);
  const names = (fixture.points || []).map((p) => String(p.name || "").toLowerCase());
  assert(!names.some((n) => /sample|placeholder/.test(n)), `${rel} has no sample names`);
}

const mexicoPlan = buildCountryPlanPayload("Mexico", mexico, {
  summary: { demandAnchors: 0, travelInfrastructure: 0, totalRadarPoints: 0 },
});
assert(mexicoPlan.nextBuildMarket === "Completed / Tier 1 Markets", "Mexico plan payload includes next market");
assert(mexicoPlan.recommendedBuildSequence === 4, "Mexico plan sequence 4");

const fields = buildRadarBuildPlanAirtableFields(mexicoPlan);
assert(fields[F.recommendedBuildSequence] === 4, "Airtable fields include recommended build sequence");
assert(fields[F.nextBuildMarket] === "Completed / Tier 1 Markets", "Airtable fields include next build market");
assert(fields[F.buildApproachNotes], "Airtable fields include build approach notes");

assert(POST_COLOMBIA_BUILD_SEQUENCE.Peru.recommendedBuildSequence === 7, "Peru sequence 7");
assert(POST_COLOMBIA_BUILD_SEQUENCE.Jamaica.recommendedBuildSequence === 9, "Jamaica sequence 9");
assert(POST_COLOMBIA_BUILD_SEQUENCE.Bahamas.recommendedBuildSequence === 10, "Bahamas sequence 10");
assert(POST_COLOMBIA_BUILD_SEQUENCE.Brazil.recommendedBuildSequence === 16, "Brazil sequence 16");

const barbados = getCountryConfig("Barbados");
assert(barbados.buildStrategy === BUILD_STRATEGY_TYPES.ISLAND_COUNTRYWIDE, "Barbados island countrywide");
assert(/Completed|Deal Ready/i.test(barbados.nextBuildMarket), "Barbados next build market marked complete");

const cayman = getCountryConfig("Cayman Islands");
assert(cayman.buildStrategy === BUILD_STRATEGY_TYPES.ISLAND_COUNTRYWIDE, "Cayman Islands island countrywide");
assert(/Completed|Deal Ready/i.test(cayman.nextBuildMarket), "Cayman Islands next build market marked complete");

const turks = getCountryConfig("Turks & Caicos");
assert(turks.buildStrategy === BUILD_STRATEGY_TYPES.ISLAND_COUNTRYWIDE, "Turks & Caicos island countrywide");
assert(/Completed|Deal Ready/i.test(turks.nextBuildMarket), "Turks & Caicos next build market marked complete");

for (const country of [
  "Saint Lucia",
  "Antigua and Barbuda",
  "Grenada",
  "Saint Vincent and the Grenadines",
  "Dominica",
  "Saint Kitts and Nevis",
  "Trinidad and Tobago",
  "British Virgin Islands",
]) {
  const c = getCountryConfig(country);
  assert(c.buildStrategy === BUILD_STRATEGY_TYPES.ISLAND_COUNTRYWIDE, `${country} island countrywide`);
  assert(/Completed|Deal Ready/i.test(c.nextBuildMarket), `${country} next build market marked complete`);
}

for (const country of ["Belize", "Guatemala", "Honduras", "Nicaragua", "El Salvador"]) {
  const c = getCountryConfig(country);
  assert(c.region === "Central America", `${country} region Central America`);
  assert(c.buildStrategy === BUILD_STRATEGY_TYPES.CORRIDOR_BASED, `${country} corridor-based`);
  assert(/Completed|Deal Ready/i.test(c.nextBuildMarket), `${country} next build market marked complete`);
  const fixtureRel = `fixtures/demand-anchors-${country.toLowerCase().replace(/\s+/g, "-")}-countrywide-candidates.json`;
  const abs = join(root, fixtureRel);
  assert(existsSync(abs), `fixture exists: ${fixtureRel}`);
}

for (const country of ["Argentina", "Ecuador", "Uruguay"]) {
  const c = getCountryConfig(country);
  assert(c.region === "South America", `${country} region South America`);
  assert(c.buildStrategy === BUILD_STRATEGY_TYPES.CORRIDOR_BASED, `${country} corridor-based`);
  assert(/Completed|Deal Ready/i.test(c.nextBuildMarket), `${country} next build market marked complete`);
  const slug = country.toLowerCase();
  const fixtureRel = `fixtures/demand-anchors-${slug}-countrywide-candidates.json`;
  assert(existsSync(join(root, fixtureRel)), `fixture exists: ${fixtureRel}`);
}

const peru = getCountryConfig("Peru");
assert(/Completed|Deal Ready/i.test(peru.nextBuildMarket), "Peru next build market marked complete");

if (failed) {
  console.error("\n" + failed + " test(s) failed");
  process.exit(1);
}
console.log("\nAll post-Colombia build sequence tests passed.");
