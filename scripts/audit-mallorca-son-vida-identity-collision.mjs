#!/usr/bin/env node
/**
 * Cross-hotel identity collision audit for Castillo ↔ Sheraton Son Vida.
 */
import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { canonicalizeForProperty } from "../lib/ai-demand-positioning/metrics/adp-property-entity-registries.js";

const CASTILLO = "adp_castillo_hotel_son_vida";
const SHERATON = "adp_sheraton_mallorca_arabella_golf";

const castilloProfile = loadPropertyProfile(CASTILLO);
const sheratonProfile = loadPropertyProfile(SHERATON);

const probes = [
  { mention: "Castillo Hotel Son Vida", expectCastillo: "castillo_hotel_son_vida", expectSheraton: "castillo_hotel_son_vida" },
  { mention: "Castillo Hotel Son Vida, a Luxury Collection Hotel", expectCastillo: "castillo_hotel_son_vida", expectSheraton: "castillo_hotel_son_vida" },
  { mention: "Castillo Hotel Son Vida Mallorca", expectCastillo: "castillo_hotel_son_vida", expectSheraton: "castillo_hotel_son_vida" },
  { mention: "Castillo Son Vida", expectCastillo: "castillo_hotel_son_vida", expectSheraton: "castillo_hotel_son_vida" },
  { mention: "Sheraton Mallorca Arabella Golf Hotel", expectCastillo: "sheraton_mallorca_arabella_golf", expectSheraton: "sheraton_mallorca_arabella_golf" },
  { mention: "Sheraton Mallorca Arabella", expectCastillo: "sheraton_mallorca_arabella_golf", expectSheraton: "sheraton_mallorca_arabella_golf" },
  { mention: "Sheraton Arabella Golf Hotel", expectCastillo: "sheraton_mallorca_arabella_golf", expectSheraton: "sheraton_mallorca_arabella_golf" },
  { mention: "Sheraton Mallorca", expectCastillo: "sheraton_mallorca_arabella_golf", expectSheraton: "sheraton_mallorca_arabella_golf" },
  { mention: "Arabella Golf Hotel", expectCastillo: null, expectSheraton: null, note: "ambiguous — must not bind either subject" },
  { mention: "Son Vida Golf", expectCastillo: null, expectSheraton: null, note: "venue/club — not hotel" },
  { mention: "Luxury Collection", expectCastillo: null, expectSheraton: null, note: "brand-only" },
  { mention: "Sheraton", expectCastillo: null, expectSheraton: null, note: "brand-only" },
  { mention: "Marriott", expectCastillo: null, expectSheraton: null, note: "brand-only" },
];

const rows = probes.map((p) => {
  const viaCastillo = canonicalizeForProperty(CASTILLO, p.mention);
  const viaSheraton = canonicalizeForProperty(SHERATON, p.mention);
  const castilloOk =
    p.expectCastillo === null ? !viaCastillo || viaCastillo === "AMBIGUOUS" || viaCastillo.startsWith?.("UNRESOLVED") || viaCastillo === null
      : viaCastillo === p.expectCastillo || viaCastillo?.entityId === p.expectCastillo || viaCastillo === p.expectCastillo;
  // canonicalize may return entityId string or object depending on registry API
  const cId = typeof viaCastillo === "string" ? viaCastillo : viaCastillo?.entityId || viaCastillo;
  const sId = typeof viaSheraton === "string" ? viaSheraton : viaSheraton?.entityId || viaSheraton;
  const passC =
    p.expectCastillo == null
      ? !cId || cId === "AMBIGUOUS" || String(cId).includes("UNRESOLVED") || cId === "GENERIC_PHRASE" || cId === "VENUE_ONLY" || cId === "BRAND_NOT_PROPERTY" || cId === "LOCATION"
      : cId === p.expectCastillo;
  const passS =
    p.expectSheraton == null
      ? !sId || sId === "AMBIGUOUS" || String(sId).includes("UNRESOLVED") || sId === "GENERIC_PHRASE" || sId === "VENUE_ONLY" || sId === "BRAND_NOT_PROPERTY" || sId === "LOCATION"
      : sId === p.expectSheraton;
  return {
    mention: p.mention,
    note: p.note || null,
    resolveViaCastilloRegistry: cId,
    resolveViaSheratonRegistry: sId,
    pass: Boolean(passC && passS),
    passCastillo: Boolean(passC),
    passSheraton: Boolean(passS),
  };
});

const collisions = [];
if (castilloProfile?.hpcHotelId === sheratonProfile?.hpcHotelId) {
  collisions.push("SHARED_HPC_HOTEL_ID");
}
if (castilloProfile?.censusRecordId === sheratonProfile?.censusRecordId) {
  collisions.push("SHARED_CENSUS_RECORD_ID");
}
const sharedAlias = (castilloProfile?.identityAliases || []).filter((a) =>
  (sheratonProfile?.identityAliases || []).map((x) => x.toLowerCase()).includes(a.toLowerCase())
);
if (sharedAlias.length) collisions.push(`SHARED_ALIASES:${sharedAlias.join("|")}`);

const audit = {
  ok: rows.every((r) => r.pass) && collisions.length === 0,
  castillo: {
    propertyId: CASTILLO,
    hpcHotelId: castilloProfile?.hpcHotelId,
    name: castilloProfile?.name,
    brand: castilloProfile?.brand,
    rooms: castilloProfile?.rooms,
    aliases: castilloProfile?.identityAliases,
    exclusions: castilloProfile?.identityConfusableExclusions,
  },
  sheraton: {
    propertyId: SHERATON,
    hpcHotelId: sheratonProfile?.hpcHotelId,
    name: sheratonProfile?.name,
    brand: sheratonProfile?.brand,
    rooms: sheratonProfile?.rooms,
    aliases: sheratonProfile?.identityAliases,
    exclusions: sheratonProfile?.identityConfusableExclusions,
  },
  probeResults: rows,
  structuralCollisions: collisions,
  IDENTITY_COLLISIONS_FOUND: collisions.length + rows.filter((r) => !r.pass).length,
};

const outDir = join(process.cwd(), "reports/adp");
mkdirSync(join(outDir, "castillo-hotel-son-vida-e2e"), { recursive: true });
mkdirSync(join(outDir, "sheraton-mallorca-arabella-golf-e2e"), { recursive: true });
writeFileSync(
  join(outDir, "castillo-hotel-son-vida-e2e", "IDENTITY_AUDIT.md"),
  renderMd(audit)
);
writeFileSync(
  join(outDir, "sheraton-mallorca-arabella-golf-e2e", "IDENTITY_AUDIT.md"),
  renderMd(audit)
);
writeFileSync(join(process.cwd(), "reports/ai-demand-positioning", "mallorca-son-vida-identity-collision-audit.json"), JSON.stringify(audit, null, 2));
console.log(JSON.stringify(audit, null, 2));
process.exit(audit.ok ? 0 : 1);

function renderMd(a) {
  return `# Mallorca Son Vida Cross-Hotel Identity Collision Audit

## Verdict
${a.ok ? "PASS — no identity crossover detected" : "FAIL — see collisions / failed probes"}

## Castillo
- HPC: \`${a.castillo.hpcHotelId}\`
- Name: ${a.castillo.name}
- Brand: ${a.castillo.brand}
- Rooms: ${a.castillo.rooms}

## Sheraton
- HPC: \`${a.sheraton.hpcHotelId}\`
- Name: ${a.sheraton.name}
- Brand: ${a.sheraton.brand}
- Rooms: ${a.sheraton.rooms}

## Structural collisions
${a.structuralCollisions.length ? a.structuralCollisions.map((c) => `- ${c}`).join("\n") : "- none"}

## Probe results
| Mention | Via Castillo registry | Via Sheraton registry | Pass |
|---|---|---|---|
${a.probeResults.map((r) => `| ${r.mention} | ${r.resolveViaCastilloRegistry} | ${r.resolveViaSheratonRegistry} | ${r.pass ? "YES" : "NO"} |`).join("\n")}

## IDENTITY_COLLISIONS_FOUND
${a.IDENTITY_COLLISIONS_FOUND}
`;
}
