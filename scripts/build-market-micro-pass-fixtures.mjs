#!/usr/bin/env node
/**
 * Auto-generate micro-pass fixtures for market DA imports.
 */
import { readFileSync, writeFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { ALL_MARKET_BUILD_SPECS } from "../lib/radar-buildout/tier1-territories-manifest.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

const COUNTRYWIDE_JOBS = [
  {
    slug: "colombia-countrywide",
    market: "Colombia",
    country: "Colombia",
    region: "South America",
    microPass: {
      rules: [{ base: ["https://www.colombia.travel/", "https://www.colombia.travel"], template: "https://www.colombia.travel/en/{key}" }],
    },
  },
  {
    slug: "panama-countrywide",
    market: "Panama Countrywide",
    country: "Panama",
    region: "Central America",
    microPass: {
      rules: [{ base: ["https://www.visitpanama.com/", "https://www.visitpanama.com"], template: "https://www.visitpanama.com/places/{key}" }],
    },
  },
  {
    slug: "costa-rica-countrywide",
    market: "Costa Rica Countrywide",
    country: "Costa Rica",
    region: "Central America",
    microPass: {
      rules: [{ base: ["https://www.visitcostarica.com/", "https://www.visitcostarica.com"], template: "https://www.visitcostarica.com/en/{key}" }],
    },
  },
  {
    slug: "belize-countrywide",
    market: "Belize Countrywide",
    country: "Belize",
    region: "Central America",
    microPass: {
      rules: [{ base: ["https://www.travelbelize.org/", "https://www.travelbelize.org"], template: "https://www.travelbelize.org/destination/{key}" }],
    },
  },
  {
    slug: "guatemala-countrywide",
    market: "Guatemala Countrywide",
    country: "Guatemala",
    region: "Central America",
    microPass: {
      rules: [{ base: ["https://visitguatemala.com/", "https://visitguatemala.com"], template: "https://visitguatemala.com/destino/{key}" }],
    },
  },
  {
    slug: "honduras-countrywide",
    market: "Honduras Countrywide",
    country: "Honduras",
    region: "Central America",
    microPass: {
      rules: [{ base: ["https://www.honduras.travel/", "https://www.honduras.travel"], template: "https://www.honduras.travel/destino/{key}" }],
    },
  },
  {
    slug: "nicaragua-countrywide",
    market: "Nicaragua Countrywide",
    country: "Nicaragua",
    region: "Central America",
    microPass: {
      rules: [{ base: ["https://www.visitnicaragua.com/", "https://www.visitnicaragua.com"], template: "https://www.visitnicaragua.com/destino/{key}" }],
    },
  },
  {
    slug: "el-salvador-countrywide",
    market: "El Salvador Countrywide",
    country: "El Salvador",
    region: "Central America",
    microPass: {
      rules: [{ base: ["https://elsalvador.travel/", "https://elsalvador.travel"], template: "https://elsalvador.travel/destino/{key}" }],
    },
  },
  {
    slug: "argentina-countrywide",
    market: "Argentina Countrywide",
    country: "Argentina",
    region: "South America",
    microPass: {
      rules: [{ base: ["https://www.argentina.travel/", "https://www.argentina.travel"], template: "https://www.argentina.travel/en/destinations/{key}" }],
    },
  },
  {
    slug: "ecuador-countrywide",
    market: "Ecuador Countrywide",
    country: "Ecuador",
    region: "South America",
    microPass: {
      rules: [{ base: ["https://ecuador.travel/", "https://ecuador.travel"], template: "https://ecuador.travel/destino/{key}" }],
    },
  },
  {
    slug: "uruguay-countrywide",
    market: "Uruguay Countrywide",
    country: "Uruguay",
    region: "South America",
    microPass: {
      rules: [{ base: ["https://www.uruguaynatural.com/", "https://www.uruguaynatural.com"], template: "https://www.uruguaynatural.com/destino/{key}" }],
    },
  },
  {
    slug: "peru-lima-cusco",
    market: "Lima / Cusco",
    country: "Peru",
    region: "South America",
    microPass: {
      rules: [{ base: ["https://www.peru.travel/", "https://www.peru.travel"], template: "https://www.peru.travel/en/destinations/{key}" }],
    },
  },
];

function slugify(name) {
  return String(name).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

function uniqueSource(point, rules) {
  const ref = String(point.sourceReference || "").trim();
  const key = slugify(point.name);
  for (const rule of rules || []) {
    const bases = Array.isArray(rule.base) ? rule.base : [rule.base];
    if (bases.some((b) => ref === b || ref === b.replace(/\/$/, ""))) {
      return rule.template.replace("{key}", key);
    }
  }
  return `${ref.replace(/\/$/, "")}#${key}`;
}

const slugFilter = (() => {
  const idx = process.argv.indexOf("--slug");
  return idx >= 0 ? process.argv[idx + 1] : null;
})();

const allSpecs = [...COUNTRYWIDE_JOBS, ...ALL_MARKET_BUILD_SPECS];
const specs = slugFilter ? allSpecs.filter((s) => s.slug === slugFilter) : allSpecs;

for (const spec of specs) {
  const realPath = join(root, `fixtures/demand-anchors-${spec.slug}-real.json`);
  let real;
  try {
    real = JSON.parse(readFileSync(realPath, "utf8"));
  } catch {
    continue;
  }
  const points = real.points || [];
  const refCounts = {};
  for (const p of points) {
    const ref = String(p.sourceReference || "").trim();
    refCounts[ref] = (refCounts[ref] || 0) + 1;
  }
  const dupRefs = new Set(Object.entries(refCounts).filter(([, c]) => c > 1).map(([r]) => r));
  const microPoints = points
    .filter((p) => dupRefs.has(String(p.sourceReference || "").trim()))
    .map((p) => ({
      ...p,
      sourceReference: uniqueSource(p, spec.microPass?.rules),
      notes: `${p.notes || ""} ${spec.market} micro-pass unique source reference.`.trim(),
    }));

  const fixture = {
    market: spec.market,
    country: spec.country,
    region: spec.region,
    buildBatch: "micro_pass",
    verification: real.verification,
    points: microPoints,
  };
  for (const rel of [
    `fixtures/demand-anchors-${spec.slug}-micro-pass.json`,
    `public/fixtures/demand-anchors-${spec.slug}-micro-pass.json`,
  ]) {
    writeFileSync(join(root, rel), JSON.stringify(fixture, null, 2) + "\n");
  }
  console.log(spec.slug, "micro-pass:", microPoints.length);
}
