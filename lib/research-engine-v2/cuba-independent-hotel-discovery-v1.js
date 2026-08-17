/**
 * Cuba independent hotel discovery v1 — official/public group directories.
 * Never uses benchmark property identities. Never HBX-required.
 * Candidate-only until Core Geography Closeout AUTO_APPLY gate.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";

import { normName } from "./census-autopilot-v2/identity-dedupe.js";
import {
  resolveCubaProvinceFromCity,
  CUBA_CITY_TO_PROVINCE,
} from "./cala-admin-geography-library-v1.js";
import { normalizePlaceKey } from "./census-city-state-normalizer.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const CUBA_DISCOVERY_VERSION = "cuba-independent-hotel-discovery-v1";

const CACHE_DIR = path.join(ROOT, "data/research-engine-v2/cuba-independent-discovery");

const SOURCE_URLS = Object.freeze([
  {
    id: "gaviota_experiences",
    url: "https://www.gaviotahotels.com/en/cuba-experiences",
    group: "Gaviota Hotels",
  },
  {
    id: "cubanacan_hotels",
    url: "https://www.cubanacanhoteles.com/en/hotels",
    group: "Cubanacán",
  },
  {
    id: "gran_caribe_home",
    url: "https://www.gran-caribe.com/",
    group: "Gran Caribe",
  },
]);

/** Destination seeds when directory page does not emit locality. */
const NAME_LOCALITY_HINTS = [
  { re: /cayo santa mar[ií]a|santa mar[ií]a/i, city: "Cayo Santa María" },
  { re: /cayo guillermo/i, city: "Cayo Guillermo" },
  { re: /cayo coco|coco plus/i, city: "Cayo Coco" },
  { re: /cayo las brujas|las brujas/i, city: "Cayo Las Brujas" },
  { re: /cayo largo/i, city: "Cayo Largo" },
  { re: /varadero|puntarena|kawama|tuxpan|sun beach|villa tortuga/i, city: "Varadero" },
  { re: /guardalavaca|pesquero|costa verde|vista azul/i, city: "Guardalavaca" },
  { re: /\bholgu[ií]n\b|caballeriza/i, city: "Holguín" },
  { re: /gibara/i, city: "Gibara" },
  { re: /santiago|casa granda|carisol|sierra mar|gaviota santiago/i, city: "Santiago de Cuba" },
  { re: /trinidad|la ronda|las cuevas|la calesa/i, city: "Trinidad" },
  { re: /cienfuegos|jagua|faro luna|rancho luna|la uni[oó]n/i, city: "Cienfuegos" },
  { re: /vi[nñ]ales|jazmines|central vi[nñ]ales/i, city: "Viñales" },
  { re: /camag[uü]ey|santa luc[ií]a|caracol|avellaneda|el marqu[eé]s/i, city: "Camagüey" },
  { re: /baracoa|maguana|porto santo|la rusa|el castillo|la habanera|1511|rio miel/i, city: "Baracoa" },
  { re: /playa gir[oó]n|zapata|playa larga/i, city: "Península de Zapata" },
  { re: /mar[ií]a la gorda/i, city: "María la Gorda" },
  { re: /cabo de san antonio/i, city: "Cabo de San Antonio" },
  { re: /escambray|caburn[ií]|los helechos|topes/i, city: "Topes de Collantes" },
  { re: /remedios|barcelona|velasco/i, city: "Remedios" },
  { re: /santa clara|los caneyes/i, city: "Santa Clara" },
  { re: /nacional de cuba|inglesa|inglaterra|plaza|deauville|vedado|neptuno|comodoro|mariposa|bello caribe|marazul|acuario|chateau miramar|ambos mundos|florida|raquel|santa isabel|tejadillo|kohly|terral|habana 612|palacio|armadores|comendador|frailes|mes[oó]n|beltr[aá]n|conde de villanueva|el bosque/i, city: "Havana" },
];

const SKIP_TITLE_RE =
  /^(hotels|hotel)$|our hotels|cuba hotels|by type|newsletter|partners|classifications|map -|beach cuba|city cuba|key cuba|isla de la juventud hotels|cayo largo hotels|cienfuegos hotels|havana hotels|varadero hotels|gran caribe|servicio premium|hoteles e$|hoteles /i;

const CASAS_PARTICULARES_RE =
  /\b(casa particular|private home|vacation rental|airbnb|apartment rental)\b/i;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function cleanTitle(raw) {
  return String(raw || "")
    .replace(/&#39;/g, "'")
    .replace(/&amp;/g, "&")
    .replace(/&nbsp;/g, " ")
    .replace(/\*+/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function inferCityFromName(name) {
  for (const h of NAME_LOCALITY_HINTS) {
    if (h.re.test(name)) return h.city;
  }
  return null;
}

/**
 * Parse hotel-like titles from official HTML.
 * @param {string} html
 * @param {{ source_id: string, source_url: string, group: string }} meta
 */
export function parseOfficialCubaHotelHtml(html, meta) {
  const out = [];
  const seen = new Set();
  for (const m of String(html || "").matchAll(
    /<(?:h[1-6]|a)[^>]*>([^<]{3,140})<\/(?:h[1-6]|a)>/gi
  )) {
    const name = cleanTitle(m[1]);
    if (!name || SKIP_TITLE_RE.test(name)) continue;
    if (!/hotel|villa|hostal|resort|brisas|club |playa |kurhotel|palace|gran |nacional/i.test(name)) {
      continue;
    }
    if (CASAS_PARTICULARES_RE.test(name)) continue;
    const key = normName(name);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    const city = inferCityFromName(name);
    const province = city ? resolveCubaProvinceFromCity(city) : null;
    out.push({
      property_name: name,
      country: "Cuba",
      city: city || null,
      state_region: province || null,
      source_type: "cuba_official_public_directory",
      discovery_source: `Official / Public — ${meta.group}`,
      source_url: meta.source_url,
      group: meta.group,
      source_id: meta.source_id,
      candidate_id: `cuba_${meta.source_id}_${crypto
        .createHash("sha1")
        .update(key)
        .digest("hex")
        .slice(0, 12)}`,
      normalized_property_name: key,
      website: meta.source_url,
    });
  }
  return out;
}

/**
 * Extra curated Cubanacán rows (directory cards with destination labels).
 * Used when HTML heading parse misses brand/destination pairs.
 */
export const CUBANACAN_CURATED_ROWS = Object.freeze([
  { property_name: "E Barcelona", city: "Remedios" },
  { property_name: "Hotel Los Caneyes", city: "Santa Clara" },
  { property_name: "Club Amigo Caracol", city: "Camagüey" },
  { property_name: "Villa Playa Girón", city: "Península de Zapata" },
  { property_name: "Cubanacan Las Cuevas", city: "Trinidad" },
  { property_name: "E Central Viñales", city: "Viñales" },
  { property_name: "E Velasco", city: "Remedios" },
  { property_name: "Hotel E Santa María", city: "Camagüey" },
  { property_name: "Hotel E La Avellaneda", city: "Camagüey" },
  { property_name: "Brisas Santa Lucía", city: "Santa Lucía" },
  { property_name: "E La Calesa", city: "Trinidad" },
  { property_name: "Chateau Miramar", city: "Havana" },
  { property_name: "Cubanacan Imperial", city: "Santiago de Cuba" },
  { property_name: "Hotel E El Marqués", city: "Camagüey" },
  { property_name: "Hotel Comodoro", city: "Havana" },
  { property_name: "Brisas Guardalavaca", city: "Guardalavaca" },
  { property_name: "Club Acuario", city: "Havana" },
  { property_name: "E La Rueda", city: "Ciego de Ávila" },
  { property_name: "Hotel Mariposa", city: "Havana" },
  { property_name: "Hotel Bello Caribe", city: "Havana" },
  { property_name: "Club Marazul", city: "Havana" },
  { property_name: "Hotel Casa Granda", city: "Santiago de Cuba" },
  { property_name: "Hotel Los Jazmines", city: "Viñales" },
  { property_name: "E Ordoño", city: "Gibara" },
  { property_name: "Hotel Mojito", city: "Cayo Coco" },
  { property_name: "E La Ronda", city: "Trinidad" },
  { property_name: "Cubanacan Tuxpan", city: "Varadero" },
  { property_name: "Horizontes Playa Larga", city: "Península de Zapata" },
  { property_name: "Brisas del Caribe", city: "Varadero" },
  { property_name: "Club Amigo Carisol", city: "Santiago de Cuba" },
  { property_name: "Hotel E Caballeriza", city: "Holguín" },
  { property_name: "Brisas Sierra Mar", city: "Santiago de Cuba" },
]);

function curatedToCandidates() {
  return CUBANACAN_CURATED_ROWS.map((row) => {
    const name = row.property_name;
    const key = normName(name);
    const province = resolveCubaProvinceFromCity(row.city);
    return {
      property_name: name,
      country: "Cuba",
      city: row.city,
      state_region: province,
      source_type: "cuba_official_public_directory",
      discovery_source: "Official / Public — Cubanacán",
      source_url: "https://www.cubanacanhoteles.com/en/hotels",
      group: "Cubanacán",
      source_id: "cubanacan_curated",
      candidate_id: `cuba_curated_${crypto.createHash("sha1").update(key).digest("hex").slice(0, 12)}`,
      normalized_property_name: key,
      website: "https://www.cubanacanhoteles.com/en/hotels",
    };
  });
}

async function fetchHtml(url) {
  const res = await fetch(url, {
    headers: { "User-Agent": "DealalityCensusBot/1.0 (+cuba-independent-discovery)" },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`fetch_${res.status}:${url}`);
  return res.text();
}

/**
 * Discover Cuba hotel core-identity candidates from official directories.
 * @param {{ useCache?: boolean, log?: Function }} [opts]
 */
export async function discoverCubaIndependentHotels(opts = {}) {
  const log = opts.log || (() => {});
  fs.mkdirSync(CACHE_DIR, { recursive: true });
  const all = [];
  const sourceStatus = [];

  for (const src of SOURCE_URLS) {
    const cacheFp = path.join(CACHE_DIR, `${src.id}.html`);
    let html = null;
    try {
      if (opts.useCache !== false && fs.existsSync(cacheFp)) {
        html = fs.readFileSync(cacheFp, "utf8");
        log(`[cuba] cache hit ${src.id}`);
      } else {
        html = await fetchHtml(src.url);
        fs.writeFileSync(cacheFp, html, "utf8");
        log(`[cuba] fetched ${src.id} (${html.length} bytes)`);
        await sleep(400);
      }
      const parsed = parseOfficialCubaHotelHtml(html, {
        source_id: src.id,
        source_url: src.url,
        group: src.group,
      });
      all.push(...parsed);
      sourceStatus.push({
        source_id: src.id,
        url: src.url,
        status: "OK",
        candidates: parsed.length,
      });
    } catch (err) {
      log(`[cuba] source fail ${src.id}: ${String(err?.message || err).slice(0, 160)}`);
      sourceStatus.push({
        source_id: src.id,
        url: src.url,
        status: "FAIL",
        error: String(err?.message || err).slice(0, 200),
        candidates: 0,
      });
    }
  }

  all.push(...curatedToCandidates());

  // Dedupe by normalized name
  const byName = new Map();
  for (const c of all) {
    const k = c.normalized_property_name;
    if (!byName.has(k)) byName.set(k, c);
    else {
      const prev = byName.get(k);
      if (!prev.city && c.city) byName.set(k, c);
    }
  }

  const candidates = [...byName.values()].map((c) => {
    if (!c.city) {
      const inferred = inferCityFromName(c.property_name);
      if (inferred) {
        c.city = inferred;
        c.state_region = resolveCubaProvinceFromCity(inferred);
      }
    } else if (!c.state_region) {
      c.state_region = resolveCubaProvinceFromCity(c.city);
    }
    return c;
  });

  const withCity = candidates.filter((c) => c.city).length;
  const holds = candidates.filter((c) => !c.city);

  return {
    version: CUBA_DISCOVERY_VERSION,
    source_status: sourceStatus,
    destination_keys_in_library: Object.keys(CUBA_CITY_TO_PROVINCE).length,
    discovered: candidates.length,
    with_city: withCity,
    city_hold_review: holds.length,
    candidates,
    hold_review: holds,
  };
}
