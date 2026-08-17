#!/usr/bin/env node
/**
 * GIATA MultiCodes TEST + MHG TEST capability validation (schema only).
 *
 * SAFETY:
 * - Forces Airtable/census writes OFF
 * - Never logs credentials / Authorization headers
 * - TEST samples are NOT valid for geographic coverage
 *
 * Usage:
 *   node scripts/hotel-intelligence-giata-test-products-validation.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/hotel-intelligence/giata-test-products-validation-v1"
);

process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES = "0";
process.env.ENABLE_HBX_CENSUS_WRITES = "0";

const SAMPLE_TARGET = 15;
const WARNING = "TEST_SAMPLE_NOT_VALID_FOR_GEOGRAPHIC_COVERAGE";

function ensureDir(d) {
  fs.mkdirSync(d, { recursive: true });
}
function writeJson(file, data) {
  ensureDir(path.dirname(file));
  fs.writeFileSync(file, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function sanitizeError(msg) {
  return String(msg || "")
    .replace(/Basic\s+[A-Za-z0-9+/=]+/gi, "Basic [REDACTED]")
    .replace(/Authorization:\s*\S+/gi, "Authorization: [REDACTED]")
    .replace(/password[=:]\s*\S+/gi, "password=[REDACTED]")
    .slice(0, 240);
}

function basicAuthHeader(user, pass) {
  const token = Buffer.from(`${user}:${pass}`, "utf8").toString("base64");
  return `Basic ${token}`;
}

function presence(v) {
  return Boolean(String(v || "").trim());
}

/**
 * GIATA MultiCodes/MHG Basic auth username is:
 *   {name}|{company}
 * e.g. giata|aohospitalityadvisors.com
 * Env may store the pipe form, an email (converted), or name+company separately.
 */
function resolveGiataBasicUsername(rawUser, opts = {}) {
  const u = String(rawUser || "").trim();
  const company = String(
    opts.company ||
      process.env.GIATA_AUTH_COMPANY ||
      process.env.GIATA_COMPANY ||
      ""
  ).trim();
  const nameOverride = String(
    opts.name || process.env.GIATA_AUTH_NAME || ""
  ).trim();
  if (u.includes("|")) return u;
  if (u.includes("@")) {
    const [local, domain] = u.split("@");
    if (local && domain) return `${local}|${domain}`;
  }
  if (nameOverride && company) return `${nameOverride}|${company}`;
  if (nameOverride && u) return `${nameOverride}|${u}`;
  if (u && company) return `${u}|${company}`;
  // Activation-mail fallback used only when explicitly opted in for validation
  if (opts.allowActivationFallback) {
    const fb = String(opts.activationFallback || "").trim();
    if (fb.includes("|")) return fb;
  }
  return u;
}

function detectFormat(contentType, body) {
  const ct = String(contentType || "").toLowerCase();
  const head = String(body || "").trim().slice(0, 80);
  if (ct.includes("json") || head.startsWith("{") || head.startsWith("[")) {
    return "json";
  }
  if (ct.includes("xml") || head.startsWith("<?xml") || head.startsWith("<")) {
    return "xml";
  }
  return ct || "unknown";
}

async function request(baseUrl, pathname, { user, pass, method = "GET" } = {}) {
  const base = String(baseUrl || "").replace(/\/$/, "");
  const url = `${base}${pathname.startsWith("/") ? pathname : `/${pathname}`}`;
  const t0 = Date.now();
  const headers = {
    Accept: "application/xml, text/xml, application/json, */*",
  };
  if (user && pass) {
    headers.Authorization = basicAuthHeader(user, pass);
  }
  try {
    const res = await fetch(url, { method, headers });
    const text = await res.text();
    const rate = {};
    for (const [k, v] of res.headers.entries()) {
      if (/rate|quota|limit|retry/i.test(k)) rate[k] = v;
    }
    return {
      ok: res.ok,
      status: res.status,
      ms: Date.now() - t0,
      contentType: res.headers.get("content-type"),
      format: detectFormat(res.headers.get("content-type"), text),
      text,
      rate_limit_headers: rate,
      error: res.ok ? null : sanitizeError(`http_${res.status}:${text.slice(0, 120)}`),
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      ms: Date.now() - t0,
      contentType: null,
      format: "none",
      text: "",
      rate_limit_headers: {},
      error: sanitizeError(err?.message || err),
    };
  }
}

/** Minimal XML helpers (no dependency). */
function attr(tag, name) {
  const m = String(tag || "").match(new RegExp(`${name}="([^"]*)"`, "i"));
  return m ? m[1] : null;
}

function allMatches(xml, re) {
  const out = [];
  const s = String(xml || "");
  let m;
  const r = new RegExp(re.source, re.flags.includes("g") ? re.flags : `${re.flags}g`);
  while ((m = r.exec(s))) out.push(m);
  return out;
}

function textBetween(xml, tag) {
  const m = String(xml || "").match(
    new RegExp(`<${tag}(?:\\s[^>]*)?>([\\s\\S]*?)</${tag}>`, "i")
  );
  if (!m) return null;
  return m[1].replace(/<[^>]+>/g, "").trim() || null;
}

function extractGiataIdsFromList(xml, limit = SAMPLE_TARGET) {
  const ids = [];
  for (const m of allMatches(xml, /giataId="(\d+)"/g)) {
    const id = m[1];
    if (!ids.includes(id)) ids.push(id);
    if (ids.length >= limit) break;
  }
  return ids;
}

function walkRoomCandidates(xml) {
  const findings = [];
  const patterns = [
    { name: "roomCount", re: /<(?:[\w:]+)?roomCount[^>]*>([\s\S]*?)<\/(?:[\w:]+)?roomCount>/gi },
    { name: "numberOfRooms", re: /<(?:[\w:]+)?numberOfRooms[^>]*>([\s\S]*?)<\/(?:[\w:]+)?numberOfRooms>/gi },
    { name: "roomsNumber", re: /<(?:[\w:]+)?roomsNumber[^>]*>([\s\S]*?)<\/(?:[\w:]+)?roomsNumber>/gi },
    { name: "totalRooms", re: /<(?:[\w:]+)?totalRooms[^>]*>([\s\S]*?)<\/(?:[\w:]+)?totalRooms>/gi },
    { name: "numberOfGuestRooms", re: /numberOfGuestRooms[^>]*>[\s\S]*?<\/[^>]+>/gi },
    { name: "hotelRooms", re: /<(?:[\w:]+)?hotelRooms[^>]*>([\s\S]*?)<\/(?:[\w:]+)?hotelRooms>/gi },
    { name: "keys", re: /<(?:[\w:]+)?keys[^>]*>([\s\S]*?)<\/(?:[\w:]+)?keys>/gi },
    {
      name: "fact_name_rooms",
      re: /<fact[^>]*name="([^"]*room[^"]*)"[^>]*>([\s\S]*?)<\/fact>/gi,
    },
    {
      name: "rooms_element",
      re: /<(?:[\w:]+)?rooms(?:\s[^>]*)?>([\s\S]*?)<\/(?:[\w:]+)?rooms>/gi,
    },
  ];

  for (const p of patterns) {
    for (const m of allMatches(xml, p.re)) {
      const raw = (m[2] || m[1] || m[0] || "").replace(/\s+/g, " ").trim().slice(0, 300);
      const numeric = raw.match(/\b(\d{1,5})\b/);
      findings.push({
        field_name: p.name === "fact_name_rooms" ? `fact:${m[1]}` : p.name,
        path_hint: p.name,
        raw_snippet: raw.slice(0, 200),
        raw_type: numeric ? "contains_number" : typeof raw,
        numeric_candidate: numeric ? Number(numeric[1]) : null,
        surrounding_context: String(xml)
          .slice(Math.max(0, m.index - 80), m.index + Math.min(m[0].length + 80, 220))
          .replace(/\s+/g, " ")
          .slice(0, 280),
      });
    }
  }

  // Text mentions like "184 rooms" in CDATA / texts
  for (const m of allMatches(
    xml,
    /\b(\d{2,5})\s+(?:guest\s+)?rooms?\b|\bof\s+(\d{2,5})\s+rooms\b/gi
  )) {
    findings.push({
      field_name: "text_rooms_mention",
      path_hint: "free_text",
      raw_snippet: m[0].slice(0, 120),
      raw_type: "text",
      numeric_candidate: Number(m[1] || m[2]),
      surrounding_context: String(xml)
        .slice(Math.max(0, m.index - 60), m.index + 100)
        .replace(/\s+/g, " ")
        .slice(0, 220),
      likely_semantic_meaning: "narrative_total_or_ambiguous",
    });
  }

  return findings;
}

function classifyRoomFinding(f) {
  const name = String(f.field_name || "").toLowerCase();
  const ctx = String(f.surrounding_context || "").toLowerCase();
  if (/roomtype|room_type|category|occupancy|max.?person|available/.test(name + ctx)) {
    if (/available/.test(ctx)) return "AVAILABILITY";
    if (/max.?person|occupancy/.test(ctx)) return "OCCUPANCY";
    return "ROOM_TYPE_CATALOG";
  }
  if (
    /numberofrooms|roomcount|totalrooms|numberofguestrooms|hotelrooms|fact:.*rooms/.test(
      name
    ) ||
    (/^\d+$/.test(String(f.numeric_candidate || "")) &&
      /object_information|hotel_information|general/.test(ctx))
  ) {
    return "TOTAL_PROPERTY_ROOM_COUNT_CANDIDATE";
  }
  if (name === "text_rooms_mention") return "NARRATIVE_POSSIBLE_TOTAL";
  if (name === "rooms_element") return "ROOM_TYPE_OR_SECTION_AMBIGUOUS";
  return "UNRESOLVED";
}

function parseMultiCodesProperty(xml) {
  const block =
    String(xml).match(/<property\b[\s\S]*?<\/property>/i)?.[0] || String(xml);
  const providers = [];
  for (const pm of allMatches(
    block,
    /<provider\b([^>]*)>([\s\S]*?)<\/provider>/gi
  )) {
    const providerCode = attr(pm[1], "providerCode");
    const providerType = attr(pm[1], "providerType");
    const codes = [];
    for (const cm of allMatches(pm[2], /<code\b[^>]*>([\s\S]*?)<\/code>/gi)) {
      const values = [];
      for (const vm of allMatches(
        cm[1],
        /<value(?:\s+name="([^"]*)")?[^>]*>([^<]*)<\/value>/gi
      )) {
        values.push({ name: vm[1] || null, value: (vm[2] || "").trim() });
      }
      if (!values.length) {
        const plain = cm[1].replace(/<[^>]+>/g, "").trim();
        if (plain) values.push({ name: null, value: plain });
      }
      codes.push(values);
    }
    providers.push({ providerCode, providerType, codes });
  }

  const chains = [];
  for (const cm of allMatches(block, /<chain\b([^/]*)\/>|<chain\b([^>]*)>([\s\S]*?)<\/chain>/gi)) {
    const attrs = cm[1] || cm[2] || "";
    chains.push({
      chainId: attr(attrs, "chainId"),
      chainName: attr(attrs, "chainName"),
      chainCode: attr(attrs, "chainCode"),
    });
  }

  const altNames = [];
  for (const nm of allMatches(block, /<name(?:\s[^>]*)?>([\s\S]*?)<\/name>/gi)) {
    const v = nm[1].replace(/<[^>]+>/g, "").trim();
    if (v) altNames.push(v);
  }

  return {
    giata_id: attr(block.match(/<property\b[^>]*>/i)?.[0] || "", "giataId"),
    status: attr(block.match(/<property\b[^>]*>/i)?.[0] || "", "status"),
    name: textBetween(block, "name"),
    city: textBetween(block, "city"),
    country: textBetween(block, "country"),
    destination: textBetween(block, "destination"),
    category: textBetween(block, "category"),
    address_street: textBetween(block, "street"),
    address_street_number: textBetween(block, "streetNumber"),
    postal_code: textBetween(block, "postalCode"),
    city_name: textBetween(block, "cityName"),
    latitude: textBetween(block, "latitude"),
    longitude: textBetween(block, "longitude"),
    phone: textBetween(block, "phone"),
    website: textBetween(block, "url"),
    chains,
    providers,
    name_nodes_count: altNames.length,
    names_sample: altNames.slice(0, 5),
    has_moved_to: /movedTo=/.test(block),
    has_parent: /<parent\b/i.test(block),
    has_ghgml_link: /<ghgml\b/i.test(block),
    room_findings: walkRoomCandidates(block).map((f) => ({
      ...f,
      classification: classifyRoomFinding(f),
    })),
  };
}

function parseMhgItem(xml) {
  const block =
    String(xml).match(/<item\b[\s\S]*?<\/item>/i)?.[0] || String(xml);
  const roomFindings = walkRoomCandidates(xml).map((f) => ({
    ...f,
    classification: classifyRoomFinding(f),
  }));

  const factNames = [];
  for (const m of allMatches(xml, /<fact\b[^>]*name="([^"]+)"/gi)) {
    factNames.push(m[1]);
  }

  return {
    giata_id: attr(block.match(/<item\b[^>]*>/i)?.[0] || "", "giataId"),
    name: textBetween(block, "name"),
    street: textBetween(block, "street"),
    city: textBetween(block, "city"),
    country: textBetween(block, "country"),
    has_texts: /<texts?\b/i.test(xml) || /<text\b/i.test(xml),
    has_images: /<images?\b/i.test(xml) || /<image\b/i.test(xml),
    has_factsheet: /<factsheet\b/i.test(xml),
    fact_names: [...new Set(factNames)].slice(0, 80),
    room_findings: roomFindings,
    latitude: textBetween(xml, "latitude"),
    longitude: textBetween(xml, "longitude"),
  };
}

function mapSupplierLabel(providerCode) {
  const c = String(providerCode || "").toLowerCase();
  const known = {
    hotelbeds: "Hotelbeds",
    hbx: "Hotelbeds",
    booking: "Booking.com",
    bookingcom: "Booking.com",
    bcom: "Booking.com",
    expedia: "Expedia",
    epc: "Expedia",
    agoda: "Agoda",
    amadeus: "Amadeus",
    sabre: "Sabre",
    travelport: "Travelport",
    restel: "Restel",
    goglobal: "GoGlobal",
    gta: "GTA / Travelport-family",
    aic: "AIC",
  };
  return known[c] || providerCode || "unknown";
}

async function probeAuth(label, baseUrl, user, pass, pathnames) {
  const attempts = [];
  for (const pathname of pathnames) {
    const res = await request(baseUrl, pathname, { user, pass });
    attempts.push({
      pathname,
      status: res.status,
      ok: res.ok,
      format: res.format,
      ms: res.ms,
      error: res.error,
      body_bytes: res.text?.length || 0,
      rate_limit_headers: res.rate_limit_headers,
    });
    if (res.ok) {
      return {
        label,
        credentials_present: presence(user) && presence(pass),
        reachable: res.status > 0,
        HTTP_status: res.status,
        authenticated: true,
        response_format: res.format,
        sanitized_error: null,
        probe_path: pathname,
        attempts,
        first_ok: res,
      };
    }
    if (res.status === 401 || res.status === 403) {
      // continue trying other paths; auth may still be wrong
    }
  }
  const last = attempts[attempts.length - 1] || {};
  return {
    label,
    credentials_present: presence(user) && presence(pass),
    reachable: (last.status || 0) > 0,
    HTTP_status: last.status ?? 0,
    authenticated: false,
    response_format: last.format || "unknown",
    sanitized_error: last.error || "auth_or_path_failed",
    probe_path: null,
    attempts,
    first_ok: null,
  };
}

async function main() {
  ensureDir(OUT_DIR);

  const audit = {
    marker: "GIATA_TEST_PRODUCTS_EXISTING_CAPABILITY_AUDIT",
    items: [
      {
        name: "GIATA Drive client",
        path: "lib/research-engine-v2/providers/giata-drive/",
        status: "ALREADY_EXISTS",
      },
      {
        name: "GIATA Drive HI provider adapter",
        path: "lib/hotel-intelligence/providers/giata-drive.js",
        status: "ALREADY_EXISTS",
      },
      {
        name: "GIATA Drive sync / deletedUrls",
        path: "lib/hotel-intelligence/providers/giata-drive-sync.js",
        status: "ALREADY_EXISTS",
      },
      {
        name: "HI registry entry giata_drive",
        path: "lib/hotel-intelligence/providers/registry.js",
        status: "ALREADY_EXISTS",
      },
      {
        name: "external-hotel-source-registry giata stub",
        path: "lib/research-engine-v2/external-hotel-source-registry.js",
        status: "REUSABLE",
        note: "Commercial stub only — not a live MultiCodes/MHG client",
      },
      {
        name: "MultiCodes REST client",
        status: "MISSING",
      },
      {
        name: "MHG REST client",
        status: "MISSING",
      },
      {
        name: "XML parser utilities for GIATA MultiCodes/MHG",
        status: "NEEDS_EXTENSION",
        note: "No dedicated GIATA XML parser; validation uses minimal inline XML helpers",
      },
      {
        name: "MultiCodes/MHG tests",
        status: "MISSING",
      },
      {
        name: "Drive docs / prior validation",
        path: "docs/data-intelligence/hotel-intelligence-mcp-v1.md + reports/.../giata-*",
        status: "REUSABLE",
      },
    ],
  };

  const mcBase =
    process.env.GIATA_MULTICODES_BASE_URL ||
    "https://multicodes.giatamedia.com/webservice/rest/1.latest";
  const mhgBase =
    process.env.GIATA_MHG_BASE_URL ||
    "https://ghgml.giatamedia.com/webservice/rest/1.0";
  const mcPass = String(process.env.GIATA_MULTICODES_PASSWORD || "").trim();
  const mhgPass = String(process.env.GIATA_MHG_PASSWORD || "").trim();
  const activationFallback = "giata|aohospitalityadvisors.com";
  const mcUserRaw = String(process.env.GIATA_MULTICODES_USERNAME || "").trim();
  const mhgUserRaw = String(process.env.GIATA_MHG_USERNAME || "").trim();
  let mcUser = resolveGiataBasicUsername(mcUserRaw, {
    allowActivationFallback: true,
    activationFallback,
  });
  let mhgUser = resolveGiataBasicUsername(mhgUserRaw, {
    allowActivationFallback: true,
    activationFallback,
  });
  // If raw username lacks pipe and is not an email, prefer activation-mail form for TEST
  if (!mcUser.includes("|")) mcUser = activationFallback;
  if (!mhgUser.includes("|")) mhgUser = activationFallback;

  const metrics = {
    multicodes: { calls: 0, successes: 0, errors: 0, latencies_ms: [] },
    mhg: { calls: 0, successes: 0, errors: 0, latencies_ms: [] },
  };

  async function tracked(product, fn) {
    metrics[product].calls += 1;
    const res = await fn();
    metrics[product].latencies_ms.push(res.ms);
    if (res.ok) metrics[product].successes += 1;
    else metrics[product].errors += 1;
    return res;
  }

  console.log(
    JSON.stringify({
      module: "giata-test-products-validation",
      event: "start",
      warning: WARNING,
      writes: 0,
    })
  );

  // --- MultiCodes connectivity ---
  const mcProbe = await probeAuth(
    "multicodes",
    mcBase,
    mcUser,
    mcPass,
    ["/properties/", "/properties", "/providers/", "/chains/"]
  );
  // count probe calls into metrics
  metrics.multicodes.calls += mcProbe.attempts.length;
  metrics.multicodes.successes += mcProbe.attempts.filter((a) => a.ok).length;
  metrics.multicodes.errors += mcProbe.attempts.filter((a) => !a.ok).length;
  metrics.multicodes.latencies_ms.push(...mcProbe.attempts.map((a) => a.ms));

  // --- MHG connectivity ---
  const mhgProbe = await probeAuth("mhg", mhgBase, mhgUser, mhgPass, [
    "/items/",
    "/items",
    "/factsheets/",
    "/factsheetdefinitions/en",
  ]);
  metrics.mhg.calls += mhgProbe.attempts.length;
  metrics.mhg.successes += mhgProbe.attempts.filter((a) => a.ok).length;
  metrics.mhg.errors += mhgProbe.attempts.filter((a) => !a.ok).length;
  metrics.mhg.latencies_ms.push(...mhgProbe.attempts.map((a) => a.ms));

  const multicodes = {
    connectivity: {
      credentials_present: mcProbe.credentials_present,
      reachable: mcProbe.reachable,
      HTTP_status: mcProbe.HTTP_status,
      authenticated: mcProbe.authenticated,
      response_format: mcProbe.response_format,
      sanitized_error: mcProbe.sanitized_error,
      auth_method_used: "HTTP Basic",
      username_shape: {
        length: mcUser.length,
        has_pipe: mcUser.includes("|"),
        resolved_from_env_pipe: mcUserRaw.includes("|"),
        note: "GIATA Basic auth requires user|company (activation mail format)",
      },
    },
    warning: WARNING,
    sample: [],
    supplier_matrix: [],
    capability: {},
    external_id_graph_value: "LOW",
    identity_value: {},
    verdict: "MULTICODES_ACCESS_BLOCKED",
  };

  if (mcProbe.authenticated && mcProbe.first_ok) {
    const listXml = mcProbe.first_ok.text;
    let ids = extractGiataIdsFromList(listXml, SAMPLE_TARGET);
    // If list is huge index-only, still take first N
    if (!ids.length) {
      // try providers list only — no properties
      multicodes.connectivity.note = "Authenticated but no giataId in first probe body";
    }

    for (const id of ids) {
      const detail = await tracked("multicodes", () =>
        request(mcBase, `/properties/${id}`, { user: mcUser, pass: mcPass })
      );
      if (!detail.ok) continue;
      const parsed = parseMultiCodesProperty(detail.text);
      multicodes.sample.push(parsed);
    }

    // Supplier matrix
    const byProvider = new Map();
    for (const row of multicodes.sample) {
      for (const p of row.providers || []) {
        const key = `${p.providerType || "?"}::${p.providerCode || "?"}`;
        const cur = byProvider.get(key) || {
          supplier_provider: mapSupplierLabel(p.providerCode),
          providerCode: p.providerCode,
          providerType: p.providerType,
          mapping_field_path: "property/propertyCodes/provider[@providerCode]/code/value",
          sample_count: 0,
          persistent_id_examples: [],
          persistent_ID: true,
          usable_for_cross_provider_identity: true,
          entitled_in_TEST: true,
        };
        cur.sample_count += 1;
        const flat = (p.codes || [])
          .flat()
          .map((v) => v.value)
          .filter(Boolean);
        for (const v of flat.slice(0, 2)) {
          if (cur.persistent_id_examples.length < 3) {
            cur.persistent_id_examples.push(v);
          }
        }
        byProvider.set(key, cur);
      }
    }
    multicodes.supplier_matrix = [...byProvider.values()];

    const n = multicodes.sample.length || 1;
    const rate = (fn) =>
      Number(
        (
          multicodes.sample.filter(fn).length / Math.max(multicodes.sample.length, 1)
        ).toFixed(3)
      );

    multicodes.capability = {
      giata_id: rate((r) => r.giata_id) ? "CONFIRMED_SUPPORTED" : "NOT_OBSERVED",
      name: rate((r) => r.name) ? "CONFIRMED_SUPPORTED" : "NOT_OBSERVED",
      alternate_names:
        rate((r) => (r.name_nodes_count || 0) > 1) > 0
          ? "CONFIRMED_SUPPORTED"
          : "NOT_OBSERVED",
      former_names: "NOT_OBSERVED",
      city: rate((r) => r.city) ? "CONFIRMED_SUPPORTED" : "NOT_OBSERVED",
      country: rate((r) => r.country) ? "CONFIRMED_SUPPORTED" : "NOT_OBSERVED",
      address:
        rate((r) => r.address_street || r.city_name)
          ? "CONFIRMED_SUPPORTED"
          : "NOT_OBSERVED",
      coordinates:
        rate((r) => r.latitude && r.longitude)
          ? "CONFIRMED_SUPPORTED"
          : "NOT_OBSERVED",
      brand_chain: rate((r) => r.chains?.length) ? "CONFIRMED_SUPPORTED" : "NOT_OBSERVED",
      category: rate((r) => r.category) ? "CONFIRMED_SUPPORTED" : "NOT_OBSERVED",
      inactive_status: rate((r) => r.status) ? "CONFIRMED_SUPPORTED" : "NOT_OBSERVED",
      supplier_mappings:
        multicodes.supplier_matrix.length > 0
          ? "CONFIRMED_SUPPORTED"
          : "NOT_OBSERVED",
      room_count: "NOT_OBSERVED",
    };

    multicodes.identity_value = {
      official_current_name: multicodes.capability.name,
      alternate_names: multicodes.capability.alternate_names,
      former_names: multicodes.capability.former_names,
      city: multicodes.capability.city,
      country: multicodes.capability.country,
      address: multicodes.capability.address,
      coordinates: multicodes.capability.coordinates,
      brand_chain: multicodes.capability.brand_chain,
      inactive_status: multicodes.capability.inactive_status,
      moved_properties_endpoint: "documented (not sampled heavily)",
    };

    const hasUsefulCrosswalk = multicodes.supplier_matrix.some(
      (s) => s.sample_count > 0 && s.persistent_ID
    );
    const hasIdentity =
      multicodes.capability.name === "CONFIRMED_SUPPORTED" &&
      multicodes.capability.giata_id === "CONFIRMED_SUPPORTED";

    if (hasUsefulCrosswalk && hasIdentity) {
      multicodes.external_id_graph_value = "HIGH";
      multicodes.verdict = "MULTICODES_HIGH_VALUE_IDENTITY_CROSSWALK";
    } else if (hasIdentity) {
      multicodes.external_id_graph_value = "MEDIUM";
      multicodes.verdict = "MULTICODES_IDENTITY_ONLY";
    } else if (multicodes.sample.length) {
      multicodes.external_id_graph_value = "LOW";
      multicodes.verdict = "MULTICODES_LOW_VALUE_TEST_ENTITLEMENT";
    }
  }

  const mhg = {
    connectivity: {
      credentials_present: mhgProbe.credentials_present,
      reachable: mhgProbe.reachable,
      HTTP_status: mhgProbe.HTTP_status,
      authenticated: mhgProbe.authenticated,
      response_format: mhgProbe.response_format,
      sanitized_error: mhgProbe.sanitized_error,
      auth_method_used: "HTTP Basic",
      username_shape: {
        length: mhgUser.length,
        has_pipe: mhgUser.includes("|"),
      },
    },
    warning: WARNING,
    sample: [],
    room_semantic_analysis: [],
    capability: {},
    room_count_verdict: "MHG_TOTAL_PROPERTY_ROOM_COUNT_NOT_FOUND_IN_TEST_ENTITLEMENT",
    room_count_detail: {
      field_path: null,
      sample_presence_rate: 0,
      sample_numeric_values: [],
    },
  };

  if (mhgProbe.authenticated && mhgProbe.first_ok) {
    const ids = extractGiataIdsFromList(mhgProbe.first_ok.text, SAMPLE_TARGET);
    // Also try factsheet definitions for room-related fact names
    const defs = await tracked("mhg", () =>
      request(mhgBase, "/factsheetdefinitions/en", {
        user: mhgUser,
        pass: mhgPass,
      })
    );
    const roomFactDefs = [];
    if (defs.ok) {
      for (const m of allMatches(
        defs.text,
        /name="([^"]*room[^"]*)"|name="([^"]*keys?[^"]*)"/gi
      )) {
        roomFactDefs.push(m[1] || m[2]);
      }
    }

    for (const id of ids) {
      const item = await tracked("mhg", () =>
        request(mhgBase, `/items/${id}`, { user: mhgUser, pass: mhgPass })
      );
      const texts = await tracked("mhg", () =>
        request(mhgBase, `/texts/en/${id}`, { user: mhgUser, pass: mhgPass })
      );
      const facts = await tracked("mhg", () =>
        request(mhgBase, `/factsheets/${id}`, { user: mhgUser, pass: mhgPass })
      );
      const images = await tracked("mhg", () =>
        request(mhgBase, `/images/${id}`, { user: mhgUser, pass: mhgPass })
      );

      const combined = [item, texts, facts, images]
        .filter((r) => r.ok)
        .map((r) => r.text)
        .join("\n");
      const parsed = parseMhgItem(combined || item.text || "");
      parsed.giata_id = parsed.giata_id || id;
      parsed.sources = {
        item: item.ok,
        texts_en: texts.ok,
        factsheet: facts.ok,
        images: images.ok,
      };
      if (facts.ok) {
        const fp = parseMhgItem(facts.text);
        parsed.fact_names = fp.fact_names;
        parsed.room_findings = [
          ...(parsed.room_findings || []),
          ...(fp.room_findings || []),
        ];
      }
      if (texts.ok) {
        const tp = parseMhgItem(texts.text);
        parsed.room_findings = [
          ...(parsed.room_findings || []),
          ...(tp.room_findings || []),
        ];
        parsed.has_texts = true;
      }
      if (images.ok) parsed.has_images = true;
      mhg.sample.push(parsed);
      mhg.room_semantic_analysis.push(
        ...(parsed.room_findings || []).map((f) => ({
          giata_id: parsed.giata_id,
          ...f,
        }))
      );
    }

    mhg.room_fact_definitions_en = [...new Set(roomFactDefs)].slice(0, 40);

    const n = Math.max(mhg.sample.length, 1);
    const rate = (fn) =>
      Number((mhg.sample.filter(fn).length / n).toFixed(3));

    const totalCandidates = mhg.room_semantic_analysis.filter(
      (f) =>
        f.classification === "TOTAL_PROPERTY_ROOM_COUNT_CANDIDATE" ||
        f.classification === "NARRATIVE_POSSIBLE_TOTAL"
    );
    const structuredTotals = mhg.room_semantic_analysis.filter(
      (f) => f.classification === "TOTAL_PROPERTY_ROOM_COUNT_CANDIDATE"
    );
    const numericValues = [
      ...new Set(
        totalCandidates
          .map((f) => f.numeric_candidate)
          .filter((v) => Number.isFinite(v) && v >= 5 && v <= 5000)
      ),
    ].slice(0, 20);

    const hotelsWithStructured = new Set(
      structuredTotals.map((f) => f.giata_id)
    ).size;
    const hotelsWithAny = new Set(totalCandidates.map((f) => f.giata_id)).size;

    mhg.room_count_detail = {
      field_path:
        structuredTotals[0]?.field_name ||
        totalCandidates[0]?.field_name ||
        null,
      sample_presence_rate: Number((hotelsWithAny / n).toFixed(3)),
      structured_presence_rate: Number((hotelsWithStructured / n).toFixed(3)),
      sample_numeric_values: numericValues,
      classifications_observed: [...new Set(mhg.room_semantic_analysis.map((f) => f.classification))],
    };

    if (hotelsWithStructured / n >= 0.5 && numericValues.length >= 3) {
      mhg.room_count_verdict = "MHG_TOTAL_PROPERTY_ROOM_COUNT_CONFIRMED";
    } else if (hotelsWithStructured > 0 || hotelsWithAny / n >= 0.3) {
      mhg.room_count_verdict = "MHG_TOTAL_PROPERTY_ROOM_COUNT_PROBABLE";
    } else if (mhg.room_semantic_analysis.length && hotelsWithAny === 0) {
      mhg.room_count_verdict =
        "MHG_TOTAL_PROPERTY_ROOM_COUNT_SEMANTICS_UNRESOLVED";
    } else {
      mhg.room_count_verdict =
        "MHG_TOTAL_PROPERTY_ROOM_COUNT_NOT_FOUND_IN_TEST_ENTITLEMENT";
    }

    mhg.capability = {
      name: rate((r) => r.name) ? "CONFIRMED_SUPPORTED" : "NOT_OBSERVED",
      address: rate((r) => r.street) ? "CONFIRMED_SUPPORTED" : "NOT_OBSERVED",
      city: rate((r) => r.city) ? "CONFIRMED_SUPPORTED" : "NOT_OBSERVED",
      country: rate((r) => r.country) ? "CONFIRMED_SUPPORTED" : "NOT_OBSERVED",
      postal:
        rate((r) => (r.fact_names || []).some((f) => /postcode|postal/i.test(f)))
          ? "CONFIRMED_SUPPORTED"
          : "NOT_OBSERVED",
      latitude: rate((r) => r.latitude) ? "CONFIRMED_SUPPORTED" : "NOT_OBSERVED",
      longitude: rate((r) => r.longitude) ? "CONFIRMED_SUPPORTED" : "NOT_OBSERVED",
      room_count_total_keys:
        mhg.room_count_verdict === "MHG_TOTAL_PROPERTY_ROOM_COUNT_CONFIRMED"
          ? "CONFIRMED_SUPPORTED"
          : mhg.room_count_verdict === "MHG_TOTAL_PROPERTY_ROOM_COUNT_PROBABLE"
            ? "UNKNOWN"
            : "NOT_OBSERVED",
      room_types: "NOT_OBSERVED",
      brand:
        rate((r) => (r.fact_names || []).some((f) => /chain|brand/i.test(f)))
          ? "CONFIRMED_SUPPORTED"
          : "NOT_OBSERVED",
      chain:
        rate((r) => (r.fact_names || []).some((f) => /chain/i.test(f)))
          ? "CONFIRMED_SUPPORTED"
          : "NOT_OBSERVED",
      star_category:
        rate((r) =>
          (r.fact_names || []).some((f) => /star|category|class/i.test(f))
        )
          ? "CONFIRMED_SUPPORTED"
          : "NOT_OBSERVED",
      website:
        rate((r) => (r.fact_names || []).some((f) => /url|website|web/i.test(f)))
          ? "CONFIRMED_SUPPORTED"
          : "NOT_OBSERVED",
      phone:
        rate((r) => (r.fact_names || []).some((f) => /phone/i.test(f)))
          ? "CONFIRMED_SUPPORTED"
          : "NOT_OBSERVED",
      descriptions: rate((r) => r.has_texts) ? "CONFIRMED_SUPPORTED" : "NOT_OBSERVED",
      facts: rate((r) => r.has_factsheet || (r.fact_names || []).length)
        ? "CONFIRMED_SUPPORTED"
        : "NOT_OBSERVED",
      amenities: rate((r) => (r.fact_names || []).length > 5)
        ? "CONFIRMED_SUPPORTED"
        : "NOT_OBSERVED",
      images: rate((r) => r.has_images) ? "CONFIRMED_SUPPORTED" : "NOT_OBSERVED",
      opening_year:
        rate((r) =>
          (r.fact_names || []).some((f) => /open|construction|built|year/i.test(f))
        )
          ? "CONFIRMED_SUPPORTED"
          : "NOT_OBSERVED",
      renovation_year:
        rate((r) =>
          (r.fact_names || []).some((f) => /renov/i.test(f))
        )
          ? "CONFIRMED_SUPPORTED"
          : "NOT_OBSERVED",
      status: "NOT_OBSERVED",
    };
  }

  // GIATA ID format consistency across products (Drive known numeric)
  const mcIds = multicodes.sample.map((s) => s.giata_id).filter(Boolean);
  const mhgIds = mhg.sample.map((s) => s.giata_id).filter(Boolean);
  const idFormatOk = (ids) =>
    ids.length === 0 || ids.every((id) => /^\d+$/.test(String(id)));
  const giataIdFormat = {
    multicodes_numeric: idFormatOk(mcIds),
    mhg_numeric: idFormatOk(mhgIds),
    drive_known_numeric: true,
    overlap_count: mcIds.filter((id) => mhgIds.includes(id)).length,
    note: "Overlap not expected on random TEST; format consistency is the check",
    GIATA_ID_FORMAT_CONSISTENT:
      idFormatOk(mcIds) && idFormatOk(mhgIds) ? "YES" : mcIds.length || mhgIds.length ? "NO" : "UNKNOWN",
  };

  const roles = {
    multicodes: [],
    mhg: [],
  };
  if (multicodes.verdict === "MULTICODES_HIGH_VALUE_IDENTITY_CROSSWALK") {
    roles.multicodes.push(
      "IDENTITY_CORE",
      "SUPPLIER_CROSSWALK",
      "DEDUPLICATION"
    );
    if (multicodes.capability.alternate_names === "CONFIRMED_SUPPORTED") {
      roles.multicodes.push("REBRAND_HISTORY");
    }
  } else if (multicodes.verdict === "MULTICODES_IDENTITY_ONLY") {
    roles.multicodes.push("IDENTITY_CORE", "DEDUPLICATION");
  } else if (multicodes.connectivity.authenticated === false && !multicodes.sample.length) {
    /* access blocked */
  } else {
    roles.multicodes.push("LOW_VALUE");
  }

  if (mhg.sample.length) {
    if (
      mhg.room_count_verdict === "MHG_TOTAL_PROPERTY_ROOM_COUNT_CONFIRMED" ||
      mhg.room_count_verdict === "MHG_TOTAL_PROPERTY_ROOM_COUNT_PROBABLE"
    ) {
      roles.mhg.push("ROOM_COUNT_SOURCE");
    }
    if (mhg.capability.descriptions === "CONFIRMED_SUPPORTED" || mhg.capability.facts === "CONFIRMED_SUPPORTED") {
      roles.mhg.push("RICH_CONTENT_SOURCE");
    }
    if (mhg.capability.city === "CONFIRMED_SUPPORTED") roles.mhg.push("GEO_SOURCE");
    if (mhg.capability.phone === "CONFIRMED_SUPPORTED" || mhg.capability.website === "CONFIRMED_SUPPORTED") {
      roles.mhg.push("CONTACT_SOURCE");
    }
    if (mhg.capability.brand === "CONFIRMED_SUPPORTED" || mhg.capability.chain === "CONFIRMED_SUPPORTED") {
      roles.mhg.push("BRAND_SOURCE");
    }
    if (!roles.mhg.length) roles.mhg.push("LOW_VALUE");
  }

  let nextStep = "REMEDIATE_GIATA_TEST_ACCESS_FIRST";
  if (multicodes.sample.length && mhg.sample.length) {
    if (
      multicodes.verdict === "MULTICODES_HIGH_VALUE_IDENTITY_CROSSWALK" &&
      (mhg.room_count_verdict === "MHG_TOTAL_PROPERTY_ROOM_COUNT_CONFIRMED" ||
        mhg.room_count_verdict === "MHG_TOTAL_PROPERTY_ROOM_COUNT_PROBABLE")
    ) {
      nextStep = "REQUEST_PRODUCTION_GIATA_ENTITLEMENT";
    } else if (multicodes.verdict === "MULTICODES_HIGH_VALUE_IDENTITY_CROSSWALK") {
      nextStep = "BUILD_MULTICODES_READ_ONLY_PROVIDER_ADAPTER";
    } else if (
      mhg.room_count_verdict === "MHG_TOTAL_PROPERTY_ROOM_COUNT_CONFIRMED" ||
      mhg.room_count_verdict === "MHG_TOTAL_PROPERTY_ROOM_COUNT_PROBABLE"
    ) {
      nextStep = "BUILD_MHG_READ_ONLY_PROVIDER_ADAPTER";
    } else {
      nextStep = "BUILD_BOTH_GIATA_TEST_ADAPTERS";
    }
  } else if (multicodes.sample.length) {
    nextStep =
      multicodes.verdict === "MULTICODES_HIGH_VALUE_IDENTITY_CROSSWALK"
        ? "BUILD_MULTICODES_READ_ONLY_PROVIDER_ADAPTER"
        : "REQUEST_PRODUCTION_GIATA_ENTITLEMENT";
  } else if (mhg.sample.length) {
    nextStep = "BUILD_MHG_READ_ONLY_PROVIDER_ADAPTER";
  }

  const summary = {
    marker: "DEALALITY_GIATA_TEST_PRODUCTS_VALIDATION_COMPLETE",
    generated_at: new Date().toISOString(),
    warning: WARNING,
    safety: {
      Airtable_writes: 0,
      Census_writes: 0,
      Brand_Explorer_writes: 0,
      Automatic_merges: 0,
      Canonical_writes: 0,
      Schema_changes: 0,
      Migrations: 0,
      Secrets_exposed: false,
      flags: {
        ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES: "0",
        ENABLE_HBX_CENSUS_WRITES: "0",
        GIATA_MULTICODES_USERNAME: presence(mcUser) ? "present" : "missing",
        GIATA_MULTICODES_PASSWORD: presence(mcPass) ? "present" : "missing",
        GIATA_MHG_USERNAME: presence(mhgUser) ? "present" : "missing",
        GIATA_MHG_PASSWORD: presence(mhgPass) ? "present" : "missing",
        GIATA_DRIVE_USERNAME: presence(process.env.GIATA_DRIVE_USERNAME)
          ? "present"
          : "missing",
        GIATA_DRIVE_PASSWORD: presence(process.env.GIATA_DRIVE_PASSWORD)
          ? "present"
          : "missing",
      },
    },
    audit,
    multicodes,
    mhg,
    giata_id_format: giataIdFormat,
    role_separation: {
      multicodes:
        "identity + deduplication + alternate/former names + supplier ID crosswalk",
      mhg: "rich hotel content + geo/address + room count if confirmed + descriptions/amenities/images",
      giata_drive:
        "SECONDARY_UNIVERSE_DISCOVERY / IDENTITY_VALIDATION / EXTERNAL_ID_GRAPH / GEO_ENRICHMENT / BRAND_ENRICHMENT (unchanged)",
    },
    provider_structure_recommendation: {
      marker: "GIATA_PROVIDER_STRUCTURE_RECOMMENDATION",
      adapters: ["giata_drive", "giata_multicodes", "giata_mhg"],
      reason:
        "Different auth (Bearer vs Basic), endpoints, entitlements, field semantics, and production roles — do not collapse into one generic giata adapter",
    },
    production_potential: {
      multicodes: roles.multicodes,
      mhg: roles.mhg,
      note: "Based on TEST schemas only — no geographic/CALA coverage extrapolation",
      room_count_business_impact:
        mhg.room_count_verdict === "MHG_TOTAL_PROPERTY_ROOM_COUNT_CONFIRMED" ||
        mhg.room_count_verdict === "MHG_TOTAL_PROPERTY_ROOM_COUNT_PROBABLE"
          ? "IF_PRODUCTION_MHG_CALA_COVERAGE_IS_SUFFICIENT: MHG could become a candidate primary/fallback room-count provider. Current Dealality gap ~5,765 missing room counts — production recovery unknown until production entitlement."
          : "No confirmed structured total keys in TEST sample — do not plan MHG as room-count provider yet.",
      supplier_crosswalk_business_impact: multicodes.supplier_matrix.length
        ? "MultiCodes supplier codes could reduce Dealality↔Hotelbeds/Booking/Expedia/Cvent matching work IF production entitlement includes those providers — TEST proves schema, not coverage."
        : "Supplier mappings not observed in TEST sample.",
    },
    api_quota: {
      multicodes: {
        ...metrics.multicodes,
        avg_latency_ms:
          metrics.multicodes.latencies_ms.length > 0
            ? Math.round(
                metrics.multicodes.latencies_ms.reduce((a, b) => a + b, 0) /
                  metrics.multicodes.latencies_ms.length
              )
            : null,
        pagination: "property list + per-id detail",
        rate_limit_headers_seen: mcProbe.attempts.some(
          (a) => Object.keys(a.rate_limit_headers || {}).length
        ),
      },
      mhg: {
        ...metrics.mhg,
        avg_latency_ms:
          metrics.mhg.latencies_ms.length > 0
            ? Math.round(
                metrics.mhg.latencies_ms.reduce((a, b) => a + b, 0) /
                  metrics.mhg.latencies_ms.length
              )
            : null,
        pagination: "items list + per-id item/texts/factsheets/images",
        rate_limit_headers_seen: mhgProbe.attempts.some(
          (a) => Object.keys(a.rate_limit_headers || {}).length
        ),
      },
    },
    highest_value_next_step: nextStep,
  };

  // Strip bulky raw room analysis to separate file
  writeJson(path.join(OUT_DIR, "existing-capability-audit.json"), audit);
  writeJson(path.join(OUT_DIR, "multicodes-sample.json"), {
    warning: WARNING,
    connectivity: multicodes.connectivity,
    sample: multicodes.sample,
    supplier_matrix: multicodes.supplier_matrix,
  });
  writeJson(path.join(OUT_DIR, "mhg-sample.json"), {
    warning: WARNING,
    connectivity: mhg.connectivity,
    sample: mhg.sample,
    room_semantic_analysis: mhg.room_semantic_analysis.slice(0, 200),
    room_count_detail: mhg.room_count_detail,
    room_count_verdict: mhg.room_count_verdict,
  });
  writeJson(path.join(OUT_DIR, "validation-summary.json"), summary);

  console.log(
    JSON.stringify({
      module: "giata-test-products-validation",
      event: "complete",
      warning: WARNING,
      multicodes_auth: multicodes.connectivity.authenticated,
      mhg_auth: mhg.connectivity.authenticated,
      multicodes_sample: multicodes.sample.length,
      mhg_sample: mhg.sample.length,
      multicodes_verdict: multicodes.verdict,
      mhg_room_verdict: mhg.room_count_verdict,
      next: nextStep,
      out_dir: "reports/hotel-intelligence/giata-test-products-validation-v1",
    })
  );
}

main().catch((err) => {
  console.error(
    JSON.stringify({
      module: "giata-test-products-validation",
      event: "fatal",
      message: sanitizeError(err?.message || err),
    })
  );
  process.exit(1);
});
