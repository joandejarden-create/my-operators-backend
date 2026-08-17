/**
 * DataForSEO API client (Basic Auth from env).
 * Never log login/password. Never write Census from this module.
 */

export const DATAFORSEO_CLIENT_VERSION = "dataforseo-client-v1";
export const DATAFORSEO_API_BASE = "https://api.dataforseo.com/v3";

/**
 * @param {NodeJS.ProcessEnv|Record<string,string|undefined>} [env]
 */
export function resolveDataForSeoCredentials(env = process.env) {
  const login = String(env.DATAFORSEO_LOGIN || "").trim();
  const password = String(env.DATAFORSEO_PASSWORD || "").trim();
  return {
    ok: Boolean(login && password),
    login_present: Boolean(login),
    password_present: Boolean(password),
    // never return secrets
  };
}

/**
 * @param {NodeJS.ProcessEnv|Record<string,string|undefined>} [env]
 */
export function buildDataForSeoAuthHeader(env = process.env) {
  const login = String(env.DATAFORSEO_LOGIN || "").trim();
  const password = String(env.DATAFORSEO_PASSWORD || "").trim();
  if (!login || !password) {
    throw new Error("DATAFORSEO_LOGIN_or_PASSWORD_missing");
  }
  const token = Buffer.from(`${login}:${password}`, "utf8").toString("base64");
  return `Basic ${token}`;
}

/**
 * Country / city → DataForSEO location_name heuristic for CALA.
 * @param {{ country?: string, city?: string }} loc
 */
export function resolveDataForSeoLocationName(loc = {}) {
  const country = String(loc.country || "").trim();
  const city = String(loc.city || "").trim();
  const map = {
    mexico: "Mexico",
    colombia: "Colombia",
    "dominican republic": "Dominican Republic",
    panama: "Panama",
    "costa rica": "Costa Rica",
    peru: "Peru",
    chile: "Chile",
    argentina: "Argentina",
    brazil: "Brazil",
    jamaica: "Jamaica",
  };
  const cKey = country.toLowerCase();
  const countryLoc = map[cKey] || country || "United States";
  // DataForSEO Maps often returns empty for accented location_name (e.g. Bogotá).
  const asciiCity = stripDiacritics(city);
  if (asciiCity) return `${asciiCity},${countryLoc}`;
  return countryLoc;
}

/** Normalize location tokens for DataForSEO location_name matching. */
export function stripDiacritics(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/ñ/gi, (m) => (m === "Ñ" ? "N" : "n"))
    .trim();
}

/**
 * POST to DataForSEO live endpoint.
 * @param {string} path — e.g. /serp/google/organic/live/advanced
 * @param {object[]} tasks
 * @param {{ env?: NodeJS.ProcessEnv, fetchImpl?: typeof fetch, timeoutMs?: number }} [opts]
 */
export async function dataForSeoPost(path, tasks, opts = {}) {
  const env = opts.env || process.env;
  const fetchImpl = opts.fetchImpl || fetch;
  const timeoutMs = opts.timeoutMs ?? 60000;
  const url = `${DATAFORSEO_API_BASE}${path.startsWith("/") ? path : `/${path}`}`;
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetchImpl(url, {
      method: "POST",
      signal: ctrl.signal,
      headers: {
        Authorization: buildDataForSeoAuthHeader(env),
        "Content-Type": "application/json",
      },
      body: JSON.stringify(tasks),
    });
    const json = await res.json().catch(() => ({}));
    return {
      ok: res.ok && Number(json.status_code) === 20000,
      http_status: res.status,
      status_code: json.status_code,
      status_message: json.status_message,
      cost: Number(json.cost) || 0,
      tasks: json.tasks || [],
      raw_meta: {
        time: json.time,
        cost: json.cost,
        tasks_count: json.tasks_count,
      },
    };
  } catch (err) {
    return {
      ok: false,
      http_status: 0,
      status_code: null,
      status_message: err?.name === "AbortError" ? "timeout" : err?.message || String(err),
      cost: 0,
      tasks: [],
      network_error: true,
    };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Live Google Organic SERP (advanced).
 * @param {{ keyword: string, location_name?: string, language_code?: string, depth?: number }} task
 * @param {object} [opts]
 */
export async function fetchGoogleOrganicLive(task, opts = {}) {
  const payload = [
    {
      keyword: String(task.keyword || "").trim(),
      location_name: task.location_name || "United States",
      language_code: task.language_code || "en",
      depth: task.depth ?? 10,
      device: "desktop",
      os: "windows",
    },
  ];
  const res = await dataForSeoPost(
    "/serp/google/organic/live/advanced",
    payload,
    opts
  );
  const items = [];
  for (const t of res.tasks || []) {
    for (const result of t.result || []) {
      for (const item of result.items || []) {
        if (item.type === "organic" || item.type === "local_pack" || item.url) {
          items.push({
            type: item.type,
            rank_absolute: item.rank_absolute,
            title: item.title,
            url: item.url,
            description: item.description,
            domain: item.domain,
          });
        }
      }
    }
  }
  return { ...res, items, query_kind: "serp_organic" };
}

/**
 * Live Google Maps SERP (advanced).
 * @param {{ keyword: string, location_name?: string, language_code?: string, depth?: number }} task
 * @param {object} [opts]
 */
export async function fetchGoogleMapsLive(task, opts = {}) {
  const payload = [
    {
      keyword: String(task.keyword || "").trim(),
      location_name: task.location_name || "United States",
      language_code: task.language_code || "en",
      depth: task.depth ?? 10,
    },
  ];
  const res = await dataForSeoPost(
    "/serp/google/maps/live/advanced",
    payload,
    opts
  );
  const items = [];
  for (const t of res.tasks || []) {
    for (const result of t.result || []) {
      for (const item of result.items || []) {
        items.push({
          type: item.type || "maps",
          rank_absolute: item.rank_absolute,
          title: item.title,
          url: item.url || item.website || null,
          website: item.website || item.url || null,
          domain: item.domain,
          address: item.address,
          phone: item.phone,
          latitude: item.latitude ?? item.gps_coordinates?.latitude,
          longitude: item.longitude ?? item.gps_coordinates?.longitude,
          rating: item.rating,
          category: item.category,
          place_id: item.place_id,
          cid: item.cid,
        });
      }
    }
  }
  return { ...res, items, query_kind: "google_maps" };
}
