/**
 * GLEIF LEI client — corporate resolution only (P1E).
 * Does not discover hotel owners from scratch.
 *
 * API: https://api.gleif.org/api/v1 (no auth)
 */

export const GLEIF_CLIENT_VERSION = "ownership-gleif-client-v1";
export const GLEIF_API_BASE = "https://api.gleif.org/api/v1";

/**
 * @param {string} legalName
 * @param {{ jurisdiction?: string|null, env?: object, timeoutMs?: number }} [opts]
 */
export async function searchLeiByLegalName(legalName, opts = {}) {
  const name = String(legalName || "").trim();
  if (!name || name.length < 3) {
    return { ok: false, error: "name_too_short", records: [] };
  }
  const enabled =
    String(opts.env?.OWNERSHIP_GLEIF_ENABLE || process.env.OWNERSHIP_GLEIF_ENABLE || "1").trim() !==
    "0";
  if (!enabled) {
    return { ok: false, error: "gleif_disabled", records: [] };
  }

  const params = new URLSearchParams();
  params.set("filter[entity.legalName]", name);
  params.set("page[size]", "5");
  if (opts.jurisdiction) {
    params.set(
      "filter[entity.legalAddress.country]",
      String(opts.jurisdiction).slice(0, 2).toUpperCase()
    );
  }

  try {
    const res = await fetch(`${GLEIF_API_BASE}/lei-records?${params}`, {
      headers: { Accept: "application/vnd.api+json" },
      signal: AbortSignal.timeout(opts.timeoutMs || 20000),
    });
    if (!res.ok) {
      return {
        ok: false,
        error: `http_${res.status}`,
        records: [],
      };
    }
    const body = await res.json();
    const data = Array.isArray(body?.data) ? body.data : [];
    return {
      ok: true,
      records: data.map(mapLeiRecord),
      raw_count: data.length,
    };
  } catch (err) {
    return {
      ok: false,
      error: String(err?.message || err).slice(0, 120),
      records: [],
    };
  }
}

/**
 * @param {string} lei
 * @param {{ env?: object, timeoutMs?: number }} [opts]
 */
export async function getLeiRecord(lei, opts = {}) {
  const code = String(lei || "").trim().toUpperCase();
  if (!/^[A-Z0-9]{20}$/.test(code)) {
    return { ok: false, error: "invalid_lei", record: null };
  }
  try {
    const res = await fetch(`${GLEIF_API_BASE}/lei-records/${code}`, {
      headers: { Accept: "application/vnd.api+json" },
      signal: AbortSignal.timeout(opts.timeoutMs || 20000),
    });
    if (!res.ok) {
      return { ok: false, error: `http_${res.status}`, record: null };
    }
    const body = await res.json();
    return { ok: true, record: mapLeiRecord(body?.data) };
  } catch (err) {
    return {
      ok: false,
      error: String(err?.message || err).slice(0, 120),
      record: null,
    };
  }
}

/**
 * Fetch direct parent LEI relationship when linked.
 * @param {string} lei
 * @param {{ env?: object, timeoutMs?: number }} [opts]
 */
export async function getDirectParentLei(lei, opts = {}) {
  const code = String(lei || "").trim().toUpperCase();
  if (!/^[A-Z0-9]{20}$/.test(code)) {
    return { ok: false, error: "invalid_lei", parent: null };
  }
  try {
    const res = await fetch(
      `${GLEIF_API_BASE}/lei-records/${code}/direct-parent-relationship`,
      {
        headers: { Accept: "application/vnd.api+json" },
        signal: AbortSignal.timeout(opts.timeoutMs || 20000),
      }
    );
    if (res.status === 404) {
      return { ok: true, parent: null, note: "no_direct_parent" };
    }
    if (!res.ok) {
      return { ok: false, error: `http_${res.status}`, parent: null };
    }
    const body = await res.json();
    const rel = body?.data;
    const parentLei =
      rel?.relationships?.parent?.data?.id ||
      rel?.attributes?.relationship?.parent?.id ||
      null;
    if (!parentLei) {
      return { ok: true, parent: null, note: "parent_lei_missing_in_payload" };
    }
    const parentRec = await getLeiRecord(parentLei, opts);
    return {
      ok: true,
      parent_lei: parentLei,
      parent: parentRec.record,
    };
  } catch (err) {
    return {
      ok: false,
      error: String(err?.message || err).slice(0, 120),
      parent: null,
    };
  }
}

function mapLeiRecord(data) {
  if (!data) return null;
  const attrs = data.attributes || {};
  const entity = attrs.entity || {};
  return {
    lei: data.id || attrs.lei || null,
    legal_name: entity.legalName?.name || entity.legalName || null,
    status: entity.status || null,
    jurisdiction:
      entity.jurisdiction ||
      entity.legalAddress?.country ||
      null,
    legal_address: entity.legalAddress || null,
    registration_status: attrs.registration?.status || null,
  };
}
