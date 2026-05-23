/**
 * Read-only Airtable Metadata API helpers (Hotel Census on AIRTABLE_BASE_ID_ALT).
 */

export async function metaFetch(baseId, token, path, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${baseId}${path}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

/**
 * @param {string} tableName
 * @returns {Promise<{ table: object, fields: Array<{name,id,type}>, metadataAvailable: boolean }>}
 */
export async function fetchTableSchema(tableName) {
  const token = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!token || !baseId) {
    throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");
  }

  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) {
    return {
      table: null,
      fields: [],
      metadataAvailable: false,
      metadataError: `Metadata API ${res.status}: ${JSON.stringify(json)}`,
    };
  }

  const table = (json.tables || []).find((t) => t.name === tableName);
  if (!table) {
    throw new Error(`Table not found in metadata: ${tableName}`);
  }

  const fields = (table.fields || []).map((f) => ({
    name: f.name,
    id: f.id,
    type: f.type,
  }));

  return { table, fields, metadataAvailable: true, metadataError: null };
}
