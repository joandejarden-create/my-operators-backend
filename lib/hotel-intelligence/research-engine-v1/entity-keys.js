/**
 * Shared organization / person / relationship / channel dedupe keys.
 */

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .replace(/\s+/g, "_");
}

export function orgDedupeKey({ legalName, jurisdiction = "", registryId = "" } = {}) {
  const name = norm(legalName);
  if (!name) return null;
  const j = norm(jurisdiction) || "unk";
  const r = norm(registryId);
  return r ? `org::${name}::${j}::${r}` : `org::${name}::${j}`;
}

export function personDedupeKey({ displayName, orgId = "", orgDedupe = "" } = {}) {
  const n = norm(displayName);
  if (!n) return null;
  const org = orgId || orgDedupe || "unk_org";
  return `person::${n}::${org}`;
}

export function relationshipDedupeKey({
  hpcId,
  relationshipType,
  orgId = "",
  orgDedupe = "",
} = {}) {
  const hotel = String(hpcId || "").trim();
  const rel = norm(relationshipType);
  const org = orgId || orgDedupe;
  if (!hotel || !rel || !org) return null;
  return `rel::${hotel}::${rel}::${org}`;
}

export function channelDedupeKey({
  subjectId,
  kind,
  normalizedValue,
} = {}) {
  const subj = String(subjectId || "").trim();
  const k = norm(kind);
  const v = norm(normalizedValue);
  if (!subj || !k || !v) return null;
  return `ch::${subj}::${k}::${v}`;
}

/**
 * Canonical HI Graph Evidence key: one source document (URL) per hotel.
 * Multiple claims may attach to the same evidence row via notes/snippet.
 */
export function evidenceDedupeKey({ hpcId, sourceUrl } = {}) {
  const hotel = String(hpcId || "").trim();
  let url = String(sourceUrl || "")
    .trim()
    .toLowerCase()
    .replace(/\/+$/, "");
  try {
    const u = new URL(url);
    u.hash = "";
    url = u.toString().replace(/\/+$/, "");
  } catch {
    /* keep normalized string */
  }
  if (!hotel || !url) return null;
  return `ev::${hotel}::${url}`;
}

/**
 * Assert no duplicate keys in a list of { dedupe_key }.
 */
export function assertNoDuplicateKeys(items, label = "entities") {
  const seen = new Set();
  const dupes = [];
  for (const item of items || []) {
    const k = item?.dedupe_key;
    if (!k) continue;
    if (seen.has(k)) dupes.push(k);
    else seen.add(k);
  }
  return {
    ok: dupes.length === 0,
    label,
    duplicate_keys: [...new Set(dupes)],
    unique_count: seen.size,
  };
}
