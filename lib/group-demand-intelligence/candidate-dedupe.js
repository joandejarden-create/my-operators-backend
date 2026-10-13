/**
 * Cross-provider GDI candidate dedupe / entity resolution.
 * Preserve separate annual cycles (2027 vs 2028).
 */

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\b(the|annual|conference|meeting|championship|tournament|inc|llc)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function extractYear(c) {
  const raw =
    c.eventStartDate ||
    c.startDate ||
    c.dates?.start ||
    c.title ||
    c.eventName ||
    "";
  const m = String(raw).match(/\b(20[2-3][0-9])\b/);
  return m ? m[1] : null;
}

function orgKey(c) {
  return norm(c.organizationName || c.organization || "");
}

function nameKey(c) {
  return norm(c.title || c.eventName || c.name || "");
}

function locationKey(c) {
  return norm(c.location || c.destinationStatus || c.city || c.geographyEvidence || "");
}

function officialKey(c) {
  const url =
    c.officialSource ||
    c.officialUrl ||
    (c.evidenceSources || [])[0]?.url ||
    (c.sources || [])[0]?.url ||
    "";
  try {
    const u = new URL(String(url));
    return `${u.hostname}${u.pathname}`.toLowerCase().replace(/\/$/, "");
  } catch {
    return String(url || "").toLowerCase();
  }
}

export function candidateIdentityKey(c) {
  const year = extractYear(c) || "noyear";
  const name = nameKey(c) || "noname";
  const org = orgKey(c) || "noorg";
  const loc = locationKey(c) || "noloc";
  const official = officialKey(c);
  // Prefer official URL + year when present; else name+org+year+loc
  if (official && official.length > 8) {
    return `url:${official}|y:${year}`;
  }
  return `n:${name}|o:${org}|y:${year}|l:${loc}`;
}

/**
 * Dedupe candidates. First occurrence wins unless later has stronger evidence.
 */
export function dedupeDiscoveryCandidates(candidates = []) {
  const map = new Map();
  const rejected = [];

  const strength = (c) => {
    const sources = c.evidenceSources || c.sources || [];
    let s = Array.isArray(sources) ? sources.length : 0;
    if (c.officialSource || c.officialUrl) s += 2;
    if (c.eventStartDate || c.startDate) s += 1;
    if (c.estimatedPeakRooms != null || c.estimatedAttendance != null) s += 1;
    if (c.venueSourcingStatus && c.venueSourcingStatus !== "UNKNOWN") s += 1;
    return s;
  };

  for (const c of candidates || []) {
    if (!c || !(c.title || c.eventName || c.name)) {
      rejected.push({ reason: "missing_title", candidate: c });
      continue;
    }
    const key = candidateIdentityKey(c);
    const existing = map.get(key);
    if (!existing) {
      map.set(key, { ...c, _dedupeKey: key });
      continue;
    }
    // Same cycle — keep stronger evidence; do not merge different years (year in key)
    if (strength(c) > strength(existing)) {
      rejected.push({ reason: "superseded_weaker_duplicate", candidate: existing, key });
      map.set(key, { ...c, _dedupeKey: key });
    } else {
      rejected.push({ reason: "duplicate", candidate: c, key });
    }
  }

  return {
    candidates: [...map.values()],
    rejected,
    duplicatesRemoved: rejected.filter((r) => r.reason === "duplicate" || r.reason === "superseded_weaker_duplicate").length,
  };
}
