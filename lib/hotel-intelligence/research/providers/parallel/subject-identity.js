/**
 * Provider-neutral subject identity contract + fail-closed gate.
 * Identity must pass before any downstream ownership/contact scoring.
 */

export const SUBJECT_IDENTITY_CONTRACT_VERSION = "subject-identity-contract-v1";

const MEXICO_ALIASES = new Set(["mexico", "méxico", "mx", "united mexican states"]);

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function tokens(s) {
  return norm(s).split(/\s+/).filter(Boolean);
}

function nameCompatible(requested, resolved) {
  const a = norm(requested);
  const b = norm(resolved);
  if (!a || !b) return false;
  if (a === b || a.includes(b) || b.includes(a)) return true;
  const ta = new Set(tokens(a));
  const tb = tokens(b);
  const overlap = tb.filter((t) => ta.has(t) && t.length > 2).length;
  return overlap >= Math.min(3, ta.size);
}

function countryExactMatch(requested, resolved) {
  const a = norm(requested);
  const b = norm(resolved);
  if (!a || !b) return false;
  if (a === b) return true;
  if (MEXICO_ALIASES.has(a) && MEXICO_ALIASES.has(b)) return true;
  // "United Kingdom (Scotland)" vs "Mexico" → fail
  if (b.includes(a) || a.includes(b)) return true;
  return false;
}

function cityCompatible(requested, resolved) {
  const a = norm(requested);
  const b = norm(resolved);
  if (!a || !b) return false;
  if (a === b || a.includes(b) || b.includes(a)) return true;
  // Puerto Vallarta / Pto Vallarta / Bahia de Banderas adjacency only with PV evidence
  const pv = ["puerto vallarta", "pto vallarta", "pto. vallarta"];
  if (pv.some((x) => a.includes(x.replace(".", "")) || a.includes("puerto vallarta"))) {
    if (b.includes("puerto vallarta") || b.includes("pto vallarta")) return true;
    if (b.includes("bahia de banderas") || b.includes("bay of banderas")) return "ADJACENT_NEEDS_PROPERTY_EVIDENCE";
  }
  // Bermuda Sandys / Somerset / West End
  const bermudaLocal = ["somerset", "sandys", "sandy s", "west end", "kings point"];
  if (bermudaLocal.some((x) => a.includes(x))) {
    if (bermudaLocal.some((x) => b.includes(x))) return true;
  }
  // Guadalajara metro / Zapopan Expo corridor
  const gdl = ["guadalajara", "zapopan", "expo"];
  if (gdl.some((x) => a.includes(x))) {
    if (gdl.some((x) => b.includes(x))) return true;
  }
  return false;
}

function haversineKm(lat1, lon1, lat2, lon2) {
  const toRad = (d) => (Number(d) * Math.PI) / 180;
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const aa =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(aa));
}

/**
 * Build canonical subject payload for provider input (no ownership gold).
 */
export function buildCanonicalSubjectInput(seed = {}) {
  const coords =
    seed.coordinates ||
    (seed.latitude != null && seed.longitude != null
      ? { lat: Number(seed.latitude), lng: Number(seed.longitude) }
      : null);
  return {
    name: seed.hotel_name || seed.name || null,
    city: seed.city || null,
    country: seed.country || null,
    address: seed.address || null,
    coordinates: coords,
    canonical_id: seed.hotel_id || seed.canonical_id || null,
    census_record_id: seed.census_record_id || seed.airtable_record_id || seed.hotel_id || null,
    aliases: Array.isArray(seed.aliases) ? seed.aliases.filter(Boolean) : [],
    market: seed.market || null,
    rooms: seed.rooms ?? null,
  };
}

/**
 * Short identity-first instruction — not a buried prose dump.
 */
export function buildSubjectIdentityLockText(subject = {}) {
  return [
    "SUBJECT IDENTITY LOCK",
    "",
    "Research ONLY the exact subject below.",
    "",
    `Hotel: ${subject.name || "UNKNOWN"}`,
    `City: ${subject.city || "UNKNOWN"}`,
    `Country: ${subject.country || "UNKNOWN"}`,
    subject.address ? `Address: ${subject.address}` : null,
    subject.coordinates
      ? `Coordinates: ${subject.coordinates.lat}, ${subject.coordinates.lng}`
      : null,
    subject.canonical_id ? `Canonical hotel ID: ${subject.canonical_id}` : null,
    subject.census_record_id ? `Census / record ID: ${subject.census_record_id}` : null,
    subject.aliases?.length ? `Aliases: ${subject.aliases.join("; ")}` : null,
    "",
    "If the source material appears to describe another hotel, another city,",
    "another country, or a similarly named property: REJECT IT.",
    "Do not substitute another subject.",
    "",
    "Before conducting ownership or contact research:",
    "1. Resolve the hotel identity.",
    "2. Confirm city/country match the requested subject.",
    "3. Confirm the resolved hotel name is the same property.",
    "4. Return identity evidence in subject_identity.",
    "5. If exact identity cannot be established, stop and return identity_match=UNRESOLVED",
    "   (or FALSE) and do NOT invent ownership findings for a different property.",
  ]
    .filter((line) => line != null)
    .join("\n");
}

/**
 * Compact research objective AFTER identity — no gold ownership answers.
 */
export function buildPostIdentityResearchObjective(template = {}, extras = {}) {
  const lines = [
    "PHASE 1 — IDENTITY FIRST (mandatory).",
    "PHASE 2 — Only after identity_match can be TRUE, research and SEPARATE:",
    "- property-owning legal entity (PropCo / deed / Order-named owner)",
    "- economic sponsor / sponsor group (do NOT equate sponsor with deed owner)",
    "- current operator vs former/announced operator (do NOT collapse)",
    "- lender / financing party (do NOT equate lender with owner)",
    "- developer / development sponsor (do NOT equate with deed owner)",
    "- acquisition / ownership timeline with CURRENT|FORMER|HISTORICAL|ANNOUNCED|UNRESOLVED",
    "- key owner-side / sponsor-side people",
    "- owner-side contact paths (candidate evidence only; never mark inferred email VERIFIED)",
    "",
    "Required relationship predicates when evidence supports them:",
    "OWNED_BY, SPONSORED_BY, OPERATED_BY, FORMERLY_OPERATED_BY, FINANCED_BY, DEVELOPED_BY, CONTROLLED_BY.",
    "Do not use generic 'associated with' when a precise predicate is available.",
    "",
    `Template: ${template.template_id || "UNKNOWN"}`,
    template.research_objective ? `Objective: ${template.research_objective}` : null,
    extras.negative_screens?.length
      ? `Negative screens: ${extras.negative_screens.join(", ")}`
      : null,
    extras.open_questions?.length
      ? `Focus questions:\n${extras.open_questions.map((q) => `- ${q}`).join("\n")}`
      : null,
  ].filter(Boolean);
  return lines.join("\n");
}

/**
 * Fail-closed identity gate. Generic for all providers/benchmarks.
 * @returns {{ ok: boolean, identity_match: 'TRUE'|'FALSE'|'UNRESOLVED', failure_reason?: string, checks: object }}
 */
export function assertSubjectIdentityMatch(requested = {}, resolved = {}, opts = {}) {
  const reqName = requested.name || requested.hotel_name || requested.requested_name;
  const reqCity = requested.city || requested.requested_city;
  const reqCountry = requested.country || requested.requested_country;
  const reqCoords = requested.coordinates || requested.requested_coordinates;

  const resName =
    resolved.resolved_name ||
    resolved.canonical_name ||
    resolved.hotel_name ||
    resolved.name;
  const resCity = resolved.resolved_city || resolved.city;
  const resCountry = resolved.resolved_country || resolved.country;
  const resCoords = resolved.resolved_coordinates || resolved.coordinates;

  const declaredMatch = String(resolved.identity_match || "")
    .trim()
    .toUpperCase();

  const checks = {
    country_match: countryExactMatch(reqCountry, resCountry),
    city_match: cityCompatible(reqCity, resCity),
    name_match: nameCompatible(reqName, resName),
    coords_km: null,
    coords_ok: null,
    provider_declared_match: declaredMatch || null,
  };

  if (
    reqCoords?.lat != null &&
    reqCoords?.lng != null &&
    resCoords?.lat != null &&
    resCoords?.lng != null
  ) {
    const km = haversineKm(reqCoords.lat, reqCoords.lng, resCoords.lat, resCoords.lng);
    checks.coords_km = Number(km.toFixed(2));
    checks.coords_ok = km <= Number(opts.coord_tolerance_km || 5);
  }

  // Country hard fail — geography overrides semantic name similarity
  if (reqCountry && resCountry && !checks.country_match) {
    return {
      ok: false,
      identity_match: "FALSE",
      failure_reason: "WRONG_SUBJECT_IDENTITY",
      checks,
      detail: `Country mismatch: requested ${reqCountry} vs resolved ${resCountry}`,
    };
  }

  if (declaredMatch === "UNRESOLVED" || (!resName && !resCountry)) {
    return {
      ok: false,
      identity_match: "UNRESOLVED",
      failure_reason: "IDENTITY_UNRESOLVED",
      checks,
    };
  }

  if (declaredMatch === "FALSE") {
    return {
      ok: false,
      identity_match: "FALSE",
      failure_reason: "WRONG_SUBJECT_IDENTITY",
      checks,
    };
  }

  const cityOk = checks.city_match === true;
  const cityAdjacent = checks.city_match === "ADJACENT_NEEDS_PROPERTY_EVIDENCE";
  const nameOk = checks.name_match === true;
  const coordsOk = checks.coords_ok !== false; // null (missing) allowed

  if (!nameOk || (!cityOk && !cityAdjacent) || !coordsOk) {
    return {
      ok: false,
      identity_match: "FALSE",
      failure_reason: "WRONG_SUBJECT_IDENTITY",
      checks,
      detail: `Identity failed name=${nameOk} city=${checks.city_match} coords=${checks.coords_ok}`,
    };
  }

  // Adjacent market only with strong name + country + optional coords
  if (cityAdjacent && !(nameOk && checks.country_match && checks.coords_ok === true)) {
    return {
      ok: false,
      identity_match: "UNRESOLVED",
      failure_reason: "IDENTITY_UNRESOLVED",
      checks,
      detail: "Adjacent market without exact property coordinate proof",
    };
  }

  return {
    ok: true,
    identity_match: "TRUE",
    failure_reason: null,
    checks,
  };
}

/**
 * Extract resolved identity fields from normalized Parallel / provider output.
 */
export function extractResolvedSubjectIdentity(normalized = {}) {
  const si = normalized.subject_identity || normalized.SUBJECT_IDENTITY || {};
  return {
    requested_name: si.requested_name || null,
    resolved_name: si.resolved_name || si.canonical_name || si.hotel_name || null,
    requested_city: si.requested_city || null,
    resolved_city: si.resolved_city || si.city || null,
    requested_country: si.requested_country || null,
    resolved_country: si.resolved_country || si.country || null,
    requested_address: si.requested_address || null,
    resolved_address: si.resolved_address || si.address || null,
    requested_coordinates: si.requested_coordinates || null,
    resolved_coordinates: si.resolved_coordinates || si.coordinates || null,
    identity_match: si.identity_match || null,
    identity_confidence: si.identity_confidence || null,
    identity_evidence: si.identity_evidence || [],
    identity_conflicts: si.identity_conflicts || [],
  };
}
