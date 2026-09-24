/**
 * Strict person-boundary validation + multi-person block splitting (Native WHO V5).
 */

const TITLE_TOKENS =
  /^(coordinator|manager|director|president|ceo|coo|cfo|vp|volunteer|phone|forms?|inquiries|emails?|general|operations|associate|senior|assistant|state|administrator|laboratory|programs?|phase|headquarters|futures|club)$/i;

const NON_PERSON_PHRASE =
  /^(entry forms|emails? general inquiries|general inquiries|event operations phone|volunteer coordinator|community engagement coordinator|emails general inquiries)$/i;

const BAD_PREFIX = /^(says|state director|director|email|phone|contact|the|our)\s+/i;

/**
 * Strict human name for WHO confirmation (stricter than isLikelyPersonName).
 */
export function isStrictPersonName(name) {
  const n = String(name || "").trim();
  if (!n || n.length < 5) return false;
  if (NON_PERSON_PHRASE.test(n)) return false;
  if (BAD_PREFIX.test(n)) return false;
  if (/[@<>&#]|email\s*protected/i.test(n)) return false;
  if (/\d{3}/.test(n)) return false;

  const parts = n.split(/\s+/).filter(Boolean);
  if (parts.length < 2 || parts.length > 4) return false;

  // Reject if any token is purely a title word used as a name part incorrectly
  // e.g. "Moise Manager", "De Vito Associate Director", "Event Operations Phone"
  const last = parts[parts.length - 1];
  if (TITLE_TOKENS.test(last)) return false;
  if (parts.some((p) => /^(phone|forms?|inquiries|emails?)$/i.test(p))) return false;

  // First token must look like a given name (not a title)
  if (TITLE_TOKENS.test(parts[0])) return false;

  // Classic First Last / First M. Last / First Middle Last
  if (!/^[A-Z][a-z]+(?:\s+[A-Z](?:\.|[a-z'.-]+)){1,3}$/.test(n)) return false;

  // Reject common place / non-person bigrams / UI chrome
  if (
    /^(palm beach|new york|los angeles|san diego|delray beach|boca raton|united states|north america|jimmy awards|tony awards|ticketing ticket|offers account|instagram facebook|other related|quick links|landing page|get involved|coach mentor|presidents cup|safe sport|our staff|contact form)$/i.test(
      n
    )
  ) {
    return false;
  }

  // Reject if any token is a department/title/org-noun / UI word used as a name
  if (
    parts.some((p) =>
      /^(business|development|operations|engagement|services|communications|marketing|partnerships?|executive|tournament|conference|ticketing|ticket|policy|awards|education|programs?|audience|membership|logistics|officials?|awareness|coaches?|affiliate|concussion|registration|directory|committee|board|staff|team|instagram|facebook|linkedin|twitter|links?|page|involved|related|mentor|cup|soccer|academy|educat|summer|coaching|spotlight|welcome|partners?|schools?|community|olympic|invitational|showcase|united|sanctioning|documents?|boys|girls|technical|greater|organizational|chart|loudoun|mini|media|camp|areas|advisory|officer|international|important|worth|natural|social|chief|operating|management)$/i.test(
        p
      )
    )
  ) {
    return false;
  }

  // Reject title-case noun phrases / role fragments mistaken for names
  if (
    /^(heads up|team officials|affiliate logistics|concussion awareness|head coaches|safe sport|our team|contact us|club administrator|futures phase|headquarters laboratory|laboratory programs|associate director|view bio|national institute)$/i.test(
      n
    )
  ) {
    return false;
  }

  // Reject markdown / UI fragments
  if (/^(view|click|read|learn|meet|skip|register)\b/i.test(n)) return false;
  if (/\b(office|here|now|today|inc|llc|solutions)\b/i.test(n)) return false;

  return true;
}

/**
 * Split multi-person leadership strings into separate names.
 * Patterns: A / B | A & B | A and B | A, B | A | B
 */
export function splitMultiPersonBlock(raw) {
  const s = String(raw || "").trim();
  if (!s) return [];

  // "Co-Directors: Jane Smith and John Doe"
  const labeled = s.match(
    /^(?:co-?directors?|tournament directors?|executive staff|directors?|contacts?)\s*[:\-–]\s*(.+)$/i
  );
  const body = labeled ? labeled[1] : s;

  let parts;
  if (/\s+\/\s+/.test(body)) parts = body.split(/\s+\/\s+/);
  else if (/\s+\|\s+/.test(body)) parts = body.split(/\s+\|\s+/);
  else if (/\s+&\s+/.test(body)) parts = body.split(/\s+&\s+/);
  else if (/\s+and\s+/i.test(body) && !/,/.test(body)) parts = body.split(/\s+and\s+/i);
  else if (/,/.test(body) && body.split(",").length === 2) {
    const bits = body.split(",").map((x) => x.trim());
    // "Jane Smith, John Doe" vs "Smith, Jane" — require both look like First Last
    if (bits.every((b) => /^[A-Z][a-z]+\s+[A-Z]/.test(b))) parts = bits;
  }

  if (!parts) {
    return isStrictPersonName(s) ? [s] : [];
  }

  return parts.map((p) => p.trim()).filter((p) => isStrictPersonName(p));
}

/**
 * Share role context across multi-person block when grammar supports it.
 */
export function expandMultiPersonWithSharedRole(nameField, roleField) {
  const names = splitMultiPersonBlock(nameField);
  if (names.length >= 2) {
    return names.map((name) => ({ name, role: roleField || null, fromMultiPersonBlock: true }));
  }
  if (isStrictPersonName(nameField)) {
    // Role field may embed the next person: "Exec Dir Mark Baron, Tournament Director"
    const sequential = splitSequentialNameRoleString(
      `${nameField}, ${roleField || ""}`.replace(/,\s*$/, "")
    );
    if (sequential.length >= 2) {
      return sequential.map((e) => ({ ...e, fromMultiPersonBlock: true }));
    }
    return [{ name: nameField, role: roleField || null, fromMultiPersonBlock: false }];
  }
  // Role field may contain "Tournament Directors: A / B"
  const fromRole = splitMultiPersonBlock(roleField || "");
  if (fromRole.length >= 2) {
    return fromRole.map((name) => ({
      name,
      role: String(nameField || "Tournament leadership").slice(0, 80),
      fromMultiPersonBlock: true,
    }));
  }
  const sequential = splitSequentialNameRoleString(
    [nameField, roleField].filter(Boolean).join(", ")
  );
  if (sequential.length >= 1) {
    return sequential.map((e) => ({
      ...e,
      fromMultiPersonBlock: sequential.length >= 2,
    }));
  }
  return [];
}

const ROLE_WORD =
  /^(?:executive|tournament|conference|doubles|housing|registration|operations|program|member|business|development|vice|president|director|coordinator|manager|chair|lead|staff|tour|open|assistant|senior|associate|services|engagement|partnerships?|marketing|communications|of|and|the|&)$/i;

/**
 * Split flattened leadership strings:
 * "Adam Baron, Executive Director & Doubles Coordinator Mark Baron, Tournament Director"
 */
export function splitSequentialNameRoleString(raw) {
  const s = String(raw || "").replace(/\s+/g, " ").trim();
  if (!s) return [];

  // Exactly First Last (optional middle initial) — never absorb title words into the name.
  const nameRe = /\b([A-Z][a-z]+(?:\s+[A-Z]\.)?\s+[A-Z][a-z'.-]+)\b/g;
  const hits = [];
  let m;
  while ((m = nameRe.exec(s))) {
    const name = m[1].replace(/\s+/g, " ").trim();
    if (!isStrictPersonName(name)) continue;
    const parts = name.split(/\s+/);
    if (parts.some((p) => ROLE_WORD.test(p))) continue;
    hits.push({ name, index: m.index, end: m.index + m[1].length });
  }
  if (!hits.length) return [];

  const out = [];
  for (let i = 0; i < hits.length; i++) {
    const start = hits[i].end;
    const stop = i + 1 < hits.length ? hits[i + 1].index : s.length;
    let role = s
      .slice(start, stop)
      .replace(/^[\s,;:\-–&\/]+/, "")
      .replace(/[\s,;:\-–&\/]+$/, "")
      .trim();
    // Keep role up to next person; strip trailing connectors
    role = role.replace(/\s+&\s*$/, "").trim();
    if (role.length > 80) role = role.slice(0, 80);
    if (role && !/[a-z]/i.test(role)) role = null;
    out.push({ name: hits[i].name, role: role || null });
  }
  return out;
}
