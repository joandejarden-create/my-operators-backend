/**
 * Brand Explorer — no repeated card headings in descriptions.
 *
 * Title appears once in the title field. Body begins with substantive content.
 * Do not emit "Title: …" echoes at the start of descriptions.
 *
 * Contract: BE_NO_REPEATED_CARD_HEADING_V1
 */

export const BE_NO_REPEATED_CARD_HEADING_V1 = "BE_NO_REPEATED_CARD_HEADING_V1";

export const REPEATED_HEADING_FAIL = Object.freeze({
  TITLE_ECHO_PREFIX: "repeated_card_heading_in_description",
});

function nz(s) {
  return s == null ? "" : String(s).trim();
}

/** Normalize for echo comparison — punctuation/case insensitive, collapse whitespace. */
export function normalizeHeadingForEchoCompare(text) {
  return nz(text)
    .toLowerCase()
    .replace(/[\u2013\u2014]/g, "-")
    .replace(/[“”]/g, '"')
    .replace(/[‘’]/g, "'")
    .replace(/[^a-z0-9&]+/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Strip a leading title echo from body.
 * Only removes a prefix match at the start (optional colon/dash/em-dash), never later mentions.
 */
export function stripRepeatedCardHeading(title, body) {
  const t = nz(title);
  const b = nz(body);
  if (!t || !b) return b;

  const normTitle = normalizeHeadingForEchoCompare(t);
  if (!normTitle || normTitle.length < 3) return b;

  // Direct prefix: "Title: rest" / "Title — rest" / "Title - rest" / "Title. rest"
  const escaped = t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const prefixRe = new RegExp(
    `^${escaped}\\s*[:\\-–—.|]\\s*`,
    "i",
  );
  if (prefixRe.test(b)) {
    const stripped = b.replace(prefixRe, "").trim();
    return stripped || b;
  }

  // Normalized prefix when punctuation/casing differs
  const normBody = normalizeHeadingForEchoCompare(b);
  if (normBody === normTitle) {
    // Body is only the title — clear to avoid empty-looking duplicate card
    return "";
  }
  if (normBody.startsWith(normTitle + " ")) {
    // Reconstruct by cutting the original body at the first separator after title-length span
    const sepMatch = b.match(/^[\s\S]{1,160}?[:\-–—.|]\s+/);
    if (sepMatch) {
      const head = sepMatch[0];
      const headNorm = normalizeHeadingForEchoCompare(head.replace(/[:\-–—.|]\s*$/, ""));
      if (headNorm === normTitle) {
        return b.slice(head.length).trim() || b;
      }
    }
  }

  return b;
}

export function bodyRepeatsCardHeading(title, body) {
  const stripped = stripRepeatedCardHeading(title, body);
  return nz(body).length > 0 && stripped !== nz(body);
}

export function applyNoRepeatedCardHeadingToBlocks(blocks) {
  const list = Array.isArray(blocks) ? blocks : [];
  const changed = [];
  const out = list.map((b) => {
    const title = nz(b.title);
    const body = nz(b.body);
    if (!title || !body) return b;
    const nextBody = stripRepeatedCardHeading(title, body);
    if (nextBody === body) return b;
    changed.push({
      slotKey: nz(b.slotKey || b.slot),
      from: body,
      to: nextBody,
      reason: REPEATED_HEADING_FAIL.TITLE_ECHO_PREFIX,
    });
    return {
      ...b,
      body: nextBody,
      __noRepeatedCardHeading: {
        action: "stripped_leading_title_echo",
        from: body,
        to: nextBody,
      },
    };
  });
  return {
    contract: BE_NO_REPEATED_CARD_HEADING_V1,
    blocks: out,
    changed,
    changedCount: changed.length,
  };
}

export function validateNoRepeatedCardHeading(blocks) {
  const failures = [];
  for (const b of blocks || []) {
    const title = nz(b.title);
    const body = nz(b.body);
    if (!title || !body) continue;
    if (bodyRepeatsCardHeading(title, body)) {
      failures.push({
        code: REPEATED_HEADING_FAIL.TITLE_ECHO_PREFIX,
        slotKey: nz(b.slotKey || b.slot),
        title,
        bodyPreview: body.slice(0, 120),
      });
    }
  }
  return {
    contract: BE_NO_REPEATED_CARD_HEADING_V1,
    ok: failures.length === 0,
    pass: failures.length === 0,
    failures,
    failureCount: failures.length,
  };
}

/** Rendering helper: body for cards that already show title separately. */
export function customerFacingCardBody(title, body) {
  const b = nz(body);
  if (!b) return "";
  return stripRepeatedCardHeading(title, b);
}
