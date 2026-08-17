/**
 * Source availability classification for Research Engine V2.
 * Source failures must NEVER become material change proposals.
 */

export const SOURCE_STATES = Object.freeze([
  "Available",
  "Blocked",
  "Failed",
  "Empty",
  "Not Applicable",
]);

/**
 * @param {{ status?: number, ok?: boolean, error?: string|null, text?: string, hotelFound?: boolean, notes?: string }} input
 */
export function classifySourceState(input = {}) {
  const status = Number(input.status || 0);
  const err = String(input.error || input.notes || "").toLowerCase();
  const text = String(input.text || "");

  if (input.notApplicable) return { state: "Not Applicable", reason: input.reason || "not_applicable" };

  if (status === 403 || status === 429 || /bot|blocked|captcha|cloudflare|access denied/.test(err)) {
    return { state: "Blocked", reason: status ? `http_${status}` : "anti_bot_or_block", httpStatus: status || null };
  }
  if (status === 404 || status === 410) {
    return { state: "Empty", reason: `http_${status}`, httpStatus: status };
  }
  if (status >= 500 || /timeout|abort|network|econn|enotfound|fetch failed|graphql_error/.test(err)) {
    return { state: "Failed", reason: status ? `http_${status}` : "network_or_timeout", httpStatus: status || null };
  }
  if (input.ok === false && status > 0) {
    return { state: "Failed", reason: `http_${status}`, httpStatus: status };
  }
  if (input.ok === true || input.hotelFound === true) {
    if (!text && input.requireBody) {
      return { state: "Empty", reason: "empty_body", httpStatus: status || 200 };
    }
    return { state: "Available", reason: "ok", httpStatus: status || 200 };
  }

  // Directory / match miss with explanatory notes is Empty — not Failed
  if (
    /no .*match|below medium|fetch skipped|missing_ctyhocn|not found in directory|no usable/i.test(err) ||
    input.hotelFound === false
  ) {
    return {
      state: "Empty",
      reason: err ? "not_found_or_unmatched" : "no_usable_observation",
      detail: err ? err.slice(0, 200) : undefined,
    };
  }

  if (err) {
    return { state: "Failed", reason: "error", detail: err.slice(0, 200) };
  }
  return { state: "Empty", reason: "no_usable_observation" };
}

/**
 * True when observation must not generate material change proposals.
 * @param {string} state
 */
export function isSourceUnsafeForProposals(state) {
  return state === "Blocked" || state === "Failed";
}

/**
 * Attach sourceState onto an adapter observation (mutates copy).
 * @param {object} observation
 * @param {{ status?: number, ok?: boolean, error?: string }} [hint]
 */
export function withSourceState(observation, hint = {}) {
  const raw = observation?.rawSignals || {};
  const classified = classifySourceState({
    status: hint.status ?? raw.httpStatus,
    ok: hint.ok,
    error: hint.error || observation?.notes,
    hotelFound: observation?.hotelFound,
    text: hint.text,
    notes: observation?.notes,
  });
  return {
    ...observation,
    sourceState: classified.state,
    sourceStateReason: classified.reason,
    rawSignals: {
      ...raw,
      sourceState: classified,
      httpStatus: classified.httpStatus ?? raw.httpStatus ?? null,
    },
  };
}
