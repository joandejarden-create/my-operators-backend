/**
 * DcAuthHelper — thin sync auth header helper for pages that expect window.DcAuthHelper.
 *
 * Recovered for pre-crash dangling reference on operator-fit-alignment.html.
 * Never existed as a tracked file in Oct 1 backups; behavior mirrors the sync JWT
 * publish pattern used by dealality-app-shell-auth.js / dealality-memberstack-auth.js
 * (Authorization: Bearer <Memberstack JWT> when already available).
 *
 * Prefer DealalityMemberstackAuth.getAuthHeaders() for new async call sites.
 */
(function (global) {
  "use strict";

  function isValidBearerToken(token) {
    if (!token || typeof token !== "string") return false;
    var t = token.trim();
    if (t.indexOf("mem_") === 0) return false;
    return t.indexOf("eyJ") === 0 && t.split(".").length >= 3;
  }

  function readPublishedJwt() {
    try {
      if (isValidBearerToken(global.__dealalityMemberstackJwt)) {
        return String(global.__dealalityMemberstackJwt).trim();
      }
    } catch (_) {}
    try {
      var q = new URLSearchParams(global.location.search);
      var fromQuery = q.get("msToken") || q.get("token");
      if (isValidBearerToken(fromQuery)) return fromQuery.trim();
    } catch (_) {}
    try {
      var ls = global.localStorage && global.localStorage.getItem("dealality_ms_jwt");
      if (isValidBearerToken(ls)) return ls.trim();
    } catch (_) {}
    return null;
  }

  function authHeaders(extra) {
    var headers = Object.assign({}, extra || {});
    var jwt = readPublishedJwt();
    if (jwt) headers.Authorization = "Bearer " + jwt;
    return headers;
  }

  global.DcAuthHelper = {
    authHeaders: authHeaders,
    getJwt: readPublishedJwt,
  };
})(typeof window !== "undefined" ? window : globalThis);
