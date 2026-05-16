/**
 * Browser-side Memberstack JWT for protected Dealality API calls.
 * Pattern aligned with public/js/brand-explorer-favorites.js — never use mem_sb_ / mem_ as Bearer.
 */
(function (global) {
  'use strict';

  var LOGIN_MSG = 'Please log in again to continue.';

  function isValidBearerToken(token) {
    if (!token || typeof token !== 'string') return false;
    var t = token.trim();
    if (!t) return false;
    if (t.indexOf('mem_') === 0 || t.indexOf('mem_sb_') === 0) return false;
    return true;
  }

  function getMemberstackDom() {
    return global.$memberstackDom || global.memberstack || null;
  }

  function memberHasSession(member) {
    return !!(member && member.data);
  }

  function unwrapTokenValue(raw) {
    if (raw == null) return null;
    if (typeof raw === 'string') {
      return isValidBearerToken(raw) ? raw.trim() : null;
    }
    if (typeof raw === 'object') {
      if (typeof raw.token === 'string' && isValidBearerToken(raw.token)) return raw.token.trim();
      if (typeof raw.accessToken === 'string' && isValidBearerToken(raw.accessToken)) {
        return raw.accessToken.trim();
      }
      if (raw.data != null) return unwrapTokenValue(raw.data);
    }
    return null;
  }

  function extractTokenFromMember(member) {
    if (!memberHasSession(member)) return null;
    var candidates = [
      member.data.tokens && member.data.tokens.accessToken,
      member.data.tokens && member.data.tokens.access_token,
      member.data.token,
      member.data.accessToken,
      member.token,
    ];
    for (var i = 0; i < candidates.length; i++) {
      var t = unwrapTokenValue(candidates[i]);
      if (t) return t;
    }
    return null;
  }

  async function tokenFromMemberstackDom(ms) {
    if (!ms) return null;
    if (typeof ms.getToken === 'function') {
      var tok = unwrapTokenValue(await ms.getToken());
      if (tok) return tok;
    }
    if (typeof ms.getMemberCookie === 'function') {
      var cookie = unwrapTokenValue(await ms.getMemberCookie());
      if (cookie) return cookie;
    }
    if (typeof ms.getCurrentMember === 'function') {
      var member = await ms.getCurrentMember();
      var fromMember = extractTokenFromMember(member);
      if (fromMember) return fromMember;
    }
    return null;
  }

  function sleep(ms) {
    return new Promise(function (resolve) {
      setTimeout(resolve, ms);
    });
  }

  /**
   * Wait until $memberstackDom / memberstack is on window (Webflow loads scripts async).
   * @param {number} [maxMs=20000]
   * @returns {Promise<object|null>}
   */
  async function waitForMemberstackDom(maxMs) {
    var limit = maxMs == null ? 20000 : maxMs;
    var start = Date.now();
    while (Date.now() - start < limit) {
      var ms = getMemberstackDom();
      if (ms && (typeof ms.getCurrentMember === 'function' || typeof ms.getToken === 'function')) {
        return ms;
      }
      await sleep(150);
    }
    return null;
  }

  /**
   * Wait until getCurrentMember() returns data (logged-in session).
   * @param {number} [maxMs=20000]
   * @returns {Promise<object|null>} member payload from getCurrentMember
   */
  async function waitForLoggedInMember(maxMs) {
    var ms = await waitForMemberstackDom(maxMs);
    if (!ms || typeof ms.getCurrentMember !== 'function') return null;
    var limit = maxMs == null ? 20000 : maxMs;
    var start = Date.now();
    while (Date.now() - start < limit) {
      try {
        var member = await ms.getCurrentMember();
        if (memberHasSession(member)) return member;
      } catch (_) {}
      await sleep(200);
    }
    return null;
  }

  /**
   * @returns {Promise<string|null>}
   */
  async function getMemberstackJwt() {
    try {
      return await tokenFromMemberstackDom(getMemberstackDom());
    } catch (_) {}
    return null;
  }

  /**
   * Same as getMemberstackJwt but waits for Webflow/Memberstack init after redirect.
   * @param {number} [maxMs=20000]
   * @returns {Promise<string|null>}
   */
  async function getMemberstackJwtWhenReady(maxMs) {
    var member = await waitForLoggedInMember(maxMs);
    if (member) {
      var t = extractTokenFromMember(member);
      if (t) return t;
    }
    return getMemberstackJwt();
  }

  /**
   * @param {Record<string, string>} [extra]
   * @returns {Promise<{ headers: Record<string, string> }|{ error: string }>}
   */
  async function getAuthHeaders(extra, options) {
    var opts = options || {};
    var jwt = opts.waitForLogin
      ? await getMemberstackJwtWhenReady(opts.maxWaitMs)
      : await getMemberstackJwt();
    if (!jwt) return { error: LOGIN_MSG };
    var headers = Object.assign(
      { Authorization: 'Bearer ' + jwt, Accept: 'application/json' },
      extra || {}
    );
    return { headers: headers };
  }

  /**
   * @param {string} message
   */
  function notifyLoginRequired(message) {
    var msg = message || LOGIN_MSG;
    if (typeof global.alert === 'function') {
      try {
        global.alert(msg);
      } catch (_) {}
    }
  }

  /**
   * @param {string} url
   * @param {RequestInit} [options]
   * @returns {Promise<Response>}
   */
  async function authFetch(url, options) {
    var auth = await getAuthHeaders();
    if (auth.error) {
      notifyLoginRequired(auth.error);
      throw new Error(auth.error);
    }
    options = options || {};
    var merged = Object.assign({}, auth.headers, options.headers || {});
    return fetch(url, Object.assign({}, options, { headers: merged }));
  }

  /**
   * GET /api/my-deals (list only).
   * @param {string} url
   * @returns {Promise<Response>}
   */
  async function fetchMyDealsList(url) {
    return authFetch(url, { method: 'GET' });
  }

  global.DealalityMemberstackAuth = {
    LOGIN_MSG: LOGIN_MSG,
    getMemberstackDom: getMemberstackDom,
    waitForMemberstackDom: waitForMemberstackDom,
    waitForLoggedInMember: waitForLoggedInMember,
    getMemberstackJwt: getMemberstackJwt,
    getMemberstackJwtWhenReady: getMemberstackJwtWhenReady,
    getAuthHeaders: getAuthHeaders,
    authFetch: authFetch,
    fetchMyDealsList: fetchMyDealsList,
    notifyLoginRequired: notifyLoginRequired,
  };
})(typeof window !== 'undefined' ? window : globalThis);
