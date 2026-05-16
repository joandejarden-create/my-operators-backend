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

  function getMemberstackClients() {
    var list = [];
    var seen = new Set();
    function add(ms) {
      if (!ms || typeof ms !== 'object' || seen.has(ms)) return;
      seen.add(ms);
      list.push(ms);
    }
    add(global.$memberstackDom);
    add(global.memberstack);
    add(global.memberstackDom);
    if (global.MemberStack && typeof global.MemberStack === 'object') add(global.MemberStack);
    if (global.memberStack && typeof global.memberStack === 'object') add(global.memberStack);
    return list;
  }

  function getMemberstackDom() {
    var clients = getMemberstackClients();
    return clients.length ? clients[0] : null;
  }

  function deepFindJwt(value, depth) {
    if (depth > 6 || value == null) return null;
    if (typeof value === 'string') {
      var s = value.trim();
      if (s.indexOf('eyJ') === 0 && isValidBearerToken(s)) return s;
      try {
        var parsed = JSON.parse(s);
        return deepFindJwt(parsed, depth + 1);
      } catch (_) {}
      return null;
    }
    if (Array.isArray(value)) {
      for (var i = 0; i < value.length; i++) {
        var found = deepFindJwt(value[i], depth + 1);
        if (found) return found;
      }
      return null;
    }
    if (typeof value === 'object') {
      var keys = Object.keys(value);
      for (var k = 0; k < keys.length; k++) {
        var f = deepFindJwt(value[keys[k]], depth + 1);
        if (f) return f;
      }
    }
    return null;
  }

  function jwtFromBrowserStorage() {
    try {
      if (global.document && global.document.cookie) {
        var chunks = global.document.cookie.split(';');
        for (var c = 0; c < chunks.length; c++) {
          var part = chunks[c].trim();
          var eq = part.indexOf('=');
          var val = eq >= 0 ? part.slice(eq + 1) : part;
          try {
            val = decodeURIComponent(val);
          } catch (_) {}
          var fromCookie = deepFindJwt(val, 0);
          if (fromCookie) return fromCookie;
        }
      }
    } catch (_) {}
    try {
      if (global.localStorage) {
        for (var i = 0; i < global.localStorage.length; i++) {
          var key = global.localStorage.key(i);
          if (!key || !/member|memberstack|ms[-_]/i.test(key)) continue;
          var raw = global.localStorage.getItem(key);
          var fromLs = deepFindJwt(raw, 0);
          if (fromLs) return fromLs;
        }
      }
    } catch (_) {}
    try {
      if (global.sessionStorage) {
        for (var j = 0; j < global.sessionStorage.length; j++) {
          var skey = global.sessionStorage.key(j);
          if (!skey || !/member|memberstack|ms[-_]/i.test(skey)) continue;
          var sraw = global.sessionStorage.getItem(skey);
          var fromSs = deepFindJwt(sraw, 0);
          if (fromSs) return fromSs;
        }
      }
    } catch (_) {}
    return null;
  }

  function callMaybePromise(fn) {
    try {
      var out = fn();
      if (out && typeof out.then === 'function') return out;
      return Promise.resolve(out);
    } catch (e) {
      return Promise.reject(e);
    }
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
    var methodNames = [
      'getToken',
      'getMemberCookie',
      'getIdToken',
      'getAuthToken',
      'getAccessToken',
    ];
    for (var m = 0; m < methodNames.length; m++) {
      var name = methodNames[m];
      if (typeof ms[name] !== 'function') continue;
      try {
        var raw = await callMaybePromise(function () {
          return ms[name]();
        });
        var tok = unwrapTokenValue(raw);
        if (tok) return tok;
      } catch (_) {}
    }
    if (typeof ms.getCurrentMember === 'function') {
      try {
        var member = await ms.getCurrentMember();
        var fromMember = extractTokenFromMember(member);
        if (fromMember) return fromMember;
        var deep = deepFindJwt(member, 0);
        if (deep) return deep;
      } catch (_) {}
    }
    return null;
  }

  async function getMemberstackJwtFromAnyClient() {
    var clients = getMemberstackClients();
    for (var i = 0; i < clients.length; i++) {
      var t = await tokenFromMemberstackDom(clients[i]);
      if (t) return t;
    }
    return jwtFromBrowserStorage();
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
      if (
        ms &&
        (typeof ms.getCurrentMember === 'function' ||
          typeof ms.getToken === 'function' ||
          typeof ms.getMemberCookie === 'function')
      ) {
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
      return await getMemberstackJwtFromAnyClient();
    } catch (_) {}
    return null;
  }

  /**
   * Debug helper (safe): which Memberstack APIs exist; whether a JWT was found (not the token itself).
   */
  async function inspectMemberstackAuth() {
    var clients = getMemberstackClients();
    var apis = [];
    clients.forEach(function (ms, idx) {
      var names = [];
      [
        'getToken',
        'getMemberCookie',
        'getCurrentMember',
        'getIdToken',
        'openModal',
      ].forEach(function (n) {
        if (typeof ms[n] === 'function') names.push(n);
      });
      apis.push({ clientIndex: idx, methods: names });
    });
    var jwt = await getMemberstackJwtFromAnyClient();
    return {
      clientCount: clients.length,
      apis: apis,
      hasJwt: !!jwt,
      jwtPreview: jwt ? jwt.slice(0, 12) + '…' : null,
      storageJwt: !!jwtFromBrowserStorage(),
    };
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
    getMemberstackClients: getMemberstackClients,
    waitForMemberstackDom: waitForMemberstackDom,
    waitForLoggedInMember: waitForLoggedInMember,
    getMemberstackJwt: getMemberstackJwt,
    getMemberstackJwtWhenReady: getMemberstackJwtWhenReady,
    getAuthHeaders: getAuthHeaders,
    authFetch: authFetch,
    fetchMyDealsList: fetchMyDealsList,
    notifyLoginRequired: notifyLoginRequired,
    inspectMemberstackAuth: inspectMemberstackAuth,
  };
})(typeof window !== 'undefined' ? window : globalThis);
