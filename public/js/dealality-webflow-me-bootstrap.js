/**
 * Webflow / Dealality: wait for Memberstack session, then GET /api/me.
 * Load AFTER Memberstack (v1 / DOM package) and set window.DEALALITY_API_BASE to Railway.
 *
 * Dispatches:
 *   dealality-me-ready  — detail: { ok, status, data, error }
 *   dealality-me-error  — detail: { status, error, message } (ok === false)
 */
(function (global) {
  'use strict';

  var DEFAULT_STAGING_API = 'https://my-operators-backend-staging.up.railway.app';

  function apiBase() {
    var b = (global.DEALALITY_API_BASE || global.DEALALITY_API_BASE_URL || '').trim();
    return (b || DEFAULT_STAGING_API).replace(/\/$/, '');
  }

  function dispatch(name, detail) {
    try {
      global.dispatchEvent(new CustomEvent(name, { detail: detail }));
    } catch (_) {}
  }

  async function bootstrap() {
    var auth = global.DealalityMemberstackAuth;
    if (!auth || typeof auth.waitForLoggedInMember !== 'function') {
      console.warn('[DealalityMe] Load /js/dealality-memberstack-auth.js before this script.');
      return;
    }

    var member = await auth.waitForLoggedInMember(25000);
    if (!member || !member.data) {
      console.warn('[DealalityMe] No Memberstack session — log in on the published site (not Webflow Designer preview).');
      dispatch('dealality-me-error', {
        ok: false,
        status: 0,
        error: 'memberstack_not_logged_in',
        message: 'No Memberstack session. Log in, then reload.',
      });
      return;
    }

    var headers = await auth.getAuthHeaders(null, { waitForLogin: false });
    if (headers.error) {
      dispatch('dealality-me-error', {
        ok: false,
        status: 0,
        error: 'no_jwt',
        message: headers.error,
      });
      return;
    }

    var url = apiBase() + '/api/me';
    var res;
    try {
      res = await fetch(url, { method: 'GET', headers: headers.headers, credentials: 'omit' });
    } catch (e) {
      dispatch('dealality-me-error', {
        ok: false,
        status: 0,
        error: 'network_error',
        message: e && e.message ? e.message : 'Could not reach API',
      });
      return;
    }

    var data = null;
    try {
      data = await res.json();
    } catch (_) {
      data = {};
    }

    if (!res.ok) {
      dispatch('dealality-me-error', {
        ok: false,
        status: res.status,
        error: (data && data.error) || 'api_me_failed',
        message: (data && data.message) || 'Account lookup failed',
        memberstackId: data && data.memberstackId,
        email: data && data.email,
      });
      dispatch('dealality-me-ready', { ok: false, status: res.status, data: data, error: (data && data.error) || 'api_me_failed' });
      return;
    }

    global.__dealalityUserContext = data;
    if (global.DealalityWebflowUserChrome && typeof global.DealalityWebflowUserChrome.apply === 'function') {
      global.DealalityWebflowUserChrome.apply(data);
    }
    if (global.DealalityWebflowAccountNotice && typeof global.DealalityWebflowAccountNotice.apply === 'function') {
      global.DealalityWebflowAccountNotice.apply(data);
    }
    dispatch('dealality-me-ready', { ok: true, status: res.status, data: data, error: null });
  }

  if (global.document && global.document.readyState === 'loading') {
    global.document.addEventListener('DOMContentLoaded', function () {
      bootstrap();
    });
  } else {
    bootstrap();
  }
})(typeof window !== 'undefined' ? window : globalThis);
