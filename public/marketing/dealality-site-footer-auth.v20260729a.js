
(function (global) {
  'use strict';

  $('.page-loader').addClass('active');

  function isApiJwt(token) {
    if (!token || typeof token !== 'string') return false;
    var t = token.trim();
    return t.indexOf('eyJ') === 0 && t.indexOf('mem_') !== 0 && t.indexOf('mem_sb_') !== 0;
  }
  var broadcastIntervalId = null;
  function appendTokenToIframeSrc(frame, token) {
    if (!frame || !isApiJwt(token)) return;
    try {
      var src = frame.getAttribute('src') || frame.src || '';
      if (src.indexOf('my-operators-backend') === -1 || src.indexOf('my-deals') === -1) return;
      var url = new URL(src, global.location.href);
      if (url.searchParams.get('msToken')) return;
      url.searchParams.set('msToken', token.trim());
      frame.src = url.toString();
    } catch (_) {}
  }
  function broadcastJwtToIframes(token) {
    if (!isApiJwt(token)) return;
    var payload = { type: 'dealality-memberstack-jwt', token: token.trim() };
    try {
      var frames = global.document.querySelectorAll('iframe');
      for (var i = 0; i < frames.length; i++) {
        try {
          appendTokenToIframeSrc(frames[i], token);
          if (frames[i].contentWindow) frames[i].contentWindow.postMessage(payload, '*');
        } catch (_) {}
      }
    } catch (_) {}
  }
  function startBroadcastInterval() {
    if (broadcastIntervalId != null) return;
    var attempts = 0;
    broadcastIntervalId = global.setInterval(function () {
      attempts += 1;
      if (global.__dealalityMemberstackJwt) broadcastJwtToIframes(global.__dealalityMemberstackJwt);
      if (attempts >= 40) { global.clearInterval(broadcastIntervalId); broadcastIntervalId = null; }
    }, 500);
  }
  function publishJwt(token) {
    if (!isApiJwt(token)) return false;
    global.__dealalityMemberstackJwt = token.trim();
    broadcastJwtToIframes(global.__dealalityMemberstackJwt);
    startBroadcastInterval();
    return true;
  }
  global.addEventListener('message', function (ev) {
    var d = ev && ev.data;
    if (!d || d.type !== 'dealality-request-memberstack-jwt') return;
    var jwt = global.__dealalityMemberstackJwt;
    if (!isApiJwt(jwt) || !ev.source || typeof ev.source.postMessage !== 'function') return;
    try { ev.source.postMessage({ type: 'dealality-memberstack-jwt', token: jwt }, ev.origin || '*'); } catch (_) {}
  });
  global.DealalityEmbedParent = { publishJwt: publishJwt, broadcastJwtToIframes: broadcastJwtToIframes };
})(window);

;

  const DEALALITY_API = (window.DEALALITY_API_BASE || 'https://my-operators-backend-staging.up.railway.app').replace(/\/$/, '');

  $(document).ready(function() {
    let readOnlyElem = $('#user-role').text() == "Company Admin" ? $('.read-only:not(".dynamic-read-only")') : $('.read-only');
    setInterval(function() {
      for (let e = 0; e < readOnlyElem.length; e++) $(readOnlyElem[e]).attr('readonly', true);
    }, 2000);
  });

  function isApiJwt(token) {
    if (!token || typeof token !== 'string') return false;
    const t = token.trim();
    return t.indexOf('eyJ') === 0 && t.indexOf('mem_') !== 0 && t.indexOf('mem_sb_') !== 0;
  }

  function deepFindJwt(value, depth) {
    if (depth > 6 || value == null) return null;
    if (typeof value === 'string') {
      const s = value.trim();
      if (isApiJwt(s)) return s;
      try { return deepFindJwt(JSON.parse(s), depth + 1); } catch (_) {}
      return null;
    }
    if (Array.isArray(value)) {
      for (let i = 0; i < value.length; i++) {
        const f = deepFindJwt(value[i], depth + 1);
        if (f) return f;
      }
      return null;
    }
    if (typeof value === 'object') {
      for (const k of Object.keys(value)) {
        const f = deepFindJwt(value[k], depth + 1);
        if (f) return f;
      }
    }
    return null;
  }

  function jwtFromBrowserStorage() {
    try {
      if (document.cookie) {
        for (const part of document.cookie.split(';')) {
          const eq = part.trim().indexOf('=');
          let val = eq >= 0 ? part.trim().slice(eq + 1) : part.trim();
          try { val = decodeURIComponent(val); } catch (_) {}
          const f = deepFindJwt(val, 0);
          if (f) return f;
        }
      }
    } catch (_) {}
    try {
      for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i);
        if (!key || !/member|memberstack|ms[-_]/i.test(key)) continue;
        const f = deepFindJwt(localStorage.getItem(key), 0);
        if (f) return f;
      }
    } catch (_) {}
    return null;
  }

  async function getMemberstackBearerToken(member) {
    const fromMember =
      member?.data?.tokens?.accessToken ||
      member?.data?.tokens?.access_token ||
      member?.data?.auth?.jwt ||
      member?.data?.auth?.token;
    if (isApiJwt(fromMember)) return fromMember.trim();
    const deepMember = deepFindJwt(member, 0);
    if (deepMember) return deepMember;
    const fromStorage = jwtFromBrowserStorage();
    if (fromStorage) return fromStorage;
    const ms = window.$memberstackDom;
    if (ms && typeof ms.getToken === 'function') {
      try {
        const raw = await ms.getToken();
        const tok = typeof raw === 'string' ? raw : (raw && (raw.token || raw.accessToken));
        if (isApiJwt(tok)) return tok.trim();
      } catch (_) {}
    }
    return null;
  }

  function publishJwtToEmbeds(token) {
    if (token && window.DealalityEmbedParent) window.DealalityEmbedParent.publishJwt(token);
  }

  async function loadUserContext() {
    try {
      const ms = window.$memberstackDom;
      if (!ms || typeof ms.getCurrentMember !== 'function') {
        console.info('Memberstack not ready yet');
        return;
      }
      const member = await ms.getCurrentMember();
      if (!member || !member.data) {
        console.info('No Member has logged in');
        return;
      }

      const token = await getMemberstackBearerToken(member);
      if (!token) {
        console.error('No eyJ session token — cannot auth My Deals iframe. Check member.data.tokens in console.');
        return;
      }

      publishJwtToEmbeds(token);

      const response = await fetch(DEALALITY_API + '/api/me', {
        method: 'GET',
        headers: { Authorization: 'Bearer ' + token, 'Content-Type': 'application/json' }
      });

      const data = await response.json();
      console.info('HTTP Status:', response.status);

      if (response.status === 200) {
        $('.page-loader').remove();
        $('body').removeClass('overflow-hidden');
        publishJwtToEmbeds(token);
      } else if (response.status === 401) {
        $.toast({ heading: 'Information', text: 'Unauthorized / Invalid Token', icon: 'info', position: 'top-right', hideAfter: 5000 });
      } else if (response.status === 404) {
        $.toast({ heading: 'Error', text: "Your account isn't set up in our system yet", icon: 'error', position: 'top-right', hideAfter: 8000 });
        $('.page-loader h3').text("Account isn't set up yet");
        setTimeout(forceLogout, 5000);
      } else if (response.status >= 500) {
        $.toast({ heading: 'Error', text: 'Something went wrong, please try again later', icon: 'error', position: 'top-right', hideAfter: 8000 });
        $('.page-loader h3').text('Something went wrong');
        setTimeout(forceLogout, 5000);
      } else {
        $.toast({ heading: 'Error', text: 'Unexpected error', icon: 'error', position: 'top-right', hideAfter: 8000 });
        $('.page-loader h3').text('Unexpected error');
        setTimeout(forceLogout, 3000);
      }

      if (!response.ok) {
        console.error('API Error:', data);
        return;
      }

      console.log('User Context:', data);
      window.__dealalityUserContext = data;
      window.__dealalityMemberstackJwt = token;

      if (window.DealalityWebflowUserChrome && data) {
        window.DealalityWebflowUserChrome.apply(data);
      }

      if (window.DealalityWebflowAccountNotice && data) {
        window.DealalityWebflowAccountNotice.apply(data);
      }

      const d = data?.dealality;
      const access = data?.accountAccess;
      const isOwnerOrAdmin = !!(d && (d.isOwner || d.isAdmin));
      const isPendingApproval = !!(access && access.pendingApproval);
      const allowedBrands = data?.permissions?.allowedBrandNames || [];

      if (!isOwnerOrAdmin && !isPendingApproval && !window.__dealalitySuppressBrandToast && !allowedBrands.length) {
        $.toast({ heading: 'Information', text: 'No brands assigned', icon: 'info', position: 'top-right', hideAfter: 8000 });
      }

      if (isOwnerOrAdmin) {
        const warn = document.querySelector('.dc-header__warning');
        if (warn) warn.hidden = true;
      }
    } catch (error) {
      console.error('Request failed:', error);
    }
  }

  window.addEventListener('DOMContentLoaded', loadUserContext);

  async function forceLogout() {
    try {
      await window.$memberstackDom.logout();
      window.__dealalityMemberstackJwt = null;
      window.location.href = '/login';
    } catch (error) {
      console.error('Logout failed:', error);
    }
  }
