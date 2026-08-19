/**
 * Webflow site footer: pass Memberstack JWT to Railway iframes (My Deals, etc.).
 * Load after Memberstack. Call DealalityEmbedParent.publishJwt(token) after you obtain a valid eyJ… token.
 */
(function (global) {
  'use strict';

  function isApiJwt(token) {
    if (!token || typeof token !== 'string') return false;
    var t = token.trim();
    if (!t || t.indexOf('eyJ') !== 0) return false;
    if (t.indexOf('mem_') === 0 || t.indexOf('mem_sb_') === 0) return false;
    return true;
  }

  function embedPageNeedsMsToken(pathname) {
    if (!pathname) return false;
    if (/\/app\/home\.html$/i.test(pathname)) return true;
    return /\/(my-deals|new-deal-setup|deal-summary|brand-development-dashboard|deal-room-owner|deal-room-brand|outreach-plan-wizard|brand-explorer|ai-visibility-brand)(\.html)?$/i.test(
      pathname
    );
  }

  function appendTokenToIframeSrc(frame, token) {
    if (!frame || !isApiJwt(token)) return;
    try {
      var src = frame.getAttribute('src') || frame.src || '';
      if (!src || src === 'about:blank') return;
      var url = new URL(src, global.location.href);
      var isRailway = src.indexOf('my-operators-backend') !== -1;
      var isLocalEmbed = embedPageNeedsMsToken(url.pathname);
      if (!isRailway && !isLocalEmbed) return;
      if (isRailway && !embedPageNeedsMsToken(url.pathname)) return;
      if (url.searchParams.get('msToken')) return;
      url.searchParams.set('msToken', token.trim());
      frame.src = url.toString();
    } catch (_) {}
  }

  function broadcastJwtToIframes(token) {
    if (!isApiJwt(token)) return;
    var payload = { type: 'dealality-memberstack-jwt', token: token.trim() };
    try {
      var frames = global.document ? global.document.querySelectorAll('iframe') : [];
      for (var i = 0; i < frames.length; i++) {
        try {
          appendTokenToIframeSrc(frames[i], token);
          if (frames[i].contentWindow) {
            frames[i].contentWindow.postMessage(payload, '*');
          }
        } catch (_) {}
      }
    } catch (_) {}
  }

  var broadcastIntervalId = null;

  function startBroadcastInterval() {
    if (broadcastIntervalId != null) return;
    var attempts = 0;
    broadcastIntervalId = global.setInterval(function () {
      attempts += 1;
      if (global.__dealalityMemberstackJwt) {
        broadcastJwtToIframes(global.__dealalityMemberstackJwt);
      }
      if (attempts >= 30) {
        global.clearInterval(broadcastIntervalId);
        broadcastIntervalId = null;
      }
    }, 500);
  }

  function publishJwt(token) {
    if (!isApiJwt(token)) return false;
    global.__dealalityMemberstackJwt = token.trim();
    broadcastJwtToIframes(global.__dealalityMemberstackJwt);
    startBroadcastInterval();
    return true;
  }

  function stripMsTokenFromUrl(url) {
    try {
      url.searchParams.delete('msToken');
      url.searchParams.delete('memberstackToken');
      var hash = url.hash || '';
      if (hash.indexOf('?') >= 0) {
        var base = hash.slice(0, hash.indexOf('?'));
        var hp = new URLSearchParams(hash.slice(hash.indexOf('?') + 1));
        hp.delete('msToken');
        hp.delete('memberstackToken');
        var qs = hp.toString();
        url.hash = base + (qs ? '?' + qs : '');
      }
    } catch (_) {}
    return url;
  }

  function clearJwt() {
    global.__dealalityMemberstackJwt = null;
    if (broadcastIntervalId != null) {
      global.clearInterval(broadcastIntervalId);
      broadcastIntervalId = null;
    }
    try {
      var frames = global.document ? global.document.querySelectorAll('iframe') : [];
      for (var i = 0; i < frames.length; i++) {
        try {
          var src = frames[i].getAttribute('src') || frames[i].src || '';
          if (!src || src === 'about:blank') continue;
          var url = new URL(src, global.location.href);
          stripMsTokenFromUrl(url);
          frames[i].src = url.toString();
        } catch (_) {}
      }
    } catch (_) {}
  }

  global.addEventListener('message', function (ev) {
    var d = ev && ev.data;
    if (!d || d.type !== 'dealality-request-memberstack-jwt') return;
    var jwt = global.__dealalityMemberstackJwt;
    if (!isApiJwt(jwt) || !ev.source || typeof ev.source.postMessage !== 'function') return;
    try {
      ev.source.postMessage({ type: 'dealality-memberstack-jwt', token: jwt }, ev.origin || '*');
    } catch (_) {}
  });

  global.DealalityEmbedParent = {
    publishJwt: publishJwt,
    broadcastJwtToIframes: broadcastJwtToIframes,
    clearJwt: clearJwt,
  };
})(typeof window !== 'undefined' ? window : globalThis);
