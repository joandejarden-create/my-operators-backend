/**
 * Brand Explorer combined — per-user favorites (Airtable via API; localStorage fallback).
 */
(function () {
  'use strict';

  var STORAGE_KEY = 'dealCapture.brandExplorer.favorites';
  var USER_ID_CACHE_KEY = 'dealCapture.brandExplorer.userId';
  var API_BASE = window.DEALALITY_API_BASE || '';

  var STAR_SVG =
    '<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">' +
    '<path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/>' +
    '</svg>';

  var brandIds = [];
  var recordByBrandId = {};
  var currentUserId = null;
  var serverEnabled = false;
  var readyPromise = null;

  function apiUrl(path) {
    return API_BASE + path;
  }

  function loadLocal() {
    try {
      var raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return [];
      var parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return [];
      return parsed
        .map(function (id) {
          return String(id || '').trim();
        })
        .filter(function (id) {
          return id.indexOf('rec') === 0;
        });
    } catch (e) {
      return [];
    }
  }

  function saveLocal(ids) {
    var unique = [];
    (ids || []).forEach(function (id) {
      id = String(id || '').trim();
      if (id && unique.indexOf(id) === -1) unique.push(id);
    });
    localStorage.setItem(STORAGE_KEY, JSON.stringify(unique));
  }

  function applyIds(ids, records) {
    brandIds = (ids || []).slice();
    recordByBrandId = {};
    (records || []).forEach(function (r) {
      if (r && r.brandId && r.id) recordByBrandId[r.brandId] = r.id;
    });
    brandIds.forEach(function (id) {
      if (!recordByBrandId[id]) recordByBrandId[id] = null;
    });
  }

  function getSyncUserId() {
    var el = document.getElementById('airtable-user-id');
    if (el && el.textContent) {
      var t = el.textContent.trim();
      if (t.indexOf('rec') === 0) return t;
    }
    try {
      var urlParams = new URLSearchParams(window.location.search);
      var p = urlParams.get('userId');
      if (p && p.indexOf('rec') === 0) return p;
    } catch (_) {}
    try {
      var cached = sessionStorage.getItem(USER_ID_CACHE_KEY);
      if (cached && cached.indexOf('rec') === 0) return cached;
    } catch (_) {}
    return null;
  }

  async function getMemberstackBearer() {
    try {
      var ms = window.$memberstackDom || window.memberstack;
      if (!ms) return null;
      if (typeof ms.getCurrentMember === 'function') {
        var member = await ms.getCurrentMember();
        var token =
          (member && member.data && (member.data.tokens && member.data.tokens.accessToken)) ||
          (member && member.data && member.data.token) ||
          (member && member.token);
        if (token) return token;
      }
      if (typeof ms.getToken === 'function') {
        var tok = await ms.getToken();
        if (tok) return tok;
      }
    } catch (_) {}
    return null;
  }

  async function resolveUserId() {
    var sync = getSyncUserId();
    if (sync) {
      currentUserId = sync;
      try {
        sessionStorage.setItem(USER_ID_CACHE_KEY, sync);
      } catch (_) {}
      return sync;
    }

    var bearer = await getMemberstackBearer();
    if (!bearer) return null;

    try {
      var res = await fetch(apiUrl('/api/me'), {
        headers: { Authorization: 'Bearer ' + bearer },
      });
      if (!res.ok) return null;
      var data = await res.json();
      var recId = data && data.airtable && data.airtable.userRecordId;
      if (recId && String(recId).indexOf('rec') === 0) {
        currentUserId = recId;
        try {
          sessionStorage.setItem(USER_ID_CACHE_KEY, recId);
        } catch (_) {}
        return recId;
      }
    } catch (e) {
      console.warn('[BrandExplorerFavorites] /api/me failed:', e);
    }
    return null;
  }

  async function loadFromServer() {
    currentUserId = await resolveUserId();
    if (!currentUserId) {
      serverEnabled = false;
      applyIds(loadLocal(), []);
      return { server: false, userId: null };
    }

    try {
      var res = await fetch(
        apiUrl('/api/brand-explorer/favorites?userId=' + encodeURIComponent(currentUserId))
      );
      if (res.status === 503) {
        serverEnabled = false;
        applyIds(loadLocal(), []);
        return { server: false, userId: currentUserId };
      }
      if (!res.ok) throw new Error('HTTP ' + res.status);
      var data = await res.json();
      serverEnabled = true;
      var favorites = data.favorites || [];
      var ids = favorites.map(function (f) {
        return f.brandId;
      });
      applyIds(ids, favorites);
      await migrateLocalToServer();
      return { server: true, userId: currentUserId };
    } catch (e) {
      console.warn('[BrandExplorerFavorites] server load failed, using localStorage:', e);
      serverEnabled = false;
      applyIds(loadLocal(), []);
      return { server: false, userId: currentUserId };
    }
  }

  async function migrateLocalToServer() {
    if (!serverEnabled || !currentUserId) return;
    var local = loadLocal();
    if (!local.length) return;
    var toUpload = local.filter(function (id) {
      return brandIds.indexOf(id) === -1;
    });
    for (var i = 0; i < toUpload.length; i++) {
      try {
        await fetch(apiUrl('/api/brand-explorer/favorites'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ userId: currentUserId, brandId: toUpload[i] }),
        });
        brandIds.push(toUpload[i]);
      } catch (_) {}
    }
    try {
      localStorage.removeItem(STORAGE_KEY);
    } catch (_) {}
  }

  function notifyChanged() {
    try {
      window.dispatchEvent(new CustomEvent('brand-explorer-favorites-changed'));
    } catch (_) {}
  }

  function load() {
    return brandIds.slice();
  }

  function save(ids) {
    if (serverEnabled) return;
    saveLocal(ids);
    notifyChanged();
  }

  function isFavorite(brandRecordId) {
    var id = String(brandRecordId || '').trim();
    if (!id) return false;
    return brandIds.indexOf(id) !== -1;
  }

  async function toggle(brandRecordId) {
    var id = String(brandRecordId || '').trim();
    if (!id) return false;

    if (serverEnabled && currentUserId) {
      if (isFavorite(id)) {
        var favRec = recordByBrandId[id];
        var delUrl = favRec
          ? apiUrl('/api/brand-explorer/favorites/' + encodeURIComponent(favRec)) +
            '?userId=' +
            encodeURIComponent(currentUserId)
          : apiUrl('/api/brand-explorer/favorites') +
            '?userId=' +
            encodeURIComponent(currentUserId) +
            '&brandId=' +
            encodeURIComponent(id);
        var delRes = await fetch(delUrl, { method: 'DELETE' });
        if (!delRes.ok) throw new Error('Failed to remove favorite');
        brandIds = brandIds.filter(function (x) {
          return x !== id;
        });
        delete recordByBrandId[id];
        notifyChanged();
        return false;
      }
      var postRes = await fetch(apiUrl('/api/brand-explorer/favorites'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId: currentUserId, brandId: id }),
      });
      if (!postRes.ok) throw new Error('Failed to save favorite');
      var postData = await postRes.json();
      if (brandIds.indexOf(id) === -1) brandIds.push(id);
      if (postData.favorite && postData.favorite.id) {
        recordByBrandId[id] = postData.favorite.id;
      }
      notifyChanged();
      return true;
    }

    var ids = loadLocal();
    var idx = ids.indexOf(id);
    if (idx === -1) {
      ids.push(id);
      saveLocal(ids);
      brandIds = ids.slice();
      notifyChanged();
      return true;
    }
    ids.splice(idx, 1);
    saveLocal(ids);
    brandIds = ids.slice();
    notifyChanged();
    return false;
  }

  function updateButton(btn, saved) {
    if (!btn) return;
    btn.setAttribute('aria-pressed', saved ? 'true' : 'false');
    btn.textContent = saved ? 'Saved' : 'Save';
    btn.classList.toggle('be-brand-save-btn--saved', !!saved);
    btn.title = saved ? 'Remove from favorites' : 'Save to favorites';
  }

  function updateCardStar(btn, saved) {
    if (!btn) return;
    btn.classList.toggle('favorited', !!saved);
    btn.setAttribute('aria-pressed', saved ? 'true' : 'false');
    btn.setAttribute('aria-label', saved ? 'Remove from favorites' : 'Add to favorites');
    btn.title = saved ? 'Remove from favorites' : 'Save to favorites';
  }

  function syncFavoriteUi(brandId, saved) {
    var id = String(brandId || '').trim();
    if (!id) return;
    document.querySelectorAll('.be-brand-save-btn[data-be-brand-id="' + id + '"]').forEach(function (btn) {
      updateButton(btn, saved);
    });
    document.querySelectorAll('.favorite-star[data-be-brand-id="' + id + '"]').forEach(function (btn) {
      updateCardStar(btn, saved);
    });
  }

  function onFavoriteToggleClick(e, brandId, btn) {
    e.preventDefault();
    e.stopPropagation();
    if (btn) btn.disabled = true;
    toggle(brandId)
      .then(function (nowSaved) {
        syncFavoriteUi(brandId, nowSaved);
      })
      .catch(function (err) {
        console.error('[BrandExplorerFavorites] toggle failed:', err);
        alert('Could not update favorites. Please try again or sign in.');
      })
      .finally(function () {
        if (btn) btn.disabled = false;
      });
  }

  function wireSaveButtons(root) {
    var scope = root && root.querySelectorAll ? root : document;
    var buttons = scope.querySelectorAll('.be-brand-save-btn[data-be-brand-id]');
    for (var i = 0; i < buttons.length; i++) {
      (function (btn) {
        var id = btn.getAttribute('data-be-brand-id');
        if (!id) return;
        updateButton(btn, isFavorite(id));
        if (btn._beSaveWired) return;
        btn._beSaveWired = true;
        btn.addEventListener('click', function (e) {
          onFavoriteToggleClick(e, id, btn);
        });
      })(buttons[i]);
    }
  }

  function wireCardStars(root) {
    var scope = root && root.querySelectorAll ? root : document;
    var stars = scope.querySelectorAll('.favorite-star[data-be-brand-id]');
    for (var i = 0; i < stars.length; i++) {
      (function (btn) {
        var id = btn.getAttribute('data-be-brand-id');
        if (!id) return;
        if (!btn.querySelector('svg')) btn.innerHTML = STAR_SVG;
        updateCardStar(btn, isFavorite(id));
        if (btn._beStarWired) return;
        btn._beStarWired = true;
        btn.addEventListener('click', function (e) {
          onFavoriteToggleClick(e, id, btn);
        });
      })(stars[i]);
    }
  }

  function ready() {
    if (!readyPromise) {
      readyPromise = loadFromServer();
    }
    return readyPromise;
  }

  window.BrandExplorerFavorites = {
    load: load,
    save: save,
    isFavorite: isFavorite,
    toggle: toggle,
    wireSaveButtons: wireSaveButtons,
    wireCardStars: wireCardStars,
    ready: ready,
    getUserId: function () {
      return currentUserId;
    },
    isServerEnabled: function () {
      return serverEnabled;
    },
  };

  window.addEventListener('brand-explorer-detail-loaded', function () {
    wireSaveButtons(document);
  });
})();
