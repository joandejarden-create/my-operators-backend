/**
 * Founder-only Current Saved / Proposed Update switch for the real Brand Explorer product.
 * Requires beInternalPreview=1. Never writes to Airtable.
 */
(function () {
  'use strict';

  var TOOLBAR_ID = 'beFounderProfileModeToolbar';
  var HOST_ATTR = 'data-be-founder-profile-mode-host';

  function escapeHtml(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function getApi() {
    return window.BrandExplorerAtelierFromApi || null;
  }

  function getRouteState() {
    try {
      if (window.BrandExplorerRouteState && window.BrandExplorerRouteState.getBrandExplorerRouteState) {
        return window.BrandExplorerRouteState.getBrandExplorerRouteState();
      }
    } catch (e) {
      /* ignore */
    }
    return null;
  }

  function isInternalPreview() {
    var api = getApi();
    if (api && typeof api.isInternalPreviewRequest === 'function') {
      return !!api.isInternalPreviewRequest();
    }
    var st = getRouteState();
    return !!(st && st.isInternalPreviewRequest);
  }

  function ensureHost() {
    var existing = document.getElementById(TOOLBAR_ID);
    if (existing) return existing;
    var host =
      document.querySelector('[' + HOST_ATTR + ']') ||
      document.getElementById('beAtelierRoot') ||
      document.getElementById('brandRoot') ||
      document.querySelector('.be-combined-wrap');
    if (!host) return null;
    var toolbar = document.createElement('div');
    toolbar.id = TOOLBAR_ID;
    toolbar.className = 'be-founder-profile-mode';
    toolbar.setAttribute('hidden', 'hidden');
    toolbar.setAttribute('data-be-founder-profile-mode', '1');
    if (host.id === 'beAtelierRoot' || host.hasAttribute('data-be-atelier-root')) {
      host.insertAdjacentElement('beforebegin', toolbar);
    } else {
      host.insertAdjacentElement('afterbegin', toolbar);
    }
    return toolbar;
  }

  function metaFromBrand(brand, mode) {
    var live = (getApi() && getApi().getLastLiveSavedBrand && getApi().getLastLiveSavedBrand()) || brand;
    var cfg =
      (getApi() && getApi().resolveStagedCandidateConfig && getApi().resolveStagedCandidateConfig(live)) ||
      null;
    var overlayMeta = (brand && brand.beCandidateOverlayMeta) || {};
    var brandId =
      overlayMeta.brandId ||
      (cfg && cfg.id) ||
      (live && (live.id || live.recordId)) ||
      '—';
    var brandName =
      (live && (live.name || live.brandName)) ||
      (cfg && cfg.name) ||
      (brand && (brand.name || brand.brandName)) ||
      '—';
    if (mode !== 'proposed') {
      return {
        brandId: brandId,
        brandName: brandName,
        candidateVersion: '— (current saved Presentation)',
        contentHash: '—',
        source: 'airtable-presentation',
        founderApprovalStatus: 'N/A (published/current)'
      };
    }
    return {
      brandId: brandId,
      brandName: brandName,
      candidateVersion: overlayMeta.candidateVersion || (cfg && cfg.candidateVersion) || '—',
      contentHash: overlayMeta.contentHash || '—',
      source: overlayMeta.source || (cfg && cfg.source) || 'phase2h-cumulative-staged',
      founderApprovalStatus:
        overlayMeta.founderApprovalStatus || (cfg && cfg.founderApprovalStatus) || 'NOT_APPROVED'
    };
  }

  function compareStudioHref(brandId) {
    var id = String(brandId || '').trim();
    var q = 'beInternalPreview=1';
    if (id && id !== '—') q += '&brand=' + encodeURIComponent(id);
    // Prefer app-shell Admin Resources route on the active origin.
    try {
      if (window.parent && window.parent !== window) {
        var parentLoc = window.parent.location;
        if (/^\/app(?:\.html)?\/?$/i.test(parentLoc.pathname || '')) {
          return parentLoc.pathname + '#/brand-explorer-compare?' + q;
        }
      }
    } catch (e) {
      /* fall through */
    }
    try {
      if (/^\/app(?:\.html)?\/?$/i.test(window.location.pathname || '')) {
        return window.location.pathname + '#/brand-explorer-compare?' + q;
      }
    } catch (e2) {
      /* fall through */
    }
    return '/app#/brand-explorer-compare?' + q;
  }

  function renderToolbar() {
    var toolbar = ensureHost();
    if (!toolbar) return;
    if (!isInternalPreview()) {
      toolbar.setAttribute('hidden', 'hidden');
      toolbar.innerHTML = '';
      return;
    }
    toolbar.removeAttribute('hidden');
    var api = getApi();
    var mode =
      api && typeof api.getProfileMode === 'function' ? api.getProfileMode() : 'saved';
    var brand = api && api.getLastAtelierBrand ? api.getLastAtelierBrand() : null;
    var err = api && api.getLastProposedError ? api.getLastProposedError() : null;
    var meta = metaFromBrand(brand, mode);
    var hasCandidate =
      api && api.resolveStagedCandidateConfig
        ? !!api.resolveStagedCandidateConfig(api.getLastLiveSavedBrand && api.getLastLiveSavedBrand())
        : false;

    var compareHref = compareStudioHref(meta.brandId);
    toolbar.innerHTML =
      '<div class="be-founder-profile-mode__row">' +
      '<div class="be-founder-profile-mode__label">Founder profile mode</div>' +
      '<div class="be-founder-profile-mode__modes" role="group" aria-label="Profile mode">' +
      '<button type="button" class="be-founder-profile-mode__btn' +
      (mode === 'saved' ? ' is-active' : '') +
      '" data-be-profile-mode-btn="saved" aria-pressed="' +
      (mode === 'saved' ? 'true' : 'false') +
      '">CURRENT SAVED</button>' +
      '<button type="button" class="be-founder-profile-mode__btn' +
      (mode === 'proposed' ? ' is-active' : '') +
      '" data-be-profile-mode-btn="proposed" aria-pressed="' +
      (mode === 'proposed' ? 'true' : 'false') +
      '"' +
      (hasCandidate ? '' : ' disabled title="No staged candidate for this brand"') +
      '>PROPOSED UPDATE</button>' +
      '</div>' +
      '<a class="be-founder-profile-mode__compare" href="' +
      escapeHtml(compareHref) +
      '" target="_top" rel="noopener" data-be-compare-changes="1">Compare Changes</a>' +
      '</div>' +
      '<p class="be-founder-profile-mode__brandline"><strong>' +
      escapeHtml(meta.brandName) +
      '</strong> · mode <strong>' +
      (mode === 'proposed' ? 'PROPOSED UPDATE' : 'CURRENT SAVED') +
      '</strong></p>' +
      '<p class="be-founder-profile-mode__note">Read-only product preview. Switching modes does not publish or write Airtable. Customer sessions without <code>beInternalPreview=1</code> always see CURRENT SAVED. Use Compare Changes for CURRENT vs PROPOSED side-by-side (Admin Resources).</p>' +
      '<dl class="be-founder-profile-mode__meta">' +
      '<div><dt>Brand</dt><dd>' +
      escapeHtml(meta.brandName) +
      '</dd></div>' +
      '<div><dt>Brand ID</dt><dd>' +
      escapeHtml(meta.brandId) +
      '</dd></div>' +
      '<div><dt>Candidate version</dt><dd>' +
      escapeHtml(meta.candidateVersion) +
      '</dd></div>' +
      '<div><dt>Content hash</dt><dd>' +
      escapeHtml(String(meta.contentHash).slice(0, 64)) +
      '</dd></div>' +
      '<div><dt>Source</dt><dd>' +
      escapeHtml(meta.source) +
      '</dd></div>' +
      '<div><dt>Founder approval</dt><dd>' +
      escapeHtml(meta.founderApprovalStatus) +
      '</dd></div>' +
      '</dl>' +
      (err && mode === 'proposed'
        ? '<div class="be-founder-profile-mode__error" role="alert">' +
          escapeHtml(err) +
          '</div>'
        : '');

    var buttons = toolbar.querySelectorAll('[data-be-profile-mode-btn]');
    for (var i = 0; i < buttons.length; i++) {
      buttons[i].addEventListener('click', onModeClick);
    }
  }

  function updateUrlMode(mode) {
    var nextMode = mode === 'proposed' ? 'proposed' : 'saved';
    try {
      var url = new URL(window.location.href);
      url.searchParams.set('beInternalPreview', '1');
      url.searchParams.set('beProfileMode', nextMode);
      if (nextMode === 'proposed') {
        url.searchParams.set('beCandidateOverlay', '1');
      } else {
        url.searchParams.delete('beCandidateOverlay');
      }
      window.history.replaceState({}, '', url.pathname + url.search + url.hash);
    } catch (e) {
      /* ignore */
    }
    // Keep /app# hash in sync when embedded.
    try {
      if (window.parent && window.parent !== window) {
        var parentLoc = window.parent.location;
        if (/^\/app(?:\.html)?\/?$/i.test(parentLoc.pathname || '')) {
          var hash = String(parentLoc.hash || '');
          var qIdx = hash.indexOf('?');
          var pathPart = qIdx >= 0 ? hash.slice(0, qIdx) : hash;
          var params = new URLSearchParams(qIdx >= 0 ? hash.slice(qIdx + 1) : '');
          params.set('beInternalPreview', '1');
          params.set('beProfileMode', nextMode);
          if (nextMode === 'proposed') params.set('beCandidateOverlay', '1');
          else params.delete('beCandidateOverlay');
          var nextHash = pathPart + '?' + params.toString();
          window.parent.history.replaceState({}, '', parentLoc.pathname + parentLoc.search + nextHash);
        }
      }
    } catch (e2) {
      /* cross-origin or unavailable */
    }
  }

  function onModeClick(ev) {
    var btn = ev.currentTarget;
    var mode = btn && btn.getAttribute('data-be-profile-mode-btn');
    if (!mode) return;
    var api = getApi();
    if (!api || typeof api.remountProfileMode !== 'function') return;
    updateUrlMode(mode);
    Promise.resolve(api.remountProfileMode(mode, { forceRebuild: mode === 'proposed' })).then(
      function () {
        renderToolbar();
      }
    );
  }

  function boot() {
    renderToolbar();
  }

  window.addEventListener('brand-explorer-detail-loaded', function () {
    window.setTimeout(renderToolbar, 0);
  });
  window.addEventListener('brand-explorer-profile-mode-mounted', function () {
    window.setTimeout(renderToolbar, 0);
  });
  window.addEventListener('brand-explorer-profile-mode-error', function () {
    window.setTimeout(renderToolbar, 0);
  });
  window.addEventListener('DOMContentLoaded', boot);
  if (document.readyState !== 'loading') boot();

  window.BrandExplorerFounderProfileMode = {
    render: renderToolbar,
    updateUrlMode: updateUrlMode
  };
})();
