/**
 * Canonical Brand Explorer route-state parser.
 * Supports both:
 *   /brand-explorer-combined.html?id=…&beInternalPreview=1
 *   /app#/brand-explorer-combined.html?id=…&beInternalPreview=1
 *
 * When embedded in the /app shell, parent hash query params are merged so
 * preview flags survive even if the iframe search briefly lags.
 */
(function (root) {
  'use strict';

  /** Query keys forwarded from /app# hash → iframe embed URL (app.js). */
  var FORWARD_QUERY_KEYS = [
    'recordId',
    'operatorId',
    'id',
    'brandId',
    'brand',
    'name',
    'beInternalPreview',
    'factoryPreview',
    'exportPdf',
    'refresh',
    'spacious',
    'share',
    'pack'
  ];

  function parseQueryString(qs) {
    var s = String(qs == null ? '' : qs);
    if (s.charAt(0) === '?') s = s.slice(1);
    try {
      return new URLSearchParams(s);
    } catch (e) {
      return new URLSearchParams('');
    }
  }

  /**
   * Parse `#/route?a=1` or `#route?a=1` into { route, params }.
   * route keeps a leading slash when present in the hash path.
   */
  function parseHashRoute(hash) {
    var raw = String(hash == null ? '' : hash);
    if (raw.charAt(0) === '#') raw = raw.slice(1);
    raw = raw.trim();
    if (!raw) {
      return { route: '', params: new URLSearchParams('') };
    }
    var qIdx = raw.indexOf('?');
    var pathPart = qIdx >= 0 ? raw.slice(0, qIdx) : raw;
    var queryPart = qIdx >= 0 ? raw.slice(qIdx + 1) : '';
    pathPart = pathPart.trim();
    if (pathPart && pathPart.charAt(0) !== '/') {
      pathPart = '/' + pathPart;
    }
    if (pathPart.length > 1 && pathPart.charAt(pathPart.length - 1) === '/') {
      pathPart = pathPart.slice(0, -1);
    }
    return {
      route: pathPart,
      params: parseQueryString(queryPart),
    };
  }

  function flagTrue(params, key) {
    var v = params.get(key);
    return v === '1' || v === 'true';
  }

  function mergeParamsFill(target, source) {
    if (!source || typeof source.forEach !== 'function') return target;
    source.forEach(function (value, key) {
      if (value == null || String(value) === '') return;
      if (!target.get(key)) target.set(key, value);
    });
    return target;
  }

  function mergeParamsPreferSource(target, source, keys) {
    if (!source || typeof source.forEach !== 'function') return target;
    var keySet = null;
    if (keys && keys.length) {
      keySet = {};
      for (var i = 0; i < keys.length; i++) keySet[keys[i]] = true;
    }
    source.forEach(function (value, key) {
      if (value == null || String(value) === '') return;
      if (keySet && !keySet[key]) return;
      target.set(key, value);
    });
    return target;
  }

  function isAppShellPathname(pathname) {
    return /^\/app(?:\.html)?\/?$/i.test(String(pathname || ''));
  }

  function getBrandExplorerRouteState(options) {
    options = options || {};
    var loc =
      options.location ||
      (typeof window !== 'undefined' ? window.location : { pathname: '', search: '', hash: '' });

    var params = new URLSearchParams();
    mergeParamsFill(params, parseQueryString(loc.search));

    var ownHash = parseHashRoute(loc.hash);
    mergeParamsFill(params, ownHash.params);

    var parentLoc = options.parentLocation;
    if (parentLoc === undefined && typeof window !== 'undefined') {
      try {
        if (window.parent && window.parent !== window) {
          parentLoc = window.parent.location;
        }
      } catch (e) {
        parentLoc = null;
      }
    }

    var parentHash = { route: '', params: new URLSearchParams('') };
    if (parentLoc && isAppShellPathname(parentLoc.pathname)) {
      parentHash = parseHashRoute(parentLoc.hash);
      mergeParamsPreferSource(params, parentHash.params, [
        'id',
        'brandId',
        'brand',
        'name',
        'recordId',
        'beInternalPreview',
        'factoryPreview',
        'exportPdf',
        'refresh',
      ]);
      mergeParamsFill(params, parentHash.params);
    }

    var route = ownHash.route || parentHash.route || '';
    if (!route) {
      try {
        var path = String(loc.pathname || '');
        var leaf = path.split('/').filter(Boolean).pop() || '';
        route = leaf ? '/' + leaf : '/brand-explorer-combined.html';
      } catch (e2) {
        route = '/brand-explorer-combined.html';
      }
    }

    var brandId = String(
      params.get('id') ||
        params.get('brandId') ||
        params.get('brand') ||
        params.get('name') ||
        params.get('recordId') ||
        ''
    ).trim();

    var beInternalPreview = flagTrue(params, 'beInternalPreview');
    var exportPdf = flagTrue(params, 'exportPdf');
    var factoryPreview = flagTrue(params, 'factoryPreview');

    return {
      route: route,
      brandId: brandId,
      beInternalPreview: beInternalPreview,
      exportPdf: exportPdf,
      factoryPreview: factoryPreview,
      refresh: flagTrue(params, 'refresh'),
      embed: flagTrue(params, 'embed'),
      appShell: flagTrue(params, 'appShell'),
      isInternalPreviewRequest: beInternalPreview || exportPdf,
      params: params,
    };
  }

  function buildBrandExplorerQuery(brandId, state, extra) {
    var next = new URLSearchParams();
    var src = state && state.params ? state.params : null;
    if (src && typeof src.forEach === 'function') {
      src.forEach(function (value, key) {
        if (value == null || String(value) === '') return;
        if (key === 'embed' || key === 'appShell' || key === 'msToken') return;
        next.set(key, value);
      });
    }
    if (brandId) next.set('id', String(brandId));
    if (state && state.beInternalPreview) next.set('beInternalPreview', '1');
    if (state && state.factoryPreview) next.set('factoryPreview', '1');
    if (state && state.exportPdf) next.set('exportPdf', '1');
    if (state && state.refresh) next.set('refresh', '1');
    if (extra && typeof extra === 'object') {
      Object.keys(extra).forEach(function (k) {
        if (extra[k] == null || extra[k] === '') next.delete(k);
        else next.set(k, String(extra[k]));
      });
    }
    return next;
  }

  var api = {
    FORWARD_QUERY_KEYS: FORWARD_QUERY_KEYS,
    parseQueryString: parseQueryString,
    parseHashRoute: parseHashRoute,
    getBrandExplorerRouteState: getBrandExplorerRouteState,
    buildBrandExplorerQuery: buildBrandExplorerQuery,
    isAppShellPathname: isAppShellPathname,
  };

  if (typeof module === 'object' && module.exports) {
    module.exports = api;
  }
  if (root) {
    root.BrandExplorerRouteState = api;
  }
})(typeof globalThis !== 'undefined' ? globalThis : typeof window !== 'undefined' ? window : this);
