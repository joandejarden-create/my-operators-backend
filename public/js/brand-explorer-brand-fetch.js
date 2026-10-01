/**
 * Shared in-flight + short TTL cache for GET /api/brand-library/brand.
 * Prevents duplicate Airtable-backed loads when popup and detail view open together.
 */
(function () {
  'use strict';

  var CACHE_TTL_MS = 5 * 60 * 1000;
  /** @type {Map<string, { at: number, promise: Promise<any>, data: object|null }>} */
  var cache = new Map();

  function cacheKey(brandId, useHpc, factoryPreview) {
    return (
      String(brandId || '').trim() +
      (useHpc ? '|hpc' : '|legacy') +
      (factoryPreview ? '|fp' : '')
    );
  }

  function isFresh(entry) {
    return entry && Date.now() - entry.at < CACHE_TTL_MS;
  }

  function beHpcActive() {
    try {
      var src = typeof window !== 'undefined' && window.DealalityBrandExplorerCensusSource;
      return !!(src && typeof src.isBeHpcOptInActive === 'function' && src.isBeHpcOptInActive());
    } catch (err) {
      return false;
    }
  }

  function isFactoryPreviewActive(options) {
    if (options && options.factoryPreview === true) return true;
    try {
      var search = typeof window !== 'undefined' ? String(window.location.search || '') : '';
      return (
        /(?:\?|&)beInternalPreview=1(?:&|$)/.test(search) &&
        /(?:\?|&)factoryPreview=1(?:&|$)/.test(search)
      );
    } catch (err) {
      return false;
    }
  }

  function fetchBrandDetail(brandId, options) {
    options = options || {};
    var useHpc = beHpcActive();
    var factoryPreview = isFactoryPreviewActive(options);
    var key = cacheKey(brandId, useHpc, factoryPreview);
    if (!key || key === '|legacy' || key === '|hpc' || key === '|legacy|fp' || key === '|hpc|fp') {
      return Promise.reject(new Error('Brand id is required'));
    }

    var existing = cache.get(key);
    if (!options.refresh && existing) {
      if (existing.data && isFresh(existing)) {
        return Promise.resolve(existing.data);
      }
      if (existing.promise && isFresh(existing)) {
        return existing.promise;
      }
    }

    var url = '/api/brand-library/brand?brandId=' + encodeURIComponent(String(brandId || '').trim());
    if (options.refresh) {
      url += '&refresh=1';
    }
    if (useHpc) {
      url += '&censusSource=hpc&product=brand-explorer&beHpc=1';
    }
    // Forward Factory Preview query so API can apply staged fixture overlays
    // (e.g. brandWebsite) without writing Airtable.
    try {
      var search = typeof window !== 'undefined' ? String(window.location.search || '') : '';
      if (/(?:\?|&)beInternalPreview=1(?:&|$)/.test(search)) {
        url += '&beInternalPreview=1';
      }
      if (/(?:\?|&)factoryPreview=1(?:&|$)/.test(search)) {
        url += '&factoryPreview=1';
      }
      if (options.factoryPreview === true) {
        if (url.indexOf('beInternalPreview=1') === -1) url += '&beInternalPreview=1';
        if (url.indexOf('factoryPreview=1') === -1) url += '&factoryPreview=1';
      }
    } catch (errFwd) {
      /* ignore */
    }

    var headers = { Accept: 'application/json', 'ngrok-skip-browser-warning': 'true' };
    try {
      var src = typeof window !== 'undefined' && window.DealalityBrandExplorerCensusSource;
      if (src && typeof src.beCensusHeaders === 'function') {
        headers = src.beCensusHeaders();
      } else if (useHpc) {
        headers['X-Dealality-Census-Source'] = 'hpc';
        headers['X-Dealality-Product'] = 'brand-explorer';
      }
    } catch (err) {
      /* ignore */
    }

    var promise = fetch(url, { cache: 'no-store', headers: headers })
      .then(function (res) {
        if (!res.ok) {
          throw new Error('Failed to load brand (' + res.status + ')');
        }
        return res.json();
      })
      .then(function (data) {
        var entry = cache.get(key);
        if (entry && entry.promise === promise) {
          entry.data = data;
          entry.at = Date.now();
        }
        return data;
      })
      .catch(function (err) {
        var entry = cache.get(key);
        if (entry && entry.promise === promise) {
          cache.delete(key);
        }
        throw err;
      });

    cache.set(key, { at: Date.now(), promise: promise, data: null });
    return promise;
  }

  function clearBrandDetailCache(brandId) {
    if (brandId) {
      cache.delete(cacheKey(brandId, false));
      cache.delete(cacheKey(brandId, true));
      return;
    }
    cache.clear();
  }

  var LIST_CACHE_TTL_MS = 5 * 60 * 1000;
  /** @type {{ at: number, promise: Promise<any>, brands: object[]|null, filterOptions: object|null }} */
  var listCache = { at: 0, promise: null, brands: null, filterOptions: null };

  function isListFresh() {
    return listCache.brands && Date.now() - listCache.at < LIST_CACHE_TTL_MS;
  }

  function publishBrandList(brands) {
    var list = Array.isArray(brands) ? brands.slice() : [];
    window.getBrandExplorerListBrands = function () {
      return list.slice();
    };
    try {
      window.dispatchEvent(
        new CustomEvent('brand-explorer-list-loaded', { detail: { brands: list } })
      );
    } catch (_) {}
    return list;
  }

  /**
   * Loads /api/brand-library/brands for Portfolio Context sibling tiers and other list-derived UI.
   * Safe to call from share URLs and deep links before BrandExplorerGoldDetail.load.
   */
  function ensureBrandList(options) {
    options = options || {};
    if (!options.refresh && isListFresh()) {
      return Promise.resolve(listCache.brands);
    }
    if (!options.refresh && listCache.promise && Date.now() - listCache.at < LIST_CACHE_TTL_MS) {
      return listCache.promise;
    }

    var url = '/api/brand-library/brands?refresh=1';
    if (options.refresh === false) {
      url = '/api/brand-library/brands';
    }

    var promise = fetch(url, { cache: 'no-store' })
      .then(function (res) {
        if (!res.ok) {
          throw new Error('Failed to fetch brands (' + res.status + ')');
        }
        return res.json();
      })
      .then(function (data) {
        var brands = publishBrandList(data && data.brands ? data.brands : []);
        listCache.brands = brands;
        listCache.filterOptions = (data && data.filterOptions) || {};
        listCache.at = Date.now();
        listCache.promise = promise;
        return brands;
      })
      .catch(function (err) {
        if (listCache.promise === promise) {
          listCache.promise = null;
        }
        throw err;
      });

    listCache.at = Date.now();
    listCache.promise = promise;
    return promise;
  }

  function getBrandListFilterOptions() {
    return listCache.filterOptions ? Object.assign({}, listCache.filterOptions) : {};
  }

  function publishBrandListPayload(data) {
    var brands = publishBrandList(data && data.brands ? data.brands : []);
    listCache.brands = brands;
    listCache.filterOptions = (data && data.filterOptions) || {};
    listCache.at = Date.now();
    return brands;
  }

  window.BrandExplorerBrandFetch = {
    fetchBrandDetail: fetchBrandDetail,
    clearBrandDetailCache: clearBrandDetailCache,
    ensureBrandList: ensureBrandList,
    getBrandListFilterOptions: getBrandListFilterOptions,
    publishBrandListPayload: publishBrandListPayload
  };
})();
