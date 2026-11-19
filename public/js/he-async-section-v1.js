/**
 * Hotel Explorer — independent async section loading (canonical wave).
 * States: IDLE | LOADING | READY | EMPTY | ERROR | STALE
 * No GET-triggered writes. No blocking of sibling sections.
 */
(function (global) {
  "use strict";

  var STATES = Object.freeze({
    IDLE: "IDLE",
    LOADING: "LOADING",
    READY: "READY",
    EMPTY: "EMPTY",
    ERROR: "ERROR",
    STALE: "STALE",
  });

  var DEFAULT_TIMEOUT_MS = 20000;
  var DEFAULT_CACHE_TTL_MS = 5 * 60 * 1000;

  function waveCompact(text, sub) {
    var api = global.DealalityWaveLoader;
    if (api && typeof api.compactHtml === "function") {
      return api.compactHtml(text || "Loading…", { subText: sub || "", progress: false });
    }
    if (api && typeof api.html === "function") {
      return api.html(text || "Loading…", {
        fixed: false,
        compact: true,
        progress: false,
        subText: sub || "",
      });
    }
    return (
      '<div class="dealality-wave-loader dealality-wave-loader--inline dealality-wave-loader--compact" role="status" aria-live="polite">' +
      '<div class="loading-text-main">' +
      String(text || "Loading…") +
      "</div></div>"
    );
  }

  function errorHtml(message, retryAttr) {
    var msg = String(message || "This information is temporarily unavailable.");
    var retry = retryAttr
      ? '<button type="button" class="hiw-btn hiw-btn--ghost hiw-btn--sm" data-he-async-retry="' +
        String(retryAttr).replace(/"/g, "") +
        '">Retry</button>'
      : "";
    return (
      '<div class="he-async-error" role="status">' +
      '<p class="hiw-prose hiw-prose--quiet">' +
      msg.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;") +
      "</p>" +
      retry +
      "</div>"
    );
  }

  function createController(opts) {
    opts = opts || {};
    var key = String(opts.key || "section");
    var cacheTtlMs = opts.cacheTtlMs != null ? Number(opts.cacheTtlMs) : DEFAULT_CACHE_TTL_MS;
    var timeoutMs = opts.timeoutMs != null ? Number(opts.timeoutMs) : DEFAULT_TIMEOUT_MS;
    var cache = new Map();
    var generation = 0;
    var inflight = null;
    var state = STATES.IDLE;
    var lastHotelId = null;

    function cacheGet(hotelId) {
      var hit = cache.get(String(hotelId || ""));
      if (!hit) return null;
      if (Date.now() - hit.at > cacheTtlMs) return null;
      return hit.payload;
    }

    function cacheSet(hotelId, payload) {
      cache.set(String(hotelId || ""), { at: Date.now(), payload: payload });
    }

    function begin(hotelId) {
      generation += 1;
      lastHotelId = String(hotelId || "");
      if (inflight && typeof inflight.abort === "function") {
        try {
          inflight.abort();
        } catch (_e) {}
      }
      inflight = typeof AbortController !== "undefined" ? new AbortController() : null;
      state = STATES.LOADING;
      return { generation: generation, signal: inflight ? inflight.signal : null };
    }

    function isCurrent(gen, hotelId) {
      return gen === generation && String(hotelId || "") === String(lastHotelId || "");
    }

    function setState(next) {
      state = next;
      return state;
    }

    /**
     * @param {string} hotelId
     * @param {() => Promise<any>} fetcher — receives no args; use closure for URL/signal
     * @param {{ force?: boolean, onLoading?: Function, onResult?: Function }} handlers
     */
    function load(hotelId, fetcher, handlers) {
      handlers = handlers || {};
      var id = String(hotelId || "");
      if (!id) {
        setState(STATES.EMPTY);
        if (handlers.onResult) handlers.onResult({ state: STATES.EMPTY, payload: null });
        return Promise.resolve({ state: STATES.EMPTY, payload: null });
      }

      var cached = !handlers.force ? cacheGet(id) : null;
      if (cached != null && handlers.preferCache !== false) {
        setState(STATES.READY);
        if (handlers.onResult) {
          handlers.onResult({ state: STATES.READY, payload: cached, fromCache: true });
        }
        // Optional background refresh without flipping UI to LOADING
        if (handlers.softRefresh === true) {
          softRefresh(id, fetcher, handlers);
        }
        return Promise.resolve({ state: STATES.READY, payload: cached, fromCache: true });
      }

      var ticket = begin(id);
      if (handlers.onLoading) handlers.onLoading({ state: STATES.LOADING, stale: false });

      var timedOut = false;
      var timer =
        timeoutMs > 0
          ? setTimeout(function () {
              timedOut = true;
              if (inflight && typeof inflight.abort === "function") {
                try {
                  inflight.abort();
                } catch (_e) {}
              }
            }, timeoutMs)
          : null;

      return Promise.resolve()
        .then(function () {
          return fetcher({ signal: ticket.signal, hotelId: id, generation: ticket.generation });
        })
        .then(function (payload) {
          if (timer) clearTimeout(timer);
          if (!isCurrent(ticket.generation, id)) {
            return { state: STATES.STALE, payload: null, ignored: true };
          }
          cacheSet(id, payload);
          var next = handlers.isEmpty && handlers.isEmpty(payload) ? STATES.EMPTY : STATES.READY;
          setState(next);
          if (handlers.onResult) {
            handlers.onResult({ state: next, payload: payload, fromCache: false });
          }
          return { state: next, payload: payload };
        })
        .catch(function (err) {
          if (timer) clearTimeout(timer);
          if (!isCurrent(ticket.generation, id)) {
            return { state: STATES.STALE, payload: null, ignored: true };
          }
          var staleCache = cacheGet(id);
          if (staleCache != null) {
            setState(STATES.STALE);
            if (handlers.onResult) {
              handlers.onResult({
                state: STATES.STALE,
                payload: staleCache,
                fromCache: true,
                error: timedOut ? "timeout" : "error",
              });
            }
            return { state: STATES.STALE, payload: staleCache };
          }
          setState(STATES.ERROR);
          if (handlers.onResult) {
            handlers.onResult({
              state: STATES.ERROR,
              payload: null,
              error: timedOut ? "timeout" : "error",
              message: timedOut
                ? "This is taking longer than expected. Please try again."
                : "This information is temporarily unavailable.",
            });
          }
          return { state: STATES.ERROR, payload: null, error: err };
        });
    }

    function softRefresh(hotelId, fetcher, handlers) {
      var id = String(hotelId || "");
      var genAtStart = generation;
      Promise.resolve()
        .then(function () {
          return fetcher({ signal: null, hotelId: id, generation: genAtStart });
        })
        .then(function (payload) {
          if (genAtStart !== generation || String(lastHotelId) !== id) return;
          cacheSet(id, payload);
          if (handlers && handlers.onSoftRefresh) {
            handlers.onSoftRefresh({ payload: payload });
          }
        })
        .catch(function () {});
    }

    function cancel() {
      generation += 1;
      if (inflight && typeof inflight.abort === "function") {
        try {
          inflight.abort();
        } catch (_e) {}
      }
      inflight = null;
      setState(STATES.IDLE);
    }

    function clearCache(hotelId) {
      if (hotelId == null || hotelId === "") {
        cache.clear();
        return;
      }
      cache.delete(String(hotelId));
    }

    return {
      key: key,
      STATES: STATES,
      getState: function () {
        return state;
      },
      getCached: cacheGet,
      setCached: cacheSet,
      clearCache: clearCache,
      load: load,
      cancel: cancel,
      waveCompact: waveCompact,
      errorHtml: errorHtml,
    };
  }

  global.HeAsyncSection = {
    STATES: STATES,
    createController: createController,
    waveCompact: waveCompact,
    errorHtml: errorHtml,
    DEFAULT_TIMEOUT_MS: DEFAULT_TIMEOUT_MS,
    DEFAULT_CACHE_TTL_MS: DEFAULT_CACHE_TTL_MS,
  };
})(typeof window !== "undefined" ? window : globalThis);
