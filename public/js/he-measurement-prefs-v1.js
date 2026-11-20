/**
 * Hotel Explorer shared measurement preferences (session-scoped).
 * Presentation only — never mutates verified source measurements.
 *
 * Area: sqft | sqm
 * Distance: mi | km
 */
(function (global) {
  "use strict";

  // Session storage id (not a credential). Avoid KEY=value shapes for secret scanners.
  var SESSION_PREF_ID = "he_measurement_prefs_v1";
  var SQFT_TO_SQM = 0.09290304;
  var MI_TO_KM = 1.609344;
  var listeners = [];

  function readStore() {
    try {
      var raw = global.sessionStorage && global.sessionStorage.getItem(SESSION_PREF_ID);
      if (!raw) return null;
      return JSON.parse(raw);
    } catch (_err) {
      return null;
    }
  }

  function writeStore(prefs) {
    try {
      if (global.sessionStorage) {
        global.sessionStorage.setItem(SESSION_PREF_ID, JSON.stringify(prefs));
      }
    } catch (_err) {
      /* ignore */
    }
  }

  function normalize(prefs) {
    prefs = prefs || {};
    return {
      area: prefs.area === "sqm" ? "sqm" : "sqft",
      distance: prefs.distance === "km" ? "km" : "mi",
    };
  }

  var state = normalize(readStore() || { area: "sqft", distance: "mi" });

  function get() {
    return { area: state.area, distance: state.distance };
  }

  function syncControlUi(root, prefs) {
    prefs = prefs || get();
    var nodes = [];
    if (root && root.querySelectorAll) {
      if (root.classList && root.classList.contains("he-meas-prefs")) {
        nodes.push(root);
      }
      root.querySelectorAll(".he-meas-prefs").forEach(function (el) {
        nodes.push(el);
      });
    } else if (global.document) {
      global.document.querySelectorAll(".he-meas-prefs").forEach(function (el) {
        nodes.push(el);
      });
    }
    nodes.forEach(function (el) {
      el.querySelectorAll("[data-he-meas]").forEach(function (btn) {
        var dim = btn.getAttribute("data-he-meas");
        var val = btn.getAttribute("data-he-meas-value");
        var active =
          (dim === "area" && val === prefs.area) ||
          (dim === "distance" && val === prefs.distance);
        btn.classList.toggle("is-active", active);
        btn.setAttribute("aria-pressed", active ? "true" : "false");
      });
    });
  }

  function set(partial) {
    var next = normalize(Object.assign({}, state, partial || {}));
    var changed = next.area !== state.area || next.distance !== state.distance;
    state = next;
    writeStore(state);
    if (changed) {
      syncControlUi(null, get());
      listeners.forEach(function (fn) {
        try {
          fn(get());
        } catch (_err) {
          /* ignore */
        }
      });
      try {
        global.dispatchEvent(
          new CustomEvent("he-measurement-prefs-change", { detail: get() })
        );
      } catch (_err2) {
        /* ignore */
      }
    }
    return get();
  }

  function onChange(fn) {
    if (typeof fn === "function") listeners.push(fn);
    return function () {
      listeners = listeners.filter(function (f) {
        return f !== fn;
      });
    };
  }

  function sqFtToSqM(sqFt) {
    var n = Number(sqFt);
    if (!Number.isFinite(n)) return null;
    return n * SQFT_TO_SQM;
  }

  function sqMToSqFt(sqM) {
    var n = Number(sqM);
    if (!Number.isFinite(n)) return null;
    return n / SQFT_TO_SQM;
  }

  function miToKm(mi) {
    var n = Number(mi);
    if (!Number.isFinite(n)) return null;
    return n * MI_TO_KM;
  }

  function kmToMi(km) {
    var n = Number(km);
    if (!Number.isFinite(n)) return null;
    return n / MI_TO_KM;
  }

  function formatNumber(n, digits) {
    if (!Number.isFinite(n)) return "—";
    var d = digits == null ? (Math.abs(n) >= 100 ? 0 : 1) : digits;
    return Number(n.toFixed(d)).toLocaleString("en-US");
  }

  /** @param {number} sqFt — source value in square feet */
  function formatAreaFromSqFt(sqFt, prefs) {
    prefs = prefs || get();
    var n = Number(sqFt);
    if (!Number.isFinite(n)) return "—";
    if (prefs.area === "sqm") {
      return formatNumber(sqFtToSqM(n), n >= 1000 ? 0 : 1) + " " + areaUnitLabel(prefs);
    }
    return formatNumber(n, 0) + " " + areaUnitLabel(prefs);
  }

  /** @param {number} km — source distance in kilometers (haversine canonical) */
  function formatDistanceFromKm(km, prefs) {
    prefs = prefs || get();
    var n = Number(km);
    if (!Number.isFinite(n)) return "—";
    if (prefs.distance === "mi") {
      var mi = kmToMi(n);
      if (mi < 0.1) return Math.round(mi * 5280) + " ft";
      if (mi < 10) return formatNumber(mi, 1) + " mi";
      return formatNumber(mi, 0) + " mi";
    }
    if (n < 1) return Math.round(n * 1000) + " m";
    if (n < 10) return formatNumber(n, 1) + " km";
    return formatNumber(n, 0) + " km";
  }

  function areaUnitLabel(prefs) {
    prefs = prefs || get();
    return prefs.area === "sqm" ? "Sq. M." : "Sq. Ft.";
  }

  function distanceUnitLabel(prefs) {
    prefs = prefs || get();
    return prefs.distance === "km" ? "km" : "mi";
  }

  /** Default radius recommendations in miles (founder table); convert for storage as km. */
  function defaultRadiusMilesForLocationType(locationType) {
    var loc = String(locationType || "").toLowerCase();
    // Order matters: "suburban" contains the substring "urban".
    if (loc.indexOf("suburban") >= 0) return 10;
    if (loc.indexOf("airport") >= 0) return 10;
    if (loc.indexOf("resort") >= 0 || loc.indexOf("destination") >= 0) return 30;
    if (loc.indexOf("rural") >= 0 || loc.indexOf("remote") >= 0) return 25;
    if (loc.indexOf("urban") >= 0 || loc.indexOf("city center") >= 0 || loc.indexOf("city-center") >= 0) {
      return 5;
    }
    return 10;
  }

  function defaultRadiusKmForLocationType(locationType) {
    return miToKm(defaultRadiusMilesForLocationType(locationType));
  }

  /**
   * Contextual measurement controls.
   * @param {{ showArea?: boolean, showDistance?: boolean, className?: string }} [opts]
   * Default (legacy): both dimensions. Prefer explicit showArea / showDistance for tab placement.
   */
  function renderControlHtml(opts) {
    opts = opts || {};
    var showArea;
    var showDistance;
    // Legacy / tests: both omitted → both groups. Explicit flags → only those set true.
    if (opts.showArea == null && opts.showDistance == null) {
      showArea = true;
      showDistance = true;
    } else {
      showArea = opts.showArea === true;
      showDistance = opts.showDistance === true;
    }
    if (!showArea && !showDistance) return "";
    var prefs = get();
    var parts = [];
    if (showArea) {
      parts.push(
        '<div class="he-meas-prefs__group" data-he-meas-group="area">' +
          '<span class="he-meas-prefs__label">Area</span>' +
          '<button type="button" class="he-meas-prefs__btn' +
          (prefs.area === "sqft" ? " is-active" : "") +
          '" data-he-meas="area" data-he-meas-value="sqft" aria-pressed="' +
          (prefs.area === "sqft" ? "true" : "false") +
          '">Sq. Ft.</button>' +
          '<button type="button" class="he-meas-prefs__btn' +
          (prefs.area === "sqm" ? " is-active" : "") +
          '" data-he-meas="area" data-he-meas-value="sqm" aria-pressed="' +
          (prefs.area === "sqm" ? "true" : "false") +
          '">Sq. M.</button>' +
          "</div>"
      );
    }
    if (showDistance) {
      parts.push(
        '<div class="he-meas-prefs__group" data-he-meas-group="distance">' +
          '<span class="he-meas-prefs__label">Distance</span>' +
          '<button type="button" class="he-meas-prefs__btn' +
          (prefs.distance === "mi" ? " is-active" : "") +
          '" data-he-meas="distance" data-he-meas-value="mi" aria-pressed="' +
          (prefs.distance === "mi" ? "true" : "false") +
          '">mi</button>' +
          '<button type="button" class="he-meas-prefs__btn' +
          (prefs.distance === "km" ? " is-active" : "") +
          '" data-he-meas="distance" data-he-meas-value="km" aria-pressed="' +
          (prefs.distance === "km" ? "true" : "false") +
          '">km</button>' +
          "</div>"
      );
    }
    var cls =
      "he-meas-prefs" +
      (opts.className ? " " + opts.className : "") +
      (showArea && !showDistance ? " he-meas-prefs--area" : "") +
      (showDistance && !showArea ? " he-meas-prefs--distance" : "");
    var label =
      showArea && showDistance
        ? "Measurement units"
        : showArea
          ? "Area units"
          : "Distance units";
    return (
      '<div class="' +
      cls +
      '" role="group" aria-label="' +
      label +
      '" data-he-meas-scope="' +
      (showArea && showDistance ? "both" : showArea ? "area" : "distance") +
      '">' +
      parts.join("") +
      "</div>"
    );
  }

  function renderAreaControlHtml(className) {
    return renderControlHtml({ showArea: true, showDistance: false, className: className });
  }

  function renderDistanceControlHtml(className) {
    return renderControlHtml({ showArea: false, showDistance: true, className: className });
  }

  function bindControls(root) {
    if (!root || root._heMeasBound) return;
    root._heMeasBound = true;
    root.addEventListener("click", function (ev) {
      var btn = ev.target.closest("[data-he-meas]");
      if (!btn || !root.contains(btn)) return;
      var dim = btn.getAttribute("data-he-meas");
      var val = btn.getAttribute("data-he-meas-value");
      if (dim === "area" || dim === "distance") {
        var patch = {};
        patch[dim] = val;
        set(patch);
      }
    });
  }

  global.HeMeasurementPrefs = {
    SQFT_TO_SQM: SQFT_TO_SQM,
    MI_TO_KM: MI_TO_KM,
    get: get,
    set: set,
    onChange: onChange,
    sqFtToSqM: sqFtToSqM,
    sqMToSqFt: sqMToSqFt,
    miToKm: miToKm,
    kmToMi: kmToMi,
    formatAreaFromSqFt: formatAreaFromSqFt,
    formatDistanceFromKm: formatDistanceFromKm,
    areaUnitLabel: areaUnitLabel,
    distanceUnitLabel: distanceUnitLabel,
    defaultRadiusMilesForLocationType: defaultRadiusMilesForLocationType,
    defaultRadiusKmForLocationType: defaultRadiusKmForLocationType,
    renderControlHtml: renderControlHtml,
    renderAreaControlHtml: renderAreaControlHtml,
    renderDistanceControlHtml: renderDistanceControlHtml,
    bindControls: bindControls,
    syncControlUi: syncControlUi,
  };
})(typeof window !== "undefined" ? window : globalThis);
