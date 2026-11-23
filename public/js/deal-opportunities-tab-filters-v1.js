/**
 * DealSignal — tab-scoped filter option derivation (browser + Node tests via vm).
 * CUSTOMER_BLOCKED. No Airtable writes.
 */
(function (root) {
  "use strict";

  function tabMembership(o, activeFilter) {
    const f = String(activeFilter || "qualified");
    if (f === "all") return true;
    if (f === "qualified") return o && o.group === "qualified";
    if (f === "watch") return o && o.group === "watch";
    if (f === "BCT") return o && o.group === "qualified" && o.kind === "BCT";
    if (f === "MCT") return o && o.group === "qualified" && o.kind === "MCT";
    return true;
  }

  function tabUniverse(opps, activeFilter) {
    return (Array.isArray(opps) ? opps : []).filter(function (o) {
      return tabMembership(o, activeFilter);
    });
  }

  function uniqueSorted(entries) {
    return Array.from(entries).sort(function (a, b) {
      return String(a[1]).localeCompare(String(b[1]), undefined, {
        sensitivity: "base",
        numeric: true,
      });
    });
  }

  /** Prefer previous selection when still valid; otherwise "all". */
  function resolveSelectValue(options, prev) {
    const p = prev == null || prev === "" ? "all" : String(prev);
    return options.some(function (pair) {
      return String(pair[0]) === p;
    })
      ? p
      : "all";
  }

  function deriveRegionOptions(universe) {
    const regions = new Map();
    universe.forEach(function (o) {
      const r = String((o && o.region) || "").trim();
      if (!r) return;
      regions.set(
        r,
        r === "Other" ? "Other International" : r === "US" ? "United States" : r,
      );
    });
    const options = [["all", "All Markets"]];
    uniqueSorted(regions).forEach(function (pair) {
      options.push(pair);
    });
    return options;
  }

  function deriveCountryOptions(universe, keyFn, labelFn) {
    const countries = new Map();
    let hasUnspecified = false;
    universe.forEach(function (o) {
      const key = keyFn(o);
      if (key === "__unspecified__") {
        hasUnspecified = true;
        return;
      }
      countries.set(key, labelFn(key));
    });
    const options = [["all", "All Countries"]];
    uniqueSorted(countries).forEach(function (pair) {
      options.push(pair);
    });
    if (hasUnspecified) options.push(["__unspecified__", "Unspecified"]);
    return options;
  }

  function deriveBrandOptions(universe, keyFn, labelFn) {
    const brands = new Map();
    let hasUnspecified = false;
    universe.forEach(function (o) {
      const key = keyFn(o);
      if (key === "__unspecified__") {
        hasUnspecified = true;
        return;
      }
      brands.set(key, labelFn(key));
    });
    const options = [["all", "All Brands"]];
    uniqueSorted(brands).forEach(function (pair) {
      options.push(pair);
    });
    if (hasUnspecified) options.push(["__unspecified__", "Unspecified"]);
    return options;
  }

  function deriveTimingOptions(universe, horizonKeyFn, horizonLabelFn) {
    const present = new Map();
    universe.forEach(function (o) {
      const key = horizonKeyFn(o);
      present.set(key, horizonLabelFn(key));
    });
    const order = ["0_6", "6_12", "12_24", "unknown"];
    const options = [["all", "All Timing"]];
    order.forEach(function (k) {
      if (present.has(k)) options.push([k, present.get(k)]);
    });
    Array.from(present.keys())
      .filter(function (k) {
        return order.indexOf(k) === -1;
      })
      .sort()
      .forEach(function (k) {
        options.push([k, present.get(k)]);
      });
    return options;
  }

  function deriveDecisionWindowOptions(universe, keyFn, labelFn) {
    const windows = new Map();
    universe.forEach(function (o) {
      const key = keyFn(o);
      windows.set(key, labelFn(key));
    });
    const options = [["all", "All Decision Windows"]];
    Array.from(windows.entries())
      .sort(function (a, b) {
        if (a[0] === "unknown") return 1;
        if (b[0] === "unknown") return -1;
        return a[1].localeCompare(b[1], undefined, {
          sensitivity: "base",
          numeric: true,
        });
      })
      .forEach(function (pair) {
        options.push(pair);
      });
    return options;
  }

  function chainScalePresentInUniverse(universe, getScalesFn) {
    const set = new Set();
    universe.forEach(function (o) {
      (getScalesFn(o) || []).forEach(function (s) {
        set.add(String(s).toLowerCase());
      });
    });
    return set;
  }

  root.DealSignalOpportunityFilters = {
    tabMembership: tabMembership,
    tabUniverse: tabUniverse,
    resolveSelectValue: resolveSelectValue,
    deriveRegionOptions: deriveRegionOptions,
    deriveCountryOptions: deriveCountryOptions,
    deriveBrandOptions: deriveBrandOptions,
    deriveTimingOptions: deriveTimingOptions,
    deriveDecisionWindowOptions: deriveDecisionWindowOptions,
    chainScalePresentInUniverse: chainScalePresentInUniverse,
  };
})(typeof globalThis !== "undefined" ? globalThis : this);
