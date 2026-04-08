/**
 * Shared behavior for Operator Footprint (geo grid, chain scale totals, location type %).
 * Used by third-party-operator-intake.html and third-party-operator-setup-gold-mock.html.
 *
 * initThirdPartyOperatorFootprint(formElement, { toast?: (msg, ok?: boolean) => void })
 */
(function (global) {
  var DEBOUNCE_MS = 300;

  function debounce(fn, wait) {
    var t;
    return function () {
      var ctx = this;
      var args = arguments;
      clearTimeout(t);
      t = setTimeout(function () {
        fn.apply(ctx, args);
      }, wait);
    };
  }

  function formatFootprintHotelsRoomsDisplay(n) {
    var v = Number(n);
    if (!Number.isFinite(v) || v <= 0) return "";
    return v.toLocaleString("en-US", { maximumFractionDigits: 0 });
  }

  function toInt(value) {
    var n = parseInt(String(value || "").replace(/[^\d-]/g, ""), 10);
    return Number.isFinite(n) && n > 0 ? n : 0;
  }

  function toFloat(value) {
    var n = parseFloat(String(value || "").replace(/[^\d.-]/g, ""));
    return Number.isFinite(n) && n > 0 ? n : 0;
  }

  function splitIntegerAcrossSlots(total, n) {
    var t = Math.max(0, Math.floor(Number(total) || 0));
    var slots = Math.max(0, Math.floor(Number(n) || 0));
    if (slots === 0) return [];
    var base = Math.floor(t / slots);
    var rem = t - base * slots;
    return Array.from({ length: slots }, function (_, i) {
      return base + (i < rem ? 1 : 0);
    });
  }

  function redistributeProportionalInts(values, targetTotal) {
    var n = values.length;
    var T = Math.max(0, Math.floor(Number(targetTotal) || 0));
    if (n === 0) return [];
    if (T === 0) return new Array(n).fill(0);
    var sum = values.reduce(function (a, b) {
      return a + b;
    }, 0);
    if (sum <= 0) return splitIntegerAcrossSlots(T, n);
    var raw = values.map(function (v) {
      return (v * T) / sum;
    });
    var ints = raw.map(function (x) {
      return Math.floor(x);
    });
    var rem = T - ints.reduce(function (a, b) {
      return a + b;
    }, 0);
    var order = raw
      .map(function (x, idx) {
        return { idx: idx, f: x - Math.floor(x) };
      })
      .sort(function (a, b) {
        return b.f - a.f;
      });
    var i = 0;
    while (rem > 0 && order.length > 0) {
      ints[order[i % order.length].idx] += 1;
      rem -= 1;
      i += 1;
    }
    return ints;
  }

  function initThirdPartyOperatorFootprint(form, options) {
    if (!form) return;
    options = options || {};
    var toast =
      typeof options.toast === "function"
        ? options.toast
        : function () {};

    function handleFootprintHotelsRoomsInput(ev) {
      var el = ev.target;
      if (!el || !el.classList.contains("footprint-hotels-rooms") || el.readOnly) return;
      var raw = el.value;
      var cursor = typeof el.selectionStart === "number" ? el.selectionStart : raw.length;
      var digitsBefore = raw.slice(0, cursor).replace(/\D/g, "").length;
      var allDigits = raw.replace(/\D/g, "");
      if (allDigits === "") {
        el.value = "";
        return;
      }
      var n = parseInt(allDigits, 10);
      if (!Number.isFinite(n)) {
        el.value = "";
        return;
      }
      var formatted = n.toLocaleString("en-US");
      el.value = formatted;
      var pos = 0;
      var seen = 0;
      var targetDigits = Math.min(digitsBefore, String(n).length);
      for (; pos < formatted.length && seen < targetDigits; pos += 1) {
        if (/\d/.test(formatted.charAt(pos))) seen += 1;
      }
      el.setSelectionRange(pos, pos);
    }

    function reformatPrefilledFootprintHotelsRooms() {
      form.querySelectorAll("input.footprint-hotels-rooms:not([readonly])").forEach(function (el) {
        var raw = (el.value || "").trim();
        if (!raw) return;
        var n = parseInt(String(raw).replace(/\D/g, ""), 10);
        if (Number.isFinite(n) && n > 0) el.value = n.toLocaleString("en-US");
      });
    }

    var cachedElements = {
      regionsHidden: form.querySelector("#regions"),
      chainTotalExistingProps: form.querySelector("#chain_total_existing_properties"),
      chainTotalExistingRooms: form.querySelector("#chain_total_existing_rooms"),
      chainTotalPipelineProps: form.querySelector("#chain_total_pipeline_properties"),
      chainTotalPipelineRooms: form.querySelector("#chain_total_pipeline_rooms"),
      chainTotalProps: form.querySelector("#chain_total_properties"),
      chainTotalRooms: form.querySelector("#chain_total_rooms"),
      locationTypeTotal: form.querySelector("#locationTypeTotal"),
      locationTypeValidation: form.querySelector("#locationTypeValidation"),
      brandUnitsStaffingContainer: form.querySelector("#brandUnitsStaffingContainer"),
      brandUnitsGrid: form.querySelector("#brandUnitsGrid"),
      specificMarkets: form.querySelector("#specificMarkets"),
      specificMarketsCount: form.querySelector("#specificMarketsCount"),
    };

    var geoRegions = [
      { key: "na", label: "North America (NA)" },
      { key: "cala", label: "Caribbean & Latin America (CALA)" },
      { key: "eu", label: "Europe (EU)" },
      { key: "mea", label: "Middle East & Africa (MEA)" },
      { key: "apac", label: "Asia Pacific (APAC)" },
    ];

    function getChainScaleBaseFourTotals() {
      return {
        exH: toInt(cachedElements.chainTotalExistingProps && cachedElements.chainTotalExistingProps.value),
        exR: toInt(cachedElements.chainTotalExistingRooms && cachedElements.chainTotalExistingRooms.value),
        piH: toInt(cachedElements.chainTotalPipelineProps && cachedElements.chainTotalPipelineProps.value),
        piR: toInt(cachedElements.chainTotalPipelineRooms && cachedElements.chainTotalPipelineRooms.value),
      };
    }

    function setGeoValue(regionKey, kind, value) {
      var el = form.querySelector('[data-geo="' + regionKey + '"][data-kind="' + kind + '"]');
      if (!el) return;
      var n = Number(value);
      if (el.classList.contains("footprint-hotels-rooms")) {
        el.value = Number.isFinite(n) && n > 0 ? n.toLocaleString("en-US") : "";
      } else {
        el.value = String(value);
      }
    }

    function calculateGeoRow(regionKey) {
      var elExH = form.querySelector('[name="geo_' + regionKey + '_existing_hotels"]');
      var elExR = form.querySelector('[name="geo_' + regionKey + '_existing_rooms"]');
      var elPiH = form.querySelector('[name="geo_' + regionKey + '_pipeline_hotels"]');
      var elPiR = form.querySelector('[name="geo_' + regionKey + '_pipeline_rooms"]');
      var existingHotels = toInt(elExH && elExH.value);
      var existingRooms = toInt(elExR && elExR.value);
      var pipelineHotels = toInt(elPiH && elPiH.value);
      var pipelineRooms = toInt(elPiR && elPiR.value);

      setGeoValue(regionKey, "total_hotels", existingHotels + pipelineHotels);
      setGeoValue(regionKey, "total_rooms", existingRooms + pipelineRooms);

      return { existingHotels: existingHotels, existingRooms: existingRooms, pipelineHotels: pipelineHotels, pipelineRooms: pipelineRooms };
    }

    function calculateGeoTotals() {
      var totals = {
        existingHotels: 0,
        existingRooms: 0,
        pipelineHotels: 0,
        pipelineRooms: 0,
      };
      var supported = [];

      geoRegions.forEach(function (r) {
        var row = calculateGeoRow(r.key);
        totals.existingHotels += row.existingHotels;
        totals.existingRooms += row.existingRooms;
        totals.pipelineHotels += row.pipelineHotels;
        totals.pipelineRooms += row.pipelineRooms;

        if (row.existingHotels + row.existingRooms + row.pipelineHotels + row.pipelineRooms > 0) {
          supported.push(r.label);
        }
      });

      setGeoValue("total", "existing_hotels", totals.existingHotels);
      setGeoValue("total", "existing_rooms", totals.existingRooms);
      setGeoValue("total", "pipeline_hotels", totals.pipelineHotels);
      setGeoValue("total", "pipeline_rooms", totals.pipelineRooms);
      setGeoValue("total", "total_hotels", totals.existingHotels + totals.pipelineHotels);
      setGeoValue("total", "total_rooms", totals.existingRooms + totals.pipelineRooms);

      if (cachedElements.regionsHidden) {
        cachedElements.regionsHidden.value = supported.join(", ");
      }
    }

    var debouncedCalculateGeoTotals = debounce(calculateGeoTotals, DEBOUNCE_MS);

    reformatPrefilledFootprintHotelsRooms();

    var geoGrid = form.querySelector("#geoGrid");
    if (geoGrid) {
      geoGrid.querySelectorAll('input.footprint-hotels-rooms[data-geo]:not([readonly])').forEach(function (inp) {
        inp.addEventListener("input", function (e) {
          handleFootprintHotelsRoomsInput(e);
          debouncedCalculateGeoTotals();
        });
        inp.addEventListener("change", calculateGeoTotals);
      });
    }

    var chainScaleFields = ["luxury", "upperUpscale", "upscale", "upperMidscale", "midscale", "economy"];

    function calculateChainScaleTotals() {
      var totalExistingProps = 0;
      var totalExistingRooms = 0;
      var totalPipelineProps = 0;
      var totalPipelineRooms = 0;
      var totalProps = 0;
      var totalRooms = 0;
      var activeChainScales = [];

      chainScaleFields.forEach(function (key) {
        var existingProps = toInt(form.querySelector('[name="' + key + 'ExistingProperties"]') && form.querySelector('[name="' + key + 'ExistingProperties"]').value);
        var pipelineProps = toInt(form.querySelector('[name="' + key + 'PipelineProperties"]') && form.querySelector('[name="' + key + 'PipelineProperties"]').value);
        var existingRooms = toInt(form.querySelector('[name="' + key + 'ExistingRooms"]') && form.querySelector('[name="' + key + 'ExistingRooms"]').value);
        var pipelineRooms = toInt(form.querySelector('[name="' + key + 'PipelineRooms"]') && form.querySelector('[name="' + key + 'PipelineRooms"]').value);

        var rowTotalProps = existingProps + pipelineProps;
        var rowTotalRooms = existingRooms + pipelineRooms;

        var propsTotalInput = form.querySelector('[name="' + key + 'Properties"]');
        var roomsTotalInput = form.querySelector('[name="' + key + 'Rooms"]');
        if (propsTotalInput) propsTotalInput.value = formatFootprintHotelsRoomsDisplay(rowTotalProps);
        if (roomsTotalInput) roomsTotalInput.value = formatFootprintHotelsRoomsDisplay(rowTotalRooms);

        var chainScaleKeyToLabel = {
          luxury: "Luxury",
          upperUpscale: "Upper Upscale",
          upscale: "Upscale",
          upperMidscale: "Upper Midscale",
          midscale: "Midscale",
          economy: "Economy",
        };
        if (rowTotalProps + rowTotalRooms > 0 && chainScaleKeyToLabel[key]) {
          activeChainScales.push(chainScaleKeyToLabel[key]);
        }

        totalExistingProps += existingProps;
        totalExistingRooms += existingRooms;
        totalPipelineProps += pipelineProps;
        totalPipelineRooms += pipelineRooms;
        totalProps += rowTotalProps;
        totalRooms += rowTotalRooms;
      });

      if (cachedElements.chainTotalExistingProps)
        cachedElements.chainTotalExistingProps.value = formatFootprintHotelsRoomsDisplay(totalExistingProps);
      if (cachedElements.chainTotalExistingRooms)
        cachedElements.chainTotalExistingRooms.value = formatFootprintHotelsRoomsDisplay(totalExistingRooms);
      if (cachedElements.chainTotalPipelineProps)
        cachedElements.chainTotalPipelineProps.value = formatFootprintHotelsRoomsDisplay(totalPipelineProps);
      if (cachedElements.chainTotalPipelineRooms)
        cachedElements.chainTotalPipelineRooms.value = formatFootprintHotelsRoomsDisplay(totalPipelineRooms);
      if (cachedElements.chainTotalProps)
        cachedElements.chainTotalProps.value = formatFootprintHotelsRoomsDisplay(totalProps);
      if (cachedElements.chainTotalRooms)
        cachedElements.chainTotalRooms.value = formatFootprintHotelsRoomsDisplay(totalRooms);

      var totalPropsHidden = form.querySelector("#totalPropertiesHidden");
      var totalRoomsHidden = form.querySelector("#totalRoomsHidden");
      var chainScaleHidden = form.querySelector("#chainScaleHidden");
      if (totalPropsHidden) totalPropsHidden.value = totalProps > 0 ? String(totalProps) : "";
      if (totalRoomsHidden) totalRoomsHidden.value = totalRooms > 0 ? String(totalRooms) : "";
      if (chainScaleHidden) chainScaleHidden.value = activeChainScales.join(", ");
    }

    var debouncedCalculateChainScaleTotals = debounce(calculateChainScaleTotals, DEBOUNCE_MS);

    var chainScaleGrid = form.querySelector("#chainScaleGrid");
    if (chainScaleGrid) {
      chainScaleGrid.querySelectorAll("input").forEach(function (inp) {
        if (inp.readOnly) return;
        if (inp.classList.contains("footprint-hotels-rooms")) {
          inp.addEventListener("input", function (e) {
            handleFootprintHotelsRoomsInput(e);
            debouncedCalculateChainScaleTotals();
          });
        } else {
          inp.addEventListener("input", debouncedCalculateChainScaleTotals);
        }
        inp.addEventListener("change", calculateChainScaleTotals);
      });
      calculateChainScaleTotals();
    }

    var locationTypeFields = [
      "locationTypeUrban",
      "locationTypeSuburban",
      "locationTypeResort",
      "locationTypeAirport",
      "locationTypeSmallMetro",
      "locationTypeInterstate",
    ];

    function updateLocationTypeTotal() {
      var locationTypeElements = locationTypeFields
        .map(function (id) {
          return form.querySelector("#" + id);
        })
        .filter(Boolean);
      var total = locationTypeElements.reduce(function (sum, el) {
        return sum + toFloat(el && el.value);
      }, 0);
      var totalEl = cachedElements.locationTypeTotal;
      var validationEl = cachedElements.locationTypeValidation;
      if (totalEl) totalEl.value = total.toFixed(2);

      if (validationEl) {
        if (total > 0 && Math.abs(total - 100) > 0.01) {
          validationEl.classList.remove("hidden");
          validationEl.classList.add("show");
          validationEl.style.color = "var(--system--300, #ff5a65)";
          validationEl.textContent = "Total is " + total.toFixed(2) + "%. Must equal 100%.";
          locationTypeElements.forEach(function (el) {
            if (el) el.setCustomValidity("Location type percentages must total 100%");
          });
        } else {
          validationEl.classList.add("hidden");
          validationEl.classList.remove("show");
          locationTypeElements.forEach(function (el) {
            if (el) el.setCustomValidity("");
          });
        }
      }
    }

    var locationTypeElements = locationTypeFields
      .map(function (id) {
        return form.querySelector("#" + id);
      })
      .filter(Boolean);

    locationTypeElements.forEach(function (el) {
      var debouncedUpdateTotal = debounce(updateLocationTypeTotal, DEBOUNCE_MS);
      el.addEventListener("input", debouncedUpdateTotal);
      el.addEventListener("change", updateLocationTypeTotal);
      el.addEventListener("input", function () {
        if (parseFloat(this.value) > 100) {
          this.setCustomValidity("Percentage cannot exceed 100%");
        } else {
          this.setCustomValidity("");
        }
      });
    });
    updateLocationTypeTotal();

    if (cachedElements.specificMarkets && cachedElements.specificMarketsCount) {
      cachedElements.specificMarkets.addEventListener("input", function () {
        cachedElements.specificMarketsCount.textContent = String(this.value.length);
      });
      cachedElements.specificMarketsCount.textContent = String(cachedElements.specificMarkets.value.length);
    }

    calculateGeoTotals();
    calculateChainScaleTotals();
    updateLocationTypeTotal();
  }

  global.initThirdPartyOperatorFootprint = initThirdPartyOperatorFootprint;
})(typeof window !== "undefined" ? window : globalThis);
