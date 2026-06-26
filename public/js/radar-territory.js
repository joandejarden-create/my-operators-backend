/**
 * Radar Brand Territory — open vs conflict zones for a selected brand.
 * Standalone exclusivity / portfolio logic (not tied to White Space scoring).
 */
(function () {
  "use strict";

  var TERRITORY_CONFIG = {
    units: {
      submarket: { label: "Submarket — Franchise-Style Market", minHotels: 2 },
      market: { label: "Market — City / Corridor", minHotels: 2 },
      country: { label: "Country — Portfolio / Regional", minHotels: 1 },
      radius: { label: "Radius — Site-Specific (Pin)", minHotels: 1 },
    },
    radiusKmOptions: [3, 5, 10, 15, 25, 50],
    defaultUnit: "submarket",
    defaultRadiusKm: 10,
    maxMarkers: 60,
    countryMaxMarkers: 120,
    colors: {
      open: "#4CAF50",
      review_same: "#f59e0b",
      review_adjacent: "#ffeb3b",
      caution: "#ffeb3b",
      conflict: "#ef4444",
    },
    pipelineAlwaysConflict: true,
    sameParentPipelineIsConflict: false,
    showConflictOnMap: false,
    sisterReview: {
      reviewSameScale: true,
      reviewAdjacentScale: true,
      adjacentScaleDistance: 1,
      unknownScaleTreatment: "review",
      includeDistantInPopup: true,
    },
  };

  var CHAIN_SCALE_ORDER = [
    "Luxury",
    "Upper Upscale",
    "Upscale",
    "Upper Midscale",
    "Midscale",
    "Economy",
    "Extended Stay",
    "Select Service",
    "Independant",
    "Independent",
  ];

  function normalizeText(value) {
    return String(value || "")
      .trim()
      .replace(/\s+/g, " ");
  }

  function normalizeKey(value) {
    return normalizeText(value).toLowerCase();
  }

  function brandsMatch(a, b) {
    return normalizeKey(a) === normalizeKey(b) && !!normalizeKey(a);
  }

  function parentsMatch(a, b) {
    var ka = normalizeKey(a);
    var kb = normalizeKey(b);
    if (!ka || !kb || ka === "unknown" || ka === "independent") return false;
    if (!kb || kb === "unknown" || kb === "independent") return false;
    return ka === kb;
  }

  function isOpenHotel(hotel) {
    return normalizeKey(hotel && hotel.status) === "open";
  }

  function isPipelineHotel(hotel) {
    return normalizeKey(hotel && hotel.status) === "pipeline";
  }

  function parseRooms(hotel) {
    var n = parseInt(hotel && hotel.rooms, 10);
    return Number.isFinite(n) && n > 0 ? n : 0;
  }

  function haversineKm(lat1, lng1, lat2, lng2) {
    var toRad = Math.PI / 180;
    var dLat = (lat2 - lat1) * toRad;
    var dLng = (lng2 - lng1) * toRad;
    var a =
      Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(lat1 * toRad) * Math.cos(lat2 * toRad) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
    return 6371 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  }

  function hotelCoords(hotel) {
    var lat = parseFloat(hotel && hotel.lat);
    var lng = parseFloat(hotel && hotel.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lng) || (lat === 0 && lng === 0)) return null;
    return { lat: lat, lng: lng };
  }

  function getChainScale(hotel) {
    return normalizeText(hotel && (hotel.chainScale || hotel.propertyType));
  }

  function chainScaleSortIndex(label) {
    var raw = String(label || "").trim();
    var key = raw.replace(/\s+chain\s*$/i, "").trim() || raw;
    var normalized = key.toLowerCase();
    for (var i = 0; i < CHAIN_SCALE_ORDER.length; i++) {
      var option = CHAIN_SCALE_ORDER[i].toLowerCase();
      if (normalized === option || normalized.indexOf(option + " ") === 0) {
        return i;
      }
    }
    return CHAIN_SCALE_ORDER.length + 1;
  }

  function chainScaleGap(subjectScale, sisterScale) {
    if (!subjectScale || !sisterScale) return null;
    if (subjectScale === "Unknown" || sisterScale === "Unknown") return null;
    var subjectIndex = chainScaleSortIndex(subjectScale);
    var sisterIndex = chainScaleSortIndex(sisterScale);
    if (
      subjectIndex > CHAIN_SCALE_ORDER.length ||
      sisterIndex > CHAIN_SCALE_ORDER.length
    ) {
      return null;
    }
    return Math.abs(subjectIndex - sisterIndex);
  }

  function classifySisterScaleProximity(subjectScale, sisterScale) {
    var reviewConfig = TERRITORY_CONFIG.sisterReview || {};
    var gap = chainScaleGap(subjectScale, sisterScale);
    if (gap === null) {
      return reviewConfig.unknownScaleTreatment === "distant" ? "distant" : "review";
    }
    if (gap === 0) return "same";
    if (gap <= (reviewConfig.adjacentScaleDistance || 1)) return "adjacent";
    return "distant";
  }

  function shouldReviewSisterProximity(proximity) {
    var reviewConfig = TERRITORY_CONFIG.sisterReview || {};
    if (proximity === "same") return reviewConfig.reviewSameScale !== false;
    if (proximity === "adjacent") return reviewConfig.reviewAdjacentScale !== false;
    if (proximity === "review") return true;
    return false;
  }

  function resolveSubjectChainScale(hotels, subjectBrand) {
    var scaleCounts = {};
    (hotels || []).forEach(function (hotel) {
      if (!brandsMatch(hotel.brand, subjectBrand)) return;
      var scale = getChainScale(hotel);
      if (!scale || scale === "Unknown") return;
      scaleCounts[scale] = (scaleCounts[scale] || 0) + 1;
    });
    var scales = Object.keys(scaleCounts);
    if (!scales.length) return "";
    scales.sort(function (a, b) {
      return scaleCounts[b] - scaleCounts[a];
    });
    return scales[0];
  }

  function sisterHotelLabel(name, sisterBrand, statusWord, sisterScale, proximity, subjectScale) {
    var label = name + " · " + sisterBrand + " (" + statusWord + ")";
    if (sisterScale) label += " · " + sisterScale;
    if (proximity === "same") {
      label += " · same chain scale as subject";
    } else if (proximity === "adjacent") {
      label +=
        " · adjacent chain scale to subject" +
        (subjectScale ? " (" + subjectScale + ")" : "");
    } else if (proximity === "distant") {
      label +=
        " · distant chain scale from subject" +
        (subjectScale ? " (" + subjectScale + ")" : "");
    } else {
      label += " · chain scale unknown — review";
    }
    return label;
  }

  function trackSisterBrandCount(map, brandName) {
    var key = normalizeText(brandName) || "Sister brand";
    map[key] = (map[key] || 0) + 1;
  }

  function finalizeSisterReviewStats(result) {
    if (result.sisterSameScaleOpen + result.sisterSameScalePipeline > 0) {
      result.reviewIntensity = "same";
    } else if (
      result.sisterAdjacentScaleOpen + result.sisterAdjacentScalePipeline > 0
    ) {
      result.reviewIntensity = "adjacent";
    } else {
      result.reviewIntensity = "none";
    }
    result.sameParentOpen =
      result.sisterSameScaleOpen +
      result.sisterAdjacentScaleOpen +
      result.sisterUnknownScaleOpen;
    result.sameParentPipeline =
      result.sisterSameScalePipeline +
      result.sisterAdjacentScalePipeline +
      result.sisterUnknownScalePipeline;
    result.sisterOpenBrands = Object.assign({}, result.sisterReviewOpenBrands);
    result.sisterPipelineBrands = Object.assign({}, result.sisterReviewPipelineBrands);
    result.sisterOpenBrandCount = Object.keys(result.sisterReviewOpenBrands).length;
    result.sisterPipelineBrandCount = Object.keys(result.sisterReviewPipelineBrands).length;
    return result;
  }

  function resolveSubmarketLabel(hotel) {
    return (
      normalizeText(hotel.dealalitySubmarket) ||
      normalizeText(hotel.submarket) ||
      normalizeText(hotel.market) ||
      ""
    );
  }

  function bucketKeyForHotel(hotel, unit) {
    var country = normalizeText(hotel.country);
    var market = normalizeText(hotel.market);
    var submarket = resolveSubmarketLabel(hotel);
    if (!country) return null;

    if (unit === "country") return country + "|country";
    if (unit === "market") {
      if (!market) return null;
      return country + "|market|" + market;
    }
    if (unit === "submarket") {
      if (!submarket) return null;
      return country + "|submarket|" + market + "|" + submarket;
    }
    return null;
  }

  function bucketLabelFromKey(key, unit) {
    var parts = String(key || "").split("|");
    if (unit === "country") return parts[0] || "Unknown";
    if (unit === "market") return parts[2] || parts[0] || "Unknown";
    if (unit === "submarket") {
      var sub = parts[3] || parts[2] || "Unknown";
      var mkt = parts[2] && parts[3] ? parts[2] : "";
      return mkt && mkt !== sub ? sub + " · " + mkt : sub;
    }
    return "Territory";
  }

  function aggregateTerritoryBuckets(hotels, unit) {
    var map = {};
    (hotels || []).forEach(function (hotel) {
      var key = bucketKeyForHotel(hotel, unit);
      if (!key) return;
      if (!map[key]) {
        map[key] = {
          id: key,
          unit: unit,
          label: bucketLabelFromKey(key, unit),
          country: normalizeText(hotel.country),
          market: normalizeText(hotel.market),
          submarket: resolveSubmarketLabel(hotel),
          hotels: [],
          latSum: 0,
          lngSum: 0,
          coordCount: 0,
        };
      }
      var bucket = map[key];
      bucket.hotels.push(hotel);
      var coords = hotelCoords(hotel);
      if (coords) {
        bucket.latSum += coords.lat;
        bucket.lngSum += coords.lng;
        bucket.coordCount += 1;
      }
    });
    return map;
  }

  function analyzeBrandConflicts(hotels, subjectBrand, subjectParent, subjectChainScale) {
    var result = {
      conflicts: [],
      cautions: [],
      portfolioNotes: [],
      sameBrandOpen: 0,
      sameBrandPipeline: 0,
      sisterSameScaleOpen: 0,
      sisterSameScalePipeline: 0,
      sisterAdjacentScaleOpen: 0,
      sisterAdjacentScalePipeline: 0,
      sisterUnknownScaleOpen: 0,
      sisterUnknownScalePipeline: 0,
      distantSisterOpen: 0,
      distantSisterPipeline: 0,
      sisterReviewOpenBrands: {},
      sisterReviewPipelineBrands: {},
      sisterDistantOpenBrands: {},
      sisterDistantPipelineBrands: {},
      subjectChainScale: subjectChainScale || "",
      reviewIntensity: "none",
      hasHardConflict: false,
    };

    (hotels || []).forEach(function (hotel) {
      var name = normalizeText(hotel.name) || "Hotel";
      if (brandsMatch(hotel.brand, subjectBrand)) {
        if (isPipelineHotel(hotel)) {
          result.sameBrandPipeline += 1;
          result.conflicts.push({
            type: "same_brand_pipeline",
            label: name + " (pipeline)",
            hotel: hotel,
          });
        } else if (isOpenHotel(hotel)) {
          result.sameBrandOpen += 1;
          result.conflicts.push({
            type: "same_brand_open",
            label: name + " (open)",
            hotel: hotel,
          });
        }
        return;
      }

      if (!subjectParent || !parentsMatch(hotel.parentCompany, subjectParent)) {
        return;
      }

      var sisterBrand = normalizeText(hotel.brand) || "Sister brand";
      var sisterScale = getChainScale(hotel);
      var proximity = classifySisterScaleProximity(subjectChainScale, sisterScale);
      var statusWord = isPipelineHotel(hotel) ? "pipeline" : "open";
      var entry = {
        type: proximity === "distant" ? "same_parent_distant_" + statusWord : "same_parent_" + statusWord,
        label: sisterHotelLabel(
          name,
          sisterBrand,
          statusWord,
          sisterScale,
          proximity,
          subjectChainScale
        ),
        hotel: hotel,
        sisterBrand: sisterBrand,
        sisterScale: sisterScale,
        proximity: proximity,
      };

      if (isPipelineHotel(hotel)) {
        if (
          TERRITORY_CONFIG.sameParentPipelineIsConflict &&
          shouldReviewSisterProximity(proximity) &&
          proximity !== "distant"
        ) {
          result.conflicts.push(entry);
          return;
        }

        if (proximity === "distant") {
          result.distantSisterPipeline += 1;
          trackSisterBrandCount(result.sisterDistantPipelineBrands, sisterBrand);
          if (TERRITORY_CONFIG.sisterReview.includeDistantInPopup !== false) {
            result.portfolioNotes.push(entry);
          }
          return;
        }

        if (proximity === "same") result.sisterSameScalePipeline += 1;
        else if (proximity === "adjacent") result.sisterAdjacentScalePipeline += 1;
        else result.sisterUnknownScalePipeline += 1;

        if (shouldReviewSisterProximity(proximity)) {
          trackSisterBrandCount(result.sisterReviewPipelineBrands, sisterBrand);
          result.cautions.push(entry);
        }
        return;
      }

      if (!isOpenHotel(hotel)) return;

      if (proximity === "distant") {
        result.distantSisterOpen += 1;
        trackSisterBrandCount(result.sisterDistantOpenBrands, sisterBrand);
        if (TERRITORY_CONFIG.sisterReview.includeDistantInPopup !== false) {
          result.portfolioNotes.push(entry);
        }
        return;
      }

      if (proximity === "same") result.sisterSameScaleOpen += 1;
      else if (proximity === "adjacent") result.sisterAdjacentScaleOpen += 1;
      else result.sisterUnknownScaleOpen += 1;

      if (shouldReviewSisterProximity(proximity)) {
        trackSisterBrandCount(result.sisterReviewOpenBrands, sisterBrand);
        result.cautions.push(entry);
      }
    });

    result.hasHardConflict = result.conflicts.length > 0;
    return finalizeSisterReviewStats(result);
  }

  function resolveTerritoryStatus(conflictResult) {
    if (conflictResult.hasHardConflict) {
      return {
        status: "conflict",
        mapVisible: TERRITORY_CONFIG.showConflictOnMap,
      };
    }
    if (conflictResult.reviewIntensity === "same") {
      return { status: "review_same", mapVisible: true };
    }
    if (conflictResult.cautions.length > 0) {
      return { status: "review_adjacent", mapVisible: true };
    }
    return { status: "open", mapVisible: true };
  }

  function analyzeTerritoryBucket(bucket, options) {
    var unit = options.unit;
    var subjectBrand = options.subjectBrand;
    var subjectParent = options.subjectParent;
    var minHotels = (TERRITORY_CONFIG.units[unit] && TERRITORY_CONFIG.units[unit].minHotels) || 2;

    if (!bucket || bucket.hotels.length < minHotels) {
      return null;
    }

    var conflictResult = analyzeBrandConflicts(
      bucket.hotels,
      subjectBrand,
      subjectParent,
      options.subjectChainScale
    );
    var statusInfo = resolveTerritoryStatus(conflictResult);

    if (!statusInfo.mapVisible) {
      return null;
    }

    return {
      id: bucket.id,
      unit: unit,
      label: bucket.label,
      country: bucket.country,
      market: bucket.market,
      submarket: bucket.submarket,
      lat: bucket.coordCount ? bucket.latSum / bucket.coordCount : null,
      lng: bucket.coordCount ? bucket.lngSum / bucket.coordCount : null,
      status: statusInfo.status,
      subjectBrand: subjectBrand,
      subjectParent: subjectParent,
      subjectChainScale: options.subjectChainScale || "",
      conflictResult: conflictResult,
      hotelCount: bucket.hotels.length,
      openRooms: bucket.hotels.filter(isOpenHotel).reduce(function (sum, h) {
        return sum + parseRooms(h);
      }, 0),
    };
  }

  function hotelsWithinRadius(hotels, pin, radiusKm) {
    if (!pin || pin.lat == null || pin.lng == null) return [];
    return (hotels || []).filter(function (hotel) {
      var coords = hotelCoords(hotel);
      if (!coords) return false;
      return haversineKm(pin.lat, pin.lng, coords.lat, coords.lng) <= radiusKm;
    });
  }

  function analyzeRadiusTerritory(hotels, options) {
    var pin = options.pin;
    var radiusKm = options.radiusKm || TERRITORY_CONFIG.defaultRadiusKm;
    if (!pin || pin.lat == null || pin.lng == null) return null;

    var inRadius = hotelsWithinRadius(hotels, pin, radiusKm);
    var bucket = {
      id: "radius|" + pin.lat.toFixed(4) + "|" + pin.lng.toFixed(4) + "|" + radiusKm,
      unit: "radius",
      label: formatRadiusKmMiLabel(radiusKm) + " Radius",
      country: "",
      market: "",
      submarket: "",
      hotels: inRadius,
      latSum: pin.lat,
      lngSum: pin.lng,
      coordCount: 1,
    };

    var conflictResult = analyzeBrandConflicts(
      inRadius,
      options.subjectBrand,
      options.subjectParent,
      options.subjectChainScale
    );
    var statusInfo = resolveTerritoryStatus(conflictResult);

    return {
      id: bucket.id,
      unit: "radius",
      label: bucket.label,
      country: bucket.country,
      market: bucket.market,
      submarket: bucket.submarket,
      lat: pin.lat,
      lng: pin.lng,
      status: statusInfo.status,
      mapEligible: statusInfo.mapVisible,
      subjectBrand: options.subjectBrand,
      subjectParent: options.subjectParent,
      subjectChainScale: options.subjectChainScale || "",
      conflictResult: conflictResult,
      hotelCount: inRadius.length,
      openRooms: inRadius.filter(isOpenHotel).reduce(function (sum, h) {
        return sum + parseRooms(h);
      }, 0),
      radiusKm: radiusKm,
      pin: pin,
    };
  }

  function computeTerritories(hotels, options) {
    options = options || {};
    var unit = options.unit || TERRITORY_CONFIG.defaultUnit;
    var subjectBrand = normalizeText(options.subjectBrand);
    if (!subjectBrand) {
      return { error: "Select a Subject Brand to scan territories.", territories: [] };
    }

    var subjectParent =
      normalizeText(options.subjectParent) ||
      (function () {
        var match = (hotels || []).find(function (h) {
          return brandsMatch(h.brand, subjectBrand);
        });
        return match ? normalizeText(match.parentCompany) : "";
      })();

    var subjectChainScale =
      normalizeText(options.subjectChainScale) ||
      resolveSubjectChainScale(hotels, subjectBrand);

    var filters = Object.assign({}, options.filters || {}, { brand: subjectBrand });
    var territories = [];

    if (unit === "radius") {
      var radiusTerritory = analyzeRadiusTerritory(hotels, {
        pin: options.pin,
        radiusKm: options.radiusKm || TERRITORY_CONFIG.defaultRadiusKm,
        subjectBrand: subjectBrand,
        subjectParent: subjectParent,
        subjectChainScale: subjectChainScale,
        filters: filters,
      });
      if (radiusTerritory) territories.push(radiusTerritory);
      return {
        territories: territories,
        subjectBrand: subjectBrand,
        subjectParent: subjectParent,
        subjectChainScale: subjectChainScale,
        unit: unit,
      };
    }

    var buckets = aggregateTerritoryBuckets(hotels, unit);
    Object.keys(buckets).forEach(function (key) {
      var territory = analyzeTerritoryBucket(buckets[key], {
        unit: unit,
        subjectBrand: subjectBrand,
        subjectParent: subjectParent,
        subjectChainScale: subjectChainScale,
      });
      if (territory) territories.push(territory);
    });

    territories.sort(function (a, b) {
      var rank = { open: 4, review_adjacent: 3, review_same: 2, caution: 2, conflict: 1 };
      var diff = (rank[b.status] || 0) - (rank[a.status] || 0);
      if (diff !== 0) return diff;
      return (b.openRooms || 0) - (a.openRooms || 0);
    });

    return {
      territories: territories.slice(
        0,
        unit === "country" ? TERRITORY_CONFIG.countryMaxMarkers : TERRITORY_CONFIG.maxMarkers
      ),
      subjectBrand: subjectBrand,
      subjectParent: subjectParent,
      subjectChainScale: subjectChainScale,
      unit: unit,
    };
  }

  function escHtml(value) {
    return String(value != null ? value : "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function statusTitle(status, conflictResult) {
    if (status === "open") return "Open Territory";
    if (status === "review_same" || (status === "caution" && conflictResult && conflictResult.reviewIntensity === "same")) {
      return "Review — Same Chain Scale";
    }
    if (status === "review_adjacent" || status === "caution") {
      return "Review — Adjacent Chain Scale";
    }
    return "Conflict";
  }

  function formatHotelCount(count, label) {
    return count + " " + label + (count === 1 ? "" : "s");
  }

  function formatSisterBrandSummary(hotelCount, brandMap, statusWord, conflictResult) {
    var brandNames = Object.keys(brandMap || {}).sort();
    var brandCount = brandNames.length;
    if (!hotelCount || !brandCount) return "";

    var sampleBrands = brandNames.slice(0, 3).map(escHtml).join(", ");
    var moreBrands = brandCount > 3 ? " +" + (brandCount - 3) + " more" : "";
    var intensity = conflictResult && conflictResult.reviewIntensity;
    var scaleNote =
      intensity === "same"
        ? " at the same chain scale"
        : intensity === "adjacent"
          ? " at an adjacent chain scale"
          : "";

    return (
      formatHotelCount(hotelCount, statusWord + " Sister-Brand Hotel") +
      scaleNote +
      " Across " +
      formatHotelCount(brandCount, "Sister Brand") +
      (sampleBrands ? " (" + sampleBrands + moreBrands + ")" : "") +
      " — Review."
    );
  }

  function buildPopupHtml(territory) {
    var color = statusColor(territory.status);
    var conflict = territory.conflictResult || { conflicts: [], cautions: [], portfolioNotes: [] };
    var facts = [];

    facts.push(
      "<strong>Subject Brand:</strong> " +
        escHtml(territory.subjectBrand) +
        (territory.subjectParent ? " (" + escHtml(territory.subjectParent) + ")" : "")
    );
    if (conflict.subjectChainScale || territory.subjectChainScale) {
      facts.push(
        "<strong>Subject Chain Scale:</strong> " +
          escHtml(conflict.subjectChainScale || territory.subjectChainScale)
      );
    }
    facts.push(
      "<strong>Territory Supply:</strong> " +
        territory.hotelCount.toLocaleString() +
        " hotels · " +
        territory.openRooms.toLocaleString() +
        " open keys"
    );

    if (conflict.sameBrandOpen > 0) {
      facts.push(formatHotelCount(conflict.sameBrandOpen, "same-brand open hotel") + " — conflict.");
    }
    if (conflict.sameBrandPipeline > 0) {
      facts.push(formatHotelCount(conflict.sameBrandPipeline, "same-brand pipeline hotel") + " — conflict.");
    }
    if (conflict.sameParentOpen > 0) {
      facts.push(
        formatSisterBrandSummary(
          conflict.sameParentOpen,
          conflict.sisterOpenBrands,
          "open",
          conflict
        )
      );
    }
    if (conflict.sameParentPipeline > 0 && !TERRITORY_CONFIG.sameParentPipelineIsConflict) {
      facts.push(
        formatSisterBrandSummary(
          conflict.sameParentPipeline,
          conflict.sisterPipelineBrands,
          "pipeline",
          conflict
        )
      );
    }
    if (conflict.distantSisterOpen + conflict.distantSisterPipeline > 0) {
      facts.push(
        formatHotelCount(
          conflict.distantSisterOpen + conflict.distantSisterPipeline,
          "sister-brand hotel"
        ) +
          " at distant chain scales — informational only (does not trigger review)."
      );
    }

    if (territory.unit === "country" && conflict.sameBrandOpen === 0 && conflict.sameBrandPipeline === 0) {
      facts.push("No " + escHtml(territory.subjectBrand) + " open or pipeline hotels in this country.");
    } else if (territory.status === "open") {
      if (conflict.distantSisterOpen + conflict.distantSisterPipeline > 0) {
        facts.push("No same-brand conflict. Sister brands are only at distant chain scales.");
      } else {
        facts.push("No same-brand open or pipeline hotels in this territory.");
      }
    }

    if (territory.unit === "radius" && territory.radiusKm) {
      facts.push(formatRadiusKmMiLabel(territory.radiusKm) + " radius from dropped pin.");
    }

    var conflictLines = conflict.conflicts
      .slice(0, 4)
      .map(function (c) {
        return "<li>" + escHtml(c.label) + "</li>";
      })
      .join("");
    var cautionLines = conflict.cautions
      .slice(0, 4)
      .map(function (c) {
        return "<li>" + escHtml(c.label) + "</li>";
      })
      .join("");
    var cautionMore =
      conflict.cautions.length > 4
        ? '<p class="radar-territory-popup__more">+' +
          (conflict.cautions.length - 4) +
          " More Sister-Brand Hotels to Review</p>"
        : "";
    var portfolioLines = (conflict.portfolioNotes || [])
      .slice(0, 4)
      .map(function (note) {
        return "<li>" + escHtml(note.label) + "</li>";
      })
      .join("");
    var portfolioMore =
      (conflict.portfolioNotes || []).length > 4
        ? '<p class="radar-territory-popup__more">+' +
          ((conflict.portfolioNotes || []).length - 4) +
          " More Distant Sister-Brand Hotels</p>"
        : "";

    return (
      '<div class="radar-territory-popup">' +
      '<div class="radar-territory-popup__title">' +
      escHtml(territory.label) +
      "</div>" +
      (territory.country
        ? '<div class="radar-territory-popup__geo">' +
          escHtml(territory.country) +
          (territory.market ? " · " + escHtml(territory.market) : "") +
          "</div>"
        : "") +
      '<div class="radar-territory-popup__status" style="border-left-color:' +
      color +
      ';">' +
      '<div class="radar-territory-popup__status-title" style="color:' +
      color +
      ';">' +
      escHtml(statusTitle(territory.status, conflict)) +
      "</div>" +
      '<div class="radar-territory-popup__status-meta">Unit: ' +
      escHtml((TERRITORY_CONFIG.units[territory.unit] || {}).label || territory.unit) +
      "</div></div>" +
      '<ul class="radar-territory-popup__facts">' +
      facts
        .map(function (f) {
          return "<li>" + f + "</li>";
        })
        .join("") +
      "</ul>" +
      (conflictLines
        ? '<div class="radar-territory-popup__section-title">Conflicts</div><ul class="radar-territory-popup__list">' +
          conflictLines +
          "</ul>"
        : "") +
      (cautionLines
        ? '<div class="radar-territory-popup__section-title">Review — Chain-Scale Overlap</div><ul class="radar-territory-popup__list">' +
          cautionLines +
          "</ul>" +
          cautionMore
        : "") +
      (portfolioLines
        ? '<div class="radar-territory-popup__section-title">Portfolio Note — Distant Chain Scale</div><ul class="radar-territory-popup__list">' +
          portfolioLines +
          "</ul>" +
          portfolioMore
        : "") +
      "</div>"
    );
  }

  function statusColor(status) {
    if (status === "review_same") return TERRITORY_CONFIG.colors.review_same;
    if (status === "review_adjacent") return TERRITORY_CONFIG.colors.review_adjacent;
    return TERRITORY_CONFIG.colors[status] || TERRITORY_CONFIG.colors.review_adjacent;
  }

  function kmToMiles(km) {
    return Math.round(Number(km) * 0.621371);
  }

  function formatRadiusKmMiLabel(km) {
    var kmVal = Math.max(1, Math.round(Number(km) || 1));
    var miVal = kmToMiles(kmVal);
    return kmVal + " km / " + miVal + " mi";
  }

  function statusTextColor(status) {
    if (status === "open") return "#15803d";
    if (status === "review_same") return "#b45309";
    if (status === "review_adjacent" || status === "caution") return "#a16207";
    if (status === "conflict") return "#dc2626";
    return "#6c72ff";
  }

  function buildRadiusResultSummary(territory) {
    if (!territory) return "";
    var conflict = territory.conflictResult || {};
    if (territory.status === "open") {
      if (conflict.distantSisterOpen + conflict.distantSisterPipeline > 0) {
        return "Open — sister brands only at distant chain scales.";
      }
      return "No same-brand conflict in this radius.";
    }
    if (territory.status === "conflict") {
      if (conflict.sameBrandOpen > 0) {
        return (
          formatHotelCount(conflict.sameBrandOpen, "same-brand open hotel") + " in this radius."
        );
      }
      if (conflict.sameBrandPipeline > 0) {
        return (
          formatHotelCount(conflict.sameBrandPipeline, "same-brand pipeline hotel") +
          " in this radius."
        );
      }
      return "Same-brand conflict in this radius.";
    }
    var sisterHotels = (conflict.sameParentOpen || 0) + (conflict.sameParentPipeline || 0);
    if (territory.status === "review_same") {
      return (
        formatHotelCount(sisterHotels, "same-chain-scale sister-brand hotel") +
        " in this radius."
      );
    }
    if (territory.status === "review_adjacent" || territory.status === "caution") {
      if (sisterHotels > 0) {
        return (
          formatHotelCount(sisterHotels, "adjacent-chain-scale sister-brand hotel") +
          " in this radius."
        );
      }
      return "Sister-brand overlap at an adjacent chain scale — review recommended.";
    }
    return "";
  }

  function buildRadiusResultTileOptions(territory, options) {
    options = options || {};
    var radiusKm = options.radiusKm || (territory && territory.radiusKm) || 10;
    var statusKey = territory ? territory.status : "pending";
    var color = territory ? statusColor(territory.status) : "#6c72ff";
    var textColor = territory ? statusTextColor(territory.status) : "#6c72ff";
    var statusLabel;
    var summary;

    if (!territory) {
      statusLabel = "Site Pin";
      summary = options.awaitingBrand
        ? formatRadiusKmMiLabel(radiusKm) + " · select brand to scan"
        : formatRadiusKmMiLabel(radiusKm) + " · scanning…";
      statusKey = "pending";
      color = "#6c72ff";
    } else {
      statusLabel = statusTitle(territory.status, territory.conflictResult);
      summary = buildRadiusResultSummary(territory);
    }

    var html =
      '<div class="radar-territory-result-pin">' +
      '<div class="radar-territory-result-tile radar-territory-result-tile--' +
      escHtml(statusKey) +
      '" style="border-left-color:' +
      color +
      ';">' +
      '<div class="radar-territory-result-tile__status" style="color:' +
      textColor +
      ';">' +
      escHtml(statusLabel) +
      "</div>" +
      '<div class="radar-territory-result-tile__summary">' +
      escHtml(summary) +
      "</div>" +
      "</div>" +
      '<div class="radar-territory-result-tile__stem" style="background:' +
      color +
      ';"></div>' +
      '<div class="radar-territory-result-tile__dot" style="background:' +
      color +
      ';"></div>' +
      "</div>";

    return {
      html: html,
      iconSize: [196, 78],
      iconAnchor: [98, 78],
      className: "radar-territory-result-pin-icon",
      color: color,
    };
  }

  window.RadarTerritory = {
    TERRITORY_CONFIG: TERRITORY_CONFIG,
    computeTerritories: computeTerritories,
    buildPopupHtml: buildPopupHtml,
    buildRadiusResultTileOptions: buildRadiusResultTileOptions,
    formatRadiusKmMiLabel: formatRadiusKmMiLabel,
    kmToMiles: kmToMiles,
    statusColor: statusColor,
    statusTitle: statusTitle,
    hotelsWithinRadius: hotelsWithinRadius,
  };
})();
