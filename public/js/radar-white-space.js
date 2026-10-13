/**
 * Radar White Space — underpenetration scoring for market / submarket opportunities.
 * Same hex map markers (green / yellow / red); popups explain the rating with census facts.
 */
(function () {
  "use strict";

  var DEFAULT_CONFIG = {
    minOpenHotels: 3,
    minBrandedOpenHotels: 2,
    highIndependentOpenHotels: 15,
    maxMarkers: 75,
    minMapOpportunityScore: 18,
    generalBrandedShareHighMax: 0.2,
    generalBrandedShareMediumMax: 0.4,
    strongGapSharePoints: 0.1,
    moderateGapSharePoints: 0.04,
    relativeHighShare: 0.35,
    relativeLowShare: 0.35,
    maxSegmentGaps: 12,
    popupMaxReasons: 5,
    minSegmentBenchmarkGeos: 2,
    minPeerGeos: 3,
    maturityEmergingMax: 0.22,
    maturityDevelopingMax: 0.38,
    minDepthRoomsStrong: 400,
    minDepthHotelsStrong: 8,
    minDepthRoomsThin: 120,
    tierPipelineCrowdRatio: 0.4,
    demandAnchorMatchKm: 35,
    demandAnchorMinForBoost: 2,
    tierFitLadderScales: {
      Luxury: ["Upper Upscale", "Upscale"],
      "Upper Upscale": ["Upscale", "Upper Midscale"],
      Upscale: ["Upper Midscale", "Midscale"],
    },
    minLadderRooms: 120,
    minLadderHotels: 2,
    minPeerScalePresenceGeos: 2,
    tierFitUnsupportedGapMultiplier: 0.3,
    tierFitSpeculativeGapMultiplier: 0.65,
    colors: {
      high: "#4CAF50",
      medium: "#ffeb3b",
      low: "#f44336",
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

  function sortSegmentsByChainScale(segments) {
    return (segments || []).slice().sort(function (a, b) {
      return chainScaleSortIndex(a.segment) - chainScaleSortIndex(b.segment);
    });
  }

  function normalizeText(value) {
    return String(value || "")
      .trim()
      .replace(/\s+/g, " ");
  }

  function normalizeKey(value) {
    return normalizeText(value).toLowerCase();
  }

  function isOpenHotel(hotel) {
    return normalizeKey(hotel && hotel.status) === "open";
  }

  function isPipelineHotel(hotel) {
    return normalizeKey(hotel && hotel.status) === "pipeline";
  }

  function isIndependentHotel(hotel) {
    var brand = normalizeKey(hotel && hotel.brand);
    var parent = normalizeKey(hotel && hotel.parentCompany);
    return brand === "independent" || parent === "independent";
  }

  function getChainScale(hotel) {
    return normalizeText(hotel && (hotel.chainScale || hotel.propertyType));
  }

  function parseRooms(hotel) {
    var n = parseInt(hotel && hotel.rooms, 10);
    return Number.isFinite(n) && n > 0 ? n : 0;
  }

  function formatPct(value) {
    return (Math.round(value * 1000) / 10).toFixed(1);
  }

  function formatParentLabel(value) {
    if (!value || value === "Unknown") return "No parent company (blank)";
    return value;
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

  function getMaturityTier(brandedShare, config) {
    if (brandedShare < config.maturityEmergingMax) {
      return { tier: "emerging", label: "Emerging market" };
    }
    if (brandedShare < config.maturityDevelopingMax) {
      return { tier: "developing", label: "Developing market" };
    }
    return { tier: "mature", label: "Mature branded market" };
  }

  function computeCountryMaturityBaselines(buckets, config) {
    var stats = {};

    Object.keys(buckets || {}).forEach(function (key) {
      var bucket = buckets[key];
      var metrics = computeGeoMetrics(bucket, {}, "general");
      if (!metrics || !bucket.country) return;
      var countryKey = normalizeKey(bucket.country);
      if (!stats[countryKey]) {
        stats[countryKey] = {
          country: bucket.country,
          brandedRooms: 0,
          totalRooms: 0,
          openHotels: 0,
          geoCount: 0,
        };
      }
      var row = stats[countryKey];
      row.brandedRooms += metrics.brandedOpenRooms;
      row.totalRooms += metrics.openRooms;
      row.openHotels += metrics.openHotels;
      row.geoCount += 1;
    });

    var out = {};
    Object.keys(stats).forEach(function (countryKey) {
      var row = stats[countryKey];
      var brandedShare = row.totalRooms ? row.brandedRooms / row.totalRooms : 0;
      var tierInfo = getMaturityTier(brandedShare, config);
      out[countryKey] = {
        country: row.country,
        brandedShare: brandedShare,
        tier: tierInfo.tier,
        tierLabel: tierInfo.label,
        openHotels: row.openHotels,
        openRooms: row.totalRooms,
        geoCount: row.geoCount,
      };
    });
    return out;
  }

  function assessMarketDepth(metrics, config) {
    if (
      metrics.openRooms >= config.minDepthRoomsStrong &&
      metrics.openHotels >= config.minDepthHotelsStrong
    ) {
      return { tier: "deep", label: "Deep market", scoreBoost: 8 };
    }
    if (metrics.openRooms < config.minDepthRoomsThin || metrics.openHotels < config.minOpenHotels) {
      return { tier: "thin", label: "Thin market", scoreBoost: -8, capStrong: true };
    }
    return { tier: "moderate", label: "Moderate depth", scoreBoost: 0 };
  }

  var LUXURY_FRIENDLY_ANCHOR_TYPES = {
    "beach / waterfront": true,
    "tourist attraction": true,
    "entertainment district": true,
    "convention center": true,
    "sports venue": true,
    "mixed-use development": true,
  };

  function isUpperChainScale(scale) {
    return chainScaleSortIndex(scale) <= chainScaleSortIndex("Upscale");
  }

  function countLuxuryFriendlyAnchors(demandContext) {
    var count = 0;
    (demandContext && demandContext.matches ? demandContext.matches : []).forEach(function (match) {
      var pointType = normalizeKey(match.pointType || match.name);
      if (LUXURY_FRIENDLY_ANCHOR_TYPES[pointType]) count += 1;
    });
    if (!count && demandContext && demandContext.pointTypes) {
      demandContext.pointTypes.forEach(function (pointType) {
        if (LUXURY_FRIENDLY_ANCHOR_TYPES[normalizeKey(pointType)]) count += 1;
      });
    }
    return count;
  }

  function marketSkewsBelowTier(openHotels, focusScale) {
    var leaders = topChainScaleByRooms(openHotels, 1);
    if (!leaders.length) return false;
    var leaderScale = String(leaders[0]).split(" (")[0];
    return chainScaleSortIndex(leaderScale) > chainScaleSortIndex(focusScale) + 1;
  }

  function assessSegmentTierFit(
    focusScale,
    openHotels,
    pipelineHotels,
    bench,
    demandContext,
    marketDepth,
    config
  ) {
    var cfg = config || DEFAULT_CONFIG;
    var local = chainScaleRoomShares(openHotels);
    var pipelineByScale = chainScalePipelineRooms(pipelineHotels);
    var localShare = local.sharesByScale[focusScale] || 0;
    var localHotels = countOpenHotelsForScale(openHotels, focusScale);
    var pipelineRooms = pipelineByScale[focusScale] || 0;

    if (localShare > 0 || localHotels > 0) {
      return {
        fit: "validated",
        label: "Tier present in market",
        gapMultiplier: 1,
        scoreAdjust: 0,
        capLevel: null,
        reasons: [],
      };
    }

    if (!isUpperChainScale(focusScale)) {
      return {
        fit: "speculative",
        label: "Tier not yet present",
        gapMultiplier: cfg.tierFitSpeculativeGapMultiplier,
        scoreAdjust: -4,
        capLevel: "medium",
        reasons: [],
      };
    }

    var ladderTiers = cfg.tierFitLadderScales[focusScale] || [];
    var ladderRooms = 0;
    var ladderHotels = 0;
    ladderTiers.forEach(function (tier) {
      ladderRooms += local.roomsByScale[tier] || 0;
      ladderHotels += countOpenHotelsForScale(openHotels, tier);
    });

    var peerPresence = bench ? bench.presenceGeoCount || bench.geoCount || 0 : 0;
    var anchorSupport = countLuxuryFriendlyAnchors(demandContext);
    var reasons = [];
    var fit = "speculative";
    var scoreAdjust = -6;
    var capLevel = "medium";
    var gapMultiplier = cfg.tierFitSpeculativeGapMultiplier;

    if (peerPresence < cfg.minPeerScalePresenceGeos) {
      fit = "unsupported";
      scoreAdjust = -20;
      capLevel = "low";
      gapMultiplier = cfg.tierFitUnsupportedGapMultiplier;
      reasons.push(
        "Only " +
          peerPresence +
          " peer market" +
          (peerPresence === 1 ? "" : "s") +
          " in your view have open " +
          focusScale +
          " hotels — absence here may reflect market structure, not a greenfield luxury play."
      );
    }

    if (ladderRooms < cfg.minLadderRooms && ladderHotels < cfg.minLadderHotels) {
      if (fit !== "unsupported") {
        fit = "unsupported";
        scoreAdjust = -20;
        capLevel = "low";
        gapMultiplier = cfg.tierFitUnsupportedGapMultiplier;
      }
      reasons.push(
        "No " +
          focusScale +
          " supply and limited upper-tier ladder (" +
          (ladderTiers.join(" / ") || "n/a") +
          ", " +
          ladderRooms.toLocaleString() +
          " keys) — tier may be ahead of current market positioning."
      );
    } else if (ladderRooms < cfg.minLadderRooms * 2) {
      reasons.push(
        "Upper-tier ladder exists (" +
          ladderRooms.toLocaleString() +
          " keys in " +
          ladderTiers.join(" / ") +
          ") but is still thin for " +
          focusScale +
          " entry."
      );
    }

    if (marketDepth && marketDepth.tier === "thin") {
      reasons.push("Thin market depth — treat " + focusScale + " opportunity as directional only.");
      if (capLevel !== "low") capLevel = "medium";
      scoreAdjust -= 6;
    }

    if (marketSkewsBelowTier(openHotels, focusScale)) {
      reasons.push(
        "Open supply skews below " +
          focusScale +
          " — conversion or market uplift likely needed before a new-build " +
          focusScale +
          " play."
      );
      if (fit === "speculative") {
        scoreAdjust -= 4;
      }
    }

    if (pipelineRooms > 0) {
      reasons.push(
        focusScale +
          " pipeline is building (" +
          pipelineRooms.toLocaleString() +
          " keys) — incoming supply may close the gap."
      );
      if (fit === "unsupported") {
        fit = "speculative";
        capLevel = "medium";
        gapMultiplier = cfg.tierFitSpeculativeGapMultiplier;
        scoreAdjust = -10;
      } else {
        scoreAdjust += 4;
      }
    }

    if (anchorSupport > 0 || (demandContext && demandContext.strongCount > 0)) {
      reasons.push(
        "Demand anchors nearby support upper-tier lodging (" +
          (anchorSupport || demandContext.strongCount) +
          " relevant anchor" +
          (anchorSupport === 1 ? "" : "s") +
          ")."
      );
      scoreAdjust += 5;
      if (fit === "unsupported" && ladderHotels >= cfg.minLadderHotels) {
        fit = "speculative";
        capLevel = "medium";
        gapMultiplier = cfg.tierFitSpeculativeGapMultiplier;
        scoreAdjust = -8;
      }
    } else if (fit === "speculative") {
      reasons.push("No strong luxury-oriented demand anchors mapped within range.");
    }

    return {
      fit: fit,
      label:
        fit === "unsupported"
          ? "Tier ahead of market"
          : fit === "speculative"
            ? "Tier gap — needs validation"
            : "Tier validated",
      gapMultiplier: gapMultiplier,
      scoreAdjust: scoreAdjust,
      capLevel: capLevel,
      reasons: reasons,
      ladderRooms: ladderRooms,
      ladderHotels: ladderHotels,
      peerPresenceGeos: peerPresence,
    };
  }

  function isBrandedUnderpenetrated(metrics, mode, config) {
    var gap = metrics.shareGap || 0;
    if (mode === "general") {
      return (
        gap >= config.moderateGapSharePoints &&
        metrics.brandedShare < config.generalBrandedShareMediumMax
      );
    }
    return gap > 0 && metrics.segmentShare < (metrics.benchmarkShare || 0);
  }

  function resolveOpportunityRating(opp, config) {
    var metrics = opp.metrics || {};
    var level = opp.level;
    var variant = opp.ratingVariant;

    if (variant === "unvalidated_gap") {
      return {
        title: level === "high" ? "Strong White Space" : "Some White Space",
        colorName: level === "high" ? "Green" : "Yellow",
        summary:
          "Branded keys are below peers in an independent-heavy pocket — directional opportunity (conversion or first branded entry), not a saturated branded market.",
      };
    }

    if (variant === "tier_unsupported" || (metrics.segmentTierFit && metrics.segmentTierFit.fit === "unsupported")) {
      return {
        title: "Limited Tier Fit",
        colorName: "Red",
        summary:
          opp.ratingSummary ||
          "Upper-tier absence here likely reflects market structure — not enough ladder, peer proof, or demand validation for a strong white-space call.",
      };
    }

    if (
      level === "low" &&
      metrics.brandedOpenHotels === 0 &&
      isBrandedUnderpenetrated(metrics, opp.mode || "general", config)
    ) {
      return {
        title: "Some White Space",
        colorName: "Yellow",
        summary:
          "No branded supply yet, but peers carry more branded share — unvalidated gap, not a well-represented market.",
      };
    }

    var base = levelRatingCopy(level);
    return {
      title: base.title,
      colorName: base.colorName,
      summary: opp.ratingSummary || base.summary,
    };
  }

  function applySegmentTierFitCap(opp) {
    var tierFit = opp.metrics && opp.metrics.segmentTierFit;
    if (!tierFit || !tierFit.capLevel) return;
    var rank = { high: 3, medium: 2, low: 1 };
    if ((rank[opp.level] || 0) > (rank[tierFit.capLevel] || 0)) {
      opp.level = tierFit.capLevel;
      if (tierFit.fit === "unsupported") {
        opp.ratingVariant = "tier_unsupported";
        opp.ratingSummary =
          "Upper-tier absence here likely reflects market structure — not enough ladder, peer proof, or demand validation for a strong white-space call.";
      } else if (tierFit.fit === "speculative") {
        opp.ratingSummary =
          "A tier gap exists, but market readiness is only partial — validate ladder supply, demand anchors, and depth before treating this as expansion-ready.";
      }
    }
  }

  function isHighRelevanceAnchor(anchor) {
    var relevance = normalizeKey(anchor && (anchor.demandRelevance || anchor.demand_relevance));
    return (
      relevance.indexOf("high") >= 0 ||
      relevance.indexOf("primary") >= 0 ||
      relevance === "strong"
    );
  }

  function anchorCoordinates(anchor) {
    var lat = parseFloat(anchor.latitude != null ? anchor.latitude : anchor.lat);
    var lng = parseFloat(anchor.longitude != null ? anchor.longitude : anchor.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lng) || lat === 0 || lng === 0) return null;
    return { lat: lat, lng: lng };
  }

  function matchDemandAnchorsToBucket(bucket, anchorPoints, config) {
    var matches = [];
    var seen = {};
    var bucketLat = bucket.coordCount ? bucket.latSum / bucket.coordCount : null;
    var bucketLng = bucket.coordCount ? bucket.lngSum / bucket.coordCount : null;

    (anchorPoints || []).forEach(function (anchor) {
      if (!anchor || seen[anchor.id || anchor.name]) return;
      if (bucket.country && anchor.country && normalizeKey(anchor.country) !== normalizeKey(bucket.country)) {
        return;
      }

      var matchType = "";
      if (
        bucket.submarket &&
        anchor.submarket &&
        normalizeKey(anchor.submarket) === normalizeKey(bucket.submarket)
      ) {
        matchType = "submarket";
      } else if (
        bucket.market &&
        anchor.submarket &&
        normalizeKey(anchor.submarket).indexOf(normalizeKey(bucket.market)) >= 0
      ) {
        matchType = "market";
      } else if (
        bucket.market &&
        anchor.city &&
        (normalizeKey(bucket.market).indexOf(normalizeKey(anchor.city)) >= 0 ||
          normalizeKey(anchor.city).indexOf(normalizeKey(bucket.market)) >= 0)
      ) {
        matchType = "market";
      } else if (bucketLat != null && bucketLng != null) {
        var coords = anchorCoordinates(anchor);
        if (
          coords &&
          haversineKm(bucketLat, bucketLng, coords.lat, coords.lng) <= config.demandAnchorMatchKm
        ) {
          matchType = "proximity";
        }
      }

      if (!matchType) return;
      seen[anchor.id || anchor.name + matchType] = true;
      matches.push({
        id: anchor.id,
        name: anchor.name || anchor.pointType || "Demand anchor",
        pointType: anchor.pointType || anchor.type,
        matchType: matchType,
        highRelevance: isHighRelevanceAnchor(anchor),
      });
    });

    var strongCount = matches.filter(function (match) {
      return match.highRelevance;
    }).length;
    var labels = matches.slice(0, 3).map(function (match) {
      return match.name;
    });

    return {
      count: matches.length,
      strongCount: strongCount,
      labels: labels,
      matches: matches,
      pointTypes: summarizeDemandAnchorPointTypes(matches),
      strongLabels: matches
        .filter(function (match) {
          return match.highRelevance && match.name;
        })
        .map(function (match) {
          return match.name;
        })
        .slice(0, 3),
    };
  }

  function summarizeDemandAnchorPointTypes(matches) {
    var pointTypes = [];
    var seen = {};
    (matches || []).forEach(function (match) {
      var pointType = normalizeText(match.pointType || match.name);
      if (!pointType || seen[normalizeKey(pointType)]) return;
      seen[normalizeKey(pointType)] = true;
      pointTypes.push(pointType);
    });
    return pointTypes;
  }

  var DEMAND_ANCHOR_REGIONS = {
    Caribbean: true,
    Mexico: true,
    "Central America": true,
    Colombia: true,
    "South America": true,
    CALA: true,
  };

  function dominantRegionFromHotels(hotels) {
    var counts = {};
    var leader = "";
    var leaderCount = 0;
    (hotels || []).forEach(function (hotel) {
      var region = normalizeText(hotel && hotel.region);
      if (!region || !DEMAND_ANCHOR_REGIONS[region]) return;
      counts[region] = (counts[region] || 0) + 1;
      if (counts[region] > leaderCount) {
        leader = region;
        leaderCount = counts[region];
      }
    });
    return leader;
  }

  function mergeDemandAnchorLists(lists) {
    var merged = [];
    var ids = {};
    (lists || []).forEach(function (list) {
      (list || []).forEach(function (anchor) {
        var id = anchor.id || anchor.name + "|" + (anchor.lat || anchor.latitude);
        if (ids[id]) return;
        ids[id] = true;
        merged.push(anchor);
      });
    });
    return merged;
  }

  function fetchDemandAnchorItems(DAR, opts) {
    return DAR.fetchDemandAnchors(opts || {})
      .then(function (data) {
        return DAR.parseItems(data);
      })
      .catch(function () {
        return [];
      });
  }

  function fetchDemandAnchorsForScope(filters, scopedHotels) {
    var DAR = typeof window !== "undefined" ? window.DemandAnchorsRadar : null;
    if (!DAR || typeof DAR.fetchDemandAnchors !== "function") {
      return Promise.resolve([]);
    }

    if (filters && filters.country) {
      return fetchDemandAnchorItems(DAR, { country: filters.country });
    }

    var region = filters && filters.region ? normalizeText(filters.region) : "";
    if (region && DEMAND_ANCHOR_REGIONS[region]) {
      return fetchDemandAnchorItems(DAR, { region: region });
    }

    region = dominantRegionFromHotels(scopedHotels);
    if (region) {
      return fetchDemandAnchorItems(DAR, { region: region });
    }

    var countries = [];
    var seen = {};
    (scopedHotels || []).forEach(function (hotel) {
      var country = normalizeText(hotel.country);
      if (!country || seen[normalizeKey(country)]) return;
      seen[normalizeKey(country)] = true;
      countries.push(country);
    });

    if (!countries.length) return Promise.resolve([]);

    return Promise.all(
      countries.map(function (country) {
        return fetchDemandAnchorItems(DAR, { country: country });
      })
    ).then(mergeDemandAnchorLists);
  }

  function normalizeRoomFilterValue(raw) {
    var trimmed = String(raw != null ? raw : "").trim();
    if (!trimmed) return null;
    var num = parseInt(trimmed, 10);
    if (!Number.isFinite(num) || num < 0) return null;
    return num;
  }

  function hotelPassesRoomBounds(hotel, filters) {
    var minRooms = normalizeRoomFilterValue(filters && filters.roomsMin);
    var maxRooms = normalizeRoomFilterValue(filters && filters.roomsMax);
    if (minRooms != null && maxRooms != null && minRooms > maxRooms) {
      var swap = minRooms;
      minRooms = maxRooms;
      maxRooms = swap;
    }
    var rooms = parseRooms(hotel);
    if (minRooms != null && rooms < minRooms) return false;
    if (maxRooms != null && rooms > maxRooms) return false;
    return true;
  }

  function hotelMatchesGeoScope(hotel, filters) {
    if (!hotel) return false;
    if (filters.region && hotel.region !== filters.region) return false;
    if (filters.country && hotel.country !== filters.country) return false;
    if (filters.market && hotel.market !== filters.market) return false;
    if (filters.submarket && hotel.submarket !== filters.submarket) return false;
    if (filters.locationType && hotel.locationType !== filters.locationType) return false;
    if (filters.hotelType && hotel.censusPropertyType !== filters.hotelType) return false;
    if (filters.hotelServiceModel && hotel.hotelServiceModel !== filters.hotelServiceModel) return false;
    if (filters.operationType && hotel.operationType !== filters.operationType) return false;
    if (filters.managementCompany && hotel.managementCompany !== filters.managementCompany) return false;
    if (!hotelPassesRoomBounds(hotel, filters)) return false;
    if (filters.search) {
      var term = normalizeKey(filters.search);
      var haystack = [
        hotel.name,
        hotel.city,
        hotel.country,
        hotel.brand,
        hotel.market,
        hotel.submarket,
      ]
        .map(normalizeKey)
        .join(" ");
      if (haystack.indexOf(term) === -1) return false;
    }
    if (filters.statuses && filters.statuses.length) {
      if (filters.statuses.indexOf(hotel.status) === -1) return false;
    } else if (filters.status && hotel.status !== filters.status) {
      return false;
    }
    return true;
  }

  function scopeHotelsFromSource(hotels, filters) {
    return (hotels || []).filter(function (hotel) {
      return hotelMatchesGeoScope(hotel, filters);
    });
  }

  function detectMode(filters) {
    if (filters && filters.brand) return "brand";
    if (filters && filters.propertyType) return "chain_scale";
    if (filters && filters.parentCompany) return "parent_company";
    return "general";
  }

  function getSegmentFocusLabel(filters, mode) {
    if (mode === "brand") return normalizeText(filters.brand) || "Selected brand";
    if (mode === "chain_scale") return normalizeText(filters.propertyType) || "Selected chain scale";
    if (mode === "parent_company") return formatParentLabel(normalizeText(filters.parentCompany));
    return "Branded supply";
  }

  function modeLabel(mode, filters) {
    return "White Space · " + getSegmentFocusLabel(filters || {}, mode);
  }

  function segmentMatches(hotel, filters, mode) {
    if (mode === "brand") {
      return normalizeText(hotel.brand) === normalizeText(filters.brand);
    }
    if (mode === "chain_scale") {
      return getChainScale(hotel) === normalizeText(filters.propertyType);
    }
    if (mode === "parent_company") {
      return normalizeText(hotel.parentCompany) === normalizeText(filters.parentCompany);
    }
    return !isIndependentHotel(hotel);
  }

  function geographyFromHotel(hotel) {
    var submarket = normalizeText(hotel.submarket);
    var market = normalizeText(hotel.market);
    var country = normalizeText(hotel.country);
    if (!submarket && !market) return null;
    var label = submarket || market;
    var id = country + "|" + market + "|" + (submarket || market);
    return {
      id: id,
      label: label,
      submarket: submarket,
      market: market,
      country: country,
      geographyType: submarket ? "submarket" : "market",
    };
  }

  function aggregateGeographies(hotels) {
    var map = {};
    (hotels || []).forEach(function (hotel) {
      var geo = geographyFromHotel(hotel);
      if (!geo) return;
      if (!map[geo.id]) {
        map[geo.id] = {
          id: geo.id,
          label: geo.label,
          submarket: geo.submarket,
          market: geo.market,
          country: geo.country,
          geographyType: geo.geographyType,
          hotels: [],
          latSum: 0,
          lngSum: 0,
          coordCount: 0,
        };
      }
      var bucket = map[geo.id];
      bucket.hotels.push(hotel);
      var lat = parseFloat(hotel.lat);
      var lng = parseFloat(hotel.lng);
      if (Number.isFinite(lat) && Number.isFinite(lng) && lat !== 0 && lng !== 0) {
        bucket.latSum += lat;
        bucket.lngSum += lng;
        bucket.coordCount += 1;
      }
    });
    return map;
  }

  function buildGeographyTitle(bucket) {
    if (bucket.submarket && bucket.market && bucket.submarket !== bucket.market) {
      return bucket.submarket + " · " + bucket.market;
    }
    return bucket.label;
  }

  function isValidChainScale(scale) {
    var key = normalizeKey(scale);
    return !!scale && key !== "unknown" && key !== "independent" && key !== "independant";
  }

  function chainScaleRoomShares(openHotels) {
    var totalRooms = 0;
    var roomsByScale = {};
    (openHotels || []).forEach(function (hotel) {
      if (isIndependentHotel(hotel)) return;
      var scale = getChainScale(hotel);
      if (!isValidChainScale(scale)) return;
      var rooms = parseRooms(hotel);
      totalRooms += rooms;
      roomsByScale[scale] = (roomsByScale[scale] || 0) + rooms;
    });
    var sharesByScale = {};
    Object.keys(roomsByScale).forEach(function (scale) {
      sharesByScale[scale] = totalRooms ? roomsByScale[scale] / totalRooms : 0;
    });
    return {
      totalRooms: totalRooms,
      roomsByScale: roomsByScale,
      sharesByScale: sharesByScale,
    };
  }

  function chainScalePipelineRooms(pipelineHotels) {
    var roomsByScale = {};
    (pipelineHotels || []).forEach(function (hotel) {
      if (isIndependentHotel(hotel)) return;
      var scale = getChainScale(hotel);
      if (!isValidChainScale(scale)) return;
      roomsByScale[scale] = (roomsByScale[scale] || 0) + parseRooms(hotel);
    });
    return roomsByScale;
  }

  function countOpenHotelsForScale(openHotels, scale) {
    return (openHotels || []).filter(function (hotel) {
      return isOpenHotel(hotel) && !isIndependentHotel(hotel) && getChainScale(hotel) === scale;
    }).length;
  }

  function countEligiblePeerBuckets(peerBuckets, config) {
    var count = 0;
    (peerBuckets || []).forEach(function (bucket) {
      var openHotels = bucket.hotels.filter(isOpenHotel);
      if (openHotels.length >= config.minOpenHotels) count += 1;
    });
    return count;
  }

  function bucketsToList(buckets) {
    return Object.keys(buckets || {}).map(function (key) {
      return buckets[key];
    });
  }

  function filterBucketsByCountry(buckets, country) {
    var out = {};
    Object.keys(buckets || {}).forEach(function (key) {
      if (normalizeText(buckets[key].country) === normalizeText(country)) {
        out[key] = buckets[key];
      }
    });
    return out;
  }

  function filterBucketsByRegion(buckets, region) {
    var out = {};
    Object.keys(buckets || {}).forEach(function (key) {
      var bucket = buckets[key];
      var hotel = bucket.hotels && bucket.hotels[0];
      var hotelRegion = hotel && hotel.region;
      if (normalizeText(hotelRegion) === normalizeText(region)) {
        out[key] = bucket;
      }
    });
    return out;
  }

  function bucketDominantLocationType(bucket) {
    var counts = {};
    (bucket.hotels || []).filter(isOpenHotel).forEach(function (hotel) {
      var locationType = normalizeText(hotel.locationType);
      if (!locationType) return;
      counts[locationType] = (counts[locationType] || 0) + 1;
    });
    var leader = "";
    var leaderCount = 0;
    Object.keys(counts).forEach(function (locationType) {
      if (counts[locationType] > leaderCount) {
        leader = locationType;
        leaderCount = counts[locationType];
      }
    });
    return leader;
  }

  function filterBucketsByLocationType(buckets, locationType) {
    if (!locationType) return buckets;
    var out = {};
    Object.keys(buckets || {}).forEach(function (key) {
      if (bucketDominantLocationType(buckets[key]) === normalizeText(locationType)) {
        out[key] = buckets[key];
      }
    });
    return out;
  }

  function selectPeerBuckets(targetBucket, allBuckets, filters, config) {
    var countryPeers = filterBucketsByCountry(allBuckets, targetBucket.country);
    if (countEligiblePeerBuckets(bucketsToList(countryPeers), config) >= config.minPeerGeos) {
      var scoped = countryPeers;
      var label = targetBucket.country || "same country";
      if (filters && filters.locationType) {
        var locPeers = filterBucketsByLocationType(countryPeers, filters.locationType);
        if (countEligiblePeerBuckets(bucketsToList(locPeers), config) >= config.minPeerGeos) {
          scoped = locPeers;
          label += " · " + filters.locationType;
        }
      } else {
        var dominantLocation = bucketDominantLocationType(targetBucket);
        if (dominantLocation) {
          var typedPeers = filterBucketsByLocationType(countryPeers, dominantLocation);
          if (countEligiblePeerBuckets(bucketsToList(typedPeers), config) >= config.minPeerGeos) {
            scoped = typedPeers;
            label += " · " + dominantLocation;
          }
        }
      }
      return { peers: scoped, scopeLabel: label, scopeType: "country" };
    }

    var regionHotel = targetBucket.hotels && targetBucket.hotels[0];
    var region = regionHotel && regionHotel.region;
    if (region) {
      var regionPeers = filterBucketsByRegion(allBuckets, region);
      if (countEligiblePeerBuckets(bucketsToList(regionPeers), config) >= config.minPeerGeos) {
        return { peers: regionPeers, scopeLabel: region + " region", scopeType: "region" };
      }
    }

    return { peers: allBuckets, scopeLabel: "current filter view", scopeType: "view" };
  }

  function computePeerBenchmarkShare(peerBuckets, mode, config) {
    var shares = [];
    Object.keys(peerBuckets || {}).forEach(function (key) {
      var metrics = computeGeoMetrics(peerBuckets[key], {}, mode);
      if (!metrics || metrics.openHotels < config.minOpenHotels) return;
      if (mode === "general") shares.push(metrics.brandedRoomShare);
      else shares.push(metrics.segmentRoomShare);
    });
    if (shares.length) return median(shares);
    return mode === "general" ? config.generalBrandedShareMediumMax : 0.12;
  }

  function computePeerChainScaleBenchmarks(peerBuckets, config) {
    var scaleSharesAcrossGeos = {};
    var scalePresenceGeos = {};

    Object.keys(peerBuckets || {}).forEach(function (key) {
      var openHotels = peerBuckets[key].hotels.filter(isOpenHotel);
      if (openHotels.length < config.minOpenHotels) return;
      var local = chainScaleRoomShares(openHotels);
      Object.keys(local.sharesByScale).forEach(function (scale) {
        if (!scaleSharesAcrossGeos[scale]) {
          scaleSharesAcrossGeos[scale] = [];
          scalePresenceGeos[scale] = 0;
        }
        scaleSharesAcrossGeos[scale].push(local.sharesByScale[scale]);
        scalePresenceGeos[scale] += 1;
      });
    });

    var benchmarks = {};
    Object.keys(scaleSharesAcrossGeos).forEach(function (scale) {
      var shares = scaleSharesAcrossGeos[scale];
      if (shares.length < config.minSegmentBenchmarkGeos) return;
      benchmarks[scale] = {
        medianShare: median(shares),
        geoCount: shares.length,
        presenceGeoCount: scalePresenceGeos[scale] || shares.length,
      };
    });
    return benchmarks;
  }

  function computeScopeChainScaleBenchmarks(buckets, config) {
    return computePeerChainScaleBenchmarks(buckets, config);
  }

  function computeUnderpenetratedChainScales(openHotels, pipelineHotels, benchmarks, config) {
    if (!benchmarks || !Object.keys(benchmarks).length) return [];

    var local = chainScaleRoomShares(openHotels);
    var pipelineByScale = chainScalePipelineRooms(pipelineHotels);
    var gaps = [];
    var seen = {};

    Object.keys(benchmarks).forEach(function (scale) {
      var bench = benchmarks[scale];
      var localShare = local.sharesByScale[scale] || 0;
      var gap = bench.medianShare - localShare;
      if (gap < config.moderateGapSharePoints) return;

      seen[scale] = true;
      gaps.push({
        segment: scale,
        segmentType: "chain_scale",
        localShare: localShare,
        benchmarkShare: bench.medianShare,
        shareGap: gap,
        openHotels: countOpenHotelsForScale(openHotels, scale),
        openRooms: local.roomsByScale[scale] || 0,
        pipelineRooms: pipelineByScale[scale] || 0,
        pipelineCrowded:
          (pipelineByScale[scale] || 0) > 0 &&
          (!(local.roomsByScale[scale] > 0) ||
            (pipelineByScale[scale] || 0) / local.roomsByScale[scale] >= config.tierPipelineCrowdRatio),
        peerGeoCount: bench.geoCount,
        peerPresenceGeoCount: bench.presenceGeoCount || bench.geoCount,
        zeroPresence: localShare === 0,
      });
    });

    Object.keys(benchmarks).forEach(function (scale) {
      if (seen[scale] || local.sharesByScale[scale]) return;
      var bench = benchmarks[scale];
      if (bench.medianShare < config.moderateGapSharePoints) return;
      gaps.push({
        segment: scale,
        segmentType: "chain_scale",
        localShare: 0,
        benchmarkShare: bench.medianShare,
        shareGap: bench.medianShare,
        openHotels: 0,
        openRooms: 0,
        pipelineRooms: pipelineByScale[scale] || 0,
        pipelineCrowded:
          (pipelineByScale[scale] || 0) > 0 &&
          (!(local.roomsByScale[scale] > 0) ||
            (pipelineByScale[scale] || 0) / Math.max(local.roomsByScale[scale] || 0, 1) >=
              config.tierPipelineCrowdRatio),
        peerGeoCount: bench.geoCount,
        peerPresenceGeoCount: bench.presenceGeoCount || bench.geoCount,
        zeroPresence: true,
      });
    });

    gaps.sort(function (a, b) {
      return chainScaleSortIndex(a.segment) - chainScaleSortIndex(b.segment);
    });

    return gaps;
  }

  function getBrandChainScalesInScope(scopedHotels, brand) {
    var scales = {};
    (scopedHotels || []).filter(isOpenHotel).forEach(function (hotel) {
      if (normalizeText(hotel.brand) !== normalizeText(brand)) return;
      var scale = getChainScale(hotel);
      if (isValidChainScale(scale)) scales[scale] = true;
    });
    return Object.keys(scales);
  }

  function getParentChainScalesInScope(scopedHotels, parentCompany) {
    var scales = {};
    (scopedHotels || []).filter(isOpenHotel).forEach(function (hotel) {
      if (normalizeText(hotel.parentCompany) !== normalizeText(parentCompany)) return;
      var scale = getChainScale(hotel);
      if (isValidChainScale(scale)) scales[scale] = true;
    });
    return Object.keys(scales);
  }

  function buildSegmentInsightLine(segment) {
    if (segment.zeroPresence) {
      return (
        segment.segment +
        ": no open hotels here; peer median is " +
        formatPct(segment.benchmarkShare) +
        "% of keys across " +
        segment.peerGeoCount +
        " markets in your view" +
        (segment.pipelineRooms
          ? " (" + segment.pipelineRooms.toLocaleString() + " pipeline keys building)"
          : "") +
        "."
      );
    }
    return (
      segment.segment +
      ": " +
      formatPct(segment.localShare) +
      "% of local keys vs " +
      formatPct(segment.benchmarkShare) +
      "% peer median (−" +
      formatPct(segment.shareGap) +
      " pts, " +
      segment.openHotels +
      " hotel" +
      (segment.openHotels === 1 ? "" : "s") +
      ", " +
      segment.openRooms.toLocaleString() +
      " keys" +
      (segment.pipelineRooms
        ? ", " + segment.pipelineRooms.toLocaleString() + " pipeline keys"
        : "") +
      ")."
    );
  }

  function buildSegmentInsights(bucket, filters, mode, scopedHotels, chainScaleBenchmarks, config) {
    var openHotels = bucket.hotels.filter(isOpenHotel);
    var pipelineHotels = bucket.hotels.filter(isPipelineHotel);
    var segments = [];
    var title = "Where the white space is";

    if (mode === "general") {
      segments = computeUnderpenetratedChainScales(
        openHotels,
        pipelineHotels,
        chainScaleBenchmarks,
        config
      );
      title = "Chain Scales With the Most White Space";
    } else if (mode === "chain_scale") {
      var focusScale = normalizeText(filters.propertyType);
      var bench = chainScaleBenchmarks[focusScale];
      var local = chainScaleRoomShares(openHotels);
      var pipelineByScale = chainScalePipelineRooms(pipelineHotels);
      if (bench) {
        segments.push({
          segment: focusScale,
          segmentType: "chain_scale",
          localShare: local.sharesByScale[focusScale] || 0,
          benchmarkShare: bench.medianShare,
          shareGap: Math.max(0, bench.medianShare - (local.sharesByScale[focusScale] || 0)),
          openHotels: countOpenHotelsForScale(openHotels, focusScale),
          openRooms: local.roomsByScale[focusScale] || 0,
          pipelineRooms: pipelineByScale[focusScale] || 0,
          pipelineCrowded:
            (pipelineByScale[focusScale] || 0) > 0 &&
            (!(local.roomsByScale[focusScale] > 0) ||
              (pipelineByScale[focusScale] || 0) /
                Math.max(local.roomsByScale[focusScale] || 0, 1) >=
                config.tierPipelineCrowdRatio),
          peerGeoCount: bench.geoCount,
          peerPresenceGeoCount: bench.presenceGeoCount || bench.geoCount,
          zeroPresence: !(local.sharesByScale[focusScale] > 0),
        });
      }
      title = "Focus Chain Scale";
    } else if (mode === "brand") {
      var brandScales = getBrandChainScalesInScope(scopedHotels, filters.brand);
      if (brandScales.length) {
        brandScales.forEach(function (scale) {
          var scaleBench = chainScaleBenchmarks[scale];
          var scaleLocal = chainScaleRoomShares(openHotels);
          var scalePipeline = chainScalePipelineRooms(pipelineHotels);
          if (!scaleBench) return;
          segments.push({
            segment: scale,
            segmentType: "chain_scale",
            localShare: scaleLocal.sharesByScale[scale] || 0,
            benchmarkShare: scaleBench.medianShare,
            shareGap: Math.max(0, scaleBench.medianShare - (scaleLocal.sharesByScale[scale] || 0)),
            openHotels: countOpenHotelsForScale(openHotels, scale),
            openRooms: scaleLocal.roomsByScale[scale] || 0,
            pipelineRooms: scalePipeline[scale] || 0,
            pipelineCrowded:
              (scalePipeline[scale] || 0) > 0 &&
              (!(scaleLocal.roomsByScale[scale] > 0) ||
                (scalePipeline[scale] || 0) /
                  Math.max(scaleLocal.roomsByScale[scale] || 0, 1) >=
                  config.tierPipelineCrowdRatio),
            peerGeoCount: scaleBench.geoCount,
            peerPresenceGeoCount: scaleBench.presenceGeoCount || scaleBench.geoCount,
            zeroPresence: !(scaleLocal.sharesByScale[scale] > 0),
          });
        });
        title = "Brand Chain Scale in This Market";
      }
    } else if (mode === "parent_company") {
      var parentScales = getParentChainScalesInScope(scopedHotels, filters.parentCompany);
      var absentScales = computeUnderpenetratedChainScales(
        openHotels,
        pipelineHotels,
        chainScaleBenchmarks,
        config
      ).filter(function (segment) {
        return parentScales.indexOf(segment.segment) === -1;
      });
      segments = absentScales;
      title = "Chain Scales Where Parent Is Absent";
    }

    return {
      title: title,
      segments: sortSegmentsByChainScale(segments),
      primaryLabel: segments.length
        ? sortSegmentsByChainScale(segments)
            .slice(0, 2)
            .map(function (segment) {
              return segment.segment;
            })
            .join(" · ")
        : "",
    };
  }

  function buildSegmentInsightLineShort(segment) {
    var suffix = "";
    if (segment.pipelineCrowded) suffix = " · pipeline crowded";
    else if (segment.pipelineRooms > 0) suffix = " · pipeline building";

    if (segment.zeroPresence) {
      var line =
        segment.segment +
        ": none open · peer avg " +
        formatPct(segment.benchmarkShare) +
        "%";
      if (segment.tierFit && segment.tierFit.fit === "unsupported") {
        line += " · tier ahead of market";
      } else if (segment.tierFit && segment.tierFit.fit === "speculative") {
        line += " · needs validation";
      }
      return line + suffix;
    }
    return (
      segment.segment +
      ": " +
      formatPct(segment.localShare) +
      "% vs " +
      formatPct(segment.benchmarkShare) +
      "% peer (−" +
      formatPct(segment.shareGap) +
      " pts)" +
      suffix
    );
  }

  function appendSegmentInsightReasons(reasons, segmentInsights, mode) {
    if (!segmentInsights || !segmentInsights.segments.length) {
      if (mode === "general") {
        reasons.push(
          "No single chain scale stands out as underpenetrated vs peers — rating is driven by overall branded share."
        );
      }
      return;
    }
    // Segment detail is shown in the popup chain-scale section — keep scoring reasons compact.
  }

  function buildDemandAnchorKeyFacts(demandAnchors, config) {
    if (!demandAnchors || !demandAnchors.count) return [];

    var cfg = config || DEFAULT_CONFIG;
    var facts = [];
    var matchKm = cfg.demandAnchorMatchKm || 35;
    var names = demandAnchors.labels || [];
    var countLine =
      demandAnchors.count +
      " demand anchor" +
      (demandAnchors.count === 1 ? "" : "s") +
      " within " +
      matchKm +
      " km";

    if (names.length) {
      var shownNames = names.slice(0, 2);
      countLine += " — " + shownNames.join("; ");
      if (demandAnchors.count > shownNames.length) {
        countLine += "; +" + (demandAnchors.count - shownNames.length) + " more";
      }
    }
    countLine += ".";
    facts.push(countLine);

    if (demandAnchors.strongCount > 0) {
      var strongLine =
        demandAnchors.strongCount +
        " high-relevance demand anchor" +
        (demandAnchors.strongCount === 1 ? "" : "s");
      var strongNames = demandAnchors.strongLabels || [];
      if (strongNames.length) {
        strongLine += " (" + strongNames.slice(0, 2).join("; ") + ")";
      }
      strongLine += " support branded lodging demand.";
      facts.push(strongLine);
    } else if (demandAnchors.pointTypes && demandAnchors.pointTypes.length) {
      facts.push(
        "Demand anchor types nearby: " + demandAnchors.pointTypes.slice(0, 3).join("; ") + "."
      );
    }

    return facts.slice(0, 2);
  }

  function buildCompactPopupReasons(opportunity) {
    var metrics = opportunity.metrics || {};
    var reasons = [];
    var mode = opportunity.mode;

    if (mode === "general") {
      reasons.push(
        formatPct(metrics.brandedShare || 0) +
          "% branded keys vs " +
          formatPct(metrics.benchmarkShare || 0) +
          "% peer median in " +
          (opportunity.benchmarkScopeLabel || "view") +
          " (−" +
          formatPct(metrics.shareGap || 0) +
          " pts)."
      );
    } else {
      reasons.push(
        formatPct(metrics.segmentShare || 0) +
          "% of open keys (−" +
          formatPct(metrics.shareGap || 0) +
          " pts vs peers in " +
          (opportunity.benchmarkScopeLabel || "view") +
          ")."
      );
    }

    reasons.push(
      metrics.brandedOpenHotels +
        " branded · " +
        metrics.independentOpenHotels +
        " independent · " +
        metrics.openRooms.toLocaleString() +
        " open keys."
    );

    if (metrics.segmentPipelineRooms > 0) {
      reasons.push(
        metrics.segmentPipelineHotels +
          " pipeline hotel" +
          (metrics.segmentPipelineHotels === 1 ? "" : "s") +
          " in focus (" +
          metrics.segmentPipelineRooms.toLocaleString() +
          " keys)."
      );
    } else if (metrics.tierPipelinePenalty > 0) {
      reasons.push("Pipeline is building in one or more underpenetrated chain scales.");
    } else if (metrics.independentOpenHotels >= 10) {
      reasons.push(metrics.independentOpenHotels + " independents — conversion optionality.");
    } else if (metrics.chainScaleLeaders && metrics.chainScaleLeaders.length) {
      reasons.push("Current supply skews to " + metrics.chainScaleLeaders[0] + ".");
    }

    if (opportunity.metrics.countryMaturity) {
      reasons.push(
        "Country baseline: " +
          formatPct(opportunity.metrics.countryMaturity.brandedShare) +
          "% branded (" +
          opportunity.metrics.countryMaturity.tierLabel +
          ")."
      );
    }

    if (opportunity.metrics.segmentTierFit && opportunity.metrics.segmentTierFit.label) {
      reasons.push("Tier readiness: " + opportunity.metrics.segmentTierFit.label + ".");
    }

    if (opportunity.metrics.marketDepth) {
      reasons.push("Market depth: " + opportunity.metrics.marketDepth.label + ".");
    }

    (opportunity.reasons || []).forEach(function (reason) {
      if (reason.indexOf("Ranks among") === 0) {
        reasons.push(reason);
      }
    });

    var demandAnchorFacts = buildDemandAnchorKeyFacts(
      opportunity.metrics.demandAnchors,
      DEFAULT_CONFIG
    );
    var maxCore = Math.max(1, DEFAULT_CONFIG.popupMaxReasons - demandAnchorFacts.length);
    return reasons.slice(0, maxCore).concat(demandAnchorFacts);
  }

  function topChainScaleByRooms(openHotels, limit) {
    var roomByScale = {};
    openHotels.forEach(function (h) {
      if (isIndependentHotel(h)) return;
      var scale = getChainScale(h);
      if (!scale || scale === "Unknown") return;
      roomByScale[scale] = (roomByScale[scale] || 0) + parseRooms(h);
    });
    return Object.keys(roomByScale)
      .sort(function (a, b) {
        return roomByScale[b] - roomByScale[a];
      })
      .slice(0, limit || 3)
      .map(function (scale) {
        return scale + " (" + roomByScale[scale].toLocaleString() + " keys)";
      });
  }

  function computeGeoMetrics(bucket, filters, mode) {
    var openHotels = bucket.hotels.filter(isOpenHotel);
    var pipelineHotels = bucket.hotels.filter(isPipelineHotel);
    if (!openHotels.length) return null;

    var brandedOpen = openHotels.filter(function (h) {
      return !isIndependentHotel(h);
    });
    var independentOpen = openHotels.filter(isIndependentHotel);
    var totalOpenRooms = openHotels.reduce(function (sum, h) {
      return sum + parseRooms(h);
    }, 0);
    var brandedOpenRooms = brandedOpen.reduce(function (sum, h) {
      return sum + parseRooms(h);
    }, 0);

    var segmentOpen = openHotels.filter(function (h) {
      return segmentMatches(h, filters, mode);
    });
    var segmentPipeline = pipelineHotels.filter(function (h) {
      return segmentMatches(h, filters, mode);
    });
    var segmentOpenRooms = segmentOpen.reduce(function (sum, h) {
      return sum + parseRooms(h);
    }, 0);
    var segmentPipelineRooms = segmentPipeline.reduce(function (sum, h) {
      return sum + parseRooms(h);
    }, 0);

    var segmentRoomShare = totalOpenRooms ? segmentOpenRooms / totalOpenRooms : 0;
    var brandedRoomShare = totalOpenRooms ? brandedOpenRooms / totalOpenRooms : 0;

    return {
      openHotels: openHotels.length,
      openRooms: totalOpenRooms,
      brandedOpenHotels: brandedOpen.length,
      brandedOpenRooms: brandedOpenRooms,
      independentOpenHotels: independentOpen.length,
      segmentOpenHotels: segmentOpen.length,
      segmentOpenRooms: segmentOpenRooms,
      segmentRoomShare: segmentRoomShare,
      brandedRoomShare: brandedRoomShare,
      pipelineHotels: pipelineHotels.length,
      segmentPipelineHotels: segmentPipeline.length,
      segmentPipelineRooms: segmentPipelineRooms,
      chainScaleLeaders: topChainScaleByRooms(openHotels, 3),
    };
  }

  function median(values) {
    if (!values.length) return 0;
    var sorted = values.slice().sort(function (a, b) {
      return a - b;
    });
    var mid = Math.floor(sorted.length / 2);
    if (sorted.length % 2) return sorted[mid];
    return (sorted[mid - 1] + sorted[mid]) / 2;
  }

  function computeScopeBenchmarkShare(buckets, filters, mode, config) {
    return computePeerBenchmarkShare(buckets, mode, config);
  }

  function confidenceLabel(metrics, bucket) {
    if (metrics.openHotels >= 10 && bucket.submarket) return "High";
    if (metrics.openHotels >= 5) return "Medium";
    return "Directional";
  }

  function levelRatingCopy(level) {
    if (level === "high") {
      return {
        title: "Strong White Space",
        colorName: "Green",
        summary:
          "Your focus segment is underrepresented here compared with similar markets in your view, with enough branded hotels to support demand.",
      };
    }
    if (level === "medium") {
      return {
        title: "Some White Space",
        colorName: "Yellow",
        summary:
          "There is room to grow, but the gap is smaller, pipeline is building, or market validation is only moderate.",
      };
    }
    return {
      title: "Well Represented",
      colorName: "Red",
      summary:
        "Your focus segment already has meaningful share here, pipeline is crowding the tier, or branded validation is too thin to justify expansion.",
    };
  }

  function applyPipelineLevelDowngrade(level, metrics) {
    if (!level || level === "low") return level;
    var crowded =
      metrics.segmentPipelineRooms > 0 &&
      (metrics.segmentOpenRooms === 0 ||
        metrics.segmentPipelineRooms / metrics.segmentOpenRooms >= 0.45);
    if (!crowded) return level;
    if (level === "high") return "medium";
    return "low";
  }

  function isMapEligibleOpportunity(opp, config) {
    if (!opp || !opp.level) return false;
    if (opp.level === "low") return false;
    if (opp.mapEligible === false) return false;
    var cfg = config || DEFAULT_CONFIG;
    var minScore = cfg.minMapOpportunityScore || 0;
    if (minScore > 0 && (opp.opportunityScore || 0) < minScore) {
      return false;
    }
    return opp.level === "high" || opp.level === "medium";
  }

  function assignRelativeLevels(group, config, mode) {
    if (!group.length) return;

    var unassigned = group.filter(function (opp) {
      return !opp.level;
    });
    if (!unassigned.length) return;

    unassigned.sort(function (a, b) {
      return (b.opportunityScore || 0) - (a.opportunityScore || 0);
    });

    var total = unassigned.length;
    var highCount = Math.max(1, Math.round(total * config.relativeHighShare));
    var lowCount = Math.max(1, Math.round(total * config.relativeLowShare));
    if (highCount + lowCount >= total) {
      highCount = Math.max(1, Math.floor(total / 3));
      lowCount = Math.max(1, Math.floor(total / 3));
    }

    unassigned.forEach(function (opp, index) {
      var level;
      if (index < highCount) {
        level = "high";
        opp.reasons.unshift(
          "Ranks among the most underpenetrated geographies vs peers in " +
            (opp.benchmarkScopeLabel || "this view") +
            "."
        );
      } else if (index >= total - lowCount) {
        if (isBrandedUnderpenetrated(opp.metrics || {}, mode || "general", config)) {
          level = "medium";
          opp.ratingVariant = opp.ratingVariant || "unvalidated_gap";
          opp.reasons.unshift(
            "Lower relative rank in country, but branded share is still below peers — not a saturated market."
          );
        } else {
          level = "low";
          opp.reasons.unshift(
            "Ranks among the better-represented geographies vs peers in " +
              (opp.benchmarkScopeLabel || "this view") +
              "."
          );
        }
      } else {
        level = "medium";
      }
      opp.level = applyPipelineLevelDowngrade(level, opp.metrics);
    });
  }

  function finalizeOpportunityLevels(opportunities, mode, config) {
    if (!opportunities.length) return;

    opportunities.forEach(function (opp) {
      opp.level = null;
      var metrics = opp.metrics;
      var gap = metrics.shareGap || 0;

      if (
        metrics.brandedOpenHotels < config.minBrandedOpenHotels &&
        !(metrics.segmentOpenHotels === 0 && metrics.brandedOpenHotels > 0)
      ) {
        if (
          isBrandedUnderpenetrated(metrics, mode, config) &&
          metrics.openHotels >= config.minOpenHotels
        ) {
          opp.level = "medium";
          opp.ratingVariant = "unvalidated_gap";
          opp.reasons.unshift(
            "Independent-heavy pocket with branded share below peers — directional opportunity, not a saturated market."
          );
          return;
        }
        if (metrics.openHotels < config.minOpenHotels) {
          opp.level = "low";
          opp.reasons.unshift("Very thin open supply — rating is directional only.");
          return;
        }
        opp.level = "low";
        return;
      }

      if (
        metrics.segmentOpenHotels === 0 &&
        metrics.brandedOpenHotels >= config.minBrandedOpenHotels &&
        (!metrics.segmentTierFit || metrics.segmentTierFit.fit === "validated")
      ) {
        opp.level = "high";
        return;
      }

      if (gap <= 0 && metrics.segmentOpenHotels > 0) {
        opp.level = "low";
        return;
      }

      if (mode === "general") {
        var maturity = metrics.countryMaturity;
        var satThreshold = config.generalBrandedShareMediumMax;
        if (maturity) {
          satThreshold = Math.max(satThreshold, maturity.brandedShare * 1.12);
        }
        if (metrics.brandedShare >= satThreshold) {
          opp.level = "low";
          return;
        }
      }

      if (gap >= config.strongGapSharePoints) {
        opp.level = applyPipelineLevelDowngrade("high", metrics);
      }
    });

    var byCountry = {};
    opportunities.forEach(function (opp) {
      var countryKey = normalizeKey(opp.country) || "unknown";
      if (!byCountry[countryKey]) byCountry[countryKey] = [];
      byCountry[countryKey].push(opp);
    });

    Object.keys(byCountry).forEach(function (countryKey) {
      assignRelativeLevels(byCountry[countryKey], config, mode);
    });

    opportunities.forEach(function (opp) {
      if (!opp.level) {
        opp.level = "medium";
      }

      if (
        opp.metrics.marketDepth &&
        opp.metrics.marketDepth.capStrong &&
        opp.level === "high" &&
        !(
          opp.metrics.segmentOpenHotels === 0 &&
          opp.metrics.brandedOpenHotels >= config.minBrandedOpenHotels &&
          (!opp.metrics.segmentTierFit || opp.metrics.segmentTierFit.fit === "validated")
        )
      ) {
        opp.level = "medium";
      }

      if (
        opp.level === "medium" &&
        opp.metrics.demandAnchors &&
        opp.metrics.demandAnchors.count >= config.demandAnchorMinForBoost &&
        (opp.metrics.demandAnchors.strongCount >= 1 || opp.metrics.demandAnchors.count >= 3) &&
        (opp.metrics.shareGap || 0) > 0 &&
        (!opp.metrics.segmentTierFit || opp.metrics.segmentTierFit.fit !== "unsupported")
      ) {
        opp.level = applyPipelineLevelDowngrade("high", opp.metrics);
      }

      applySegmentTierFitCap(opp);

      var rating = resolveOpportunityRating(opp, config);
      opp.ratingTitle = rating.title;
      opp.ratingColorName = rating.colorName;
      opp.ratingSummary = rating.summary;
      opp.mapEligible = isMapEligibleOpportunity(opp, config);
    });
  }

  function annotateSegmentTierFits(
    segmentInsights,
    openHotels,
    pipelineHotels,
    demandContext,
    marketDepth,
    config
  ) {
    if (!segmentInsights || !segmentInsights.segments || !segmentInsights.segments.length) return;

    var kept = [];
    segmentInsights.segments.forEach(function (segment) {
      if (!segment.zeroPresence || !isUpperChainScale(segment.segment)) {
        kept.push(segment);
        return;
      }
      var fit = assessSegmentTierFit(
        segment.segment,
        openHotels,
        pipelineHotels,
        {
          medianShare: segment.benchmarkShare,
          geoCount: segment.peerGeoCount,
          presenceGeoCount: segment.peerPresenceGeoCount || segment.peerGeoCount,
        },
        demandContext,
        marketDepth,
        config
      );
      segment.tierFit = fit;
      if (fit.fit !== "unsupported") {
        kept.push(segment);
      }
    });

    segmentInsights.segments = kept;
    segmentInsights.primaryLabel = kept
      .slice(0, 2)
      .map(function (segment) {
        return segment.segment;
      })
      .join(" · ");
  }

  function scoreOpportunity(
    bucket,
    metrics,
    filters,
    mode,
    benchmarkShare,
    config,
    segmentInsights,
    benchmarkContext,
    enrichmentContext
  ) {
    var focus = getSegmentFocusLabel(filters, mode);
    var displayFocus = focus;
    if (segmentInsights && segmentInsights.primaryLabel) {
      if (mode === "general") {
        displayFocus = segmentInsights.primaryLabel;
      } else if (mode === "parent_company") {
        displayFocus = focus + " · gap in " + segmentInsights.primaryLabel;
      }
    }
    var reasons = [];
    var gap = 0;
    var gapPoints = 0;
    var validationPoints = 0;
    var independentPoints = 0;
    var pipelinePenalty = 0;
    var opportunityType = "white_space";
    var maturity =
      (enrichmentContext &&
        enrichmentContext.countryMaturity &&
        enrichmentContext.countryMaturity[normalizeKey(bucket.country)]) ||
      null;
    var demandContext = (enrichmentContext && enrichmentContext.demandContext) || {
      count: 0,
      strongCount: 0,
      labels: [],
      matches: [],
    };
    var marketDepth = (enrichmentContext && enrichmentContext.marketDepth) || assessMarketDepth(metrics, config);
    var tierPipelinePenalty = 0;
    var segmentTierFit = null;
    var tierFitScoreAdjust = 0;
    var openHotels = bucket.hotels.filter(isOpenHotel);
    var pipelineHotels = bucket.hotels.filter(isPipelineHotel);

    annotateSegmentTierFits(
      segmentInsights,
      openHotels,
      pipelineHotels,
      demandContext,
      marketDepth,
      config
    );

    if (mode === "chain_scale" && filters && filters.propertyType) {
      var focusScale = normalizeText(filters.propertyType);
      var focusSegment =
        segmentInsights && segmentInsights.segments && segmentInsights.segments[0];
      segmentTierFit = assessSegmentTierFit(
        focusScale,
        openHotels,
        pipelineHotels,
        {
          medianShare: benchmarkShare,
          geoCount: focusSegment ? focusSegment.peerGeoCount : 0,
          presenceGeoCount: focusSegment
            ? focusSegment.peerPresenceGeoCount || focusSegment.peerGeoCount
            : 0,
        },
        demandContext,
        marketDepth,
        config
      );
      if (focusSegment) {
        focusSegment.tierFit = segmentTierFit;
      }
    }

    if (segmentInsights && segmentInsights.segments) {
      segmentInsights.segments.forEach(function (segment) {
        if (segment.pipelineCrowded) tierPipelinePenalty += 6;
        else if (segment.pipelineRooms > 0) tierPipelinePenalty += 3;
      });
    }

    if (mode === "general") {
      gap = Math.max(0, benchmarkShare - metrics.brandedRoomShare);
      reasons.push(
        "Branded open keys are " +
          formatPct(metrics.brandedRoomShare) +
          "% of supply (" +
          metrics.brandedOpenRooms.toLocaleString() +
          " of " +
          metrics.openRooms.toLocaleString() +
          " open keys)."
      );
      reasons.push(
        "Peer median in " +
          (benchmarkContext.scopeLabel || "view") +
          " is " +
          formatPct(benchmarkShare) +
          "% branded keys — gap of " +
          formatPct(gap) +
          " points."
      );
      if (metrics.brandedRoomShare >= config.generalBrandedShareMediumMax) {
        reasons.push("Branded penetration is already relatively high in this geography.");
      }
      if (maturity) {
        reasons.push(
          "Country baseline: " +
            maturity.country +
            " averages " +
            formatPct(maturity.brandedShare) +
            "% branded (" +
            maturity.tierLabel +
            ")."
        );
      }
    } else if (mode === "parent_company") {
      if (metrics.segmentOpenHotels === 0 && metrics.brandedOpenHotels > 0) {
        gap = Math.max(benchmarkShare, 0.08);
        reasons.push(
          focus +
            " has no open hotels here while " +
            metrics.brandedOpenHotels +
            " other branded hotel" +
            (metrics.brandedOpenHotels === 1 ? "" : "s") +
            " operate (" +
            metrics.brandedOpenRooms.toLocaleString() +
            " open keys)."
        );
        opportunityType = "parent_company_market_gap";
      } else {
        gap = Math.max(0, benchmarkShare - metrics.segmentRoomShare);
        reasons.push(
          focus +
            " accounts for " +
            formatPct(metrics.segmentRoomShare) +
            "% of open keys (" +
            metrics.segmentOpenRooms.toLocaleString() +
            " of " +
            metrics.openRooms.toLocaleString() +
            ")."
        );
        reasons.push(
          "Peer median for this focus in your view is " +
            formatPct(benchmarkShare) +
            "% — gap of " +
            formatPct(gap) +
            " points."
        );
      }
    } else {
      gap = Math.max(0, benchmarkShare - metrics.segmentRoomShare);
      reasons.push(
        focus +
          " accounts for " +
          formatPct(metrics.segmentRoomShare) +
          "% of open keys (" +
          metrics.segmentOpenRooms.toLocaleString() +
          " of " +
          metrics.openRooms.toLocaleString() +
          ", " +
          metrics.segmentOpenHotels +
          " hotel" +
          (metrics.segmentOpenHotels === 1 ? "" : "s") +
          ")."
      );
      reasons.push(
        "Peer median in your current view is " +
          formatPct(benchmarkShare) +
          "% — gap of " +
          formatPct(gap) +
          " points."
      );
      if (metrics.segmentOpenHotels === 0 && metrics.brandedOpenHotels > 0) {
        opportunityType = mode + "_market_gap";
        if (segmentTierFit && segmentTierFit.fit === "unsupported") {
          reasons.push(
            "Other branded hotels operate here, but " +
              focus +
              " readiness is limited — absence of the tier is not treated as strong white space."
          );
        } else {
          reasons.push(
            "No " +
              focus +
              " open hotels yet, but " +
              metrics.brandedOpenHotels +
              " other branded hotel" +
              (metrics.brandedOpenHotels === 1 ? "" : "s") +
              " validate demand."
          );
        }
      }
    }

    if (segmentTierFit) {
      segmentTierFit.reasons.forEach(function (reason) {
        reasons.push(reason);
      });
      if (metrics.segmentOpenHotels === 0 && segmentTierFit.gapMultiplier < 1) {
        gap = gap * segmentTierFit.gapMultiplier;
        reasons.push(
          "Adjusted tier gap for market readiness (" +
            focus +
            " absence alone is not treated as strong white space)."
        );
      }
      tierFitScoreAdjust = segmentTierFit.scoreAdjust || 0;
    }

    appendSegmentInsightReasons(reasons, segmentInsights, mode);

    if (metrics.brandedOpenHotels >= config.minBrandedOpenHotels) {
      validationPoints += 6;
      reasons.push(
        "Market validation: " +
          metrics.brandedOpenHotels +
          " branded open hotel" +
          (metrics.brandedOpenHotels === 1 ? "" : "s") +
          " (" +
          metrics.brandedOpenRooms.toLocaleString() +
          " keys)."
      );
    } else {
      reasons.push("Limited branded validation — only " + metrics.brandedOpenHotels + " branded open hotel(s).");
    }
    if (metrics.brandedOpenHotels >= 8) validationPoints += 4;

    if (metrics.chainScaleLeaders.length) {
      reasons.push("Leading chain scales by open keys: " + metrics.chainScaleLeaders.join("; ") + ".");
    }

    if (metrics.independentOpenHotels >= config.highIndependentOpenHotels) {
      independentPoints = 12;
      reasons.push(
        metrics.independentOpenHotels +
          " independent open hotels — conversion / re-flag optionality for owners."
      );
    } else if (metrics.independentOpenHotels >= 5) {
      independentPoints = 6;
      reasons.push(metrics.independentOpenHotels + " independent open hotels in this geography.");
    }

    if (metrics.segmentPipelineRooms > 0) {
      var pipelineShare =
        metrics.segmentOpenRooms > 0
          ? metrics.segmentPipelineRooms / metrics.segmentOpenRooms
          : metrics.segmentPipelineRooms / Math.max(metrics.openRooms, 1);
      pipelinePenalty = Math.min(28, Math.round(pipelineShare * 35));
      reasons.push(
        "Pipeline pressure: " +
          metrics.segmentPipelineHotels +
          " focus-segment pipeline hotel" +
          (metrics.segmentPipelineHotels === 1 ? "" : "s") +
          " (" +
          metrics.segmentPipelineRooms.toLocaleString() +
          " pipeline keys" +
          (metrics.segmentOpenRooms
            ? " vs " + metrics.segmentOpenRooms.toLocaleString() + " open focus keys"
            : "") +
          ")."
      );
    } else if (metrics.pipelineHotels > 0) {
      reasons.push(
        metrics.pipelineHotels +
          " pipeline hotel" +
          (metrics.pipelineHotels === 1 ? "" : "s") +
          " in this geography (outside focus segment)."
      );
    }

    if (demandContext.count > 0) {
      reasons.push(
        "Demand anchors nearby: " +
          demandContext.count +
          (demandContext.labels.length ? " (" + demandContext.labels.join("; ") + ")" : "") +
          "."
      );
    }

    if (marketDepth.tier === "thin") {
      reasons.push("Thin market depth — interpret opportunity directionally until supply grows.");
    } else if (marketDepth.tier === "deep") {
      reasons.push("Deep market depth supports scalable branded expansion.");
    }

    gapPoints = Math.min(55, Math.round(gap * 320));

    var demandBoost = 0;
    if (demandContext.count >= config.demandAnchorMinForBoost) demandBoost += 4;
    if (demandContext.strongCount >= 1) demandBoost += 4;

    var opportunityScore = Math.max(
      0,
      Math.min(
        100,
        gapPoints +
          validationPoints +
          independentPoints +
          demandBoost +
          (marketDepth.scoreBoost || 0) +
          tierFitScoreAdjust -
          pipelinePenalty -
          tierPipelinePenalty
      )
    );

    return {
      geographyId: bucket.id,
      label: buildGeographyTitle(bucket),
      submarket: bucket.submarket,
      market: bucket.market,
      country: bucket.country,
      geographyType: bucket.geographyType,
      lat: bucket.coordCount ? bucket.latSum / bucket.coordCount : null,
      lng: bucket.coordCount ? bucket.lngSum / bucket.coordCount : null,
      level: null,
      mode: mode,
      modeLabel: modeLabel(mode, filters),
      focusLabel: displayFocus,
      segmentInsights: segmentInsights,
      benchmarkScopeLabel: benchmarkContext.scopeLabel,
      benchmarkScopeType: benchmarkContext.scopeType,
      benchmarkPeerCount: benchmarkContext.peerCount,
      opportunityType: opportunityType,
      opportunityScore: opportunityScore,
      ratingTitle: "",
      ratingColorName: "",
      ratingSummary: "",
      reasons: reasons,
      metrics: {
        openHotels: metrics.openHotels,
        openRooms: metrics.openRooms,
        segmentOpenHotels: metrics.segmentOpenHotels,
        segmentOpenRooms: metrics.segmentOpenRooms,
        segmentShare: metrics.segmentRoomShare,
        brandedOpenHotels: metrics.brandedOpenHotels,
        brandedOpenRooms: metrics.brandedOpenRooms,
        brandedShare: metrics.brandedRoomShare,
        independentOpenHotels: metrics.independentOpenHotels,
        pipelineHotels: metrics.pipelineHotels,
        segmentPipelineHotels: metrics.segmentPipelineHotels,
        segmentPipelineRooms: metrics.segmentPipelineRooms,
        benchmarkShare: benchmarkShare,
        benchmarkScopeLabel: benchmarkContext.scopeLabel,
        benchmarkScopeType: benchmarkContext.scopeType,
        benchmarkPeerCount: benchmarkContext.peerCount,
        shareGap: gap,
        countryMaturity: maturity,
        marketDepth: marketDepth,
        demandAnchors: {
          count: demandContext.count,
          strongCount: demandContext.strongCount,
          labels: demandContext.labels,
          pointTypes: demandContext.pointTypes,
          strongLabels: demandContext.strongLabels,
        },
        tierPipelinePenalty: tierPipelinePenalty,
        segmentTierFit: segmentTierFit,
        confidence: confidenceLabel(metrics, bucket),
        segmentGaps: segmentInsights ? segmentInsights.segments : [],
        chainScaleLeaders: metrics.chainScaleLeaders,
        scoreBreakdown: {
          gapPoints: gapPoints,
          validationPoints: validationPoints,
          independentPoints: independentPoints,
          demandBoost: demandBoost,
          marketDepthBoost: marketDepth.scoreBoost || 0,
          pipelinePenalty: pipelinePenalty,
          tierPipelinePenalty: tierPipelinePenalty,
          tierFitScoreAdjust: tierFitScoreAdjust,
        },
      },
    };
  }

  function getPeerContext(bucket, allBuckets, filters, config, mode, peerContextCache) {
    var locationScope =
      (filters && filters.locationType) || bucketDominantLocationType(bucket) || "";
    var cacheKey =
      normalizeKey(bucket.country) + "::" + normalizeKey(locationScope) + "::" + mode;
    if (peerContextCache && peerContextCache[cacheKey]) {
      return peerContextCache[cacheKey];
    }

    var peerSelection = selectPeerBuckets(bucket, allBuckets, filters, config);
    var context = {
      peerSelection: peerSelection,
      benchmarkShare: computePeerBenchmarkShare(peerSelection.peers, mode, config),
      chainScaleBenchmarks: computePeerChainScaleBenchmarks(peerSelection.peers, config),
      benchmarkContext: {
        scopeLabel: peerSelection.scopeLabel,
        scopeType: peerSelection.scopeType,
        peerCount: countEligiblePeerBuckets(bucketsToList(peerSelection.peers), config),
      },
    };

    if (peerContextCache) {
      peerContextCache[cacheKey] = context;
    }
    return context;
  }

  function analyzeGeography(
    bucket,
    filters,
    mode,
    allBuckets,
    config,
    scopedHotels,
    enrichment,
    peerContextCache
  ) {
    var metrics = computeGeoMetrics(bucket, filters, mode);
    if (!metrics || metrics.openHotels < config.minOpenHotels) return null;
    if (mode === "parent_company" && metrics.segmentOpenHotels > 0 && metrics.brandedOpenHotels === 0) {
      return null;
    }

    var peerContext = getPeerContext(
      bucket,
      allBuckets,
      filters,
      config,
      mode,
      peerContextCache
    );
    var benchmarkContext = peerContext.benchmarkContext;
    var demandContext = matchDemandAnchorsToBucket(
      bucket,
      (enrichment && enrichment.demandAnchorPoints) || [],
      config
    );
    var marketDepth = assessMarketDepth(metrics, config);
    var enrichmentContext = {
      countryMaturity: (enrichment && enrichment.countryMaturity) || {},
      demandContext: demandContext,
      marketDepth: marketDepth,
    };

    var segmentInsights = buildSegmentInsights(
      bucket,
      filters,
      mode,
      scopedHotels,
      peerContext.chainScaleBenchmarks,
      config
    );
    return scoreOpportunity(
      bucket,
      metrics,
      filters,
      mode,
      peerContext.benchmarkShare,
      config,
      segmentInsights,
      benchmarkContext,
      enrichmentContext
    );
  }

  function computeOpportunities(hotels, filters, userConfig, enrichment) {
    var config = Object.assign({}, DEFAULT_CONFIG, userConfig || {});
    var mode = detectMode(filters || {});
    var scoped = scopeHotelsFromSource(hotels, filters || {});
    var activeRows = scoped.filter(function (hotel) {
      return isOpenHotel(hotel) || isPipelineHotel(hotel);
    });
    var buckets = aggregateGeographies(activeRows);
    var countryMaturity = computeCountryMaturityBaselines(buckets, config);
    enrichment = Object.assign({ countryMaturity: countryMaturity }, enrichment || {});
    var opportunities = [];
    var peerContextCache = {};

    Object.keys(buckets).forEach(function (key) {
      var opp = analyzeGeography(
        buckets[key],
        filters || {},
        mode,
        buckets,
        config,
        scoped,
        enrichment,
        peerContextCache
      );
      if (!opp || opp.lat == null || opp.lng == null) return;
      opportunities.push(opp);
    });

    finalizeOpportunityLevels(opportunities, mode, config);

    opportunities = opportunities.filter(function (opp) {
      return isMapEligibleOpportunity(opp, config);
    });

    opportunities.sort(function (a, b) {
      var rank = { high: 3, medium: 2, low: 1 };
      var levelDiff = (rank[b.level] || 0) - (rank[a.level] || 0);
      if (levelDiff !== 0) return levelDiff;
      return (b.opportunityScore || 0) - (a.opportunityScore || 0);
    });

    return opportunities.slice(0, config.maxMarkers);
  }

  function computeOpportunitiesAsync(hotels, filters, userConfig) {
    var scoped = scopeHotelsFromSource(hotels, filters || {});
    return fetchDemandAnchorsForScope(filters || {}, scoped)
      .then(function (demandAnchorPoints) {
        return computeOpportunities(hotels, filters, userConfig, { demandAnchorPoints: demandAnchorPoints });
      })
      .catch(function () {
        return computeOpportunities(hotels, filters, userConfig);
      });
  }

  function levelColor(level, config) {
    var colors = (config && config.colors) || DEFAULT_CONFIG.colors;
    return colors[level] || colors.medium;
  }

  function escHtml(value) {
    return String(value != null ? value : "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function buildSegmentInsightsHtml(segmentInsights) {
    if (!segmentInsights || !segmentInsights.segments.length) return "";
    var shown = sortSegmentsByChainScale(segmentInsights.segments);
    var items = shown
      .map(function (segment) {
        return "<li>" + escHtml(buildSegmentInsightLineShort(segment)) + "</li>";
      })
      .join("");
    return (
      '<div style="margin-bottom:8px;">' +
      '<div style="font-size:12px;color:#333;margin-bottom:4px;"><strong>' +
      escHtml(segmentInsights.title) +
      "</strong></div>" +
      '<ul style="margin:0 0 0 16px;padding:0;font-size:11px;color:#444;line-height:1.35;">' +
      items +
      "</ul></div>"
    );
  }

  function buildPopupHtml(opportunity, config) {
    var cfg = config || DEFAULT_CONFIG;
    var color = levelColor(opportunity.level, cfg);
    var compactReasons = buildCompactPopupReasons(opportunity);
    var reasonsHtml = compactReasons
      .map(function (reason) {
        return "<li>" + escHtml(reason) + "</li>";
      })
      .join("");
    var segmentInsightsHtml = buildSegmentInsightsHtml(opportunity.segmentInsights);
    var focusCaption =
      opportunity.mode === "general" && opportunity.segmentInsights && opportunity.segmentInsights.primaryLabel
        ? "Top Chain Scales: "
        : "Focus: ";

    return (
      '<div class="radar-white-space-popup">' +
      '<h3 style="margin:0 0 4px 0;color:#333;font-size:15px;line-height:1.25;">' +
      escHtml(opportunity.label) +
      "</h3>" +
      (opportunity.country
        ? '<div style="font-size:10px;color:#666;margin-bottom:6px;">' +
          escHtml(opportunity.country) +
          (opportunity.market && opportunity.market !== opportunity.label
            ? " · " + escHtml(opportunity.market)
            : "") +
          "</div>"
        : "") +
      '<div style="background:#f8fafc;padding:8px;border-radius:6px;margin-bottom:8px;border-left:4px solid ' +
      color +
      ';">' +
      '<div style="font-size:14px;font-weight:bold;color:' +
      color +
      ';line-height:1.25;">' +
      escHtml(opportunity.ratingColorName) +
      " — " +
      escHtml(opportunity.ratingTitle) +
      "</div>" +
      (opportunity.focusLabel
        ? '<div style="font-size:10px;color:#666;margin-top:3px;">' +
          escHtml(focusCaption) +
          escHtml(opportunity.focusLabel) +
          "</div>"
        : "") +
      (opportunity.benchmarkScopeLabel
        ? '<div style="font-size:10px;color:#888;margin-top:3px;">Peers: ' +
          escHtml(opportunity.benchmarkScopeLabel) +
          (opportunity.benchmarkPeerCount
            ? " (" + opportunity.benchmarkPeerCount + " geographies)"
            : "") +
          "</div>"
        : "") +
      "</div>" +
      segmentInsightsHtml +
      (reasonsHtml
        ? '<div style="font-size:11px;color:#333;margin-bottom:4px;"><strong>Key Facts</strong></div>' +
          '<ul style="margin:0 0 6px 16px;padding:0;font-size:11px;color:#444;">' +
          reasonsHtml +
          "</ul>"
        : "") +
      '<div style="font-size:10px;color:#666;border-top:1px solid #eee;padding-top:6px;">' +
      opportunity.metrics.openHotels +
      " open hotels · " +
      opportunity.metrics.openRooms.toLocaleString() +
      " keys · " +
      "Confidence: " +
      escHtml(opportunity.metrics.confidence) +
      (opportunity.metrics.countryMaturity
        ? " · " + escHtml(opportunity.metrics.countryMaturity.tierLabel)
        : "") +
      "</div>" +
      "</div>"
    );
  }

  window.RadarWhiteSpace = {
    DEFAULT_CONFIG: DEFAULT_CONFIG,
    detectMode: detectMode,
    computeOpportunities: computeOpportunities,
    computeOpportunitiesAsync: computeOpportunitiesAsync,
    isMapEligibleOpportunity: isMapEligibleOpportunity,
    buildPopupHtml: buildPopupHtml,
    levelColor: levelColor,
  };
})();
