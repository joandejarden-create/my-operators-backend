/**
 * Shared radar geography metrics — submarket buckets (White Space pattern).
 * Brand penetration and pipeline pressure layers + hotel detail submarket snapshot.
 */
(function (global) {
  "use strict";

  var PENETRATION_DEFAULTS = {
    minOpenHotels: 2,
    highThreshold: 70,
    mediumThreshold: 40,
  };

  var PIPELINE_DEFAULTS = {
    minOpenHotels: 1,
    highThreshold: 100,
    mediumThreshold: 50,
  };

  function normalizeKey(value) {
    return String(value != null ? value : "")
      .trim()
      .toLowerCase();
  }

  function normalizeText(value) {
    var text = String(value != null ? value : "").trim();
    return text || "";
  }

  function normalizeGeoField(value) {
    return String(value || "").trim();
  }

  function matchesGeoField(hotelValue, filterValue) {
    if (!filterValue) return true;
    return normalizeGeoField(hotelValue) === normalizeGeoField(filterValue);
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

  function parseRooms(hotel) {
    var n = parseInt(hotel && hotel.rooms, 10);
    return Number.isFinite(n) && n > 0 ? n : 0;
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

  /** Match active radar filters (geo + segment); status handled per metric. */
  function hotelMatchesMapLayerScope(hotel, filters) {
    if (!hotel || !filters) return true;
    if (filters.region && !matchesGeoField(hotel.region, filters.region)) return false;
    if (filters.country && !matchesGeoField(hotel.country, filters.country)) return false;
    if (filters.market && !matchesGeoField(hotel.market, filters.market)) return false;
    if (filters.submarket && filters.market && !matchesGeoField(hotel.submarket, filters.submarket)) {
      return false;
    }
    if (filters.locationType && hotel.locationType !== filters.locationType) return false;
    if (filters.hotelType && hotel.censusPropertyType !== filters.hotelType) return false;
    if (filters.hotelServiceModel && hotel.hotelServiceModel !== filters.hotelServiceModel) return false;
    if (filters.operationType && hotel.operationType !== filters.operationType) return false;
    if (filters.managementCompany && hotel.managementCompany !== filters.managementCompany) return false;
    if (filters.parentCompany && hotel.parentCompany !== filters.parentCompany) return false;
    if (filters.brand && hotel.brand !== filters.brand) return false;
    if (filters.propertyType && hotel.propertyType !== filters.propertyType) return false;
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
    return true;
  }

  function scopeHotelsForPenetration(hotels, filters) {
    return (hotels || []).filter(function (hotel) {
      return isOpenHotel(hotel) && hotelMatchesMapLayerScope(hotel, filters || {});
    });
  }

  function scopeHotelsForPipelinePressure(hotels, filters) {
    return (hotels || []).filter(function (hotel) {
      if (!isOpenHotel(hotel) && !isPipelineHotel(hotel)) return false;
      return hotelMatchesMapLayerScope(hotel, filters || {});
    });
  }

  function sumRooms(hotels) {
    return (hotels || []).reduce(function (sum, hotel) {
      return sum + parseRooms(hotel);
    }, 0);
  }

  function geographyFromHotel(hotel) {
    var submarket = normalizeText(hotel && hotel.submarket);
    var market = normalizeText(hotel && hotel.market);
    var country = normalizeText(hotel && hotel.country);
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

  function aggregateGeographyBuckets(hotels) {
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
    if (!bucket) return "";
    if (bucket.submarket && bucket.market && bucket.submarket !== bucket.market) {
      return bucket.submarket + " · " + bucket.market;
    }
    return bucket.label || "";
  }

  function penetrationLevel(percentage, options) {
    var opts = options || {};
    var high = opts.highThreshold != null ? opts.highThreshold : PENETRATION_DEFAULTS.highThreshold;
    var medium = opts.mediumThreshold != null ? opts.mediumThreshold : PENETRATION_DEFAULTS.mediumThreshold;
    if (percentage >= high) return "high";
    if (percentage >= medium) return "medium";
    return "low";
  }

  function pipelinePressureLevel(percentage, options) {
    var opts = options || {};
    var high = opts.highThreshold != null ? opts.highThreshold : PIPELINE_DEFAULTS.highThreshold;
    var medium = opts.mediumThreshold != null ? opts.mediumThreshold : PIPELINE_DEFAULTS.mediumThreshold;
    if (percentage >= high) return "high";
    if (percentage >= medium) return "medium";
    return "low";
  }

  function pipelineLevelLabel(level) {
    if (level === "high") return "High Pressure";
    if (level === "medium") return "Medium Pressure";
    return "Low Pressure";
  }

  function computeBrandedPenetration(hotels, options) {
    var opts = Object.assign({}, PENETRATION_DEFAULTS, options || {});
    var openHotels = (hotels || []).filter(isOpenHotel);
    var totalHotels = openHotels.length;
    if (totalHotels < opts.minOpenHotels) {
      return {
        percentage: null,
        brandedCount: 0,
        independentCount: 0,
        totalHotels: totalHotels,
        level: null,
        levelLabel: null,
        sufficientSample: false,
      };
    }
    var brandedCount = 0;
    var independentCount = 0;
    openHotels.forEach(function (hotel) {
      if (isIndependentHotel(hotel)) independentCount += 1;
      else brandedCount += 1;
    });
    var percentage = Math.round((brandedCount / totalHotels) * 100);
    var level = penetrationLevel(percentage, opts);
    var levelLabel = level === "high" ? "High" : level === "medium" ? "Medium" : "Low";
    return {
      percentage: percentage,
      brandedCount: brandedCount,
      independentCount: independentCount,
      totalHotels: totalHotels,
      level: level,
      levelLabel: levelLabel,
      sufficientSample: true,
    };
  }

  function computePipelinePressure(hotels, options) {
    var opts = Object.assign({}, PIPELINE_DEFAULTS, options || {});
    var openHotels = (hotels || []).filter(isOpenHotel);
    var pipelineHotels = (hotels || []).filter(isPipelineHotel);
    var openCount = openHotels.length;
    var pipelineCount = pipelineHotels.length;

    if (openCount < opts.minOpenHotels || pipelineCount === 0) {
      return {
        unitPercentage: null,
        keyPercentage: null,
        classificationPercentage: null,
        openHotels: openCount,
        pipelineHotels: pipelineCount,
        openRooms: 0,
        pipelineRooms: 0,
        level: null,
        levelLabel: null,
        riskLabel: null,
        sufficientSample: false,
        usesKeyWeighting: false,
      };
    }

    var openRooms = sumRooms(openHotels);
    var pipelineRooms = sumRooms(pipelineHotels);
    var unitPercentage = Math.round((pipelineCount / openCount) * 100);
    var keyPercentage = openRooms > 0 ? Math.round((pipelineRooms / openRooms) * 100) : null;
    var usesKeyWeighting = keyPercentage != null;
    var classificationPercentage = usesKeyWeighting ? keyPercentage : unitPercentage;
    var level = pipelinePressureLevel(classificationPercentage, opts);
    var riskLabel =
      classificationPercentage >= PIPELINE_DEFAULTS.highThreshold
        ? "Oversupply Risk"
        : classificationPercentage >= PIPELINE_DEFAULTS.mediumThreshold
          ? "Moderate Growth"
          : "Healthy Growth";

    return {
      unitPercentage: unitPercentage,
      keyPercentage: keyPercentage,
      classificationPercentage: classificationPercentage,
      openHotels: openCount,
      pipelineHotels: pipelineCount,
      openRooms: openRooms,
      pipelineRooms: pipelineRooms,
      level: level,
      levelLabel: pipelineLevelLabel(level),
      riskLabel: riskLabel,
      sufficientSample: true,
      usesKeyWeighting: usesKeyWeighting,
    };
  }

  function bucketCentroid(bucket) {
    if (!bucket || !bucket.coordCount) return { lat: null, lng: null };
    return {
      lat: bucket.latSum / bucket.coordCount,
      lng: bucket.lngSum / bucket.coordCount,
    };
  }

  function hotelsInGeographyBucket(hotel, sourceHotels) {
    var geo = geographyFromHotel(hotel);
    if (!geo) return [];
    return (sourceHotels || []).filter(function (h) {
      var g = geographyFromHotel(h);
      return g && g.id === geo.id;
    });
  }

  function computePenetrationBuckets(hotels, filters, options) {
    var opts = Object.assign({}, PENETRATION_DEFAULTS, options || {});
    var scoped = scopeHotelsForPenetration(hotels, filters);
    var buckets = aggregateGeographyBuckets(scoped);
    var results = [];

    Object.keys(buckets).forEach(function (key) {
      var bucket = buckets[key];
      var metrics = computeBrandedPenetration(bucket.hotels, opts);
      if (!metrics.sufficientSample) return;
      var centroid = bucketCentroid(bucket);
      if (centroid.lat == null || centroid.lng == null) return;
      results.push({
        id: bucket.id,
        title: buildGeographyTitle(bucket),
        label: bucket.label,
        submarket: bucket.submarket,
        market: bucket.market,
        country: bucket.country,
        geographyType: bucket.geographyType,
        lat: centroid.lat,
        lng: centroid.lng,
        hotels: bucket.hotels,
        metrics: metrics,
      });
    });

    return results;
  }

  function computePipelinePressureBuckets(hotels, filters, options) {
    var opts = Object.assign({}, PIPELINE_DEFAULTS, options || {});
    var scoped = scopeHotelsForPipelinePressure(hotels, filters);
    var buckets = aggregateGeographyBuckets(scoped);
    var results = [];

    Object.keys(buckets).forEach(function (key) {
      var bucket = buckets[key];
      var metrics = computePipelinePressure(bucket.hotels, opts);
      if (!metrics.sufficientSample) return;
      var centroid = bucketCentroid(bucket);
      if (centroid.lat == null || centroid.lng == null) return;
      results.push({
        id: bucket.id,
        title: buildGeographyTitle(bucket),
        label: bucket.label,
        submarket: bucket.submarket,
        market: bucket.market,
        country: bucket.country,
        geographyType: bucket.geographyType,
        lat: centroid.lat,
        lng: centroid.lng,
        hotels: bucket.hotels,
        metrics: metrics,
      });
    });

    return results;
  }

  function getPenetrationForHotel(hotel, sourceHotels, filters, options) {
    var geo = geographyFromHotel(hotel);
    if (!geo) return null;
    var opts = Object.assign({}, PENETRATION_DEFAULTS, options || {});
    var scoped = scopeHotelsForPenetration(sourceHotels, filters);
    var bucketHotels = scoped.filter(function (h) {
      var g = geographyFromHotel(h);
      return g && g.id === geo.id;
    });
    var metrics = computeBrandedPenetration(bucketHotels, opts);
    return {
      geography: geo,
      title: buildGeographyTitle({
        label: geo.label,
        submarket: geo.submarket,
        market: geo.market,
      }),
      metrics: metrics,
      bucketHotels: bucketHotels,
    };
  }

  function getPipelinePressureForHotel(hotel, sourceHotels, filters, options) {
    var geo = geographyFromHotel(hotel);
    if (!geo) return null;
    var opts = Object.assign({}, PIPELINE_DEFAULTS, options || {});
    var scoped = scopeHotelsForPipelinePressure(sourceHotels, filters);
    var bucketHotels = scoped.filter(function (h) {
      var g = geographyFromHotel(h);
      return g && g.id === geo.id;
    });
    var metrics = computePipelinePressure(bucketHotels, opts);
    return {
      geography: geo,
      title: buildGeographyTitle({
        label: geo.label,
        submarket: geo.submarket,
        market: geo.market,
      }),
      metrics: metrics,
      bucketHotels: bucketHotels,
    };
  }

  function resolveMapFilters() {
    if (typeof global.getRadarMapFilters === "function") {
      return global.getRadarMapFilters() || {};
    }
    return global.radarCurrentFilters || {};
  }

  global.RadarBrandedPenetration = {
    PENETRATION_DEFAULTS: PENETRATION_DEFAULTS,
    PIPELINE_DEFAULTS: PIPELINE_DEFAULTS,
    DEFAULTS: PENETRATION_DEFAULTS,
    normalizeKey: normalizeKey,
    isOpenHotel: isOpenHotel,
    isPipelineHotel: isPipelineHotel,
    isIndependentHotel: isIndependentHotel,
    geographyFromHotel: geographyFromHotel,
    aggregateGeographyBuckets: aggregateGeographyBuckets,
    buildGeographyTitle: buildGeographyTitle,
    hotelMatchesMapLayerScope: hotelMatchesMapLayerScope,
    hotelMatchesPenetrationScope: hotelMatchesMapLayerScope,
    scopeHotelsForPenetration: scopeHotelsForPenetration,
    scopeHotelsForPipelinePressure: scopeHotelsForPipelinePressure,
    computeBrandedPenetration: computeBrandedPenetration,
    computePipelinePressure: computePipelinePressure,
    computePenetrationBuckets: computePenetrationBuckets,
    computePipelinePressureBuckets: computePipelinePressureBuckets,
    hotelsInGeographyBucket: hotelsInGeographyBucket,
    getPenetrationForHotel: getPenetrationForHotel,
    getPipelinePressureForHotel: getPipelinePressureForHotel,
    resolveMapFilters: resolveMapFilters,
    penetrationLevel: penetrationLevel,
    pipelinePressureLevel: pipelinePressureLevel,
  };

  global.RadarPipelinePressure = {
    computePipelinePressure: computePipelinePressure,
    computePipelinePressureBuckets: computePipelinePressureBuckets,
    getPipelinePressureForHotel: getPipelinePressureForHotel,
    resolveMapFilters: resolveMapFilters,
    hotelsInGeographyBucket: hotelsInGeographyBucket,
    geographyFromHotel: geographyFromHotel,
    buildGeographyTitle: buildGeographyTitle,
  };
})(typeof window !== "undefined" ? window : globalThis);
