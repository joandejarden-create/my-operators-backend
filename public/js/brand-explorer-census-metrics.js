/**
 * Brand Explorer — footprint display trust (census vs MVP vs unverified).
 * censusSummary is only present when BRAND_EXPLORER_CENSUS_METRICS=1 on server.
 * Keep trust rules in sync with lib/brand-explorer-footprint-trust.js (Node audit/tests).
 */
(function (global) {
  var SOURCE_NOTE_CENSUS = 'Based on current Dealality census records.';
  var SOURCE_NOTE_MVP = 'Based on brand setup footprint data.';
  var SOURCE_NOTE_MVP_VERIFIED = 'Based on verified brand setup footprint data.';
  var SOURCE_NOTE_MVP_ESTIMATED = 'Based on estimated brand setup footprint data.';
  var VERIFIED_EMPTY_MESSAGE = 'Portfolio data being verified.';
  var BREAKDOWN_EMPTY_MESSAGE = 'Detailed footprint data is being verified.';

  function hasVal(v) {
    return v != null && v !== '';
  }

  function hasNum(v) {
    var n = Number(v);
    return Number.isFinite(n) && n > 0;
  }

  function normText(v) {
    return String(v == null ? '' : v)
      .trim()
      .toLowerCase();
  }

  function useCensusSummary(brand) {
    var cs = brand && brand.censusSummary;
    return !!(cs && cs.available === true && cs.fallbackRecommended === false);
  }

  function isCensusFallbackRecommended(brand) {
    var cs = brand && brand.censusSummary;
    if (!cs) return false;
    return cs.fallbackRecommended === true;
  }

  function footprintHasMetricValues(fp) {
    if (!fp || typeof fp !== 'object') return false;
    if (hasNum(fp.totalExistingHotels) || hasNum(fp.totalExistingRooms)) return true;
    var rd = fp.regionalDistribution;
    if (rd && typeof rd === 'object') {
      var keys = Object.keys(rd);
      for (var i = 0; i < keys.length; i++) {
        var o = rd[keys[i]] || {};
        if (hasNum(o.hotels) || hasNum(o.rooms)) return true;
      }
    }
    return false;
  }

  function verificationLooksUnverified(text) {
    var t = normText(text);
    if (!t) return false;
    return /unverified|draft|placeholder|demo|sample|pending|tbd|not verified|under review/.test(t);
  }

  function verificationLooksVerified(text) {
    var t = normText(text);
    if (!t) return false;
    if (verificationLooksUnverified(t)) return false;
    return /verified|audited|confirmed|published|franchise disclosure|fdd|annual report|census|brand setup/.test(
      t
    );
  }

  function dataSourceLooksUnverified(text) {
    var t = normText(text);
    if (!t) return false;
    return /placeholder|demo|sample|draft|unverified|tbd|mock/.test(t);
  }

  function dataSourceLooksCurated(text) {
    var t = normText(text);
    if (!t) return false;
    if (dataSourceLooksUnverified(t)) return false;
    return /brand setup|airtable|fdd|franchise|annual|investor|curated|operations|published|ye20\d{2}/.test(
      t
    );
  }

  function isGenericHeroVerification(text) {
    return normText(text) === 'verified by brand';
  }

  function isGenericHeroDataSource(text) {
    return normText(text) === 'live airtable / brand setup data';
  }

  function explicitFootprintStatus(fp) {
    var status = fp && fp.verification && fp.verification.status;
    return status ? String(status).trim() : '';
  }

  /** Phase 1E — explicit Footprint Data Status when set on brand.footprint.verification */
  function resolveExplicitFootprintTrust(fp) {
    var status = explicitFootprintStatus(fp);
    if (!status) return null;

    var hasMetrics = footprintHasMetricValues(fp);

    if (status === 'Verified') {
      return {
        sourceUsed: 'mvp-footprint',
        isCensusBacked: false,
        isMvpFallback: true,
        isUnverifiedFallback: false,
        displaySourceLabel: SOURCE_NOTE_MVP_VERIFIED,
        showVerifiedMetrics: hasMetrics
      };
    }

    if (status === 'Estimated') {
      return {
        sourceUsed: 'mvp-footprint',
        isCensusBacked: false,
        isMvpFallback: true,
        isUnverifiedFallback: false,
        displaySourceLabel: SOURCE_NOTE_MVP_ESTIMATED,
        showVerifiedMetrics: hasMetrics
      };
    }

    if (status === 'Placeholder' || status === 'Needs Review') {
      return {
        sourceUsed: 'unverified',
        isCensusBacked: false,
        isMvpFallback: false,
        isUnverifiedFallback: true,
        displaySourceLabel: VERIFIED_EMPTY_MESSAGE,
        showVerifiedMetrics: hasMetrics
      };
    }

    return null;
  }

  function censusFallbackWithZeroOpenHotels(brand) {
    if (!isCensusFallbackRecommended(brand)) return false;
    var open = brand.censusSummary && brand.censusSummary.metrics && brand.censusSummary.metrics.totalOpenHotels;
    return open == null || Number(open) === 0;
  }

  /** Phase 1D legacy trust when explicit Footprint Data Status is absent. */
  function isMvpFootprintVerifiedLegacy(brand, fp) {
    if (!footprintHasMetricValues(fp)) return false;

    var ver = fp && fp.verification;
    if (ver && hasVal(ver.figuresAsOf)) return true;

    var fv = (fp && fp.formValues) || {};
    var blockLegacyFiguresAsOf = censusFallbackWithZeroOpenHotels(brand);
    if (!blockLegacyFiguresAsOf && hasVal(fv.figuresAsOf)) return true;

    if (isCensusFallbackRecommended(brand)) return false;

    if (hasVal(brand.explorerHeroVerification)) {
      if (isGenericHeroVerification(brand.explorerHeroVerification)) return false;
      if (verificationLooksUnverified(brand.explorerHeroVerification)) return false;
      if (verificationLooksVerified(brand.explorerHeroVerification)) return true;
      return true;
    }

    if (hasVal(brand.explorerHeroDataSource)) {
      if (isGenericHeroDataSource(brand.explorerHeroDataSource)) return false;
      if (dataSourceLooksUnverified(brand.explorerHeroDataSource)) return false;
      if (dataSourceLooksCurated(brand.explorerHeroDataSource)) return true;
      return true;
    }

    return false;
  }

  function logCensusWarningsToConsole(brand) {
    var cs = brand && brand.censusSummary;
    if (!cs || !cs.warnings || !cs.warnings.length) return;
    if (typeof console !== 'undefined' && typeof console.debug === 'function') {
      console.debug(
        '[BrandExplorer] censusSummary warnings',
        brand.name || brand.brandName || '',
        cs.warnings
      );
    }
  }

  function footprintTrustModel(brand) {
    logCensusWarningsToConsole(brand);

    var fp = (brand && brand.footprint) || {};

    if (useCensusSummary(brand)) {
      return {
        sourceUsed: 'census',
        isCensusBacked: true,
        isMvpFallback: false,
        isUnverifiedFallback: false,
        displaySourceLabel: SOURCE_NOTE_CENSUS,
        showVerifiedMetrics: true
      };
    }

    var explicit = resolveExplicitFootprintTrust(fp);
    if (explicit) return explicit;

    var mvpVerified = isMvpFootprintVerifiedLegacy(brand, fp);
    if (mvpVerified) {
      return {
        sourceUsed: 'mvp-footprint',
        isCensusBacked: false,
        isMvpFallback: true,
        isUnverifiedFallback: false,
        displaySourceLabel: SOURCE_NOTE_MVP,
        showVerifiedMetrics: true
      };
    }

    return {
      sourceUsed: 'unverified',
      isCensusBacked: false,
      isMvpFallback: false,
      isUnverifiedFallback: true,
      displaySourceLabel: VERIFIED_EMPTY_MESSAGE,
      showVerifiedMetrics: false
    };
  }

  function emptyDisplayFootprint(baseFp) {
    var fp = Object.assign({}, baseFp || {});
    fp.formValues = Object.assign({}, (baseFp && baseFp.formValues) || {});
    fp.totalExistingHotels = null;
    fp.totalExistingRooms = null;
    fp.totalNewBuildHotels = null;
    fp.totalConversionHotels = null;
    fp.totalNewBuildRooms = null;
    fp.totalConversionRooms = null;
    fp.regionalDistribution = {};
    fp.locationDistribution = {};
    return fp;
  }

  /**
   * Portfolio pipeline totals aligned with regional "Pipeline Hotel/Rooms" columns.
   * Prefers summing regionalDistribution; falls back to totalPipeline* then new build + conversion.
   */
  function footprintPipelineTotals(fp) {
    if (!fp || typeof fp !== 'object') return { hotels: 0, rooms: 0 };
    var reg = fp.regionalDistribution;
    if (reg && typeof reg === 'object') {
      var keys = Object.keys(reg);
      if (keys.length) {
        var sumH = 0;
        var sumR = 0;
        for (var i = 0; i < keys.length; i++) {
          var o = reg[keys[i]] || {};
          sumH += Number(o.pipelineHotels) || 0;
          sumR += Number(o.pipelineRooms) || 0;
        }
        return { hotels: sumH, rooms: sumR };
      }
    }
    var ph = Number(fp.totalPipelineHotels);
    var pr = Number(fp.totalPipelineRooms);
    if ((!isNaN(ph) && ph > 0) || (!isNaN(pr) && pr > 0)) {
      return { hotels: isNaN(ph) ? 0 : ph, rooms: isNaN(pr) ? 0 : pr };
    }
    return {
      hotels: (Number(fp.totalNewBuildHotels) || 0) + (Number(fp.totalConversionHotels) || 0),
      rooms: (Number(fp.totalNewBuildRooms) || 0) + (Number(fp.totalConversionRooms) || 0)
    };
  }

  function breakdownToRegionalDistribution(rows) {
    var rd = {};
    (rows || []).forEach(function (row) {
      var label = row && row.label;
      if (!label) return;
      rd[label] = {
        hotels: Number(row.hotels) || 0,
        rooms: Number(row.keys) || 0,
        pipelineHotels: Number(row.pipelineHotels) || 0,
        pipelineRooms: Number(row.pipelineKeys) || 0
      };
    });
    return rd;
  }

  function normalizeBreakdownRow(row) {
    if (!row || !row.label) return null;
    return {
      label: row.label,
      hotels: Number(row.hotels) || 0,
      keys: Number(row.keys) || 0,
      pipelineHotels: Number(row.pipelineHotels) || 0,
      pipelineKeys: Number(row.pipelineKeys) || 0,
      keysPct: row.keysPct
    };
  }

  function sortBreakdownForPortfolioDisplay(rows) {
    return (rows || [])
      .map(normalizeBreakdownRow)
      .filter(Boolean)
      .filter(function (r) {
        return r.hotels + r.pipelineHotels > 0;
      })
      .sort(function (a, b) {
        var ta = a.hotels + a.pipelineHotels;
        var tb = b.hotels + b.pipelineHotels;
        var aUnk = a.label === 'Unknown' && a.pipelineHotels > 0;
        var bUnk = b.label === 'Unknown' && b.pipelineHotels > 0;
        if (aUnk && !bUnk) return -1;
        if (bUnk && !aUnk) return 1;
        if (a.label === 'Unknown') return 1;
        if (b.label === 'Unknown') return -1;
        return tb - ta || b.keys + b.pipelineKeys - (a.keys + a.pipelineKeys);
      });
  }

  function censusBreakdownFromBrand(brand, breakdownKey) {
    var cs = brand && brand.censusSummary;
    if (!cs || cs.available !== true || cs.fallbackRecommended === true) return null;
    var b = cs.breakdowns || {};
    var rows = b[breakdownKey];
    return Array.isArray(rows) && rows.length ? rows : null;
  }

  /** Portfolio distribution table row: label, existing H/R, pipeline H/R, totals. */
  function breakdownRowToPortfolioRow(row) {
    var n = normalizeBreakdownRow(row);
    if (!n) return null;
    return [
      n.label,
      n.hotels,
      n.keys,
      n.pipelineHotels,
      n.pipelineKeys,
      n.hotels + n.pipelineHotels,
      n.keys + n.pipelineKeys
    ];
  }

  function breakdownToLocationDistribution(rows) {
    var loc = {};
    (rows || []).forEach(function (row) {
      var label = row && row.label;
      if (!label) return;
      var pct = row.keysPct;
      if (pct == null || pct === '') pct = 0;
      loc[label] = pct;
    });
    return loc;
  }

  function breakdownToTableRows(rows) {
    return (rows || [])
      .map(function (row) {
        var base = breakdownRowToPortfolioRow(row);
        if (!base) return null;
        var h = base[1];
        var r = base[2];
        return base.concat([h > 0 ? Math.round(r / h) : 0]);
      })
      .filter(Boolean);
  }

  function unverifiedFootprintHtml(message) {
    return (
      '<p class="be-footprint-unverified" data-footprint-verified="false">' +
      message +
      '</p>'
    );
  }

  function isDemoOrMockFootprintBrand(brand) {
    if (hasVal(brand.explorerHeroVerification)) {
      if (isGenericHeroVerification(brand.explorerHeroVerification)) return true;
      if (verificationLooksUnverified(brand.explorerHeroVerification)) return true;
    }
    if (hasVal(brand.explorerHeroDataSource)) {
      if (isGenericHeroDataSource(brand.explorerHeroDataSource)) return true;
      if (dataSourceLooksUnverified(brand.explorerHeroDataSource)) return true;
    }
    return false;
  }

  function censusPipelineMetrics(brand) {
    var cs = brand && brand.censusSummary;
    if (!cs || cs.available !== true) return null;
    var m = cs.metrics || {};
    return {
      hotels: Number(m.totalPipelineHotels) || 0,
      rooms: Number(m.totalPipelineKeys) || 0
    };
  }

  /**
   * When census ran successfully, pipeline totals come from census — not Brand Footprint
   * placeholders (New Build + Conversion columns used when Pipeline Hotel is blank).
   */
  function applyCensusPipelineAuthority(brand, fp) {
    var cp = censusPipelineMetrics(brand);
    if (!cp) return fp;
    var out = Object.assign({}, fp);
    out.formValues = Object.assign({}, fp.formValues || {});
    out.totalPipelineHotels = cp.hotels;
    out.totalPipelineRooms = cp.rooms;
    out.totalNewBuildHotels = cp.hotels;
    out.totalNewBuildRooms = cp.rooms;
    out.totalConversionHotels = 0;
    out.totalConversionRooms = 0;

    var censusRegions =
      brand.censusSummary &&
      brand.censusSummary.breakdowns &&
      brand.censusSummary.breakdowns.dealalityRegion;
    if (Array.isArray(censusRegions) && censusRegions.length) {
      out.regionalDistribution = breakdownToRegionalDistribution(censusRegions);
    } else if (out.regionalDistribution && typeof out.regionalDistribution === 'object') {
      var next = {};
      Object.keys(out.regionalDistribution).forEach(function (k) {
        next[k] = Object.assign({}, out.regionalDistribution[k], {
          pipelineHotels: 0,
          pipelineRooms: 0,
          newBuildHotels: 0,
          newBuildRooms: 0,
          conversionHotels: 0,
          conversionRooms: 0
        });
      });
      out.regionalDistribution = next;
    }
    return out;
  }

  function finalizeFootprintDisplayModel(model, brand) {
    if (!model || !model.fp) return model;
    model.fp = applyCensusPipelineAuthority(brand, model.fp);
    return model;
  }

  function footprintDisplayModel(brand) {
    var baseFp = (brand && brand.footprint) || {};
    var trust = footprintTrustModel(brand);

    if (trust.sourceUsed === 'unverified') {
      if (footprintHasMetricValues(baseFp) && !isDemoOrMockFootprintBrand(brand)) {
        var mvpFromUnverified = Object.assign({}, baseFp);
        mvpFromUnverified.formValues = Object.assign({}, baseFp.formValues || {});
        return finalizeFootprintDisplayModel({
          useCensus: false,
          sourceUsed: 'mvp-footprint',
          isCensusBacked: false,
          isMvpFallback: true,
          isUnverifiedFallback: true,
          showVerifiedMetrics: true,
          displaySourceLabel: trust.displaySourceLabel,
          sourceNote: SOURCE_NOTE_MVP,
          metricsBanner: trust.displaySourceLabel,
          verifiedEmptyMessage: VERIFIED_EMPTY_MESSAGE,
          breakdownEmptyMessage: BREAKDOWN_EMPTY_MESSAGE,
          fp: mvpFromUnverified,
          countryBreakdown: null,
          chainScaleBreakdown: null,
          locationTypeBreakdown: null
        }, brand);
      }
      return finalizeFootprintDisplayModel({
        useCensus: false,
        sourceUsed: 'unverified',
        isCensusBacked: false,
        isMvpFallback: false,
        isUnverifiedFallback: true,
        showVerifiedMetrics: false,
        displaySourceLabel: trust.displaySourceLabel,
        sourceNote: null,
        metricsBanner: trust.displaySourceLabel,
        verifiedEmptyMessage: VERIFIED_EMPTY_MESSAGE,
        breakdownEmptyMessage: BREAKDOWN_EMPTY_MESSAGE,
        fp: emptyDisplayFootprint(baseFp),
        countryBreakdown: null,
        chainScaleBreakdown: null,
        locationTypeBreakdown: null
      }, brand);
    }

    if (trust.sourceUsed === 'mvp-footprint') {
      var mvpFp = Object.assign({}, baseFp);
      mvpFp.formValues = Object.assign({}, baseFp.formValues || {});
      return finalizeFootprintDisplayModel({
        useCensus: false,
        sourceUsed: 'mvp-footprint',
        isCensusBacked: false,
        isMvpFallback: true,
        isUnverifiedFallback: false,
        showVerifiedMetrics: trust.showVerifiedMetrics,
        displaySourceLabel: trust.displaySourceLabel,
        sourceNote: trust.displaySourceLabel,
        verifiedEmptyMessage: VERIFIED_EMPTY_MESSAGE,
        breakdownEmptyMessage: BREAKDOWN_EMPTY_MESSAGE,
        fp: mvpFp,
        countryBreakdown: null,
        chainScaleBreakdown: null,
        locationTypeBreakdown: null
      }, brand);
    }

    var cs = brand.censusSummary;
    var m = cs.metrics || {};
    var b = cs.breakdowns || {};
    var fp = Object.assign({}, baseFp);
    fp.formValues = Object.assign({}, baseFp.formValues || {});
    fp.totalExistingHotels = m.totalOpenHotels;
    fp.totalExistingRooms = m.totalOpenKeys;
    fp.totalNewBuildHotels = m.totalPipelineHotels;
    fp.totalNewBuildRooms = m.totalPipelineKeys;
    fp.totalConversionHotels = 0;
    fp.totalConversionRooms = 0;
    fp.totalPipelineHotels = m.totalPipelineHotels || 0;
    fp.totalPipelineRooms = m.totalPipelineKeys || 0;
    fp.formValues.numberOfMarkets = m.countryCount;
    var censusRegionRows = sortBreakdownForPortfolioDisplay(b.dealalityRegion || []);
    var censusCountryRows = sortBreakdownForPortfolioDisplay(b.country || []);
    var regionalDistribution = breakdownToRegionalDistribution(b.dealalityRegion);
    var censusDistributionMissing =
      !censusRegionRows.length && !censusCountryRows.length;
    if (
      censusDistributionMissing &&
      baseFp.regionalDistribution &&
      typeof baseFp.regionalDistribution === 'object' &&
      Object.keys(baseFp.regionalDistribution).length
    ) {
      regionalDistribution = Object.assign({}, baseFp.regionalDistribution);
    }
    var stillNoDistribution = !Object.keys(regionalDistribution).length;
    fp.regionalDistribution = regionalDistribution;
    fp.locationDistribution = breakdownToLocationDistribution(b.locationType);

    return finalizeFootprintDisplayModel({
      useCensus: true,
      sourceUsed: 'census',
      isCensusBacked: true,
      isMvpFallback: false,
      isUnverifiedFallback: false,
      showVerifiedMetrics: true,
      displaySourceLabel: trust.displaySourceLabel,
      sourceNote: SOURCE_NOTE_CENSUS,
      metricsBanner: null,
      censusBreakdownNotice: stillNoDistribution ? BREAKDOWN_EMPTY_MESSAGE : null,
      verifiedEmptyMessage: VERIFIED_EMPTY_MESSAGE,
      breakdownEmptyMessage: BREAKDOWN_EMPTY_MESSAGE,
      fp: fp,
      countryBreakdown: censusCountryRows.length ? censusCountryRows : null,
      dealalityRegionBreakdown: censusRegionRows.length ? censusRegionRows : null,
      chainScaleBreakdown: sortBreakdownForPortfolioDisplay(b.chainScale || []),
      locationTypeBreakdown: sortBreakdownForPortfolioDisplay(b.locationType || [])
    }, brand);
  }

  global.BrandExplorerCensusMetrics = {
    SOURCE_NOTE_CENSUS: SOURCE_NOTE_CENSUS,
    SOURCE_NOTE_MVP: SOURCE_NOTE_MVP,
    SOURCE_NOTE_MVP_VERIFIED: SOURCE_NOTE_MVP_VERIFIED,
    SOURCE_NOTE_MVP_ESTIMATED: SOURCE_NOTE_MVP_ESTIMATED,
    VERIFIED_EMPTY_MESSAGE: VERIFIED_EMPTY_MESSAGE,
    BREAKDOWN_EMPTY_MESSAGE: BREAKDOWN_EMPTY_MESSAGE,
    useCensusSummary: useCensusSummary,
    footprintTrustModel: footprintTrustModel,
    footprintDisplayModel: footprintDisplayModel,
    footprintPipelineTotals: footprintPipelineTotals,
    footprintHasMetricValues: footprintHasMetricValues,
    breakdownToTableRows: breakdownToTableRows,
    breakdownRowToPortfolioRow: breakdownRowToPortfolioRow,
    normalizeBreakdownRow: normalizeBreakdownRow,
    sortBreakdownForPortfolioDisplay: sortBreakdownForPortfolioDisplay,
    censusBreakdownFromBrand: censusBreakdownFromBrand,
    unverifiedFootprintHtml: unverifiedFootprintHtml
  };
})(typeof window !== 'undefined' ? window : globalThis);
