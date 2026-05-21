/**
 * Brand Explorer detail — Operator Gold Mock shell + full Brand Setup API payload.
 * Tabs: Requirements & Standards, Dev. Support & Legal.
 */
(function () {
  'use strict';

  var TAB_DEFS = [
    { id: 'requirements', label: 'Requirements &<br>Standards' },
    { id: 'support-legal', label: 'Dev. Support<br>&amp; Legal' }
  ];

  var TAB_ICONS = {
    requirements: '<svg viewBox="0 0 24 24"><path d="M9 11l3 3L22 4"></path><path d="M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11"></path></svg>',
    'support-legal': '<svg viewBox="0 0 24 24"><path d="M12 3v18"></path><path d="M5 10h14"></path><path d="M5 14h14"></path><path d="M8 7l4-4 4 4"></path><path d="M8 21l4 4 4-4"></path></svg>'
  };

  var NESTED = {
    loyaltyCommercial: 1,
    feeStructure: 1,
    brandStandards: 1,
    dealTerms: 1,
    portfolioPerformance: 1,
    projectFit: 1,
    operationalSupport: 1,
    legalTerms: 1,
    loadWarnings: 1,
    projectFitDebug: 1,
    brandExplorer: 1
  };

  function escapeHtml(text) {
    if (text == null || text === '') return '';
    return String(text)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function humanizeKey(k) {
    var s = String(k)
      .replace(/([a-z\d])([A-Z])/g, '$1 $2')
      .replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
      .replace(/_/g, ' ')
      .replace(/\s+/g, ' ')
      .trim()
      .toLowerCase();
    s = s.replace(/\b[a-z]/g, function (m) {
      return m.toUpperCase();
    });
    return s
      .replace(/\bOta\b/g, 'OTA')
      .replace(/\bCrs\b/g, 'CRS')
      .replace(/\bIhg\b/g, 'IHG')
      .replace(/\bGds\b/g, 'GDS')
      .replace(/\bUrl\b/g, 'URL')
      .replace(/\bApi\b/g, 'API')
      .replace(/\bPms\b/g, 'PMS')
      .replace(/\bRevpar\b/g, 'RevPAR')
      .replace(/\bRfp\b/g, 'RFP')
      .replace(/\bQa\b/g, 'QA')
      .replace(/\bNa\b/g, 'NA')
      .replace(/\bEu\b/g, 'EU')
      .replace(/\bMea\b/g, 'MEA')
      .replace(/\bApac\b/g, 'APAC')
      .replace(/\bCala\b/g, 'CALA')
      .replace(/\bEmea\b/g, 'EMEA')
      .replace(/\bLatam\b/g, 'LATAM');
  }

  function formatValue(v) {
    if (v == null || v === '') return '';
    if (Array.isArray(v)) {
      return v
        .map(function (x) {
          return formatValue(x);
        })
        .filter(Boolean)
        .join(', ');
    }
    if (typeof v === 'boolean') return v ? 'Yes' : 'No';
    if (typeof v === 'number') return String(v);
    if (typeof v === 'object') return escapeHtml(JSON.stringify(v));
    return String(v);
  }

  function hasVal(v) {
    if (v == null || v === '') return false;
    if (Array.isArray(v)) return v.length > 0;
    return true;
  }

  function explorerBlocksForSlot(brand, slotKey) {
    var be = brand.brandExplorer;
    if (!be || !Array.isArray(be.blocks)) return [];
    function imgRank(b) {
      if (!b || !b.imageUrl) return 0;
      var u = String(b.imageUrl).trim();
      return u.indexOf('http') === 0 ? 1 : 0;
    }
    var rows = be.blocks.filter(function (b) {
      return b && String(b.slotKey) === String(slotKey);
    });
    rows.sort(function (a, b) {
      var ir = imgRank(b) - imgRank(a);
      if (ir !== 0) return ir;
      var as = typeof a.sort === 'number' && !isNaN(a.sort) ? a.sort : 0;
      var bs = typeof b.sort === 'number' && !isNaN(b.sort) ? b.sort : 0;
      if (as !== bs) return as - bs;
      return String(a.recordId || '').localeCompare(String(b.recordId || ''));
    });
    return rows;
  }

  function explorerMergedBody(brand, slotKey, joinStr) {
    joinStr = joinStr == null ? '\n\n' : joinStr;
    return explorerBlocksForSlot(brand, slotKey)
      .map(function (r) {
        var t = hasVal(r.title) ? String(r.title).trim() : '';
        var bd = hasVal(r.body) ? String(r.body).trim() : '';
        if (t && bd) return t + ': ' + bd;
        return bd || t;
      })
      .filter(hasVal)
      .join(joinStr);
  }

  function splitToTags(val) {
    if (Array.isArray(val)) return val.map(String).filter(Boolean);
    return String(val)
      .split(/[;,]\s*/)
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean);
  }

  function isYesNoDisplay(v) {
    if (typeof v === 'boolean') return true;
    var s = String(v).trim().toLowerCase();
    return s === 'yes' || s === 'no';
  }

  function boolBadgeHtml(v) {
    var yes = typeof v === 'boolean' ? v : String(v).trim().toLowerCase() === 'yes';
    return (
      '<span class="be-bool ' +
      (yes ? 'be-bool--yes' : 'be-bool--no') +
      '">' +
      escapeHtml(yes ? 'Yes' : 'No') +
      '</span>'
    );
  }

  function fieldCardHtml(label, innerHtml, wide) {
    return (
      '<div class="be-field-card' +
      (wide ? ' be-field-card--wide' : '') +
      '"><div class="be-field-label">' +
      escapeHtml(label) +
      '</div><div class="be-field-value">' +
      innerHtml +
      '</div></div>'
    );
  }

  function subsectionHtml(title, inner) {
    return (
      '<div class="be-subsection"><h3 class="be-subsection-title">' +
      escapeHtml(title) +
      '</h3>' +
      inner +
      '</div>'
    );
  }

  function footprintFormKeyHumanize(k) {
    var s = String(k);
    var geo = s.match(/^geo\s+([a-z0-9]+)\s+(.+)$/i);
    if (geo) {
      var code = geo[1].toLowerCase();
      var regionNames = {
        na: 'North America',
        am: 'Americas',
        emea: 'EMEA',
        eu: 'Europe',
        apac: 'APAC',
        cala: 'CALA',
        mea: 'MEA',
        latam: 'Latin America',
        global: 'Global'
      };
      var rn = regionNames[code] || code.toUpperCase();
      return rn + ' — ' + humanizeKey(geo[2]);
    }
    return humanizeKey(k);
  }

  function locationMixBars(loc) {
    var entries = Object.keys(loc || {}).map(function (k) {
      var n = typeof loc[k] === 'number' ? loc[k] : parseFloat(loc[k]) || 0;
      if (n > 0 && n <= 1) n = n * 100;
      return { name: k, n: n };
    });
    entries.sort(function (a, b) {
      return b.n - a.n;
    });
    return (
      '<div class="be-loc-wrap">' +
      entries
        .map(function (e) {
          var pct = Math.min(100, Math.max(0, Math.round(e.n * 10) / 10));
          var w = Math.min(100, Math.max(0, pct));
          return (
            '<div class="be-loc-row">' +
            '<span class="be-loc-name">' +
            escapeHtml(e.name) +
            '</span>' +
            '<div class="be-loc-track" aria-hidden="true"><div class="be-loc-fill" style="width:' +
            w +
            '%"></div></div>' +
            '<span class="be-loc-pct">' +
            escapeHtml(String(pct)) +
            '%</span></div>'
          );
        })
        .join('') +
      '</div>'
    );
  }

  function toNumber(v) {
    if (typeof v === 'number') return Number.isFinite(v) ? v : 0;
    var n = parseFloat(v);
    return Number.isFinite(n) ? n : 0;
  }

  function regionalDistributionExplorerHtml(rd) {
    var metrics = [
      { id: 'hotels', label: 'Hotels' },
      { id: 'rooms', label: 'Rooms' },
      { id: 'pipelineHotels', label: 'Pipeline Hotels' },
      { id: 'pipelineRooms', label: 'Pipeline Rooms' }
    ];
    var regions = Object.keys(rd || {}).map(function (name) {
      var x = rd[name] || {};
      return {
        name: name,
        hotels: toNumber(x.hotels),
        rooms: toNumber(x.rooms),
        pipelineHotels: toNumber(x.pipelineHotels),
        pipelineRooms: toNumber(x.pipelineRooms)
      };
    });
    if (!regions.length) return '';

    function rowsFor(metricId) {
      var max = regions.reduce(function (m, r) {
        return Math.max(m, r[metricId] || 0);
      }, 0);
      if (!max) max = 1;
      return regions
        .slice()
        .sort(function (a, b) {
          return (b[metricId] || 0) - (a[metricId] || 0);
        })
        .map(function (r) {
          var value = r[metricId] || 0;
          var width = Math.max(2, Math.round((value / max) * 100));
          return (
            '<div class="be-dist-row" data-metric="' +
            metricId +
            '">' +
            '<div class="be-dist-row__head"><span class="be-dist-row__region">' +
            escapeHtml(r.name) +
            '</span><span class="be-dist-row__value">' +
            escapeHtml(String(value)) +
            '</span></div>' +
            '<div class="be-dist-row__track"><div class="be-dist-row__fill" style="width:' +
            width +
            '%"></div></div></div>'
          );
        })
        .join('');
    }

    var toggles = metrics
      .map(function (m, i) {
        return (
          '<button type="button" class="be-dist-toggle' +
          (i === 0 ? ' active' : '') +
          '" data-metric="' +
          m.id +
          '">' +
          escapeHtml(m.label) +
          '</button>'
        );
      })
      .join('');

    var allRows = metrics
      .map(function (m, i) {
        return (
          '<div class="be-dist-group' +
          (i === 0 ? ' active' : '') +
          '" data-metric-group="' +
          m.id +
          '">' +
          rowsFor(m.id) +
          '</div>'
        );
      })
      .join('');

    return (
      '<div class="be-dist" id="brandRegionalDistribution">' +
      '<div class="be-dist__toolbar">' +
      toggles +
      '</div>' +
      '<div class="be-dist__body">' +
      allRows +
      '</div>' +
      '</div>'
    );
  }

  function portfolioDistributionViews(brand, fp) {
    var regionRows = [];
    var rd = fp.regionalDistribution || {};
    Object.keys(rd).forEach(function (region) {
      var x = rd[region] || {};
      var eh = toNumber(x.hotels);
      var er = toNumber(x.rooms);
      var ph = toNumber(x.pipelineHotels);
      var pr = toNumber(x.pipelineRooms);
      regionRows.push([
        region,
        eh,
        er,
        ph,
        pr,
        eh + ph,
        er + pr
      ]);
    });
    regionRows.sort(function (a, b) {
      return b[5] - a[5];
    });

    var cs = brand.hotelChainScale || 'Unknown';
    var chainRows = [[
      cs,
      toNumber(fp.totalExistingHotels),
      toNumber(fp.totalExistingRooms),
      toNumber(fp.totalNewBuildHotels) + toNumber(fp.totalConversionHotels),
      toNumber(fp.totalNewBuildRooms) + toNumber(fp.totalConversionRooms),
      toNumber(fp.totalExistingHotels) + toNumber(fp.totalNewBuildHotels) + toNumber(fp.totalConversionHotels),
      toNumber(fp.totalExistingRooms) + toNumber(fp.totalNewBuildRooms) + toNumber(fp.totalConversionRooms)
    ]];

    var bname = brand.name || brand.brandName || 'Brand';
    var brandRows = [[
      bname,
      toNumber(fp.totalExistingHotels),
      toNumber(fp.totalExistingRooms),
      toNumber(fp.totalNewBuildHotels) + toNumber(fp.totalConversionHotels),
      toNumber(fp.totalNewBuildRooms) + toNumber(fp.totalConversionRooms),
      toNumber(fp.totalExistingHotels) + toNumber(fp.totalNewBuildHotels) + toNumber(fp.totalConversionHotels),
      toNumber(fp.totalExistingRooms) + toNumber(fp.totalNewBuildRooms) + toNumber(fp.totalConversionRooms)
    ]];

    return {
      region: regionRows,
      chain: chainRows,
      brand: brandRows
    };
  }

  function distributionTableHtml(headerLabel, rows) {
    if (!rows || !rows.length) {
      return '<p class="be-note">No distribution rows available for this view.</p>';
    }
    var html =
      '<div class="gold-footprint-table-wrap"><table class="gold-footprint-table"><thead><tr>' +
      '<th scope="col">' + escapeHtml(headerLabel) + '</th>' +
      '<th scope="col">Existing Hotels</th>' +
      '<th scope="col">Existing Rooms</th>' +
      '<th scope="col">Pipeline Hotels</th>' +
      '<th scope="col">Pipeline Rooms</th>' +
      '<th scope="col">Total Hotels</th>' +
      '<th scope="col">Total Rooms</th>' +
      '</tr></thead><tbody>';
    rows.forEach(function (r) {
      html +=
        '<tr>' +
        '<th scope="row">' + escapeHtml(formatValue(r[0])) + '</th>' +
        '<td>' + escapeHtml(formatValue(r[1])) + '</td>' +
        '<td>' + escapeHtml(formatValue(r[2])) + '</td>' +
        '<td>' + escapeHtml(formatValue(r[3])) + '</td>' +
        '<td>' + escapeHtml(formatValue(r[4])) + '</td>' +
        '<td>' + escapeHtml(formatValue(r[5])) + '</td>' +
        '<td>' + escapeHtml(formatValue(r[6])) + '</td>' +
        '</tr>';
    });
    html += '</tbody></table></div>';
    return html;
  }

  function portfolioDistributionHtml(brand, fp) {
    var views = portfolioDistributionViews(brand, fp);
    return (
      '<div class="be-portdist" id="brandPortfolioDistribution">' +
      '<div class="be-portdist__hint">View By Region, Chain Scale, or Brand</div>' +
      '<div class="be-portdist__toggles">' +
      '<button type="button" class="be-portdist-toggle active" data-view="region">Region</button>' +
      '<button type="button" class="be-portdist-toggle" data-view="chain">Chain Scale</button>' +
      '<button type="button" class="be-portdist-toggle" data-view="brand">Brand</button>' +
      '</div>' +
      '<div class="be-portdist-panel active" data-panel="region">' +
      distributionTableHtml('Region', views.region) +
      '</div>' +
      '<div class="be-portdist-panel" data-panel="chain">' +
      distributionTableHtml('Chain Scale', views.chain) +
      '</div>' +
      '<div class="be-portdist-panel" data-panel="brand">' +
      distributionTableHtml('Brand', views.brand) +
      '</div>' +
      '</div>'
    );
  }

  function groupRowsByLabel(rows, rules) {
    var buckets = rules.map(function (r) {
      return { title: r.title, rows: [] };
    });
    var other = [];
    rows.forEach(function (row) {
      var label = row[0];
      var placed = false;
      for (var i = 0; i < rules.length; i++) {
        if (rules[i].match(label)) {
          buckets[i].rows.push(row);
          placed = true;
          break;
        }
      }
      if (!placed) other.push(row);
    });
    if (other.length) buckets.push({ title: 'Other', rows: other });
    return buckets.filter(function (b) {
      return b.rows.length > 0;
    });
  }

  function renderRowsAsCardGrid(rows) {
    return (
      '<div class="be-card-grid be-card-grid--2">' +
      rows
        .map(function (row) {
          var label = row[0];
          var val = row[1];
          var inner;
          var wide = false;
          if (typeof val === 'string' && val.indexOf('<') !== -1) inner = val;
          else if (isYesNoDisplay(val)) inner = boolBadgeHtml(val);
          else if (typeof val === 'string' && val.length > 220) {
            inner = '<div class="be-prose">' + renderLongTextAsHtml(val) + '</div>';
            wide = true;
          } else if (
            typeof val === 'string' &&
            val.length > 24 &&
            (val.indexOf(',') !== -1 || val.indexOf(';') !== -1)
          ) {
            inner =
              '<div class="be-tags">' +
              splitToTags(val)
                .map(function (t) {
                  return '<span class="be-tag">' + escapeHtml(t) + '</span>';
                })
                .join('') +
              '</div>';
          } else inner = '<span class="be-field-value--muted">' + escapeHtml(formatValue(val)) + '</span>';
          return fieldCardHtml(label, inner, wide);
        })
        .join('') +
      '</div>'
    );
  }

  function renderRowGroupsAsSubsections(rowGroups) {
    return rowGroups
      .map(function (g) {
        return subsectionHtml(g.title, renderRowsAsCardGrid(g.rows));
      })
      .join('');
  }

  function section(title, inner) {
    return (
      '<section class="section"><h2 class="section-title">' +
      escapeHtml(title) +
      '</h2>' +
      inner +
      '</section>'
    );
  }

  function clusterHtml(title, inner) {
    return (
      '<div class="cluster"><h3>' +
      escapeHtml(title) +
      '</h3>' +
      inner +
      '</div>'
    );
  }

  function cardHtml(title, body) {
    return (
      '<div class="card"><h3>' +
      escapeHtml(title) +
      '</h3><p>' +
      body +
      '</p></div>'
    );
  }

  function kvTableFromRows(rows) {
    if (!rows || !rows.length) return '';
    var html =
      '<div class="gold-footprint-table-wrap"><table class="gold-footprint-table units-staffing-table"><tbody>';
    rows.forEach(function (row) {
      html +=
        '<tr><th scope="row">' +
        escapeHtml(row[0]) +
        '</th><td>' +
        (typeof row[1] === 'string' && row[1].indexOf('<') !== -1 ? row[1] : escapeHtml(formatValue(row[1]))) +
        '</td></tr>';
    });
    html += '</tbody></table></div>';
    return html;
  }

  function rowsFromObject(obj, options) {
    options = options || {};
    var skip = options.skip || {};
    var labelMap = options.labelMap || {};
    var rows = [];
    if (!obj || typeof obj !== 'object') return rows;
    Object.keys(obj).forEach(function (k) {
      if (skip[k]) return;
      var v = obj[k];
      if (!hasVal(v)) return;
      if (typeof v === 'object' && v !== null && !Array.isArray(v) && !(v instanceof Date)) return;
      var label = labelMap[k] || humanizeKey(k);
      rows.push([label, v]);
    });
    return rows;
  }

  function renderLongTextAsHtml(text) {
    if (!text) return '';
    var p = escapeHtml(String(text)).split(/\n\n+/);
    return p
      .map(function (chunk) {
        return '<p>' + chunk.replace(/\n/g, '<br>') + '</p>';
      })
      .join('');
  }

  function chainStripeColor(scale) {
    if (!scale) return null;
    var s = String(scale).toLowerCase();
    if (s.indexOf('luxury') !== -1) return '#d4af37';
    if (s.indexOf('upper upscale') !== -1) return '#9b59b6';
    if (s.indexOf('upscale') !== -1 && s.indexOf('upper') === -1) return '#3498db';
    if (s.indexOf('upper midscale') !== -1) return '#2ecc71';
    if (s.indexOf('midscale') !== -1) return '#1abc9c';
    if (s.indexOf('economy') !== -1) return '#e67e22';
    return null;
  }

  function chainScaleFromBrand(brand) {
    if (!brand) return '';
    return brand.hotelChainScale || brand.chainScale || '';
  }

  function parseHexRgb(hex) {
    var h = String(hex || '')
      .trim()
      .replace(/^#/, '');
    if (h.length === 3) {
      h = h
        .split('')
        .map(function (c) {
          return c + c;
        })
        .join('');
    }
    if (!/^[0-9a-fA-F]{6}$/.test(h)) return null;
    return {
      r: parseInt(h.slice(0, 2), 16),
      g: parseInt(h.slice(2, 4), 16),
      b: parseInt(h.slice(4, 6), 16)
    };
  }

  var CHAIN_SCALE_THEME_VAR_NAMES = [
    '--accent--primary-1',
    '--accent',
    '--hero-stripe-bg',
    '--hero-tag',
    '--accent-soft',
    '--accent-line'
  ];

  function applyChainScaleThemeVars(el, hex) {
    if (!el) return;
    if (!hex) {
      CHAIN_SCALE_THEME_VAR_NAMES.forEach(function (name) {
        el.style.removeProperty(name);
      });
      el.removeAttribute('data-be-chain-scale-theme');
      return;
    }
    var rgb = parseHexRgb(hex);
    if (!rgb) return;
    var accent = hex.indexOf('#') === 0 ? hex : '#' + hex;
    el.style.setProperty('--accent--primary-1', accent);
    el.style.setProperty('--accent', accent);
    el.style.setProperty('--hero-stripe-bg', accent);
    el.style.setProperty('--hero-tag', accent);
    el.style.setProperty(
      '--accent-soft',
      'rgba(' + rgb.r + ', ' + rgb.g + ', ' + rgb.b + ', 0.14)'
    );
    el.style.setProperty(
      '--accent-line',
      'rgba(' + rgb.r + ', ' + rgb.g + ', ' + rgb.b + ', 0.45)'
    );
    el.setAttribute('data-be-chain-scale-theme', '1');
  }

  function chainScaleThemeRoots(extra) {
    var roots = [
      document.getElementById('brandRoot'),
      document.getElementById('beCombinedDetailView'),
      document.getElementById('beAtelierExplorer'),
      document.getElementById('beAtelierRoot'),
      document.getElementById('beCombinedPopupAtelierRoot')
    ];
    var popupPanel = document.querySelector(
      '#beCombinedBrandDetailPopup .brand-detail-popup-panel'
    );
    if (popupPanel) roots.push(popupPanel);
    if (extra) {
      if (extra.nodeType === 1) roots.push(extra);
      else if (extra.length) {
        for (var i = 0; i < extra.length; i++) roots.push(extra[i]);
      }
    }
    var seen = [];
    return roots.filter(function (el) {
      if (!el || seen.indexOf(el) !== -1) return false;
      seen.push(el);
      return true;
    });
  }

  function applyChainScaleTheme(brand, extraRoots) {
    var hex = chainStripeColor(chainScaleFromBrand(brand));
    chainScaleThemeRoots(extraRoots).forEach(function (el) {
      applyChainScaleThemeVars(el, hex);
    });
    applyHeroStripe(brand);
  }

  function clearChainScaleTheme() {
    chainScaleThemeRoots().forEach(function (el) {
      applyChainScaleThemeVars(el, null);
    });
    ['brandHero', 'brandHeroPopup'].forEach(function (hid) {
      var el = document.getElementById(hid);
      if (el) el.style.removeProperty('--hero-stripe-bg');
    });
  }

  function renderHero(brand) {
    var logo = brand.logo && String(brand.logo).indexOf('http') === 0 ? brand.logo : '';
    var name = brand.name || brand.brandName || 'Brand';
    var tag =
      brand.brandTaglineMotto ||
      brand.hotelChainScale ||
      brand.parentCompany ||
      '';
    var statement = brand.brandPositioning || '';
    if (statement.length > 520) statement = statement.slice(0, 517) + '…';

    var meta = [];
    if (brand.parentCompany) meta.push(['Parent', brand.parentCompany]);
    if (brand.hotelChainScale) meta.push(['Chain Scale', brand.hotelChainScale]);
    if (brand.hotelServiceModel) meta.push(['Service Model', brand.hotelServiceModel]);
    if (brand.brandModelFormat) meta.push(['Brand Model', brand.brandModelFormat]);
    if (brand.yearBrandLaunched) meta.push(['Launched', brand.yearBrandLaunched]);
    if (brand.brandWebsite) meta.push(['Website', brand.brandWebsite]);

    var metaHtml = meta
      .map(function (pair) {
        return (
          '<div class="meta-card"><div class="label">' +
          escapeHtml(pair[0]) +
          '</div><div class="value">' +
          escapeHtml(formatValue(pair[1])) +
          '</div></div>'
        );
      })
      .join('');

    var logoBlock = logo
      ? '<img class="hero-logo" src="' +
        escapeHtml(logo) +
        '" alt="' +
        escapeHtml(name) +
        '" referrerpolicy="no-referrer" />'
      : '';

    return (
      '<header class="hero" id="brandHero">' +
      '<div class="hero-title">' +
      logoBlock +
      '<h1 id="heroBrandName">' +
      escapeHtml(name) +
      '</h1></div>' +
      (tag ? '<div class="tag">' + escapeHtml(tag) + '</div>' : '') +
      (statement
        ? '<div class="statement">' + renderLongTextAsHtml(statement) + '</div>'
        : '') +
      (metaHtml ? '<div class="hero-meta">' + metaHtml + '</div>' : '') +
      '</header>'
    );
  }

  function presentationFootprintLine(brand) {
    var fp = brand.footprint || {};
    if (typeof BrandExplorerCensusMetrics !== 'undefined' && BrandExplorerCensusMetrics.footprintDisplayModel) {
      var disp = BrandExplorerCensusMetrics.footprintDisplayModel(brand);
      if (!disp.showVerifiedMetrics) return '';
      fp = disp.fp || fp;
    }
    var openH = fp.totalExistingHotels;
    var pipH = (Number(fp.totalNewBuildHotels) || 0) + (Number(fp.totalConversionHotels) || 0);
    var rd = fp.regionalDistribution && typeof fp.regionalDistribution === 'object' ? fp.regionalDistribution : {};
    var rdKeys = Object.keys(rd);
    if (openH == null || openH === '') {
      if (rdKeys.length) {
        var sumOpen = 0;
        rdKeys.forEach(function (k) {
          sumOpen += Number((rd[k] || {}).hotels) || 0;
        });
        if (sumOpen) openH = sumOpen;
      }
    }
    if (!pipH && rdKeys.length) {
      var sumPipe = 0;
      rdKeys.forEach(function (k) {
        sumPipe += Number((rd[k] || {}).pipelineHotels) || 0;
      });
      if (sumPipe) pipH = sumPipe;
    }
    var parts = [];
    if ((openH != null && openH !== '') || pipH > 0) {
      parts.push(String(openH != null && openH !== '' ? openH : '0') + ' open / ' + (pipH > 0 ? String(pipH) : '0') + ' pipeline hotels');
    }
    var markets = fp.formValues && fp.formValues.numberOfMarkets;
    if (markets != null && markets !== '') parts.push(String(markets) + ' markets');
    var sm = fp.priorityCities || (fp.formValues && fp.formValues.specificMarkets);
    if (sm) parts.push(String(sm).trim());
    return parts.length ? parts.join(' · ') : '';
  }

  /** Short “benefit zones” line — optional Airtable slot hero.benefit_zones, else differentiators / footprint mix. */
  function presentationBenefitZonesLine(brand) {
    var slot = explorerMergedBody(brand, 'hero.benefit_zones', ', ');
    if (hasVal(slot)) {
      return String(slot).trim();
    }
    var diff = brand.keyBrandDifferentiators;
    if (diff) {
      var bullets = String(diff)
        .split(/\n|;|•/g)
        .map(function (s) {
          return s.replace(/^\s*[-*]\s*/, '').trim();
        })
        .filter(Boolean)
        .slice(0, 3);
      if (bullets.length) return bullets.join(', ');
    }
    var fv = brand.footprint && brand.footprint.formValues ? brand.footprint.formValues : {};
    var parts = [];
    if (hasVal(fv.newBuildExperience)) parts.push('New build: ' + String(fv.newBuildExperience).trim());
    if (hasVal(fv.conversionExperience)) parts.push('Conversion: ' + String(fv.conversionExperience).trim());
    if (hasVal(fv.renovationExperience)) parts.push('Renovation/rebrand: ' + String(fv.renovationExperience).trim());
    return parts.join(' · ');
  }

  /** One-line operator fit — optional slot hero.operator_compat, else Operational Support / profile. */
  function presentationOperatorCompatLine(brand) {
    var slot = explorerMergedBody(brand, 'hero.operator_compat');
    if (hasVal(slot)) {
      return String(slot).trim();
    }
    var op = brand.operationalSupport || {};
    if (hasVal(op.specializations)) return String(op.specializations).trim();
    if (hasVal(op.testimonials)) {
      var t0 = String(op.testimonials).trim().split(/\n+/)[0];
      if (t0) return t0;
    }
    var proof = explorerMergedBody(brand, 'overview.proof_operator');
    if (hasVal(proof)) {
      var proofFirst = String(proof)
        .split(/\n+/)
        .map(function (s) {
          return s.trim();
        })
        .filter(Boolean)[0];
      if (proofFirst) return proofFirst;
    }
    if (hasVal(brand.brandValueProposition)) {
      var vpFirst = String(brand.brandValueProposition)
        .split(/\n\n+/)
        .map(function (s) {
          return s.trim();
        })
        .filter(Boolean)[0];
      if (vpFirst) return vpFirst;
    }
    return '';
  }

  function presentationLoyaltyLine(brand) {
    var fv = brand.loyaltyCommercial && brand.loyaltyCommercial.formValues;
    if (!fv) return '';
    var name = fv.typicalLoyaltyProgramName;
    var mem = fv.totalGlobalMembersMillions;
    var parts = [];
    if (name) parts.push(String(name));
    if (mem) parts.push('~' + String(mem).replace(/\s*m\s*$/i, '') + 'M members (est.)');
    var pct = fv.typicalLoyaltyRoomsPercent;
    if (pct) parts.push(String(pct) + '% rooms from loyalty (est.)');
    return parts.join(' — ');
  }

  function presentationOperatingModel(brand) {
    var a = brand.brandModelFormat;
    var b = brand.hotelServiceModel;
    if (!a && !b) return '';
    return [a, b].filter(Boolean).join(' · ');
  }

  function presentationIsDemoBrand(brand) {
    if (!brand) return false;
    var id = String(brand.id || '').toLowerCase();
    if (id.indexOf('mock') !== -1) return true;
    var n = String(brand.name || '').toLowerCase().trim();
    return (
      n === 'atelier north' ||
      n === 'velvet crown' ||
      n === 'summit house' ||
      n === 'voco' ||
      n.indexOf('voco ') === 0
    );
  }

  /** Atelier North education–style header (unified combined + modal). opts.heroId defaults to brandHero; use brandHeroPopup in modal. */
  function renderPresentationHero(brand, opts) {
    opts = opts || {};
    var heroId = opts.heroId || 'brandHero';
    var name = brand.name || brand.brandName || 'Brand';
    var logo = brand.logo && String(brand.logo).indexOf('http') === 0 ? brand.logo : '';
    var pid = brand.id != null && String(brand.id).trim() !== '' ? String(brand.id).trim() : '';
    var setupHref = pid ? '/brand-setup?id=' + encodeURIComponent(pid) : '/brand-setup';
    var posRaw = brand.brandPositioning ? String(brand.brandPositioning) : '';
    var positionLine = brand.brandTaglineMotto || (posRaw ? posRaw.slice(0, 140) + (posRaw.length > 140 ? '…' : '') : '');
    var summaryRaw =
      brand.brandCustomerPromise ||
      brand.brandValueProposition ||
      posRaw ||
      '';
    var summary = summaryRaw ? String(summaryRaw) : '';
    var isDemo = presentationIsDemoBrand(brand);

    var chips = [];
    if (brand.hotelChainScale) chips.push(brand.hotelChainScale);
    if (brand.brandModelFormat) chips.push(brand.brandModelFormat);
    if (brand.brandArchitecture) chips.push(brand.brandArchitecture);
    var chipHtml = chips
      .slice(0, 5)
      .map(function (c) {
        return '<span class="tag-chip">' + escapeHtml(String(c).toUpperCase()) + '</span>';
      })
      .join('');

    function metaCard(label, val) {
      var raw = formatValue(val);
      var text = raw != null && String(raw).trim() !== '' ? String(raw).trim() : '';
      if (!text) {
        return (
          '<div class="meta-card"><div class="label">' +
          escapeHtml(label) +
          '</div><div class="value"><span class="meta-card__value-clamp meta-card__value-clamp--empty">—</span></div></div>'
        );
      }
      return (
        '<div class="meta-card"><div class="label">' +
        escapeHtml(label) +
        '</div><div class="value"><span class="meta-card__value-clamp">' +
        escapeHtml(text) +
        '</span></div></div>'
      );
    }

    var logoBlock = logo
      ? '<img class="brand-hero__logo" src="' +
        escapeHtml(logo) +
        '" alt="" width="120" height="38" referrerpolicy="no-referrer" />'
      : '<div class="brand-hero__logo brand-hero__logo--initial" aria-hidden="true">' +
        escapeHtml((name || '?').charAt(0).toUpperCase()) +
        '</div>';

    var bannerHtml = isDemo
      ? '<div class="brand-hero__mock-warning" role="note" aria-live="polite">' +
        '<svg class="brand-hero__mock-warning-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
        '<path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/>' +
        '<line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>' +
        '</svg>' +
        '<span class="brand-hero__mock-warning-text"><strong>Mock Data Display:</strong> This dashboard shows sample data to demonstrate the intended presentation format.</span>' +
        '</div>'
      : '';

    var updatedMeta = isDemo
      ? '<span class="meta-muted">Last updated March 2026</span>'
      : '<span class="meta-muted">' +
        escapeHtml(
          hasVal(brand.explorerHeroDataSource) && String(brand.explorerHeroDataSource).trim()
            ? String(brand.explorerHeroDataSource).trim()
            : 'Live Airtable / Brand Setup data'
        ) +
        '</span>';

    var verificationText =
      hasVal(brand.explorerHeroVerification) && String(brand.explorerHeroVerification).trim()
        ? String(brand.explorerHeroVerification).trim()
        : 'Verified by brand';

    return (
      '<section class="brand-hero be-combined-presentation-hero" id="' +
      escapeHtml(heroId) +
      '" aria-labelledby="brand-profile-name-' +
      escapeHtml(heroId) +
      '">' +
      '<div class="brand-hero__title-row">' +
      logoBlock +
      '<div class="brand-hero__title-block">' +
      '<div class="brand-hero__name-row">' +
      '<h2 id="brand-profile-name-' +
      escapeHtml(heroId) +
      '">' +
      escapeHtml(name) +
      '</h2>' +
      bannerHtml +
      '</div>' +
      (brand.parentCompany
        ? '<p class="brand-hero__parent">' + escapeHtml(brand.parentCompany) + '</p>'
        : '<p class="brand-hero__parent meta-muted">Parent company not set</p>') +
      '<div class="brand-hero__verified-line">' +
      '<span class="badge-verified"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M9 12l2 2 4-4"/><circle cx="12" cy="12" r="9"/></svg>' +
      escapeHtml(verificationText) +
      '</span>' +
      updatedMeta +
      '</div>' +
      '</div>' +
      '<div class="brand-hero__actions">' +
      '<div class="btn-row">' +
      (pid
        ? '<button type="button" class="btn be-brand-save-btn" data-be-brand-id="' +
          escapeHtml(pid) +
          '" aria-pressed="false">Save</button>'
        : '<button type="button" class="btn be-brand-save-btn" disabled title="Brand record id unavailable">Save</button>') +
      '<button type="button" class="btn btn--primary" disabled title="Coming soon">Request Introduction</button>' +
      '</div>' +
      (chipHtml ? '<div class="tag-chip-row" aria-label="Brand Highlights">' + chipHtml + '</div>' : '') +
      '</div>' +
      '</div>' +
      (positionLine ? '<p class="brand-hero__position">' + escapeHtml(positionLine) + '</p>' : '') +
      (summary ? '<p class="brand-hero__summary">' + renderLongTextAsHtml(summary) + '</p>' : '') +
      '<div class="brand-hero__meta" aria-label="Brand metrics">' +
      metaCard('Segment', brand.hotelChainScale || '') +
      metaCard('Operating Model', presentationOperatingModel(brand)) +
      metaCard('Typical Benefit Zones', presentationBenefitZonesLine(brand)) +
      metaCard('Footprint', presentationFootprintLine(brand)) +
      metaCard('Loyalty Strength', presentationLoyaltyLine(brand)) +
      metaCard('Operator Compatibility', presentationOperatorCompatLine(brand)) +
      '</div>' +
      '</section>'
    );
  }

  function applyHeroStripe(brand) {
    var hex = chainStripeColor(chainScaleFromBrand(brand));
    ['brandHero', 'brandHeroPopup'].forEach(function (hid) {
      var el = document.getElementById(hid);
      if (!el) return;
      if (hex) el.style.setProperty('--hero-stripe-bg', hex);
      else el.style.removeProperty('--hero-stripe-bg');
    });
  }

  /** Set native tooltip only when hero meta value is visually line-clamped (scrollHeight overflow). */
  function wirePresentationMetaValueTooltips(root) {
    var scope = root && root.querySelectorAll ? root : document;
    var nodes = scope.querySelectorAll(
      '.brand-hero.be-combined-presentation-hero .meta-card__value-clamp:not(.meta-card__value-clamp--empty)'
    );
    if (!nodes || !nodes.length) return;
    function apply() {
      for (var i = 0; i < nodes.length; i++) {
        var el = nodes[i];
        el.removeAttribute('title');
        el.style.cursor = '';
        var truncated = el.scrollHeight > el.clientHeight + 2;
        if (truncated) {
          var full = (el.textContent || '')
            .replace(/\s+/g, ' ')
            .trim();
          if (full) {
            el.setAttribute('title', full);
            el.style.cursor = 'help';
          }
        }
      }
    }
    requestAnimationFrame(function () {
      requestAnimationFrame(apply);
    });
  }

  function renderFootprint(brand) {
    var fp = brand.footprint || {};
    var parts = [];
    var warn = brand.loadWarnings;
    if (warn && warn.length) {
      parts.push(
        section(
          'Data Load Notes',
          '<p class="gold-mock-tab-empty">Some linked tables could not be loaded: ' +
            escapeHtml(warn.join(', ')) +
            '.</p>'
        )
      );
    }

    var rd = fp.regionalDistribution && typeof fp.regionalDistribution === 'object'
      ? fp.regionalDistribution
      : {};
    var regionNames = Object.keys(rd);
    var marketCount = regionNames.length;
    var cityCount = 0;
    if (Array.isArray(fp.priorityCities)) cityCount = fp.priorityCities.length;
    else if (typeof fp.priorityCities === 'string' && fp.priorityCities.trim()) cityCount = splitToTags(fp.priorityCities).length;
    if (!cityCount && fp.formValues && hasVal(fp.formValues.priorityCities)) {
      var pc = fp.formValues.priorityCities;
      cityCount = Array.isArray(pc) ? pc.length : splitToTags(pc).length;
    }
    var coverage = marketCount >= 5 ? 'Broad' : marketCount >= 3 ? 'Balanced' : marketCount > 0 ? 'Focused' : 'Limited';
    var summary =
      '<div class="be-mkt-summary-grid">' +
      fieldCardHtml('Regions (count)', '<span class="be-field-value--muted">' + escapeHtml(String(marketCount || 0)) + '</span>', false) +
      fieldCardHtml('Cities (markets list)', '<span class="be-field-value--muted">' + escapeHtml(String(cityCount || 0)) + '</span>', false) +
      fieldCardHtml('Coverage / depth', '<span class="be-field-value--muted">' + escapeHtml(coverage) + '</span>', false) +
      '</div>';
    parts.push(section('Markets & Footprint', summary));

    var existingHotels = toNumber(fp.totalExistingHotels);
    var existingRooms = toNumber(fp.totalExistingRooms);
    var pipelineHotels = toNumber(fp.totalNewBuildHotels) + toNumber(fp.totalConversionHotels);
    var pipelineRooms = toNumber(fp.totalNewBuildRooms) + toNumber(fp.totalConversionRooms);

    var totalTable = footprintTableFromRows([
      ['', 'Existing Hotels', 'Existing Rooms', 'Pipeline Hotels', 'Pipeline Rooms'],
      ['Total', existingHotels, existingRooms, pipelineHotels, pipelineRooms]
    ]);

    var metricsInner =
      subsectionHtml('Existing vs. Pipeline (Portfolio)', totalTable) +
      subsectionHtml('Portfolio Distribution', portfolioDistributionHtml(brand, fp));
    parts.push(section('Footprint Metrics', '<div class="be-panel">' + metricsInner + '</div>'));

    var loc = fp.locationDistribution;
    if (loc && typeof loc === 'object' && Object.keys(loc).length) {
      parts.push(
        section(
          'Location Type Mix',
          locationMixBars(loc) +
            '<p class="be-note">Share of properties by location type (where provided).</p>'
        )
      );
    }

    var fv = fp.formValues;
    if (fv && typeof fv === 'object') {
      var fr = [];
      Object.keys(fv).forEach(function (k) {
        if (!hasVal(fv[k])) return;
        fr.push([footprintFormKeyHumanize(k), fv[k]]);
      });
      if (fr.length) {
        // De-dupe: remove fields already represented in top footprint summaries/tables.
        var nonRepeating = fr.filter(function (row) {
          var label = String(row[0] || '').toLowerCase();
          if (/^(north america|americas|emea|europe|apac|cala|mea|latin america|global)\s—/i.test(row[0])) return false;
          if (label.indexOf('existing hotels') !== -1) return false;
          if (label.indexOf('existing rooms') !== -1) return false;
          if (label.indexOf('pipeline hotels') !== -1) return false;
          if (label.indexOf('pipeline rooms') !== -1) return false;
          if (label.indexOf('new build') !== -1) return false;
          if (label.indexOf('conversion') !== -1) return false;
          if (label.indexOf('managed') !== -1 && label.indexOf('percent') !== -1) return false;
          if (label.indexOf('franchised') !== -1 && label.indexOf('percent') !== -1) return false;
          if (label.indexOf('location distribution') !== -1) return false;
          if (/(^|[\s-])(urban|suburban|resort|airport|small metro|interstate|mixed use|mixed-use)($|[\s-])/.test(label)) return false;
          if (label.indexOf('location type') !== -1) return false;
          return true;
        });
        var fpGroups = groupRowsByLabel(nonRepeating, [
          {
            title: 'Regional Detail',
            match: function (label) {
              return /^(North America|Americas|EMEA|Europe|APAC|CALA|MEA|Latin America|Global)\s—/i.test(label);
            }
          }
        ]);
        if (fpGroups.length) {
          parts.push(
            section(
              'Footprint Detail (By Region & Metric)',
              '<div class="be-panel">' + renderRowGroupsAsSubsections(fpGroups) + '</div>'
            )
          );
        }
      }
    }

    if (!parts.length) {
      return '<p class="gold-mock-tab-empty">No footprint data linked for this brand.</p>';
    }
    return parts.join('');
  }

  function kpiBlock(label, val) {
    if (!hasVal(val)) return '';
    return (
      '<div class="kpi kpi--quant"><div class="label">' +
      escapeHtml(label) +
      '</div><div class="value">' +
      escapeHtml(formatValue(val)) +
      '</div></div>'
    );
  }

  function footprintTableFromRows(rows) {
    if (!rows || rows.length < 2) return '';
    var html = '<div class="gold-footprint-table-wrap"><table class="gold-footprint-table"><thead><tr>';
    rows[0].forEach(function (h) {
      html += '<th scope="col">' + escapeHtml(h) + '</th>';
    });
    html += '</tr></thead><tbody>';
    for (var i = 1; i < rows.length; i++) {
      html += '<tr>';
      rows[i].forEach(function (cell, j) {
        if (j === 0) {
          html += '<th scope="row">' + escapeHtml(formatValue(cell)) + '</th>';
        } else {
          html += '<td>' + escapeHtml(formatValue(cell)) + '</td>';
        }
      });
      html += '</tr>';
    }
    html += '</tbody></table></div>';
    return html;
  }

  function renderRequirements(brand) {
    var std = brand.brandStandards || {};

    function pushRow(rows, label, v) {
      if (!hasVal(v)) return;
      if (Array.isArray(v)) rows.push([label, v.join(', ')]);
      else if (typeof v === 'boolean') rows.push([label, v ? 'Yes' : 'No']);
      else rows.push([label, v]);
    }

    var groups = [
      {
        title: 'Core Spaces & Amenities',
        rows: []
      },
      {
        title: 'Food & Beverage',
        rows: []
      },
      {
        title: 'Meetings & Events',
        rows: []
      },
      {
        title: 'Parking & Program Rules',
        rows: []
      },
      {
        title: 'Sustainability & Amenities',
        rows: []
      },
      {
        title: 'Compliance & QA',
        rows: []
      }
    ];

    pushRow(groups[0].rows, 'Lobby', std.lobby);
    pushRow(groups[0].rows, 'Lobby Description', std.lobbyDescription);
    pushRow(groups[0].rows, 'Bar / Beverage', std.barBeverage);
    pushRow(groups[0].rows, 'Fitness', std.fitnessCenter);
    pushRow(groups[0].rows, 'Pool', std.pool);
    pushRow(groups[0].rows, 'Onsite Parking', std.onsiteParking);
    pushRow(groups[0].rows, 'Meeting / Event Space', std.meetingEventSpace);
    pushRow(groups[0].rows, 'Co-Working', std.coworking);
    pushRow(groups[0].rows, 'Grab & Go', std.grabGo);
    pushRow(groups[0].rows, 'Minimum Room Size (Sq Ft)', std.minimumRoomSize);
    pushRow(groups[0].rows, 'Minimum Room Size (Sq M)', std.minimumRoomSizeMeters);
    pushRow(groups[0].rows, 'Brand Standards Narrative', std.brandStandards);

    pushRow(groups[1].rows, 'F&B Outlets Required', std.brandFbOutletsRequired);
    pushRow(groups[1].rows, 'Typical F&B Outlet Count', std.brandFbOutletsCount);
    pushRow(groups[1].rows, 'F&B Program Type', std.brandFbProgramType);
    pushRow(groups[1].rows, 'Outlet Concepts', std.brandFbOutletConcepts);
    pushRow(groups[1].rows, 'F&B Outlet Size', std.brandFbOutletSize);
    pushRow(groups[1].rows, 'F&B Outlet Size Unit', std.brandFbOutletSizeUnit);

    pushRow(groups[2].rows, 'Meeting Space Required', std.brandMeetingSpaceRequired);
    pushRow(groups[2].rows, 'Meeting Rooms Count', std.brandMeetingRoomsCount);
    pushRow(groups[2].rows, 'Meeting Space Size', std.brandMeetingSpaceSize);
    pushRow(groups[2].rows, 'Condo / Residences', std.brandCondoResidencesAllowed);
    pushRow(groups[2].rows, 'Hotel Rental Program', std.brandHotelRentalProgram);

    pushRow(groups[3].rows, 'Parking Required', std.brandParkingRequired);
    pushRow(groups[3].rows, 'Parking Spaces', std.brandParkingSpacesCount);
    pushRow(groups[3].rows, 'Parking Program', std.brandParkingProgramType);

    pushRow(groups[4].rows, 'Sustainability Features', std.brandSustainability);
    pushRow(groups[4].rows, 'Other Sustainability', std.brandSustainabilityOther);
    pushRow(groups[4].rows, 'Additional Amenities', std.brandRequiredAmenities);
    pushRow(groups[4].rows, 'Other Amenities', std.brandRequiredAmenitiesOther);

    pushRow(groups[5].rows, 'Compliance & Safety', std.brandCompliance);
    pushRow(groups[5].rows, 'Other Compliance', std.brandComplianceOther);
    pushRow(groups[5].rows, 'QA / Brand Standards Expectations', std.brandQaExpectations);
    pushRow(groups[5].rows, 'Additional Notes', std.brandStandardsNotes);

    var nonempty = groups.filter(function (g) {
      return g.rows.length > 0;
    });
    if (!nonempty.length) {
      return '<p class="gold-mock-tab-empty">No brand standards record linked.</p>';
    }
    return section(
      'Requirements & Standards',
      '<div class="be-panel">' +
        nonempty
          .map(function (g) {
            return subsectionHtml(g.title, renderRowsAsCardGrid(g.rows));
          })
          .join('') +
        '</div>'
    );
  }

  function renderSupportLegal(brand) {
    var parts = [];
    var op = brand.operationalSupport || {};
    var leg = brand.legalTerms || {};

    var opRows = rowsFromObject(op);
    if (opRows.length) {
      var supRules = [
        {
          title: 'Key Money & Incentives',
          match: function (label) {
            return /incentive|key money|clawback|negotiate/i.test(label);
          }
        },
        {
          title: 'Service Model & Communication',
          match: function (label) {
            return /service|communication|response|differentiator/i.test(label);
          }
        },
        {
          title: 'Governance, Disputes & Owner Programs',
          match: function (label) {
            return /decision|dispute|resolution|concern|advisory|education|reference|involvement/i.test(label);
          }
        }
      ];
      var opGrouped = groupRowsByLabel(opRows, supRules);
      parts.push(
        section(
          'Operational Support',
          '<div class="be-panel">' + renderRowGroupsAsSubsections(opGrouped) + '</div>'
        )
      );
    }

    var legRows = rowsFromObject(leg);
    if (legRows.length) {
      parts.push(
        section(
          'Legal Terms',
          '<div class="be-panel">' + renderRowsAsCardGrid(legRows) + '</div>'
        )
      );
    }

    if (!parts.length) {
      return '<p class="gold-mock-tab-empty">No operational support or legal terms linked.</p>';
    }
    return parts.join('');
  }

  function buildPanels(brand) {
    return {
      requirements: renderRequirements(brand),
      'support-legal': renderSupportLegal(brand)
    };
  }

  function wireFootprintDistribution() {
    var root = document.getElementById('brandRegionalDistribution');
    if (!root) return;
    root.addEventListener('click', function (e) {
      var btn = e.target.closest('.be-dist-toggle');
      if (!btn) return;
      var metric = btn.getAttribute('data-metric');
      root.querySelectorAll('.be-dist-toggle').forEach(function (b) {
        b.classList.toggle('active', b === btn);
      });
      root.querySelectorAll('.be-dist-group').forEach(function (g) {
        g.classList.toggle('active', g.getAttribute('data-metric-group') === metric);
      });
    });
  }

  function wirePortfolioDistribution() {
    var root = document.getElementById('brandPortfolioDistribution');
    if (!root) return;
    root.addEventListener('click', function (e) {
      var btn = e.target.closest('.be-portdist-toggle');
      if (!btn) return;
      var view = btn.getAttribute('data-view');
      root.querySelectorAll('.be-portdist-toggle').forEach(function (b) {
        b.classList.toggle('active', b === btn);
      });
      root.querySelectorAll('.be-portdist-panel').forEach(function (p) {
        p.classList.toggle('active', p.getAttribute('data-panel') === view);
      });
    });
  }

  function wireTabs() {
    var nav = document.getElementById('brandTabs');
    if (!nav) return;
    nav.addEventListener('click', function (e) {
      var btn = e.target.closest('.section-nav-item');
      if (!btn || !btn.getAttribute('data-tab')) return;
      var tab = btn.getAttribute('data-tab');
      document.querySelectorAll('#brandTabs .section-nav-item').forEach(function (b) {
        b.classList.toggle('active', b.getAttribute('data-tab') === tab);
      });
      document.querySelectorAll('.tab-panel').forEach(function (p) {
        p.classList.toggle('active', p.getAttribute('data-panel') === tab);
      });
    });
  }

  function getBrandQuery() {
    var params = new URLSearchParams(window.location.search || '');
    return params.get('id') || params.get('brandId') || params.get('name') || '';
  }

  async function load(overrideId) {
    var id =
      overrideId != null && String(overrideId).trim() !== ''
        ? String(overrideId).trim()
        : getBrandQuery();
    var loading = document.getElementById('brandLoading');
    var errEl = document.getElementById('brandError');
    var root = document.getElementById('brandRoot');

    if (!id) {
      if (loading) loading.style.display = 'none';
      if (errEl) {
        errEl.style.display = 'block';
        var miss = document.getElementById('brandErrorMessage');
        if (miss) miss.textContent = 'Missing brand id or name in URL.';
        else errEl.textContent = 'Missing brand id or name in URL.';
      }
      return;
    }

    try {
      if (errEl) errEl.style.display = 'none';
      if (loading) loading.style.display = 'flex';
      if (root) root.style.display = 'none';

      var url = '/api/brand-library/brand?brandId=' + encodeURIComponent(id);
      var res = await fetch(url);
      if (!res.ok) throw new Error('Failed to load brand (' + res.status + ')');
      var data = await res.json();
      if (!data.success || !data.brand) throw new Error(data.error || 'No brand payload');

      var brand = data.brand;
      document.title = (brand.name || 'Brand') + ' — Brand Explorer';

      var bc = document.getElementById('breadcrumbBrandName');
      if (bc) bc.textContent = brand.name || id;

      var heroMount = document.getElementById('heroMount');
      var unifiedTabs =
        document.documentElement.getAttribute('data-brand-explorer-unified-tabs') === '1';
      if (heroMount) {
        heroMount.innerHTML = unifiedTabs ? renderPresentationHero(brand) : renderHero(brand);
      }
      applyChainScaleTheme(brand);
      if (unifiedTabs) {
        wirePresentationMetaValueTooltips();
        if (window.BrandExplorerFavorites && heroMount) {
          window.BrandExplorerFavorites.wireSaveButtons(heroMount);
        }
      }

      var nav = document.getElementById('brandTabs');
      var main = document.getElementById('brandPanels');

      if (!unifiedTabs) {
        var tabsHtml = TAB_DEFS.map(function (t, i) {
          return (
            '<button type="button" class="section-nav-item' +
            (i === 0 ? ' active' : '') +
            '" data-tab="' +
            t.id +
            '"><div class="section-nav-icon">' +
            TAB_ICONS[t.id] +
            '</div><div class="section-nav-label">' +
            t.label +
            '</div></button>'
          );
        }).join('');
        if (nav) nav.innerHTML = tabsHtml;

        var panels = buildPanels(brand);
        var panelsHtml = TAB_DEFS.map(function (t, i) {
          return (
            '<section class="tab-panel' +
            (i === 0 ? ' active' : '') +
            '" data-panel="' +
            t.id +
            '">' +
            panels[t.id] +
            '</section>'
          );
        }).join('');
        if (main) main.innerHTML = panelsHtml;
      } else {
        if (nav) nav.innerHTML = '';
        if (main) main.innerHTML = '';
      }

      if (loading) loading.style.display = 'none';
      if (root) root.style.display = 'block';

      try {
        window.dispatchEvent(new CustomEvent('brand-explorer-detail-loaded', { detail: { brand: brand } }));
      } catch (_) {}

      if (!unifiedTabs) {
        wireTabs();
      }
      wireFootprintDistribution();
      wirePortfolioDistribution();
    } catch (e) {
      console.error(e);
      if (loading) loading.style.display = 'none';
      if (errEl) {
        errEl.style.display = 'block';
        var em = document.getElementById('brandErrorMessage');
        if (em) em.textContent = e.message || String(e);
        else errEl.textContent = e.message || String(e);
      }
    }
  }

  window.BrandExplorerGoldDetail = {
    load: load,
    getBrandQuery: getBrandQuery,
    buildPanels: buildPanels,
    TAB_DEFS: TAB_DEFS,
    TAB_ICONS: TAB_ICONS,
    renderPresentationHero: renderPresentationHero,
    applyHeroStripe: applyHeroStripe,
    applyChainScaleTheme: applyChainScaleTheme,
    clearChainScaleTheme: clearChainScaleTheme,
    chainStripeColor: chainStripeColor,
    wirePresentationMetaValueTooltips: wirePresentationMetaValueTooltips
  };

  var deferGoldAutoLoad =
    document.documentElement.getAttribute('data-brand-explorer-gold-defer') === '1';

  if (!deferGoldAutoLoad) {
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', load);
    } else {
      load();
    }
  }
})();
