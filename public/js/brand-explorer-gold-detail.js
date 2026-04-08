/**
 * Brand Explorer detail — Operator Gold Mock shell + full Brand Setup API payload.
 * Tabs: Profile & Positioning, Footprint & Distribution, Deal Economics,
 * Requirements & Standards, Owner Fit & Risk, Support, Legal & Commercial.
 */
(function () {
  'use strict';

  var TAB_DEFS = [
    { id: 'profile', label: 'Profile &<br>Positioning' },
    { id: 'footprint', label: 'Footprint &<br>Distribution' },
    { id: 'economics', label: 'Deal<br>Economics' },
    { id: 'requirements', label: 'Requirements &<br>Standards' },
    { id: 'owner-fit', label: 'Owner Fit &<br>Risk' },
    { id: 'support-legal', label: 'Support, Legal &<br>Commercial' }
  ];

  var TAB_ICONS = {
    profile: '<svg viewBox="0 0 24 24"><path d="M3 9.5L12 3l9 6.5"></path><path d="M5 10v10h14V10"></path></svg>',
    footprint: '<svg viewBox="0 0 24 24"><path d="M21 10c0 7-9 11-9 11s-9-4-9-11a9 9 0 0 1 18 0z"></path><circle cx="12" cy="10" r="2.5"></circle></svg>',
    economics: '<svg viewBox="0 0 24 24"><path d="M12 1v22"></path><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"></path></svg>',
    requirements: '<svg viewBox="0 0 24 24"><path d="M9 11l3 3L22 4"></path><path d="M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11"></path></svg>',
    'owner-fit': '<svg viewBox="0 0 24 24"><path d="M12 2l8 4v6c0 5-3.5 9.5-8 10-4.5-.5-8-5-8-10V6l8-4z"></path><path d="M9 12l2 2 4-4"></path></svg>',
    'support-legal': '<svg viewBox="0 0 24 24"><path d="M12 3v18"></path><path d="M5 10h14"></path><path d="M5 14h14"></path><path d="M8 7l4-4 4 4"></path><path d="M8 21l4 4 4-4"></path></svg>'
  };

  var NESTED = {
    footprint: 1,
    loyaltyCommercial: 1,
    feeStructure: 1,
    brandStandards: 1,
    dealTerms: 1,
    portfolioPerformance: 1,
    projectFit: 1,
    operationalSupport: 1,
    legalTerms: 1,
    loadWarnings: 1,
    projectFitDebug: 1
  };

  var PROFILE_KEYS = [
    'name',
    'logo',
    'brandName',
    'parentCompany',
    'hotelChainScale',
    'brandArchitecture',
    'brandModelFormat',
    'hotelServiceModel',
    'yearBrandLaunched',
    'brandDevelopmentStage',
    'brandPositioning',
    'brandTaglineMotto',
    'brandCustomerPromise',
    'brandValueProposition',
    'brandPillars',
    'companyHistory',
    'targetGuestSegments',
    'guestPsychographics',
    'keyBrandDifferentiators',
    'sustainabilityPositioning',
    'brandWebsite',
    'brandStatus',
    'brandProfileAnalysis'
  ];

  var PROFILE_LABELS = {
    name: 'Display name',
    logo: 'Logo URL',
    brandName: 'Brand name',
    parentCompany: 'Parent company',
    hotelChainScale: 'Chain scale',
    brandArchitecture: 'Brand architecture',
    brandModelFormat: 'Brand model',
    hotelServiceModel: 'Service model',
    yearBrandLaunched: 'Year launched',
    brandDevelopmentStage: 'Development stage',
    brandPositioning: 'Positioning',
    brandTaglineMotto: 'Tagline / motto',
    brandCustomerPromise: 'Customer promise',
    brandValueProposition: 'Value proposition',
    brandPillars: 'Brand pillars',
    companyHistory: 'Company history',
    targetGuestSegments: 'Target guest segments',
    guestPsychographics: 'Guest psychographics',
    keyBrandDifferentiators: 'Key differentiators',
    sustainabilityPositioning: 'Sustainability positioning',
    brandWebsite: 'Website',
    brandStatus: 'Brand status',
    brandProfileAnalysis: 'Profile analysis'
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
    return String(k)
      .replace(/([A-Z])/g, ' $1')
      .replace(/_/g, ' ')
      .replace(/\s+/g, ' ')
      .trim()
      .replace(/^./, function (c) {
        return c.toUpperCase();
      });
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

  var LONG_PROSE_KEYS = {
    brandPositioning: 1,
    brandCustomerPromise: 1,
    brandValueProposition: 1,
    companyHistory: 1,
    guestPsychographics: 1,
    brandProfileAnalysis: 1,
    brandStandards: 1,
    brandStandardsNotes: 1,
    brandQaExpectations: 1
  };

  var TAG_KEYS = {
    brandPillars: 1,
    targetGuestSegments: 1,
    keyBrandDifferentiators: 1,
    sustainabilityPositioning: 1
  };

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

  function linkIfUrl(val) {
    var s = String(val).trim();
    if (s.indexOf('http') !== 0) return escapeHtml(s);
    return '<a class="be-link" href="' + escapeHtml(s) + '" target="_blank" rel="noopener noreferrer">' + escapeHtml(s) + '</a>';
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

  function valueHtmlForProfile(key, v) {
    if (key === 'brandWebsite') return linkIfUrl(v);
    if (TAG_KEYS[key] && hasVal(v)) {
      var tags = splitToTags(v);
      if (!tags.length) return escapeHtml(formatValue(v));
      return '<div class="be-tags">' + tags.map(function (t) {
        return '<span class="be-tag">' + escapeHtml(t) + '</span>';
      }).join('') + '</div>';
    }
    if (LONG_PROSE_KEYS[key] || (typeof v === 'string' && v.length > 140)) {
      return '<div class="be-prose">' + renderLongTextAsHtml(v) + '</div>';
    }
    if (isYesNoDisplay(v)) return boolBadgeHtml(v);
    return '<span class="be-field-value--muted">' + escapeHtml(formatValue(v)) + '</span>';
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
      { id: 'pipelineHotels', label: 'Pipeline hotels' },
      { id: 'pipelineRooms', label: 'Pipeline rooms' }
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
      '<div class="be-portdist__hint">View by region, chain scale, or brand.</div>' +
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
    if (brand.hotelChainScale) meta.push(['Chain scale', brand.hotelChainScale]);
    if (brand.hotelServiceModel) meta.push(['Service model', brand.hotelServiceModel]);
    if (brand.brandModelFormat) meta.push(['Brand model', brand.brandModelFormat]);
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

  function applyHeroStripe(brand) {
    var el = document.getElementById('brandHero');
    if (!el) return;
    var hex = chainStripeColor(brand.hotelChainScale || '');
    if (hex) el.style.setProperty('--hero-stripe-bg', hex);
    else el.style.removeProperty('--hero-stripe-bg');
  }

  function renderProfile(brand) {
    var parts = [];
    var hasLogo = brand.logo && String(brand.logo).indexOf('http') === 0;

    var identityKeys = [
      'name',
      'brandName',
      'parentCompany',
      'hotelChainScale',
      'brandArchitecture',
      'brandModelFormat',
      'hotelServiceModel',
      'yearBrandLaunched',
      'brandDevelopmentStage',
      'brandStatus',
      'brandWebsite'
    ];
    var narrativeKeys = [
      'brandPositioning',
      'brandTaglineMotto',
      'brandCustomerPromise',
      'brandValueProposition',
      'brandPillars',
      'companyHistory'
    ];
    var audienceKeys = ['targetGuestSegments', 'guestPsychographics', 'keyBrandDifferentiators', 'sustainabilityPositioning'];
    var notesKeys = ['brandProfileAnalysis'];

    function buildGrid(keys) {
      var cards = [];
      keys.forEach(function (key) {
        if (key === 'logo') return;
        if (key === 'name' && brand.brandName && brand.name === brand.brandName) return;
        var v = brand[key];
        if (!hasVal(v)) return;
        var label = PROFILE_LABELS[key] || humanizeKey(key);
        var wide = !!(LONG_PROSE_KEYS[key] || (typeof v === 'string' && v.length > 140));
        cards.push(fieldCardHtml(label, valueHtmlForProfile(key, v), wide));
      });
      if (!cards.length) return '';
      return '<div class="be-card-grid">' + cards.join('') + '</div>';
    }

    var sub = [];
    var idGrid = buildGrid(identityKeys);
    if (idGrid) sub.push(subsectionHtml('Identity & classification', idGrid));

    var narGrid = buildGrid(narrativeKeys);
    if (narGrid) sub.push(subsectionHtml('Brand story & positioning', narGrid));

    var audGrid = buildGrid(audienceKeys);
    if (audGrid) sub.push(subsectionHtml('Audience & differentiation', audGrid));

    var nGrid = buildGrid(notesKeys);
    if (nGrid) sub.push(subsectionHtml('Analysis & notes', nGrid));

    if (hasLogo && brand.logo) {
      sub.push(
        subsectionHtml(
          'Brand assets',
          fieldCardHtml('Logo', '<img class="hero-logo" src="' + escapeHtml(brand.logo) + '" alt="" referrerpolicy="no-referrer" style="max-height:48px;width:auto" />', false)
        )
      );
    }

    if (sub.length) {
      parts.push(section('Profile & positioning', '<div class="be-panel">' + sub.join('') + '</div>'));
    }

    var warn = brand.loadWarnings;
    if (warn && warn.length) {
      parts.push(
        section(
          'Data load notes',
          '<p class="gold-mock-tab-empty">Some linked tables could not be loaded: ' +
            escapeHtml(warn.join(', ')) +
            '.</p>'
        )
      );
    }

    return parts.join('') || '<p class="gold-mock-tab-empty">No profile fields returned.</p>';
  }

  function renderFootprint(brand) {
    var fp = brand.footprint || {};
    var parts = [];

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
    parts.push(section('Markets & footprint', summary));

    var existingHotels = toNumber(fp.totalExistingHotels);
    var existingRooms = toNumber(fp.totalExistingRooms);
    var pipelineHotels = toNumber(fp.totalNewBuildHotels) + toNumber(fp.totalConversionHotels);
    var pipelineRooms = toNumber(fp.totalNewBuildRooms) + toNumber(fp.totalConversionRooms);

    var totalTable = footprintTableFromRows([
      ['', 'Existing Hotels', 'Existing Rooms', 'Pipeline Hotels', 'Pipeline Rooms'],
      ['Total', existingHotels, existingRooms, pipelineHotels, pipelineRooms]
    ]);

    var metricsInner =
      subsectionHtml('Existing vs. pipeline (portfolio)', totalTable) +
      subsectionHtml('Portfolio distribution', portfolioDistributionHtml(brand, fp));
    parts.push(section('Footprint Metrics', '<div class="be-panel">' + metricsInner + '</div>'));

    var loc = fp.locationDistribution;
    if (loc && typeof loc === 'object' && Object.keys(loc).length) {
      parts.push(
        section(
          'Location type mix',
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
            title: 'Regional detail',
            match: function (label) {
              return /^(North America|Americas|EMEA|Europe|APAC|CALA|MEA|Latin America|Global)\s—/i.test(label);
            }
          }
        ]);
        if (fpGroups.length) {
          parts.push(
            section(
              'Footprint detail (by region & metric)',
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

  function renderEconomics(brand) {
    var parts = [];
    var fee = brand.feeStructure || {};
    var deal = brand.dealTerms || {};
    var port = brand.portfolioPerformance || {};

    function econSection(secTitle, obj) {
      var rows = rowsFromObject(obj);
      if (!rows.length) return '';
      return (
        subsectionHtml(
          secTitle,
          '<div class="be-card-grid be-card-grid--2">' +
            rows
              .map(function (row) {
                var label = row[0];
                var val = row[1];
                var wide = typeof val === 'string' && val.length > 180;
                var inner;
                if (isYesNoDisplay(val)) inner = boolBadgeHtml(val);
                else if (wide) inner = '<div class="be-prose">' + renderLongTextAsHtml(val) + '</div>';
                else inner = '<span class="be-field-value--muted">' + escapeHtml(formatValue(val)) + '</span>';
                return fieldCardHtml(label, inner, wide);
              })
              .join('') +
            '</div>'
        )
      );
    }

    var inner = [];
    var a = econSection('Fee structure', fee);
    if (a) inner.push(a);
    var b = econSection('Deal terms', deal);
    if (b) inner.push(b);
    var c = econSection('Portfolio & performance', port);
    if (c) inner.push(c);

    if (!inner.length) {
      return '<p class="gold-mock-tab-empty">No fee, deal, or portfolio records linked.</p>';
    }
    parts.push(section('Deal economics', '<div class="be-panel">' + inner.join('') + '</div>'));
    return parts.join('');
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
        title: 'Core spaces & amenities',
        rows: []
      },
      {
        title: 'Food & beverage',
        rows: []
      },
      {
        title: 'Meetings & events',
        rows: []
      },
      {
        title: 'Parking & program rules',
        rows: []
      },
      {
        title: 'Sustainability & amenities',
        rows: []
      },
      {
        title: 'Compliance & QA',
        rows: []
      }
    ];

    pushRow(groups[0].rows, 'Lobby', std.lobby);
    pushRow(groups[0].rows, 'Lobby description', std.lobbyDescription);
    pushRow(groups[0].rows, 'Bar / beverage', std.barBeverage);
    pushRow(groups[0].rows, 'Fitness', std.fitnessCenter);
    pushRow(groups[0].rows, 'Pool', std.pool);
    pushRow(groups[0].rows, 'Onsite parking', std.onsiteParking);
    pushRow(groups[0].rows, 'Meeting / event space', std.meetingEventSpace);
    pushRow(groups[0].rows, 'Co-working', std.coworking);
    pushRow(groups[0].rows, 'Grab & go', std.grabGo);
    pushRow(groups[0].rows, 'Minimum room size (sq ft)', std.minimumRoomSize);
    pushRow(groups[0].rows, 'Minimum room size (sq m)', std.minimumRoomSizeMeters);
    pushRow(groups[0].rows, 'Brand standards narrative', std.brandStandards);

    pushRow(groups[1].rows, 'F&B outlets required', std.brandFbOutletsRequired);
    pushRow(groups[1].rows, 'Typical F&B outlet count', std.brandFbOutletsCount);
    pushRow(groups[1].rows, 'F&B program type', std.brandFbProgramType);
    pushRow(groups[1].rows, 'Outlet concepts', std.brandFbOutletConcepts);
    pushRow(groups[1].rows, 'F&B outlet size', std.brandFbOutletSize);

    pushRow(groups[2].rows, 'Meeting space required', std.brandMeetingSpaceRequired);
    pushRow(groups[2].rows, 'Meeting rooms count', std.brandMeetingRoomsCount);
    pushRow(groups[2].rows, 'Meeting space size', std.brandMeetingSpaceSize);
    pushRow(groups[2].rows, 'Condo / residences', std.brandCondoResidencesAllowed);
    pushRow(groups[2].rows, 'Hotel rental program', std.brandHotelRentalProgram);

    pushRow(groups[3].rows, 'Parking required', std.brandParkingRequired);
    pushRow(groups[3].rows, 'Parking spaces', std.brandParkingSpacesCount);
    pushRow(groups[3].rows, 'Parking program', std.brandParkingProgramType);

    pushRow(groups[4].rows, 'Sustainability features', std.brandSustainability);
    pushRow(groups[4].rows, 'Other sustainability', std.brandSustainabilityOther);
    pushRow(groups[4].rows, 'Additional amenities', std.brandRequiredAmenities);
    pushRow(groups[4].rows, 'Other amenities', std.brandRequiredAmenitiesOther);

    pushRow(groups[5].rows, 'Compliance & safety', std.brandCompliance);
    pushRow(groups[5].rows, 'Other compliance', std.brandComplianceOther);
    pushRow(groups[5].rows, 'QA / brand standards expectations', std.brandQaExpectations);
    pushRow(groups[5].rows, 'Additional notes', std.brandStandardsNotes);

    var nonempty = groups.filter(function (g) {
      return g.rows.length > 0;
    });
    if (!nonempty.length) {
      return '<p class="gold-mock-tab-empty">No brand standards record linked.</p>';
    }
    return section(
      'Requirements & standards',
      '<div class="be-panel">' +
        nonempty
          .map(function (g) {
            return subsectionHtml(g.title, renderRowsAsCardGrid(g.rows));
          })
          .join('') +
        '</div>'
    );
  }

  function renderOwnerFit(brand) {
    var parts = [];
    var pf = brand.projectFit || {};
    var fv = pf.formValues || {};

    var pfRows = rowsFromObject(fv);
    if (pfRows.length) {
      var fitRules = [
        {
          title: 'Deal shape & agreements',
          match: function (label) {
            return /ideal|project type|building|agreement|stage/i.test(label);
          }
        },
        {
          title: 'Markets & geography',
          match: function (label) {
            return /market|avoid|priority/i.test(label);
          }
        },
        {
          title: 'Owner profile & economics',
          match: function (label) {
            return /owner|capital|brand status|fee|exit|negotiable/i.test(label);
          }
        },
        {
          title: 'Thresholds & criteria',
          match: function (label) {
            return /room|size|experience|lead time|preferred/i.test(label);
          }
        }
      ];
      var fitGrouped = groupRowsByLabel(pfRows, fitRules);
      parts.push(
        section(
          'Project fit',
          '<div class="be-panel">' + renderRowGroupsAsSubsections(fitGrouped) + '</div>'
        )
      );
    }

    var extraPf = rowsFromObject(pf, { skip: { formValues: 1 } });
    if (extraPf.length) {
      parts.push(
        section(
          'Project fit (source fields)',
          '<div class="be-panel">' + renderRowsAsCardGrid(extraPf) + '</div>'
        )
      );
    }

    var esgRows = [];
    [
      ['Sustainability programs', brand.sustainabilityPrograms],
      ['ESG reporting', brand.esgReporting],
      ['Carbon tracking', brand.carbonTracking],
      ['Energy efficiency', brand.energyEfficiency],
      ['Waste reduction', brand.wasteReduction]
    ].forEach(function (pair) {
      if (hasVal(pair[1])) esgRows.push(pair);
    });
    if (esgRows.length) {
      parts.push(
        section(
          'Sustainability & ESG',
          '<div class="be-panel">' + renderRowsAsCardGrid(esgRows) + '</div>'
        )
      );
    }

    var lcWrap = brand.loyaltyCommercial || {};
    var lc = lcWrap.formValues;
    if (lc && typeof lc === 'object') {
      var lcRows = rowsFromObject(lc);
      if (lcRows.length) {
        parts.push(
          section(
            'Loyalty & commercial',
            '<div class="be-panel">' + renderRowsAsCardGrid(lcRows) + '</div>'
          )
        );
      }
    }
    var lcExtra = rowsFromObject(lcWrap, { skip: { formValues: 1, unlinkedFields: 1 } });
    if (lcExtra.length) {
      parts.push(
        section(
          'Loyalty & commercial (additional)',
          '<div class="be-panel">' + renderRowsAsCardGrid(lcExtra) + '</div>'
        )
      );
    }

    if (!parts.length) {
      return '<p class="gold-mock-tab-empty">No project fit, ESG, or loyalty/commercial data linked.</p>';
    }
    return parts.join('');
  }

  function renderSupportLegal(brand) {
    var parts = [];
    var op = brand.operationalSupport || {};
    var leg = brand.legalTerms || {};

    var opRows = rowsFromObject(op);
    if (opRows.length) {
      var supRules = [
        {
          title: 'Key money & incentives',
          match: function (label) {
            return /incentive|key money|clawback|negotiate/i.test(label);
          }
        },
        {
          title: 'Service model & communication',
          match: function (label) {
            return /service|communication|response|differentiator/i.test(label);
          }
        },
        {
          title: 'Governance, disputes & owner programs',
          match: function (label) {
            return /decision|dispute|resolution|concern|advisory|education|reference|involvement/i.test(label);
          }
        }
      ];
      var opGrouped = groupRowsByLabel(opRows, supRules);
      parts.push(
        section(
          'Operational support',
          '<div class="be-panel">' + renderRowGroupsAsSubsections(opGrouped) + '</div>'
        )
      );
    }

    var legRows = rowsFromObject(leg);
    if (legRows.length) {
      parts.push(
        section(
          'Legal terms',
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
      profile: renderProfile(brand),
      footprint: renderFootprint(brand),
      economics: renderEconomics(brand),
      requirements: renderRequirements(brand),
      'owner-fit': renderOwnerFit(brand),
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

  async function load() {
    var id = getBrandQuery();
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
      if (heroMount) heroMount.innerHTML = renderHero(brand);
      applyHeroStripe(brand);

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
      var nav = document.getElementById('brandTabs');
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
      var main = document.getElementById('brandPanels');
      if (main) main.innerHTML = panelsHtml;

      if (loading) loading.style.display = 'none';
      if (root) root.style.display = 'block';

      wireTabs();
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

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', load);
  } else {
    load();
  }
})();
