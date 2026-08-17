/**
 * Atelier-style explorer tabs on Brand Explorer (combined), fed by GET /api/brand-library/brand.
 * Tabs 1–8 mirror brand-education-voco Operator Explorer IA under .be-atelier-oe: fixed shells,
 * oe-section / scenario grids, and Brand Setup fields where present (blank nodes when absent).
 * When Brand Setup gold detail is present, appends structured gold tabs after Dealality Insight.
 */
(function () {
  'use strict';

  /** Single source for recent-opening property cards (Footprint & Growth). Not materials.caseStudy. */
  var FOOTPRINT_OPENINGS_SLOT = 'footprint.openings';
  /** Recent Momentum timeline rows (date, headline, description, optional Choice Hotels URL). */
  var FOOTPRINT_MOMENTUM_SLOT = 'footprint.momentum';
  var FOOTPRINT_MOMENTUM_LABEL_SLOT = 'footprint.momentum_label';
  /** Portfolio Mix pills under Recent Momentum (Title = category, Body = level label). */
  var FOOTPRINT_PORTFOLIO_MIX_SLOT = 'footprint.portfolio_mix';
  var OPERATIONS_COMPLIANCE_SLOTS = [
    'operations.compliance.qa_cadence',
    'operations.compliance.training_rigor',
    'operations.compliance.reporting',
    'operations.compliance.brand_interaction'
  ];

  /** Brand Explorer Presentation rows for Geographic Footprint region cards (Title optional; Body = status line, blank line, narrative). */
  var FOOTPRINT_REGION_SLOT_DEFS = [
    { slot: 'footprint.region.am', defaultName: 'Americas' },
    { slot: 'footprint.region.cala', defaultName: 'CALA' },
    { slot: 'footprint.region.eu', defaultName: 'Europe' },
    { slot: 'footprint.region.mea', defaultName: 'MEA' },
    { slot: 'footprint.region.apac', defaultName: 'APAC' }
  ];

  var ATELIER_TAB_DEFS = [
    { id: 'atelier-overview', label: 'Overview' },
    { id: 'atelier-value-owners', label: 'Value to<br>Owners' },
    { id: 'atelier-ops', label: 'Operating<br>Model' },
    { id: 'atelier-standards-owner', label: 'Owner<br>Considerations' },
    { id: 'atelier-commercial', label: 'Commercial<br>Engine' },
    { id: 'atelier-economics', label: 'Economics &<br>Obligations' },
    { id: 'atelier-loyalty', label: 'Loyalty<br>Program' },
    { id: 'atelier-footprint', label: 'Footprint &<br>Growth' },
    { id: 'atelier-materials', label: 'Brand<br>Materials' },
    { id: 'atelier-insight', label: 'Dealality<br>Insight' }
  ];

  function getGoldAppendTabs() {
    if (document.documentElement.getAttribute('data-brand-explorer-unified-tabs') === '1') {
      return [];
    }
    var G = window.BrandExplorerGoldDetail;
    if (!G || !G.TAB_DEFS || typeof G.buildPanels !== 'function') return [];
    return G.TAB_DEFS.map(function (t) {
      return { id: 'gold-' + t.id, goldKey: t.id, label: t.label };
    });
  }

  function combinedTabRowDefs() {
    return ATELIER_TAB_DEFS.concat(getGoldAppendTabs());
  }

  var ICONS = {
    overview:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="3" width="7" height="7" rx="1.5" fill="none" stroke="currentColor" stroke-width="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5" fill="none" stroke="currentColor" stroke-width="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5" fill="none" stroke="currentColor" stroke-width="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5" fill="none" stroke="currentColor" stroke-width="1.5"/></svg>',
    chart:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18" fill="none" stroke="currentColor" stroke-width="1.5"/><polyline points="17 6 23 6 23 12" fill="none" stroke="currentColor" stroke-width="1.5"/></svg>',
    ops: '<svg viewBox="0 0 24 24" aria-hidden="true"><line x1="4" y1="21" x2="4" y2="14" stroke="currentColor" stroke-width="1.5"/><line x1="4" y1="10" x2="4" y2="3" stroke="currentColor" stroke-width="1.5"/><line x1="12" y1="21" x2="12" y2="12" stroke="currentColor" stroke-width="1.5"/><line x1="12" y1="8" x2="12" y2="3" stroke="currentColor" stroke-width="1.5"/><line x1="20" y1="21" x2="20" y2="16" stroke="currentColor" stroke-width="1.5"/><line x1="20" y1="12" x2="20" y2="3" stroke="currentColor" stroke-width="1.5"/></svg>',
    bars:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><line x1="18" y1="20" x2="18" y2="10" stroke="currentColor" stroke-width="1.5"/><line x1="12" y1="20" x2="12" y2="4" stroke="currentColor" stroke-width="1.5"/><line x1="6" y1="20" x2="6" y2="14" stroke="currentColor" stroke-width="1.5"/></svg>',
    star:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z" fill="none" stroke="currentColor" stroke-width="1.5"/></svg>',
    globe:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="10" fill="none" stroke="currentColor" stroke-width="1.5"/><path d="M2 12h20M12 2a15 15 0 0 1 4 10 15 15 0 0 1-4 10 15 15 0 0 1-4-10 15 15 0 0 1 4-10z" fill="none" stroke="currentColor" stroke-width="1.5"/></svg>',
    folder:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z" fill="none" stroke="currentColor" stroke-width="1.5"/></svg>',
    spark:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="m12 3-1.9 5.8a2 2 0 0 1-1.3 1.3L3 12l5.8 1.9a2 2 0 0 1 1.3 1.3L12 21l1.9-5.8a2 2 0 0 1 1.3-1.3L21 12l-5.8-1.9a2 2 0 0 1-1.3-1.3L12 3z" fill="none" stroke="currentColor" stroke-width="1.5"/></svg>',
    wallet:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M21 12V7H5a2 2 0 0 1 0-4h14v2" fill="none" stroke="currentColor" stroke-width="1.5"/><path d="M3 7v10a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-3H7a2 2 0 0 1-2-2 2 2 0 0 1 2-2h16" fill="none" stroke="currentColor" stroke-width="1.5"/></svg>',
    checklist:
      '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M9 11l3 3L22 4" fill="none" stroke="currentColor" stroke-width="1.5"/><path d="M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11" fill="none" stroke="currentColor" stroke-width="1.5"/></svg>'
  };

  function isExportPdfMode() {
    return !!(document.documentElement && document.documentElement.classList.contains('be-export-pdf'));
  }

  /** Lazy images below the fold never load before print unless eager in PDF export mode. */
  function exportPdfImgAttrs() {
    return isExportPdfMode()
      ? ' loading="eager" decoding="sync" referrerpolicy="no-referrer"'
      : ' loading="lazy" referrerpolicy="no-referrer"';
  }

  /**
   * PDF export shows every tab at once; wait for remote images (Airtable CDN) before print-ready.
   */
  function waitForExportImages(scopeEl, maxMs) {
    if (!isExportPdfMode()) return Promise.resolve();
    var root = scopeEl || document.getElementById('brandRoot') || document.body;
    if (!root || !root.querySelectorAll) return Promise.resolve();
    var imgs = root.querySelectorAll('img[src]');
    var pending = [];
    for (var i = 0; i < imgs.length; i++) {
      var img = imgs[i];
      try {
        img.loading = 'eager';
        img.removeAttribute('loading');
      } catch (_) {}
      if (img.complete && img.naturalWidth > 0) continue;
      pending.push(
        new Promise(function (resolve) {
          function finish() {
            resolve();
          }
          img.addEventListener('load', finish, { once: true });
          img.addEventListener('error', finish, { once: true });
          try {
            img.scrollIntoView({ block: 'center', inline: 'nearest' });
          } catch (_) {}
        })
      );
    }
    var allDone = pending.length ? Promise.all(pending) : Promise.resolve();
    var cap = typeof maxMs === 'number' && maxMs > 0 ? maxMs : 18000;
    var timeout = new Promise(function (r) {
      window.setTimeout(r, cap);
    });
    return Promise.race([allDone, timeout]);
  }

  /** Stable icon per atelier tab id (do not rely on array index when tabs are added). */
  var ATELIER_TAB_ICON_BY_ID = {
    'atelier-overview': ICONS.overview,
    'atelier-value-owners': ICONS.chart,
    'atelier-ops': ICONS.ops,
    'atelier-standards-owner': ICONS.checklist,
    'atelier-commercial': ICONS.bars,
    'atelier-economics': ICONS.wallet,
    'atelier-loyalty': ICONS.star,
    'atelier-footprint': ICONS.globe,
    'atelier-materials': ICONS.folder,
    'atelier-insight': ICONS.spark
  };

  /** Loyalty & Commercial form keys — labels align with Brand Setup / Airtable (see api/brand-library.js LOYALTY_COMMERCIAL_FORM_TO_AIRTABLE). */
  var LOYALTY_FORM_ROWS = [
    { form: 'typicalLoyaltyProgramName', label: 'Typical Loyalty Program Name' },
    { form: 'typicalLoyaltyRoomsPercent', label: 'Typical % of Rooms From Loyalty (Est.)' },
    { form: 'typicalDirectBookingPercent', label: 'Typical Direct Booking % (Est.)' },
    { form: 'typicalOTAReliancePercent', label: 'Typical OTA Reliance % (Est.)' },
    { form: 'totalGlobalMembersMillions', label: 'Total Global Members (Approx. Millions)' },
    { form: 'regionalMembersMillions_na', label: 'Regional Members — NA (Millions)' },
    { form: 'regionalMembersMillions_cala', label: 'Regional Members — CALA (Millions)' },
    { form: 'regionalMembersMillions_eu', label: 'Regional Members — EU (Millions)' },
    { form: 'regionalMembersMillions_mea', label: 'Regional Members — MEA (Millions)' },
    { form: 'regionalMembersMillions_apac', label: 'Regional Members — APAC (Millions)' },
    { form: 'loyaltyCostPerStay', label: 'Loyalty Program Cost Per Stay (Approx.)' },
    { form: 'otaCommissionPercent', label: 'OTA Commission (Typical % of Reservation)' },
    { form: 'crsUsagePercent', label: 'CRS Usage (% of Bookings)' },
    { form: 'distributionCostPerReservation', label: 'Distribution Cost (Per Reservation)' },
    { form: 'websiteAppConvRatesPercent', label: 'Website / App Conversion Rates (%)' },
    { form: 'avgCustomerAcquisitionCost', label: 'Avg. Cost of Customer Acquisition' }
  ];

  function escapeHtml(text) {
    if (text == null || text === '') return '';
    // Owner-safe scrub before HTML escape (covers Brand Basics fallbacks + Presentation).
    var cleaned = String(text)
      .replace(/\bfranchise disclosure document\b/gi, 'commercial agreement materials')
      .replace(/\bfranchise disclosure\b/gi, 'commercial agreement review')
      .replace(/\bdisclosure document\b/gi, 'commercial agreement materials')
      .replace(/\bfee stack\b/gi, 'participation costs and program fees')
      .replace(/\bnet contribution\b/gi, 'contribution after program costs')
      .replace(/\bItem\s*19\b/gi, 'public performance materials')
      .replace(/\bItem\s*7\b/gi, 'initial investment schedules')
      .replace(/\bFDD\b/g, 'commercial agreement materials')
      .replace(/\bLOI\b/g, 'commercial proposal')
      .replace(/\bADR\b/g, 'average daily rate')
      .replace(/\bRevPAR\b/g, 'revenue per available room')
      .replace(/\bparticipation cost categories\b/gi, 'participation costs and program fees')
      .replace(/\bowner economics after brand-related costs\b/gi, 'whether brand economics fit the asset after program costs')
      .replace(/\bOutput Note\.?/gi, '')
      .replace(/\binternal review\b/gi, 'owner diligence review');
    var div = document.createElement('div');
    div.textContent = cleaned;
    return div.innerHTML;
  }

  function hasVal(v) {
    if (v == null || v === '') return false;
    if (Array.isArray(v)) return v.length > 0;
    return true;
  }

  function isSafeHttpUrl(url) {
    var s = url == null ? '' : String(url).trim();
    if (!s) return false;
    if (typeof URL === 'function') {
      try {
        var u = new URL(s);
        return u.protocol === 'http:' || u.protocol === 'https:';
      } catch (_) {
        return false;
      }
    }
    // Fallback when URL ctor is unavailable (restricted embeds / test sandboxes).
    return /^https?:\/\/[^\s<>"']+$/i.test(s);
  }

  /** Pull a trailing http(s) URL out of free text so it never remains as raw body copy. */
  function splitTrailingHttpUrl(text) {
    var t = String(text == null ? '' : text).trim();
    if (!t) return { text: '', url: '' };
    var m = t.match(/^(?:([\s\S]*?)(?:\n+|\s+))?(https?:\/\/[^\s<>"']+)\s*$/i);
    if (!m || !m[2]) return { text: t, url: '' };
    return { text: String(m[1] || '').trim(), url: String(m[2]).trim() };
  }

  /** Proof Points section label — only "Brand-Verified" when Company Validated is true on brand payload. */
  function proofPointsSectionHint(brand) {
    var cv =
      brand &&
      (brand.companyValidated === true ||
        brand.companyValidated === 'Yes' ||
        brand.company_validated === true ||
        brand.company_validated === 'Yes');
    return cv
      ? 'Brand-Verified Content · Curated by Dealality'
      : 'AI-Assisted from Official Public Sources · Curated by Dealality';
  }

  function proofThemeMaterialsLabel(brand) {
    var cv =
      brand &&
      (brand.companyValidated === true ||
        brand.companyValidated === 'Yes' ||
        brand.company_validated === true ||
        brand.company_validated === 'Yes');
    return cv ? 'Brand-Verified Materials' : 'Official Public Source Materials';
  }

  function isAffiliationCurationBrand(brand) {
    var id = brand && (brand.id || brand.recordId || '');
    if (String(id) === 'rec02zPClpWUTCyXM') return true;
    var name = brand && (brand.name || brand.brandName || '');
    return /design hotels|small luxury hotels of the world|\bslh\b/i.test(String(name));
  }

  var LIFESTYLE_GALLERY_SLOT_LABELS = [
    'Exterior / Arrival',
    'Guest Room / Suite',
    'Public Space',
    'F&B or Local Experience',
    'Design Detail',
    'Property Setting'
  ];

  function normalizeImageUrlKey(url) {
    return String(url || '')
      .trim()
      .toLowerCase()
      .split('?')[0];
  }

  function isGenericLifestyleGalleryTitle(title) {
    if (!hasVal(title)) return true;
    return LIFESTYLE_GALLERY_SLOT_LABELS.indexOf(String(title).trim()) >= 0;
  }

  function propertyNameFromOpeningTitle(title) {
    if (!hasVal(title)) return '';
    return String(title)
      .trim()
      .split(/\s*[—–-]\s*/)[0]
      .trim();
  }

  function propertyNameFromHotelPageUrl(url) {
    if (!hasVal(url)) return '';
    var u = String(url).trim();
    var dh = u.match(/designhotels\.com\/hotels\/[^/]+\/[^/]+\/([^/?#]+)/i);
    if (dh) {
      return dh[1]
        .split('-')
        .map(function (w) {
          if (!w) return '';
          if (w.length <= 2) return w.toUpperCase();
          return w.charAt(0).toUpperCase() + w.slice(1);
        })
        .filter(Boolean)
        .join(' ');
    }
    var slh = u.match(/slh\.com\/hotels\/([^/?#]+)/i);
    if (slh) {
      return slh[1]
        .split('-')
        .map(function (w) {
          if (!w) return '';
          return w.charAt(0).toUpperCase() + w.slice(1);
        })
        .filter(Boolean)
        .join(' ');
    }
    return '';
  }

  function propertyNameFromDesignHotelsImageUrl(url) {
    var u = normalizeImageUrlKey(url);
    var slugHints = [
      ['wake-biohotel', 'Wake BioHotel'],
      ['condesadf', 'Condesa DF'],
      ['carlota-quito', 'Carlota'],
      ['wake-medellin', 'Wake Medellín'],
      ['downtownmexico', 'Downtown Mexico'],
      ['habita-mexicocity', 'Habita']
    ];
    for (var i = 0; i < slugHints.length; i++) {
      if (u.indexOf(slugHints[i][0]) >= 0) return slugHints[i][1];
    }
    return '';
  }

  function buildGalleryImagePropertyMap(brand) {
    var map = {};
    var be = brand && brand.brandExplorer;
    if (!be || !Array.isArray(be.blocks)) return map;
    be.blocks.forEach(function (b) {
      if (!b || !hasVal(b.imageUrl)) return;
      var key = normalizeImageUrlKey(b.imageUrl);
      var name = '';
      if (String(b.slotKey || '') === FOOTPRINT_OPENINGS_SLOT && hasVal(b.title)) {
        name = propertyNameFromOpeningTitle(b.title);
      } else if (hasVal(b.body)) {
        name = propertyNameFromHotelPageUrl(firstHttpUrlInString(b.body));
      }
      if (!hasVal(name) && hasVal(b.title) && !isGenericLifestyleGalleryTitle(b.title)) {
        name = propertyNameFromOpeningTitle(b.title);
      }
      if (!hasVal(name)) return;
      if (String(b.slotKey || '') === FOOTPRINT_OPENINGS_SLOT || !map[key]) {
        map[key] = name;
      }
    });
    return map;
  }

  /** Affiliation gallery collage: "{Space label} - {Hotel Name}" when property is known. */
  function galleryCaptionForRow(brand, row, fallback) {
    var spaceLabel = row && hasVal(row.title) ? String(row.title).trim() : fallback;
    if (!isAffiliationCurationBrand(brand)) {
      return spaceLabel;
    }
    if (!isGenericLifestyleGalleryTitle(spaceLabel)) {
      return spaceLabel;
    }
    var body = row && hasVal(row.body) ? String(row.body).trim() : '';
    if (hasVal(body)) {
      if (body.indexOf(' - ') >= 0 || body.indexOf(' — ') >= 0) return body;
      if (!isGenericLifestyleGalleryTitle(body) && body.length <= 80) {
        return spaceLabel + ' - ' + body;
      }
    }
    var imgUrl = row && hasVal(row.imageUrl) ? String(row.imageUrl).trim() : '';
    if (hasVal(imgUrl)) {
      var propMap = buildGalleryImagePropertyMap(brand);
      var propName = propMap[normalizeImageUrlKey(imgUrl)];
      if (!hasVal(propName) && /design hotels/i.test(String(brand.name || brand.brandName || ''))) {
        propName = propertyNameFromDesignHotelsImageUrl(imgUrl);
      }
      if (hasVal(propName)) return spaceLabel + ' - ' + propName;
    }
    return spaceLabel;
  }

  function modalDetailBlock(title, content) {
    if (!hasVal(content) || String(content).trim() === '—') return '';
    return (
      '<div class="be-case-detail-block"><h4>' +
      escapeHtml(title) +
      '</h4><p>' +
      escapeHtml(String(content).trim()) +
      '</p></div>'
    );
  }

  function firstHttpUrlInString(txt) {
    if (!hasVal(txt)) return '';
    var lines = String(txt).split(/\r?\n/);
    for (var i = 0; i < lines.length; i++) {
      var line = lines[i].trim();
      if (isSafeHttpUrl(line)) return line;
    }
    return '';
  }

  function lastHttpUrlInString(txt) {
    if (!hasVal(txt)) return '';
    var lines = String(txt).split(/\r?\n/);
    var found = '';
    for (var i = 0; i < lines.length; i++) {
      var line = lines[i].trim();
      if (isSafeHttpUrl(line)) found = line;
    }
    return found;
  }

  /** Badge text: URL path (Airtable attachment URLs often omit “.pdf”) — also checks Title for “.pdf” / “.zip” etc. */
  function fileKindLabelFromUrl(url, titleOpt) {
    var path = String(url || '').split('?')[0].toLowerCase();
    var title = String(titleOpt || '').toLowerCase();
    var hay = path + ' ' + title;
    if (hay.indexOf('.zip') !== -1) return 'ZIP';
    if (hay.indexOf('.pdf') !== -1) return 'PDF';
    if (hay.indexOf('.docx') !== -1) return 'DOC';
    if (hay.indexOf('.doc') !== -1) return 'DOC';
    if (hay.indexOf('.mp4') !== -1 || hay.indexOf('.mov') !== -1) return 'MP4';
    return 'LINK';
  }

  /** Brand Explorer presentation: rows from GET brand.brandExplorer.blocks (see docs/brand-explorer-presentation-slots.md). */
  function explorerBlocksForSlot(brand, slotKey, opts) {
    opts = opts || {};
    var be = brand.brandExplorer;
    if (!be || !Array.isArray(be.blocks)) return [];
    function imgRank(b) {
      if (!b || !b.imageUrl) return 0;
      var u = String(b.imageUrl).trim();
      return u.indexOf('http') === 0 ? 1 : 0;
    }
    var rows = be.blocks.filter(function (b) {
      if (!b) return false;
      var sk = String(b.slotKey || '');
      if (opts.prefix) {
        return sk === String(slotKey) || sk.indexOf(String(slotKey) + '.') === 0;
      }
      return sk === String(slotKey);
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

  /**
   * Body-only join for KV / labeled UI slots.
   * Prefer Body; fall back to Title only when Body is empty.
   * Does not prepend "Title: " — the UI already supplies the label.
   */
  function explorerMergedBodiesOnly(brand, slotKey, joinStr) {
    joinStr = joinStr == null ? '\n\n' : joinStr;
    return explorerBlocksForSlot(brand, slotKey)
      .map(function (r) {
        var bd = hasVal(r.body) ? String(r.body).trim() : '';
        if (bd) return bd;
        return hasVal(r.title) ? String(r.title).trim() : '';
      })
      .filter(hasVal)
      .join(joinStr);
  }

  /** Strip a leading "Label: " when Body already duplicates the UI KV label. */
  function stripKvLabelPrefix(text, labels) {
    var s = String(text || '').trim();
    if (!s || !labels || !labels.length) return s;
    for (var i = 0; i < labels.length; i++) {
      var label = String(labels[i] || '').trim();
      if (!label) continue;
      var escaped = label.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      var re = new RegExp('^' + escaped + '\\s*:\\s*', 'i');
      if (re.test(s)) {
        return s.replace(re, '').trim();
      }
    }
    return s;
  }

  function explorerFirstBlock(brand, slotKey) {
    var rows = explorerBlocksForSlot(brand, slotKey);
    return rows.length ? rows[0] : null;
  }

  function isV37LifestyleBatchBrand(brand) {
    var n = String((brand && brand.name) || (brand && brand.brandName) || '').trim().toLowerCase();
    return n === 'hotel indigo' || n === 'mgallery collection';
  }

  function isInternalPreviewRequest() {
    try {
      return /(?:\?|&)beInternalPreview=1(?:&|$)/.test(String(window.location.search || ''));
    } catch (e) {
      return false;
    }
  }

  /** Built-in factory candidate allowlist (mirrors server module; API may also set factoryPreview.eligible). */
  var FACTORY_PREVIEW_CANDIDATE_SLUGS = {
    'tapestry-collection-by-hilton': true,
    'dazzler-by-wyndham': true,
    'trademark-collection-by-wyndham': true
  };

  function isFactoryPreviewRequest() {
    try {
      var q = String(window.location.search || '');
      return (
        /(?:\?|&)beInternalPreview=1(?:&|$)/.test(q) &&
        /(?:\?|&)factoryPreview=1(?:&|$)/.test(q)
      );
    } catch (e) {
      return false;
    }
  }

  function resolveBrandSlugForPreview(brand) {
    if (!brand) return '';
    var slug = String(brand.slug || '').trim().toLowerCase();
    if (slug) return slug;
    if (brand.factoryPreview && brand.factoryPreview.slug) {
      return String(brand.factoryPreview.slug).trim().toLowerCase();
    }
    var id = String(brand.id || '').trim();
    if (id === 'reccXxMHEh7NNRhIE') return 'tapestry-collection-by-hilton';
    if (id === 'rec5CNMM4ZUD7ZHlM') return 'dazzler-by-wyndham';
    if (id === 'recob7tgHRryRSbeO') return 'trademark-collection-by-wyndham';
    try {
      var params = new URLSearchParams(String(window.location.search || ''));
      return String(params.get('brand') || params.get('slug') || '')
        .trim()
        .toLowerCase();
    } catch (e) {
      return '';
    }
  }

  function isFactoryPreviewCandidateSlug(slug) {
    var s = String(slug || '')
      .trim()
      .toLowerCase();
    return !!(s && FACTORY_PREVIEW_CANDIDATE_SLUGS[s]);
  }

  /**
   * Factory Preview Mode — allowlisted candidates only, with explicit query flags.
   * Does not change production public-full / shouldRenderFullProfile API truth.
   */
  function canRenderFactoryPreview(brand) {
    if (!isFactoryPreviewRequest()) return false;
    if (!hasPresentationRows(brand)) return false;
    if (brand && brand.factoryPreview && brand.factoryPreview.eligible === false) return false;
    if (brand && brand.factoryPreview && brand.factoryPreview.eligible === true) return true;
    return isFactoryPreviewCandidateSlug(resolveBrandSlugForPreview(brand));
  }

  function factoryPreviewBannerHtml() {
    return (
      '<div class="be-atelier-internal-preview-banner be-atelier-factory-preview-banner" role="note" data-be-display-gate="factory_preview_internal" data-be-factory-preview="1">' +
      '<strong>Factory Preview — Not Public / Not Active Baseline</strong>' +
      '<span> This profile is for local/internal factory review only. Brand Status is not Active/Live for production, and this brand is not part of the protected Active/Live baseline.</span>' +
      '</div>'
    );
  }

  function hasPresentationRows(brand) {
    var be = brand && brand.brandExplorer;
    return !!(be && Array.isArray(be.blocks) && be.blocks.length > 0);
  }

  /** External (public) full-profile decision — ignores founder / factory preview overrides. */
  function externalShouldRenderFullProfile(brand) {
    if (brand && brand.shouldRenderFullProfile === true) return true;
    if (brand && brand.shouldRenderFullProfile === false) {
      // Still allow legacy pending migration when API completeness supports unlock.
      if (
        brand.brandExplorerDisplayState === 'legacy_approved_pending_migration' &&
        brand.brandExplorerDisplayCompleteness &&
        brand.brandExplorerDisplayCompleteness.historicalApproved === true &&
        brand.brandExplorerDisplayCompleteness.visualsCountReady === true &&
        brand.brandExplorerDisplayCompleteness.imageUniquenessPass === true
      ) {
        return true;
      }
      return false;
    }
    if (brand && brand.brandExplorerDisplayState) {
      return (
        brand.brandExplorerDisplayState === 'external_owner_ready' ||
        brand.brandExplorerDisplayState === 'active_profile_ready' ||
        brand.brandExplorerDisplayState === 'founder_review_ready' ||
        (brand.brandExplorerDisplayState === 'legacy_approved_pending_migration' &&
          brand.brandExplorerDisplayCompleteness &&
          brand.brandExplorerDisplayCompleteness.visualsCountReady === true &&
          brand.brandExplorerDisplayCompleteness.imageUniquenessPass !== false)
      );
    }
    return false;
  }

  function shouldRenderFullProfile(brand) {
    // Factory preview: full tabs for allowlisted candidates only (distinct from public-full).
    if (canRenderFactoryPreview(brand)) return true;
    if (isInternalPreviewRequest()) {
      // Founder/internal preview: full tabs when presentation rows exist.
      return hasPresentationRows(brand) || externalShouldRenderFullProfile(brand);
    }
    return externalShouldRenderFullProfile(brand);
  }

  function isExternalQualityLocked(brand) {
    return !externalShouldRenderFullProfile(brand);
  }

  function shouldSuppressIncompleteExternalSections(brand) {
    if (canRenderFactoryPreview(brand) || isInternalPreviewRequest()) return false;
    return isExternalQualityLocked(brand);
  }

  function internalPreviewBannerHtml() {
    return (
      '<div class="be-atelier-internal-preview-banner" role="note" data-be-display-gate="internal_preview">' +
      '<strong>Founder Preview</strong>' +
      '<span> This profile is visible for internal review. Some active-profile gates may still be pending.</span>' +
      '</div>'
    );
  }

  function profileInPreparationSectionHtml() {
    return (
      '<section class="oe-section oe-section--profile-in-preparation" data-be-display-gate="profile-in-preparation">' +
      '<h2 class="oe-section-title">Profile in Preparation</h2>' +
      '<p class="oe-section-hint">This brand profile is being prepared for external review. Full Brand Explorer tabs, scenario cards, proof points, and owner-facing narrative will appear after presentation, visual assets, copy governance, founder review, and active-profile gates pass.</p>' +
      '</section>'
    );
  }

  function lockedExternalProfileHtml() {
    return (
      '<div class="be-atelier-oe be-atelier-oe--quality-locked">' +
      profileInPreparationSectionHtml() +
      '</div>'
    );
  }

  /** Gallery blocks from API only — excludes Do Not Display rows filtered server-side. */
  function explorerGalleryBlocks(brand) {
    var be = brand.brandExplorer;
    if (!be || !Array.isArray(be.blocks)) return [];
    return be.blocks
      .filter(function (b) {
        return b && /^materials\.gallery\.\d+$/.test(String(b.slotKey || ''));
      })
      .sort(function (a, b) {
        var as = typeof a.sort === 'number' && !isNaN(a.sort) ? a.sort : 0;
        var bs = typeof b.sort === 'number' && !isNaN(b.sort) ? b.sort : 0;
        if (as !== bs) return as - bs;
        return String(a.slotKey || '').localeCompare(String(b.slotKey || ''));
      });
  }

  /** Single-line copy from Brand Explorer Presentation (Body, else Title). */
  function explorerPresentationLine(brand, slotKey) {
    var merged = explorerMergedBody(brand, slotKey);
    if (hasVal(merged)) return String(merged).trim();
    var row = explorerFirstBlock(brand, slotKey);
    if (row && hasVal(row.body)) return String(row.body).trim();
    if (row && hasVal(row.title)) return String(row.title).trim();
    return '';
  }

  /** Dealality Insight tab — presentation slot first; no Brand Basics “Profile Analysis” column required. */
  function dealalitySummaryFromBrand(brand) {
    var slot = explorerMergedBody(brand, 'insight.summary');
    if (hasVal(slot)) return String(slot).trim();
    if (hasVal(brand.brandValueProposition)) return String(brand.brandValueProposition).trim();
    if (hasVal(brand.keyBrandDifferentiators)) {
      var diffPara = String(brand.keyBrandDifferentiators)
        .split(/\n\n+/)
        .map(function (s) {
          return s.trim();
        })
        .filter(Boolean)[0];
      if (diffPara) return diffPara;
    }
    if (hasVal(brand.brandPositioning)) return String(brand.brandPositioning).trim();
    if (hasVal(brand.brandProfileAnalysis)) return String(brand.brandProfileAnalysis).trim();
    return '';
  }

  /** Multiple rows with same slotKey → list of { title, body } (sorted). opts.prefix matches insight.similar.1 style keys. */
  function explorerCardRowsForSlot(brand, slotKey, opts) {
    return explorerBlocksForSlot(brand, slotKey, opts).map(function (r) {
      return {
        title: hasVal(r.title) ? String(r.title).trim() : '',
        body: hasVal(r.body) ? String(r.body).trim() : ''
      };
    });
  }

  /** Split double-newline body into up to max paragraphs (for scenario cards, etc.). */
  function explorerParagraphs(brand, slotKey, max) {
    var raw = explorerMergedBody(brand, slotKey, '\n\n');
    if (!hasVal(raw)) return [];
    return String(raw)
      .split(/\n\n+/)
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean)
      .slice(0, max || 20);
  }

  function explorerLinesAsUl(htmlEscaper, raw) {
    if (!hasVal(raw)) return '';
    var lines = String(raw)
      .split(/\n+/)
      .map(function (s) {
        return s.replace(/^\s*[-*]\s*/, '').trim();
      })
      .filter(Boolean);
    if (!lines.length) return '';
    return (
      '<ul>' +
      lines
        .map(function (line) {
          return '<li>' + htmlEscaper(line) + '</li>';
        })
        .join('') +
      '</ul>'
    );
  }

  function fmtNum(n) {
    if (n == null || n === '' || (typeof n === 'number' && !isFinite(n))) return '';
    var x = typeof n === 'number' ? n : parseFloat(String(n).replace(/,/g, ''));
    if (!isFinite(x)) return String(n);
    if (Math.abs(x - Math.round(x)) < 1e-6) {
      return Math.round(x).toLocaleString('en-US');
    }
    return x.toLocaleString('en-US', { maximumFractionDigits: 4, minimumFractionDigits: 0 });
  }

  function sanitizeDisplayCopy(text) {
    var raw = text == null ? '' : String(text);
    if (typeof window !== 'undefined' && window.DealalitySanitizeExternalCopy) {
      raw = window.DealalitySanitizeExternalCopy.sanitizeExternalCopy(raw);
    }
    // Owner-safe display scrub for Brand Basics / Presentation fallbacks in full profile + internal preview.
    raw = raw.replace(/\bfranchise disclosure document\b/gi, 'commercial agreement materials');
    raw = raw.replace(/\bfranchise disclosure\b/gi, 'commercial agreement review');
    raw = raw.replace(/\bdisclosure document\b/gi, 'commercial agreement materials');
    raw = raw.replace(/\bfee stack\b/gi, 'participation costs and program fees');
    raw = raw.replace(/\bnet contribution\b/gi, 'contribution after program costs');
    raw = raw.replace(/\bItem\s*19\b/gi, 'public performance materials');
    raw = raw.replace(/\bItem\s*7\b/gi, 'initial investment schedules');
    raw = raw.replace(/\bFDD\b/g, 'commercial agreement materials');
    raw = raw.replace(/\bLOI\b/g, 'commercial proposal');
    raw = raw.replace(/\bADR\b/g, 'average daily rate');
    raw = raw.replace(/\bRevPAR\b/g, 'revenue per available room');
    raw = raw.replace(/\bparticipation cost categories\b/gi, 'participation costs and program fees');
    raw = raw.replace(/\bowner economics after brand-related costs\b/gi, 'whether brand economics fit the asset after program costs');
    raw = raw.replace(/\bOutput Note\.?/gi, '');
    raw = raw.replace(/\binternal review\b/gi, 'owner diligence review');
    return raw;
  }

  function fmtCell(v) {
    if (v == null || v === '') return '';
    if (Array.isArray(v)) {
      return v
        .map(function (x) {
          return fmtCell(x);
        })
        .filter(Boolean)
        .join(', ');
    }
    if (typeof v === 'boolean') return v ? 'Yes' : 'No';
    return sanitizeDisplayCopy(String(v));
  }

  function linkIfUrl(val) {
    var s = String(val || '').trim();
    if (!s) return '';
    if (s.indexOf('http') !== 0) return escapeHtml(s);
    var label = 'Official brand website';
    try {
      var u = new URL(s);
      if (u && u.hostname) label = u.hostname.replace(/^www\./, '');
    } catch (e) {
      label = 'Official brand website';
    }
    return (
      '<a class="be-link" href="' +
      escapeHtml(s) +
      '" target="_blank" rel="noopener noreferrer">' +
      escapeHtml(label) +
      '</a>'
    );
  }

  function atelierTabShell(html) {
    var s = String(html || '').trim();
    if (!s) {
      return '<p class="be-atelier-tab-empty-message">No Brand Setup Fields Are Populated for This Tab Yet.</p>';
    }
    return s;
  }

  function splitBullets(val) {
    if (!val) return [];
    if (Array.isArray(val)) return val.map(String).filter(Boolean);
    return String(val)
      .split(/\n|;|•/g)
      .map(function (s) {
        return s.replace(/^\s*[-*]\s*/, '').trim();
      })
      .filter(Boolean);
  }

  function footprintRegionStatusClass(statusLabel) {
    var l = String(statusLabel || '').toLowerCase();
    if (l.indexOf('limited') >= 0) return 'status-label--limited';
    if (l.indexOf('selective') >= 0) return 'status-label--selective';
    if (l.indexOf('emerging') >= 0 || l.indexOf('high relevance') >= 0) return 'status-label--emerging';
    return 'status-label--established';
  }

  function footprintRegionCardDim(statusLabel) {
    var l = String(statusLabel || '').toLowerCase();
    return l.indexOf('limited') >= 0 || l.indexOf('selective') >= 0;
  }

  function parseFootprintRegionBlock(block, defaultName) {
    var name =
      block && hasVal(block.title) ? String(block.title).trim() : defaultName || 'Region';
    var raw = block && hasVal(block.body) ? String(block.body).trim() : '';
    var paras = raw
      .split(/\n\n+/)
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean);
    var statusLabel = paras[0] || '';
    var narrative = paras.length > 1 ? paras.slice(1).join('\n\n') : '';
    if (!narrative && paras.length === 1 && paras[0].length > 120) {
      narrative = paras[0];
      statusLabel = 'Directional presence';
    }
    return { name: name, statusLabel: statusLabel, narrative: narrative };
  }

  function footprintRegionCardsFromPresentation(brand) {
    var cards = [];
    FOOTPRINT_REGION_SLOT_DEFS.forEach(function (def) {
      var row = explorerFirstBlock(brand, def.slot);
      if (!row || (!hasVal(row.body) && !hasVal(row.title))) return;
      var parsed = parseFootprintRegionBlock(row, def.defaultName);
      if (!hasVal(parsed.narrative) && !hasVal(parsed.statusLabel)) return;
      cards.push(parsed);
    });
    return cards;
  }

  /** Hero / overview footprint line — same display footprint as Footprint Metrics tables. */
  function footprintSummaryLine(brand) {
    var disp = resolveFootprintDisplay(brand);
    if (!disp.showVerifiedMetrics) return '';
    var fp = disp.fp || {};
    if (!fp || typeof fp !== 'object') return '';
    var openH = fp.totalExistingHotels;
    var pipeTotals = footprintPipelineTotals(fp);
    var pipH = pipeTotals.hotels;
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
    var parts = [];
    if (hasVal(openH) || pipH > 0) {
      parts.push((fmtNum(openH) || '0') + ' open / ' + (pipH > 0 ? fmtNum(pipH) : '0') + ' pipeline hotels');
    }
    var markets = fp.formValues && fp.formValues.numberOfMarkets;
    if (hasVal(markets)) parts.push(fmtNum(markets) + ' markets');
    var sm = fp.priorityCities || (fp.formValues && fp.formValues.specificMarkets);
    if (hasVal(sm)) parts.push(String(sm).trim());
    return parts.length ? parts.join(' · ') : '';
  }

  function lcFv(brand, key) {
    var fv = brand.loyaltyCommercial && brand.loyaltyCommercial.formValues;
    if (!fv) return '';
    var v = fv[key];
    return v != null && v !== '' ? v : '';
  }

  function joinOpMulti(val) {
    if (Array.isArray(val) && val.length) return val.join(', ');
    if (val != null && val !== '') return String(val);
    return '';
  }

  function opSystemsIntegrationLine(brand) {
    var op = brand.operationalSupport || {};
    var parts = [];
    var ts = joinOpMulti(op.technologyServices);
    if (ts) parts.push(ts);
    if (hasVal(op.technologyServicesOther)) parts.push(String(op.technologyServicesOther).trim());
    return parts.join(' · ');
  }

  function opTechnologyExpectationsLine(brand) {
    var op = brand.operationalSupport || {};
    var lc = brand.loyaltyCommercial && brand.loyaltyCommercial.formValues ? brand.loyaltyCommercial.formValues : {};
    var parts = [];
    if (hasVal(op.crsParticipation)) parts.push('CRS: ' + String(op.crsParticipation).trim());
    if (hasVal(op.gdsParticipation)) parts.push('GDS: ' + String(op.gdsParticipation).trim());
    if (hasVal(op.ownerPortalTier) || hasVal(op.ownerPortalFeatures)) {
      var portal = [op.ownerPortalTier, op.ownerPortalFeatures].filter(hasVal).join(' — ');
      if (portal) parts.push('Owner portal: ' + portal);
    }
    if (hasVal(lc.crsUsagePercent)) parts.push('CRS usage (est.): ' + String(lc.crsUsagePercent).trim() + '%');
    return parts.join(' · ');
  }

  /** Footprint table: typical managed vs franchised systemwide mix (agreement style, not service tier). */
  function managedFranchisedMixLine(brand) {
    var fv = brand.footprint && brand.footprint.formValues ? brand.footprint.formValues : {};
    var m = fv.typicalManagedPercent;
    var f = fv.typicalFranchisedPercent;
    if (!hasVal(m) && !hasVal(f)) return '';
    function pctLabel(raw) {
      if (raw == null || raw === '') return '';
      var n = parseFloat(String(raw).replace(/%/g, '').trim());
      if (!isFinite(n)) return String(raw).replace(/%/g, '').trim();
      return String(Math.round(n)) + '%';
    }
    var ms = pctLabel(m);
    var fs = pctLabel(f);
    if (ms && fs) return ms + ' managed · ' + fs + ' franchised';
    if (ms) return ms + ' managed';
    if (fs) return fs + ' franchised';
    return '';
  }

  /**
   * Management Option (atelier static: franchise vs managed posture).
   * Uses Footprint managed/franchised %; does not reuse Hotel Service Model /
   * Service / Operating Model (that is operational style, not affiliation control).
   */
  function managementOptionLine(brand) {
    var mix = managedFranchisedMixLine(brand);
    if (mix) return mix;
    if (hasVal(brand.brandModelFormat)) return String(brand.brandModelFormat).trim();
    return '';
  }

  /**
   * Typical Ownership Structure — owner/operator relationship (Operational Support), not parent company.
   */
  function typicalOwnershipStructureLine(brand) {
    var op = brand.operationalSupport || {};
    var parts = [];
    if (hasVal(op.ownerInvolvement)) parts.push(String(op.ownerInvolvement).trim());
    if (hasVal(op.ownerAdvisoryBoard)) parts.push('Advisory board: ' + String(op.ownerAdvisoryBoard).trim());
    if (!parts.length && hasVal(op.decisionMaking)) parts.push(String(op.decisionMaking).trim());
    return parts.join(' · ');
  }

  /** Staffing / labor intensity proxy: service tier, scale, F&B and meeting footprint from Brand Standards. */
  function staffingIntensityLine(brand) {
    var std = brand.brandStandards || {};
    var parts = [brand.hotelServiceModel, brand.hotelChainScale].filter(hasVal);
    if (hasVal(std.brandFbProgramType)) parts.push(std.brandFbProgramType);
    if (hasVal(std.brandFbOutletsCount)) parts.push('~' + fmtNum(std.brandFbOutletsCount) + ' F&B outlets');
    if (hasVal(std.brandMeetingRoomsCount)) parts.push('~' + fmtNum(std.brandMeetingRoomsCount) + ' meeting rooms');
    return parts.join(' · ');
  }

  /** Training Requirements — HR & training services + owner education (not QA cadence text). */
  function trainingRequirementsLine(brand) {
    var op = brand.operationalSupport || {};
    var parts = [];
    var hr = joinOpMulti(op.hrTrainingServices);
    if (hr) parts.push(hr);
    if (hasVal(op.hrTrainingServicesOther)) parts.push(String(op.hrTrainingServicesOther).trim());
    if (hasVal(op.ownerEducation)) parts.push(String(op.ownerEducation).trim());
    return parts.join(' · ');
  }

  /** QA Rhythm — Brand Standards QA expectations + deal QA clause + additional standards notes. */
  function qaRhythmLine(brand) {
    var std = brand.brandStandards || {};
    var dt = brand.dealTerms && typeof brand.dealTerms === 'object' ? brand.dealTerms : {};
    var parts = [];
    if (hasVal(std.brandQaExpectations)) parts.push(String(std.brandQaExpectations).trim());
    if (hasVal(dt.qaComplianceRequirement)) parts.push('Agreement: ' + String(dt.qaComplianceRequirement).trim());
    if (hasVal(std.brandStandardsNotes)) parts.push(String(std.brandStandardsNotes).trim());
    return parts.join(' · ');
  }

  function brandInvolvementLine(brand) {
    var std = brand.brandStandards || {};
    var op = brand.operationalSupport || {};
    if (hasVal(op.serviceDifferentiators)) return String(op.serviceDifferentiators).trim();
    var out = [];
    if (Array.isArray(std.brandCompliance) && std.brandCompliance.length) {
      out.push(std.brandCompliance.slice(0, 6).join(', '));
    } else if (hasVal(std.brandCompliance)) out.push(fmtCell(std.brandCompliance));
    if (hasVal(op.ongoingSupportIncluded)) out.push(String(op.ongoingSupportIncluded).trim());
    return out.join(' · ');
  }

  function preOpeningServicesLine(brand) {
    var op = brand.operationalSupport || {};
    var parts = [];
    var dev = joinOpMulti(op.developmentServices);
    if (dev) parts.push(dev);
    var des = joinOpMulti(op.designRenovationSupport);
    if (des) parts.push(des);
    if (hasVal(op.developmentServicesOther)) parts.push(String(op.developmentServicesOther).trim());
    if (hasVal(op.designRenovationSupportOther)) parts.push(String(op.designRenovationSupportOther).trim());
    return parts.join(' · ');
  }

  function loyaltyStrengthLine(brand) {
    var name = lcFv(brand, 'typicalLoyaltyProgramName');
    var mem = lcFv(brand, 'totalGlobalMembersMillions');
    var parts = [];
    if (hasVal(name)) parts.push(String(name));
    if (hasVal(mem)) parts.push('~' + String(mem).replace(/\s*m\s*$/i, '') + 'M members (est.)');
    var pct = lcFv(brand, 'typicalLoyaltyRoomsPercent');
    if (hasVal(pct)) parts.push(String(pct) + '% rooms from loyalty (est.)');
    return parts.join(' — ');
  }

  /** Overview snapshot: where the brand succeeds (not Footprint city lists). */
  function typicalUseCaseFromBrand(brand) {
    var slot = explorerMergedBodiesOnly(brand, 'overview.typical_use_case');
    if (hasVal(slot)) return stripKvLabelPrefix(slot, ['Typical Use Case']);
    var row = explorerFirstBlock(brand, 'overview.typical_use_case');
    if (row && hasVal(row.body)) {
      return stripKvLabelPrefix(String(row.body).trim(), ['Typical Use Case']);
    }
    if (row && hasVal(row.title) && !/^typical use case$/i.test(String(row.title).trim())) {
      return String(row.title).trim();
    }

    var pf = brand.projectFit || {};
    var pfFv = pf.formValues || pf;
    var notes = pfFv.idealProjectsAdditionalNotes || pf.idealProjectsAdditionalNotes;
    if (hasVal(notes)) return String(notes).trim();

    return '';
  }

  /** Overview snapshot: conversion vs new-build emphasis (not Basics stage + model join). */
  function developmentModelFromBrand(brand) {
    var slot = explorerMergedBodiesOnly(brand, 'overview.development_model');
    if (hasVal(slot)) return stripKvLabelPrefix(slot, ['Development Model']);
    var row = explorerFirstBlock(brand, 'overview.development_model');
    if (row && hasVal(row.body)) {
      return stripKvLabelPrefix(String(row.body).trim(), ['Development Model']);
    }
    if (row && hasVal(row.title) && !/^development model$/i.test(String(row.title).trim())) {
      return String(row.title).trim();
    }
    return '';
  }

  function brandedResidencesLine(brand) {
    var r = brand && brand.residences ? brand.residences : null;
    var status = r && hasVal(r.status) ? String(r.status).trim() : 'Not Confirmed';
    return status;
  }

  function brandedResidencesHelperHtml(brand) {
    var r = brand && brand.residences ? brand.residences : null;
    if (!r) return '';
    var status = brandedResidencesLine(brand);
    var parts = [];
    if (hasVal(r.notes) && (status === 'Yes' || status === 'Case-by-Case')) {
      parts.push('<p class="oe-section-hint">' + escapeHtml(String(r.notes).trim()) + '</p>');
    } else if (status === 'Case-by-Case' || status === 'Yes') {
      parts.push(
        '<p class="oe-section-hint">Confirm market, license structure, and brand approval requirements before underwriting.</p>'
      );
    }
    if (hasVal(r.sourceUrl)) {
      parts.push(
        '<p class="oe-section-hint"><a href="' +
          escapeHtml(String(r.sourceUrl).trim()) +
          '" target="_blank" rel="noopener noreferrer">Source</a></p>'
      );
    }
    return parts.join('');
  }

  /** Overview snapshot: position within parent portfolio (not Brand Positioning long copy). */
  function relativePositioningFromBrand(brand) {
    var relLabels = ['Relative Positioning'];
    var relSlot = explorerMergedBodiesOnly(brand, 'overview.relative_positioning');
    if (hasVal(relSlot)) return stripKvLabelPrefix(relSlot, relLabels);
    var relRow = explorerFirstBlock(brand, 'overview.relative_positioning');
    if (relRow && hasVal(relRow.body)) {
      return stripKvLabelPrefix(String(relRow.body).trim(), relLabels);
    }
    if (relRow && hasVal(relRow.title) && !/^relative positioning$/i.test(String(relRow.title).trim())) {
      return String(relRow.title).trim();
    }
    var ctxRow = explorerFirstBlock(brand, 'overview.portfolio_context');
    if (ctxRow && hasVal(ctxRow.body)) {
      var tierInTitle = hasVal(ctxRow.title) ? String(ctxRow.title).trim() : '';
      var bodyText = String(ctxRow.body).trim();
      if (tierInTitle || /^\d\s/.test(bodyText) || /^\d$/.test(bodyText.split(/\n/)[0])) {
        if (!tierInTitle && bodyText) {
          var lines = bodyText.split(/\n+/).map(function (s) {
            return s.trim();
          });
          if (lines.length && /^\d$/.test(lines[0])) {
            return lines.slice(1).join('\n\n').trim();
          }
        }
        return bodyText;
      }
      return bodyText;
    }
    return '';
  }

  /** Portfolio & Performance: min/max property size (rooms) → snapshot “typical keys” line. */
  function typicalKeysRangeFromPortfolio(brand) {
    var pp = brand.portfolioPerformance || {};
    var minK = pp.minPropertySize;
    var maxK = pp.maxPropertySize;
    if (hasVal(minK) && hasVal(maxK)) return fmtNum(minK) + '–' + fmtNum(maxK) + ' rooms';
    if (hasVal(minK)) return fmtNum(minK) + '+ rooms (minimum)';
    if (hasVal(maxK)) return 'Up to ' + fmtNum(maxK) + ' rooms';
    return '';
  }

  function loyaltyProofHeadline(brand) {
    var n = lcFv(brand, 'typicalLoyaltyProgramName');
    if (hasVal(n)) return String(n).trim() + ' · Loyalty';
    return 'Loyalty Strength';
  }

  function oeDd(v) {
    if (!hasVal(v)) return '<dd class="oe-dd oe-dd--empty">&nbsp;</dd>';
    var s = String(v).trim();
    if (s.toLowerCase().indexOf('http') === 0) return '<dd class="oe-dd">' + linkIfUrl(v) + '</dd>';
    return '<dd class="oe-dd">' + escapeHtml(fmtCell(v)).replace(/\n/g, '<br>') + '</dd>';
  }

  /** For active_profile_ready: omit blank KV rows instead of rendering empty shells. */
  function oeKvBlock(title, rows, opts) {
    opts = opts || {};
    var suppressEmpty = opts.suppressEmpty === true;
    var visible = (rows || []).filter(function (r) {
      return !suppressEmpty || hasVal(r.v);
    });
    if (suppressEmpty && !visible.length) return '';
    var inner = visible
      .map(function (r) {
        return '<dt>' + escapeHtml(r.k) + '</dt>' + oeDd(r.v);
      })
      .join('');
    return '<div class="oe-cluster"><h3>' + escapeHtml(title) + '</h3><dl class="kv">' + inner + '</dl></div>';
  }

  function pipelineLineForProof(fp) {
    if (!fp || typeof fp !== 'object') return '';
    var pipeTotals = footprintPipelineTotals(fp);
    var pipH = pipeTotals.hotels;
    var pipR = pipeTotals.rooms;
    if (!pipH && !pipR) return '';
    return (fmtNum(pipH) || '0') + ' pipeline hotels / ' + (fmtNum(pipR) || '0') + ' pipeline rooms';
  }

  function ladderIndexForScale(scale) {
    var s = String(scale || '').toLowerCase();
    if (!s) return 2;
    if (s.indexOf('luxury') !== -1 || s.indexOf('upper upscale') !== -1) return 3;
    if (s.indexOf('upscale') !== -1 && s.indexOf('upper') === -1) return 2;
    if (s.indexOf('upper mid') !== -1 || s.indexOf('midscale') !== -1) return 1;
    if (s.indexOf('economy') !== -1) return 0;
    return 2;
  }

  /** Prefer fixed parent-ladder seat over chain-scale so Courtyard isn’t left of SpringHill while highlighting as “upscale”. */
  function staticPortfolioTierIndexForBrand(brand) {
    var parentKey = portfolioParentKeyForSiblingMatch(brand && brand.parentCompany);
    var name = brand && hasVal(brand.name) ? String(brand.name).trim() : '';
    if (!name) return null;
    var matrix = staticPortfolioMatrixForParentKey(parentKey);
    if (!matrix) return null;
    for (var i = 0; i < matrix.length; i++) {
      for (var j = 0; j < (matrix[i] || []).length; j++) {
        if (portfolioBrandNamesReferToSameBrand(matrix[i][j], name)) return i;
      }
    }
    return null;
  }

  function ladderTierIndexFromPresentationRaw(raw) {
    if (!hasVal(raw)) return null;
    var text = String(raw).trim();
    var firstToken = text.split(/\s+/)[0];
    var n = parseInt(firstToken, 10);
    if (!isNaN(n) && n >= 0 && n <= 3) return n;
    var key = text.toLowerCase();
    if (key === 'economy' || key === 'tier0' || key === 'tier-0') return 0;
    if (key === 'upper_mid' || key === 'upper-mid' || key === 'midscale' || key === 'tier1' || key === 'tier-1')
      return 1;
    if (key === 'upscale' || key === 'premium' || key === 'tier2' || key === 'tier-2') return 2;
    if (
      key === 'upper_upscale' ||
      key === 'upper-upscale' ||
      key === 'luxury' ||
      key === 'flagship' ||
      key === 'tier3' ||
      key === 'tier-3'
    )
      return 3;
    return null;
  }

  /** Overview Portfolio Context ladder step (0–3); API + presentation slot override chain scale. */
  function portfolioLadderTierIndex(brand) {
    if (brand && typeof brand.portfolioLadderTier === 'number') {
      var preset = brand.portfolioLadderTier;
      if (preset >= 0 && preset <= 3) return preset;
    }
    var ctxRow = explorerFirstBlock(brand, 'overview.portfolio_context');
    if (ctxRow) {
      var tierRaw = hasVal(ctxRow.title) ? String(ctxRow.title).trim() : '';
      if (!tierRaw && hasVal(ctxRow.body)) {
        var firstLine = String(ctxRow.body).trim().split(/\n+/)[0];
        if (/^\d$/.test(firstLine)) tierRaw = firstLine;
      }
      var fromCtx = ladderTierIndexFromPresentationRaw(tierRaw);
      if (fromCtx != null) return fromCtx;
    }
    var row = explorerFirstBlock(brand, 'overview.portfolio_ladder_tier');
    if (row) {
      var raw = hasVal(row.body)
        ? String(row.body).trim()
        : hasVal(row.title)
          ? String(row.title).trim()
          : '';
      var fromLegacy = ladderTierIndexFromPresentationRaw(raw);
      if (fromLegacy != null) return fromLegacy;
    }
    var fromStaticLadder = staticPortfolioTierIndexForBrand(brand);
    if (fromStaticLadder != null) return fromStaticLadder;
    return ladderIndexForScale(brand && (brand.hotelChainScale || brand.chainScale));
  }

  function ladderTierFallbackLabels() {
    return [
      'Lower-scale brands',
      'Mid-scale brands',
      'Upscale brands',
      'Upper-scale brands'
    ];
  }

  /** IHG portfolio ladder — Dealality owner-planning context (illustrative; not company-validated). */
  var IHG_PORTFOLIO_LADDER_TIER_LABELS = [
    'Essential & Midscale Extended Stay',
    'Mainstream Upper Mid / Core Upscale',
    'Premium Upscale & Lifestyle',
    'Luxury & Soft Collections'
  ];

  var IHG_PORTFOLIO_TIER_BRANDS = [
    ['avid hotels', 'Candlewood Suites', 'Holiday Inn Express', 'Atwell Suites'],
    ['Holiday Inn', 'Staybridge Suites', 'Garner Hotels'],
    ['Crowne Plaza', 'Hotel Indigo', 'Voco Hotels', 'EVEN Hotels', 'HUALUXE'],
    ['InterContinental', 'Kimpton Hotels', 'Regent', 'Six Senses', 'Vignette Collection']
  ];

  /** Hilton Worldwide portfolio ladder — Dealality owner-planning context (illustrative; not company-validated). */
  var HILTON_PORTFOLIO_LADDER_TIER_LABELS = [
    'Focused Service & Value Extended Stay',
    'Upscale Focused & Lifestyle Select',
    'Premium Full-Service & Soft Collections',
    'Luxury Flagship'
  ];

  var HILTON_PORTFOLIO_TIER_BRANDS = [
    ['Tru by Hilton', 'Spark by Hilton', 'Hampton by Hilton', 'Home2 Suites', 'LivSmart Studios by Hilton'],
    ['Hilton Garden Inn', 'Homewood Suites', 'Motto by Hilton', 'Tempo by Hilton'],
    [
      'DoubleTree by Hilton',
      'Embassy Suites by Hilton',
      'Hilton Hotels & Resorts',
      'Canopy by Hilton',
      'Signia by Hilton',
      'Tapestry Collection by Hilton',
      'Curio Collection by Hilton'
    ],
    ['Waldorf Astoria', 'Conrad Hotels & Resorts', 'LXR Hotels & Resorts']
  ];

  /** Marriott International portfolio ladder — Dealality owner-planning context (illustrative; not company-validated). */
  var MARRIOTT_PORTFOLIO_LADDER_TIER_LABELS = [
    'Upper Midscale Select & Extended Stay',
    'Mainstream Upscale Select',
    'Lifestyle Select & Soft Collections',
    'Luxury & Lifestyle Flagship'
  ];

  var MARRIOTT_PORTFOLIO_TIER_BRANDS = [
    ['Fairfield by Marriott', 'TownePlace Suites', 'City Express by Marriott'],
    ['Courtyard by Marriott', 'Residence Inn', 'SpringHill Suites', 'Four Points'],
    [
      'Aloft',
      'AC Hotels by Marriott',
      'Moxy Hotels',
      'Element Hotels',
      'Autograph Collection',
      'Tribute Portfolio',
      'Design Hotels'
    ],
    ['The Ritz-Carlton', 'St. Regis', 'W Hotels', 'The Luxury Collection', 'Edition']
  ];

  /** Wyndham — Dealality owner-planning context (illustrative; not company-validated). */
  var WYNDHAM_PORTFOLIO_LADDER_TIER_LABELS = [
    'Economy & Midscale Core',
    'Upper Mid / Focused Upscale',
    'Lifestyle & Soft Collections',
    'Premium Soft / Upscale Flag'
  ];

  var WYNDHAM_PORTFOLIO_TIER_BRANDS = [
    ['Super 8', 'Days Inn', 'Travelodge', 'Microtel'],
    ['La Quinta', 'Wingate', 'Ramada', 'Baymont'],
    ['Trademark Collection by Wyndham', 'Dazzler by Wyndham', 'Esplendor by Wyndham'],
    ['Wyndham Grand', 'Wyndham Hotels & Resorts', 'Dolce Hotels and Resorts']
  ];

  /** Best Western — Dealality owner-planning context (illustrative; not company-validated). */
  var BEST_WESTERN_PORTFOLIO_LADDER_TIER_LABELS = [
    'Economy / Core Midscale',
    'Mainstream Midscale',
    'Soft Collections & Lifestyle',
    'Upper Soft / Premier'
  ];

  var BEST_WESTERN_PORTFOLIO_TIER_BRANDS = [
    ['Best Western', 'Best Western Plus'],
    ['Best Western Premier'],
    ['BW Signature Collection', 'Handwritten Collection'],
    ['BW Premier Collection']
  ];

  /** Accor — Dealality owner-planning context (illustrative; not company-validated). */
  var ACCOR_PORTFOLIO_LADDER_TIER_LABELS = [
    'Economy & Midscale',
    'Upscale Mainstream',
    'Lifestyle & Soft Collections',
    'Luxury Flagship'
  ];

  var ACCOR_PORTFOLIO_TIER_BRANDS = [
    ['ibis', 'ibis Styles', 'ibis budget', 'Mercure'],
    ['Novotel', 'Pullman', 'Swissôtel'],
    ['MGallery Collection', 'JO&JOE', 'tribe'],
    ['Sofitel', 'Raffles', 'Fairmont']
  ];

  /** Choice Hotels International portfolio ladder — Dealality owner-planning context (illustrative; not company-validated). */
  var CHOICE_PORTFOLIO_LADDER_TIER_LABELS = [
    'Economy & Value Extended Stay',
    'Midscale / Upper Midscale',
    'Upscale & Soft Mid Collections',
    'Upper Upscale / Soft Flagship'
  ];

  var CHOICE_PORTFOLIO_TIER_BRANDS = [
    ['Econo Lodge', 'Rodeway Inn', 'Suburban Studios', 'WoodSpring Suites'],
    [
      'Quality Inn',
      'Sleep Inn',
      'Clarion Pointe',
      'Everhome Suites',
      'MainStay Suites',
      'Comfort Inn & Suites',
      'Country Inn & Suites',
      'Park Inn by Choice',
      'Radisson Inn & Suites'
    ],
    [
      'Ascend Hotel Collection',
      'Cambria Hotels',
      'Clarion',
      'Park Plaza by Choice',
      'Radisson by Choice',
      'Radisson RED by Choice'
    ],
    ['Radisson Blu by Choice', 'Radisson Collection by Choice', 'Radisson Individuals by Choice']
  ];

  /** Collapse "AC Hotels by Marriott" / "AC Hotels" via parent-suffix strip only (keep Inn/Blu/RED/etc.). */
  function normalizePortfolioBrandCompareKey(name) {
    return String(name || '')
      .trim()
      .toLowerCase()
      .replace(/\s+/g, ' ')
      .replace(/\s+by\s+(marriott|hilton|ihg|choice|wyndham|hyatt|radisson|sheraton)\b/g, '')
      .replace(/[^a-z0-9]+/g, '');
  }

  function portfolioBrandNamesReferToSameBrand(a, b) {
    var na = normalizePortfolioBrandCompareKey(a);
    var nb = normalizePortfolioBrandCompareKey(b);
    if (!na || !nb) return false;
    // Exact normalized match only. Prefix matching incorrectly collapses
    // "Radisson Blu" / "Radisson RED" / "Radisson Inn & Suites" into one seat.
    return na === nb;
  }

  function filterCurrentBrandFromPortfolioTier(tiers, currentBrandName) {
    if (!currentBrandName || !tiers) return tiers;
    for (var i = 0; i < tiers.length; i++) {
      tiers[i] = (tiers[i] || []).filter(function (nm) {
        return !portfolioBrandNamesReferToSameBrand(nm, currentBrandName);
      });
    }
    return tiers;
  }

  /** Drop short/long-name duplicates inside a tier (e.g. "AC Hotels" + "AC Hotels by Marriott"). */
  function dedupePortfolioTierByBrandAlias(tiers) {
    if (!tiers) return tiers;
    for (var i = 0; i < tiers.length; i++) {
      var seen = {};
      var out = [];
      (tiers[i] || []).forEach(function (nm) {
        var key = normalizePortfolioBrandCompareKey(nm);
        if (!key || seen[key]) return;
        seen[key] = true;
        out.push(nm);
      });
      tiers[i] = out;
    }
    return tiers;
  }

  function isIhgParentCompanyKey(parentKey) {
    return (
      parentKey.indexOf('ihg hotels') !== -1 ||
      parentKey.indexOf('intercontinental hotels group') !== -1
    );
  }

  function isHiltonParentCompanyKey(parentKey) {
    return (
      parentKey.indexOf('hilton worldwide') !== -1 ||
      parentKey === 'hilton' ||
      (parentKey.indexOf('hilton') !== -1 && parentKey.indexOf('hilton garden') === -1)
    );
  }

  function isMarriottParentCompanyKey(parentKey) {
    return parentKey.indexOf('marriott') !== -1;
  }

  function isChoiceParentCompanyKey(parentKey) {
    return parentKey.indexOf('choice hotels') !== -1;
  }

  function isWyndhamParentCompanyKey(parentKey) {
    return parentKey.indexOf('wyndham') !== -1;
  }

  function isBestWesternParentCompanyKey(parentKey) {
    return (
      parentKey.indexOf('best western') !== -1 ||
      parentKey === 'bwh' ||
      parentKey.indexOf('bwh hotels') !== -1
    );
  }

  function isAccorParentCompanyKey(parentKey) {
    return parentKey.indexOf('accor') !== -1;
  }

  function staticPortfolioMatrixForParentKey(parentKey) {
    if (isMarriottParentCompanyKey(parentKey)) return MARRIOTT_PORTFOLIO_TIER_BRANDS;
    if (isChoiceParentCompanyKey(parentKey)) return CHOICE_PORTFOLIO_TIER_BRANDS;
    if (isIhgParentCompanyKey(parentKey)) return IHG_PORTFOLIO_TIER_BRANDS;
    if (isHiltonParentCompanyKey(parentKey)) return HILTON_PORTFOLIO_TIER_BRANDS;
    if (isWyndhamParentCompanyKey(parentKey)) return WYNDHAM_PORTFOLIO_TIER_BRANDS;
    if (isBestWesternParentCompanyKey(parentKey)) return BEST_WESTERN_PORTFOLIO_TIER_BRANDS;
    if (isAccorParentCompanyKey(parentKey)) return ACCOR_PORTFOLIO_TIER_BRANDS;
    return null;
  }

  function portfolioParentKeyForSiblingMatch(parent) {
    var key = normalizePortfolioParentKey(parent);
    // Radisson CALA rows sometimes use Alpha Brand Studios in Basics; siblings are CHI.
    if (key === 'alpha brand studios') return 'choice hotels international';
    return key;
  }

  function ladderTierFallbackLabelsForBrand(brand) {
    var parentKey = portfolioParentKeyForSiblingMatch(brand && brand.parentCompany);
    if (isChoiceParentCompanyKey(parentKey) || parentKey.indexOf('choice hotels') !== -1) {
      return CHOICE_PORTFOLIO_LADDER_TIER_LABELS.slice();
    }
    if (isIhgParentCompanyKey(parentKey)) {
      return IHG_PORTFOLIO_LADDER_TIER_LABELS.slice();
    }
    if (isHiltonParentCompanyKey(parentKey)) {
      return HILTON_PORTFOLIO_LADDER_TIER_LABELS.slice();
    }
    if (isMarriottParentCompanyKey(parentKey)) {
      return MARRIOTT_PORTFOLIO_LADDER_TIER_LABELS.slice();
    }
    if (isWyndhamParentCompanyKey(parentKey)) {
      return WYNDHAM_PORTFOLIO_LADDER_TIER_LABELS.slice();
    }
    if (isBestWesternParentCompanyKey(parentKey)) {
      return BEST_WESTERN_PORTFOLIO_LADDER_TIER_LABELS.slice();
    }
    if (isAccorParentCompanyKey(parentKey)) {
      return ACCOR_PORTFOLIO_LADDER_TIER_LABELS.slice();
    }
    return ladderTierFallbackLabels();
  }

  function portfolioLadderTierForListBrand(b) {
    if (b && typeof b.portfolioLadderTier === 'number' && b.portfolioLadderTier >= 0 && b.portfolioLadderTier <= 3) {
      return b.portfolioLadderTier;
    }
    var fromStatic = staticPortfolioTierIndexForBrand(b);
    if (fromStatic != null) return fromStatic;
    return ladderIndexForScale(b && (b.hotelChainScale || b.chainScale));
  }

  function normalizePortfolioParentKey(parent) {
    return String(parent || '')
      .trim()
      .toLowerCase();
  }

  function explorerBrandListForPortfolio() {
    if (typeof window.getBrandExplorerListBrands === 'function') {
      return window.getBrandExplorerListBrands() || [];
    }
    return [];
  }

  function portfolioSiblingNamesByLadderTier(brand, brandList) {
    var tiers = [[], [], [], []];
    var parentKey = portfolioParentKeyForSiblingMatch(brand && brand.parentCompany);
    if (!parentKey) return tiers;
    if (brandList && brandList.length) {
      var currentId = brand && (brand.id || brand.brandId) ? String(brand.id || brand.brandId) : '';
      var currentNmLive = brand && hasVal(brand.name) ? String(brand.name).trim() : '';
      brandList.forEach(function (b) {
        if (!b) return;
        if (portfolioParentKeyForSiblingMatch(b.parentCompany) !== parentKey) return;
        var nm = hasVal(b.name) ? String(b.name).trim() : '';
        if (!nm) return;
        var bid = b.id != null ? String(b.id) : '';
        if (currentId && bid === currentId) return;
        if (currentNmLive && portfolioBrandNamesReferToSameBrand(nm, currentNmLive)) return;
        tiers[portfolioLadderTierForListBrand(b)].push(nm);
      });
      tiers.forEach(function (names) {
        names.sort(function (a, b) {
          return a.localeCompare(b, undefined, { sensitivity: 'base' });
        });
      });
    }
    if (isIhgParentCompanyKey(parentKey)) {
      for (var ti = 0; ti < 4; ti++) {
        tiers[ti] = IHG_PORTFOLIO_TIER_BRANDS[ti].slice();
      }
    }
    if (isHiltonParentCompanyKey(parentKey)) {
      for (var hj = 0; hj < 4; hj++) {
        tiers[hj] = HILTON_PORTFOLIO_TIER_BRANDS[hj].slice();
      }
    }
    if (isMarriottParentCompanyKey(parentKey)) {
      for (var mj = 0; mj < 4; mj++) {
        tiers[mj] = MARRIOTT_PORTFOLIO_TIER_BRANDS[mj].slice();
      }
    }
    if (isChoiceParentCompanyKey(parentKey)) {
      for (var cj = 0; cj < 4; cj++) {
        tiers[cj] = CHOICE_PORTFOLIO_TIER_BRANDS[cj].slice();
      }
    }
    if (isWyndhamParentCompanyKey(parentKey)) {
      for (var wj = 0; wj < 4; wj++) {
        tiers[wj] = WYNDHAM_PORTFOLIO_TIER_BRANDS[wj].slice();
      }
    }
    if (isBestWesternParentCompanyKey(parentKey)) {
      for (var bj = 0; bj < 4; bj++) {
        tiers[bj] = BEST_WESTERN_PORTFOLIO_TIER_BRANDS[bj].slice();
      }
    }
    if (isAccorParentCompanyKey(parentKey)) {
      for (var aj = 0; aj < 4; aj++) {
        tiers[aj] = ACCOR_PORTFOLIO_TIER_BRANDS[aj].slice();
      }
    }
    var currentNm =
      brand && hasVal(brand.name) ? String(brand.name).trim() : '';
    // Same alias strip for Marriott / Choice / IHG / Hilton / live sibling lists.
    filterCurrentBrandFromPortfolioTier(tiers, currentNm);
    dedupePortfolioTierByBrandAlias(tiers);
    return tiers;
  }

  function portfolioLadderStepLabel(tierNames, fallback, active, brandName) {
    if (active) return hasVal(brandName) ? String(brandName).trim() : fallback;
    if (tierNames && tierNames.length) return tierNames.join(', ');
    return fallback;
  }

  function portfolioContextNarrativeFromBrand(brand) {
    var ctxRow = explorerFirstBlock(brand, 'overview.portfolio_context');
    if (!ctxRow || !hasVal(ctxRow.body)) return '';
    return String(ctxRow.body).trim();
  }

  function portfolioContextSectionHint(brand) {
    var parentKey = normalizePortfolioParentKey(brand && brand.parentCompany);
    var brandNm = hasVal(brand && brand.name) ? String(brand.name) : 'This brand';
    var parentNm = hasVal(brand && brand.parentCompany) ? String(brand.parentCompany) : 'the parent company';
    if (isMarriottParentCompanyKey(parentKey)) {
      return (
        'Dealality owner-planning context — where <strong>' +
        escapeHtml(brandNm) +
        '</strong> sits among illustrative <strong>' +
        escapeHtml(parentNm) +
        '</strong> lifestyle and soft-collection paths (not an official company-validated hierarchy).'
      );
    }
    if (hasVal(brand && brand.parentCompany)) {
      return (
        'Where <strong>' +
        escapeHtml(brandNm) +
        '</strong> sits among <strong>' +
        escapeHtml(parentNm) +
        '</strong> brands by chain scale—lower-scale flags on the left, higher-scale on the right (not a quality ranking).'
      );
    }
    return 'Sibling brands by chain scale on the portfolio spectrum—lower on the left, higher on the right (not a quality ranking).';
  }

  function portfolioContextNarrativeHtml(brand) {
    var narrative = portfolioContextNarrativeFromBrand(brand);
    if (!hasVal(narrative)) return '';
    var parentKey = normalizePortfolioParentKey(brand && brand.parentCompany);
    var heading = isMarriottParentCompanyKey(parentKey)
      ? 'Marriott Soft-Collection Context'
      : 'Portfolio Context';
    return (
      '<div class="portfolio-context-narrative">' +
      '<h3 class="portfolio-context-narrative__title">' +
      escapeHtml(heading) +
      '</h3>' +
      '<div class="portfolio-context-narrative__body">' +
      escapeHtml(narrative).replace(/\n/g, '<br>') +
      '</div></div>'
    );
  }

  function buildPortfolioLadderCellsHtml(brand) {
    var fallbacks = ladderTierFallbackLabelsForBrand(brand);
    var tierNames = portfolioSiblingNamesByLadderTier(brand, explorerBrandListForPortfolio());
    var ladderIdx = portfolioLadderTierIndex(brand);
    var brandName = brand && brand.name ? String(brand.name).trim() : '';
    return fallbacks
      .map(function (fallback, i) {
        var active = i === ladderIdx;
        var label = portfolioLadderStepLabel(tierNames[i], fallback, active, brandName);
        return (
          '<div class="ladder__step' +
          (active ? ' ladder__step--active' : '') +
          '"' +
          (active ? ' aria-current="true"' : '') +
          '>' +
          escapeHtml(label) +
          '</div>'
        );
      })
      .join('');
  }

  function positionBody(text) {
    if (!hasVal(text)) {
      return '<p class="brand-position-card__body"><span class="oe-dd--empty">&nbsp;</span></p>';
    }
    return '<p class="brand-position-card__body">' + escapeHtml(String(text)) + '</p>';
  }

  /** Trusted static HTML inside brand-position-card (education parity). */
  function positionBodyHtml(html) {
    return '<p class="brand-position-card__body">' + (html || '') + '</p>';
  }

  function regionOfferedLine(brand) {
    var r = brand && brand.regionOffered;
    if (Array.isArray(r) && r.length) return r.filter(Boolean).join('; ');
    if (typeof r === 'string' && r.trim()) return r.trim();
    return '';
  }

  function renderAtelierOverview(brand) {
    var disp = resolveFootprintDisplay(brand);
    var fp = disp.fp || brand.footprint || {};
    var fv = fp.formValues || {};
    var footLine = footprintSummaryLine(brand);
    var loyaltyLine = loyaltyStrengthLine(brand);
    var geoFocus = regionOfferedLine(brand);
    var typicalUse = typicalUseCaseFromBrand(brand);
    var relativePositioning = relativePositioningFromBrand(brand);
    var devModel = developmentModelFromBrand(brand);

    var suppressEmptySnapshot = shouldRenderFullProfile(brand);
    var snapshotGrid =
      '<div class="oe-grid-2 oe-grid-2--snapshot">' +
      oeKvBlock(
        'Identity & lineage',
        [
          { k: 'Parent Company', v: brand.parentCompany },
          { k: 'Brand Family', v: brand.brandArchitecture },
          { k: 'Launch Year', v: brand.yearBrandLaunched },
          { k: 'Brand Website', v: brand.brandWebsite }
        ],
        { suppressEmpty: suppressEmptySnapshot }
      ) +
      oeKvBlock(
        'Product & segment',
        [
          { k: 'Segment', v: brand.hotelChainScale },
          { k: 'Affiliation Model', v: brand.brandModelFormat },
          { k: 'Service / Operating Model', v: brand.hotelServiceModel }
        ],
        { suppressEmpty: suppressEmptySnapshot }
      ) +
      oeKvBlock(
        'Scale & geography',
        [
          { k: 'Typical Keys Range', v: typicalKeysRangeFromPortfolio(brand) },
          { k: 'Typical Use Case', v: typicalUse },
          { k: 'Geographic Focus', v: geoFocus }
        ],
        { suppressEmpty: suppressEmptySnapshot }
      ) +
      oeKvBlock(
        'Development & positioning',
        [
          { k: 'Development Model', v: devModel },
          { k: 'Relative Positioning', v: relativePositioning },
          { k: 'Branded Residences', v: brandedResidencesLine(brand) }
        ],
        { suppressEmpty: suppressEmptySnapshot }
      ) +
      '</div>' +
      brandedResidencesHelperHtml(brand);

    var posAudience =
      [brand.targetGuestSegments, brand.guestPsychographics, brand.brandCustomerPromise]
        .map(function (x) {
          return Array.isArray(x) ? x.filter(Boolean).join(', ') : x;
        })
        .filter(hasVal)
        .join(' ');

    var suppressStagingFallbacks = shouldSuppressIncompleteExternalSections(brand);
    var scenarioBodies = suppressStagingFallbacks
      ? []
      : splitBullets(brand.keyBrandDifferentiators).slice(0, 3);
    if (suppressStagingFallbacks) scenarioBodies.length = 0;
    while (!suppressStagingFallbacks && scenarioBodies.length < 3) scenarioBodies.push('');
    var scenarioApiBlocks = [1, 2, 3]
      .map(function (i) {
        return explorerFirstBlock(brand, 'overview.scenario.' + i);
      })
      .filter(Boolean);
    var scenarioCards = scenarioApiBlocks
      .map(function (srowOv, idx) {
        var body = scenarioBodies[idx] || '';
        var scen3Para = explorerParagraphs(brand, 'overview.scenarios', 3);
        var sj;
        for (sj = 0; sj < scen3Para.length; sj++) {
          if (hasVal(scen3Para[sj])) scenarioBodies[sj] = scen3Para[sj];
        }
        if (srowOv && hasVal(srowOv.body)) body = String(srowOv.body).trim();
        var title = srowOv && hasVal(srowOv.title) ? String(srowOv.title).trim() : '';
        if (!hasVal(title)) title = 'Scenario ' + (idx + 1);
        var imgUrl =
          srowOv && hasVal(srowOv.imageUrl) ? String(srowOv.imageUrl).trim() : '';
        var visual = hasVal(imgUrl)
          ? '<div class="scenario-card__visual"><img src="' +
            escapeHtml(imgUrl) +
            '" alt=""' +
            exportPdfImgAttrs() +
            '/></div>'
          : '<div class="scenario-card__visual scenario-card__visual--empty" aria-hidden="true">Image</div>';
        return (
          '<div class="scenario-card scenario-card--visual">' +
          visual +
          '<div class="scenario-card__body"><h4>' +
          escapeHtml(title) +
          '</h4><p>' +
          (hasVal(body) ? escapeHtml(body) : '&nbsp;') +
          '</p></div></div>'
        );
      })
      .join('');
    if (!hasVal(scenarioCards)) {
      scenarioCards = suppressStagingFallbacks
        ? ''
        : '<p class="oe-dd--empty">Scenario cards will appear when overview.scenario presentation rows are visible in Brand Explorer.</p>';
    }

    var whySlotMerged = explorerMergedBody(brand, 'overview.why_value');
    var whyLines = suppressStagingFallbacks
      ? []
      : hasVal(whySlotMerged)
        ? splitBullets(whySlotMerged)
        : splitBullets(brand.brandValueProposition || brand.keyBrandDifferentiators);
    if (!suppressStagingFallbacks) {
      while (whyLines.length < 5) whyLines.push('');
    }
    var whyList = whyLines
      .slice(0, 5)
      .map(function (line) {
        return '<li>' + (hasVal(line) ? escapeHtml(fmtCell(line)) : '&nbsp;') + '</li>';
      })
      .join('');

    var diffIdentitySlot = explorerMergedBody(brand, 'overview.differentiators.identity');
    var diffCommercialSlot = explorerMergedBody(brand, 'overview.differentiators.commercial');
    var leftDiff;
    var rightDiff;
    if (hasVal(diffIdentitySlot) || hasVal(diffCommercialSlot)) {
      leftDiff = splitBullets(diffIdentitySlot);
      rightDiff = splitBullets(diffCommercialSlot);
    } else if (suppressStagingFallbacks) {
      leftDiff = [];
      rightDiff = [];
    } else {
      var diffAll = splitBullets(brand.keyBrandDifferentiators);
      var mid = Math.ceil(diffAll.length / 2) || 0;
      leftDiff = diffAll.slice(0, mid);
      rightDiff = diffAll.slice(mid);
    }
    if (!suppressStagingFallbacks) {
      while (leftDiff.length < 4) leftDiff.push('');
      while (rightDiff.length < 4) rightDiff.push('');
    }
    function diffUl(arr) {
      return (
        '<ul>' +
        arr
          .slice(0, 4)
          .map(function (x) {
            return '<li>' + (hasVal(x) ? escapeHtml(fmtCell(x)) : '&nbsp;') + '</li>';
          })
          .join('') +
        '</ul>'
      );
    }

    var pillarParts = suppressStagingFallbacks ? [] : splitBullets(brand.brandPillars);
    var bestTitles = ['Conversion & Repositioning', 'Blended-Demand Markets', 'Owner Speed-to-Flag'];
    var bestCards = suppressStagingFallbacks
      ? ''
      : bestTitles
      .map(function (t, i) {
        var sk = 'overview.bestAt.' + (i + 1);
        var row = explorerFirstBlock(brand, sk);
        var title = t;
        if (row && hasVal(row.title)) title = String(row.title).trim();
        var slotBody = explorerMergedBody(brand, sk);
        var body;
        if (hasVal(slotBody)) {
          body = String(slotBody)
            .trim()
            .split(/\n+/)
            .map(function (s) {
              return s.trim();
            })
            .filter(Boolean)
            .join(' ');
        } else {
          body = pillarParts[i] || '';
        }
        return (
          '<div class="oe-card"><h3>' +
          escapeHtml(title) +
          '</h3><p>' +
          (hasVal(body) ? escapeHtml(body) : '&nbsp;') +
          '</p></div>'
        );
      })
      .join('');

    function linesFromText(txt, max) {
      if (!hasVal(txt)) {
        var empty = [];
        for (var z = 0; z < max; z++) empty.push('');
        return empty;
      }
      // Treat literal \n / \r\n from copy-paste (e.g. chat → Airtable) like real line breaks
      var normalized = String(txt)
        .replace(/\r\n/g, '\n')
        .replace(/\r/g, '\n')
        .replace(/\\r\\n/g, '\n')
        .replace(/\\n/g, '\n');
      var lines = normalized
        .split(/\n+/)
        .map(function (s) {
          return s.trim();
        })
        .filter(Boolean);
      while (lines.length < max) lines.push('');
      return lines.slice(0, max);
    }

    var ownerOut = suppressStagingFallbacks
      ? []
      : linesFromText(brand.brandValueProposition, 4).filter(hasVal).slice(0, 4);
    var exOwner = explorerMergedBody(brand, 'overview.owner_experience');
    var ownerEx = suppressStagingFallbacks
      ? []
      : linesFromText(hasVal(exOwner) ? exOwner : brand.companyHistory, 4).filter(hasVal).slice(0, 4);

    var proofOpSlot = explorerMergedBody(brand, 'overview.proof_operator');
    var proofFallbackBodies = [
      footLine,
      pipelineLineForProof(fp),
      [brand.brandModelFormat, brand.brandDevelopmentStage].filter(hasVal).join(' · '),
      hasVal(fv.specificMarkets) ? fv.specificMarkets : fp.priorityCities || '',
      loyaltyLine,
      hasVal(proofOpSlot) ? proofOpSlot : brand.brandValueProposition || ''
    ];
    var proofFallbackHeads = [
      'Global Open Footprint',
      'Pipeline Depth',
      'Conversion-Led Growth',
      'Multi-Region Relevance',
      loyaltyProofHeadline(brand),
      'Operator-Enabled Execution'
    ];
    var proofRowsInApi = [1, 2, 3, 4, 5, 6]
      .map(function (i) {
        return explorerFirstBlock(brand, 'overview.proof.' + i);
      })
      .filter(Boolean);
    var useProofFallbacks = !suppressStagingFallbacks && proofRowsInApi.length === 0;
    var proofGrid = suppressStagingFallbacks
      ? ''
      : (useProofFallbacks ? proofFallbackHeads : proofRowsInApi)
      .map(function (fallbackHead, i) {
        var sk = useProofFallbacks
          ? 'overview.proof.' + (i + 1)
          : proofRowsInApi[i].slotKey || 'overview.proof.' + (i + 1);
        var row = useProofFallbacks ? null : proofRowsInApi[i];
        var slotBody = explorerMergedBody(brand, sk);
        var title = useProofFallbacks ? fallbackHead : '';
        if (row && hasVal(row.title)) title = String(row.title).trim();
        var body;
        if (hasVal(slotBody)) {
          body = String(slotBody).trim();
        } else if (row && hasVal(row.body)) {
          body = String(row.body).trim();
        } else if (useProofFallbacks) {
          body = proofFallbackBodies[i];
        } else {
          body = '';
        }
        var empty = !hasVal(body);
        var bodyText = empty ? '&nbsp;' : escapeHtml(fmtCell(body));
        if (!empty && bodyText.length > 420) bodyText = bodyText.slice(0, 417) + '…';
        return (
          '<article class="proof-point-card"><div class="proof-point-card__icon">◇</div><h3 class="proof-point-card__headline">' +
          escapeHtml(title) +
          '</h3><p class="proof-point-card__support' +
          (empty ? ' oe-dd--empty' : '') +
          '">' +
          bodyText +
          '</p></article>'
        );
      })
      .join('');

    var themeLabels = [
      'Portfolio Footprint',
      'Recent Applications',
      'Repositioning Examples',
      'Operator-Enabled Scenarios',
      'Market Relevance',
      proofThemeMaterialsLabel(brand)
    ];
    var themeChips = themeLabels
      .map(function (t) {
        return '<li>' + escapeHtml(t) + '</li>';
      })
      .join('');

    var featTitle = 'Featured Application · Conversion Example';
    var featLead = brand.brandTaglineMotto || '';
    var featBody = brand.brandPositioning || brand.brandCustomerPromise || '';
    var featSlot = explorerFirstBlock(brand, 'overview.featured_application');
    var featSub = '';
    if (featSlot && (hasVal(featSlot.body) || hasVal(featSlot.title))) {
      var slotTitle = hasVal(featSlot.title) ? String(featSlot.title).trim() : '';
      var slotBody = hasVal(featSlot.body) ? String(featSlot.body).trim() : '';
      featSub =
        (slotTitle ? '<strong>' + escapeHtml(slotTitle) + '</strong>' : '') +
        (slotTitle && slotBody ? ' — ' : '') +
        (slotBody ? escapeHtml(slotBody) : '');
    } else if (!hasVal(featBody) && !hasVal(featLead)) {
      featSub = '&nbsp;';
    } else {
      featSub =
        (hasVal(featLead) ? '<strong>' + escapeHtml(String(featLead)) + '</strong> — ' : '') +
        (hasVal(featBody)
          ? escapeHtml(String(featBody).slice(0, 220)) + (String(featBody).length > 220 ? '…' : '')
          : '');
    }
    var pillarsRaw = suppressStagingFallbacks ? '' : brand.brandPillars || brand.keyBrandDifferentiators;
    var tagSrc = splitBullets(pillarsRaw).filter(hasVal);
    if (!tagSrc.length && hasVal(pillarsRaw)) {
      tagSrc = chipListFromCsv(pillarsRaw);
    }
    tagSrc = tagSrc.slice(0, 6);
    var featTags = tagSrc.length
      ? tagSrc
          .map(function (t) {
            return '<span class="tag-chip">' + escapeHtml(String(t)) + '</span>';
          })
          .join('')
      : '';

    var ladderCells = buildPortfolioLadderCellsHtml(brand);
    var portfolioContextNarrative = portfolioContextNarrativeHtml(brand);

    var hasOpenings = explorerBlocksForSlot(brand, FOOTPRINT_OPENINGS_SLOT).length > 0;
    var openingsJumpBtn =
      '<button type="button" class="btn btn--primary"' +
      (hasOpenings
        ? ' data-be-jump-atelier-tab="atelier-footprint" title="Open Footprint &amp; Growth — Recent openings"'
        : ' disabled title="Add Brand Explorer Presentation rows with Slot Key footprint.openings"') +
      '>View Recent Openings</button>';

    var valueSectionsHtml = suppressStagingFallbacks
      ? profileInPreparationSectionHtml()
      : '<section class="oe-section">' +
        '<h2 class="oe-section-title">Where This Brand Creates the Most Value</h2>' +
        '<div class="scenario-card-grid">' +
        scenarioCards +
        '</div>' +
        (hasVal(whyList)
          ? '<div class="oe-cluster"><h3>Why Value Is Strongest in These Scenarios</h3><ul>' + whyList + '</ul></div>'
          : '') +
        '</section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Key Differentiators</h2>' +
        '<div class="oe-grid-2">' +
        '<div class="oe-cluster"><h3>Experience &amp; Identity</h3>' +
        diffUl(leftDiff) +
        '</div>' +
        '<div class="oe-cluster"><h3>Commercial &amp; Distribution</h3>' +
        diffUl(rightDiff) +
        '</div></div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">What This Brand Is Best At</h2>' +
        '<div class="oe-grid-3">' +
        bestCards +
        '</div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Owner Value Snapshot</h2>' +
        '<div class="oe-grid-2">' +
        '<div class="oe-cluster"><h3>Owner Outcomes</h3><ul>' +
        ownerOut
          .map(function (x) {
            return '<li>' + escapeHtml(x) + '</li>';
          })
          .join('') +
        '</ul></div>' +
        '<div class="oe-cluster"><h3>Owner Experience</h3><ul>' +
        ownerEx
          .map(function (x) {
            return '<li>' + escapeHtml(x) + '</li>';
          })
          .join('') +
        '</ul></div></div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Proof Points</h2>' +
        '<p class="oe-section-hint">' + escapeHtml(proofPointsSectionHint(brand)) + '</p>' +
        '<p class="proof-meta-line">Directional proof only—no confidential performance metrics, fee terms, or undisclosed pipeline detail.</p>' +
        '<div class="evidence-row"><span class="evidence-row__intro">Themes Covered by the Proof Points Below</span>' +
        '<ul class="evidence-chips" role="list">' +
        themeChips +
        '</ul></div>' +
        '<div class="proof-points-grid">' +
        proofGrid +
        '</div>' +
        '<div class="featured-case-preview">' +
        '<div class="featured-case-preview__text"><strong>' +
        escapeHtml(featTitle) +
        '</strong><span class="featured-case-preview__sub">' +
        featSub +
        '</span><div class="tag-chip-row">' +
        featTags +
        '</div></div>' +
        openingsJumpBtn +
        '</div></section>';

    return (
      '<div class="be-atelier-oe">' +
      '<section class="oe-section">' +
      '<h2 class="oe-section-title">Brand Snapshot</h2>' +
      '<p class="oe-section-hint" style="margin-bottom:12px">Public reference: where corporate disclosures exist, they may name the flag and publish portfolio-scale figures (open hotels/rooms, pipeline, countries) and loyalty program scale. Regional splits, case studies, and tables on this page are <strong style="color:var(--text,#fff);font-weight:600">illustrative</strong> for this view—not audited financials or property-level performance.</p>' +
      snapshotGrid +
      '</section>' +
      '<section class="oe-section">' +
      '<h2 class="oe-section-title">Brand Positioning</h2>' +
      '<div class="brand-positioning__stack">' +
      '<div class="brand-position-card"><h3 class="brand-position-card__label">Positioning</h3>' +
      positionBody(brand.brandPositioning) +
      '</div>' +
      '<div class="brand-position-card"><h3 class="brand-position-card__label">Audience</h3>' +
      positionBody(posAudience) +
      '</div></div></section>' +
      valueSectionsHtml +
      '<section class="oe-section">' +
      '<h2 class="oe-section-title">Portfolio Context</h2>' +
      portfolioContextNarrative +
      '<p class="oe-section-hint">' +
      portfolioContextSectionHint(brand) +
      '</p>' +
      '<div class="ladder-axis-labels" aria-hidden="true"><span>← Lower On the Portfolio Spectrum</span><span>Higher On the Portfolio Spectrum →</span></div>' +
      '<div class="ladder" role="group" aria-label="Portfolio tiers relative to sibling brands, lower to higher on the company spectrum">' +
      ladderCells +
      '</div></section>' +
      '</div>'
    );
  }

  function wrapOe(inner) {
    return '<div class="be-atelier-oe">' + inner + '</div>';
  }

  function explorerDetailCard(label, bodyText, opts) {
    opts = opts || {};
    if (!hasVal(bodyText)) {
      if (opts.suppressEmpty) return '';
      return (
        '<div class="explorer-detail-card"><h3 class="explorer-detail-card__label">' +
        escapeHtml(label) +
        '</h3><p class="explorer-detail-card__body oe-dd--empty">&nbsp;</p></div>'
      );
    }
    return (
      '<div class="explorer-detail-card"><h3 class="explorer-detail-card__label">' +
      escapeHtml(label) +
      '</h3><p class="explorer-detail-card__body">' +
      escapeHtml(fmtCell(bodyText)).replace(/\n/g, '<br>') +
      '</p></div>'
    );
  }

  function explorerDetailCardMultiline(label, bodyText) {
    if (!hasVal(bodyText)) return explorerDetailCard(label, '');
    var paras = String(bodyText)
      .split(/\n\n+/)
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean);
    if (paras.length <= 1) return explorerDetailCard(label, bodyText);
    var body = paras
      .map(function (p) {
        return (
          '<p class="explorer-detail-card__body">' +
          escapeHtml(fmtCell(p)).replace(/\n/g, '<br>') +
          '</p>'
        );
      })
      .join('');
    return (
      '<div class="explorer-detail-card"><h3 class="explorer-detail-card__label">' +
      escapeHtml(label) +
      '</h3>' +
      body +
      '</div>'
    );
  }

  function scenarioDetailCard(title, body) {
    var inner = hasVal(body) ? escapeHtml(fmtCell(body)) : '&nbsp;';
    return (
      '<div class="scenario-card scenario-card--detail">' +
      '<h3 class="explorer-detail-card__label">' +
      escapeHtml(title) +
      '</h3>' +
      '<p class="explorer-detail-card__body">' +
      inner +
      '</p></div>'
    );
  }

  function pillSelect(labels) {
    return (
      '<div class="pill-select">' +
      labels
        .map(function (l) {
          return '<span>' + escapeHtml(l) + '</span>';
        })
        .join('') +
      '</div>'
    );
  }

  function timelinePhase(strong, spanDetail, opts) {
    opts = opts || {};
    if (opts.suppressEmpty && !hasVal(spanDetail)) return '';
    return (
      '<div class="timeline__phase"><strong>' +
      escapeHtml(strong) +
      '</strong><span>' +
      (hasVal(spanDetail) ? escapeHtml(fmtCell(spanDetail)) : '&nbsp;') +
      '</span></div>'
    );
  }

  function kpiCard(label, value, opts) {
    opts = opts || {};
    if (!hasVal(value)) {
      // Public full profiles must not render empty KPI shells (empty-component gate).
      if (opts.suppressEmpty) return '';
      return (
        '<div class="brand-markets-kpi__card">' +
        '<div class="brand-markets-kpi__label">' +
        escapeHtml(label) +
        '</div>' +
        '<div class="brand-markets-kpi__value oe-dd--empty">&nbsp;</div></div>'
      );
    }
    return (
      '<div class="brand-markets-kpi__card">' +
      '<div class="brand-markets-kpi__label">' +
      escapeHtml(label) +
      '</div>' +
      '<div class="brand-markets-kpi__value">' +
      escapeHtml(fmtCell(value)) +
      '</div></div>'
    );
  }

  function presenceIntelCard(label, value, opts) {
    opts = opts || {};
    if (!hasVal(value)) {
      if (opts.suppressEmpty) return '';
      return (
        '<div class="presence-intel-card">' +
        '<div class="presence-intel-card__label">' +
        escapeHtml(label) +
        '</div><div class="presence-intel-card__value oe-dd--empty">&nbsp;</div></div>'
      );
    }
    return (
      '<div class="presence-intel-card">' +
      '<div class="presence-intel-card__label">' +
      escapeHtml(label) +
      '</div><div class="presence-intel-card__value">' +
      escapeHtml(fmtCell(value)) +
      '</div></div>'
    );
  }

  function presenceIntelFootprintMetric(label, hotels, rooms) {
    function metricBlock(count, unit) {
      var n = Number(count);
      // Treat missing OR literal 0 as unavailable — never render "0 hotels/rooms" publicly.
      var show = hasVal(count) && Number.isFinite(n) && n > 0;
      if (!show) return '';
      return (
        '<div class="presence-intel-card__metric">' +
        '<span class="presence-intel-card__metric-value">' +
        escapeHtml(fmtNum(count)) +
        '</span>' +
        '<span class="presence-intel-card__metric-unit">' +
        escapeHtml(unit) +
        '</span></div>'
      );
    }
    var hotelsBlock = metricBlock(hotels, 'hotels');
    var roomsBlock = metricBlock(rooms, 'rooms');
    if (!hotelsBlock && !roomsBlock) return '';
    return (
      '<div class="presence-intel-card presence-intel-card--footprint-metrics">' +
      '<div class="presence-intel-card__label">' +
      escapeHtml(label) +
      '</div>' +
      '<div class="presence-intel-card__metrics" aria-label="' +
      escapeHtml(label + ' hotels and rooms') +
      '">' +
      hotelsBlock +
      roomsBlock +
      '</div></div>'
    );
  }

  function demandCell(label, statusText) {
    var pill = hasVal(statusText)
      ? '<span class="status-pill">' + escapeHtml(statusText) + '</span>'
      : '<span class="status-pill status-pill--empty">&nbsp;</span>';
    return '<div class="demand-cell"><strong>' + escapeHtml(label) + '</strong> ' + pill + '</div>';
  }

  var COMMERCIAL_LEVER_KEYS = [
    'distribution',
    'revenue_management',
    'digital_marketing',
    'corporate_group',
    'leisure_destination',
    'international',
    'sales_catering',
    'reputation_qa',
    'data_analytics'
  ];

  function parseCommercialLeverBody(body) {
    var raw = body == null ? '' : String(body).trim();
    if (!raw) return { mechanism: '', projectImpact: '' };
    var parts = raw.split(/\n\n(?:Project impact|Owner lens):\s*/i);
    if (parts.length >= 2) {
      return {
        mechanism: parts[0].trim(),
        projectImpact: parts.slice(1).join('\n\nProject impact: ').trim()
      };
    }
    return { mechanism: raw, projectImpact: '' };
  }

  function commercialLeverFromSlot(brand, slotKey, defTitle, defMechanism, defProjectImpact) {
    var block = explorerFirstBlock(brand, slotKey);
    var title = defTitle;
    var mechanism = defMechanism;
    var projectImpact = defProjectImpact;
    if (block) {
      if (hasVal(block.title)) title = String(block.title).trim();
      if (hasVal(block.body)) {
        var parsed = parseCommercialLeverBody(block.body);
        if (hasVal(parsed.mechanism)) mechanism = parsed.mechanism;
        if (hasVal(parsed.projectImpact)) projectImpact = parsed.projectImpact;
      }
    }
    return commercialStrengthCardHtml(
      title,
      hasVal(mechanism) ? escapeHtml(mechanism) : '',
      hasVal(projectImpact) ? escapeHtml(projectImpact) : ''
    );
  }

  function commercialKpiFromSlot(brand, slotKey, defLabel, defVal) {
    var r = explorerFirstBlock(brand, slotKey);
    var lbl = defLabel;
    var val = defVal;
    if (r) {
      if (hasVal(r.title)) lbl = String(r.title).trim();
      if (hasVal(r.body)) val = String(r.body).trim();
    }
    return kpiCard(lbl, val);
  }

  /** Trusted static HTML — owner-facing commercial lever cards (no sales-script labels). */
  function commercialStrengthCardHtml(h4, htmlMechanism, htmlOwnerLens) {
    var ownerBlock = htmlOwnerLens
      ? '<p><span class="scenario-card__label">Project impact</span>' + htmlOwnerLens + '</p>'
      : '';
    return (
      '<div class="scenario-card"><h4>' +
      escapeHtml(h4) +
      '</h4><p>' +
      (htmlMechanism || '&nbsp;') +
      '</p>' +
      ownerBlock +
      '</div>'
    );
  }

  function materialsFileBodyLines(body) {
    if (!hasVal(body)) return [];
    return String(body)
      .split(/\r?\n/)
      .map(function (line) {
        return line.trim();
      })
      .filter(Boolean);
  }

  function materialsFileMetaFromBody(body) {
    return materialsFileBodyLines(body)
      .filter(function (line) {
        return !isSafeHttpUrl(line) && !/^badge\s*:/i.test(line);
      })
      .join(' · ')
      .trim();
  }

  function materialsFileBadgeFromBody(body) {
    var badgeLine = materialsFileBodyLines(body).find(function (line) {
      return /^badge\s*:/i.test(line);
    });
    if (!badgeLine) return '';
    return badgeLine.replace(/^badge\s*:\s*/i, '').trim();
  }

  function fileCard(icon, title, meta, hrefOpt, badge) {
    var href = hrefOpt && isSafeHttpUrl(String(hrefOpt)) ? String(hrefOpt).trim() : '';
    var badgeLabel = hasVal(badge) ? String(badge).trim() : 'Unverified by Brand';
    var metaHtml = hasVal(meta)
      ? '<div class="file-card__meta">' + escapeHtml(meta) + '</div>'
      : '<div class="file-card__meta oe-dd--empty">&nbsp;</div>';
    var actions =
      '<div style="margin-top:10px">' +
      (href
        ? '<a class="btn" href="' +
          escapeHtml(href) +
          '" target="_blank" rel="noopener noreferrer">View</a> <a class="btn" href="' +
          escapeHtml(href) +
          '" target="_blank" rel="noopener noreferrer" download>Download</a>'
        : '<button type="button" class="btn" disabled>View</button> <button type="button" class="btn" disabled>Download</button>') +
      '</div>';
    return (
      '<div class="file-card">' +
      '<div class="file-card__icon">' +
      escapeHtml(icon) +
      '</div><div>' +
      '<p class="file-card__title">' +
      (hasVal(title) ? escapeHtml(title) : '&nbsp;') +
      '</p>' +
      metaHtml +
      '<span class="file-card__badge">' +
      escapeHtml(badgeLabel) +
      '</span>' +
      actions +
      '</div></div>'
    );
  }

  function renderValueToOwners(brand) {
    var VALUE_SCENARIOS = [
      'Independent Reflag',
      'Tired Upscale Asset',
      'Markets With Strong Brand Presence',
      'Third-Party Operator–Led'
    ];
    var VALUE_PILLS = [
      'Conversion-Minded Owner',
      'Institutional / Fund Owner',
      'Regional Multi-Asset Sponsor',
      'Independent Seeking Network Scale',
      'Third-Party Operator Partnership',
      'Capital Cycle Repositioning'
    ];
    var TIMELINE = [
      'Phase 1 · Evaluation',
      'Phase 2 · Conversion Design',
      'Phase 3 · Pre-Opening',
      'Phase 4 · Opening',
      'Phase 5 · Ramp-Up',
      'Phase 6 · Ongoing'
    ];
    var overviewBits = [brand.brandValueProposition, brand.companyHistory, brand.brandCustomerPromise]
      .filter(hasVal)
      .map(function (x) {
        return String(x).trim();
      });
    var slotVo = explorerMergedBody(brand, 'valueOwners.overview');
    var overviewBody = hasVal(slotVo) ? slotVo : overviewBits.join('\n\n');
    // Prefer dedicated valueOwners.scenario.1–4 Title/Body cards (Ascend gold).
    // Do not mix brandValueProposition bullets into empty siblings when any
    // dedicated scenario body exists — that caused one long card + blanks.
    var scenBodies = ['', '', '', ''];
    var si;
    var hasDedicatedScenarioBodies = false;
    for (si = 0; si < 4; si++) {
      var srowProbe = explorerFirstBlock(brand, 'valueOwners.scenario.' + (si + 1));
      if (srowProbe && hasVal(srowProbe.body)) hasDedicatedScenarioBodies = true;
    }
    if (!hasDedicatedScenarioBodies) {
      scenBodies = splitBullets(brand.brandValueProposition || brand.keyBrandDifferentiators);
      while (scenBodies.length < 4) scenBodies.push('');
      var scenFromSlot = explorerParagraphs(brand, 'valueOwners.scenarios', 4);
      for (si = 0; si < scenFromSlot.length; si++) {
        if (hasVal(scenFromSlot[si])) scenBodies[si] = scenFromSlot[si];
      }
    }
    for (si = 0; si < 4; si++) {
      var srowV = explorerFirstBlock(brand, 'valueOwners.scenario.' + (si + 1));
      if (srowV && hasVal(srowV.body)) scenBodies[si] = String(srowV.body).trim();
    }
    var scenGrid = VALUE_SCENARIOS.map(function (title, i) {
      var srowT = explorerFirstBlock(brand, 'valueOwners.scenario.' + (i + 1));
      var cardTitle = srowT && hasVal(srowT.title) ? String(srowT.title).trim() : title;
      return scenarioDetailCard(cardTitle, scenBodies[i]);
    }).join('');
    var watchSlot = explorerMergedBody(brand, 'valueOwners.watchouts');
    var watchLines = hasVal(watchSlot)
      ? splitBullets(watchSlot)
      : splitBullets(brand.keyBrandDifferentiators || brand.brandValueProposition);
    watchLines = watchLines.filter(hasVal).slice(0, 5);
    var watchUl = watchLines
      .map(function (line) {
        return '<li>' + escapeHtml(line) + '</li>';
      })
      .join('');
    var timelineHtml = TIMELINE.map(function (ph, idx) {
      var slotL = explorerFirstBlock(brand, 'valueOwners.lifecycle.' + (idx + 1));
      var label = ph;
      var det = '';
      if (slotL) {
        if (hasVal(slotL.title)) label = String(slotL.title).trim();
        if (hasVal(slotL.body)) det = String(slotL.body).trim();
      }
      return timelinePhase(label, det, { suppressEmpty: shouldRenderFullProfile(brand) });
    }).join('');
    return wrapOe(
      '<section class="oe-section">' +
        '<h2 class="oe-section-title">What the Owner Is Really Buying</h2>' +
        '<p class="oe-section-hint">Owner Education</p>' +
        explorerDetailCard('Overview', overviewBody) +
        '</section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Value Creation Scenarios</h2>' +
        '<div class="scenario-card-grid scenario-card-grid--owner-value" style="grid-template-columns:repeat(2,1fr)">' +
        scenGrid +
        '</div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Ownership Profiles That Benefit Most</h2>' +
        pillSelect(VALUE_PILLS) +
        '</section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Support Across the Lifecycle</h2>' +
        '<div class="timeline">' +
        timelineHtml +
        '</div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Key Watchouts</h2>' +
        '<p class="oe-section-hint">Considerations, Not Alarms</p>' +
        '<div class="explorer-detail-card">' +
        '<h3 class="explorer-detail-card__label">Considerations</h3>' +
        '<ul class="explorer-detail-card__list bullet-list">' +
        watchUl +
        '</ul></div></section>'
    );
  }

  var FLEXIBILITY_INDICATOR_DEFS = [
    { label: 'Design Flexibility', slot: 'operations.flexibility.design' },
    { label: 'Conversion Friendliness', slot: 'operations.flexibility.conversion' },
    { label: 'Localization Flexibility', slot: 'operations.flexibility.localization' },
    { label: 'Operational Rigidity', slot: 'operations.flexibility.operational_rigidity' },
    { label: 'PIP Sensitivity', slot: 'operations.flexibility.pip' },
    { label: 'Prototype Dependence', slot: 'operations.flexibility.prototype' }
  ];

  /** Maps Airtable Body → distinct bar class (see docs/brand-explorer-presentation-slots.md). */
  function flexibilityFillClass(tag) {
    var s = String(tag || '')
      .toLowerCase()
      .trim()
      .replace(/[–—]/g, '-');
    if (!s || /^(n\/?a|na|none|unknown|not\s+applicable|tbd|-)$/.test(s)) return 'empty';
    if (/^very\s*high$/.test(s)) return 'very-high';
    if (/^high$/.test(s)) return 'high';
    if (/^medium$/.test(s)) return 'medium';
    if (/^moderate$/.test(s)) return 'moderate';
    if (/^minimal$/.test(s)) return 'minimal';
    if (/^low$/.test(s)) return 'low';
    var num = s.match(/^([1-6])(?:\s*\/\s*6)?$/);
    if (num) {
      return (
        ['empty', 'minimal', 'low', 'moderate', 'medium', 'high', 'very-high'][
          parseInt(num[1], 10)
        ] || 'moderate'
      );
    }
    if (/\bvery\s*high\b/.test(s)) return 'very-high';
    if (/\b(very\s*low|minimal|negligible)\b/.test(s)) return 'minimal';
    if (/\b(exceptional|maximum|extensive)\b/.test(s)) return 'very-high';
    if (/\b(high|strong|significant|substantial|robust|elevated|considerable)\b/.test(s))
      return 'high';
    if (/\b(low|limited|light|minor|weak|restricted|tight)\b/.test(s)) return 'low';
    if (/\bmedium\b/.test(s)) return 'medium';
    if (/\b(moderate|mid|average|fair|balanced|typical|some)\b/.test(s)) return 'moderate';
    if (/\b(low[- ]moderate|moderate[- ]low)\b/.test(s)) return 'low';
    if (/\b(moderate[- ]high|high[- ]moderate)\b/.test(s)) return 'high';
    return 'moderate';
  }

  function flexibilityTagFromBrand(brand, slotKey) {
    var row = explorerFirstBlock(brand, slotKey);
    if (!row) return '';
    if (hasVal(row.body)) return String(row.body).trim();
    if (hasVal(row.title)) return String(row.title).trim();
    return '';
  }

  /**
   * Display tag must match Ascend/Curio: canonical level only (High, Moderate, …).
   * Strips compound "Very high\nprose…" / "High Independent…" bodies.
   */
  function flexibilityDisplayTag(raw) {
    var s = String(raw || '')
      .trim()
      .replace(/[–—]/g, '-');
    if (!s) return '';
    var firstLine = s.split(/\r?\n/)[0].trim();
    var candidates = [firstLine];
    if (firstLine !== s) candidates.push(s);
    for (var i = 0; i < candidates.length; i++) {
      var c = candidates[i];
      var lower = c.toLowerCase();
      if (
        /^(moderate\s+to\s+high|high\s+to\s+moderate|high-moderate|medium-high|medium\s+to\s+high)\b/.test(
          lower
        )
      ) {
        return 'High';
      }
      if (
        /^(low\s+to\s+moderate|moderate\s+to\s+low|low-moderate|low\s+to\s+medium|low-medium)\b/.test(
          lower
        )
      ) {
        return 'Low';
      }
      if (/^very\s*high\b/.test(lower)) return 'Very high';
      if (/^very\s*low\b/.test(lower)) return 'Minimal';
      if (/^minimal\b/.test(lower)) return 'Minimal';
      if (/^moderate\b/.test(lower)) return 'Moderate';
      if (/^medium\b/.test(lower)) return 'Medium';
      if (/^high\b/.test(lower)) return 'High';
      if (/^low\b/.test(lower)) return 'Low';
    }
    var fill = flexibilityFillClass(s);
    var FILL_LABEL = {
      'very-high': 'Very high',
      high: 'High',
      medium: 'Medium',
      moderate: 'Moderate',
      low: 'Low',
      minimal: 'Minimal',
      empty: ''
    };
    return FILL_LABEL[fill] || firstLine;
  }

  function flexibilityIndicatorsHtml(brand) {
    var suppressEmpty = shouldRenderFullProfile(brand);
    return FLEXIBILITY_INDICATOR_DEFS.map(function (d) {
      var raw = flexibilityTagFromBrand(brand, d.slot);
      if (!hasVal(raw)) {
        if (suppressEmpty) return '';
        return (
          '<div class="indicator-bar"><span class="indicator-bar__label">' +
          escapeHtml(d.label) +
          '</span><div class="indicator-bar__track"><div class="indicator-bar__fill indicator-bar__fill--empty"></div></div><span class="indicator-bar__tag oe-dd--empty">&nbsp;</span></div>'
        );
      }
      var tag = flexibilityDisplayTag(raw);
      var fill = flexibilityFillClass(tag || raw);
      return (
        '<div class="indicator-bar"><span class="indicator-bar__label">' +
        escapeHtml(d.label) +
        '</span><div class="indicator-bar__track"><div class="indicator-bar__fill indicator-bar__fill--' +
        fill +
        '"></div></div><span class="indicator-bar__tag">' +
        escapeHtml(tag) +
        '</span></div>'
      );
    }).join('');
  }

  function operatorCompatTagsFromBrand(brand) {
    var tags = [];
    explorerBlocksForSlot(brand, 'operations.operator_compat.tags').forEach(function (r) {
      if (hasVal(r.body)) tags = tags.concat(splitBullets(r.body));
      else if (hasVal(r.title)) tags.push(String(r.title).trim());
    });
    return tags;
  }

  function operatorCompatTagRowHtml(brand) {
    var tags = operatorCompatTagsFromBrand(brand);
    if (!tags.length) {
      if (shouldRenderFullProfile(brand)) return '';
      return (
        '<div class="tag-chip-row" style="margin:0">' +
        '<span class="tag-chip oe-dd--empty">&nbsp;</span></div>'
      );
    }
    return (
      '<div class="tag-chip-row" style="margin:0">' +
      tags
        .map(function (t) {
          return '<span class="tag-chip">' + escapeHtml(t) + '</span>';
        })
        .join('') +
      '</div>'
    );
  }

  function renderOperationsStandards(brand) {
    var std = brand.brandStandards || {};
    var op = brand.operationalSupport || {};
    var suppressEmpty = shouldRenderFullProfile(brand);
    var grid =
      '<div class="oe-grid-2 oe-grid-2--operating-model">' +
      oeKvBlock(
        'Structure & ownership',
        [
          { k: 'Primary Model', v: explorerPresentationLine(brand, 'operations.model.primary_model') },
          { k: 'Management Option', v: explorerPresentationLine(brand, 'operations.model.management_option') },
          { k: 'Typical Ownership Structure', v: explorerPresentationLine(brand, 'operations.model.typical_ownership') }
        ],
        { suppressEmpty: suppressEmpty }
      ) +
      oeKvBlock(
        'Brand involvement & systems',
        [
          { k: 'Brand Involvement', v: explorerPresentationLine(brand, 'operations.model.brand_involvement') },
          { k: 'Systems Integration', v: explorerPresentationLine(brand, 'operations.model.systems_integration') },
          { k: 'Pre-opening Discipline', v: explorerPresentationLine(brand, 'operations.model.pre_opening') }
        ],
        { suppressEmpty: suppressEmpty }
      ) +
      oeKvBlock(
        'Operations & complexity',
        [
          { k: 'Staffing Intensity', v: explorerPresentationLine(brand, 'operations.model.staffing_intensity') },
          { k: 'F&B Complexity', v: explorerPresentationLine(brand, 'operations.model.fb_complexity') },
          { k: 'Training Requirements', v: explorerPresentationLine(brand, 'operations.model.training') }
        ],
        { suppressEmpty: suppressEmpty }
      ) +
      oeKvBlock(
        'Governance & technology',
        [
          { k: 'Reporting Discipline', v: explorerPresentationLine(brand, 'operations.model.reporting_discipline') },
          { k: 'QA Rhythm', v: explorerPresentationLine(brand, 'operations.model.qa_rhythm') },
          { k: 'Technology Expectations', v: explorerPresentationLine(brand, 'operations.model.technology') }
        ],
        { suppressEmpty: suppressEmpty }
      ) +
      '</div>';
    var indRows = flexibilityIndicatorsHtml(brand);
    var standardsPhilosophy = explorerMergedBody(brand, 'operations.standards_philosophy');
    var opCompatSummary = explorerMergedBody(brand, 'operations.operator_compat.summary');
    var opCompatFit = explorerMergedBody(brand, 'operations.operator_compat.fit');
    var opCompatTagRow = operatorCompatTagRowHtml(brand);
    var diffTitles = ['QA Cadence', 'Training Rigor', 'Reporting Expectations', 'Brand Interaction Frequency'];
    var diffFallbacks = [
      qaRhythmLine(brand) || std.brandQaExpectations,
      trainingRequirementsLine(brand) || joinOpMulti(op.hrTrainingServices),
      std.brandCompliance,
      [op.communicationStyle, op.ownerResponseTime, op.decisionMaking].filter(hasVal).join(' · ') ||
        std.brandStandardsNotes
    ];
    var diffVals = OPERATIONS_COMPLIANCE_SLOTS.map(function (slotKey, i) {
      var fromSlot = explorerPresentationLine(brand, slotKey);
      return hasVal(fromSlot) ? fromSlot : diffFallbacks[i];
    });
    var diffGrid = diffTitles
      .map(function (title, i) {
        var val = diffVals[i];
        if (suppressEmpty && !hasVal(val)) return '';
        var inner = hasVal(val) ? escapeHtml(fmtCell(val)) : '&nbsp;';
        return '<div class="diff-card"><strong>' + escapeHtml(title) + '</strong><br/>' + inner + '</div>';
      })
      .join('');
    var philosophySection =
      suppressEmpty && !hasVal(standardsPhilosophy)
        ? ''
        : '<section class="oe-section"><h2 class="oe-section-title">Standards Philosophy</h2>' +
          explorerDetailCard('Philosophy', standardsPhilosophy, { suppressEmpty: suppressEmpty }) +
          '</section>';
    var flexibilitySection =
      suppressEmpty && !String(indRows || '').trim()
        ? ''
        : '<section class="oe-section"><h2 class="oe-section-title">Flexibility Indicators</h2>' +
          '<div class="info-card"><div class="indicator-row">' +
          indRows +
          '</div></div></section>';
    var compatSection =
      suppressEmpty && !hasVal(opCompatSummary) && !hasVal(opCompatFit) && !hasVal(opCompatTagRow)
        ? ''
        : '<section class="oe-section"><h2 class="oe-section-title">Third-Party Operator Compatibility</h2>' +
          '<div class="explorer-detail-stack">' +
          explorerDetailCard('Summary', opCompatSummary, { suppressEmpty: suppressEmpty }) +
          opCompatTagRow +
          explorerDetailCard('Fit', opCompatFit, { suppressEmpty: suppressEmpty }) +
          '</div></section>';
    var complianceSection =
      suppressEmpty && !String(diffGrid || '').trim()
        ? ''
        : '<section class="oe-section"><h2 class="oe-section-title">Compliance &amp; Oversight</h2>' +
          '<div class="diff-grid">' +
          diffGrid +
          '</div></section>';
    return wrapOe(
      '<section class="oe-section"><h2 class="oe-section-title">Operating Model</h2>' +
        grid +
        '</section>' +
        philosophySection +
        flexibilitySection +
        compatSection +
        complianceSection
    );
  }

  var STANDARDS_OWNER_OUTPUT_NOTE =
    'This section summarizes typical brand requirement areas and owner planning considerations from Dealality presentation data. ' +
    'It supports internal review and does not constitute a recommendation, franchise advice, or a property-specific compliance determination—confirm all items with brand disclosure and your agreement.';

  function parseRequirementBody(raw) {
    var out = {
      typical: '',
      owner: '',
      applies: '',
      status: '',
      notesToConfirm: '',
      flexibilityNotes: '',
      sourceConfidence: ''
    };
    if (!hasVal(raw)) return out;
    String(raw)
      .split(/\n/)
      .forEach(function (line) {
        var t = line.trim();
        if (!t) return;
        if (/^Typical consideration:/i.test(t) || /^Blu consideration:/i.test(t)) {
          out.typical = t
            .replace(/^Typical consideration:\s*/i, '')
            .replace(/^Blu consideration:\s*/i, '')
            .trim();
        } else if (/^Owner planning consideration:/i.test(t)) {
          out.owner = t.replace(/^Owner planning consideration:\s*/i, '').trim();
        } else if (/^Owner planning:/i.test(t)) {
          out.owner = t.replace(/^Owner planning:\s*/i, '').trim();
        } else if (/^Applies to:/i.test(t)) {
          out.applies = t.replace(/^Applies to:\s*/i, '').trim();
        } else if (/^Typical status:/i.test(t)) {
          out.status = t.replace(/^Typical status:\s*/i, '').trim();
        } else if (/^Flexibility\s*\/?\s*exception notes:/i.test(t) || /^Flexibility:/i.test(t)) {
          out.flexibilityNotes = t
            .replace(/^Flexibility\s*\/?\s*exception notes:\s*/i, '')
            .replace(/^Flexibility:\s*/i, '')
            .trim();
        } else if (/^Notes to confirm:/i.test(t)) {
          out.notesToConfirm = t.replace(/^Notes to confirm:\s*/i, '').trim();
        } else if (/^Source confidence:/i.test(t)) {
          out.sourceConfidence = t.replace(/^Source confidence:\s*/i, '').trim();
        }
      });
    return out;
  }

  function standardsOwnerTableCell(text) {
    return hasVal(text) ? escapeHtml(text).replace(/\n/g, '<br>') : '—';
  }

  function standardsOwnerNotesConfirmCell(parsed) {
    var parts = [];
    if (parsed.applies) {
      parts.push(
        '<span class="be-standards-owner-table__subnote"><strong>Applies To:</strong> ' +
          escapeHtml(parsed.applies).replace(/\n/g, '<br>') +
          '</span>'
      );
    }
    if (parsed.notesToConfirm) {
      parts.push(escapeHtml(parsed.notesToConfirm).replace(/\n/g, '<br>'));
    }
    if (parsed.flexibilityNotes) {
      parts.push(
        '<span class="be-standards-owner-table__subnote"><strong>Flexibility / Exception:</strong> ' +
          escapeHtml(parsed.flexibilityNotes).replace(/\n/g, '<br>') +
          '</span>'
      );
    }
    if (parsed.sourceConfidence) {
      parts.push(
        '<span class="be-standards-owner-table__subnote"><strong>Source Confidence:</strong> ' +
          escapeHtml(parsed.sourceConfidence) +
          '</span>'
      );
    }
    return parts.length ? parts.join('<br><br>') : '—';
  }

  function buildStandardsOwnerTableHtml(requirementRows) {
    if (!requirementRows.length) return '';
    var bodyRows = requirementRows
      .map(function (row) {
        var area = hasVal(row.title) ? String(row.title).trim() : 'Requirement Area';
        var parsed = parseRequirementBody(row.body);
        var typical = standardsOwnerTableCell(parsed.typical);
        var owner = standardsOwnerTableCell(parsed.owner);
        var status = standardsOwnerTableCell(parsed.status || 'Confirm With Brand');
        var notes = standardsOwnerNotesConfirmCell(parsed);
        if (typical === '—' && owner === '—' && notes === '—' && hasVal(row.body)) {
          typical = standardsOwnerTableCell(row.body);
        }
        return (
          '<tr>' +
          '<th scope="row" class="be-standards-owner-table__area">' +
          escapeHtml(area) +
          '</th>' +
          '<td class="be-standards-owner-table__typical">' +
          typical +
          '</td>' +
          '<td class="be-standards-owner-table__owner">' +
          owner +
          '</td>' +
          '<td class="be-standards-owner-table__status">' +
          status +
          '</td>' +
          '<td class="be-standards-owner-table__notes">' +
          notes +
          '</td></tr>'
        );
      })
      .join('');
    return (
      '<div class="be-standards-table-wrap" role="region" aria-label="Standard Detail Table">' +
      '<table class="be-atelier-mini-table be-standards-owner-table be-standards-owner-table--five-col">' +
      '<thead><tr>' +
      '<th scope="col">Requirement Area</th>' +
      '<th scope="col">Typical Consideration</th>' +
      '<th scope="col">Owner Planning Consideration</th>' +
      '<th scope="col">Typical Status</th>' +
      '<th scope="col">Notes to Confirm</th>' +
      '</tr></thead><tbody>' +
      bodyRows +
      '</tbody></table></div>'
    );
  }

  function renderStandardsOwnerConsiderations(brand) {
    var intro = explorerMergedBody(brand, 'standards.intro');
    var lastReviewed = explorerPresentationLine(brand, 'standards.last_reviewed');
    var sourceConfidence = explorerPresentationLine(brand, 'standards.source_confidence');
    var requirementRows = explorerCardRowsForSlot(brand, 'standards.requirement');
    var conversion = explorerMergedBody(brand, 'standards.conversion');
    var questionsRaw = explorerMergedBody(brand, 'standards.questions');
    var dealInputsRaw = explorerMergedBody(brand, 'standards.deal_inputs');
    var std = brand.brandStandards && typeof brand.brandStandards === 'object' ? brand.brandStandards : {};

    var metaBits = [];
    if (hasVal(lastReviewed)) {
      metaBits.push('<span><strong>Last Reviewed:</strong> ' + escapeHtml(lastReviewed) + '</span>');
    }
    if (hasVal(sourceConfidence)) {
      metaBits.push('<span><strong>Source Confidence:</strong> ' + escapeHtml(sourceConfidence) + '</span>');
    }
    var metaHtml = metaBits.length
      ? '<p class="be-standards-meta">' + metaBits.join(' · ') + '</p>'
      : '';

    var tableHtml = buildStandardsOwnerTableHtml(requirementRows);
    var introHint = hasVal(intro)
      ? '<p class="oe-section-hint be-standards-intro">' + escapeHtml(intro).replace(/\n/g, '<br>') + '</p>'
      : '<p class="oe-section-hint">Confirm each row with brand disclosure and your agreement. Status labels are illustrative, not a property audit.</p>';

    var requirementsSection = '';
    if (requirementRows.length) {
      requirementsSection =
        '<section class="oe-section oe-section--standards-table">' +
        '<h2 class="oe-section-title">Standard Detail, Where Available</h2>' +
        introHint +
        tableHtml +
        '</section>';
    } else {
      var fallback =
        hasVal(std.brandStandards) || hasVal(std.brandStandardsNotes)
          ? explorerDetailCard(
              'Brand Setup Narrative (Structured Fields)',
              [std.brandStandards, std.brandStandardsNotes].filter(hasVal).join('\n\n')
            )
          : '';
      requirementsSection =
        '<section class="oe-section oe-section--standards-table">' +
        '<h2 class="oe-section-title">Standard Detail, Where Available</h2>' +
        (shouldSuppressIncompleteExternalSections(brand)
          ? ''
          : '<p class="be-atelier-placeholder">No owner planning checklist is published in Brand Explorer presentation for this brand yet. Confirm requirements with brand disclosure, the franchise agreement, and design standards manuals.</p>' +
            fallback) +
        '</section>';
    }

    var secondaryParts = [];
    if (hasVal(conversion)) {
      secondaryParts.push(
        '<section class="oe-section oe-section--standards-secondary">' +
          '<h2 class="oe-section-title">Conversion &amp; Sister-Flag Transitions</h2>' +
          '<p class="be-standards-secondary-body">' +
          escapeHtml(conversion).replace(/\n/g, '<br>') +
          '</p></section>'
      );
    }
    if (hasVal(questionsRaw)) {
      secondaryParts.push(
        '<section class="oe-section oe-section--standards-secondary">' +
          '<h2 class="oe-section-title">Confirm With Brand</h2>' +
          explorerLinesAsUl(escapeHtml, questionsRaw) +
          '</section>'
      );
    }
    if (hasVal(dealInputsRaw)) {
      secondaryParts.push(
        '<section class="oe-section oe-section--standards-secondary">' +
          '<h2 class="oe-section-title">Deal Context Inputs</h2>' +
          '<p class="be-standards-deal-inputs">' +
          escapeHtml(fmtCell(dealInputsRaw)).replace(/\n/g, '<br>') +
          '</p></section>'
      );
    }

    return wrapOe(
      metaHtml +
        requirementsSection +
        (secondaryParts.length
          ? '<div class="be-standards-secondary-wrap">' + secondaryParts.join('') + '</div>'
          : '')
    );
  }

  function renderCommercialEngine(brand) {
    var bn = brand.name ? String(brand.name).trim() : 'This brand';
    var commIntroSlot = explorerMergedBody(brand, 'commercial.intro');
    var COMM_STATIC = [
      [
        'Distribution & Retail Reach',
        'Branded retail paths guests already use—CRS connectivity, brand.com and app, retail OTA relationships, and packages—so the property shows up in consideration sets where independents often under-index.',
        'Evaluate whether you get more qualified demand without funding a global platform alone—portfolio campaigns and rate plans that match how guests shop in your segment.'
      ],
      [
        'Revenue Management & Pricing Discipline',
        'Forecasting tools, competitive sets, restriction strategies, and brand-level playbooks tuned to the chain scale—not only discounting.',
        'Underwrite average daily rate protection in peak windows and escalation support during shocks; confirm what is included in your agreement tier.'
      ],
      [
        'Digital Marketing & Performance Media',
        'Paid and owned media, search, social, and retargeting at portfolio scale, with creative templates that can still carry property-level story.',
        'Pooled spend can lower acquisition cost at the margin; expect always-on brand search and seasonal bursts aligned to holidays, events, and city calendars.'
      ],
      [
        'Corporate, SME & Group Pull',
        'Contracted travelers, small meetings, and negotiated programs where the flag acts as a trusted filter—especially in urban and gateway mixed-use assets.',
        'RFP tools, account coverage, and standard proposals can open corporate doors; size addressable demand directionally for your market tier.'
      ],
      [
        'Leisure & Destination Visibility',
        'Inspiration content, packages, partnerships, and destination narratives for high-intent leisure shoppers—when rate premium depends on aspiration and uniqueness.',
        'Local design, F&amp;B, and ties still matter; distribution should convert the story—earlier visibility to the right guests in the booking journey.'
      ],
      [
        'International & Feeder Markets',
        'Inbound and cross-border feeders where global recognition reduces perceived risk—gateways, hubs, and resorts with international mix.',
        'Language, currency, and channel coverage in feeder countries; portfolio campaigns tied to holidays and routes—performance varies by market maturity and airlift.'
      ],
      [
        'Sales & Catering Brand Pull',
        'Brand credibility, central inquiry flow, and proposal tools for weddings, SMERF, and small corporate meetings.',
        'Compare lead quality and trust transfer against going independent; weigh brand contribution vs. in-house sales and catering cost.'
      ],
      [
        'Reputation, Reviews & QA Lift',
        'Recognizable flags improve post-click conversion; QA and service standards reduce variance that hurts reviews and repeat visits.',
        'Review response, recovery playbooks, and brand-led offers can protect long-term rate power when executed consistently on property.'
      ],
      [
        'Data, Analytics & Experimentation',
        'Portfolio benchmarks, test-and-learn, and guest insights to refine offers, room types, and channel mix.',
        'Network learning, test campaigns, and reporting many lenders expect—treat as commercial intelligence for decisions, not vanity metrics.'
      ]
    ];
    var scenGrid =
      '<div class="scenario-card-grid" style="grid-template-columns:repeat(3,1fr)">' +
      COMM_STATIC.map(function (row, idx) {
        var key = COMMERCIAL_LEVER_KEYS[idx];
        var slotKey = key ? 'commercial.lever.' + key : '';
        if (slotKey && explorerBlocksForSlot(brand, slotKey).length) {
          return commercialLeverFromSlot(brand, slotKey, row[0], row[1], row[2]);
        }
        return commercialStrengthCardHtml(row[0], row[1], row[2]);
      }).join('') +
      '</div>';
    var kpis =
      '<div class="brand-markets-kpi" style="margin-bottom:16px" aria-label="Illustrative Commercial Footprint">' +
      commercialKpiFromSlot(
        brand,
        'commercial.kpi.channels',
        'Channels in Franchise Materials',
        'Brand.com · major OTAs · GDS · metasearch'
      ) +
      commercialKpiFromSlot(
        brand,
        'commercial.kpi.campaigns',
        'Campaign Rhythm',
        'Always-on + seasonal / market bursts'
      ) +
      commercialKpiFromSlot(brand, 'commercial.kpi.b2b', 'B2B Programs', 'RFP & account programs (where active)') +
      commercialKpiFromSlot(
        brand,
        'commercial.kpi.lens',
        'Owner Underwriting Lens',
        'Net contribution after fees and channel costs'
      ) +
      '</div>';
    var themeRows = explorerCardRowsForSlot(brand, 'commercial.theme').filter(function (x) {
      return hasVal(x.body) || hasVal(x.title);
    });
    var anchorUl;
    if (themeRows.length) {
      anchorUl = themeRows
        .map(function (tr) {
          var line = hasVal(tr.body) ? tr.body : tr.title;
          return '<li>' + positionBody(line) + '</li>';
        })
        .join('');
    } else {
      anchorUl =
        '<li><strong style="color:var(--text,#fff);font-weight:600;">More demand at the top of the funnel</strong> — retail presence, search, and inspiration media guests see before they choose a city or date.</li>' +
        '<li><strong style="color:var(--text,#fff);font-weight:600;">Better conversion at the bottom</strong> — trust, reviews, loyalty, and frictionless booking paths that turn lookers into stays.</li>' +
        '<li><strong style="color:var(--text,#fff);font-weight:600;">Repeat and higher-quality guests</strong> — loyalty, corporate accounts, and recognition that increase lifetime value versus one-off OTA transactions.</li>' +
        '<li><strong style="color:var(--text,#fff);font-weight:600;">Commercial systems, not just a logo</strong> — pricing, sales support, and analytics that should earn the fee in your pro forma.</li>';
    }
    var demandRows = explorerCardRowsForSlot(brand, 'commercial.demand').filter(function (x) {
      return hasVal(x.title) || hasVal(x.body);
    });
    var demandHtml;
    if (demandRows.length) {
      demandHtml = demandRows
        .map(function (dr) {
          return demandCell(dr.title || 'Scenario', dr.body || '');
        })
        .join('');
    } else {
      var COMM_DEMAND = [
        ['Gateway Urban', 'Strong'],
        ['Regional & Secondary Upscale', 'Moderate–strong'],
        ['Corporate-Led Urban', 'Strong'],
        ['Resort / Coastal Leisure', 'Strong'],
        ['Conversion / Repositioning', 'Strong'],
        ['Pure Economy / Highway', 'Not a fit']
      ];
      demandHtml = COMM_DEMAND.map(function (pair) {
        return demandCell(pair[0], pair[1]);
      }).join('');
    }
    var diffBlock = explorerFirstBlock(brand, 'commercial.differentiator');
    var diffHtml = '';
    if (diffBlock && (hasVal(diffBlock.body) || hasVal(diffBlock.title))) {
      diffHtml =
        '<div class="explorer-detail-card" style="margin-bottom:14px">' +
        '<h3 class="explorer-detail-card__label">' +
        escapeHtml(hasVal(diffBlock.title) ? String(diffBlock.title).trim() : 'Commercial edge on this brand') +
        '</h3>' +
        positionBody(hasVal(diffBlock.body) ? diffBlock.body : diffBlock.title) +
        '</div>';
    }
    var commIntro = hasVal(commIntroSlot)
      ? positionBody(commIntroSlot)
      : positionBodyHtml(
          escapeHtml(bn) +
            ' — how affiliation with this flag can affect <strong style="color:var(--text,#fff);font-weight:600;">demand, rate, and channel mix</strong> on your project. Each lever includes a <strong style="color:var(--text,#fff);font-weight:600;">project impact</strong> line specific to this brand where published; illustrative only—not a performance guarantee.'
        );
    return wrapOe(
      '<section class="oe-section">' +
        '<h2 class="oe-section-title">Commercial Strengths</h2>' +
        '<p class="oe-section-hint">Brand-Specific Benefits and Channel Levers (Illustrative; Not Property-Specific Performance)</p>' +
        '<div class="explorer-detail-card" style="margin-bottom:14px">' +
        '<h3 class="explorer-detail-card__label">How This Brand Can Lift Your Project</h3>' +
        commIntro +
        '</div>' +
        diffHtml +
        kpis +
        scenGrid +
        '<div class="oe-cluster" style="margin-top:4px"><h3>Where This Brand Tends to Win</h3><ul>' +
        anchorUl +
        '</ul></div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Demand Scenario View</h2>' +
        '<p class="oe-section-hint">Directional Labels Only</p>' +
        '<div class="demand-matrix">' +
        demandHtml +
        '</div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Market Perception</h2>' +
        explorerDetailCard('Summary', brand.brandPositioning) +
        '</section>'
    );
  }

  var ECON_FEE_TYPE_DEFS = [
    {
      key: 'application',
      label: 'Application / entry',
      min: 'typicalApplicationFeeMin',
      max: 'typicalApplicationFeeMax',
      basis: 'typicalApplicationFeeBasis',
      notes: 'typicalApplicationFeeNotes',
      signals: ['typicalApplicationFeeBasis', 'typicalApplicationFeeNotes', 'typicalApplicationFeeMin']
    },
    {
      key: 'royalty',
      label: 'Royalty / brand fee',
      min: 'typicalRoyaltyPercentMin',
      max: 'typicalRoyaltyPercentMax',
      basis: 'typicalRoyaltyPercentBasis',
      notes: 'typicalRoyaltyNotes',
      percentRange: true,
      signals: ['typicalRoyaltyPercentBasis', 'typicalRoyaltyNotes', 'typicalRoyaltyPercentMin']
    },
    {
      key: 'marketing',
      label: 'Marketing / brand fund',
      min: 'typicalMarketingFeePercentMin',
      max: 'typicalMarketingFeePercentMax',
      basis: 'typicalMarketingFeePercentBasis',
      notes: 'typicalMarketingFeeNotes',
      percentRange: true,
      signals: ['typicalMarketingFeePercentBasis', 'typicalMarketingFeeNotes', 'typicalMarketingFeePercentMin']
    },
    {
      key: 'technology',
      label: 'Technology / systems',
      min: 'typicalTechnologyFeeMin',
      max: 'typicalTechnologyFeeMax',
      basis: 'typicalTechnologyFeeBasis',
      notes: 'typicalTechnologyFeeNotes',
      signals: ['typicalTechnologyFeeBasis', 'typicalTechnologyFeeNotes', 'typicalTechnologyFeeMin']
    },
    {
      key: 'loyalty',
      label: 'Loyalty / program participation',
      min: 'typicalLoyaltyFeePercentMin',
      max: 'typicalLoyaltyFeePercentMax',
      basis: 'typicalLoyaltyFeePercentBasis',
      notes: 'typicalLoyaltyFeeNotes',
      percentRange: true,
      signals: ['typicalLoyaltyFeePercentBasis', 'typicalLoyaltyFeeNotes', 'typicalLoyaltyFeePercentMin']
    },
    {
      key: 'reservation',
      label: 'Reservation / distribution',
      min: 'typicalReservationFeeMin',
      max: 'typicalReservationFeeMax',
      basis: 'typicalReservationFeeBasis',
      notes: 'typicalReservationFeeNotes',
      signals: ['typicalReservationFeeBasis', 'typicalReservationFeeNotes', 'typicalReservationFeeMin']
    },
    {
      key: 'training',
      label: 'Training / opening support',
      min: 'typicalTrainingFeeMin',
      max: 'typicalTrainingFeeMax',
      basis: 'typicalTrainingFeeBasis',
      notes: 'typicalTrainingFeeNotes',
      signals: ['typicalTrainingFeeBasis', 'typicalTrainingFeeNotes', 'typicalTrainingFeeMin']
    }
  ];

  var ECON_FDD_KPI_DEFS = [
    {
      slot: 'economics.kpi.royalty',
      label: 'Brand Royalty',
      feeKey: 'royalty'
    },
    {
      slot: 'economics.kpi.marketing',
      label: 'Marketing / brand fund',
      feeKey: 'marketing'
    },
    {
      slot: 'economics.kpi.application',
      label: 'Application fee',
      feeKey: 'application'
    },
    {
      slot: 'economics.kpi.training',
      label: 'Training / opening fee',
      feeKey: 'training'
    },
    {
      slot: 'economics.kpi.term',
      label: 'Initial franchise term',
      term: true
    },
    {
      slot: 'economics.kpi.technology',
      label: 'Technology fee',
      feeKey: 'technology'
    },
    {
      slot: 'economics.kpi.loyalty',
      label: 'Loyalty program fee',
      feeKey: 'loyalty'
    }
  ];

  function econNormalizePercentValue(v) {
    if (!hasVal(v)) return '';
    var s = String(v).trim().replace(/%$/, '');
    var n = parseFloat(s.replace(/,/g, ''));
    if (!isFinite(n)) return s;
    if (n > 0 && n <= 1) n = Math.round(n * 1000) / 10;
    return String(n).replace(/\.0$/, '');
  }

  function econIsPercentBasis(basis) {
    if (!hasVal(basis)) return false;
    var b = String(basis).toLowerCase();
    if (b.indexOf('%') >= 0) return true;
    return /\b(gross|total)\s+revenue\b/.test(b) || /\brooms\s+revenue\b/.test(b);
  }

  function econLooksLikeRangeText(s) {
    if (!hasVal(s)) return false;
    return /[\d]/.test(String(s));
  }

  function econFddKpiValueFromApi(brand, def) {
    var fs = brand.feeStructure && typeof brand.feeStructure === 'object' ? brand.feeStructure : {};
    var dt = brand.dealTerms && typeof brand.dealTerms === 'object' ? brand.dealTerms : {};
    if (def.term) {
      var term = econInitialTermDisplay(dt);
      if (hasVal(term)) return term;
      return (
        explorerPresentationLine(brand, 'economics.kpi.term') ||
        explorerPresentationLine(brand, 'economics.kpi.agreement') ||
        ''
      );
    }
    if (def.feeKey) {
      var feeDef = econFeeDefByKey(def.feeKey);
      if (feeDef) return econFeeRangeLine(fs, feeDef);
    }
    return '';
  }

  function econFormatPercentRange(min, max) {
    var mn = econNormalizePercentValue(min);
    var mx = econNormalizePercentValue(max);
    if (!mn && !mx) return '';
    if (mn && mx && mn !== mx) return mn + '% – ' + mx + '%';
    return (mn || mx) + '%';
  }

  function econFormatMoneyRange(min, max) {
    function moneyOne(v) {
      if (!hasVal(v)) return '';
      var s = String(v).trim();
      if (s.indexOf('$') >= 0) return s;
      var n = parseFloat(s.replace(/,/g, ''));
      if (!isFinite(n)) return s;
      if (n >= 1000) return '$' + fmtNum(n);
      return '$' + s;
    }
    var a = moneyOne(min);
    var b = moneyOne(max);
    if (a && b && a !== b) return a + ' – ' + b;
    return a || b || '';
  }

  function econFeeRangeLine(fs, def) {
    if (!fs || !def) return '';
    var basis = def.basis && hasVal(fs[def.basis]) ? fmtCell(fs[def.basis]) : '';
    var min = def.min ? fs[def.min] : '';
    var max = def.max ? fs[def.max] : '';
    var range = '';
    if (def.percentRange || econIsPercentBasis(basis)) {
      range = econFormatPercentRange(min, max);
    } else {
      range = econFormatMoneyRange(min, max);
    }
    if (!range) return '';
    if (basis) return range + ' · ' + basis;
    return range;
  }

  function econInitialTermDisplay(dt) {
    if (!dt || typeof dt !== 'object') return '';
    var q = dt.minInitialTermQty;
    var l = dt.minInitialTermLength;
    var d = dt.minInitialTermDuration;
    if (hasVal(q) && hasVal(l)) {
      return String(q).trim() + ' × ' + String(l).trim() + (hasVal(d) ? ' ' + String(d).trim() : '');
    }
    if (hasVal(l)) return String(l).trim() + (hasVal(d) ? ' ' + String(d).trim() : '');
    if (hasVal(dt.minInitialTerm)) return fmtCell(dt.minInitialTerm);
    return '';
  }

  function econRenewalDisplay(dt) {
    if (!dt || typeof dt !== 'object') return '';
    var q = dt.renewalOptionQty;
    var l = dt.renewalOptionLength;
    var d = dt.renewalOptionDuration;
    if (hasVal(q) && hasVal(l)) {
      return String(q).trim() + ' × ' + String(l).trim() + (hasVal(d) ? ' ' + String(d).trim() : '');
    }
    if (hasVal(dt.renewalStructure)) return fmtCell(dt.renewalStructure);
    return '';
  }

  function econFddKpiValue(brand, def) {
    var slotRow = explorerFirstBlock(brand, def.slot);
    if (slotRow && hasVal(slotRow.body)) {
      var slotBody = String(slotRow.body).trim();
      if (econLooksLikeRangeText(slotBody)) return slotBody;
    }
    return econFddKpiValueFromApi(brand, def);
  }

  function econSanitizeOwnerFacingChrome(text) {
    if (!hasVal(text)) return '';
    var out = String(text);
    out = out.replace(/\bConfirm in Item 7 and your LOI\.?/gi, 'Ask brand development for current participation costs and timing before you commit.');
    out = out.replace(/\bconfirm every line in your disclosure document and LOI\b/gi, 'ask brand development for current participation costs, operating obligations, and agreement terms before you commit');
    out = out.replace(/\bConfirm participation costs and timing directly during brand engagement(?: and legal review)?\.?/gi, 'Ask brand development for current participation costs and timing before you commit');
    out = out.replace(/\bconfirm participation costs, operating obligations, and agreement terms directly during brand engagement(?: and legal review)?\.?/gi, 'ask brand development for current participation costs, operating obligations, and agreement terms before you commit');
    out = out.replace(/\bconfirm participation costs and agreement terms directly during brand engagement(?: and legal review)?\.?/gi, 'ask brand development for current participation costs and agreement terms before you commit');
    out = out.replace(/\bfranchise disclosure document\b/gi, 'commercial agreement materials');
    out = out.replace(/\bfranchise disclosure\b/gi, 'commercial agreement review');
    out = out.replace(/\bdisclosure document\b/gi, 'commercial agreement materials');
    out = out.replace(/\bfee stack\b/gi, 'participation costs and program fees');
    out = out.replace(/\bnet contribution\b/gi, 'contribution after program costs');
    out = out.replace(/\bItem\s*19\b/gi, 'portfolio-level brand performance materials');
    out = out.replace(/\bItem\s*7\b/gi, 'initial investment schedules');
    out = out.replace(/\bFDD\b/g, 'commercial agreement materials');
    out = out.replace(/\bLOI\b/g, 'commercial proposal');
    out = out.replace(/\bADR\b/g, 'average daily rate');
    out = out.replace(/\bRevPAR\b/g, 'revenue per available room');
    out = out.replace(/https?:\/\/\S+/gi, '');
    out = out.replace(/\bSources?:\s*/gi, '');
    out = out.replace(/\bSource:\s*/gi, '');
    return out.replace(/[ \t]{2,}/g, ' ').trim();
  }

  function econFeeSupportFromApi(fs, def) {
    if (!fs || !def) return '';
    if (def.notes && hasVal(fs[def.notes])) return econSanitizeOwnerFacingChrome(String(fs[def.notes]).trim());
    if (def.basis && hasVal(fs[def.basis])) {
      return (
        'Basis: ' +
        fmtCell(fs[def.basis]) +
        '. Ask brand development for current participation costs and timing before you commit.'
      );
    }
    return '';
  }

  function econFddKpiCard(label, rangeLine, support) {
    return (
      '<div class="brand-markets-kpi__card brand-markets-kpi__card--econ-glance">' +
      '<div class="brand-markets-kpi__label">' +
      escapeHtml(label) +
      '</div>' +
      (hasVal(rangeLine)
        ? '<div class="brand-markets-kpi__value">' + escapeHtml(fmtCell(rangeLine)) + '</div>'
        : '<div class="brand-markets-kpi__value oe-dd--empty">—</div>') +
      (hasVal(support)
        ? '<p class="brand-markets-kpi__note">' + escapeHtml(fmtCell(support)) + '</p>'
        : '') +
      '</div>'
    );
  }

  function econFddKpiRowHtml(brand) {
    var fs = brand.feeStructure && typeof brand.feeStructure === 'object' ? brand.feeStructure : {};
    var cards = [];

    ECON_FDD_KPI_DEFS.forEach(function (kpiDef) {
      var slotRow = explorerFirstBlock(brand, kpiDef.slot);
      var lbl = kpiDef.label;
      if (slotRow && hasVal(slotRow.title)) lbl = String(slotRow.title).trim();

      if (kpiDef.term) {
        var termVal = econFddKpiValue(brand, kpiDef);
        if (hasVal(termVal)) cards.push(econFddKpiCard(lbl, termVal, ''));
        return;
      }

      var range = '';
      if (slotRow && hasVal(slotRow.body) && econLooksLikeRangeText(slotRow.body)) {
        range = String(slotRow.body).trim();
      }
      var feeDef = kpiDef.feeKey ? econFeeDefByKey(kpiDef.feeKey) : null;
      if (!hasVal(range) && feeDef) range = econFeeRangeLine(fs, feeDef);
      if (!hasVal(range)) return;
      cards.push(econFddKpiCard(lbl, range, feeDef ? econFeeSupportFromApi(fs, feeDef) : ''));
    });

    var resDef = econFeeDefByKey('reservation');
    if (resDef && econFeeTypePresent(fs, resDef)) {
      var resRange = econFeeRangeLine(fs, resDef);
      if (hasVal(resRange)) {
        cards.push(econFddKpiCard(resDef.label, resRange, econFeeSupportFromApi(fs, resDef)));
      }
    }

    if (!cards.length) {
      var legacy = [
        { slot: 'economics.kpi.fee_stack', label: 'Participation costs' },
        { slot: 'economics.kpi.agreement', label: 'Agreement shape' },
        { slot: 'economics.kpi.capital', label: 'Capital rhythm' },
        { slot: 'economics.kpi.incentives', label: 'Incentives' }
      ];
      legacy.forEach(function (leg) {
        var val = explorerPresentationLine(brand, leg.slot);
        if (!hasVal(val)) return;
        var row = explorerFirstBlock(brand, leg.slot);
        var legLbl = leg.label;
        if (row && hasVal(row.title)) legLbl = String(row.title).trim();
        cards.push(econFddKpiCard(legLbl, val, ''));
      });
    }

    if (!cards.length) return '';
    return (
      '<div class="brand-markets-kpi brand-markets-kpi--econ-fdd" aria-label="Typical disclosed ranges">' +
      cards.join('') +
      '</div>'
    );
  }

  function econProofPointCard(headline, rangeLine, support) {
    var rangeHtml = hasVal(rangeLine)
      ? '<div class="econ-fdd-range">' + escapeHtml(rangeLine) + '</div>'
      : '';
    var body = hasVal(support) ? escapeHtml(fmtCell(support)) : '&nbsp;';
    return (
      '<article class="proof-point-card">' +
      '<div class="proof-point-card__icon">◇</div>' +
      rangeHtml +
      '<h3 class="proof-point-card__headline">' +
      escapeHtml(headline) +
      '</h3><p class="proof-point-card__support' +
      (hasVal(support) ? '' : ' oe-dd--empty') +
      '">' +
      body +
      '</p></article>'
    );
  }

  function econTitleCaseWords(s) {
    if (!hasVal(s)) return '';
    var small = { a: 1, an: 1, and: 1, or: 1, of: 1, in: 1, on: 1, at: 1, to: 1, for: 1, the: 1, per: 1 };
    return String(s)
      .split(/\s+/)
      .map(function (word, i) {
        if (/^[%$]/.test(word)) return word;
        var m = word.match(/^([^a-zA-Z]*)([a-zA-Z]+)(.*)$/);
        if (!m) return word;
        var lead = m[1];
        var core = m[2];
        var tail = m[3];
        if (i > 0 && small[core.toLowerCase()]) return lead + core.toLowerCase() + tail;
        return lead + core.charAt(0).toUpperCase() + core.slice(1).toLowerCase() + tail;
      })
      .join(' ');
  }

  /** Title case for fee-bucket bullets; handles /, -, and acronyms (PIP, FDD, …). */
  function econFeeBucketBulletCase(s) {
    if (!hasVal(s)) return '';
    var small = { a: 1, an: 1, and: 1, or: 1, of: 1, in: 1, on: 1, at: 1, to: 1, for: 1, the: 1, per: 1 };
    // Do not promote FDD/LOI as owner-facing acronyms in economics chrome.
    var acronyms = { pip: 'PIP', crs: 'CRS', pms: 'PMS', qa: 'QA' };
    var wi = 0;
    return String(s).replace(/[a-zA-Z]+/g, function (core) {
      var low = core.toLowerCase();
      if (acronyms[low]) {
        wi++;
        return acronyms[low];
      }
      var out =
        wi > 0 && small[low] ? low : core.charAt(0).toUpperCase() + core.slice(1).toLowerCase();
      wi++;
      return out;
    });
  }

  function econListItemCase(s) {
    if (!hasVal(s)) return '';
    return econFeeBucketBulletCase(s).replace(/\bCo-op\b/g, 'Co-Op');
  }

  function econTitleCaseLabel(label) {
    if (!hasVal(label)) return '';
    return String(label)
      .split(/\s*\/\s*/)
      .map(function (part) {
        return econTitleCaseWords(part.trim());
      })
      .join(' / ');
  }

  function econParseFeeBucketBullets(body) {
    if (!hasVal(body)) return [];
    var text = String(body).trim();
    var dotMatch = text.match(/\.\s+/);
    var listPart = dotMatch && dotMatch.index != null ? text.slice(0, dotMatch.index).trim() : text;
    return listPart
      .split(/;\s*|,\s*(?:and\s+)?/i)
      .map(function (s) {
        return s.trim().replace(/^and\s+/i, '');
      })
      .filter(hasVal)
      .map(econFeeBucketBulletCase);
  }

  var ECON_FEE_BUCKET_THEME_BY_KEY = {
    application: 'Application and entry fees',
    training: 'Training and opening support',
    royalty: 'Royalty or brand fee',
    marketing: 'Marketing or brand fund',
    technology: 'Technology and systems',
    loyalty: 'Loyalty program participation',
    reservation: 'Reservation and distribution charges'
  };

  var ECON_FEE_CHANGE_THEMES = [
    'Renewal PIP',
    'Conversion PIP',
    'Termination-related obligations',
    'Owner-funded reserves'
  ];

  function econFeeBucketThemesFromApi(brand, bucket) {
    var fs = brand.feeStructure && typeof brand.feeStructure === 'object' ? brand.feeStructure : {};
    var themes = [];
    (bucket.typeKeys || []).forEach(function (key) {
      var def = econFeeDefByKey(key);
      if (!def || !econFeeTypePresent(fs, def)) return;
      themes.push(ECON_FEE_BUCKET_THEME_BY_KEY[def.key] || econTitleCaseLabel(def.label));
    });
    if (bucket.slot === 'economics.fee.change' && !themes.length) {
      return ECON_FEE_CHANGE_THEMES.map(econFeeBucketBulletCase);
    }
    return themes.map(econFeeBucketBulletCase);
  }

  function econFeeBucketCard(headline, bullets, support) {
    var listHtml = '';
    if (bullets && bullets.length) {
      listHtml =
        '<ul class="proof-point-card__fee-list bullet-list">' +
        bullets
          .map(function (line) {
            return '<li>' + escapeHtml(line) + '</li>';
          })
          .join('') +
        '</ul>';
    }
    var supportHtml = '';
    if (hasVal(support)) {
      supportHtml =
        '<p class="proof-point-card__support proof-point-card__footnote">' +
        escapeHtml(fmtCell(support)) +
        '</p>';
    } else if (!bullets.length) {
      supportHtml =
        '<p class="proof-point-card__support proof-point-card__footnote">Confirm participation categories and timing directly with brand representatives.</p>';
    }
    return (
      '<article class="proof-point-card proof-point-card--fee-bucket">' +
      '<div class="proof-point-card__icon">◇</div>' +
      '<h3 class="proof-point-card__headline">' +
      escapeHtml(econTitleCaseLabel(headline)) +
      '</h3>' +
      listHtml +
      supportHtml +
      '</article>'
    );
  }

  function econFeeBucketProofHtml(brand, bucket) {
    var slotRow = explorerFirstBlock(brand, bucket.slot);
    var title = slotRow && hasVal(slotRow.title) ? String(slotRow.title).trim() : bucket.title;
    var slotBody = slotRow && hasVal(slotRow.body) ? String(slotRow.body).trim() : '';
    var bullets = [];
    var slotFootnote = '';
    if (slotBody) {
      var footParts = slotBody.split(/\n\n+/);
      if (footParts.length > 1) {
        slotFootnote = footParts.slice(1).join('\n\n').trim();
        slotBody = footParts[0].trim();
      }
    }
    if (bucket.defaultBullets && bucket.defaultBullets.length && !slotBody) {
      bullets = bucket.defaultBullets.map(econFeeBucketBulletCase);
    } else {
      bullets = econParseFeeBucketBullets(slotBody);
      if (!bullets.length) {
        bullets = econFeeBucketThemesFromApi(brand, bucket);
      }
    }
    var footnote = slotFootnote || bucket.footnote || '';
    if (/fdd|loi|franchise disclosure|net contribution|fee stack|item\s*7|item\s*19|disclosure document|directly during brand engagement/i.test(footnote)) {
      footnote =
        'Ask brand development for current participation costs, operating obligations, and agreement terms before you commit.';
    }
    if (!footnote && isAffiliationCurationBrand(brand)) {
      footnote = 'Public materials do not disclose amounts—confirm participation terms during owner diligence.';
    }
    return econFeeBucketCard(title, bullets, footnote);
  }

  function econCashPhaseCardStyled(label, parsed) {
    var owner = parsed.owner || '';
    var brandLine = parsed.brand || '';
    var inner =
      (hasVal(owner)
        ? '<p class="econ-cash-line"><span class="econ-cash-line__who">Owner</span> ' + escapeHtml(fmtCell(owner)) + '</p>'
        : '') +
      (hasVal(brandLine)
        ? '<p class="econ-cash-line"><span class="econ-cash-line__who">Brand</span> ' + escapeHtml(fmtCell(brandLine)) + '</p>'
        : '&nbsp;');
    return (
      '<article class="scenario-card scenario-card--detail econ-cash-phase">' +
      '<h3 class="explorer-detail-card__label">' +
      escapeHtml(label) +
      '</h3>' +
      inner +
      '</article>'
    );
  }

  function econFeeTypePresent(fs, def) {
    if (!fs || !def) return false;
    return (def.signals || []).some(function (k) {
      return hasVal(fs[k]);
    });
  }

  function econFeeCardBodyFromApi(fs, def) {
    var parts = [];
    if (def.basis && hasVal(fs[def.basis])) {
      parts.push('Typically assessed on: ' + fmtCell(fs[def.basis]) + '.');
    }
    if (def.notes && hasVal(fs[def.notes])) {
      parts.push(econSanitizeOwnerFacingChrome(String(fs[def.notes]).trim()));
    }
    if (!parts.length) {
      parts.push(
        'This cost category is part of typical brand participation; confirm basis and timing directly with the brand and legal counsel.'
      );
    }
    return econSanitizeOwnerFacingChrome(parts.join(' '));
  }

  function econFeeCardsFromApi(brand) {
    var fs = brand.feeStructure && typeof brand.feeStructure === 'object' ? brand.feeStructure : {};
    return ECON_FEE_TYPE_DEFS.filter(function (def) {
      return econFeeTypePresent(fs, def);
    }).map(function (def) {
      return { title: def.label, body: econFeeCardBodyFromApi(fs, def) };
    });
  }

  function econFeeStackKpiFromApi(brand) {
    var fs = brand.feeStructure && typeof brand.feeStructure === 'object' ? brand.feeStructure : {};
    var labels = ECON_FEE_TYPE_DEFS.filter(function (def) {
      return econFeeTypePresent(fs, def);
    }).map(function (def) {
      return def.label;
    });
    if (hasVal(fs.typicalIncentivesOffered)) labels.push('Incentives (typical)');
    return labels.join(' · ');
  }

  function econPlainLinesFromObject(obj, keys) {
    var lines = [];
    (keys || []).forEach(function (k) {
      if (!obj || !hasVal(obj[k])) return;
      var v = fmtCell(obj[k]);
      if (!v) return;
      if (/^\d+(\.\d+)?$/.test(String(v).replace(/,/g, ''))) return;
      if (/^\d+(\.\d+)?\s*%$/.test(String(v).trim())) return;
      lines.push(v);
    });
    return lines;
  }

  function econNegotiabilityLabel(brand) {
    var os = brand.operationalSupport && typeof brand.operationalSupport === 'object' ? brand.operationalSupport : {};
    var slot = explorerPresentationLine(brand, 'economics.negotiability') || explorerPresentationLine(brand, 'economics.kpi.negotiability');
    if (hasVal(slot)) return slot;
    var w = fmtCell(os.willingToNegotiateIncentives);
    if (!w) return '';
    var wl = String(w).toLowerCase();
    if (wl.indexOf('yes') >= 0) return 'Often negotiated';
    if (wl.indexOf('case') >= 0) return 'Case by case';
    if (wl.indexOf('no') >= 0) return 'Mostly standard';
    return w;
  }

  var ECON_FEE_BUCKET_DEFS = [
    {
      slot: 'economics.fee.join',
      title: 'To Join',
      typeKeys: ['application', 'training'],
      defaultBullets: [
        'Application and entry fees',
        'Training and opening support',
        'Initial / franchise license fee',
        'Technology implementation / setup',
        'Plan review, design, or inspection fees'
      ],
      footnote:
        'Basis and timing vary by keys, market, and deal—ask brand development for current participation costs and agreement terms before you commit.'
    },
    {
      slot: 'economics.fee.operate',
      title: 'To Operate',
      typeKeys: ['royalty', 'marketing', 'technology', 'loyalty', 'reservation'],
      footnote:
        'Owners evaluate whether distribution, operating requirements, and program costs fit the asset after brand-related obligations.'
    },
    {
      slot: 'economics.fee.change',
      title: 'When Things Change',
      typeKeys: [],
      footnote:
        'Triggered at renewal, repositioning, or exit—not steady-state operations.'
    }
  ];

  var ECON_CASH_PHASE_DEFS = [
    {
      slot: 'economics.cash.preopening',
      label: 'Pre-Opening',
      legacySlot: 'economics.lifecycle.preopening',
      ownerDefault:
        'Front-loaded standards and FF&E alignment, technology cutover, working capital through opening, and application or training cash outlays.',
      brandDefault:
        'Design and standards review, opening playbooks, pre-opening support, and milestone QA—not day-to-day operating spend.'
    },
    {
      slot: 'economics.cash.ramp',
      label: 'Early Years (Ramp)',
      legacySlot: 'economics.lifecycle.ramp',
      ownerDefault:
        'Ramp marketing and loyalty enrollment while occupancy and rate build; recurring fees scale up as revenue mix stabilizes.',
      brandDefault:
        'Negotiated ramp relief or co-op when offered, plus channel and mix guidance as the asset proves out.'
    },
    {
      slot: 'economics.cash.steadystate',
      label: 'Steady State',
      legacySlot: 'economics.lifecycle.steadystate',
      ownerDefault:
        'The full set of recurring participation costs—royalty, marketing, technology, loyalty, and distribution—plus mandatory program participation once stabilized.',
      brandDefault:
        'Sales and revenue support, brand systems access, and portfolio benchmarks—not property payroll or routine FF&E.'
    },
    {
      slot: 'economics.cash.renewal',
      label: 'Renewal / Repositioning',
      legacySlot: 'economics.lifecycle.renewal',
      ownerDefault:
        'Renewal or conversion PIP, owner reserves, and re-licensing work when triggers hit—not part of steady-state operations.',
      brandDefault:
        'Clear renewal standards; co-investment or phased PIP timing may be negotiable in competitive renewals.'
    }
  ];

  var ECON_OPENING_STEPS = [
    'Application & Feasibility',
    'Design & Standards',
    'Pre-Opening Planning',
    'Opening Support',
    'Stabilization'
  ];

  function econFeeDefByKey(key) {
    for (var i = 0; i < ECON_FEE_TYPE_DEFS.length; i++) {
      if (ECON_FEE_TYPE_DEFS[i].key === key) return ECON_FEE_TYPE_DEFS[i];
    }
    return null;
  }

  function econParseOwnerBrandBody(body) {
    if (!hasVal(body)) return { owner: '', brand: '' };
    var paras = String(body)
      .split(/\n\n+/)
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean);
    if (paras.length >= 2) {
      return {
        owner: paras[0].replace(/^Owner(\s+typically)?(\s+funds)?:\s*/i, '').trim(),
        brand: paras[1].replace(/^Brand(\s+typically)?(\s+provides)?:\s*/i, '').trim()
      };
    }
    var single = paras[0] || '';
    if (/^owner/i.test(single) && single.indexOf('\n') < 0) {
      return { owner: single.replace(/^Owner[^:]*:\s*/i, '').trim(), brand: '' };
    }
    return { owner: single, brand: '' };
  }

  function econCashPhaseCard(label, ownerPays, brandProvides) {
    var parts = [];
    if (hasVal(ownerPays)) parts.push('Owner typically funds: ' + ownerPays);
    if (hasVal(brandProvides)) parts.push('Brand typically provides: ' + brandProvides);
    return scenarioDetailCard(label, parts.join('\n\n') || '');
  }

  function econRiskCardsFromApi(brand) {
    var dt = brand.dealTerms && typeof brand.dealTerms === 'object' ? brand.dealTerms : {};
    var fs = brand.feeStructure && typeof brand.feeStructure === 'object' ? brand.feeStructure : {};
    var lt = brand.legalTerms && typeof brand.legalTerms === 'object' ? brand.legalTerms : {};
    var cards = [];
    var term = econPlainLinesFromObject(dt, [
      'renewalStructure',
      'renewalConditions',
      'renewalNoticeResponsibility',
      'minInitialTermLength',
      'minInitialTermDuration',
      'pipAtRenewal',
      'pipForConversions'
    ]).join(' ');
    if (hasVal(term)) cards.push({ title: 'Term & renewal', body: term });
    var perf = econPlainLinesFromObject(Object.assign({}, dt, fs), [
      'performanceTestRequirement',
      'qaComplianceRequirement',
      'performanceTerminationRights',
      'ownerEarlyTerminationRights',
      'terminationFeeStructure'
    ]).join(' ');
    if (hasVal(perf)) cards.push({ title: 'Performance & exit', body: perf });
    var legal = econPlainLinesFromObject(lt, [
      'assignmentRestrictions',
      'buyoutTransferProvisions',
      'terminationOnSale',
      'aopRadius',
      'aopRestrictions'
    ]);
    legal.forEach(function (line, idx) {
      if (idx < 2) cards.push({ title: idx === 0 ? 'Transfer & sale' : 'Area of protection', body: line });
    });
    return cards;
  }

  function econNormalizeRiskTitle(title) {
    var raw = String(title || '').trim();
    if (!raw) return raw;
    var key = raw.toLowerCase();
    if (key === 'term & renewal' || key === 'term and renewal' || key === 'initial term & renewal') {
      return 'Terms & Renewals';
    }
    if (key === 'performance & exit' || key === 'performance and exit') {
      return 'Performance & Exit';
    }
    if (key === 'transfer & sale' || key === 'transfer and sale') {
      return 'Transfer & Sale';
    }
    if (key === 'area of protection') {
      return 'Area of Protection';
    }
    return raw;
  }

  function renderAtelierEconomicsObligations(brand) {
    var fs = brand.feeStructure && typeof brand.feeStructure === 'object' ? brand.feeStructure : {};
    var dt = brand.dealTerms && typeof brand.dealTerms === 'object' ? brand.dealTerms : {};
    var lt = brand.legalTerms && typeof brand.legalTerms === 'object' ? brand.legalTerms : {};
    var os = brand.operationalSupport && typeof brand.operationalSupport === 'object' ? brand.operationalSupport : {};

    var disclaimer =
      explorerMergedBody(brand, 'economics.intro') ||
      'Illustrative brand-level patterns only—not a quote, financial model, or substitute for agreement review with the brand and your advisors. Use this tab to know what to ask and model, not what to sign.';

    var cashGrid = ECON_CASH_PHASE_DEFS.map(function (def) {
      var raw =
        explorerMergedBody(brand, def.slot) ||
        explorerMergedBody(brand, def.legacySlot) ||
        '';
      var parsed = econParseOwnerBrandBody(raw);
      if (!hasVal(parsed.owner) && !hasVal(parsed.brand) && hasVal(raw) && raw.indexOf('\n\n') < 0) {
        parsed = { owner: raw, brand: '' };
      }
      if (!hasVal(parsed.owner) && def.legacySlot === 'economics.lifecycle.renewal') {
        var pipBits = [dt.pipAtRenewal, dt.pipForConversions, fs.ownerFundedReserves]
          .filter(hasVal)
          .map(fmtCell);
        if (pipBits.length) parsed.owner = pipBits.join('; ');
      }
      if (!hasVal(parsed.owner) && def.ownerDefault) parsed.owner = def.ownerDefault;
      if (!hasVal(parsed.brand) && def.brandDefault) parsed.brand = def.brandDefault;
      return econCashPhaseCardStyled(def.label, parsed);
    }).join('');

    var fddKpiRow = econFddKpiRowHtml(brand);

    var openingHasStepDetail = false;
    var openingTimeline = ECON_OPENING_STEPS.map(function (label, idx) {
      var slotRow = explorerFirstBlock(brand, 'economics.opening.step.' + (idx + 1));
      var stepLabel = slotRow && hasVal(slotRow.title) ? String(slotRow.title).trim() : label;
      stepLabel = econListItemCase(stepLabel);
      var det = slotRow && hasVal(slotRow.body) ? String(slotRow.body).trim() : '';
      if (hasVal(det)) openingHasStepDetail = true;
      return timelinePhase(stepLabel, det, { suppressEmpty: shouldRenderFullProfile(brand) });
    }).join('');

    var openingProcess =
      explorerMergedBody(brand, 'economics.opening.process') ||
      'Align with brand development, complete design and standards review, execute pre-opening with systems cutover, and stabilize with brand QA touchpoints. Third-party operators often run day-to-day opening while the brand approves milestones.';

    var openingBelowTimeline = openingHasStepDetail
      ? ''
      : explorerDetailCardMultiline('Process summary', openingProcess);

    var feeBucketGrid = ECON_FEE_BUCKET_DEFS.map(function (bucket) {
      return econFeeBucketProofHtml(brand, bucket);
    }).join('');

    var riskCards = explorerCardRowsForSlot(brand, 'economics.risk');
    riskCards = riskCards.filter(function (r) {
      return hasVal(r.title) || hasVal(r.body);
    });
    if (!riskCards.length) {
      var riskMerged =
        explorerMergedBody(brand, 'economics.risk_exit') ||
        [explorerMergedBody(brand, 'economics.term_renewal'), explorerMergedBody(brand, 'economics.performance_exit')]
          .filter(hasVal)
          .join('\n\n');
      if (hasVal(riskMerged)) {
        riskCards = [{ title: 'Term, performance & exit', body: riskMerged }];
      } else {
        riskCards = econRiskCardsFromApi(brand);
      }
    }
    if (!riskCards.length) {
      var legalLegacy = explorerCardRowsForSlot(brand, 'economics.legal');
      riskCards = legalLegacy.filter(function (r) {
        return hasVal(r.title) || hasVal(r.body);
      });
    }
    var termAnchor = econInitialTermDisplay(dt);
    var renewalAnchor = econRenewalDisplay(dt);
    if (!riskCards.length && (hasVal(termAnchor) || hasVal(renewalAnchor))) {
      riskCards = [
        {
          title: 'Initial term & renewal',
          body: [termAnchor ? 'Initial term: ' + termAnchor : '', renewalAnchor ? 'Renewal: ' + renewalAnchor : '']
            .filter(hasVal)
            .join('\n\n')
        }
      ];
    }
    riskCards = riskCards
      .map(function (r) {
        return Object.assign({}, r, { title: econNormalizeRiskTitle(r && r.title) });
      })
      .sort(function (a, b) {
        var order = {
          'Terms & Renewals': 0,
          'Performance & Exit': 1,
          'Transfer & Sale': 2,
          'Area of Protection': 3
        };
        var ao = Object.prototype.hasOwnProperty.call(order, a.title) ? order[a.title] : 99;
        var bo = Object.prototype.hasOwnProperty.call(order, b.title) ? order[b.title] : 99;
        return ao - bo;
      });
    var riskGrid = riskCards
      .map(function (r) {
        var title = r.title || 'Risk theme';
        var isTermsRenewals = /terms?\s*&\s*renewals?/i.test(title) || /term|renewal/i.test(title);
        var isPerformanceExit = /performance\s*&\s*exit/i.test(title);
        var anchor =
          isTermsRenewals && hasVal(termAnchor)
            ? termAnchor + (hasVal(renewalAnchor) ? ' · Renewal ' + renewalAnchor : '')
            : '';
        if (anchor || isPerformanceExit) {
          // Keep headline baseline aligned for the first two risk cards.
          return econProofPointCard(title, anchor || '\u00A0', r.body);
        }
        return scenarioDetailCard(title, r.body);
      })
      .join('');

    var negotiabilityLine = econNegotiabilityLabel(brand);
    var negotiabilityBody =
      explorerMergedBody(brand, 'economics.negotiability') ||
      explorerMergedBody(brand, 'economics.incentives') ||
      [
        negotiabilityLine ? 'Posture: ' + negotiabilityLine : '',
        hasVal(fs.typicalIncentivesOffered) ? 'Typical incentives: ' + fmtCell(fs.typicalIncentivesOffered) : '',
        Array.isArray(os.typesOfIncentives) && os.typesOfIncentives.length
          ? 'Often discussed: ' + os.typesOfIncentives.map(fmtCell).join(', ')
          : ''
      ]
        .filter(hasVal)
        .join('\n\n');

    var negotiableItems = splitBullets(explorerMergedBody(brand, 'economics.negotiable_items')).map(
      econListItemCase
    );
    var rarelyItems = splitBullets(explorerMergedBody(brand, 'economics.rarely_negotiable')).map(
      econListItemCase
    );
    if (!negotiableItems.length) {
      negotiableItems = [
        'Key Money or Ramp Relief',
        'Marketing Co-Op or Opening Support',
        'PIP Scope or Timing',
        'Application or Training Fee Structure',
        'Fee Ramp in Early Operating Years'
      ];
    }
    if (!rarelyItems.length) {
      rarelyItems = [
        'Core Royalty and Program Participation',
        'Mandatory Technology Stack and Integrations',
        'Brand Standards Compliance Framework',
        'Fundamental QA and Reporting Obligations'
      ];
    }
    var negotiableUl =
      '<ul class="explorer-detail-card__list bullet-list">' +
      negotiableItems
        .map(function (line) {
          return '<li>' + escapeHtml(line) + '</li>';
        })
        .join('') +
      '</ul>';
    var rarelyUl =
      '<ul class="explorer-detail-card__list bullet-list">' +
      rarelyItems
        .map(function (line) {
          return '<li>' + escapeHtml(line) + '</li>';
        })
        .join('') +
      '</ul>';

    var feeVariability =
      explorerMergedBody(brand, 'economics.fee_variability') ||
      'Room count, market tier, new build versus conversion, incentive package, and operator model usually change how fees and capital line items land on your deal.';

    var affiliationBrand = isAffiliationCurationBrand(brand);
    // Owner-safe economics chrome for all brands (including internal preview / founder review).
    var econGlanceHint = affiliationBrand
      ? 'Owner diligence categories for affiliation and collection participation—confirm commercial terms directly with brand representatives'
      : 'Typical ranges and fee-schedule notes from Brand Setup—ask brand development for current participation costs, operating obligations, and agreement terms before you commit';
    var confirmSectionTitle = 'Confirm With Brand / Legal Counsel';
    var confirmSectionHint = 'This page orients diligence—it does not replace agreement review';
    var confirmSectionBody = affiliationBrand
      ? 'All participation costs, distribution scope, design and curation expectations, and legal terms for your hotel must be confirmed directly with the brand and your counsel. Use this page to structure diligence—not as a quote or signed term sheet.'
      : 'All economics, fees, capital obligations, and legal terms for your hotel must be confirmed directly with the brand and your counsel. Work with your advisor to tie the items on this page to your model and term sheet.';
    if (/franchise disclosure|fdd|\bloi\b|disclosure document|item\s*7|item\s*19|fee stack|net contribution/i.test(disclaimer)) {
      disclaimer =
        explorerMergedBody(brand, 'economics.intro') ||
        (affiliationBrand
          ? 'Illustrative affiliation and participation considerations only—not a quote, financial model, or substitute for your agreement review. Use this tab to know what to ask and model, not what to sign.'
          : 'Illustrative brand-level patterns only—not a quote, financial model, or substitute for agreement review with the brand and your advisors. Use this tab to know what to ask and model, not what to sign.');
    }

    return wrapOe(
      '<section class="oe-section oe-section--econ-intro">' +
        explorerDetailCardMultiline('How to use this tab', disclaimer) +
        '</section>' +
      '<section class="oe-section">' +
        '<h2 class="oe-section-title">Typical Economics at a Glance</h2>' +
        '<p class="oe-section-hint">' + escapeHtml(econGlanceHint) + '</p>' +
        fddKpiRow +
        '</section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Cash &amp; Capital Rhythm</h2>' +
        '<p class="oe-section-hint">Who Typically Funds What—and When</p>' +
        '<div class="scenario-card-grid scenario-card-grid--owner-value econ-cash-grid">' +
        cashGrid +
        '</div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Opening &amp; Conversion Path</h2>' +
        '<p class="oe-section-hint">' +
        (openingHasStepDetail
          ? 'Milestone path for opening and conversion'
          : 'Typical opening and conversion process') +
        '</p>' +
        '<div class="timeline">' +
        openingTimeline +
        '</div>' +
        (openingBelowTimeline ? '<div style="margin-top:14px">' + openingBelowTimeline + '</div>' : '') +
        '</section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Fees in Three Buckets</h2>' +
        '<p class="oe-section-hint">Join · Operate · Change—Typical Fee Categories in Each Phase</p>' +
        '<div class="proof-points-grid proof-points-grid--econ-buckets">' +
        feeBucketGrid +
        '</div>' +
        '<p class="proof-meta-line">Ranges are brand-level typicals, not your property quote. Application, training, PIP, and incentives vary by deal.</p>' +
        '<div class="oe-cluster"><h3>What drives variability</h3><p class="explorer-detail-card__body">' +
        escapeHtml(fmtCell(feeVariability)).replace(/\n/g, '<br>') +
        '</p></div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Term, Renewal &amp; Exit Risk</h2>' +
        '<p class="oe-section-hint">Lock-in, performance, and liquidity themes—confirm in franchise agreement</p>' +
        '<div class="scenario-card-grid scenario-card-grid--owner-value">' +
        riskGrid +
        '</div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Negotiability &amp; Incentives</h2>' +
        '<p class="oe-section-hint">Where the conversation usually has room vs what tends to stay standard</p>' +
        '<div class="oe-cluster"><h3>Negotiation posture</h3><p class="explorer-detail-card__body">' +
        escapeHtml(fmtCell(negotiabilityBody)).replace(/\n\n/g, '</p><p class="explorer-detail-card__body">').replace(/\n/g, '<br>') +
        '</p></div>' +
        '<div class="oe-grid-2" style="margin-top:12px">' +
        '<div class="oe-cluster"><h3>Often Negotiated</h3>' +
        negotiableUl +
        '</div>' +
        '<div class="oe-cluster"><h3>Usually Standard</h3>' +
        rarelyUl +
        '</div></div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">' + confirmSectionTitle + '</h2>' +
        '<p class="oe-section-hint">' + escapeHtml(confirmSectionHint) + '</p>' +
        '<div class="explorer-detail-card">' +
        '<p class="explorer-detail-card__body">' + escapeHtml(confirmSectionBody) + '</p></div></section>'
    );
  }

  function renderLoyaltyProgram(brand) {
    var fp = brand.footprint || {};
    var fpFv = fp.formValues || {};
    var progNameEarly = lcFv(brand, 'typicalLoyaltyProgramName');
    var isChoiceFamily =
      String(brand && brand.parentCompany || '').toLowerCase().indexOf('choice hotels') >= 0 ||
      String(progNameEarly || '').toLowerCase().indexOf('choice privileges') >= 0 ||
      String(brand && brand.name || '').toLowerCase().indexOf('choice') >= 0;
    var isMarriottBonvoy =
      String(progNameEarly || '').toLowerCase().indexOf('bonvoy') >= 0 ||
      String(brand && brand.parentCompany || '').toLowerCase().indexOf('marriott') >= 0;
    var memM = lcFv(brand, 'totalGlobalMembersMillions');
    var defMembersVal = hasVal(memM)
      ? '~' + String(memM).trim() + 'M members (est.)'
      : isChoiceFamily
        ? '~70M members (program-wide)'
        : isMarriottBonvoy
          ? '200M+ members (Bonvoy program scale)'
          : '—';
    var openHotels = Number(fp.totalExistingHotels);
    var defHotelsVal = '—';
    if (hasVal(fp.totalExistingHotels) && openHotels > 0) {
      defHotelsVal = fmtNum(fp.totalExistingHotels) + '+ hotels in portfolio (open)';
    } else if (isChoiceFamily) {
      defHotelsVal = '7,100+ properties (Choice Privileges network)';
    } else if (isMarriottBonvoy) {
      defHotelsVal = 'Global member directory · Bonvoy participation varies';
    }
    var marketsN = fpFv.numberOfMarkets;
    var defMarketsVal = hasVal(marketsN)
      ? fmtNum(marketsN) +
        ' markets · ' +
        (hasVal(fpFv.specificMarkets)
          ? String(fpFv.specificMarkets).trim()
          : hasVal(fp.priorityCities)
            ? String(fp.priorityCities).trim()
            : 'regional mix')
      : isChoiceFamily
        ? 'Global · 20+ brands in portfolio'
        : '—';
    var mixPct = lcFv(brand, 'typicalLoyaltyRoomsPercent');
    var defMixVal = hasVal(mixPct)
      ? '~' + String(mixPct).trim() + '% of rooms from loyalty (est.)'
      : isMarriottBonvoy
        ? 'Varies by property · confirm during diligence'
        : '—';

    var heroRow = explorerFirstBlock(brand, 'loyalty.hero_title');
    var loyaltyH2;
    if (heroRow) {
      if (hasVal(heroRow.body)) loyaltyH2 = String(heroRow.body).trim();
      else if (hasVal(heroRow.title)) loyaltyH2 = String(heroRow.title).trim();
      else loyaltyH2 = '';
    }
    if (!loyaltyH2) {
      var progNameH = lcFv(brand, 'typicalLoyaltyProgramName');
      loyaltyH2 = (progNameH ? String(progNameH).trim() : 'Loyalty program') + ' — Loyalty at a Glance';
    }

    var kpiRow =
      '<div class="brand-markets-kpi" aria-label="Loyalty Program Snapshot">' +
      (function () {
        function kpiSlot(sk, defLbl, defVal) {
          var r = explorerFirstBlock(brand, sk);
          var lbl = defLbl;
          var val = defVal;
          if (r) {
            if (hasVal(r.title)) lbl = String(r.title).trim();
            if (hasVal(r.body)) val = String(r.body).trim();
          }
          return kpiCard(lbl, val);
        }
        return (
          kpiSlot('loyalty.kpi.members', 'Members (approx.)', defMembersVal) +
          kpiSlot('loyalty.kpi.hotels', 'Participating hotels (portfolio)', defHotelsVal) +
          kpiSlot('loyalty.kpi.markets', 'Markets & corridors', defMarketsVal) +
          kpiSlot('loyalty.kpi.mix', 'Typical loyalty mix (est.)', defMixVal)
        );
      })() +
      '</div>';

    var bn = brand.name ? String(brand.name) : 'This brand';
    var progName = lcFv(brand, 'typicalLoyaltyProgramName');
    var progDisplay = progName ? String(progName).trim() : 'the loyalty program';
    var ecoSlot = explorerMergedBody(brand, 'loyalty.ecosystem');
    var ecoInner = hasVal(ecoSlot)
      ? positionBody(ecoSlot)
      : positionBodyHtml(
          escapeHtml(bn) +
            ' participates in <strong style="color:var(--text,#fff);font-weight:600;">' +
            escapeHtml(progDisplay) +
            '</strong>—earn/redeem across the brand portfolio with member benefits, promotions, and program scale. For owners, the headline value is <strong style="color:var(--text,#fff);font-weight:600;">repeat demand and direct-channel contribution</strong> backed by a recognized program.'
        );
    var ownerLensSlot = explorerMergedBody(brand, 'loyalty.owner_lens');
    var ownerInner = hasVal(ownerLensSlot)
      ? positionBody(ownerLensSlot)
      : positionBodyHtml(
          'Brands typically position loyalty as a <strong style="color:var(--text,#fff);font-weight:600;">commercial system</strong>: member rates and packages that support direct contribution, corporate contracted demand, and a recognizable earn story that travels with the guest—especially in gateway and corporate-leisure crossover markets.'
        );

    var staticProofGrid =
      '<article class="proof-point-card">' +
      '<div class="proof-point-card__icon">◇</div>' +
      '<h3 class="proof-point-card__headline">Repeat Guest Capture</h3>' +
      '<p class="proof-point-card__support">Structured earn and recognition that increase the probability of second and third stays where the hotel already competes on experience.</p></article>' +
      '<article class="proof-point-card">' +
      '<div class="proof-point-card__icon">◇</div>' +
      '<h3 class="proof-point-card__headline">Direct Channel &amp; Member Pricing</h3>' +
      '<p class="proof-point-card__support">Brand-led booking paths and member constructs that help properties defend contribution against high-commission intermediaries in competitive urban markets.</p></article>' +
      '<article class="proof-point-card">' +
      '<div class="proof-point-card__icon">◇</div>' +
      '<h3 class="proof-point-card__headline">Elite Member Value</h3>' +
      '<p class="proof-point-card__support">Tiered program benefits (upgrades where available, late checkout, milestone rewards) that support contribution while staying consistent with premium positioning.</p></article>' +
      '<article class="proof-point-card">' +
      '<div class="proof-point-card__icon">◇</div>' +
      '<h3 class="proof-point-card__headline">Cross-Brand Traveler Flow</h3>' +
      '<p class="proof-point-card__support">Recognition across sister brands—meaningful for guests who split stays within the same network on recurring travel patterns.</p></article>' +
      '<article class="proof-point-card">' +
      '<div class="proof-point-card__icon">◇</div>' +
      '<h3 class="proof-point-card__headline">Cobrand &amp; Payment Scale</h3>' +
      '<p class="proof-point-card__support">Illustrative accelerators—credit-card partnerships and bonus-point campaigns that lift acquisition and keep the program top-of-wallet for high-value travelers.</p></article>' +
      '<article class="proof-point-card">' +
      '<div class="proof-point-card__icon">◇</div>' +
      '<h3 class="proof-point-card__headline">Corporate &amp; Small Meetings</h3>' +
      '<p class="proof-point-card__support">Account linkage and negotiated transient patterns that matter where corporate and SMERF mix supports midweek compression.</p></article>';
    var proofRows = explorerCardRowsForSlot(brand, 'loyalty.proof').filter(function (x) {
      return hasVal(x.title) || hasVal(x.body);
    });
    var proofGrid =
      proofRows.length > 0
        ? proofRows
            .map(function (x) {
              var h = hasVal(x.title) ? x.title : 'Proof';
              var b = x.body || '';
              var empty = !hasVal(b);
              var bodyText = empty ? '&nbsp;' : escapeHtml(fmtCell(b));
              if (!empty && bodyText.length > 420) bodyText = bodyText.slice(0, 417) + '…';
              return (
                '<article class="proof-point-card">' +
                '<div class="proof-point-card__icon">◇</div>' +
                '<h3 class="proof-point-card__headline">' +
                escapeHtml(h) +
                '</h3><p class="proof-point-card__support' +
                (empty ? ' oe-dd--empty' : '') +
                '">' +
                bodyText +
                '</p></article>'
              );
            })
            .join('')
        : staticProofGrid;

    var isChoicePrivileges =
      String(brand && brand.parentCompany || '').toLowerCase().indexOf('choice hotels') >= 0 ||
      String(progName || '').toLowerCase().indexOf('choice privileges') >= 0 ||
      String(brand && brand.name || '').toLowerCase().indexOf('choice') >= 0;

    var earnMerged = explorerMergedBody(brand, 'loyalty.earn', '\n');
    var earnUl = explorerLinesAsUl(escapeHtml, earnMerged);
    if (!earnUl) {
      if (isChoicePrivileges) {
        earnUl =
          '<ul>' +
          '<li><strong>Base Earn:</strong> Up to 10 points per US$1 on qualifying stays booked direct.</li>' +
          '<li><strong>Status Progression:</strong> Elite status can be earned via nights or Elite Qualifying Credits (EQCs).</li>' +
          '<li><strong>Program Accelerators:</strong> Campaign bonuses and card-linked EQCs may increase earn velocity by season.</li>' +
          '</ul>';
      } else {
        earnUl =
          '<ul>' +
          '<li><strong>Base Earn:</strong> 10 points per US$1 eligible spend on room and qualifying folio charges.</li>' +
          '<li><strong>Promotions:</strong> Seasonal accelerators on direct bookings (e.g. +2–4k bonus points per stay during campaign windows).</li>' +
          '<li><strong>Partners:</strong> Car, rideshare, and retail partners with periodic bonus campaigns.</li>' +
          '</ul>';
      }
    }
    var redeemMerged = explorerMergedBody(brand, 'loyalty.redeem', '\n');
    var redeemUl = explorerLinesAsUl(escapeHtml, redeemMerged);
    if (!redeemUl) {
      if (isChoicePrivileges) {
        redeemUl =
          '<ul>' +
          '<li><strong>Reward Nights:</strong> Redemption starts at approximately 8,000 points (property/date dependent).</li>' +
          '<li><strong>Cash + Points:</strong> Points + cash options are available on eligible stays where enabled.</li>' +
          '<li><strong>Additional Uses:</strong> Points can also be used for partner rewards and select non-room redemption paths.</li>' +
          '</ul>';
      } else {
        redeemUl =
          '<ul>' +
          '<li><strong>Free Nights:</strong> Dynamic award nights tied to demand (illustrative band: 22k–58k points/night by season).</li>' +
          '<li><strong>Cash + Points:</strong> Partial redemption options on direct paths where enabled.</li>' +
          '<li><strong>Experiences:</strong> Curated events and on-property credits in select tiers.</li>' +
          '</ul>';
      }
    }

    var eliteRows = explorerCardRowsForSlot(brand, 'loyalty.elite').filter(function (x) {
      return hasVal(x.title) || hasVal(x.body);
    });
    var elite =
      eliteRows.length > 0
        ? eliteRows
            .map(function (x) {
              var t = hasVal(x.title) ? x.title : 'Tier';
              var b = x.body || '';
              var inner = hasVal(b) ? escapeHtml(fmtCell(b)) : '&nbsp;';
              return (
                '<div class="diff-card"><strong>' + escapeHtml(t) + '</strong><br/>' + inner + '</div>'
              );
            })
            .join('')
        : isChoicePrivileges
          ? '<div class="diff-card"><strong>Gold</strong><br/>5 nights or 10,000 EQCs. Entry elite tier with bonus points and recognition benefits.</div>' +
            '<div class="diff-card"><strong>Platinum</strong><br/>15 nights or 30,000 EQCs. Higher bonus earn and stronger recognition benefits.</div>' +
            '<div class="diff-card"><strong>Diamond</strong><br/>35 nights or 70,000 EQCs. Top-tier mainstream recognition and elevated service benefits.</div>' +
            '<div class="diff-card"><strong>Titanium</strong><br/>55 nights or 110,000 EQCs. Highest published tier with premium program benefits.</div>'
          : '<div class="diff-card"><strong>Club / Silver</strong><br/>Member rates, milestone rewards path, baseline perks (illustrative).</div>' +
            '<div class="diff-card"><strong>Gold</strong><br/>Enhanced earn, welcome amenities pattern, better upgrade priority vs base tiers.</div>' +
            '<div class="diff-card"><strong>Platinum</strong><br/>Stronger upgrade priority, late checkout benefits, annual choice benefits (where program rules apply).</div>' +
            '<div class="diff-card"><strong>Diamond</strong><br/>Top-tier recognition—highest upgrade priority and dedicated support pathways where offered.</div>';

    var LOY_DEMAND = [
      ['Gateway Urban', 'Strong'],
      ['Corporate / Blended Transient', 'Strong'],
      ['Resort & Leisure Crossover', 'Moderate to strong'],
      ['Secondary / Drive-To Only', 'Selective'],
      ['Pure Commodity Highway', 'Limited'],
      ['Post-Conversion Repositioning', 'Strong']
    ];
    var loyaltyDemand = LOY_DEMAND.map(function (pair) {
      return demandCell(pair[0], pair[1]);
    }).join('');

    var implPnl =
      explorerMergedBody(brand, 'loyalty.implications.pnl') ||
      'Owners typically evaluate loyalty through contribution after program costs: member discounts, channel mix, and redemption liability assumptions. Brands highlight disciplined revenue management alignment so member constructs support rate integrity—not only top-line room nights.';
    var implOps =
      explorerMergedBody(brand, 'loyalty.implications.ops') ||
      'Front-of-house recognition, elite fulfillment, and digital app flows are where brand promises meet operating reality. Strong programs emphasize playbooks without homogenizing the property’s local character.';
    var implSys =
      explorerMergedBody(brand, 'loyalty.implications.systems') ||
      'Participation implies integration with CRS/PMS and campaign tooling—relevant for owners comparing affiliation to staying independent or white-label.';

    return wrapOe(
      '<section class="oe-section">' +
        '<h2 class="oe-section-title">' +
        loyaltyH2 +
        '</h2>' +
        '<p class="oe-section-hint">Public-Scale Snapshot · Not a Disclosure of Confidential Economics</p>' +
        kpiRow +
        '</section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Program Positioning</h2>' +
        '<div class="brand-positioning__stack">' +
        '<div class="brand-position-card">' +
        '<h3 class="brand-position-card__label">Ecosystem</h3>' +
        ecoInner +
        '</div>' +
        '<div class="brand-position-card">' +
        '<h3 class="brand-position-card__label">Owner Lens</h3>' +
        ownerInner +
        '</div></div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Key Benefits &amp; Program Strengths</h2>' +
        '<p class="oe-section-hint">Core Advantages Brands Highlight—How the Program Drives Value for Owners and Guests</p>' +
        '<div class="proof-points-grid">' +
        proofGrid +
        '</div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Sample Mechanics (Illustrative)</h2>' +
        '<p class="oe-section-hint">Rounded Examples Only—Actual Earn/Redeem Rules Vary by Market and Channel</p>' +
        '<div class="oe-grid-2">' +
        '<div class="oe-cluster"><h3>Earn (Examples)</h3>' +
        earnUl +
        '</div>' +
        '<div class="oe-cluster"><h3>Redeem (Examples)</h3>' +
        redeemUl +
        '</div></div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Elite Tiers — Illustrative Benefits</h2>' +
        '<div class="diff-grid">' +
        elite +
        '</div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Property &amp; Owner Implications</h2>' +
        '<div class="explorer-detail-stack">' +
        explorerDetailCard('P&L & Contribution', implPnl) +
        explorerDetailCard('Operations & Guest Experience', implOps) +
        explorerDetailCard('Systems & Data', implSys) +
        '</div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Where Loyalty Lifts Demand Most</h2>' +
        '<p class="oe-section-hint">Directional Fit Labels</p>' +
        '<div class="demand-matrix">' +
        loyaltyDemand +
        '</div></section>'
    );
  }

  /**
   * footprint.momentum Body: line 1 = date label; blank line; description; optional blank line; https URL (announcement).
   * Title = headline (strong).
   * Also recovers space-collapsed / single-newline bodies produced by older scrubbers.
   */
  function looksLikeMomentumDateLabel(text) {
    if (!hasVal(text)) return false;
    var t = String(text).trim();
    if (t.length > 56 || /https?:\/\//i.test(t)) return false;
    if (/^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b/i.test(t)) return true;
    if (/^Q[1-4]\b/i.test(t)) return true;
    if (/^\d{4}(\s*[–—-]\s*\d{4})?$/.test(t)) return true;
    if (/^(Editorial|Collection|Directory|Affiliation|Pipeline|Member growth|CALA)$/i.test(t)) return true;
    return t.split(/\s+/).length <= 5 && t.length <= 36 && !/[.!?]$/.test(t);
  }

  function splitMomentumBodyUnits(rawBody) {
    var raw = String(rawBody || '').trim();
    if (!raw) return [];
    var paras = raw
      .split(/\n\n+/)
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean);
    if (paras.length > 1) return paras;

    var lines = raw
      .split(/\n/)
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean);
    if (lines.length > 1) return lines;

    // Space-collapsed: "2024 Summary…" or "2024–2025 Summary… https://…"
    var leadingDate = raw.match(/^(\d{4}(?:\s*[–—-]\s*\d{4})?)\s+([\s\S]+)$/);
    if (leadingDate) {
      var rest = leadingDate[2].trim();
      var trailingUrl = rest.match(/^(.*?)\s+(https?:\/\/\S+)\s*$/i);
      if (trailingUrl) {
        return [leadingDate[1], trailingUrl[1].trim(), trailingUrl[2].trim()].filter(Boolean);
      }
      return [leadingDate[1], rest];
    }
    return paras.length ? paras : [raw];
  }

  function parseMomentumPresentationBlock(block) {
    var headline = block && hasVal(block.title) ? String(block.title).trim() : '';
    var paras = splitMomentumBodyUnits((block && block.body) || '');
    var date = paras.length ? paras[0] : '';
    var url = '';
    var descParts = [];
    for (var i = 1; i < paras.length; i++) {
      if (isSafeHttpUrl(paras[i])) url = paras[i].trim();
      else descParts.push(paras[i]);
    }
    if (paras.length === 1 && !descParts.length) {
      if (headline && !looksLikeMomentumDateLabel(date)) {
        descParts = [date];
        date = '';
      } else if (!headline && !looksLikeMomentumDateLabel(date)) {
        descParts = [date];
        date = '';
      }
    }
    if (!descParts.length && date) {
      var fauxStatus = date.match(/^(Ongoing|Active|Collection)\s+([\s\S]+)$/i);
      if (fauxStatus) {
        date = fauxStatus[1];
        descParts = [fauxStatus[2].trim()];
      }
    }
    if (/^ongoing$/i.test(date) && !descParts.length && paras.length === 1) {
      date = '';
    }
    if (date && !looksLikeMomentumDateLabel(date) && descParts.length) {
      descParts = [date].concat(descParts);
      date = '';
    }
    // Defense: recover trailing URL stuck inside description (collapsed body / sandbox gaps).
    if (!url && descParts.length) {
      var lastIdx = descParts.length - 1;
      var splitLast = splitTrailingHttpUrl(descParts[lastIdx]);
      if (splitLast.url && isSafeHttpUrl(splitLast.url)) {
        url = splitLast.url;
        if (splitLast.text) descParts[lastIdx] = splitLast.text;
        else descParts.pop();
      }
    }
    var description = descParts.join('\n\n');
    if (url) {
      // Never leave the captured URL (or any raw URL line) in public body copy.
      description = description
        .split(/\n+/)
        .map(function (line) {
          return String(line || '').trim();
        })
        .filter(function (line) {
          return line && !isSafeHttpUrl(line) && line.indexOf(url) === -1;
        })
        .join('\n\n');
    } else {
      var scrubAll = splitTrailingHttpUrl(description);
      if (scrubAll.url && isSafeHttpUrl(scrubAll.url)) {
        url = scrubAll.url;
        description = scrubAll.text;
      }
    }
    return {
      date: date,
      headline: headline,
      description: description,
      url: url
    };
  }

  function parsePortfolioMixEntry(row) {
    if (!row) return null;
    var category = hasVal(row.title) ? String(row.title).trim() : '';
    var level = hasVal(row.body) ? String(row.body).trim() : '';
    if (!category && level) {
      var pipe = level.split('|');
      if (pipe.length >= 2) {
        category = pipe[0].trim();
        level = pipe.slice(1).join('|').trim();
      } else {
        var colon = level.indexOf(':');
        if (colon > 0) {
          category = level.slice(0, colon).trim();
          level = level.slice(colon + 1).trim();
        }
      }
    }
    if (!category && !level) return null;
    return { category: category, level: level };
  }

  function portfolioMixPillsFromBrand(brand) {
    var rows = explorerCardRowsForSlot(brand, FOOTPRINT_PORTFOLIO_MIX_SLOT);
    if (!rows.length) return [];
    var pills = rows.map(parsePortfolioMixEntry).filter(Boolean);
    if (pills.length) return pills;
    var merged = explorerMergedBody(brand, FOOTPRINT_PORTFOLIO_MIX_SLOT, '\n');
    if (!hasVal(merged)) return [];
    return String(merged)
      .split(/\n+/)
      .map(function (line) {
        return parsePortfolioMixEntry({ title: '', body: line.trim() });
      })
      .filter(Boolean);
  }

  function portfolioMixSectionHtml(brand) {
    var pills = portfolioMixPillsFromBrand(brand);
    if (!pills.length) {
      if (shouldRenderFullProfile(brand)) return '';
      return (
        '<div class="portfolio-mix">' +
        '<h3>Portfolio Mix</h3>' +
        '<div class="portfolio-mix__row">' +
        '<span class="portfolio-mix__pill oe-dd--empty">&nbsp;</span>' +
        '</div></div>'
      );
    }
    var rowInner = pills
      .map(function (p) {
        var level = hasVal(p.level) ? ' ' + escapeHtml(p.level) : '';
        return (
          '<span class="portfolio-mix__pill"><strong>' +
          escapeHtml(p.category) +
          '</strong>' +
          level +
          '</span>'
        );
      })
      .join('');
    return (
      '<div class="portfolio-mix">' +
      '<h3>Portfolio Mix</h3>' +
      '<div class="portfolio-mix__row">' +
      rowInner +
      '</div></div>'
    );
  }

  /** Parent company or brand name for momentum link / section labels. */
  function momentumPublisherDisplayName(brand) {
    var parent = String((brand && brand.parentCompany) || '').trim();
    var brandName = String((brand && brand.name) || '').trim();
    var parentKey = normalizePortfolioParentKey(parent);
    if (isIhgParentCompanyKey(parentKey)) return 'IHG';
    if (parentKey.indexOf('choice hotels') !== -1) return 'Choice Hotels';
    if (parent) return parent;
    if (brandName) return brandName;
    return '';
  }

  function isMomentumPressOrNewsroomUrl(url) {
    var u = String(url || '').toLowerCase();
    return (
      u.indexOf('newsroom') !== -1 ||
      u.indexOf('press-release') !== -1 ||
      u.indexOf('press_release') !== -1 ||
      u.indexOf('/news/') !== -1 ||
      u.indexOf('media.choicehotels.com') !== -1 ||
      u.indexOf('ihgplc.com/news') !== -1 ||
      u.indexOf('marriott.com/newsroom') !== -1
    );
  }

  function isMarriottPropertyPageUrl(url) {
    var u = String(url || '').toLowerCase();
    return u.indexOf('marriott.com') !== -1 && u.indexOf('/hotels/') !== -1;
  }

  function isPrWireUrl(url) {
    var u = String(url || '').toLowerCase();
    return (
      u.indexOf('prnewswire.com') !== -1 ||
      u.indexOf('globenewswire.com') !== -1 ||
      u.indexOf('businesswire.com') !== -1
    );
  }

  function isMarriottPressAreaUrl(url) {
    var u = String(url || '').toLowerCase();
    return u.indexOf('marriott.pressarea.com') !== -1 || u.indexOf('marriott.africa-newsroom.com') !== -1;
  }

  function isOwnerPressUrl(url) {
    var u = String(url || '').toLowerCase();
    return u.indexOf('hotel-online.com/press') !== -1;
  }

  function isHospitalityTradeUrl(url) {
    var u = String(url || '').toLowerCase();
    return (
      u.indexOf('travelweekly.com') !== -1 ||
      u.indexOf('traveldailynews.com') !== -1 ||
      u.indexOf('hotel-online.com/news') !== -1 ||
      u.indexOf('ladevi.info') !== -1 ||
      u.indexOf('semana.com') !== -1 ||
      u.indexOf('ithic.it') !== -1 ||
      u.indexOf('journaldespalaces.com') !== -1 ||
      u.indexOf('hotelmanagement-network.com') !== -1 ||
      u.indexOf('travelprnews.com') !== -1 ||
      u.indexOf('breakingtravelnews.com') !== -1 ||
      u.indexOf('hotelnewsresource.com') !== -1 ||
      u.indexOf('newsismybusiness.com') !== -1
    );
  }

  function isMarriottNewsroomUrl(url) {
    var u = String(url || '').toLowerCase();
    return (
      u.indexOf('marriott.com/newsroom') !== -1 ||
      u.indexOf('marriott.pressarea.com') !== -1 ||
      u.indexOf('marriott.africa-newsroom.com') !== -1
    );
  }

  function isMarriottPrWireRelease(url) {
    if (!isPrWireUrl(url)) return false;
    var u = String(url || '').toLowerCase();
    return /marriott|tribute[-_]portfolio/.test(u);
  }

  /** Choice property listing market from URL path (e.g. cartagena, panama-city). v31M-R2 parity. */
  function choiceHotelsPropertyMarketFromUrl(url) {
    var u = String(url || '');
    var m = u.match(/choicehotels\.com\/[^/]+\/([^/]+)\/radisson-individuals-hotels\//i);
    if (!m) return '';
    return m[1]
      .replace(/-/g, ' ')
      .split(/\s+/)
      .filter(Boolean)
      .map(function (w) {
        return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
      })
      .join(' ');
  }

  function designHotelsPropertyMarketFromUrl(url) {
    var u = String(url || '');
    var m = u.match(/designhotels\.com\/hotels\/[^/]+\/([^/]+)\//i);
    if (!m) return '';
    return m[1]
      .replace(/-/g, ' ')
      .split(/\s+/)
      .filter(Boolean)
      .map(function (w) {
        return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
      })
      .join(' ');
  }

  function momentumAnnouncementLinkLabel(url, brand) {
    function properCaseLinkLabel(label) {
      return String(label || '')
        .split(/\s+/)
        .filter(Boolean)
        .map(function (w) {
          if (/^IHG$/i.test(w)) return 'IHG';
          if (/^eHotelier$/i.test(w)) return 'eHotelier';
          if (/^PR$/i.test(w)) return 'PR';
          return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
        })
        .join(' ');
    }
    var u = String(url || '').toLowerCase();
    var raw = '';
    if (u.indexOf('designhotels.com') !== -1 && u.indexOf('/hotels/') !== -1) {
      var dhMarket = designHotelsPropertyMarketFromUrl(url);
      raw = dhMarket ? 'View ' + dhMarket + ' Property Listing' : 'View Design Hotels Property Listing';
    } else if (u.indexOf('einpresswire.com') !== -1) {
      raw = 'View Press Release';
    } else if (u.indexOf('businesstraveller.com') !== -1) {
      raw = 'View Article';
    } else if (isMarriottPropertyPageUrl(url)) {
      raw = 'View Property';
    } else if (u.indexOf('tribute-portfolio.marriott.com') !== -1) {
      raw = 'View Tribute Portfolio Site';
    } else if (isOwnerPressUrl(url)) {
      raw = 'View Owner Announcement';
    } else if (isHospitalityTradeUrl(url)) {
      raw = 'View Article';
    } else if (isMarriottNewsroomUrl(url) || isMarriottPressAreaUrl(url) || isMarriottPrWireRelease(url)) {
      raw = 'View Marriott Announcement';
    } else if (
      u.indexOf('ihgplc.com') !== -1 ||
      u.indexOf('ihg.com') !== -1 ||
      u.indexOf('kimptonhotels.com') !== -1
    ) {
      raw = 'View IHG Announcement';
    } else if (u.indexOf('hotelbusiness.com') !== -1) {
      raw = 'View Hotel Business Article';
    } else if (u.indexOf('hotelmanagement.net') !== -1) {
      raw = 'View Hotel Management Article';
    } else if (u.indexOf('lodgingmagazine.com') !== -1) {
      raw = 'View Lodging Article';
    } else if (u.indexOf('insights.ehotelier.com') !== -1) {
      raw = 'View eHotelier Article';
    } else if (u.indexOf('choicehotelsdevelopment.com') !== -1) {
      raw = 'View Choice Development News';
    } else if (u.indexOf('press-kit') !== -1 || u.indexOf('press_kit') !== -1) {
      raw = 'View Choice Hotels Press Kit';
    } else if (u.indexOf('choicehotels.com') !== -1 && /\/radisson-individuals-hotels\//.test(u)) {
      var choiceMarket = choiceHotelsPropertyMarketFromUrl(url);
      raw = choiceMarket ? 'View ' + choiceMarket + ' Property Listing' : 'View Property Listing';
    } else if (
      u.indexOf('choicehotels.com') !== -1 &&
      (u.indexOf('/news') !== -1 || u.indexOf('newsroom') !== -1)
    ) {
      raw = 'View Choice Hotels Press Release';
    } else if (u.indexOf('media.choicehotels.com') !== -1) {
      raw = 'View Choice Hotels Press Release';
    } else if (u.indexOf('investor.choicehotels.com') !== -1) {
      raw = 'View Choice Hotels Press Release';
    } else if (u.indexOf('choicehotels.com') !== -1) {
      raw = 'View Choice Hotels Source';
    } else if (u.indexOf('press.accor.com') !== -1 || u.indexOf('accor.com') !== -1) {
      raw = 'View Accor Announcement';
    } else if (u.indexOf('travelpulse.com') !== -1) {
      raw = 'View Article';
    } else if (isMomentumPressOrNewsroomUrl(url)) {
      var publisher = momentumPublisherDisplayName(brand);
      raw = publisher ? 'View ' + publisher + ' Announcement' : 'View Announcement';
    } else if (isPrWireUrl(url)) {
      raw = 'View Press Release';
    } else if (u.indexOf('marriott.com') !== -1) {
      raw = 'View Marriott Source';
    } else {
      raw = 'View Source';
    }
    return properCaseLinkLabel(raw);
  }

  function momentumSectionDefaultLabel(brand) {
    var publisher = momentumPublisherDisplayName(brand);
    var parentKey = normalizePortfolioParentKey(String((brand && brand.parentCompany) || ''));
    var brandName = String((brand && brand.name) || '').toLowerCase();
    if (brandName.indexOf('design hotels') >= 0) {
      return 'Recent openings · linked announcements';
    }
    if (parentKey.indexOf('marriott') !== -1) {
      return 'Recent portfolio momentum · linked announcements';
    }
    if (publisher) return publisher + ' CALA openings · linked announcements';
    return 'Recent openings · linked announcements';
  }

  function momentumFeedItemHtml(item, brand) {
    if (!item || (!hasVal(item.headline) && !hasVal(item.description))) return '';
    var descP = hasVal(item.description)
      ? '<p>' + escapeHtml(item.description) + '</p>'
      : '';
    var linkP = isSafeHttpUrl(item.url)
      ? '<p class="momentum-feed__link"><a href="' +
        escapeHtml(item.url) +
        '" target="_blank" rel="noopener noreferrer">' +
        escapeHtml(momentumAnnouncementLinkLabel(item.url, brand)) +
        '</a></p>'
      : '';
    var itemClass =
      'momentum-feed__item' + (hasVal(item.date) ? '' : ' momentum-feed__item--no-date');
    return (
      '<div class="' +
      itemClass +
      '">' +
      (hasVal(item.date)
        ? '<div class="momentum-feed__date">' + escapeHtml(item.date) + '</div>'
        : '') +
      '<div class="momentum-feed__body">' +
      (hasVal(item.headline) ? '<strong>' + escapeHtml(item.headline) + '</strong>' : '') +
      descP +
      linkP +
      '</div></div>'
    );
  }

  function momentumDateSortKey(dateLabel) {
    var t = String(dateLabel || '').trim();
    if (!t) return 0;
    var range = t.match(/^(\d{4})\s*[–—-]\s*(\d{4})$/);
    if (range) return parseInt(range[2], 10) * 100 + 12;
    var mon = t.match(
      /^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(\d{4})$/i
    );
    if (mon) {
      var monthMap = {
        jan: 1,
        feb: 2,
        mar: 3,
        apr: 4,
        may: 5,
        jun: 6,
        jul: 7,
        aug: 8,
        sep: 9,
        oct: 10,
        nov: 11,
        dec: 12
      };
      var m = monthMap[mon[1].slice(0, 3).toLowerCase()] || 12;
      return parseInt(mon[2], 10) * 100 + m;
    }
    var q = t.match(/^Q([1-4])\s+(\d{4})$/i);
    if (q) return parseInt(q[2], 10) * 100 + parseInt(q[1], 10) * 3;
    var y = t.match(/^(\d{4})$/);
    if (y) return parseInt(y[1], 10) * 100 + 12;
    return 0;
  }

  function renderMomentumSection(brand) {
    var blocks = explorerBlocksForSlot(brand, FOOTPRINT_MOMENTUM_SLOT);
    var labelRaw = explorerPresentationLine(brand, FOOTPRINT_MOMENTUM_LABEL_SLOT);
    var label = hasVal(labelRaw) ? labelRaw : momentumSectionDefaultLabel(brand);
    var parsedItems = blocks
      .map(function (b) {
        return parseMomentumPresentationBlock(b);
      })
      .filter(function (item) {
        return item && (hasVal(item.headline) || hasVal(item.description));
      })
      .sort(function (a, b) {
        var db = momentumDateSortKey(b.date);
        var da = momentumDateSortKey(a.date);
        if (db !== da) return db - da;
        return String(b.headline || '').localeCompare(String(a.headline || ''));
      });
    var itemsHtml = parsedItems
      .map(function (item) {
        return momentumFeedItemHtml(item, brand);
      })
      .filter(Boolean)
      .join('');
    if (!itemsHtml) {
      var mixOnly = portfolioMixSectionHtml(brand);
      if (shouldRenderFullProfile(brand)) {
        if (!String(mixOnly || '').trim()) return '';
        return (
          '<section class="oe-section">' +
          '<h2 class="oe-section-title">Recent Momentum</h2>' +
          '<p class="oe-section-hint">Publicly disclosed recent activity is limited</p>' +
          mixOnly +
          '</section>'
        );
      }
      return (
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Recent Momentum</h2>' +
        '<p class="oe-section-hint">Publicly disclosed recent activity is limited</p>' +
        '<p class="momentum-feed__label oe-dd--empty">&nbsp;</p>' +
        '<div class="momentum-feed oe-dd--empty">&nbsp;</div>' +
        mixOnly +
        '</section>'
      );
    }
    return (
      '<section class="oe-section">' +
      '<h2 class="oe-section-title">Recent Momentum</h2>' +
      '<p class="oe-section-hint">Recent openings and development signals</p>' +
      '<p class="momentum-feed__label">' +
      escapeHtml(label) +
      '</p>' +
      '<div class="momentum-feed">' +
      itemsHtml +
      '</div>' +
      portfolioMixSectionHtml(brand) +
      '</section>'
    );
  }

  function fpMetricNum(v) {
    var n = Number(v);
    return Number.isFinite(n) ? n : 0;
  }

  function fpMetricCell(v) {
    if (v == null || v === '') return '&nbsp;';
    var n = Number(v);
    if (!Number.isFinite(n)) return '&nbsp;';
    return escapeHtml(fmtNum(n));
  }

  function fpMetricAvgKeys(hotels, rooms) {
    var h = fpMetricNum(hotels);
    var r = fpMetricNum(rooms);
    if (h <= 0) return '&nbsp;';
    return escapeHtml(fmtNum(Math.round(r / h)));
  }

  var FP_METRIC_COLGROUP =
    '<colgroup><col class="brand-fp-col-label" /><col class="brand-fp-col-metric" span="7" /></colgroup>';

  var FP_METRIC_HEADER =
    '<thead><tr>' +
    '<th scope="col">Region</th>' +
    '<th scope="col">Open Hotels</th>' +
    '<th scope="col">Open Rooms</th>' +
    '<th scope="col">Pipeline Hotels</th>' +
    '<th scope="col">Pipeline Rooms</th>' +
    '<th scope="col">Total Hotels</th>' +
    '<th scope="col">Total Rooms</th>' +
    '<th scope="col">Avg Keys</th>' +
    '</tr></thead><tbody>';

  function fpMetricRowHtml(label, openH, openR, pipeH, pipeR, isTotal) {
    var totalH = fpMetricNum(openH) + fpMetricNum(pipeH);
    var totalR = fpMetricNum(openR) + fpMetricNum(pipeR);
    return (
      '<tr' +
      (isTotal ? ' class="brand-ft-total-row"' : '') +
      '>' +
      '<th scope="row">' +
      escapeHtml(label) +
      '</th><td>' +
      fpMetricCell(openH) +
      '</td><td>' +
      fpMetricCell(openR) +
      '</td><td>' +
      fpMetricCell(pipeH) +
      '</td><td>' +
      fpMetricCell(pipeR) +
      '</td><td>' +
      fpMetricCell(totalH) +
      '</td><td>' +
      fpMetricCell(totalR) +
      '</td><td>' +
      (isTotal && totalH <= 0 ? '&nbsp;' : fpMetricAvgKeys(totalH, totalR)) +
      '</td></tr>'
    );
  }

  function resolveFootprintDisplay(brand) {
    var M = typeof BrandExplorerCensusMetrics !== 'undefined' ? BrandExplorerCensusMetrics : null;
    if (M && typeof M.footprintDisplayModel === 'function') {
      return M.footprintDisplayModel(brand);
    }
    return {
      fp: brand.footprint || {},
      showVerifiedMetrics: true,
      displaySourceLabel: null,
      sourceNote: null,
      countryBreakdown: null,
      locationTypeBreakdown: null
    };
  }

  function footprintPipelineTotals(fp) {
    var M = typeof BrandExplorerCensusMetrics !== 'undefined' ? BrandExplorerCensusMetrics : null;
    if (M && typeof M.footprintPipelineTotals === 'function') {
      return M.footprintPipelineTotals(fp);
    }
    if (!fp || typeof fp !== 'object') return { hotels: 0, rooms: 0 };
    return {
      hotels: fpMetricNum(fp.totalNewBuildHotels) + fpMetricNum(fp.totalConversionHotels),
      rooms: fpMetricNum(fp.totalNewBuildRooms) + fpMetricNum(fp.totalConversionRooms)
    };
  }

  function footprintMetricsNoticeHtml(disp) {
    if (!disp) return '';
    var M = typeof BrandExplorerCensusMetrics !== 'undefined' ? BrandExplorerCensusMetrics : null;
    if (!M || typeof M.unverifiedFootprintHtml !== 'function') return '';
    var msg =
      disp.metricsBanner ||
      (disp.showVerifiedMetrics ? disp.censusBreakdownNotice : null) ||
      (!disp.showVerifiedMetrics ? disp.verifiedEmptyMessage : null);
    if (!msg) return '';
    return M.unverifiedFootprintHtml(msg);
  }

  function buildFpRowsFromCensusBreakdown(breakdownRows) {
    if (!breakdownRows || !breakdownRows.length) return '';
    var M = typeof BrandExplorerCensusMetrics !== 'undefined' ? BrandExplorerCensusMetrics : null;
    var sorted =
      M && typeof M.sortBreakdownForPortfolioDisplay === 'function'
        ? M.sortBreakdownForPortfolioDisplay(breakdownRows)
        : breakdownRows;
    var sumOpenH = 0;
    var sumOpenR = 0;
    var sumPipeH = 0;
    var sumPipeR = 0;
    var body = sorted
      .map(function (row) {
        var n =
          M && typeof M.normalizeBreakdownRow === 'function' ? M.normalizeBreakdownRow(row) : null;
        if (!n && row && row.label) {
          n = {
            label: row.label,
            hotels: Number(row.hotels) || 0,
            keys: Number(row.keys) || 0,
            pipelineHotels: Number(row.pipelineHotels) || 0,
            pipelineKeys: Number(row.pipelineKeys) || 0
          };
        }
        if (!n || !n.label) return '';
        sumOpenH += n.hotels;
        sumOpenR += n.keys;
        sumPipeH += n.pipelineHotels;
        sumPipeR += n.pipelineKeys;
        return fpMetricRowHtml(n.label, n.hotels, n.keys, n.pipelineHotels, n.pipelineKeys, false);
      })
      .join('');
    if (!body) return '';
    body += fpMetricRowHtml('Total (Portfolio)', sumOpenH, sumOpenR, sumPipeH, sumPipeR, true);
    return body;
  }

  function buildFpRegionDistributionRows(reg, regionKeys) {
    if (!regionKeys.length) return '';
    var sorted = regionKeys.slice().sort(function (a, b) {
      var ta = fpMetricNum((reg[a] || {}).hotels) + fpMetricNum((reg[a] || {}).pipelineHotels);
      var tb = fpMetricNum((reg[b] || {}).hotels) + fpMetricNum((reg[b] || {}).pipelineHotels);
      return tb - ta;
    });
    var sumOpenH = 0;
    var sumOpenR = 0;
    var sumPipeH = 0;
    var sumPipeR = 0;
    var body = sorted
      .map(function (region) {
        var o = reg[region] || {};
        var openH = fpMetricNum(o.hotels);
        var openR = fpMetricNum(o.rooms);
        var pipeH = fpMetricNum(o.pipelineHotels);
        var pipeR = fpMetricNum(o.pipelineRooms);
        sumOpenH += openH;
        sumOpenR += openR;
        sumPipeH += pipeH;
        sumPipeR += pipeR;
        return fpMetricRowHtml(region, openH, openR, pipeH, pipeR, false);
      })
      .join('');
    body += fpMetricRowHtml('Total (Portfolio)', sumOpenH, sumOpenR, sumPipeH, sumPipeR, true);
    return body;
  }

  function renderFootprintGrowth(brand, footprintPropertyPayloadSink) {
    var disp = resolveFootprintDisplay(brand);
    var fp = disp.fp || {};
    var metricsNotice = footprintMetricsNoticeHtml(disp);
    var fv = fp.formValues || {};
    var pipeTotals = footprintPipelineTotals(fp);
    var pipH = pipeTotals.hotels;
    var pipR = pipeTotals.rooms;
    var reg = fp.regionalDistribution && typeof fp.regionalDistribution === 'object' ? fp.regionalDistribution : {};
    var regionKeys = Object.keys(reg);
    var regionsSummary =
      regionKeys.length > 0
        ? regionKeys.slice(0, 8).join(' · ')
        : hasVal(fv.specificMarkets)
          ? String(fv.specificMarkets).trim()
          : '';
    var suppressEmptyKpi = shouldRenderFullProfile(brand);
    var kpiRow =
      '<div class="brand-markets-kpi" aria-label="Markets and Footprint Summary">' +
      kpiCard('Regions', regionsSummary, { suppressEmpty: suppressEmptyKpi }) +
      kpiCard('Markets Operated In', fv.numberOfMarkets, { suppressEmpty: suppressEmptyKpi }) +
      kpiCard('Open Hotels (Public YE2025)', fp.totalExistingHotels > 0 ? fp.totalExistingHotels : '', {
        suppressEmpty: suppressEmptyKpi,
      }) +
      kpiCard(
        'Coverage Model',
        [brand.brandModelFormat, brand.hotelChainScale].filter(hasVal).join(' · '),
        { suppressEmpty: suppressEmptyKpi }
      ) +
      '</div>';
    var openH = fp.totalExistingHotels;
    var openR = fp.totalExistingRooms;
    var portfolioTotalsRow = fpMetricRowHtml('Total (Portfolio)', openH, openR, pipH, pipR, true);
    var fp8Empty =
      '<tr><th scope="row">&nbsp;</th><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td></tr>';
    var fp8HeadCountry =
      '<thead><tr>' +
      '<th scope="col">Country</th>' +
      '<th scope="col">Open Hotels</th>' +
      '<th scope="col">Open Rooms</th>' +
      '<th scope="col">Pipeline Hotels</th>' +
      '<th scope="col">Pipeline Rooms</th>' +
      '<th scope="col">Total Hotels</th>' +
      '<th scope="col">Total Rooms</th>' +
      '<th scope="col">Avg Keys</th>' +
      '</tr></thead><tbody>';
    var fp8HeadArchetype =
      '<thead><tr>' +
      '<th scope="col">Asset Archetype</th>' +
      '<th scope="col">Open Hotels</th>' +
      '<th scope="col">Open Rooms</th>' +
      '<th scope="col">Pipeline Hotels</th>' +
      '<th scope="col">Pipeline Rooms</th>' +
      '<th scope="col">Total Hotels</th>' +
      '<th scope="col">Total Rooms</th>' +
      '<th scope="col">Avg Keys</th>' +
      '</tr></thead><tbody>';
    var distRows = regionKeys.length ? buildFpRegionDistributionRows(reg, regionKeys) : fp8Empty;
    var countryRows = disp.countryBreakdown
      ? buildFpRowsFromCensusBreakdown(disp.countryBreakdown)
      : '';
    var archetypeRows = disp.locationTypeBreakdown
      ? buildFpRowsFromCensusBreakdown(disp.locationTypeBreakdown)
      : '';
    var regionPanel =
      '<div class="brand-fp-table-wrap brand-fp-panel brand-fp-panel-region" role="tabpanel" aria-label="By Region">' +
      '<table class="brand-fp-table brand-fp-table--metrics">' +
      FP_METRIC_COLGROUP +
      FP_METRIC_HEADER +
      distRows +
      '</tbody></table></div>';
    var countryPanel =
      '<div class="brand-fp-table-wrap brand-fp-panel brand-fp-panel-country" role="tabpanel" aria-label="By Country">' +
      '<table class="brand-fp-table brand-fp-table--metrics">' +
      FP_METRIC_COLGROUP +
      fp8HeadCountry +
      (countryRows || fp8Empty) +
      '</tbody></table></div>';
    var archetypePanel =
      '<div class="brand-fp-table-wrap brand-fp-panel brand-fp-panel-archetype" role="tabpanel" aria-label="By Asset Archetype">' +
      '<table class="brand-fp-table brand-fp-table--metrics">' +
      FP_METRIC_COLGROUP +
      fp8HeadArchetype +
      (archetypeRows || fp8Empty) +
      '</tbody></table></div>';
    var fpSourceNote = disp.sourceNote
      ? '<p class="brand-fp-table-note brand-fp-source-note">' + escapeHtml(disp.sourceNote) + '</p>'
      : '';
    var portfolioDistribution =
      '<div class="brand-fp-subsection brand-fp-distribution be-atelier-fp-dist">' +
      '<div class="brand-fp-distribution-head">' +
      '<h3 class="brand-fp-table-title">Portfolio Distribution</h3>' +
      '<p class="brand-fp-view-hint">View By Region, Country, or Asset Archetype</p>' +
      '</div>' +
      '<input type="radio" name="beAtFpView" id="beAtFpReg" class="brand-fp-view-input" checked />' +
      '<input type="radio" name="beAtFpView" id="beAtFpCtry" class="brand-fp-view-input" />' +
      '<input type="radio" name="beAtFpView" id="beAtFpArch" class="brand-fp-view-input" />' +
      '<div class="brand-fp-view-toggle" role="tablist" aria-label="Portfolio Distribution View">' +
      '<label class="brand-fp-view-label" for="beAtFpReg">Region</label>' +
      '<label class="brand-fp-view-label" for="beAtFpCtry">Country</label>' +
      '<label class="brand-fp-view-label" for="beAtFpArch">Asset Archetype</label>' +
      '</div>' +
      regionPanel +
      countryPanel +
      archetypePanel +
      fpSourceNote +
      '<p class="brand-fp-table-note">Illustrative Directional View · Not Audited Financials or Property-Level Disclosure</p>' +
      '</div>';
    var openPipelineSub =
      '<div class="brand-fp-subsection">' +
      '<h3 class="brand-fp-table-title">Open vs. Pipeline (Portfolio)</h3>' +
      '<div class="brand-fp-table-wrap" role="region" aria-label="Open Versus Pipeline Portfolio Totals">' +
      '<table class="brand-fp-table brand-fp-table--metrics">' +
      FP_METRIC_COLGROUP +
      FP_METRIC_HEADER +
      portfolioTotalsRow +
      '</tbody></table></div></div>';
    var metricsBlock =
      '<div class="brand-fp-metrics">' +
      metricsNotice +
      (disp.showVerifiedMetrics
        ? openPipelineSub + portfolioDistribution
        : '<p class="brand-fp-table-note">Footprint tables are hidden until portfolio metrics are verified.</p>') +
      '</div>';
    var suppressEmptyFp = shouldRenderFullProfile(brand);
    var presenceRow =
      '<div class="presence-intel-row">' +
      presenceIntelFootprintMetric('Open Hotels (Public)', openH, openR) +
      presenceIntelFootprintMetric('Pipeline (Public)', pipH, pipR) +
      presenceIntelCard('Primary Regions', regionsSummary, { suppressEmpty: suppressEmptyFp }) +
      presenceIntelCard('Typical Asset Pattern', brand.hotelServiceModel, { suppressEmpty: suppressEmptyFp }) +
      presenceIntelCard('Growth Style', brand.brandDevelopmentStage, { suppressEmpty: suppressEmptyFp }) +
      presenceIntelCard('Brand Maturity', brand.yearBrandLaunched, { suppressEmpty: suppressEmptyFp }) +
      '</div>';
    var geoSrc = String(explorerMergedBody(brand, 'footprint.geo_intro') || fv.specificMarkets || '').trim();
    var geoIntro;
    if (!hasVal(geoSrc)) {
      geoIntro = suppressEmptyFp
        ? ''
        : '<p style="font-size:0.8125rem;color:#d7e4fa;margin:0 0 14px;max-width:720px;line-height:1.5" class="oe-dd--empty">&nbsp;</p>';
    } else {
      var geoSn = geoSrc.slice(0, 420);
      geoIntro =
        '<p style="font-size:0.8125rem;color:#d7e4fa;margin:0 0 14px;max-width:720px;line-height:1.5">' +
        escapeHtml(geoSn) +
        (geoSrc.length > 420 ? '…' : '') +
        '</p>';
    }
    var presentationRegions = footprintRegionCardsFromPresentation(brand);
    var schematicNames = presentationRegions.length
      ? presentationRegions.map(function (c) {
          return c.name;
        })
      : regionKeys.slice(0, 8);
    var schematic = '';
    if (schematicNames.length) {
      schematic =
        '<div class="footprint-schematic" aria-hidden="true">' +
        schematicNames
          .map(function (region, idx) {
            var dim =
              presentationRegions.length && presentationRegions[idx]
                ? footprintRegionCardDim(presentationRegions[idx].statusLabel)
                : false;
            return (
              '<div class="footprint-schematic__seg footprint-schematic__seg--' +
              (dim ? 'off' : 'on') +
              '">' +
              escapeHtml(region) +
              '</div>'
            );
          })
          .join('') +
        '</div>' +
        '<p class="footprint-schematic__caption">Schematic presence strip · illustrative, not geographic precision</p>';
    }
    function regionStatusCard(name, dim, statusClass, statusLabel, narrative, opts) {
      opts = opts || {};
      var narrHtml = '';
      if (hasVal(narrative)) {
        narrHtml = '<p>' + escapeHtml(narrative) + '</p>';
      } else if (!opts.suppressEmpty) {
        narrHtml = '<p class="oe-dd--empty">&nbsp;</p>';
      } else {
        return '';
      }
      return (
        '<div class="region-status-card' +
        (dim ? ' region-status-card--dim' : '') +
        '">' +
        '<div class="region-status-card__name">' +
        escapeHtml(name) +
        '</div>' +
        '<span class="status-label ' +
        statusClass +
        '">' +
        escapeHtml(statusLabel || 'Directional presence') +
        '</span>' +
        narrHtml +
        '</div>'
      );
    }
    var regionGrid = '';
    if (presentationRegions.length) {
      regionGrid =
        '<div class="region-footprint-grid">' +
        presentationRegions
          .map(function (card) {
            return regionStatusCard(
              card.name,
              footprintRegionCardDim(card.statusLabel),
              footprintRegionStatusClass(card.statusLabel),
              card.statusLabel,
              card.narrative,
              { suppressEmpty: suppressEmptyFp }
            );
          })
          .filter(Boolean)
          .join('') +
        '</div>';
      if (suppressEmptyFp && !String(regionGrid).replace(/<\/?div[^>]*>/g, '').trim()) {
        regionGrid = '';
      }
    } else if (regionKeys.length && !suppressEmptyFp) {
      regionGrid =
        '<div class="region-footprint-grid">' +
        regionKeys
          .slice(0, 8)
          .map(function (region, idx) {
            return regionStatusCard(
              region,
              idx > 3,
              'status-label--established',
              'From footprint data',
              '',
              { suppressEmpty: false }
            );
          })
          .join('') +
        '</div>';
    }
    var growthThemesRaw = explorerMergedBody(brand, 'footprint.growth_themes');
    var growthTagSrc = splitBullets(growthThemesRaw).filter(hasVal);
    if (!growthTagSrc.length && hasVal(growthThemesRaw)) {
      growthTagSrc = chipListFromCsv(growthThemesRaw);
    }
    if (!growthTagSrc.length) {
      growthTagSrc = splitBullets(brand.brandDevelopmentStage || fv.specificMarkets || '')
        .filter(hasVal)
        .slice(0, 6);
    } else {
      growthTagSrc = growthTagSrc.slice(0, 6);
    }
    var growthChips =
      growthTagSrc.length > 0
        ? growthTagSrc
            .map(function (t) {
              return '<span class="tag-chip">' + escapeHtml(String(t)) + '</span>';
            })
            .join('')
        : suppressEmptyFp
          ? ''
          : '<span class="tag-chip">&nbsp;</span>';
    var growthEditorial = explorerMergedBody(brand, 'footprint.growth_editorial');
    var growthRightP = hasVal(growthEditorial)
      ? '<p>' + escapeHtml(fmtCell(growthEditorial)).replace(/\n/g, '<br>') + '</p>'
      : suppressEmptyFp
        ? ''
        : '<p class="oe-dd--empty">&nbsp;</p>';
    var fitItems = splitBullets(explorerMergedBody(brand, 'footprint.growth_fit')).filter(hasVal);
    var growthSection = '';
    if (
      suppressEmptyFp &&
      !String(growthChips || '').trim() &&
      !hasVal(growthEditorial) &&
      !fitItems.length
    ) {
      growthSection = '';
    } else {
      growthSection =
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Growth Priorities</h2>' +
        '<p class="oe-section-hint">Directional Themes</p>' +
        '<div class="growth-priority-layout">' +
        '<div class="growth-priority-layout__left">' +
        '<h3>Priority Growth Themes</h3>' +
        '<div class="tag-chip-row" style="margin:0">' +
        growthChips +
        '</div></div>' +
        '<div class="growth-priority-layout__right">' +
        growthRightP +
        '<div class="growth-fit-sub">' +
        '<h4>Most Likely Growth Fit</h4>' +
        (fitItems.length
          ? '<ul>' +
            fitItems
              .map(function (li) {
                return '<li>' + escapeHtml(String(li)) + '</li>';
              })
              .join('') +
            '</ul>'
          : suppressEmptyFp
            ? ''
            : '<ul><li class="oe-dd--empty">&nbsp;</li></ul>') +
        '</div></div></div></section>';
    }
    function propertyShell() {
      return (
        '<article class="property-example-card">' +
        '<div class="property-example-card__top">' +
        '<span class="property-example-card__badge">Open</span>' +
        '<div class="property-example-card__titles"><h4>&nbsp;</h4><span>&nbsp;</span></div></div>' +
        '<div class="property-example-card__mid">' +
        '<div class="property-example-card__meta oe-dd--empty">&nbsp;</div>' +
        '<div class="property-example-card__scenario oe-dd--empty">&nbsp;</div>' +
        '<p class="property-example-card__photo-credit oe-dd--empty">&nbsp;</p>' +
        '<p class="oe-dd--empty">&nbsp;</p></div>' +
        '<div class="property-example-card__bottom">' +
        '<div class="property-example-card__tags"><span class="tag-chip">&nbsp;</span></div>' +
        '<button type="button" class="btn" disabled>View Property</button></div></article>'
      );
    }
    var openingBlocks = explorerBlocksForSlot(brand, FOOTPRINT_OPENINGS_SLOT);
    var openingsGrid = '';
    var openingsSection = '';
    if (openingBlocks.length && footprintPropertyPayloadSink) {
      var openingsHint = 'Curated · Not a Full Directory';
      if (String(brand && brand.name || '').toLowerCase().indexOf('woodspring') >= 0) {
        openingsHint = 'Curated U.S. examples · Not a full directory';
      }
      openingsGrid = openingBlocks
        .map(function (block) {
          return propertyExampleCardFromBlock(block, footprintPropertyPayloadSink);
        })
        .join('');
      openingsSection =
        '<section class="oe-section" style="margin-top:8px">' +
        '<h2 class="oe-section-title">Openings / Examples / Properties</h2>' +
        '<p class="oe-section-hint">' + escapeHtml(openingsHint) + '</p>' +
        '<div class="property-example-grid">' +
        openingsGrid +
        '</div></section>';
    }
    var momentumSection = renderMomentumSection(brand);
    var fpEditorialP;
    var fpEditorial = explorerMergedBody(brand, 'footprint.editorial');
    if (hasVal(fpEditorial)) {
      fpEditorialP =
        '<p>' + escapeHtml(fmtCell(fpEditorial)).replace(/\n/g, '<br>') + '</p>';
    } else if (suppressEmptyFp) {
      fpEditorialP = '';
    } else {
      fpEditorialP = '<p class="oe-dd--empty">&nbsp;</p>';
    }
    var fpBulletSrc = splitBullets(explorerMergedBody(brand, 'footprint.editorial_bullets')).filter(hasVal);
    var fpEditorialUl =
      fpBulletSrc.length > 0
        ? '<ul>' +
          fpBulletSrc
            .map(function (li) {
              return '<li>' + escapeHtml(String(li)) + '</li>';
            })
            .join('') +
          '</ul>'
        : '';
    var dealalitySection = '';
    if (hasVal(fpEditorial) || fpBulletSrc.length) {
      dealalitySection =
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Dealality View on Market Presence</h2>' +
        '<p class="oe-section-hint">Interpretation</p>' +
        '<div class="dealality-editorial-card">' +
        '<div class="dealality-editorial-card__brand">Dealality</div>' +
        fpEditorialP +
        fpEditorialUl +
        '</div></section>';
    }
    return wrapOe(
      '<section class="oe-section">' +
        '<h2 class="oe-section-title">Markets &amp; Footprint</h2>' +
        '<p class="oe-section-hint">Brand-Scale Geography</p>' +
        kpiRow +
        '</section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Footprint Metrics</h2>' +
        '<p class="oe-section-hint">Open vs Pipeline · Illustrative</p>' +
        metricsBlock +
        '</section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Portfolio Presence</h2>' +
        '<p class="oe-section-hint">Scale &amp; Composition</p>' +
        presenceRow +
        '</section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Geographic Footprint</h2>' +
        '<p class="oe-section-hint">Where Presence Is Directional</p>' +
        geoIntro +
        schematic +
        regionGrid +
        '</section>' +
        growthSection +
        openingsSection +
        momentumSection +
        dealalitySection
    );
  }


  function blockCaseSummaryField(block, key) {
    if (!block || block[key] == null || block[key] === '') return '';
    return String(block[key]).trim();
  }

  function joinLocAssetForModal(p) {
    var parts = [];
    if (p && hasVal(p.loc)) parts.push(String(p.loc).trim());
    if (p && hasVal(p.asset)) parts.push(String(p.asset).trim());
    return parts.join(' · ');
  }

  /** Split `materials.caseStudy` Body: card copy then optional `\n\n---\n\n` + five modal paragraphs (overview, owner, brand, interpretation, tags csv). */
  function splitCaseStudyCardBodyAndModalAppendix(bodyRaw) {
    var raw = String(bodyRaw || '');
    var seps = ['\n\n---\n\n', '\r\n\r\n---\r\n\r\n', '\n\n---\r\n\n'];
    var i;
    var idx = -1;
    var sepUsed = '';
    for (i = 0; i < seps.length; i++) {
      var j = raw.indexOf(seps[i]);
      if (j !== -1) {
        idx = j;
        sepUsed = seps[i];
        break;
      }
    }
    if (idx === -1) return { cardBody: raw.trim(), modalAppendix: '' };
    return {
      cardBody: raw.slice(0, idx).trim(),
      modalAppendix: raw.slice(idx + sepUsed.length).trim()
    };
  }

  function parseCaseSummaryModalAppendix(raw) {
    if (!hasVal(raw)) return null;
    var paras = String(raw)
      .split(/\n\n+/)
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean);
    if (!paras.length) return null;
    return {
      overview: paras[0] || '',
      ownerObj: paras[1] || '',
      brandRel: paras[2] || '',
      interp: paras[3] || '',
      tagsStr: paras[4] || ''
    };
  }

  function parseCaseStudyParas(bodyRaw) {
    var paras = String(bodyRaw || '')
      .split(/\n\n+/)
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean);
    var summaryHref = '';
    if (paras.length && isSafeHttpUrl(paras[paras.length - 1])) {
      summaryHref = paras[paras.length - 1];
      paras = paras.slice(0, -1);
    }
    var chips = '';
    var loc = '';
    var asset = '';
    var situation = '';
    var why = '';
    var takeaway = '';
    if (paras.length >= 6) {
      chips = paras[0];
      loc = paras[1];
      asset = paras[2];
      situation = paras[3];
      why = paras[4];
      takeaway = paras[5];
    } else if (paras.length >= 3) {
      situation = paras[0];
      why = paras[1];
      takeaway = paras[2];
    } else if (paras.length === 2) {
      situation = paras[0];
      why = paras[1];
    } else if (paras.length === 1) {
      situation = paras[0];
    }
    if (!summaryHref) summaryHref = lastHttpUrlInString(bodyRaw);
    return {
      summaryHref: summaryHref && isSafeHttpUrl(summaryHref) ? summaryHref : '',
      chips: chips,
      loc: loc,
      asset: asset,
      situation: situation,
      why: why,
      takeaway: takeaway
    };
  }

  /**
   * footprint.openings Body — Ascend / voco property-example card shape.
   * Accepts blank-line paragraphs OR Ascend-style single-newline structural lines.
   * 4 blocks: chips, location, asset/meta, opening teaser (+ optional https URL).
   * 5 blocks: + scenario accent line (property-example-card__scenario).
   * 6 blocks: full case-study card blocks (chips … takeaway).
   */
  function splitOpeningsBodyUnits(bodyRaw) {
    var raw = String(bodyRaw || '').trim();
    if (!raw) return [];
    var blank = raw
      .split(/\n\n+/)
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean);
    if (blank.length >= 4) return blank;
    var single = raw
      .split(/\n/)
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean);
    if (single.length >= 4) return single;
    return blank.length ? blank : single;
  }

  function parseFootprintOpeningParas(bodyRaw) {
    var paras = splitOpeningsBodyUnits(bodyRaw);
    var summaryHref = '';
    if (paras.length && isSafeHttpUrl(paras[paras.length - 1])) {
      summaryHref = paras[paras.length - 1];
      paras = paras.slice(0, -1);
    }
    var chips = '';
    var loc = '';
    var asset = '';
    var scenario = '';
    var situation = '';
    var why = '';
    var takeaway = '';
    if (paras.length >= 6) {
      chips = paras[0];
      loc = paras[1];
      asset = paras[2];
      situation = paras[3];
      why = paras[4];
      takeaway = paras[5];
    } else if (paras.length === 5) {
      chips = paras[0];
      loc = paras[1];
      asset = paras[2];
      scenario = paras[3];
      situation = paras[4];
    } else if (paras.length === 4) {
      chips = paras[0];
      loc = paras[1];
      asset = paras[2];
      situation = paras[3];
    } else {
      var short = parseCaseStudyParas(bodyRaw);
      return {
        summaryHref: short.summaryHref,
        chips: short.chips,
        loc: short.loc,
        asset: short.asset,
        scenario: '',
        situation: short.situation,
        why: short.why,
        takeaway: short.takeaway
      };
    }
    if (!summaryHref) summaryHref = lastHttpUrlInString(bodyRaw);
    return {
      summaryHref: summaryHref && isSafeHttpUrl(summaryHref) ? summaryHref : '',
      chips: chips,
      loc: loc,
      asset: asset,
      scenario: scenario,
      situation: situation,
      why: why,
      takeaway: takeaway
    };
  }

  function chipListFromCsv(csv) {
    return String(csv || '')
      .split(',')
      .map(function (s) {
        return s.trim();
      })
      .filter(Boolean);
  }

  function buildCaseStudyModalPayload(block, p, chipParts, externalUrl, modalAppendixParsed) {
    var m = modalAppendixParsed || null;
    var titleBase = block && hasVal(block.title) ? String(block.title).trim() : 'Case study';
    var overview =
      blockCaseSummaryField(block, 'caseSummaryOverview') ||
      (m && hasVal(m.overview) ? String(m.overview).trim() : '') ||
      (p && hasVal(p.situation) ? String(p.situation).trim() : '');
    var ownerObj =
      blockCaseSummaryField(block, 'caseSummaryOwnerObjective') ||
      (m && hasVal(m.ownerObj) ? String(m.ownerObj).trim() : '') ||
      (p && joinLocAssetForModal(p)) ||
      '';
    var brandRel =
      blockCaseSummaryField(block, 'caseSummaryBrandRelevance') ||
      (m && hasVal(m.brandRel) ? String(m.brandRel).trim() : '') ||
      (p && hasVal(p.why) ? String(p.why).trim() : '');
    var interp =
      blockCaseSummaryField(block, 'caseSummaryInterpretation') ||
      (m && hasVal(m.interp) ? String(m.interp).trim() : '') ||
      (p && hasVal(p.takeaway) ? String(p.takeaway).trim() : '');
    var tagsStr = blockCaseSummaryField(block, 'caseSummaryTags');
    if (!hasVal(tagsStr) && m && hasVal(m.tagsStr)) tagsStr = String(m.tagsStr).trim();
    var tags = [];
    if (hasVal(tagsStr)) {
      tags = tagsStr
        .split(',')
        .map(function (s) {
          return s.trim();
        })
        .filter(Boolean);
    } else {
      tags = chipParts.slice();
    }
    var ext = externalUrl && isSafeHttpUrl(String(externalUrl).trim()) ? String(externalUrl).trim() : '';
    return {
      title: titleBase + ' — Case summary',
      overview: overview || '—',
      ownerObj: ownerObj || '—',
      brandRel: brandRel || '—',
      dealalityInterp: interp || '—',
      tags: tags,
      externalUrl: ext
    };
  }

  function openBrandCaseStudyModal(payload) {
    var modal = document.getElementById('beCaseStudyModal');
    var titleEl = document.getElementById('beCsModalTitle');
    var innerEl = document.getElementById('beCsModalInner');
    if (!modal || !titleEl || !innerEl || !payload) return;
    titleEl.textContent = payload.title || 'Case summary';
    var tagsHtml = (payload.tags || [])
      .map(function (t) {
        return '<span class="tag-chip">' + escapeHtml(t) + '</span>';
      })
      .join('');
    var linkBlock = '';
    if (payload.externalUrl) {
      linkBlock =
        '<div class="be-case-modal__external">' +
        '<a class="btn btn--ghost" href="' +
        escapeHtml(payload.externalUrl) +
        '" target="_blank" rel="noopener noreferrer">Open external link</a>' +
        '</div>';
    }
    innerEl.innerHTML =
      '<div class="be-case-detail-block"><h4>Property overview</h4><p>' +
      escapeHtml(payload.overview) +
      '</p></div>' +
      '<div class="be-case-detail-block"><h4>Owner objective</h4><p>' +
      escapeHtml(payload.ownerObj) +
      '</p></div>' +
      '<div class="be-case-detail-block"><h4>Brand relevance</h4><p>' +
      escapeHtml(payload.brandRel) +
      '</p></div>' +
      '<div class="be-case-detail-block"><h4>Dealality interpretation</h4><p>' +
      escapeHtml(payload.dealalityInterp) +
      '</p></div>' +
      '<div class="be-case-detail-block"><h4>Related tags</h4><div class="be-case-modal__tags">' +
      (tagsHtml || '<span class="be-case-modal__tags-empty">—</span>') +
      '</div></div>' +
      linkBlock;
    modal.classList.add('be-case-modal-overlay--open');
    document.body.style.overflow = 'hidden';
  }

  function closeBrandCaseStudyModal() {
    var modal = document.getElementById('beCaseStudyModal');
    if (!modal) return;
    modal.classList.remove('be-case-modal-overlay--open');
    document.body.style.overflow = '';
  }

  /** Footprint “View Property” modal — Case Summary columns on footprint.openings rows (voco footprint IA). */
  function buildFootprintPropertyPayload(block, p, chipParts, modalAppendixParsed) {
    var m = modalAppendixParsed || null;
    var title = block && hasVal(block.title) ? String(block.title).trim() : 'Property';
    var locLine = p && hasVal(p.loc) ? String(p.loc).trim() : '';
    var subtitle = locLine ? locLine + ' · Open' : 'Open';
    var overview =
      blockCaseSummaryField(block, 'caseSummaryOverview') ||
      (m && hasVal(m.overview) ? String(m.overview).trim() : '') ||
      (p && hasVal(p.situation) ? String(p.situation).trim() : '');
    var relevance =
      blockCaseSummaryField(block, 'caseSummaryBrandRelevance') ||
      (m && hasVal(m.brandRel) ? String(m.brandRel).trim() : '') ||
      (p && hasVal(p.why) ? String(p.why).trim() : '');
    var suggests =
      blockCaseSummaryField(block, 'caseSummaryOwnerObjective') ||
      (m && hasVal(m.ownerObj) ? String(m.ownerObj).trim() : '') ||
      (p && hasVal(p.asset) ? String(p.asset).trim() : '');
    var dealalityTakeaway =
      blockCaseSummaryField(block, 'caseSummaryInterpretation') ||
      (m && hasVal(m.interp) ? String(m.interp).trim() : '') ||
      (p && hasVal(p.takeaway) ? String(p.takeaway).trim() : '');
    var tagsStr = blockCaseSummaryField(block, 'caseSummaryTags');
    if (!hasVal(tagsStr) && m && hasVal(m.tagsStr)) tagsStr = String(m.tagsStr).trim();
    var tags = [];
    if (hasVal(tagsStr)) {
      tags = tagsStr
        .split(',')
        .map(function (s) {
          return s.trim();
        })
        .filter(Boolean);
    } else {
      tags = chipParts.slice();
    }
    var ext =
      block && hasVal(block.summaryUrl) && isSafeHttpUrl(String(block.summaryUrl).trim())
        ? String(block.summaryUrl).trim()
        : p && p.summaryHref
          ? p.summaryHref
          : '';
    return {
      title: title,
      subtitle: subtitle,
      overview: overview || '',
      relevance: relevance || '',
      suggests: suggests || '',
      dealalityTakeaway: dealalityTakeaway || '',
      tags: tags,
      externalUrl: ext
    };
  }

  function openBrandFootprintPropertyModal(payload) {
    var modal = document.getElementById('beCaseStudyModal');
    var titleEl = document.getElementById('beCsModalTitle');
    var innerEl = document.getElementById('beCsModalInner');
    if (!modal || !titleEl || !innerEl || !payload) return;
    titleEl.textContent = payload.title || 'Property';
    var tagsHtml = (payload.tags || [])
      .map(function (t) {
        return '<span class="tag-chip">' + escapeHtml(t) + '</span>';
      })
      .join('');
    var linkBlock = '';
    if (payload.externalUrl) {
      linkBlock =
        '<div class="be-case-modal__external">' +
        '<a class="btn btn--ghost" href="' +
        escapeHtml(payload.externalUrl) +
        '" target="_blank" rel="noopener noreferrer">Open external link</a>' +
        '</div>';
    }
    innerEl.innerHTML =
      '<p style="margin:0 0 14px;font-size:0.8125rem;color:var(--muted,#9fb0d0)">' +
      escapeHtml(payload.subtitle || '') +
      '</p>' +
      modalDetailBlock('Property overview', payload.overview) +
      modalDetailBlock('Why it is relevant', payload.relevance) +
      modalDetailBlock('What it suggests about the brand', payload.suggests) +
      modalDetailBlock('Dealality takeaway', payload.dealalityTakeaway) +
      (tagsHtml
        ? '<div class="be-case-detail-block"><h4>Similar property types</h4><div class="be-case-modal__tags">' +
          tagsHtml +
          '</div></div>'
        : '') +
      linkBlock;
    modal.classList.add('be-case-modal-overlay--open');
    document.body.style.overflow = 'hidden';
  }

  function propertyExampleCardFromBlock(block, footprintPropertyPayloadSink) {
    if (!footprintPropertyPayloadSink) footprintPropertyPayloadSink = [];
    var title = block && hasVal(block.title) ? String(block.title).trim() : '';
    var bodyRaw = block && hasVal(block.body) ? String(block.body).trim() : '';
    var split = splitCaseStudyCardBodyAndModalAppendix(bodyRaw);
    var p = parseFootprintOpeningParas(split.cardBody);
    var modalAppendixParsed = parseCaseSummaryModalAppendix(split.modalAppendix);
    var chipParts = chipListFromCsv(p.chips);
    if (!chipParts.length) chipParts = chipListFromCsv(blockCaseSummaryField(block, 'caseSummaryTags'));
    var scenario = hasVal(p.scenario)
      ? String(p.scenario).trim()
      : chipParts.length > 1
        ? chipParts.slice(1).join(' / ')
        : '';
    var tagsHtml = chipParts.length
      ? chipParts
          .map(function (c) {
            return '<span class="tag-chip">' + escapeHtml(c) + '</span>';
          })
          .join('')
      : '<span class="tag-chip">&nbsp;</span>';
    var imgUrl = block && hasVal(block.imageUrl) ? String(block.imageUrl).trim() : '';
    var topInner = '';
    if (imgUrl && isSafeHttpUrl(imgUrl)) {
      topInner =
        '<img src="' +
        escapeHtml(imgUrl) +
        '" alt="' +
        escapeHtml(title || 'Property') +
        '"' +
        exportPdfImgAttrs() +
        ' />';
    }
    var situationSn = hasVal(p.situation) ? String(p.situation).trim() : '';
    if (!hasVal(situationSn)) {
      var ovSn = blockCaseSummaryField(block, 'caseSummaryOverview');
      if (hasVal(ovSn)) situationSn = String(ovSn).trim();
    }
    if (situationSn.length > 280) situationSn = situationSn.slice(0, 277) + '…';
    var metaFallback = hasVal(p.asset)
      ? String(p.asset).trim()
      : hasVal(p.loc)
        ? String(p.loc).trim()
        : chipParts.length
          ? chipParts[0]
          : '';
    var metaHtml = hasVal(metaFallback) ? escapeHtml(metaFallback) : '';
    var scenarioHtml = hasVal(scenario) ? escapeHtml(scenario) : '';
    var teaserHtml = hasVal(situationSn) ? escapeHtml(situationSn) : '';
    if (!hasVal(title) || (!hasVal(metaHtml) && !hasVal(scenarioHtml) && !hasVal(teaserHtml))) {
      return '';
    }
    var fpPayload = buildFootprintPropertyPayload(block, p, chipParts, modalAppendixParsed);
    var fpIdx = footprintPropertyPayloadSink.length;
    footprintPropertyPayloadSink.push(fpPayload);
    return (
      '<article class="property-example-card">' +
      '<div class="property-example-card__top">' +
      topInner +
      '<span class="property-example-card__badge">Open</span>' +
      '<div class="property-example-card__titles">' +
      '<h4>' +
      escapeHtml(title) +
      '</h4>' +
      '<span>' +
      (hasVal(p.loc) ? escapeHtml(p.loc) : '') +
      '</span></div></div>' +
      '<div class="property-example-card__mid">' +
      (hasVal(metaHtml) ? '<div class="property-example-card__meta">' + metaHtml + '</div>' : '') +
      (hasVal(scenarioHtml)
        ? '<div class="property-example-card__scenario">' + scenarioHtml + '</div>'
        : '') +
      (hasVal(teaserHtml) ? '<p>' + teaserHtml + '</p>' : '') +
      '</div>' +
      '<div class="property-example-card__bottom">' +
      '<div class="property-example-card__tags">' +
      tagsHtml +
      '</div>' +
      '<button type="button" class="btn" data-be-footprint-property="' +
      fpIdx +
      '">View Property</button></div></article>'
    );
  }

  function wireBrandCaseStudyModalOnce() {
    if (window._beCaseStudyModalDocumentWired) return;
    window._beCaseStudyModalDocumentWired = true;
    document.addEventListener('click', function (e) {
      var panelsWrap = e.target.closest('[data-be-atelier-panels]');
      if (!panelsWrap) return;
      var fpBtn = e.target.closest('[data-be-footprint-property]');
      if (fpBtn) {
        if (!Array.isArray(panelsWrap._beFootprintPropertyPayloads)) return;
        var fpIdx = parseInt(fpBtn.getAttribute('data-be-footprint-property'), 10);
        if (isNaN(fpIdx) || fpIdx < 0 || fpIdx >= panelsWrap._beFootprintPropertyPayloads.length) return;
        e.preventDefault();
        openBrandFootprintPropertyModal(panelsWrap._beFootprintPropertyPayloads[fpIdx]);
        return;
      }
      var btn = e.target.closest('[data-be-case-summary]');
      if (!btn) return;
      if (!Array.isArray(panelsWrap._beCaseStudyPayloads)) return;
      var idx = parseInt(btn.getAttribute('data-be-case-summary'), 10);
      if (isNaN(idx) || idx < 0 || idx >= panelsWrap._beCaseStudyPayloads.length) return;
      e.preventDefault();
      openBrandCaseStudyModal(panelsWrap._beCaseStudyPayloads[idx]);
    });
    var modal = document.getElementById('beCaseStudyModal');
    var closeBtn = document.getElementById('beCsModalClose');
    if (modal && closeBtn) {
      closeBtn.addEventListener('click', closeBrandCaseStudyModal);
      modal.addEventListener('click', function (e) {
        if (e.target === modal) closeBrandCaseStudyModal();
      });
    }
    document.addEventListener('keydown', function (e) {
      if (e.key !== 'Escape') return;
      var m = document.getElementById('beCaseStudyModal');
      if (m && m.classList.contains('be-case-modal-overlay--open')) {
        e.preventDefault();
        closeBrandCaseStudyModal();
      }
    });
  }

  function materialsFileDisplayTitle(row, href) {
    var label = hasVal(row.title) ? String(row.title).trim() : '';
    if (label && /^undefined$/i.test(label)) label = '';
    if (label) return label;
    var meta = materialsFileMetaFromBody(row.body);
    if (meta) {
      var parts = meta
        .split('·')
        .map(function (s) {
          return s.trim();
        })
        .filter(Boolean);
      var filePart = parts.length ? parts[parts.length - 1] : '';
      if (filePart && /\.[a-z0-9]{2,5}$/i.test(filePart)) {
        return filePart.replace(/_/g, ' ');
      }
    }
    if (href) {
      var pathBit = '';
      try {
        pathBit = decodeURIComponent(String(href).split('/').pop() || '').split('?')[0];
      } catch (e) {
        pathBit = String(href).split('/').pop() || '';
      }
      if (pathBit && !/airtableusercontent\.com/i.test(String(href)) && pathBit.length < 80) {
        return pathBit;
      }
    }
    return 'Brand material';
  }

  function renderBrandMaterials(brand) {
    function materialsFileHref(block) {
      if (!block) return '';
      var fromBody = firstHttpUrlInString(block.body);
      if (fromBody) return fromBody;
      var img = hasVal(block.imageUrl) ? String(block.imageUrl).trim() : '';
      return isSafeHttpUrl(img) ? img : '';
    }
    var fileRows = explorerBlocksForSlot(brand, 'materials.file');
    var fileGrid;
    if (fileRows.length) {
      fileGrid = fileRows
        .map(function (row) {
          var href = materialsFileHref(row);
          var label = materialsFileDisplayTitle(row, href);
          var kind = href ? fileKindLabelFromUrl(href, label + ' ' + (row.body || '')) : 'FILE';
          var metaLine = materialsFileMetaFromBody(row.body);
          var badgeLine = materialsFileBadgeFromBody(row.body);
          return fileCard(kind, label, metaLine, href, badgeLine);
        })
        .join('');
    } else {
      fileGrid =
        fileCard('PDF', 'Brand Overview Deck.pdf', 'PDF · 4.2 MB · Updated Feb 12, 2026') +
        fileCard('PDF', 'Development Snapshot.pdf', 'PDF · 1.8 MB · Updated Jan 28, 2026') +
        fileCard('PDF', 'Positioning Summary.pdf', 'PDF · 956 KB · Updated Mar 4, 2026') +
        fileCard('ZIP', 'Design Reference Gallery.zip', 'ZIP · 128 MB · Updated Dec 9, 2025');
    }
    var galleryDefaultLabels = {
      'materials.gallery.1': 'Exterior / Prototype',
      'materials.gallery.2': 'Guest Room',
      'materials.gallery.3': 'Kitchen-Equipped Suite',
      'materials.gallery.4': 'Extended-Stay Suite',
      'materials.gallery.5': 'Brand Platform Visual',
      'materials.gallery.6': 'Property Example'
    };
    var galleryRows = explorerGalleryBlocks(brand);
    var galleryHasImage = false;
    var gallery = galleryRows
      .map(function (row) {
        var slot = String(row.slotKey || '');
        var imgUrl = row && hasVal(row.imageUrl) ? String(row.imageUrl).trim() : '';
        var fallback = galleryDefaultLabels[slot] || 'Gallery';
        var caption = galleryCaptionForRow(brand, row, fallback);
        if (imgUrl && isSafeHttpUrl(imgUrl)) {
          galleryHasImage = true;
          return (
            '<div class="gallery-card gallery-card--has-image" role="img" aria-label="' +
            escapeHtml(caption) +
            '"><img src="' +
            escapeHtml(imgUrl) +
            '" alt=""' +
            (isExportPdfMode() ? ' loading="eager" decoding="sync"' : ' loading="lazy" decoding="async"') +
            ' referrerpolicy="no-referrer" />' +
            '<span class="gallery-card__cap">' +
            escapeHtml(caption) +
            '</span></div>'
          );
        }
        return (
          '<div class="gallery-card gallery-card--empty" role="img" aria-label="' +
          escapeHtml(caption) +
          '"><span>' +
          escapeHtml(caption) +
          '</span></div>'
        );
      })
      .join('');
    var galleryHint = '';
    if (!isExportPdfMode()) {
      galleryHint = galleryHasImage
        ? '<p class="oe-section-hint">Curated · Representative property photography</p>'
        : '<p class="oe-section-hint oe-section-hint--admin">Slots <code>materials.gallery.1</code> … <code>materials.gallery.6</code> — attach image to each row’s Image field</p>';
    }
    return wrapOe(
      '<section class="oe-section">' +
        '<h2 class="oe-section-title">Official Brand Materials</h2>' +
        '<p class="oe-section-hint">Unverified by Brand</p>' +
        '<div class="file-card-grid">' +
        fileGrid +
        '</div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Image Gallery</h2>' +
        galleryHint +
        '<div class="gallery-grid">' +
        gallery +
        '</div></section>'
    );
  }

  function renderDealalityInsight(brand) {
    var STRENGTH_PAIRS = [
      ['Character-Rich Conversions', 'When the story is already there and needs commercial scaffolding.'],
      ['Premium Urban Repositioning', 'Where design credibility and distribution lift both matter.'],
      ['Design-Led Independent Alternatives', 'For guests who want individuality with dependable systems.'],
      ['Owner / Operator Structures', 'With branded operating experience and reporting discipline.']
    ];
    var CAUTION_PAIRS = [
      ['Assets Without Identity', 'Weaker story limits lifestyle premium capture.'],
      ['Markets That Won\u2019t Reward Premium', 'Rate and revenue per available room may not support positioning.'],
      ['Limited Branded Experience', 'Operators without lifestyle depth may struggle with calibration.'],
      ['Expectations of Total Design Freedom', 'Guardrails still apply for guest confidence and QA.']
    ];
    var strengthGrid = STRENGTH_PAIRS.map(function (row) {
      return scenarioDetailCard(row[0], row[1]);
    }).join('');
    var cautionGrid = CAUTION_PAIRS.map(function (row) {
      return scenarioDetailCard(row[0], row[1]);
    }).join('');
    var checklist =
      '<ul class="explorer-detail-card__list checklist">' +
      '<li>Is the asset\u2019s physical story strong enough to support lifestyle positioning?</li>' +
      '<li>Will the market pay for a more premium affiliation?</li>' +
      '<li>Is the operator capable of delivering the service tone and reporting rigor?</li>' +
      '<li>Is the owner looking for individuality plus structure, rather than a highly standardized model?</li>' +
      '<li>Is loyalty contribution likely to matter in this demand mix?</li>' +
      '</ul>';
    var similarRows = explorerCardRowsForSlot(brand, 'insight.similar', { prefix: true });
    var similar =
      similarRows.length > 0
        ? similarRows
            .map(function (r) {
              var label = hasVal(r.title) ? r.title : r.body;
              var sub =
                hasVal(r.title) && hasVal(r.body) && String(r.body).trim() !== String(r.title).trim()
                  ? ' ' + escapeHtml(String(r.body).trim())
                  : '';
              return (
                '<div class="scenario-card"><strong>' +
                escapeHtml(String(label).slice(0, 120)) +
                '</strong>' +
                sub +
                '</div>'
              );
            })
            .join('')
        : '<div class="scenario-card"><strong>&nbsp;</strong></div>' +
          '<div class="scenario-card"><strong>&nbsp;</strong></div>' +
          '<div class="scenario-card"><strong>&nbsp;</strong></div>';
    return wrapOe(
      '<section class="oe-section">' +
        '<h2 class="oe-section-title">Dealality Summary</h2>' +
        '<p class="oe-section-hint">Dealality Editorial Summary</p>' +
        explorerDetailCard('Summary', dealalitySummaryFromBrand(brand)) +
        '</section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Key Strengths &amp; Strategic Fit</h2>' +
        '<p class="oe-section-hint">Brand-to-Owner Framing — Where Affiliation Tends to Show the Most Upside (Illustrative)</p>' +
        '<div class="scenario-card-grid scenario-card-grid--owner-value" style="grid-template-columns:repeat(2,1fr)">' +
        strengthGrid +
        '</div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Caution Areas &amp; Tradeoffs</h2>' +
        '<p class="oe-section-hint">Limits and Risk — Illustrative Diligence Themes (Not Property-Specific)</p>' +
        '<div class="scenario-card-grid scenario-card-grid--owner-value" style="grid-template-columns:repeat(2,1fr)">' +
        cautionGrid +
        '</div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Before Shortlisting, Evaluate</h2>' +
        '<div class="explorer-detail-card">' +
        '<h3 class="explorer-detail-card__label">Checklist</h3>' +
        checklist +
        '</div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Similar Brands</h2>' +
        '<p class="oe-section-hint">Competitive Set — Illustrative Peers (Not Equivalency Claims)</p>' +
        '<div class="scenario-card-grid" style="grid-template-columns:repeat(3,1fr)">' +
        similar +
        '</div></section>'
    );
  }

  function buildAtelierPanelsHtml(brand) {
    if (!isInternalPreviewRequest() && !canRenderFactoryPreview(brand) && isExternalQualityLocked(brand)) {
      return {
        html:
          '<section class="be-atelier-tab-panel active" data-atelier-panel="atelier-locked">' +
          atelierTabShell(lockedExternalProfileHtml()) +
          '</section>',
        footprintPropertyPayloads: [],
        qualityLocked: true
      };
    }

    if (
      (isInternalPreviewRequest() || canRenderFactoryPreview(brand)) &&
      !hasPresentationRows(brand) &&
      isExternalQualityLocked(brand)
    ) {
      return {
        html:
          '<section class="be-atelier-tab-panel active" data-atelier-panel="atelier-locked">' +
          atelierTabShell(lockedExternalProfileHtml()) +
          '</section>',
        footprintPropertyPayloads: [],
        qualityLocked: true
      };
    }

    var footprintPropertyPayloadSink = [];
    var atelierMap = {
      'atelier-overview': renderAtelierOverview(brand),
      'atelier-value-owners': renderValueToOwners(brand),
      'atelier-ops': renderOperationsStandards(brand),
      'atelier-standards-owner': renderStandardsOwnerConsiderations(brand),
      'atelier-commercial': renderCommercialEngine(brand),
      'atelier-economics': renderAtelierEconomicsObligations(brand),
      'atelier-loyalty': renderLoyaltyProgram(brand),
      'atelier-footprint': renderFootprintGrowth(brand, footprintPropertyPayloadSink),
      'atelier-materials': renderBrandMaterials(brand),
      'atelier-insight': renderDealalityInsight(brand)
    };
    var goldAppend = getGoldAppendTabs();
    var goldPanels =
      goldAppend.length && window.BrandExplorerGoldDetail && window.BrandExplorerGoldDetail.buildPanels
        ? window.BrandExplorerGoldDetail.buildPanels(brand)
        : null;
    var rows = combinedTabRowDefs();
    var htmlStr = rows
      .map(function (t, i) {
        var isAtelier = i < ATELIER_TAB_DEFS.length;
        var inner;
        if (isAtelier) {
          inner = atelierTabShell(atelierMap[t.id]);
        } else {
          var body =
            goldPanels && goldPanels[t.goldKey] != null
              ? goldPanels[t.goldKey]
              : '<p class="gold-mock-tab-empty">Brand Setup panels are not available on this page.</p>';
          inner = '<div class="be-atelier-gold-embed">' + body + '</div>';
        }
        return (
          '<section class="be-atelier-tab-panel' +
          (i === 0 ? ' active' : '') +
          '" data-atelier-panel="' +
          t.id +
          '">' +
          inner +
          '</section>'
        );
      })
      .join('');

    if (canRenderFactoryPreview(brand) && isExternalQualityLocked(brand)) {
      htmlStr = factoryPreviewBannerHtml() + htmlStr;
    } else if (isInternalPreviewRequest() && isExternalQualityLocked(brand)) {
      htmlStr = internalPreviewBannerHtml() + htmlStr;
    }

    return {
      html: htmlStr,
      footprintPropertyPayloads: footprintPropertyPayloadSink,
      qualityLocked: false
    };
  }

  function buildAtelierTabsHtml(brand) {
    if (!isInternalPreviewRequest() && !canRenderFactoryPreview(brand) && brand && isExternalQualityLocked(brand)) {
      return '';
    }
    if (
      (isInternalPreviewRequest() || canRenderFactoryPreview(brand)) &&
      brand &&
      !hasPresentationRows(brand) &&
      isExternalQualityLocked(brand)
    ) {
      return '';
    }
    var G = window.BrandExplorerGoldDetail;
    var rows = combinedTabRowDefs();
    return rows
      .map(function (t, i) {
        var isAtelier = i < ATELIER_TAB_DEFS.length;
        var icon = isAtelier
          ? ATELIER_TAB_ICON_BY_ID[t.id] || ''
          : G && G.TAB_ICONS
            ? G.TAB_ICONS[t.goldKey]
            : '';
        return (
          '<button type="button" class="section-nav-item' +
          (i === 0 ? ' active' : '') +
          '" data-tab-source="' +
          (isAtelier ? 'atelier' : 'gold') +
          '" data-atelier-tab="' +
          t.id +
          '" role="tab"><div class="section-nav-icon">' +
          icon +
          '</div><div class="section-nav-label">' +
          t.label +
          '</div></button>'
        );
      })
      .join('');
  }

  function wireAtelierTabs(rootEl) {
    var nav = rootEl.querySelector('[data-be-atelier-nav]');
    var panelsWrap = rootEl.querySelector('[data-be-atelier-panels]');
    if (!nav || !panelsWrap || nav._beAtelierWired) return;
    nav._beAtelierWired = true;
    nav.addEventListener('click', function (e) {
      var btn = e.target.closest('.section-nav-item[data-atelier-tab]');
      if (!btn) return;
      var tab = btn.getAttribute('data-atelier-tab');
      nav.querySelectorAll('.section-nav-item[data-atelier-tab]').forEach(function (b) {
        b.classList.toggle('active', b.getAttribute('data-atelier-tab') === tab);
      });
      panelsWrap.querySelectorAll('.be-atelier-tab-panel').forEach(function (p) {
        p.classList.toggle('active', p.getAttribute('data-atelier-panel') === tab);
      });
    });
    panelsWrap.addEventListener('click', function (e) {
      var jump = e.target.closest('[data-be-jump-atelier-tab]');
      if (!jump || jump.disabled) return;
      var tabId = jump.getAttribute('data-be-jump-atelier-tab');
      if (!tabId) return;
      var navBtn = nav.querySelector('.section-nav-item[data-atelier-tab="' + tabId + '"]');
      if (navBtn) navBtn.click();
    });
  }

  function tabLabelPlain(labelHtml) {
    return String(labelHtml || '')
      .replace(/<br\s*\/?>/gi, ' ')
      .replace(/<[^>]+>/g, '')
      .replace(/\s+/g, ' ')
      .trim();
  }

  /** PDF export: show every tab panel with a section heading (no tab switching). */
  function prepareAllPanelsForExport(rootEl) {
    if (!rootEl) return;
    var panelsWrap = rootEl.querySelector('[data-be-atelier-panels]');
    if (!panelsWrap) return;
    var labelById = {};
    combinedTabRowDefs().forEach(function (t) {
      labelById[t.id] = tabLabelPlain(t.label);
    });
    panelsWrap.querySelectorAll('.be-atelier-tab-panel').forEach(function (panel) {
      panel.classList.add('active');
      var panelId = panel.getAttribute('data-atelier-panel') || '';
      var titleText = labelById[panelId] || panelId;
      if (!panel.querySelector('.be-export-section-title')) {
        var heading = document.createElement('h2');
        heading.className = 'be-export-section-title';
        heading.textContent = titleText;
        panel.insertBefore(heading, panel.firstChild);
      }
    });
    var nav = rootEl.querySelector('[data-be-atelier-nav]');
    if (nav) nav.setAttribute('hidden', 'hidden');
  }

  function mountAtelierIntoRoot(rootEl, brand) {
    if (!rootEl || !brand) return;
    var nav = rootEl.querySelector('[data-be-atelier-nav]');
    var panelsWrap = rootEl.querySelector('[data-be-atelier-panels]');
    if (!nav || !panelsWrap) return;
    nav.innerHTML = buildAtelierTabsHtml(brand);
    var built = buildAtelierPanelsHtml(brand);
    panelsWrap.innerHTML = built.html;
    panelsWrap._beFootprintPropertyPayloads = built.footprintPropertyPayloads;
    if (built.qualityLocked) {
      nav.setAttribute('hidden', 'hidden');
      nav.innerHTML = '';
    } else {
      nav.removeAttribute('hidden');
    }
    wireAtelierTabs(rootEl);
    if (window.BrandExplorerGoldDetail && window.BrandExplorerGoldDetail.applyChainScaleTheme) {
      window.BrandExplorerGoldDetail.applyChainScaleTheme(brand, rootEl);
    }
    if (document.documentElement.classList.contains('be-export-pdf')) {
      prepareAllPanelsForExport(rootEl);
    }
  }

  function mountAtelierFromBrand(brand) {
    var root = document.getElementById('beAtelierRoot');
    mountAtelierIntoRoot(root, brand);
  }

  function notifyExportReady() {
    if (!document.documentElement.classList.contains('be-export-pdf')) return;
    try {
      window.dispatchEvent(new CustomEvent('brand-explorer-export-ready'));
    } catch (_) {}
    try {
      if (window.parent && window.parent !== window) {
        window.parent.postMessage({ type: 'brand-explorer-export-ready' }, '*');
      }
    } catch (_) {}
    var p = new URLSearchParams(window.location.search || '');
    if (p.get('print') === '1' || p.get('print') === 'true') {
      window.setTimeout(function () {
        try {
          window.print();
        } catch (_) {}
      }, 400);
    }
  }

  var lastAtelierBrand = null;

  function onDetailLoaded(ev) {
    var brand = ev.detail && ev.detail.brand;
    if (!brand) return;
    lastAtelierBrand = brand;
    mountAtelierFromBrand(brand);
    var root = document.getElementById('brandRoot');
    if (isExportPdfMode()) {
      waitForExportImages(root).then(function () {
        notifyExportReady();
      });
      return;
    }
    notifyExportReady();
  }

  function onBrandListLoaded() {
    if (lastAtelierBrand) {
      mountAtelierFromBrand(lastAtelierBrand);
    }
  }

  wireBrandCaseStudyModalOnce();

  window.addEventListener('brand-explorer-detail-loaded', onDetailLoaded);
  window.addEventListener('brand-explorer-list-loaded', onBrandListLoaded);

  window.BrandExplorerAtelierFromApi = {
    mountIntoRoot: mountAtelierIntoRoot,
    mountFromBrand: mountAtelierFromBrand,
    prepareAllPanelsForExport: prepareAllPanelsForExport,
    buildPanelsHtmlForTest: buildAtelierPanelsHtml,
    renderOverviewHtmlForTest: renderAtelierOverview,
    shouldRenderFullProfile: shouldRenderFullProfile,
    externalShouldRenderFullProfile: externalShouldRenderFullProfile,
    isExternalQualityLocked: isExternalQualityLocked,
    isInternalPreviewRequest: isInternalPreviewRequest,
    isFactoryPreviewRequest: isFactoryPreviewRequest,
    canRenderFactoryPreview: canRenderFactoryPreview
  };
})();
