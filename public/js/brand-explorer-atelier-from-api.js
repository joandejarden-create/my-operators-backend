/**
 * Atelier-style explorer tabs on Brand Explorer (combined), fed by GET /api/brand-library/brand.
 * Tabs 1–8 mirror brand-education-voco Operator Explorer IA under .be-atelier-oe: fixed shells,
 * oe-section / scenario grids, and Brand Setup fields where present (blank nodes when absent).
 * When Brand Setup gold detail is present, appends structured gold tabs after Dealality Insight.
 */
(function () {
  'use strict';

  var ATELIER_TAB_DEFS = [
    { id: 'atelier-overview', label: 'Overview' },
    { id: 'atelier-value-owners', label: 'Value to<br>Owners' },
    { id: 'atelier-ops', label: 'Operations &<br>Standards' },
    { id: 'atelier-commercial', label: 'Commercial<br>Engine' },
    { id: 'atelier-loyalty', label: 'Loyalty<br>Program' },
    { id: 'atelier-footprint', label: 'Footprint &<br>Growth' },
    { id: 'atelier-materials', label: 'Brand<br>Materials' },
    { id: 'atelier-insight', label: 'Dealality<br>Insight' }
  ];

  function getGoldAppendTabs() {
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
      '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="m12 3-1.9 5.8a2 2 0 0 1-1.3 1.3L3 12l5.8 1.9a2 2 0 0 1 1.3 1.3L12 21l1.9-5.8a2 2 0 0 1 1.3-1.3L21 12l-5.8-1.9a2 2 0 0 1-1.3-1.3L12 3z" fill="none" stroke="currentColor" stroke-width="1.5"/></svg>'
  };

  var TAB_ICONS = [
    ICONS.overview,
    ICONS.chart,
    ICONS.ops,
    ICONS.bars,
    ICONS.star,
    ICONS.globe,
    ICONS.folder,
    ICONS.spark
  ];

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
    var div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
  }

  function hasVal(v) {
    if (v == null || v === '') return false;
    if (Array.isArray(v)) return v.length > 0;
    return true;
  }

  function isSafeHttpUrl(u) {
    if (!u || typeof u !== 'string') return false;
    var s = u.trim();
    return s.indexOf('https://') === 0 || s.indexOf('http://') === 0;
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
    return 'LINK';
  }

  /** Brand Explorer presentation: rows from GET brand.brandExplorer.blocks (see docs/brand-explorer-presentation-slots.md). */
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

  function explorerFirstBlock(brand, slotKey) {
    var rows = explorerBlocksForSlot(brand, slotKey);
    return rows.length ? rows[0] : null;
  }

  /** Multiple rows with same slotKey → list of { title, body } (sorted). */
  function explorerCardRowsForSlot(brand, slotKey) {
    return explorerBlocksForSlot(brand, slotKey).map(function (r) {
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
    return String(v);
  }

  function linkIfUrl(val) {
    var s = String(val || '').trim();
    if (!s) return '';
    if (s.indexOf('http') !== 0) return escapeHtml(s);
    return (
      '<a class="be-link" href="' +
      escapeHtml(s) +
      '" target="_blank" rel="noopener noreferrer">' +
      escapeHtml(s) +
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

  function footprintSummaryLine(fp) {
    if (!fp || typeof fp !== 'object') return '';
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
    var ms = m != null && m !== '' ? String(m).replace(/%/g, '').trim() : '';
    var fs = f != null && f !== '' ? String(f).replace(/%/g, '').trim() : '';
    if (ms && fs) return ms + '% managed · ' + fs + '% franchised';
    if (ms) return ms + '% managed';
    if (fs) return fs + '% franchised';
    return '';
  }

  /**
   * Management Option (atelier static: franchise vs managed posture).
   * Uses Footprint managed/franchised %; does not reuse Hotel Service Model (that is operational style).
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

  /** Portfolio & Performance: min/max property size (rooms) → snapshot “typical keys” line. */
  function typicalKeysRangeFromPortfolio(brand) {
    var pp = brand.portfolioPerformance || {};
    var minK = pp.minPropertySize;
    var maxK = pp.maxPropertySize;
    if (hasVal(minK) && hasVal(maxK)) return fmtNum(minK) + '–' + fmtNum(maxK) + ' rooms (stated brand band)';
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

  function oeKvBlock(title, rows) {
    var inner = (rows || [])
      .map(function (r) {
        return '<dt>' + escapeHtml(r.k) + '</dt>' + oeDd(r.v);
      })
      .join('');
    return '<div class="oe-cluster"><h3>' + escapeHtml(title) + '</h3><dl class="kv">' + inner + '</dl></div>';
  }

  function pipelineLineForProof(fp) {
    if (!fp || typeof fp !== 'object') return '';
    var pipH = (Number(fp.totalNewBuildHotels) || 0) + (Number(fp.totalConversionHotels) || 0);
    var pipR = (Number(fp.totalNewBuildRooms) || 0) + (Number(fp.totalConversionRooms) || 0);
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
    var fp = brand.footprint || {};
    var fv = fp.formValues || {};
    var footLine = footprintSummaryLine(fp);
    var loyaltyLine = loyaltyStrengthLine(brand);
    var geoFocus = regionOfferedLine(brand);
    var typicalUse = hasVal(fv.specificMarkets)
      ? String(fv.specificMarkets).trim()
      : hasVal(fp.priorityCities)
        ? String(fp.priorityCities).trim()
        : '';
    var devModel = [brand.brandDevelopmentStage, brand.brandModelFormat].filter(hasVal).join(' · ');

    var snapshotGrid =
      '<div class="oe-grid-2 oe-grid-2--snapshot">' +
      oeKvBlock('Identity & lineage', [
        { k: 'Parent Company', v: brand.parentCompany },
        { k: 'Brand Family', v: brand.brandArchitecture },
        { k: 'Launch Year', v: brand.yearBrandLaunched },
        { k: 'Brand Website', v: brand.brandWebsite }
      ]) +
      oeKvBlock('Product & segment', [
        { k: 'Segment', v: brand.hotelChainScale },
        { k: 'Brand Type', v: brand.brandModelFormat },
        { k: 'Service Level', v: brand.hotelServiceModel }
      ]) +
      oeKvBlock('Scale & geography', [
        { k: 'Typical Keys Range', v: typicalKeysRangeFromPortfolio(brand) },
        { k: 'Typical Use Case', v: typicalUse },
        { k: 'Geographic Focus', v: geoFocus }
      ]) +
      oeKvBlock('Development & positioning', [
        { k: 'Development Model', v: devModel },
        { k: 'Relative Positioning', v: brand.brandPositioning }
      ]) +
      '</div>';

    var posAudience =
      [brand.targetGuestSegments, brand.guestPsychographics, brand.brandCustomerPromise]
        .map(function (x) {
          return Array.isArray(x) ? x.filter(Boolean).join(', ') : x;
        })
        .filter(hasVal)
        .join(' ');

    var scenarioBodies = splitBullets(brand.keyBrandDifferentiators).slice(0, 3);
    while (scenarioBodies.length < 3) scenarioBodies.push('');
    var scenarioTitles = [
      'Urban Repositioning',
      'Leisure-Forward Conversions',
      'Boutique Resort Adjacency'
    ];
    var scen3Para = explorerParagraphs(brand, 'overview.scenarios', 3);
    var sj;
    for (sj = 0; sj < scen3Para.length; sj++) {
      if (hasVal(scen3Para[sj])) scenarioBodies[sj] = scen3Para[sj];
    }
    for (sj = 0; sj < 3; sj++) {
      var srowOv = explorerFirstBlock(brand, 'overview.scenario.' + (sj + 1));
      if (srowOv && hasVal(srowOv.body)) scenarioBodies[sj] = String(srowOv.body).trim();
      if (srowOv && hasVal(srowOv.title)) scenarioTitles[sj] = String(srowOv.title).trim();
    }
    var scenarioCards = scenarioTitles
      .map(function (title, i) {
        var body = scenarioBodies[i];
        var srowImg = explorerFirstBlock(brand, 'overview.scenario.' + (i + 1));
        var imgUrl =
          srowImg && hasVal(srowImg.imageUrl)
            ? String(srowImg.imageUrl).trim()
            : '';
        var visual = hasVal(imgUrl)
          ? '<div class="scenario-card__visual"><img src="' +
            escapeHtml(imgUrl) +
            '" alt="" loading="lazy" referrerpolicy="no-referrer"/></div>'
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

    var whySlotMerged = explorerMergedBody(brand, 'overview.why_value');
    var whyLines = hasVal(whySlotMerged)
      ? splitBullets(whySlotMerged)
      : splitBullets(brand.brandProfileAnalysis || brand.brandValueProposition);
    while (whyLines.length < 5) whyLines.push('');
    var whyList = whyLines
      .slice(0, 5)
      .map(function (line) {
        return '<li>' + (hasVal(line) ? escapeHtml(line) : '&nbsp;') + '</li>';
      })
      .join('');

    var diffIdentitySlot = explorerMergedBody(brand, 'overview.differentiators.identity');
    var diffCommercialSlot = explorerMergedBody(brand, 'overview.differentiators.commercial');
    var leftDiff;
    var rightDiff;
    if (hasVal(diffIdentitySlot) || hasVal(diffCommercialSlot)) {
      leftDiff = splitBullets(diffIdentitySlot);
      rightDiff = splitBullets(diffCommercialSlot);
    } else {
      var diffAll = splitBullets(brand.keyBrandDifferentiators);
      var mid = Math.ceil(diffAll.length / 2) || 0;
      leftDiff = diffAll.slice(0, mid);
      rightDiff = diffAll.slice(mid);
    }
    while (leftDiff.length < 4) leftDiff.push('');
    while (rightDiff.length < 4) rightDiff.push('');
    function diffUl(arr) {
      return (
        '<ul>' +
        arr
          .slice(0, 4)
          .map(function (x) {
            return '<li>' + (hasVal(x) ? escapeHtml(x) : '&nbsp;') + '</li>';
          })
          .join('') +
        '</ul>'
      );
    }

    var pillarParts = splitBullets(brand.brandPillars);
    var bestTitles = ['Conversion & Repositioning', 'Blended-Demand Markets', 'Owner Speed-to-Flag'];
    var bestCards = bestTitles
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
    var ownerOut = linesFromText(brand.brandValueProposition, 4).filter(hasVal).slice(0, 4);
    var exOwner = explorerMergedBody(brand, 'overview.owner_experience');
    var ownerEx = linesFromText(hasVal(exOwner) ? exOwner : brand.companyHistory, 4).filter(hasVal).slice(0, 4);

    var proofOpSlot = explorerMergedBody(brand, 'overview.proof_operator');
    var proofBodies = [
      footLine,
      pipelineLineForProof(fp),
      [brand.brandModelFormat, brand.brandDevelopmentStage].filter(hasVal).join(' · '),
      hasVal(fv.specificMarkets) ? fv.specificMarkets : fp.priorityCities || '',
      loyaltyLine,
      hasVal(proofOpSlot) ? proofOpSlot : brand.brandProfileAnalysis || ''
    ];
    var proofHeads = [
      'Global Open Footprint',
      'Pipeline Depth',
      'Conversion-Led Growth',
      'Multi-Region Relevance',
      loyaltyProofHeadline(brand),
      'Operator-Enabled Execution'
    ];
    var proofGrid = proofHeads
      .map(function (h, i) {
        var b = proofBodies[i];
        var empty = !hasVal(b);
        var bodyText = empty ? '&nbsp;' : escapeHtml(fmtCell(b));
        if (!empty && bodyText.length > 420) bodyText = bodyText.slice(0, 417) + '…';
        return (
          '<article class="proof-point-card"><div class="proof-point-card__icon">◇</div><h3 class="proof-point-card__headline">' +
          escapeHtml(h) +
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
      'Brand-Verified Materials'
    ];
    var themeChips = themeLabels
      .map(function (t) {
        return '<li>' + escapeHtml(t) + '</li>';
      })
      .join('');

    var featTitle = 'Featured Application · Conversion Example';
    var featLead = brand.brandTaglineMotto || '';
    var featBody = brand.brandPositioning || brand.brandCustomerPromise || '';
    var featSub = '';
    if (!hasVal(featBody) && !hasVal(featLead)) {
      featSub = '&nbsp;';
    } else {
      featSub =
        (hasVal(featLead) ? '<strong>' + escapeHtml(String(featLead)) + '</strong> — ' : '') +
        (hasVal(featBody)
          ? escapeHtml(String(featBody).slice(0, 220)) + (String(featBody).length > 220 ? '…' : '')
          : '');
    }
    var tagSrc = splitBullets(brand.brandPillars || brand.keyBrandDifferentiators).slice(0, 3);
    while (tagSrc.length < 3) tagSrc.push('');
    var featTags = tagSrc
      .map(function (t) {
        return (
          '<span class="tag-chip">' + (hasVal(t) ? escapeHtml(String(t)) : '&nbsp;') + '</span>'
        );
      })
      .join('');

    var ladderIdx = ladderIndexForScale(brand.hotelChainScale);
    var ladderTierLabels = [
      'Economy / Core Midscale',
      'Upper Mid / Mainstream Upscale',
      'Premium / Upper Upscale',
      'Luxury & Lifestyle Flagship'
    ];
    var ladderCells = ladderTierLabels
      .map(function (lbl, i) {
        var active = i === ladderIdx;
        var label = active ? (hasVal(brand.name) ? String(brand.name) : lbl) : lbl;
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

    var hasCaseStudies = explorerBlocksForSlot(brand, 'materials.caseStudy').length > 0;
    var caseStudiesJumpBtn =
      '<button type="button" class="btn btn--primary"' +
      (hasCaseStudies
        ? ' data-be-jump-atelier-tab="atelier-materials" title="Open Brand Materials — Case studies & proof of application"'
        : ' disabled title="Add Brand Explorer Presentation rows with Slot Key materials.caseStudy"') +
      '>View Case Studies</button>';

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
      '<section class="oe-section">' +
      '<h2 class="oe-section-title">Where This Brand Creates the Most Value</h2>' +
      '<div class="scenario-card-grid">' +
      scenarioCards +
      '</div>' +
      '<div class="oe-cluster"><h3>Why Value Is Strongest in These Scenarios</h3><ul>' +
      whyList +
      '</ul></div></section>' +
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
      '<p class="oe-section-hint">Brand-Verified Content · Curated by Dealality</p>' +
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
      caseStudiesJumpBtn +
      '</div></section>' +
      '<section class="oe-section">' +
      '<h2 class="oe-section-title">Portfolio Context</h2>' +
      '<p class="oe-section-hint">' +
      (hasVal(brand.parentCompany)
        ? 'Where <strong>' +
          escapeHtml(String(brand.name || 'This brand')) +
          '</strong> sits under <strong>' +
          escapeHtml(String(brand.parentCompany)) +
          '</strong> on an illustrative portfolio spectrum (not a quality ranking).'
        : 'Where this brand sits on an illustrative portfolio spectrum (not a quality ranking).') +
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

  function explorerDetailCard(label, bodyText) {
    var body = hasVal(bodyText)
      ? '<p class="explorer-detail-card__body">' +
        escapeHtml(fmtCell(bodyText)).replace(/\n/g, '<br>') +
        '</p>'
      : '<p class="explorer-detail-card__body oe-dd--empty">&nbsp;</p>';
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

  function timelinePhase(strong, spanDetail) {
    return (
      '<div class="timeline__phase"><strong>' +
      escapeHtml(strong) +
      '</strong><span>' +
      (hasVal(spanDetail) ? escapeHtml(fmtCell(spanDetail)) : '&nbsp;') +
      '</span></div>'
    );
  }

  function kpiCard(label, value) {
    return (
      '<div class="brand-markets-kpi__card">' +
      '<div class="brand-markets-kpi__label">' +
      escapeHtml(label) +
      '</div>' +
      (hasVal(value)
        ? '<div class="brand-markets-kpi__value">' + escapeHtml(fmtCell(value)) + '</div>'
        : '<div class="brand-markets-kpi__value oe-dd--empty">&nbsp;</div>') +
      '</div>'
    );
  }

  function presenceIntelCard(label, value) {
    return (
      '<div class="presence-intel-card">' +
      '<div class="presence-intel-card__label">' +
      escapeHtml(label) +
      '</div>' +
      (hasVal(value)
        ? '<div class="presence-intel-card__value">' + escapeHtml(fmtCell(value)) + '</div>'
        : '<div class="presence-intel-card__value oe-dd--empty">&nbsp;</div>') +
      '</div>'
    );
  }

  function demandCell(label, statusText) {
    var pill = hasVal(statusText)
      ? '<span class="status-pill">' + escapeHtml(statusText) + '</span>'
      : '<span class="status-pill status-pill--empty">&nbsp;</span>';
    return '<div class="demand-cell"><strong>' + escapeHtml(label) + '</strong> ' + pill + '</div>';
  }

  /** Trusted static HTML (education parity) — do not pass user input. */
  function commercialScenarioCardHtml(h4, htmlMain, htmlSample) {
    return (
      '<div class="scenario-card"><h4>' +
      escapeHtml(h4) +
      '</h4><p>' +
      (htmlMain || '&nbsp;') +
      '</p><p><span class="scenario-card__label">Sample Brand-to-Owner Message</span>' +
      (htmlSample || '&nbsp;') +
      '</p></div>'
    );
  }

  function fileCard(icon, title, hrefOpt) {
    var href = hrefOpt && isSafeHttpUrl(String(hrefOpt)) ? String(hrefOpt).trim() : '';
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
      '<div class="file-card__meta oe-dd--empty">&nbsp;</div>' +
      '<span class="file-card__badge">Unverified by Brand</span>' +
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
    var scenBodies = splitBullets(brand.brandValueProposition || brand.keyBrandDifferentiators);
    while (scenBodies.length < 4) scenBodies.push('');
    var scenFromSlot = explorerParagraphs(brand, 'valueOwners.scenarios', 4);
    var si;
    for (si = 0; si < scenFromSlot.length; si++) {
      if (hasVal(scenFromSlot[si])) scenBodies[si] = scenFromSlot[si];
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
      : splitBullets(brand.brandProfileAnalysis || brand.keyBrandDifferentiators);
    while (watchLines.length < 5) watchLines.push('');
    var watchUl = watchLines
      .slice(0, 5)
      .map(function (line) {
        return '<li>' + (hasVal(line) ? escapeHtml(line) : '&nbsp;') + '</li>';
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
      return timelinePhase(label, det);
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

  function renderOperationsStandards(brand) {
    var std = brand.brandStandards || {};
    var op = brand.operationalSupport || {};
    var grid =
      '<div class="oe-grid-2 oe-grid-2--operating-model">' +
      oeKvBlock('Structure & ownership', [
        { k: 'Primary Model', v: brand.brandModelFormat },
        { k: 'Management Option', v: managementOptionLine(brand) },
        { k: 'Typical Ownership Structure', v: typicalOwnershipStructureLine(brand) }
      ]) +
      oeKvBlock('Brand involvement & systems', [
        { k: 'Brand Involvement', v: brandInvolvementLine(brand) },
        { k: 'Systems Integration', v: opSystemsIntegrationLine(brand) },
        { k: 'Pre-opening Discipline', v: preOpeningServicesLine(brand) }
      ]) +
      oeKvBlock('Operations & complexity', [
        {
          k: 'Staffing Intensity',
          v: staffingIntensityLine(brand)
        },
        { k: 'F&B Complexity', v: std.brandFbProgramType },
        { k: 'Training Requirements', v: trainingRequirementsLine(brand) }
      ]) +
      oeKvBlock('Governance & technology', [
        { k: 'Reporting Discipline', v: std.brandCompliance },
        { k: 'QA Rhythm', v: qaRhythmLine(brand) },
        { k: 'Technology Expectations', v: opTechnologyExpectationsLine(brand) }
      ]) +
      '</div>';
    var flexLabels = [
      'Design Flexibility',
      'Conversion Friendliness',
      'Localization Flexibility',
      'Operational Rigidity',
      'PIP Sensitivity',
      'Prototype Dependence'
    ];
    var indRows = flexLabels
      .map(function (label) {
        return (
          '<div class="indicator-bar"><span class="indicator-bar__label">' +
          escapeHtml(label) +
          '</span><div class="indicator-bar__track"><div class="indicator-bar__fill indicator-bar__fill--empty"></div></div><span class="indicator-bar__tag oe-dd--empty">&nbsp;</span></div>'
        );
      })
      .join('');
    var thirdTags = [
      '3rd-Party Operator Friendly',
      'Best With Experienced Branded Operator',
      'Moderate Compliance Demands',
      'Better With Lifestyle/Full-Service Depth',
      'Stronger Fit for Organized Platforms'
    ];
    var tagRow =
      '<div class="tag-chip-row" style="margin:0">' +
      thirdTags
        .map(function (t) {
          return '<span class="tag-chip">' + escapeHtml(t) + '</span>';
        })
        .join('') +
      '</div>';
    var diffTitles = ['QA Cadence', 'Training Rigor', 'Reporting Expectations', 'Brand Interaction Frequency'];
    var qaCadence = qaRhythmLine(brand);
    var trainingRigor = trainingRequirementsLine(brand);
    var diffVals = [
      qaCadence || std.brandQaExpectations,
      trainingRigor || joinOpMulti(op.hrTrainingServices),
      std.brandCompliance,
      [op.communicationStyle, op.ownerResponseTime, op.decisionMaking].filter(hasVal).join(' · ') || std.brandStandardsNotes
    ];
    var diffGrid = diffTitles
      .map(function (title, i) {
        var val = diffVals[i];
        var inner = hasVal(val) ? escapeHtml(fmtCell(val)) : '&nbsp;';
        return '<div class="diff-card"><strong>' + escapeHtml(title) + '</strong><br/>' + inner + '</div>';
      })
      .join('');
    return wrapOe(
      '<section class="oe-section"><h2 class="oe-section-title">Operating Model</h2>' +
        grid +
        '</section>' +
        '<section class="oe-section"><h2 class="oe-section-title">Standards Philosophy</h2>' +
        explorerDetailCard('Philosophy', std.brandStandards) +
        '</section>' +
        '<section class="oe-section"><h2 class="oe-section-title">Flexibility Indicators</h2>' +
        '<div class="info-card"><div class="indicator-row">' +
        indRows +
        '</div></div></section>' +
        '<section class="oe-section"><h2 class="oe-section-title">Third-Party Operator Compatibility</h2>' +
        '<div class="explorer-detail-stack">' +
        explorerDetailCard('Summary', brand.brandProfileAnalysis) +
        tagRow +
        explorerDetailCard('Fit', brand.hotelServiceModel) +
        '</div></section>' +
        '<section class="oe-section"><h2 class="oe-section-title">Compliance &amp; Oversight</h2>' +
        '<div class="diff-grid">' +
        diffGrid +
        '</div></section>'
    );
  }

  function renderCommercialEngine(brand) {
    var COMM_STATIC = [
      [
        'Distribution & Retail Reach',
        'Puts the hotel in branded retail paths guests already use—CRS connectivity, brand.com and app, retail OTA relationships, and packages—so the property shows up in consideration sets where independents often under-index.',
        '“We expand your shelf space”—more qualified traffic without you funding a global platform alone. Directional example: participation in portfolio-wide retail campaigns and rate plans that match how guests actually shop.'
      ],
      [
        'Revenue Management & Pricing Discipline',
        'Helps translate demand into better revenue outcomes through forecasting tools, competitive sets, restriction strategies, and brand-level playbooks tuned to upper-upscale / luxury-adjacent positioning—not just discounting.',
        'Consistency between sales story and price: protecting ADR where the asset can support it and avoiding race-to-the-bottom in high-demand windows. Owners hear about 24/7 support models and escalation paths during peaks or shocks.'
      ],
      [
        'Digital Marketing & Performance Media',
        'Drives customers through paid/owned media, search, social, and retargeting at a scale few independents match—often with creative templates that still allow property-level storytelling.',
        'Lower customer-acquisition cost <em>at the margin</em> because spend is pooled; always-on brand search defending the flag; seasonal demand bursts aligned to holidays, events, and city calendars.'
      ],
      [
        'Corporate, SME & Group Pull',
        'Surfaces the hotel to contracted travelers, small meetings, and negotiated programs where the brand acts as a trusted filter—especially in urban and gateway markets with mixed transient/group mix.',
        'Access to RFP tools, account coverage, and brand-standard proposals that help sales teams open doors the property could not open as easily alone. Brands often quantify “addressable corporate demand” directionally by market tier.'
      ],
      [
        'Leisure & Destination Visibility',
        'Captures high-intent leisure shoppers through inspiration content, packages, partnerships, and destination narratives—critical when rate premium depends on aspiration and uniqueness.',
        'Creative differentiation (design, F&amp;B, local ties) <em>plus</em> distribution—so the story converts, not just looks good. Common owner talking point: “We help the right guests find you earlier in the journey.”'
      ],
      [
        'International & Feeder Markets',
        'Improves visibility to inbound guests and cross-border feeders where the brand’s global recognition reduces perceived risk—airport gateways, hub cities, and resort endpoints with international mix.',
        'Language, currency, and channel coverage in key feeder countries; participation in portfolio campaigns timed to holidays and carrier routes. Brands caveat performance by market maturity and airlift.'
      ],
      [
        'Sales & Catering Brand Pull',
        'Helps group and event buyers shortlist the property faster—brand credibility, lead flow from central inquiries, and tools for proposals where the hotel competes for weddings, SMERF, and small corporate meetings.',
        'Higher lead quality and faster “trust transfer” than an unknown independent; standardized collateral that still allows local customization. Owners evaluate contribution vs. in-house sales cost.'
      ],
      [
        'Reputation, Reviews & QA Lift',
        'Improves conversion after the click—guests choose brands they recognize; QA programs and service standards reduce variance that hurts reviews and repeat visits.',
        'Review response frameworks, service recovery playbooks, and brand-led recovery offers that protect long-term rate power. “Fewer surprises for guests” is a recurring sales line tied to RevPAR resilience.'
      ],
      [
        'Data, Analytics & Experimentation',
        'Gives owners and operators portfolio benchmarks, test-and-learn programs, and guest insights that refine offers, room types, and channel mix—turning soft demand signals into actions.',
        'Access to network learning (what works across similar assets), test campaigns, and reporting that lenders and institutional owners expect. Brands position this as <strong style="color:var(--text,#fff);font-weight:600;">commercial intelligence</strong>, not just reporting.'
      ]
    ];
    var scenGrid =
      '<div class="scenario-card-grid" style="grid-template-columns:repeat(3,1fr)">' +
      COMM_STATIC.map(function (row) {
        return commercialScenarioCardHtml(row[0], row[1], row[2]);
      }).join('') +
      '</div>';
    var kpis =
      '<div class="brand-markets-kpi" style="margin-bottom:16px" aria-label="Illustrative Commercial Footprint">' +
      kpiCard('Channels & Brand Names in Materials', 'Brand.com · major OTAs · GDS · metasearch') +
      kpiCard('Campaign Story in Owner Docs', 'Always-on + seasonal / market bursts') +
      kpiCard('B2B Storyline', 'RFP & account programs (where active)') +
      kpiCard('Lens Owners Still Apply', 'Net contribution after costs') +
      '</div>';
    var anchorUl =
      '<li><strong style="color:var(--text,#fff);font-weight:600;">“More demand at the top of the funnel”</strong> — retail presence, search, and inspiration media guests see before they choose a city or date.</li>' +
      '<li><strong style="color:var(--text,#fff);font-weight:600;">“Better conversion at the bottom”</strong> — trust, reviews, loyalty, and frictionless booking paths that turn lookers into stays.</li>' +
      '<li><strong style="color:var(--text,#fff);font-weight:600;">“Repeat and higher-quality guests”</strong> — loyalty, corporate accounts, and recognition that increase lifetime value versus one-off OTA transactions.</li>' +
      '<li><strong style="color:var(--text,#fff);font-weight:600;">“Commercial systems, not just a logo”</strong> — pricing, sales support, and analytics framed as how the brand helps owners <em>earn</em> the fee.</li>';
    var COMM_DEMAND = [
      ['Gateway Urban', 'Strong'],
      ['Regional & Secondary Upscale', 'Moderate–strong'],
      ['Corporate-Led Urban', 'Strong'],
      ['Resort / Coastal Leisure', 'Strong'],
      ['Conversion / Repositioning', 'Strong'],
      ['Pure Economy / Highway', 'Not a fit']
    ];
    var demandHtml = COMM_DEMAND.map(function (pair) {
      return demandCell(pair[0], pair[1]);
    }).join('');
    var commIntro =
      '<p class="explorer-detail-card__body">This section is built as a <strong style="color:var(--text,#fff);font-weight:600;">marketing reference</strong>: for each commercial lever you get the underlying business idea, then a <strong style="color:var(--text,#fff);font-weight:600;">sample brand-to-owner message</strong>—the kind of language and storyline brands use in decks, calls, and emails to show how affiliation helps <strong style="color:var(--text,#fff);font-weight:600;">pull demand and protect rate</strong>. Use it to understand positioning, not as a performance guarantee.</p>';
    return wrapOe(
      '<section class="oe-section">' +
        '<h2 class="oe-section-title">Commercial Strengths</h2>' +
        '<p class="oe-section-hint">Brand-to-Owner Messaging — Sample Narratives and Proof Points Used in Development, Diligence, and Sales Conversations (Illustrative; Not Property-Specific Performance)</p>' +
        '<div class="explorer-detail-card" style="margin-bottom:14px">' +
        '<h3 class="explorer-detail-card__label">How Brands Communicate Commercial Value</h3>' +
        commIntro +
        '</div>' +
        kpis +
        scenGrid +
        '<div class="oe-cluster" style="margin-top:4px"><h3>Anchor Lines Brands Repeat in Owner-Facing Materials</h3><ul>' +
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

  function renderLoyaltyProgram(brand) {
    var fp = brand.footprint || {};
    var fpFv = fp.formValues || {};
    var memM = lcFv(brand, 'totalGlobalMembersMillions');
    var defMembersVal = hasVal(memM) ? '~' + String(memM).trim() + 'M members (est.)' : '—';
    var defHotelsVal =
      hasVal(fp.totalExistingHotels) || Number(fp.totalExistingHotels) === 0
        ? fmtNum(fp.totalExistingHotels) + '+ hotels in portfolio (open)'
        : '—';
    var marketsN = fpFv.numberOfMarkets;
    var defMarketsVal = hasVal(marketsN)
      ? fmtNum(marketsN) +
        ' markets · ' +
        (hasVal(fpFv.specificMarkets)
          ? String(fpFv.specificMarkets).trim()
          : hasVal(fp.priorityCities)
            ? String(fp.priorityCities).trim()
            : 'regional mix')
      : '—';
    var mixPct = lcFv(brand, 'typicalLoyaltyRoomsPercent');
    var defMixVal = hasVal(mixPct) ? '~' + String(mixPct).trim() + '% of rooms from loyalty (est.)' : '—';

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

    var earnMerged = explorerMergedBody(brand, 'loyalty.earn', '\n');
    var earnUl = explorerLinesAsUl(escapeHtml, earnMerged);
    if (!earnUl) {
      earnUl =
        '<ul>' +
        '<li><strong>Base Earn:</strong> 10 points per US$1 eligible spend on room and qualifying folio charges.</li>' +
        '<li><strong>Promotions:</strong> Seasonal accelerators on direct bookings (e.g. +2–4k bonus points per stay during campaign windows).</li>' +
        '<li><strong>Partners:</strong> Car, rideshare, and retail partners with periodic bonus campaigns.</li>' +
        '</ul>';
    }
    var redeemMerged = explorerMergedBody(brand, 'loyalty.redeem', '\n');
    var redeemUl = explorerLinesAsUl(escapeHtml, redeemMerged);
    if (!redeemUl) {
      redeemUl =
        '<ul>' +
        '<li><strong>Free Nights:</strong> Dynamic award nights tied to demand (illustrative band: 22k–58k points/night by season).</li>' +
        '<li><strong>Cash + Points:</strong> Partial redemption options on direct paths where enabled.</li>' +
        '<li><strong>Experiences:</strong> Curated events and on-property credits in select tiers.</li>' +
        '</ul>';
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
      'Owners typically evaluate loyalty through net contribution after costs: member discounts, channel mix, and redemption liability assumptions. Brands highlight disciplined revenue management alignment so member constructs support rate integrity—not only top-line room nights.';
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
        explorerDetailCard('P&amp;L &amp; Contribution', implPnl) +
        explorerDetailCard('Operations &amp; Guest Experience', implOps) +
        explorerDetailCard('Systems &amp; Data', implSys) +
        '</div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Where Loyalty Lifts Demand Most</h2>' +
        '<p class="oe-section-hint">Directional Fit Labels</p>' +
        '<div class="demand-matrix">' +
        loyaltyDemand +
        '</div></section>'
    );
  }

  function renderFootprintGrowth(brand) {
    var fp = brand.footprint || {};
    var fv = fp.formValues || {};
    var pipH = (Number(fp.totalNewBuildHotels) || 0) + (Number(fp.totalConversionHotels) || 0);
    var pipR = (Number(fp.totalNewBuildRooms) || 0) + (Number(fp.totalConversionRooms) || 0);
    var reg = fp.regionalDistribution && typeof fp.regionalDistribution === 'object' ? fp.regionalDistribution : {};
    var regionKeys = Object.keys(reg);
    var regionsSummary =
      regionKeys.length > 0
        ? regionKeys.slice(0, 8).join(' · ')
        : hasVal(fv.specificMarkets)
          ? String(fv.specificMarkets).trim()
          : '';
    var kpiRow =
      '<div class="brand-markets-kpi" aria-label="Markets and Footprint Summary">' +
      kpiCard('Regions (IHG-Style)', regionsSummary) +
      kpiCard('Markets Operated In', fv.numberOfMarkets) +
      kpiCard('Open Hotels (Public YE2025)', fp.totalExistingHotels) +
      kpiCard('Coverage Model', [brand.brandModelFormat, brand.hotelChainScale].filter(hasVal).join(' · ')) +
      '</div>';
    var openH = fp.totalExistingHotels;
    var openR = fp.totalExistingRooms;
    var tableRow =
      '<tr class="brand-ft-data-row">' +
      '<th scope="row">Total (Public IHG Figures, Illustrative)</th>' +
      '<td>' +
      (hasVal(openH) ? escapeHtml(fmtNum(openH)) : '&nbsp;') +
      '</td><td>' +
      (hasVal(openR) ? escapeHtml(fmtNum(openR)) : '&nbsp;') +
      '</td><td>' +
      (pipH > 0 ? escapeHtml(fmtNum(pipH)) : '&nbsp;') +
      '</td><td>' +
      (pipR > 0 ? escapeHtml(fmtNum(pipR)) : '&nbsp;') +
      '</td></tr>';
    var fp8Empty =
      '<tr><th scope="row">&nbsp;</th><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td></tr>';
    var fp8Head =
      '<thead><tr>' +
      '<th scope="col">Region</th>' +
      '<th scope="col">Open Hotels</th>' +
      '<th scope="col">Open Rooms</th>' +
      '<th scope="col">Pipeline Hotels</th>' +
      '<th scope="col">Pipeline Rooms</th>' +
      '<th scope="col">Total Hotels</th>' +
      '<th scope="col">Total rooms</th>' +
      '<th scope="col">Avg Keys</th>' +
      '</tr></thead><tbody>';
    var fp8HeadCountry =
      '<thead><tr>' +
      '<th scope="col">Country</th>' +
      '<th scope="col">Open Hotels</th>' +
      '<th scope="col">Open Rooms</th>' +
      '<th scope="col">Pipeline Hotels</th>' +
      '<th scope="col">Pipeline Rooms</th>' +
      '<th scope="col">Total Hotels</th>' +
      '<th scope="col">Total rooms</th>' +
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
      '<th scope="col">Total rooms</th>' +
      '<th scope="col">Avg Keys</th>' +
      '</tr></thead><tbody>';
    var distRows = '';
    if (regionKeys.length) {
      distRows = regionKeys
        .map(function (region) {
          var o = reg[region] || {};
          var h = o.hotels;
          var r = o.rooms;
          return (
            '<tr><th scope="row">' +
            escapeHtml(region) +
            '</th><td>' +
            (hasVal(h) ? escapeHtml(fmtNum(h)) : '&nbsp;') +
            '</td><td>' +
            (hasVal(r) ? escapeHtml(fmtNum(r)) : '&nbsp;') +
            '</td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td></tr>'
          );
        })
        .join('');
    } else {
      distRows = fp8Empty;
    }
    var regionPanel =
      '<div class="brand-fp-table-wrap brand-fp-panel brand-fp-panel-region" role="tabpanel" aria-label="By Region">' +
      '<table class="brand-fp-table">' +
      fp8Head +
      distRows +
      '</tbody></table></div>';
    var countryPanel =
      '<div class="brand-fp-table-wrap brand-fp-panel brand-fp-panel-country" role="tabpanel" aria-label="By Country">' +
      '<table class="brand-fp-table">' +
      fp8HeadCountry +
      fp8Empty +
      '</tbody></table></div>';
    var archetypePanel =
      '<div class="brand-fp-table-wrap brand-fp-panel brand-fp-panel-archetype" role="tabpanel" aria-label="By Asset Archetype">' +
      '<table class="brand-fp-table">' +
      fp8HeadArchetype +
      fp8Empty +
      '</tbody></table></div>';
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
      '<p class="brand-fp-table-note">Illustrative Directional View · Not Audited Financials or Property-Level Disclosure</p>' +
      '</div>';
    var openPipelineSub =
      '<div class="brand-fp-subsection">' +
      '<h3 class="brand-fp-table-title">Open vs. Pipeline (Portfolio)</h3>' +
      '<div class="brand-fp-table-wrap" role="region" aria-label="Open Versus Pipeline Portfolio Totals">' +
      '<table class="brand-fp-table">' +
      '<thead><tr><th scope="col"></th>' +
      '<th scope="col">Open Hotels</th><th scope="col">Open Rooms</th>' +
      '<th scope="col">Pipeline Hotels</th><th scope="col">Pipeline Rooms</th></tr></thead><tbody>' +
      tableRow +
      '</tbody></table></div></div>';
    var metricsBlock = '<div class="brand-fp-metrics">' + openPipelineSub + portfolioDistribution + '</div>';
    var presenceRow =
      '<div class="presence-intel-row">' +
      presenceIntelCard('Open Hotels (Public)', openH) +
      presenceIntelCard('Pipeline (Public)', pipH > 0 ? pipH : '') +
      presenceIntelCard('Primary Regions', regionsSummary) +
      presenceIntelCard('Typical Asset Pattern', brand.hotelServiceModel) +
      presenceIntelCard('Growth Style', brand.brandDevelopmentStage) +
      presenceIntelCard('Brand Maturity', brand.yearBrandLaunched) +
      '</div>';
    var geoSrc = String(brand.brandProfileAnalysis || fv.specificMarkets || '').trim();
    var geoIntro;
    if (!hasVal(geoSrc)) {
      geoIntro =
        '<p style="font-size:0.8125rem;color:#d7e4fa;margin:0 0 14px;max-width:720px;line-height:1.5" class="oe-dd--empty">&nbsp;</p>';
    } else {
      var geoSn = geoSrc.slice(0, 420);
      geoIntro =
        '<p style="font-size:0.8125rem;color:#d7e4fa;margin:0 0 14px;max-width:720px;line-height:1.5">' +
        escapeHtml(geoSn) +
        (geoSrc.length > 420 ? '…' : '') +
        '</p>';
    }
    var schematic =
      '<div class="footprint-schematic" aria-hidden="true">' +
      '<div class="footprint-schematic__seg footprint-schematic__seg--on">Americas</div>' +
      '<div class="footprint-schematic__seg footprint-schematic__seg--on">EMEAA</div>' +
      '<div class="footprint-schematic__seg footprint-schematic__seg--on">Greater China</div>' +
      '<div class="footprint-schematic__seg footprint-schematic__seg--on">APAC Leisure</div>' +
      '<div class="footprint-schematic__seg footprint-schematic__seg--off">Select LATAM</div>' +
      '</div>' +
      '<p class="footprint-schematic__caption">Schematic Presence Strip · Illustrative, Not Geographic Precision</p>';
    function regionStatusCard(name, dim, statusClass, statusLabel) {
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
        escapeHtml(statusLabel) +
        '</span>' +
        '<p class="oe-dd--empty">&nbsp;</p></div>'
      );
    }
    var regionGrid =
      '<div class="region-footprint-grid">' +
      regionStatusCard('EMEAA', false, 'status-label--established', 'Strong Conversion Activity') +
      regionStatusCard('Americas', false, 'status-label--established', 'Established Presence') +
      regionStatusCard('Asia–Pacific', false, 'status-label--emerging', 'High Relevance') +
      regionStatusCard('Greater China', true, 'status-label--selective', 'Selective / Market-Specific') +
      regionStatusCard('Latin America', true, 'status-label--limited', 'Limited vs Other Regions') +
      '</div>';
    var growthChips =
      '<span class="tag-chip">Urban Repositioning</span>' +
      '<span class="tag-chip">Independent Conversions</span>' +
      '<span class="tag-chip">Boutique Leisure</span>' +
      '<span class="tag-chip">Selected Gateway City Entries</span>' +
      '<span class="tag-chip">Resort-Adjacent Lifestyle Assets</span>' +
      '<span class="tag-chip">Design-Led Affiliation Opportunities</span>';
    var growthRightP = hasVal(brand.brandProfileAnalysis)
      ? '<p>' + escapeHtml(fmtCell(brand.brandProfileAnalysis)).replace(/\n/g, '<br>') + '</p>'
      : '<p class="oe-dd--empty">&nbsp;</p>';
    var growthSection =
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
      '<ul>' +
      '<li>Urban Conversions</li>' +
      '<li>Premium Independent Repositioning</li>' +
      '<li>Lifestyle-Forward Leisure Assets</li>' +
      '<li>Smaller Full-Service or Boutique Assets With a Strong Story</li>' +
      '</ul></div></div></div></section>';
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
    var openingsSection =
      '<section class="oe-section" style="margin-top:8px">' +
      '<h2 class="oe-section-title">Openings / Examples / Properties</h2>' +
      '<p class="oe-section-hint">Curated · Not a Full Directory</p>' +
      '<p style="font-size:0.8125rem;color:#d7e4fa;margin:0 0 16px;max-width:820px;line-height:1.5">' +
      '<strong style="color:var(--text,#fff);font-weight:600">Illustrative placeholders</strong> — Property cards mirror the education layout; add verified examples when available.</p>' +
      '<div class="property-example-grid">' +
      propertyShell() +
      propertyShell() +
      propertyShell() +
      '</div></section>';
    var momentumSection =
      '<section class="oe-section">' +
      '<h2 class="oe-section-title">Recent Momentum</h2>' +
      '<p class="oe-section-hint">Illustrative Activity</p>' +
      '<p class="momentum-feed__label">Illustrative Activity View</p>' +
      '<div class="momentum-feed">' +
      '<div class="momentum-feed__item"><div class="momentum-feed__date">Jan 2026</div><div class="momentum-feed__body"><strong>New urban conversion opening added to portfolio</strong><p>Illustrative example of continued momentum in independent repositioning.</p></div></div>' +
      '<div class="momentum-feed__item"><div class="momentum-feed__date">Feb 2026</div><div class="momentum-feed__body"><strong>Expanded leisure-forward presence in resort-adjacent market</strong><p>Signals relevance beyond pure urban applications.</p></div></div>' +
      '<div class="momentum-feed__item"><div class="momentum-feed__date">Mar 2026</div><div class="momentum-feed__body"><strong>Pipeline entry in gateway European market</strong><p>Supports the brand’s emerging premium affiliation story in select cities.</p></div></div>' +
      '<div class="momentum-feed__item"><div class="momentum-feed__date">Q1 2026</div><div class="momentum-feed__body"><strong>Repeat-owner affiliation interest</strong><p>Suggests continued appeal among owners seeking flexibility with stronger commercial structure.</p></div></div>' +
      '</div>' +
      '<div class="portfolio-mix">' +
      '<h3>Portfolio Mix</h3>' +
      '<div class="portfolio-mix__row">' +
      '<span class="portfolio-mix__pill"><strong>Urban</strong> High</span>' +
      '<span class="portfolio-mix__pill"><strong>Leisure / Resort-Adjacent</strong> Moderate</span>' +
      '<span class="portfolio-mix__pill"><strong>Secondary Market</strong> Selective</span>' +
      '<span class="portfolio-mix__pill"><strong>New Build Prototype-Led</strong> Low</span>' +
      '<span class="portfolio-mix__pill"><strong>Conversion / Repositioning</strong> High</span>' +
      '</div></div></section>';
    var fpEditorialP;
    if (hasVal(brand.brandProfileAnalysis)) {
      fpEditorialP =
        '<p>' + escapeHtml(fmtCell(brand.brandProfileAnalysis)).replace(/\n/g, '<br>') + '</p>';
    } else if (hasVal(brand.name)) {
      fpEditorialP =
        '<p>' +
        escapeHtml(String(brand.name).trim()) +
        '\u2019s footprint reads as \u201cPremium scale with conversion DNA\u201d: large enough for parent-network retail credibility, still positioned as personality-forward versus rigid prototype brands. It tends to show best where owners need systems and loyalty\u2014not a bespoke luxury operating theater.</p>';
    } else {
      fpEditorialP = '<p class="oe-dd--empty">&nbsp;</p>';
    }
    var fpEditorialUl =
      '<ul>' +
      '<li>Strongest Current Relevance in Urban Repositioning</li>' +
      '<li>Credible Use in Boutique Leisure and Resort-Adjacent Contexts</li>' +
      '<li>Still Selective Rather Than Ubiquitous</li>' +
      '<li>More Compelling Where Asset Identity Already Exists</li>' +
      '</ul>';
    var dealalitySection =
      '<section class="oe-section">' +
      '<h2 class="oe-section-title">Dealality View on Market Presence</h2>' +
      '<p class="oe-section-hint">Interpretation</p>' +
      '<div class="dealality-editorial-card">' +
      '<div class="dealality-editorial-card__brand">Dealality</div>' +
      fpEditorialP +
      fpEditorialUl +
      '</div></section>';
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

  function wireBrandCaseStudyModalOnce() {
    if (window._beCaseStudyModalDocumentWired) return;
    window._beCaseStudyModalDocumentWired = true;
    document.addEventListener('click', function (e) {
      var btn = e.target.closest('[data-be-case-summary]');
      if (!btn) return;
      var panelsWrap = btn.closest('[data-be-atelier-panels]');
      if (!panelsWrap || !Array.isArray(panelsWrap._beCaseStudyPayloads)) return;
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

  function renderBrandMaterials(brand, caseStudyPayloadSink) {
    if (!caseStudyPayloadSink) caseStudyPayloadSink = [];
    var caseTags = [
      'Conversion Example',
      'Resort Example',
      'Urban Example',
      'Operator-Led Example',
      'Repositioning Example',
      'Boutique Asset',
      'Independent Reflag'
    ];
    var tagRow =
      '<div class="tag-chip-row" style="margin-bottom:14px" aria-label="Proof Taxonomy">' +
      caseTags
        .map(function (t) {
          return '<span class="tag-chip">' + escapeHtml(t) + '</span>';
        })
        .join('') +
      '</div>';
    function caseStudyShell() {
      return (
        '<article class="case-study-card">' +
        '<div class="case-study-card__thumb" aria-hidden="true"></div>' +
        '<div class="case-study-card__body">' +
        '<div class="case-study-card__chips"><span class="tag-chip oe-dd--empty">&nbsp;</span></div>' +
        '<h4 class="oe-dd--empty">&nbsp;</h4>' +
        '<div class="case-study-card__loc oe-dd--empty">&nbsp;</div>' +
        '<div class="case-study-card__asset oe-dd--empty">&nbsp;</div>' +
        '<p class="case-study-card__narr"><span class="case-study-card__narr-label">Situation.</span> <span class="case-study-card__narr-text oe-dd--empty">&nbsp;</span></p>' +
        '<p class="case-study-card__narr"><span class="case-study-card__narr-label">Why the brand was relevant.</span> <span class="case-study-card__narr-text oe-dd--empty">&nbsp;</span></p>' +
        '<div class="case-study-card__takeaway">' +
        '<div class="case-study-card__takeaway-kicker" role="presentation">' +
        '<span class="case-study-card__takeaway-kicker-up">Owner takeaway</span> ' +
        '<span class="case-study-card__takeaway-kicker-sub">(Dealality summary)</span>' +
        '</div>' +
        '<span class="case-study-card__takeaway-body"><span class="oe-dd--empty">&nbsp;</span></span>' +
        '</div>' +
        '<div class="case-study-card__actions">' +
        '<button type="button" class="btn case-study-card__btn" disabled>View Summary</button>' +
        '</div>' +
        '</div></article>'
      );
    }
    function materialsFileHref(block) {
      if (!block) return '';
      var fromBody = firstHttpUrlInString(block.body);
      if (fromBody) return fromBody;
      var img = hasVal(block.imageUrl) ? String(block.imageUrl).trim() : '';
      return isSafeHttpUrl(img) ? img : '';
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
    function caseStudyFromBlock(block) {
      var title = block && hasVal(block.title) ? String(block.title).trim() : '';
      var bodyRaw = block && hasVal(block.body) ? String(block.body).trim() : '';
      var split = splitCaseStudyCardBodyAndModalAppendix(bodyRaw);
      var p = parseCaseStudyParas(split.cardBody);
      var modalAppendixParsed = parseCaseSummaryModalAppendix(split.modalAppendix);
      var chipParts = p.chips
        ? p.chips
            .split(',')
            .map(function (s) {
              return s.trim();
            })
            .filter(Boolean)
        : [];
      var chipsHtml = chipParts.length
        ? chipParts
            .map(function (c) {
              return '<span class="tag-chip">' + escapeHtml(c) + '</span>';
            })
            .join('')
        : '<span class="tag-chip oe-dd--empty">&nbsp;</span>';
      var imgUrl = block && hasVal(block.imageUrl) ? String(block.imageUrl).trim() : '';
      var thumbExtra = '';
      if (imgUrl && isSafeHttpUrl(imgUrl)) {
        thumbExtra =
          ' style="background-image:url(' +
          escapeHtml(imgUrl) +
          ');background-size:cover;background-position:center"';
      }
      function narrBlock(label, val) {
        if (!hasVal(val)) {
          return (
            '<p class="case-study-card__narr"><span class="case-study-card__narr-label">' +
            escapeHtml(label) +
            '</span> <span class="case-study-card__narr-text oe-dd--empty">&nbsp;</span></p>'
          );
        }
        return (
          '<p class="case-study-card__narr"><span class="case-study-card__narr-label">' +
          escapeHtml(label) +
          '</span> <span class="case-study-card__narr-text">' +
          escapeHtml(val) +
          '</span></p>'
        );
      }
      function takeawayHtml(val) {
        var head =
          '<div class="case-study-card__takeaway-kicker" role="presentation">' +
          '<span class="case-study-card__takeaway-kicker-up">Owner takeaway</span> ' +
          '<span class="case-study-card__takeaway-kicker-sub">(Dealality summary)</span>' +
          '</div>';
        if (!hasVal(val)) {
          return (
            '<div class="case-study-card__takeaway">' +
            head +
            '<span class="case-study-card__takeaway-body"><span class="oe-dd--empty">&nbsp;</span></span>' +
            '</div>'
          );
        }
        return (
          '<div class="case-study-card__takeaway">' +
          head +
          '<span class="case-study-card__takeaway-body">' +
          escapeHtml(val) +
          '</span>' +
          '</div>'
        );
      }
      var summaryFromField =
        block && hasVal(block.summaryUrl) && isSafeHttpUrl(String(block.summaryUrl).trim())
          ? String(block.summaryUrl).trim()
          : '';
      var summaryHref = summaryFromField || p.summaryHref;
      var modalPayload = buildCaseStudyModalPayload(block, p, chipParts, summaryHref, modalAppendixParsed);
      var summaryIdx = caseStudyPayloadSink.length;
      caseStudyPayloadSink.push(modalPayload);
      var summaryBtn =
        '<button type="button" class="btn case-study-card__btn" data-be-case-summary="' +
        summaryIdx +
        '">View Summary</button>';
      return (
        '<article class="case-study-card">' +
        '<div class="case-study-card__thumb"' +
        thumbExtra +
        ' aria-hidden="true"></div>' +
        '<div class="case-study-card__body">' +
        '<div class="case-study-card__chips">' +
        chipsHtml +
        '</div>' +
        '<h4>' +
        (hasVal(title) ? escapeHtml(title) : '<span class="oe-dd--empty">&nbsp;</span>') +
        '</h4>' +
        '<div class="case-study-card__loc">' +
        (hasVal(p.loc) ? escapeHtml(p.loc) : '<span class="oe-dd--empty">&nbsp;</span>') +
        '</div>' +
        '<div class="case-study-card__asset">' +
        (hasVal(p.asset) ? escapeHtml(p.asset) : '<span class="oe-dd--empty">&nbsp;</span>') +
        '</div>' +
        narrBlock('Situation.', p.situation) +
        narrBlock('Why the brand was relevant.', p.why) +
        takeawayHtml(p.takeaway) +
        '<div class="case-study-card__actions">' +
        summaryBtn +
        '</div>' +
        '</div></article>'
      );
    }
    var fileRows = explorerBlocksForSlot(brand, 'materials.file');
    var fileGrid;
    if (fileRows.length) {
      fileGrid = fileRows
        .map(function (row) {
          var href = materialsFileHref(row);
          var label = hasVal(row.title) ? String(row.title).trim() : '';
          var kind = href ? fileKindLabelFromUrl(href, label) : 'FILE';
          if (!label && href) {
            try {
              label = decodeURIComponent(String(href).split('/').pop() || '').split('?')[0] || 'Download';
            } catch (e) {
              label = 'Download';
            }
          }
          if (!label) label = 'Brand material';
          return fileCard(kind, label, href);
        })
        .join('');
    } else {
      fileGrid =
        fileCard('PDF', 'Brand Overview Deck.pdf') +
        fileCard('PDF', 'Development Snapshot.pdf') +
        fileCard('PDF', 'Positioning Summary.pdf') +
        fileCard('ZIP', 'Design Reference Gallery.zip');
    }
    var csBlocks = explorerBlocksForSlot(brand, 'materials.caseStudy');
    var placeholders = caseStudyShell() + caseStudyShell() + caseStudyShell();
    var caseStudyHtml = csBlocks.length ? csBlocks.map(caseStudyFromBlock).join('') : placeholders;
    var galleryLabels = ['Lobby', 'Guest Room', 'Rooftop / Bar', 'Arrival', 'Pool & Resort Setting', 'Restaurant'];
    var gallery = galleryLabels
      .map(function (lab, i) {
        var slot = 'materials.gallery.' + (i + 1);
        var row = explorerFirstBlock(brand, slot);
        var imgUrl = row && hasVal(row.imageUrl) ? String(row.imageUrl).trim() : '';
        var caption = row && hasVal(row.title) ? String(row.title).trim() : lab;
        if (imgUrl && isSafeHttpUrl(imgUrl)) {
          return (
            '<div class="gallery-card gallery-card--has-image" role="img" aria-label="' +
            escapeHtml(caption) +
            '"><img src="' +
            escapeHtml(imgUrl) +
            '" alt="" loading="lazy" decoding="async" referrerpolicy="no-referrer" />' +
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
    return wrapOe(
      '<section class="oe-section">' +
        '<h2 class="oe-section-title">Official Brand Materials</h2>' +
        '<p class="oe-section-hint">Unverified by Brand · Populate with Brand Explorer Presentation rows (<code>materials.file</code>)</p>' +
        '<div class="file-card-grid">' +
        fileGrid +
        '</div></section>' +
        '<section class="oe-section" id="case-studies-section" style="margin-top:8px">' +
        '<h2 class="oe-section-title">Case Studies &amp; Proof of Application</h2>' +
        '<p class="oe-section-hint">Unverified by Brand · Curated by Dealality · Optional slot <code>materials.caseStudy</code></p>' +
        '<p class="case-study-intro"><strong>How the brand shows up in practice</strong> — Each card can name a real, open property with a verifiable photo. Dealality’s situation and takeaway copy is editorial (not verified by the brand or a statement of property-level performance).</p>' +
        tagRow +
        '<div class="case-study-grid">' +
        caseStudyHtml +
        '</div>' +
        '<div class="locked-mini-grid">' +
        '<div class="locked-mini"><h5>🔒 Performance Detail</h5><p>Available only where public or shared during active evaluation. No property-level RevPAR, ADR, or occupancy claims shown here.</p></div>' +
        '<div class="locked-mini"><h5>🔒 Deal Structure Context</h5><p>Visible in Match &amp; Compare or during brand-approved diligence. Fee economics and key terms remain gated.</p></div>' +
        '</div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Image Gallery</h2>' +
        '<p class="oe-section-hint">Slots <code>materials.gallery.1</code> … <code>materials.gallery.6</code> — attach image to each row’s Image field</p>' +
        '<div class="gallery-grid">' +
        gallery +
        '</div></section>'
    );
  }

  function renderDealalityInsight(brand) {
    var STRENGTH_PAIRS = [
      ['Conversion & reflag velocity', 'When the goal is faster IHG integration vs. long new-build cycles.'],
      ['IHG-heavy corridors', 'Where IHG recognition, corporate accounts, and loyalty meaningfully shift share.'],
      ['Upscale full-service bones', 'Assets that already run full-service but need brand-led retail discipline.'],
      ['Operator + franchise flexibility', 'Structures where IHG-approved operators can execute Premium standards reliably.']
    ];
    var CAUTION_PAIRS = [
      ['Product not truly upscale', 'If rooms and public spaces cannot support Premium ADR, fees won’t clear.'],
      ['Markets with weak IHG relevance', 'In some regions, another flag may convert demand more efficiently.'],
      ['Operators unaccustomed to IHG tooling', 'Systems and training load can overwhelm lightly resourced teams.'],
      ['Expectations of ultra-luxury', '']
    ];
    var bn = brand.name ? String(brand.name).trim() : 'this brand';
    var cautionUltraBody =
      escapeHtml(bn) +
      ' is Premium—not a substitute for InterContinental or Luxury & Lifestyle positioning.';
    var strengthGrid = STRENGTH_PAIRS.map(function (row) {
      return scenarioDetailCard(row[0], row[1]);
    }).join('');
    var cautionGrid =
      scenarioDetailCard(CAUTION_PAIRS[0][0], CAUTION_PAIRS[0][1]) +
      scenarioDetailCard(CAUTION_PAIRS[1][0], CAUTION_PAIRS[1][1]) +
      scenarioDetailCard(CAUTION_PAIRS[2][0], CAUTION_PAIRS[2][1]) +
      scenarioDetailCard(CAUTION_PAIRS[3][0], cautionUltraBody);
    var checklist =
      '<ul class="explorer-detail-card__list checklist">' +
      '<li>Does the asset meet ' +
      escapeHtml(bn) +
      '\u2019s physical and F&amp;B expectations for Premium?</li>' +
      '<li>Will IHG retail + loyalty materially change channel mix vs. staying independent?</li>' +
      '<li>Is the operator ready for IHG systems, QA, and reporting cadence?</li>' +
      '<li>Is the owner optimizing for conversion economics and speed—not bespoke luxury craft?</li>' +
      '<li>How does ' +
      escapeHtml(bn) +
      ' compare to sibling IHG options (e.g., Hotel Indigo) on design and guest promise?</li>' +
      '</ul>';
    var similar =
      '<div class="scenario-card"><strong>Hotel Indigo</strong> (IHG · Design-Led Premium)</div>' +
      '<div class="scenario-card"><strong>Hyatt Centric</strong> (Upscale Lifestyle)</div>' +
      '<div class="scenario-card"><strong>DoubleTree by Hilton</strong> (Full-Service Conversion Mainstream)</div>';
    return wrapOe(
      '<section class="oe-section">' +
        '<h2 class="oe-section-title">Dealality Summary</h2>' +
        '<p class="oe-section-hint">Dealality Editorial Summary</p>' +
        explorerDetailCard('Summary', brand.brandProfileAnalysis) +
        '</section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Key Strengths &amp; Strategic Fit</h2>' +
        '<p class="oe-section-hint">Brand-to-Owner Framing — Where Affiliation Tends to Show the Most Upside (Illustrative)</p>' +
        '<div class="scenario-card-grid scenario-card-grid--owner-value" style="grid-template-columns:repeat(2,1fr)">' +
        strengthGrid +
        '</div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Caution Areas &amp; Tradeoffs</h2>' +
        '<p class="oe-section-hint">How Brands Surface Limits and Risk — Typical Diligence Talking Points (Illustrative)</p>' +
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
    var caseStudyPayloadSink = [];
    var atelierMap = {
      'atelier-overview': renderAtelierOverview(brand),
      'atelier-value-owners': renderValueToOwners(brand),
      'atelier-ops': renderOperationsStandards(brand),
      'atelier-commercial': renderCommercialEngine(brand),
      'atelier-loyalty': renderLoyaltyProgram(brand),
      'atelier-footprint': renderFootprintGrowth(brand),
      'atelier-materials': renderBrandMaterials(brand, caseStudyPayloadSink),
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
    return { html: htmlStr, caseStudyPayloads: caseStudyPayloadSink };
  }

  function buildAtelierTabsHtml() {
    var G = window.BrandExplorerGoldDetail;
    var rows = combinedTabRowDefs();
    return rows
      .map(function (t, i) {
        var isAtelier = i < ATELIER_TAB_DEFS.length;
        var icon = isAtelier ? TAB_ICONS[i] : G && G.TAB_ICONS ? G.TAB_ICONS[t.goldKey] : '';
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

  function mountAtelierIntoRoot(rootEl, brand) {
    if (!rootEl || !brand) return;
    var nav = rootEl.querySelector('[data-be-atelier-nav]');
    var panelsWrap = rootEl.querySelector('[data-be-atelier-panels]');
    if (!nav || !panelsWrap) return;
    nav.innerHTML = buildAtelierTabsHtml();
    var built = buildAtelierPanelsHtml(brand);
    panelsWrap.innerHTML = built.html;
    panelsWrap._beCaseStudyPayloads = built.caseStudyPayloads;
    wireAtelierTabs(rootEl);
  }

  function mountAtelierFromBrand(brand) {
    var root = document.getElementById('beAtelierRoot');
    mountAtelierIntoRoot(root, brand);
  }

  function onDetailLoaded(ev) {
    var brand = ev.detail && ev.detail.brand;
    if (!brand) return;
    mountAtelierFromBrand(brand);
  }

  wireBrandCaseStudyModalOnce();

  window.addEventListener('brand-explorer-detail-loaded', onDetailLoaded);

  window.BrandExplorerAtelierFromApi = {
    mountIntoRoot: mountAtelierIntoRoot,
    mountFromBrand: mountAtelierFromBrand
  };
})();
