/**
 * Brand Explorer Comparison Studio — Phase 2H paired-subsection document.
 * ONE page-level scroll. Each subsection renders actual Brand Explorer OE components
 * (left existing | right reconstructed). Next pair begins below the taller column.
 */
(function () {
  'use strict';

  // Prefer shared staged-candidate registry (same source as product PROPOSED UPDATE).
  var BRANDS =
    (globalThis.BrandExplorerStagedCandidateRegistry &&
      typeof globalThis.BrandExplorerStagedCandidateRegistry.list === 'function' &&
      globalThis.BrandExplorerStagedCandidateRegistry.list()) ||
    [
      {
        id: 'receQkxgjlezsc1xg',
        slug: 'curio-collection',
        name: 'Curio Collection by Hilton',
        overlayUrl: '/brand-explorer-phase2e-preview/curio-collection-phase2h-candidate.json',
        scorecardUrl: '/brand-explorer-phase2e-preview/curio-collection-phase2h-scorecard.json',
        baselineUrl: '/brand-explorer-phase2e-preview/curio-frozen-baseline-snapshot.json',
        manifestUrl: '/brand-explorer-phase2e-preview/curio-collection-staged-candidate-manifest-v1.json',
        candidateVersion: 'phase2h-component-parity'
        // No silent 2G fallback — older candidates must never replace latest staged without explicit founder intent
      },
      {
        id: 'reclkgOzvAcBheUSo',
        slug: 'ascend',
        name: 'Ascend Hotel Collection',
        overlayUrl: '/brand-explorer-phase2e-preview/ascend-phase2h-candidate.json',
        scorecardUrl: '/brand-explorer-phase2e-preview/ascend-phase2h-scorecard.json',
        baselineUrl: null,
        manifestUrl: '/brand-explorer-phase2e-preview/ascend-staged-candidate-manifest-v1.json',
        candidateVersion: 'phase2h-component-parity'
      }
    ];

  var REGISTRY_INDEX_URL = '/brand-explorer-phase2e-preview/founder-requirements-studio-index-v1.json';

  var FOUNDER_DISPOSITIONS = [
    { id: 'improved', label: 'Improved' },
    { id: 'unchanged', label: 'Unchanged' },
    { id: 'incomplete', label: 'Incomplete' },
    { id: 'requires_research', label: 'Requires Research' },
    { id: 'rejected', label: 'Rejected' },
    { id: 'approved', label: 'Approved' }
  ];

  var GATE1_PAYLOAD_URL = '/brand-explorer-phase2e-preview/curio-gate1-renderer-parity-payload.json';
  /** Gate 1 must cover all ten atelier tabs (same surface as product mount). */
  var GATE1_TEST_TABS = [
    { tabId: 'atelier-overview', tabName: 'Overview', focus: 'Brand Snapshot / Positioning / Value scenarios' },
    { tabId: 'atelier-value-owners', tabName: 'Value', focus: 'Owner Buying / Scenarios / Lifecycle' },
    { tabId: 'atelier-ops', tabName: 'Ops', focus: 'Operating Model / Flexibility / Operator Fit' },
    { tabId: 'atelier-standards-owner', tabName: 'Standards', focus: 'Standards Table / Conversion / Confirm' },
    { tabId: 'atelier-commercial', tabName: 'Commercial', focus: 'Commercial Strengths / Demand Scenarios' },
    { tabId: 'atelier-economics', tabName: 'Economics', focus: 'Fees / Opening Path / Term & Exit Risk' },
    { tabId: 'atelier-loyalty', tabName: 'Loyalty', focus: 'Benefits / Tiers / Owner Implications' },
    { tabId: 'atelier-footprint', tabName: 'Footprint', focus: 'Geography / Momentum / Growth' },
    { tabId: 'atelier-materials', tabName: 'Materials', focus: 'Materials / Image Gallery' },
    { tabId: 'atelier-insight', tabName: 'Insight', focus: 'Summary / Similar Brands' }
  ];

  var state = {
    brand: BRANDS[0],
    activeTab: 'atelier-overview',
    viewMode: 'full',
    filter: 'all',
    qualityFilter: 'all',
    contract: null,
    diffs: [],
    selectedSlot: null,
    leftSource: 'live',
    leftMeta: null,
    rightMeta: null,
    leftBlocks: [],
    rightBlocks: [],
    leftBrand: null,
    rightBrand: null,
    scorecard: null,
    manifest: null,
    registry: null,
    reviews: {},
    brandDecision: null,
    lastPublishResult: null,
    rendererParity: false,
    payloadHash: null,
    equalWidth: false,
    loadedAt: null,
    assembly: null,
    incompleteDrafts: [],
    provenanceBySlot: {},
    highlightChanges: true,
    highlightStats: null,
    issuesFilter: 'all',
    // Monotonic brand-load generation — ignore stale async completions after switch.
    loadGeneration: 0
  };

  function $(sel, root) {
    return (root || document).querySelector(sel);
  }

  function nz(s) {
    return s == null ? '' : String(s).trim();
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function storageKey(brand) {
    return 'becs-review:' + brand.id + ':' + brand.candidateVersion;
  }

  function loadReviews() {
    try {
      state.reviews = JSON.parse(localStorage.getItem(storageKey(state.brand)) || '{}') || {};
    } catch (e) {
      state.reviews = {};
    }
  }

  function saveReviewsLocal() {
    localStorage.setItem(storageKey(state.brand), JSON.stringify(state.reviews));
  }

  async function fetchJson(url) {
    var res = await fetch(url, { credentials: 'same-origin' });
    if (!res.ok) throw new Error(url + ' → HTTP ' + res.status);
    return res.json();
  }

  function blocksFromBrand(brand) {
    var be = brand && brand.brandExplorer;
    var blocks = (be && be.blocks) || [];
    return Array.isArray(blocks) ? blocks : [];
  }

  function baselineToBlocks(snapshot) {
    var rows =
      (snapshot && snapshot.presentation && snapshot.presentation.blocks) ||
      (snapshot && snapshot.presentation && snapshot.presentation.rows) ||
      [];
    return rows
      .map(function (r, i) {
        return {
          slotKey: nz(r.slotKey || (r.fields && r.fields['Slot Key'])),
          title: nz(r.title || (r.fields && r.fields.Title)),
          body: nz(r.body || (r.fields && r.fields.Body)),
          imageUrl: r.imageUrl || null,
          sort: typeof r.sort === 'number' ? r.sort : i + 1
        };
      })
      .filter(function (b) {
        return b.slotKey;
      });
  }

  function listSubsections(tabId) {
    if (!state.contract) return [];
    var out = [];
    (state.contract.tabs || []).forEach(function (tab) {
      if (tabId && tab.tabId !== tabId) return;
      (tab.subsections || []).forEach(function (sub) {
        out.push(Object.assign({ tabId: tab.tabId, tabName: tab.tabName }, sub));
      });
    });
    return out;
  }

  function slotMatches(slotKey, patterns) {
    return (patterns || []).some(function (p) {
      return new RegExp(p).test(slotKey);
    });
  }

  function itemsForSubsection(blocks, sub) {
    return (blocks || []).filter(function (b) {
      return slotMatches(nz(b.slotKey), sub.slotPatterns);
    });
  }

  function matchItems(leftItems, rightItems) {
    var used = {};
    var pairs = [];
    leftItems.forEach(function (L) {
      var best = -1;
      var bestScore = 0;
      rightItems.forEach(function (R, i) {
        if (used[i]) return;
        var score = 0;
        if (nz(L.slotKey) && nz(L.slotKey) === nz(R.slotKey)) score += 2;
        if (nz(L.title) && nz(L.title).toLowerCase() === nz(R.title).toLowerCase()) score += 4;
        else if (nz(L.title) && nz(R.title) && nz(L.title).toLowerCase().indexOf(nz(R.title).toLowerCase()) >= 0) score += 2;
        if (nz(L.body).slice(0, 40) && nz(L.body).slice(0, 40) === nz(R.body).slice(0, 40)) score += 2;
        if (score > bestScore) {
          bestScore = score;
          best = i;
        }
      });
      if (best >= 0 && bestScore > 0) {
        used[best] = true;
        pairs.push({ left: L, right: rightItems[best], status: 'matched' });
      } else {
        pairs.push({ left: L, right: null, status: 'missing_right' });
      }
    });
    rightItems.forEach(function (R, i) {
      if (!used[i]) pairs.push({ left: null, right: R, status: 'missing_left' });
    });
    return pairs;
  }

  function itemSig(it) {
    if (!it) return '';
    return [nz(it.title), nz(it.body), nz(it.imageUrl)].join('\u0001');
  }

  function classifyPair(pair) {
    if (!pair.left && pair.right) return 'added';
    if (pair.left && !pair.right) return 'removed';
    if (itemSig(pair.left) !== itemSig(pair.right)) return 'modified';
    return 'unchanged';
  }

  function qualityIssues(item, slotKey) {
    var issues = [];
    if (!item) return ['missing_item'];
    var title = nz(item.title);
    var body = nz(item.body);
    if (title.split(/\s+/).length > 12 || title.length > 80) issues.push('title_paragraph');
    if (/Proof\s*\d+/i.test(title)) issues.push('generic_proof');
    if (!item.imageUrl && /scenario|gallery/i.test(slotKey || '')) issues.push('missing_image');
    if (/Focus this card|RESEARCH_REQUIRED|Evidence-grounded|slotKey|phase2/i.test(title + body)) issues.push('internal_meta');
    if (body.split(/\s+/).filter(Boolean).length > 140) issues.push('excessive_text');
    if (body.split(/\s+/).filter(Boolean).length > 0 && body.split(/\s+/).filter(Boolean).length < 12) issues.push('insufficient_content');
    if (!body && !title) issues.push('empty_item');
    return issues;
  }

  function buildDiffs() {
    var diffs = [];
    listSubsections().forEach(function (sub) {
      var L = itemsForSubsection(state.leftBlocks, sub);
      var R = itemsForSubsection(state.rightBlocks, sub);
      var pairs = matchItems(L, R);
      pairs.forEach(function (p, idx) {
        var change = classifyPair(p);
        var slotKey = nz((p.right || p.left || {}).slotKey) || sub.id + '#' + idx;
        var issues = qualityIssues(p.right, slotKey).concat(
          change !== 'unchanged' && change !== 'removed' ? [] : []
        );
        diffs.push({
          slotKey: slotKey,
          subsectionId: sub.id,
          tabId: sub.tabId,
          change: change,
          needsReview: change !== 'unchanged',
          conflict: false,
          left: p.left ? [p.left] : [],
          right: p.right ? [p.right] : [],
          leftBody: p.left ? nz(p.left.body) : '',
          rightBody: p.right ? nz(p.right.body) : '',
          qualityIssues: issues,
          status: p.status
        });
      });
    });
    return diffs;
  }

  function summarize(diffs) {
    var s = { totalChanged: 0, added: 0, removed: 0, modified: 0, unchanged: 0, conflicts: 0, needsReview: 0 };
    diffs.forEach(function (d) {
      if (d.change === 'unchanged') s.unchanged++;
      else {
        s.totalChanged++;
        s[d.change] = (s[d.change] || 0) + 1;
      }
      if (d.needsReview) s.needsReview++;
    });
    return s;
  }

  function reviewStats() {
    var keys = Object.keys(state.reviews);
    var counts = {
      reviewed: keys.length,
      approve_new: 0,
      keep_existing: 0,
      needs_correction: 0,
      needs_research: 0,
      unresolved: 0
    };
    keys.forEach(function (k) {
      var d = state.reviews[k] && state.reviews[k].decision;
      if (d && counts[d] != null) counts[d]++;
    });
    counts.unresolved = state.diffs.filter(function (d) {
      return d.needsReview && !state.reviews[d.slotKey];
    }).length;
    return counts;
  }

  function renderCard(item, side, status) {
    if (!item) {
      return (
        '<div class="becs-card becs-card-empty becs-' +
        (status === 'missing_left' ? 'missing-left' : 'missing-right') +
        '"><div class="becs-empty-label">' +
        (status === 'missing_left' ? 'Missing in existing' : 'Missing in reconstructed / New') +
        '</div></div>'
      );
    }
    var img = nz(item.imageUrl)
      ? '<div class="becs-card-media"><img src="' + escapeHtml(item.imageUrl) + '" alt="" loading="lazy" /></div>'
      : '';
    return (
      '<article class="becs-card becs-card-' +
      side +
      '" data-becs-slot="' +
      escapeHtml(nz(item.slotKey)) +
      '">' +
      img +
      (nz(item.title) ? '<h4 class="becs-card-title">' + escapeHtml(item.title) + '</h4>' : '') +
      (nz(item.body) ? '<div class="becs-card-body">' + escapeHtml(item.body).replace(/\n/g, '<br/>') + '</div>' : '') +
      '<div class="becs-card-slot"><code>' +
      escapeHtml(nz(item.slotKey)) +
      '</code></div></article>'
    );
  }

  function filteredSubsections() {
    var subs = listSubsections(state.activeTab);
    if (state.viewMode !== 'changes') return subs;
    return subs.filter(function (sub) {
      return state.diffs.some(function (d) {
        return d.subsectionId === sub.id && d.change !== 'unchanged';
      });
    });
  }

  function pairPassesFilters(pair, sub) {
    if (state.viewMode === 'changes' && classifyPair(pair) === 'unchanged') return false;
    if (state.qualityFilter && state.qualityFilter !== 'all') {
      var slot = nz((pair.right || pair.left || {}).slotKey);
      var issues = qualityIssues(pair.right, slot);
      if (state.qualityFilter === 'content_diff') return classifyPair(pair) !== 'unchanged';
      if (state.qualityFilter === 'styling') return false; // geometry flagged separately when empty/missing image
      if (state.qualityFilter === 'missing_image') return issues.indexOf('missing_image') >= 0 || issues.indexOf('missing_item') >= 0;
      if (state.qualityFilter === 'quality_failures') return issues.length > 0;
      return issues.indexOf(state.qualityFilter) >= 0;
    }
    return true;
  }

  function atelierApi() {
    return window.BrandExplorerAtelierFromApi || null;
  }

  function renderProductColumn(brand, tabId, sub) {
    var api = atelierApi();
    if (!api || !brand || !api.renderTabInnerHtml || !api.extractSubsectionHtml) {
      return '<div class="becs-card becs-card-empty"><div class="becs-empty-label">Product renderer unavailable</div></div>';
    }
    try {
      var tabHtml = api.renderTabInnerHtml(brand, tabId || sub.tabId);
      var frag = api.extractSubsectionHtml(tabHtml, sub);
      if (!nz(frag)) {
        return '<div class="becs-card becs-card-empty"><div class="becs-empty-label">No product subsection content</div></div>';
      }
      return '<div class="be-atelier-oe becs-product-mount">' + frag + '</div>';
    } catch (err) {
      return (
        '<div class="becs-card becs-card-empty"><div class="becs-empty-label">Render error: ' +
        escapeHtml(err.message || String(err)) +
        '</div></div>'
      );
    }
  }

  function renderItemPairs(sub) {
    var L = itemsForSubsection(state.leftBlocks, sub);
    var R = itemsForSubsection(state.rightBlocks, sub);
    var pairs = matchItems(L, R).filter(function (p) {
      return pairPassesFilters(p, sub);
    });
    return {
      pairs: pairs,
      leftHtml: pairs.length
        ? pairs
            .map(function (p) {
              return renderCard(p.left, 'left', p.status);
            })
            .join('')
        : '<div class="becs-card becs-card-empty"><div class="becs-empty-label">No items</div></div>',
      rightHtml: pairs.length
        ? pairs
            .map(function (p) {
              return renderCard(p.right, 'right', p.status);
            })
            .join('')
        : '<div class="becs-card becs-card-empty"><div class="becs-empty-label">No items</div></div>',
      changeCount: pairs.filter(function (p) {
        return classifyPair(p) !== 'unchanged';
      }).length
    };
  }

  function normalizeVisibleText(s) {
    return nz(s).replace(/\s+/g, ' ');
  }

  /**
   * LOSSLESS split of canonical atelier tab HTML into subsection pairs.
   *
   * Walks product root children in order so non-section chrome is never dropped
   * (e.g. .be-standards-meta "Last Reviewed", banners, interstitial hints).
   * - Sections under .be-standards-secondary-wrap are included.
   * - Untitled oe-sections fold into the next titled section (Economics intro).
   * - If reconstruction would lose source text, falls back to one full-tab mount.
   */
  function splitOeSections(tabHtml) {
    var wrap = document.createElement('div');
    wrap.innerHTML = tabHtml || '';
    var root = wrap.querySelector('.be-atelier-oe') || wrap;
    var pendingPrefix = '';
    var out = [];
    var seenTitles = {};

    function pushTitledSection(el) {
      var titleEl =
        el.querySelector(':scope > .oe-section-title') ||
        el.querySelector('.oe-section-title');
      var title = nz(titleEl && titleEl.textContent);
      if (!title) {
        pendingPrefix += el.outerHTML;
        return;
      }
      if (seenTitles[title]) {
        // Preserve duplicate-titled markup — never silently omit
        pendingPrefix += el.outerHTML;
        return;
      }
      seenTitles[title] = true;
      out.push({
        title: title,
        html: pendingPrefix + el.outerHTML,
        index: out.length
      });
      pendingPrefix = '';
    }

    function walk(parent) {
      var kids = parent.children ? Array.prototype.slice.call(parent.children) : [];
      for (var i = 0; i < kids.length; i++) {
        var el = kids[i];
        if (el.classList && el.classList.contains('oe-section')) {
          pushTitledSection(el);
        } else if (el.classList && el.classList.contains('be-standards-secondary-wrap')) {
          walk(el);
        } else {
          // Non-section product content (meta, placeholders, interstitial chrome)
          pendingPrefix += el.outerHTML;
        }
      }
    }

    walk(root);

    if (pendingPrefix) {
      if (out.length) {
        out[out.length - 1].html = out[out.length - 1].html + pendingPrefix;
      } else if (nz(tabHtml)) {
        out.push({ title: 'Full tab', html: pendingPrefix, index: 0 });
      }
    }
    if (!out.length && nz(tabHtml)) {
      out.push({ title: 'Full tab', html: root.innerHTML || tabHtml, index: 0 });
    }

    // Content integrity: reconstructed mounts must preserve source visible text
    var srcText = normalizeVisibleText(root.innerText || wrap.innerText || '');
    if (srcText) {
      var joined = document.createElement('div');
      joined.innerHTML = out
        .map(function (s) {
          return s.html;
        })
        .join('');
      var joinedText = normalizeVisibleText(joined.innerText || '');
      if (joinedText.length < Math.floor(srcText.length * 0.98)) {
        return [
          {
            title: 'Full tab (lossless product mount)',
            html: root.innerHTML || tabHtml,
            index: 0,
            losslessFallback: true,
            integrity: {
              sourceTextLen: srcText.length,
              joinedTextLen: joinedText.length
            }
          }
        ];
      }
    }
    return out;
  }

  function mountHtml(frag) {
    return '<div class="be-atelier-oe becs-product-mount" data-gate1-mount="1">' + frag + '</div>';
  }

  function extractOeInnerHtml(tabHtml) {
    var w = document.createElement('div');
    w.innerHTML = tabHtml || '';
    var r = w.querySelector('.be-atelier-oe');
    return r ? r.innerHTML : tabHtml || '';
  }

  /**
   * Annotate oe-sections with stable ids for subnav without removing any markup.
   */
  function annotateSectionsForSubnav(innerHtml, tabId) {
    var w = document.createElement('div');
    w.innerHTML = '<div class="be-atelier-oe">' + (innerHtml || '') + '</div>';
    var root = w.firstChild;
    var titles = [];
    var idx = 0;
    function walk(parent) {
      var kids = parent.children ? Array.prototype.slice.call(parent.children) : [];
      for (var i = 0; i < kids.length; i++) {
        var el = kids[i];
        if (el.classList && el.classList.contains('oe-section')) {
          var titleEl =
            el.querySelector(':scope > .oe-section-title') ||
            el.querySelector('.oe-section-title');
          var title = nz(titleEl && titleEl.textContent) || 'Section ' + (idx + 1);
          // Skip inventing Section N for untitled — use descriptive fallback only for anchor
          if (!nz(titleEl && titleEl.textContent)) {
            title = 'Preserved block ' + (idx + 1);
          }
          var id = tabId + '::' + idx;
          el.id = 'becs-anchor-' + id.replace(/[^a-zA-Z0-9_-]/g, '_');
          el.setAttribute('data-gate1-section', title);
          titles.push({ title: title, id: id, anchorId: el.id, index: idx });
          idx += 1;
        } else if (el.classList && el.classList.contains('be-standards-secondary-wrap')) {
          walk(el);
        }
      }
    }
    walk(root);
    return { html: root.innerHTML, sections: titles };
  }

  /** Gate 1: identical saved Curio payload — FULL tab product mount (no content loss from splitting). */
  function renderRendererParityDoc() {
    var root = $('#becsPairedDoc');
    if (!root) return;
    var api = atelierApi();
    if (!api || !api.renderTabInnerHtml) {
      root.innerHTML = '<div class="becs-unavailable">Product renderer unavailable</div>';
      return;
    }
    var brand = state.leftBrand;
    var tab = GATE1_TEST_TABS.find(function (t) {
      return t.tabId === state.activeTab;
    });
    if (!tab) {
      root.innerHTML =
        '<p class="becs-muted">Unknown Gate 1 tab: ' + escapeHtml(state.activeTab) + '</p>';
      return;
    }
    var tabHtml = api.renderTabInnerHtml(brand, tab.tabId);
    var inner = extractOeInnerHtml(tabHtml);
    var annotated = annotateSectionsForSubnav(inner, tab.tabId);
    var col = mountHtml(annotated.html);
    // Single paired full-tab mount — identical markup both columns, 100% product content
    root.innerHTML =
      '<section class="becs-pair becs-pair-gate1 becs-pair-fulltab" id="becs-sub-' +
      escapeHtml(tab.tabId + '::full') +
      '" data-subsection-id="' +
      escapeHtml(tab.tabId + '::full') +
      '" data-tab-id="' +
      escapeHtml(tab.tabId) +
      '" data-gate1-section="Full tab (product-native)">' +
      '<header class="becs-pair-head">' +
      '<div class="becs-pair-label">' +
      '<span class="becs-tab-chip">' +
      escapeHtml(tab.tabName) +
      '</span> Full tab (product-native)' +
      ' <span class="becs-pill same">identical payload</span>' +
      ' <span class="becs-pill same">canonical atelier HTML</span>' +
      ' <span class="becs-pill same">lossless full-tab</span>' +
      '</div></header>' +
      '<div class="becs-pair-cols' +
      (state.equalWidth ? ' becs-pair-cols-equalwidth' : '') +
      '">' +
      '<div class="becs-col becs-col-left becs-col-product" data-side="left" data-gate1-side="A-compare-left">' +
      col +
      '</div>' +
      '<div class="becs-col becs-col-right becs-col-product" data-side="right" data-gate1-side="B-compare-right">' +
      col +
      '</div></div></section>';

    // Subnav targets: section anchors inside the full mount (not separate pairs)
    state.gate1SectionNav = annotated.sections;

    var leftMount = root.querySelector('[data-gate1-side="A-compare-left"] .becs-product-mount');
    var rightMount = root.querySelector('[data-gate1-side="B-compare-right"] .becs-product-mount');
    var identicalColumns = !!(leftMount && rightMount && leftMount.innerHTML === rightMount.innerHTML);
    // Integrity: left === right HTML, and mount HTML equals annotated renderer output
    var htmlPass = !!(leftMount && leftMount.innerHTML === annotated.html);
    var mountedNorm = normalizeVisibleText((leftMount && leftMount.innerText) || '');
    var contentPass = identicalColumns && htmlPass && mountedNorm.length > 0;
    window.__BE_GATE1_COMPARE__ = {
      env: 'B-compare-studio',
      gate: 1,
      brandId: brand && brand.id,
      hash: state.payloadHash,
      blockCount: (state.leftBlocks && state.leftBlocks.length) || 0,
      activeTab: tab.tabId,
      sectionCount: annotated.sections.length,
      identicalLeftRightHtml: identicalColumns,
      renderer: 'BrandExplorerAtelierFromApi.renderTabInnerHtml',
      contentMutation: false,
      equalWidth: state.equalWidth,
      mountMode: 'lossless-full-tab',
      contentIntegrity: {
        pass: contentPass,
        sourceHtmlLen: annotated.html.length,
        mountedHtmlLen: leftMount ? leftMount.innerHTML.length : 0,
        mountedTextLen: mountedNorm.length,
        htmlExactMatch: htmlPass,
        losslessFullTab: true
      }
    };
  }

  /**
   * Default Comparison Studio pairing: actual product oe-sections for the active tab.
   * Aligns left (saved profile) vs right (staged candidate) by section title.
   * Untitled intros are already folded by splitOeSections (no "Section 1").
   */
  function productSectionPairsForActiveTab() {
    var api = atelierApi();
    if (!api || !api.renderTabInnerHtml || !state.leftBrand || !state.rightBrand) return null;
    var tabId = state.activeTab;
    var tabMeta = (state.contract && state.contract.tabs || []).find(function (t) {
      return t.tabId === tabId;
    });
    var tabName = (tabMeta && tabMeta.tabName) || tabId;
    var leftSecs = splitOeSections(api.renderTabInnerHtml(state.leftBrand, tabId));
    var rightSecs = splitOeSections(api.renderTabInnerHtml(state.rightBrand, tabId));
    var rightByTitle = {};
    rightSecs.forEach(function (s) {
      rightByTitle[s.title] = s;
    });
    var usedRight = {};
    var pairs = [];
    leftSecs.forEach(function (L, idx) {
      var R = rightByTitle[L.title] || rightSecs[idx] || null;
      if (R) usedRight[R.title] = true;
      pairs.push({
        id: tabId + '::' + idx + '::' + (L.title || 'section'),
        tabId: tabId,
        tabName: tabName,
        label: L.title,
        leftHtml: L.html,
        rightHtml: R ? R.html : '',
        index: idx
      });
    });
    rightSecs.forEach(function (R, idx) {
      if (usedRight[R.title]) return;
      pairs.push({
        id: tabId + '::r' + idx + '::' + (R.title || 'section'),
        tabId: tabId,
        tabName: tabName,
        label: R.title,
        leftHtml: '',
        rightHtml: R.html,
        index: pairs.length
      });
    });
    return pairs;
  }

  function renderPairedDoc() {
    if (state.rendererParity) {
      renderRendererParityDoc();
      return;
    }
    var root = $('#becsPairedDoc');
    if (!root) return;
    var html = [];
    var useProduct = state.viewMode !== 'items' && !!atelierApi();

    // Full / changes product mode: pair by actual atelier oe-sections (Standards 4, Economics no Section 1)
    if (useProduct && state.viewMode !== 'items') {
      var productPairs = productSectionPairsForActiveTab() || [];
      productPairs.forEach(function (pair) {
        var leftNorm = nz(pair.leftHtml);
        var rightNorm = nz(pair.rightHtml);
        var sectionChanged = leftNorm !== rightNorm;
        if (state.viewMode === 'changes' && !sectionChanged) return;
        html.push(
          '<section class="becs-pair' +
            (sectionChanged ? ' becs-pair-changed' : '') +
            '" id="becs-sub-' +
            escapeHtml(pair.id) +
            '" data-subsection-id="' +
            escapeHtml(pair.id) +
            '" data-tab-id="' +
            escapeHtml(pair.tabId) +
            '" data-product-section="' +
            escapeHtml(pair.label) +
            '">' +
            '<header class="becs-pair-head">' +
            '<div class="becs-pair-label">' +
            '<span class="becs-tab-chip">' +
            escapeHtml(pair.tabName) +
            '</span> ' +
            escapeHtml(pair.label) +
            (sectionChanged ? ' <span class="becs-pill mod">changed</span>' : '') +
            ' <span class="becs-pill same">product components</span>' +
            '</div></header>' +
            '<div class="becs-pair-cols">' +
            '<div class="becs-col becs-col-left becs-col-product" data-side="left">' +
            (leftNorm
              ? '<div class="be-atelier-oe becs-product-mount">' + pair.leftHtml + '</div>'
              : '<div class="becs-card becs-card-empty"><div class="becs-empty-label">' +
                (state.leftSource === 'empty-live'
                  ? 'Empty live Presentation (no CURRENT blocks yet)'
                  : 'Missing in existing') +
                '</div></div>') +
            '</div>' +
            '<div class="becs-col becs-col-right becs-col-product" data-side="right">' +
            (rightNorm
              ? '<div class="be-atelier-oe becs-product-mount">' + pair.rightHtml + '</div>'
              : '<div class="becs-card becs-card-empty"><div class="becs-empty-label">Missing in reconstructed / New</div></div>') +
            '</div></div></section>'
        );
      });
      root.innerHTML =
        html.join('') ||
        (state.leftSource === 'empty-live' && !(state.rightBlocks && state.rightBlocks.length)
          ? '<p class="becs-muted">Empty live Presentation and no staged Proposed blocks for this tab.</p>'
          : '<p class="becs-muted">No product sections for this tab.</p>');
      window.__BE_COMPARE_STUDIO__ = {
        env: 'comparison-studio',
        mode: 'product-section-pairs',
        brandId: state.brand && state.brand.id,
        activeTab: state.activeTab,
        sectionCount: productPairs.length,
        sectionTitles: productPairs.map(function (p) {
          return p.label;
        }),
        leftSource: state.leftSource,
        rightOverlayUrl: state.rightOverlayUrl || null,
        contentMutation: false,
        writes: 0
      };
      applyHighlightChanges();
      return;
    }

    filteredSubsections().forEach(function (sub) {
      var itemView = renderItemPairs(sub);
      if (state.viewMode === 'changes' && !itemView.changeCount && !itemView.pairs.length) return;
      if (state.viewMode === 'changes' && !itemView.changeCount) {
        var hasDiff = state.diffs.some(function (d) {
          return d.subsectionId === sub.id && d.change !== 'unchanged';
        });
        if (!hasDiff) return;
      }
      var changed = state.diffs.some(function (d) {
        return d.subsectionId === sub.id && d.change !== 'unchanged';
      });
      html.push(
        '<section class="becs-pair' +
          (changed ? ' becs-pair-changed' : '') +
          '" id="becs-sub-' +
          escapeHtml(sub.id) +
          '" data-subsection-id="' +
          escapeHtml(sub.id) +
          '" data-tab-id="' +
          escapeHtml(sub.tabId) +
          '">' +
          '<header class="becs-pair-head">' +
          '<div class="becs-pair-label">' +
          '<span class="becs-tab-chip">' +
          escapeHtml(sub.tabName) +
          '</span> ' +
          escapeHtml(sub.label) +
          (changed ? ' <span class="becs-pill mod">changed</span>' : '') +
          ' <span class="becs-pill">item view</span>' +
          '</div></header>' +
          '<div class="becs-pair-cols">' +
          '<div class="becs-col becs-col-left" data-side="left">' +
          itemView.leftHtml +
          '</div>' +
          '<div class="becs-col becs-col-right" data-side="right">' +
          itemView.rightHtml +
          '</div></div></section>'
      );
    });
    root.innerHTML = html.join('') || '<p class="becs-muted">No subsections match the current filters.</p>';
    applyHighlightChanges();
  }

  function applyHighlightChanges() {
    var api = globalThis.BeCompareHighlight;
    if (!api || state.rendererParity) {
      state.highlightStats = null;
      document.body.classList.remove('becs-hl-on', 'becs-hl-off');
      return;
    }
    var result = api.highlightPairedDoc($('#becsPairedDoc'), {
      incompleteDrafts: state.incompleteDrafts || []
    });
    state.highlightStats = result && result.totals ? result.totals : null;
    api.wireJumpHandlers(document);
    api.setHighlightEnabled(!!state.highlightChanges, { persist: true });
    var meta = $('#becsRightMeta');
    if (meta && state.highlightStats) {
      var base = meta.getAttribute('data-becs-base-meta') || meta.textContent || '';
      if (!meta.getAttribute('data-becs-base-meta')) {
        meta.setAttribute('data-becs-base-meta', base);
      }
      var t = state.highlightStats;
      meta.textContent =
        meta.getAttribute('data-becs-base-meta') +
        ' · HL +' +
        (t.added || 0) +
        ' ~' +
        (t.modified || 0) +
        ' −' +
        (t.removed || 0) +
        ' ?' +
        (t.incomplete || 0);
    }
  }

  function setHighlightChanges(on) {
    state.highlightChanges = !!on;
    var api = globalThis.BeCompareHighlight;
    if (api) api.setHighlightEnabled(state.highlightChanges, { persist: true });
    else {
      document.body.classList.toggle('becs-hl-on', state.highlightChanges);
      document.body.classList.toggle('becs-hl-off', !state.highlightChanges);
    }
    // Intentionally do NOT re-render — preserves content, geometry, and scroll
  }

  function renderSubnav() {
    var nav = $('#becsSubnav');
    if (!nav) return;
    if (state.rendererParity) {
      var sectionNav = state.gate1SectionNav || [];
      if (sectionNav.length) {
        nav.innerHTML = sectionNav
          .map(function (sec) {
            return (
              '<a href="#' +
              encodeURIComponent(sec.anchorId) +
              '" class="becs-subnav-link" data-becs-gate1-section="' +
              escapeHtml(sec.title) +
              '" data-becs-anchor="' +
              escapeHtml(sec.anchorId) +
              '">' +
              escapeHtml(sec.title) +
              '</a>'
            );
          })
          .join('');
      } else {
        var focus =
          (GATE1_TEST_TABS.find(function (t) {
            return t.tabId === state.activeTab;
          }) || {}).focus || state.activeTab;
        nav.innerHTML =
          '<div class="becs-muted" style="padding:8px 10px">' + escapeHtml(focus) + '</div>';
      }
      return;
    }
    var productPairs = document.querySelectorAll('#becsPairedDoc .becs-pair[data-product-section]');
    if (productPairs.length) {
      nav.innerHTML = Array.prototype.map
        .call(productPairs, function (pair) {
          var title = pair.getAttribute('data-product-section') || 'Section';
          var id = pair.id || '';
          return (
            '<a href="#' +
            encodeURIComponent(id) +
            '" class="becs-subnav-link" data-becs-sub="' +
            escapeHtml(id) +
            '">' +
            escapeHtml(title) +
            '</a>'
          );
        })
        .join('');
      return;
    }
    nav.innerHTML = listSubsections(state.activeTab)
      .map(function (sub) {
        var changed = state.diffs.some(function (d) {
          return d.subsectionId === sub.id && d.change !== 'unchanged';
        });
        return (
          '<a href="#becs-sub-' +
          encodeURIComponent(sub.id) +
          '" class="becs-subnav-link" data-becs-sub="' +
          escapeHtml(sub.id) +
          '">' +
          escapeHtml(sub.label) +
          (changed ? ' · Δ' : '') +
          '</a>'
        );
      })
      .join('');
  }

  function renderSummary() {
    var s = summarize(state.diffs.filter(function (d) {
      return d.tabId === state.activeTab;
    }));
    var all = summarize(state.diffs || []);
    var held = (state.incompleteDrafts || []).length;
    var blocking = (state.incompleteDrafts || []).filter(function (d) {
      return /regress|incomplete|research/i.test(d.reason || d.provenance || '');
    }).length;
    var compact = $('#becsReviewCompact') && $('#becsCompactSummary');
    if (compact) {
      $('#becsCompactSummary').innerHTML =
        '<strong>' +
        (all.totalChanged || 0) +
        ' Changed</strong> · <strong>' +
        (all.added || 0) +
        ' Added</strong> · <strong>' +
        (all.modified || 0) +
        ' Modified</strong> · <strong>' +
        (all.needsReview || 0) +
        ' Need Review</strong>' +
        (held
          ? ' · <button type="button" class="becs-linkish" id="becsCompactIssues">' +
            held +
            ' research/issue gaps</button>'
          : '') +
        (blocking
          ? ' · <span class="becs-pill del">' + blocking + ' held-out regressions</span>'
          : '');
      var cbtn = $('#becsCompactIssues');
      if (cbtn) {
        cbtn.addEventListener('click', function () {
          setIssuesDrawerOpen(true);
        });
      }
    }
    var el = $('#becsSummary');
    if (!el) return;
    var cards = [
      ['changed', 'Changed', s.totalChanged],
      ['added', 'Added', s.added],
      ['removed', 'Removed', s.removed],
      ['modified', 'Modified', s.modified],
      ['unchanged', 'Unchanged', s.unchanged],
      ['needsReview', 'Needs review', s.needsReview]
    ];
    el.innerHTML = cards
      .map(function (c) {
        return (
          '<div class="becs-summary-card' +
          (state.filter === c[0] ? ' active' : '') +
          '" data-becs-filter="' +
          c[0] +
          '"><div class="n">' +
          c[2] +
          '</div><div class="l">' +
          c[1] +
          '</div></div>'
        );
      })
      .join('');
  }

  function setIssuesDrawerOpen(on) {
    var drawer = $('#becsIssuesDrawer');
    var backdrop = $('#becsIssuesBackdrop');
    if (!drawer) return;
    drawer.hidden = !on;
    if (backdrop) backdrop.hidden = !on;
    document.body.classList.toggle('becs-issues-open', !!on);
    if (on) renderIssuesDrawer();
  }

  function renderIssuesDrawer() {
    var body = $('#becsIssuesBody');
    var filtersEl = $('#becsIssuesFilters');
    if (!body) return;
    var drafts = state.incompleteDrafts || [];
    var filter = state.issuesFilter || 'all';
    if (filtersEl) {
      var filters = [
        ['all', 'All'],
        ['blocking', 'Blocking publication'],
        ['research', 'Missing research'],
        ['incomplete', 'Incomplete components'],
        ['regression', 'Content regressions'],
        ['visual', 'Visual defects'],
        ['unverified', 'Unverified numerical claims']
      ];
      filtersEl.innerHTML = filters
        .map(function (f) {
          return (
            '<button type="button" class="becs-issue-filter' +
            (filter === f[0] ? ' active' : '') +
            '" data-becs-issue-filter="' +
            f[0] +
            '">' +
            f[1] +
            '</button>'
          );
        })
        .join('');
    }
    function matches(d) {
      var r = String(d.reason || '') + ' ' + String(d.provenance || '') + ' ' + String(d.slotKey || '');
      if (filter === 'all') return true;
      if (filter === 'blocking') return /regress|incomplete|research_required|prose_or_list/i.test(r);
      if (filter === 'research') return /research/i.test(r);
      if (filter === 'incomplete') return /incomplete/i.test(r);
      if (filter === 'regression') return /regress|prose_or_list|cardinality/i.test(r);
      if (filter === 'visual') return /visual|overflow|image/i.test(r);
      if (filter === 'unverified') return /kpi|numeric|unverified|mix|members/i.test(r);
      return true;
    }
    var rows = drafts.filter(matches);
    body.innerHTML =
      '<p class="becs-muted">All unresolved staged issues remain here. Hiding this panel does not clear blockers.</p>' +
      (rows.length
        ? rows
            .map(function (d) {
              return (
                '<button type="button" class="becs-issue-row" data-becs-issue-slot="' +
                escapeHtml(d.slotKey) +
                '"><code>' +
                escapeHtml(d.slotKey) +
                '</code> <span class="becs-pill del">' +
                escapeHtml(d.provenance || 'ISSUE') +
                '</span><span class="becs-muted">' +
                escapeHtml(d.reason || '') +
                '</span></button>'
              );
            })
            .join('')
        : '<p class="becs-muted">No issues in this filter.</p>');
    // Also mirror completeness inventory into drawer for full access
    var comp = $('#becsCompleteness');
    if (comp && !comp.dataset.mirrored) {
      /* completeness still rendered into hidden host; clone visible copy */
    }
    var host = document.createElement('div');
    host.className = 'becs-issues-completeness';
    host.innerHTML = ($('#becsCompleteness') && $('#becsCompleteness').innerHTML) || '';
    body.appendChild(host);
  }

  function renderScorecard() {
    var el = $('#becsScorecard');
    if (!el) return;
    var sc = state.scorecard && (state.scorecard.metrics || state.scorecard);
    var rs = reviewStats();
    var visual = (sc && sc.visual) || sc || {};
    var anti = (sc && sc.antiPadding) || {};
    if (state.rendererParity) {
      el.innerHTML =
        'GATE 1 renderer parity (not founder-approved): identical Curio payload · hash <code>' +
        escapeHtml((state.payloadHash || '—').slice(0, 24)) +
        '</code> · blocks <strong>' +
        ((state.leftBlocks && state.leftBlocks.length) || 0) +
        '</strong> · contentMutation=<strong>false</strong> · status <strong>NOT_FOUNDER_APPROVED</strong>';
      return;
    }
    el.innerHTML =
      'Phase 2H (not founder-approved): substantive priority blocked <strong>' +
      (sc && sc.priorityBlocked != null ? sc.priorityBlocked : visual.priorityBlocked != null ? visual.priorityBlocked : '—') +
      '</strong> · research gaps <strong>' +
      ((sc && sc.researchGapCount) || (anti && anti.researchGapCount) || 0) +
      '</strong> · status <strong>' +
      escapeHtml((sc && sc.founderApprovalStatus) || 'NOT_FOUNDER_APPROVED') +
      '</strong> · founder reviewed <strong>' +
      rs.reviewed +
      '</strong> · unresolved <strong>' +
      rs.unresolved +
      '</strong>';
  }

  function renderQualityFilters() {
    var el = $('#becsQualityFilters');
    if (!el) return;
    var filters = [
      ['all', 'All'],
      ['content_diff', 'Content differences'],
      ['missing_image', 'Missing imagery'],
      ['quality_failures', 'Quality failures'],
      ['generic_proof', 'Generic proof titles'],
      ['internal_meta', 'Internal meta language'],
      ['insufficient_content', 'Insufficient content'],
      ['excessive_text', 'Excessive text']
    ];
    el.innerHTML = filters
      .map(function (f) {
        return (
          '<button type="button" class="becs-qfilter' +
          (state.qualityFilter === f[0] ? ' active' : '') +
          '" data-becs-qfilter="' +
          f[0] +
          '">' +
          f[1] +
          '</button>'
        );
      })
      .join('');
  }

  function filteredDiffs() {
    return state.diffs.filter(function (d) {
      if (d.tabId !== state.activeTab) return false;
      if (state.filter === 'changed') return d.change !== 'unchanged';
      if (state.filter === 'added') return d.change === 'added';
      if (state.filter === 'removed') return d.change === 'removed';
      if (state.filter === 'modified') return d.change === 'modified';
      if (state.filter === 'unchanged') return d.change === 'unchanged';
      if (state.filter === 'needsReview') return d.needsReview;
      if (state.qualityFilter && state.qualityFilter !== 'all' && state.qualityFilter !== 'content_diff') {
        return (d.qualityIssues || []).indexOf(state.qualityFilter) >= 0 ||
          (state.qualityFilter === 'quality_failures' && (d.qualityIssues || []).length);
      }
      if (state.qualityFilter === 'content_diff') return d.change !== 'unchanged';
      return true;
    });
  }

  function renderChangeList() {
    var ul = $('#becsChangeList');
    if (!ul) return;
    ul.innerHTML = filteredDiffs()
      .slice(0, 400)
      .map(function (d) {
        var pill = d.change === 'added' ? 'add' : d.change === 'removed' ? 'del' : d.change === 'modified' ? 'mod' : 'same';
        return (
          '<li data-becs-slot-jump="' +
          escapeHtml(d.slotKey) +
          '" data-becs-sub="' +
          escapeHtml(d.subsectionId) +
          '" class="' +
          (state.selectedSlot === d.slotKey ? 'active' : '') +
          '"><span class="becs-pill ' +
          pill +
          '">' +
          d.change +
          '</span>' +
          escapeHtml(d.slotKey) +
          '</li>'
        );
      })
      .join('');
  }

  function jumpToSubsection(subId, slotKey) {
    var el = document.getElementById('becs-sub-' + subId);
    if (el) {
      el.scrollIntoView({ behavior: 'smooth', block: 'start' });
      el.classList.add('becs-pair-flash');
      setTimeout(function () {
        el.classList.remove('becs-pair-flash');
      }, 1200);
    }
    if (slotKey) {
      document.querySelectorAll('[data-becs-slot="' + slotKey + '"]').forEach(function (node) {
        node.classList.add('becs-field-flash');
        setTimeout(function () {
          node.classList.remove('becs-field-flash');
        }, 1600);
      });
    }
  }

  function selectSlot(slotKey, subId) {
    state.selectedSlot = slotKey;
    renderChangeList();
    var d = state.diffs.find(function (x) {
      return x.slotKey === slotKey;
    });
    renderReviewDrawer(d);
    jumpToSubsection(subId || (d && d.subsectionId), slotKey);
  }

  function renderReviewDrawer(d) {
    var box = $('#becsReviewDrawer');
    if (!box) return;
    if (!d) {
      box.innerHTML =
        '<h3>Subsection / field review</h3><p class="becs-review-meta">Select a changed field. Dispositions persist locally — never publish automatically.</p>';
      return;
    }
    var rev = state.reviews[d.slotKey] || {};
    var reqs = requirementsForSection(d.subsectionId || d.section || '').concat(
      requirementsForActiveTab().filter(function (r) {
        return r.tabId === d.tabId;
      })
    );
    var seen = {};
    var reqHtml = reqs
      .filter(function (r) {
        if (seen[r.requirementId]) return false;
        seen[r.requirementId] = true;
        return true;
      })
      .slice(0, 8)
      .map(function (r) {
        return (
          '<li><code>' +
          escapeHtml(r.requirementId) +
          '</code> · ' +
          escapeHtml(r.status) +
          (r.outstanding ? ' · outstanding' : '') +
          '</li>'
        );
      })
      .join('');
    box.innerHTML =
      '<h3>Review · <code>' +
      escapeHtml(d.slotKey) +
      '</code></h3>' +
      '<div class="becs-review-compare">' +
      '<div><strong>CURRENT</strong><p>' +
      escapeHtml(
        (function () {
          var it = Array.isArray(d.left) ? d.left[0] : d.left;
          return it ? nz(it.title) + (nz(it.body) ? ' — ' + nz(it.body) : '') : '—';
        })().slice(0, 280)
      ) +
      '</p></div>' +
      '<div><strong>PROPOSED</strong><p>' +
      escapeHtml(
        (function () {
          var it = Array.isArray(d.right) ? d.right[0] : d.right;
          return it ? nz(it.title) + (nz(it.body) ? ' — ' + nz(it.body) : '') : '—';
        })().slice(0, 280)
      ) +
      '</p></div></div>' +
      '<div class="becs-review-row">' +
      FOUNDER_DISPOSITIONS.map(function (x) {
        return (
          '<button type="button" data-becs-decision="' +
          x.id +
          '"' +
          (rev.decision === x.id ? ' class="active"' : '') +
          '>' +
          x.label +
          '</button>'
        );
      }).join('') +
      '<textarea id="becsComment" placeholder="Evidence / notes (optional)">' +
      escapeHtml(rev.comment || '') +
      '</textarea></div>' +
      '<div class="becs-review-meta">Change: <strong>' +
      escapeHtml(d.change) +
      '</strong> · Quality: <strong>' +
      escapeHtml((d.qualityIssues || []).join(', ') || 'none flagged') +
      '</strong> · Disposition: <strong>' +
      escapeHtml(rev.decision || 'unresolved') +
      '</strong></div>' +
      '<div class="becs-req-drawer"><strong>Related requirements</strong><ul>' +
      (reqHtml || '<li class="becs-muted">None mapped for this slot</li>') +
      '</ul></div>';
  }

  function recordDecision(decision) {
    if (!state.selectedSlot) return;
    var comment = ($('#becsComment') && $('#becsComment').value) || '';
    state.reviews[state.selectedSlot] = {
      brandId: state.brand.id,
      brandSlug: state.brand.slug,
      slotKey: state.selectedSlot,
      candidateVersion: state.brand.candidateVersion,
      decision: decision,
      comment: comment,
      updatedAt: new Date().toISOString()
    };
    saveReviewsLocal();
    fetch('/api/brand-explorer-compare/review', {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ brandId: state.brand.id, entry: state.reviews[state.selectedSlot], all: state.reviews })
    }).catch(function () {});
    renderScorecard();
    renderChangeList();
    renderReviewDrawer(state.diffs.find(function (x) {
      return x.slotKey === state.selectedSlot;
    }));
    setStatus('Saved decision for ' + state.selectedSlot);
  }

  function exportReport() {
    var report = {
      contract: 'BE_COMPARISON_STUDIO_FOUNDER_WORKSPACE_V1',
      exportedAt: new Date().toISOString(),
      canonicalUrl:
        (typeof location !== 'undefined' && location.origin
          ? location.origin
          : '') + '/brand-explorer-compare.html?beInternalPreview=1',
      brand: { id: state.brand.id, slug: state.brand.slug, name: state.brand.name },
      candidateVersion: state.brand.candidateVersion,
      leftMeta: state.leftMeta,
      rightMeta: state.rightMeta,
      leftSource: state.leftSource,
      rightOverlayUrl: state.rightOverlayUrl || null,
      manifest: state.manifest,
      registryTotals: (state.registry && state.registry.totals) || null,
      renderer: atelierApi() ? 'BrandExplorerAtelierFromApi' : 'flattened-fallback',
      summary: summarize(state.diffs),
      founderReview: reviewStats(),
      brandDecision: state.brandDecision,
      scorecard: state.scorecard,
      note: 'CURRENT saved vs PROPOSED staged. Automated metrics ≠ founder approval. No Airtable writes. Gate 2 blocked until Gate 1.',
      airtableWrites: 0,
      publicationApplied: false,
      reviews: state.reviews,
      diffs: state.diffs.map(function (d) {
        return {
          slotKey: d.slotKey,
          subsectionId: d.subsectionId,
          change: d.change,
          qualityIssues: d.qualityIssues,
          decision: (state.reviews[d.slotKey] && state.reviews[d.slotKey].decision) || null
        };
      })
    };
    var blob = new Blob([JSON.stringify(report, null, 2)], { type: 'application/json' });
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'be-comparison-paired-' + state.brand.slug + '-' + Date.now() + '.json';
    a.click();
    URL.revokeObjectURL(a.href);
  }

  function setStatus(msg) {
    var el = $('#becsStatus');
    if (el) el.textContent = msg;
  }

  function setActiveTab(tabId, opts) {
    opts = opts || {};
    state.activeTab = tabId;
    document.querySelectorAll('[data-becs-tab]').forEach(function (btn) {
      btn.classList.toggle('active', btn.getAttribute('data-becs-tab') === tabId);
    });
    renderSummary();
    renderChangeList();
    renderReqList();
    renderPairedDoc();
    // Subnav depends on rendered pairs — refresh after doc mount
    renderSubnav();
    var scroll = $('#becsDocScroll');
    if (scroll) scroll.scrollTop = 0;
    // Product CTA jumps (e.g. Overview "View Recent Openings" → Footprint & Growth)
    if (opts.focusSubsection) {
      window.requestAnimationFrame(function () {
        jumpToSubsection(opts.focusSubsection);
        if (opts.focusHeadingRe) scrollPairedDocToHeading(opts.focusHeadingRe);
      });
    } else if (opts.focusHeadingRe) {
      window.requestAnimationFrame(function () {
        scrollPairedDocToHeading(opts.focusHeadingRe);
      });
    }
  }

  /**
   * Honor product-renderer jump CTAs inside Compare mounts.
   * Atelier shell wires data-be-jump-atelier-tab via wireAtelierTabs; Compare
   * mounts tab fragments only — without this, Overview "View Recent Openings"
   * is a dead button.
   */
  function handleProductJumpCta(jumpEl) {
    if (!jumpEl || jumpEl.disabled) return false;
    var tabId = jumpEl.getAttribute('data-be-jump-atelier-tab');
    if (!tabId) return false;
    var focusSubsection = null;
    var focusHeadingRe = null;
    if (tabId === 'atelier-footprint') {
      // Footprint & Growth → Recent Momentum / Recent Openings cluster
      focusSubsection = 'footprint.momentum';
      focusHeadingRe = /Recent Momentum|Openings\s*\/\s*Examples|Recent Openings/i;
    }
    setActiveTab(tabId, {
      focusSubsection: focusSubsection,
      focusHeadingRe: focusHeadingRe
    });
    return true;
  }

  function scrollPairedDocToHeading(re) {
    if (!re) return;
    var root = $('#becsPairedDoc') || document;
    var headings = root.querySelectorAll('.oe-section-title, h2, h3');
    var hit = Array.prototype.find.call(headings, function (el) {
      return re.test(nz(el.textContent));
    });
    if (!hit) return;
    var pair = hit.closest('.becs-pair') || hit;
    pair.scrollIntoView({ behavior: 'smooth', block: 'start' });
    pair.classList.add('becs-pair-flash');
    setTimeout(function () {
      pair.classList.remove('becs-pair-flash');
    }, 1200);
  }

  function recordBrandDecision(decision) {
    state.brandDecision = {
      decision: decision,
      brandId: state.brand.id,
      brandSlug: state.brand.slug,
      candidateVersion: state.brand.candidateVersion,
      recordedAt: new Date().toISOString(),
      airtableWrites: 0,
      publicationApplied: false,
      note: 'Recorded in Comparison Studio only — separate authorized publication required to apply'
    };
    fetch('/api/brand-explorer-compare/review', {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        brandId: state.brand.id,
        all: state.reviews,
        brandDecision: state.brandDecision
      })
    }).catch(function () {});
    renderBrandApproval();
    setStatus('Brand decision recorded: ' + decision + ' (no Airtable write)');
  }

  function preparePublicationPackage() {
    var approved = Object.keys(state.reviews)
      .map(function (k) {
        return state.reviews[k];
      })
      .filter(function (r) {
        return r && (r.decision === 'approved' || r.decision === 'improved');
      });
    var rejected = Object.keys(state.reviews)
      .map(function (k) {
        return state.reviews[k];
      })
      .filter(function (r) {
        return r && r.decision === 'rejected';
      });
    var pkg = {
      contract: 'BE_PUBLICATION_PACKAGE_DRAFT_V1',
      status: 'DRAFT_NOT_APPLIED',
      brandId: state.brand.id,
      brandSlug: state.brand.slug,
      brandName: state.brand.name,
      candidateVersion: state.brand.candidateVersion,
      leftSource: state.leftSource,
      rightOverlayUrl: state.rightOverlayUrl,
      preparedAt: new Date().toISOString(),
      airtableWrites: 0,
      applied: false,
      brandDecision: state.brandDecision || null,
      approvedChanges: approved.map(function (r) {
        var d = state.diffs.find(function (x) {
          return x.slotKey === r.slotKey;
        });
        return {
          slotKey: r.slotKey,
          disposition: r.decision,
          before: d ? d.leftBody : null,
          after: d ? d.rightBody : null,
          intendedField: 'Brand Explorer Presentation · Slot Key ' + r.slotKey,
          provenance: state.rightOverlayUrl || null
        };
      }),
      rejectedChanges: rejected.map(function (r) {
        return { slotKey: r.slotKey, disposition: r.decision, comment: r.comment || null };
      }),
      outstandingRequirements: ((state.registry && state.registry.requirements) || [])
        .filter(function (r) {
          return r.outstanding;
        })
        .map(function (r) {
          return r.requirementId;
        }),
      migrationRequirements: [
        'Map approved slotKeys to Presentation rows in Airtable',
        'Validate select options and linked records before write',
        'Dry-run before apply'
      ],
      rollbackPlan: [
        'Retain CURRENT saved profile as authoritative until apply succeeds',
        'Keep staged candidate JSON immutable',
        'If apply fails, leave Airtable unchanged and restore from CURRENT snapshot'
      ],
      gate2Note: 'Content regeneration remains blocked until Gate 1 founder approval'
    };
    fetch('/api/brand-explorer-compare/review', {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        brandId: state.brand.id,
        all: state.reviews,
        brandDecision: state.brandDecision,
        publicationPackage: pkg
      })
    }).catch(function () {});
    var blob = new Blob([JSON.stringify(pkg, null, 2)], { type: 'application/json' });
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'be-publication-package-draft-' + state.brand.slug + '-' + Date.now() + '.json';
    a.click();
    URL.revokeObjectURL(a.href);
    setStatus(
      'Publication package DRAFT prepared (' +
        approved.length +
        ' approved slots) — NOT applied · airtableWrites=0'
    );
  }

  function brandFromBaseline(snap, cfg) {
    var id = (snap && snap.identity) || {};
    return {
      id: cfg.id,
      name: id.brandName || cfg.name,
      parentCompany: id.parentCompany || '',
      brandWebsite: id.website || '',
      brandExplorer: { version: 1, blocks: baselineToBlocks(snap) }
    };
  }

  function emptyLiveBrandShell(cfg, liveBrand) {
    return Object.assign({}, liveBrand || {}, {
      id: cfg.id,
      name: (liveBrand && (liveBrand.name || liveBrand.brandName)) || cfg.name,
      brandName: (liveBrand && (liveBrand.name || liveBrand.brandName)) || cfg.name,
      brandExplorer: { version: 1, blocks: [] },
      beEmptyLivePresentation: true
    });
  }

  async function loadLeftBrand(cfg) {
    var loadedAt = new Date().toISOString();
    try {
      var data = await fetchJson(
        '/api/brand-library/brand?brandId=' + encodeURIComponent(cfg.id) + '&beInternalPreview=1'
      );
      if (!data || !data.success || !data.brand) throw new Error('no brand');
      var blocks = blocksFromBrand(data.brand);
      // Draft / Under Review Factory brands often have empty live Presentation.
      // Do NOT abort the studio — keep an empty CURRENT shell so PROPOSED can still render.
      if (!blocks.length) {
        state.leftSource = 'empty-live';
        state.leftBrand = emptyLiveBrandShell(cfg, data.brand);
        state.leftMeta = {
          label: 'CURRENT · SAVED PROFILE (empty live Presentation)',
          source:
            'Live Airtable Presentation has no blocks yet (common for Draft/Under Review Factory brands). Proposed column shows the staged Factory candidate.',
          brandId: cfg.id,
          brandName: data.brand.name || data.brand.brandName || cfg.name,
          blockCount: 0,
          snapshotTimestamp:
            data.brand.activeProfileApprovedDate || data.brand.brandExplorerLastReviewed || null,
          loadedAt: loadedAt,
          readOnly: true,
          writeGuard: 'never_overwrite_during_review',
          emptyLivePresentation: true
        };
        return [];
      }
      state.leftSource = 'live';
      state.leftBrand = data.brand;
      state.leftMeta = {
        label: 'CURRENT · SAVED PROFILE',
        source: 'Canonical live Presentation (Airtable via brand-library API)',
        brandId: cfg.id,
        brandName: data.brand.name || data.brand.brandName || cfg.name,
        blockCount: blocks.length,
        snapshotTimestamp: data.brand.activeProfileApprovedDate || data.brand.brandExplorerLastReviewed || null,
        loadedAt: loadedAt,
        readOnly: true,
        writeGuard: 'never_overwrite_during_review'
      };
      return blocks;
    } catch (err) {
      if (cfg.baselineUrl) {
        var snap = await fetchJson(cfg.baselineUrl);
        state.leftSource = 'baseline';
        state.leftBrand = brandFromBaseline(snap, cfg);
        var bb = baselineToBlocks(snap);
        state.leftMeta = {
          label: 'CURRENT · SAVED PROFILE (verified snapshot fallback)',
          source: 'Labeled frozen baseline snapshot — live Presentation unavailable',
          brandId: cfg.id,
          brandName: cfg.name,
          blockCount: bb.length,
          snapshotTimestamp: (snap && (snap.capturedAt || snap.snapshotAt || snap.generatedAt)) || null,
          baselineUrl: cfg.baselineUrl,
          loadedAt: loadedAt,
          readOnly: true,
          writeGuard: 'never_overwrite_during_review',
          fallbackReason: err.message || String(err)
        };
        return bb;
      }
      // Last resort: empty CURRENT shell so PROPOSED frozen payload can still load
      // (includes NEW_* provisional brands awaiting Brand Basics create).
      state.leftSource = 'empty-live';
      state.leftBrand = emptyLiveBrandShell(cfg, null);
      state.leftMeta = {
        label: cfg.awaitingBrandBasicsCreate
          ? 'CURRENT · NO AIRTABLE BASICS YET'
          : 'CURRENT · SAVED PROFILE (unavailable)',
        source: cfg.awaitingBrandBasicsCreate
          ? 'Provisional NEW_* staging key — Brand Basics create not yet founder-authorized. RIGHT shows staged Factory candidate; LEFT has no saved Presentation.'
          : 'Live Presentation could not be loaded (' +
            (err.message || String(err)) +
            '). Proposed column still attempts staged Factory candidate.',
        brandId: cfg.id,
        brandName: cfg.name,
        blockCount: 0,
        loadedAt: loadedAt,
        readOnly: true,
        writeGuard: 'never_overwrite_during_review',
        emptyLivePresentation: true,
        awaitingBrandBasicsCreate: Boolean(cfg.awaitingBrandBasicsCreate),
        fallbackReason: err.message || String(err)
      };
      return [];
    }
  }

  function sha256HexSync(text) {
    // Prefer SubtleCrypto when available; otherwise FNV-1a fallback for display only.
    try {
      if (globalThis.crypto && crypto.subtle && typeof TextEncoder !== 'undefined') {
        return null; // async path used by callers when needed
      }
    } catch (e) {
      /* fall through */
    }
    var h = 2166136261;
    var s = String(text || '');
    for (var i = 0; i < s.length; i++) {
      h ^= s.charCodeAt(i);
      h = Math.imul(h, 16777619);
    }
    return ('00000000' + (h >>> 0).toString(16)).slice(-8);
  }

  async function hashBlocks(blocks) {
    var payload = JSON.stringify(blocks || []);
    try {
      if (globalThis.crypto && crypto.subtle && typeof TextEncoder !== 'undefined') {
        var buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(payload));
        return Array.from(new Uint8Array(buf))
          .map(function (b) {
            return b.toString(16).padStart(2, '0');
          })
          .join('');
      }
    } catch (e) {
      /* fallback */
    }
    return 'fnv1a:' + sha256HexSync(payload);
  }

  function assertOverlayMatchesBrand(cfg, overlay, usedUrl) {
    if (!overlay || typeof overlay !== 'object') {
      throw new Error('Staged overlay at ' + usedUrl + ' is empty or invalid');
    }
    var overlayId = nz(overlay.brandId || overlay.id || overlay.recordId);
    var overlaySlug = nz(overlay.slug || overlay.brandSlug).toLowerCase();
    var overlayName = nz(overlay.brandName || overlay.name);
    var cfgId = nz(cfg.id);
    var cfgSlug = nz(cfg.slug).toLowerCase();
    var idMismatch = overlayId && cfgId && overlayId !== cfgId;
    var slugMismatch = overlaySlug && cfgSlug && overlaySlug !== cfgSlug;
    if (idMismatch || slugMismatch) {
      throw new Error(
        'Staged overlay identity mismatch for ' +
          (cfg.name || cfgSlug || cfgId) +
          ': expected id/slug ' +
          cfgId +
          '/' +
          cfgSlug +
          ' but ' +
          usedUrl +
          ' has ' +
          overlayId +
          '/' +
          overlaySlug +
          (overlayName ? ' (' + overlayName + ')' : '') +
          '. Refusing to render cross-brand leftover content.'
      );
    }
  }

  async function loadRightBrand(cfg, leftBrand) {
    // Cumulative proposal: CURRENT baseline + only complete non-regressive staged changes.
    // Refuse silent older Phase 2F/2G fallback. Never replace complete content with weaker reconstruction.
    var usedUrl = cfg.overlayUrl;
    var overlay;
    try {
      overlay = await fetchJson(cfg.overlayUrl);
    } catch (e) {
      throw new Error(
        'Latest staged candidate unavailable at ' +
          cfg.overlayUrl +
          ' (' +
          (e.message || e) +
          '). Silent fallback to older Phase 2F/2G is forbidden.'
      );
    }
    assertOverlayMatchesBrand(cfg, overlay, usedUrl);
    if (cfg.manifestUrl) {
      try {
        state.manifest = await fetchJson(cfg.manifestUrl);
      } catch (me) {
        state.manifest = null;
      }
    } else {
      state.manifest = null;
    }

    var assembler = globalThis.BeCumulativeProposalAssembler;
    if (!assembler || !assembler.buildCumulativeProposedBrand) {
      throw new Error(
        'Cumulative proposal assembler unavailable — cannot safely build RIGHT column without overwriting CURRENT'
      );
    }
    if (!leftBrand) {
      throw new Error('CURRENT saved profile required before assembling cumulative PROPOSED profile');
    }

    var built = assembler.buildCumulativeProposedBrand(leftBrand, overlay, {
      candidateVersion: overlay.candidateVersion || cfg.candidateVersion
    });
    state.rightBrand = built.brand;
    // Never inherit empty-CURRENT marker onto PROPOSED (frozen/assembled may be rich)
    if (state.rightBrand) {
      delete state.rightBrand.beEmptyLivePresentation;
    }
    state.assembly = built.assembly;
    state.incompleteDrafts = (built.assembly && built.assembly.incompleteDrafts) || [];
    state.provenanceBySlot = (built.assembly && built.assembly.provenanceBySlot) || {};

    var blocks = blocksFromBrand(state.rightBrand);
    var contentHash = await hashBlocks(blocks);
    var rawCandidateBlocks = blocksFromBrand(overlay);
    var rawCandidateHash = await hashBlocks(rawCandidateBlocks);

    if (state.rightBrand) {
      state.rightBrand.beCandidateOverlayMeta = Object.assign(
        {},
        state.rightBrand.beCandidateOverlayMeta || {},
        {
          phase: overlay.phase || null,
          candidateVersion: overlay.candidateVersion || cfg.candidateVersion,
          note: 'Cumulative publication proposal (CURRENT + validated staged changes)',
          overlayUrl: usedUrl,
          assembly: assembler.BE_CUMULATIVE_PROPOSAL_ASSEMBLER_V1,
          contentHash: contentHash,
          rawCandidateHash: rawCandidateHash,
          isolatedFromLiveBaseline: false,
          retainsCurrentBaseline: true,
          olderFallbackUsed: false
        }
      );
    }
    state.rightOverlayUrl = usedUrl;
    state.rightMeta = {
      label: 'PROPOSED · CUMULATIVE PUBLICATION PROFILE',
      source: 'CURRENT baseline + complete staged changes from ' + usedUrl,
      stagedCandidateUrl: usedUrl,
      brandId: cfg.id,
      candidateVersion: overlay.candidateVersion || cfg.candidateVersion,
      assemblyContract: assembler.BE_CUMULATIVE_PROPOSAL_ASSEMBLER_V1,
      phaseLabel: overlay.phase || null,
      blockCount: blocks.length,
      contentHash: contentHash,
      rawCandidateBlockCount: rawCandidateBlocks.length,
      rawCandidateHash: rawCandidateHash,
      olderFallbackUsed: false,
      repairedAt: overlay.repairedAt || null,
      enrichedAt: overlay.enrichedAt || null,
      researchGapCount: Array.isArray(overlay.researchGaps)
        ? overlay.researchGaps.length
        : overlay.researchGaps || 0,
      incompleteDraftSlotCount: state.incompleteDrafts.length,
      provenanceCounts: (built.assembly && built.assembly.provenanceCounts) || null,
      approvalBlockedByRegression: !!(built.assembly && built.assembly.approvalBlockedByRegression),
      founderApprovalStatus: overlay.founderApprovalStatus || 'NOT_FOUNDER_APPROVED',
      gate2ContentRegen: 'BLOCKED_UNTIL_GATE1',
      writes: 0,
      note:
        'RIGHT is the cumulative replacement candidate. Incomplete factory reconstructions are listed separately and do not overwrite CURRENT.',
      completenessSummary: (state.manifest && state.manifest.completenessSummary) || null
    };
    return blocks;
  }

  async function loadGate1Payload() {
    var brand = await fetchJson(GATE1_PAYLOAD_URL);
    var blocks = blocksFromBrand(brand);
    if (!blocks.length) throw new Error('Gate 1 payload has empty Presentation blocks');
    state.leftBrand = brand;
    state.rightBrand = JSON.parse(JSON.stringify(brand));
    state.leftBlocks = blocks;
    state.rightBlocks = blocks.slice();
    state.leftSource = (brand.gate1 && brand.gate1.payloadSource) || 'gate1-payload';
    state.payloadHash = (brand.gate1 && brand.gate1.presentationBlocksHashSha256) || null;
    state.diffs = [];
    state.scorecard = {
      founderApprovalStatus: 'NOT_FOUNDER_APPROVED',
      gate: 1,
      note: 'Renderer parity only — identical saved Curio payload; no content regeneration',
      presentationBlocksHashSha256: state.payloadHash,
      researchGapCount: 0,
      priorityBlocked: 0
    };
    return brand;
  }

  function setStudioBadge(mode) {
    var badge = document.querySelector('.becs-badge');
    if (!badge) return;
    if (mode === 'gate1') {
      badge.textContent =
        'Internal · Gate 1 Renderer Parity (diagnostic) · Identical Curio payload · No content mutation';
    } else {
      badge.textContent =
        'Internal · Founder review · CURRENT saved vs CUMULATIVE PROPOSED · No Airtable writes';
    }
    var hlWrap = document.querySelector('.becs-hl-toggle');
    var hlLegend = $('#becsHighlightLegend');
    var hideHl = mode === 'gate1';
    if (hlWrap) hlWrap.hidden = hideHl;
    if (hlLegend) hlLegend.hidden = hideHl;
  }

  function requirementsForActiveTab() {
    if (!state.registry || !state.registry.requirements) return [];
    return state.registry.requirements.filter(function (r) {
      return !r.tabId || r.tabId === state.activeTab || r.tab === 'all' || r.tab == null;
    });
  }

  function requirementsForSection(sectionTitle) {
    var title = nz(sectionTitle).toLowerCase();
    return requirementsForActiveTab().filter(function (r) {
      if (!r.section) return r.tabId === state.activeTab;
      return nz(r.section).toLowerCase().indexOf(title) >= 0 || title.indexOf(nz(r.section).toLowerCase()) >= 0;
    });
  }

  function renderSourceBanner() {
    var el = $('#becsSourceBanner');
    if (!el) return;
    if (state.rendererParity) {
      el.innerHTML =
        '<div class="becs-source-card"><strong>DIAGNOSTIC</strong> Renderer Parity — identical frozen payload both columns. Not the founder approval comparison.</div>';
      return;
    }
    var L = state.leftMeta || {};
    var R = state.rightMeta || {};
    var reg = state.registry && state.registry.totals ? state.registry.totals : {};
    var pc = R.provenanceCounts || {};
    el.innerHTML =
      '<div class="becs-source-card becs-source-current">' +
      '<strong>LEFT · CURRENT SAVED PROFILE</strong>' +
      '<span>Source: ' +
      escapeHtml(L.source || '—') +
      '</span>' +
      '<span>Brand ID: <code>' +
      escapeHtml(L.brandId || state.brand.id) +
      '</code></span>' +
      '<span>Blocks: ' +
      escapeHtml(String(L.blockCount != null ? L.blockCount : '—')) +
      '</span>' +
      '<span>Snapshot: ' +
      escapeHtml(L.snapshotTimestamp || 'live at load ' + (L.loadedAt || '—')) +
      '</span>' +
      '<span class="becs-pill same">read-only</span>' +
      '</div>' +
      '<div class="becs-source-card becs-source-proposed">' +
      '<strong>RIGHT · CUMULATIVE PROPOSED PROFILE</strong>' +
      '<span>Assembly: <code>' +
      escapeHtml(R.assemblyContract || 'BE_CUMULATIVE_PROPOSAL_ASSEMBLER_V1') +
      '</code></span>' +
      '<span>Staged change-set: <code>' +
      escapeHtml(R.candidateVersion || state.brand.candidateVersion) +
      '</code></span>' +
      '<span>Staged file: <code>' +
      escapeHtml(R.stagedCandidateUrl || R.source || '—') +
      '</code></span>' +
      '<span>Proposal blocks: ' +
      escapeHtml(String(R.blockCount != null ? R.blockCount : '—')) +
      ' · hash <code>' +
      escapeHtml(String(R.contentHash || '—').slice(0, 24)) +
      '</code></span>' +
      '<span>Raw candidate blocks: ' +
      escapeHtml(String(R.rawCandidateBlockCount != null ? R.rawCandidateBlockCount : '—')) +
      ' · hash <code>' +
      escapeHtml(String(R.rawCandidateHash || '—').slice(0, 24)) +
      '</code></span>' +
      '<span>Older fallback used: <strong>' +
      (R.olderFallbackUsed ? 'YES' : 'NO') +
      '</strong></span>' +
      '<span>Incomplete drafts held out: ' +
      escapeHtml(String(R.incompleteDraftSlotCount != null ? R.incompleteDraftSlotCount : 0)) +
      '</span>' +
      (R.approvalBlockedByRegression
        ? '<span class="becs-pill del">approval blocked — regressions held out</span>'
        : '<span class="becs-pill same">no weaker overwrite</span>') +
      '</div>' +
      '<div class="becs-source-card becs-provenance-legend">' +
      '<strong>PROVENANCE LEGEND (publication proposal)</strong>' +
      '<span><span class="becs-pill same">RETAINED</span> ' +
      escapeHtml(String(pc.RETAINED_FROM_CURRENT || 0)) +
      ' · <span class="becs-pill mod">MODIFIED</span> ' +
      escapeHtml(String(pc.MODIFIED_EXISTING || 0)) +
      ' · <span class="becs-pill add">NEW</span> ' +
      escapeHtml(String((pc.NEW_COMPONENT || 0) + (pc.NEWLY_RESEARCHED || 0))) +
      ' · <span class="becs-pill del">INCOMPLETE DRAFT</span> ' +
      escapeHtml(String(pc.INCOMPLETE_DRAFT || 0)) +
      ' · <span class="becs-pill del">RESEARCH REQUIRED</span> ' +
      escapeHtml(String(pc.RESEARCH_REQUIRED || 0)) +
      '</span>' +
      '<span class="becs-muted">Retained = valid for cumulative publish, not independent research. Incomplete drafts do not replace CURRENT.</span>' +
      '</div>' +
      '<div class="becs-source-card">' +
      '<strong>REGISTRY</strong>' +
      '<span>82 reqs · outstanding ' +
      escapeHtml(String(reg.outstanding != null ? reg.outstanding : '—')) +
      ' · browser-verified ' +
      escapeHtml(String(reg.browserVerified != null ? reg.browserVerified : '—')) +
      ' · founder-approved ' +
      escapeHtml(String(reg.founderApproved != null ? reg.founderApproved : 0)) +
      '</span>' +
      '<span>Gate 2 content regen: <strong>BLOCKED</strong> until Gate 1 approval</span>' +
      '</div>';
  }

  function renderCompleteness() {
    var el = $('#becsCompleteness');
    if (!el) return;
    if (state.rendererParity) {
      el.innerHTML = '';
      return;
    }
    var drafts = state.incompleteDrafts || [];
    var draftHtml =
      '<div class="becs-completeness-head">Outstanding staged changes (not applied to cumulative PROPOSED)</div>' +
      (drafts.length
        ? '<div class="becs-incomplete-drafts">' +
          drafts
            .slice(0, 40)
            .map(function (d) {
              return (
                '<div class="becs-draft-item"><code>' +
                escapeHtml(d.slotKey) +
                '</code> <span class="becs-pill del">' +
                escapeHtml(d.provenance) +
                '</span> <span class="becs-muted">' +
                escapeHtml(d.reason || '') +
                '</span></div>'
              );
            })
            .join('') +
          (drafts.length > 40
            ? '<div class="becs-muted">… +' + (drafts.length - 40) + ' more held-out drafts</div>'
            : '') +
          '</div>'
        : '<p class="becs-muted">No incomplete drafts held out for this brand.</p>');

    var c = (state.manifest && state.manifest.completeness) || null;
    var factoryHtml = c
      ? '<div class="becs-completeness-head">Factory quality scoring (separate from publication-draft completeness)</div>' +
        '<div class="becs-completeness-grid">' +
        Object.keys(c)
          .map(function (k) {
            var item = c[k] || {};
            var st = item.status || 'OUTSTANDING';
            return (
              '<div class="becs-comp-item becs-comp-' +
              escapeHtml(String(st).toLowerCase()) +
              '"><span class="becs-comp-key">' +
              escapeHtml(k) +
              '</span><span class="becs-comp-status">' +
              escapeHtml(st) +
              '</span>' +
              (item.note ? '<span class="becs-comp-note">' + escapeHtml(item.note) + '</span>' : '') +
              '</div>'
            );
          })
          .join('') +
        '</div>'
      : '<p class="becs-muted">Factory completeness manifest unavailable.</p>';

    el.innerHTML = draftHtml + factoryHtml;
  }

  function renderReqList() {
    var ul = $('#becsReqList');
    if (!ul) return;
    if (state.rendererParity) {
      ul.innerHTML = '<li class="becs-muted">Renderer Parity diagnostic — registry deferred</li>';
      return;
    }
    var reqs = requirementsForActiveTab()
      .filter(function (r) {
        return r.tabId === state.activeTab || (r.tab === 'all' && state.activeTab === 'atelier-overview');
      })
      .slice(0, 40);
    if (!reqs.length) {
      ul.innerHTML = '<li class="becs-muted">No tab-scoped requirements indexed</li>';
      return;
    }
    ul.innerHTML = reqs
      .map(function (r) {
        var cls = r.outstanding ? 'outstanding' : 'present';
        return (
          '<li class="becs-req-' +
          cls +
          '" title="' +
          escapeHtml(r.founderInstruction || '') +
          '"><code>' +
          escapeHtml(r.requirementId) +
          '</code> ' +
          escapeHtml(r.status || '') +
          (r.outstanding ? ' · <span class="becs-pill del">outstanding</span>' : ' · <span class="becs-pill same">in product</span>') +
          '</li>'
        );
      })
      .join('');
  }

  function renderBrandApproval() {
    var el = $('#becsBrandApproval');
    if (!el) return;
    if (state.rendererParity) {
      el.innerHTML = '';
      return;
    }
    var bd = state.brandDecision || {};
    var pub = state.lastPublishResult || {};
    el.innerHTML =
      '<div class="becs-brand-approval-inner">' +
      '<strong>Brand-level founder decision &amp; publication</strong>' +
      '<span class="becs-muted">Approve &amp; Publish runs the controlled workflow (lock → gates → write plan → snapshot → gated apply). Production Airtable writes stay off until separate founder activation. Curio/Ascend remain NOT APPROVED. Gate 1/2 restrictions remain in force.</span>' +
      '<div class="becs-brand-actions">' +
      '<button type="button" data-becs-brand-decision="approve_proposed">Approve proposed (record only)</button>' +
      '<button type="button" data-becs-brand-decision="reject_proposed">Reject proposed</button>' +
      '<button type="button" data-becs-brand-decision="defer">Defer</button>' +
      '<button type="button" id="becsApprovePublishInline" class="becs-approve-publish">Approve &amp; Publish (dry-run)</button>' +
      '</div>' +
      '<div class="becs-review-meta">Decision: <strong>' +
      escapeHtml(bd.decision || 'none') +
      '</strong>' +
      (bd.recordedAt ? ' · ' + escapeHtml(bd.recordedAt) : '') +
      ' · publicationApplied=<strong>false</strong>' +
      (pub.status
        ? ' · last workflow: <strong>' +
          escapeHtml(pub.status) +
          '</strong> · writes=<strong>' +
          escapeHtml(String(pub.airtableWrites != null ? pub.airtableWrites : 0)) +
          '</strong>'
        : '') +
      '</div></div>';
    var inline = $('#becsApprovePublishInline');
    if (inline) {
      inline.addEventListener('click', function () {
        runApproveAndPublish();
      });
    }
  }

  async function runApproveAndPublish() {
    if (state.rendererParity) {
      setStatus('Approve & Publish is not available in Renderer Parity diagnostic mode.');
      return;
    }
    if (!state.brand) return;
    var blockingGaps = (state.incompleteDrafts || []).filter(function (d) {
      return /regress|prose_or_list|empty_or_placeholder|diligence_or_invalid_kpi/i.test(
        String(d.reason || '')
      );
    });
    if (blockingGaps.length || (state.rightMeta && state.rightMeta.approvalBlockedByRegression)) {
      setStatus(
        'Approve & Publish blocked: ' +
          blockingGaps.length +
          ' content-regression / invalid-KPI gaps remain. Open View Issues — research gaps stay visible but do not masquerade as ready.'
      );
      setIssuesDrawerOpen(true);
      return;
    }
    setStatus('Running Approve & Publish workflow (DRY_RUN)…');
    try {
      var body = {
        brandId: state.brand.id,
        brandSlug: state.brand.slug,
        brandName: state.brand.name,
        candidateVersion: state.brand.candidateVersion,
        overlayUrl: state.rightOverlayUrl || state.brand.overlayUrl,
        currentBlocks: state.leftBlocks || [],
        proposedBlocks: state.rightBlocks || [],
        mode: 'DRY_RUN',
        approveAndPublish: true,
        approveAllChanged: false,
        approvedSlotKeys: Object.keys(state.reviews || {}).filter(function (k) {
          var d = state.reviews[k] && state.reviews[k].decision;
          return d === 'approved' || d === 'improved';
        }),
        gate1: { founderApproved: false },
        gate2: { status: 'BLOCKED', founderApproved: false },
        scorecard: state.scorecard || {
          founderApprovalStatus: 'NOT_FOUNDER_APPROVED',
          priorityBlocked: 0,
          researchGapCount: (state.rightMeta && state.rightMeta.researchGapCount) || 0
        },
        outstandingRequirementCount:
          (state.registry && state.registry.totals && state.registry.totals.outstanding) || 0,
        approver: 'founder-studio'
      };
      var res = await fetch('/api/brand-explorer-compare/publish', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });
      var data = await res.json();
      if (!res.ok || !data.success) {
        throw new Error((data && data.error) || 'HTTP ' + res.status);
      }
      state.lastPublishResult = data;
      state.brandDecision = data.audit && data.audit.brandDecision
        ? data.audit.brandDecision
        : {
            decision: 'approve_and_publish',
            recordedAt: data.startedAt || new Date().toISOString()
          };
      renderBrandApproval();
      renderSourceBanner();
      setStatus(
        'Approve & Publish · ' +
          (data.status || 'DONE') +
          ' · airtableWrites=' +
          (data.airtableWrites || 0) +
          ' · hash=' +
          String(data.contentHash || '—').slice(0, 16) +
          (data.auditRelativePath ? ' · audit=' + data.auditRelativePath : '') +
          ' · ' +
          (data.message || '')
      );
    } catch (err) {
      setStatus('Approve & Publish ERROR: ' + (err.message || String(err)));
    }
  }

  function syncModeButtons() {
    document.querySelectorAll('[data-becs-mode]').forEach(function (b) {
      var mode = b.getAttribute('data-becs-mode');
      if (mode === 'gate1') {
        b.classList.toggle('active', !!state.rendererParity);
      } else if (!state.rendererParity) {
        b.classList.toggle('active', mode === state.viewMode);
      } else {
        b.classList.remove('active');
      }
    });
    var eq = document.querySelector('[data-becs-equalwidth]');
    if (eq) {
      eq.hidden = !state.rendererParity;
      eq.classList.toggle('active', !!state.equalWidth);
    }
    var brandSel = $('#becsBrand');
    if (brandSel) brandSel.disabled = !!state.rendererParity;
  }

  /** Toggle Gate 1 diagnostic mode; canonical URL stays brand-explorer-compare.html?beInternalPreview=1 */
  function setRendererParityMode(on) {
    state.rendererParity = !!on;
    var u = new URL(location.href);
    if (state.rendererParity) {
      u.searchParams.set('rendererParity', '1');
    } else {
      u.searchParams.delete('rendererParity');
      u.searchParams.delete('gate');
      u.searchParams.delete('gate1');
      u.searchParams.delete('equalWidth');
      state.equalWidth = false;
      document.body.classList.remove('becs-equalwidth');
    }
    u.searchParams.set('beInternalPreview', '1');
    history.replaceState({}, '', u.toString());
    syncModeButtons();
    var cfg = state.brand || BRANDS[0];
    if (state.rendererParity) cfg = BRANDS[0];
    if ($('#becsBrand')) $('#becsBrand').value = cfg.id;
    loadBrand(cfg);
  }

  function clearBrandColumnsForLoad(cfg) {
    // Immediately drop prior brand columns so a failed/slow switch cannot leave
    // Choice/Sleep Inn (etc.) content visible under a different brand label.
    state.leftBlocks = [];
    state.rightBlocks = [];
    state.leftBrand = null;
    state.rightBrand = null;
    state.leftMeta = null;
    state.rightMeta = null;
    state.rightOverlayUrl = null;
    state.diffs = [];
    state.assembly = null;
    state.incompleteDrafts = [];
    state.provenanceBySlot = {};
    state.scorecard = null;
    state.manifest = null;
    state.highlightStats = null;
    state.selectedSlot = null;
    state.leftSource = 'pending';
    var leftTag = $('#becsLeftTag');
    var rightTag = $('#becsRightTag');
    var leftMetaEl = $('#becsLeftMeta');
    var rightMetaEl = $('#becsRightMeta');
    if (leftTag) leftTag.textContent = 'Loading CURRENT for ' + (cfg.name || cfg.slug || cfg.id) + '…';
    if (rightTag) rightTag.textContent = 'Loading PROPOSED for ' + (cfg.name || cfg.slug || cfg.id) + '…';
    if (leftMetaEl) leftMetaEl.textContent = 'ID ' + cfg.id + ' · loading…';
    if (rightMetaEl) {
      rightMetaEl.textContent = 'staged ' + (cfg.overlayUrl || '—') + ' · loading…';
      rightMetaEl.setAttribute('data-becs-base-meta', rightMetaEl.textContent);
    }
  }

  function isStaleBrandLoad(loadGen, cfg) {
    if (loadGen !== state.loadGeneration) return true;
    if (!state.brand || state.brand.id !== cfg.id) return true;
    var brandSel = $('#becsBrand');
    if (brandSel && brandSel.value && brandSel.value !== cfg.id) return true;
    return false;
  }

  async function loadBrand(cfg) {
    var loadGen = ++state.loadGeneration;
    state.brand = cfg;
    clearBrandColumnsForLoad(cfg);
    var paired = $('#becsPairedDoc');
    if (paired) paired.innerHTML = '<p class="becs-muted">Loading ' + escapeHtml(cfg.name || cfg.slug || '') + '…</p>';
    var subnav = $('#becsSubnav');
    if (subnav) subnav.innerHTML = '';
    loadReviews();
    setStatus('Loading ' + cfg.name + '…');
    try {
      if (!state.contract) {
        state.contract = await fetchJson('/brand-explorer-phase2e-preview/subsection-parity-contract-v1.json');
      }
      if (isStaleBrandLoad(loadGen, cfg)) return;
      if (state.rendererParity) {
        await loadGate1Payload();
        if (isStaleBrandLoad(loadGen, cfg)) return;
        document.body.classList.add('becs-gate1');
        setStudioBadge('gate1');
        syncModeButtons();
        $('#becsLeftTag').textContent =
          'A — Compare column (canonical atelier HTML) · hash ' + (state.payloadHash || '—').slice(0, 12);
        $('#becsRightTag').textContent =
          'B — Compare column (same payload + same renderer) · identical markup';
        renderSourceBanner();
        renderSummary();
        renderScorecard();
        renderCompleteness();
        renderQualityFilters();
        renderReqList();
        renderChangeList();
        renderPairedDoc();
        renderSubnav();
        renderReviewDrawer(null);
        renderBrandApproval();
        setStatus(
          'GATE 1 diagnostic · Curio renderer parity · hash=' +
            (state.payloadHash || '—').slice(0, 16) +
            ' · blocks=' +
            state.leftBlocks.length +
            ' · atelier=' +
            (atelierApi() ? 'yes' : 'no') +
            ' · contentMutation=false · founderApproval=false'
        );
        return;
      }

      document.body.classList.remove('becs-gate1');
      setStudioBadge('compare');
      syncModeButtons();
      if (!state.registry) {
        try {
          state.registry = await fetchJson(REGISTRY_INDEX_URL);
        } catch (re) {
          state.registry = null;
        }
      }
      if (isStaleBrandLoad(loadGen, cfg)) return;
      try {
        state.scorecard = cfg.scorecardUrl ? await fetchJson(cfg.scorecardUrl) : null;
      } catch (e) {
        state.scorecard = null;
      }
      try {
        var persisted = await fetchJson(
          '/api/brand-explorer-compare/review?brandId=' + encodeURIComponent(cfg.id)
        );
        if (isStaleBrandLoad(loadGen, cfg)) return;
        if (persisted && persisted.reviews) state.reviews = persisted.reviews;
        if (persisted && persisted.brandDecision) state.brandDecision = persisted.brandDecision;
      } catch (pe) {
        /* localStorage already loaded */
      }
      state.leftBlocks = await loadLeftBrand(cfg);
      if (isStaleBrandLoad(loadGen, cfg)) return;
      state.rightBlocks = await loadRightBrand(cfg, state.leftBrand);
      if (isStaleBrandLoad(loadGen, cfg)) return;
      if (state.rightOverlayUrl && state.rightOverlayUrl !== cfg.overlayUrl) {
        throw new Error(
          'Proposed overlay URL drifted from selected brand (' +
            cfg.slug +
            '): ' +
            state.rightOverlayUrl
        );
      }
      state.diffs = buildDiffs();
      state.loadedAt = new Date().toISOString();
      $('#becsLeftTag').textContent =
        state.leftSource === 'baseline'
          ? 'Verified snapshot fallback (labeled) · read-only'
          : state.leftSource === 'empty-live'
            ? 'Empty live Presentation · Draft/Under Review · read-only'
            : 'Canonical live Presentation · read-only';
      $('#becsRightTag').textContent =
        'Cumulative proposal · ' +
        (state.rightMeta && state.rightMeta.candidateVersion
          ? state.rightMeta.candidateVersion
          : cfg.candidateVersion) +
        ' · hash ' +
        String((state.rightMeta && state.rightMeta.contentHash) || '—').slice(0, 12) +
        ' · no weaker overwrite';
      var leftMetaEl = $('#becsLeftMeta');
      var rightMetaEl = $('#becsRightMeta');
      if (leftMetaEl) {
        leftMetaEl.textContent =
          'ID ' +
          cfg.id +
          ' · ' +
          (state.leftMeta && state.leftMeta.blockCount) +
          ' blocks · loaded ' +
          ((state.leftMeta && state.leftMeta.loadedAt) || '—');
      }
      if (rightMetaEl) {
        var rightBase =
          (state.rightMeta && state.rightMeta.blockCount) +
          ' blocks · held-out drafts ' +
          ((state.rightMeta && state.rightMeta.incompleteDraftSlotCount) != null
            ? state.rightMeta.incompleteDraftSlotCount
            : '—') +
          ' · staged ' +
          (state.rightOverlayUrl || cfg.overlayUrl);
        rightMetaEl.setAttribute('data-becs-base-meta', rightBase);
        rightMetaEl.textContent = rightBase;
      }
      renderSourceBanner();
      renderSummary();
      renderScorecard();
      renderCompleteness();
      renderQualityFilters();
      renderReqList();
      renderChangeList();
      renderSubnav();
      renderPairedDoc();
      renderReviewDrawer(null);
      renderBrandApproval();
      setStatus(
        cfg.name +
          ' · CURRENT vs CUMULATIVE PROPOSED · left=' +
          state.leftSource +
          ' · right=' +
          ((state.rightMeta && state.rightMeta.assemblyContract) || 'cumulative') +
          ' · hash=' +
          String((state.rightMeta && state.rightMeta.contentHash) || '—').slice(0, 16) +
          ' · heldOut=' +
          ((state.rightMeta && state.rightMeta.incompleteDraftSlotCount) || 0) +
          ' · atelier=' +
          (atelierApi() ? 'yes' : 'no') +
          ' · items=' +
          state.diffs.length +
          ' · airtableWrites=0'
      );
    } catch (err) {
      if (isStaleBrandLoad(loadGen, cfg)) return;
      // Do not keep the previous brand's paired columns on failure.
      state.leftBlocks = [];
      state.rightBlocks = [];
      state.diffs = [];
      state.rightOverlayUrl = null;
      setStatus('ERROR: ' + err.message);
      var leftTag = $('#becsLeftTag');
      var rightTag = $('#becsRightTag');
      if (leftTag) leftTag.textContent = 'CURRENT unavailable';
      if (rightTag) rightTag.textContent = 'PROPOSED unavailable';
      $('#becsPairedDoc').innerHTML =
        '<div class="becs-unavailable" role="alert">' + escapeHtml(err.message) + '</div>';
    }
  }

  function brandFilterMeta(b) {
    return {
      parent: String(b.parentCompany || '').trim(),
      batch: String(b.batchTag || '').trim(),
      status: b.awaitingBrandBasicsCreate
        ? 'AWAITING_BASICS'
        : String(b.factoryStatus || b.founderApprovalStatus || 'NOT_APPROVED').trim(),
      hay: [
        b.name,
        b.slug,
        b.id,
        b.parentCompany,
        b.batchTag,
        b.factoryStatus,
        b.founderApprovalStatus
      ]
        .map(function (x) {
          return String(x || '').toLowerCase();
        })
        .join(' ')
    };
  }

  function filteredBrands() {
    var q = (($('#becsBrandSearch') && $('#becsBrandSearch').value) || '').trim().toLowerCase();
    var parent = (($('#becsParentFilter') && $('#becsParentFilter').value) || '').trim();
    var batch = (($('#becsBatchFilter') && $('#becsBatchFilter').value) || '').trim();
    var status = (($('#becsStatusFilter') && $('#becsStatusFilter').value) || '').trim();
    return BRANDS.slice()
      .filter(function (b) {
        var m = brandFilterMeta(b);
        if (q && m.hay.indexOf(q) === -1) return false;
        if (parent && m.parent !== parent) return false;
        if (batch && m.batch !== batch) return false;
        if (status) {
          if (status === 'AWAITING_BASICS' && !b.awaitingBrandBasicsCreate) return false;
          if (status !== 'AWAITING_BASICS' && m.status !== status && b.founderApprovalStatus !== status)
            return false;
        }
        return true;
      })
      .sort(function (a, b) {
        return String(a.name || a.slug || '').localeCompare(String(b.name || b.slug || ''), undefined, {
          sensitivity: 'base'
        });
      });
  }

  function rebuildBrandSelect(preferId) {
    var brandSel = $('#becsBrand');
    if (!brandSel) return;
    var keep = preferId || brandSel.value || (state.brand && state.brand.id) || '';
    var brandsForSelect = filteredBrands();
    brandSel.innerHTML = '';
    brandsForSelect.forEach(function (b, i) {
      var opt = document.createElement('option');
      opt.value = b.id;
      var suffix = b.awaitingBrandBasicsCreate
        ? ' · awaiting Basics'
        : b.factoryStatus === 'NEEDS_REMEDIATION'
          ? ' · remediation'
          : '';
      opt.textContent = (b.name || b.slug || b.id) + suffix;
      opt.setAttribute('data-becs-slug', b.slug || '');
      opt.setAttribute('data-becs-overlay', b.overlayUrl || '');
      if (b.parentCompany) opt.setAttribute('data-becs-parent', b.parentCompany);
      if (b.batchTag) opt.setAttribute('data-becs-batch', b.batchTag);
      brandSel.appendChild(opt);
      if (b.id === keep) opt.selected = true;
      else if (!keep && i === 0) opt.selected = true;
    });
    var countEl = $('#becsBrandFilterCount');
    if (countEl) {
      countEl.textContent = brandsForSelect.length + ' / ' + BRANDS.length + ' brands';
    }
    if (!brandsForSelect.length) {
      var empty = document.createElement('option');
      empty.value = '';
      empty.textContent = 'No brands match filters';
      brandSel.appendChild(empty);
    }
  }

  function populateBrandFilters() {
    var parents = {};
    var batches = {};
    BRANDS.forEach(function (b) {
      if (b.parentCompany) parents[b.parentCompany] = true;
      if (b.batchTag) batches[b.batchTag] = true;
    });
    var parentSel = $('#becsParentFilter');
    if (parentSel) {
      Object.keys(parents)
        .sort()
        .forEach(function (p) {
          var opt = document.createElement('option');
          opt.value = p;
          opt.textContent = p;
          parentSel.appendChild(opt);
        });
    }
    var batchSel = $('#becsBatchFilter');
    if (batchSel) {
      Object.keys(batches)
        .sort()
        .forEach(function (p) {
          var opt = document.createElement('option');
          opt.value = p;
          opt.textContent = p;
          batchSel.appendChild(opt);
        });
    }
  }

  function wireUi() {
    var brandSel = $('#becsBrand');
    populateBrandFilters();
    rebuildBrandSelect();
    function onFilterChange() {
      var prev = brandSel.value;
      rebuildBrandSelect(prev);
      if (state.rendererParity) return;
      if (brandSel.value && brandSel.value !== prev) {
        var cfg = BRANDS.find(function (b) {
          return b.id === brandSel.value;
        });
        if (cfg) loadBrand(cfg);
      }
    }
    ['becsBrandSearch', 'becsParentFilter', 'becsBatchFilter', 'becsStatusFilter'].forEach(function (id) {
      var el = $('#' + id);
      if (!el) return;
      el.addEventListener(id === 'becsBrandSearch' ? 'input' : 'change', onFilterChange);
    });
    brandSel.addEventListener('change', function () {
      if (state.rendererParity) return; // Gate 1 diagnostic locked to Curio saved payload
      var cfg = BRANDS.find(function (b) {
        return b.id === brandSel.value;
      });
      if (cfg) loadBrand(cfg);
    });

    // All ten atelier tabs on the permanent Comparison Studio surface
    var labels = {
      'atelier-overview': 'Overview',
      'atelier-value-owners': 'Value',
      'atelier-ops': 'Ops',
      'atelier-standards-owner': 'Standards',
      'atelier-commercial': 'Commercial',
      'atelier-economics': 'Economics',
      'atelier-loyalty': 'Loyalty',
      'atelier-footprint': 'Footprint',
      'atelier-materials': 'Materials',
      'atelier-insight': 'Insight'
    };
    var tabBar = $('#becsTabs');
    Object.keys(labels).forEach(function (id) {
      var btn = document.createElement('button');
      btn.type = 'button';
      btn.setAttribute('data-becs-tab', id);
      btn.textContent = labels[id];
      if (id === state.activeTab) btn.classList.add('active');
      btn.addEventListener('click', function () {
        setActiveTab(id);
      });
      tabBar.appendChild(btn);
    });

    document.querySelectorAll('[data-becs-mode]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var mode = btn.getAttribute('data-becs-mode') || 'full';
        if (mode === 'gate1') {
          setRendererParityMode(!state.rendererParity);
          return;
        }
        if (state.rendererParity) {
          state.viewMode = mode;
          setRendererParityMode(false);
          return;
        }
        state.viewMode = mode;
        syncModeButtons();
        renderPairedDoc();
        renderSubnav();
      });
    });
    syncModeButtons();

    var viewIssues = $('#becsViewIssues');
    if (viewIssues) {
      viewIssues.addEventListener('click', function () {
        setIssuesDrawerOpen(true);
      });
    }
    var issuesClose = $('#becsIssuesClose');
    if (issuesClose) {
      issuesClose.addEventListener('click', function () {
        setIssuesDrawerOpen(false);
      });
    }
    var issuesBackdrop = $('#becsIssuesBackdrop');
    if (issuesBackdrop) {
      issuesBackdrop.addEventListener('click', function () {
        setIssuesDrawerOpen(false);
      });
    }
    var issuesFilters = $('#becsIssuesFilters');
    if (issuesFilters) {
      issuesFilters.addEventListener('click', function (e) {
        var btn = e.target.closest('[data-becs-issue-filter]');
        if (!btn) return;
        state.issuesFilter = btn.getAttribute('data-becs-issue-filter') || 'all';
        renderIssuesDrawer();
      });
    }
    var issuesBody = $('#becsIssuesBody');
    if (issuesBody) {
      issuesBody.addEventListener('click', function (e) {
        var row = e.target.closest('[data-becs-issue-slot]');
        if (!row) return;
        var slot = row.getAttribute('data-becs-issue-slot');
        setIssuesDrawerOpen(false);
        // Jump to first pair / changed field mentioning slot family
        var hit = document.querySelector('[data-becs-slot-jump="' + slot + '"]');
        if (hit) hit.click();
        else {
          var pair = Array.prototype.find.call(document.querySelectorAll('.becs-pair'), function (p) {
            return (p.textContent || '').toLowerCase().indexOf(String(slot).split('.')[0]) >= 0;
          });
          if (pair) pair.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }
      });
    }

    $('#becsSummary').addEventListener('click', function (e) {
      var card = e.target.closest('[data-becs-filter]');
      if (!card) return;
      state.filter = card.getAttribute('data-becs-filter');
      renderSummary();
      renderChangeList();
    });

    $('#becsQualityFilters').addEventListener('click', function (e) {
      var btn = e.target.closest('[data-becs-qfilter]');
      if (!btn) return;
      state.qualityFilter = btn.getAttribute('data-becs-qfilter') || 'all';
      renderQualityFilters();
      renderChangeList();
      renderPairedDoc();
    });

    $('#becsChangeList').addEventListener('click', function (e) {
      var li = e.target.closest('[data-becs-slot-jump]');
      if (!li) return;
      selectSlot(li.getAttribute('data-becs-slot-jump'), li.getAttribute('data-becs-sub'));
    });

    $('#becsSubnav').addEventListener('click', function (e) {
      var gateTab = e.target.closest('[data-becs-gate1-tab]');
      if (gateTab) {
        e.preventDefault();
        setActiveTab(gateTab.getAttribute('data-becs-gate1-tab'));
        return;
      }
      var gateAnchor = e.target.closest('[data-becs-anchor]');
      if (gateAnchor) {
        e.preventDefault();
        var aid = gateAnchor.getAttribute('data-becs-anchor');
        var target = aid && document.getElementById(aid);
        if (target) {
          target.scrollIntoView({ behavior: 'smooth', block: 'start' });
          target.classList.add('becs-pair-flash');
          setTimeout(function () {
            target.classList.remove('becs-pair-flash');
          }, 1200);
        }
        return;
      }
      var a = e.target.closest('[data-becs-sub]');
      if (!a) return;
      e.preventDefault();
      jumpToSubsection(a.getAttribute('data-becs-sub'));
    });

    $('#becsReviewDrawer').addEventListener('click', function (e) {
      var btn = e.target.closest('[data-becs-decision]');
      if (!btn) return;
      recordDecision(btn.getAttribute('data-becs-decision'));
    });

    var brandApproval = $('#becsBrandApproval');
    if (brandApproval) {
      brandApproval.addEventListener('click', function (e) {
        var btn = e.target.closest('[data-becs-brand-decision]');
        if (!btn) return;
        recordBrandDecision(btn.getAttribute('data-becs-brand-decision'));
      });
    }

    $('#becsExport').addEventListener('click', exportReport);
    var prep = $('#becsPrepPublish');
    if (prep) prep.addEventListener('click', preparePublicationPackage);
    var approvePub = $('#becsApprovePublish');
    if (approvePub) {
      approvePub.addEventListener('click', function () {
        runApproveAndPublish();
      });
    }

    var hlToggle = $('#becsHighlightChanges');
    if (hlToggle) {
      hlToggle.checked = !!state.highlightChanges;
      hlToggle.addEventListener('change', function () {
        setHighlightChanges(!!hlToggle.checked);
      });
    }

    // Product-renderer in-tab CTAs (View Recent Openings → Footprint & Growth)
    var pairedDoc = $('#becsPairedDoc');
    if (pairedDoc && !pairedDoc._becsProductJumpWired) {
      pairedDoc._becsProductJumpWired = true;
      pairedDoc.addEventListener('click', function (e) {
        var jump = e.target.closest('[data-be-jump-atelier-tab]');
        if (!jump) return;
        e.preventDefault();
        e.stopPropagation();
        handleProductJumpCta(jump);
      });
    }
  }

  function boot() {
    if (state._booted) return;
    state._booted = true;
    if (!/[?&]beInternalPreview=1/.test(location.search)) {
      var u = new URL(location.href);
      u.searchParams.set('beInternalPreview', '1');
      history.replaceState({}, '', u.toString());
    }
    var q = new URLSearchParams(location.search);
    state.rendererParity =
      q.get('rendererParity') === '1' || q.get('gate') === '1' || q.get('gate1') === '1';
    state.equalWidth = q.get('equalWidth') === '1';
    if (state.equalWidth) document.body.classList.add('becs-equalwidth');
    if (globalThis.BeCompareHighlight && globalThis.BeCompareHighlight.readStoredDefault) {
      state.highlightChanges = !!globalThis.BeCompareHighlight.readStoredDefault();
    } else {
      state.highlightChanges = true;
    }
    document.body.classList.toggle('becs-hl-on', !!state.highlightChanges && !state.rendererParity);
    document.body.classList.toggle('becs-hl-off', !state.highlightChanges || !!state.rendererParity);
    wireUi();
    var brandParam = q.get('brand') || q.get('id');
    var cfg =
      BRANDS.find(function (b) {
        return b.id === brandParam || b.slug === brandParam;
      }) || BRANDS[0];
    if (state.rendererParity) {
      cfg = BRANDS[0]; // Curio only for Gate 1
    }
    $('#becsBrand').value = cfg.id;
    loadBrand(cfg);
  }

  // Access-gated pages call boot after SupportAdminGate.requireInternalRunbook succeeds.
  // Ungated/legacy harnesses may set window.__BECS_SKIP_ACCESS_GATE__ = true before this script.
  globalThis.BrandExplorerCompareStudioBoot = boot;
  function startWhenAllowed() {
    if (globalThis.__BECS_ACCESS_ALLOWED__ || globalThis.__BECS_SKIP_ACCESS_GATE__) {
      boot();
      return;
    }
    // Wait briefly for the HTML gate bootstrap; fall back only when explicitly skipped.
    var tries = 0;
    var timer = setInterval(function () {
      tries += 1;
      if (globalThis.__BECS_ACCESS_ALLOWED__ || globalThis.__BECS_SKIP_ACCESS_GATE__) {
        clearInterval(timer);
        boot();
      } else if (tries >= 40) {
        clearInterval(timer);
        // Gate script missing — deny silent customer exposure by not auto-booting.
        try {
          console.warn(
            '[BrandExplorerCompare] Waiting for SupportAdminGate; studio boot deferred until access is granted.'
          );
        } catch (e) {
          /* ignore */
        }
      }
    }, 100);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', startWhenAllowed);
  else startWhenAllowed();
})();
