/**
 * Brand Explorer Comparison Studio — Phase 2G paired-subsection document.
 * ONE page-level scroll. Each subsection is a shared grid row (left existing | right reconstructed).
 * Next pair begins below the taller column — no dual full-profile scroll sync.
 */
(function () {
  'use strict';

  var BRANDS = [
    {
      id: 'receQkxgjlezsc1xg',
      slug: 'curio-collection',
      name: 'Curio Collection by Hilton',
      overlayUrl: '/brand-explorer-phase2e-preview/curio-collection-phase2g-candidate.json',
      scorecardUrl: '/brand-explorer-phase2e-preview/curio-collection-phase2g-scorecard.json',
      baselineUrl: '/brand-explorer-phase2e-preview/curio-frozen-baseline-snapshot.json',
      candidateVersion: 'phase2g-parity',
      fallbackOverlayUrl: '/brand-explorer-phase2e-preview/curio-collection-phase2f-candidate.json'
    },
    {
      id: 'reclkgOzvAcBheUSo',
      slug: 'ascend',
      name: 'Ascend Hotel Collection',
      overlayUrl: '/brand-explorer-phase2e-preview/ascend-phase2g-candidate.json',
      scorecardUrl: '/brand-explorer-phase2e-preview/ascend-phase2g-scorecard.json',
      baselineUrl: null,
      candidateVersion: 'phase2g-parity',
      fallbackOverlayUrl: '/brand-explorer-phase2e-preview/ascend-phase2f-candidate.json'
    }
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
    leftBlocks: [],
    rightBlocks: [],
    scorecard: null,
    reviews: {}
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

  function renderPairedDoc() {
    var root = $('#becsPairedDoc');
    if (!root) return;
    var html = [];
    filteredSubsections().forEach(function (sub) {
      var L = itemsForSubsection(state.leftBlocks, sub);
      var R = itemsForSubsection(state.rightBlocks, sub);
      var pairs = matchItems(L, R).filter(function (p) {
        return pairPassesFilters(p, sub);
      });
      if (state.viewMode === 'changes' && !pairs.length) return;
      var changeCount = pairs.filter(function (p) {
        return classifyPair(p) !== 'unchanged';
      }).length;
      html.push(
        '<section class="becs-pair" id="becs-sub-' +
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
          (changeCount ? ' <span class="becs-pill mod">' + changeCount + ' changed</span>' : '') +
          '</div></header>' +
          '<div class="becs-pair-cols">' +
          '<div class="becs-col becs-col-left" data-side="left">' +
          (pairs.length
            ? pairs
                .map(function (p) {
                  return renderCard(p.left, 'left', p.status);
                })
                .join('')
            : '<div class="becs-card becs-card-empty"><div class="becs-empty-label">No items</div></div>') +
          '</div>' +
          '<div class="becs-col becs-col-right" data-side="right">' +
          (pairs.length
            ? pairs
                .map(function (p) {
                  return renderCard(p.right, 'right', p.status);
                })
                .join('')
            : '<div class="becs-card becs-card-empty"><div class="becs-empty-label">No items</div></div>') +
          '</div></div></section>'
      );
    });
    root.innerHTML = html.join('') || '<p class="becs-muted">No subsections match the current filters.</p>';
  }

  function renderSubnav() {
    var nav = $('#becsSubnav');
    if (!nav) return;
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

  function renderScorecard() {
    var el = $('#becsScorecard');
    if (!el) return;
    var sc = state.scorecard && (state.scorecard.metrics || state.scorecard);
    var rs = reviewStats();
    var visual = (sc && sc.visual) || sc || {};
    var anti = (sc && sc.antiPadding) || {};
    el.innerHTML =
      'Phase 2G (not founder-approved): visual B <strong>' +
      (visual.visualQualityPassRate != null ? visual.visualQualityPassRate : '—') +
      '</strong> · anti-padding failures <strong>' +
      (anti.paddingFailures != null ? anti.paddingFailures : sc && sc.researchGapCount != null ? sc.researchGapCount : '—') +
      '</strong> · research gaps <strong>' +
      ((sc && sc.researchGapCount) || 0) +
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
      box.innerHTML = '<h3>Field review</h3><p class="becs-review-meta">Select a changed field.</p>';
      return;
    }
    var rev = state.reviews[d.slotKey] || {};
    box.innerHTML =
      '<h3>Review · <code>' +
      escapeHtml(d.slotKey) +
      '</code></h3>' +
      '<div class="becs-review-row">' +
      '<button type="button" data-becs-decision="approve_new">Approve new</button>' +
      '<button type="button" data-becs-decision="keep_existing">Keep existing</button>' +
      '<button type="button" data-becs-decision="needs_correction">Needs correction</button>' +
      '<button type="button" data-becs-decision="needs_research">Needs research</button>' +
      '<textarea id="becsComment" placeholder="Optional comment">' +
      escapeHtml(rev.comment || '') +
      '</textarea></div>' +
      '<div class="becs-review-meta">Change: <strong>' +
      escapeHtml(d.change) +
      '</strong> · Subsection: <strong>' +
      escapeHtml(d.subsectionId) +
      '</strong> · Decision: <strong>' +
      escapeHtml(rev.decision || 'unresolved') +
      '</strong></div>';
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
      contract: 'BE_COMPARISON_STUDIO_PAIRED_V2G',
      exportedAt: new Date().toISOString(),
      brand: { id: state.brand.id, slug: state.brand.slug, name: state.brand.name },
      candidateVersion: state.brand.candidateVersion,
      leftSource: state.leftSource,
      summary: summarize(state.diffs),
      founderReview: reviewStats(),
      scorecard: state.scorecard,
      note: 'Paired-subsection comparison. Automated metrics ≠ founder approval. No Airtable writes.',
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

  function setActiveTab(tabId) {
    state.activeTab = tabId;
    document.querySelectorAll('[data-becs-tab]').forEach(function (btn) {
      btn.classList.toggle('active', btn.getAttribute('data-becs-tab') === tabId);
    });
    renderSubnav();
    renderSummary();
    renderChangeList();
    renderPairedDoc();
    var scroll = $('#becsDocScroll');
    if (scroll) scroll.scrollTop = 0;
  }

  async function loadLeftBlocks(cfg) {
    try {
      var data = await fetchJson(
        '/api/brand-library/brand?brandId=' + encodeURIComponent(cfg.id) + '&beInternalPreview=1'
      );
      if (!data || !data.success || !data.brand) throw new Error('no brand');
      var blocks = blocksFromBrand(data.brand);
      if (!blocks.length) throw new Error('empty presentation');
      state.leftSource = 'live';
      return blocks;
    } catch (err) {
      if (cfg.baselineUrl) {
        var snap = await fetchJson(cfg.baselineUrl);
        state.leftSource = 'baseline';
        return baselineToBlocks(snap);
      }
      state.leftSource = 'unavailable';
      throw err;
    }
  }

  async function loadRightBlocks(cfg) {
    var overlay = null;
    try {
      overlay = await fetchJson(cfg.overlayUrl);
    } catch (e) {
      if (cfg.fallbackOverlayUrl) overlay = await fetchJson(cfg.fallbackOverlayUrl);
      else throw e;
    }
    return blocksFromBrand(overlay);
  }

  async function loadBrand(cfg) {
    state.brand = cfg;
    loadReviews();
    setStatus('Loading ' + cfg.name + '…');
    try {
      if (!state.contract) {
        state.contract = await fetchJson('/brand-explorer-phase2e-preview/subsection-parity-contract-v1.json');
      }
      try {
        state.scorecard = await fetchJson(cfg.scorecardUrl);
      } catch (e) {
        state.scorecard = null;
      }
      state.leftBlocks = await loadLeftBlocks(cfg);
      state.rightBlocks = await loadRightBlocks(cfg);
      state.diffs = buildDiffs();
      $('#becsLeftTag').textContent =
        state.leftSource === 'baseline'
          ? 'FROZEN BASELINE FALLBACK (labeled)'
          : 'Live Airtable Presentation (read-only)';
      $('#becsRightTag').textContent = 'Phase 2G candidate · ' + cfg.overlayUrl;
      renderSummary();
      renderScorecard();
      renderQualityFilters();
      renderChangeList();
      renderSubnav();
      renderPairedDoc();
      renderReviewDrawer(null);
      setStatus(
        cfg.name +
          ' · paired subsections · left=' +
          state.leftSource +
          ' · items compared=' +
          state.diffs.length +
          ' · writes=0'
      );
    } catch (err) {
      setStatus('ERROR: ' + err.message);
      $('#becsPairedDoc').innerHTML =
        '<div class="becs-unavailable" role="alert">' + escapeHtml(err.message) + '</div>';
    }
  }

  function wireUi() {
    var brandSel = $('#becsBrand');
    BRANDS.forEach(function (b, i) {
      var opt = document.createElement('option');
      opt.value = b.id;
      opt.textContent = b.name;
      if (i === 0) opt.selected = true;
      brandSel.appendChild(opt);
    });
    brandSel.addEventListener('change', function () {
      var cfg = BRANDS.find(function (b) {
        return b.id === brandSel.value;
      });
      if (cfg) loadBrand(cfg);
    });

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
        state.viewMode = btn.getAttribute('data-becs-mode') || 'full';
        document.querySelectorAll('.becs-mode').forEach(function (b) {
          b.classList.toggle('active', b.getAttribute('data-becs-mode') === state.viewMode);
        });
        renderPairedDoc();
      });
    });

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

    $('#becsExport').addEventListener('click', exportReport);
  }

  function boot() {
    if (!/[?&]beInternalPreview=1/.test(location.search)) {
      var u = new URL(location.href);
      u.searchParams.set('beInternalPreview', '1');
      history.replaceState({}, '', u.toString());
    }
    wireUi();
    var q = new URLSearchParams(location.search);
    var brandParam = q.get('brand') || q.get('id');
    var cfg =
      BRANDS.find(function (b) {
        return b.id === brandParam || b.slug === brandParam;
      }) || BRANDS[0];
    $('#becsBrand').value = cfg.id;
    loadBrand(cfg);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
