/**
 * Brand Explorer Comparison Studio — founder visual QA (internal).
 * Left: live Presentation (or labeled frozen baseline). Right: Phase 2E candidate overlay.
 * Diff + review feedback are local only — never production Airtable writes.
 */
(function () {
  'use strict';

  var BRANDS = [
    {
      id: 'receQkxgjlezsc1xg',
      slug: 'curio-collection',
      name: 'Curio Collection by Hilton',
      overlayUrl: '/brand-explorer-phase2e-preview/curio-collection-quality-candidate.json',
      scorecardUrl: '/brand-explorer-phase2e-preview/curio-collection-quality-scorecard.json',
      baselineUrl: '/brand-explorer-phase2e-preview/curio-frozen-baseline-snapshot.json',
      candidateVersion: 'phase2e-quality'
    },
    {
      id: 'reclkgOzvAcBheUSo',
      slug: 'ascend',
      name: 'Ascend Hotel Collection',
      overlayUrl: '/brand-explorer-phase2e-preview/ascend-quality-candidate.json',
      scorecardUrl: '/brand-explorer-phase2e-preview/ascend-quality-scorecard.json',
      baselineUrl: null,
      candidateVersion: 'phase2e-quality'
    }
  ];

  var TAB_IDS = [
    'atelier-overview',
    'atelier-value-owners',
    'atelier-ops',
    'atelier-standards-owner',
    'atelier-commercial',
    'atelier-economics',
    'atelier-loyalty',
    'atelier-footprint',
    'atelier-materials',
    'atelier-insight'
  ];

  var state = {
    brand: BRANDS[0],
    activeTab: TAB_IDS[0],
    syncScroll: true,
    filter: 'all',
    diffs: [],
    selectedSlot: null,
    leftSource: 'live', // live | baseline | unavailable
    leftBrand: null,
    rightBrand: null,
    overlay: null,
    scorecard: null,
    reviews: {}
  };

  function $(sel, root) {
    return (root || document).querySelector(sel);
  }

  function nz(s) {
    return s == null ? '' : String(s).trim();
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

  function blocksFromBrand(brand) {
    var be = brand && brand.brandExplorer;
    var blocks = (be && be.blocks) || [];
    return Array.isArray(blocks) ? blocks : [];
  }

  function indexBlocks(blocks) {
    var map = new Map();
    blocks.forEach(function (b, i) {
      var key = nz(b.slotKey || b.slot_key);
      if (!key) return;
      if (!map.has(key)) map.set(key, []);
      map.get(key).push({
        slotKey: key,
        title: nz(b.title),
        body: nz(b.body),
        imageUrl: nz(b.imageUrl || b.image_url),
        sort: typeof b.sort === 'number' ? b.sort : i + 1,
        index: i
      });
    });
    return map;
  }

  function itemSignature(item) {
    return [item.title, item.body, item.imageUrl].join('\u0001');
  }

  function tokenizeWords(text) {
    return nz(text).split(/(\s+)/).filter(function (t) {
      return t.length;
    });
  }

  /** Simple word-level LCS diff → HTML with ins/del */
  function wordDiffHtml(a, b) {
    var A = tokenizeWords(a);
    var B = tokenizeWords(b);
    var n = A.length;
    var m = B.length;
    if (n + m > 800) {
      if (a === b) return escapeHtml(a);
      return (
        '<del class="becs-diff-del">' +
        escapeHtml(a) +
        '</del> <ins class="becs-diff-add">' +
        escapeHtml(b) +
        '</ins>'
      );
    }
    var dp = Array(n + 1);
    for (var i = 0; i <= n; i++) {
      dp[i] = Array(m + 1).fill(0);
    }
    for (i = n - 1; i >= 0; i--) {
      for (var j = m - 1; j >= 0; j--) {
        dp[i][j] = A[i] === B[j] ? dp[i + 1][j + 1] + 1 : Math.max(dp[i + 1][j], dp[i][j + 1]);
      }
    }
    var out = [];
    i = 0;
    j = 0;
    while (i < n && j < m) {
      if (A[i] === B[j]) {
        out.push(escapeHtml(A[i]));
        i++;
        j++;
      } else if (dp[i + 1][j] >= dp[i][j + 1]) {
        out.push('<del class="becs-diff-del">' + escapeHtml(A[i]) + '</del>');
        i++;
      } else {
        out.push('<ins class="becs-diff-add">' + escapeHtml(B[j]) + '</ins>');
        j++;
      }
    }
    while (i < n) {
      out.push('<del class="becs-diff-del">' + escapeHtml(A[i++]) + '</del>');
    }
    while (j < m) {
      out.push('<ins class="becs-diff-add">' + escapeHtml(B[j++]) + '</ins>');
    }
    return out.join('');
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function lookLikeConflict(leftBody, rightBody) {
    var feeL = /\d+(\.\d+)?%/.test(leftBody);
    var feeR = /\d+(\.\d+)?%/.test(rightBody);
    if (feeL && feeR && leftBody !== rightBody) return true;
    var numL = leftBody.match(/\b\d{2,5}\b/g) || [];
    var numR = rightBody.match(/\b\d{2,5}\b/g) || [];
    if (numL.length && numR.length && numL.join(',') !== numR.join(',') && /hotel|key|room|pipeline|matched/i.test(leftBody + rightBody)) {
      return true;
    }
    return false;
  }

  function buildDiffs(leftBrand, rightBrand) {
    var leftMap = indexBlocks(blocksFromBrand(leftBrand));
    var rightMap = indexBlocks(blocksFromBrand(rightBrand));
    var keys = new Set([].concat(Array.from(leftMap.keys()), Array.from(rightMap.keys())));
    var diffs = [];
    keys.forEach(function (slotKey) {
      var L = leftMap.get(slotKey) || [];
      var R = rightMap.get(slotKey) || [];
      var leftSig = L.map(itemSignature).join('\n---\n');
      var rightSig = R.map(itemSignature).join('\n---\n');
      var leftBody = L.map(function (x) {
        return x.body;
      }).join('\n');
      var rightBody = R.map(function (x) {
        return x.body;
      }).join('\n');
      var change = 'unchanged';
      if (!L.length && R.length) change = 'added';
      else if (L.length && !R.length) change = 'removed';
      else if (leftSig !== rightSig) change = 'modified';
      var conflict = change === 'modified' && lookLikeConflict(leftBody, rightBody);
      var needsReview = change !== 'unchanged' || conflict;
      diffs.push({
        slotKey: slotKey,
        change: change,
        conflict: conflict,
        needsReview: needsReview,
        left: L,
        right: R,
        leftBody: leftBody,
        rightBody: rightBody,
        wordDiffHtml: change === 'modified' ? wordDiffHtml(leftBody, rightBody) : ''
      });
    });
    diffs.sort(function (a, b) {
      var order = { added: 0, removed: 1, modified: 2, unchanged: 3 };
      return (order[a.change] - order[b.change]) || a.slotKey.localeCompare(b.slotKey);
    });
    return diffs;
  }

  function summarize(diffs) {
    var s = { totalChanged: 0, added: 0, removed: 0, modified: 0, unchanged: 0, conflicts: 0, needsReview: 0 };
    diffs.forEach(function (d) {
      if (d.change === 'unchanged') s.unchanged++;
      else {
        s.totalChanged++;
        s[d.change]++;
      }
      if (d.conflict) s.conflicts++;
      if (d.needsReview) s.needsReview++;
    });
    return s;
  }

  function reviewStats() {
    var keys = Object.keys(state.reviews);
    var counts = { reviewed: keys.length, approve_new: 0, keep_existing: 0, needs_correction: 0, needs_research: 0, unresolved: 0 };
    keys.forEach(function (k) {
      var d = state.reviews[k] && state.reviews[k].decision;
      if (d && counts[d] != null) counts[d]++;
    });
    counts.unresolved = state.diffs.filter(function (d) {
      return d.needsReview && !state.reviews[d.slotKey];
    }).length;
    return counts;
  }

  async function fetchJson(url) {
    var res = await fetch(url, { credentials: 'same-origin' });
    if (!res.ok) throw new Error(url + ' → HTTP ' + res.status);
    return res.json();
  }

  async function loadLiveBrand(brandId) {
    var url = '/api/brand-library/brand?brandId=' + encodeURIComponent(brandId) + '&beInternalPreview=1';
    var data = await fetchJson(url);
    if (!data || !data.success || !data.brand) throw new Error('Brand API returned no brand');
    return data.brand;
  }

  function baselineToBrand(snapshot, meta) {
    var rows = (snapshot && snapshot.presentation && snapshot.presentation.rows) || snapshot.rows || [];
    var blocks = rows
      .map(function (r, i) {
        return {
          slotKey: nz(r.slotKey || r.fields && r.fields['Slot Key']),
          title: nz(r.title || r.fields && r.fields.Title),
          body: nz(r.body || r.fields && r.fields.Body),
          imageUrl: r.imageUrl || null,
          sort: i + 1
        };
      })
      .filter(function (b) {
        return b.slotKey;
      });
    return {
      id: meta.id,
      name: meta.name,
      slug: meta.slug,
      brandExplorer: { version: 1, blocks: blocks, overlaySource: 'frozen_baseline' },
      brandExplorerDisplayState: 'external_owner_ready',
      beComparisonBaseline: true
    };
  }

  async function loadLeftBrand(cfg) {
    try {
      var live = await loadLiveBrand(cfg.id);
      if (!blocksFromBrand(live).length) throw new Error('Live brand has no Presentation blocks');
      state.leftSource = 'live';
      return live;
    } catch (err) {
      if (cfg.baselineUrl) {
        try {
          var snap = await fetchJson(cfg.baselineUrl);
          state.leftSource = 'baseline';
          return baselineToBrand(snap, cfg);
        } catch (e2) {
          state.leftSource = 'unavailable';
          throw new Error('Live profile unavailable (' + err.message + '); baseline also failed (' + e2.message + ')');
        }
      }
      state.leftSource = 'unavailable';
      throw err;
    }
  }

  async function loadRightBrand(cfg, leftBrand) {
    var overlay = await fetchJson(cfg.overlayUrl);
    state.overlay = overlay;
    var clone = JSON.parse(JSON.stringify(leftBrand));
    var api = window.BrandExplorerAtelierFromApi;
    if (!api || !api.applyCandidateOverlayToBrand) throw new Error('Atelier overlay API missing');
    return api.applyCandidateOverlayToBrand(clone, overlay);
  }

  function mountPane(rootSel, brand, unavailableMsg) {
    var root = $(rootSel);
    if (!root) return;
    var nav = root.querySelector('[data-be-atelier-nav]');
    var panels = root.querySelector('[data-be-atelier-panels]');
    if (unavailableMsg) {
      if (nav) nav.innerHTML = '';
      if (panels) {
        panels.innerHTML = '<div class="becs-unavailable" role="alert">' + escapeHtml(unavailableMsg) + '</div>';
      }
      return;
    }
    var api = window.BrandExplorerAtelierFromApi;
    if (!api) {
      if (panels) panels.innerHTML = '<div class="becs-unavailable">Atelier renderer failed to load.</div>';
      return;
    }
    // Ensure internal-preview unlock for locked profiles
    brand.shouldRenderFullProfile = true;
    if (!brand.brandExplorerDisplayState) brand.brandExplorerDisplayState = 'external_owner_ready';
    api.mountIntoRoot(root, brand);
  }

  function setActiveTab(tabId) {
    state.activeTab = tabId;
    var api = window.BrandExplorerAtelierFromApi;
    if (api && api.activateAtelierTab) {
      api.activateAtelierTab($('#becsLeftAtelier'), tabId);
      api.activateAtelierTab($('#becsRightAtelier'), tabId);
    }
    document.querySelectorAll('[data-becs-tab]').forEach(function (btn) {
      btn.classList.toggle('active', btn.getAttribute('data-becs-tab') === tabId);
    });
  }

  function wireSyncScroll() {
    var left = $('#becsLeftScroll');
    var right = $('#becsRightScroll');
    if (!left || !right || left._becsScrollWired) return;
    left._becsScrollWired = true;
    var lock = false;
    function sync(from, to) {
      if (!state.syncScroll || lock) return;
      lock = true;
      var ratio = from.scrollTop / Math.max(1, from.scrollHeight - from.clientHeight);
      to.scrollTop = ratio * Math.max(1, to.scrollHeight - to.clientHeight);
      lock = false;
    }
    left.addEventListener('scroll', function () {
      sync(left, right);
    });
    right.addEventListener('scroll', function () {
      sync(right, left);
    });
  }

  function applyTextHighlights() {
    // Annotate right pane bodies for modified slots with amber marker on section titles when possible
    state.diffs.forEach(function (d) {
      if (d.change === 'unchanged') return;
      var side = d.change === 'removed' ? '#becsLeftAtelier' : '#becsRightAtelier';
      var root = $(side);
      if (!root) return;
      // Mark first text node containers that contain a distinctive needle
      var needle = (d.change === 'removed' ? d.leftBody : d.rightBody).slice(0, 40);
      if (needle.length < 12) return;
      root.querySelectorAll('p, li, h2, h3, .brand-position-card__body, .oe-section-hint').forEach(function (el) {
        if (el.getAttribute('data-becs-marked')) return;
        if (el.textContent && el.textContent.indexOf(needle) !== -1) {
          el.setAttribute('data-becs-slot', d.slotKey);
          el.setAttribute('data-becs-marked', d.change);
          el.classList.add(
            d.change === 'added' ? 'becs-diff-add' : d.change === 'removed' ? 'becs-diff-del' : d.conflict ? 'becs-diff-amber' : 'becs-diff-amber'
          );
        }
      });
    });
  }

  function renderSummary() {
    var s = summarize(state.diffs);
    var el = $('#becsSummary');
    if (!el) return;
    var cards = [
      ['changed', 'Changed', s.totalChanged],
      ['added', 'Added', s.added],
      ['removed', 'Removed', s.removed],
      ['modified', 'Modified', s.modified],
      ['unchanged', 'Unchanged', s.unchanged],
      ['conflicts', 'Source conflicts', s.conflicts],
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
    var sc = state.scorecard && state.scorecard.metrics;
    var rs = reviewStats();
    if (!sc) {
      el.innerHTML =
        '<span class="warn">Automated quality scorecard not loaded — AI gate pass ≠ founder approval.</span>' +
        ' · Founder reviewed: <strong>' +
        rs.reviewed +
        '</strong> · Unresolved changed: <strong>' +
        rs.unresolved +
        '</strong>';
      return;
    }
    el.innerHTML =
      'Automated gates (not founder-approved): ' +
      'A <strong>' +
      sc.A_populationCompleteness +
      '</strong> · B <strong>' +
      sc.B_contentQualityPassRate +
      '</strong> · C <strong>' +
      sc.C_qualityAdjustedCompleteness +
      '</strong> · Failures <strong>' +
      ((sc.counts && sc.counts.failures) || 0) +
      '</strong>' +
      ' · Founder reviewed <strong>' +
      rs.reviewed +
      '</strong> · Approve new <strong>' +
      rs.approve_new +
      '</strong> · Keep existing <strong>' +
      rs.keep_existing +
      '</strong> · Needs correction <strong>' +
      rs.needs_correction +
      '</strong> · Needs research <strong>' +
      rs.needs_research +
      '</strong> · Unresolved <strong>' +
      rs.unresolved +
      '</strong>';
  }

  function filteredDiffs() {
    return state.diffs.filter(function (d) {
      if (state.filter === 'all' || state.filter === 'changed') {
        if (state.filter === 'changed') return d.change !== 'unchanged';
        return true;
      }
      if (state.filter === 'conflicts') return d.conflict;
      if (state.filter === 'needsReview') return d.needsReview;
      if (state.filter === 'added') return d.change === 'added';
      if (state.filter === 'removed') return d.change === 'removed';
      if (state.filter === 'modified') return d.change === 'modified';
      if (state.filter === 'unchanged') return d.change === 'unchanged';
      return true;
    });
  }

  function renderChangeList() {
    var ul = $('#becsChangeList');
    if (!ul) return;
    var rows = filteredDiffs();
    ul.innerHTML = rows
      .slice(0, 400)
      .map(function (d) {
        var pill =
          d.conflict
            ? 'conflict'
            : d.change === 'added'
              ? 'add'
              : d.change === 'removed'
                ? 'del'
                : d.change === 'modified'
                  ? 'mod'
                  : 'same';
        var rev = state.reviews[d.slotKey];
        return (
          '<li data-becs-slot-jump="' +
          escapeHtml(d.slotKey) +
          '" class="' +
          (state.selectedSlot === d.slotKey ? 'active' : '') +
          '"><span class="becs-pill ' +
          pill +
          '">' +
          (d.conflict ? 'conflict' : d.change) +
          '</span>' +
          escapeHtml(d.slotKey) +
          (rev ? ' · <em>' + escapeHtml(rev.decision) + '</em>' : '') +
          '</li>'
        );
      })
      .join('');
  }

  function selectSlot(slotKey) {
    state.selectedSlot = slotKey;
    var d = state.diffs.find(function (x) {
      return x.slotKey === slotKey;
    });
    renderChangeList();
    renderReviewDrawer(d);
    // Jump both panes — try activate tab heuristically by slot prefix
    var tabGuess = guessTabForSlot(slotKey);
    if (tabGuess) setActiveTab(tabGuess);
    flashSlot(slotKey);
    var preview = $('#becsWordDiff');
    if (preview && d) {
      preview.innerHTML = d.wordDiffHtml || '<span class="becs-muted">No word-level diff (added/removed/unchanged).</span>';
    }
  }

  function guessTabForSlot(slotKey) {
    if (/^overview\.|^snapshot\.|^positioning\.|^identity\./.test(slotKey)) return 'atelier-overview';
    if (/^valueOwners\.|^owner\./.test(slotKey)) return 'atelier-value-owners';
    if (/^operations\./.test(slotKey)) return 'atelier-ops';
    if (/^standards\./.test(slotKey)) return 'atelier-standards-owner';
    if (/^commercial\./.test(slotKey)) return 'atelier-commercial';
    if (/^economics\./.test(slotKey)) return 'atelier-economics';
    if (/^loyalty\./.test(slotKey)) return 'atelier-loyalty';
    if (/^footprint\.|^metric\./.test(slotKey)) return 'atelier-footprint';
    if (/^materials\./.test(slotKey)) return 'atelier-materials';
    if (/^insight\./.test(slotKey)) return 'atelier-insight';
    return state.activeTab;
  }

  function flashSlot(slotKey) {
    document.querySelectorAll('[data-becs-slot="' + slotKey + '"]').forEach(function (el) {
      el.classList.add('becs-field-flash');
      el.scrollIntoView({ block: 'center', behavior: 'smooth' });
      setTimeout(function () {
        el.classList.remove('becs-field-flash');
      }, 1600);
    });
  }

  function renderReviewDrawer(d) {
    var box = $('#becsReviewDrawer');
    if (!box) return;
    if (!d) {
      box.innerHTML = '<h3>Field review</h3><p class="becs-review-meta">Select a changed field from the list.</p>';
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
      '</textarea>' +
      '</div>' +
      '<div class="becs-review-meta">Automated change: <strong>' +
      escapeHtml(d.change) +
      '</strong>' +
      (d.conflict ? ' · <span class="warn">possible source/numeric conflict</span>' : '') +
      ' · Current decision: <strong>' +
      escapeHtml(rev.decision || 'unresolved') +
      '</strong>' +
      ' · Color = change type, not quality judgment.</div>' +
      '<div id="becsWordDiff" style="margin-top:8px;font-size:0.8rem;line-height:1.45;max-height:120px;overflow:auto;padding:8px;background:#121a33;border-radius:8px;border:1px solid rgba(255,255,255,0.08)"></div>';
    var preview = $('#becsWordDiff');
    if (preview) preview.innerHTML = d.wordDiffHtml || '—';
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
    persistReviewRemote(state.reviews[state.selectedSlot]);
    renderScorecard();
    renderChangeList();
    renderReviewDrawer(
      state.diffs.find(function (x) {
        return x.slotKey === state.selectedSlot;
      })
    );
    setStatus('Saved decision for ' + state.selectedSlot);
  }

  function persistReviewRemote(entry) {
    fetch('/api/brand-explorer-compare/review', {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ brandId: state.brand.id, entry: entry, all: state.reviews })
    }).catch(function () {
      /* localStorage remains source of truth if API unavailable */
    });
  }

  function exportReport() {
    var report = {
      contract: 'BE_COMPARISON_STUDIO_REVIEW_V1',
      exportedAt: new Date().toISOString(),
      brand: { id: state.brand.id, slug: state.brand.slug, name: state.brand.name },
      candidateVersion: state.brand.candidateVersion,
      leftSource: state.leftSource,
      summary: summarize(state.diffs),
      founderReview: reviewStats(),
      automatedScorecard: state.scorecard && state.scorecard.metrics ? state.scorecard.metrics : null,
      note: 'Automated gate pass ≠ founder approval. Decisions are local review only — no Airtable writes.',
      reviews: state.reviews,
      diffs: state.diffs.map(function (d) {
        return {
          slotKey: d.slotKey,
          change: d.change,
          conflict: d.conflict,
          needsReview: d.needsReview,
          decision: (state.reviews[d.slotKey] && state.reviews[d.slotKey].decision) || null
        };
      })
    };
    var blob = new Blob([JSON.stringify(report, null, 2)], { type: 'application/json' });
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'be-comparison-review-' + state.brand.slug + '-' + Date.now() + '.json';
    a.click();
    URL.revokeObjectURL(a.href);
  }

  function setStatus(msg) {
    var el = $('#becsStatus');
    if (el) el.textContent = msg;
  }

  async function loadBrand(cfg) {
    state.brand = cfg;
    loadReviews();
    setStatus('Loading ' + cfg.name + '…');
    $('#becsLeftLabel').textContent = 'CURRENT PROFILE';
    $('#becsRightLabel').textContent = 'PHASE 2E RECREATED';
    try {
      state.scorecard = null;
      try {
        state.scorecard = await fetchJson(cfg.scorecardUrl);
      } catch (e) {
        /* optional */
      }
      var left = await loadLeftBrand(cfg);
      state.leftBrand = left;
      var right = await loadRightBrand(cfg, left);
      state.rightBrand = right;
      state.diffs = buildDiffs(left, right);
      var leftMsg =
        state.leftSource === 'baseline'
          ? null
          : null;
      mountPane('#becsLeftAtelier', left, null);
      mountPane('#becsRightAtelier', right, null);
      if (state.leftSource === 'baseline') {
        $('#becsLeftTag').textContent = 'FROZEN BASELINE FALLBACK (not silent live substitute)';
      } else {
        $('#becsLeftTag').textContent = 'Live Airtable Presentation (read-only)';
      }
      $('#becsRightTag').textContent = 'Universal factory candidate · ' + cfg.overlayUrl;
      setActiveTab(TAB_IDS[0]);
      wireSyncScroll();
      setTimeout(applyTextHighlights, 50);
      renderSummary();
      renderScorecard();
      renderChangeList();
      renderReviewDrawer(null);
      setStatus(
        cfg.name +
          ' · left=' +
          state.leftSource +
          ' · slots compared=' +
          state.diffs.length +
          ' · writes=0'
      );
    } catch (err) {
      state.diffs = [];
      mountPane('#becsLeftAtelier', null, 'Existing profile unavailable: ' + err.message);
      mountPane('#becsRightAtelier', null, 'Recreated profile not mounted because left comparison context failed.');
      $('#becsLeftTag').textContent = 'UNAVAILABLE';
      renderSummary();
      renderScorecard();
      renderChangeList();
      setStatus('ERROR: ' + err.message);
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

    var tabBar = $('#becsTabs');
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
    TAB_IDS.forEach(function (id) {
      var btn = document.createElement('button');
      btn.type = 'button';
      btn.setAttribute('data-becs-tab', id);
      btn.textContent = labels[id] || id;
      btn.addEventListener('click', function () {
        setActiveTab(id);
      });
      tabBar.appendChild(btn);
    });

    $('#becsSummary').addEventListener('click', function (e) {
      var card = e.target.closest('[data-becs-filter]');
      if (!card) return;
      state.filter = card.getAttribute('data-becs-filter');
      renderSummary();
      renderChangeList();
    });

    $('#becsChangeList').addEventListener('click', function (e) {
      var li = e.target.closest('[data-becs-slot-jump]');
      if (!li) return;
      selectSlot(li.getAttribute('data-becs-slot-jump'));
    });

    $('#becsReviewDrawer').addEventListener('click', function (e) {
      var btn = e.target.closest('[data-becs-decision]');
      if (!btn) return;
      recordDecision(btn.getAttribute('data-becs-decision'));
    });

    $('#becsSyncScroll').addEventListener('change', function (e) {
      state.syncScroll = !!e.target.checked;
    });

    $('#becsExport').addEventListener('click', exportReport);

    $('#becsFullLeft').addEventListener('click', function () {
      $('#becsPanes').className = 'becs-panes full-left';
    });
    $('#becsFullRight').addEventListener('click', function () {
      $('#becsPanes').className = 'becs-panes full-right';
    });
    $('#becsFullBoth').addEventListener('click', function () {
      $('#becsPanes').className = 'becs-panes';
    });
  }

  function atelierShell(id) {
    return (
      '<div class="be-atelier-root" id="' +
      id +
      '" data-be-atelier-root="1">' +
      '<nav class="section-nav" data-be-atelier-nav role="tablist"></nav>' +
      '<div data-be-atelier-panels></div>' +
      '</div>'
    );
  }

  function boot() {
    // Ensure internal preview gate for locked brands
    if (!/[?&]beInternalPreview=1/.test(location.search)) {
      var u = new URL(location.href);
      u.searchParams.set('beInternalPreview', '1');
      history.replaceState({}, '', u.toString());
    }
    wireUi();
    $('#becsLeftScroll').innerHTML = atelierShell('becsLeftAtelier');
    $('#becsRightScroll').innerHTML = atelierShell('becsRightAtelier');
    var q = new URLSearchParams(location.search);
    var brandParam = q.get('brand') || q.get('id');
    var cfg = BRANDS.find(function (b) {
      return b.id === brandParam || b.slug === brandParam;
    }) || BRANDS[0];
    $('#becsBrand').value = cfg.id;
    loadBrand(cfg);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();
