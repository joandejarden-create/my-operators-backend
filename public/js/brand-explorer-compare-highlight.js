/**
 * Brand Explorer Comparison Studio — Highlight Changes (RIGHT column).
 * Annotates product-rendered DOM in place. Visibility is CSS-gated via body.becs-hl-on
 * so toggling never rebuilds content or moves scroll.
 */
(function (global) {
  'use strict';

  var HL_ATTR = 'data-becs-hl';
  var HL_ID_ATTR = 'data-becs-hl-id';
  var HL_TWIN_ATTR = 'data-becs-hl-twin';
  var UNIT_ATTR = 'data-becs-hl-unit';
  var MARKER_CLASS = 'becs-hl-marker';
  var REMOVED_CLASS = 'becs-hl-removed-indicator';
  var INCOMPLETE_RE =
    /RESEARCH_REQUIRED|Focus this card|Evidence-grounded|TODO_RESEARCH|\[research\]|incomplete draft/i;

  function nz(s) {
    return s == null ? '' : String(s).trim();
  }

  function normText(s) {
    return nz(s)
      .toLowerCase()
      .replace(/\s+/g, ' ')
      .replace(/[“”]/g, '"')
      .replace(/[‘’]/g, "'");
  }

  /**
   * Title identity only: treat spaced "and" and "&" as the same conjunction so
   * heading And→& corrections pair as MODIFIED (not remove+add). Never use in
   * fingerprint — body "and" must remain distinct from heading punctuation.
   */
  function normTitleIdentity(s) {
    return normText(s)
      .replace(/\s+&\s+/g, ' & ')
      .replace(/\s+\band\b\s+/g, ' & ');
  }

  function fingerprint(el) {
    if (!el) return '';
    if (el.tagName === 'IMG') {
      return (
        'img::' +
        imageBasename(el.getAttribute('src') || '') +
        '::' +
        normText(el.getAttribute('alt') || '')
      );
    }
    var clone = el.cloneNode(true);
    // Strip prior highlight chrome before hashing
    clone.querySelectorAll('.' + MARKER_CLASS + ', .' + REMOVED_CLASS).forEach(function (n) {
      n.remove();
    });
    // Include nested image basenames so gallery/card visual swaps register as modified
    var imgSig = Array.prototype.map
      .call(clone.querySelectorAll('img'), function (img) {
        return imageBasename(img.getAttribute('src') || '');
      })
      .join('|');
    return normText((clone.textContent || '') + '##' + imgSig);
  }

  function titleOf(el) {
    if (!el) return '';
    var t =
      el.querySelector(
        '.proof-point-card__headline, .scenario-card__body h4, h3, h4, .becs-card-title, th, dt'
      ) || null;
    if (t) return nz(t.textContent);
    if (el.tagName === 'LI') return nz(el.textContent).slice(0, 120);
    if (el.tagName === 'TR') {
      var cell = el.querySelector('th, td');
      return cell ? nz(cell.textContent) : '';
    }
    if (el.tagName === 'IMG') return nz(el.getAttribute('alt') || el.getAttribute('src'));
    return nz(el.getAttribute('aria-label') || '');
  }

  function unitType(el) {
    if (!el) return 'unknown';
    if (el.matches && el.matches('img')) return 'image';
    if (el.matches && el.matches('tr')) return 'table-row';
    if (el.matches && el.matches('li')) return 'bullet';
    if (el.matches && el.matches('.proof-point-card, article.proof-point-card')) return 'card';
    if (el.matches && el.matches('.scenario-card, .scenario-card--visual, .scenario-card--detail'))
      return 'card';
    if (el.matches && el.matches('.property-example-card, article.property-example-card')) return 'card';
    if (el.matches && el.matches('.becs-card')) return 'card';
    if (el.matches && el.matches('.oe-cluster')) return 'cluster';
    if (el.matches && el.matches('.kv')) return 'metrics';
    return 'block';
  }

  function imageBasename(src) {
    var s = nz(src).split('?')[0];
    var parts = s.split('/');
    var name = parts[parts.length - 1] || s;
    return normText(name).replace(/\.[a-z0-9]+$/i, '');
  }

  function stableId(el, index) {
    var type = unitType(el);
    var title = normTitleIdentity(titleOf(el));
    var src = '';
    var base = '';
    if (type === 'image') {
      src = nz(el.getAttribute('src') || '');
      base = imageBasename(src);
      var alt = normText(el.getAttribute('alt') || '');
      // Gallery slots: match by ordinal within the nearest section so CDN host
      // differences become MODIFIED (same slot, new asset) instead of remove+add.
      var host =
        el.closest('.oe-section, .gallery, .materials-gallery, .becs-product-mount') ||
        el.parentElement;
      var imgs = host ? host.querySelectorAll('img') : [];
      var ord = Array.prototype.indexOf.call(imgs, el);
      if (ord >= 0) return type + '::ord::' + ord;
      if (alt) return type + '::alt::' + alt;
      if (base && base.length > 8) return type + '::file::' + base;
    }
    if (title) return type + '::title::' + title + (base ? '::' + base : '');
    if (base) return type + '::file::' + base;
    var fp = fingerprint(el).slice(0, 160);
    if (fp) return type + '::fp::' + fp;
    return type + '::idx::' + index;
  }

  function collectUnits(root) {
    if (!root) return [];
    var units = [];
    var seen = new Set();

    function push(el) {
      if (!el || seen.has(el)) return;
      // Skip nested units already covered by a closer card/cluster parent collected later
      seen.add(el);
      units.push(el);
    }

    root.querySelectorAll('.proof-point-card, article.proof-point-card').forEach(push);
    root
      .querySelectorAll(
        '.scenario-card, .scenario-card--visual, .scenario-card--detail, .scenario-card--owner-value'
      )
      .forEach(push);
    root.querySelectorAll('.property-example-card, article.property-example-card').forEach(push);
    root.querySelectorAll('.becs-card:not(.becs-card-empty)').forEach(push);

    root.querySelectorAll('table tbody tr, table tr').forEach(function (tr) {
      if (tr.querySelector('th') && !tr.querySelector('td')) return; // header-only
      push(tr);
    });

    // Bullet-level first for clusters that are list wrappers (Key Differentiators)
    root.querySelectorAll('ul > li, ol > li').forEach(function (li) {
      var parentCard = li.closest('.proof-point-card, .scenario-card, .becs-card');
      if (parentCard) return; // card identity covers nested blurbs
      push(li);
    });

    root.querySelectorAll('.oe-cluster').forEach(function (cluster) {
      // Skip clusters whose semantic content is already covered by list items
      if (cluster.querySelector('ul > li, ol > li')) return;
      push(cluster);
    });

    root.querySelectorAll('img').forEach(function (img) {
      push(img);
    });

    return units;
  }

  function clearHighlights(root) {
    if (!root) return;
    root.querySelectorAll('[' + HL_ATTR + ']').forEach(function (el) {
      el.removeAttribute(HL_ATTR);
      el.removeAttribute(HL_ID_ATTR);
      el.removeAttribute(HL_TWIN_ATTR);
      el.classList.remove(
        'becs-hl-added',
        'becs-hl-modified',
        'becs-hl-incomplete',
        'becs-hl-unchanged-skip'
      );
    });
    root.querySelectorAll('.' + REMOVED_CLASS + ', .' + MARKER_CLASS).forEach(function (n) {
      n.remove();
    });
    root.querySelectorAll('[' + UNIT_ATTR + ']').forEach(function (el) {
      el.removeAttribute(UNIT_ATTR);
    });
  }

  function isIncompleteText(text) {
    return INCOMPLETE_RE.test(String(text || ''));
  }

  function markerHtml(kind, label) {
    return (
      '<span class="' +
      MARKER_CLASS +
      ' becs-hl-badge becs-hl-badge-' +
      kind +
      '" data-becs-hl-badge="' +
      kind +
      '" aria-hidden="true">' +
      label +
      '</span>'
    );
  }

  function annotate(el, kind, id, twinId) {
    if (!el) return;
    el.setAttribute(HL_ATTR, kind);
    el.setAttribute(HL_ID_ATTR, id);
    if (twinId) el.setAttribute(HL_TWIN_ATTR, twinId);
    el.classList.add('becs-hl-' + kind);
    var labels = {
      added: 'Added',
      modified: 'Modified',
      incomplete: 'Incomplete',
      removed: 'Removed'
    };
    var voidLike = el.tagName === 'IMG' || el.tagName === 'TR' || el.tagName === 'INPUT';
    var existing = voidLike
      ? el.previousElementSibling &&
        el.previousElementSibling.classList &&
        el.previousElementSibling.classList.contains(MARKER_CLASS)
      : el.querySelector(':scope > .' + MARKER_CLASS + ', :scope > .becs-hl-badge');
    if (!existing) {
      if (voidLike) {
        el.insertAdjacentHTML('beforebegin', markerHtml(kind, labels[kind] || kind));
      } else {
        el.insertAdjacentHTML('afterbegin', markerHtml(kind, labels[kind] || kind));
      }
    }
  }

  function addRemovedIndicator(rightCol, leftEl, id) {
    if (!rightCol || !leftEl) return;
    var title = titleOf(leftEl) || fingerprint(leftEl).slice(0, 80) || 'item';
    var type = unitType(leftEl);
    var btn = document.createElement('button');
    btn.type = 'button';
    btn.className = REMOVED_CLASS;
    btn.setAttribute(HL_ATTR, 'removed');
    btn.setAttribute(HL_ID_ATTR, id);
    btn.setAttribute('data-becs-hl-jump-left', id);
    btn.title = 'Removed vs CURRENT — jump to left-side original';
    btn.innerHTML =
      '<span class="becs-hl-badge becs-hl-badge-removed">Removed</span> ' +
      '<span class="becs-hl-removed-label">' +
      escapeHtml(type + ': ' + title).slice(0, 140) +
      '</span> ' +
      '<span class="becs-hl-removed-hint">View on CURRENT →</span>';
    rightCol.appendChild(btn);
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function indexUnits(units) {
    var byId = Object.create(null);
    units.forEach(function (el, i) {
      var id = stableId(el, i);
      el.setAttribute(UNIT_ATTR, id);
      if (!byId[id]) byId[id] = [];
      byId[id].push(el);
    });
    return byId;
  }

  function takeMatch(map, id) {
    var list = map[id];
    if (!list || !list.length) return null;
    return list.shift();
  }

  /**
   * Highlight one paired section (leftCol, rightCol are .becs-col nodes).
   */
  function highlightPair(leftCol, rightCol, opts) {
    opts = opts || {};
    if (!rightCol) return { added: 0, modified: 0, removed: 0, incomplete: 0, unchanged: 0 };

    clearHighlights(leftCol);
    clearHighlights(rightCol);

    var leftUnits = collectUnits(leftCol);
    var rightUnits = collectUnits(rightCol);
    var leftMap = indexUnits(leftUnits);
    var stats = { added: 0, modified: 0, removed: 0, incomplete: 0, unchanged: 0 };
    var matchedLeft = new Set();

    rightUnits.forEach(function (rEl, i) {
      var id = rEl.getAttribute(UNIT_ATTR) || stableId(rEl, i);
      var lEl = takeMatch(leftMap, id);
      var rFp = fingerprint(rEl);
      var incomplete = isIncompleteText(rFp) || isIncompleteText(titleOf(rEl));

      if (!lEl) {
        // Soft match: same type + title (collection reorders / minor markup drift).
        // Use title-identity so And vs & still pairs.
        var softKey = unitType(rEl) + '::title::' + normTitleIdentity(titleOf(rEl));
        if (normTitleIdentity(titleOf(rEl))) {
          Object.keys(leftMap).some(function (k) {
            if (k.indexOf(softKey) === 0 && leftMap[k].length) {
              lEl = leftMap[k].shift();
              return true;
            }
            return false;
          });
        }
      }

      if (!lEl) {
        if (incomplete) {
          annotate(rEl, 'incomplete', id);
          stats.incomplete++;
        } else {
          annotate(rEl, 'added', id);
          stats.added++;
        }
        return;
      }

      matchedLeft.add(lEl);
      lEl.setAttribute(HL_ID_ATTR, id);
      var lFp = fingerprint(lEl);
      if (lFp === rFp) {
        stats.unchanged++;
        // Do not annotate unchanged retained content
        return;
      }
      if (incomplete) {
        annotate(rEl, 'incomplete', id, id);
        stats.incomplete++;
        return;
      }
      annotate(rEl, 'modified', id, id);
      stats.modified++;
    });

    // Remaining left units were not matched → removed from proposed (non-destructive indicator)
    Object.keys(leftMap).forEach(function (id) {
      (leftMap[id] || []).forEach(function (lEl) {
        if (matchedLeft.has(lEl)) return;
        addRemovedIndicator(rightCol, lEl, id);
        lEl.setAttribute(HL_ATTR, 'removed-source');
        lEl.setAttribute(HL_ID_ATTR, id);
        stats.removed++;
      });
    });

    return stats;
  }

  function highlightPairedDoc(docRoot, opts) {
    opts = opts || {};
    var root = docRoot || document.getElementById('becsPairedDoc');
    if (!root) return { pairs: 0, totals: {} };
    var totals = { added: 0, modified: 0, removed: 0, incomplete: 0, unchanged: 0 };
    var pairs = root.querySelectorAll('.becs-pair');
    pairs.forEach(function (pair) {
      var left = pair.querySelector('.becs-col[data-side="left"], .becs-col-left');
      var right = pair.querySelector('.becs-col[data-side="right"], .becs-col-right');
      if (!right) return;
      var s = highlightPair(left, right, opts);
      Object.keys(totals).forEach(function (k) {
        totals[k] += s[k] || 0;
      });
      if ((s.added || 0) + (s.modified || 0) + (s.removed || 0) + (s.incomplete || 0) > 0) {
        pair.setAttribute('data-becs-hl-pair', 'changed');
      } else {
        pair.removeAttribute('data-becs-hl-pair');
      }
    });
    return { pairs: pairs.length, totals: totals };
  }

  function setHighlightEnabled(on, opts) {
    opts = opts || {};
    var enabled = !!on;
    document.body.classList.toggle('becs-hl-on', enabled);
    document.body.classList.toggle('becs-hl-off', !enabled);
    try {
      if (opts.persist !== false) {
        localStorage.setItem('becs-highlight-changes', enabled ? '1' : '0');
      }
    } catch (e) {
      /* ignore */
    }
    var toggle = document.getElementById('becsHighlightChanges');
    if (toggle) {
      toggle.checked = enabled;
      toggle.setAttribute('aria-pressed', enabled ? 'true' : 'false');
    }
    var lab = document.querySelector('[data-becs-hl-toggle-label]');
    if (lab) lab.textContent = enabled ? 'Highlight Changes: ON' : 'Highlight Changes: OFF';
    return enabled;
  }

  function readStoredDefault() {
    try {
      var v = localStorage.getItem('becs-highlight-changes');
      if (v === '0') return false;
      if (v === '1') return true;
    } catch (e) {
      /* ignore */
    }
    return true; // default ON
  }

  function wireJumpHandlers(root) {
    var el = root || document;
    if (el.__becsHlJumpWired) return;
    el.__becsHlJumpWired = true;
    el.addEventListener('click', function (e) {
      var btn = e.target.closest('[data-becs-hl-jump-left]');
      if (!btn) return;
      e.preventDefault();
      var id = btn.getAttribute('data-becs-hl-jump-left');
      var esc =
        typeof CSS !== 'undefined' && CSS.escape
          ? CSS.escape(id)
          : String(id).replace(/\\/g, '\\\\').replace(/"/g, '\\"');
      var left = document.querySelector(
        '.becs-col-left [' +
          HL_ID_ATTR +
          '="' +
          esc +
          '"], .becs-col[data-side="left"] [' +
          HL_ID_ATTR +
          '="' +
          esc +
          '"]'
      );
      if (left) {
        left.scrollIntoView({ behavior: 'smooth', block: 'center' });
        left.classList.add('becs-hl-jump-flash');
        setTimeout(function () {
          left.classList.remove('becs-hl-jump-flash');
        }, 1200);
      }
    });
  }

  // Pure helpers exported for tests
  function matchUnitLists(leftHtmlUnits, rightHtmlUnits) {
    // Test helper: arrays of {id, fp, title, incomplete}
    var leftMap = Object.create(null);
    leftHtmlUnits.forEach(function (u) {
      if (!leftMap[u.id]) leftMap[u.id] = [];
      leftMap[u.id].push(u);
    });
    var out = [];
    rightHtmlUnits.forEach(function (r) {
      var list = leftMap[r.id] || [];
      var l = list.length ? list.shift() : null;
      if (!l) {
        out.push({ id: r.id, change: r.incomplete ? 'incomplete' : 'added' });
        return;
      }
      if (l.fp === r.fp) out.push({ id: r.id, change: 'unchanged' });
      else if (r.incomplete) out.push({ id: r.id, change: 'incomplete' });
      else out.push({ id: r.id, change: 'modified' });
    });
    Object.keys(leftMap).forEach(function (id) {
      (leftMap[id] || []).forEach(function () {
        out.push({ id: id, change: 'removed' });
      });
    });
    return out;
  }

  global.BeCompareHighlight = {
    HL_ATTR: HL_ATTR,
    collectUnits: collectUnits,
    stableId: stableId,
    fingerprint: fingerprint,
    highlightPair: highlightPair,
    highlightPairedDoc: highlightPairedDoc,
    clearHighlights: clearHighlights,
    setHighlightEnabled: setHighlightEnabled,
    readStoredDefault: readStoredDefault,
    wireJumpHandlers: wireJumpHandlers,
    matchUnitLists: matchUnitLists,
    normText: normText,
    normTitleIdentity: normTitleIdentity
  };
})(typeof window !== 'undefined' ? window : globalThis);
