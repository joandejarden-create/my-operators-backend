(function () {
    'use strict';

    const API = {
        list: '/api/market-alerts',
        rail: '/api/market-alerts/rail'
    };

    const CATEGORIES = [
        { key: 'all', label: 'All' },
        { key: 'Deals', label: 'Deals' },
        { key: 'Capital', label: 'Capital' },
        { key: 'Brand', label: 'Brand' },
        { key: 'Supply', label: 'Supply' },
        { key: 'Demand', label: 'Demand' },
        { key: 'Loyalty', label: 'Loyalty' },
        { key: 'Risk', label: 'Risk' }
    ];

    const REGION_GROUPS = [
        { key: null, label: 'All' },
        { key: 'Global', label: 'Global' },
        { key: 'Europe', label: 'Europe' },
        { key: 'Asia Pacific', label: 'Asia Pacific' },
        { key: 'Caribbean', label: 'Caribbean' },
        { key: 'Latin America', label: 'Latin America' },
        { key: 'Other', label: 'Other' }
    ];

    const SAVED_STORAGE_KEY = 'dc_saved_alert_ids';
    const SEARCH_DEBOUNCE_MS = 400;
    const DEFAULT_LIMIT = 100;

    let feedItems = [];
    let railData = { topRead: [], liveFeed: [] };
    let selectedTimeWindow = '7d';
    let selectedCategory = null;
    let selectedRegionGroup = null;
    let searchTerm = '';
    let savedFilterOn = false;
    let searchDebounceTimer = null;
    const DRAWER_SUMMARY_CLAMP = 6;

    function getSavedIds() {
        try {
            var raw = localStorage.getItem(SAVED_STORAGE_KEY);
            if (!raw) return [];
            return JSON.parse(raw);
        } catch (_) {
            return [];
        }
    }

    function setSavedIds(ids) {
        try {
            localStorage.setItem(SAVED_STORAGE_KEY, JSON.stringify(ids));
        } catch (_) {}
    }

    function toggleSaved(id) {
        var ids = getSavedIds();
        var i = ids.indexOf(id);
        if (i === -1) ids.push(id);
        else ids.splice(i, 1);
        setSavedIds(ids);
        return ids.indexOf(id) !== -1;
    }

    function isSaved(id) {
        return getSavedIds().indexOf(id) !== -1;
    }

    function showToast(message) {
        var el = document.getElementById('marketAlertsToast');
        if (!el) {
            el = document.createElement('div');
            el.id = 'marketAlertsToast';
            el.setAttribute('role', 'status');
            el.style.cssText = 'position:fixed;bottom:20px;left:50%;transform:translateX(-50%);background:var(--dc-bg-card);border:1px solid rgba(255,255,255,0.2);color:var(--dc-text);padding:10px 16px;border-radius:8px;font-size:12px;z-index:9999;box-shadow:0 4px 12px rgba(0,0,0,0.3);display:none;';
            document.body.appendChild(el);
        }
        el.textContent = message;
        el.style.display = 'block';
        clearTimeout(showToast._t);
        showToast._t = setTimeout(function () {
            el.style.display = 'none';
        }, 2500);
    }

    function escapeHtml(s) {
        if (s == null) return '';
        var div = document.createElement('div');
        div.textContent = s;
        return div.innerHTML;
    }

    function escapeAttr(s) {
        if (s == null) return '';
        return String(s)
            .replace(/&/g, '&amp;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    }

    function timeAgo(dateStr) {
        if (!dateStr) return '';
        try {
            var d = new Date(dateStr);
            if (isNaN(d.getTime())) return dateStr;
            var now = new Date();
            var diff = now - d;
            if (diff < 60000) return 'Just now';
            if (diff < 3600000) return Math.floor(diff / 60000) + 'm ago';
            if (diff < 86400000) return Math.floor(diff / 3600000) + 'h ago';
            if (diff < 172800000) return 'Yesterday';
            if (diff < 604800000) return Math.floor(diff / 86400000) + 'd ago';
            return d.toLocaleDateString([], { month: 'short', day: 'numeric' });
        } catch (_) {
            return dateStr;
        }
    }

    function normalizeItem(apiItem) {
        var f = apiItem.fields || {};
        var publishedAt = f['Published At'] || f.publishedAt || null;
        return {
            id: apiItem.id,
            title: f['Title'] || f.title || 'Untitled',
            summary: f['Summary'] || f.summary || '',
            category: f['Category'] || f.category || '',
            regionGroup: f['Region Group'] || f.regionGroup || 'Global',
            sourceName: f['Source Name'] || f.sourceName || '',
            sourceUrl: f['Source URL'] || f.sourceUrl || '',
            publishedAt: publishedAt,
            priority: f['Priority'] || f.priority || '',
            timeAgo: timeAgo(publishedAt),
            sortDate: publishedAt ? new Date(publishedAt).getTime() : 0
        };
    }

    function iconForCategory(category) {
        var svg = CATEGORY_ICONS[category] || CATEGORY_ICONS.all;
        return { svg: svg, cls: 'dc-activity-feed__icon' };
    }

    function setViewState(loading, content, empty, emptyMessage) {
        var feedLoading = document.getElementById('feedLoading');
        var newsContent = document.getElementById('newsContent');
        var feedEmpty = document.getElementById('feedEmpty');
        if (feedLoading) feedLoading.style.display = loading ? 'flex' : 'none';
        if (newsContent) newsContent.style.display = content ? 'block' : 'none';
        if (feedEmpty) {
            feedEmpty.style.display = empty ? 'block' : 'none';
            if (emptyMessage && feedEmpty.querySelector('p')) feedEmpty.querySelector('p').textContent = emptyMessage;
        }
    }

    var CATEGORY_ICONS = {
        all: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/></svg>',
        Deals: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 21h18"/><path d="M9 8h1"/><path d="M9 12h1"/><path d="M9 16h1"/><path d="M14 8h1"/><path d="M14 12h1"/><path d="M14 16h1"/><path d="M5 21V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2v16"/></svg>',
        Capital: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2v20"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>',
        Brand: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 15s1-1 4-1 5 2 8 2 4-1 4-1V3s-1 1-4 1-5-2-8-2-4 1-4 1z"/><line x1="4" y1="22" x2="4" y2="15"/></svg>',
        Supply: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 21h18"/><path d="M5 21V7l8-4v18"/><path d="M19 21V11l-6-4"/><path d="M9 9v.01"/><path d="M9 12v.01"/><path d="M9 15v.01"/><path d="M9 18v.01"/></svg>',
        Demand: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3v18h18"/><path d="M7 14l4-4 4 2 5-6"/></svg>',
        Loyalty: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l7.78-7.78 1.06-1.06a5.5 5.5 0 0 0 0-7.78z"/></svg>',
        Risk: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg>'
    };

    function renderCategoryNav() {
        var nav = document.getElementById('newsCatNav');
        if (!nav) return;
        nav.innerHTML = CATEGORIES.map(function (c) {
            var active = (c.key === 'all' && !selectedCategory) || (c.key === selectedCategory);
            var icon = CATEGORY_ICONS[c.key] || CATEGORY_ICONS.all;
            return '<a href="#" class="' + (active ? 'active' : '') + '" data-category="' + escapeAttr(c.key) + '">' +
                '<span class="cat-nav-icon">' + icon + '</span>' +
                '<span class="cat-nav-label">' + escapeHtml(c.label) + '</span>' +
                '</a>';
        }).join('');
        nav.querySelectorAll('a').forEach(function (a) {
            a.addEventListener('click', function (e) {
                e.preventDefault();
                selectedCategory = a.getAttribute('data-category') === 'all' ? null : a.getAttribute('data-category');
                nav.querySelectorAll('a').forEach(function (x) { x.classList.remove('active'); });
                a.classList.add('active');
                updateResetViewButton();
                loadFeed();
            });
        });
    }

    var REGION_ICONS = {
        all: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg>',
        Global: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/></svg>',
        Europe: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/></svg>',
        'Asia Pacific': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/></svg>',
        Caribbean: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/></svg>',
        'Latin America': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/></svg>',
        Other: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/><circle cx="12" cy="10" r="3"/></svg>'
    };

    function renderRegionNav() {
        var nav = document.getElementById('newsRegionNav');
        if (!nav) return;
        nav.innerHTML = REGION_GROUPS.map(function (r) {
            var val = r.key === undefined || r.key === null ? 'all' : r.key;
            var active = (val === 'all' && !selectedRegionGroup) || (selectedRegionGroup === r.key);
            var iconKey = val === 'all' ? 'all' : r.key;
            var icon = REGION_ICONS[iconKey] || REGION_ICONS.all;
            return '<a href="#" class="' + (active ? 'active' : '') + '" data-region="' + escapeAttr(val) + '">' +
                '<span class="region-nav-icon">' + icon + '</span>' +
                '<span class="region-nav-label">' + escapeHtml(r.label) + '</span>' +
                '</a>';
        }).join('');
        nav.querySelectorAll('a').forEach(function (a) {
            a.addEventListener('click', function (e) {
                e.preventDefault();
                var v = a.getAttribute('data-region');
                selectedRegionGroup = (v === 'all') ? null : v;
                nav.querySelectorAll('a').forEach(function (x) { x.classList.remove('active'); });
                a.classList.add('active');
                updateResetViewButton();
                loadFeed();
            });
        });
    }

    function applySavedFilter(items) {
        if (!savedFilterOn) return items;
        var ids = getSavedIds();
        if (ids.length === 0) return [];
        return items.filter(function (i) { return ids.indexOf(i.id) !== -1; });
    }

    function openDrawer(item) {
        if (!item) return;
        var overlay = document.getElementById('drawerOverlay');
        var drawer = document.getElementById('drawer');
        var pillsEl = document.getElementById('drawerPills');
        var titleEl = document.getElementById('drawerTitle');
        var metaEl = document.getElementById('drawerMeta');
        var summaryEl = document.getElementById('drawerSummary');
        var expandBtn = document.getElementById('drawerExpandSummary');
        var openSource = document.getElementById('drawerOpenSource');
        var starBtn = document.getElementById('drawerStar');
        if (!drawer || !titleEl) return;

        var pills = [item.category || 'Alert', item.regionGroup || 'Global'];
        if (item.priority) pills.push(item.priority);
        pillsEl.innerHTML = pills.map(function (p) {
            return '<span class="news-drawer-pill">' + escapeHtml(p) + '</span>';
        }).join('');

        titleEl.textContent = item.title;
        metaEl.textContent = [item.sourceName || 'Source', item.timeAgo || ''].filter(Boolean).join(' \u2022 ');

        var summary = item.summary || '';
        summaryEl.textContent = summary;
        var clampThreshold = 2400;
        var shouldClamp = summary.length > clampThreshold;
        summaryEl.classList.toggle('is-clamped', shouldClamp);
        if (expandBtn) {
            expandBtn.style.display = shouldClamp ? 'block' : 'none';
            expandBtn.textContent = 'Show more';
            expandBtn.onclick = function () {
                if (summaryEl.classList.contains('is-clamped')) {
                    summaryEl.classList.remove('is-clamped');
                    expandBtn.textContent = 'Show less';
                } else {
                    summaryEl.classList.add('is-clamped');
                    expandBtn.textContent = 'Show more';
                }
            };
        }

        openSource.href = item.sourceUrl || '#';
        openSource.style.display = item.sourceUrl ? 'inline-block' : 'none';

        starBtn.textContent = isSaved(item.id) ? '\u2605 Saved' : '\u2606 Save';
        starBtn.classList.toggle('saved', isSaved(item.id));
        starBtn.onclick = function (e) {
            e.preventDefault();
            toggleSaved(item.id);
            starBtn.textContent = isSaved(item.id) ? '\u2605 Saved' : '\u2606 Save';
            starBtn.classList.toggle('saved', isSaved(item.id));
            showToast(isSaved(item.id) ? 'Saved' : 'Removed from saved');
            if (savedFilterOn) renderAll();
        };

        var copyBtn = document.getElementById('drawerCopyLink');
        if (copyBtn) {
            copyBtn.onclick = function () {
                var url = item.sourceUrl || (window.location.origin + window.location.pathname);
                if (navigator.clipboard && navigator.clipboard.writeText) {
                    navigator.clipboard.writeText(url).then(function () { showToast('Link copied'); });
                } else {
                    var ta = document.createElement('textarea');
                    ta.value = url;
                    document.body.appendChild(ta);
                    ta.select();
                    document.execCommand('copy');
                    document.body.removeChild(ta);
                    showToast('Link copied');
                }
            };
        }

        overlay.classList.add('is-open');
        drawer.classList.add('is-open');
        overlay.setAttribute('aria-hidden', 'false');
        drawer.setAttribute('aria-hidden', 'false');
    }

    function closeDrawer() {
        var overlay = document.getElementById('drawerOverlay');
        var drawer = document.getElementById('drawer');
        if (overlay) overlay.classList.remove('is-open');
        if (drawer) drawer.classList.remove('is-open');
        if (overlay) overlay.setAttribute('aria-hidden', 'true');
        if (drawer) drawer.setAttribute('aria-hidden', 'true');
    }

    var HEADLINE_COLUMN_CATEGORIES = ['Deals', 'Capital', 'Brand', 'Supply'];
    var HEADLINES_PER_COLUMN = 3;

    function cardHtml(i) {
        var tag = escapeHtml(i.category || 'Alert');
        var title = escapeHtml(i.title);
        var dek = escapeHtml((i.summary || '').slice(0, 220));
        if (i.summary && i.summary.length > 220) dek += '\u2026';
        var metaParts = [i.regionGroup, i.sourceName, i.timeAgo].filter(Boolean);
        var meta = escapeHtml(metaParts.join(' \u2022 '));
        var saved = isSaved(i.id);
        var starClass = 'card-star' + (saved ? ' saved' : '');
        return '<div class="news-headline-card" data-id="' + escapeAttr(i.id) + '">' +
            '<div class="card-top">' +
            '<span class="card-label">' + tag + '</span>' +
            '<span class="card-top-right">' +
            (meta ? '<span class="card-meta">' + meta + '</span>' : '') +
            '<button type="button" class="' + starClass + '" data-id="' + escapeAttr(i.id) + '" title="' + (saved ? 'Unsave' : 'Save (local)') + '" aria-label="' + (saved ? 'Unsave' : 'Save') + '">&#9733;</button>' +
            '</span>' +
            '</div>' +
            '<div class="card-title">' + title + '</div>' +
            (dek ? '<div class="card-dek">' + dek + '</div>' : '') +
            '</div>';
    }

    function renderHeroGrid(items) {
        var el = document.getElementById('heroGrid');
        var emptyEl = document.getElementById('heroGridEmpty');
        if (!el) return;
        var list = applySavedFilter(items);

        if (selectedCategory || selectedRegionGroup) {
            /* Category or region filter selected: fill grid with all matching items (same card size). */
            if (list.length === 0) {
                el.style.display = 'none';
                if (emptyEl) emptyEl.style.display = 'block';
                return;
            }
            if (emptyEl) emptyEl.style.display = 'none';
            el.style.display = '';
            el.innerHTML = list.map(cardHtml).join('');
        } else {
            /* All category + All region: show items from four main categories first, then others */
            var priorityItems = [];
            var otherItems = [];
            
            // Separate items by whether they're in the four main categories
            list.forEach(function (i) {
                var cat = i.category || '';
                if (HEADLINE_COLUMN_CATEGORIES.indexOf(cat) !== -1) {
                    priorityItems.push(i);
                } else {
                    otherItems.push(i);
                }
            });

            // Combine: priority categories first (up to 3 each), then all others
            var displayItems = [];
            var countByCategory = {};
            HEADLINE_COLUMN_CATEGORIES.forEach(function (cat) { countByCategory[cat] = 0; });
            
            priorityItems.forEach(function (i) {
                var cat = i.category || '';
                if (countByCategory[cat] < HEADLINES_PER_COLUMN) {
                    displayItems.push(i);
                    countByCategory[cat]++;
                }
            });
            
            // Add all other category items after
            displayItems = displayItems.concat(otherItems);

            if (displayItems.length === 0) {
                el.style.display = 'none';
                if (emptyEl) emptyEl.style.display = 'block';
                return;
            }

            if (emptyEl) emptyEl.style.display = 'none';
            el.style.display = '';
            el.innerHTML = displayItems.map(cardHtml).join('');
        }

        el.querySelectorAll('.news-headline-card').forEach(function (card) {
            card.addEventListener('click', function (e) {
                if (e.target.classList.contains('card-star')) return;
                var id = card.getAttribute('data-id');
                var item = feedItems.filter(function (i) { return i.id === id; })[0];
                if (item) openDrawer(item);
            });
        });
        el.querySelectorAll('.news-headline-card .card-star').forEach(function (btn) {
            btn.addEventListener('click', function (e) {
                e.preventDefault();
                e.stopPropagation();
                var id = btn.getAttribute('data-id');
                toggleSaved(id);
                btn.classList.toggle('saved', isSaved(id));
                btn.setAttribute('title', isSaved(id) ? 'Unsave' : 'Save (local)');
                btn.setAttribute('aria-label', isSaved(id) ? 'Unsave' : 'Save');
                showToast(isSaved(id) ? 'Saved' : 'Removed from saved');
                if (savedFilterOn) renderAll();
            });
        });
    }

    function renderTopRead(items) {
        var el = document.getElementById('topReadList');
        if (!el) return;
        var list = (items && items.length) ? items : (railData && railData.topRead) || [];
        var normalized = Array.isArray(list) && list.length && list[0] && list[0].fields
            ? list.map(function (i) { return normalizeItem(i); })
            : list;
        var slice = normalized.slice(0, 5);
        el.innerHTML = slice.map(function (i, idx) {
            var title = escapeHtml(i.title);
            var href = i.sourceUrl || '#';
            var target = href !== '#' ? ' target="_blank" rel="noopener"' : '';
            var num = idx + 1;
            return '<li><span class="top-read-num-circle" aria-hidden="true">' + num + '</span><a href="' + escapeAttr(href) + '"' + target + '>' + title + '</a></li>';
        }).join('');
    }

    function renderLiveFeed(items) {
        var el = document.getElementById('liveFeedList');
        if (!el) return;
        var list = applySavedFilter(items);
        el.innerHTML = list.map(function (i) {
            var iconInfo = iconForCategory(i.category);
            var meta = [i.category, i.regionGroup, i.sourceName, i.timeAgo].filter(Boolean).join(' \u2022 ');
            var title = escapeHtml(i.title);
            var saved = isSaved(i.id);
            var starClass = 'feed-star' + (saved ? ' saved' : '');
            return '<li class="news-feed-row" data-id="' + escapeAttr(i.id) + '">' +
                '<span class="feed-icon" aria-hidden="true">' + iconInfo.svg + '</span>' +
                '<div class="feed-body">' +
                '<div class="feed-title">' + title + '</div>' +
                '<div class="feed-meta">' + escapeHtml(meta) + '</div>' +
                '</div>' +
                '<button type="button" class="' + starClass + '" data-id="' + escapeAttr(i.id) + '" title="' + (saved ? 'Unsave' : 'Save (local)') + '" aria-label="' + (saved ? 'Unsave' : 'Save') + '">&#9733;</button>' +
                '</li>';
        }).join('');
        el.querySelectorAll('.news-feed-row').forEach(function (row) {
            row.addEventListener('click', function (e) {
                if (e.target.classList.contains('feed-star')) return;
                e.preventDefault();
                var id = row.getAttribute('data-id');
                var item = feedItems.filter(function (i) { return i.id === id; })[0];
                if (item) openDrawer(item);
            });
        });
        el.querySelectorAll('.feed-star').forEach(function (btn) {
            btn.addEventListener('click', function (e) {
                e.preventDefault();
                e.stopPropagation();
                var id = btn.getAttribute('data-id');
                toggleSaved(id);
                btn.classList.toggle('saved', isSaved(id));
                btn.setAttribute('title', isSaved(id) ? 'Unsave' : 'Save (local)');
                if (savedFilterOn) renderAll();
            });
        });
    }

    function countActiveFilters() {
        var n = 0;
        if (selectedCategory) n += 1;
        if (selectedRegionGroup) n += 1;
        if (searchTerm.trim()) n += 1;
        if (savedFilterOn) n += 1;
        if (selectedTimeWindow !== '7d') n += 1;
        return n;
    }

    function updateResetViewButton() {
        var badge = document.getElementById('resetViewBadge');
        if (!badge) return;
        var n = countActiveFilters();
        if (n > 0) {
            badge.textContent = n;
            badge.style.display = 'inline-flex';
        } else {
            badge.style.display = 'none';
        }
    }

    function renderAll() {
        updateResetViewButton();
        var list = applySavedFilter(feedItems);
        if (savedFilterOn && list.length === 0) {
            setViewState(false, false, true, 'No saved alerts. Save items with the star to see them here (saved locally until you sign in).');
            return;
        }
        renderHeroGrid(feedItems);
        renderTopRead(railData.topRead);
        renderLiveFeed(feedItems);
    }

    function loadRail() {
        fetch(API.rail)
            .then(function (res) { return res.json(); })
            .then(function (data) {
                railData = { topRead: data.topRead || [], liveFeed: data.liveFeed || [] };
                renderTopRead(railData.topRead);
            })
            .catch(function () {});
    }

    function loadFeed() {
        var feedLoading = document.getElementById('feedLoading');
        var newsContent = document.getElementById('newsContent');
        var feedEmpty = document.getElementById('feedEmpty');

        setViewState(true, false, false);

        var params = new URLSearchParams();
        params.set('timeWindow', selectedTimeWindow);
        params.set('limit', String(DEFAULT_LIMIT));
        if (selectedCategory) params.set('category', selectedCategory);
        if (selectedRegionGroup) params.set('regionGroup', selectedRegionGroup);
        if (searchTerm.trim()) params.set('search', searchTerm.trim());

        fetch(API.list + '?' + params.toString())
            .then(function (res) {
                if (!res.ok) {
                    return res.json().then(function (body) {
                        var msg = (body && body.error) ? body.error : res.statusText || 'Load failed';
                        throw new Error(msg);
                    }).catch(function (parseErr) {
                        var m = parseErr && parseErr.message;
                        if (m === 'Airtable not configured' || m === 'Failed to load market alerts' || m === 'Load failed' || m === res.statusText) throw parseErr;
                        throw new Error(res.status + ' ' + (res.statusText || 'Request failed'));
                    });
                }
                return res.json();
            })
            .then(function (data) {
                try {
                    var raw = Array.isArray(data && data.items) ? data.items : [];
                    feedItems = raw.map(function (r) { return normalizeItem(r || {}); }).sort(function (a, b) { return (b.sortDate || 0) - (a.sortDate || 0); });

                    setViewState(false, false, false);
                    if (feedItems.length === 0) {
                        setViewState(false, false, true, 'No alerts found. Try widening time window or changing filters.');
                        return;
                    }
                    setViewState(false, true, false);
                    updateResetViewButton();
                    renderAll();
                } catch (e) {
                    setViewState(false, false, true, (e && e.message) ? e.message + ' Please try Refresh.' : 'Unable to load alerts. Please try Refresh.');
                }
            })
            .catch(function (err) {
                var message = (err && err.message) ? err.message : 'Unable to load alerts.';
                if (message.indexOf('Failed to fetch') !== -1 || message.indexOf('NetworkError') !== -1) {
                    message = 'Network error. Is the server running? Please try Refresh.';
                } else if (message !== 'Airtable not configured' && message !== 'Failed to load market alerts') {
                    message = message + ' Please try Refresh.';
                }
                setViewState(false, false, true, message);
            });
    }

    function setup() {
        renderCategoryNav();
        renderRegionNav();

        var overlay = document.getElementById('drawerOverlay');
        var drawer = document.getElementById('drawer');
        var drawerClose = document.getElementById('drawerClose');
        if (overlay) overlay.addEventListener('click', closeDrawer);
        if (drawerClose) drawerClose.addEventListener('click', closeDrawer);
        document.addEventListener('keydown', function (e) {
            if (e.key === 'Escape') closeDrawer();
        });

        var refreshBtn = document.getElementById('refreshBtn');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', function () {
                loadFeed();
                loadRail();
            });
        }

        var resetViewBtn = document.getElementById('resetViewBtn');
        if (resetViewBtn) {
            resetViewBtn.addEventListener('click', function () {
                selectedCategory = null;
                selectedRegionGroup = null;
                searchTerm = '';
                savedFilterOn = false;
                selectedTimeWindow = '7d';
                renderCategoryNav();
                renderRegionNav();
                var searchInputEl = document.getElementById('searchInput');
                if (searchInputEl) searchInputEl.value = '';
                var savedToggleEl = document.getElementById('savedToggle');
                if (savedToggleEl) savedToggleEl.classList.remove('active');
                document.querySelectorAll('.btn-time').forEach(function (b) {
                    b.classList.toggle('active', (b.getAttribute('data-window') || '7d') === '7d');
                });
                loadFeed();
                loadRail();
                updateResetViewButton();
                showToast('Filters cleared');
            });
        }

        document.querySelectorAll('.btn-time').forEach(function (btn) {
            btn.addEventListener('click', function () {
                document.querySelectorAll('.btn-time').forEach(function (b) { b.classList.remove('active'); });
                btn.classList.add('active');
                selectedTimeWindow = btn.getAttribute('data-window') || '7d';
                updateResetViewButton();
                loadFeed();
            });
        });

        var searchInput = document.getElementById('searchInput');
        if (searchInput) {
            searchInput.addEventListener('input', function () {
                clearTimeout(searchDebounceTimer);
                searchDebounceTimer = setTimeout(function () {
                    searchTerm = searchInput.value.trim();
                    loadFeed();
                }, SEARCH_DEBOUNCE_MS);
            });
        }

        var savedToggle = document.getElementById('savedToggle');
        if (savedToggle) {
            savedToggle.addEventListener('click', function () {
                savedFilterOn = !savedFilterOn;
                savedToggle.classList.toggle('active', savedFilterOn);
                updateResetViewButton();
                renderAll();
            });
        }

        updateResetViewButton();
        loadFeed();
        loadRail();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', setup);
    } else {
        setup();
    }
})();
