(function () {
    'use strict';

    const STORAGE_KEYS = {
        userId: 'marketAlertsUserId',
        prefs: 'marketAlertsPrefs',
        readIds: 'marketAlertsReadIds'
    };

    const API = {
        user: '/api/market-alerts/user',
        read: '/api/market-alerts/read',
        categories: '/api/market-alerts/categories',
        subscribe: '/api/market-alerts/subscribe',
        news: '/api/market-alerts/news'
    };

    var activityIconMap = {
        message: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/></svg>',
        'trending-up': '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M23 6l-9.5 9.5-5-5L1 18"/><path d="M17 6h6v6"/></svg>',
        building: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M3 21h18"/><path d="M9 8h1"/><path d="M9 12h1"/><path d="M9 16h1"/><path d="M14 8h1"/><path d="M14 12h1"/><path d="M14 16h1"/><path d="M5 21V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2v16"/></svg>',
        clipboard: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M16 4h2a2 2 0 0 1 2 2v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h2"/><path d="M15 2H9a1 1 0 0 0-1 1v2a1 1 0 0 0 1 1h6a1 1 0 0 0 1-1V3a1 1 0 0 0-1-1z"/><path d="M16 13H8"/><path d="M16 17H8"/></svg>'
    };

    let categories = [];
    let alerts = [];

    function getUserId() {
        try {
            return localStorage.getItem(STORAGE_KEYS.userId) || 'demo-user';
        } catch (_) {
            return 'demo-user';
        }
    }

    function getReadIds() {
        try {
            const raw = localStorage.getItem(STORAGE_KEYS.readIds);
            if (!raw) return new Set();
            return new Set(JSON.parse(raw));
        } catch (_) {
            return new Set();
        }
    }

    function setReadIds(ids) {
        try {
            localStorage.setItem(STORAGE_KEYS.readIds, JSON.stringify([...ids]));
        } catch (_) {}
    }

    function markAsReadInStorage(alertId) {
        const ids = getReadIds();
        ids.add(alertId);
        setReadIds(ids);
    }

    function getPrefs() {
        try {
            const raw = localStorage.getItem(STORAGE_KEYS.prefs);
            return raw ? JSON.parse(raw) : { categories: [], region: 'all', propertyTypes: 'all' };
        } catch (_) {
            return { categories: [], region: 'all', propertyTypes: 'all' };
        }
    }

    function setPrefs(prefs) {
        try {
            localStorage.setItem(STORAGE_KEYS.prefs, JSON.stringify(prefs));
        } catch (_) {}
    }

    async function fetchCategories() {
        try {
            const res = await fetch(API.categories);
            const data = await res.json();
            if (data.success && data.categories) {
                categories = data.categories;
                return data.categories;
            }
        } catch (e) {
            console.warn('Failed to load categories', e);
        }
        categories = [
            { id: 'market-trends', name: 'Market Trends' },
            { id: 'deal-opportunities', name: 'Deal Opportunities' },
            { id: 'regulatory', name: 'Regulatory' },
            { id: 'news', name: 'News' }
        ];
        return categories;
    }

    function renderPrefCategories() {
        const container = document.getElementById('prefCategories');
        if (!container) return;
        container.innerHTML = categories.map(function (c) {
            return '<label><input type="checkbox" name="prefCat" value="' + c.id + '"> ' + (c.name || c.id) + '</label>';
        }).join('');
    }

    function populateFilterCategory() {
        const sel = document.getElementById('filterCategory');
        if (!sel) return;
        var opts = '<option value="all">All</option>';
        categories.forEach(function (c) {
            opts += '<option value="' + escapeAttr(c.name || c.id) + '">' + escapeHtml(c.name || c.id) + '</option>';
        });
        sel.innerHTML = opts;
    }

    function applySavedPrefs() {
        const prefs = getPrefs();
        document.querySelectorAll('#prefCategories input[name="prefCat"]').forEach(function (cb) {
            cb.checked = prefs.categories && prefs.categories.indexOf(cb.value) !== -1;
        });
        var regionEl = document.getElementById('prefRegions');
        var propEl = document.getElementById('prefPropertyTypes');
        if (regionEl && prefs.region) regionEl.value = prefs.region;
        if (propEl && prefs.propertyTypes) propEl.value = prefs.propertyTypes;
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

    function formatNewsDate(pubDate) {
        if (!pubDate) return '';
        try {
            var d = new Date(pubDate);
            if (isNaN(d.getTime())) return pubDate;
            var now = new Date();
            var diff = now - d;
            if (diff < 86400000) return 'Today ' + d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
            if (diff < 172800000) return 'Yesterday';
            return d.toLocaleDateString([], { month: 'short', day: 'numeric', year: 'numeric' });
        } catch (_) {
            return pubDate;
        }
    }

    function iconForFeedItem(item) {
        var tag = (item.tag || item.type || '').toLowerCase();
        if (tag.indexOf('news') !== -1 || item.source) return activityIconMap['trending-up'];
        if (tag.indexOf('regulatory') !== -1 || tag.indexOf('financing') !== -1) return activityIconMap.clipboard;
        if (tag.indexOf('supply') !== -1 || tag.indexOf('market') !== -1) return activityIconMap['trending-up'];
        return activityIconMap.building;
    }

    function iconClassForFeedItem(item) {
        var tag = (item.tag || item.type || '').toLowerCase();
        if (item.source) return 'dc-activity-feed__icon dc-activity-feed__icon--news';
        if (tag.indexOf('deal') !== -1) return 'dc-activity-feed__icon dc-activity-feed__icon--deal';
        if (tag.indexOf('market') !== -1 || tag.indexOf('supply') !== -1) return 'dc-activity-feed__icon dc-activity-feed__icon--market';
        return 'dc-activity-feed__icon dc-activity-feed__icon--default';
    }

    function toTitleCase(s) {
        if (s == null || s === '') return '';
        var small = ['a','an','the','and','or','but','in','on','of','to','for','with','from','by','at','as','is','it'];
        return String(s).split(' ').map(function (word, i) {
            var lower = word.toLowerCase();
            if (word.length >= 2 && word === word.toUpperCase()) return word;
            if (small.indexOf(lower) !== -1 && i !== 0) return lower;
            return lower.charAt(0).toUpperCase() + lower.slice(1);
        }).join(' ');
    }

    /** Build unified feed items from alerts + news, sort by date (newest first) */
    function buildFeedItems(alertList, newsItems) {
        var feed = [];
        var readIds = getReadIds();

        (alertList || []).forEach(function (a) {
            a.isRead = readIds.has(a.id);
            var d = (a.createdAt || a.updatedAt || a.date);
            feed.push({
                type: 'alert',
                id: a.id,
                title: a.title || 'Untitled',
                subtitle: a.description || a.region || '',
                tag: a.category || 'Alert',
                timeAgo: timeAgo(d) || (a.priority || ''),
                sortDate: d ? new Date(d).getTime() : 0,
                href: null,
                isRead: a.isRead,
                priority: a.priority,
                raw: a
            });
        });

        (newsItems || []).forEach(function (item) {
            var d = item.pubDate;
            feed.push({
                type: 'news',
                id: 'news-' + (item.link || item.title || Math.random()).toString(36).slice(0, 9),
                title: item.title || 'Untitled',
                subtitle: (item.summary || '').slice(0, 200),
                tag: item.source || 'News',
                timeAgo: formatNewsDate(d) || timeAgo(d),
                sortDate: d ? new Date(d).getTime() : 0,
                href: (item.link || '').trim(),
                raw: item
            });
        });

        feed.sort(function (a, b) {
            return (b.sortDate || 0) - (a.sortDate || 0);
        });
        return feed;
    }

    function renderFeed(feedItems) {
        var listEl = document.getElementById('marketIntelFeed');
        var bodyEl = document.getElementById('feedSectionBody');
        var emptyEl = document.getElementById('feedEmpty');
        if (!listEl) return;

        listEl.innerHTML = '';
        if (feedItems.length === 0) {
            if (bodyEl) bodyEl.style.display = 'none';
            if (emptyEl) { emptyEl.style.display = 'block'; emptyEl.querySelector('p').textContent = 'No items in the feed. Adjust filters or click Refresh.'; }
            return;
        }

        if (emptyEl) emptyEl.style.display = 'none';
        if (bodyEl) bodyEl.style.display = 'block';

        var html = feedItems.map(function (i) {
            var iconClass = iconClassForFeedItem(i);
            var iconSvg = iconForFeedItem(i);
            var href = i.href;
            var linkStart = href ? '<a href="' + escapeAttr(href) + '" target="_blank" rel="noopener" class="dc-activity-feed__link">' : '';
            var linkEnd = href ? '</a>' : '';
            var title = escapeHtml(toTitleCase(i.title));
            var subtitle = (i.subtitle ? '<div class="dc-activity-feed__deal">' + escapeHtml(String(i.subtitle).slice(0, 280)) + (i.subtitle.length > 280 ? '…' : '') + '</div>' : '');
            var tag = i.tag ? '<span class="dc-activity-feed__tag">' + escapeHtml(String(i.tag)) + '</span>' : '';
            var time = '<span class="dc-activity-feed__time">' + escapeHtml(i.timeAgo) + '</span>';
            var event = '<div class="dc-activity-feed__event">' + title + '</div>';

            var actions = '';
            if (i.type === 'alert' && i.raw && !i.isRead) {
                actions = '<div class="feed-item-actions"><button type="button" class="btn btn-primary btn-mark-read" data-alert-id="' + escapeAttr(i.id) + '">Mark as read</button></div>';
            }

            var inner = time + event + tag + subtitle + actions;
            return '<li class="dc-activity-feed__item">' +
                '<span class="dc-activity-feed__line" aria-hidden="true"></span>' +
                '<span class="' + iconClass + '" aria-hidden="true">' + iconSvg + '</span>' +
                '<div class="dc-activity-feed__content">' + linkStart + inner + linkEnd + '</div></li>';
        }).join('');

        listEl.innerHTML = html;

        listEl.querySelectorAll('.btn-mark-read').forEach(function (btn) {
            var id = btn.getAttribute('data-alert-id');
            btn.addEventListener('click', function () {
                markAlertAsRead(id);
                markAsReadInStorage(id);
                btn.closest('.dc-activity-feed__item').querySelector('.feed-item-actions').remove();
            });
        });
    }

    async function loadFeed() {
        var feedLoading = document.getElementById('feedLoading');
        var demoBanner = document.getElementById('demoBanner');
        var filterCat = document.getElementById('filterCategory');
        var filterRegion = document.getElementById('filterRegion');
        var category = filterCat ? filterCat.value : 'all';
        var region = filterRegion ? filterRegion.value : 'all';

        if (feedLoading) feedLoading.style.display = 'flex';
        if (document.getElementById('feedSectionBody')) document.getElementById('feedSectionBody').style.display = 'none';
        if (document.getElementById('feedEmpty')) document.getElementById('feedEmpty').style.display = 'none';

        var alertList = [];
        var newsItems = [];

        try {
            var res = await fetch(API.user, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    userId: getUserId(),
                    category: category,
                    region: region,
                    limit: 50
                })
            });
            var data = await res.json();
            if (data.demo && demoBanner) demoBanner.classList.add('visible');
            if (data.success && data.alerts) {
                alertList = data.alerts;
                alerts = alertList;
                var readIds = getReadIds();
                alertList.forEach(function (a) {
                    if (readIds.has(a.id)) a.isRead = true;
                });
            }
        } catch (e) {
            console.warn('Load alerts failed', e);
        }

        try {
            var newsRes = await fetch(API.news + '?limit=30');
            var newsData = await newsRes.json();
            if (newsData.success && newsData.items && newsData.items.length) {
                newsItems = newsData.items;
            }
        } catch (e) {
            console.warn('Load news failed', e);
        }

        if (feedLoading) feedLoading.style.display = 'none';

        var feedItems = buildFeedItems(alertList, newsItems);
        renderFeed(feedItems);
    }

    async function markAlertAsRead(alertId) {
        try {
            await fetch(API.read, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ userId: getUserId(), alertId: alertId })
            });
        } catch (_) {}
    }

    function savePrefs() {
        var checked = [];
        document.querySelectorAll('#prefCategories input[name="prefCat"]:checked').forEach(function (cb) {
            checked.push(cb.value);
        });
        var prefs = {
            categories: checked,
            region: document.getElementById('prefRegions').value,
            propertyTypes: document.getElementById('prefPropertyTypes').value
        };
        setPrefs(prefs);

        fetch(API.subscribe, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                userId: getUserId(),
                categories: checked,
                regions: prefs.region,
                propertyTypes: prefs.propertyTypes
            })
        }).catch(function () {});

        loadFeed();
    }

    function setup() {
        fetchCategories().then(function () {
            renderPrefCategories();
            populateFilterCategory();
            applySavedPrefs();
            loadFeed();
        });

        var saveBtn = document.getElementById('savePrefsBtn');
        var refreshAlertsBtn = document.getElementById('refreshAlertsBtn');
        var refreshNewsBtn = document.getElementById('refreshNewsBtn');
        var filterCategory = document.getElementById('filterCategory');
        var filterRegion = document.getElementById('filterRegion');

        if (saveBtn) saveBtn.addEventListener('click', savePrefs);
        if (refreshAlertsBtn) refreshAlertsBtn.addEventListener('click', loadFeed);
        if (refreshNewsBtn) refreshNewsBtn.addEventListener('click', loadFeed);
        if (filterCategory) filterCategory.addEventListener('change', loadFeed);
        if (filterRegion) filterRegion.addEventListener('change', loadFeed);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', setup);
    } else {
        setup();
    }
})();
