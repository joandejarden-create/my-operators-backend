(function () {
    'use strict';

    var frameContainer = document.getElementById('frameContainer');
    var shellNav = document.getElementById('shellNav');
    var shellSearchInput = document.getElementById('shellSearchInput');
    var sidebar = document.getElementById('sidebar');
    var sidebarToggle = document.getElementById('sidebarToggle');
    var mobileNavToggle = document.getElementById('mobileNavToggle');
    var shellPageTitle = document.getElementById('shellPageTitle');
    var shellPageSubtitle = document.getElementById('shellPageSubtitle');
    var devWorkspaceWrap = document.getElementById('devWorkspaceWrap');
    var devWorkspaceSelect = document.getElementById('devWorkspaceSelect');
    var DEV_WORKSPACE_STORAGE_KEY = 'DEALALITY_DEV_WORKSPACE';
    var ALLOWED_ROLES = ['owner', 'brand', 'operator', 'admin'];
    var ALLOWED_WORKSPACES = ['owner', 'brand', 'operator', 'admin', 'all'];
    var currentBaseRole = 'owner';
    var devWorkspace = '';
    var isDevMode = false;

    var ROUTES = {
        '/home': { file: '/app/home.html', title: 'Home' },
        '/my-deals': { file: '/my-deals.html', title: 'My Deals' },
        '/new-deal-setup': { file: '/new-deal-setup.html', title: 'Add New Deal' },
        '/deal-setup': { file: '/deal-setup.html', title: 'Deal Setup' },
        '/deal-summary': { file: '/deal-summary.html', title: 'Deal Summary' },
        '/deal-compare': { file: '/deal-compare.html', title: 'Deal Compare' },
        '/market-alerts': { file: '/market-alerts.html', title: 'Market Alerts' },
        '/opportunity-radar': { file: '/deal-capture-radar-with-ranked-list.html', title: 'The Radar' },
        // Retired route kept as archived source file only (public/management-operator-radar.html).
        // Intentionally removed from active app shell routing/navigation.
        '/loi-database-dashboard': { file: '/loi-database-dashboard.html', title: 'LOI Market Hub' },
        // Legacy Brand Directory route retained as alias target for old bookmarks.
        // It now loads the active Brand Explorer (Mock Up) experience.
        '/brand-explorer': { file: '/brand-library-atelier-north.html', title: 'Brand Explorer (Mock Up)' },
        '/operator-explorer': { file: '/operator-explorer.html', title: 'Operator Explorer' },
        '/operator-explorer-mockup': { file: '/operator-explorer-gold-mock.html', title: 'Operator Explorer Mockup' },
        '/deal-room-owner': { file: '/deal-room-owner.html', title: 'Owner Deal Room' },
        '/deal-room-brand': { file: '/deal-room-brand.html', title: 'Brand Deal Room' },
        '/brand-deal-request': { file: '/brand-deal-request.html', title: 'Brand Deal Request' },
        '/outreach': { file: '/outreach-plans.html', title: 'Outreach Plans' },
        '/activity-log': { file: '/outreach-deal-activity-log.html', title: 'Activity Log' },
        '/outreach/inbox': { file: '/outreach-inbox.html', title: 'Outreach Inbox' },
        '/outreach/sequences': { file: '/outreach-sequences.html', title: 'Outreach Sequences' },
        '/outreach/analytics': { file: '/outreach-analytics.html', title: 'Outreach Analytics' },
        '/outreach/deal-activity-log': { file: '/outreach-deal-activity-log.html', title: 'Outreach Deal Activity Log' },
        '/outreach/templates': { file: '/outreach-template-manager.html', title: 'Outreach Templates' },
        '/brand-library': { file: '/brand-library.html', title: 'Brand Library' },
        '/brand-library-atelier': { file: '/brand-library-atelier-north.html', title: 'Brand Explorer (Mock Up)' },
        '/financial-term-library': { file: '/financial-term-library.html', title: 'Financial Term Library' },
        '/clause-library': { file: '/clause-library.html', title: 'Clause Library' },
        '/franchise-fee-estimator': { file: '/franchise-fee-estimator.html', title: 'Franchise Fee Estimator' },
        '/partner-directory': { file: '/partner-directory.html', title: 'Partner Directory' },
        '/my-brands': { file: '/all-brands-dashboard.html', title: 'My Brands' },
        '/my-operators': { file: '/my-third-party-operators.html', title: 'My Operators' },
        '/brand-development-dashboard': { file: '/brand-development-dashboard.html', title: 'My Brand Deals' },
        '/third-party-operator-intake': { file: '/third-party-operator-intake.html', title: 'Third-Party Operator Intake' },
        '/brand-setup': { file: '/brand-setup.html', title: 'Brand Setup' },
        '/reports': { file: '/reports-dashboard.html', title: 'Reports' },
        '/route-map': { placeholder: true, title: 'Route Map' },
        '/support': { placeholder: true, title: 'Support' },
        '/company-settings': { file: '/company-settings.html', title: 'Company Settings' },
        '/profile-settings': { file: '/profile-settings.html', title: 'Profile Settings' },
        '/user-management': { file: '/user-management.html', title: 'User Management' }
    };

    var NAV_ICONS = {
        home: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 10.5L12 3l9 7.5"/><path d="M5 9.5V20h14V9.5"/><path d="M9.5 20v-6h5v6"/></svg>',
        deals: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="3" width="8" height="8"/><rect x="13" y="3" width="8" height="8"/><rect x="3" y="13" width="8" height="8"/><rect x="13" y="13" width="8" height="8"/></svg>',
        market: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="5"/><circle cx="12" cy="12" r="1.5" fill="currentColor" stroke="none"/></svg>',
        room: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="7" width="18" height="12" rx="2"/><path d="M8 7V5h8v2"/><path d="M8 13h8"/><path d="M8 17h5"/></svg>',
        outreach: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="4" r="2"/><circle cx="4" cy="12" r="2"/><circle cx="20" cy="12" r="2"/><circle cx="12" cy="20" r="2"/><path d="M12 6v12"/><path d="M6 12h12"/></svg>',
        toolbox: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 6h5v14H4z"/><path d="M10 4h5v16h-5z"/><path d="M16 7h4v13h-4z"/></svg>',
        partner: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="2.5" width="18" height="19" rx="2"/><circle cx="12" cy="8" r="2.8"/><path d="M7.5 16c1.5-2.2 7.5-2.2 9 0"/><path d="M6.5 4.5h3"/><path d="M14.5 4.5h3"/></svg>',
        platform: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3l9 4.5-9 4.5-9-4.5z"/><path d="M3 12l9 4.5 9-4.5"/><path d="M3 16.5 12 21l9-4.5"/></svg>',
        reports: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 20h18"/><rect x="5" y="11" width="3" height="7"/><rect x="10.5" y="8" width="3" height="10"/><rect x="16" y="5" width="3" height="13"/></svg>',
        support: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M21 11.5a8.5 8.5 0 1 1-4.2-7.35"/><path d="M8.8 9.2a3.2 3.2 0 1 1 5 2.6c-.8.6-1.4 1.1-1.4 2.2"/><circle cx="12.1" cy="17.4" r="0.7" fill="currentColor" stroke="none"/></svg>',
        routeMap: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 5h5v5H5z"/><path d="M14 5h5v5h-5z"/><path d="M5 14h5v5H5z"/><path d="M10 7.5h4"/><path d="M7.5 10v4"/></svg>',
        settings: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="3.2"/><path d="M19.4 15a1 1 0 0 0 .2 1.1l.1.1a2 2 0 0 1-2.8 2.8l-.1-.1a1 1 0 0 0-1.1-.2 1 1 0 0 0-.6.9V20a2 2 0 1 1-4 0v-.2a1 1 0 0 0-.6-.9 1 1 0 0 0-1.1.2l-.1.1a2 2 0 0 1-2.8-2.8l.1-.1a1 1 0 0 0 .2-1.1 1 1 0 0 0-.9-.6H4a2 2 0 1 1 0-4h.2a1 1 0 0 0 .9-.6 1 1 0 0 0-.2-1.1l-.1-.1a2 2 0 0 1 2.8-2.8l.1.1a1 1 0 0 0 1.1.2 1 1 0 0 0 .6-.9V4a2 2 0 1 1 4 0v.2a1 1 0 0 0 .6.9 1 1 0 0 0 1.1-.2l.1-.1a2 2 0 0 1 2.8 2.8l-.1.1a1 1 0 0 0-.2 1.1 1 1 0 0 0 .9.6H20a2 2 0 1 1 0 4h-.2a1 1 0 0 0-.9.6z"/></svg>'
    };

    var NAV_SECTIONS = [
        {
            name: 'top',
            entries: [
                { type: 'item', label: 'Home', icon: NAV_ICONS.home, route: '/home', roles: ['owner', 'brand', 'operator', 'admin'] }
            ]
        },
        {
            name: 'primary',
            entries: [
                {
                    type: 'group',
                    label: 'Deals',
                    icon: NAV_ICONS.deals,
                    children: [
                        { label: 'Add New Deal', route: '/new-deal-setup', roles: ['owner', 'admin'] },
                        { label: 'My Deals', route: '/my-deals', roles: ['owner', 'operator', 'admin'] },
                        { label: 'My Brand Deals', route: '/brand-development-dashboard', roles: ['brand', 'admin'] },
                        { label: 'My Brands', route: '/my-brands', roles: ['owner', 'brand', 'admin'] },
                        { label: 'My Operators', route: '/my-operators', roles: ['owner', 'operator', 'admin'] }
                    ]
                },
                {
                    type: 'group',
                    label: 'Market Intelligence',
                    icon: NAV_ICONS.market,
                    children: [
                        { label: 'The Radar', route: '/opportunity-radar', roles: ['owner', 'brand', 'operator', 'admin'] },
                        { label: 'Market Alerts', route: '/market-alerts', roles: ['owner', 'brand', 'operator', 'admin'] },
                        { label: 'LOI Market Hub', route: '/loi-database-dashboard', roles: ['owner', 'brand', 'admin'] }
                    ]
                },
                {
                    type: 'group',
                    label: 'Deal Room',
                    icon: NAV_ICONS.room,
                    children: [
                        { label: 'Owner Deal Room', route: '/deal-room-owner', roles: ['owner', 'admin'] },
                        { label: 'Brand Deal Room', route: '/deal-room-brand', roles: ['brand', 'admin'] },
                        { label: 'Brand Deal Request', route: '/brand-deal-request', roles: ['brand', 'admin'] }
                    ]
                },
                {
                    type: 'group',
                    label: 'Outreach Hub',
                    icon: NAV_ICONS.outreach,
                    children: [
                        { label: 'Outreach Plans', route: '/outreach', roles: ['owner', 'brand', 'operator', 'admin'] },
                        { label: 'Templates', route: '/outreach/templates', roles: ['owner', 'brand', 'operator', 'admin'] },
                        { label: 'Inbox', route: '/outreach/inbox', roles: ['owner', 'brand', 'operator', 'admin'] },
                        { label: 'Sequences', route: '/outreach/sequences', roles: ['owner', 'brand', 'operator', 'admin'] },
                        { label: 'Analytics', route: '/outreach/analytics', roles: ['owner', 'brand', 'operator', 'admin'] },
                        { label: 'Activity Log', route: '/activity-log', roles: ['owner', 'brand', 'operator', 'admin'] }
                    ]
                }
            ]
        },
        {
            name: 'secondary',
            entries: [
                {
                    type: 'group',
                    label: 'Deal Toolbox',
                    icon: NAV_ICONS.toolbox,
                    children: [
                        { label: 'Brand Explorer', route: '/brand-library-atelier', roles: ['owner', 'brand', 'admin'] },
                        { label: 'Operator Explorer', route: '/operator-explorer', roles: ['owner', 'brand', 'operator', 'admin'] },
                        { label: 'Brand Library', route: '/brand-library', roles: ['owner', 'brand', 'admin'] },
                        { label: 'Financial Term Library', route: '/financial-term-library', roles: ['owner', 'brand', 'admin'] },
                        { label: 'Clause Library', route: '/clause-library', roles: ['owner', 'brand', 'admin'] },
                        { label: 'Franchise Fee Estimator', route: '/franchise-fee-estimator', roles: ['owner', 'brand', 'admin'] }
                    ]
                },
                {
                    type: 'item',
                    label: 'Partner Directory',
                    icon: NAV_ICONS.partner,
                    route: '/partner-directory',
                    roles: ['owner', 'operator', 'admin']
                }
            ]
        },
        {
            name: 'tertiary',
            entries: [
                { type: 'item', label: 'Reports', icon: NAV_ICONS.reports, route: '/reports', roles: ['admin'] },
                { type: 'item', label: 'Support', icon: NAV_ICONS.support, route: '/support', roles: ['owner', 'brand', 'operator', 'admin'] },
                { type: 'item', label: 'Route Map', icon: NAV_ICONS.routeMap, route: '/route-map', roles: ['admin'], devOnly: true },
                {
                    type: 'group',
                    label: 'Settings',
                    icon: NAV_ICONS.settings,
                    children: [
                        { label: 'Company Settings', route: '/company-settings', roles: ['owner', 'brand', 'admin'] },
                        { label: 'Profile Settings', route: '/profile-settings', roles: ['owner', 'brand', 'operator', 'admin'] },
                        { label: 'User Management', route: '/user-management', roles: ['admin'] },
                        { label: 'Third-Party Operator Intake', route: '/third-party-operator-intake', roles: ['operator', 'admin'] },
                        { label: 'Brand Setup', route: '/brand-setup', roles: ['admin'] }
                    ]
                }
            ]
        }
    ];

    var currentRole = 'owner';
    var currentRoute = '/home';
    var openGroups = {};
    var searchText = '';

    var ROUTE_ALIASES = {
        '/': '/home',
        '/app/home': '/home',
        '/app/home.html': '/home',
        '/my-deals.html': '/my-deals',
        '/new-deal-setup.html': '/new-deal-setup',
        '/deal-setup.html': '/deal-setup',
        '/deal-summary.html': '/deal-summary',
        '/deal-compare.html': '/deal-compare',
        '/market-alerts.html': '/market-alerts',
        '/deal-capture-radar-with-ranked-list.html': '/opportunity-radar',
        '/operator-intelligence-radar-with-list.html': '/opportunity-radar',
        '/loi-database-dashboard.html': '/loi-database-dashboard',
        '/brand-explorer.html': '/brand-library-atelier',
        '/operator-explorer.html': '/operator-explorer',
        '/operator-explorer-gold-mock.html': '/operator-explorer-mockup',
        '/deal-room-owner.html': '/deal-room-owner',
        '/deal-room-brand.html': '/deal-room-brand',
        '/brand-deal-request.html': '/brand-deal-request',
        '/outreach-plans.html': '/outreach',
        '/activity-log.html': '/activity-log',
        '/outreach-inbox.html': '/outreach/inbox',
        '/outreach-sequences.html': '/outreach/sequences',
        '/outreach-analytics.html': '/outreach/analytics',
        '/outreach-deal-activity-log.html': '/outreach/deal-activity-log',
        '/outreach-template-manager.html': '/outreach/templates',
        '/brand-library.html': '/brand-library',
        '/brand-library-atelier-north.html': '/brand-library-atelier',
        '/financial-term-library.html': '/financial-term-library',
        '/clause-library.html': '/clause-library',
        '/franchise-fee-estimator.html': '/franchise-fee-estimator',
        '/fit-analyzer': '/my-deals',
        '/deal-brand-fit-analyzer.html': '/my-deals',
        '/partner-directory.html': '/partner-directory',
        '/my-brands.html': '/my-brands',
        '/all-brands-dashboard.html': '/my-brands',
        '/webflow-my-brands.html': '/my-brands',
        '/brand-workspace': '/brand-development-dashboard',
        '/webflow-brand-dashboard.html': '/brand-development-dashboard',
        '/brand-development-dashboard.html': '/brand-development-dashboard',
        '/my-operators.html': '/my-operators',
        '/my-third-party-operators': '/my-operators',
        '/my-third-party-operators.html': '/my-operators',
        '/third-party-operator-intake.html': '/third-party-operator-intake',
        '/brand-setup.html': '/brand-setup',
        '/reports-dashboard.html': '/reports',
        '/route-map.html': '/route-map',
        '/company-settings.html': '/company-settings',
        '/profile-settings.html': '/profile-settings',
        '/user-management.html': '/user-management'
    };

    function canSee(role, roles, devOnly) {
        if (role === 'all') return true;
        if (devOnly) {
            if (isDevMode || role === 'admin') return true;
            return false;
        }
        return !roles || roles.indexOf(role) !== -1;
    }

    function getBaseRole() {
        if (/localhost|127\.0\.0\.1/i.test(window.location.hostname)) return 'owner';
        var hint = window.__DEALALITY_APP_ROLE;
        if (typeof hint === 'string') {
            var normalized = hint.toLowerCase();
            if (ALLOWED_ROLES.indexOf(normalized) !== -1) {
                return normalized;
            }
        }
        return 'owner';
    }

    function computeDevMode() {
        return /localhost|127\.0\.0\.1/i.test(window.location.hostname) || /(?:\?|&)devNav=1(?:&|$)/.test(window.location.search);
    }

    function getStoredDevWorkspace() {
        try {
            var stored = (window.localStorage && window.localStorage.getItem(DEV_WORKSPACE_STORAGE_KEY)) || '';
            var normalized = String(stored || '').toLowerCase();
            return ALLOWED_WORKSPACES.indexOf(normalized) !== -1 ? normalized : '';
        } catch (_err) {
            return '';
        }
    }

    function setStoredDevWorkspace(nextWorkspace) {
        try {
            if (!window.localStorage) return;
            if (!nextWorkspace) {
                window.localStorage.removeItem(DEV_WORKSPACE_STORAGE_KEY);
            } else {
                window.localStorage.setItem(DEV_WORKSPACE_STORAGE_KEY, nextWorkspace);
            }
        } catch (_err) {
            // Ignore storage failures in restricted environments.
        }
    }

    function getEffectiveRole() {
        if (isDevMode && devWorkspace) return devWorkspace;
        return currentBaseRole;
    }

    function normalizeRoute(route) {
        var value = route || '/home';
        if (value.indexOf('#') === 0) value = value.slice(1);
        if (value.charAt(0) !== '/') value = '/' + value;
        value = value.split('?')[0];
        if (value.length > 1 && value.charAt(value.length - 1) === '/') value = value.slice(0, -1);

        return ROUTE_ALIASES[value] || value;
    }

    function getDefaultRoute(role) {
        return role === 'brand' ? '/brand-development-dashboard' : '/home';
    }

    function getPath(role) {
        var hash = window.location.hash.slice(1);
        return hash ? normalizeRoute(hash) : getDefaultRoute(role);
    }

    function routeToEmbedUrl(route, role) {
        if (route === '/opportunity-radar' && role === 'operator') {
            return '/operator-intelligence-radar-with-list.html?embed=1';
        }
        var mapped = ROUTES[route];
        if (!mapped || !mapped.file) return '/app/home.html?embed=1';
        return mapped.file + (mapped.file.indexOf('?') === -1 ? '?embed=1' : '&embed=1');
    }

    function getFrameForPath(path) {
        return frameContainer ? frameContainer.querySelector('.app-frame[data-path="' + path + '"]') : null;
    }

    function applyEmbeddedPageOverrides(frame, route, role) {
        if (!frame || !frame.contentDocument) return;
        try {
            var doc = frame.contentDocument;
            var styleId = 'dealality-embed-overrides';
            var styleEl = doc.getElementById(styleId);
            if (!styleEl) {
                styleEl = doc.createElement('style');
                styleEl.id = styleId;
                (doc.head || doc.documentElement).appendChild(styleEl);
            }

            var css = ''
                + '.deal-capture-logo{display:none !important;}'
                // Header alignment/typography baseline across embedded platform pages.
                + '.intake-header,.dashboard-header,.page-header,.brand-review__header,.explorer-page-header,.news-page-header{'
                + 'margin:0 0 24px 0 !important;padding-left:0 !important;padding-right:0 !important;}'
                + '.intake-title-container,.dashboard-header-left,.brand-review-title-container,.news-title-container{'
                + 'gap:10px !important;margin-bottom:0 !important;}'
                + '.intake-header h1,.dashboard-header h1,.page-header h1,.brand-review__header h1,.explorer-page-header__title,.mapping-title,.news-page-header h1{'
                + 'font-family:Inter,"Segoe UI",Arial,sans-serif !important;font-style:normal !important;'
                + 'font-size:2rem !important;line-height:1.2 !important;font-weight:700 !important;'
                + 'letter-spacing:0 !important;text-transform:none !important;'
                + 'margin:0 !important;color:#ffffff !important;}'
                + '.intake-header p,.dashboard-header p,.page-header p,.brand-review__header p,.explorer-page-header__subtitle,.mapping-subtitle,.news-page-header p{'
                + 'font-family:Inter,"Segoe UI",Arial,sans-serif !important;font-style:normal !important;'
                + 'font-size:0.75rem !important;line-height:1.35 !important;font-weight:400 !important;'
                + 'letter-spacing:0 !important;text-transform:none !important;'
                + 'margin-top:8px !important;max-width:760px !important;color:#ffffff !important;}'
                + '.intake-header p *, .dashboard-header p *, .page-header p *, .brand-review__header p *, .explorer-page-header__subtitle *, .mapping-subtitle *, .news-page-header p * {'
                + 'font-family:inherit !important;font-size:inherit !important;font-style:inherit !important;font-weight:inherit !important;line-height:inherit !important;letter-spacing:inherit !important;color:inherit !important;}';
            // LOI tabs should be visible only to admin + dev "all".
            if (route === '/loi-database-dashboard' && role !== 'admin' && role !== 'all') {
                css += '#databaseTab,#benchmarkTab,#databasePanel,#benchmarkPanel{display:none !important;}';
            }
            styleEl.textContent = css;

            if (route === '/loi-database-dashboard' && role !== 'admin' && role !== 'all') {
                var overviewTab = doc.getElementById('overviewTab');
                var overviewPanel = doc.getElementById('overviewPanel');
                if (overviewTab) overviewTab.classList.add('active');
                if (overviewPanel) overviewPanel.classList.add('active');
            }
        } catch (_err) {
            // Ignore pages that are unavailable during navigation transitions.
        }
    }

    function passesSearch(label, childLabels, query) {
        if (!query) return true;
        var source = (label + ' ' + (childLabels || []).join(' ')).toLowerCase();
        return source.indexOf(query) !== -1;
    }

    function getAliasLookup() {
        var lookup = {};
        Object.keys(ROUTE_ALIASES).forEach(function (alias) {
            var normalizedRoute = ROUTE_ALIASES[alias];
            if (!lookup[normalizedRoute]) lookup[normalizedRoute] = [];
            lookup[normalizedRoute].push(alias);
        });
        return lookup;
    }

    function escapeHtml(text) {
        return String(text == null ? '' : text)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function getRouteMapRows(activeRole) {
        var rows = [];
        NAV_SECTIONS.forEach(function (section) {
            section.entries.forEach(function (entry) {
                if (entry.type === 'item') {
                    rows.push({
                        route: entry.route,
                        label: entry.label,
                        group: section.name + ' / (item)',
                        roles: entry.roles || [],
                        devOnly: !!entry.devOnly
                    });
                    return;
                }
                (entry.children || []).forEach(function (child) {
                    rows.push({
                        route: child.route,
                        label: child.label,
                        group: section.name + ' / ' + entry.label,
                        roles: child.roles || [],
                        devOnly: !!child.devOnly
                    });
                });
            });
        });

        Object.keys(ROUTES).forEach(function (route) {
            if (!rows.some(function (row) { return row.route === route; })) {
                rows.push({
                    route: route,
                    label: ROUTES[route].title || route,
                    group: '(not in nav)',
                    roles: [],
                    devOnly: false
                });
            }
        });

        var aliasLookup = getAliasLookup();
        return rows.map(function (row) {
            var routeMeta = ROUTES[row.route] || {};
            return {
                route: row.route,
                title: routeMeta.title || row.label || row.route,
                file: routeMeta.file || 'placeholder',
                group: row.group,
                roles: row.roles.length ? row.roles.join(', ') : 'all',
                visible: canSee(activeRole, row.roles, row.devOnly) ? 'yes' : 'no',
                placeholder: routeMeta.placeholder ? 'yes' : 'no',
                aliases: (aliasLookup[row.route] || []).join(', ') || '-',
                mapped: routeMeta.file ? 'mapped' : (routeMeta.placeholder ? 'placeholder' : 'mapped')
            };
        }).sort(function (a, b) { return a.route.localeCompare(b.route); });
    }

    function renderRouteMapHtml(activeRole) {
        var rows = getRouteMapRows(activeRole);
        var workspaceLabel = activeRole === 'all' ? 'All' : (activeRole.charAt(0).toUpperCase() + activeRole.slice(1));
        var bodyRows = rows.map(function (row) {
            return '<tr>' +
                '<td>' + escapeHtml(row.route) + '</td>' +
                '<td>' + escapeHtml(row.title) + '</td>' +
                '<td>' + escapeHtml(row.file) + '</td>' +
                '<td>' + escapeHtml(row.group) + '</td>' +
                '<td>' + escapeHtml(row.roles) + '</td>' +
                '<td>' + escapeHtml(row.visible) + '</td>' +
                '<td>' + escapeHtml(row.placeholder) + '</td>' +
                '<td>' + escapeHtml(row.mapped) + '</td>' +
                '<td>' + escapeHtml(row.aliases) + '</td>' +
            '</tr>';
        }).join('');

        return '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Route Map</title>' +
            '<style>body{font-family:Inter,Segoe UI,sans-serif;background:#060d22;color:#d9e1ff;margin:0;padding:20px;}' +
            'h1{margin:0 0 8px;font-size:22px;}p{margin:0 0 16px;color:#9facd6;}table{width:100%;border-collapse:collapse;font-size:12px;}' +
            'th,td{border:1px solid #2f3e68;padding:8px;vertical-align:top;text-align:left;}th{background:#0f1d45;color:#fff;position:sticky;top:0;}' +
            'tr:nth-child(even){background:#0b1534;} code{color:#8fb3ff;}</style></head><body>' +
            '<h1>Dealality Route Map</h1>' +
            '<p>Workspace: <strong>' + escapeHtml(workspaceLabel) + '</strong> | Total routes: <strong>' + rows.length + '</strong></p>' +
            '<table><thead><tr><th>route</th><th>title/label</th><th>file</th><th>group</th><th>roles</th><th>visible now</th><th>placeholder</th><th>status</th><th>aliases</th></tr></thead>' +
            '<tbody>' + bodyRows + '</tbody></table></body></html>';
    }

    function renderNav(role, query) {
        if (!shellNav) return;
        var parts = [];
        var firstSectionRendered = false;
        query = (query || '').trim().toLowerCase();

        NAV_SECTIONS.forEach(function (section) {
            var sectionParts = [];

            section.entries.forEach(function (entry) {
                if (entry.type === 'item') {
                    if (!canSee(role, entry.roles, entry.devOnly)) return;
                    if (!passesSearch(entry.label, [], query)) return;
                    sectionParts.push(
                        '<button type="button" class="nav-item' + (currentRoute === entry.route ? ' active' : '') + '" data-href="' + entry.route + '">' +
                            '<span class="nav-icon" aria-hidden="true">' + entry.icon + '</span>' +
                            '<span class="nav-label">' + entry.label + '</span>' +
                            '<span class="nav-arrow">&gt;</span>' +
                        '</button>'
                    );
                    return;
                }

                var visibleChildren = (entry.children || []).filter(function (child) { return canSee(role, child.roles, child.devOnly); });
                if (!visibleChildren.length) return;
                if (!passesSearch(entry.label, visibleChildren.map(function (c) { return c.label; }), query)) return;

                var groupId = entry.label.toLowerCase().replace(/[^a-z0-9]+/g, '-');
                var hasActiveChild = visibleChildren.some(function (child) { return child.route === currentRoute; });
                if (hasActiveChild && !openGroups[groupId]) openGroups[groupId] = true;
                var isOpen = !!openGroups[groupId];

                var childrenHtml = visibleChildren.map(function (child) {
                    return '<button type="button" class="nav-item nav-item-child' + (child.route === currentRoute ? ' active' : '') + '" data-href="' + child.route + '">' +
                        '<span class="nav-label">' + child.label + '</span>' +
                    '</button>';
                }).join('');

                sectionParts.push(
                    '<div class="nav-group' + (isOpen ? ' open' : '') + '" data-group-id="' + groupId + '">' +
                        '<button type="button" class="nav-item nav-item-parent' + (hasActiveChild ? ' active' : '') + '" data-group-toggle="' + groupId + '">' +
                            '<span class="nav-icon" aria-hidden="true">' + entry.icon + '</span>' +
                            '<span class="nav-label">' + entry.label + '</span>' +
                            '<span class="nav-arrow nav-arrow-dropdown">▾</span>' +
                        '</button>' +
                        '<div class="nav-dropdown">' + childrenHtml + '</div>' +
                    '</div>'
                );
            });

            if (!sectionParts.length) return;
            if (firstSectionRendered) parts.push('<div class="nav-section-divider"></div>');
            parts.push(sectionParts.join(''));
            firstSectionRendered = true;
        });

        shellNav.innerHTML = parts.join('');
    }

    function updateShellHeader(route, role) {
        if (shellPageTitle) shellPageTitle.textContent = ROUTES[route] ? ROUTES[route].title : 'Dealality App';
        if (shellPageSubtitle) {
            // Keep header cleaner in dev mode; workspace context is shown on the Dev Workspace control.
            if (isDevMode) {
                shellPageSubtitle.textContent = '';
                shellPageSubtitle.style.display = 'none';
            } else {
                shellPageSubtitle.style.display = '';
                shellPageSubtitle.textContent = role.charAt(0).toUpperCase() + role.slice(1) + ' workspace';
            }
        }
    }

    function showFrame(route) {
        if (!frameContainer) return;
        frameContainer.querySelectorAll('.app-frame').forEach(function (frame) {
            frame.classList.toggle('active', frame.getAttribute('data-path') === route);
        });
    }

    function ensurePlaceholder(route) {
        var frame = getFrameForPath(route);
        if (frame) return frame;
        frame = document.createElement('iframe');
        frame.className = 'app-frame';
        frame.setAttribute('data-path', route);
        frame.title = ROUTES[route] ? ROUTES[route].title : 'Placeholder';
        if (route === '/route-map') {
            frame.srcdoc = renderRouteMapHtml(currentRole);
        } else {
            frame.srcdoc = '<!DOCTYPE html><html><head><meta charset="utf-8"><style>body{font-family:Inter,Segoe UI,sans-serif;background:#080f25;color:#fff;padding:40px;}h1{font-size:24px;margin:0 0 12px;}p{color:#aeb9e1;margin:0;}</style></head><body><h1>Coming soon</h1><p>This route is reserved in the app shell.</p></body></html>';
        }
        frameContainer.appendChild(frame);
        return frame;
    }

    function navigate(route, role, updateHash) {
        var normalized = normalizeRoute(route);
        var target = ROUTES[normalized] ? normalized : getDefaultRoute(role);
        currentRoute = target;

        if (updateHash !== false && window.location.hash !== '#' + target) {
            window.location.hash = target;
        }

        var frame = getFrameForPath(target);
        if (!frame) {
            if (ROUTES[target].placeholder) {
                frame = ensurePlaceholder(target);
            } else {
                frame = document.createElement('iframe');
                frame.className = 'app-frame';
                frame.setAttribute('data-path', target);
                frame.title = ROUTES[target].title || 'Page content';
                frame.src = routeToEmbedUrl(target, role);
                frame.addEventListener('load', function () {
                    applyEmbeddedPageOverrides(frame, target, role);
                });
                frameContainer.appendChild(frame);
            }
        } else if (target === '/route-map') {
            frame.srcdoc = renderRouteMapHtml(role);
        } else {
            applyEmbeddedPageOverrides(frame, target, role);
        }

        showFrame(target);
        updateShellHeader(target, role);
        renderNav(role, searchText);

        if (sidebar && sidebar.classList.contains('mobile-open')) {
            sidebar.classList.remove('mobile-open');
        }
    }

    function initNavEvents() {
        if (shellNav) {
            shellNav.addEventListener('click', function (e) {
                var toggleBtn = e.target.closest('[data-group-toggle]');
                if (toggleBtn) {
                    var groupId = toggleBtn.getAttribute('data-group-toggle');
                    openGroups[groupId] = !openGroups[groupId];
                    renderNav(currentRole, searchText);
                    return;
                }

                var linkBtn = e.target.closest('.nav-item[data-href]');
                if (linkBtn) {
                    e.preventDefault();
                    navigate(linkBtn.getAttribute('data-href'), currentRole, true);
                }
            });
        }

        if (shellSearchInput) {
            shellSearchInput.addEventListener('input', function () {
                searchText = shellSearchInput.value || '';
                renderNav(currentRole, searchText);
            });
        }

        var accountWrap = document.getElementById('accountDropdownWrap');
        var accountTrigger = document.getElementById('accountDropdownTrigger');
        var accountMenu = document.getElementById('accountDropdownMenu');
        var accountLogout = document.getElementById('accountLogout');

        if (accountTrigger && accountMenu && accountWrap) {
            accountTrigger.addEventListener('click', function (e) {
                e.preventDefault();
                e.stopPropagation();
                accountWrap.classList.toggle('open');
                var isOpen = accountWrap.classList.contains('open');
                accountTrigger.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
                accountMenu.setAttribute('aria-hidden', isOpen ? 'false' : 'true');
            });
        }

        if (accountMenu && accountWrap) {
            accountMenu.querySelectorAll('.account-dropdown-item[data-href]').forEach(function (el) {
                el.addEventListener('click', function (e) {
                    var href = el.getAttribute('data-href');
                    if (!href || href === '#') return;
                    e.preventDefault();
                    navigate(href, currentRole, true);
                    accountWrap.classList.remove('open');
                    if (accountTrigger) accountTrigger.setAttribute('aria-expanded', 'false');
                    if (accountMenu) accountMenu.setAttribute('aria-hidden', 'true');
                });
            });
        }

        if (accountLogout) {
            accountLogout.addEventListener('click', function (e) {
                e.preventDefault();
                navigate('/profile-settings', currentRole, true);
            });
        }

        document.addEventListener('click', function (e) {
            if (accountWrap && accountWrap.classList.contains('open') && !accountWrap.contains(e.target)) {
                accountWrap.classList.remove('open');
                if (accountTrigger) accountTrigger.setAttribute('aria-expanded', 'false');
                if (accountMenu) accountMenu.setAttribute('aria-hidden', 'true');
            }
        });
    }

    function applyWorkspace(nextWorkspace) {
        var normalized = (nextWorkspace || '').toLowerCase();
        if (ALLOWED_WORKSPACES.indexOf(normalized) === -1) normalized = currentBaseRole;
        devWorkspace = normalized;
        currentRole = getEffectiveRole();

        if (devWorkspaceSelect) devWorkspaceSelect.value = devWorkspace;
        if (devWorkspaceWrap) devWorkspaceWrap.setAttribute('data-workspace', currentRole);
        var devWorkspaceLabel = document.querySelector('.dev-workspace-label');
        if (devWorkspaceLabel) {
            devWorkspaceLabel.textContent = currentRole === 'all'
                ? 'Dev Workspace: All (dev)'
                : 'Dev Workspace: ' + currentRole.charAt(0).toUpperCase() + currentRole.slice(1);
        }
        setStoredDevWorkspace(isDevMode ? devWorkspace : '');

        if (ROUTES[currentRoute]) {
            navigate(currentRoute, currentRole, false);
        } else {
            navigate(getDefaultRoute(currentRole), currentRole, true);
        }
    }

    function initDevWorkspaceSwitcher() {
        if (!devWorkspaceWrap || !devWorkspaceSelect) return;
        if (!isDevMode) {
            devWorkspaceWrap.hidden = true;
            return;
        }
        devWorkspaceWrap.hidden = false;
        devWorkspaceSelect.value = devWorkspace;
        devWorkspaceSelect.addEventListener('change', function () {
            var selected = (devWorkspaceSelect.value || '').toLowerCase();
            applyWorkspace(selected);
        });
    }

    function initSidebarControls() {
        if (sidebarToggle && sidebar) {
            sidebarToggle.addEventListener('click', function () {
                sidebar.classList.toggle('collapsed');
            });
        }
        if (mobileNavToggle && sidebar) {
            mobileNavToggle.addEventListener('click', function () {
                sidebar.classList.toggle('mobile-open');
            });
        }
    }

    function initShellMessageListener() {
        window.addEventListener('message', function (e) {
            if (!e.data || e.data.type !== 'dealality-navigate') return;
            if (e.origin !== window.location.origin) return;
            if (typeof e.data.path !== 'string') return;
            navigate(e.data.path, currentRole, true);
        });
    }

    function init() {
        isDevMode = computeDevMode();
        currentBaseRole = getBaseRole();
        devWorkspace = isDevMode ? (getStoredDevWorkspace() || currentBaseRole) : '';
        if (!devWorkspace && currentBaseRole) devWorkspace = currentBaseRole;
        currentRole = getEffectiveRole();

        initNavEvents();
        initDevWorkspaceSwitcher();
        initSidebarControls();
        initShellMessageListener();
        navigate(getPath(currentRole), currentRole, false);
        window.addEventListener('hashchange', function () {
            navigate(getPath(currentRole), currentRole, false);
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
