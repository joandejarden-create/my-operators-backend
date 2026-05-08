(function () {
    'use strict';

    var frameContainer = document.getElementById('frameContainer');
    var shellNav = document.getElementById('shellNav');
    var sidebar = document.getElementById('sidebar');
    var sidebarToggle = document.getElementById('sidebarToggle');
    var mobileNavToggle = document.getElementById('mobileNavToggle');
    var shellPageTitle = document.getElementById('shellPageTitle');
    var shellPageSubtitle = document.getElementById('shellPageSubtitle');

    var ROUTES = {
        '/my-deals': { file: '/my-deals.html', title: 'My Deals' },
        '/new-deal-setup': { file: '/new-deal-setup.html', title: 'New Deal Setup' },
        '/deal-setup': { file: '/deal-setup.html', title: 'Deal Setup' },
        '/deal-summary': { file: '/deal-summary.html', title: 'Deal Summary' },
        '/deal-compare': { file: '/deal-compare.html', title: 'Deal Compare' },
        '/partner-directory': { file: '/partner-directory.html', title: 'Partner Directory' },
        '/brand-library': { file: '/brand-library.html', title: 'Brand Library' },
        '/operator-explorer': { file: '/operator-explorer.html', title: 'Operator Explorer' },
        '/financial-term-library': { file: '/financial-term-library.html', title: 'Financial Term Library' },
        '/opportunity-radar': { file: '/deal-capture-radar-with-ranked-list.html', title: 'Opportunity Radar' },
        '/deal-room-owner': { file: '/deal-room-owner.html', title: 'Deal Room Owner' },
        '/deal-room-brand': { file: '/deal-room-brand.html', title: 'Deal Room Brand' },
        '/outreach': { file: '/outreach-plans.html', title: 'Outreach' },
        '/company-settings': { file: '/company-settings.html', title: 'Company Settings' },
        '/fit-analyzer': { file: '/deal-brand-fit-analyzer.html', title: 'Fit Analyzer' },
        '/brand-workspace': { file: '/webflow-brand-dashboard.html', title: 'Brand Workspace' },
        '/brand-development-dashboard': { file: '/brand-development-dashboard.html', title: 'Brand Development Dashboard' },
        '/brand-deal-request': { file: '/brand-deal-request.html', title: 'Brand Deal Request' },
        '/my-third-party-operators': { file: '/my-third-party-operators.html', title: 'My Third-Party Operators' },
        '/third-party-operator-intake': { file: '/third-party-operator-intake.html', title: 'Third-Party Operator Intake' },
        '/user-management': { file: '/user-management.html', title: 'User Management' },
        '/brand-setup': { file: '/brand-setup.html', title: 'Brand Setup' },
        '/reports': { file: '/reports-dashboard.html', title: 'Reports' },
        '/profile-settings': { file: '/profile-settings.html', title: 'Profile Settings' },
        '/process-management': { placeholder: true, title: 'Process Management' }
    };

    var ROLE_NAV = {
        owner: [
            { route: '/my-deals', icon: '▦', label: 'My Deals' },
            { route: '/new-deal-setup', icon: '✚', label: 'New Deal Setup' },
            { route: '/deal-setup', icon: '⚙', label: 'Deal Setup' },
            { route: '/deal-summary', icon: '▣', label: 'Deal Summary' },
            { route: '/deal-compare', icon: '▧', label: 'Deal Compare' },
            { route: '/partner-directory', icon: '👤', label: 'Partner Directory' },
            { route: '/brand-library', icon: '▥', label: 'Brand Library' },
            { route: '/operator-explorer', icon: '◉', label: 'Operator Explorer' },
            { route: '/financial-term-library', icon: '▤', label: 'Financial Terms' },
            { route: '/opportunity-radar', icon: '◎', label: 'Opportunity Radar' },
            { route: '/deal-room-owner', icon: '🏢', label: 'Deal Room Owner' },
            { route: '/outreach', icon: '✉', label: 'Outreach' },
            { route: '/company-settings', icon: '⚒', label: 'Company Settings' },
            { route: '/fit-analyzer', icon: '◍', label: 'Fit Analyzer' },
            { route: '/process-management', icon: '⏱', label: 'Process Management' }
        ],
        brand: [
            { route: '/brand-workspace', icon: '🏢', label: 'Brand Workspace' },
            { route: '/brand-development-dashboard', icon: '▣', label: 'Brand Development Dashboard' },
            { route: '/brand-deal-request', icon: '✉', label: 'Brand Deal Request' },
            { route: '/deal-compare', icon: '▧', label: 'Deal Compare' },
            { route: '/deal-room-brand', icon: '🏢', label: 'Deal Room Brand' },
            { route: '/brand-library', icon: '▥', label: 'Brand Library' },
            { route: '/financial-term-library', icon: '▤', label: 'Financial Terms' },
            { route: '/outreach', icon: '✉', label: 'Outreach' },
            { route: '/company-settings', icon: '⚒', label: 'Company Settings' },
            { route: '/process-management', icon: '⏱', label: 'Process Management' }
        ],
        operator: [
            { route: '/my-deals', icon: '▦', label: 'My Deals' },
            { route: '/operator-explorer', icon: '◉', label: 'Operator Explorer' },
            { route: '/my-third-party-operators', icon: '⌂', label: 'My Third-Party Operators' },
            { route: '/third-party-operator-intake', icon: '✚', label: 'Operator Intake' },
            { route: '/partner-directory', icon: '👤', label: 'Partner Directory' },
            { route: '/opportunity-radar', icon: '◎', label: 'Opportunity Radar' },
            { route: '/outreach', icon: '✉', label: 'Outreach' },
            { route: '/process-management', icon: '⏱', label: 'Process Management' }
        ],
        admin: [
            { route: '/my-deals', icon: '▦', label: 'My Deals' },
            { route: '/new-deal-setup', icon: '✚', label: 'New Deal Setup' },
            { route: '/deal-setup', icon: '⚙', label: 'Deal Setup' },
            { route: '/deal-summary', icon: '▣', label: 'Deal Summary' },
            { route: '/deal-compare', icon: '▧', label: 'Deal Compare' },
            { route: '/partner-directory', icon: '👤', label: 'Partner Directory' },
            { route: '/brand-library', icon: '▥', label: 'Brand Library' },
            { route: '/operator-explorer', icon: '◉', label: 'Operator Explorer' },
            { route: '/financial-term-library', icon: '▤', label: 'Financial Terms' },
            { route: '/opportunity-radar', icon: '◎', label: 'Opportunity Radar' },
            { route: '/deal-room-owner', icon: '🏢', label: 'Deal Room Owner' },
            { route: '/deal-room-brand', icon: '🏢', label: 'Deal Room Brand' },
            { route: '/brand-workspace', icon: '🏢', label: 'Brand Workspace' },
            { route: '/brand-development-dashboard', icon: '▣', label: 'Brand Development Dashboard' },
            { route: '/brand-deal-request', icon: '✉', label: 'Brand Deal Request' },
            { route: '/my-third-party-operators', icon: '⌂', label: 'My Third-Party Operators' },
            { route: '/third-party-operator-intake', icon: '✚', label: 'Operator Intake' },
            { route: '/outreach', icon: '✉', label: 'Outreach' },
            { route: '/user-management', icon: '👥', label: 'User Management' },
            { route: '/company-settings', icon: '⚒', label: 'Company Settings' },
            { route: '/brand-setup', icon: '▥', label: 'Brand Setup' },
            { route: '/reports', icon: '▁', label: 'Reports' },
            { route: '/fit-analyzer', icon: '◍', label: 'Fit Analyzer' },
            { route: '/process-management', icon: '⏱', label: 'Process Management' }
        ]
    };

    function getRole() {
        if (/localhost|127\.0\.0\.1/i.test(window.location.hostname)) return 'owner';
        var roleHint = window.__DEALALITY_APP_ROLE;
        if (typeof roleHint === 'string') {
            var key = roleHint.toLowerCase();
            if (ROLE_NAV[key]) return key;
        }
        return 'owner';
    }

    function getDefaultRoute(role) {
        var nav = ROLE_NAV[role] || ROLE_NAV.owner;
        return nav.length ? nav[0].route : '/my-deals';
    }

    function normalizeRoute(route) {
        var value = route || '/my-deals';
        if (value.indexOf('#') === 0) value = value.slice(1);
        if (value.charAt(0) !== '/') value = '/' + value;
        value = value.split('?')[0];
        if (value.length > 1 && value.charAt(value.length - 1) === '/') value = value.slice(0, -1);

        var aliasMap = {
            '/my-deals.html': '/my-deals',
            '/new-deal-setup.html': '/new-deal-setup',
            '/deal-setup.html': '/deal-setup',
            '/deal-summary.html': '/deal-summary',
            '/deal-compare.html': '/deal-compare',
            '/partner-directory.html': '/partner-directory',
            '/brand-library.html': '/brand-library',
            '/operator-explorer.html': '/operator-explorer',
            '/financial-term-library.html': '/financial-term-library',
            '/deal-capture-radar-with-ranked-list.html': '/opportunity-radar',
            '/operator-intelligence-radar-with-list.html': '/opportunity-radar',
            '/deal-room-owner.html': '/deal-room-owner',
            '/deal-room-brand.html': '/deal-room-brand',
            '/outreach-plans.html': '/outreach',
            '/company-settings.html': '/company-settings',
            '/deal-brand-fit-analyzer.html': '/fit-analyzer',
            '/webflow-brand-dashboard.html': '/brand-workspace',
            '/brand-development-dashboard.html': '/brand-development-dashboard',
            '/brand-deal-request.html': '/brand-deal-request',
            '/my-third-party-operators.html': '/my-third-party-operators',
            '/third-party-operator-intake.html': '/third-party-operator-intake',
            '/user-management.html': '/user-management',
            '/brand-setup.html': '/brand-setup',
            '/reports-dashboard.html': '/reports',
            '/profile-settings.html': '/profile-settings'
        };
        return aliasMap[value] || value;
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
        var file = mapped && mapped.file ? mapped.file : '/my-deals.html';
        return file + (file.indexOf('?') !== -1 ? '&embed=1' : '?embed=1');
    }

    function getFrameForPath(path) {
        if (!frameContainer) return null;
        return frameContainer.querySelector('.app-frame[data-path="' + path + '"]');
    }

    function renderNav(role) {
        if (!shellNav) return;
        var items = ROLE_NAV[role] || ROLE_NAV.owner;
        shellNav.innerHTML = items.map(function (item) {
            return (
                '<button type="button" class="nav-item" data-href="' + item.route + '">' +
                    '<span class="nav-icon" aria-hidden="true">' + item.icon + '</span>' +
                    '<span class="nav-label">' + item.label + '</span>' +
                    '<span class="nav-arrow">&gt;</span>' +
                '</button>'
            );
        }).join('');
    }

    function showFrame(path) {
        if (!frameContainer) return;
        frameContainer.querySelectorAll('.app-frame').forEach(function (f) {
            f.classList.toggle('active', f.getAttribute('data-path') === path);
        });
    }

    function setActive(path) {
        document.querySelectorAll('.sidebar .nav-item[data-href]').forEach(function (el) {
            el.classList.toggle('active', el.getAttribute('data-href') === path);
        });
    }

    function updateShellHeader(path, role) {
        if (shellPageTitle) shellPageTitle.textContent = ROUTES[path] ? ROUTES[path].title : 'Dealality App';
        if (shellPageSubtitle) shellPageSubtitle.textContent = role.charAt(0).toUpperCase() + role.slice(1) + ' workspace';
    }

    function ensurePlaceholder(path) {
        var existing = getFrameForPath(path);
        if (existing) return existing;
        var iframe = document.createElement('iframe');
        iframe.className = 'app-frame';
        iframe.setAttribute('data-path', path);
        iframe.title = 'Process Management';
        iframe.srcdoc = '<!DOCTYPE html><html><head><meta charset="utf-8"><style>body{font-family:Inter,Segoe UI,sans-serif;background:#080f25;color:#fff;padding:40px;}h1{font-size:24px;margin:0 0 12px;}p{margin:0;color:#aeb9e1;}</style></head><body><h1>Process Management</h1><p>This route is reserved in the app shell and will be added in a future release.</p></body></html>';
        frameContainer.appendChild(iframe);
        return iframe;
    }

    function navigate(path, role, updateHash) {
        var normalized = normalizeRoute(path);
        var target = ROUTES[normalized] ? normalized : getDefaultRoute(role);

        if (updateHash !== false && window.location.hash !== '#' + target) {
            window.location.hash = target;
        }

        var existing = getFrameForPath(target);
        if (!existing) {
            if (ROUTES[target].placeholder) {
                existing = ensurePlaceholder(target);
            } else {
                existing = document.createElement('iframe');
                existing.className = 'app-frame';
                existing.setAttribute('data-path', target);
                existing.title = ROUTES[target].title || 'Page content';
                existing.src = routeToEmbedUrl(target, role);
                frameContainer.appendChild(existing);
            }
        }

        showFrame(target);
        setActive(target);
        updateShellHeader(target, role);

        if (sidebar && sidebar.classList.contains('mobile-open')) {
            sidebar.classList.remove('mobile-open');
        }
    }

    function initNavLinks(role) {
        if (shellNav) {
            shellNav.addEventListener('click', function (e) {
                var btn = e.target.closest('.nav-item[data-href]');
                if (!btn) return;
                e.preventDefault();
                navigate(btn.getAttribute('data-href'), role, true);
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
                    navigate(href, role, true);
                    accountWrap.classList.remove('open');
                    if (accountTrigger) accountTrigger.setAttribute('aria-expanded', 'false');
                    if (accountMenu) accountMenu.setAttribute('aria-hidden', 'true');
                });
            });
        }

        if (accountLogout) {
            accountLogout.addEventListener('click', function (e) {
                e.preventDefault();
                navigate(getDefaultRoute(role), role, true);
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

    function initShellMessageListener(role) {
        window.addEventListener('message', function (e) {
            if (!e.data || e.data.type !== 'dealality-navigate') return;
            if (e.origin !== window.location.origin) return;
            if (typeof e.data.path !== 'string') return;
            navigate(e.data.path, role, true);
        });
    }

    function init() {
        var role = getRole();
        renderNav(role);
        initNavLinks(role);
        initSidebarControls();
        initShellMessageListener(role);
        navigate(getPath(role), role, false);
        window.addEventListener('hashchange', function () {
            navigate(getPath(role), role, false);
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
