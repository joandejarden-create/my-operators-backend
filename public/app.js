(function () {
    'use strict';

    var frameContainer = document.getElementById('frameContainer');
    var sidebar = document.getElementById('sidebar');
    var sidebarToggle = document.getElementById('sidebarToggle');
    var currentPath = null;

    // Paths that belong to each dropdown (so we can open the right one and set active)
    var pathToDropdown = {
        '/my-deals.html': 'Deals',
        '/new-deal-setup': 'Deals',
        '/brand-development-dashboard': 'Deals',
        '/deal-compare': 'Deals',
        '/deal-capture-radar-with-ranked-list': 'Market Intelligence',
        '/loi-database-dashboard': 'Market Intelligence',
        '/market-alerts': 'Market Intelligence',
        '/deal-room-owner.html': 'Deal Room',
        '/deal-room-brand.html': 'Deal Room',
        '/outreach-plans': 'Outreach Hub',
        '/outreach-inbox': 'Outreach Hub',
        '/outreach-templates': 'Outreach Hub',
        '/outreach-sequences': 'Outreach Hub',
        '/outreach-analytics': 'Outreach Hub',
        '/outreach-deal-activity-log': 'Outreach Hub',
        '/clause-library': 'Deal Toolbox',
        '/financial-term-library': 'Deal Toolbox',
        '/brand-library': 'Deal Toolbox',
        '/operator-explorer': 'Deal Toolbox',
        '/franchise-fee-estimator': 'Deal Toolbox',
        '/index.html': 'Platform Resources',
        '/new-landing.html': 'Platform Resources',
        '/valuation-widget-enhanced': 'Platform Resources',
        '/brand-review': 'Platform Resources',
        '/brand-explorer': 'Platform Resources',
        '/signup': 'Platform Resources',
        '/profile-settings': 'Settings',
        '/company-settings': 'Settings',
        '/user-management': 'Settings',
        '/my-brands': 'Settings',
        '/my-third-party-operators': 'Settings',
        '/third-party-operator-intake': 'Settings',
        '/brand-setup': 'Settings'
    };

    // Paths that are served as extensionless URLs; iframe should request the .html file so static/server can serve it
    var pathToHtml = {
        '/': '/app/home.html',
        '/app/home': '/app/home.html',
        '/outreach-plans': '/outreach-plans.html',
        '/outreach-inbox': '/outreach-inbox.html',
        '/outreach-templates': '/outreach-template-manager.html',
        '/outreach-sequences': '/outreach-sequences.html',
        '/outreach-analytics': '/outreach-analytics.html',
        '/outreach-deal-activity-log': '/outreach-deal-activity-log.html',
        '/company-settings': '/company-settings.html',
        '/profile-settings': '/profile-settings.html',
        '/my-brands': '/all-brands-dashboard.html',
        '/my-third-party-operators': '/my-third-party-operators.html',
        '/brand-setup': '/brand-setup.html',
        '/third-party-operator-intake': '/third-party-operator-intake.html',
        '/franchise-fee-estimator': '/franchise-fee-estimator.html',
        '/brand-library': '/brand-library.html',
        '/brand-explorer': '/brand-explorer.html',
        '/operator-explorer': '/operator-explorer.html'
    };

    function getPath() {
        var hash = window.location.hash.slice(1);
        if (hash.charAt(0) === '/') {
            return hash;
        }
        return hash ? '/' + hash : '/';
    }

    function embedUrl(path) {
        var base = pathToHtml[path] || path || '/';
        if (base.charAt(0) !== '/') base = '/' + base;
        return base + (base.indexOf('?') !== -1 ? '&embed=1' : '?embed=1');
    }

    function getFrameForPath(path) {
        if (!frameContainer) return null;
        return frameContainer.querySelector('.app-frame[data-path="' + path + '"]');
    }

    function defaultIntroByPath(path, titleText) {
        var map = {
            '/': 'Enter and manage your deals to get feedback from brands.',
            '/app/home': 'Your command center for deal metrics, pipeline status, signals, and next actions.',
            '/my-deals.html': 'Enter and manage your deals to get feedback from brands.',
            '/new-deal-setup': 'Create and configure a new deal with the required project details.',
            '/brand-development-dashboard': 'Evaluate prospective deals with quantitative and qualitative match scores to identify the best opportunities for your brand.',
            '/deal-capture-radar-with-ranked-list': 'Explore ranked opportunities and market movement to prioritize the right deals.',
            '/loi-database-dashboard': 'Explore 100 sample hotel LOI deals across franchise, third-party management, and lease structures. Filter and compare terms to see how you\'ll be able to benchmark your deals.',
            '/market-alerts': 'Monitor market signals and alerts that impact your deal strategy.',
            '/deal-room-owner.html': 'Review and manage owner-side deal room activity and documents.',
            '/deal-room-brand.html': 'Review and manage brand-side deal room activity and documents.',
            '/outreach-plans': 'Plan and coordinate your outreach strategy across target deals and contacts.',
            '/outreach-inbox': 'Review inbound and outbound outreach communications in one place.',
            '/outreach-templates': 'Manage outreach templates for consistent communication.',
            '/outreach-sequences': 'Build and track multi-step outreach sequences.',
            '/outreach-analytics': 'Track outreach performance and engagement trends.',
            '/outreach-deal-activity-log': 'Review recent deal activities across your outreach and deal workflows.',
            '/clause-library': 'Browse and manage legal clauses used across deals.',
            '/financial-term-library': 'Review and manage financial terms used across deal negotiations.',
            '/brand-library': 'Explore brand profiles, requirements, and fit considerations.',
            '/operator-explorer': 'Explore operators and evaluate capabilities, footprint, and fit.',
            '/franchise-fee-estimator': 'Estimate franchise fee ranges based on deal assumptions.',
            '/partner-directory': 'Search and connect with partners supporting your deal workflows.',
            '/index.html': 'Explore Dealality resources and platform information.',
            '/new-landing.html': 'Explore Dealality resources and platform information.',
            '/valuation-widget-enhanced': 'Review valuation benchmarks and scenario insights.',
            '/brand-review': 'Review submitted deals and update brand decisions.',
            '/signup': 'Create an account to access the Dealality platform.',
            '/market-analytics': 'Analyze market performance and reporting metrics.',
            '/profile-settings': 'Manage your profile preferences and account details.',
            '/company-settings': 'Manage company-level settings and configuration.',
            '/user-management': 'Manage users, access levels, and permissions.',
            '/my-brands': 'Manage your brands and keep portfolio information up to date.',
            '/my-third-party-operators': 'Manage your operators and keep company information up to date.',
            '/third-party-operator-intake': 'Set up operator profile details, capabilities, and footprint.',
            '/brand-setup': 'Set up and maintain your brand profile and criteria.'
        };
        if (map[path]) return map[path];
        return 'Review and manage ' + (titleText || 'this page').toLowerCase() + ' in one place.';
    }

    function ensureHeaderIntro(doc, path) {
        if (!doc || !doc.body) return;

        var titleSelectors = [
            '.intake-title-container h1',
            '.deal-capture-news-title-container h1',
            '.dashboard-title-container h1',
            '.operator-explorer-title-container h1',
            '.dc-header__page-title',
            '.operator-header__title-section h1'
        ];
        var titleEl = null;
        for (var i = 0; i < titleSelectors.length; i++) {
            var candidate = doc.querySelector(titleSelectors[i]);
            if (candidate && (candidate.textContent || '').trim()) {
                titleEl = candidate;
                break;
            }
        }
        if (!titleEl) return;

        var container = titleEl.closest('.intake-title-container, .deal-capture-news-title-container, .dashboard-title-container, .operator-explorer-title-container, .dc-header__brand, .operator-header__title-section') || titleEl.parentElement;
        if (!container) return;

        var generatedIntros = container.querySelectorAll('.app-shell-generated-intro');
        var headerScope = titleEl.closest('.intake-header, .dashboard-header, .operator-explorer__header, .dc-header, .news-page-header, .deal-capture-news-header, .operator-header__top') || container;
        var hasNativeIntro = !!container.querySelector('p:not(.app-shell-generated-intro), .dc-header__description');
        if (!hasNativeIntro && headerScope) {
            var scopeParagraphs = headerScope.querySelectorAll('p:not(.app-shell-generated-intro), .dc-header__description');
            for (var pIdx = 0; pIdx < scopeParagraphs.length; pIdx++) {
                var p = scopeParagraphs[pIdx];
                if (p.closest('.mock-data-banner, .dc-header__warning')) continue;
                if ((p.textContent || '').trim()) {
                    hasNativeIntro = true;
                    break;
                }
            }
        }
        if (hasNativeIntro) {
            generatedIntros.forEach(function (el) { el.remove(); });
            return;
        }
        if (generatedIntros.length) return;

        var intro = doc.createElement('p');
        intro.className = 'app-shell-generated-intro';
        intro.textContent = defaultIntroByPath(path, (titleEl.textContent || '').trim());
        container.appendChild(intro);
    }

    function removeHeaderLogosInFrame(iframe, path) {
        if (!iframe || !iframe.contentDocument) return;
        var doc = iframe.contentDocument;
        var head = doc.head || doc.getElementsByTagName('head')[0];
        if (!head) return;

        var styleId = 'app-shell-hide-header-logos';
        if (!doc.getElementById(styleId)) {
            var style = doc.createElement('style');
            style.id = styleId;
            style.textContent =
                '.deal-capture-logo, .dc-header__logo { display: none !important; }' +
                '.intake-title-container, .deal-capture-news-title-container, .dc-header__title-row { gap: 0 !important; }' +
                '.intake-title-container, .deal-capture-news-title-container, .dashboard-title-container, .dc-header__brand, .operator-explorer-title-container, .operator-header__title-section {' +
                    'display: flex !important;' +
                    'flex-direction: column !important;' +
                    'align-items: flex-start !important;' +
                    'justify-content: flex-start !important;' +
                '}' +
                '.intake-title-container h1, .deal-capture-news-title-container h1, .dashboard-title-container h1, .dc-header__page-title, .operator-explorer-title-container h1, .operator-header__title-section h1 {' +
                    'margin: 0 !important;' +
                    'color: #ffffff !important;' +
                    'font-size: 2rem !important;' +
                    'line-height: 1.2 !important;' +
                    'font-weight: 700 !important;' +
                '}' +
                '.intake-title-container p, .deal-capture-news-title-container p, .dashboard-title-container p, .dc-header__description, .operator-explorer-title-container p, .operator-header__title-section p, .app-shell-generated-intro {' +
                    'margin: 5px 0 0 0 !important;' +
                    'color: #ffffff !important;' +
                    'font-size: 0.75rem !important;' +
                    'line-height: 1.3 !important;' +
                    'font-weight: 300 !important;' +
                    'max-width: 100% !important;' +
                '}' +
                '.filter-input, .filter-search-input, input[type="search"], input[id*="search" i], input[placeholder*="search" i] {' +
                    'background: #101935 !important;' +
                    'border: 1px solid rgba(255, 255, 255, 0.2) !important;' +
                    'color: #ffffff !important;' +
                    'padding: 8px 12px !important;' +
                    'border-radius: 6px !important;' +
                    'font-size: 13px !important;' +
                    'font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif !important;' +
                    'box-sizing: border-box !important;' +
                '}' +
                '.filter-input::placeholder, .filter-search-input::placeholder, input[type="search"]::placeholder, input[id*="search" i]::placeholder, input[placeholder*="search" i]::placeholder {' +
                    'color: rgba(255, 255, 255, 0.5) !important;' +
                '}' +
                '.filter-input:focus, .filter-search-input:focus, input[type="search"]:focus, input[id*="search" i]:focus, input[placeholder*="search" i]:focus {' +
                    'outline: none !important;' +
                    'border-color: #6c72ff !important;' +
                    'box-shadow: 0 0 0 2px rgba(102, 126, 234, 0.2) !important;' +
                '}' +
                '.filter-input:-webkit-autofill, .filter-input:-webkit-autofill:hover, .filter-input:-webkit-autofill:focus, .filter-search-input:-webkit-autofill, .filter-search-input:-webkit-autofill:hover, .filter-search-input:-webkit-autofill:focus {' +
                    '-webkit-text-fill-color: #ffffff !important;' +
                    '-webkit-box-shadow: 0 0 0px 1000px #101935 inset !important;' +
                    'transition: background-color 5000s ease-in-out 0s !important;' +
                '}';
            head.appendChild(style);
        }

        ensureHeaderIntro(doc, path);
    }

    function showFrame(path) {
        if (!frameContainer) return;
        var frames = frameContainer.querySelectorAll('.app-frame');
        frames.forEach(function (f) {
            f.classList.toggle('active', f.getAttribute('data-path') === path);
        });
        currentPath = path;
    }

    function navigate(path) {
        path = path || '/';
        if (path.charAt(0) !== '/') {
            path = '/' + path;
        }
        window.location.hash = path;
        var existing = getFrameForPath(path);
        if (existing) {
            removeHeaderLogosInFrame(existing, path);
            showFrame(path);
            setActive(path);
            openDropdownForPath(path);
            return;
        }
        var iframe = document.createElement('iframe');
        iframe.className = 'app-frame';
        iframe.setAttribute('data-path', path);
        iframe.title = 'Page content';
        iframe.addEventListener('load', function () {
            removeHeaderLogosInFrame(iframe, path);
        });
        iframe.src = embedUrl(path);
        frameContainer.appendChild(iframe);
        showFrame(path);
        setActive(path);
        openDropdownForPath(path);
    }

    function setActive(path) {
        // No longer collapse sidebar on Home – show full left nav on all pages
        if (sidebar) {
            sidebar.classList.remove('sidebar--home');
        }
        var all = document.querySelectorAll('.sidebar .nav-item[data-href], .sidebar .user-block[data-href]');
        all.forEach(function (el) {
            var href = el.getAttribute('data-href');
            if (href === path || (path !== '/' && href && path.indexOf(href) === 0)) {
                el.classList.add('active');
            } else {
                el.classList.remove('active');
            }
        });
        // Highlight dropdown parent when current path is a child of that section
        var parentLabel = pathToDropdown[path];
        if (parentLabel) {
            document.querySelectorAll('.sidebar .nav-group[data-dropdown]').forEach(function (group) {
                var trigger = group.querySelector('[data-dropdown-trigger]');
                var labelEl = group.querySelector('.nav-item-parent .nav-label');
                if (trigger && labelEl && labelEl.textContent.trim() === parentLabel) {
                    trigger.classList.add('active');
                } else if (trigger) {
                    trigger.classList.remove('active');
                }
            });
        }
    }

    function openDropdownForPath(path) {
        var label = pathToDropdown[path];
        if (!label) return;

        document.querySelectorAll('.sidebar .nav-group[data-dropdown]').forEach(function (group) {
            var trigger = group.querySelector('[data-dropdown-trigger]');
            var labelEl = group.querySelector('.nav-label');
            if (trigger && labelEl && labelEl.textContent.trim() === label) {
                group.classList.add('open');
                trigger.setAttribute('aria-expanded', 'true');
            } else {
                group.classList.remove('open');
                if (trigger) trigger.setAttribute('aria-expanded', 'false');
            }
        });
    }

    function initDropdowns() {
        document.querySelectorAll('.sidebar .nav-group[data-dropdown]').forEach(function (group) {
            var trigger = group.querySelector('[data-dropdown-trigger]');
            if (!trigger) return;

            trigger.addEventListener('click', function (e) {
                e.preventDefault();
                var isOpen = group.classList.toggle('open');
                trigger.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
            });
        });

        document.querySelectorAll('.sidebar .nav-item-child').forEach(function (link) {
            link.addEventListener('click', function (e) {
                var href = link.getAttribute('data-href');
                if (href && href !== '#') {
                    navigate(href);
                }
            });
        });
    }

    function initNavLinks() {
        document.querySelectorAll('.sidebar .nav-item[data-href]:not([data-dropdown-trigger])').forEach(function (el) {
            el.addEventListener('click', function (e) {
                var href = el.getAttribute('data-href');
                if (href && href !== '#') {
                    e.preventDefault();
                    navigate(href);
                }
            });
        });

        // Account dropdown (user block in footer)
        var accountWrap = document.getElementById('accountDropdownWrap');
        var accountTrigger = document.getElementById('accountDropdownTrigger');
        var accountMenu = document.getElementById('accountDropdownMenu');
        var accountLogout = document.getElementById('accountLogout');

        if (accountTrigger && accountMenu) {
            accountTrigger.addEventListener('click', function (e) {
                e.preventDefault();
                e.stopPropagation();
                accountWrap.classList.toggle('open');
                var isOpen = accountWrap.classList.contains('open');
                accountTrigger.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
                accountMenu.setAttribute('aria-hidden', isOpen ? 'false' : 'true');
            });
        }

        document.addEventListener('click', function (e) {
            if (accountWrap && accountWrap.classList.contains('open') &&
                !accountWrap.contains(e.target)) {
                accountWrap.classList.remove('open');
                if (accountTrigger) accountTrigger.setAttribute('aria-expanded', 'false');
                if (accountMenu) accountMenu.setAttribute('aria-hidden', 'true');
            }
        });

        if (accountMenu) {
            accountMenu.querySelectorAll('.account-dropdown-item[data-href]').forEach(function (el) {
                el.addEventListener('click', function (e) {
                    var href = el.getAttribute('data-href');
                    if (href && href !== '#') {
                        e.preventDefault();
                        navigate(href);
                        accountWrap.classList.remove('open');
                        if (accountTrigger) accountTrigger.setAttribute('aria-expanded', 'false');
                        if (accountMenu) accountMenu.setAttribute('aria-hidden', 'true');
                    }
                });
            });
        }

        if (accountLogout) {
            accountLogout.addEventListener('click', function (e) {
                e.preventDefault();
                accountWrap.classList.remove('open');
                if (accountTrigger) accountTrigger.setAttribute('aria-expanded', 'false');
                if (accountMenu) accountMenu.setAttribute('aria-hidden', 'true');
                navigate('/');
            });
        }
    }

    function initSidebarToggle() {
        if (!sidebarToggle || !sidebar) return;
        sidebarToggle.addEventListener('click', function () {
            sidebar.classList.toggle('collapsed');
        });
    }

    function onHashChange() {
        navigate(getPath());
    }

    function initShellMessageListener() {
        window.addEventListener('message', function (e) {
            if (!e.data || e.data.type !== 'dealality-navigate') return;
            if (e.origin !== window.location.origin) return;
            var msgPath = e.data.path;
            if (typeof msgPath !== 'string' || msgPath.charAt(0) !== '/') return;
            navigate(msgPath);
        });
    }

    function init() {
        initDropdowns();
        initNavLinks();
        initSidebarToggle();
        initShellMessageListener();

        if (window.location.hash) {
            onHashChange();
        } else {
            navigate('/');
        }

        window.addEventListener('hashchange', onHashChange);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
