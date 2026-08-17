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
    var workspaceDemoContext = document.getElementById('workspaceDemoContext');
    var workspaceDevTools = document.getElementById('workspaceDevTools');
    var workspaceDemoBrandPortfolio = document.getElementById('workspaceDemoBrandPortfolio');
    var devBrandPortfolioSelect = document.getElementById('devBrandPortfolioSelect');
    var devNavOverrideSelect = document.getElementById('devNavOverrideSelect');
    /**
     * dealality_active_workspace — real app-shell workspace context (nav + routing).
     * dealality_demo_brand_portfolio — founder/demo showcase Brand portfolio context.
     * dc_dashboard_role_view (public/app/dashboard.js) — Command Center sample dashboard preview only.
     * Changing one must not overwrite the other.
     */
    var ACTIVE_WORKSPACE_STORAGE_KEY = 'dealality_active_workspace';
    var DEMO_BRAND_PORTFOLIO_STORAGE_KEY = 'dealality_demo_brand_portfolio';
    /** Localhost-only nav override (admin / all) — does not replace active workspace. */
    var DEV_NAV_OVERRIDE_STORAGE_KEY = 'DEALALITY_DEV_NAV_OVERRIDE';
    var ACTIVE_WORKSPACE_ORDER = ['Owner', 'Operator', 'Brand'];
    var ALLOWED_ROLES = ['owner', 'brand', 'operator', 'admin', 'owner-operator', 'owner_operator'];
    var ALLOWED_DEV_NAV_OVERRIDES = ['admin', 'all'];
    var uiLabels = window.DEALALITY_UI_LABELS;
    var meDealality = null;
    /** True after a successful /api/me apply — required before admin-only nav/routes. */
    var meContextLoaded = false;
    var switchableWorkspaces = [];
    var activeWorkspace = 'Owner';
    var showDemoModeBadge = false;
    var devNavOverride = '';
    var demoBrandPortfolioKey = 'marriott';
    var demoBrandPortfolioOptions = [];

    function formatWorkspaceUiLabel(workspaceOrRoleKey) {
        var key = String(workspaceOrRoleKey || '');
        if (uiLabels && typeof uiLabels.formatWorkspaceSideLabel === 'function') {
            if (ACTIVE_WORKSPACE_ORDER.indexOf(key) !== -1) {
                return uiLabels.formatWorkspaceSideLabel(key);
            }
        }
        if (uiLabels && typeof uiLabels.formatDevWorkspaceSwitcherLabel === 'function') {
            return uiLabels.formatDevWorkspaceSwitcherLabel(key);
        }
        if (key === 'all') return 'All Workspaces';
        if (key === 'Owner') return 'Owner-Side';
        if (key === 'Operator') return 'Operator-Side';
        if (key === 'Brand') return 'Brand-Side';
        var r = key.toLowerCase();
        if (r === 'owner') return 'Owner-Side';
        if (r === 'operator') return 'Operator-Side';
        if (r === 'brand') return 'Brand-Side';
        return key.charAt(0).toUpperCase() + key.slice(1);
    }

    var currentBaseRole = 'owner';
    var authenticatedRole = '';
    var isDevMode = false;

    var MARKET_ALERTS_EMBED_VERSION = '1.3.4';

    var ROUTES = {
        '/home': { file: '/app/home.html', title: 'Home' },
        '/my-deals': { file: '/my-deals.html', title: 'My Deals' },
        '/new-deal-setup': { file: '/new-deal-setup.html', title: 'Add New Deal' },
        '/deal-setup': { file: '/deal-setup.html', title: 'Deal Setup' },
        '/deal-summary': { file: '/deal-summary.html', title: 'Deal Summary' },
        '/deal-brief-snapshot': { file: '/deal-brief-snapshot.html', title: 'Deal Brief' },
        '/deal-compare': { file: '/deal-compare.html', title: 'Deal Compare' },
        '/market-alerts': { file: '/market-alerts.html', title: 'Market Alerts' },
        '/opportunity-radar': { file: '/deal-capture-radar-with-ranked-list.html', title: 'The Radar' },
        '/scout-market-map': { file: '/app/scout-market-map.html', title: 'Scout Market Map' },
        // Retired route kept as archived source file only (public/management-operator-radar.html).
        // Intentionally removed from active app shell routing/navigation.
        '/loi-database-dashboard': { file: '/loi-database-dashboard.html', title: 'LOI Market Hub' },
        // Legacy hash routes → combined Brand Explorer (replaces brand-library-atelier-north.html).
        '/brand-explorer': { file: '/brand-explorer-combined.html', title: 'Brand Explorer' },
        '/operator-explorer': { file: '/operator-explorer.html', title: 'Operator Explorer' },
        // Capital Explorer — UI not ready; routes removed from shell until pages ship.
        '/operator-explorer-mockup': { file: '/operator-explorer-gold-mock.html', title: 'Operator Explorer Mockup' },
        '/operator-dna-profile': { file: '/operator-dna-profile.html', title: 'Operator DNA Profile (Prototype)' },
        '/deal-room-owner': { file: '/deal-room-owner.html', title: 'Deal Room (Owner)' },
        '/deal-room-brand': { file: '/deal-room-brand.html', title: 'Deal Room (Brand)' },
        '/brand-deal-request': { file: '/brand-deal-request.html', title: 'Brand Deal Request' },
        '/outreach': { file: '/outreach-plans.html', title: 'Outreach Plans' },
        '/activity-log': { file: '/outreach-deal-activity-log.html', title: 'Activity Log' },
        '/outreach/inbox': { file: '/outreach-inbox.html', title: 'Outreach Inbox' },
        '/outreach/sequences': { file: '/outreach-sequences.html', title: 'Outreach Sequences' },
        '/outreach/analytics': { file: '/outreach-analytics.html', title: 'Outreach Analytics' },
        '/outreach/deal-activity-log': { file: '/outreach-deal-activity-log.html', title: 'Outreach Deal Activity Log' },
        '/outreach/templates': { file: '/outreach-template-manager.html', title: 'Outreach Templates' },
        '/brand-explorer-combined': { file: '/brand-explorer-combined.html', title: 'Brand Explorer' },
        '/brand-explorer-export': { file: '/brand-explorer-export.html', title: 'Brand Explorer PDF' },
        '/financial-term-library': { file: '/financial-term-library.html', title: 'Financial Term Library' },
        '/clause-library': { file: '/clause-library.html', title: 'Clause Library' },
        '/franchise-fee-estimator': { file: '/franchise-fee-estimator.html', title: 'Franchise Fee Estimator' },
        '/partner-directory': { file: '/partner-directory.html', title: 'Partner Directory' },
        '/my-brands': { file: '/all-brands-dashboard.html', title: 'My Brands' },
        '/my-operators': { file: '/my-third-party-operators-new.html', title: 'My Operators' },
        '/brand-development-dashboard': { file: '/brand-development-dashboard.html', title: 'My Brand Deals', roles: ['brand', 'admin'] },
        '/ai-visibility': { file: '/ai-visibility-brand.html', title: 'Brand AI Visibility', roles: ['brand', 'admin'], stakeholderProduct: 'brand_ai_visibility' },
        '/ai-visibility-brand': { file: '/ai-visibility-brand.html', title: 'Brand AI Visibility', roles: ['brand', 'admin'], stakeholderProduct: 'brand_ai_visibility' },
        '/operator-development-dashboard': { file: '/operator-development-dashboard.html', title: 'My Operator Deals', roles: ['operator', 'admin'] },
        '/third-party-operator-intake': { file: '/third-party-operator-setup-new-two.html', title: 'Operator Setup' },
        '/third-party-operator-setup-sandbox': { file: '/third-party-operator-setup-sandbox.html', title: 'Operator Setup (Sandbox)' },
        '/brand-setup': { file: '/brand-setup.html', title: 'Brand Setup' },
        '/reports': { file: '/reports-dashboard.html', title: 'Reports' },
        '/route-map': { placeholder: true, title: 'Route Map' },
        '/ai-intelligence-validation': {
            file: '/ai-intelligence-validation.html',
            title: 'Validation Scorecard',
            roles: ['admin'],
            internalRunbookOnly: false,
            validationScorecard: true
        },
        '/ai-intelligence-golden-set-review': {
            file: '/ai-intelligence-golden-set-review.html',
            title: 'Golden Set Review',
            roles: ['admin'],
            internalRunbookOnly: false,
            validationScorecard: true
        },
        '/support': { file: '/app/support/index.html', title: 'Support' },
        '/support/owner-pilot-provisioning': {
            file: '/app/support/owner-pilot-provisioning.html',
            title: 'Owner Pilot Runbook',
            roles: ['admin'],
            internalRunbookOnly: true
        },
        '/support/scoring-weight-model': {
            file: '/app/support/scoring-weight-model.html',
            title: 'Scoring Weight Model',
            roles: ['admin'],
            internalRunbookOnly: true
        },
        '/company-settings': { file: '/company-settings.html', title: 'Company Settings' },
        '/profile-settings': { file: '/profile-settings.html', title: 'Profile Settings' },
        '/user-management': { file: '/user-management.html', title: 'User Management' },
        '/owner-diagnostic-sample': { file: '/owner-diagnostic-sample.html', title: 'Deal Readiness Report (Sample)' }
    };

    var NAV_ICONS = {
        home: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 10.5L12 3l9 7.5"/><path d="M5 9.5V20h14V9.5"/><path d="M9.5 20v-6h5v6"/></svg>',
        deals: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="3" width="8" height="8"/><rect x="13" y="3" width="8" height="8"/><rect x="3" y="13" width="8" height="8"/><rect x="13" y="13" width="8" height="8"/></svg>',
        market: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="5"/><circle cx="12" cy="12" r="1.5" fill="currentColor" stroke="none"/></svg>',
        room: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="7" width="18" height="12" rx="2"/><path d="M8 7V5h8v2"/><path d="M8 13h8"/><path d="M8 17h5"/></svg>',
        outreach: '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="4" r="2"/><circle cx="4" cy="12" r="2"/><circle cx="20" cy="12" r="2"/><circle cx="12" cy="20" r="2"/><path d="M12 6v12"/><path d="M6 12h12"/></svg>',
        toolbox: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 6h5v14H4z"/><path d="M10 4h5v16h-5z"/><path d="M16 7h4v13h-4z"/></svg>',
        financing: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 1v22"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 1 1 0 7H6"/></svg>',
        partner: '<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="3" y="2.5" width="18" height="19" rx="2"/><circle cx="12" cy="8" r="2.8"/><path d="M7.5 16c1.5-2.2 7.5-2.2 9 0"/><path d="M6.5 4.5h3"/><path d="M14.5 4.5h3"/></svg>',
        platform: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3l9 4.5-9 4.5-9-4.5z"/><path d="M3 12l9 4.5 9-4.5"/><path d="M3 16.5 12 21l9-4.5"/></svg>',
        reports: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 20h18"/><rect x="5" y="11" width="3" height="7"/><rect x="10.5" y="8" width="3" height="10"/><rect x="16" y="5" width="3" height="13"/></svg>',
        support: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M21 11.5a8.5 8.5 0 1 1-4.2-7.35"/><path d="M8.8 9.2a3.2 3.2 0 1 1 5 2.6c-.8.6-1.4 1.1-1.4 2.2"/><circle cx="12.1" cy="17.4" r="0.7" fill="currentColor" stroke="none"/></svg>',
        routeMap: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 5h5v5H5z"/><path d="M14 5h5v5h-5z"/><path d="M5 14h5v5H5z"/><path d="M10 7.5h4"/><path d="M7.5 10v4"/></svg>',
        adminResources: '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 2l8 4v6c0 5-3.5 9.5-8 10-4.5-.5-8-5-8-10V6l8-4z"/><path d="M9 12l2 2 4-4"/></svg>',
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
                        { label: 'My Operator Deals', route: '/operator-development-dashboard', roles: ['operator', 'admin'] },
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
                        { label: 'LOI Market Hub', route: '/loi-database-dashboard', roles: ['owner', 'brand', 'admin'] },
                        { label: 'Brand AI Visibility', route: '/ai-visibility', roles: ['brand', 'admin'], stakeholderProduct: 'brand_ai_visibility' }
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
                        { label: 'Brand Explorer', route: '/brand-explorer-combined', roles: ['owner', 'brand', 'admin'] },
                        { label: 'Operator Explorer', route: '/operator-explorer', roles: ['owner', 'brand', 'operator', 'admin'] },
                        { label: 'Clause Library', route: '/clause-library', roles: ['owner', 'brand', 'admin'] },
                        { label: 'Financial Term Library', route: '/financial-term-library', roles: ['owner', 'brand', 'admin'] },
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
                {
                    type: 'group',
                    label: 'Support',
                    icon: NAV_ICONS.support,
                    children: [
                        { label: 'Help Center', route: '/support', roles: ['owner', 'brand', 'operator', 'admin'] }
                    ]
                },
                {
                    type: 'group',
                    label: 'Settings',
                    icon: NAV_ICONS.settings,
                    children: [
                        { label: 'Company Settings', route: '/company-settings', roles: ['owner', 'brand', 'operator', 'admin'] },
                        { label: 'Profile Settings', route: '/profile-settings', roles: ['owner', 'brand', 'operator', 'admin'] },
                        { label: 'User Management', route: '/user-management', roles: ['admin'] },
                        { label: 'Operator Setup', route: '/third-party-operator-intake', roles: ['operator', 'admin'] },
                        { label: 'Brand Setup', route: '/brand-setup', roles: ['admin'] }
                    ]
                }
            ]
        },
        {
            name: 'adminResources',
            entries: [
                {
                    type: 'group',
                    label: 'Admin Resources',
                    icon: NAV_ICONS.adminResources,
                    children: [
                        { label: 'Owner Pilot Runbook', route: '/support/owner-pilot-provisioning', roles: ['admin'], internalRunbookOnly: true },
                        { label: 'Scoring Weight Model', route: '/support/scoring-weight-model', roles: ['admin'], internalRunbookOnly: true },
                        { label: 'Route Map', route: '/route-map', roles: ['admin'], devOnly: true },
                        {
                            label: 'Validation Scorecard',
                            route: '/ai-intelligence-validation',
                            roles: ['admin'],
                            validationScorecard: true
                        },
                        {
                            label: 'Golden Set Review',
                            route: '/ai-intelligence-golden-set-review',
                            roles: ['admin'],
                            validationScorecard: true
                        },
                        { label: 'Scout Market Map', route: '/scout-market-map', roles: ['admin'] },
                        { label: 'Deal Readiness Report (Sample)', route: '/owner-diagnostic-sample', roles: ['admin'] }
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
        '/deal-brief-snapshot.html': '/deal-brief-snapshot',
        '/deal-compare.html': '/deal-compare',
        '/market-alerts.html': '/market-alerts',
        '/deal-capture-radar-with-ranked-list.html': '/opportunity-radar',
        '/app/scout-market-map.html': '/scout-market-map',
        '/operator-intelligence-radar-with-list.html': '/opportunity-radar',
        '/loi-database-dashboard.html': '/loi-database-dashboard',
        '/brand-explorer.html': '/brand-explorer-combined',
        '/operator-explorer.html': '/operator-explorer',
        '/operator-explorer-gold-mock.html': '/operator-explorer-mockup',
        '/operator-dna-profile.html': '/operator-dna-profile',
        '/deal-room-owner.html': '/deal-room-owner',
        '/deal-room-brand.html': '/deal-room-brand',
        '/brand-deal-request.html': '/brand-deal-request',
        '/outreach-plans': '/outreach',
        '/outreach-plans.html': '/outreach',
        '/outreach-inbox': '/outreach/inbox',
        '/outreach-inbox.html': '/outreach/inbox',
        '/outreach-deal-activity-log': '/activity-log',
        '/activity-log.html': '/activity-log',
        '/outreach-deal-activity-log.html': '/activity-log',
        '/outreach-sequences.html': '/outreach/sequences',
        '/outreach-analytics.html': '/outreach/analytics',
        '/outreach/deal-activity-log': '/activity-log',
        '/outreach-template-manager.html': '/outreach/templates',
        '/brand-library': '/brand-explorer-combined',
        '/brand-library.html': '/brand-explorer-combined',
        '/brand-explorer-combined.html': '/brand-explorer-combined',
        '/brand-library-atelier': '/brand-explorer-combined',
        '/brand-library-atelier-north.html': '/brand-explorer-combined',
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
        '/brand-workspace-pipeline': '/brand-development-dashboard',
        '/brand-workspace-pipeline.html': '/brand-development-dashboard',
        '/webflow-brand-dashboard.html': '/brand-development-dashboard',
        '/brand-development-dashboard.html': '/brand-development-dashboard',
        '/operator-development-dashboard.html': '/operator-development-dashboard',
        '/my-operator-deals': '/operator-development-dashboard',
        '/my-operator-deals.html': '/operator-development-dashboard',
        '/recommended-fit-list': '/my-deals',
        '/recommended-fit-list.html': '/my-deals',
        '/my-operators.html': '/my-operators',
        '/my-third-party-operators': '/my-operators',
        '/my-third-party-operators.html': '/my-operators',
        '/my-third-party-operators-new': '/my-operators',
        '/my-third-party-operators-new.html': '/my-operators',
        '/third-party-operator-intake.html': '/third-party-operator-intake',
        '/third-party-operator-setup-new-two.html': '/third-party-operator-intake',
        '/third-party-operator-setup-sandbox.html': '/third-party-operator-setup-sandbox',
        '/brand-setup.html': '/brand-setup',
        '/reports-dashboard.html': '/reports',
        '/route-map.html': '/route-map',
        '/company-settings.html': '/company-settings',
        '/profile-settings.html': '/profile-settings',
        '/user-management.html': '/user-management',
        '/owner-diagnostic-sample.html': '/owner-diagnostic-sample',
        '/app/support/index.html': '/support',
        '/app/support/owner-pilot-provisioning.html': '/support/owner-pilot-provisioning',
        '/app/support/scoring-weight-model.html': '/support/scoring-weight-model'
    };

    /**
     * Platform admin — from /api/me dealality.isAdmin only (not workspace preview, dev override, or legacy role).
     * Admins previewing Owner workspace still pass when isAdmin is true on their profile.
     */
    function hasAdminNavAccess() {
        if (!meContextLoaded || !meDealality) return false;
        if (meDealality.isAdmin === true) return true;
        if (meDealality.flags && meDealality.flags.isAdmin === true) return true;
        return false;
    }

    /** Owner Pilot Runbook — same platform admin signal as /api/me dealality.isAdmin. */
    function hasInternalRunbookNavAccess() {
        return hasAdminNavAccess();
    }

    /** Nav/route roles that include admin and no workspace role (e.g. ['admin'] only). */
    function isAdminExclusiveRoles(roles) {
        if (!roles || !roles.length) return false;
        if (roles.indexOf('admin') === -1) return false;
        var workspaceRoles = ['owner', 'brand', 'operator', 'owner-operator', 'owner_operator'];
        for (var i = 0; i < roles.length; i++) {
            if (workspaceRoles.indexOf(roles[i]) !== -1) return false;
        }
        return true;
    }

    function isInternalRunbookRoute(route) {
        var meta = ROUTES[normalizeRoute(route)];
        return !!(meta && meta.internalRunbookOnly);
    }

    function isAdminExclusiveRoute(route) {
        var meta = ROUTES[normalizeRoute(route)];
        if (!meta || !meta.roles) return false;
        return isAdminExclusiveRoles(meta.roles);
    }

    function canSee(role, roles, devOnly, internalRunbookOnly) {
        if (internalRunbookOnly) return hasInternalRunbookNavAccess();
        if (devOnly) {
            if (isDevMode || hasAdminNavAccess()) return true;
            return false;
        }
        if (!roles || !roles.length) {
            if (role === 'all') return true;
            return true;
        }
        if (isAdminExclusiveRoles(roles)) return hasAdminNavAccess();
        if (role === 'all') return true;
        if (roles.indexOf('admin') !== -1 && hasAdminNavAccess()) return true;
        return roles.indexOf(role) !== -1;
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

    /** True when this shell document is embedded in Webflow (or another parent), not opened standalone. */
    function isEmbeddedInParent() {
        try {
            return window.self !== window.top;
        } catch (_err) {
            return true;
        }
    }

    /** Hide sidebar/footer when the shell itself is iframed on dealality.com (Webflow dashboard). */
    function applyChromelessShellIfEmbedded() {
        if (!isEmbeddedInParent() || computeDevMode()) return;
        document.documentElement.classList.add('app-chromeless-embed');
        if (sidebar) sidebar.setAttribute('hidden', '');
        var footer = document.querySelector('.app-shell-footer');
        if (footer) footer.setAttribute('hidden', '');
    }

    function normalizeWorkspaceFromApi(raw) {
        var s = String(raw || '').trim();
        if (!s) return '';
        if (s === 'Owner' || /^owner/i.test(s)) return 'Owner';
        if (s === 'Operator' || /^operator/i.test(s)) return 'Operator';
        if (s === 'Brand' || /^brand/i.test(s)) return 'Brand';
        return '';
    }

    function workspaceToNavRole(workspace) {
        var w = String(workspace || '').trim();
        if (w === 'Owner') return 'owner';
        if (w === 'Operator') return 'operator';
        if (w === 'Brand') return 'brand';
        return normalizeRoleForShell(w) || 'owner';
    }

    function navRoleToWorkspace(role) {
        var r = String(role || '').toLowerCase().replace(/_/g, '-');
        if (r === 'owner' || r === 'owner-operator') return 'Owner';
        if (r === 'operator') return 'Operator';
        if (r === 'brand') return 'Brand';
        return '';
    }

    /**
     * Render-only: use server canonicalWorkspaceOptions.
     * Do NOT rebuild a competing list from workspaceAccess / portfolio / storage.
     * Storage may select active workspace only if still in this list.
     */
    function buildSwitchableWorkspaces(dealality) {
        if (!dealality) return [];
        var canonical = dealality.canonicalWorkspaceOptions;
        if (canonical && Array.isArray(canonical.workspaces) && canonical.workspaces.length) {
            return ACTIVE_WORKSPACE_ORDER.filter(function (ws) {
                return canonical.workspaces.indexOf(ws) !== -1;
            });
        }
        // Production fallback when older /api/me omits canonical (no demo expansion).
        var access = Array.isArray(dealality.workspaceAccess) ? dealality.workspaceAccess : [];
        var allowed = {};
        ACTIVE_WORKSPACE_ORDER.forEach(function (ws) {
            if (access.indexOf(ws) !== -1) allowed[ws] = true;
        });
        var isOwnerOperator = !!(
            dealality.isOwnerOperator ||
            (dealality.flags && dealality.flags.isOwnerOperator)
        );
        if (isOwnerOperator) {
            allowed.Owner = true;
            allowed.Operator = true;
        }
        if (dealality.canAccessOwnerWorkspace) allowed.Owner = true;
        if (dealality.canAccessOperatorWorkspace) allowed.Operator = true;
        if (dealality.canAccessBrandWorkspace) allowed.Brand = true;
        if (!Object.keys(allowed).length) {
            var legacy = normalizeRoleForShell(
                dealality.legacyRole || dealality.role || dealality.primaryRole || ''
            );
            var fallbackWs = navRoleToWorkspace(legacy);
            if (fallbackWs) allowed[fallbackWs] = true;
        }
        return ACTIVE_WORKSPACE_ORDER.filter(function (ws) {
            return !!allowed[ws];
        });
    }

    function assertDemoWorkspaceConstellationForShell(dealality, workspaces) {
        if (!dealality) return;
        var expectsDemo = !!(
            (dealality.canonicalWorkspaceOptions &&
                dealality.canonicalWorkspaceOptions.DEMO_WORKSPACE_CONSTELLATION_EXPECTED) ||
            dealality.demoStakeholderMode ||
            dealality.isDemo ||
            (dealality.flags && dealality.flags.isDemo)
        );
        if (!expectsDemo) return;
        var hasBrand = workspaces && workspaces.indexOf('Brand') !== -1;
        var hasOwner = workspaces && workspaces.indexOf('Owner') !== -1;
        var hasOperator = workspaces && workspaces.indexOf('Operator') !== -1;
        if (hasBrand && hasOwner && hasOperator) return;
        if (!isDevMode) return;
        if (typeof console !== 'undefined' && console.error) {
            console.error(
                'DEMO_WORKSPACE_CONSTELLATION_INVALID',
                {
                    workspaces: workspaces || [],
                    source:
                        (dealality.canonicalWorkspaceOptions &&
                            dealality.canonicalWorkspaceOptions.source) ||
                        'missing_canonical',
                }
            );
        }
    }

    /**
     * Normalize auth payloads: whenReady().me is { ok, data };
     * dealality-shell-auth-ready detail is { ok, jwt, me, authorized }.
     */
    function normalizeMeResultPayload(raw) {
        if (!raw || typeof raw !== 'object') return null;
        if (raw.data && raw.data.dealality) return raw;
        if (raw.me && raw.me.data && raw.me.data.dealality) return raw.me;
        if (raw.dealality) {
            return { ok: true, data: { dealality: raw.dealality } };
        }
        return null;
    }

    function canShowFounderNavOverrides() {
        if (isDevMode) return true;
        if (!meDealality) return false;
        if (
            meDealality.canonicalWorkspaceOptions &&
            meDealality.canonicalWorkspaceOptions.founderNavOverridesAvailable === true
        ) {
            return true;
        }
        if (meDealality.founderNavOverridesAvailable === true) return true;
        if (meDealality.isAdmin === true) return true;
        if (meDealality.flags && meDealality.flags.isAdmin === true) return true;
        // Founder constellation: demo mode with Brand in the switchable set.
        if (
            (meDealality.demoStakeholderMode === true || meDealality.isDemo) &&
            switchableWorkspaces.indexOf('Brand') !== -1 &&
            switchableWorkspaces.indexOf('Owner') !== -1
        ) {
            return true;
        }
        return false;
    }

    function canShowDemoBrandPortfolioSelector() {
        if (!meDealality) return false;
        if (meDealality.demoBrandPortfolioSwitchAvailable === false) return false;
        if (
            !(
                meDealality.demoBrandPortfolioSwitchAvailable === true ||
                meDealality.demoStakeholderMode === true ||
                meDealality.isDemo ||
                canShowFounderNavOverrides()
            )
        ) {
            return false;
        }
        if (activeWorkspace === 'Brand') return true;
        if (devNavOverride === 'admin' || devNavOverride === 'all') return true;
        return false;
    }

    function getStoredDemoBrandPortfolio() {
        try {
            var stored =
                (window.localStorage && window.localStorage.getItem(DEMO_BRAND_PORTFOLIO_STORAGE_KEY)) ||
                '';
            stored = String(stored || '').trim().toLowerCase();
            if (stored === 'marriott' || stored === 'hilton' || stored === 'choice' || stored === 'ihg') {
                return stored;
            }
        } catch (_err) {}
        return '';
    }

    function setStoredDemoBrandPortfolio(key) {
        try {
            if (!window.localStorage) return;
            var normalized = String(key || '').trim().toLowerCase();
            if (
                normalized !== 'marriott' &&
                normalized !== 'hilton' &&
                normalized !== 'choice' &&
                normalized !== 'ihg'
            ) {
                window.localStorage.removeItem(DEMO_BRAND_PORTFOLIO_STORAGE_KEY);
                return;
            }
            window.localStorage.setItem(DEMO_BRAND_PORTFOLIO_STORAGE_KEY, normalized);
        } catch (_err) {}
    }

    function populateDemoBrandPortfolioSelect() {
        if (!devBrandPortfolioSelect) return;
        var options = demoBrandPortfolioOptions.slice();
        if (!options.length && meDealality && Array.isArray(meDealality.demoBrandPortfolioOptions)) {
            options = meDealality.demoBrandPortfolioOptions;
            demoBrandPortfolioOptions = options;
        }
        if (!options.length) {
            options = [
                { companyKey: 'marriott', label: 'Marriott International' },
                { companyKey: 'hilton', label: 'Hilton' },
                { companyKey: 'ihg', label: 'IHG' },
                { companyKey: 'choice', label: 'Choice Hotels' },
            ];
        }
        var html = '';
        options.forEach(function (opt) {
            var key = opt.companyKey || opt.id || '';
            var label = opt.label || opt.canonicalCompanyName || key;
            html +=
                '<option value="' +
                escapeHtml(key) +
                '">' +
                escapeHtml(label) +
                '</option>';
        });
        devBrandPortfolioSelect.innerHTML = html;
        var want = demoBrandPortfolioKey || getStoredDemoBrandPortfolio() || 'marriott';
        var keys = options.map(function (o) {
            return o.companyKey || o.id;
        });
        if (keys.indexOf(want) < 0) want = keys[0] || 'marriott';
        demoBrandPortfolioKey = want;
        setStoredDemoBrandPortfolio(want);
        devBrandPortfolioSelect.value = want;
    }

    function applyDemoBrandPortfolio(nextKey) {
        var key = String(nextKey || '').trim().toLowerCase();
        if (key !== 'marriott' && key !== 'hilton' && key !== 'choice' && key !== 'ihg') return;
        demoBrandPortfolioKey = key;
        setStoredDemoBrandPortfolio(key);
        // Clear Detailed View brand selection so stale cross-portfolio IDs cannot persist.
        try {
            sessionStorage.removeItem('aiv_brand_selected_brand');
        } catch (_err) {}
        syncWorkspaceSwitcherUi();
        // Must hard-reload the active embed. navigate() alone skips reload when the
        // iframe is already on this route — leaving Hilton brands in memory after Choice.
        forceReloadActiveEmbedForPortfolioSwitch();
    }

    function forceReloadActiveEmbedForPortfolioSwitch() {
        var target = currentRoute;
        if (!target || !ROUTES[target] || ROUTES[target].placeholder) {
            if (ROUTES[target]) navigate(target, currentRole, false);
            return;
        }
        var frame = getFrameForPath(target);
        var embedUrl = routeToEmbedUrl(target, currentRole);
        if (!frame) {
            navigate(target, currentRole, false);
            return;
        }
        try {
            var u = new URL(embedUrl, window.location.origin);
            u.searchParams.set('_demoPortfolio', demoBrandPortfolioKey || '');
            u.searchParams.set('_ts', String(Date.now()));
            embedUrl = u.pathname + u.search + u.hash;
        } catch (_err) {
            embedUrl =
                embedUrl +
                (embedUrl.indexOf('?') >= 0 ? '&' : '?') +
                '_demoPortfolio=' +
                encodeURIComponent(demoBrandPortfolioKey || '') +
                '&_ts=' +
                Date.now();
        }
        frame.removeAttribute('data-saved-src');
        frame.src = embedUrl;
        frame.addEventListener('load', function onPortfolioEmbedReload() {
            frame.removeEventListener('load', onPortfolioEmbedReload);
            broadcastJwtToActiveFrames();
            applyEmbeddedPageOverrides(frame, target, currentRole);
        });
        showFrame(target);
        updateShellHeader(target, currentRole);
        renderNav(currentRole, searchText);
    }

    function getStoredActiveWorkspace() {
        try {
            var stored = (window.localStorage && window.localStorage.getItem(ACTIVE_WORKSPACE_STORAGE_KEY)) || '';
            if (ACTIVE_WORKSPACE_ORDER.indexOf(stored) !== -1) return stored;
        } catch (_err) {
            // Ignore storage failures.
        }
        return '';
    }

    function setStoredActiveWorkspace(workspace) {
        try {
            if (!window.localStorage) return;
            if (!workspace || ACTIVE_WORKSPACE_ORDER.indexOf(workspace) === -1) {
                window.localStorage.removeItem(ACTIVE_WORKSPACE_STORAGE_KEY);
            } else {
                window.localStorage.setItem(ACTIVE_WORKSPACE_STORAGE_KEY, workspace);
            }
        } catch (_err) {
            // Ignore storage failures in restricted environments.
        }
    }

    function getStoredDevNavOverride() {
        try {
            var stored = (window.localStorage && window.localStorage.getItem(DEV_NAV_OVERRIDE_STORAGE_KEY)) || '';
            var normalized = String(stored || '').toLowerCase();
            return ALLOWED_DEV_NAV_OVERRIDES.indexOf(normalized) !== -1 ? normalized : '';
        } catch (_err) {
            return '';
        }
    }

    function setStoredDevNavOverride(value) {
        try {
            if (!window.localStorage) return;
            if (!value) {
                window.localStorage.removeItem(DEV_NAV_OVERRIDE_STORAGE_KEY);
            } else {
                window.localStorage.setItem(DEV_NAV_OVERRIDE_STORAGE_KEY, value);
            }
        } catch (_err) {
            // Ignore storage failures.
        }
    }

    function resolveActiveWorkspaceFromContext(dealality) {
        // Allowed list is canonical — never shrink it from storage.
        var list = buildSwitchableWorkspaces(dealality);
        var stored = getStoredActiveWorkspace();
        if (stored && list.indexOf(stored) !== -1) return stored;
        var serverActive = normalizeWorkspaceFromApi(dealality && dealality.activeWorkspace);
        if (serverActive && list.indexOf(serverActive) !== -1) return serverActive;
        return list[0] || 'Owner';
    }

    function shouldShowWorkspaceSwitcher() {
        if (showDemoModeBadge) return switchableWorkspaces.length > 0;
        return switchableWorkspaces.length > 1;
    }

    function resolveCurrentNavRole() {
        if (isDevMode && devNavOverride) return devNavOverride;
        if (canShowFounderNavOverrides() && devNavOverride) return devNavOverride;
        return workspaceToNavRole(activeWorkspace);
    }

    function getEffectiveRole() {
        return resolveCurrentNavRole();
    }

    /** Permissions / API — not the active workspace preview selection. */
    function getRouteAccessRole() {
        return authenticatedRole || currentBaseRole;
    }

    function isRouteAllowedForRole(route, role) {
        var meta = ROUTES[normalizeRoute(route)];
        if (!meta || !meta.roles || !meta.roles.length) return true;
        if (role === 'admin' || role === 'all') return true;
        if (meta.stakeholderProduct && window.DealalityStakeholderNav) {
            return window.DealalityStakeholderNav.stakeholderProductVisible(meta.stakeholderProduct, role);
        }
        return meta.roles.indexOf(role) !== -1;
    }

    /**
     * Shell navigation visibility follows active workspace (nav context).
     * API write gates still use workspaceAccess on the server — not this function.
     */
    function canNavigateToRoute(route) {
        var normalized = normalizeRoute(route);
        if (isInternalRunbookRoute(normalized) && !hasInternalRunbookNavAccess()) return false;
        if (normalized === '/ai-intelligence-validation' || normalized === '/ai-intelligence-golden-set-review') {
            return hasAdminNavAccess() || canShowFounderNavOverrides() || isDevMode;
        }
        if (isAdminExclusiveRoute(normalized) && !hasAdminNavAccess()) return false;
        var navRole = resolveCurrentNavRole();
        if (isRouteAllowedForRole(normalized, navRole)) return true;
        if (hasAdminNavAccess() && isRouteAllowedForRole(normalized, 'admin')) return true;
        if (isDevMode && devNavOverride === 'all' && !isAdminExclusiveRoute(normalized) && !isInternalRunbookRoute(normalized)) return true;
        if (canShowFounderNavOverrides() && devNavOverride === 'all' && !isAdminExclusiveRoute(normalized) && !isInternalRunbookRoute(normalized)) return true;
        return false;
    }

    function redirectRouteWhenBlocked(route) {
        var normalized = normalizeRoute(route);
        if (normalized === '/support/owner-pilot-provisioning') return '/support';
        if (normalized === '/support/scoring-weight-model') return '/support';
        return getLandingRouteForNavRole(resolveCurrentNavRole());
    }

    function getLandingRouteForNavRole(role) {
        if (role === 'brand') return '/brand-development-dashboard';
        if (role === 'operator') return '/operator-development-dashboard';
        return '/home';
    }

    function normalizeRoleForShell(rawRole) {
        var nextRole = String(rawRole || '').toLowerCase().replace(/_/g, '-');
        if (nextRole === 'owner-operator') return 'owner';
        if (ALLOWED_ROLES.indexOf(nextRole) === -1) return '';
        return nextRole;
    }

    function applyRoleFromMe(meResultRaw) {
        var meResult = normalizeMeResultPayload(meResultRaw);
        if (!meResult || !meResult.ok || !meResult.data || !meResult.data.dealality) return false;
        var dealality = meResult.data.dealality;
        meDealality = dealality;
        meContextLoaded = true;
        switchableWorkspaces = buildSwitchableWorkspaces(dealality);
        assertDemoWorkspaceConstellationForShell(dealality, switchableWorkspaces);
        showDemoModeBadge = !!(
            dealality.isDemo ||
            (dealality.flags && dealality.flags.isDemo) ||
            dealality.demoStakeholderMode
        );
        if (Array.isArray(dealality.demoBrandPortfolioOptions)) {
            demoBrandPortfolioOptions = dealality.demoBrandPortfolioOptions;
        }
        var storedPortfolio = getStoredDemoBrandPortfolio();
        if (storedPortfolio) {
            demoBrandPortfolioKey = storedPortfolio;
        } else if (dealality.demoBrandPortfolioKey) {
            demoBrandPortfolioKey = String(dealality.demoBrandPortfolioKey).toLowerCase();
            setStoredDemoBrandPortfolio(demoBrandPortfolioKey);
        } else if (!demoBrandPortfolioKey) {
            demoBrandPortfolioKey = 'marriott';
            setStoredDemoBrandPortfolio(demoBrandPortfolioKey);
        }
        activeWorkspace = resolveActiveWorkspaceFromContext(dealality);
        setStoredActiveWorkspace(activeWorkspace);
        if (!canShowFounderNavOverrides()) {
            devNavOverride = '';
            setStoredDevNavOverride('');
        } else if (!devNavOverride) {
            devNavOverride = getStoredDevNavOverride() || '';
        }
        var nextRole = normalizeRoleForShell(
            dealality.legacyRole || dealality.role || dealality.primaryRole || ''
        );
        if (nextRole) {
            authenticatedRole = nextRole;
            currentBaseRole = nextRole;
        } else if (switchableWorkspaces.length) {
            authenticatedRole = workspaceToNavRole(activeWorkspace);
            currentBaseRole = authenticatedRole;
        } else {
            return false;
        }
        currentRole = resolveCurrentNavRole();
        syncWorkspaceSwitcherUi();
        return true;
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
        if (role === 'brand') return '/brand-development-dashboard';
        if (role === 'operator') return '/operator-development-dashboard';
        return '/home';
    }

    function getPath(role) {
        var hash = window.location.hash.slice(1);
        return hash ? normalizeRoute(hash) : getLandingRouteForNavRole(role);
    }

    function appendMsTokenToEmbedUrl(url) {
        try {
            var jwt = window.__dealalityMemberstackJwt;
            if (!jwt) return url;
            var u = new URL(url, window.location.origin);
            if (!u.searchParams.get('msToken')) {
                u.searchParams.set('msToken', jwt);
            }
            return u.pathname + u.search + (u.hash || '');
        } catch (_err) {
            return url;
        }
    }

    function broadcastJwtToActiveFrames() {
        var jwt = window.__dealalityMemberstackJwt;
        if (!jwt || !window.DealalityEmbedParent || typeof window.DealalityEmbedParent.publishJwt !== 'function') {
            return;
        }
        window.DealalityEmbedParent.publishJwt(jwt);
    }

    function getHashQueryParams() {
        var hash = window.location.hash.slice(1);
        var qIdx = hash.indexOf('?');
        if (qIdx < 0) return new URLSearchParams('');
        return new URLSearchParams(hash.slice(qIdx + 1));
    }

    function routeToEmbedUrl(route, role) {
        var embedQs = 'embed=1&appShell=1';
        if (route === '/market-alerts') {
            embedQs += '&v=' + MARKET_ALERTS_EMBED_VERSION;
        }
        if (route === '/opportunity-radar' && role === 'operator') {
            return appendMsTokenToEmbedUrl('/operator-intelligence-radar-with-list.html?' + embedQs);
        }
        var mapped = ROUTES[route];
        if (!mapped || !mapped.file) return appendMsTokenToEmbedUrl('/app/home.html?' + embedQs);
        var sep = mapped.file.indexOf('?') === -1 ? '?' : '&';
        var url = mapped.file + sep + embedQs;
        try {
            var hashQs = getHashQueryParams();
            var u = new URL(url, window.location.origin);
            ['recordId', 'operatorId', 'id'].forEach(function (key) {
                var v = hashQs.get(key);
                if (v && !u.searchParams.get(key)) u.searchParams.set(key, v);
            });
            url = u.pathname + u.search;
        } catch (_hashQsErr) {
            /* keep base embed URL */
        }
        return appendMsTokenToEmbedUrl(url);
    }

    /** Pathname the shell iframe should show for this route (matches routeToEmbedUrl, including operator radar exception). */
    function getEmbedPathnameForRoute(route, role) {
        var rel = routeToEmbedUrl(route, role).split('?')[0];
        if (rel.charAt(0) !== '/') rel = '/' + rel;
        return rel;
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
                + 'font-family:inherit !important;font-size:inherit !important;font-style:inherit !important;font-weight:inherit !important;line-height:inherit !important;letter-spacing:inherit !important;color:inherit !important;}'
                + '/* Help / SVG-adjacent popovers — same surfaces as Brand Workspace (secondary--1 + neutral--600 border) */'
                + '.input-help:hover::after,.input-help:focus-visible::after{'
                + 'background:#101935!important;border:1px solid #37446b!important;color:#f0f4ff!important;'
                + 'box-shadow:0 8px 24px rgba(0,0,0,0.45)!important;}'
                + '.score-info-icon::after{'
                + 'background:#101935!important;border:1px solid #37446b!important;color:#f0f4ff!important;'
                + 'box-shadow:0 8px 20px rgba(0,0,0,0.4)!important;}'
                + '.info-tooltip .tooltip-content,.tooltip-content{'
                + 'background:#101935!important;border:1px solid #37446b!important;color:#f0f4ff!important;}'
                + '#partnerDirectoryTooltipContainer .tooltip-content{'
                + 'background:#101935!important;border:1px solid #37446b!important;color:#f0f4ff!important;}'
                + '#tooltipContainer .tooltip-content{'
                + 'background:#101935!important;border:1px solid #37446b!important;color:#f0f4ff!important;}'
                + '.tooltip-close-btn{background:#37446b!important;color:#fff!important;border:none!important;}'
                + '.tooltip-close-btn:hover{background:#7e89ac!important;color:#fff!important;}';
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

    function isNavRouteChild(child) {
        return !!(child && child.type !== 'divider' && child.route);
    }

    function isNavChildVisible(role, child) {
        if (!isNavRouteChild(child)) return false;
        if (child.validationScorecard) {
            return hasAdminNavAccess() || canShowFounderNavOverrides() || isDevMode;
        }
        if (child.stakeholderProduct && window.DealalityStakeholderNav) {
            return window.DealalityStakeholderNav.stakeholderProductVisible(child.stakeholderProduct, role);
        }
        return canSee(role, child.roles, child.devOnly, child.internalRunbookOnly);
    }

    function renderNavGroupChildren(children, role) {
        var html = '';
        var visibleBeforeDivider = false;
        var pendingDivider = false;

        (children || []).forEach(function (child) {
            if (child.type === 'divider') {
                pendingDivider = true;
                return;
            }
            if (!isNavChildVisible(role, child)) return;
            if (pendingDivider) {
                if (visibleBeforeDivider) {
                    html += '<div class="nav-dropdown-divider"></div>';
                }
                pendingDivider = false;
            }
            visibleBeforeDivider = true;
            html += '<button type="button" class="nav-item nav-item-child' + (child.route === currentRoute ? ' active' : '') + '" data-href="' + child.route + '">' +
                '<span class="nav-label">' + child.label + '</span>' +
            '</button>';
        });
        return html;
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
                    if (!isNavRouteChild(child)) return;
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
        var workspaceLabel = formatWorkspaceUiLabel(activeRole);
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
                    if (!canSee(role, entry.roles, entry.devOnly, entry.internalRunbookOnly)) return;
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

                var visibleChildren = (entry.children || []).filter(function (child) {
                    return isNavChildVisible(role, child);
                });
                if (!visibleChildren.length) return;
                if (!passesSearch(entry.label, visibleChildren.map(function (c) { return c.label; }), query)) return;

                if (visibleChildren.length === 1) {
                    var only = visibleChildren[0];
                    sectionParts.push(
                        '<button type="button" class="nav-item' + (currentRoute === only.route ? ' active' : '') + '" data-href="' + only.route + '">' +
                            '<span class="nav-icon" aria-hidden="true">' + entry.icon + '</span>' +
                            '<span class="nav-label">' + only.label + '</span>' +
                            '<span class="nav-arrow">&gt;</span>' +
                        '</button>'
                    );
                    return;
                }

                var groupId = entry.label.toLowerCase().replace(/[^a-z0-9]+/g, '-');
                var hasActiveChild = visibleChildren.some(function (child) { return child.route === currentRoute; });
                if (hasActiveChild && !openGroups[groupId]) openGroups[groupId] = true;
                var isOpen = !!openGroups[groupId];

                var childrenHtml = renderNavGroupChildren(entry.children, role);
                if (!childrenHtml) return;

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
            if (shouldShowWorkspaceSwitcher() || showDemoModeBadge) {
                shellPageSubtitle.style.display = '';
                shellPageSubtitle.textContent = formatWorkspaceUiLabel(activeWorkspace);
            } else {
                shellPageSubtitle.textContent = '';
                shellPageSubtitle.style.display = 'none';
            }
        }
    }

    var WORKSPACE_DASHBOARD_ROUTES = {
        '/brand-development-dashboard': true,
        '/operator-development-dashboard': true,
    };

    function showFrame(route) {
        if (!frameContainer) return;
        frameContainer.querySelectorAll('.app-frame').forEach(function (frame) {
            var framePath = frame.getAttribute('data-path');
            var isActive = framePath === route;
            frame.classList.toggle('active', isActive);
            if (!isActive && WORKSPACE_DASHBOARD_ROUTES[framePath]) {
                try {
                    if (frame.src && frame.src.indexOf('about:blank') === -1) {
                        frame.setAttribute('data-saved-src', frame.src);
                        frame.src = 'about:blank';
                    }
                } catch (_blankErr) {
                    /* ignore cross-origin src read */
                }
            }
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
        var rawRoute = route || getDefaultRoute(getRouteAccessRole());
        if (typeof rawRoute !== 'string') rawRoute = String(rawRoute);
        if (rawRoute.charAt(0) !== '/') rawRoute = '/' + rawRoute;
        var routeQuery = '';
        var routeQueryIdx = rawRoute.indexOf('?');
        if (routeQueryIdx >= 0) {
            routeQuery = rawRoute.slice(routeQueryIdx);
            rawRoute = rawRoute.slice(0, routeQueryIdx);
        }
        var normalized = normalizeRoute(rawRoute);
        if (!canNavigateToRoute(normalized)) {
            normalized = redirectRouteWhenBlocked(normalized);
        }
        var target = ROUTES[normalized] ? normalized : getLandingRouteForNavRole(resolveCurrentNavRole());
        currentRoute = target;

        var hashRoute = target + routeQuery;
        if (updateHash !== false && window.location.hash !== '#' + hashRoute) {
            window.location.hash = hashRoute;
        }

        var frame = getFrameForPath(target);
        var embedUrl = routeToEmbedUrl(target, role);
        if (!frame) {
            if (ROUTES[target].placeholder) {
                frame = ensurePlaceholder(target);
            } else {
                frame = document.createElement('iframe');
                frame.className = 'app-frame';
                frame.setAttribute('data-path', target);
                frame.title = ROUTES[target].title || 'Page content';
                frame.src = embedUrl;
                frame.addEventListener('load', function () {
                    broadcastJwtToActiveFrames();
                    applyEmbeddedPageOverrides(frame, target, role);
                });
                frameContainer.appendChild(frame);
            }
        } else if (target === '/route-map') {
            frame.srcdoc = renderRouteMapHtml(role);
        } else {
            /*
             * Embedded list pages often do in-frame navigation (e.g. my-deals → deal-summary via
             * location.href) without changing this iframe's data-path. Hash can stay #/my-deals while
             * the child document is no longer my-deals.html — reload when pathname does not match.
             */
            var expectedPath = getEmbedPathnameForRoute(target, role);
            var mustReloadFrame = false;
            var savedSrc = frame.getAttribute('data-saved-src');
            if (savedSrc) {
                mustReloadFrame = true;
            } else if (frame.src && frame.src.indexOf('about:blank') !== -1) {
                mustReloadFrame = true;
            } else if (expectedPath && frame.contentWindow) {
                try {
                    var childLoc = frame.contentWindow.location;
                    if (childLoc.pathname !== expectedPath) {
                        mustReloadFrame = true;
                    } else {
                        var expectedUrlObj = new URL(embedUrl, window.location.origin);
                        var childQs = new URLSearchParams(childLoc.search || '');
                        var expectedRecordId = expectedUrlObj.searchParams.get('recordId') || '';
                        var childRecordId = childQs.get('recordId') || '';
                        if (expectedRecordId !== childRecordId) {
                            mustReloadFrame = true;
                        }
                    }
                } catch (_crossOrigin) {
                    /* Assume in sync if we cannot read the child (should not happen for same-origin embeds). */
                }
            }
            if (mustReloadFrame) {
                frame.removeAttribute('data-saved-src');
                frame.src = embedUrl;
                frame.addEventListener('load', function onShellEmbedRouteLoad() {
                    frame.removeEventListener('load', onShellEmbedRouteLoad);
                    broadcastJwtToActiveFrames();
                    applyEmbeddedPageOverrides(frame, target, role);
                });
            } else {
                applyEmbeddedPageOverrides(frame, target, role);
            }
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
                    closeAccountDropdown(true);
                });
            });
        }

        function closeAccountDropdown(returnFocus) {
            if (accountWrap) accountWrap.classList.remove('open');
            if (accountTrigger) {
                accountTrigger.setAttribute('aria-expanded', 'false');
                if (returnFocus) accountTrigger.focus();
            }
            if (accountMenu) accountMenu.setAttribute('aria-hidden', 'true');
        }

        if (accountLogout) {
            accountLogout.addEventListener('click', function (e) {
                e.preventDefault();
                closeAccountDropdown(true);
                if (window.DealalityAppShellAuth && typeof window.DealalityAppShellAuth.logout === 'function') {
                    window.DealalityAppShellAuth.logout();
                    return;
                }
                window.location.reload();
            });
        }

        document.addEventListener('click', function (e) {
            if (accountWrap && accountWrap.classList.contains('open') && !accountWrap.contains(e.target)) {
                closeAccountDropdown(false);
            }
        });
    }

    function populateWorkspaceSelectOptions() {
        if (!devWorkspaceSelect) return;
        devWorkspaceSelect.innerHTML = '';
        switchableWorkspaces.forEach(function (ws) {
            var opt = document.createElement('option');
            opt.value = ws;
            opt.textContent = formatWorkspaceUiLabel(ws);
            devWorkspaceSelect.appendChild(opt);
        });
        devWorkspaceSelect.value = activeWorkspace;
    }

    function syncWorkspaceSwitcherUi() {
        if (!devWorkspaceWrap) return;
        var visible = shouldShowWorkspaceSwitcher();
        devWorkspaceWrap.hidden = !visible;
        if (!visible) return;
        populateWorkspaceSelectOptions();
        if (devWorkspaceWrap) {
            devWorkspaceWrap.setAttribute('data-workspace', workspaceToNavRole(activeWorkspace));
        }
        if (workspaceDemoContext) workspaceDemoContext.hidden = !showDemoModeBadge;
        var showFounderOverrides = canShowFounderNavOverrides();
        if (workspaceDevTools) workspaceDevTools.hidden = !showFounderOverrides;
        if (devNavOverrideSelect && showFounderOverrides) {
            devNavOverrideSelect.value = devNavOverride || '';
        }
        var showPortfolio = canShowDemoBrandPortfolioSelector();
        if (workspaceDemoBrandPortfolio) {
            workspaceDemoBrandPortfolio.hidden = !showPortfolio;
            if (showPortfolio) populateDemoBrandPortfolioSelect();
        }
        var labelEl = document.querySelector('.workspace-switcher-label, .dev-workspace-label');
        if (labelEl) labelEl.textContent = 'Workspace';
    }

    function applyActiveWorkspace(nextWorkspace) {
        if (ACTIVE_WORKSPACE_ORDER.indexOf(nextWorkspace) === -1) return;
        if (switchableWorkspaces.indexOf(nextWorkspace) === -1) return;
        activeWorkspace = nextWorkspace;
        setStoredActiveWorkspace(activeWorkspace);
        if (!devNavOverride) {
            currentRole = workspaceToNavRole(activeWorkspace);
        }
        syncWorkspaceSwitcherUi();
        renderNav(currentRole, searchText);
        updateShellHeader(currentRoute, currentRole);
        if (!canNavigateToRoute(currentRoute)) {
            navigate(redirectRouteWhenBlocked(currentRoute), currentRole, true);
        } else if (ROUTES[currentRoute]) {
            navigate(currentRoute, currentRole, false);
        } else {
            navigate(redirectRouteWhenBlocked(currentRoute), currentRole, true);
        }
    }

    function applyDevNavOverride(nextOverride) {
        var normalized = String(nextOverride || '').toLowerCase();
        if (normalized && ALLOWED_DEV_NAV_OVERRIDES.indexOf(normalized) === -1) normalized = '';
        devNavOverride = normalized;
        setStoredDevNavOverride(canShowFounderNavOverrides() ? devNavOverride : '');
        currentRole = resolveCurrentNavRole();
        syncWorkspaceSwitcherUi();
        renderNav(currentRole, searchText);
        if (!canNavigateToRoute(currentRoute)) {
            navigate(redirectRouteWhenBlocked(currentRoute), currentRole, true);
        } else {
            navigate(currentRoute, currentRole, false);
        }
    }

    function initWorkspaceSwitcher() {
        if (!devWorkspaceWrap || !devWorkspaceSelect) return;
        var storedPortfolio = getStoredDemoBrandPortfolio();
        if (storedPortfolio) demoBrandPortfolioKey = storedPortfolio;
        syncWorkspaceSwitcherUi();
        devWorkspaceSelect.addEventListener('change', function () {
            var selected = devWorkspaceSelect.value || '';
            if (ACTIVE_WORKSPACE_ORDER.indexOf(selected) !== -1) {
                applyActiveWorkspace(selected);
            }
        });
        if (devNavOverrideSelect) {
            devNavOverrideSelect.addEventListener('change', function () {
                applyDevNavOverride(devNavOverrideSelect.value || '');
            });
        }
        if (devBrandPortfolioSelect) {
            devBrandPortfolioSelect.addEventListener('change', function () {
                applyDemoBrandPortfolio(devBrandPortfolioSelect.value || '');
            });
        }
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

    var shellNavStarted = false;

    function startShellNavigation() {
        if (shellNavStarted) return;
        shellNavStarted = true;
        navigate(getPath(currentRole), currentRole, false);
        window.addEventListener('hashchange', function () {
            navigate(getPath(currentRole), currentRole, false);
        });
    }

    function init() {
        applyChromelessShellIfEmbedded();

        var copyrightYearEl = document.getElementById('appShellCopyrightYear');
        if (copyrightYearEl) copyrightYearEl.textContent = String(new Date().getFullYear());

        isDevMode = computeDevMode();
        currentBaseRole = getBaseRole();
        activeWorkspace = navRoleToWorkspace(currentBaseRole) || 'Owner';
        // Restore stored override; cleared later if founder/demo/admin overrides are not allowed.
        devNavOverride = getStoredDevNavOverride() || '';
        if (!isDevMode && !canShowFounderNavOverrides()) {
            devNavOverride = '';
        }
        currentRole = resolveCurrentNavRole();

        initNavEvents();
        initWorkspaceSwitcher();
        initSidebarControls();
        initShellMessageListener();
        /* Same-origin iframes (deal-setup, new-deal-setup, etc.) call this so navigation works even if postMessage is flaky. */
        window.dealalityAppShellNavigate = function (path) {
            if (typeof path !== 'string') return;
            try {
                navigate(path, currentRole, true);
            } catch (_err) {
                // Ignore — embed may call during teardown.
            }
        };

        window.addEventListener('dealality-shell-auth-ready', function (e) {
            if (applyRoleFromMe(e.detail)) {
                currentRole = resolveCurrentNavRole();
                renderNav(currentRole, searchText);
                var path = getPath(currentRole);
                if (!canNavigateToRoute(path)) {
                    navigate(redirectRouteWhenBlocked(path), currentRole, true);
                }
            }
            broadcastJwtToActiveFrames();
            startShellNavigation();
        });

        window.addEventListener('dealality-shell-auth-logout', function () {
            meDealality = null;
            meContextLoaded = false;
            if (frameContainer) {
                frameContainer.querySelectorAll('.app-frame').forEach(function (frame) {
                    try {
                        frame.src = 'about:blank';
                    } catch (_err) {}
                });
            }
        });

        if (window.DealalityAppShellAuth && typeof window.DealalityAppShellAuth.whenReady === 'function') {
            window.DealalityAppShellAuth.whenReady().then(function (result) {
                if (result && result.ok) {
                    if (applyRoleFromMe(result.me)) {
                        currentRole = resolveCurrentNavRole();
                    }
                    renderNav(currentRole, searchText);
                    broadcastJwtToActiveFrames();
                    var path = getPath(currentRole);
                    if (!canNavigateToRoute(path)) {
                        navigate(redirectRouteWhenBlocked(path), currentRole, true);
                    } else {
                        startShellNavigation();
                    }
                }
            });
        } else {
            startShellNavigation();
        }
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
