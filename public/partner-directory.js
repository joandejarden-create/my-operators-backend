// Partner Directory JavaScript - Direct Airtable API Integration
// Version: 2025-01-29 - Airtable Favorites Integration
// ============================================================================
// CONFIGURATION - UPDATE THESE VALUES BEFORE DEPLOYMENT
// ============================================================================
// 
// For LOCAL DEVELOPMENT: The config will be automatically loaded from the server.
// For PRODUCTION/WEBFLOW: Update the values below with your actual Airtable credentials.
//
// INSTRUCTIONS (for production/Webflow deployment):
// 1. Get your Airtable API Key: https://airtable.com/create/tokens
//    - Create a token with read access to your base
//    - Copy the token (starts with 'pat...')
// 
// 2. Get your Airtable Base ID: https://airtable.com/api
//    - Select your base
//    - Copy the Base ID (starts with 'app...')
//
// 3. Update the values below:
//
// Required CONFIG shape: AIRTABLE_API_KEY, AIRTABLE_BASE_ID, table IDs, CACHE_TTL, MAX_RECORDS.
// All Airtable data is fetched via https://api.airtable.com/v0/${CONFIG.AIRTABLE_BASE_ID}/...
// Optional: /api/partner-directory/config is only for loading this config from the server (local dev); for standalone/production set values below.
// CONFIG: Airtable credentials – all fetches use https://api.airtable.com/v0/${CONFIG.AIRTABLE_BASE_ID}/...
const CONFIG = {
    AIRTABLE_API_KEY: 'YOUR_AIRTABLE_API_KEY_HERE',
    AIRTABLE_BASE_ID: 'YOUR_AIRTABLE_BASE_ID_HERE',
    AIRTABLE_TABLE_NAME: 'Company_Profile',
    CACHE_TTL: 5 * 60 * 1000,
    MAX_RECORDS: 50000
};
const PARTNER_DIRECTORY_CONFIG = {
    ...CONFIG,
    COMPANY_PROFILE_TABLE_ID: 'tblItyfH6MlOnMKZ9',
    USERS_TABLE_ID: 'tbl6shiyz2wdUqE5F',
    USER_MANAGEMENT_TABLE_ID: 'tblQEpYKf2aYNKKjw',
    USER_FAVORITES_TABLE_ID: '',
    MAX_RECORDS_PER_REQUEST: 100
};

// Set localStorage.PARTNER_DIRECTORY_DEBUG = 'true' in browser console to enable verbose logs (e.g. for debugging fetch/format). Reload after setting.
function isPartnerDirectoryDebug() {
    try { return localStorage.getItem('PARTNER_DIRECTORY_DEBUG') === 'true'; } catch (e) { return false; }
}

// Load config from server for local development (falls back to hardcoded config for production)
async function loadConfig() {
    try {
        const response = await fetch('/api/partner-directory/config');
        if (response.ok) {
            const serverConfig = await response.json();
            Object.assign(PARTNER_DIRECTORY_CONFIG, serverConfig);
            return true;
        }
    } catch (error) {
        // Server endpoint not available (e.g., in Webflow/production)
        // Will use hardcoded config below
    }
    
    // Validate hardcoded config for production
    if (PARTNER_DIRECTORY_CONFIG.AIRTABLE_API_KEY === 'YOUR_AIRTABLE_API_KEY_HERE' || 
        PARTNER_DIRECTORY_CONFIG.AIRTABLE_BASE_ID === 'YOUR_AIRTABLE_BASE_ID_HERE') {
        console.warn('⚠️ Configuration Required: Please update PARTNER_DIRECTORY_CONFIG with your actual Airtable credentials.');
        return false;
    }
    
    return true;
}

// Partner Directory JavaScript
class PartnerDirectory {
    constructor() {
        this.companies = [];
        this.individuals = [];
        this.filteredCompanies = [];
        this.filteredIndividuals = [];
        this.companyUserTypesMap = {}; // Map of company record IDs to userTypes for color coding
        this.currentUserTypeFilter = '';
        this.currentRegionFilter = '';
        this.currentResponsivenessSpeedFilter = '';
        this.currentResponsivenessFrequencyFilter = '';
        this.currentSearchQuery = '';
        this.currentSort = 'name-asc';
        this.currentTab = 'companies';
        this.favorites = []; // Will be loaded from Airtable API
        this.currentFavoriteCategory = 'all'; // Filter favorites by category
        this.insightsCharts = {}; // Store chart instances
        this.currentUserId = this.getCurrentUserId(); // Get current user ID
        this.favoritesMap = new Map(); // Map for quick lookup: key = "type-id", value = favorite object
        // TTL cache for partner list (uses PARTNER_DIRECTORY_CONFIG.CACHE_TTL)
        this.PARTNERS_CACHE_TTL_MS = PARTNER_DIRECTORY_CONFIG.CACHE_TTL || 5 * 60 * 1000;
        this.partnersCache = null; // { companies, individuals, timestamp }
        
        // Caching for modal data to improve performance
        this.teamMembersCache = new Map(); // Cache: companyId -> teamMembers array
        this.brandNamesCache = new Map(); // Cache: recordId -> brandName
        
        // Cache for rendered cards to prevent image reloading
        this.renderedCardsCache = new Map(); // Cache: "type-id" -> DOM element
        
        // Per-tab DOM cache: one wrapper per tab so switching tabs only show/hide (no re-render, preserves scroll)
        this.tabContainers = { companies: null, individuals: null, favorites: null };
        
        // Caching for modal data to improve performance
        this.teamMembersCache = new Map(); // Cache: companyId -> teamMembers array
        this.brandNamesCache = new Map(); // Cache: recordId -> brandName
        
        // Field mapping constants - maps JavaScript property names to Airtable field names
        // This ensures consistency between display and any future edit forms
        this.FIELD_MAPPING = {
            // Individual/User fields (from formatIndividualRecord return object)
            firstName: 'First Name',
            lastName: 'Last Name',
            companyTitle: 'Company Title',
            phoneNumber: 'Phone Number',
            email: 'Email', // Note: Airtable uses "Email", not "Company Email" or "companyEmail"
            userType: 'User Type',
            location: 'Country', // Primary location field (also checks "Location")
            website: 'Website', // Also checks "Personal Website"
            closedDeals: 'Closed Deals',
            brandCount: 'Brand Count',
            submittedBids: 'Submitted Bids',
            regions: 'Regions', // Can be array or checkbox fields
            companyName: 'Company Name', // From Company Profile linked record
            companyRecordId: null, // Not a field, but stored for lookups
            profilePicture: 'Profile' // Or 'Profile Picture', 'Headshot', 'Photo'
        };
        
        this.init();
    }

    async init() {
        this.setupEventListeners();
        this.setupResponsivenessTooltips();
        this.updateFilterCount();
        // Load partners and favorites in parallel for faster initial load
        await Promise.all([this.fetchPartners(), this.loadFavorites()]);
        this.updateStats();
        this.applyFilters();
        // Setup lazy image loading observer
        this.setupLazyImageLoading();
    }

    setupResponsivenessTooltips() {
        const container = document.getElementById('partnerDirectoryTooltipContainer');
        if (!container) return;
        const wrapper = document.getElementById('responsivenessFiltersWrapper');
        if (!wrapper) return;
        const icons = wrapper.querySelectorAll('.info-icon');
        icons.forEach(icon => {
            icon.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                const tooltip = icon.closest('.info-tooltip');
                const tooltipContent = tooltip?.querySelector('.tooltip-content');
                if (!tooltipContent) return;
                container.innerHTML = '';
                const clone = tooltipContent.cloneNode(true);
                clone.style.visibility = 'visible';
                clone.style.opacity = '1';
                clone.style.display = 'block';
                clone.style.position = 'relative';
                clone.style.pointerEvents = 'auto';
                clone.style.background = '#101935';
                clone.style.color = '#ffffff';
                clone.style.borderRadius = '8px';
                clone.style.padding = '12px 14px';
                clone.style.border = '1px solid #343b4e';
                clone.style.boxShadow = '0 8px 25px rgba(0,0,0,0.5)';
                clone.style.width = '320px';
                clone.style.maxWidth = 'calc(100vw - 40px)';
                clone.style.fontSize = '12px';
                clone.style.lineHeight = '1.5';
                clone.style.paddingRight = '36px';
                const closeBtn = document.createElement('button');
                closeBtn.className = 'tooltip-close-btn';
                closeBtn.innerHTML = '×';
                closeBtn.type = 'button';
                closeBtn.addEventListener('click', (ev) => {
                    ev.preventDefault();
                    ev.stopPropagation();
                    container.innerHTML = '';
                });
                clone.appendChild(closeBtn);
                container.appendChild(clone);
            });
        });
        document.addEventListener('click', (e) => {
            if (!e.target.closest('.responsiveness-info-tooltip') && !e.target.closest('#partnerDirectoryTooltipContainer')) {
                container.innerHTML = '';
            }
        });
    }

    setupEventListeners() {
        // Tab switching
        document.getElementById('companiesTab')?.addEventListener('click', () => {
            this.switchTab('companies');
        });

        document.getElementById('individualsTab')?.addEventListener('click', () => {
            this.switchTab('individuals');
        });

        document.getElementById('favoritesTab')?.addEventListener('click', () => {
            this.switchTab('favorites');
        });

        document.getElementById('insightsTab')?.addEventListener('click', () => {
            this.switchTab('insights');
        });

        // Favorites category filter
        const favoritesCategoryFilter = document.getElementById('favoritesCategoryFilter');
        if (favoritesCategoryFilter) {
            favoritesCategoryFilter.addEventListener('change', (e) => {
                this.currentFavoriteCategory = e.target.value;
                this.invalidateAllTabCaches();
                this.renderResults();
            });
        }

        // Insights filters
        document.getElementById('insightsDateRange')?.addEventListener('change', () => {
            this.updateInsightsFilterCount();
            this.updateInsights();
        });
        document.getElementById('insightsRegion')?.addEventListener('change', () => {
            this.updateInsightsFilterCount();
            this.updateInsights();
        });
        document.getElementById('insightsType')?.addEventListener('change', () => {
            this.updateInsightsFilterCount();
            this.updateInsights();
        });
        document.getElementById('insightsPartnerType')?.addEventListener('change', () => {
            this.updateInsightsFilterCount();
            this.updateInsights();
        });

        // Insights reset button
        document.getElementById('resetInsightsBtn')?.addEventListener('click', (e) => {
            e.preventDefault();
            this.resetInsightsFilters();
        });

        // Filter button clicks - use event delegation to handle dynamically added buttons
        document.addEventListener('click', (e) => {
            if (e.target.closest('.filter-button')) {
                const btn = e.target.closest('.filter-button');
                const type = btn.dataset.type;
                this.toggleFilterButton(type);
            }
        });

        // Filter dropdown changes
        document.getElementById('userTypeFilter')?.addEventListener('change', (e) => {
            this.currentUserTypeFilter = e.target.value;
            this.updateFilterButtons();
            this.invalidateAllTabCaches();
            this.updateFilterCount();
            this.applyFilters();
        });

        document.getElementById('regionFilter')?.addEventListener('change', (e) => {
            this.currentRegionFilter = e.target.value;
            this.invalidateAllTabCaches();
            this.updateFilterCount();
            this.applyFilters();
        });

        document.getElementById('responsivenessSpeedFilter')?.addEventListener('change', (e) => {
            this.currentResponsivenessSpeedFilter = (e.target.value || '').trim();
            this.invalidateAllTabCaches();
            this.updateFilterCount();
            this.applyFilters();
        });

        document.getElementById('responsivenessFrequencyFilter')?.addEventListener('change', (e) => {
            this.currentResponsivenessFrequencyFilter = (e.target.value || '').trim();
            this.invalidateAllTabCaches();
            this.updateFilterCount();
            this.applyFilters();
        });

        document.getElementById('resetFiltersBtn')?.addEventListener('click', (e) => {
            e.preventDefault();
            this.resetFilters();
        });

        // Search input
        document.getElementById('searchInput')?.addEventListener('input', (e) => {
            this.currentSearchQuery = e.target.value.trim().toLowerCase();
            this.invalidateAllTabCaches();
            this.applyFilters();
        });

        // Sort dropdown
        document.getElementById('sortSelect')?.addEventListener('change', (e) => {
            this.currentSort = e.target.value;
            this.invalidateAllTabCaches();
            this.applyFilters();
        });

        // Sort icon - toggle between A-Z and Z-A
        document.getElementById('sortIcon')?.addEventListener('click', () => {
            const sortSelect = document.getElementById('sortSelect');
            if (sortSelect) {
                const currentValue = sortSelect.value;
                // Toggle between name-asc and name-desc
                if (currentValue === 'name-asc') {
                    this.currentSort = 'name-desc';
                    sortSelect.value = 'name-desc';
                } else if (currentValue === 'name-desc') {
                    this.currentSort = 'name-asc';
                    sortSelect.value = 'name-asc';
                } else {
                    // If sorting by something else, default to name-asc
                    this.currentSort = 'name-asc';
                    sortSelect.value = 'name-asc';
                }
                this.invalidateAllTabCaches();
                this.applyFilters();
            }
        });

        // Company modal close
        document.getElementById('companyModalClose')?.addEventListener('click', () => {
            this.closeCompanyModal();
        });

        document.getElementById('companyModalOverlay')?.addEventListener('click', () => {
            this.closeCompanyModal();
        });

        // Close modal on Escape key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                this.closeCompanyModal();
            }
        });

        // Note: Add/Edit user modals are not implemented in HTML
        // Event listeners removed to prevent errors
    }

    // Format company record from Airtable (similar to formatTermRecord in Financial Term Library)
    formatCompanyRecord(record) {
        const fields = record.fields || {};
        const companyName = fields["Company Name"] || '';
        const companyId = fields["Company ID"] || '';
        const companyType = fields["Company Type"] || '';
        const userType = fields["User Type"] || '';
        const location = fields["Company HQ Country"] || '';
        const website = fields["Company Website"] ? String(fields["Company Website"]).trim() : '';
        const companyOverview = fields["Company Overview"] || '';
        
        // Get regions: 1) multi-select "Regions" field, 2) checkbox fields by exact/normalized name, 3) pattern match on any region checkbox
        let regions = [];
        const normalizeFieldKey = (key) => (key || '').replace(/\u2013|\u2014/g, '-').replace(/\s+/g, ' ').trim();

        // 1) Multi-select "Regions" or "Region" field (array or comma-separated string)
        const regionsField = fields["Regions"] ?? fields["Region"] ?? fields["REGIONS"] ?? fields["REGION"] ?? '';
        if (regionsField) {
            const raw = Array.isArray(regionsField) ? regionsField : String(regionsField).split(',').map(r => (r && r.trim())).filter(Boolean);
            const toCode = (v) => {
                const u = String(v).toUpperCase().trim();
                if (u === 'AMERICAS' || u === 'AMERICA') return 'AMERICAS';
                if (u === 'CALA' || u.includes('CARIBBEAN') || u.includes('LATIN')) return 'CALA';
                if (u === 'EUROPE') return 'EUROPE';
                if (u === 'MEA' || (u.includes('MIDDLE') && u.includes('EAST')) || u.includes('AFRICA')) return 'MEA';
                if (u === 'AP' || u.includes('ASIA') || u.includes('PACIFIC')) return 'AP';
                return null;
            };
            raw.forEach((v) => {
                const code = toCode(v);
                if (code && !regions.includes(code)) regions.push(code);
            });
        }

        // 2) Checkbox fields: exact and normalized key match (handles en-dash vs hyphen)
        if (regions.length === 0) {
            const regionFieldMap = {
                "AMERICAS": ["Region - America", "Region - Americas", "Americas", "America"],
                "CALA": ["Region - Caribbean & Latin America", "Region - Caribbean and Latin America", "CALA", "Caribbean & Latin America"],
                "EUROPE": ["Region - Europe", "Europe"],
                "MEA": ["Region - Middle East & Africa", "Region - Middle East and Africa", "MEA", "Middle East & Africa"],
                "AP": ["Region - Asia Pacific", "Region - Asia-Pacific", "AP", "Asia Pacific", "Asia-Pacific"]
            };
            const fieldKeys = Object.keys(fields);
            for (const [regionCode, possibleNames] of Object.entries(regionFieldMap)) {
                if (regions.includes(regionCode)) continue;
                for (const fieldName of possibleNames) {
                    const exact = fields[fieldName] === true;
                    const normalized = fieldKeys.some(
                        (k) => normalizeFieldKey(k) === normalizeFieldKey(fieldName) && fields[k] === true
                    );
                    if (exact || normalized) {
                        regions.push(regionCode);
                        break;
                    }
                }
            }
        }

        // 3) Pattern match on any boolean true field whose name looks like a region
        if (regions.length === 0) {
            for (const fieldName of Object.keys(fields)) {
                const fieldValue = fields[fieldName];
                if (typeof fieldValue !== 'boolean' || fieldValue !== true) continue;
                const fieldNameLower = fieldName.toLowerCase().trim();
                if (!fieldNameLower.includes('region') && !fieldNameLower.includes('america') && !fieldNameLower.includes('europe') && !fieldNameLower.includes('asia') && !fieldNameLower.includes('caribbean') && !fieldNameLower.includes('latin') && !fieldNameLower.includes('mea') && !fieldNameLower.includes('pacific')) continue;
                if (fieldNameLower.includes('america') && !fieldNameLower.includes('latin') && !fieldNameLower.includes('caribbean') && !regions.includes("AMERICAS")) regions.push("AMERICAS");
                else if ((fieldNameLower.includes('caribbean') || fieldNameLower.includes('latin')) && !regions.includes("CALA")) regions.push("CALA");
                else if (fieldNameLower.includes('europe') && !fieldNameLower.includes('middle') && !fieldNameLower.includes('africa') && !regions.includes("EUROPE")) regions.push("EUROPE");
                else if (fieldNameLower.includes('middle') && fieldNameLower.includes('east') && fieldNameLower.includes('africa') && !regions.includes("MEA")) regions.push("MEA");
                else if (fieldNameLower.includes('asia') && fieldNameLower.includes('pacific') && !regions.includes("AP")) regions.push("AP");
            }
        }
        
        const closedDeals = fields["Closed Deals"] || 0;
        const brandCount = fields["Brand Count"] || 0;
        const submittedBids = fields["Submitted Bids"] || 0;
        const logo = fields["Logo"] || fields["Company Logo"];
        
        // Capture additional fields for modal
        // Industries - could be multiple select, linked records, or text field
        let industries = [];
        const industriesField = fields["Industries"] || fields["Industry"] || fields["Sectors"] || fields["Sector"] || '';
        if (industriesField) {
        }
        if (Array.isArray(industriesField)) {
            industries = industriesField.map(ind => typeof ind === 'string' ? ind : (ind.name || String(ind)));
        } else if (typeof industriesField === 'string' && industriesField.trim()) {
            // If it's a comma-separated string, split it
            industries = industriesField.split(',').map(ind => ind.trim()).filter(ind => ind);
        }
        
        // Transactions/Deals - could be linked records or count
        let transactions = [];
        const transactionsField = fields["Transactions"] || fields["Deals"] || fields["Deal History"] || '';
        if (transactionsField) {
        }
        if (Array.isArray(transactionsField)) {
            transactions = transactionsField.map(txn => {
                if (typeof txn === 'string') return { name: txn };
                return { name: txn.name || txn.company || String(txn), type: txn.type || '', logo: txn.logo || '' };
            });
        }
        
        // Brands - from "Brands You Operate / Support" or "Brand Name" field
        // Store both record IDs and any expanded data for later fetching
        let brandRecordIds = [];
        let brands = [];
        const brandsField = fields["Brands You Operate / Support"] || fields["Brand Name"] || fields["Brands"] || fields["Brand"] || '';
        if (brandsField) {
        }
        if (Array.isArray(brandsField)) {
            brandsField.forEach((brand, index) => {
                // If it's just a string (record ID), store it to fetch later
                if (typeof brand === 'string') {
                    if (brand.startsWith('rec')) {
                        brandRecordIds.push(brand);
                    } else {
                        brands.push(brand);
                    }
                }
                // Handle linked records - Airtable linked records have a 'fields' property when expanded
                else if (brand && typeof brand === 'object') {
                    // Try various field names for brand name
                    const brandName = brand.fields?.["Brand Name"] || 
                                     brand.fields?.name || 
                                     brand.fields?.Name ||
                                     brand.fields?.["name"] ||
                                     brand.name ||
                                     brand.fields?.["Name"] ||
                                     '';
                    
                    if (brandName) {
                        brands.push(brandName);
                    } else if (brand.id) {
                        // If it's an object with an id but no name, store the ID to fetch later
                        brandRecordIds.push(brand.id);
                    }
                }
            });
        } else if (typeof brandsField === 'string' && brandsField.trim()) {
            // If it's a comma-separated string, split it
            const brandList = brandsField.split(',').map(b => b.trim()).filter(b => b);
            brandList.forEach(brand => {
                if (brand.startsWith('rec')) {
                    brandRecordIds.push(brand);
                } else {
                    brands.push(brand);
                }
            });
        }
        
        // Store record IDs to fetch brand names later if needed
        // We'll fetch them when opening the modal
        
        // Capture User Management field for team members
        let userManagementRecordIds = [];
        const userManagementField = fields["User Management"] || fields["USER MANAGEMENT"] || fields["user management"] || '';
        if (userManagementField) {
            if (Array.isArray(userManagementField)) {
                userManagementField.forEach(user => {
                    if (typeof user === 'string' && user.startsWith('rec')) {
                        userManagementRecordIds.push(user);
                    } else if (user && typeof user === 'object' && user.id) {
                        userManagementRecordIds.push(user.id);
                    }
                });
            }
        }
        
        // Capture service checkbox fields (boolean fields that are true)
        // Services are identified as checkbox fields that are not region-related
        const services = [];
        const primaryServices = [];
        
        // Capture all other fields that might be useful
        const rawFields = {};
        for (const [key, value] of Object.entries(fields)) {
            // Skip fields we've already processed
            if (['Company Name', 'Company ID', 'Company Type', 'User Type', 'Company HQ Country', 
                 'Company Website', 'Company Overview', 'Closed Deals', 'Brand Count', 'Submitted Bids', 
                 'Logo', 'Company Logo', 'Industries', 'Industry', 'Sectors', 'Sector', 
                 'Transactions', 'Deals', 'Deal History', 'Brands You Operate / Support', 
                 'Brand Name', 'Brands', 'Brand', 'User Management', 'USER MANAGEMENT', 'user management'].includes(key)) {
                continue;
            }
            
            // Handle boolean checkbox fields - check if they're services (not regions)
            if (typeof value === 'boolean' && value === true) {
                const keyLower = key.toLowerCase();
                // Skip region checkbox fields
                if (keyLower.includes('region') || 
                    keyLower.includes('americas') || 
                    keyLower.includes('caribbean') || 
                    keyLower.includes('latin') || 
                    keyLower.includes('europe') || 
                    keyLower.includes('middle east') || 
                    keyLower.includes('africa') || 
                    keyLower.includes('asia pacific') ||
                    keyLower === 'ap' ||
                    keyLower === 'mea' ||
                    keyLower === 'cala') {
                    continue;
                }
                
                // This is a service checkbox field
                // Clean the service name (remove prefixes like "Primary", "Addl", etc.)
                let serviceName = key;
                let isPrimary = false;
                
                // Check if it's a primary service (field name contains "Primary")
                if (keyLower.includes('primary')) {
                    isPrimary = true;
                }
                
                // Clean the name: remove common prefixes and suffixes
                serviceName = serviceName
                    // Remove "Primary" prefix/suffix
                    .replace(/^Primary\s*[-:]\s*/i, '')
                    .replace(/\s*[-:]\s*Primary$/i, '')
                    .replace(/^Primary\s+/i, '')
                    .replace(/\s+Primary$/i, '')
                    // Remove "Addl" or "Additional" prefix
                    .replace(/^Addl\s*[-:]\s*/i, '')
                    .replace(/^Additional\s*[-:]\s*/i, '')
                    .replace(/^Addl\s+/i, '')
                    .replace(/^Additional\s+/i, '')
                    // Clean up any remaining leading/trailing spaces and dashes
                    .replace(/^[-:\s]+/, '')
                    .replace(/[-:\s]+$/, '')
                    .trim();
                
                // Add to services list (use cleaned name)
                if (serviceName && !services.includes(serviceName)) {
                    services.push(serviceName);
                }
                
                // If it's primary, add to primary services list
                if (isPrimary && serviceName && !primaryServices.includes(serviceName)) {
                    primaryServices.push(serviceName);
                }
                continue;
            }
            
            // Skip empty values
            if (value === null || value === undefined || value === '') continue;
            rawFields[key] = value;
        }
        
        // Normalize user type
        let normalizedUserType = companyType || userType || '';
        if (normalizedUserType) {
            const upperType = String(normalizedUserType).trim().toUpperCase();
            if (upperType === "HOTEL OWNERS" || upperType === "HOTEL OWNER" || upperType === "OWNER" || upperType === "OWNERS") {
                normalizedUserType = "HOTEL OWNERS";
            } else if (upperType === "HOTEL BRANDS (FRANCHISE)" || upperType === "HOTEL BRAND" || upperType === "HOTEL BRANDS" || upperType === "BRAND" || upperType === "BRANDS" || upperType === "FRANCHISE") {
                normalizedUserType = "HOTEL BRANDS (FRANCHISE)";
            } else if (upperType === "HOTEL MGMT. COMPANY" || upperType === "HOTEL MGMT COMPANY" || upperType === "HOTEL MANAGEMENT COMPANY" || upperType === "MGMT" || upperType === "MANAGEMENT" || upperType === "OPERATOR") {
                normalizedUserType = "HOTEL MGMT. COMPANY";
            } else if (upperType.includes('BRAND') || upperType.includes('FRANCHISE')) {
                normalizedUserType = "HOTEL BRANDS (FRANCHISE)";
            } else if (upperType.includes('MGMT') || upperType.includes('MANAGEMENT') || upperType.includes('OPERATOR')) {
                normalizedUserType = "HOTEL MGMT. COMPANY";
            } else if (upperType.includes('OWNER')) {
                normalizedUserType = "HOTEL OWNERS";
            }
        }
        
        // Get logo
        let logoDisplay = companyName && companyName.length > 0 ? companyName.charAt(0).toUpperCase() : '?';
        if (logo && Array.isArray(logo) && logo.length > 0 && logo[0] && logo[0].url) {
            logoDisplay = { type: 'image', url: logo[0].url };
        } else if (logo && typeof logo === 'string' && logo.startsWith('http')) {
            logoDisplay = { type: 'image', url: logo };
        }
        
        return {
            id: record.id || '',
            companyId: companyId || '',
            name: companyName || '',
            userType: normalizedUserType,
            companyType: companyType || '',
            location: location || '',
            website: website || '',
            description: companyOverview || '',
            companyOverview: companyOverview || '',
            regions: regions,
            closedDeals: closedDeals ? Number(closedDeals) : 0,
            brandCount: brandCount ? Number(brandCount) : 0,
            submittedBids: submittedBids ? Number(submittedBids) : 0,
            logo: logoDisplay,
            industries: industries,
            transactions: transactions,
            brands: brands,
            brandRecordIds: brandRecordIds, // Store record IDs to fetch brand names later
            userManagementRecordIds: userManagementRecordIds, // Store record IDs to fetch team members
            services: services, // All service checkbox fields that are checked
            primaryServices: primaryServices, // Service fields marked as primary
            _createdTime: record.createdTime || null, // Store Airtable's createdTime for growth trends
            rawFields: rawFields // Store all other fields for potential use
        };
    }

    // Format individual record from Airtable
    formatIndividualRecord(record) {
        const fields = record.fields || {};
        const sourceTable = record._sourceTable || 'Unknown';
        const firstName = fields["First Name"] || '';
        const lastName = fields["Last Name"] || '';
        const companyTitle = fields["Company Title"] || '';
        
        // Debug: Log all field names for User Management table records to see what fields are available
        if (sourceTable === 'User Management' && (firstName || lastName)) {
            const fullName = `${firstName || ''} ${lastName || ''}`.trim();
        }
        
        // Try multiple field name variations for company name
        // Handle both direct text fields and linked records
        // Both Users table and User Management table have "Company Profile" field that links to Company Profile table
        let companyName = '';
        let companyRecordId = null; // Store record ID if we need to fetch company name separately
        
        // First, check for "Company Profile" linked record field (this is the primary field in both Users and User Management tables)
        const companyProfileField = fields["Company Profile"] || '';
        
        if (companyProfileField) {
            if (Array.isArray(companyProfileField) && companyProfileField.length > 0) {
                // It's an array (linked record)
                if (typeof companyProfileField[0] === 'object' && companyProfileField[0].fields) {
                    // Expanded linked record - get the company name from the fields
                    companyName = companyProfileField[0].fields["Company Name"] || 
                                 companyProfileField[0].fields["Name"] || 
                                 '';
                } else if (typeof companyProfileField[0] === 'string') {
                    // Array of strings - could be record IDs
                    if (companyProfileField[0].startsWith('rec')) {
                        companyRecordId = companyProfileField[0]; // Store to fetch later
                    } else {
                        companyName = companyProfileField[0];
                    }
                } else if (typeof companyProfileField[0] === 'object' && companyProfileField[0].id) {
                    // Linked record object with ID
                    companyRecordId = companyProfileField[0].id;
                }
            } else if (typeof companyProfileField === 'string') {
                if (companyProfileField.startsWith('rec')) {
                    companyRecordId = companyProfileField; // Store to fetch later
                } else {
                    companyName = companyProfileField;
                }
            } else if (typeof companyProfileField === 'object') {
                if (companyProfileField.fields) {
                    // Single expanded linked record object
                    companyName = companyProfileField.fields["Company Name"] || 
                                 companyProfileField.fields["Name"] || 
                                 '';
                } else if (companyProfileField.id) {
                    // Linked record object with ID
                    companyRecordId = companyProfileField.id;
                }
            }
        }
        
        // If we still don't have a company name, try other field variations
        if (!companyName && !companyRecordId) {
            const companyField = fields["Company Name"] || 
                                fields["Company"] ||
                                fields["Company Name (from Company Profile)"] ||
                                fields["Company/Organization"] ||
                                fields["Organization"] || 
                                '';
            
            if (companyField) {
                if (typeof companyField === 'string') {
                    if (companyField.startsWith('rec')) {
                        companyRecordId = companyField;
                    } else {
                        companyName = companyField;
                    }
                } else if (Array.isArray(companyField) && companyField.length > 0) {
                    if (typeof companyField[0] === 'object' && companyField[0].fields) {
                        companyName = companyField[0].fields["Company Name"] || 
                                     companyField[0].fields["Name"] || 
                                     '';
                    } else if (typeof companyField[0] === 'string') {
                        if (companyField[0].startsWith('rec')) {
                            companyRecordId = companyField[0];
                        } else {
                            companyName = companyField[0];
                        }
                    } else if (typeof companyField[0] === 'object' && companyField[0].id) {
                        companyRecordId = companyField[0].id;
                    }
                } else if (typeof companyField === 'object') {
                    if (companyField.fields) {
                        companyName = companyField.fields["Company Name"] || 
                                     companyField.fields["Name"] || 
                                     '';
                    } else if (companyField.id) {
                        companyRecordId = companyField.id;
                    }
                }
            }
        }
        
        const phoneNumber = fields["Phone Number"] || '';
        const email = fields["Email"] || '';
        
        // Try multiple field name variations for User Type
        // User Management table might use different field names
        let userType = fields["User Type"] || 
                      fields["Company Type"] || 
                      fields["USER TYPE"] || 
                      fields["COMPANY TYPE"] ||
                      fields["Type"] ||
                      fields["TYPE"] ||
                      '';
        
        // If userType is still empty, try to get it from the expanded Company Profile linked record
        if (!userType && companyProfileField) {
            if (Array.isArray(companyProfileField) && companyProfileField.length > 0) {
                if (typeof companyProfileField[0] === 'object' && companyProfileField[0].fields) {
                    // Expanded linked record - get userType from company
                    userType = companyProfileField[0].fields["User Type"] || 
                              companyProfileField[0].fields["Company Type"] ||
                              companyProfileField[0].fields["USER TYPE"] ||
                              companyProfileField[0].fields["COMPANY TYPE"] ||
                              '';
                }
            } else if (typeof companyProfileField === 'object' && companyProfileField.fields) {
                userType = companyProfileField.fields["User Type"] || 
                          companyProfileField.fields["Company Type"] ||
                          companyProfileField.fields["USER TYPE"] ||
                          companyProfileField.fields["COMPANY TYPE"] ||
                          '';
            }
        }
        
        const location = fields["Country"] || fields["Location"] || '';
        const website = fields["Website"] || fields["Personal Website"] || '';
        const closedDeals = fields["Closed Deals"] ? Number(fields["Closed Deals"]) : 0;
        const brandCount = fields["Brand Count"] ? Number(fields["Brand Count"]) : 0;
        const submittedBids = fields["Submitted Bids"] ? Number(fields["Submitted Bids"]) : 0;
        
        // Debug: Log company name extraction
        if (firstName || lastName) {
            const fullName = `${firstName || ''} ${lastName || ''}`.trim();
            const companyFieldRaw = fields["Company Name"] || fields["Company/Organization"] || fields["Company"] || fields["Organization"] || fields["Company Profile"] || '';
        }
        
        // Get regions from checkbox fields (same logic as companies)
        // First check if there's a "Region" field (could be multi-select or array)
        let regions = [];
        const regionField = fields["Region"] || fields["Regions"] || fields["REGION"] || fields["REGIONS"] || '';
        
        if (regionField) {
            if (Array.isArray(regionField)) {
                // If it's an array, use the values directly
                regions = regionField.map(r => {
                    if (typeof r === 'string') {
                        // Normalize region names
                        const rUpper = r.trim().toUpperCase();
                        if (rUpper.includes('AMERICAS') && !rUpper.includes('LATIN')) return "AMERICAS";
                        if (rUpper.includes('CALA') || (rUpper.includes('CARIBBEAN') || rUpper.includes('LATIN'))) return "CALA";
                        if (rUpper.includes('EUROPE')) return "EUROPE";
                        if (rUpper.includes('MEA') || (rUpper.includes('MIDDLE') && rUpper.includes('EAST'))) return "MEA";
                        if (rUpper.includes('AP') || (rUpper.includes('ASIA') && rUpper.includes('PACIFIC'))) return "AP";
                        return rUpper;
                    }
                    return String(r).toUpperCase();
                }).filter(r => r);
            } else if (typeof regionField === 'string') {
                // If it's a comma-separated string
                regions = regionField.split(',').map(r => r.trim().toUpperCase()).filter(r => r);
            }
        }
        
        // Also check checkbox fields - check exact field names from User Management table
        if (regions.length === 0) {
            // Use same region field mapping as companies
            const regionFieldMap = {
                "AMERICAS": [
                    "Region - America",
                    "Region - Americas"
                ],
                "CALA": [
                    "Region - Caribbean & Latin America",
                    "Region - Caribbean and Latin America"
                ],
                "EUROPE": [
                    "Region - Europe"
                ],
                "MEA": [
                    "Region - Middle East & Africa",
                    "Region - Middle East and Africa"
                ],
                "AP": [
                    "Region - Asia Pacific",
                    "Region - Asia-Pacific"
                ]
            };
            
            // First try exact field name matches
            for (const [regionCode, fieldNames] of Object.entries(regionFieldMap)) {
                for (const fieldName of fieldNames) {
                    if (fields[fieldName] === true && !regions.includes(regionCode)) {
                        regions.push(regionCode);
                        break;
                    }
                }
            }
            
            // If no regions found with exact matches, try pattern matching
            if (regions.length === 0) {
                const allFieldNames = Object.keys(fields);
                for (const fieldName of allFieldNames) {
                    const fieldValue = fields[fieldName];
                    if (typeof fieldValue === 'boolean' && fieldValue === true) {
                        const fieldNameLower = fieldName.toLowerCase().trim();
                        
                        if (!fieldNameLower.includes('region')) continue;
                        
                        if (fieldNameLower.includes('america') && 
                            !fieldNameLower.includes('latin') && 
                            !fieldNameLower.includes('caribbean') &&
                            !regions.includes("AMERICAS")) {
                            regions.push("AMERICAS");
                        }
                        else if ((fieldNameLower.includes('caribbean') || fieldNameLower.includes('latin')) &&
                                 !regions.includes("CALA")) {
                            regions.push("CALA");
                        }
                        else if (fieldNameLower.includes('europe') &&
                                 !fieldNameLower.includes('middle') &&
                                 !fieldNameLower.includes('africa') &&
                                 !regions.includes("EUROPE")) {
                            regions.push("EUROPE");
                        }
                        else if (fieldNameLower.includes('middle') &&
                                 fieldNameLower.includes('east') &&
                                 fieldNameLower.includes('africa') &&
                                 !regions.includes("MEA")) {
                            regions.push("MEA");
                        }
                        else if (fieldNameLower.includes('asia') &&
                                 fieldNameLower.includes('pacific') &&
                                 !regions.includes("AP")) {
                            regions.push("AP");
                        }
                    }
                }
            }
        }
        
        // Debug: Log regions for this individual
        const fullName = `${firstName || ''} ${lastName || ''}`.trim();
        if (fullName) {
        }
        
        // Get profile picture/headshot
        let profilePicture = null;
        const profileField = fields["Profile"] || fields["Profile Picture"] || fields["Headshot"] || fields["Photo"] || '';
        if (profileField) {
            if (Array.isArray(profileField) && profileField.length > 0 && profileField[0] && profileField[0].url) {
                profilePicture = profileField[0].url;
            } else if (typeof profileField === 'string' && profileField.startsWith('http')) {
                profilePicture = profileField;
            }
        }

        // Dealality Response Behavior Classification (responsiveness badge)
        const responsivenessCombinedBadge = fields["responsiveness_combined_badge"] || fields["Responsiveness Combined Badge"] || '';
        const responsivenessTimeCategory = fields["responsiveness_response_time_category"] || fields["Responsiveness Response Time Category"] || '';
        const responsivenessFrequencyCategory = fields["responsiveness_frequency_category"] || fields["Responsiveness Frequency Category"] || '';
        
        return {
            id: record.id || '',
            firstName: firstName,
            lastName: lastName,
            companyTitle: companyTitle,
            companyName: companyName,
            companyRecordId: companyRecordId, // Store record ID if we need to fetch company name from Company Profile table
            phoneNumber: phoneNumber,
            email: email,
            userType: userType,
            location: location,
            website: website,
            closedDeals: closedDeals,
            brandCount: brandCount,
            submittedBids: submittedBids,
            _createdTime: record.createdTime || null, // Store Airtable's createdTime for growth trends
            profilePicture: profilePicture,
            regions: regions,
            responsivenessCombinedBadge: responsivenessCombinedBadge,
            responsivenessTimeCategory: responsivenessTimeCategory,
            responsivenessFrequencyCategory: responsivenessFrequencyCategory
        };
    }

    async fetchPartners() {
        this.showLoading();
        try {
            // Use cached data if still valid (speeds up tab switches and repeat visits)
            if (this.partnersCache && (Date.now() - this.partnersCache.timestamp) < this.PARTNERS_CACHE_TTL_MS) {
                this.companies = this.partnersCache.companies;
                this.individuals = this.partnersCache.individuals;
                this.hideLoading();
                this.updateStats();
                this.applyFilters();
                return;
            }
            // Validate configuration (should already be validated in loadConfig, but double-check)
            if (!PARTNER_DIRECTORY_CONFIG.AIRTABLE_API_KEY || 
                !PARTNER_DIRECTORY_CONFIG.AIRTABLE_BASE_ID ||
                PARTNER_DIRECTORY_CONFIG.AIRTABLE_API_KEY === 'YOUR_AIRTABLE_API_KEY_HERE' || 
                PARTNER_DIRECTORY_CONFIG.AIRTABLE_BASE_ID === 'YOUR_AIRTABLE_BASE_ID_HERE') {
                throw new Error('⚠️ Configuration Required: Please update PARTNER_DIRECTORY_CONFIG in partner-directory.js with your actual Airtable API key and Base ID. See comments at the top of the file for instructions.');
            }
            
            const baseId = PARTNER_DIRECTORY_CONFIG.AIRTABLE_BASE_ID;
            const apiKey = PARTNER_DIRECTORY_CONFIG.AIRTABLE_API_KEY;
            const pageSize = PARTNER_DIRECTORY_CONFIG.MAX_RECORDS_PER_REQUEST.toString();
            const headers = { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' };

            // Fetch companies and individuals in parallel for faster load
            const [allCompanyRecords, allIndividualRecords] = await Promise.all([
                this._fetchAllPages(baseId, PARTNER_DIRECTORY_CONFIG.COMPANY_PROFILE_TABLE_ID, pageSize, headers),
                this._fetchIndividualsFromAllTables(baseId, pageSize, headers)
            ]);

            // Format company records
            this.companies = allCompanyRecords
                .filter(record => {
                    const fields = record.fields || {};
                    const companyName = fields["Company Name"] || '';
                    return companyName && companyName.trim();
                })
                .map(record => this.formatCompanyRecord(record));

            // Format individual records (allIndividualRecords from parallel fetch above)
            this.individuals = allIndividualRecords.map(record => {
                const formatted = this.formatIndividualRecord(record);
                formatted._sourceTable = record._sourceTable || 'Unknown';
                return formatted;
            });
            
            // Show grid and data first for faster perceived load (especially Individuals tab)
            this.hideLoading();
            this.updateStats();
            this.applyFilters();

            // Fetch company names in background so Individuals tab shows immediately; names fill in when ready
            const individualsNeedingCompanyNames = this.individuals.filter(ind => ind.companyRecordId && !ind.companyName);
            if (individualsNeedingCompanyNames.length > 0) {
                const companyRecordIds = [...new Set(individualsNeedingCompanyNames.map(ind => ind.companyRecordId).filter(id => id))];
                this.fetchCompanyNames(companyRecordIds).then(companyNamesMap => {
                    this.individuals.forEach(individual => {
                        if (individual.companyRecordId) {
                            if (!individual.companyName && companyNamesMap[individual.companyRecordId]) {
                                individual.companyName = companyNamesMap[individual.companyRecordId];
                            }
                            if (!individual.userType && this.companyUserTypesMap && this.companyUserTypesMap[individual.companyRecordId]) {
                                individual.userType = this.companyUserTypesMap[individual.companyRecordId];
                            }
                        }
                        if (!individual.userType && individual.companyName) {
                            const matchingCompany = this.companies.find(c => c.name === individual.companyName);
                            if (matchingCompany && matchingCompany.userType) {
                                individual.userType = matchingCompany.userType;
                            }
                        }
                    });
                    this.updateStats();
                    this.applyFilters();
                }).catch(err => console.warn('Background company names fetch failed:', err));
            }

            // Store in cache for TTL (avoids refetch on tab switch / repeat visits)
            this.partnersCache = { companies: this.companies, individuals: this.individuals, timestamp: Date.now() };
        } catch (error) {
            console.error('Error fetching partners:', error);
            this.hideLoading();
            this.showLoadingError('Failed to load partners. Please try again later.');
        }
    }

    async _fetchAllPages(baseId, tableId, pageSize, headers) {
        const all = [];
        let offset = null;
        do {
            const url = new URL(`https://api.airtable.com/v0/${baseId}/${tableId}`);
            url.searchParams.append('pageSize', pageSize);
            if (offset) url.searchParams.append('offset', offset);
            const response = await fetch(url, { headers });
            if (!response.ok) {
                const errorText = await response.text().catch(() => '');
                throw new Error(`Airtable API error: ${response.status} ${response.statusText}. ${errorText}`);
            }
            const data = await response.json();
            all.push(...(data.records || []));
            offset = data.offset;
        } while (offset);
        return all;
    }

    async _fetchIndividualsFromAllTables(baseId, pageSize, headers) {
        const fetchUsers = async () => {
            const all = [];
            let offset = null;
            do {
                const url = new URL(`https://api.airtable.com/v0/${baseId}/${PARTNER_DIRECTORY_CONFIG.USERS_TABLE_ID}`);
                url.searchParams.append('pageSize', pageSize);
                if (offset) url.searchParams.append('offset', offset);
                url.searchParams.append('cellFormat', 'json');
                url.searchParams.append('returnFieldsByFieldId', 'false');
                url.searchParams.append('expand[]', 'Company Profile');
                const response = await fetch(url, { headers });
                if (!response.ok) {
                    const errorText = await response.text().catch(() => '');
                    throw new Error(`Airtable API error (Users): ${response.status} ${response.statusText}. ${errorText}`);
                }
                const data = await response.json();
                (data.records || []).forEach(r => { r._sourceTable = 'Users'; });
                all.push(...(data.records || []));
                offset = data.offset;
            } while (offset);
            return all;
        };
        const fetchUserManagement = async () => {
            if (!PARTNER_DIRECTORY_CONFIG.USER_MANAGEMENT_TABLE_ID) return [];
            const all = [];
            let offset = null;
            do {
                const url = new URL(`https://api.airtable.com/v0/${baseId}/${PARTNER_DIRECTORY_CONFIG.USER_MANAGEMENT_TABLE_ID}`);
                url.searchParams.append('pageSize', pageSize);
                if (offset) url.searchParams.append('offset', offset);
                url.searchParams.append('cellFormat', 'json');
                url.searchParams.append('returnFieldsByFieldId', 'false');
                url.searchParams.append('expand[]', 'Company Profile');
                const response = await fetch(url, { headers });
                if (!response.ok) {
                    console.warn('⚠️ Could not fetch User Management table, continuing with Users only');
                    return all;
                }
                const data = await response.json();
                (data.records || []).forEach(r => { r._sourceTable = 'User Management'; });
                all.push(...(data.records || []));
                offset = data.offset;
            } while (offset);
            return all;
        };
        const [userRecords, umRecords] = await Promise.all([fetchUsers(), fetchUserManagement()]);
        return [...userRecords, ...umRecords];
    }

    hideLoading() {
        const loadingElement = document.getElementById('loadingState');
        if (loadingElement) {
            loadingElement.style.display = 'none';
        }
        document.getElementById('filtersSection').style.display = 'flex';
        document.getElementById('resultsGrid').classList.remove('hidden');
    }

    showLoading() {
        const loadingElement = document.getElementById('loadingState');
        if (loadingElement) {
            loadingElement.style.display = 'block';
        }
        document.getElementById('filtersSection').style.display = 'none';
        document.getElementById('resultsGrid').classList.add('hidden');
    }

    updateStats() {
        document.getElementById('companiesCount').textContent = this.companies.length;
        document.getElementById('individualsCount').textContent = this.individuals.length;
        const favoritesCount = this.favorites.length;
        document.getElementById('favoritesCount').textContent = favoritesCount;
    }

    switchTab(tab) {
        this.currentTab = tab;
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        
        // Hide all content sections
        document.getElementById('filtersSection')?.classList.add('hidden');
        document.getElementById('resultsGrid')?.classList.add('hidden');
        document.getElementById('insightsContainer')?.classList.add('hidden');
        document.getElementById('emptyState')?.classList.add('hidden');
        
        // Destroy Chart.js instances when leaving Insights to free memory (recreated when returning)
        if (tab !== 'insights') {
            this.destroyInsightsCharts();
        }
        
        const responsivenessWrapper = document.getElementById('responsivenessFiltersWrapper');
        const resultsGrid = document.getElementById('resultsGrid');
        const emptyStateEl = document.getElementById('emptyState');

        const showCachedTab = (tabKey) => {
            const cached = this.tabContainers[tabKey];
            if (cached && Array.isArray(cached) && cached.length > 0) {
                resultsGrid?.replaceChildren(...cached);
                resultsGrid?.classList.remove('hidden');
                emptyStateEl?.classList.add('hidden');
                return true;
            }
            return false;
        };

        if (tab === 'companies') {
            document.getElementById('companiesTab').classList.add('active');
            document.getElementById('filtersSection')?.classList.remove('hidden');
            document.getElementById('resultsGrid')?.classList.remove('hidden');
            document.getElementById('insightsContainer')?.classList.add('hidden');
            // Ensure companies grid never uses the 3-column individuals layout
            resultsGrid?.classList.remove('results-grid--individuals');
            if (responsivenessWrapper) {
                // Hide responsiveness filters visually but keep layout space on Companies tab
                responsivenessWrapper.style.visibility = 'hidden';
                responsivenessWrapper.setAttribute('aria-hidden', 'true');
                responsivenessWrapper.querySelectorAll('select').forEach(sel => sel.disabled = true);
            }
            if (!showCachedTab('companies')) {
                this.applyFilters();
                this.renderResults();
            }
        } else if (tab === 'individuals') {
            document.getElementById('individualsTab').classList.add('active');
            document.getElementById('filtersSection')?.classList.remove('hidden');
            document.getElementById('resultsGrid')?.classList.remove('hidden');
            document.getElementById('insightsContainer')?.classList.add('hidden');
            // Ensure individuals grid uses the 3-column layout helper class
            resultsGrid?.classList.add('results-grid--individuals');
            if (responsivenessWrapper) {
                responsivenessWrapper.style.visibility = 'visible';
                responsivenessWrapper.setAttribute('aria-hidden', 'false');
                responsivenessWrapper.querySelectorAll('select').forEach(sel => sel.disabled = false);
            }
            // Sync responsiveness filter state from DOM when returning to Individuals (keeps dropdowns and results in sync)
            const speedEl = document.getElementById('responsivenessSpeedFilter');
            const freqEl = document.getElementById('responsivenessFrequencyFilter');
            if (speedEl) this.currentResponsivenessSpeedFilter = (speedEl.value || '').trim();
            if (freqEl) this.currentResponsivenessFrequencyFilter = (freqEl.value || '').trim();
            if (!showCachedTab('individuals')) {
                this.applyFilters();
                this.renderResults();
            }
        } else if (tab === 'favorites') {
            document.getElementById('favoritesTab').classList.add('active');
            document.getElementById('filtersSection')?.classList.remove('hidden');
            document.getElementById('resultsGrid')?.classList.remove('hidden');
            document.getElementById('insightsContainer')?.classList.add('hidden');
            responsivenessWrapper?.classList.add('hidden');
            if (responsivenessWrapper) responsivenessWrapper.setAttribute('aria-hidden', 'true');
            this.ensureCategoryFilter();
            if (!showCachedTab('favorites')) {
                this.applyFilters();
                this.updateFilterCount();
                this.renderResults();
            } else {
                this.updateFilterCount();
            }
        } else if (tab === 'insights') {
            document.getElementById('insightsTab').classList.add('active');
            document.getElementById('filtersSection')?.classList.add('hidden');
            document.getElementById('resultsGrid')?.classList.add('hidden');
            document.getElementById('emptyState')?.classList.add('hidden');
            document.getElementById('insightsContainer')?.classList.remove('hidden');
            // Show insights filters section
            document.getElementById('insightsFiltersSection')?.classList.remove('hidden');
            this.updateInsightsFilterCount();
            this.updateInsights();
        }
    }

    toggleFilterButton(type) {
        if (this.currentUserTypeFilter === type) {
            this.currentUserTypeFilter = '';
        } else {
            this.currentUserTypeFilter = type;
        }
        this.updateFilterButtons();
        document.getElementById('userTypeFilter').value = this.currentUserTypeFilter;
        this.invalidateAllTabCaches();
        this.updateFilterCount();
        this.applyFilters();
    }

    updateFilterButtons() {
        document.querySelectorAll('.filter-button').forEach(btn => {
            if (btn.dataset.type === this.currentUserTypeFilter) {
                btn.classList.add('active');
            } else {
                btn.classList.remove('active');
            }
        });
    }

    /** Call when user changes search/filter/sort so tab caches are rebuilt on next render */
    invalidateAllTabCaches() {
        this.tabContainers.companies = null;
        this.tabContainers.individuals = null;
        this.tabContainers.favorites = null;
    }

    applyFilters() {
        // Normalize filter values and company types for comparison
        const normalizeType = (type) => {
            if (!type) return '';
            const upperType = String(type).trim().toUpperCase();
            
            // Normalize singular to plural and handle variations
            if (upperType === "HOTEL OWNERS" || upperType === "HOTEL OWNER" || upperType === "OWNER" || upperType === "OWNERS") {
                return "HOTEL OWNERS";
            } else if (upperType === "HOTEL BRANDS (FRANCHISE)" || upperType === "HOTEL BRAND" || upperType === "HOTEL BRANDS" || upperType === "BRAND" || upperType === "BRANDS" || upperType === "FRANCHISE") {
                return "HOTEL BRANDS (FRANCHISE)";
            } else if (upperType === "HOTEL MGMT. COMPANY" || upperType === "HOTEL MGMT COMPANY" || upperType === "HOTEL MANAGEMENT COMPANY" || upperType === "MGMT" || upperType === "MANAGEMENT" || upperType === "OPERATOR") {
                return "HOTEL MGMT. COMPANY";
            } else if (upperType.includes('BRAND') || upperType.includes('FRANCHISE')) {
                return "HOTEL BRANDS (FRANCHISE)";
            } else if (upperType.includes('MGMT') || upperType.includes('MANAGEMENT') || upperType.includes('OPERATOR')) {
                return "HOTEL MGMT. COMPANY";
            } else if (upperType.includes('OWNER')) {
                return "HOTEL OWNERS";
            }
            return upperType;
        };

        const filterType = normalizeType(this.currentUserTypeFilter);

        // Apply filters
        let filtered = this.companies.filter(company => {
            // Search filter
            if (this.currentSearchQuery) {
                const companyName = (company.name || '').toLowerCase();
                if (!companyName.includes(this.currentSearchQuery)) {
                    return false;
                }
            }
            
            // Type filter
            if (filterType) {
                const companyType = normalizeType(company.userType);
                if (companyType !== filterType) {
                    return false;
                }
            }
            
            // Region filter
            if (this.currentRegionFilter && !this.hasRegion(company.regions, this.currentRegionFilter)) {
                return false;
            }
            
            return true;
        });

        // Apply sorting
        filtered = this.sortCompanies(filtered);

        this.filteredCompanies = filtered;

        // Filter individuals (same logic as companies)
        let filteredIndividuals = this.individuals.filter(individual => {
            // Search filter
            if (this.currentSearchQuery) {
                const fullName = `${individual.firstName || ''} ${individual.lastName || ''}`.trim().toLowerCase();
                const companyName = (individual.companyName || '').toLowerCase();
                if (!fullName.includes(this.currentSearchQuery) && !companyName.includes(this.currentSearchQuery)) {
                    return false;
                }
            }
            
            // Type filter
            if (filterType) {
                const individualType = normalizeType(individual.userType);
                if (individualType !== filterType) {
                    return false;
                }
            }
            
            // Region filter
            if (this.currentRegionFilter && !this.hasRegion(individual.regions, this.currentRegionFilter)) {
                return false;
            }

            // Responsiveness (speed) filter – applies to individuals only
            if (this.currentResponsivenessSpeedFilter) {
                const individualSpeed = (individual.responsivenessTimeCategory || '').trim();
                if (individualSpeed !== this.currentResponsivenessSpeedFilter) return false;
            }

            // Responsiveness (frequency) filter – applies to individuals only
            if (this.currentResponsivenessFrequencyFilter) {
                const individualFreq = (individual.responsivenessFrequencyCategory || '').trim();
                if (individualFreq !== this.currentResponsivenessFrequencyFilter) return false;
            }
            
            return true;
        });

        // Apply sorting to individuals (same as companies)
        filteredIndividuals = this.sortIndividuals(filteredIndividuals);

        this.filteredIndividuals = filteredIndividuals;

        this.updateFilterCount();
        this.renderResults();
    }

    sortCompanies(companies) {
        const [field, direction] = this.currentSort.split('-');
        const sorted = [...companies];

        sorted.sort((a, b) => {
            let aVal, bVal;

            switch (field) {
                case 'name':
                    aVal = (a.name || '').toLowerCase();
                    bVal = (b.name || '').toLowerCase();
                    break;
                case 'deals':
                    aVal = a.closedDeals || 0;
                    bVal = b.closedDeals || 0;
                    break;
                case 'brands':
                    aVal = a.brandCount || 0;
                    bVal = b.brandCount || 0;
                    break;
                default:
                    return 0;
            }

            if (aVal < bVal) return direction === 'asc' ? -1 : 1;
            if (aVal > bVal) return direction === 'asc' ? 1 : -1;
            return 0;
        });

        return sorted;
    }

    sortIndividuals(individuals) {
        const [field, direction] = this.currentSort.split('-');
        const sorted = [...individuals];

        sorted.sort((a, b) => {
            let aVal, bVal;

            switch (field) {
                case 'name':
                    const aName = `${a.firstName || ''} ${a.lastName || ''}`.trim().toLowerCase();
                    const bName = `${b.firstName || ''} ${b.lastName || ''}`.trim().toLowerCase();
                    aVal = aName;
                    bVal = bName;
                    break;
                case 'deals':
                    aVal = a.closedDeals || 0;
                    bVal = b.closedDeals || 0;
                    break;
                case 'brands':
                    aVal = a.brandCount || 0;
                    bVal = b.brandCount || 0;
                    break;
                default:
                    return 0;
            }

            if (aVal < bVal) return direction === 'asc' ? -1 : 1;
            if (aVal > bVal) return direction === 'asc' ? 1 : -1;
            return 0;
        });

        return sorted;
    }

    updateFilterCount() {
        const filterCountBadge = document.getElementById('filterCountBadge');
        if (!filterCountBadge) return;

        let count = 0;
        if (this.currentUserTypeFilter) count++;
        if (this.currentRegionFilter) count++;
        if (this.currentResponsivenessSpeedFilter) count++;
        if (this.currentResponsivenessFrequencyFilter) count++;
        
        // Include category filter if on favorites tab
        if (this.currentTab === 'favorites' && this.currentFavoriteCategory && this.currentFavoriteCategory !== 'all') {
            count++;
        }

        if (count > 0) {
            filterCountBadge.textContent = count;
            filterCountBadge.style.display = 'inline-flex';
        } else {
            filterCountBadge.style.display = 'none';
        }
    }

    hasRegion(regions, filterRegion) {
        if (!regions || !Array.isArray(regions)) return false;
        
        // Normalize regions to uppercase for comparison
        const normalizedRegions = regions.map(r => String(r).toUpperCase().trim()).filter(r => r);
        
        // For GLOBAL filter: require ALL 5 regions to be checked
        if (filterRegion === 'GLOBAL') {
            const requiredRegions = ['AMERICAS', 'CALA', 'EUROPE', 'MEA', 'AP'];
            return requiredRegions.every(region => normalizedRegions.includes(region));
        }
        
        // For specific region filters: match the region OR GLOBAL
        const regionMap = {
            'AMERICAS': ['AMERICAS', 'GLOBAL'],
            'CALA': ['CALA', 'GLOBAL'],
            'EUROPE': ['EUROPE', 'GLOBAL'],
            'MEA': ['MEA', 'GLOBAL'],
            'AP': ['AP', 'GLOBAL']
        };

        const allowedRegions = regionMap[filterRegion] || [filterRegion];
        return normalizedRegions.some(r => allowedRegions.includes(r));
    }

    resetFilters() {
        this.currentUserTypeFilter = '';
        this.currentRegionFilter = '';
        this.currentResponsivenessSpeedFilter = '';
        this.currentResponsivenessFrequencyFilter = '';
        this.currentSearchQuery = '';
        this.currentSort = 'name-asc';
        this.invalidateAllTabCaches();
        
        // Reset category filter if on favorites tab
        if (this.currentTab === 'favorites') {
            this.currentFavoriteCategory = 'all';
            const categoryFilter = document.getElementById('favoritesCategoryFilter');
            if (categoryFilter) {
                categoryFilter.value = 'all';
            }
        }
        
        document.getElementById('userTypeFilter').value = '';
        document.getElementById('regionFilter').value = '';
        const speedEl = document.getElementById('responsivenessSpeedFilter');
        const freqEl = document.getElementById('responsivenessFrequencyFilter');
        if (speedEl) speedEl.value = '';
        if (freqEl) freqEl.value = '';
        const searchInput = document.getElementById('searchInput');
        const sortSelect = document.getElementById('sortSelect');
        if (searchInput) searchInput.value = '';
        if (sortSelect) sortSelect.value = 'name-asc';
        this.updateFilterButtons();
        this.updateFilterCount();
        this.applyFilters();
    }

    updateInsightsFilterCount() {
        const filterCountBadge = document.getElementById('insightsFilterCountBadge');
        if (!filterCountBadge) return;

        let count = 0;
        const dateRange = document.getElementById('insightsDateRange')?.value || 'all';
        const region = document.getElementById('insightsRegion')?.value || 'all';
        const type = document.getElementById('insightsType')?.value || 'all';
        const partnerType = document.getElementById('insightsPartnerType')?.value || 'all';

        if (dateRange !== 'all') count++;
        if (region !== 'all') count++;
        if (type !== 'all') count++;
        if (partnerType !== 'all') count++;

        if (count > 0) {
            filterCountBadge.textContent = count;
            filterCountBadge.style.display = 'inline-flex';
        } else {
            filterCountBadge.style.display = 'none';
        }
    }

    resetInsightsFilters() {
        // Reset insights filter dropdowns to default values
        const dateRangeSelect = document.getElementById('insightsDateRange');
        const regionSelect = document.getElementById('insightsRegion');
        const typeSelect = document.getElementById('insightsType');
        const partnerTypeSelect = document.getElementById('insightsPartnerType');
        
        if (dateRangeSelect) dateRangeSelect.value = 'all';
        if (regionSelect) regionSelect.value = 'all';
        if (typeSelect) typeSelect.value = 'all';
        if (partnerTypeSelect) partnerTypeSelect.value = 'all';
        
        // Update filter count badge
        this.updateInsightsFilterCount();
        
        // Update insights with reset filters
        this.updateInsights();
    }

    // Favorites Management - Airtable API Integration
    getCurrentUserId() {
        // Try to get user ID from HTML element (for Webflow/Memberstack integration)
        const userIdElement = document.getElementById('airtable-user-id');
        if (userIdElement && userIdElement.textContent) {
            return userIdElement.textContent.trim();
        }
        
        // Try to get from URL parameter
        const urlParams = new URLSearchParams(window.location.search);
        const userIdParam = urlParams.get('userId');
        if (userIdParam) {
            return userIdParam;
        }
        
        // Fallback: use localStorage (for development/testing)
        // In production, this should be set via authentication
        let storedUserId = localStorage.getItem('partnerDirectoryUserId');
        if (!storedUserId) {
            // No user ID found - API will use first available user from Users table for testing
            // This is fine for development/testing, but in production you should set a real user ID
            
            // Return null - the API will handle finding a user
            return null;
        }
        
        // Validate stored user ID is a valid Airtable record ID
        if (!storedUserId.startsWith('rec')) {
            console.warn('⚠️ Stored user ID is not a valid Airtable record ID. Clearing and using fallback.');
            localStorage.removeItem('partnerDirectoryUserId');
            return null;
        }
        
        return storedUserId;
    }

    async loadFavorites() {
        try {
            const tableId = PARTNER_DIRECTORY_CONFIG.USER_FAVORITES_TABLE_ID;
            if (!tableId) {
                console.warn('⚠️ USER_FAVORITES_TABLE_ID not configured, favorites will not be loaded');
                this.favorites = [];
                this.updateFavoritesMap();
                return;
            }

            // Get userId - use currentUserId or find first available user for fallback
            let userId = this.currentUserId;
            if (!userId || !userId.startsWith('rec')) {
                // Try to get first user from Users table as fallback
                const USERS_TABLE_ID = PARTNER_DIRECTORY_CONFIG.USERS_TABLE_ID;
                if (USERS_TABLE_ID) {
                    try {
                        const url = `https://api.airtable.com/v0/${PARTNER_DIRECTORY_CONFIG.AIRTABLE_BASE_ID}/${USERS_TABLE_ID}?maxRecords=1`;
                        const response = await fetch(url, {
                            headers: {
                                'Authorization': `Bearer ${PARTNER_DIRECTORY_CONFIG.AIRTABLE_API_KEY}`,
                                'Content-Type': 'application/json'
                            }
                        });
                        if (response.ok) {
                            const data = await response.json();
                            if (data.records && data.records.length > 0) {
                                userId = data.records[0].id;
                            }
                        }
                    } catch (e) {
                        // Fallback failed, continue without userId
                    }
                }
            }

            if (!userId || !userId.startsWith('rec')) {
                // No valid userId, return empty favorites
                this.favorites = [];
                this.updateFavoritesMap();
                return;
            }

            // Build direct Airtable API URL with filter
            const url = new URL(`https://api.airtable.com/v0/${PARTNER_DIRECTORY_CONFIG.AIRTABLE_BASE_ID}/${tableId}`);
            url.searchParams.append('filterByFormula', `{User_ID} = '${userId}'`);
            url.searchParams.append('maxRecords', '1000');

            const response = await fetch(url, {
                headers: {
                    'Authorization': `Bearer ${PARTNER_DIRECTORY_CONFIG.AIRTABLE_API_KEY}`,
                    'Content-Type': 'application/json'
                }
            });
            
            if (!response.ok) {
                throw new Error(`Failed to load favorites: ${response.status} ${response.statusText}`);
            }

            const data = await response.json();
            
            // Map Airtable records to favorites format
            this.favorites = (data.records || []).map(record => {
                const fields = record.fields || {};
                const partnerType = fields['Partner Type'] || '';
                let partnerId = null;
                
                // Get partner ID based on type
                if (partnerType === 'Company') {
                    const companyProfile = fields['Company Profile'];
                    partnerId = Array.isArray(companyProfile) ? companyProfile[0] : companyProfile;
                } else if (partnerType === 'Individual') {
                    // Check both Individual Profile and User Profile fields
                    const individualProfile = fields['Individual Profile'];
                    const userProfile = fields['User Profile'];
                    partnerId = Array.isArray(individualProfile) && individualProfile.length > 0 
                        ? individualProfile[0] 
                        : (Array.isArray(userProfile) && userProfile.length > 0 ? userProfile[0] : null);
                }
                
                return {
                    id: partnerId, // Use partnerId as the id for compatibility
                    type: partnerType.toLowerCase(),
                    category: fields['Category'] || 'Important',
                    favoritedDate: fields['Favorited Date'] || new Date().toISOString(),
                    lastViewed: fields['Last Viewed'] || null,
                    favoriteRecordId: record.id // Store the Airtable record ID for deletion
                };
            });

            this.updateFavoritesMap();
            this.updateStats();
            this.tabContainers.favorites = null;
            if (this.currentTab === 'favorites') {
                this.renderFavorites();
            }
        } catch (error) {
            console.error('❌ Error loading favorites:', error);
            this.favorites = [];
            this.updateFavoritesMap();
            this.tabContainers.favorites = null;
            if (this.currentTab === 'favorites') {
                this.renderFavorites();
            }
        }
    }

    updateFavoritesMap() {
        this.favoritesMap.clear();
        this.favorites.forEach(fav => {
            const key = `${fav.type}-${fav.id}`;
            this.favoritesMap.set(key, fav);
        });
    }

    isFavorited(id, type) {
        const key = `${type}-${id}`;
        return this.favoritesMap.has(key);
    }

    getFavoriteRecordId(id, type) {
        const key = `${type}-${id}`;
        const favorite = this.favoritesMap.get(key);
        return favorite ? favorite.favoriteRecordId : null;
    }

    showCategorySelector(id, type, starButton) {
        // Check if already favorited - if so, just toggle off without showing modal
        const isFavorited = starButton.classList.contains('favorited');
        if (isFavorited) {
            // If already favorited, just remove it directly
            this.toggleFavorite(id, type, 'Important', starButton, true);
            return;
        }
        
        // Create modal overlay
        const modal = document.createElement('div');
        modal.className = 'category-selector-modal';
        modal.innerHTML = `
            <div class="category-selector-content">
                <h3>Select Category</h3>
                <p>Choose a category for this favorite:</p>
                <div class="category-options">
                    <button class="category-option" data-category="Hot Leads">Hot Leads</button>
                    <button class="category-option" data-category="Follow Up">Follow Up</button>
                    <button class="category-option" data-category="Important">Important</button>
                    <button class="category-option" data-category="Research">Research</button>
                    <button class="category-option" data-category="Competitors">Competitors</button>
                    <button class="category-option" data-category="Partners">Partners</button>
                </div>
                <div class="category-selector-actions">
                    <button class="category-cancel-btn">Cancel</button>
                </div>
            </div>
        `;
        
        document.body.appendChild(modal);
        
        // Handle category selection
        modal.querySelectorAll('.category-option').forEach(btn => {
            btn.addEventListener('click', () => {
                const category = btn.dataset.category;
                document.body.removeChild(modal);
                this.toggleFavorite(id, type, category, starButton);
            });
        });
        
        // Handle cancel
        modal.querySelector('.category-cancel-btn').addEventListener('click', () => {
            document.body.removeChild(modal);
        });
        
        // Close on overlay click
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                document.body.removeChild(modal);
            }
        });
    }

    async toggleFavorite(id, type, category = 'Important', starButton = null, isRemoval = false) {
        const tableId = PARTNER_DIRECTORY_CONFIG.USER_FAVORITES_TABLE_ID;
        if (!tableId) {
            console.warn('⚠️ USER_FAVORITES_TABLE_ID not configured');
            alert('Favorites feature is not configured. Please contact support.');
            return;
        }

        // Allow favoriting for all individuals (both Users and User Management tables)
        // If the Individual Profile field doesn't support Users table, Airtable will return an error which we'll handle gracefully
        if (type === 'individual') {
            const individual = this.individuals.find(ind => ind.id === id);
            if (!individual) {
                console.error('❌ Individual not found:', id);
                alert('Individual not found. Please refresh the page and try again.');
                return;
            }
            
            // Log the source table for debugging, but don't block the attempt
            const sourceTable = individual._sourceTable || 'Unknown';
        }

        // Note: API will handle user ID fallback if currentUserId is not available
        if (!this.currentUserId) {
        }

        const isFavorited = isRemoval || this.isFavorited(id, type);
        
        try {
            if (isFavorited) {
                // Remove from favorites
                const favoriteRecordId = this.getFavoriteRecordId(id, type);
                if (!favoriteRecordId) {
                    console.error('❌ Favorite record ID not found');
                    return;
                }

                // Delete using direct Airtable API
                const deleteUrl = `https://api.airtable.com/v0/${PARTNER_DIRECTORY_CONFIG.AIRTABLE_BASE_ID}/${tableId}/${favoriteRecordId}`;
                
                const response = await fetch(deleteUrl, {
                    method: 'DELETE',
                    headers: {
                        'Authorization': `Bearer ${PARTNER_DIRECTORY_CONFIG.AIRTABLE_API_KEY}`,
                        'Content-Type': 'application/json'
                    }
                });

                if (!response.ok) {
                    throw new Error(`Failed to delete favorite: ${response.status} ${response.statusText}`);
                }

                // Remove from local array
                const key = `${type}-${id}`;
                this.favoritesMap.delete(key);
                this.favorites = this.favorites.filter(fav => !(fav.id === id && fav.type === type));
                
                // Update star button visual state and icon
                if (starButton) {
                    starButton.classList.remove('favorited');
                    // Remove category classes
                    starButton.className = starButton.className.replace(/category-\S+/g, '').trim();
                    starButton.innerHTML = this.getCategoryIcon(null);
                }
                
            } else {
                // Add to favorites using direct Airtable API
                // Use provided category or default to 'Important'
                const selectedCategory = category || 'Important';
                
                // Get userId - use currentUserId or find first available user for fallback
                let userId = this.currentUserId;
                if (!userId || !userId.startsWith('rec')) {
                    // Try to get first user from Users table as fallback
                    const USERS_TABLE_ID = PARTNER_DIRECTORY_CONFIG.USERS_TABLE_ID;
                    if (USERS_TABLE_ID) {
                        try {
                            const url = `https://api.airtable.com/v0/${PARTNER_DIRECTORY_CONFIG.AIRTABLE_BASE_ID}/${USERS_TABLE_ID}?maxRecords=1`;
                            const response = await fetch(url, {
                                headers: {
                                    'Authorization': `Bearer ${PARTNER_DIRECTORY_CONFIG.AIRTABLE_API_KEY}`,
                                    'Content-Type': 'application/json'
                                }
                            });
                            if (response.ok) {
                                const data = await response.json();
                                if (data.records && data.records.length > 0) {
                                    userId = data.records[0].id;
                                }
                            }
                        } catch (e) {
                            // Fallback failed
                        }
                    }
                }
                
                if (!userId || !userId.startsWith('rec')) {
                    throw new Error('Invalid user ID. User_ID must be a valid Airtable record ID (starts with "rec").');
                }
                
                // Determine which field to use based on partner type and source table
                const normalizedPartnerType = type.charAt(0).toUpperCase() + type.slice(1).toLowerCase(); // "Company" or "Individual"
                let partnerField;
                if (normalizedPartnerType === 'Company') {
                    partnerField = 'Company Profile';
                } else if (normalizedPartnerType === 'Individual') {
                    // Check source table for individuals
                    const individual = this.individuals.find(ind => ind.id === id);
                    if (individual && individual._sourceTable === 'Users') {
                        partnerField = 'User Profile';
                    } else {
                        partnerField = 'Individual Profile';
                    }
                }
                
                // Check for existing favorite first
                const checkUrl = new URL(`https://api.airtable.com/v0/${PARTNER_DIRECTORY_CONFIG.AIRTABLE_BASE_ID}/${tableId}`);
                let checkFilter = `AND({User_ID} = '${userId}', {${partnerField}} = '${id}')`;
                if (normalizedPartnerType === 'Individual') {
                    // Check both fields for individuals
                    const otherField = partnerField === 'User Profile' ? 'Individual Profile' : 'User Profile';
                    checkFilter = `OR(AND({User_ID} = '${userId}', {${partnerField}} = '${id}'), AND({User_ID} = '${userId}', {${otherField}} = '${id}'))`;
                }
                checkUrl.searchParams.append('filterByFormula', checkFilter);
                checkUrl.searchParams.append('maxRecords', '1');
                
                const checkResponse = await fetch(checkUrl, {
                    headers: {
                        'Authorization': `Bearer ${PARTNER_DIRECTORY_CONFIG.AIRTABLE_API_KEY}`,
                        'Content-Type': 'application/json'
                    }
                });
                
                if (checkResponse.ok) {
                    const checkData = await checkResponse.json();
                    if (checkData.records && checkData.records.length > 0) {
                        // Update existing favorite
                        const existingRecord = checkData.records[0];
                        const updateUrl = `https://api.airtable.com/v0/${PARTNER_DIRECTORY_CONFIG.AIRTABLE_BASE_ID}/${tableId}/${existingRecord.id}`;
                        const updateFields = {
                            'Category': selectedCategory,
                            'Last Viewed': new Date().toISOString()
                        };
                        
                        const updateResponse = await fetch(updateUrl, {
                            method: 'PATCH',
                            headers: {
                                'Authorization': `Bearer ${PARTNER_DIRECTORY_CONFIG.AIRTABLE_API_KEY}`,
                                'Content-Type': 'application/json'
                            },
                            body: JSON.stringify({
                                fields: updateFields
                            })
                        });
                        
                        if (!updateResponse.ok) {
                            throw new Error(`Failed to update favorite: ${updateResponse.status} ${updateResponse.statusText}`);
                        }
                        
                        const updateData = await updateResponse.json();
                        const newFavorite = {
                            id: id,
                            type: type,
                            category: updateData.fields['Category'] || selectedCategory,
                            favoritedDate: updateData.fields['Favorited Date'] || new Date().toISOString(),
                            favoriteRecordId: updateData.id
                        };
                        
                        // Update star button visual state and icon
                        if (starButton) {
                            starButton.classList.add('favorited');
                            const categoryClass = selectedCategory ? `category-${selectedCategory.toLowerCase().replace(/\s+/g, '-')}` : '';
                            starButton.className = starButton.className.replace(/category-\S+/g, '').trim();
                            starButton.classList.add(categoryClass);
                            starButton.innerHTML = this.getCategoryIcon(selectedCategory);
                        }
                        
                        // Update local favorites array
                        const key = `${type}-${id}`;
                        const existingIndex = this.favorites.findIndex(fav => fav.id === id && fav.type === type);
                        if (existingIndex >= 0) {
                            this.favorites[existingIndex] = newFavorite;
                        } else {
                            this.favorites.push(newFavorite);
                        }
                        this.favoritesMap.set(key, newFavorite);
                        return;
                    }
                }
                
                // Create new favorite
                const createUrl = `https://api.airtable.com/v0/${PARTNER_DIRECTORY_CONFIG.AIRTABLE_BASE_ID}/${tableId}`;
                const fields = {
                    'User_ID': [userId], // Linked record array
                    'Partner Type': normalizedPartnerType,
                    [partnerField]: [id], // Linked record array
                    'Category': selectedCategory,
                    'Favorited Date': new Date().toISOString(),
                    'Last Viewed': new Date().toISOString()
                };
                
                const response = await fetch(createUrl, {
                    method: 'POST',
                    headers: {
                        'Authorization': `Bearer ${PARTNER_DIRECTORY_CONFIG.AIRTABLE_API_KEY}`,
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        fields: fields,
                        typecast: true
                    })
                });

                if (!response.ok) {
                    const errorData = await response.json().catch(() => ({}));
                    throw new Error(`Failed to create favorite: ${response.status} ${response.statusText} - ${errorData.error?.message || ''}`);
                }

                const data = await response.json();
                const newFavorite = {
                    id: id,
                    type: type,
                    category: data.fields['Category'] || selectedCategory,
                    favoritedDate: data.fields['Favorited Date'] || new Date().toISOString(),
                    favoriteRecordId: data.id
                };
                
                // Update star button visual state and icon
                if (starButton) {
                    starButton.classList.add('favorited');
                    const category = data.fields['Category'] || selectedCategory;
                    const categoryClass = category ? `category-${category.toLowerCase().replace(/\s+/g, '-')}` : '';
                    // Remove existing category classes
                    starButton.className = starButton.className.replace(/category-\S+/g, '').trim();
                    starButton.classList.add(categoryClass);
                    starButton.innerHTML = this.getCategoryIcon(category);
                }

                this.favorites.push(newFavorite);
                const key = `${type}-${id}`;
                this.favoritesMap.set(key, newFavorite);
                
            }

            this.updateStats();
            this.tabContainers.favorites = null;
            if (this.currentTab === 'favorites') {
                this.renderResults();
            }
        } catch (error) {
            console.error('❌ Error toggling favorite:', error);
            alert(`Failed to ${isFavorited ? 'remove' : 'add'} favorite: ${error.message}`);
        }
    }

    getFavoriteCategory(id, type) {
        const key = `${type}-${id}`;
        const favorite = this.favoritesMap.get(key);
        return favorite ? favorite.category : null;
    }

    getCategoryIcon(category) {
        // Return SVG icon based on category
        const icons = {
            'Hot Leads': `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M12 2v20M2 12h20M12 2l4 8 8 4-8 4-4 8-4-8-8-4 8-4z"/>
            </svg>`,
            'Follow Up': `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/>
                <path d="M13.73 21a2 2 0 0 1-3.46 0"/>
            </svg>`,
            'Important': `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/>
            </svg>`,
            'Research': `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <circle cx="11" cy="11" r="8"/>
                <path d="m21 21-4.35-4.35"/>
            </svg>`,
            'Competitors': `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/>
                <circle cx="9" cy="7" r="4"/>
                <path d="M23 21v-2a4 4 0 0 0-3-3.87"/>
                <path d="M16 3.13a4 4 0 0 1 0 7.75"/>
            </svg>`,
            'Partners': `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/>
                <circle cx="9" cy="7" r="4"/>
                <path d="M23 21v-2a4 4 0 0 0-3-3.87"/>
                <path d="M16 3.13a4 4 0 0 1 0 7.75"/>
                <line x1="9" y1="11" x2="23" y2="11"/>
            </svg>`
        };
        
        // Default to star if category not found or not favorited
        return icons[category] || `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/>
        </svg>`;
    }

    // Global click interceptor for star icons - prevents invalid clicks from cached JavaScript
    setupStarClickInterceptor() {
        // Remove old listeners
        document.removeEventListener('click', this.starClickInterceptor);
        
        // Create new interceptor
        this.starClickInterceptor = (e) => {
            const starButton = e.target.closest('.favorite-star');
            if (!starButton) return;
            
            // Extract individual ID from onclick attribute
            const onclickAttr = starButton.getAttribute('onclick');
            if (!onclickAttr || !onclickAttr.includes('individual')) return;
            
            const match = onclickAttr.match(/toggleFavorite\('([^']+)',\s*'individual'\)/);
            if (!match) return;
            
            const individualId = match[1];
            const individual = this.individuals.find(ind => ind.id === individualId);
            
            if (individual) {
                const sourceTable = individual._sourceTable || 'Unknown';
                if (sourceTable !== 'User Management') {
                    // Block the click
                    e.preventDefault();
                    e.stopPropagation();
                    e.stopImmediatePropagation();
                    
                    // Remove the star icon
                    starButton.style.display = 'none';
                    starButton.remove();
                    
                    // Show alert
                    const reason = sourceTable === 'Users' 
                        ? 'This individual is from the Users table.'
                        : `This individual is from the "${sourceTable}" table.`;
                    
                    alert(`This individual cannot be favorited.\n\n${reason}\n\nOnly individuals from the User Management table can be favorited.\n\n⚠️ Please do a hard refresh (Ctrl+F5) to clear your browser cache.`);
                    
                    console.error('❌ Star click BLOCKED by interceptor:', {
                        id: individualId,
                        name: `${individual.firstName} ${individual.lastName}`,
                        sourceTable: sourceTable
                    });
                    
                    return false;
                }
            }
        };
        
        // Add listener with capture phase to catch before inline onclick
        document.addEventListener('click', this.starClickInterceptor, true);
    }

    // Safety function: Remove star icons from individuals that shouldn't have them
    // This helps if browser cache shows old JavaScript
    removeInvalidStarIcons() {
        const allCards = document.querySelectorAll('.individual-card');
        let removedCount = 0;
        
        allCards.forEach(card => {
            // Get ALL star buttons in this card (in case there are multiple)
            const starButtons = card.querySelectorAll('.favorite-star');
            starButtons.forEach(starButton => {
                // Try to find the individual by checking the onclick handler or data attributes
                const onclickAttr = starButton.getAttribute('onclick');
                if (onclickAttr && onclickAttr.includes('individual')) {
                    // Extract individual ID from onclick: partnerDirectory.toggleFavorite('recXXX', 'individual')
                    const match = onclickAttr.match(/toggleFavorite\('([^']+)',\s*'individual'\)/);
                    if (match) {
                        const individualId = match[1];
                        const individual = this.individuals.find(ind => ind.id === individualId);
                        
                        // We now allow favoriting for both Users and User Management individuals
                        // So we only remove stars if the individual doesn't exist in our data
                        if (!individual) {
                            // Individual not found in our data - remove star to be safe
                            starButton.style.display = 'none';
                            starButton.style.pointerEvents = 'none';
                            starButton.remove();
                            removedCount++;
                        }
                    }
                }
            });
        });
        
        if (removedCount > 0) {
            console.warn(`⚠️ Removed ${removedCount} invalid star icon(s). Please do a hard refresh (Ctrl+F5) to clear browser cache.`);
        }
    }

    renderResults() {
        const resultsGrid = document.getElementById('resultsGrid');
        const emptyState = document.getElementById('emptyState');

        if (this.currentTab === 'favorites') {
            this.renderFavorites();
            return;
        }

        const itemsToShow = this.currentTab === 'companies' ? this.filteredCompanies : this.filteredIndividuals;
        const tabKey = this.currentTab;

        if (itemsToShow.length === 0) {
            this.tabContainers[tabKey] = null;
            resultsGrid.classList.add('hidden');
            emptyState.classList.remove('hidden');
            return;
        }

        resultsGrid.classList.remove('hidden');
        emptyState.classList.add('hidden');
        
        const type = this.currentTab === 'companies' ? 'company' : 'individual';
        
        // On Individuals tab, add a helper class so CSS can limit max columns on wide screens
        if (type === 'individual') {
            resultsGrid.classList.add('results-grid--individuals');
        } else {
            resultsGrid.classList.remove('results-grid--individuals');
        }
        
        // Reuse existing cards from this tab's cached list if we're rebuilding (preserve images)
        const existingList = this.tabContainers[tabKey];
        const existingCards = new Map();
        if (Array.isArray(existingList)) {
            existingList.forEach(card => {
                const cardId = card.dataset?.itemId;
                if (cardId) existingCards.set(cardId, card);
            });
        }
        
        const newCards = [];
        const orderedCards = itemsToShow.map(item => {
            let card = existingCards.get(item.id);
            if (!card) {
                card = type === 'company' ? this.createCompanyCard(item) : this.createIndividualCard(item);
                newCards.push(card);
            }
            return card;
        });

        this.tabContainers[tabKey] = orderedCards;
        resultsGrid.replaceChildren(...orderedCards);

        if (type === 'individual') {
            this.removeInvalidStarIcons();
        }
        if (newCards.length > 0) {
            this.loadLazyImages();
        }
    }

    ensureCategoryFilter() {
        // Show/hide category filter based on current tab
        const categoryFilterGroup = document.getElementById('favoritesCategoryFilterGroup');
        if (categoryFilterGroup) {
            if (this.currentTab === 'favorites') {
                categoryFilterGroup.style.visibility = 'visible';
                categoryFilterGroup.style.position = 'static';
                categoryFilterGroup.style.width = 'auto';
                categoryFilterGroup.style.height = 'auto';
                categoryFilterGroup.style.overflow = 'visible';
            } else {
                categoryFilterGroup.style.visibility = 'hidden';
                categoryFilterGroup.style.position = 'absolute';
                categoryFilterGroup.style.width = '0';
                categoryFilterGroup.style.height = '0';
                categoryFilterGroup.style.overflow = 'hidden';
            }
        }
        
        // Ensure event listener is attached (only once)
        const categoryFilter = document.getElementById('favoritesCategoryFilter');
        if (categoryFilter && !categoryFilter.hasAttribute('data-listener-attached')) {
            categoryFilter.setAttribute('data-listener-attached', 'true');
            categoryFilter.addEventListener('change', (e) => {
                this.currentFavoriteCategory = e.target.value;
                this.invalidateAllTabCaches();
                this.updateFilterCount();
                this.renderFavorites();
            });
        }
    }

    renderFavorites() {
        const resultsGrid = document.getElementById('resultsGrid');
        const emptyState = document.getElementById('emptyState');
        
        if (!resultsGrid) return;
        
        // Ensure category filter exists
        this.ensureCategoryFilter();
        
        // Step 1: Filter favorites by category
        let filteredFavorites = this.favorites;
        if (this.currentFavoriteCategory !== 'all') {
            filteredFavorites = this.favorites.filter(fav => fav.category === this.currentFavoriteCategory);
        }
        
        // Step 2: Get the actual company/individual objects from favorites
        const favoriteCompanies = [];
        const favoriteIndividuals = [];
        
        filteredFavorites.forEach(favorite => {
            if (favorite.type === 'company') {
                const company = this.companies.find(c => c.id === favorite.id);
                if (company) {
                    favoriteCompanies.push(company);
                } else {
                }
            } else if (favorite.type === 'individual') {
                const individual = this.individuals.find(i => i.id === favorite.id);
                if (individual) {
                    favoriteIndividuals.push(individual);
                } else {
                }
            }
        });
        
        // Step 3: Apply all filters (search, user type, region) to favorite companies
        const normalizeType = (type) => {
            if (!type) return '';
            const upperType = String(type).trim().toUpperCase();
            
            if (upperType === "HOTEL OWNERS" || upperType === "HOTEL OWNER" || upperType === "OWNER" || upperType === "OWNERS") {
                return "HOTEL OWNERS";
            } else if (upperType === "HOTEL BRANDS (FRANCHISE)" || upperType === "HOTEL BRAND" || upperType === "HOTEL BRANDS" || upperType === "BRAND" || upperType === "BRANDS" || upperType === "FRANCHISE") {
                return "HOTEL BRANDS (FRANCHISE)";
            } else if (upperType === "HOTEL MGMT. COMPANY" || upperType === "HOTEL MGMT COMPANY" || upperType === "HOTEL MANAGEMENT COMPANY" || upperType === "MGMT" || upperType === "MANAGEMENT" || upperType === "OPERATOR") {
                return "HOTEL MGMT. COMPANY";
            } else if (upperType.includes('BRAND') || upperType.includes('FRANCHISE')) {
                return "HOTEL BRANDS (FRANCHISE)";
            } else if (upperType.includes('MGMT') || upperType.includes('MANAGEMENT') || upperType.includes('OPERATOR')) {
                return "HOTEL MGMT. COMPANY";
            } else if (upperType.includes('OWNER')) {
                return "HOTEL OWNERS";
            }
            return upperType;
        };
        
        const filterType = normalizeType(this.currentUserTypeFilter);
        
        let filteredFavoriteCompanies = favoriteCompanies.filter(company => {
            // Search filter
            if (this.currentSearchQuery) {
                const companyName = (company.name || '').toLowerCase();
                if (!companyName.includes(this.currentSearchQuery)) {
                    return false;
                }
            }
            
            // Type filter
            if (filterType) {
                const companyType = normalizeType(company.userType);
                if (companyType !== filterType) {
                    return false;
                }
            }
            
            // Region filter
            if (this.currentRegionFilter && !this.hasRegion(company.regions, this.currentRegionFilter)) {
                return false;
            }
            
            return true;
        });
        
        // Step 4: Apply all filters to favorite individuals
        let filteredFavoriteIndividuals = favoriteIndividuals.filter(individual => {
            // Search filter
            if (this.currentSearchQuery) {
                const fullName = `${individual.firstName || ''} ${individual.lastName || ''}`.trim().toLowerCase();
                const companyName = (individual.companyName || '').toLowerCase();
                if (!fullName.includes(this.currentSearchQuery) && !companyName.includes(this.currentSearchQuery)) {
                    return false;
                }
            }
            
            // Type filter
            if (filterType) {
                const individualType = normalizeType(individual.userType);
                if (individualType !== filterType) {
                    return false;
                }
            }
            
            // Region filter
            if (this.currentRegionFilter && !this.hasRegion(individual.regions, this.currentRegionFilter)) {
                return false;
            }

            // Responsiveness (speed) filter
            if (this.currentResponsivenessSpeedFilter) {
                const individualSpeed = (individual.responsivenessTimeCategory || '').trim();
                if (individualSpeed !== this.currentResponsivenessSpeedFilter) return false;
            }

            // Responsiveness (frequency) filter
            if (this.currentResponsivenessFrequencyFilter) {
                const individualFreq = (individual.responsivenessFrequencyCategory || '').trim();
                if (individualFreq !== this.currentResponsivenessFrequencyFilter) return false;
            }
            
            return true;
        });
        
        // Step 5: Apply sorting
        filteredFavoriteCompanies = this.sortCompanies(filteredFavoriteCompanies);
        filteredFavoriteIndividuals = this.sortIndividuals(filteredFavoriteIndividuals);
        
        // Step 6: Combine and render
        const totalFiltered = filteredFavoriteCompanies.length + filteredFavoriteIndividuals.length;
        
        if (totalFiltered === 0) {
            this.tabContainers.favorites = null;
            resultsGrid.classList.add('hidden');
            emptyState.classList.remove('hidden');
            const hasFilters = this.currentSearchQuery || this.currentUserTypeFilter || this.currentRegionFilter || this.currentResponsivenessSpeedFilter || this.currentResponsivenessFrequencyFilter || this.currentFavoriteCategory !== 'all';
            emptyState.innerHTML = `
                <h3>No favorites found</h3>
                <p>${hasFilters ? 'No favorites match the current filters.' : 'Click the star icon on any company or individual card to add them to your favorites.'}</p>
            `;
            return;
        }
        
        resultsGrid.classList.remove('hidden');
        emptyState.classList.add('hidden');
        const cardList = [];
        filteredFavoriteCompanies.forEach(company => {
            cardList.push(this.createCompanyCard(company));
        });
        filteredFavoriteIndividuals.forEach(individual => {
            cardList.push(this.createIndividualCard(individual));
        });
        this.tabContainers.favorites = cardList;
        resultsGrid.replaceChildren(...cardList);
    }

    createCompanyCard(company) {
        const card = document.createElement('div');
        card.dataset.itemId = company.id || company.companyId;
        
        // Determine company type for color differentiation
        const normalizeType = (type) => {
            if (!type) return '';
            const upperType = String(type).trim().toUpperCase();
            
            if (upperType === "HOTEL OWNERS" || upperType === "HOTEL OWNER" || upperType === "OWNER" || upperType === "OWNERS") {
                return "owners";
            } else if (upperType === "HOTEL BRANDS (FRANCHISE)" || upperType === "HOTEL BRAND" || upperType === "HOTEL BRANDS" || upperType === "BRAND" || upperType === "BRANDS" || upperType === "FRANCHISE") {
                return "brands";
            } else if (upperType === "HOTEL MGMT. COMPANY" || upperType === "HOTEL MGMT COMPANY" || upperType === "HOTEL MANAGEMENT COMPANY" || upperType === "MGMT" || upperType === "MANAGEMENT" || upperType === "OPERATOR") {
                return "mgmt";
            } else if (upperType.includes('BRAND') || upperType.includes('FRANCHISE')) {
                return "brands";
            } else if (upperType.includes('MGMT') || upperType.includes('MANAGEMENT') || upperType.includes('OPERATOR')) {
                return "mgmt";
            } else if (upperType.includes('OWNER')) {
                return "owners";
            }
            return '';
        };
        
        const companyType = normalizeType(company.userType);
        const typeClass = companyType ? `company-type-${companyType}` : '';
        card.className = `company-card ${typeClass}`;
        
        // Check if favorited
        const isFavorited = this.isFavorited(company.id, 'company');
        const favoriteCategory = this.getFavoriteCategory(company.id, 'company');
        const categoryIcon = isFavorited ? this.getCategoryIcon(favoriteCategory) : this.getCategoryIcon(null);
        const categoryClass = isFavorited && favoriteCategory ? `category-${favoriteCategory.toLowerCase().replace(/\s+/g, '-')}` : '';
        
        // Handle logo with lazy loading - can be string (initial) or object with image URL
        let logoHtml = '';
        if (company.logo && typeof company.logo === 'object' && company.logo.type === 'image' && company.logo.url) {
            // Use lazy loading and show initial as placeholder while image loads
            const initial = company.name ? company.name.charAt(0).toUpperCase() : '?';
            logoHtml = `<div class="company-card__logo company-card__logo-placeholder">${this.escapeHtml(initial)}</div>`;
            logoHtml += `<img data-src="${this.escapeHtml(company.logo.url)}" alt="${this.escapeHtml(company.name)}" class="company-logo-image" width="60" height="60" loading="lazy" onerror="this.style.display='none'; this.previousElementSibling.style.display='flex';" onload="this.previousElementSibling.style.display='none'; this.style.display='block';" />`;
        } else {
            // Use initial letter
            const initial = (company.logo && typeof company.logo === 'string') ? company.logo : (company.name ? company.name.charAt(0).toUpperCase() : '?');
            logoHtml = `<div class="company-card__logo">${this.escapeHtml(initial)}</div>`;
        }
        
        const regions = Array.isArray(company.regions) ? company.regions.filter(r => r && r.trim()) : [];
        const regionsText = regions.length > 0 ? regions.join(', ') : '';
        // Prioritize companyOverview from Airtable, fallback to description
        const description = company.companyOverview || company.description || '';
        const website = company.website || '#';
        // Show HQ badge if location exists (location indicates headquarters)
        const isHQ = company.location && company.location.trim().length > 0;
        const locationText = company.location ? company.location.replace(/HQ/gi, '').trim() : '';
        const companyName = company.name || 'Unknown Company';
        
        card.innerHTML = `
            <button class="favorite-star ${isFavorited ? 'favorited' : ''} ${categoryClass}" 
                    onclick="event.stopPropagation(); partnerDirectory.showCategorySelector('${company.id}', 'company', this);">
                ${categoryIcon}
            </button>
            <div class="company-card__header">
                ${logoHtml}
                <div class="company-card__info">
                    <div class="company-card__name">${this.escapeHtml(companyName)}</div>
                    <div class="company-card__type">${this.escapeHtml(this.getUserTypeDisplayLabel(company.userType) || '')}</div>
                    <div class="company-card__location">
                        ${this.escapeHtml(locationText)}
                        ${isHQ ? '<span class="hq-badge">HQ</span>' : ''}
                    </div>
                </div>
                <div class="company-card__header-stats">
                    <div class="company-card__stat-icons">
                        <span class="stat-icon">🤝</span>
                        <span class="stat-icon">🏢</span>
                    </div>
                    <div class="company-card__stat-values">
                        <span class="company-card__stat-value">${company.closedDeals || 0}</span>
                        <span class="company-card__stat-value">${company.brandCount || 0}</span>
                    </div>
                </div>
            </div>
            ${regionsText ? `
                <div class="company-card__regions">${this.escapeHtml(regionsText)}</div>
            ` : ''}
            <p class="company-card__description">${description ? this.escapeHtml(description) : ''}</p>
            <div class="company-card__footer">
                <a href="${website}" target="_blank" class="company-card__website" onclick="event.stopPropagation();">${this.escapeHtml(website)}</a>
                <button class="company-card__more-btn" data-company-id="${company.id || ''}">More...</button>
            </div>
        `;

        // Add click handler for More button
        const moreBtn = card.querySelector('.company-card__more-btn');
        if (moreBtn) {
            moreBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                this.openCompanyModal(company);
            });
        }

        return card;
    }

    createIndividualCard(individual) {
        const card = document.createElement('div');
        card.dataset.itemId = individual.id;
        
        // Determine user type for color differentiation (same logic as companies)
        const normalizeType = (type) => {
            if (!type) return '';
            const upperType = String(type).trim().toUpperCase();
            
            if (upperType === "HOTEL OWNERS" || upperType === "HOTEL OWNER" || upperType === "OWNER" || upperType === "OWNERS") {
                return "owners";
            } else if (upperType === "HOTEL BRANDS (FRANCHISE)" || upperType === "HOTEL BRAND" || upperType === "HOTEL BRANDS" || upperType === "BRAND" || upperType === "BRANDS" || upperType === "FRANCHISE") {
                return "brands";
            } else if (upperType === "HOTEL MGMT. COMPANY" || upperType === "HOTEL MGMT COMPANY" || upperType === "HOTEL MANAGEMENT COMPANY" || upperType === "MGMT" || upperType === "MANAGEMENT" || upperType === "OPERATOR") {
                return "mgmt";
            } else if (upperType.includes('BRAND') || upperType.includes('FRANCHISE')) {
                return "brands";
            } else if (upperType.includes('MGMT') || upperType.includes('MANAGEMENT') || upperType.includes('OPERATOR')) {
                return "mgmt";
            } else if (upperType.includes('OWNER')) {
                return "owners";
            }
            return '';
        };
        
        // Try to get user type from multiple sources
        // 1. Direct userType field
        // 2. Company Title (might contain user type info)
        // 3. Company name lookup (if we have company info)
        let userTypeToCheck = individual.userType || individual.companyTitle || '';
        
        // If we have a company name, we could also try to look it up from companies array
        // But for now, use what we have
        const userType = normalizeType(userTypeToCheck);
        const typeClass = userType ? `individual-type-${userType}` : '';
        card.className = `individual-card ${typeClass}`;
        
        // Debug: Log user type determination
        if (!userType && (individual.firstName || individual.lastName)) {
            const fullName = `${individual.firstName || ''} ${individual.lastName || ''}`.trim();
        }
        
        const firstName = (individual.firstName || '').trim();
        const lastName = (individual.lastName || '').trim();
        const initials = this.getInitials(firstName, lastName);
        const fullName = `${firstName} ${lastName}`.trim();
        const hasProfilePicture = individual.profilePicture && individual.profilePicture.trim();
        
        // Handle profile picture (eager load so avatars display reliably; hidden img breaks IntersectionObserver)
        let profileHtml = '';
        if (hasProfilePicture) {
            profileHtml = `<img src="${this.escapeHtml(individual.profilePicture)}" alt="${this.escapeHtml(fullName)}" class="individual-card__avatar-image" width="80" height="80" decoding="async" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" />`;
            profileHtml += `<div class="individual-card__avatar" style="display:none;">${initials}</div>`;
        } else {
            profileHtml = `<div class="individual-card__avatar">${initials}</div>`;
        }
        
        const website = individual.website || '';
        const websiteUrl = website.startsWith('http') ? website : (website ? `https://${website}` : '');
        const location = individual.location || '';
        const regions = Array.isArray(individual.regions) ? individual.regions.filter(r => r && r.trim()) : [];
        const regionsText = regions.length > 0 ? regions.join(', ') : '';
        
        // Check if favorited - show star for all individuals (both Users and User Management tables)
        // Note: Users table individuals may not be favoritable if Individual Profile field only links to User Management
        // We'll allow the attempt and handle any Airtable errors gracefully
        const sourceTable = individual._sourceTable || 'Unknown';
        const isFavorited = this.isFavorited(individual.id, 'individual');
        const favoriteCategory = this.getFavoriteCategory(individual.id, 'individual');
        const categoryIcon = isFavorited ? this.getCategoryIcon(favoriteCategory) : this.getCategoryIcon(null);
        const categoryClass = isFavorited && favoriteCategory ? `category-${favoriteCategory.toLowerCase().replace(/\s+/g, '-')}` : '';
        
        // Show star icon for all individuals - allow favoriting attempt
        // If the Individual Profile field doesn't support Users table, Airtable will return an error which we'll handle
        const starIconHtml = `
            <button class="favorite-star ${isFavorited ? 'favorited' : ''} ${categoryClass}" 
                    onclick="event.stopPropagation(); partnerDirectory.showCategorySelector('${individual.id}', 'individual', this);">
                ${categoryIcon}
            </button>
        `;

        card.innerHTML = `
            ${starIconHtml}
            <div class="individual-card__header">
                <div class="individual-card__profile-section">
                    <div class="individual-card__avatar-wrapper">
                        ${profileHtml}
                    </div>
                    <div class="individual-card__info">
                        <div class="individual-card__name">
                            <div class="individual-card__first-name">${this.escapeHtml(firstName || fullName)}</div>
                            ${lastName ? `<div class="individual-card__last-name">${this.escapeHtml(lastName)}</div>` : ''}
                        </div>
                        ${individual.companyName ? `<div class="individual-card__type">${this.escapeHtml(individual.companyName)}</div>` : ''}
                        ${individual.responsivenessCombinedBadge ? `<div class="individual-card__responsiveness-badge" title="Response behavior">${this.escapeHtml(individual.responsivenessCombinedBadge)}</div>` : ''}
                    </div>
                </div>
                <div class="individual-card__header-stats">
                    <div class="individual-card__stat-icons">
                        <span class="stat-icon">🤝</span>
                        <span class="stat-icon">👥</span>
                        <span class="stat-icon">📄</span>
                    </div>
                    <div class="individual-card__stat-values">
                        <div class="individual-card__stat-row">
                            <span class="individual-card__stat-value">${individual.closedDeals || 0}</span>
                            <span class="individual-card__stat-label">Closed Deal(s)</span>
                        </div>
                        <div class="individual-card__stat-row">
                            <span class="individual-card__stat-value">${individual.brandCount || 0}</span>
                            <span class="individual-card__stat-label"># of Brand(s)</span>
                        </div>
                        <div class="individual-card__stat-row">
                            <span class="individual-card__stat-value">${individual.submittedBids || 0}</span>
                            <span class="individual-card__stat-label">Submitted Bid(s)</span>
                        </div>
                    </div>
                </div>
            </div>
            <div class="individual-card__body">
                ${location || website ? `
                    <div class="individual-card__footer-info">
                        ${location ? `
                            <div class="individual-card__location">
                                <span class="location-icon">📍</span>
                                <span>${this.escapeHtml(location)}</span>
                            </div>
                        ` : ''}
                        ${website ? `
                            <div class="individual-card__website">
                                <a href="${this.escapeHtml(websiteUrl)}" target="_blank" class="individual-card__website-link" onclick="event.stopPropagation();">
                                    ${this.escapeHtml(website)}
                                </a>
                            </div>
                        ` : ''}
                    </div>
                ` : ''}
            </div>
            <div class="individual-card__footer">
                ${regionsText ? `
                    <div class="individual-card__regions">${this.escapeHtml(regionsText)}</div>
                ` : '<div></div>'}
                <button class="individual-card__connect-btn" onclick="event.stopPropagation(); partnerDirectory.connectUser('${individual.id || ''}')">
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M16 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"></path>
                        <circle cx="8.5" cy="7" r="4"></circle>
                        <line x1="20" y1="8" x2="20" y2="14"></line>
                        <line x1="23" y1="11" x2="17" y2="11"></line>
                    </svg>
                    Connect
                </button>
            </div>
        `;

        // Note: Edit modal functionality removed - modals not implemented in HTML
        // Card click handler removed to prevent errors

        return card;
    }

    getInitials(firstName, lastName) {
        const first = (firstName || '').charAt(0).toUpperCase();
        const last = (lastName || '').charAt(0).toUpperCase();
        return (first + last) || '?';
    }

    escapeHtml(text) {
        if (!text) return '';
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    /** Map internal Airtable user type to display label (Airtable values unchanged) */
    getUserTypeDisplayLabel(userType) {
        if (!userType) return '';
        const upper = String(userType).trim().toUpperCase();
        if (upper === 'HOTEL MGMT. COMPANY' || upper === 'HOTEL MGMT COMPANY' || upper.includes('MGMT') || upper.includes('MANAGEMENT') || upper.includes('OPERATOR')) {
            return '3rd Party Operator';
        }
        return userType;
    }

    async openCompanyModal(company) {
        const modal = document.getElementById('companyModal');
        const header = document.getElementById('companyModalHeader');
        const body = document.getElementById('companyModalBody');

        if (!modal || !header || !body) return;


        // Build header matching the design
        const companyName = this.escapeHtml(company.name || 'Unknown Company');
        const companyType = this.escapeHtml(this.getUserTypeDisplayLabel(company.userType || company.companyType) || '');
        const location = this.escapeHtml(company.location || '');
        const website = company.website || '';
        const websiteUrl = website.startsWith('http') ? website : `https://${website}`;
        const regions = Array.isArray(company.regions) ? company.regions.filter(r => r && r.trim()) : [];
        const regionsText = regions.length > 0 ? regions.join(', ') : '';
        
        // Get and format Created Date for Member Since
        let memberSinceDate = '';
        if (company.rawFields && company.rawFields["Created Date"]) {
            try {
                const createdDate = new Date(company.rawFields["Created Date"]);
                if (!isNaN(createdDate.getTime())) {
                    const months = ['January', 'February', 'March', 'April', 'May', 'June', 
                                   'July', 'August', 'September', 'October', 'November', 'December'];
                    const month = months[createdDate.getMonth()];
                    const day = createdDate.getDate();
                    const year = createdDate.getFullYear();
                    memberSinceDate = `${month} ${day}, ${year}`;
                }
            } catch (e) {
                console.error('Error formatting date:', e);
            }
        }
        
        // Handle logo display - can be string (initial) or object with image URL
        let logoHtml = '';
        if (company.logo && typeof company.logo === 'object' && company.logo.type === 'image' && company.logo.url) {
            // Use image
            logoHtml = `<img src="${this.escapeHtml(company.logo.url)}" alt="${companyName}" class="company-modal-logo-image" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" />`;
            logoHtml += `<div class="company-modal-logo" style="display:none;">${companyName.charAt(0).toUpperCase()}</div>`;
        } else {
            // Use initial letter
            const initial = (company.logo && typeof company.logo === 'string') ? company.logo : (company.name ? company.name.charAt(0).toUpperCase() : '?');
            logoHtml = `<div class="company-modal-logo">${this.escapeHtml(initial)}</div>`;
        }
        
        header.innerHTML = `
            <div class="company-modal-header-top">
                <div class="company-modal-header-left">
                    ${logoHtml}
                    <div class="company-modal-header-name-section">
                        <h2>${companyName}</h2>
                    </div>
                </div>
            </div>
            <div class="company-modal-header-info">
                ${companyType ? `
                    <div class="company-modal-header-info-row">
                        <div class="company-type">${companyType}</div>
                    </div>
                ` : ''}
                ${location ? `
                    <div class="company-modal-header-info-row">
                        <div class="company-location">${location}</div>
                        <div class="company-modal-hq-badge">HQ</div>
                    </div>
                ` : ''}
                ${regionsText ? `
                    <div class="company-modal-header-info-row">
                        <div class="company-modal-regions-label">Regions:</div>
                        <div class="company-modal-regions">${this.escapeHtml(regionsText)}</div>
                    </div>
                ` : ''}
                ${website || memberSinceDate ? `
                    <div class="company-modal-header-info-row company-modal-header-info-row-last">
                        ${website ? `
                            <a href="${this.escapeHtml(websiteUrl)}" target="_blank" class="company-modal-website">
                                ${this.escapeHtml(website)}
                                <svg class="company-modal-website-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                    <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path>
                                    <polyline points="15 3 21 3 21 9"></polyline>
                                    <line x1="10" y1="14" x2="21" y2="3"></line>
                                </svg>
                            </a>
                        ` : ''}
                        ${memberSinceDate ? `
                            <div class="company-modal-member-since">
                                Member Since: <strong>${this.escapeHtml(memberSinceDate)}</strong>
                            </div>
                        ` : ''}
                    </div>
                ` : ''}
            </div>
        `;

        // Show modal immediately with available data, then load additional data asynchronously
        const description = company.companyOverview || company.description || '';
        
        // Get industries from company data
        let industries = company.industries || [];
        // Get transactions from company data
        let transactions = company.transactions || [];
        
        // Get brands from company data
        let brands = company.brands || [];
        const brandRecordIds = company.brandRecordIds || [];
        
        // Initialize caches if they don't exist
        if (!this.teamMembersCache) this.teamMembersCache = new Map();
        if (!this.brandNamesCache) this.brandNamesCache = new Map();
        
        // Check cache for team members
        const companyCacheKey = company.id || company.companyId;
        let teamMembers = this.teamMembersCache.get(companyCacheKey) || [];
        
        // Check cache for brands
        const cachedBrands = [];
        const uncachedBrandIds = [];
        if (brandRecordIds.length > 0) {
            brandRecordIds.forEach(id => {
                if (this.brandNamesCache.has(id)) {
                    cachedBrands.push(this.brandNamesCache.get(id));
                } else {
                    uncachedBrandIds.push(id);
                }
            });
        }
        brands = [...brands, ...cachedBrands];
        
        // Start async data fetching (don't await - show modal immediately)
        const loadAsyncData = async () => {
            // Fetch team members in parallel if not cached
            if (!this.teamMembersCache.has(companyCacheKey)) {
                if (company.userManagementRecordIds && company.userManagementRecordIds.length > 0) {
                    teamMembers = await this.fetchTeamMembersFromUserManagement(company.userManagementRecordIds);
                } else {
                    teamMembers = await this.fetchTeamMembers(company.companyId || company.id);
                }
                this.teamMembersCache.set(companyCacheKey, teamMembers);
                // Update team section
                this.updateModalTeamSection(modal, teamMembers);
            }
            
            // Fetch uncached brand names in parallel
            if (uncachedBrandIds.length > 0) {
                try {
                    const fetchedBrandNames = await this.fetchBrandNames(uncachedBrandIds);
                    // Cache the fetched brands
                    fetchedBrandNames.forEach((name, index) => {
                        if (uncachedBrandIds[index]) {
                            this.brandNamesCache.set(uncachedBrandIds[index], name);
                        }
                    });
                    brands = [...brands, ...fetchedBrandNames];
                    // Update brands section
                    this.updateModalBrandsSection(modal, brands);
                } catch (error) {
                    console.error('Error fetching brand names:', error);
                }
            }
        };
        
        // Start loading async data (fire and forget)
        loadAsyncData();
        
        // If no brands found, check rawFields for brand-related data
        if (brands.length === 0 && company.rawFields) {
            for (const [key, value] of Object.entries(company.rawFields)) {
                const keyLower = key.toLowerCase();
                if ((keyLower.includes('brand') && (keyLower.includes('operate') || keyLower.includes('support') || keyLower.includes('control'))) && value) {
                    if (Array.isArray(value)) {
                        // Collect record IDs to fetch
                        const recordIdsToFetch = [];
                        const directBrands = [];
                        
                        value.forEach(v => {
                            if (typeof v === 'string') {
                                if (v.startsWith('rec')) {
                                    recordIdsToFetch.push(v);
                                } else {
                                    directBrands.push(v);
                                }
                            } else if (v && typeof v === 'object') {
                                // Extract brand name from linked record if available
                                const brandName = v.fields?.["Brand Name"] || 
                                               v.fields?.name || 
                                               v.fields?.Name ||
                                               v.name ||
                                               '';
                                if (brandName) {
                                    directBrands.push(brandName);
                                } else if (v.id) {
                                    recordIdsToFetch.push(v.id);
                                }
                            }
                        });
                        
                        brands = [...brands, ...directBrands];
                        
                        // Fetch brand names from record IDs
                        if (recordIdsToFetch.length > 0) {
                            try {
                                const fetchedBrandNames = await this.fetchBrandNames(recordIdsToFetch);
                                brands = [...brands, ...fetchedBrandNames];
                            } catch (error) {
                                console.error('Error fetching brand names from rawFields:', error);
                            }
                        }
                    } else if (typeof value === 'string') {
                        const extractedBrands = value.split(',').map(b => b.trim()).filter(b => b && !b.startsWith('rec'));
                        brands = [...brands, ...extractedBrands];
                    }
                    break;
                }
            }
        }
        
        // If no industries in the industries field, check rawFields for industry-related data
        if (industries.length === 0 && company.rawFields) {
            // Handle both object and array cases
            const fieldsToCheck = Array.isArray(company.rawFields) ? {} : company.rawFields;
            
            for (const [key, value] of Object.entries(fieldsToCheck)) {
                const keyLower = key.toLowerCase();
                if ((keyLower.includes('industry') || keyLower.includes('sector')) && value) {
                    if (Array.isArray(value)) {
                        industries = value.map(v => typeof v === 'string' ? v : (v.name || String(v)));
                    } else if (typeof value === 'string') {
                        industries = value.split(',').map(v => v.trim()).filter(v => v);
                    }
                    break;
                }
            }
        }
        
        // If no transactions in the transactions field, check rawFields for transaction-related data
        if (transactions.length === 0 && company.rawFields) {
            // Handle both object and array cases
            const fieldsToCheck = Array.isArray(company.rawFields) ? {} : company.rawFields;
            
            for (const [key, value] of Object.entries(fieldsToCheck)) {
                const keyLower = key.toLowerCase();
                if ((keyLower.includes('transaction') || keyLower.includes('deal')) && value) {
                    if (Array.isArray(value)) {
                        transactions = value.map(v => {
                            if (typeof v === 'string') return { name: v, type: '', logo: '' };
                            return { name: v.name || v.company || String(v), type: v.type || '', logo: v.logo || '' };
                        });
                    }
                    break;
                }
            }
        }
        
        // Display additional fields from rawFields
        let additionalFields = [];
        // Fields to exclude from display
        const excludedFields = [
            'Company_ID',
            'Record ID',
            'Open to Contact',
            "Company's role in the hotel ecosystem",
            'Company Platform Visibility',
            'Created Date',
            'USER MANAGEMENT',
            'User Management',
            'user management',
            'User_ID',
            'User ID',
            'user_id',
            'USER_ID',
            'User_Favorites',
            'User Favorites',
            'user_favorites',
            'USER_FAVORITES',
            'BRAND NAME (FROM BRANDS YOU OPERATE / SUPPORT)',
            'Brand Name (from Brands You Operate / Support)',
            'Brand Name',
            'BRAND_BASICS_ID (FROM BRAND NAME (FROM BRANDS YOU OPERATE / SUPPORT))',
            'BRAND_BASICS_ID',
            'Brand_Basics_ID (from Brand Name (from Brands You Operate / Support))',
            'Brand_Basics_ID',
            'Primary Services',
            'Additional Services',
            'PRIMARY SERVICES',
            'ADDITIONAL SERVICES',
            'Primary Services Provided',
            'Additional Services Provided'
        ];
        
        // Derive services from company or rawFields (rawFields may have comma-separated "Primary Services"/"Additional Services")
        const toArray = (v) => {
            if (Array.isArray(v)) return v.filter(Boolean);
            if (typeof v === 'string') return v.split(',').map(s => s.trim()).filter(Boolean);
            return [];
        };
        let primary = toArray(company.primaryServices);
        let allServices = toArray(company.services);
        if (allServices.length === 0 && company.rawFields && typeof company.rawFields === 'object') {
            const rf = company.rawFields;
            const keys = Object.keys(rf);
            const primaryKey = keys.find(k => /primary/i.test(k) && /services/i.test(k) && !/additional/i.test(k));
            const additionalKey = keys.find(k => /additional/i.test(k) && /services/i.test(k));
            if (primaryKey && rf[primaryKey]) primary = toArray(rf[primaryKey]);
            if (additionalKey && rf[additionalKey]) {
                const addl = toArray(rf[additionalKey]);
                allServices = [...primary, ...addl.filter(s => !primary.includes(s))];
            }
            if (allServices.length === 0 && primary.length > 0) allServices = [...primary];
        }
        const nonPrimary = allServices.filter(s => !primary.includes(s));
        
        if (company.rawFields && typeof company.rawFields === 'object' && !Array.isArray(company.rawFields)) {
            for (const [key, value] of Object.entries(company.rawFields)) {
                // Skip excluded fields (including Primary/Additional Services - shown in dedicated section)
                if (excludedFields.includes(key)) continue;
                if (/primary/i.test(key) && /services/i.test(key)) continue;
                if (/additional/i.test(key) && /services/i.test(key)) continue;
                
                // Skip empty values
                if (value === null || value === undefined || value === '') continue;
                
                // Format the value for display
                let displayValue = '';
                if (Array.isArray(value)) {
                    displayValue = value.map(v => typeof v === 'string' ? v : (v.name || String(v))).join(', ');
                } else if (typeof value === 'object') {
                    displayValue = JSON.stringify(value).substring(0, 100);
                } else {
                    displayValue = String(value);
                }
                
                // Only show fields that have meaningful content
                if (displayValue && displayValue.trim()) {
                    additionalFields.push({ key, value: displayValue });
                }
            }
        }
        
        
        body.innerHTML = `
            <div class="company-modal-body-left">
                ${description ? `
                    <div class="company-modal-section">
                        <div class="company-modal-section-title">OVERVIEW</div>
                        <div class="company-modal-section-content">${this.escapeHtml(description)}</div>
                    </div>
                ` : ''}
                
                ${industries.length > 0 ? `
                    <div class="company-modal-section">
                        <div class="company-modal-section-title">INDUSTRIES</div>
                        <div class="company-modal-industries-list">
                            ${industries.slice(0, 3).map(industry => {
                                const industryName = typeof industry === 'string' ? industry : (industry.name || String(industry));
                                const industryCount = typeof industry === 'object' && industry.count ? industry.count : '';
                                return `
                                <div class="company-modal-industry-item">
                                    <span class="company-modal-industry-name">${this.escapeHtml(industryName)}</span>
                                    <div class="company-modal-industry-count">
                                        ${industryCount ? `<span>(${industryCount})</span>` : ''}
                                        <span class="company-modal-industry-arrow">→</span>
                                    </div>
                                </div>
                            `;
                            }).join('')}
                        </div>
                        ${industries.length > 3 ? `
                            <a href="#" class="company-modal-show-all">
                                Show all industries
                                <span>↓</span>
                            </a>
                        ` : ''}
                    </div>
                ` : ''}

                ${transactions.length > 0 ? `
                    <div class="company-modal-section">
                        <div class="company-modal-transactions-header">
                            <div class="company-modal-section-title">TRANSACTIONS (${transactions.length})</div>
                            <select class="company-modal-transactions-dropdown">
                                <option>All Sectors</option>
                            </select>
                        </div>
                        <div class="company-modal-transactions-grid">
                            ${transactions.slice(0, 3).map(transaction => {
                                const txnName = transaction.name || transaction.company || String(transaction);
                                const txnType = transaction.type || 'TRANSACTION';
                                const txnLogo = transaction.logo || (txnName ? txnName.charAt(0).toUpperCase() : '?');
                                return `
                                <div class="company-modal-transaction-card">
                                    <div class="company-modal-transaction-badge">${companyName.toUpperCase()}</div>
                                    <div class="company-modal-transaction-type">${this.escapeHtml(txnType)}</div>
                                    <div class="company-modal-transaction-logo">${this.escapeHtml(txnLogo)}</div>
                                </div>
                            `;
                            }).join('')}
                        </div>
                        ${transactions.length > 3 ? `
                            <a href="#" class="company-modal-show-all">
                                Show all transactions
                                <span>↓</span>
                            </a>
                        ` : ''}
                    </div>
                ` : ''}
                
                ${brands.length > 0 ? `
                    <div class="company-modal-section">
                        <div class="company-modal-section-title">BRANDS (${brands.length})</div>
                        <div class="company-modal-brands-grid">
                            ${(() => {
                                const visibleBrands = brands.slice(0, 16); // Up to 4 rows x 4 columns = 16 items
                                const hiddenBrands = brands.slice(16);
                                
                                // Distribute visible brands across 4 columns
                                const columns = [[], [], [], []];
                                visibleBrands.forEach((brand, index) => {
                                    const columnIndex = index % 4;
                                    columns[columnIndex].push(brand);
                                });
                                
                                let html = '<div class="company-modal-brands-columns">';
                                columns.forEach((column, colIndex) => {
                                    const hasHidden = hiddenBrands.length > 0;
                                    html += `<div class="company-modal-brands-column${hasHidden ? ' has-hidden' : ''}">`;
                                    column.forEach(brand => {
                                        const brandName = typeof brand === 'string' ? brand : (brand.name || String(brand));
                                        html += `
                                            <div class="company-modal-brand-item">
                                                <span class="company-modal-brand-name">${this.escapeHtml(brandName)}</span>
                                            </div>
                                        `;
                                    });
                                    html += '</div>';
                                });
                                html += '</div>';
                                
                                if (hiddenBrands.length > 0) {
                                    html += `<div class="company-modal-brands-hidden" style="display: none;">`;
                                    // Distribute hidden brands across 4 columns
                                    const hiddenColumns = [[], [], [], []];
                                    hiddenBrands.forEach((brand, index) => {
                                        const columnIndex = index % 4;
                                        hiddenColumns[columnIndex].push(brand);
                                    });
                                    html += '<div class="company-modal-brands-columns">';
                                    hiddenColumns.forEach((column) => {
                                        html += '<div class="company-modal-brands-column">';
                                        column.forEach(brand => {
                                            const brandName = typeof brand === 'string' ? brand : (brand.name || String(brand));
                                            html += `
                                                <div class="company-modal-brand-item">
                                                    <span class="company-modal-brand-name">${this.escapeHtml(brandName)}</span>
                                                </div>
                                            `;
                                        });
                                        html += '</div>';
                                    });
                                    html += '</div>';
                                    html += `</div>`;
                                }
                                
                                return html;
                            })()}
                        </div>
                        ${brands.length > 16 ? `
                            <a href="#" class="company-modal-show-all" id="companyModalShowAllBrands">
                                Show all brands
                                <span class="company-modal-show-all-icon">▼</span>
                            </a>
                        ` : ''}
                    </div>
                ` : ''}
                
                ${(allServices.length > 0 || primary.length > 0) ? `
                    <div class="company-modal-section">
                        ${(() => {
                            const renderServicesGrid = (items, emptyMsg) => {
                                if (items.length === 0) {
                                    return '<div class="company-modal-services-grid"><div class="company-modal-services-columns"><div class="company-modal-services-column"><div class="company-modal-service-item"><span class="company-modal-service-name" style="color: var(--neutral--500);">' + emptyMsg + '</span></div></div></div></div>';
                                }
                                const visible = items.slice(0, 8);
                                const hidden = items.slice(8);
                                const columns = [[], []];
                                visible.forEach((item, i) => columns[i % 2].push(item));
                                let html = '<div class="company-modal-services-grid"><div class="company-modal-services-columns">';
                                columns.forEach((col) => {
                                    html += '<div class="company-modal-services-column' + (hidden.length > 0 ? ' has-hidden' : '') + '">';
                                    col.forEach(s => {
                                        html += '<div class="company-modal-service-item"><span class="company-modal-service-name">' + this.escapeHtml(s) + '</span></div>';
                                    });
                                    html += '</div>';
                                });
                                html += '</div>';
                                if (hidden.length > 0) {
                                    const hiddenCols = [[], []];
                                    hidden.forEach((item, i) => hiddenCols[i % 2].push(item));
                                    html += '<div class="company-modal-services-hidden" style="display: none;"><div class="company-modal-services-columns">';
                                    hiddenCols.forEach((col) => {
                                        html += '<div class="company-modal-services-column">';
                                        col.forEach(s => {
                                            html += '<div class="company-modal-service-item"><span class="company-modal-service-name">' + this.escapeHtml(s) + '</span></div>';
                                        });
                                        html += '</div>';
                                    });
                                    html += '</div></div>';
                                }
                                html += '</div>';
                                return html;
                            };
                            let out = '<div class="company-modal-section-title">PRIMARY SERVICES (' + primary.length + ')</div>';
                            out += renderServicesGrid(primary, 'No primary services');
                            if (primary.length > 8) out += '<a href="#" class="company-modal-show-all company-modal-show-all-primary">Show all primary services<span class="company-modal-show-all-icon">▼</span></a>';
                            out += '<div class="company-modal-section-title" style="margin-top: 20px;">ADDITIONAL SERVICES (' + nonPrimary.length + ')</div>';
                            out += renderServicesGrid(nonPrimary, 'No additional services');
                            if (nonPrimary.length > 8) out += '<a href="#" class="company-modal-show-all company-modal-show-all-additional" id="companyModalShowAllServices">Show all additional services<span class="company-modal-show-all-icon">▼</span></a>';
                            return out;
                        })()}
                    </div>
                ` : ''}
                
                ${additionalFields.length > 0 ? `
                    <div class="company-modal-section">
                        <div class="company-modal-section-title">ADDITIONAL INFORMATION</div>
                        <div class="company-modal-section-content">
                            ${additionalFields.map(field => `
                                <div style="margin-bottom: 12px;">
                                    <div style="font-weight: 600; color: var(--neutral--100); margin-bottom: 4px; text-transform: uppercase; font-size: 11px; letter-spacing: 0.5px;">
                                        ${this.escapeHtml(field.key)}
                                    </div>
                                    <div style="color: var(--neutral--400); font-size: 14px;">
                                        ${this.escapeHtml(field.value)}
                                    </div>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                ` : ''}
            </div>

            <div class="company-modal-body-right">
                <div class="company-modal-section">
                    <div class="company-modal-section-title">TEAM (${teamMembers.length})</div>
                    <div class="company-modal-team-list">
                        ${teamMembers.length > 0 ? teamMembers.map(member => {
                            const initials = this.getInitials(member.firstName, member.lastName);
                            const fullName = `${member.firstName || ''} ${member.lastName || ''}`.trim();
                            const hasProfilePicture = member.profilePicture && member.profilePicture.trim();
                            
                            return `
                                <div class="company-modal-team-member">
                                    ${hasProfilePicture ? `
                                        <img src="${this.escapeHtml(member.profilePicture)}" alt="${this.escapeHtml(fullName)}" class="company-modal-team-avatar company-modal-team-avatar-image" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" />
                                        <div class="company-modal-team-avatar" style="display:none;">${initials}</div>
                                    ` : `
                                        <div class="company-modal-team-avatar">${initials}</div>
                                    `}
                                    <div class="company-modal-team-info">
                                        <div class="company-modal-team-name">${this.escapeHtml(fullName)}</div>
                                    </div>
                                    <div class="company-modal-team-actions">
                                        ${member.email ? `
                                            <a href="mailto:${this.escapeHtml(member.email)}" class="company-modal-team-action" title="Email">
                                                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                                    <path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"></path>
                                                    <polyline points="22,6 12,13 2,6"></polyline>
                                                </svg>
                                            </a>
                                        ` : ''}
                                    </div>
                                </div>
                            `;
                        }).join('') : `
                            <div style="color: var(--neutral--500); font-size: 14px; padding: 16px 0;">
                                No team members found
                            </div>
                        `}
                    </div>
                </div>
                
                ${company.closedDeals > 0 || company.brandCount > 0 || company.submittedBids > 0 ? `
                    <div class="company-modal-section">
                        <div class="company-modal-section-title">STATISTICS</div>
                        <div class="company-modal-stats">
                            ${company.closedDeals > 0 ? `
                                <div class="company-modal-stat">
                                    <div class="company-modal-stat-label">Closed Deals</div>
                                    <div class="company-modal-stat-value">${company.closedDeals}</div>
                                </div>
                            ` : ''}
                            ${company.brandCount > 0 ? `
                                <div class="company-modal-stat">
                                    <div class="company-modal-stat-label">Brand Count</div>
                                    <div class="company-modal-stat-value">${company.brandCount}</div>
                                </div>
                            ` : ''}
                            ${company.submittedBids > 0 ? `
                                <div class="company-modal-stat">
                                    <div class="company-modal-stat-label">Submitted Bids</div>
                                    <div class="company-modal-stat-value">${company.submittedBids}</div>
                                </div>
                            ` : ''}
                        </div>
                    </div>
                ` : ''}
            </div>
        `;

        modal.style.display = 'flex';
        document.body.style.overflow = 'hidden';
        
        // Add event listeners for "Show all primary/additional services" links (event delegation)
        modal.querySelectorAll('.company-modal-show-all-primary, .company-modal-show-all-additional').forEach(link => {
            link.addEventListener('click', (e) => {
                e.preventDefault();
                const grid = link.previousElementSibling;
                const hidden = grid && grid.querySelector('.company-modal-services-hidden');
                if (hidden) {
                    const isExpanded = hidden.style.display !== 'none';
                    if (isExpanded) {
                        hidden.style.display = 'none';
                        link.innerHTML = link.classList.contains('company-modal-show-all-primary') ? 'Show all primary services<span class="company-modal-show-all-icon">▼</span>' : 'Show all additional services<span class="company-modal-show-all-icon">▼</span>';
                    } else {
                        hidden.style.display = 'block';
                        link.innerHTML = link.classList.contains('company-modal-show-all-primary') ? 'Show less primary services<span class="company-modal-show-all-icon">▲</span>' : 'Show less additional services<span class="company-modal-show-all-icon">▲</span>';
                    }
                }
            });
        });
        
        // Add event listener for "Show all brands" link
        const showAllBrandsLink = document.getElementById('companyModalShowAllBrands');
        if (showAllBrandsLink) {
            showAllBrandsLink.addEventListener('click', (e) => {
                e.preventDefault();
                const hiddenBrands = modal.querySelector('.company-modal-brands-hidden');
                if (hiddenBrands) {
                    const isExpanded = hiddenBrands.style.display !== 'none';
                    if (isExpanded) {
                        hiddenBrands.style.display = 'none';
                        showAllBrandsLink.innerHTML = 'Show all brands <span class="company-modal-show-all-icon">▼</span>';
                    } else {
                        hiddenBrands.style.display = 'block';
                        showAllBrandsLink.innerHTML = 'Show less brands <span class="company-modal-show-all-icon">▲</span>';
                    }
                }
            });
        }
    }

    updateModalTeamSection(modal, teamMembers) {
        if (!modal) return;
        
        // Find the team section by looking for the section with team-list
        const teamList = modal.querySelector('.company-modal-team-list');
        if (!teamList) return;
        
        const teamSection = teamList.closest('.company-modal-section');
        if (!teamSection) return;
        
        const teamTitle = teamSection.querySelector('.company-modal-section-title');
        
        if (teamTitle) {
            teamTitle.textContent = `TEAM (${teamMembers.length})`;
        }
        
        if (teamList) {
            if (teamMembers.length > 0) {
                teamList.innerHTML = teamMembers.map(member => {
                    const initials = this.getInitials(member.firstName, member.lastName);
                    const fullName = `${member.firstName || ''} ${member.lastName || ''}`.trim();
                    const hasProfilePicture = member.profilePicture && member.profilePicture.trim();
                    
                    return `
                        <div class="company-modal-team-member">
                            ${hasProfilePicture ? `
                                <img src="${this.escapeHtml(member.profilePicture)}" alt="${this.escapeHtml(fullName)}" class="company-modal-team-avatar company-modal-team-avatar-image" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" />
                                <div class="company-modal-team-avatar" style="display:none;">${initials}</div>
                            ` : `
                                <div class="company-modal-team-avatar">${initials}</div>
                            `}
                            <div class="company-modal-team-info">
                                <div class="company-modal-team-name">${this.escapeHtml(fullName)}</div>
                            </div>
                            <div class="company-modal-team-actions">
                                ${member.email ? `
                                    <a href="mailto:${this.escapeHtml(member.email)}" class="company-modal-team-action" title="Email">
                                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                            <path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"></path>
                                            <polyline points="22,6 12,13 2,6"></polyline>
                                        </svg>
                                    </a>
                                ` : ''}
                            </div>
                        </div>
                    `;
                }).join('');
            } else {
                teamList.innerHTML = `
                    <div style="color: var(--neutral--500); font-size: 14px; padding: 16px 0;">
                        No team members found
                    </div>
                `;
            }
        }
    }

    updateModalBrandsSection(modal, brands) {
        if (!modal || !brands || brands.length === 0) return;
        
        // Find the brands section by looking for the brands-grid
        const brandsGrid = modal.querySelector('.company-modal-brands-grid');
        if (!brandsGrid) return;
        
        const brandsSection = brandsGrid.closest('.company-modal-section');
        if (!brandsSection) return;
        
        const brandsTitle = brandsSection.querySelector('.company-modal-section-title');
        
        // Update title
        if (brandsTitle && brandsTitle.textContent.includes('BRANDS')) {
            brandsTitle.textContent = `BRANDS (${brands.length})`;
        }
        
        // Build brands HTML
        const visibleBrands = brands.slice(0, 16);
        const hiddenBrands = brands.slice(16);
        
        // Distribute visible brands across 4 columns
        const columns = [[], [], [], []];
        visibleBrands.forEach((brand, index) => {
            const columnIndex = index % 4;
            columns[columnIndex].push(brand);
        });
        
        let html = '<div class="company-modal-brands-columns">';
        columns.forEach((column, colIndex) => {
            const hasHidden = hiddenBrands.length > 0;
            html += `<div class="company-modal-brands-column${hasHidden ? ' has-hidden' : ''}">`;
            column.forEach(brand => {
                const brandName = typeof brand === 'string' ? brand : (brand.name || String(brand));
                html += `
                    <div class="company-modal-brand-item">
                        <span class="company-modal-brand-name">${this.escapeHtml(brandName)}</span>
                    </div>
                `;
            });
            html += '</div>';
        });
        html += '</div>';
        
        if (hiddenBrands.length > 0) {
            html += `<div class="company-modal-brands-hidden" style="display: none;">`;
            // Distribute hidden brands across 4 columns
            const hiddenColumns = [[], [], [], []];
            hiddenBrands.forEach((brand, index) => {
                const columnIndex = index % 4;
                hiddenColumns[columnIndex].push(brand);
            });
            html += '<div class="company-modal-brands-columns">';
            hiddenColumns.forEach((column) => {
                html += '<div class="company-modal-brands-column">';
                column.forEach(brand => {
                    const brandName = typeof brand === 'string' ? brand : (brand.name || String(brand));
                    html += `
                        <div class="company-modal-brand-item">
                            <span class="company-modal-brand-name">${this.escapeHtml(brandName)}</span>
                        </div>
                    `;
                });
                html += '</div>';
            });
            html += '</div>';
            html += `</div>`;
        }
        
        brandsGrid.innerHTML = html;
        
        // Update or add "Show all brands" link
        let showAllBrandsLink = modal.querySelector('#companyModalShowAllBrands');
        if (brands.length > 16) {
            if (!showAllBrandsLink) {
                // Create the link if it doesn't exist
                const brandsSection = brandsGrid.closest('.company-modal-section');
                if (brandsSection) {
                    showAllBrandsLink = document.createElement('a');
                    showAllBrandsLink.href = '#';
                    showAllBrandsLink.className = 'company-modal-show-all';
                    showAllBrandsLink.id = 'companyModalShowAllBrands';
                    brandsSection.appendChild(showAllBrandsLink);
                }
            }
            if (showAllBrandsLink) {
                showAllBrandsLink.innerHTML = 'Show all brands <span class="company-modal-show-all-icon">▼</span>';
                // Re-attach event listener
                showAllBrandsLink.onclick = (e) => {
                    e.preventDefault();
                    const hiddenBrands = modal.querySelector('.company-modal-brands-hidden');
                    if (hiddenBrands) {
                        const isExpanded = hiddenBrands.style.display !== 'none';
                        if (isExpanded) {
                            hiddenBrands.style.display = 'none';
                            showAllBrandsLink.innerHTML = 'Show all brands <span class="company-modal-show-all-icon">▼</span>';
                        } else {
                            hiddenBrands.style.display = 'block';
                            showAllBrandsLink.innerHTML = 'Show less brands <span class="company-modal-show-all-icon">▲</span>';
                        }
                    }
                };
            }
        } else if (showAllBrandsLink) {
            showAllBrandsLink.remove();
        }
    }

    setupLazyImageLoading() {
        // Use Intersection Observer for efficient lazy loading
        if ('IntersectionObserver' in window) {
            this.imageObserver = new IntersectionObserver((entries, observer) => {
                entries.forEach(entry => {
                    if (entry.isIntersecting) {
                        const img = entry.target;
                        if (img.dataset.src) {
                            img.src = img.dataset.src;
                            img.removeAttribute('data-src');
                            img.removeAttribute('loading');
                            observer.unobserve(img);
                        }
                    }
                });
            }, {
                rootMargin: '50px' // Start loading 50px before image enters viewport
            });
        }
    }

    loadLazyImages() {
        // Load images that are in viewport or about to be
        const lazyImages = document.querySelectorAll('img[data-src]');
        
        if (this.imageObserver) {
            // Use Intersection Observer for efficient loading
            lazyImages.forEach(img => {
                this.imageObserver.observe(img);
            });
        } else {
            // Fallback: load all images immediately if Intersection Observer not supported
            lazyImages.forEach(img => {
                if (img.dataset.src) {
                    img.src = img.dataset.src;
                    img.removeAttribute('data-src');
                }
            });
        }
    }

    async fetchCompanyNames(companyRecordIds) {
        if (!companyRecordIds || companyRecordIds.length === 0) return {};
        const baseId = PARTNER_DIRECTORY_CONFIG.AIRTABLE_BASE_ID;
        const apiKey = PARTNER_DIRECTORY_CONFIG.AIRTABLE_API_KEY;
        const companyProfileTableId = PARTNER_DIRECTORY_CONFIG.COMPANY_PROFILE_TABLE_ID;
        const headers = { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' };
        try {
            // Fetch all company records in parallel (was sequential - major speedup)
            const results = await Promise.all(companyRecordIds.map(async (recordId) => {
                try {
                    const url = `https://api.airtable.com/v0/${baseId}/${companyProfileTableId}/${recordId}`;
                    const response = await fetch(url, { headers });
                    if (!response.ok) return { recordId, companyName: null, userType: null };
                    const data = await response.json();
                    const fields = data.fields || {};
                    const companyName = fields["Company Name"] || '';
                    const userType = fields["User Type"] || fields["Company Type"] || '';
                    return { recordId, companyName: companyName || null, userType: userType || null };
                } catch (e) {
                    console.error(`Error fetching company record ${recordId}:`, e);
                    return { recordId, companyName: null, userType: null };
                }
            }));
            const companyNamesMap = {};
            const companyUserTypesMap = {};
            results.forEach(({ recordId, companyName, userType }) => {
                if (companyName) {
                    companyNamesMap[recordId] = companyName;
                    if (userType) companyUserTypesMap[recordId] = userType;
                }
            });
            this.companyUserTypesMap = { ...this.companyUserTypesMap, ...companyUserTypesMap };
            return companyNamesMap;
        } catch (error) {
            console.error('Error in fetchCompanyNames:', error);
            return {};
        }
    }

    async fetchBrandNames(brandRecordIds) {
        if (!brandRecordIds || brandRecordIds.length === 0) return [];
        
        const brandNames = [];
        const baseId = PARTNER_DIRECTORY_CONFIG.AIRTABLE_BASE_ID;
        const apiKey = PARTNER_DIRECTORY_CONFIG.AIRTABLE_API_KEY;
        
        // Try common Brand table IDs - you may need to update this with the correct table ID
        const possibleBrandTableIds = [
            'tbl1x6S7I7JwTcRdV', // Brand Setup - Brand Basics (from try-fetch-linked-record.js)
        ];
        
        // Fetch all brand names in parallel instead of sequentially (much faster!)
        for (const tableId of possibleBrandTableIds) {
            try {
                const fetchPromises = brandRecordIds.map(async (recordId) => {
                    try {
                        const url = `https://api.airtable.com/v0/${baseId}/${tableId}/${recordId}`;
                        const response = await fetch(url, {
                            headers: {
                                'Authorization': `Bearer ${apiKey}`,
                                'Content-Type': 'application/json'
                            }
                        });
                        
                        if (response.ok) {
                            const data = await response.json();
                            const brandName = data.fields?.["Brand Name"] || 
                                            data.fields?.name || 
                                            data.fields?.Name ||
                                            '';
                            return brandName || null;
                        }
                        return null;
                    } catch (error) {
                        console.error(`Error fetching brand ${recordId}:`, error);
                        return null;
                    }
                });
                
                // Wait for all requests to complete in parallel
                const results = await Promise.all(fetchPromises);
                brandNames.push(...results.filter(name => name !== null));
                
                if (brandNames.length > 0) break; // Found brands, stop trying other tables
            } catch (error) {
                // Try next table or continue
                continue;
            }
        }
        
        // If we still don't have brand names, log for debugging
        if (brandNames.length === 0 && brandRecordIds.length > 0) {
        }
        
        return brandNames;
    }

    async fetchTeamMembersFromUserManagement(userManagementRecordIds) {
        if (!userManagementRecordIds || userManagementRecordIds.length === 0) return [];
        
        const baseId = PARTNER_DIRECTORY_CONFIG.AIRTABLE_BASE_ID;
        const apiKey = PARTNER_DIRECTORY_CONFIG.AIRTABLE_API_KEY;
        const usersTableId = PARTNER_DIRECTORY_CONFIG.USERS_TABLE_ID;
        
        try {
            // Fetch all user records in parallel instead of sequentially (much faster!)
            const fetchPromises = userManagementRecordIds.map(async (recordId) => {
                try {
                    const url = `https://api.airtable.com/v0/${baseId}/${usersTableId}/${recordId}`;
                    const response = await fetch(url, {
                        headers: {
                            'Authorization': `Bearer ${apiKey}`,
                            'Content-Type': 'application/json'
                        }
                    });
                    
                    if (response.ok) {
                        const data = await response.json();
                        const fields = data.fields || {};
                        
                        const firstName = fields["First Name"] || '';
                        const lastName = fields["Last Name"] || '';
                        const email = fields["Email"] || '';
                        const phoneNumber = fields["Phone Number"] || '';
                        const companyTitle = fields["Company Title"] || '';
                        
                        // Get profile picture/headshot
                        let profilePicture = null;
                        const profileField = fields["Profile"] || fields["Profile Picture"] || fields["Headshot"] || fields["Photo"] || '';
                        if (profileField) {
                            if (Array.isArray(profileField) && profileField.length > 0 && profileField[0] && profileField[0].url) {
                                profilePicture = profileField[0].url;
                            } else if (typeof profileField === 'string' && profileField.startsWith('http')) {
                                profilePicture = profileField;
                            }
                        }
                        
                        if (firstName || lastName) {
                            return {
                                firstName: firstName,
                                lastName: lastName,
                                email: email,
                                phoneNumber: phoneNumber,
                                companyTitle: companyTitle,
                                profilePicture: profilePicture
                            };
                        }
                    }
                    return null;
                } catch (error) {
                    console.error(`Error fetching user ${recordId}:`, error);
                    return null;
                }
            });
            
            // Wait for all requests to complete in parallel
            const results = await Promise.all(fetchPromises);
            return results.filter(member => member !== null);
        } catch (error) {
            console.error('Error fetching team members from User Management:', error);
            return [];
        }
    }

    async fetchTeamMembers(companyId) {
        if (!companyId) {
            // Try to match by company name if no ID
            return [];
        }
        
        try {
            // Find the company to get its name
            const company = this.companies.find(c => c.companyId === companyId || c.id === companyId);
            if (!company) return [];
            
            const companyName = (company.name || '').toLowerCase().trim();
            
            // Filter individuals by company name match
            const filtered = this.individuals.filter(ind => {
                const indCompanyName = (ind.companyName || '').toLowerCase().trim();
                return indCompanyName === companyName;
            });
            
            return filtered.slice(0, 10).map(ind => ({
                firstName: ind.firstName || '',
                lastName: ind.lastName || '',
                email: ind.email || '',
                phoneNumber: ind.phoneNumber || '',
                companyTitle: ind.companyTitle || '',
                profilePicture: null
            }));
        } catch (error) {
            console.error('Error fetching team members:', error);
            return [];
        }
    }

    closeCompanyModal() {
        const modal = document.getElementById('companyModal');
        if (modal) {
            modal.style.display = 'none';
            document.body.style.overflow = '';
        }
    }

    // Note: Add/Edit user modal functions removed - modals not implemented in HTML
    // These functions referenced non-existent DOM elements and would cause errors

    async connectUser(userId) {
        // Placeholder for connect functionality
        alert('Connect functionality coming soon!');
    }

    showSuccess(messageId, text) {
        const messageEl = document.getElementById(messageId);
        messageEl.textContent = text;
        messageEl.className = 'message success active';
    }

    showError(messageId, text) {
        const messageEl = document.getElementById(messageId);
        messageEl.textContent = text;
        messageEl.className = 'message error active';
    }

    hideMessage(messageId) {
        const messageEl = document.getElementById(messageId);
        messageEl.className = 'message';
    }

    showLoadingError(text) {
        const loadingState = document.getElementById('loadingState');
        loadingState.innerHTML = `<p style="color: var(--system--red-400);">${text}</p>`;
    }

    updateInsights() {
        // Get filter values
        const dateRange = document.getElementById('insightsDateRange')?.value || 'all';
        const region = document.getElementById('insightsRegion')?.value || 'all';
        const type = document.getElementById('insightsType')?.value || 'all';
        const partnerType = document.getElementById('insightsPartnerType')?.value || 'all';

        // Filter data based on insights filters
        let filteredCompanies = [...this.companies];
        let filteredIndividuals = [...this.individuals];
        
        // Apply partner type filter (companies vs individuals)
        if (partnerType === 'companies') {
            filteredIndividuals = [];
        } else if (partnerType === 'individuals') {
            filteredCompanies = [];
        }

        // Normalize type function for filtering
        const normalizeType = (type) => {
            if (!type) return 'Unknown';
            const upperType = String(type).trim().toUpperCase();
            
            if (upperType === "HOTEL OWNERS" || upperType === "HOTEL OWNER" || upperType === "OWNER" || upperType === "OWNERS") {
                return "HOTEL OWNERS";
            } else if (upperType === "HOTEL BRANDS (FRANCHISE)" || upperType === "HOTEL BRAND" || upperType === "HOTEL BRANDS" || upperType === "BRAND" || upperType === "BRANDS" || upperType === "FRANCHISE") {
                return "HOTEL BRANDS (FRANCHISE)";
            } else if (upperType === "HOTEL MGMT. COMPANY" || upperType === "HOTEL MGMT COMPANY" || upperType === "HOTEL MANAGEMENT COMPANY" || upperType === "MGMT" || upperType === "MANAGEMENT" || upperType === "OPERATOR") {
                return "HOTEL MGMT. COMPANY";
            } else if (upperType.includes('BRAND') || upperType.includes('FRANCHISE')) {
                return "HOTEL BRANDS (FRANCHISE)";
            } else if (upperType.includes('MGMT') || upperType.includes('MANAGEMENT') || upperType.includes('OPERATOR')) {
                return "HOTEL MGMT. COMPANY";
            } else if (upperType.includes('OWNER')) {
                return "HOTEL OWNERS";
            }
            return upperType;
        };

        // Apply type filter first - normalize both filter value and data values
        if (type !== 'all') {
            const normalizedFilterType = normalizeType(type);
            filteredCompanies = filteredCompanies.filter(company => {
                const normalizedCompanyType = normalizeType(company.userType);
                return normalizedCompanyType === normalizedFilterType;
            });
            filteredIndividuals = filteredIndividuals.filter(individual => {
                const normalizedIndividualType = normalizeType(individual.userType);
                return normalizedIndividualType === normalizedFilterType;
            });
        }

        // Apply date range filter
        if (dateRange !== 'all') {
            const daysAgo = parseInt(dateRange, 10);
            const cutoffDate = new Date();
            cutoffDate.setDate(cutoffDate.getDate() - daysAgo);
            
            filteredCompanies = filteredCompanies.filter(company => {
                let createdDate = null;
                if (company._createdTime) {
                    createdDate = new Date(company._createdTime);
                } else if (company.rawFields && company.rawFields["Created Date"]) {
                    createdDate = new Date(company.rawFields["Created Date"]);
                }
                return createdDate && !isNaN(createdDate.getTime()) && createdDate >= cutoffDate;
            });
            
            filteredIndividuals = filteredIndividuals.filter(individual => {
                let createdDate = null;
                if (individual._createdTime) {
                    createdDate = new Date(individual._createdTime);
                } else if (individual.rawFields && individual.rawFields["Created Date"]) {
                    createdDate = new Date(individual.rawFields["Created Date"]);
                }
                return createdDate && !isNaN(createdDate.getTime()) && createdDate >= cutoffDate;
            });
        }

        // Apply region filter (use hasRegion function which handles GLOBAL correctly)
        if (region !== 'all') {
            filteredCompanies = filteredCompanies.filter(company => {
                return this.hasRegion(company.regions, region);
            });
            filteredIndividuals = filteredIndividuals.filter(individual => {
                return this.hasRegion(individual.regions, region);
            });
        }

        // Calculate statistics
        const totalCompanies = filteredCompanies.length;
        const totalIndividuals = filteredIndividuals.length;
        const totalPartners = totalCompanies + totalIndividuals;
        
        // Calculate average deals
        let totalDeals = 0;
        let partnersWithDeals = 0;
        filteredCompanies.forEach(company => {
            const deals = company.closedDeals || 0;
            if (deals > 0) {
                totalDeals += deals;
                partnersWithDeals++;
            }
        });
        filteredIndividuals.forEach(individual => {
            const deals = individual.closedDeals || 0;
            if (deals > 0) {
                totalDeals += deals;
                partnersWithDeals++;
            }
        });
        const avgDeals = partnersWithDeals > 0 ? Math.round((totalDeals / partnersWithDeals) * 10) / 10 : 0;

        // Update stat displays
        const totalPartnersEl = document.getElementById('totalPartners');
        const totalCompaniesEl = document.getElementById('totalCompanies');
        const totalIndividualsEl = document.getElementById('totalIndividuals');
        const avgDealsEl = document.getElementById('avgDeals');

        if (totalPartnersEl) totalPartnersEl.textContent = totalPartners;
        if (totalCompaniesEl) totalCompaniesEl.textContent = totalCompanies;
        if (totalIndividualsEl) totalIndividualsEl.textContent = totalIndividuals;
        if (avgDealsEl) avgDealsEl.textContent = avgDeals;

        // Update charts if Chart.js is available
        if (typeof Chart !== 'undefined') {
            this.updateInsightsCharts(filteredCompanies, filteredIndividuals);
        }
        
        // Update recent updates section
        this.updateRecentUpdates(filteredCompanies, filteredIndividuals);
    }

    destroyInsightsCharts() {
        if (!this.insightsCharts) return;
        const keys = Object.keys(this.insightsCharts);
        keys.forEach(key => {
            if (this.insightsCharts[key] && typeof this.insightsCharts[key].destroy === 'function') {
                this.insightsCharts[key].destroy();
            }
            this.insightsCharts[key] = null;
        });
    }

    updateInsightsCharts(companies, individuals) {
        // Note: companies and individuals are already filtered by type, date range, and region
        // We just need to display what's in the filtered data
        
        // Normalize type function - matches the one used elsewhere in the code
        const normalizeType = (type) => {
            if (!type) return 'Unknown';
            const upperType = String(type).trim().toUpperCase();
            
            if (upperType === "HOTEL OWNERS" || upperType === "HOTEL OWNER" || upperType === "OWNER" || upperType === "OWNERS") {
                return "HOTEL OWNERS";
            } else if (upperType === "HOTEL BRANDS (FRANCHISE)" || upperType === "HOTEL BRAND" || upperType === "HOTEL BRANDS" || upperType === "BRAND" || upperType === "BRANDS" || upperType === "FRANCHISE") {
                return "HOTEL BRANDS (FRANCHISE)";
            } else if (upperType === "HOTEL MGMT. COMPANY" || upperType === "HOTEL MGMT COMPANY" || upperType === "HOTEL MANAGEMENT COMPANY" || upperType === "MGMT" || upperType === "MANAGEMENT" || upperType === "OPERATOR") {
                return "HOTEL MGMT. COMPANY";
            } else if (upperType.includes('BRAND') || upperType.includes('FRANCHISE')) {
                return "HOTEL BRANDS (FRANCHISE)";
            } else if (upperType.includes('MGMT') || upperType.includes('MANAGEMENT') || upperType.includes('OPERATOR')) {
                return "HOTEL MGMT. COMPANY";
            } else if (upperType.includes('OWNER')) {
                return "HOTEL OWNERS";
            }
            return 'Unknown';
        };
        
        // Partner Distribution by Type - Count both companies and individuals
        // Normalize types first to prevent duplicates
        const typeData = {};
        
        // Count companies by normalized type
        companies.forEach(company => {
            const normalizedType = normalizeType(company.userType);
            typeData[normalizedType] = (typeData[normalizedType] || 0) + 1;
        });
        
        // Count individuals by normalized type
        individuals.forEach(individual => {
            const normalizedType = normalizeType(individual.userType);
            typeData[normalizedType] = (typeData[normalizedType] || 0) + 1;
        });

        // LOI Dashboard color palette
        const typeColorMap = {
            'HOTEL MGMT. COMPANY': '#9a91fb',
            'HOTEL BRANDS (FRANCHISE)': '#6c72ff',
            'HOTEL OWNERS': '#fdb52a',
            'Unknown': '#57c3ff'
        };
        
        const fallbackColors = ['#9a91fb', '#6c72ff', '#57c3ff', '#fdb52a'];
        
        // Define preferred order for display
        const preferredOrder = ['HOTEL MGMT. COMPANY', 'HOTEL BRANDS (FRANCHISE)', 'HOTEL OWNERS', 'Unknown'];
        
        // Sort labels: preferred types first, then others alphabetically
        const labels = Object.keys(typeData).sort((a, b) => {
            const aIndex = preferredOrder.indexOf(a);
            const bIndex = preferredOrder.indexOf(b);
            if (aIndex !== -1 && bIndex !== -1) return aIndex - bIndex;
            if (aIndex !== -1) return -1;
            if (bIndex !== -1) return 1;
            return a.localeCompare(b);
        });
        
        const data = labels.map(label => typeData[label]);
        
        // Assign colors based on normalized type mapping
        const backgroundColor = labels.map(label => {
            return typeColorMap[label] || fallbackColors[labels.indexOf(label) % fallbackColors.length];
        });

        const totalPartnersForType = data.reduce((a, b) => a + b, 0);
        const partnerTypeValueEl = document.getElementById('partnerTypeChartValue');
        if (partnerTypeValueEl) partnerTypeValueEl.textContent = totalPartnersForType;

        const partnerTypeCanvas = document.getElementById('partnerTypeChart');
        if (partnerTypeCanvas) {
            const ctx = partnerTypeCanvas.getContext('2d');
            if (this.insightsCharts.partnerType) {
                this.insightsCharts.partnerType.destroy();
            }
            this.insightsCharts.partnerType = new Chart(ctx, {
                type: 'doughnut',
                data: {
                    labels: labels,
                    datasets: [{
                        data: data,
                        backgroundColor: backgroundColor,
                        borderWidth: 0,
                        hoverOffset: 10
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            display: true,
                            position: 'bottom',
                            labels: {
                                color: '#aeb9e1',
                                padding: 8,
                                font: { size: 11 },
                                usePointStyle: true,
                                pointStyle: 'circle',
                                boxWidth: 6,
                                boxHeight: 6
                            },
                            maxWidth: 400
                        },
                        tooltip: {
                            backgroundColor: 'rgba(255, 255, 255, 0.95)',
                            titleColor: '#080f25',
                            bodyColor: '#37446b',
                            borderColor: '#d9e1fa',
                            borderWidth: 1,
                            padding: 12,
                            callbacks: {
                                label: function(context) {
                                    const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                    const pct = total > 0 ? Math.round((context.parsed / total) * 100) : 0;
                                    return context.label + ': ' + pct + '%';
                                }
                            }
                        }
                    }
                }
            });
        }

        // Regional Distribution
        // Count partners by region from the already-filtered data
        // Note: Partners can operate in multiple regions, so they may be counted in multiple bars
        const regionData = {};
        const allRegions = ['AMERICAS', 'CALA', 'EUROPE', 'MEA', 'AP'];
        const totalPartners = companies.length + individuals.length;
        
        // Initialize all regions to 0
        allRegions.forEach(reg => {
            regionData[reg] = 0;
        });
        
        // Count partners in each region from the filtered data
        // Each partner is counted in every region they operate in
        [...companies, ...individuals].forEach(item => {
            (item.regions || []).forEach(regionItem => {
                const regionName = String(regionItem).toUpperCase().trim();
                if (allRegions.includes(regionName)) {
                    regionData[regionName] = (regionData[regionName] || 0) + 1;
                }
            });
        });

        const regionalTotal = Object.values(regionData).reduce((a, b) => a + b, 0);
        const regionalValueEl = document.getElementById('regionalChartValue');
        if (regionalValueEl) regionalValueEl.textContent = regionalTotal > 0 ? regionalTotal + ' total' : '0';

        const regionalCanvas = document.getElementById('regionalDistributionChart');
        if (regionalCanvas) {
            const ctx = regionalCanvas.getContext('2d');
            if (this.insightsCharts.regional) {
                this.insightsCharts.regional.destroy();
            }
            this.insightsCharts.regional = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: Object.keys(regionData),
                    datasets: [{
                        data: Object.values(regionData),
                        backgroundColor: '#6c72ff',
                        borderRadius: 4,
                        barThickness: 24
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            backgroundColor: 'rgba(255, 255, 255, 0.95)',
                            titleColor: '#080f25',
                            bodyColor: '#37446b',
                            borderColor: '#d9e1fa',
                            borderWidth: 1,
                            padding: 12,
                            displayColors: false,
                            callbacks: {
                                label: function(context) {
                                    return context.parsed.y + ' partners';
                                },
                                afterLabel: () => {
                                    return 'Multi-region partners may be counted multiple times';
                                }
                            }
                        }
                    },
                    scales: {
                        x: {
                            grid: { display: false, drawBorder: false },
                            ticks: {
                                color: '#7e89ac',
                                font: { size: 10 },
                                maxRotation: 45,
                                minRotation: 45
                            }
                        },
                        y: {
                            beginAtZero: true,
                            grid: {
                                color: 'rgba(126, 137, 172, 0.1)',
                                drawBorder: false
                            },
                            ticks: {
                                color: '#7e89ac',
                                font: { size: 11 },
                                callback: function(value) {
                                    return value + ' partners';
                                },
                                stepSize: 10
                            }
                        }
                    }
                }
            });
        }

        // Growth Trends - calculate based on Created Date
        const growthCanvas = document.getElementById('growthTrendsChart');
        if (growthCanvas) {
            const ctx = growthCanvas.getContext('2d');
            if (this.insightsCharts.growth) {
                this.insightsCharts.growth.destroy();
            }

            // Get current date and calculate quarters
            const now = new Date();
            const currentYear = now.getFullYear();
            const quarters = [
                { label: 'Q1', start: new Date(currentYear, 0, 1), end: new Date(currentYear, 2, 31) },
                { label: 'Q2', start: new Date(currentYear, 3, 1), end: new Date(currentYear, 5, 30) },
                { label: 'Q3', start: new Date(currentYear, 6, 1), end: new Date(currentYear, 8, 30) },
                { label: 'Q4', start: new Date(currentYear, 9, 1), end: new Date(currentYear, 11, 31) }
            ];

            // Count new partners per quarter
            const quarterCounts = [0, 0, 0, 0];
            const allPartners = [...companies, ...individuals];
            
            allPartners.forEach(partner => {
                let createdDate = null;
                
                // Try multiple sources for created date (check most reliable first)
                // 1. Use Airtable's built-in createdTime (most reliable - always available)
                if (partner._createdTime) {
                    createdDate = new Date(partner._createdTime);
                }
                // 2. Check rawFields (if Created Date field exists and wasn't excluded)
                else if (partner.rawFields && partner.rawFields["Created Date"]) {
                    createdDate = new Date(partner.rawFields["Created Date"]);
                }
                // 3. Check if we stored it separately
                else if (partner.createdDate) {
                    createdDate = new Date(partner.createdDate);
                }
                
                if (createdDate && !isNaN(createdDate.getTime())) {
                    // Only count if the date is within the current year
                    if (createdDate.getFullYear() === currentYear) {
                        // Check which quarter this date falls into
                        quarters.forEach((quarter, index) => {
                            if (createdDate >= quarter.start && createdDate <= quarter.end) {
                                quarterCounts[index]++;
                            }
                        });
                    }
                }
            });

            const totalNew = quarterCounts.reduce((a, b) => a + b, 0);
            const growthValueEl = document.getElementById('growthChartValue');
            if (growthValueEl) growthValueEl.textContent = totalNew + ' new';

            this.insightsCharts.growth = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: quarters.map(q => q.label),
                    datasets: [{
                        label: 'New Partners',
                        data: quarterCounts,
                        borderColor: '#9a91fb',
                        backgroundColor: 'rgba(154, 145, 251, 0.2)',
                        tension: 0.4,
                        fill: true,
                        borderWidth: 2,
                        pointRadius: 4,
                        pointHoverRadius: 6,
                        pointHoverBackgroundColor: '#9a91fb',
                        pointHoverBorderColor: '#fff',
                        pointHoverBorderWidth: 2
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { mode: 'index', intersect: false },
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            backgroundColor: 'rgba(255, 255, 255, 0.95)',
                            titleColor: '#080f25',
                            bodyColor: '#37446b',
                            borderColor: '#d9e1fa',
                            borderWidth: 1,
                            padding: 12,
                            displayColors: true,
                            callbacks: {
                                label: function(context) {
                                    return context.dataset.label + ': ' + context.parsed.y + ' partners';
                                }
                            }
                        }
                    },
                    scales: {
                        x: {
                            grid: { display: false, drawBorder: false },
                            ticks: {
                                color: '#7e89ac',
                                font: { size: 11 }
                            }
                        },
                        y: {
                            beginAtZero: true,
                            grid: {
                                color: 'rgba(126, 137, 172, 0.1)',
                                drawBorder: false
                            },
                            ticks: {
                                color: '#7e89ac',
                                font: { size: 11 },
                                callback: function(value) {
                                    return value + ' partners';
                                },
                                stepSize: 1
                            }
                        }
                    }
                }
            });
        }

        // Performance Metrics chart removed
    }

    updateRecentUpdates(companies, individuals) {
        const container = document.getElementById('recentUpdatesContainer');
        if (!container) return;

        // Combine companies and individuals with their type
        const allPartners = [
            ...companies.map(c => ({ ...c, _partnerType: 'company' })),
            ...individuals.map(i => ({ ...i, _partnerType: 'individual' }))
        ];

        // Sort by creation date (most recent first)
        allPartners.sort((a, b) => {
            const dateA = a._createdTime ? new Date(a._createdTime) : new Date(0);
            const dateB = b._createdTime ? new Date(b._createdTime) : new Date(0);
            return dateB - dateA;
        });

        // Show only the 20 most recent
        const recentPartners = allPartners.slice(0, 20);

        if (recentPartners.length === 0) {
            container.innerHTML = '<div class="insights-updates-empty">No recent updates found</div>';
            return;
        }

        // Format date helper
        const formatDate = (dateString) => {
            if (!dateString) return 'Date unknown';
            const date = new Date(dateString);
            if (isNaN(date.getTime())) return 'Date unknown';
            
            const now = new Date();
            const diffTime = Math.abs(now - date);
            const diffDays = Math.floor(diffTime / (1000 * 60 * 60 * 24));
            
            if (diffDays === 0) return 'Today';
            if (diffDays === 1) return 'Yesterday';
            if (diffDays < 7) return `${diffDays} days ago`;
            if (diffDays < 30) {
                const weeks = Math.floor(diffDays / 7);
                return `${weeks} ${weeks === 1 ? 'week' : 'weeks'} ago`;
            }
            if (diffDays < 365) {
                const months = Math.floor(diffDays / 30);
                return `${months} ${months === 1 ? 'month' : 'months'} ago`;
            }
            return date.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
        };

        // Generate HTML
        container.innerHTML = recentPartners.map(partner => {
            const name = partner._partnerType === 'company' 
                ? (partner.name || 'Unknown Company')
                : `${partner.firstName || ''} ${partner.lastName || ''}`.trim() || 'Unknown Individual';
            const type = partner.userType || 'Unknown Type';
            const badgeClass = partner._partnerType === 'company' ? 'company' : 'individual';
            const badgeText = partner._partnerType === 'company' ? 'Company' : 'Individual';
            const dateText = formatDate(partner._createdTime);

            return `
                <div class="insights-update-item">
                    <div class="insights-update-item-left">
                        <span class="insights-update-badge ${badgeClass}">${this.escapeHtml(badgeText)}</span>
                        <div class="insights-update-info">
                            <div class="insights-update-name">${this.escapeHtml(name)}</div>
                            <div class="insights-update-type">${this.escapeHtml(type)}</div>
                        </div>
                    </div>
                    <div class="insights-update-date">${dateText}</div>
                </div>
            `;
        }).join('');
    }
}

// Initialize on page load
let partnerDirectory;

// Initialize after DOM is ready and config is loaded
// Note: If you see "A listener indicated an asynchronous response by returning true, but the message channel closed"
//       error in console, this is typically from a browser extension (not from this code).
//       The IIFE pattern below helps avoid async event listener issues.
(function initializePartnerDirectory() {
    // Use IIFE to avoid async event listener issues and improve error handling
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initialize);
    } else {
        // DOM already loaded, initialize immediately
        initialize();
    }
    
    async function initialize() {
        try {
            // Load config from server (for local development) or use hardcoded config (for production)
            const configLoaded = await loadConfig();
            
            if (!configLoaded) {
                // Show error message if config is not available
                const resultsContainer = document.getElementById('resultsContainer');
                if (resultsContainer) {
                    resultsContainer.innerHTML = `
                        <div style="padding: 40px; text-align: center; color: var(--neutral--400);">
                            <h2 style="margin-bottom: 16px;">⚠️ Configuration Required</h2>
                            <p style="margin-bottom: 8px;">Please update PARTNER_DIRECTORY_CONFIG in partner-directory.js</p>
                            <p style="font-size: 14px; color: var(--neutral--500);">
                                For local development, ensure your server has AIRTABLE_API_KEY and AIRTABLE_BASE_ID environment variables set.<br>
                                For production/Webflow, update the hardcoded values in the config object.
                            </p>
                        </div>
                    `;
                }
                return;
            }
            
            // Initialize PartnerDirectory with loaded config
            partnerDirectory = new PartnerDirectory();
        } catch (error) {
            console.error('Error initializing Partner Directory:', error);
            // Show user-friendly error message
            const resultsContainer = document.getElementById('resultsContainer');
            if (resultsContainer) {
                resultsContainer.innerHTML = `
                    <div style="padding: 40px; text-align: center; color: var(--system--red-400);">
                        <h2 style="margin-bottom: 16px;">⚠️ Initialization Error</h2>
                        <p style="margin-bottom: 8px;">Failed to initialize Partner Directory.</p>
                        <p style="font-size: 14px; color: var(--neutral--500);">
                            Please refresh the page or contact support if the problem persists.
                        </p>
                    </div>
                `;
            }
        }
    }
})();
