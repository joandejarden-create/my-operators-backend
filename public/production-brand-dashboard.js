// Production Brand Review Dashboard - Real Airtable Integration
class ProductionBrandDashboard {
    constructor() {
        this.deals = [];
        this.filteredDeals = [];
        this.currentTab = 'new';
        this.currentPage = 1;
        this.itemsPerPage = 10;
        this.selectedDeals = new Set();
        this.brandId = null;
        this.userId = null;
        
        // Performance optimization: Enhanced caching system
        this.cache = {
            locationData: new Map(),
            userData: new Map(),
            contactData: new Map(),
            matchScores: new Map(),
            brandData: new Map(),
            marketPerformanceData: new Map(),
            strategicIntentData: new Map(),
            hotelOwnershipData: new Map(),
            companyProfileData: new Map(),
            lastCacheTime: null,
            cacheExpiry: 20 * 60 * 1000, // 20 minutes (increased for better cache hit rate)
            maxCacheSize: 1000, // Maximum items per cache
            hitCount: 0,
            missCount: 0
        };
        
        // Performance monitoring
        this.performance = {
            apiCalls: 0,
            apiCallTimes: [],
            renderTimes: [],
            sortTimes: [],
            filterTimes: [],
            debugMode: false,
            startTime: Date.now(),
            // Enhanced API tracking
            apiCallMetrics: {
                total: 0,
                successful: 0,
                failed: 0,
                current: 0,
                totalDuration: 0,
                averageDuration: 0,
                slowestCall: { url: '', duration: 0, timestamp: '' }
            },
            // Deal processing metrics
            dealProcessing: {
                totalDeals: 0,
                processedDeals: 0,
                averageProcessingTime: 0,
                totalProcessingTime: 0
            },
            // Cache performance
            cache: {
                hits: 0,
                misses: 0,
                hitRate: 0
            }
        };
        
        // Complete Airtable configuration with all your tables
        this.airtableConfig = {
            baseId: 'appvtnDurnMSjINP6',
            // Main tables
            dealsTableId: 'tblbvSxjiIhXzW6XW',           // Deals table
            usersTableId: 'tbl6shiyz2wdUqE5F',           // Users table
            locationTableId: 'tblLw3HRrlYldDPcr',        // Location & Property table
            brandsTableId: 'tbl1x6S7I7JwTcRdV',          // Brand Setup - Brand Basics table
            contactUploadsTableId: 'tbljY3jQyeSEv2UqE',  // Contact & Uploads table
            
            // Additional tables from your structure
            marketPerformanceTableId: 'tblUoZai381vbop0L',    // Market - Performance - Deal & Capital Structure table
            strategicIntentTableId: 'tblrHt9m8lfKrWbIC',      // Strategic Intent - Operational - Key Challenges table
            hotelOwnershipTableId: 'tbl7KyDvKlwfDumE6',       // Hotel Ownership table
            companyProfileTableId: 'tblItyfH6MlOnMKZ9',       // Company Profile table
            projectFitTableId: 'tblmevxQmLlml7QIp',          // Brand Setup - Project Fit table
            dealTermsTableId: 'tblxJ7lFfc6NyyMQd',           // Brand Setup - Deal Terms table
            
            // New accessible tables
            brandFootprintTableId: 'tbl108u1oTAwC5XTT',      // Brand Setup - Brand Footprint table
            userManagementTableId: 'tblQEpYKf2aYNKKjw',      // User Management table
            feeStructureTableId: 'tblIzHQUgqrziKg10',        // Brand Setup - Fee Structure table
            brandStandardsTableId: 'tbl4MdXjldw56Kkrw',      // Brand Setup - Brand Standards table
            
            // Note: These 2 tables are not accessible yet (403 Forbidden)
            // legalTermsTableId: 'tbl16FDlvS21JL6b4',          // Brand Setup - Legal Terms table
            // operationalSupportTableId: 'tblkZyvi6ELUSKWWP', // Brand Setup - Operational Support table (403 Forbidden)
            
        // New interaction tracking tables (real table IDs from Airtable)
        interactionsTableId: 'tblf0iLridTeS5xEA',   // Deal Interactions table
        statusHistoryTableId: 'tblxZR25GE7exKQ93',  // Deal Status History table
        actionsTableId: 'tblAc2w3jtzFvasAG',        // Deal Actions table
        preferencesTableId: 'tblvQakNRBoW33Wb4',    // Brand Deal Preferences table
            
            apiKey: ''
        };
        
        this.init();
        
        // Initialize sorting state
        this.currentSort = { field: null, direction: null };
    }

    getApiBaseUrl() {
        return (window.DEALALITY_API_BASE || '').replace(/\/$/, '');
    }

    // Optimized sort deals with performance tracking
    async sortDeals(field, direction) {
        return await this.timeOperation('Sort Deals', async () => {
            this.debugLog(`Sorting deals by ${field} in ${direction} order`);
            
            // Update current sort state
            this.currentSort = { field, direction };
            
            // Update active sort arrow visual state
            this.updateSortArrows(field, direction);
            
            // Sort the filtered deals
            this.filteredDeals.sort((a, b) => {
                let aValue, bValue;
                
                switch (field) {
                    case 'status':
                        aValue = a.status || '';
                        bValue = b.status || '';
                        break;
                    case 'brandMatch':
                        aValue = a.brandMatch || '';
                        bValue = b.brandMatch || '';
                        break;
                    case 'matchScore':
                        aValue = parseInt(a.matchScore) || 0;
                        bValue = parseInt(b.matchScore) || 0;
                        break;
                    case 'dealHeadline':
                        aValue = a.headline || '';
                        bValue = b.headline || '';
                        break;
                    case 'dealContact':
                        // Sort by contact name (owner name)
                        aValue = a.ownerName || '';
                        bValue = b.ownerName || '';
                        break;
                    default:
                        return 0;
                }
                
                // Handle string comparison
                if (typeof aValue === 'string' && typeof bValue === 'string') {
                    aValue = aValue.toLowerCase();
                    bValue = bValue.toLowerCase();
                }
                
                if (direction === 'asc') {
                    return aValue > bValue ? 1 : aValue < bValue ? -1 : 0;
                } else {
                    return aValue < bValue ? 1 : aValue > bValue ? -1 : 0;
                }
            });
            
            // Re-render the deals with new sort order
            await this.renderDeals();
            
            this.debugLog(`Deals sorted by ${field} in ${direction} order`);
        });
    }
    
    // Update sort arrow visual state
    updateSortArrows(field, direction) {
        // Remove active class from all arrows
        document.querySelectorAll('.sort-arrow').forEach(arrow => {
            arrow.classList.remove('active');
        });
        
        // Add active class to the clicked arrow
        const activeArrow = document.querySelector(`[data-sort="${field}"] .sort-arrow.${direction === 'asc' ? 'up' : 'down'}`);
        if (activeArrow) {
            activeArrow.classList.add('active');
        }
    }

    // Helper function to check if ownership data has meaningful content
    hasOwnershipData(ownershipData) {
        if (!ownershipData || Object.keys(ownershipData).length === 0) {
            return false;
        }
        
        // Check if any field has meaningful data (not just 'N/A' or empty)
        const meaningfulFields = [
            'Owner/Operator Name',
            'Brand', 
            'Property Name',
            'City/State',
            'Country',
            'Description of Interest'
        ];
        
        return meaningfulFields.some(field => {
            const value = ownershipData[field];
            return value && value !== 'N/A' && value.toString().trim() !== '';
        });
    }

    // Country to Region mapping based on Ref_CountryRegion table
    getCountryRegionMapping() {
        return {
            'United States': { region1: 'United States', region2: 'North America', region3: '', region: 'Global' },
            'Canada': { region1: 'Canada', region2: 'North America', region3: '', region: 'Global' },
            'Belize': { region1: 'Central America', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Costa Rica': { region1: 'Central America', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'El Salvador': { region1: 'Central America', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Guatemala': { region1: 'Central America', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Honduras': { region1: 'Central America', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Nicaragua': { region1: 'Central America', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Panama': { region1: 'Central America', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Mexico': { region1: 'Mexico', region2: 'Central America', region3: 'Latin America (Broad)', region: 'Global' },
            'Antigua & Barbuda': { region1: 'Caribbean', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Bahamas': { region1: 'Caribbean', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Barbados': { region1: 'Caribbean', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Cuba': { region1: 'Caribbean', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Dominica': { region1: 'Caribbean', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Dominican Republic': { region1: 'Caribbean', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Grenada': { region1: 'Caribbean', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Haiti': { region1: 'Caribbean', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Jamaica': { region1: 'Caribbean', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Saint Kitts & Nevis': { region1: 'Caribbean', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Saint Lucia': { region1: 'Caribbean', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'St. Vincent & Grenadines': { region1: 'Caribbean', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Trinidad & Tobago': { region1: 'Caribbean', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Argentina': { region1: 'South America', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Bolivia': { region1: 'South America', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Brazil': { region1: 'South America', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Chile': { region1: 'South America', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Colombia': { region1: 'South America', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Ecuador': { region1: 'South America', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Guyana': { region1: 'South America', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Paraguay': { region1: 'South America', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Peru': { region1: 'South America', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Suriname': { region1: 'South America', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Uruguay': { region1: 'South America', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'Venezuela': { region1: 'South America', region2: 'Latin America (Broad)', region3: '', region: 'Global' },
            'France': { region1: 'Western Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Germany': { region1: 'Western Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Ireland': { region1: 'Western Europe', region2: 'Europe', region3: '', region: 'Global' },
            'United Kingdom': { region1: 'Western Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Greece': { region1: 'Southern Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Italy': { region1: 'Southern Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Portugal': { region1: 'Southern Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Spain': { region1: 'Southern Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Holy See': { region1: 'Southern Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Croatia': { region1: 'Southern Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Cyprus': { region1: 'Southern Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Denmark': { region1: 'Northern Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Finland': { region1: 'Northern Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Norway': { region1: 'Northern Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Sweden': { region1: 'Northern Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Iceland': { region1: 'Northern Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Czech Republic': { region1: 'Eastern Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Hungary': { region1: 'Eastern Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Poland': { region1: 'Eastern Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Romania': { region1: 'Eastern Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Russia': { region1: 'Eastern Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Slovakia': { region1: 'Eastern Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Slovenia': { region1: 'Eastern Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Turkey': { region1: 'Eastern Europe', region2: 'Europe', region3: '', region: 'Global' },
            'Bahrain': { region1: 'Middle East', region2: 'Asia', region3: '', region: 'Global' },
            'Iran': { region1: 'Middle East', region2: 'Asia', region3: '', region: 'Global' },
            'Iraq': { region1: 'Middle East', region2: 'Asia', region3: '', region: 'Global' },
            'Israel': { region1: 'Middle East', region2: 'Asia', region3: '', region: 'Global' },
            'Jordan': { region1: 'Middle East', region2: 'Asia', region3: '', region: 'Global' },
            'Kuwait': { region1: 'Middle East', region2: 'Asia', region3: '', region: 'Global' },
            'Lebanon': { region1: 'Middle East', region2: 'Asia', region3: '', region: 'Global' },
            'Oman': { region1: 'Middle East', region2: 'Asia', region3: '', region: 'Global' },
            'Qatar': { region1: 'Middle East', region2: 'Asia', region3: '', region: 'Global' },
            'Saudi Arabia': { region1: 'Middle East', region2: 'Asia', region3: '', region: 'Global' },
            'Syria': { region1: 'Middle East', region2: 'Asia', region3: '', region: 'Global' },
            'State of Palestine': { region1: 'Middle East', region2: 'Asia', region3: '', region: 'Global' },
            'Egypt': { region1: 'Middle East', region2: 'Asia', region3: '', region: 'Global' },
            'United Arab Emirates': { region1: 'Middle East', region2: 'Asia', region3: '', region: 'Global' },
            // Countries in "Other (specify)" category
            'Afghanistan': { region1: 'Other (specify)', region2: '', region3: '', region: 'Global' },
            'Albania': { region1: 'Other (specify)', region2: '', region3: '', region: 'Global' },
            'Algeria': { region1: 'Other (specify)', region2: '', region3: '', region: 'Global' },
            'Australia': { region1: 'Other (specify)', region2: '', region3: '', region: 'Global' },
            'China': { region1: 'Other (specify)', region2: '', region3: '', region: 'Global' },
            'India': { region1: 'Other (specify)', region2: '', region3: '', region: 'Global' },
            'Japan': { region1: 'Other (specify)', region2: '', region3: '', region: 'Global' },
            'New Zealand': { region1: 'Other (specify)', region2: '', region3: '', region: 'Global' },
            'Nigeria': { region1: 'Other (specify)', region2: '', region3: '', region: 'Global' },
            'South Africa': { region1: 'Other (specify)', region2: '', region3: '', region: 'Global' },
            'Ukraine': { region1: 'Other (specify)', region2: '', region3: '', region: 'Global' },
            'Zimbabwe': { region1: 'Other (specify)', region2: '', region3: '', region: 'Global' },
            'Global': { region1: 'Global', region2: '', region3: '', region: 'Global' }
        };
    }

    // Utility function to create delay
    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    // Enhanced API call with performance tracking and rate limiting
    async fetchWithRateLimit(url, options = {}, delayMs = 400, maxRetries = 8) {
        const startTime = performance.now();
        const operationId = `api_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        
        return await this.timeOperation(`API Call [${operationId}]`, async () => {
            let lastError;
            
            for (let attempt = 0; attempt <= maxRetries; attempt++) {
                try {
                    // More aggressive rate limiting - wait between requests
                    await this.delay(delayMs + (attempt * 150)); // Increase delay with each attempt
                    
                    // Track API call metrics
                    this.performance.apiCallMetrics.total++;
                    this.performance.apiCallMetrics.current++;
                    
                    const response = await fetch(url, {
                        ...options,
                        headers: {
                            'Accept': 'application/json',
                            'X-Frontend-Auth-Disabled': 'true',
                            ...options.headers
                        }
                    });
                    
                    const endTime = performance.now();
                    const duration = endTime - startTime;
                    
                    // Handle 429 (Too Many Requests) with exponential backoff
                    if (response.status === 429) {
                        const retryAfter = response.headers.get('Retry-After');
                        const baseDelay = retryAfter ? parseInt(retryAfter) * 1000 : Math.pow(2, attempt) * 4000;
                        const backoffDelay = baseDelay + (Math.random() * 2000); // Add jitter
                        
                        this.debugLog(`Rate limited (429), retrying in ${Math.round(backoffDelay)}ms (attempt ${attempt + 1}/${maxRetries + 1})`, { 
                            url: url.split('?')[0],
                            attempt: attempt + 1,
                            maxRetries: maxRetries + 1,
                            backoffDelay: `${Math.round(backoffDelay)}ms`,
                            operationId 
                        }, 'warn');
                        
                        if (attempt < maxRetries) {
                            await this.delay(backoffDelay);
                            continue;
                        } else {
                            throw new Error(`Rate limited after ${maxRetries + 1} attempts`);
                        }
                    }
                    
                    // Update performance metrics
                    this.performance.apiCallMetrics.totalDuration += duration;
                    this.performance.apiCallMetrics.averageDuration = this.performance.apiCallMetrics.totalDuration / this.performance.apiCallMetrics.total;
                    
                    if (duration > this.performance.apiCallMetrics.slowestCall.duration) {
                        this.performance.apiCallMetrics.slowestCall = { url, duration, timestamp: new Date().toISOString() };
                    }
                    
                    if (!response.ok) {
                        this.performance.apiCallMetrics.failed++;
                        lastError = new Error(`API call failed: ${response.status} ${response.statusText}`);
                        this.debugLog(`API call failed: ${response.status} ${response.statusText}`, { 
                            url: url.split('?')[0], 
                            options, 
                            duration: `${duration.toFixed(2)}ms`,
                            attempt: attempt + 1,
                            operationId 
                        }, 'error');
                        
                        // For non-429 errors, don't retry
                        throw lastError;
                    }
                    
                    this.performance.apiCallMetrics.successful++;
                    this.debugLog(`API call successful: ${duration.toFixed(2)}ms`, { 
                        url: url.split('?')[0], // Hide query params for cleaner logs
                        duration: `${duration.toFixed(2)}ms`,
                        attempt: attempt + 1,
                        operationId 
                    }, 'info');
                    
                    return response;
                    
                } catch (error) {
                    lastError = error;
                    
                    // If it's not a 429 error, don't retry
                    if (!error.message.includes('429') && !error.message.includes('Rate limited')) {
                        break;
                    }
                    
                    // If we've exhausted retries for 429 errors
                    if (attempt >= maxRetries) {
                        this.performance.apiCallMetrics.failed++;
                        this.debugLog(`Rate limiting retries exhausted after ${maxRetries + 1} attempts`, { 
                            url: url.split('?')[0],
                            operationId 
                        }, 'error');
                        throw error;
                    }
                }
            }
            
            // If we get here, all retries failed
            this.performance.apiCallMetrics.failed++;
            throw lastError;
        });
    }
    
    // Batch API calls for better performance
    async batchApiCalls(requests) {
        return await this.timeOperation('Batch API Calls', async () => {
            const results = await Promise.allSettled(requests);
            const successful = results.filter(r => r.status === 'fulfilled').map(r => r.value);
            const failed = results.filter(r => r.status === 'rejected').map(r => r.reason);
            
            if (failed.length > 0) {
                this.debugLog(`Batch API calls: ${failed.length} failed`, failed, 'warn');
            }
            
            this.debugLog(`Batch API calls: ${successful.length} successful, ${failed.length} failed`);
            return { successful, failed };
        });
    }

    // Cache validation and management
    isCacheValid() {
        if (!this.cache.lastCacheTime) return false;
        return (Date.now() - this.cache.lastCacheTime) < this.cache.cacheExpiry;
    }

    clearCache() {
        this.cache.locationData.clear();
        this.cache.userData.clear();
        this.cache.contactData.clear();
        this.cache.matchScores.clear();
        this.cache.brandData.clear();
        this.cache.marketPerformanceData.clear();
        this.cache.strategicIntentData.clear();
        this.cache.hotelOwnershipData.clear();
        this.cache.companyProfileData.clear();
        this.cache.lastCacheTime = null;
        this.cache.hitCount = 0;
        this.cache.missCount = 0;
        this.debugLog('Cache cleared completely');
    }
    
    // Enhanced cache management with size limits
    manageCacheSize(cacheMap, maxSize = this.cache.maxCacheSize) {
        if (cacheMap.size > maxSize) {
            const entries = Array.from(cacheMap.entries());
            const toDelete = entries.slice(0, cacheMap.size - maxSize);
            toDelete.forEach(([key]) => cacheMap.delete(key));
            this.debugLog(`Cache trimmed: removed ${toDelete.length} entries`);
        }
    }
    
    // Smart cache get with hit/miss tracking
    getFromCache(cacheMap, key) {
        if (cacheMap.has(key)) {
            this.cache.hitCount++;
            this.performance.cache.hits++;
            this.debugLog(`Cache HIT for key: ${key}`);
            return cacheMap.get(key);
        } else {
            this.cache.missCount++;
            this.performance.cache.misses++;
            this.debugLog(`Cache MISS for key: ${key}`);
            return null;
        }
    }
    
    // Smart cache set with size management
    setToCache(cacheMap, key, value) {
        cacheMap.set(key, value);
        this.manageCacheSize(cacheMap);
        this.debugLog(`Cached value for key: ${key}`);
    }

    // Enhanced performance monitoring
    logPerformanceMetrics() {
        const uptime = Date.now() - this.performance.startTime;
        const apiMetrics = this.performance.apiCallMetrics;
        
        // Calculate cache hit rate
        const totalCacheRequests = this.performance.cache.hits + this.performance.cache.misses;
        this.performance.cache.hitRate = totalCacheRequests > 0 ? 
            (this.performance.cache.hits / totalCacheRequests * 100).toFixed(2) : 0;
        
        const cacheStats = {
            locationCacheSize: this.cache.locationData.size,
            userCacheSize: this.cache.userData.size,
            matchScoreCacheSize: this.cache.matchScores.size,
            brandDataCacheSize: this.cache.brandData.size,
            marketPerformanceCacheSize: this.cache.marketPerformanceData.size,
            strategicIntentCacheSize: this.cache.strategicIntentData.size,
            hotelOwnershipCacheSize: this.cache.hotelOwnershipData.size,
            companyProfileCacheSize: this.cache.companyProfileData.size,
            cacheAge: this.cache.lastCacheTime ? Date.now() - this.cache.lastCacheTime : 0,
            hitRate: this.cache.hitCount / (this.cache.hitCount + this.cache.missCount) * 100 || 0,
            // Enhanced cache metrics
            hits: this.performance.cache.hits,
            misses: this.performance.cache.misses,
            hitRatePercent: this.performance.cache.hitRate + '%'
        };
        
        const performanceStats = {
            // Legacy metrics
            totalApiCalls: this.performance.apiCalls,
            avgApiCallTime: this.performance.apiCallTimes.length > 0 ? 
                this.performance.apiCallTimes.reduce((a, b) => a + b, 0) / this.performance.apiCallTimes.length : 0,
            avgRenderTime: this.performance.renderTimes.length > 0 ? 
                this.performance.renderTimes.reduce((a, b) => a + b, 0) / this.performance.renderTimes.length : 0,
            avgSortTime: this.performance.sortTimes.length > 0 ? 
                this.performance.sortTimes.reduce((a, b) => a + b, 0) / this.performance.sortTimes.length : 0,
            uptime: uptime,
            
            // Enhanced API metrics
            apiMetrics: {
                total: apiMetrics.total,
                successful: apiMetrics.successful,
                failed: apiMetrics.failed,
                successRate: apiMetrics.total > 0 ? ((apiMetrics.successful / apiMetrics.total) * 100).toFixed(2) + '%' : '0%',
                averageDuration: apiMetrics.averageDuration.toFixed(2) + 'ms',
                totalDuration: (apiMetrics.totalDuration / 1000).toFixed(2) + 's',
                slowestCall: apiMetrics.slowestCall,
                currentActive: apiMetrics.current
            },
            
            // Deal processing metrics
            dealProcessing: {
                totalDeals: this.performance.dealProcessing.totalDeals,
                processedDeals: this.performance.dealProcessing.processedDeals,
                averageProcessingTime: this.performance.dealProcessing.averageProcessingTime.toFixed(2) + 'ms',
                totalProcessingTime: (this.performance.dealProcessing.totalProcessingTime / 1000).toFixed(2) + 's'
            }
        };
        
        console.log('📊 ===== COMPREHENSIVE PERFORMANCE REPORT =====');
        console.log('📊 Total Deals:', this.deals.length);
        console.log('📊 Filtered Deals:', this.filteredDeals?.length || 0);
        console.log('📊 API Performance:', performanceStats.apiMetrics);
        console.log('📊 Deal Processing:', performanceStats.dealProcessing);
        console.log('📊 Cache Performance:', cacheStats);
        console.log('📊 System Health:', {
            uptime: (uptime / 1000 / 60).toFixed(2) + ' minutes',
            memoryUsage: performance.memory ? {
                used: Math.round(performance.memory.usedJSHeapSize / 1024 / 1024) + 'MB',
                total: Math.round(performance.memory.totalJSHeapSize / 1024 / 1024) + 'MB',
                limit: Math.round(performance.memory.jsHeapSizeLimit / 1024 / 1024) + 'MB'
            } : 'Not available'
        });
        console.log('📊 ===== END PERFORMANCE REPORT =====');
        
        return { cacheStats, performanceStats };
    }
    
    // Debug mode toggle
    toggleDebugMode() {
        this.performance.debugMode = !this.performance.debugMode;
        console.log(`🐛 Debug mode ${this.performance.debugMode ? 'enabled' : 'disabled'}`);
        return this.performance.debugMode;
    }
    
    // Performance monitoring dashboard
    showPerformanceDashboard() {
        const metrics = this.logPerformanceMetrics();
        
        // Create performance dashboard modal
        const modalHtml = `
            <div id="performanceModal" class="modal" style="display: block;">
                <div class="modal-content" style="max-width: 90%; max-height: 90%; overflow-y: auto;">
                    <div class="modal-header">
                        <h2>📊 Performance Dashboard</h2>
                        <span class="close" onclick="this.closest('.modal').remove()">&times;</span>
                    </div>
                    <div class="modal-body">
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px;">
                            <div class="performance-card">
                                <h3>🚀 API Performance</h3>
                                <div class="metric">
                                    <strong>Total Calls:</strong> ${metrics.performanceStats.apiMetrics.total}
                                </div>
                                <div class="metric">
                                    <strong>Success Rate:</strong> ${metrics.performanceStats.apiMetrics.successRate}
                                </div>
                                <div class="metric">
                                    <strong>Average Duration:</strong> ${metrics.performanceStats.apiMetrics.averageDuration}
                                </div>
                                <div class="metric">
                                    <strong>Total Duration:</strong> ${metrics.performanceStats.apiMetrics.totalDuration}
                                </div>
                                <div class="metric">
                                    <strong>Active Calls:</strong> ${metrics.performanceStats.apiMetrics.currentActive}
                                </div>
                            </div>
                            
                            <div class="performance-card">
                                <h3>💾 Cache Performance</h3>
                                <div class="metric">
                                    <strong>Hit Rate:</strong> ${metrics.cacheStats.hitRatePercent}
                                </div>
                                <div class="metric">
                                    <strong>Cache Hits:</strong> ${metrics.cacheStats.hits}
                                </div>
                                <div class="metric">
                                    <strong>Cache Misses:</strong> ${metrics.cacheStats.misses}
                                </div>
                                <div class="metric">
                                    <strong>Cache Age:</strong> ${(metrics.cacheStats.cacheAge / 1000).toFixed(2)}s
                                </div>
                            </div>
                            
                            <div class="performance-card">
                                <h3>⚙️ Deal Processing</h3>
                                <div class="metric">
                                    <strong>Total Deals:</strong> ${metrics.performanceStats.dealProcessing.totalDeals}
                                </div>
                                <div class="metric">
                                    <strong>Processed Deals:</strong> ${metrics.performanceStats.dealProcessing.processedDeals}
                                </div>
                                <div class="metric">
                                    <strong>Avg Processing Time:</strong> ${metrics.performanceStats.dealProcessing.averageProcessingTime}
                                </div>
                                <div class="metric">
                                    <strong>Total Processing Time:</strong> ${metrics.performanceStats.dealProcessing.totalProcessingTime}
                                </div>
                            </div>
                            
                            <div class="performance-card">
                                <h3>🖥️ System Health</h3>
                                <div class="metric">
                                    <strong>Uptime:</strong> ${(metrics.performanceStats.uptime / 1000 / 60).toFixed(2)} minutes
                                </div>
                                <div class="metric">
                                    <strong>Memory Used:</strong> ${performance.memory ? Math.round(performance.memory.usedJSHeapSize / 1024 / 1024) + 'MB' : 'N/A'}
                                </div>
                                <div class="metric">
                                    <strong>Memory Total:</strong> ${performance.memory ? Math.round(performance.memory.totalJSHeapSize / 1024 / 1024) + 'MB' : 'N/A'}
                                </div>
                                <div class="metric">
                                    <strong>Memory Limit:</strong> ${performance.memory ? Math.round(performance.memory.jsHeapSizeLimit / 1024 / 1024) + 'MB' : 'N/A'}
                                </div>
                            </div>
                        </div>
                        
                        <div class="performance-card">
                            <h3>🔧 Cache Details</h3>
                            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px;">
                                <div class="metric">
                                    <strong>Location Cache:</strong> ${metrics.cacheStats.locationCacheSize} items
                                </div>
                                <div class="metric">
                                    <strong>User Cache:</strong> ${metrics.cacheStats.userCacheSize} items
                                </div>
                                <div class="metric">
                                    <strong>Brand Cache:</strong> ${metrics.cacheStats.brandDataCacheSize} items
                                </div>
                                <div class="metric">
                                    <strong>Match Score Cache:</strong> ${metrics.cacheStats.matchScoreCacheSize} items
                                </div>
                                <div class="metric">
                                    <strong>Market Performance Cache:</strong> ${metrics.cacheStats.marketPerformanceCacheSize} items
                                </div>
                                <div class="metric">
                                    <strong>Strategic Intent Cache:</strong> ${metrics.cacheStats.strategicIntentCacheSize} items
                                </div>
                            </div>
                        </div>
                        
                        <div style="margin-top: 20px; text-align: center;">
                            <button onclick="this.closest('.modal').remove()" style="background: #007bff; color: white; border: none; padding: 10px 20px; border-radius: 5px; cursor: pointer;">
                                Close Dashboard
                            </button>
                            <button onclick="window.dashboard.logPerformanceMetrics(); this.closest('.modal').remove()" style="background: #28a745; color: white; border: none; padding: 10px 20px; border-radius: 5px; cursor: pointer; margin-left: 10px;">
                                Refresh & Close
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        // Add styles for the performance dashboard
        const styles = `
            <style>
                .performance-card {
                    background: #f8f9fa;
                    border: 1px solid #dee2e6;
                    border-radius: 8px;
                    padding: 15px;
                    margin-bottom: 15px;
                }
                .performance-card h3 {
                    margin: 0 0 10px 0;
                    color: #495057;
                    font-size: 16px;
                }
                .metric {
                    margin: 5px 0;
                    font-size: 14px;
                }
                .metric strong {
                    color: #6c757d;
                }
            </style>
        `;
        
        // Remove existing performance modal if any
        const existingModal = document.getElementById('performanceModal');
        if (existingModal) {
            existingModal.remove();
        }
        
        // Add styles and modal to page
        document.head.insertAdjacentHTML('beforeend', styles);
        document.body.insertAdjacentHTML('beforeend', modalHtml);
    }
    
    // Performance optimization recommendations
    getPerformanceRecommendations() {
        const metrics = this.logPerformanceMetrics();
        const recommendations = [];
        
        // API Performance Analysis
        if (metrics.performanceStats.apiMetrics.averageDuration > 1000) {
            recommendations.push({
                category: 'API Performance',
                issue: 'Slow API calls detected',
                recommendation: 'Consider increasing rate limiting delay or implementing request batching',
                priority: 'High'
            });
        }
        
        if (metrics.performanceStats.apiMetrics.successRate < '95%') {
            recommendations.push({
                category: 'API Reliability',
                issue: 'Low API success rate',
                recommendation: 'Check network stability and implement retry logic',
                priority: 'High'
            });
        }
        
        // Cache Performance Analysis
        if (parseFloat(metrics.cacheStats.hitRatePercent) < 70) {
            recommendations.push({
                category: 'Cache Performance',
                issue: 'Low cache hit rate',
                recommendation: 'Increase cache expiry time or improve cache key strategy',
                priority: 'Medium'
            });
        }
        
        // Memory Usage Analysis
        if (performance.memory) {
            const memoryUsagePercent = (performance.memory.usedJSHeapSize / performance.memory.jsHeapSizeLimit) * 100;
            if (memoryUsagePercent > 80) {
                recommendations.push({
                    category: 'Memory Management',
                    issue: 'High memory usage detected',
                    recommendation: 'Consider implementing cache size limits or memory cleanup',
                    priority: 'High'
                });
            }
        }
        
        // Deal Processing Analysis
        if (metrics.performanceStats.dealProcessing.averageProcessingTime > 500) {
            recommendations.push({
                category: 'Deal Processing',
                issue: 'Slow deal processing',
                recommendation: 'Optimize deal processing logic or implement parallel processing',
                priority: 'Medium'
            });
        }
        
        return recommendations;
    }
    
    // Show performance recommendations
    showPerformanceRecommendations() {
        const recommendations = this.getPerformanceRecommendations();
        
        if (recommendations.length === 0) {
            console.log('✅ No performance issues detected! System is running optimally.');
            return;
        }
        
        console.log('🔧 ===== PERFORMANCE RECOMMENDATIONS =====');
        recommendations.forEach((rec, index) => {
            const priorityIcon = rec.priority === 'High' ? '🔴' : rec.priority === 'Medium' ? '🟡' : '🟢';
            console.log(`${index + 1}. ${priorityIcon} [${rec.category}] ${rec.issue}`);
            console.log(`   💡 Recommendation: ${rec.recommendation}`);
        });
        console.log('🔧 ===== END RECOMMENDATIONS =====');
        
        return recommendations;
    }
    
    // Performance optimization function
    optimizePerformance() {
        console.log('🚀 ===== PERFORMANCE OPTIMIZATION =====');
        
        // 1. Clear and rebuild cache with better settings
        console.log('1. Optimizing cache settings...');
        this.cache.cacheExpiry = 20 * 60 * 1000; // 20 minutes
        this.cache.maxCacheSize = 2000; // Increase cache size
        
        // 2. Reduce API rate limiting delay
        console.log('2. Optimizing API rate limiting...');
        // Already reduced from 200ms to 100ms
        
        // 3. Clear old cache entries
        console.log('3. Clearing old cache entries...');
        const now = Date.now();
        let clearedEntries = 0;
        
        // Clear expired brand data cache entries
        for (const [key, value] of this.cache.brandData.entries()) {
            if (now - value.timestamp > this.cache.cacheExpiry) {
                this.cache.brandData.delete(key);
                clearedEntries++;
            }
        }
        
        console.log(`   Cleared ${clearedEntries} expired cache entries`);
        
        // 4. Reset performance counters for fresh start
        console.log('4. Resetting performance counters...');
        this.performance.apiCallMetrics = {
            total: 0,
            successful: 0,
            failed: 0,
            current: 0,
            totalDuration: 0,
            averageDuration: 0,
            slowestCall: { url: '', duration: 0, timestamp: '' }
        };
        
        this.performance.cache.hits = 0;
        this.performance.cache.misses = 0;
        
        // 5. Enable debug mode for better monitoring
        console.log('5. Enabling enhanced monitoring...');
        this.performance.debugMode = true;
        
        console.log('✅ Performance optimization complete!');
        console.log('📊 New settings:');
        console.log(`   - Cache expiry: ${this.cache.cacheExpiry / 1000 / 60} minutes`);
        console.log(`   - Max cache size: ${this.cache.maxCacheSize} items`);
        console.log(`   - API rate limit: 100ms delay`);
        console.log(`   - Debug mode: ${this.performance.debugMode ? 'enabled' : 'disabled'}`);
        console.log('🚀 ===== OPTIMIZATION COMPLETE =====');
        
        return {
            cacheExpiry: this.cache.cacheExpiry,
            maxCacheSize: this.cache.maxCacheSize,
            apiRateLimit: 100,
            debugMode: this.performance.debugMode,
            clearedEntries: clearedEntries
        };
    }
    
    // Fix cache performance issues
    fixCachePerformance() {
        console.log('🔧 ===== CACHE PERFORMANCE FIX =====');
        
        // 1. Reset cache hit/miss counters
        console.log('1. Resetting cache counters...');
        this.cache.hitCount = 0;
        this.cache.missCount = 0;
        this.performance.cache.hits = 0;
        this.performance.cache.misses = 0;
        
        // 2. Ensure all cache maps exist
        console.log('2. Ensuring all cache maps exist...');
        if (!this.cache.contactData) {
            this.cache.contactData = new Map();
        }
        if (!this.cache.marketPerformanceData) {
            this.cache.marketPerformanceData = new Map();
        }
        if (!this.cache.strategicIntentData) {
            this.cache.strategicIntentData = new Map();
        }
        if (!this.cache.hotelOwnershipData) {
            this.cache.hotelOwnershipData = new Map();
        }
        if (!this.cache.companyProfileData) {
            this.cache.companyProfileData = new Map();
        }
        
        // 3. Update cache timestamp
        console.log('3. Updating cache timestamp...');
        this.cache.lastCacheTime = Date.now();
        
        // 4. Test cache functionality
        console.log('4. Testing cache functionality...');
        const testKey = 'test_' + Date.now();
        const testValue = { test: true, timestamp: Date.now() };
        
        // Test set
        this.setToCache(this.cache.locationData, testKey, testValue);
        
        // Test get
        const retrieved = this.getFromCache(this.cache.locationData, testKey);
        if (retrieved) {
            console.log('   ✅ Cache functionality test passed');
        } else {
            console.log('   ❌ Cache functionality test failed');
        }
        
        // Clean up test data
        this.cache.locationData.delete(testKey);
        
        console.log('✅ Cache performance fix complete!');
        console.log('📊 Cache status:');
        console.log(`   - Location cache: ${this.cache.locationData.size} items`);
        console.log(`   - User cache: ${this.cache.userData.size} items`);
        console.log(`   - Contact cache: ${this.cache.contactData.size} items`);
        console.log(`   - Brand cache: ${this.cache.brandData.size} items`);
        console.log(`   - Cache expiry: ${this.cache.cacheExpiry / 1000 / 60} minutes`);
        console.log('🔧 ===== CACHE FIX COMPLETE =====');
        
        return {
            locationCache: this.cache.locationData.size,
            userCache: this.cache.userData.size,
            contactCache: this.cache.contactData.size,
            brandCache: this.cache.brandData.size,
            cacheExpiry: this.cache.cacheExpiry,
            testPassed: !!retrieved
        };
    }
    
    // Helper function to debug contact data fields
    debugContactFields() {
        console.log('🔍 ===== CONTACT DATA FIELD ANALYSIS =====');
        const dealsWithContactData = this.deals.filter(deal => deal.contactData);
        console.log(`Found ${dealsWithContactData.length} deals with contact data`);
        
        if (dealsWithContactData.length > 0) {
            const sampleDeal = dealsWithContactData[0];
            console.log('Sample deal contact data fields:', Object.keys(sampleDeal.contactData));
            console.log('Sample deal contact data:', sampleDeal.contactData);
            
            // Check for fields that might link to User Management or Users table
            const allFields = Object.keys(sampleDeal.contactData);
            const userLinkFields = allFields.filter(field => 
                field.toLowerCase().includes('user') || 
                field.toLowerCase().includes('management') || 
                field.toLowerCase().includes('profile') ||
                field.toLowerCase().includes('contact')
            );
            
            console.log('Potential User Management/Users link fields found:', userLinkFields);
            
            // Show values for potential user link fields
            userLinkFields.forEach(field => {
                console.log(`Field "${field}":`, sampleDeal.contactData[field]);
            });
            
            // Also check for any fields that might be link fields (arrays or record IDs)
            const possibleLinkFields = allFields.filter(field => {
                const value = sampleDeal.contactData[field];
                return Array.isArray(value) || (typeof value === 'string' && value.startsWith('rec'));
            });
            
            console.log('Fields that might be links (arrays or record IDs):', possibleLinkFields);
            possibleLinkFields.forEach(field => {
                console.log(`Possible link field "${field}":`, sampleDeal.contactData[field]);
            });
            
            // Check for any fields that might contain images directly
            const imageFields = allFields.filter(field => 
                field.toLowerCase().includes('image') || 
                field.toLowerCase().includes('photo') || 
                field.toLowerCase().includes('avatar') || 
                field.toLowerCase().includes('picture') ||
                field.toLowerCase().includes('headshot')
            );
            
            console.log('Potential direct image fields found:', imageFields);
            
            // Show values for potential image fields
            imageFields.forEach(field => {
                console.log(`Field "${field}":`, sampleDeal.contactData[field]);
            });
        }
        
        // ALSO CHECK THE MAIN DEALS TABLE FOR USER LINKS
        console.log('🔍 ===== MAIN DEALS TABLE ANALYSIS =====');
        if (this.deals.length > 0) {
            const sampleDeal = this.deals[0];
            console.log('Sample deal airtableData fields:', Object.keys(sampleDeal.airtableData || {}));
            
            // Check for user link fields in the main deals table
            const dealFields = Object.keys(sampleDeal.airtableData || {});
            const userLinkFields = dealFields.filter(field => 
                field.toLowerCase().includes('user') || 
                field.toLowerCase().includes('management') || 
                field.toLowerCase().includes('profile') || 
                field.toLowerCase().includes('contact')
            );
            
            console.log('Potential user link fields in main deals table:', userLinkFields);
            
            // Show values for potential user link fields
            userLinkFields.forEach(field => {
                console.log(`Deal field "${field}":`, sampleDeal.airtableData[field]);
            });
            
            // Also check for any fields that might be link fields (arrays or record IDs)
            const possibleLinkFields = dealFields.filter(field => {
                const value = sampleDeal.airtableData[field];
                return Array.isArray(value) || (typeof value === 'string' && value.startsWith('rec'));
            });
            
            console.log('Fields that might be links in main deals table:', possibleLinkFields);
            possibleLinkFields.forEach(field => {
                console.log(`Possible link field "${field}":`, sampleDeal.airtableData[field]);
            });
        }
        
        console.log('🔍 ===== END CONTACT DATA ANALYSIS =====');
    }

    // Helper function to debug email fields specifically
    debugEmailFields() {
        console.log('🔍 ===== EMAIL FIELD ANALYSIS =====');
        const dealsWithContactData = this.filteredDeals.filter(deal => deal.contactData);
        console.log(`Found ${dealsWithContactData.length} deals with contact data`);
        
        if (dealsWithContactData.length > 0) {
            // Check first few deals for email fields
            const sampleDeals = dealsWithContactData.slice(0, 3);
            
            sampleDeals.forEach((deal, index) => {
                console.log(`\n--- Deal ${index + 1} (ID: ${deal.id}) ---`);
                console.log(`Owner Email (processed): ${deal.ownerEmail}`);
                console.log(`Contact Data Available: ${!!deal.contactData}`);
                
                if (deal.contactData) {
                    const allFields = Object.keys(deal.contactData);
                    const emailFields = allFields.filter(field => 
                        field.toLowerCase().includes('email') || 
                        field.toLowerCase().includes('mail')
                    );
                    
                    console.log(`Email-related fields:`, emailFields);
                    emailFields.forEach(field => {
                        console.log(`  "${field}": ${deal.contactData[field]}`);
                    });
                }
            });
        }
        
        console.log('🔍 ===== END EMAIL FIELD ANALYSIS =====');
    }

    // Helper function to debug all available fields for Learn More modal
    debugAllFields() {
        console.log('🔍 ===== COMPREHENSIVE FIELD ANALYSIS FOR LEARN MORE MODAL =====');
        
        if (this.deals.length > 0) {
            const sampleDeal = this.deals[0];
            
            console.log('🔍 ===== MAIN DEALS TABLE FIELDS =====');
            console.log('Available fields in main deals table:', Object.keys(sampleDeal.airtableData || {}));
            console.log('Sample main deal data:', sampleDeal.airtableData);
            
            console.log('🔍 ===== LOCATION DATA FIELDS =====');
            if (sampleDeal.locationData) {
                console.log('Available fields in location data:', Object.keys(sampleDeal.locationData));
                console.log('Sample location data:', sampleDeal.locationData);
            } else {
                console.log('No location data available');
            }
            
            console.log('🔍 ===== CONTACT DATA FIELDS =====');
            if (sampleDeal.contactData) {
                console.log('Available fields in contact data:', Object.keys(sampleDeal.contactData));
                console.log('Sample contact data:', sampleDeal.contactData);
            } else {
                console.log('No contact data available');
            }
            
            console.log('🔍 ===== MARKET PERFORMANCE DATA FIELDS =====');
            if (sampleDeal.marketPerformanceData) {
                console.log('Available fields in market performance data:', Object.keys(sampleDeal.marketPerformanceData));
                console.log('Sample market performance data:', sampleDeal.marketPerformanceData);
            } else {
                console.log('No market performance data available');
            }
            
            console.log('🔍 ===== STRATEGIC INTENT DATA FIELDS =====');
            if (sampleDeal.strategicIntentData) {
                console.log('Available fields in strategic intent data:', Object.keys(sampleDeal.strategicIntentData));
                console.log('Sample strategic intent data:', sampleDeal.strategicIntentData);
            } else {
                console.log('No strategic intent data available');
            }
            
            console.log('🔍 ===== HOTEL OWNERSHIP DATA FIELDS =====');
            if (sampleDeal.hotelOwnershipData) {
                console.log('Available fields in hotel ownership data:', Object.keys(sampleDeal.hotelOwnershipData));
                console.log('Sample hotel ownership data:', sampleDeal.hotelOwnershipData);
            } else {
                console.log('No hotel ownership data available');
            }
        }
        
        console.log('🔍 ===== END COMPREHENSIVE FIELD ANALYSIS =====');
    }
    
    // Enhanced debug logging
    debugLog(message, data = null, level = 'info') {
        if (!this.performance.debugMode && level !== 'error') return;
        
        const timestamp = new Date().toISOString();
        const prefix = `[${timestamp}] 🐛`;
        
        switch (level) {
            case 'error':
                console.error(`${prefix} ERROR: ${message}`, data);
                break;
            case 'warn':
                console.warn(`${prefix} WARN: ${message}`, data);
                break;
            case 'info':
            default:
                console.log(`${prefix} ${message}`, data);
                break;
        }
    }
    
    // Performance timing wrapper
    async timeOperation(operationName, operation) {
        const startTime = performance.now();
        try {
            const result = await operation();
            const endTime = performance.now();
            const duration = endTime - startTime;
            
            this.debugLog(`${operationName} completed in ${duration.toFixed(2)}ms`);
            
            // Store timing data based on operation type
            if (operationName.includes('API')) {
                this.performance.apiCallTimes.push(duration);
                this.performance.apiCalls++;
            } else if (operationName.includes('render')) {
                this.performance.renderTimes.push(duration);
            } else if (operationName.includes('sort')) {
                this.performance.sortTimes.push(duration);
            } else if (operationName.includes('filter')) {
                this.performance.filterTimes.push(duration);
            }
            
            return result;
        } catch (error) {
            const endTime = performance.now();
            const duration = endTime - startTime;
            this.debugLog(`${operationName} failed after ${duration.toFixed(2)}ms`, error, 'error');
            throw error;
        }
    }

    async init() {
        // Show loading animation
        this.showLoading(true, 'Initializing dashboard...', '2-3 seconds');
        
        // Get user and brand IDs from Webflow
        this.userId = document.getElementById('airtable-user-id')?.textContent;
        this.brandId = document.getElementById('airtable-brand-id')?.textContent;
        
        // 🔍 DEBUGGING: Show where the brand ID comes from
        console.log('🔍 ===== BRAND ID SOURCE DEBUGGING =====');
        console.log('  - HTML element airtable-brand-id content:', document.getElementById('airtable-brand-id')?.textContent);
        console.log('  - this.brandId set to:', this.brandId);
        console.log('  - Brand selector dropdown value:', document.getElementById('brandSelector')?.value);
        
        // Debug: Check if IDs are loaded correctly
        if (!this.userId || !this.brandId) {
            console.warn('⚠️ User ID or Brand ID not found in HTML elements');
        }
        
        // Clear cache on initialization to ensure fresh data from Airtable
        console.log('🔄 Clearing cache to fetch fresh Airtable data...');
        this.clearCache();
        this.cache.lastCacheTime = Date.now(); // Reset cache timer after clearing
        
        // Populate brand selector dropdown from Airtable
        await this.populateBrandSelector();
        
        await this.loadDeals();
        this.setupEventListeners();
        await this.renderDeals();
        this.updateTabCounts();
        
    }
    
    // Populate brand selector dropdown with all brands from Airtable
    async populateBrandSelector() {
        try {
            const brandSelector = document.getElementById('brandSelector');
            if (!brandSelector) {
                console.warn('⚠️ Brand selector dropdown not found');
                return;
            }
            
            console.log('🔄 Fetching all brands from backend API to populate dropdown...');
            const apiBase = this.getApiBaseUrl();
            const response = await fetch(`${apiBase}/api/brand-library/brands?allStatuses=1`, {
                method: 'GET',
                headers: { 'Accept': 'application/json' }
            });
            
            if (!response.ok) {
                throw new Error(`Failed to fetch brands: ${response.status} ${response.statusText}`);
            }
            
            const data = await response.json();
            const records = Array.isArray(data?.brands) ? data.brands : (Array.isArray(data?.records) ? data.records : []);
            const brandNames = [...new Set(
                records
                    .map((r) => r?.brandName || r?.fields?.['Brand Name'] || r?.name || '')
                    .filter((name) => typeof name === 'string' && name.trim() !== '')
            )].sort();
            
            console.log(`✅ Found ${brandNames.length} brands in Airtable`);
            console.log('📋 Brands:', brandNames);
            
            // Store current selected value
            const currentValue = brandSelector.value;
            
            // Clear existing options (keep the "Select Brand" placeholder)
            brandSelector.innerHTML = '<option value="">Select Brand</option>';
            
            // Add all brands as options
            brandNames.forEach(brandName => {
                const option = document.createElement('option');
                option.value = brandName;
                option.textContent = brandName;
                brandSelector.appendChild(option);
            });
            
            // Restore previously selected value if it still exists
            if (currentValue && brandNames.includes(currentValue)) {
                brandSelector.value = currentValue;
                this.brandId = currentValue;
            } else if (this.brandId && brandNames.includes(this.brandId)) {
                // Or use the brandId from initialization if it's valid
                brandSelector.value = this.brandId;
            }
            
            console.log(`✅ Brand dropdown populated with ${brandNames.length} brands`);
            
        } catch (error) {
            console.error('❌ Error populating brand selector:', error);
            // Keep the hardcoded options as fallback
            console.warn('⚠️ Using hardcoded brand options as fallback');
        }
    }
    
    
    
    // Test the scoring system with sample data
    async testScoringWithSampleData() {
        console.log('🧪 Testing Match Scoring System with Sample Data...');
        
        try {
            // Sample brand data
            const sampleBrandData = {
                brandBasics: {
                    'Priority Markets for This Brand': ['United States', 'Canada', 'Global'],
                    'Markets to Avoid or Saturated': ['Saturated Market'],
                    'Chain Scale': 'Upper Upscale',
                    'Service Model': 'Full Service',
                    'Brand Model / Format': 'Hard Brand',
                    'Preferred Owner/Investor Type': 'Developer; PE; Family Office',
                    'Brand Name': 'Luxury Hotel Brand'
                },
                brandFootprint: {
                    'Number of Open Hotels (Americas)': 15,
                    'Number of Open Hotels (CALA)': 8,
                    'Number of Open Hotels (EU)': 12,
                    'Number of Open Hotels (MEA)': 5,
                    'Number of Open Hotels (APAC)': 10
                },
                brandStandards: {
                    'Brand Standards': 'Restaurant; Fitness Center; Business Center; Concierge'
                },
                brandTerms: {
                    'Willing to Negotiate Incentives?': 'Yes',
                    'Types of Incentives Offered': 'Key Money; Co-op Marketing; Fee Discount',
                    'Royalty Min': 4.0,
                    'Royalty Max': 6.0,
                    'Marketing Min': 1.5,
                    'Marketing Max': 2.5,
                    'Tech Min': 0.5,
                    'Tech Max': 1.0,
                    'Loyalty Min': 0.3,
                    'Loyalty Max': 0.7
                },
                brandFit: {
                    'Ideal Project Size (Min Rooms)': 100,
                    'Ideal Project Size (Max Rooms)': 350
                }
            };
            
            // Sample deal data
            const sampleDealFields = {
                'Country': 'United States',
                'Importance of Brand Recognition': 4,
                'Hotel Chain Scale': 'Upper Upscale',
                'Service Model': 'Full Service',
                'Total Number of Rooms/Keys': 200,
                'Ownership Structure': 'Developer',
                'Deal Amenities & Facilities': 'Restaurant; Fitness Center; Business Center; Pool; Spa',
                'Fee Expectations': 'Royalty: 5.0%; Marketing: 2.0%; Tech: 0.8%; Loyalty: 0.5%',
                'Incentives Sought': 'Key Money; Co-op Marketing',
                'Soft vs Hard Brand Preference': 'Hard Brand',
                'Preferred Brands': 'Luxury Hotel Brand, Premium Brand'
            };
            
            const sampleLocationData = {
                'Country': 'United States',
                'Total Number of Rooms/Keys': 200,
                'Hotel Chain Scale': 'Upper Upscale'
            };
            
            console.log('📊 Sample Data:');
            console.log('Brand Data:', sampleBrandData);
            console.log('Deal Data:', sampleDealFields);
            console.log('Location Data:', sampleLocationData);
            
            // Test each scoring function
            const subscores = {};
            subscores.MKT1 = await this.calculateMKT1(sampleDealFields, sampleLocationData, sampleBrandData);
            subscores.MKT2 = await this.calculateMKT2(sampleDealFields, sampleLocationData, sampleBrandData);
            subscores.SEG1 = await this.calculateSEG1(sampleDealFields, sampleLocationData, sampleBrandData);
            subscores.SVC1 = await this.calculateSVC1(sampleDealFields, sampleLocationData, sampleBrandData);
            subscores.SIZE1 = await this.calculateSIZE1(sampleDealFields, sampleLocationData, sampleBrandData);
            subscores.OWN1 = await this.calculateOWN1(sampleDealFields, sampleLocationData, sampleBrandData);
            subscores.AMN1 = await this.calculateAMN1(sampleDealFields, sampleLocationData, sampleBrandData);
            subscores.FIN1 = await this.calculateFIN1(sampleDealFields, sampleLocationData, sampleBrandData, {});
            subscores.INC1 = await this.calculateINC1(sampleDealFields, sampleLocationData, sampleBrandData, {});
            subscores.STR1 = await this.calculateSTR1(sampleDealFields, sampleLocationData, sampleBrandData, {});
            subscores.PREF1 = await this.calculatePREF1(sampleDealFields, sampleLocationData, sampleBrandData, {});
            subscores.PROJ1 = await this.calculatePROJ1(sampleDealFields, sampleLocationData, sampleBrandData);
            subscores.PROJ2 = await this.calculatePROJ2(sampleDealFields, sampleLocationData, sampleBrandData);
            
            // Calculate weighted total
            const weights = {
                MKT1: 14, MKT2: 8, SEG1: 10, SVC1: 8, SIZE1: 12,
                OWN1: 4, AMN1: 10, FIN1: 10, INC1: 10, STR1: 4, PREF1: 10, PROJ1: 15, PROJ2: 10
            };
            
            let weightedSum = 0;
            let totalWeight = 0;
            
            for (const [key, score] of Object.entries(subscores)) {
                if (score !== null && score !== undefined) {
                    weightedSum += score * weights[key];
                    totalWeight += weights[key];
                }
            }
            
            const finalScore = totalWeight > 0 ? Math.round(weightedSum / totalWeight) : 0;
            
            console.log('🎯 Sample Scoring Results:');
            console.log('Subscores:', subscores);
            console.log('Weights:', weights);
            console.log('Final Score:', finalScore);
            
            // Test hard fail condition
            const testHardFail = this.hasHardFail(sampleDealFields, sampleLocationData, sampleBrandData);
            console.log('Hard Fail Test:', testHardFail);
            
            // Expected results for this sample data
            const expectedResults = {
                MKT1: 100, // US is in priority markets
                MKT2: 100, // 15 hotels in Americas > threshold of 10
                SEG1: 100, // Both Upper Upscale (tier 4)
                SVC1: 100, // Both Full Service
                SIZE1: 100, // 200 rooms is within 100-350 range
                OWN1: 100, // Developer is in preferred types
                AMN1: 96,  // Has most required amenities, missing concierge (-12), has bonus amenities
                FIN1: 100, // All fees within brand ranges
                INC1: 96,  // 80 + 6 (key money) + 2 (co-op) = 88, but capped at 96
                STR1: 100, // Hard brand preference matches
                PREF1: 100 // Brand name in preferred list
            };
            
            console.log('Expected Results:', expectedResults);
            
            // Verify results
            let allTestsPassed = true;
            for (const [key, expected] of Object.entries(expectedResults)) {
                const actual = subscores[key];
                const passed = Math.abs(actual - expected) <= 5; // Allow 5 point tolerance
                console.log(`${key}: Expected ${expected}, Got ${actual}, ${passed ? '✅ PASS' : '❌ FAIL'}`);
                if (!passed) allTestsPassed = false;
            }
            
            console.log(`\n🏆 Overall Test Result: ${allTestsPassed ? '✅ ALL TESTS PASSED' : '❌ SOME TESTS FAILED'}`);
            
            return {
                success: allTestsPassed,
                finalScore: finalScore,
                subscores: subscores,
                expectedResults: expectedResults
            };
            
        } catch (error) {
            console.error('❌ Error in sample test:', error);
            return { success: false, error: error.message };
        }
    }

    // Show scoring breakdown modal for a specific deal
    async showScoringBreakdown(dealId) {
        const deal = this.deals.find(d => d.id === dealId);
        if (!deal) {
            console.error('Deal not found:', dealId);
            return;
        }

        try {
            // Get the scoring breakdown from the deal's stored data or calculate it
            let scoringBreakdown = deal.scoringBreakdown;
            
            if (!scoringBreakdown) {
                // Calculate scoring breakdown if not stored
                // Use the selected brand from dropdown (if available) or fall back to this.brandId
                const brandSelector = document.getElementById('brandSelector');
                const selectedBrand = brandSelector?.value || this.brandId;
                const brandData = await this.getBrandDataForScoring(selectedBrand);
                if (brandData) {
                    scoringBreakdown = {
                        MKT1: await this.calculateMKT1(deal.airtableData, deal.locationData, brandData),
                        MKT2: await this.calculateMKT2(deal.airtableData, deal.locationData, brandData),
                        SEG1: await this.calculateSEG1(deal.airtableData, deal.locationData, brandData),
                        SVC1: await this.calculateSVC1(deal.airtableData, deal.locationData, brandData),
                        SIZE1: await this.calculateSIZE1(deal.airtableData, deal.locationData, brandData),
                        OWN1: await this.calculateOWN1(deal.airtableData, deal.locationData, brandData, deal.marketPerformanceData),
                        AMN1: await this.calculateAMN1(deal.airtableData, deal.locationData, brandData),
                        FIN1: await this.calculateFIN1(deal.airtableData, deal.locationData, brandData, deal.marketPerformanceData || {}),
                        INC1: await this.calculateINC1(deal.airtableData, deal.locationData, brandData, deal.marketPerformanceData || {}),
                        STR1: await this.calculateSTR1(deal.airtableData, deal.locationData, brandData),
                        PREF1: await this.calculatePREF1(deal.airtableData, deal.locationData, brandData),
                        PROJ1: await this.calculatePROJ1(deal.airtableData, deal.locationData, brandData),
                        PROJ2: await this.calculatePROJ2(deal.airtableData, deal.locationData, brandData)
                    };
                }
            }

            this.renderScoringBreakdownModal(deal, scoringBreakdown);
        } catch (error) {
            console.error('Error showing scoring breakdown:', error);
            alert('Error loading scoring breakdown. Please try again.');
        }
    }

    // Render the scoring breakdown modal
    renderScoringBreakdownModal(deal, scoringBreakdown) {
        const criteria = [
            { key: 'MKT1', name: 'Priority Market Fit', weight: 14, description: 'How well the deal location matches the brand\'s priority markets' },
            { key: 'MKT2', name: 'Recognition Density', weight: 8, description: 'Brand presence in the region vs. owner\'s recognition needs' },
            { key: 'SEG1', name: 'Chain Scale Proximity', weight: 10, description: 'How closely the deal\'s chain scale matches the brand\'s scale' },
            { key: 'SVC1', name: 'Service Model Alignment', weight: 8, description: 'Compatibility between deal\'s and brand\'s service models' },
            { key: 'SIZE1', name: 'Room Range Fit', weight: 12, description: 'How well the deal\'s room count fits the brand\'s ideal range' },
            { key: 'OWN1', name: 'Owner Type Match', weight: 4, description: 'Alignment between deal\'s ownership structure and brand preferences' },
            { key: 'AMN1', name: 'Amenities & Standards', weight: 10, description: 'Required amenities present and optional amenities available' },
            { key: 'FIN1', name: 'Fee Tolerance', weight: 10, description: 'How well the deal\'s fee expectations align with brand ranges' },
            { key: 'INC1', name: 'Incentives Match', weight: 10, description: 'Alignment between deal\'s incentive needs and brand offerings' },
            { key: 'STR1', name: 'Brand Model Preference', weight: 4, description: 'Match between deal\'s brand preference and brand model type' },
            { key: 'PREF1', name: 'Preferred Brand Bonus', weight: 10, description: 'Bonus points if this brand is in the owner\'s preferred list' },
            { key: 'PROJ1', name: 'Project Type Compatibility', weight: 15, description: 'Does the project type match your acceptable criteria?' },
            { key: 'PROJ2', name: 'Building Type Compatibility', weight: 10, description: 'Does the building type match your acceptable criteria?' }
        ];

        const getScoreClass = (score) => {
            if (score >= 90) return 'excellent';
            if (score >= 70) return 'good';
            if (score >= 50) return 'average';
            return 'poor';
        };

        const getScoreDescription = (key, score) => {
            const descriptions = {
                MKT1: score >= 90 ? 'Perfect market match' : score >= 70 ? 'Good market fit' : score >= 50 ? 'Average market fit' : 'Poor market fit',
                MKT2: score >= 90 ? 'Strong brand presence' : score >= 70 ? 'Adequate brand presence' : score >= 50 ? 'Limited brand presence' : 'Insufficient brand presence',
                SEG1: score >= 90 ? 'Perfect scale match' : score >= 70 ? 'Good scale proximity' : score >= 50 ? 'Acceptable scale difference' : 'Poor scale match',
                SVC1: score >= 90 ? 'Perfect service alignment' : score >= 70 ? 'Good service compatibility' : score >= 50 ? 'Some service flexibility' : 'Poor service match',
                SIZE1: score >= 90 ? 'Perfect room count fit' : score >= 70 ? 'Good room count range' : score >= 50 ? 'Acceptable room count' : 'Poor room count fit',
                OWN1: score >= 90 ? 'Perfect owner type match' : score >= 70 ? 'Good owner compatibility' : score >= 50 ? 'Acceptable owner type' : 'Poor owner type match',
                AMN1: score >= 90 ? 'All required amenities present' : score >= 70 ? 'Most amenities available' : score >= 50 ? 'Some amenities missing' : 'Many amenities missing',
                FIN1: score >= 90 ? 'Perfect fee alignment' : score >= 70 ? 'Good fee compatibility' : score >= 50 ? 'Some fee flexibility' : 'Poor fee alignment',
                INC1: score >= 90 ? 'Strong incentive match' : score >= 70 ? 'Good incentive alignment' : score >= 50 ? 'Some incentive compatibility' : 'Poor incentive match',
                STR1: score >= 90 ? 'Perfect brand model match' : score >= 70 ? 'Good brand compatibility' : score >= 50 ? 'Some brand flexibility' : 'Poor brand model match',
                PREF1: score >= 90 ? 'Brand is preferred choice' : 'Brand not in preferred list',
                PROJ1: score >= 90 ? 'Project type is acceptable' : 'Project type not acceptable',
                PROJ2: score >= 90 ? 'Building type is acceptable' : 'Building type not acceptable'
            };
            return descriptions[key] || 'Score calculated';
        };

        const criteriaHtml = criteria.map(criterion => {
            const score = scoringBreakdown[criterion.key] || 0;
            const scoreClass = getScoreClass(score);
            const description = getScoreDescription(criterion.key, score);

            return `
                <div class="scoring-criterion ${scoreClass}">
                    <div class="criterion-header">
                        <div class="criterion-name">${criterion.name}</div>
                        <div class="criterion-score ${scoreClass}">${score}</div>
                    </div>
                    <div class="criterion-description">${description}</div>
                    <div class="criterion-weight">Weight: ${criterion.weight}%</div>
                </div>
            `;
        }).join('');

        const modalHtml = `
            <div class="scoring-breakdown-modal" id="scoringBreakdownModal">
                <div class="scoring-breakdown-content">
                    <div class="scoring-breakdown-header">
                        <div class="scoring-breakdown-title">Match Score Breakdown</div>
                        <button class="scoring-breakdown-close" onclick="dashboard.closeScoringBreakdown()">&times;</button>
                    </div>
                    
                    <div class="scoring-breakdown-total">
                        <div class="scoring-breakdown-total-score">${deal.matchScore}</div>
                        <div class="scoring-breakdown-total-label">Overall Match Score</div>
                    </div>
                    
                    <div class="scoring-criteria">
                        ${criteriaHtml}
                    </div>
                </div>
            </div>
        `;

        // Remove existing modal if any
        const existingModal = document.getElementById('scoringBreakdownModal');
        if (existingModal) {
            existingModal.remove();
        }

        // Add modal to page
        document.body.insertAdjacentHTML('beforeend', modalHtml);

        // Close modal when clicking outside
        const modal = document.getElementById('scoringBreakdownModal');
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                this.closeScoringBreakdown();
            }
        });

        // Close modal with Escape key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                this.closeScoringBreakdown();
            }
        });
    }

    // Close the scoring breakdown modal
    closeScoringBreakdown() {
        const modal = document.getElementById('scoringBreakdownModal');
        if (modal) {
            modal.remove();
        }
    }

    // Show Learn More modal with comprehensive deal information
    async showDealDetails(dealId) {
        console.log('🔍 showDealDetails called with dealId:', dealId);
        
        const deal = this.deals.find(d => d.id === dealId);
        if (!deal) {
            console.error('❌ Deal not found:', dealId);
            console.log('Available deals:', this.deals.map(d => d.id));
            return;
        }

        console.log('✅ Deal found:', deal);
        console.log('📋 Rendering Learn More modal...');
        
        // Track that the Learn More modal was opened (this is just an interaction, not a status change)
        this.trackDealInteraction(dealId, 'Opened Modal', 'Learn More modal opened to view deal details').catch(console.error);
        
        // Check if this is the first time viewing this deal (Brand View Count = 1)
        // This should trigger the status change from "New" to "Viewed by Brand"
        if (deal.airtableData && this.isBrandNewDeal(deal.airtableData, this.userId)) {
            console.log(`📊 First time viewing deal: ${deal.propertyName} - tracking status change to "Viewed by Brand"`);
            
            // Track the first view (Brand View Count = 1)
            // This triggers the transition from "New" to "Viewed by Brand"
            this.trackDealInteraction(dealId, 'Viewed', 'Deal first viewed by brand user via Learn More modal').catch(console.error);
            this.trackStatusChange(dealId, 'New', 'Viewed by Brand', 'Deal first viewed by brand user via Learn More modal', 'Automatic').catch(console.error);
            
            // IMMEDIATELY update the local deal status so UI reflects the change instantly
            deal.status = 'viewed-by-brand';
            console.log(`✅ Updated local deal status to: ${deal.status}`);
        }
        
        await this.renderLearnMoreModal(deal);
    }

    // Show match score details modal
    async showMatchScoreDetails(dealId) {
        const deal = this.deals.find(d => d.id === dealId);
        if (!deal) {
            console.error('Deal not found:', dealId);
            return;
        }

        console.log(`📊 Showing match score details for deal: ${deal.headline}`);
        console.log(`📊 Current displayed score: ${deal.matchScore}`);

        // Try to get fresh brand data, but if it fails, use mock data for demonstration
        let subscores = {};
        let hasHardFail = false;
        
        // Get the brand name from the selected brand dropdown (if available) or fall back to deal's brand match
        const brandSelector = document.getElementById('brandSelector');
        const selectedBrand = brandSelector?.value || this.brandId;
        const brandNameToUse = selectedBrand || deal.brandMatch || 'Not specified';
        console.log(`🔍 Using brand for scoring: "${brandNameToUse}" (from ${selectedBrand ? 'dropdown selection' : 'deal brand match'})`);
        
        const brandData = await this.getBrandDataForScoring(brandNameToUse);
        if (!brandData) {
            alert(`❌ Brand data not available for "${brandNameToUse}". Please ensure the brand exists in the Brand Basics table.`);
            console.error('Brand data not available for scoring:', brandNameToUse);
            return;
        }

        console.log('✅ Using real brand data for scoring breakdown');
        // Cache brand data for use in commentary
        this.cachedBrandData = brandData;
        
        // Calculate detailed breakdown with real brand data
        subscores.MKT1 = await this.calculateMKT1(deal.airtableData, deal.locationData, brandData);
        subscores.MKT2 = await this.calculateMKT2(deal.airtableData, deal.locationData, brandData);
        subscores.SEG1 = await this.calculateSEG1(deal.airtableData, deal.locationData, brandData);
        subscores.SVC1 = await this.calculateSVC1(deal.airtableData, deal.locationData, brandData);
        subscores.SIZE1 = await this.calculateSIZE1(deal.airtableData, deal.locationData, brandData);
        subscores.OWN1 = await this.calculateOWN1(deal.airtableData, deal.locationData, brandData, deal.marketPerformanceData);
        subscores.AMN1 = await this.calculateAMN1(deal.airtableData, deal.locationData, brandData);
        subscores.FIN1 = await this.calculateFIN1(deal.airtableData, deal.locationData, brandData, deal.marketPerformanceData || {});
        subscores.INC1 = await this.calculateINC1(deal.airtableData, deal.locationData, brandData, deal.marketPerformanceData || {});
        subscores.STR1 = await this.calculateSTR1(deal.airtableData, deal.locationData, brandData, deal.strategicIntentData || {});
        subscores.PREF1 = await this.calculatePREF1(deal.airtableData, deal.locationData, brandData, deal.strategicIntentData || {});
        subscores.PROJ1 = await this.calculatePROJ1(deal.airtableData, deal.locationData, brandData);
        subscores.PROJ2 = await this.calculatePROJ2(deal.airtableData, deal.locationData, brandData);
        
        hasHardFail = this.hasHardFail(deal.airtableData, deal.locationData, brandData);

        const weights = {
            MKT1: 14, MKT2: 8, SEG1: 10, SVC1: 8, SIZE1: 12,
            OWN1: 4, AMN1: 10, FIN1: 10, INC1: 10, STR1: 4, PREF1: 10, PROJ1: 15, PROJ2: 10
        };

        // Use the displayed score as the actual score
        const actualScore = deal.matchScore;

        // Render modal
        const modalHtml = this.renderMatchScoreModal(deal, actualScore, subscores, weights, hasHardFail);
        document.body.insertAdjacentHTML('beforeend', modalHtml);
        this.setupMatchScoreModalEventListeners();
    }


    // Render match score details modal
    renderMatchScoreModal(deal, score, subscores, weights, hasHardFail) {
        const scoreClass = this.getScoreClass(score);
        const factorDescriptions = {
            MKT1: 'Priority Market Fit - Does the deal location match your priority markets?',
            MKT2: 'Recognition Density - Do you have enough hotels in the region for brand recognition?',
            SEG1: 'Chain Scale Proximity - How close is the deal\'s chain scale to your brand?',
            SVC1: 'Service Model Alignment - Does the service model match your brand?',
            SIZE1: 'Ideal Room Range - Does the room count fit your ideal size range?',
            OWN1: 'Owner/Investor Type - Does the owner type align with your preferences?',
            AMN1: 'Required Standards - Does the property meet your amenity standards?',
            FIN1: 'Fees Tolerance - Can the owner afford your fees?',
            INC1: 'Incentives Match - Do the incentives align with your offerings?',
            STR1: 'Strategic Brand Model - Does the strategic model match your approach?',
            PREF1: 'Preferred Brand Bonus - Is your brand in their preferred brands list?',
            PROJ1: 'Project Type Compatibility - Does the project type match your acceptable criteria?',
            PROJ2: 'Building Type Compatibility - Does the building type match your acceptable criteria?'
        };

        const factorHtml = Object.entries(subscores).map(([key, value]) => {
            const weight = weights[key];
            const description = factorDescriptions[key] || '';
            const factorClass = value >= 80 ? 'high' : value >= 60 ? 'medium' : 'low';
            
            // 🔍 DEBUGGING: Show what score is being passed to display
            if (key === 'STR1') {
                console.log('🔍 ===== STR1 DISPLAY SCORE DEBUGGING =====');
                console.log('  - subscores.STR1 value:', value);
                console.log('  - factorClass:', factorClass);
                console.log('  - Full subscores object:', subscores);
            }
            
            const commentary = this.getScoreCommentary(key, value, deal);
            
            return `
                <div class="score-factor ${factorClass}">
                    <div class="factor-info">
                        <div class="factor-name">${key}</div>
                        <div class="factor-description">${description}</div>
                        <div class="factor-commentary">${commentary}</div>
                    </div>
                    <div class="factor-score">
                        <div class="factor-value">${value || 0}</div>
                        <div class="factor-weight">(${weight}pts)</div>
                    </div>
                </div>
            `;
        }).join('');

        const hardFailHtml = hasHardFail ? `
            <div class="score-factor low">
                <div class="factor-info">
                    <div class="factor-name">Hard Fail</div>
                    <div class="factor-description">This deal is in a market you want to avoid</div>
                </div>
                <div class="factor-score">
                    <div class="factor-value">0</div>
                    <div class="factor-weight">(Override)</div>
                </div>
            </div>
        ` : '';

        return `
            <div class="match-score-modal" id="matchScoreModal">
                <div class="match-score-content">
                    <div class="match-score-header">
                        <h3 class="match-score-title">Match Score Breakdown</h3>
                        <button class="match-score-close" onclick="dashboard.closeMatchScoreModal()">
                            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <line x1="18" y1="6" x2="6" y2="18"></line>
                                <line x1="6" y1="6" x2="18" y2="18"></line>
                            </svg>
                        </button>
                    </div>
                    
                    <div class="match-score-overview">
                        <div class="match-score-badge ${scoreClass}">${score}</div>
                        <div class="match-score-info">
                            <div class="match-score-value">${deal.headline}</div>
                            <div class="match-score-description">
                                ${hasHardFail ? 'Hard Fail - Market to Avoid' : 
                                  score >= 80 ? 'Excellent Match' : 
                                  score >= 60 ? 'Good Match' : 
                                  'Poor Match'}
                            </div>
                        </div>
                    </div>
                    
                    <div class="match-score-breakdown">
                        <h4 class="breakdown-title">Score Breakdown</h4>
                        ${hardFailHtml}
                        ${factorHtml}
                    </div>
                </div>
            </div>
        `;
    }

    // Get detailed commentary for score improvements with actual data and calculations
    getScoreCommentary(factor, score, deal) {
        const dealFields = deal.airtableData || deal.fields;
        const locationData = deal.locationData;
        
        // Get brand data for comparison - use the brand data from the deal if available, otherwise fetch it
        let brandData = deal.brandData;
        if (!brandData) {
            // If deal doesn't have brand data, try to get it from cache or fetch it
            brandData = this.cachedBrandData || {};
            console.log('🔍 getScoreCommentary - No brand data in deal, using cached:', brandData);
        } else {
            console.log('🔍 getScoreCommentary - Using brand data from deal:', brandData);
        }
        
        const commentaries = {
            MKT1: () => {
                const city = locationData?.['City'] || 'Unknown';
                const country = locationData?.['Country'] || 'Unknown';
                // Get priority markets from Brand Setup - Project Fit table (same as calculation logic)
                const brandFit = brandData?.brandFit || {};
                console.log('🔍 MKT1 Commentary - brandData:', brandData);
                console.log('🔍 MKT1 Commentary - brandFit:', brandFit);
                const marketsToAvoid = (brandData?.brandBasics || {})['Markets to Avoid or Saturated'] || [];
                
                // Extract priority markets from Project Fit table columns
                const projectFitColumns = [
                    'Global - Priority Markets',
                    'United States - Priority Markets',
                    'Canada - Priority Markets',
                    'Latin America - Priority Markets',
                    'Middle East - Priority Markets', 
                    'Western Europe - Priority Markets',
                    'Eastern Europe - Priority Markets',
                    'Southern Europe - Priority Markets',
                    'Northern Europe - Priority Markets',
                    'Other - Priority Markets'
                ];
                
                const priorityMarkets = [];
                for (const column of projectFitColumns) {
                    if (brandFit[column] === true || brandFit[column] === 'Yes') {
                        const regionName = column.replace(' - Priority Markets', '');
                        priorityMarkets.push(regionName);
                    }
                }
                
                const brandPriorityMarkets = priorityMarkets.join(', ') || 'Not specified';
                const brandMarketsToAvoid = marketsToAvoid.join(', ') || 'None specified';
                
                // Get country-to-region mapping for detailed explanation
                const countryMapping = this.getCountryRegionMapping();
                const dealRegions = countryMapping[country] || { region1: '', region2: '', region3: '', region: 'Global' };
                const dealRegionsList = [country, dealRegions.region1, dealRegions.region2, dealRegions.region3].filter(r => r && r.trim() !== '');
                
                let status = '';
                let calculation = '';
                let matchDetails = '';
                
                if (score === 0) {
                    // Check if it's a hard fail
                    const isHardFail = marketsToAvoid.some(market => 
                        dealRegionsList.some(region => 
                            market.toLowerCase().includes(region.toLowerCase()) ||
                            region.toLowerCase().includes(market.toLowerCase())
                        )
                    );
                    
                    if (isHardFail) {
                        status = `❌ Hard fail: ${city}, ${country} is in markets to avoid.`;
                        calculation = `Calculation: Deal location appears in brand's markets to avoid = 0 points (HARD FAIL)`;
                        matchDetails = `🚫 Markets to Avoid: ${brandMarketsToAvoid}`;
                } else {
                    status = `❌ ${city}, ${country} is not a priority market.`;
                        calculation = `Calculation: No match with priority markets = 0 points`;
                    }
                } else if (score === 100) {
                    // Check if it's a global brand
                    const isGlobalBrand = priorityMarkets.some(market => market.toLowerCase().includes('global'));
                    
                    if (isGlobalBrand) {
                        status = `✅ Global brand: ${city}, ${country} is acceptable for this global brand.`;
                        calculation = `Calculation: Brand operates globally = 100 points`;
                    } else {
                        status = `✅ Perfect match: ${city}, ${country} is a priority market.`;
                        calculation = `Calculation: Country-level match with priority markets = 100 points`;
                    }
                } else if (score === 90) {
                    status = `✅ Strong match: ${city}, ${country} aligns with brand's regional priorities.`;
                    calculation = `Calculation: Region_1 level match = 90 points`;
                } else if (score === 80) {
                    status = `✅ Good match: ${city}, ${country} fits within brand's broader regional strategy.`;
                    calculation = `Calculation: Region_2/3 level match = 80 points`;
                } else {
                    status = `⚠️ ${city}, ${country} has limited market priority.`;
                    calculation = `Calculation: Partial match = ${score} points`;
                }
                
                return `${status}\n📊 Deal Location: ${city}, ${country}\n🗺️ Deal Regions: ${dealRegionsList.join(', ')}\n🏢 Brand Priority Markets: ${brandPriorityMarkets}\n${matchDetails}\n🧮 ${calculation}`;
            },
            
            MKT2: () => {
                const city = locationData?.['City'] || 'Unknown';
                const country = locationData?.['Country'] || 'Unknown';
                const brandRecognitionNeed = dealFields?.['Importance of Brand Recognition'] || 0;
                
                // Map deal country to footprint region (same as calculation logic)
                const region = this.mapCountryToRegion(country);
                const brandHotelsInRegion = brandData?.brandFootprint?.[`Number of Open Hotels (${region})`] || 0;
                const threshold = this.getRegionalThreshold(region);
                
                let status = '';
                let calculation = '';
                
                if (score >= 80) {
                    status = `✅ Strong brand presence: ${brandHotelsInRegion} hotels in ${region}.`;
                    calculation = `Calculation: High need + sufficient presence (${brandHotelsInRegion} >= ${threshold}) = ${score} points`;
                } else if (score >= 60) {
                    status = `⚠️ Moderate brand presence in ${region}.`;
                    calculation = `Calculation: ${brandHotelsInRegion} hotels in region = ${score} points`;
                } else {
                    status = `❌ Insufficient brand presence: Only ${brandHotelsInRegion} hotels in ${region}.`;
                    calculation = `Calculation: High need + insufficient presence (${brandHotelsInRegion} < ${threshold}) = ${score} points`;
                }
                
                return `${status}\n📊 Deal Country: ${country} (${region})\n🏨 Brand Hotels in Region: ${brandHotelsInRegion}\n🎯 Owner Recognition Need: ${brandRecognitionNeed}/5\n🧮 ${calculation}`;
            },
            
            SEG1: () => {
                // Get chain scale from location data
                const dealChainScale = locationData?.['Hotel Chain Scale'] || 'Unknown';
                // Get brand's target chain scale from Brand Basics table
                const brandTargetScale = brandData?.brandBasics?.['Hotel Chain Scale'] || 'Unknown';
                
                let status = '';
                let calculation = '';
                
                if (score >= 80) {
                    status = `✅ Perfect chain scale match: ${dealChainScale}.`;
                    calculation = `Calculation: Exact match (${dealChainScale} = ${brandTargetScale}) = ${score} points`;
                } else if (score >= 60) {
                    status = `⚠️ Good chain scale proximity: ${dealChainScale}.`;
                    calculation = `Calculation: Close match (${dealChainScale} vs ${brandTargetScale}) = ${score} points`;
                } else {
                    status = `❌ Poor chain scale match: ${dealChainScale}.`;
                    calculation = `Calculation: No match (${dealChainScale} vs ${brandTargetScale}) = ${score} points`;
                }
                
                return `${status}\n📊 Deal Chain Scale: ${dealChainScale}\n🏢 Brand Target Scale: ${brandTargetScale}\n🧮 ${calculation}`;
            },
            
            SVC1: () => {
                // Get service model from location data (same as calculation logic)
                const dealServiceModel = locationData?.['Hotel Service Model'] || 'Unknown';
                // Get brand's service model from Brand Basics table (same as calculation logic)
                const brandServiceModel = brandData?.brandBasics?.['Hotel Service Model'] || 'Unknown';
                const brandModel = brandData?.brandBasics?.['Brand Model / Format'] || '';
                
                let status = '';
                let calculation = '';
                
                if (score >= 80) {
                    status = `✅ Excellent service model alignment: ${dealServiceModel}.`;
                    calculation = `Calculation: Exact match (${dealServiceModel} = ${brandServiceModel}) = ${score} points`;
                } else if (score >= 60) {
                    status = `⚠️ Good service compatibility: Brand is flexible with ${brandModel} model.`;
                    calculation = `Calculation: Flexible brand match = ${score} points`;
                } else {
                    status = `❌ Poor service model alignment: ${dealServiceModel}.`;
                    calculation = `Calculation: No match (${dealServiceModel} vs ${brandServiceModel}) = ${score} points`;
                }
                
                return `${status}\n📊 Deal Service Model: ${dealServiceModel}\n🏢 Brand Service Model: ${brandServiceModel}\n🧮 ${calculation}`;
            },
            
            SIZE1: () => {
                const dealRoomCount = locationData?.['Total Number of Rooms/Keys'] || 'Unknown';
                // Get room count range from Project Fit data (now directly accessible)
                const projectFitData = brandData?.brandFit;
                
                // Try different possible field names for room ranges
                const brandMinRooms = projectFitData?.['A Min - Ideal Project Size'] || 
                                    projectFitData?.['Min - Ideal Project Size'] || 
                                    projectFitData?.['Minimum Rooms'] || 
                                    projectFitData?.['Min Rooms'] || 
                                    projectFitData?.['Ideal Min Rooms'] || 'Unknown';
                                    
                const brandMaxRooms = projectFitData?.['A Max - Ideal Project Size'] || 
                                    projectFitData?.['Max - Ideal Project Size'] || 
                                    projectFitData?.['Maximum Rooms'] || 
                                    projectFitData?.['Max Rooms'] || 
                                    projectFitData?.['Ideal Max Rooms'] || 'Unknown';
                
                let status = '';
                let calculation = '';
                
                if (score >= 80) {
                    status = `✅ Ideal room count: ${dealRoomCount} rooms fits brand perfectly.`;
                    calculation = `Calculation: ${dealRoomCount} rooms within ideal range (${brandMinRooms}-${brandMaxRooms}) = ${score} points`;
                } else if (score >= 60) {
                    status = `⚠️ Good room count: ${dealRoomCount} rooms is acceptable.`;
                    calculation = `Calculation: ${dealRoomCount} rooms near ideal range (${brandMinRooms}-${brandMaxRooms}) = ${score} points`;
                } else {
                    status = `❌ Room count mismatch: ${dealRoomCount} rooms.`;
                    calculation = `Calculation: ${dealRoomCount} rooms outside ideal range (${brandMinRooms}-${brandMaxRooms}) = ${score} points`;
                }
                
                return `${status}\n📊 Deal Room Count: ${dealRoomCount}\n🏢 Brand Ideal Range: ${brandMinRooms}-${brandMaxRooms}\n🧮 ${calculation}`;
            },
            
            OWN1: () => {
                // Get ownership structure from Market - Performance - Deal & Capital Structure table
                const marketPerformanceData = deal.marketPerformanceData || {};
                const dealOwnerType = marketPerformanceData['Ownership Structure'] || 'Unknown';
                // Get preferred owner types from Brand Setup - Project Fit table
                const brandPreferredOwners = (brandData?.brandFit || {})['Preferred Owner/Investor Type'] || 'Unknown';
                
                // Debug logging for display
                console.log('🔍 OWN1 Display Debug - dealFields keys:', Object.keys(dealFields || {}));
                console.log('🔍 OWN1 Display Debug - marketPerformanceData keys:', Object.keys(marketPerformanceData));
                console.log('🔍 OWN1 Display Debug - dealOwnerType:', dealOwnerType);
                console.log('🔍 OWN1 Display Debug - brandData.brandFit keys:', brandData?.brandFit ? Object.keys(brandData.brandFit) : 'No brand fit');
                console.log('🔍 OWN1 Display Debug - brandPreferredOwners:', brandPreferredOwners);
                
                let status = '';
                let calculation = '';
                
                if (score === 0) {
                    status = `❌ Cannot evaluate: Missing owner type data.`;
                    calculation = `Calculation: No data available (${dealOwnerType} vs ${brandPreferredOwners}) = ${score} points`;
                } else if (score >= 80) {
                    status = `✅ Perfect owner type match: ${dealOwnerType}.`;
                    calculation = `Calculation: Exact match (${dealOwnerType} in ${brandPreferredOwners}) = ${score} points`;
                } else if (score >= 60) {
                    status = `⚠️ Good owner alignment: ${dealOwnerType} is acceptable.`;
                    calculation = `Calculation: Compatible match (${dealOwnerType} vs ${brandPreferredOwners}) = ${score} points`;
                } else {
                    status = `❌ Poor owner type match: ${dealOwnerType}.`;
                    calculation = `Calculation: No match (${dealOwnerType} vs ${brandPreferredOwners}) = ${score} points`;
                }
                
                return `${status}\n📊 Deal Owner Type: ${dealOwnerType}\n🏢 Brand Preferred Owners: ${brandPreferredOwners}\n🧮 ${calculation}`;
            },
            
            AMN1: () => {
                // Debug: Log what dealFields contains
                console.log('🔍 AMN1 DISPLAY DEBUG - dealFields:', dealFields);
                console.log('🔍 AMN1 DISPLAY DEBUG - dealFields keys:', Object.keys(dealFields || {}));
                console.log('🔍 AMN1 DISPLAY DEBUG - Pool value:', dealFields?.['Pool']);
                console.log('🔍 AMN1 DISPLAY DEBUG - Meeting Rooms value:', dealFields?.['Number of Meeting Rooms']);
                
                // Get amenities from Deals table using actual field names
                const amenityList = [];
                
                // Boolean amenities
                if (dealFields?.['Pool']) amenityList.push('Pool');
                if (dealFields?.['Lobby']) amenityList.push('Lobby');
                if (dealFields?.['Co-working or lounge space']) amenityList.push('Co-working Space');
                if (dealFields?.['Bar or Beverage Concept']) amenityList.push('Bar/Beverage');
                if (dealFields?.['Business Center']) amenityList.push('Business Center');
                if (dealFields?.['Pet Amenities']) amenityList.push('Pet Amenities');
                if (dealFields?.['Solar Power']) amenityList.push('Solar Power');
                
                // Numeric amenities
                if (dealFields?.['Number of Meeting Rooms'] > 0) {
                    amenityList.push(`${dealFields['Number of Meeting Rooms']} Meeting Rooms`);
                }
                if (dealFields?.['Number of F&B Outlets'] > 0) {
                    amenityList.push(`${dealFields['Number of F&B Outlets']} F&B Outlets`);
                }
                if (dealFields?.['Number of Parking Spaces'] > 0) {
                    amenityList.push(`${dealFields['Number of Parking Spaces']} Parking Spaces`);
                }
                
                // String amenities
                if (dealFields?.['Meeting Space'] != null && dealFields['Meeting Space'] !== '') {
                    amenityList.push(`Meeting Space: ${dealFields['Meeting Space']}${dealFields['Meeting Space Unit'] ? ' ' + dealFields['Meeting Space Unit'] : ''}`);
                }
                
                console.log('🔍 AMN1 DISPLAY DEBUG - amenityList:', amenityList);
                const dealAmenities = amenityList.join(', ') || 'No amenities specified';
                console.log('🔍 AMN1 DISPLAY DEBUG - final dealAmenities:', dealAmenities);
                // Get brand requirements from Brand Standards table
                const brandStandards = brandData?.brandStandards || {};
                let brandRequiredAmenities = brandStandards['Brand Standards'] || 'No brand data available - cannot determine required amenities';
                
                // Check if we're using a fallback brand
                if (brandRequiredAmenities !== 'No brand data available - cannot determine required amenities' && this.brandId !== 'Quality Inn') {
                    brandRequiredAmenities = `[Using fallback brand: ${this.brandId}] ${brandRequiredAmenities}`;
                }
                
                let status = '';
                let calculation = '';
                
                if (score >= 80) {
                    status = `✅ Property meets all required amenity standards.`;
                    calculation = `Calculation: All required amenities present (${brandRequiredAmenities}) = ${score} points`;
                } else if (score >= 60) {
                    status = `⚠️ Property meets most amenity requirements.`;
                    calculation = `Calculation: Most amenities present (${dealAmenities} vs ${brandRequiredAmenities}) = ${score} points`;
                } else {
                    status = `❌ Property lacks required amenities.`;
                    calculation = `Calculation: Missing key amenities (${dealAmenities} vs ${brandRequiredAmenities}) = ${score} points`;
                }
                
                return `${status}\n📊 Deal Amenities: ${dealAmenities}\n🏢 Brand Required: ${brandRequiredAmenities}\n🧮 ${calculation}`;
            },
            
            FIN1: () => {
                // Get individual deal fee expectations from Market - Performance - Deal & Capital Structure table (same as calculation logic)
                const dealRoyaltyFee = deal?.marketPerformanceData?.['Royalty Fee Expectations'] || '';
                const dealMarketingFee = deal?.marketPerformanceData?.['Marketing Fee Expectations'] || '';
                const dealLoyaltyFee = deal?.marketPerformanceData?.['Loyalty Fee Expectations'] || '';
                
                // Get brand fee structure from Brand Setup - Fee Structure table (same as calculation logic)
                const brandFeeStructure = brandData?.brandFeeStructure || {};
                
                // Parse individual fee expectations (same as calculation logic)
                const dealFees = {
                    royalty: this.parseSingleFee(dealRoyaltyFee),
                    marketing: this.parseSingleFee(dealMarketingFee),
                    loyalty: this.parseSingleFee(dealLoyaltyFee)
                };
                
                // Check if any fees are specified
                const hasAnyFees = Object.values(dealFees).some(fee => fee !== null);
                
                // If no fees are specified, show appropriate message
                if (!hasAnyFees) {
                    let status = '';
                    let calculation = '';
                    
                    if (score >= 50) {
                        status = `⚠️ Fee expectations not specified - neutral assessment.`;
                        calculation = `Calculation: No fee data provided = ${score} points (neutral)`;
                    } else {
                        status = `❌ Fee expectations not specified - unable to assess alignment.`;
                        calculation = `Calculation: No fee data provided = ${score} points`;
                    }
                    
                    return `${status}\n📊 Deal Fee Expectations: Not specified\n🏢 Brand Fee Structure: Available for comparison\n🧮 ${calculation}`;
                }
                
                // Build fee comparison display
                const feeTypes = [
                    { key: 'royalty', minField: 'Min - Typical Royalty Fee Range', maxField: 'Max - Typical Royalty Fee Range', name: 'Royalty', dealValue: dealRoyaltyFee },
                    { key: 'marketing', minField: 'Min - Typical Marketing Fee Range', maxField: 'Max - Typical Marketing Fee Range', name: 'Marketing', dealValue: dealMarketingFee },
                    { key: 'loyalty', minField: 'Min - Typical Loyalty Program Fee', maxField: 'Max - Typical Loyalty Program Fee', name: 'Loyalty', dealValue: dealLoyaltyFee }
                ];
                
                let feeComparison = '';
                let matches = 0;
                let totalFees = 0;
                
                for (const feeType of feeTypes) {
                    const dealFee = dealFees[feeType.key];
                    const brandMinRaw = brandFeeStructure[feeType.minField] || 0;
                    const brandMaxRaw = brandFeeStructure[feeType.maxField] || 0;
                    
                    // Parse brand min/max to remove % symbols and convert to numbers
                    let brandMin = typeof brandMinRaw === 'string' ? parseFloat(brandMinRaw.replace('%', '')) : brandMinRaw;
                    let brandMax = typeof brandMaxRaw === 'string' ? parseFloat(brandMaxRaw.replace('%', '')) : brandMaxRaw;
                    
                    // Check if brand fees are stored as decimals (0.05 = 5%) and convert to percentages for display
                    // If both min and max are less than 1, assume they're stored as decimals
                    if (brandMin < 1 && brandMax < 1) {
                        brandMin = brandMin * 100;
                        brandMax = brandMax * 100;
                    }
                    
                    console.log(`🔍 FIN1 DISPLAY - ${feeType.name}: Deal=${dealFee}, Brand Min=${brandMin} (raw: ${brandMinRaw}), Brand Max=${brandMax} (raw: ${brandMaxRaw})`);
                    
                    if (dealFee !== null && brandMin !== undefined && brandMax !== undefined) {
                        totalFees++;
                        const isMatch = (dealFee >= brandMin && dealFee <= brandMax);
                        console.log(`🔍 FIN1 DISPLAY - ${feeType.name} isMatch: ${isMatch} (dealFee >= brandMin && dealFee <= brandMax: ${dealFee >= brandMin && dealFee <= brandMax})`);
                        if (isMatch) matches++;
                        
                        feeComparison += `\n  • ${feeType.name}: Deal=${dealFee}% vs Brand=${brandMin}-${brandMax}% ${isMatch ? '✅' : '❌'}`;
                    } else if (dealFee === null) {
                        feeComparison += `\n  • ${feeType.name}: Deal=Not specified vs Brand=${brandMin}-${brandMax}% ⚠️`;
                    }
                }
                
                let status = '';
                let calculation = '';
                
                if (score >= 80) {
                    status = `✅ Strong fee alignment between deal expectations and brand requirements.`;
                    calculation = `Calculation: ${matches}/${totalFees} fee types match = ${score} points`;
                } else if (score >= 60) {
                    status = `⚠️ Moderate fee alignment with some flexibility needed.`;
                    calculation = `Calculation: ${matches}/${totalFees} fee types match = ${score} points`;
                } else {
                    status = `❌ Poor fee alignment between deal expectations and brand requirements.`;
                    calculation = `Calculation: ${matches}/${totalFees} fee types match = ${score} points`;
                }
                
                return `${status}\n📊 Deal Fee Expectations: Royalty=${dealRoyaltyFee || 'Not specified'}, Marketing=${dealMarketingFee || 'Not specified'}, Loyalty=${dealLoyaltyFee || 'Not specified'}${feeComparison}\n🧮 ${calculation}`;
            },
            
            INC1: () => {
                // Get brand willingness from Operational Support table (same as calculation logic)
                const brandWillingToNegotiate = (brandData?.brandOperationalSupport || {})['Willing to Negotiate Incentives'] === 'Yes';
                
                // Get individual incentive fields from Operational Support table (same as calculation logic)
                const brandIncentives = brandData?.brandOperationalSupport || {};
                
                // Get individual incentive fields from Market Performance table (same as calculation logic)
                const dealIncentives = deal?.marketPerformanceData || {};
                
                // 🔍 INC1 DISPLAY DEBUGGING
                console.log('🔍 ===== INC1 DISPLAY DEBUGGING =====');
                console.log('brandData exists:', !!brandData);
                console.log('brandData.brandOperationalSupport exists:', !!brandData?.brandOperationalSupport);
                console.log('brandData.brandOperationalSupport keys:', brandData?.brandOperationalSupport ? Object.keys(brandData.brandOperationalSupport) : 'No data');
                console.log('deal.marketPerformanceData exists:', !!deal?.marketPerformanceData);
                console.log('deal.marketPerformanceData keys:', deal?.marketPerformanceData ? Object.keys(deal.marketPerformanceData) : 'No data');
                
                // If Operational Support data is not available, show appropriate message
                if (!brandData?.brandOperationalSupport || Object.keys(brandData.brandOperationalSupport).length === 0) {
                    return `⚠️ No Operational Support data available\n📊 Deal Incentives Sought: ${Object.keys(dealIncentives).filter(key => dealIncentives[key] === true).join(', ') || 'None specified'}\n🏢 Brand Incentives Offered: Data not available\n🧮 Calculation: Neutral score of 50 due to missing data`;
                }
                
                // List of incentive fields to compare (same as calculation logic)
                const incentiveFields = [
                    'Lower Initial Fees',
                    'Tiered Fee Structure', 
                    'Temporary Royalty Discounts',
                    'Performance-Based Royalties',
                    'Performance Bonuses',
                    'Brand Loyalty Rewards',
                    'Shorter Contract Durations',
                    'Termination Flexibility',
                    'Financing Assistance',
                    'Construction or Renovation Grants',
                    'Comprehensive Training Packages',
                    'Ongoing Operational Support',
                    'Co-op Advertising Funds',
                    'Local Marketing Programs',
                    'Technology Upgrades',
                    'Data Analytics Tools',
                    'Protected Territories',
                    'Expansion Incentives',
                    'New Brand Launch Discounts',
                    'Custom Branding Options',
                    'Franchisee Advisory Councils',
                    'Annual Franchisee Conferences',
                    'Insurance Support',
                    'Market Research Data',
                    'Equity Participation Options',
                    'Long-Term Profit Sharing',
                    'Key Money'
                ];
                
                // Count matches between brand and deal incentives
                let matches = 0;
                let totalDealIncentives = 0;
                const dealIncentiveList = [];
                const brandIncentiveList = [];
                
                for (const field of incentiveFields) {
                    const brandOffers = brandIncentives[field] === true || brandIncentives[field] === 'Yes';
                    const dealSeeks = dealIncentives[field] === true || dealIncentives[field] === 'Yes';
                    
                    if (dealSeeks) {
                        totalDealIncentives++;
                        dealIncentiveList.push(field);
                        if (brandOffers) {
                            matches++;
                            brandIncentiveList.push(field);
                        }
                    }
                }
                
                const dealIncentivesDisplay = dealIncentiveList.join(', ') || 'None specified';
                const brandIncentivesDisplay = brandIncentiveList.join(', ') || 'None offered';
                
                let status = '';
                let calculation = '';
                
                if (!brandWillingToNegotiate) {
                    status = `❌ Brand is not willing to negotiate incentives.`;
                    calculation = `Calculation: Brand unwilling to negotiate = 40 points`;
                } else if (score >= 80) {
                    status = `✅ Strong incentive alignment with brand offerings.`;
                    calculation = `Calculation: ${matches}/${totalDealIncentives} incentives matched = ${score} points`;
                } else if (score >= 60) {
                    status = `⚠️ Good incentive compatibility with some flexibility needed.`;
                    calculation = `Calculation: ${matches}/${totalDealIncentives} incentives matched = ${score} points`;
                } else {
                    status = `❌ Poor incentive alignment.`;
                    calculation = `Calculation: ${matches}/${totalDealIncentives} incentives matched = ${score} points`;
                }
                
                return `${status}\n📊 Deal Incentives Sought: ${dealIncentivesDisplay}\n🏢 Brand Incentives Offered: ${brandIncentivesDisplay}\n🧮 ${calculation}`;
            },
            
            STR1: () => {
                // Get brand soft/collection status from Project Fit table (same as calculation logic)
                const brandSoftCollection = (brandData?.brandFit || {})['Soft/Collection Brand'] || 'Unknown';
                // Get deal preference from Strategic Intent table (same as calculation logic)
                const dealPreference = deal?.strategicIntentData?.['Soft vs Hard Brand Preference'] || 'Unknown';
                
                // 🔍 STR1 DISPLAY DEBUGGING
                console.log('🔍 ===== STR1 DISPLAY DEBUGGING =====');
                console.log('  - Received score:', score);
                console.log('  - brandSoftCollection:', brandSoftCollection);
                console.log('  - dealPreference:', dealPreference);
                console.log('  - Score type:', typeof score);
                console.log('  - Score >= 80:', score >= 80);
                console.log('  - Score >= 60:', score >= 60);
                
                // Convert brand data for display
                const brandType = brandSoftCollection.toLowerCase() === 'yes' ? 'Soft Brand' : 
                                 brandSoftCollection.toLowerCase() === 'no' ? 'Hard Brand' : 'Unknown';
                
                let status = '';
                let calculation = '';
                
                if (score >= 80) {
                    status = `✅ Perfect strategic brand model match.`;
                    calculation = `Calculation: Exact strategic alignment (${dealPreference} = ${brandType}) = ${score} points`;
                } else if (score >= 60) {
                    status = `⚠️ Good strategic alignment with some adjustments needed.`;
                    calculation = `Calculation: Compatible strategy (${dealPreference} vs ${brandType}) = ${score} points`;
                } else {
                    status = `❌ Poor strategic brand model match.`;
                    calculation = `Calculation: Misaligned strategy (${dealPreference} vs ${brandType}) = ${score} points`;
                }
                
                return `${status}\n📊 Deal Strategy: ${dealPreference}\n🏢 Brand Model: ${brandType}\n🧮 ${calculation}`;
            },
            
            PREF1: () => {
                // Get preferred brands from Strategic Intent table (same as calculation logic)
                const dealPreferredBrands = deal?.strategicIntentData?.['Preferred Brands'] || 'Not specified';
                const currentBrand = this.brandId || 'Current Brand';
                
                let status = '';
                let calculation = '';
                
                if (score >= 80) {
                    status = `✅ Brand is in the preferred brands list.`;
                    calculation = `Calculation: ${currentBrand} found in preferred list (${dealPreferredBrands}) = ${score} points`;
                } else {
                    status = `❌ Brand not in preferred list.`;
                    calculation = `Calculation: ${currentBrand} not in preferred list (${dealPreferredBrands}) = ${score} points`;
                }
                
                return `${status}\n📊 Deal Preferred Brands: ${dealPreferredBrands}\n🏢 Current Brand: ${currentBrand}\n🧮 ${calculation}`;
            },
            
            PROJ1: () => {
                const dealProjectType = deal?.airtableData?.['Project Type'] || 'Unknown';
                const brandFit = brandData?.brandFit || {};
                
                console.log('🔍 PROJ1 Commentary - brandData:', brandData);
                console.log('🔍 PROJ1 Commentary - brandFit:', brandFit);
                
                // Map deal project types to brand criteria fields
                const projectTypeMapping = {
                    'New Build': ['New Build - Acceptable Project Type'],
                    'Conversion / Reflag': ['Conversion - Reflag - Acceptable Project Type'],
                    'Conversion': ['Conversion - Reflag - Acceptable Project Type'],
                    'Reflag': ['Conversion - Reflag - Acceptable Project Type'],
                    'Renovation': ['Conversion - Reflag - Acceptable Project Type'],
                    'Expansion': ['New Build - Acceptable Project Type']
                };
                
                const criteriaToCheck = projectTypeMapping[dealProjectType] || [];
                const brandAcceptableTypes = [];
                
                // Find all acceptable project types for this brand
                for (const [projectType, criteria] of Object.entries(projectTypeMapping)) {
                    for (const criterion of criteria) {
                        if (brandFit[criterion] === true || brandFit[criterion] === 'Yes' || brandFit[criterion] === 'Acceptable') {
                            brandAcceptableTypes.push(projectType);
                            break;
                        }
                    }
                }
                
                console.log('🔍 PROJ1 Commentary Debug - brandFit:', brandFit);
                console.log('🔍 PROJ1 Commentary Debug - projectTypeMapping:', projectTypeMapping);
                console.log('🔍 PROJ1 Commentary Debug - brandAcceptableTypes:', brandAcceptableTypes);
                
                const brandAcceptableTypesList = brandAcceptableTypes.join(', ') || 'None specified';
                
                let status = '';
                let calculation = '';
                
                if (score === 0) {
                    status = `❌ Project type "${dealProjectType}" is not acceptable for this brand.`;
                    calculation = `Calculation: Deal project type not in brand's acceptable criteria = 0 points`;
                } else if (score === 100) {
                    status = `✅ Project type "${dealProjectType}" is acceptable for this brand.`;
                    calculation = `Calculation: Deal project type matches brand's acceptable criteria = 100 points`;
                } else {
                    status = `⚠️ Project type "${dealProjectType}" has limited compatibility.`;
                    calculation = `Calculation: Partial match = ${score} points`;
                }
                
                return `${status}\n🏗️ Deal Project Type: ${dealProjectType}\n✅ Brand Acceptable Types: ${brandAcceptableTypesList}\n🧮 ${calculation}`;
            },
            
            PROJ2: () => {
                const buildingHeight = deal?.locationData?.['Max height Allowed By Zoning Sq. Meters'] || 'Unknown';
                const totalSiteSize = deal?.locationData?.['Total Site Size Sq. Meters'] || 'Unknown';
                const brandFit = brandData?.brandFit || {};
                
                console.log('🔍 PROJ2 Commentary - brandData:', brandData);
                console.log('🔍 PROJ2 Commentary - brandFit:', brandFit);
                
                // Map building characteristics to brand criteria fields (exact Airtable field names)
                const buildingTypeMapping = {
                    'High-Rise': ['High-Rise - Acceptable Building Type'],
                    'Historic / Renovated': ['Historic / Renovated - Acceptable Building Type'],
                    'Low-Rise': ['Low-Rise - Acceptable Building Type'],
                    'Mid-Rise': ['Mid-Rise - Acceptable Building Type'],
                    'Mixed-Use': ['Mixed-Use - Acceptable Building Type'],
                    'Podium / Tower': ['Podium / Tower - Acceptable Building Type'],
                    'Resort-Style Compound': ['Resort-Style Compound - Acceptable Building Type']
                };
                
                // Get building type directly from Location & Property table
                let buildingType = locationData?.['Building Type'] || 'Unknown';
                
                console.log('🏢 PROJ2 Commentary - Building Type from Location & Property table:', buildingType);
                
                const brandAcceptableTypes = [];
                
                // Find all acceptable building types for this brand
                for (const [type, criteria] of Object.entries(buildingTypeMapping)) {
                    for (const criterion of criteria) {
                        if (brandFit[criterion] === true || brandFit[criterion] === 'Yes' || brandFit[criterion] === 'Acceptable') {
                            brandAcceptableTypes.push(type);
                            break;
                        }
                    }
                }
                
                console.log('🔍 PROJ2 Commentary Debug - brandFit:', brandFit);
                console.log('🔍 PROJ2 Commentary Debug - buildingTypeMapping:', buildingTypeMapping);
                console.log('🔍 PROJ2 Commentary Debug - brandAcceptableTypes:', brandAcceptableTypes);
                
                const brandAcceptableTypesList = brandAcceptableTypes.join(', ') || 'None specified';
                
                let status = '';
                let calculation = '';
                
                if (score === 0) {
                    status = `❌ Building type "${buildingType}" is not acceptable for this brand.`;
                    calculation = `Calculation: Deal building type not in brand's acceptable criteria = 0 points`;
                } else if (score === 100) {
                    status = `✅ Building type "${buildingType}" is acceptable for this brand.`;
                    calculation = `Calculation: Deal building type matches brand's acceptable criteria = 100 points`;
                } else {
                    status = `⚠️ Building type "${buildingType}" has limited compatibility.`;
                    calculation = `Calculation: Partial match = ${score} points`;
                }
                
                return `${status}\n🏢 Deal Building Type: ${buildingType}\n✅ Brand Acceptable Types: ${brandAcceptableTypesList}\n🧮 ${calculation}`;
            }
        };
        
        const commentaryFunction = commentaries[factor];
        return commentaryFunction ? commentaryFunction() : 'Score calculated based on brand criteria.';
    }

    // Setup event listeners for match score modal
    setupMatchScoreModalEventListeners() {
        const modal = document.getElementById('matchScoreModal');
        if (!modal) return;

        // Close on background click
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                this.closeMatchScoreModal();
            }
        });

        // Close on escape key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && modal.style.display !== 'none') {
                this.closeMatchScoreModal();
            }
        });
    }

    // Close match score modal
    closeMatchScoreModal() {
        const modal = document.getElementById('matchScoreModal');
        if (modal) {
            modal.remove();
        }
    }

    // Render the Learn More modal with all deal information
    async renderLearnMoreModal(deal) {
        console.log('🎨 renderLearnMoreModal called for deal:', deal.propertyName);
        
        // Load contact image lazily when modal is opened
        this.loadContactImageLazily(deal);
        
        // Get comprehensive data from all related tables
        const dealData = deal.airtableData || {};
        const locationData = deal.locationData || {};
        const contactData = deal.contactData || {};
        
        // Fetch additional table data if available (specified tables only)
        const marketPerformanceData = deal.marketPerformanceData || {};
        const strategicIntentData = deal.strategicIntentData || {};
        const hotelOwnershipData = deal.hotelOwnershipData || {};
        const companyProfileData = deal.companyProfileData || {};
        
        
        const modalHtml = `
            <div class="learn-more-modal" id="learnMoreModal">
                <div class="learn-more-content-compact">
                    <div class="deal-capture-header-compact">
                        <div class="header-left-compact">
                            <div class="deal-capture-logo-compact">
                                <div class="logo-circle-compact">
                                    <img src="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/69c166836c109719f94e055e_Dealality%20Logo%20(4)%20(1).png" alt="Dealality Logo" class="logo-image-compact">
                                </div>
                                <div class="logo-text-compact">
                                    <div class="logo-line-1-compact">DEAL</div>
                                    <div class="logo-line-2-compact">CAPTURE™</div>
                                </div>
                            </div>
                        </div>
                        <div class="header-center-compact">
                            <div class="hotel-name-compact" style="font-size: 18px; font-weight: 600; color: white; margin-bottom: 4px;">${deal.propertyName || dealData['Property Name'] || 'Hotel Name'}</div>
                            <div class="brand-name-compact">${deal.currentBrand || dealData['Current Brand Affiliation'] || 'BRAND'}</div>
                            <div class="property-location-compact">${deal.city || locationData['City'] || 'City'}, ${deal.country || locationData['Country'] || 'Country'}</div>
                        </div>
                        <div class="header-right-compact">
                            <div class="room-info-compact">${deal.rooms || locationData['Total Number of Rooms/Keys'] || 'N/A'} Rooms</div>
                            <button class="learn-more-close-compact" onclick="console.log('Close button clicked'); dashboard.closeLearnMoreModal('${deal.id}').catch(console.error);">&times;</button>
                        </div>
                    </div>
                    
                    <!-- Action Buttons at Top -->
                    <div class="deal-actions-top">
                        <button class="deal-action-btn-top pursue" onclick="dashboard.approveDeal('${deal.id}')" data-deal-id="${deal.id}">
                            ✅ Pursue
                        </button>
                        <button class="deal-action-btn-top decline" onclick="dashboard.declineDeal('${deal.id}')" data-deal-id="${deal.id}">
                            ❌ Decline
                        </button>
                        <button class="deal-action-btn-top close" onclick="dashboard.closeLearnMoreModal('${deal.id}').catch(console.error);" data-deal-id="${deal.id}">
                            Close
                        </button>
                    </div>
                    
                    <!-- Comprehensive Grid Layout -->
                    <div class="modal-grid-compact">
                        <!-- Column 1: Deal Information -->
                        <div class="modal-column-compact">
                            <div class="deal-section-compact">
                                <h3 class="section-title-compact">📋 Deal Info</h3>
                                ${Object.entries(dealData).filter(([key, value]) => 
                                    !['Deal_ID', 'Last Modified', 'Record_ID'].includes(key)
                                ).map(([key, value]) => `
                                <div class="compact-field">
                                        <span class="field-label-compact">${key}:</span>
                                        <span class="field-value-compact">${Array.isArray(value) ? value.join(', ') : (value || 'N/A')}</span>
                                </div>
                                `).join('')}
                            </div>
                        </div>

                        <!-- Column 2: Location & Property -->
                        <div class="modal-column-compact">
                            <div class="deal-section-compact">
                                <h3 class="section-title-compact">📍 Location</h3>
                                ${Object.entries(locationData || {}).filter(([key, value]) => 
                                    !['Location_Property_ID', 'Deal_ID', 'Last Modified', 'Record_ID'].includes(key)
                                ).map(([key, value]) => `
                                <div class="compact-field">
                                        <span class="field-label-compact">${key}:</span>
                                        <span class="field-value-compact">${Array.isArray(value) ? value.join(', ') : (value || 'N/A')}</span>
                                </div>
                                `).join('')}
                            </div>
                        </div>

                        <!-- Column 3: Contact & Uploads -->
                        <div class="modal-column-compact">
                            <div class="deal-section-compact">
                                <h3 class="section-title-compact">👤 Contact Info</h3>
                                <div class="compact-field">
                                    <span class="field-label-compact">Contact Name:</span>
                                    <span class="field-value-compact">${contactData['Main Contact Name'] || 'N/A'}</span>
                                </div>
                                <div class="compact-field">
                                    <span class="field-label-compact">Title:</span>
                                    <span class="field-value-compact">${contactData['Main Contact Title'] || 'N/A'}</span>
                                </div>
                                <div class="compact-field">
                                    <span class="field-label-compact">Company:</span>
                                    <span class="field-value-compact">${contactData['Entity or Company Name'] || 'N/A'}</span>
                                </div>
                                <div class="compact-field">
                                    <span class="field-label-compact">HQ Location:</span>
                                    <span class="field-value-compact">${contactData['Company HQ Location'] || 'N/A'}</span>
                                </div>
                                <div class="compact-field">
                                    <span class="field-label-compact">Email:</span>
                                    <span class="field-value-compact">${contactData['Email Address'] || 'N/A'}</span>
                                </div>
                                <div class="compact-field">
                                    <span class="field-label-compact">Filter brands without key money?:</span>
                                    <span class="field-value-compact">${contactData['Would you like to filter out brands without key money?'] || 'N/A'}</span>
                                </div>
                                <div class="compact-field">
                                    <span class="field-label-compact">Legal Support Needed?:</span>
                                    <span class="field-value-compact">${contactData['Legal Support Needed?'] || 'N/A'}</span>
                                </div>
                                <div class="compact-field">
                                    <span class="field-label-compact">Proposal Deadline:</span>
                                    <span class="field-value-compact">${contactData['Proposal Deadline'] || 'N/A'}</span>
                                </div>
                                <div class="compact-field">
                                    <span class="field-label-compact">Meet consultants?:</span>
                                    <span class="field-value-compact">${contactData['Would you like to meet consultants?'] || 'N/A'}</span>
                                </div>
                                <div class="compact-field">
                                    <span class="field-label-compact">Financial Model Available?:</span>
                                    <span class="field-value-compact">${contactData['Financial Model Available?'] || 'N/A'}</span>
                                </div>
                                <div class="compact-field">
                                    <span class="field-label-compact">Receive regular updates?:</span>
                                    <span class="field-value-compact">${contactData['Would you like to receive regular updates?'] || 'N/A'}</span>
                                </div>
                                <div class="compact-field">
                                    <span class="field-label-compact">Working with Broker/Advisor?:</span>
                                    <span class="field-value-compact">${contactData['Working with Broker/Advisor?'] || 'N/A'}</span>
                                </div>
                                <div class="compact-field">
                                    <span class="field-label-compact">Other projects nearing expiration?:</span>
                                    <span class="field-value-compact">${contactData['Other Projects Nearing Contract Expiration?'] || 'N/A'}</span>
                                </div>
                                <div class="compact-field">
                                    <span class="field-label-compact">Additional Notes:</span>
                                    <span class="field-value-compact">${contactData['Additional Notes or Unique Project Aspects'] || 'N/A'}</span>
                                </div>
                                <div class="compact-field">
                                    <span class="field-label-compact">What makes this opportunity stand out?:</span>
                                    <span class="field-value-compact">${contactData['What makes this opportunity stand out to a brand or operator?'] || 'N/A'}</span>
                                </div>
                                <div class="compact-field">
                                    <span class="field-label-compact">Anything else to add?:</span>
                                    <span class="field-value-compact">${contactData['Anything else you\'d like to add?'] || 'N/A'}</span>
                                </div>
                            </div>
                        </div>

                        <!-- Column 4: Market Performance -->
                        <div class="modal-column-compact">
                            <div class="deal-section-compact">
                                <h3 class="section-title-compact">📊 Market</h3>
                                ${Object.entries(marketPerformanceData || {}).filter(([key, value]) => 
                                    !['Record_ID', 'Last Modified'].includes(key)
                                ).map(([key, value]) => `
                                <div class="compact-field">
                                    <span class="field-label-compact">${key}:</span>
                                    <span class="field-value-compact">${Array.isArray(value) ? value.join(', ') : (value || 'N/A')}</span>
                                </div>
                                `).join('')}
                                </div>
                                </div>

                        <!-- Column 5: Strategic Intent -->
                        <div class="modal-column-compact">
                            <div class="deal-section-compact">
                                <h3 class="section-title-compact">🎯 Strategy</h3>
                                ${Object.entries(strategicIntentData || {}).filter(([key, value]) => 
                                    !['Record_ID', 'Last Modified'].includes(key)
                                ).map(([key, value]) => `
                                <div class="compact-field">
                                    <span class="field-label-compact">${key}:</span>
                                    <span class="field-value-compact">${Array.isArray(value) ? value.join(', ') : (value || 'N/A')}</span>
                                </div>
                                `).join('')}
                                </div>
                                </div>

                    </div>
                </div>
            </div>
        `;
        
        // Add modal to page
        console.log('📝 Adding modal HTML to page...');
        document.body.insertAdjacentHTML('beforeend', modalHtml);
        console.log('✅ Modal added to page. Modal element:', document.getElementById('learnMoreModal'));
        
        // Track that the Learn More modal was opened (non-blocking)
        this.trackDealInteraction(deal.originalDealId || deal.id, 'Opened Modal', 'Learn More modal opened to view deal details', {
            'modal_type': 'deal_details',
            'deal_property': deal.propertyName,
            'brand_evaluated': deal.brandMatch,
            'deal_location': `${deal.city}, ${deal.country}`
        }).catch(console.error);

        // Close modal when clicking outside
        const modal = document.getElementById('learnMoreModal');
        if (modal) {
            modal.addEventListener('click', (e) => {
                if (e.target === modal) {
                    console.log('🔒 Modal clicked outside, closing...');
                    this.closeLearnMoreModal(deal.id).catch(console.error);
                }
            });

            // Close modal with Escape key
            const escapeHandler = (e) => {
                if (e.key === 'Escape') {
                    console.log('🔒 Escape key pressed, closing modal...');
                    this.closeLearnMoreModal(deal.id).catch(console.error);
                    document.removeEventListener('keydown', escapeHandler);
                }
            };
            document.addEventListener('keydown', escapeHandler);
        }
    }

    // Helper function to format checkbox fields
    formatCheckboxFields(data, fieldNames) {
        const selectedFields = fieldNames.filter(fieldName => data[fieldName] === true);
        return selectedFields.length > 0 ? selectedFields.join(', ') : 'Not specified';
    }

    // Helper function to format attachment fields
    formatAttachmentFields(data, fieldNames) {
        const availableAttachments = fieldNames.filter(fieldName => 
            data[fieldName] && Array.isArray(data[fieldName]) && data[fieldName].length > 0
        );
        return availableAttachments.length > 0 ? availableAttachments.join(', ') : 'No attachments available';
    }

    // Helper function to format compact checkbox fields (limit to 2 items)
    formatCompactCheckbox(data, fieldNames) {
        const selectedFields = fieldNames.filter(fieldName => data[fieldName] === true);
        if (selectedFields.length === 0) return 'None';
        if (selectedFields.length <= 2) return selectedFields.join(', ');
        return selectedFields.slice(0, 2).join(', ') + '...';
    }

    // Close the Learn More modal
    async closeLearnMoreModal(dealId = null) {
        console.log('🔒 Closing Learn More modal...');
        const modal = document.getElementById('learnMoreModal');
        if (modal) {
            // Get the deal ID from parameter or from modal data attribute
            if (!dealId) {
                const approveBtn = modal.querySelector('.deal-action-btn.approve');
                if (approveBtn) {
                    dealId = approveBtn.getAttribute('data-deal-id');
                }
            }
            
            // Refresh the UI immediately to show updated deal status
            await this.renderDeals();
            this.updateTabCounts();
            
            modal.remove();
            console.log('✅ Modal closed successfully');
        } else {
            console.log('❌ Modal not found for closing');
        }
    }

    // Approve deal (move to active deals)
    async approveDeal(dealId) {
        try {
            console.log('Approving deal:', dealId);
            
            // Find the deal to get the original deal ID and brand info
            const deal = this.deals.find(d => d.id === dealId);
            if (!deal) {
                console.error('Deal not found:', dealId);
                return;
            }
            
            const originalDealId = deal.originalDealId || dealId;
            const brandEvaluated = deal.brandMatch;
            
            console.log(`Approving deal ${originalDealId} for brand: ${brandEvaluated}`);
            
            // Update the main deal to track approval by adding user to Users Visited
            // This serves as our way to track that the deal has been approved
            const currentUsersVisited = await this.getUsersVisitedForDeal(originalDealId);
            const updatedUsersVisited = [...new Set([...currentUsersVisited, this.userId])];
            
            const updateSuccess = await this.updateDealStatusInAirtable(originalDealId, {
                'Users Visited': updatedUsersVisited
            });
            
            if (updateSuccess) {
                // Track the approval action
                const success = await this.trackDealAction(originalDealId, 'Approve Deal', 'Deal approved by brand developer', 'Success', {
                    'brand_evaluated': brandEvaluated,
                    'approval_reason': 'Deal meets brand criteria',
                    'next_steps': 'Move to active deals pipeline'
                });
                
                if (success) {
                    // Show success message immediately
                    alert('✅ Deal approved successfully! It has been moved to Active Deals.');
                    
                    // Close modal instantly
                    await this.closeLearnMoreModal(dealId);
                    
                    // Track additional actions in background (non-blocking)
                    this.trackStatusChange(dealId, 'Viewed by Brand', 'Approved', 'Deal approved by brand developer', 'Manual').catch(console.error);
                    this.trackDealInteraction(dealId, 'Approved', 'Deal approved and moved to active pipeline').catch(console.error);
                    
                    // Refresh dashboard in background (non-blocking)
                    this.loadDeals().then(async () => {
                        await this.renderDeals();
                        this.updateTabCounts();
                    }).catch(console.error);
                } else {
                    alert('❌ Failed to track approval action. Please try again.');
                }
            } else {
                alert('❌ Failed to update deal status in Airtable. Please try again.');
            }
        } catch (error) {
            console.error('Error approving deal:', error);
            alert('❌ Error approving deal. Please try again.');
        }
    }

    // Decline deal (move to archives)
    async declineDeal(dealId) {
        try {
            console.log('Declining deal:', dealId);
            
            // Find the deal to get the original deal ID and brand info
            const deal = this.deals.find(d => d.id === dealId);
            if (!deal) {
                console.error('Deal not found:', dealId);
                return;
            }
            
            const originalDealId = deal.originalDealId || dealId;
            const brandEvaluated = deal.brandMatch;
            
            console.log(`Declining deal ${originalDealId} for brand: ${brandEvaluated}`);
            
            // For declined deals, we'll track this in the interaction tables only
            // since the Deal Status field doesn't have a "Declined" option
            const updateSuccess = true; // Skip Airtable update for declined deals
            
            if (updateSuccess) {
                // Track the decline action
                const success = await this.trackDealAction(originalDealId, 'Decline Deal', 'Deal declined by brand developer', 'Success', {
                    'brand_evaluated': brandEvaluated,
                    'decline_reason': 'Deal does not meet brand criteria',
                    'next_steps': 'Move to archives'
                });
                
                if (success) {
                    // Show success message immediately
                    alert('❌ Deal declined successfully! It has been moved to Archives.');
                    
                    // Close modal instantly
                    await this.closeLearnMoreModal(dealId);
                    
                    // Track additional actions in background (non-blocking)
                    this.trackStatusChange(dealId, 'Viewed by Brand', 'Declined', 'Deal declined by brand developer', 'Manual').catch(console.error);
                    this.trackDealInteraction(dealId, 'Declined', 'Deal declined and moved to archives').catch(console.error);
                    
                    // Refresh dashboard in background (non-blocking)
                    this.loadDeals().then(async () => {
                        await this.renderDeals();
                        this.updateTabCounts();
                    }).catch(console.error);
                } else {
                    alert('❌ Failed to track decline action. Please try again.');
                }
            } else {
                alert('❌ Failed to update deal status in Airtable. Please try again.');
            }
        } catch (error) {
            console.error('Error declining deal:', error);
            alert('❌ Error declining deal. Please try again.');
        }
    }

    // Lazy load contact image when deal is viewed
    async loadContactImageLazily(deal) {
        if (!deal.contactImage) {
            try {
                const userImage = await this.getContactImage(deal);
                if (userImage) {
                    // Update the contact image in the table row
                    this.updateContactImage(deal.id, userImage);
                }
            } catch (error) {
                console.error(`Error loading contact image for deal ${deal.id}:`, error);
            }
        }
    }

    // Call to Action Functions
    startChat(dealId) {
        console.log('💬 Starting chat for deal:', dealId);
        // TODO: Implement chat functionality
        alert('Chat functionality will be implemented soon!');
    }

    sendEmail(dealId) {
        console.log('📧 Sending email for deal:', dealId);
        // TODO: Implement email functionality
        alert('Email functionality will be implemented soon!');
    }

    showMoreOptions(dealId) {
        console.log('⚙️ Showing more options for deal:', dealId);
        // TODO: Implement more options menu
        alert('More options will be implemented soon!');
    }

    // Debug function to show amenities fields
    async debugAmenitiesFields() {
        try {
            if (this.deals.length === 0) {
                alert('No deals loaded. Please wait for deals to load first.');
                return;
            }
            
            // Get the first deal to check its fields
            const firstDeal = this.deals[0];
            const dealFields = firstDeal.airtableData || {};
            
            const allFields = Object.keys(dealFields);
            
            // Get the actual field values to see what they contain
            const fieldValues = {};
            allFields.forEach(fieldId => {
                fieldValues[fieldId] = dealFields[fieldId];
            });
            
            const possibleFields = allFields.filter(key => 
                key.toLowerCase().includes('amenit') || 
                key.toLowerCase().includes('facilit') ||
                key.toLowerCase().includes('standard') ||
                key.toLowerCase().includes('feature') ||
                key.toLowerCase().includes('brand') ||
                key.toLowerCase().includes('service')
            );
            
            alert(`AMN1 DEBUG INFO (from first deal):

ALL DEAL FIELDS:
${allFields.join('\n')}

FIELD VALUES:
${Object.entries(fieldValues).map(([key, value]) => `${key}: ${JSON.stringify(value)}`).join('\n')}

POSSIBLE AMENITY FIELDS:
${possibleFields.length > 0 ? possibleFields.join('\n') : 'None found'}

Please tell me which field contains the amenities data.`);
            
        } catch (error) {
            console.error('Error in debugAmenitiesFields:', error);
            alert('Error getting field information: ' + error.message);
        }
    }

    // Debug function to show Brand Setup - Project Fit table fields
    async debugProjectFitFields() {
        try {
            // Clear cache first to force fresh fetch
            console.log('🧹 Clearing brand data cache...');
            if (this.cache && this.cache.brandData && typeof this.cache.brandData.clear === 'function') {
                this.cache.brandData.clear();
                console.log('✅ Brand data cache cleared successfully');
            } else {
                console.log('⚠️ Brand data cache not available or clear method not found');
            }
            
            const brandData = await this.getBrandDataForScoring(this.brandId);
            const brandFit = brandData?.brandFit || {};
            
            let message = '🔍 BRAND SETUP - PROJECT FIT TABLE DEBUGGING\n\n';
            message += `Brand: ${this.brandId}\n\n`;
            
            // Show all brand data keys
            message += 'All brand data keys:\n';
            Object.keys(brandData || {}).forEach(key => {
                message += `- ${key}\n`;
            });
            message += '\n';
            
            if (Object.keys(brandFit).length === 0) {
                message += '❌ No brand fit data found!\n';
                message += 'This could mean:\n';
                message += '1. The brand is not found in Brand Setup - Project Fit table\n';
                message += '2. The linked field is not working\n';
                message += '3. The table ID is incorrect\n';
                message += '4. The brandFit data is not being fetched\n\n';
                
                // Show what we do have
                message += 'Available brand data sections:\n';
                Object.keys(brandData || {}).forEach(key => {
                    const data = brandData[key];
                    if (data && typeof data === 'object') {
                        message += `- ${key}: ${Object.keys(data).length} fields\n`;
                    } else {
                        message += `- ${key}: ${data}\n`;
                    }
                });
            } else {
                message += '✅ Brand fit data found!\n\n';
                message += 'Available fields:\n';
                Object.keys(brandFit).forEach(field => {
                    const value = brandFit[field];
                    message += `- ${field}: ${value}\n`;
                });
                
                message += '\nLooking for these specific fields:\n';
                const projectTypeFields = ['New Build - Acceptable', 'Conversion - Reflag - Acceptable Project Type'];
                const buildingTypeFields = [
                    'Low-Rise - Acceptable Building Type',
                    'Mid-Rise - Acceptable Building Type', 
                    'High-Rise - Acceptable Building Type',
                    'Mixed-Use - Acceptable Building Type',
                    'Podium / Tower - Acceptable Building Type',
                    'Historic / Renovated - Acceptable Building Type',
                    'Resort-Style Compound - Acceptable Building Type'
                ];
                
                projectTypeFields.forEach(field => {
                    const value = brandFit[field];
                    message += `- ${field}: ${value !== undefined ? value : 'NOT FOUND'}\n`;
                });
                
                buildingTypeFields.forEach(field => {
                    const value = brandFit[field];
                    message += `- ${field}: ${value !== undefined ? value : 'NOT FOUND'}\n`;
                });
            }
            
            alert(message);
        } catch (error) {
            alert(`Error debugging project fit fields: ${error.message}`);
        }
    }

    // Debug function to help identify brand data issues
    async debugBrandData() {
        console.warn('debugBrandData is disabled: direct Airtable browser access removed.');
    }

    // Function to allow user to select a different brand
    async selectFallbackBrand() {
        console.warn('selectFallbackBrand is disabled: direct Airtable browser access removed.');
    }

    // Track deal interaction in the interactions table
    async trackDealInteraction(dealId, interactionType, description, additionalData = {}) {
        console.warn('trackDealInteraction is temporarily disabled until backend write endpoint is added.');
        return false;
    }

    // Load and display deal interactions in the modal
    async loadDealInteractions(dealId) {
        const tableBody = document.getElementById('interactionsTableBody');
        if (tableBody) {
            tableBody.innerHTML = '<tr><td colspan="4" style="text-align: center; color: #888;">Interaction history is temporarily unavailable.</td></tr>';
        }
    }

    // Track status change in the status history table
    async trackStatusChange(dealId, previousStatus, newStatus, reason, changeType = 'Manual') {
        console.warn('trackStatusChange is temporarily disabled until backend write endpoint is added.');
        return false;
    }

    // Track when deals become visible to the brand user (Brand View Count logic)
    async trackDealVisibility() {
        try {
            console.log(`📊 Checking visibility for ${this.deals.length} deals...`);
            
            // For each deal that hasn't been viewed yet by this brand user
            for (const deal of this.deals) {
                // Check if this is a new deal that hasn't been viewed
                // Use airtableData which contains the original deal.fields
                if (deal.airtableData && this.isBrandNewDeal(deal.airtableData, this.userId)) {
                    console.log(`📊 Tracking visibility for new deal: ${deal.propertyName}`);
                    
                    // Track the first view (Brand View Count = 1)
                    // This triggers the transition from "New" to "Viewed by Brand"
                    this.trackDealInteraction(deal.originalDealId || deal.id, 'Viewed', 'Deal became visible to brand user in dashboard', {
                        'brand_evaluated': deal.brandMatch
                    }).catch(console.error);
                    this.trackStatusChange(deal.originalDealId || deal.id, 'New', 'Viewed by Brand', 'Deal first viewed by brand user in dashboard', 'Automatic').catch(console.error);
                }
            }
        } catch (error) {
            console.error('Error tracking deal visibility:', error);
        }
    }

    // Get current users visited for a deal
    async getUsersVisitedForDeal(dealId) {
        return [];
    }

    // Update deal status in main Airtable Deals table
    async updateDealStatusInAirtable(dealId, fieldsToUpdate) {
        console.warn('updateDealStatusInAirtable is temporarily disabled until backend write endpoint is added.');
        return false;
    }

    // Track specific action in the actions table
    async trackDealAction(dealId, actionType, description, result = 'Success', actionData = {}) {
        console.warn('trackDealAction is temporarily disabled until backend write endpoint is added.');
        return false;
    }

    async loadDeals() {
        const startTime = performance.now();
        
        try {
            this.showLoading(true, 'Loading deals...', '2-3 seconds');
            await this.showLoadingWave(true);
            
            console.log('🔄 Starting backend deal loading process...');
            this.deals = await this.fetchDealsFromBackend();
            console.log(`✅ Loaded ${this.deals.length} deals from backend`);
            
            // Don't automatically track visibility - only track when deals are actually viewed
            // await this.trackDealVisibility(); // REMOVED: This was marking all deals as "Viewed by Brand"
            
            // Apply current tab filter
            await this.applyTabFilter();
            
            const loadTime = performance.now() - startTime;
            console.log(`🎉 Deal loading completed in ${loadTime.toFixed(2)}ms`);
            
            // Log performance metrics
            this.logPerformanceMetrics();
            
        } catch (error) {
            console.error('❌ Error loading deals:', error);
            this.showError(`Failed to load deals: ${error.message}. Please try again.`);
        } finally {
            this.showLoading(false);
            await this.showLoadingWave(false);
        }
    }

    async fetchDealsFromAirtable() {
        // Security hard-stop: direct browser Airtable calls disabled.
        return [];
    }

    async fetchDealsFromBackend() {
        const apiBase = this.getApiBaseUrl();
        const response = await window.DealalityMemberstackAuth.fetchMyDealsList(`${apiBase}/api/my-deals`);
        if (!response.ok) {
            throw new Error(`Backend API error: ${response.status}`);
        }
        const payload = await response.json();
        const rawDeals = Array.isArray(payload?.deals) ? payload.deals : (Array.isArray(payload) ? payload : []);
        return rawDeals.map((deal) => this.normalizeBackendDeal(deal));
    }

    normalizeBackendDeal(deal) {
        const status = (deal?.status || deal?.dealStatus || deal?.fields?.Status || 'new').toLowerCase();
        const propertyName = deal?.propertyName || deal?.hotelName || deal?.name || deal?.fields?.Name || 'Untitled Deal';
        const brandMatch = deal?.preferredBrand || deal?.brandName || deal?.fields?.['Brand Affiliation'] || '';
        const matchScore = deal?.matchScore || deal?.score || deal?.fields?.['Match Score'] || 0;
        const ownerName = deal?.ownerName || deal?.contactName || deal?.fields?.['Contact Name'] || '';
        const ownerEmail = deal?.ownerEmail || deal?.contactEmail || deal?.fields?.['Contact Email'] || '';

        return {
            ...deal,
            id: deal?.id || deal?.recordId || '',
            status,
            propertyName,
            brandMatch,
            matchScore: typeof matchScore === 'number' ? matchScore : parseInt(matchScore, 10) || 0,
            ownerName,
            ownerEmail,
            airtableData: deal?.fields || deal?.airtableData || {},
            matchScoresNewByBrand: deal?.matchScoresNewByBrand || {},
            matchBreakdownNewDetailsByBrand: deal?.matchBreakdownNewDetailsByBrand || {}
        };
    }

    // Fetch all location data in parallel with caching
    async fetchAllLocationData(locationIds) {
        return {};
    }

    // Fetch all user data in parallel with caching
    async fetchAllUserData(userIds) {
        return {};
    }

    // Fetch all contact data in parallel with caching
    async fetchAllContactData(contactIds) {
        return {};
    }

    async processDealsData(rawDeals) {
        const processingStartTime = performance.now();
        this.performance.dealProcessing.totalDeals = rawDeals.length;
        this.performance.dealProcessing.processedDeals = 0;
        
        console.log(`🚀 Processing ${rawDeals.length} deals with parallel API calls...`);
        
        // Collect unique IDs from specified tables only
        const locationIds = new Set();
        const contactIds = new Set();
        const marketPerformanceIds = new Set();
        const strategicIntentIds = new Set();
        const hotelOwnershipIds = new Set();
        const companyProfileIds = new Set();
        
        rawDeals.forEach(deal => {
            if (deal.fields['Location & Property']?.[0]) {
                locationIds.add(deal.fields['Location & Property'][0]);
            }
            if (deal.fields['Contact & Uploads']?.[0]) {
                contactIds.add(deal.fields['Contact & Uploads'][0]);
            }
            if (deal.fields['Market - Performance - Deal & Capital Structure']?.[0]) {
                marketPerformanceIds.add(deal.fields['Market - Performance - Deal & Capital Structure'][0]);
            }
            if (deal.fields['Strategic Intent - Operational - Key Challenges']?.[0]) {
                strategicIntentIds.add(deal.fields['Strategic Intent - Operational - Key Challenges'][0]);
            }
            if (deal.fields['Hotel Ownership']?.[0]) {
                hotelOwnershipIds.add(deal.fields['Hotel Ownership'][0]);
            }
            if (deal.fields['Company Profile']?.[0]) {
                companyProfileIds.add(deal.fields['Company Profile'][0]);
                console.log('🏢 Found Company Profile ID for deal:', deal.fields['Property Name'], '->', deal.fields['Company Profile'][0]);
            }
        });
        
        console.log(`📊 Fetching data from specified tables in parallel:`);
        console.log(`  - ${locationIds.size} locations`);
        console.log(`  - ${contactIds.size} contacts`);
        console.log(`  - ${marketPerformanceIds.size} market performance records`);
        console.log(`  - ${strategicIntentIds.size} strategic intent records`);
        console.log(`  - ${hotelOwnershipIds.size} hotel ownership records`);
        console.log(`  - ${companyProfileIds.size} company profile records`);
        console.log('🏢 Company Profile IDs collected:', Array.from(companyProfileIds));
        
        // Debug logging only in debug mode
        if (this.performance.debugMode) {
            console.log('🔍 Strategic Intent IDs being collected:', Array.from(strategicIntentIds));
            console.log('🔍 Sample deal Strategic Intent field values:');
            rawDeals.slice(0, 3).forEach((deal, index) => {
                console.log(`  Deal ${index + 1}:`, deal.fields['Strategic Intent - Operational - Key Challenges']);
            });
        }
        
        // Fetch data in parallel from specified tables only
        const [
            locationDataMap, 
            contactDataMap,
            marketPerformanceDataMap,
            strategicIntentDataMap,
            hotelOwnershipDataMap,
            companyProfileDataMap
        ] = await Promise.all([
            this.fetchAllLocationData(Array.from(locationIds)),
            this.fetchAllContactData(Array.from(contactIds)),
            this.fetchAllMarketPerformanceData(Array.from(marketPerformanceIds)),
            this.fetchAllStrategicIntentData(Array.from(strategicIntentIds)),
            this.fetchAllHotelOwnershipData(Array.from(hotelOwnershipIds)),
            this.fetchAllCompanyProfileData(Array.from(companyProfileIds))
        ]);
        
        console.log('✅ All data fetched in parallel from specified tables');
        console.log('🏢 Company Profile data map keys:', Object.keys(companyProfileDataMap));
        console.log('🏢 Company Profile data map:', companyProfileDataMap);
        
        // Update cache timestamp
        this.cache.lastCacheTime = Date.now();
        
        const processedDeals = [];
        
        for (const deal of rawDeals) {
            try {
                // Get location and property data from cache
                const locationData = deal.fields['Location & Property']?.[0] 
                    ? locationDataMap[deal.fields['Location & Property'][0]] 
                    : null;
                
                // Get contact data from cache
                const contactData = deal.fields['Contact & Uploads']?.[0] 
                    ? contactDataMap[deal.fields['Contact & Uploads'][0]] 
                    : null;
                
                // Get additional table data from cache (specified tables only)
                const marketPerformanceData = deal.fields['Market - Performance - Deal & Capital Structure']?.[0] 
                    ? marketPerformanceDataMap[deal.fields['Market - Performance - Deal & Capital Structure'][0]] 
                    : null;
                const strategicIntentData = deal.fields['Strategic Intent - Operational - Key Challenges']?.[0] 
                    ? strategicIntentDataMap[deal.fields['Strategic Intent - Operational - Key Challenges'][0]] 
                    : null;
                
                // Debug logging only in debug mode
                if (this.performance.debugMode) {
                    console.log('🔍 ===== STRATEGIC INTENT DATA RETRIEVAL DEBUGGING =====');
                    console.log('  - Deal ID:', deal.id);
                    console.log('  - Deal Strategic Intent field:', deal.fields['Strategic Intent - Operational - Key Challenges']);
                    console.log('  - Strategic Intent ID:', deal.fields['Strategic Intent - Operational - Key Challenges']?.[0]);
                    console.log('  - strategicIntentDataMap keys:', Object.keys(strategicIntentDataMap));
                    console.log('  - Retrieved strategicIntentData:', strategicIntentData);
                }
                const hotelOwnershipData = deal.fields['Hotel Ownership']?.[0] 
                    ? hotelOwnershipDataMap[deal.fields['Hotel Ownership'][0]] 
                    : null;
                const companyProfileData = deal.fields['Company Profile']?.[0] 
                    ? companyProfileDataMap[deal.fields['Company Profile'][0]] 
                    : null;
                
                // Debug Company Profile data storage
                if (deal.fields['Company Profile']?.[0]) {
                    console.log('🏢 Storing Company Profile data for deal:', deal.fields['Property Name']);
                    console.log('🏢 Company Profile ID:', deal.fields['Company Profile'][0]);
                    console.log('🏢 Company Profile data from map:', companyProfileData);
                }
                // Get the brand name(s) from this deal's Brand Match column
                const dealBrandNames = strategicIntentData?.['Preferred Brands'] || 'Not specified';
                
                // Determine if we have multiple brands
                let brandsToProcess = [];
                if (Array.isArray(dealBrandNames)) {
                    brandsToProcess = dealBrandNames;
                } else if (dealBrandNames && dealBrandNames !== 'Not specified') {
                    brandsToProcess = [dealBrandNames];
                } else {
                    brandsToProcess = ['Not specified'];
                }
                
                console.log(`🔍 Processing deal with ${brandsToProcess.length} brand(s):`, brandsToProcess);
                
                // Process each brand separately - create one row per brand
                for (let i = 0; i < brandsToProcess.length; i++) {
                    const currentBrand = brandsToProcess[i];
                    
                    // Calculate match score against the brand in the Brand Match column (currentBrand)
                    // Only score if the brand exists in the database
                    console.log(`🎯 Deal: ${deal.fields['Property Name']}, Brand Match: ${currentBrand}, this.brandId: ${this.brandId}`);
                    console.log(`🔍 Processing deal for brand: "${currentBrand}"`);
                    const matchScoreResult = await this.calculateRealMatchScore(deal.fields, locationData, currentBrand, marketPerformanceData, strategicIntentData);
                    const matchScore = matchScoreResult.score;
                    const brandExists = matchScoreResult.brandExists;
                    console.log(`🔍 Brand exists result for "${currentBrand}":`, brandExists);
                    
                    // Debug logging for data verification (only in debug mode)
                    if (this.performance.debugMode) {
                    console.log(`🔍 Deal ${deal.id} (Brand Match: ${currentBrand}) data sources:`, {
                    dealId: deal.id,
                        brandMatch: currentBrand,
                    dealFields: Object.keys(deal.fields),
                    strategicIntentLink: deal.fields['Strategic Intent - Operational - Key Challenges'],
                    hasStrategicIntentData: !!strategicIntentData,
                    strategicIntentData: strategicIntentData,
                    strategicIntentFields: strategicIntentData ? Object.keys(strategicIntentData) : [],
                    preferredBrandsRaw: strategicIntentData?.['Preferred Brands'],
                        brandMatch: currentBrand,
                    ownerName: contactData?.['Main Contact Name'],
                    ownerTitle: contactData?.['Main Contact Title'],
                    ownerCompany: contactData?.['Entity or Company Name'],
                    city: locationData?.City,
                    country: locationData?.Country,
                    chainScale: locationData?.['Hotel Chain Scale'],
                    projectType: deal.fields['Project Type'],
                    rooms: locationData?.['Total Number of Rooms/Keys']
                });
                    }
                
                // Calculate response time from user engagement
                const respondTime = this.calculateUserResponseTime(contactData);
                
                // Create processed deal object with real field names
                const processedDeal = {
                        id: `${deal.id}_${i}`, // Unique ID for each brand row
                        originalDealId: deal.id, // Keep reference to original deal
                    dealId: deal.fields['Deal_ID'],
                    propertyName: deal.fields['Property Name'] || 'Unnamed Property',
                        brandMatch: currentBrand,
                    ownerName: contactData?.['Main Contact Name'] || contactData?.['Contact Name'] || deal.fields['Contact Name'] || 'Unknown',
                    ownerTitle: contactData?.['Main Contact Title'] || contactData?.['Contact Title'] || deal.fields['Contact Title'] || 'Property Owner',
                    ownerCompany: contactData?.['Entity or Company Name'] || contactData?.['Company Name'] || deal.fields['Company Name'] || 'Unknown Company',
                    ownerEmail: contactData?.['Email'] || contactData?.['Email Address'] || contactData?.['Main Contact Email'] || contactData?.['Contact Email'] || contactData?.['Primary Email'] || '',
                    ownerPhone: contactData?.['Phone'] || '',
                    propertyType: locationData?.hotelType || deal.fields['Hotel Type'] || 'Unknown',
                    rooms: locationData?.['Total Number of Rooms/Keys'] || 0,
                    budget: this.extractBudgetFromDeal(deal.fields),
                    country: locationData?.Country || 'Unknown',
                    city: this.extractCityFromLocation(locationData),
                    stage: deal.fields['Stage of Development'] || 'Unknown',
                    submitDate: deal.createdTime,
                        status: this.calculateDealStatus(deal.fields, currentBrand) || 'new',
                    matchScore: matchScore,
                    brandNotInDatabase: !brandExists,
                    respondTime: respondTime.text,
                    respondTimeColor: respondTime.color,
                    headline: this.generateDealHeadline(deal.fields, locationData, contactData),
                    description: this.buildDealDescription(deal.fields),
                    timeline: deal.fields['Expected Opening or Rebranding Date'] || '',
                    // Additional valuable information for brand developers
                    chainScale: locationData?.['Hotel Chain Scale'] || deal.fields['Hotel Chain Scale'] || 'Not Specified',
                    expectedOpeningDate: this.formatOpeningDate(deal.fields['Expected Opening or Rebranding Date'] || 
                                                                 deal.fields['Expected Opening Date'] || 
                                                                 deal.fields['Opening Date'] ||
                                                                 deal.fields['Expected Opening/Rebranding Date'] ||
                                                                 deal.fields['Opening/Rebranding Date'] ||
                                                                 deal.fields['Target Opening Date'] ||
                                                                 deal.fields['Projected Opening Date'] || 'Not Specified'),
                    developmentBudget: this.extractDevelopmentBudget(deal.fields),
                    costPerKey: this.calculateCostPerKey(deal.fields, locationData),
                    financingStatus: deal.fields['Financing Status'] || 'Not Specified',
                    permitStatus: deal.fields['Permit Status'] || 'Unknown',
                    competitiveSet: deal.fields['Competitive Set'] || 'Not Available',
                    marketOccupancy: deal.fields['Market Occupancy Rate'] || null,
                    marketADR: deal.fields['Market ADR'] || null,
                    starRatingTarget: deal.fields['Target Star Rating'] || deal.fields['Star Rating'] || null,
                    keyAmenities: this.extractKeyAmenities(deal.fields, locationData),
                    developerExperience: this.extractDeveloperExperience(contactData),
                    previousBrands: deal.fields['Previous Brand Relationships'] || 'Unknown',
                    brandExperience: this.extractBrandExperience(deal.fields),
                    specialConsiderations: this.extractSpecialConsiderations(deal.fields),
                    projectType: deal.fields['Project Type'] || '',
                    currentBrand: deal.fields['Current Brand Affiliation'] || '',
                    parentCompany: deal.fields['Parent Company Name'] || '',
                    // Store contact data for contact information
                    contactData: contactData,
                    // Calculate and store responsiveness badge
                    responsivenessBadge: contactData ? this.calculateCombinedResponsivenessBadge(contactData) : null,
                    // Store original Airtable data for updates
                    airtableData: deal.fields,
                    locationData: locationData,
                    marketPerformanceData: marketPerformanceData,
                    strategicIntentData: strategicIntentData,
                    hotelOwnershipData: hotelOwnershipData,
                    companyProfileData: companyProfileData
                };
                
                processedDeals.push(processedDeal);
                } // End of brand loop
                
            } catch (error) {
                console.error('Error processing deal:', deal.id, error);
            }
        }
        
        // Update performance metrics
        const processingEndTime = performance.now();
        const totalProcessingTime = processingEndTime - processingStartTime;
        this.performance.dealProcessing.totalProcessingTime += totalProcessingTime;
        this.performance.dealProcessing.processedDeals = processedDeals.length;
        this.performance.dealProcessing.averageProcessingTime = 
            this.performance.dealProcessing.totalProcessingTime / this.performance.dealProcessing.processedDeals;
        
        console.log(`✅ Deal processing completed: ${processedDeals.length} deals in ${(totalProcessingTime / 1000).toFixed(2)}s`);
        console.log(`📊 Average processing time per deal: ${this.performance.dealProcessing.averageProcessingTime.toFixed(2)}ms`);
        
        return processedDeals;
    }

    async getLocationData(locationIds) {
        return null;
    }

    async getUserData(userIds) {
        return null;
    }

    async calculateRealMatchScore(dealFields, locationData, dealBrandNames = null, marketPerformanceData = {}, strategicIntentData = {}) {
        console.log('🎯 ===== MATCH SCORE CALCULATION =====');
        console.log('🎯 Deal:', dealFields['Property Name'] || 'Unknown');
        console.log('🎯 Scoring against brand:', dealBrandNames);
        console.log('🎯 this.brandId:', this.brandId);
        console.log('🎯 Deal project type:', dealFields['Project Type']);
        
        // Check if brand exists in database
        const brandExists = await this.checkBrandExists(dealBrandNames);
        if (!brandExists) {
            console.log('🎯 Brand not found in database, returning null score');
            return { score: null, brandExists: false };
        }
        
        // 🔍 COMPREHENSIVE DATA SOURCE DEBUGGING
        console.log('🔍 ===== RAW DATA SOURCES DEBUGGING =====');
        console.log('📊 DEAL FIELDS (from Deals table):', dealFields);
        console.log('📍 LOCATION DATA (from Location table):', locationData);
        console.log('🏢 MARKET PERFORMANCE DATA (from Market - Performance - Deal & Capital Structure table):', marketPerformanceData);
        console.log('🎯 STRATEGIC INTENT DATA (from Strategic Intent - Operational - Key Challenges table):', strategicIntentData);
        
        // 🔍 DEBUGGING: Check if Strategic Intent field exists in deal fields
        console.log('🔍 ===== STRATEGIC INTENT FIELD DEBUGGING =====');
        console.log('  - Deal ID:', dealFields['Deal_ID'] || 'Unknown');
        console.log('  - Strategic Intent field in dealFields:', dealFields['Strategic Intent - Operational - Key Challenges']);
        console.log('  - All deal field keys containing "Strategic":', Object.keys(dealFields).filter(key => key.toLowerCase().includes('strategic')));
        console.log('  - All deal field keys containing "Intent":', Object.keys(dealFields).filter(key => key.toLowerCase().includes('intent')));
        
        // Handle both single brands and arrays of brands
        let brandNamesToTry = [];
        
        if (Array.isArray(dealBrandNames)) {
            // If it's an array, try each brand individually
            brandNamesToTry = dealBrandNames;
            console.log('🔍 Multiple brands found for this deal:', brandNamesToTry);
        } else if (dealBrandNames && dealBrandNames !== 'Not specified') {
            // If it's a single brand
            brandNamesToTry = [dealBrandNames];
            console.log('🔍 Single brand found for this deal:', brandNamesToTry);
        } else {
            // No brand specified - this will be handled by the brandExists check above
            brandNamesToTry = [dealBrandNames];
            console.log('🔍 No brand specified for this deal:', brandNamesToTry);
        }
        
        // Try each brand until we find one that exists in the database
        let brandData = null;
        let foundBrand = null;
        
        for (const brandName of brandNamesToTry) {
            console.log(`🔍 Trying brand: "${brandName}"`);
            brandData = await this.getBrandDataForScoring(brandName);
            if (brandData) {
                foundBrand = brandName;
                console.log(`✅ Found brand data for: "${brandName}"`);
                break;
            }
        }
        
        if (!brandData) {
            console.warn('❌ No brand data available for scoring - setting score to 0');
            console.warn('🔍 Brands tried:', brandNamesToTry);
            return 0; // Score of 0 if no brand data
        }
        
        console.log(`✅ Using brand "${foundBrand}" for scoring`);
        
        // 🔍 BRAND DATA SOURCES DEBUGGING
        console.log('🔍 ===== BRAND DATA SOURCES DEBUGGING =====');
        console.log('🏢 BRAND BASICS (from Brand Setup - Brand Basics table):', brandData.brandBasics);
        console.log('🎯 BRAND FIT (from Brand Setup - Project Fit table):', brandData.brandFit);
        console.log('🏨 BRAND FOOTPRINT (from Brand Setup - Brand Footprint table):', brandData.brandFootprint);
        console.log('📋 BRAND STANDARDS (from Brand Setup - Brand Standards table):', brandData.brandStandards);
        console.log('💰 BRAND TERMS (from Brand Setup - Brand Terms table):', brandData.brandTerms);

        // Calculate individual subscores
        const subscores = {};
        
        // MKT1: Priority Market fit (weight: 14)
        subscores.MKT1 = await this.calculateMKT1(dealFields, locationData, brandData);
        
        // MKT2: Recognition density vs owner need (weight: 8)
        subscores.MKT2 = await this.calculateMKT2(dealFields, locationData, brandData);
        
        // SEG1: Chain scale proximity (weight: 10)
        subscores.SEG1 = await this.calculateSEG1(dealFields, locationData, brandData);
        
        // SVC1: Service model alignment (weight: 8)
        subscores.SVC1 = await this.calculateSVC1(dealFields, locationData, brandData);
        
        // SIZE1: Ideal room-range fit (weight: 12)
        subscores.SIZE1 = await this.calculateSIZE1(dealFields, locationData, brandData);
        
        // OWN1: Owner/Investor type (weight: 4)
        subscores.OWN1 = await this.calculateOWN1(dealFields, locationData, brandData, marketPerformanceData);
        
        // AMN1: Required standards & amenities (weight: 10)
        subscores.AMN1 = await this.calculateAMN1(dealFields, locationData, brandData);
        
        // FIN1: Fees tolerance (weight: 10)
        subscores.FIN1 = await this.calculateFIN1(dealFields, locationData, brandData, marketPerformanceData);
        
        // INC1: Incentives match (weight: 10)
        subscores.INC1 = await this.calculateINC1(dealFields, locationData, brandData, marketPerformanceData);
        
        // STR1: Strategic brand model preference (weight: 4)
        subscores.STR1 = await this.calculateSTR1(dealFields, locationData, brandData, strategicIntentData);
        
        // PREF1: Preferred brand bonus (weight: 10)
        subscores.PREF1 = await this.calculatePREF1(dealFields, locationData, brandData, strategicIntentData);
        
        // PROJ1: Project type compatibility - HARD FAIL if not acceptable
        subscores.PROJ1 = await this.calculatePROJ1(dealFields, locationData, brandData);
        
        // PROJ2: Building type compatibility - HARD FAIL if not acceptable
        subscores.PROJ2 = await this.calculatePROJ2(dealFields, locationData, brandData);
        
        // Calculate weighted total
        const weights = {
            MKT1: 14, MKT2: 8, SEG1: 10, SVC1: 8, SIZE1: 12,
            OWN1: 4, AMN1: 10, FIN1: 10, INC1: 10, STR1: 4, PREF1: 10, PROJ1: 15, PROJ2: 10
        };
        
        let weightedSum = 0;
        let totalWeight = 0;
        
        for (const [key, score] of Object.entries(subscores)) {
            if (score !== null && score !== undefined) {
                weightedSum += score * weights[key];
                totalWeight += weights[key];
            }
        }
        
        const finalScore = totalWeight > 0 ? Math.round(weightedSum / totalWeight) : 0;
        
        // Check for hard fails (KO conditions)
        if (this.hasHardFail(dealFields, locationData, brandData)) {
            return 0; // Hard fail overrides all scoring
        }
        
        // Store detailed scoring breakdown for debugging/analysis
        console.log('Match Score Breakdown:', {
            subscores,
            finalScore,
            weights: Object.keys(weights).reduce((acc, key) => {
                acc[key] = weights[key];
                return acc;
            }, {})
        });
        
        return {
            score: Math.min(100, Math.max(0, finalScore)),
            brandExists: true
        };
    }

    // Check if a brand exists in the Brand Basics database
    async checkBrandExists(brandName) {
        console.log('🔍 checkBrandExists called with:', brandName);
        
        if (!brandName || brandName === 'Not specified') {
            console.log('🔍 Brand is null, empty, or "Not specified" - returning false');
            return false;
        }
        
        try {
            const brandRecord = await this.findBrandByName(brandName);
            const exists = !!brandRecord;
            console.log(`🔍 Brand "${brandName}" exists in database:`, exists);
            if (brandRecord) {
                console.log('🔍 Found brand record:', brandRecord.fields['Brand Name']);
            }
            return exists;
        } catch (error) {
            console.warn(`Error checking if brand "${brandName}" exists:`, error);
            return false;
        }
    }

    // Get brand data for scoring calculations
    async getBrandDataForScoring(brandName = null) {
        try {
            // Use provided brand name or fall back to this.brandId
            const brandNameToUse = brandName || this.brandId;
            console.log(`🔍 Attempting to get brand data for: "${brandNameToUse}"`);
            
            if (!brandNameToUse || brandNameToUse === 'Not specified') {
                console.warn('No valid brand name available for scoring');
                return null;
            }

            // Check cache first for performance
            const cacheKey = `brandData_${brandNameToUse}`;
            if (this.cache.brandData && this.cache.brandData.has(cacheKey)) {
                const cachedData = this.cache.brandData.get(cacheKey);
                const now = Date.now();
                if (now - cachedData.timestamp < this.cache.cacheExpiry) {
                    console.log(`✅ Using cached brand data for: "${brandNameToUse}"`);
                    console.log(`🔍 ===== CACHED BRAND DATA DEBUGGING =====`);
                    console.log('Cached brandOperationalSupport exists:', !!cachedData.data.brandOperationalSupport);
                    console.log('Cached brandOperationalSupport keys:', cachedData.data.brandOperationalSupport ? Object.keys(cachedData.data.brandOperationalSupport) : 'No data');
                    console.log('Cached brandOperationalSupport full object:', cachedData.data.brandOperationalSupport);
                    return cachedData.data;
                } else {
                    // Cache expired, remove it
                    this.cache.brandData.delete(cacheKey);
                }
            }

            // Search for the brand by name in Brand Basics table
            const brandRecord = await this.findBrandByName(brandNameToUse);
            if (!brandRecord) {
                console.warn(`Brand "${brandNameToUse}" not found in Brand Basics table`);
                
                // Only use fallback if no brand was explicitly selected from dropdown
                if (!this.brandId || this.brandId === 'Quality Inn' || this.brandId === 'Not specified') {
                    console.log('🔄 Attempting to use fallback brand...');
                    const fallbackBrands = ['Radisson Blu', 'Hilton Hotels & Resorts', 'Courtyard by Marriott'];
                    
                    for (const fallbackBrand of fallbackBrands) {
                        const fallbackRecord = await this.findBrandByName(fallbackBrand);
                        if (fallbackRecord) {
                            console.log(`✅ Using fallback brand: "${fallbackBrand}"`);
                            console.log(`⚠️ WARNING: Overriding selected brand "${brandNameToUse}" with fallback "${fallbackBrand}"`);
                            // Update the brandId to the fallback brand for consistency
                            this.brandId = fallbackBrand;
                            return await this.getBrandDataForScoring(fallbackBrand);
                        }
                    }
                } else {
                    console.log(`❌ Brand "${brandNameToUse}" not found, but user selected "${this.brandId}" from dropdown - not using fallback`);
                }
                
                console.error('❌ No fallback brand found either');
                return null;
            }

            const brandRecordId = brandRecord.id;
            
            // Debug: Show all field names in the brand record to find the correct Brand ID field
            console.log(`🔍 Brand record fields:`, Object.keys(brandRecord.fields));
            console.log(`🔍 Brand record data:`, brandRecord.fields);
            
            // Try different possible field names for Brand ID
            const brandIdNumber = brandRecord.fields['Brand_Basics_ID'] || 
                                 brandRecord.fields['Brand ID'] || 
                                 brandRecord.fields['Brand_ID'] || 
                                 brandRecord.fields['ID'] ||
                                 brandRecord.fields['Brand ID Number'] ||
                                 brandRecord.fields['BrandNumber'];
                                 
            console.log(`✅ Found brand "${brandNameToUse}" with Record ID: ${brandRecordId}, Brand ID: ${brandIdNumber}`);
            
            // Fetch brand data from multiple tables - only Brand Basics uses the record ID directly
            const brandBasics = await this.fetchAirtableRecord(this.airtableConfig.brandsTableId, brandRecordId);
            
            // If we don't have a Brand ID number, we can't fetch linked data
            if (!brandIdNumber) {
                console.warn(`❌ No Brand ID number found for brand "${brandNameToUse}". Cannot fetch linked data.`);
            return {
                brandBasics: brandBasics?.fields || {},
                    brandFootprint: {},
                    brandStandards: {},
                    brandTerms: {},
                    brandFit: {},
                    brandFeeStructure: {},
                    brandOperationalSupport: {},
                    brandId: brandRecordId,
                    brandIdNumber: null
                };
            }
            
            // Fetch other brand data by searching for records linked to this brand using the Brand ID number
            console.log('🔍 ===== BRAND DATA FETCHING DEBUGGING =====');
            console.log('Brand ID Number:', brandIdNumber);
            console.log('Brand Basics fields:', brandBasics?.fields ? Object.keys(brandBasics.fields) : 'No data');
            console.log('Brand Setup - Operational Support linked field:', brandBasics?.fields?.['Brand Setup - Operational Support']);
            
            // Note: Operational Support table is not accessible (403 Forbidden), so we'll fetch it separately with error handling
            const [brandFootprint, brandStandards, brandTerms, brandFit, brandFeeStructure] = await Promise.all([
                this.fetchLinkedBrandData(this.airtableConfig.brandFootprintTableId, brandIdNumber),
                this.fetchLinkedBrandData(this.airtableConfig.brandStandardsTableId, brandIdNumber),
                this.fetchLinkedBrandData(this.airtableConfig.dealTermsTableId, brandIdNumber),
                this.fetchLinkedBrandData(this.airtableConfig.projectFitTableId, brandIdNumber),
                this.fetchLinkedBrandData(this.airtableConfig.feeStructureTableId, brandIdNumber)
            ]);
            
            console.log('🔍 ===== BRAND FIT DATA DEBUGGING =====');
            console.log('🔍 Brand Fit Raw Data:', brandFit);
            console.log('🔍 Brand Fit Fields:', brandFit?.fields ? Object.keys(brandFit.fields) : 'No fields');
            console.log('🔍 Brand Fit Values:', brandFit?.fields ? Object.values(brandFit.fields) : 'No values');
            
            // Get Operational Support data from the linked field in Brand Basics table
            let brandOperationalSupport = {};
            if (brandBasics?.fields && brandBasics.fields['Brand Setup - Operational Support']) {
                // This is a linked field, so we need to fetch the actual record
                const operationalSupportRecordId = brandBasics.fields['Brand Setup - Operational Support'][0];
                try {
                    const operationalSupportRecord = await this.fetchAirtableRecord('tblkZyvi6ELUSKWWP', operationalSupportRecordId);
                    brandOperationalSupport = operationalSupportRecord?.fields || {};
                    console.log('✅ Successfully fetched Operational Support data from linked field');
                } catch (error) {
                    console.log('⚠️ Error fetching Operational Support data from linked field:', error);
                    brandOperationalSupport = {};
                }
            } else {
                console.log('⚠️ No Operational Support linked field found in Brand Basics');
            }
            
            console.log('🔍 ===== BRAND DATA FETCHING RESULTS =====');
            console.log('brandFootprint:', brandFootprint ? Object.keys(brandFootprint) : 'No data');
            console.log('brandStandards:', brandStandards ? Object.keys(brandStandards) : 'No data');
            console.log('brandTerms:', brandTerms ? Object.keys(brandTerms) : 'No data');
            console.log('brandFit:', brandFit ? Object.keys(brandFit) : 'No data');
            console.log('brandFeeStructure:', brandFeeStructure ? Object.keys(brandFeeStructure) : 'No data');
            console.log('brandOperationalSupport:', brandOperationalSupport ? Object.keys(brandOperationalSupport) : 'No data');

            const result = {
                brandBasics: brandBasics?.fields || {},
                brandFootprint: brandFootprint || {},
                brandStandards: brandStandards || {},
                brandTerms: brandTerms || {},
                brandFit: brandFit || {},
                brandFeeStructure: brandFeeStructure || {},
                brandOperationalSupport: brandOperationalSupport || {},
                brandId: brandRecordId,
                brandIdNumber: brandIdNumber
            };
            
            console.log('🔍 Brand Data Fetching Results:', {
                brandRecordId: brandRecordId,
                brandIdNumber: brandIdNumber,
                brandBasicsFields: brandBasics?.fields ? Object.keys(brandBasics.fields) : 'No data',
                brandFootprintFields: brandFootprint ? Object.keys(brandFootprint) : 'No data',
                brandStandardsFields: brandStandards ? Object.keys(brandStandards) : 'No data',
                brandTermsFields: brandTerms ? Object.keys(brandTerms) : 'No data',
                brandFitFields: brandFit ? Object.keys(brandFit) : 'No data',
                brandOperationalSupportFields: brandOperationalSupport ? Object.keys(brandOperationalSupport) : 'No data'
            });
            
            // Cache the result for performance
            this.cache.brandData.set(cacheKey, {
                data: result,
                timestamp: Date.now()
            });
            
            return result;
        } catch (error) {
            console.error('Error fetching brand data for scoring:', error);
            return null;
        }
    }

    // MKT1: Priority Market fit (weight: 14)
    // MKT1: Priority Market fit (weight: 14)
    async calculateMKT1(dealFields, locationData, brandData) {
        const dealCountry = locationData?.Country || dealFields['Country'] || '';
        const marketsToAvoid = (brandData.brandBasics || {})['Markets to Avoid or Saturated'] || [];
        
        // Get priority markets from Brand Setup - Project Fit table
        const brandFit = brandData.brandFit || {};
        const priorityMarkets = [];
        
        // Extract priority markets from Project Fit table columns
        const projectFitColumns = [
            'Global - Priority Markets',
            'United States - Priority Markets',
            'Canada - Priority Markets',
            'Latin America - Priority Markets',
            'Middle East - Priority Markets', 
            'Western Europe - Priority Markets',
            'Eastern Europe - Priority Markets',
            'Southern Europe - Priority Markets',
            'Northern Europe - Priority Markets',
            'Other - Priority Markets'
        ];
        
        for (const column of projectFitColumns) {
            if (brandFit[column] === true || brandFit[column] === 'Yes') {
                // Extract region name from column name
                const regionName = column.replace(' - Priority Markets', '');
                priorityMarkets.push(regionName);
            }
        }
        
        // 🔍 MKT1 DEBUGGING
        console.log('🔍 ===== MKT1 DEBUGGING =====');
        console.log('📍 DEAL COUNTRY (from Location.Country OR Deal.Country):', dealCountry);
        console.log('🎯 PRIORITY MARKETS (from Brand Setup - Project Fit table):', priorityMarkets);
        console.log('🏢 BRAND PROJECT FIT DATA:', brandFit);
        console.log('🚫 MARKETS TO AVOID (from Brand Setup - Brand Basics."Markets to Avoid or Saturated"):', marketsToAvoid);
        
        // Get country-to-region mapping
        const countryMapping = this.getCountryRegionMapping();
        const dealRegions = countryMapping[dealCountry] || { region1: '', region2: '', region3: '', region: 'Global' };
        
        console.log('🗺️ DEAL REGIONS MAPPING:', dealRegions);
        
        // 1. Global Brand Check: If brand includes "Global" → Score = 100
        if (priorityMarkets.some(market => market.toLowerCase().includes('global'))) {
            console.log('✅ MKT1: Global brand detected - returning 100 points');
            return 100;
        }
        
        // 2. Hard Fail (KO) Check: If deal country/regions appear in "Markets to Avoid"
        const dealRegionsList = [dealCountry, dealRegions.region1, dealRegions.region2, dealRegions.region3].filter(r => r && r.trim() !== '');
        const isHardFail = marketsToAvoid.some(market => 
            dealRegionsList.some(region => 
                market.toLowerCase().includes(region.toLowerCase()) ||
                region.toLowerCase().includes(market.toLowerCase())
            )
        );
        
        if (isHardFail) {
            console.log('❌ MKT1: Hard fail - deal country/regions in markets to avoid');
            return 0; // Hard fail
        }
        
        // 3. Market Overlap Check with Tiered Scoring
        let bestScore = 0;
        let matchType = '';
        
        for (const market of priorityMarkets) {
            const marketLower = market.toLowerCase();
            
            // Check Country match (100 points)
            if (dealCountry.toLowerCase().includes(marketLower) || marketLower.includes(dealCountry.toLowerCase())) {
                if (100 > bestScore) {
                    bestScore = 100;
                    matchType = 'Country';
                }
            }
            
            // Check Region_1 match (90 points)
            if (dealRegions.region1 && (dealRegions.region1.toLowerCase().includes(marketLower) || marketLower.includes(dealRegions.region1.toLowerCase()))) {
                if (90 > bestScore) {
                    bestScore = 90;
                    matchType = 'Region_1';
                }
            }
            
            // Check Region_2 match (80 points)
            if (dealRegions.region2 && (dealRegions.region2.toLowerCase().includes(marketLower) || marketLower.includes(dealRegions.region2.toLowerCase()))) {
                if (80 > bestScore) {
                    bestScore = 80;
                    matchType = 'Region_2';
                }
            }
            
            // Check Region_3 match (80 points)
            if (dealRegions.region3 && (dealRegions.region3.toLowerCase().includes(marketLower) || marketLower.includes(dealRegions.region3.toLowerCase()))) {
                if (80 > bestScore) {
                    bestScore = 80;
                    matchType = 'Region_3';
                }
            }
        }
        
        console.log(`🎯 MKT1: Best match found - ${matchType} = ${bestScore} points`);
        return bestScore;
    }

    // MKT2: Recognition density vs owner need (weight: 8)
    // MKT2: Recognition density vs owner need (weight: 8)
    async calculateMKT2(dealFields, locationData, brandData) {
        const dealCountry = locationData?.Country || dealFields['Country'] || '';
        const brandRecognitionNeed = dealFields['Importance of Brand Recognition'] || 0;
        
        // 🔍 MKT2 DEBUGGING
        console.log('🔍 ===== MKT2 DEBUGGING =====');
        console.log('📍 DEAL COUNTRY (from Location.Country OR Deal.Country):', dealCountry);
        console.log('🎯 BRAND RECOGNITION NEED (from Deal."Importance of Brand Recognition"):', brandRecognitionNeed);
        
        // Map deal country to footprint region
        const region = this.mapCountryToRegion(dealCountry);
        const openHotelsInRegion = brandData.brandFootprint[`Number of Open Hotels (${region})`] || 0;
        
        // If owner need >= 4, require minimum hotel count
        if (brandRecognitionNeed >= 4) {
            const threshold = this.getRegionalThreshold(region);
            return openHotelsInRegion >= threshold ? 100 : 40;
        }
        
        // If owner need <= 3, density not required
        return 100;
    }

    // SEG1: Chain scale proximity (weight: 10)
    // SEG1: Chain scale proximity (weight: 10)
    async calculateSEG1(dealFields, locationData, brandData) {
        const brandChainScaleRaw = (brandData.brandBasics || {})['Hotel Chain Scale'] || '';
        const dealChainScaleRaw = locationData?.['Hotel Chain Scale'] || dealFields['Hotel Chain Scale'] || '';
        
        // 🔍 SEG1 DEBUGGING
        console.log('🔍 ===== SEG1 DEBUGGING =====');
        console.log('🏢 BRAND CHAIN SCALE (from Brand Setup - Brand Basics."Hotel Chain Scale"):', brandChainScaleRaw);
        console.log('📍 DEAL CHAIN SCALE (from Location."Hotel Chain Scale" OR Deal."Hotel Chain Scale"):', dealChainScaleRaw);
        
        // Return 0 if either chain scale is empty/unknown
        if (!brandChainScaleRaw.trim() || !dealChainScaleRaw.trim() ||
            brandChainScaleRaw.toLowerCase().includes('unknown') ||
            dealChainScaleRaw.toLowerCase().includes('unknown')) {
            return 0;
        }
        
        const brandChainScale = this.getChainScaleTier(brandChainScaleRaw);
        const dealChainScale = this.getChainScaleTier(dealChainScaleRaw);
        
        console.log('🔍 SEG1 Debug Info:', {
            brandChainScaleRaw: brandChainScaleRaw,
            dealChainScaleRaw: dealChainScaleRaw,
            brandChainScaleTier: brandChainScale,
            dealChainScaleTier: dealChainScale,
            areEqual: brandChainScale === dealChainScale,
            difference: Math.abs(brandChainScale - dealChainScale)
        });
        
        if (brandChainScale === dealChainScale) return 100;
        if (Math.abs(brandChainScale - dealChainScale) === 1) return 80;
        return 0;
    }

    // SVC1: Service model alignment (weight: 8)
    async calculateSVC1(dealFields, locationData, brandData) {
        const brandServiceModel = (brandData.brandBasics || {})['Hotel Service Model'] || '';
        const dealServiceModel = locationData?.['Hotel Service Model'] || '';
        const brandModel = (brandData.brandBasics || {})['Brand Model / Format'] || '';
        
        // 🔍 SVC1 DEBUGGING
        console.log('🔍 ===== SVC1 DEBUGGING =====');
        console.log('🏢 BRAND SERVICE MODEL (from Brand Setup - Brand Basics."Hotel Service Model"):', brandServiceModel);
        console.log('📊 DEAL SERVICE MODEL (from Location."Hotel Service Model"):', dealServiceModel);
        console.log('🏢 BRAND MODEL/FORMAT (from Brand Setup - Brand Basics."Brand Model / Format"):', brandModel);
        
        // Return 0 if either service model is empty/unknown
        if (!brandServiceModel.trim() || !dealServiceModel.trim() || 
            brandServiceModel.toLowerCase().includes('unknown') || 
            dealServiceModel.toLowerCase().includes('unknown')) {
            return 0;
        }
        
        // Exact match
        if (brandServiceModel.toLowerCase() === dealServiceModel.toLowerCase()) {
            return 100;
        }
        
        // Check if brand is flexible
        const isFlexible = brandModel.toLowerCase().includes('soft') || 
                          brandModel.toLowerCase().includes('conversion') || 
                          brandModel.toLowerCase().includes('collection');
        
        return isFlexible ? 70 : 0;
    }

    // SIZE1: Ideal room-range fit (weight: 12)
    async calculateSIZE1(dealFields, locationData, brandData) {
        const dealRooms = locationData?.['Total Number of Rooms/Keys'] || dealFields['Total Number of Rooms/Keys'] || 0;
        
        // Access the Project Fit data directly (it's now the fields from the Project Fit record)
        const projectFitData = brandData.brandFit;
        
        // 🔍 SIZE1 DEBUGGING
        console.log('🔍 ===== SIZE1 DEBUGGING =====');
        console.log('📍 DEAL ROOMS (from Location."Total Number of Rooms/Keys" OR Deal."Total Number of Rooms/Keys"):', dealRooms);
        console.log('🎯 PROJECT FIT DATA (from Brand Setup - Project Fit table):', projectFitData);
        
        // Try different possible field names for room ranges based on common Airtable naming patterns
        const minRooms = projectFitData?.['A Min - Ideal Project Size'] || 
                        projectFitData?.['Min - Ideal Project Size'] || 
                        projectFitData?.['Minimum Rooms'] || 
                        projectFitData?.['Min Rooms'] || 
                        projectFitData?.['Ideal Min Rooms'] || 0;
                        
        const maxRooms = projectFitData?.['A Max - Ideal Project Size'] || 
                        projectFitData?.['Max - Ideal Project Size'] || 
                        projectFitData?.['Maximum Rooms'] || 
                        projectFitData?.['Max Rooms'] || 
                        projectFitData?.['Ideal Max Rooms'] || 0;
        
        console.log('🔍 SIZE1 Debug Info:', {
            dealRooms: dealRooms,
            brandDataExists: !!brandData,
            brandFitExists: !!brandData?.brandFit,
            projectFitData: projectFitData,
            projectFitFields: projectFitData ? Object.keys(projectFitData) : [],
            minRoomsRaw: projectFitData?.['A Min - Ideal Project Size'],
            maxRoomsRaw: projectFitData?.['A Max - Ideal Project Size'],
            minRooms: minRooms,
            maxRooms: maxRooms,
            brandId: brandData?.brandId,
            allBrandDataKeys: brandData ? Object.keys(brandData) : [],
            allProjectFitFields: projectFitData ? Object.keys(projectFitData) : []
        });
        
        if (dealRooms >= minRooms && dealRooms <= maxRooms) {
            return 100;
        }
        
        // Linear decay outside range (±30% tolerance)
        const tolerance = 0.3;
        let penalty = 0;
        
        if (dealRooms < minRooms) {
            const shortfall = minRooms - dealRooms;
            const maxShortfall = minRooms * tolerance;
            penalty = Math.min(shortfall / maxShortfall, 1) * 100;
        } else if (dealRooms > maxRooms) {
            const excess = dealRooms - maxRooms;
            const maxExcess = maxRooms * tolerance;
            penalty = Math.min(excess / maxExcess, 1) * 100;
        }
        
        return Math.max(0, 100 - penalty);
    }

    // OWN1: Owner/Investor type (weight: 4)
    async calculateOWN1(dealFields, locationData, brandData, marketPerformanceData = {}) {
        // Get ownership structure from Market - Performance - Deal & Capital Structure table
        const dealOwnership = marketPerformanceData['Ownership Structure'] || '';
        
        // Get preferred owners from Brand Setup - Project Fit table
        const preferredOwners = (brandData.brandFit || {})['Preferred Owner/Investor Type'] || '';
        
        // 🔍 OWN1 DEBUGGING
        console.log('🔍 ===== OWN1 DEBUGGING =====');
        console.log('🏢 DEAL OWNERSHIP (from Market - Performance - Deal & Capital Structure."Ownership Structure"):', dealOwnership);
        console.log('🎯 BRAND PREFERRED OWNERS (from Brand Setup - Project Fit."Preferred Owner/Investor Type"):', preferredOwners);
        console.log('🔍 OWN1 Debug - Deal Fields:', Object.keys(dealFields));
        console.log('🔍 OWN1 Debug - Market Performance Data:', marketPerformanceData ? Object.keys(marketPerformanceData) : 'No market performance data');
        console.log('🔍 OWN1 Debug - Deal Ownership Structure:', marketPerformanceData['Ownership Structure']);
        console.log('🔍 OWN1 Debug - Brand Fit:', brandData.brandFit ? Object.keys(brandData.brandFit) : 'No brand fit');
        console.log('🔍 OWN1 Debug - Preferred Owner/Investor Type (from brandFit):', (brandData.brandFit || {})['Preferred Owner/Investor Type']);
        
        // If either value is empty/unknown, return a low score
        if (!dealOwnership || dealOwnership === 'Unknown' || !preferredOwners || preferredOwners === 'Unknown') {
            return 0;
        }
        
        // Check for exact match
        if (preferredOwners.toLowerCase().includes(dealOwnership.toLowerCase())) {
            return 100;
        }
        
        // Check for related types
        const relatedTypes = this.getRelatedOwnerTypes();
        const hasRelatedMatch = Object.entries(relatedTypes).some(([from, related]) => 
            dealOwnership.toLowerCase().includes(from.toLowerCase()) &&
            preferredOwners.toLowerCase().includes(related.toLowerCase())
        );
        
        return hasRelatedMatch ? 70 : 40;
    }

    // AMN1: Required standards & amenities (weight: 10)
    async calculateAMN1(dealFields, locationData, brandData) {
        // 🔍 AMN1 DEBUGGING - Let's see what fields are actually available
        console.log('🔍 ===== AMN1 DEBUGGING =====');
        console.log('🏢 BRAND DATA AVAILABLE:', !!brandData);
        console.log('🏢 BRAND STANDARDS AVAILABLE:', !!brandData?.brandStandards);
        
        const requiredStandards = brandData?.brandStandards?.['Brand Standards'] || '';
        console.log('📋 BRAND STANDARDS (from Brand Setup - Brand Standards."Brand Standards"):', requiredStandards);
        console.log('🏨 ALL AVAILABLE DEAL FIELDS:', Object.keys(dealFields));
        
        // Look for any field that might contain amenities
        const possibleAmenityFields = Object.keys(dealFields).filter(key => 
            key.toLowerCase().includes('amenit') || 
            key.toLowerCase().includes('facilit') ||
            key.toLowerCase().includes('standard') ||
            key.toLowerCase().includes('feature')
        );
        console.log('🔍 POSSIBLE AMENITY FIELDS:', possibleAmenityFields);
        
        // Extract amenity data from the actual fields we found
        const amenities = {
            pool: dealFields['Pool'] || false,
            lobby: dealFields['Lobby'] || false,
            coworking: dealFields['Co-working or lounge space'] || false,
            bar: dealFields['Bar or Beverage Concept'] || false,
            businessCenter: dealFields['Business Center'] || false,
            petAmenities: dealFields['Pet Amenities'] || false,
            solarPower: dealFields['Solar Power'] || false,
            meetingRooms: dealFields['Number of Meeting Rooms'] || 0,
            fboOutlets: dealFields['Number of F&B Outlets'] || 0,
            parkingSpaces: dealFields['Number of Parking Spaces'] || 0,
            meetingSpace: (dealFields['Meeting Space'] != null && dealFields['Meeting Space'] !== '' ? String(dealFields['Meeting Space']) + (dealFields['Meeting Space Unit'] ? ' ' + dealFields['Meeting Space Unit'] : '') : '') || '',
            fboProgram: dealFields['F&B Program Type'] || [],
            parkingProgram: dealFields['Parking Program Type'] || []
        };
        
        // Create a summary string for display
        const amenitySummary = [];
        if (amenities.pool) amenitySummary.push('Pool');
        if (amenities.lobby) amenitySummary.push('Lobby');
        if (amenities.coworking) amenitySummary.push('Co-working');
        if (amenities.bar) amenitySummary.push('Bar');
        if (amenities.businessCenter) amenitySummary.push('Business Center');
        if (amenities.petAmenities) amenitySummary.push('Pet Amenities');
        if (amenities.solarPower) amenitySummary.push('Solar Power');
        if (amenities.meetingRooms > 0) amenitySummary.push(`${amenities.meetingRooms} Meeting Rooms`);
        if (amenities.fboOutlets > 0) amenitySummary.push(`${amenities.fboOutlets} F&B Outlets`);
        if (amenities.parkingSpaces > 0) amenitySummary.push(`${amenities.parkingSpaces} Parking Spaces`);
        
        // Store the summary for display purposes
        dealFields._amenitySummary = amenitySummary.join(', ') || 'No amenities specified';
        
        console.log('🏨 EXTRACTED AMENITIES:', amenities);
        
        // If no brand standards, return default score
        if (!requiredStandards || requiredStandards.trim() === '') {
            console.log('⚠️ NO BRAND STANDARDS FOUND - using default score');
            console.log('🔍 Brand data structure:', brandData);
            return 100;
        }
        
        let score = 100;
        const requiredItems = requiredStandards.split(';').filter(item => item.trim());
        console.log('📋 REQUIRED ITEMS:', requiredItems);
        
        // Check each required item against available amenities
        for (const required of requiredItems) {
            const requiredLower = required.toLowerCase().trim();
            let hasMatch = false;
            
            // Check against boolean amenities
            if (requiredLower.includes('pool') && amenities.pool) hasMatch = true;
            else if (requiredLower.includes('lobby') && amenities.lobby) hasMatch = true;
            else if (requiredLower.includes('coworking') && amenities.coworking) hasMatch = true;
            else if (requiredLower.includes('bar') && amenities.bar) hasMatch = true;
            else if (requiredLower.includes('business') && amenities.businessCenter) hasMatch = true;
            else if (requiredLower.includes('pet') && amenities.petAmenities) hasMatch = true;
            else if (requiredLower.includes('solar') && amenities.solarPower) hasMatch = true;
            
            // Check against numeric amenities
            else if (requiredLower.includes('meeting') && amenities.meetingRooms > 0) hasMatch = true;
            else if (requiredLower.includes('f&b') && amenities.fboOutlets > 0) hasMatch = true;
            else if (requiredLower.includes('parking') && amenities.parkingSpaces > 0) hasMatch = true;
            
            // Check against string amenities
            else if (amenities.meetingSpace && amenities.meetingSpace.toLowerCase().includes(requiredLower)) hasMatch = true;
            else if (Array.isArray(amenities.fboProgram) && amenities.fboProgram.some(item => 
                item.toLowerCase().includes(requiredLower))) hasMatch = true;
            else if (Array.isArray(amenities.parkingProgram) && amenities.parkingProgram.some(item => 
                item.toLowerCase().includes(requiredLower))) hasMatch = true;
            
            if (!hasMatch) {
                console.log(`❌ MISSING REQUIRED AMENITY: "${required}"`);
                score -= 12; // Deduct 12 points for each missing required amenity
            } else {
                console.log(`✅ FOUND REQUIRED AMENITY: "${required}"`);
            }
        }
        
        // Bonus for optional amenities (using the same logic but with bonus points)
        const optionalAmenities = this.getOptionalAmenities();
        for (const optional of optionalAmenities) {
            const optionalLower = optional.toLowerCase();
            let hasOptional = false;
            
            // Check against boolean amenities
            if (optionalLower.includes('pool') && amenities.pool) hasOptional = true;
            else if (optionalLower.includes('lobby') && amenities.lobby) hasOptional = true;
            else if (optionalLower.includes('coworking') && amenities.coworking) hasOptional = true;
            else if (optionalLower.includes('bar') && amenities.bar) hasOptional = true;
            else if (optionalLower.includes('business') && amenities.businessCenter) hasOptional = true;
            else if (optionalLower.includes('pet') && amenities.petAmenities) hasOptional = true;
            else if (optionalLower.includes('solar') && amenities.solarPower) hasOptional = true;
            
            if (hasOptional) {
                score += 2; // +2 points per optional amenity
            }
        }
        
        return Math.max(0, Math.min(100, score));
    }

    // FIN1: Fees tolerance (weight: 10)
    async calculateFIN1(dealFields, locationData, brandData, marketPerformanceData = {}) {
        // Get individual deal fee expectations from Market - Performance - Deal & Capital Structure table
        const dealRoyaltyFee = marketPerformanceData['Royalty Fee Expectations'] || '';
        const dealMarketingFee = marketPerformanceData['Marketing Fee Expectations'] || '';
        const dealLoyaltyFee = marketPerformanceData['Loyalty Fee Expectations'] || '';
        
        // Get brand fee structure from Brand Setup - Fee Structure table
        const brandFeeStructure = brandData.brandFeeStructure || {};
        
        // 🔍 FIN1 DEBUGGING
        console.log('🔍 ===== FIN1 DEBUGGING =====');
        console.log('💰 DEAL ROYALTY FEE (from Market - Performance - Deal & Capital Structure."Royalty Fee Expectations"):', dealRoyaltyFee);
        console.log('💰 DEAL MARKETING FEE (from Market - Performance - Deal & Capital Structure."Marketing Fee Expectations"):', dealMarketingFee);
        console.log('💰 DEAL LOYALTY FEE (from Market - Performance - Deal & Capital Structure."Loyalty Fee Expectations"):', dealLoyaltyFee);
        console.log('🏢 BRAND FEE STRUCTURE (from Brand Setup - Fee Structure table):', brandFeeStructure);
        
        // Parse individual fee expectations
        const dealFees = {
            royalty: this.parseSingleFee(dealRoyaltyFee),
            marketing: this.parseSingleFee(dealMarketingFee),
            loyalty: this.parseSingleFee(dealLoyaltyFee)
        };
        
        console.log('🔍 PARSED DEAL FEES:', dealFees);
        
        let totalScore = 0;
        let feeCount = 0;
        
        // Check each fee type with correct field names
        const feeTypes = [
            { key: 'royalty', minField: 'Min - Typical Royalty Fee Range', maxField: 'Max - Typical Royalty Fee Range' },
            { key: 'marketing', minField: 'Min - Typical Marketing Fee Range', maxField: 'Max - Typical Marketing Fee Range' },
            { key: 'loyalty', minField: 'Min - Typical Loyalty Program Fee', maxField: 'Max - Typical Loyalty Program Fee' }
        ];
        
        for (const feeType of feeTypes) {
            const dealFee = dealFees[feeType.key];
            const brandMinRaw = brandFeeStructure[feeType.minField] || 0;
            const brandMaxRaw = brandFeeStructure[feeType.maxField] || 0;
            
            // Parse brand min/max to remove % symbols and convert to numbers
            let brandMin = typeof brandMinRaw === 'string' ? parseFloat(brandMinRaw.replace('%', '')) : brandMinRaw;
            let brandMax = typeof brandMaxRaw === 'string' ? parseFloat(brandMaxRaw.replace('%', '')) : brandMaxRaw;
            
            // Check if brand fees are stored as decimals (0.05 = 5%) and convert to percentages for calculation
            // If both min and max are less than 1, assume they're stored as decimals
            if (brandMin < 1 && brandMax < 1) {
                brandMin = brandMin * 100;
                brandMax = brandMax * 100;
            }
            
            console.log(`🔍 FIN1 - ${feeType.key}: Deal=${dealFee}, Brand Min=${brandMin} (raw: ${brandMinRaw}), Brand Max=${brandMax} (raw: ${brandMaxRaw})`);
            
            if (dealFee !== null && brandMin !== undefined && brandMax !== undefined) {
                feeCount++;
                
                if (dealFee >= brandMin && dealFee <= brandMax) {
                    // Perfect compatibility - both parties can agree on a fee within the range
                    totalScore += 100;
                    console.log(`✅ FIN1 - ${feeType.key}: Perfect match - Deal fee ${dealFee}% is within brand range ${brandMin}%-${brandMax}%`);
                } else if (dealFee > brandMax) {
                    // Deal is willing to pay more than brand max
                    const excessPercentage = ((dealFee - brandMax) / brandMax) * 100;
                    let score = 0;
                    
                    if (excessPercentage <= 10) {
                        // Deal is only slightly above brand max (≤10% excess) - minor mismatch
                        score = 85;
                    } else if (excessPercentage <= 25) {
                        // Deal is moderately above brand max (11-25% excess) - moderate mismatch
                        score = 70;
                    } else if (excessPercentage <= 50) {
                        // Deal is significantly above brand max (26-50% excess) - significant mismatch
                        score = 50;
                } else {
                        // Deal is way above brand max (>50% excess) - major mismatch
                        score = 25;
                    }
                    
                    totalScore += score;
                    console.log(`⚠️ FIN1 - ${feeType.key}: Deal fee ${dealFee}% is ${excessPercentage.toFixed(1)}% above brand max ${brandMax}% - Score: ${score}`);
                } else if (dealFee < brandMin) {
                    // Deal is willing to pay less than brand min
                    const shortfallPercentage = ((brandMin - dealFee) / brandMin) * 100;
                    let score = 0;
                    
                    if (shortfallPercentage <= 10) {
                        // Deal is only slightly below brand min (≤10% shortfall) - minor mismatch
                        score = 75;
                    } else if (shortfallPercentage <= 25) {
                        // Deal is moderately below brand min (11-25% shortfall) - moderate mismatch
                        score = 50;
                    } else if (shortfallPercentage <= 50) {
                        // Deal is significantly below brand min (26-50% shortfall) - significant mismatch
                        score = 25;
                    } else {
                        // Deal is way below brand min (>50% shortfall) - major mismatch
                        score = 0;
                    }
                    
                    totalScore += score;
                    console.log(`❌ FIN1 - ${feeType.key}: Deal fee ${dealFee}% is ${shortfallPercentage.toFixed(1)}% below brand min ${brandMin}% - Score: ${score}`);
                }
            }
        }
        
        return feeCount > 0 ? Math.round(totalScore / feeCount) : 50;
    }

    // INC1: Incentives match (weight: 10)
    async calculateINC1(dealFields, locationData, brandData, marketPerformanceData = {}) {
        // Get brand willingness from Operational Support table (may be empty due to 403 Forbidden)
        const brandWillingToNegotiate = (brandData.brandOperationalSupport || {})['Willing to Negotiate Incentives'] === 'Yes';
        
        // Get individual incentive fields from Operational Support table (may be empty due to 403 Forbidden)
        const brandIncentives = brandData.brandOperationalSupport || {};
        
        // Get individual incentive fields from Market Performance table
        const dealIncentives = marketPerformanceData || {};
        
        // 🔍 INC1 DEBUGGING
        console.log('🔍 ===== INC1 DEBUGGING =====');
        console.log('🏢 BRAND WILLING TO NEGOTIATE (from Brand Setup - Operational Support."Willing to Negotiate Incentives"):', brandWillingToNegotiate);
        console.log('🏢 BRAND INCENTIVES (from Brand Setup - Operational Support table):', brandIncentives);
        console.log('🎁 DEAL INCENTIVES (from Market - Performance - Deal & Capital Structure table):', dealIncentives);
        
        // 🔍 DETAILED INC1 DEBUGGING - Check specific incentive fields
        console.log('🔍 ===== INC1 DETAILED INCENTIVE DEBUGGING =====');
        const sampleIncentives = ['Lower Initial Fees', 'Tiered Fee Structure', 'Key Money', 'Performance Bonuses'];
        for (const incentive of sampleIncentives) {
            console.log(`🏢 Brand "${incentive}":`, brandIncentives[incentive]);
            console.log(`🎁 Deal "${incentive}":`, dealIncentives[incentive]);
        }
        
        // 🔍 Check if brandOperationalSupport data exists
        console.log('🔍 ===== BRAND OPERATIONAL SUPPORT DATA CHECK =====');
        console.log('brandData.brandOperationalSupport exists:', !!brandData.brandOperationalSupport);
        console.log('brandData.brandOperationalSupport keys:', brandData.brandOperationalSupport ? Object.keys(brandData.brandOperationalSupport) : 'No data');
        console.log('brandData.brandOperationalSupport full object:', brandData.brandOperationalSupport);
        
        // If Operational Support data is not available, return a neutral score
        if (!brandData.brandOperationalSupport || Object.keys(brandData.brandOperationalSupport).length === 0) {
            console.log('⚠️ INC1: No Operational Support data available, returning neutral score of 50');
            return 50; // Neutral score when data is not available
        }
        
        let score = 80; // Starting score
        
        if (!brandWillingToNegotiate) {
            return 40; // Brand not willing to negotiate
        }
        
        // List of incentive fields to compare
        const incentiveFields = [
            'Lower Initial Fees',
            'Tiered Fee Structure', 
            'Temporary Royalty Discounts',
            'Performance-Based Royalties',
            'Performance Bonuses',
            'Brand Loyalty Rewards',
            'Shorter Contract Durations',
            'Termination Flexibility',
            'Financing Assistance',
            'Construction or Renovation Grants',
            'Comprehensive Training Packages',
            'Ongoing Operational Support',
            'Co-op Advertising Funds',
            'Local Marketing Programs',
            'Technology Upgrades',
            'Data Analytics Tools',
            'Protected Territories',
            'Expansion Incentives',
            'New Brand Launch Discounts',
            'Custom Branding Options',
            'Franchisee Advisory Councils',
            'Annual Franchisee Conferences',
            'Insurance Support',
            'Market Research Data',
            'Equity Participation Options',
            'Long-Term Profit Sharing',
            'Key Money'
        ];
        
        // Count matches between brand and deal incentives
        let matches = 0;
        let totalDealIncentives = 0;
        
        for (const field of incentiveFields) {
            const brandOffers = brandIncentives[field] === true || brandIncentives[field] === 'Yes';
            const dealSeeks = dealIncentives[field] === true || dealIncentives[field] === 'Yes';
            
            if (dealSeeks) {
                totalDealIncentives++;
                if (brandOffers) {
                    matches++;
                }
            }
        }
        
        if (totalDealIncentives === 0) {
            return 80; // No specific incentives sought
        }
        
        const matchPercentage = matches / totalDealIncentives;
        score = Math.round(80 + (matchPercentage * 20));
        
        return Math.min(100, score);
    }

    // STR1: Strategic brand model preference (weight: 4)
    async calculateSTR1(dealFields, locationData, brandData, strategicIntentData = {}) {
        // Get brand soft/collection status from Project Fit table
        const brandSoftCollection = (brandData.brandFit || {})['Soft/Collection Brand'] || '';
        // Get deal preference from Strategic Intent table
        const dealPreference = strategicIntentData?.['Soft vs Hard Brand Preference'] || '';
        
        // 🔍 STR1 DEBUGGING
        console.log('🔍 ===== STR1 DEBUGGING =====');
        console.log('🏢 BRAND SOFT/COLLECTION (from Brand Setup - Project Fit."Soft/Collection Brand"):', brandSoftCollection);
        console.log('📊 DEAL PREFERENCE (from Strategic Intent - Operational - Key Challenges."Soft vs Hard Brand Preference"):', dealPreference);
        console.log('🔍 STR1 - Full strategicIntentData object:', strategicIntentData);
        console.log('🔍 STR1 - strategicIntentData keys:', strategicIntentData ? Object.keys(strategicIntentData) : 'No strategicIntentData');
        
        // Convert brand data: Yes = Soft Brand, No = Hard Brand
        const isBrandSoft = brandSoftCollection.toLowerCase() === 'yes';
        const isBrandHard = brandSoftCollection.toLowerCase() === 'no';
        
        // Convert deal preference to boolean logic
        const dealPrefLower = dealPreference.toLowerCase();
        const isDealSoft = dealPrefLower.includes('soft brand');
        const isDealHard = dealPrefLower.includes('hard brand');
        const isDealOpenToBoth = dealPrefLower.includes('open to both') || dealPrefLower.includes('unsure');
        
        // 🔍 DETAILED STR1 LOGIC DEBUGGING
        console.log('🔍 STR1 Logic Debug:');
        console.log('  - brandSoftCollection raw:', `"${brandSoftCollection}"`);
        console.log('  - dealPreference raw:', `"${dealPreference}"`);
        console.log('  - isBrandSoft:', isBrandSoft);
        console.log('  - isBrandHard:', isBrandHard);
        console.log('  - dealPrefLower:', `"${dealPrefLower}"`);
        console.log('  - isDealSoft:', isDealSoft);
        console.log('  - isDealHard:', isDealHard);
        console.log('  - isDealOpenToBoth:', isDealOpenToBoth);
        
        // Scoring logic
        if (isDealOpenToBoth) {
            console.log('  - Result: Deal open to both → 100 points');
            return 100; // Deal is open to both, so any brand gets full points
        }
        
        if (isBrandSoft && isDealSoft) {
            console.log('  - Result: Soft brand + Soft preference → 100 points');
            return 100; // Soft brand matches soft preference
        }
        
        if (isBrandHard && isDealHard) {
            console.log('  - Result: Hard brand + Hard preference → 100 points');
            return 100; // Hard brand matches hard preference
        }
        
        if (isBrandSoft && isDealHard) {
            console.log('  - Result: Soft brand + Hard preference → 0 points');
            return 0; // Soft brand doesn't match hard preference
        }
        
        if (isBrandHard && isDealSoft) {
            console.log('  - Result: Hard brand + Soft preference → 0 points');
            return 0; // Hard brand doesn't match soft preference
        }
        
        console.log('  - Result: No match found → 60 points (default)');
        return 60; // Default score for unknown/missing data
    }

    // PREF1: Preferred brand bonus (weight: 10)
    async calculatePREF1(dealFields, locationData, brandData, strategicIntentData = {}) {
        const brandName = (brandData.brandBasics || {})['Brand Name'] || '';
        const preferredBrands = strategicIntentData?.['Preferred Brands'] || '';
        
        // 🔍 PREF1 DEBUGGING
        console.log('🔍 ===== PREF1 DEBUGGING =====');
        console.log('🏢 BRAND NAME (from Brand Setup - Brand Basics."Brand Name"):', brandName);
        console.log('📊 PREFERRED BRANDS (from Strategic Intent - Operational - Key Challenges."Preferred Brands"):', preferredBrands);
        console.log('📊 PREFERRED BRANDS type:', typeof preferredBrands, Array.isArray(preferredBrands) ? '(array)' : '(not array)');
        
        // Handle both string and array formats for preferred brands
        let preferredList = [];
        if (Array.isArray(preferredBrands)) {
            preferredList = preferredBrands.map(brand => brand.toString().trim().toLowerCase());
        } else if (typeof preferredBrands === 'string') {
            preferredList = preferredBrands.split(',').map(brand => brand.trim().toLowerCase());
        }
        
        const hasBrandMatch = preferredList.some(brand => 
            brand.includes(brandName.toLowerCase()) ||
            brandName.toLowerCase().includes(brand)
        );
        
        return hasBrandMatch ? 100 : 0;
    }

    // PROJ1: Project type compatibility - HARD FAIL if not acceptable
    async calculatePROJ1(dealFields, locationData, brandData) {
        // Get deal's project type from Deals table
        const dealProjectType = dealFields?.['Project Type'] || '';
        
        // Get brand's acceptable project types from Brand Setup - Project Fit table
        const brandFit = brandData?.brandFit || {};
        
        console.log('🏗️ ===== PROJECT TYPE EVALUATION =====');
        console.log('🏗️ Deal Project Type:', dealProjectType);
        console.log('🏗️ Brand Fit Data:', brandFit);
        console.log('🏗️ Brand Fit Data Keys:', Object.keys(brandFit));
        console.log('🏗️ Brand Fit Data Values:', Object.values(brandFit));
        console.log('🏗️ Full Brand Data:', brandData);
        console.log('🏗️ Brand Data Keys:', Object.keys(brandData || {}));
        
        // Map deal project types to brand criteria fields
        const projectTypeMapping = {
            'New Build': ['New Build - Acceptable Project Type'],
            'Conversion / Reflag': ['Conversion - Reflag - Acceptable Project Type'],
            'Conversion': ['Conversion - Reflag - Acceptable Project Type'],
            'Reflag': ['Conversion - Reflag - Acceptable Project Type'],
            'Renovation': ['Conversion - Reflag - Acceptable Project Type'],
            'Expansion': ['New Build - Acceptable Project Type']
        };
        
        // Find the brand criteria fields to check for this project type
        const criteriaToCheck = projectTypeMapping[dealProjectType] || [];
        
        if (criteriaToCheck.length === 0) {
            console.log('🏗️ No mapping found for project type:', dealProjectType);
            return 0; // Hard fail if project type not recognized
        }
        
        // Check if any of the brand criteria are acceptable
        let isAcceptable = false;
        for (const criteria of criteriaToCheck) {
            if (brandFit[criteria] === true || brandFit[criteria] === 'Yes' || brandFit[criteria] === 'Acceptable') {
                isAcceptable = true;
                console.log('🏗️ ✅ Found acceptable criteria:', criteria, '=', brandFit[criteria]);
                break;
            }
        }
        
        if (!isAcceptable) {
            console.log('🏗️ ❌ No acceptable criteria found for project type:', dealProjectType);
            console.log('🏗️ Checked criteria:', criteriaToCheck);
            console.log('🏗️ Available brand fit fields:', Object.keys(brandFit));
        }
        
        // HARD FAIL: Return 0 if not acceptable, 100 if acceptable
        const score = isAcceptable ? 100 : 0;
        console.log('🏗️ Final PROJ1 score (HARD FAIL):', score);
        
        return score;
    }

    // PROJ2: Building type compatibility (Low-Rise, High-Rise, etc.) - HARD FAIL if not acceptable
    async calculatePROJ2(dealFields, locationData, brandData) {
        // Get deal's building characteristics from Location & Property table
        const buildingHeight = locationData?.['Max height Allowed By Zoning Sq. Meters'] || '';
        const totalSiteSize = locationData?.['Total Site Size Sq. Meters'] || '';
        
        // Get brand's acceptable building types from Brand Setup - Project Fit table
        const brandFit = brandData?.brandFit || {};
        
        console.log('🏢 ===== BUILDING TYPE EVALUATION =====');
        console.log('🏢 Building Height:', buildingHeight);
        console.log('🏢 Total Site Size:', totalSiteSize);
        console.log('🏢 Brand Fit Data Keys:', Object.keys(brandFit));
        console.log('🏢 Full Brand Data:', brandData);
        console.log('🏢 Brand Data Keys:', Object.keys(brandData || {}));
        
        // Map building characteristics to brand criteria fields (exact Airtable field names)
        const buildingTypeMapping = {
            'High-Rise': ['High-Rise - Acceptable Building Type'],
            'Historic / Renovated': ['Historic / Renovated - Acceptable Building Type'],
            'Low-Rise': ['Low-Rise - Acceptable Building Type'],
            'Mid-Rise': ['Mid-Rise - Acceptable Building Type'],
            'Mixed-Use': ['Mixed-Use - Acceptable Building Type'],
            'Podium / Tower': ['Podium / Tower - Acceptable Building Type'],
            'Resort-Style Compound': ['Resort-Style Compound - Acceptable Building Type']
        };
        
        // Get building type directly from Location & Property table
        let buildingType = locationData?.['Building Type'] || 'Unknown';
        
        console.log('🏢 Building Type from Location & Property table:', buildingType);
        
        // Find the brand criteria fields to check for this building type
        const criteriaToCheck = buildingTypeMapping[buildingType] || [];
        
        if (criteriaToCheck.length === 0) {
            console.log('🏢 No mapping found for building type:', buildingType);
            return 0; // Hard fail if building type not recognized
        }
        
        // Check if any of the brand criteria are acceptable
        let isAcceptable = false;
        for (const criteria of criteriaToCheck) {
            if (brandFit[criteria] === true || brandFit[criteria] === 'Yes' || brandFit[criteria] === 'Acceptable') {
                isAcceptable = true;
                console.log('🏢 ✅ Found acceptable criteria:', criteria, '=', brandFit[criteria]);
                break;
            }
        }
        
        if (!isAcceptable) {
            console.log('🏢 ❌ No acceptable criteria found for building type:', buildingType);
            console.log('🏢 Checked criteria:', criteriaToCheck);
        }
        
        // HARD FAIL: Return 0 if not acceptable, 100 if acceptable
        const score = isAcceptable ? 100 : 0;
        console.log('🏢 Final PROJ2 score (HARD FAIL):', score);
        
        return score;
    }

    // Helper functions for scoring calculations
    getChainScaleTier(chainScale) {
        const tiers = {
            'Luxury': 5,
            'Upper Upscale': 4,
            'Upscale': 3,
            'Upper Midscale': 2,
            'Midscale': 1,
            'Economy': 0,
            'Independent': 0
        };
        
        for (const [scale, tier] of Object.entries(tiers)) {
            if (chainScale.toLowerCase().includes(scale.toLowerCase())) {
                return tier;
            }
        }
        return 0;
    }

    mapCountryToRegion(country) {
        const regionMap = {
            'United States': 'Americas',
            'Canada': 'Americas',
            'Mexico': 'CALA',
            'Brazil': 'CALA',
            'Argentina': 'CALA',
            'Germany': 'EU',
            'France': 'EU',
            'United Kingdom': 'EU',
            'China': 'APAC',
            'Japan': 'APAC',
            'Australia': 'APAC',
            'UAE': 'MEA',
            'Saudi Arabia': 'MEA',
            'South Africa': 'MEA'
        };
        
        return regionMap[country] || 'Americas'; // Default to Americas
    }

    getRegionalThreshold(region) {
        const thresholds = {
            'Americas': 10,
            'CALA': 5,
            'EU': 8,
            'MEA': 3,
            'APAC': 6
        };
        return thresholds[region] || 5;
    }

    getRelatedOwnerTypes() {
        return {
            'Developer': 'PE',
            'Family Office': 'HNW',
            'Private Investor': 'HNW',
            'Institutional': 'PE'
        };
    }

    getOptionalAmenities() {
        return [
            'spa', 'fitness center', 'pool', 'restaurant', 'bar', 'conference room',
            'business center', 'parking', 'wifi', 'room service'
        ];
    }

    parseSingleFee(feeString) {
        // Parse a single fee expectation string
        console.log('🔍 Parsing single fee:', feeString);
        
        if (!feeString || feeString.trim() === '' || feeString.toLowerCase().includes('not specified')) {
            console.log('⚠️ No fee provided, returning null to indicate unspecified');
            return null; // Return null to indicate fee is not specified
        }
        
        // Try to extract number from the fee string
        const numbers = feeString.match(/\d+(?:\.\d+)?/g);
        console.log('🔍 Extracted numbers from fee string:', numbers);
        
        if (numbers && numbers.length >= 1) {
            const result = parseFloat(numbers[0]) || 0;
            console.log('🔍 Parsed fee:', result);
            return result;
        }
        
        // If we can't parse the string but it's not empty, return null
        console.log('⚠️ Could not parse fee string, returning null');
        return null;
    }

    parseFeeExpectations(feeString) {
        // Parse the actual fee expectations string from the deal
        console.log('🔍 Parsing fee expectations:', feeString);
        
        if (!feeString || feeString.trim() === '' || feeString.toLowerCase().includes('not specified')) {
            console.log('⚠️ No fee expectations provided, returning null to indicate unspecified');
            return null; // Return null to indicate fees are not specified
        }
        
        // Try to extract numbers from the fee string
        // This is a simple parser - in practice, you'd want more sophisticated parsing
        const numbers = feeString.match(/\d+(?:\.\d+)?/g);
        console.log('🔍 Extracted numbers from fee string:', numbers);
        
        if (numbers && numbers.length >= 3) {
            const result = {
                royalty: parseFloat(numbers[0]) || 0,
                marketing: parseFloat(numbers[1]) || 0,
                loyalty: parseFloat(numbers[2]) || 0
            };
            console.log('🔍 Parsed 3+ numbers:', result);
            return result;
        } else if (numbers && numbers.length >= 1) {
            // If only one number, assume it's royalty
            const result = {
                royalty: parseFloat(numbers[0]) || 0,
                marketing: 0,
                loyalty: 0
            };
            console.log('🔍 Parsed 1 number (royalty only):', result);
            return result;
        }
        
        // If we can't parse the string but it's not empty, return null
        console.log('⚠️ Could not parse fee string, returning null');
        return null;
    }

    hasHardFail(dealFields, locationData, brandData) {
        // Check for hard fail conditions
        const dealCountry = locationData?.Country || dealFields['Country'] || '';
        const marketsToAvoid = (brandData.brandBasics || {})['Markets to Avoid or Saturated'] || [];
        
        return marketsToAvoid.some(market => 
            market.toLowerCase().includes(dealCountry.toLowerCase())
        );
    }

    // Find brand by name in Brand Basics table
    async findBrandByName(brandName) {
        console.warn('findBrandByName is disabled in frontend; use backend brand endpoints.');
        return null;
    }


    async fetchAirtableRecord(tableId, recordId) {
        console.warn('fetchAirtableRecord is disabled in frontend.');
        return null;
    }

    // Generic method to fetch brand data from any table by searching for records linked to a specific brand
    async fetchLinkedBrandData(tableId, brandId) {
        console.warn('fetchLinkedBrandData is disabled in frontend; use backend brand endpoints.');
        return null;
    }

    // Fetch Project Fit data by searching for records linked to a specific brand
    async fetchProjectFitByBrand(brandId) {
        return await this.fetchLinkedBrandData(this.airtableConfig.projectFitTableId, brandId);
    }

    // Save match score back to Airtable
    async saveMatchScoreToAirtable(dealId, matchScore, scoringBreakdown) {
        console.warn('saveMatchScoreToAirtable is disabled in frontend.');
        return false;
    }

    // Calculate and save match scores for all deals
    async calculateAndSaveAllMatchScores() {
        try {
            console.log('🔄 Starting match score calculation for all deals...');
            
            // Get all deals
            const deals = await this.fetchDealsFromAirtable();
            let processedCount = 0;
            let successCount = 0;
            
            for (const deal of deals) {
                try {
                    // Get location data for this deal
                    const locationData = await this.getLocationData(deal.fields['Location_ID']);
                    
                    // Calculate match score (using default brand lookup for now)
                    const matchScoreResult = await this.calculateRealMatchScore(deal.fields, locationData, null, {}, {});
                    const matchScore = matchScoreResult.score;
                    
                    // Get scoring breakdown for debugging
                    const brandData = await this.getBrandDataForScoring();
                    const subscores = {
                        MKT1: await this.calculateMKT1(deal.fields, locationData, brandData),
                        MKT2: await this.calculateMKT2(deal.fields, locationData, brandData),
                        SEG1: await this.calculateSEG1(deal.fields, locationData, brandData),
                        SVC1: await this.calculateSVC1(deal.fields, locationData, brandData),
                        SIZE1: await this.calculateSIZE1(deal.fields, locationData, brandData),
                        OWN1: await this.calculateOWN1(deal.fields, locationData, brandData),
                        AMN1: await this.calculateAMN1(deal.fields, locationData, brandData),
                        FIN1: await this.calculateFIN1(deal.fields, locationData, brandData, deal.marketPerformanceData || {}),
                        INC1: await this.calculateINC1(deal.fields, locationData, brandData, deal.marketPerformanceData || {}),
                        STR1: await this.calculateSTR1(deal.fields, locationData, brandData),
                        PREF1: await this.calculatePREF1(deal.fields, locationData, brandData)
                    };
                    
                    // Save to Airtable
                    const saved = await this.saveMatchScoreToAirtable(deal.id, matchScore, subscores);
                    if (saved) successCount++;
                    
                    processedCount++;
                    
                    // Add small delay to avoid rate limiting
                    await new Promise(resolve => setTimeout(resolve, 100));
                    
                } catch (error) {
                    console.error(`Error processing deal ${deal.id}:`, error);
                }
            }
            
            console.log(`✅ Match score calculation completed: ${successCount}/${processedCount} deals processed successfully`);
            return { processed: processedCount, successful: successCount };
            
        } catch (error) {
            console.error('Error calculating match scores:', error);
            return { processed: 0, successful: 0 };
        }
    }



    calculateUserResponseTime(userData) {
        if (!userData) return { text: 'Unknown', color: 'gray' };
        
        // Calculate based on user engagement and deal activity
        const responseTime = userData.responseTime || 'medium';
        
        switch (responseTime) {
            case 'fast':
                return { text: 'Very Fast - Frequently', color: 'green' };
            case 'medium':
                return { text: 'Moderate - Regular', color: 'orange' };
            case 'slow':
                return { text: 'Slow - Infrequent', color: 'red' };
            default:
                return { text: 'New User', color: 'gray' };
        }
    }

    // Helper methods for data extraction
    extractBudgetFromDeal(dealFields) {
        // Look for budget information in various fields
        if (dealFields['Total Investment Amount']) {
            return dealFields['Total Investment Amount'];
        }
        return 'Not specified';
    }

    extractCityFromLocation(locationData) {
        if (locationData?.['Full Address']) {
            const address = locationData['Full Address'];
            // Extract city from full address
            const parts = address.split(',');
            if (parts.length >= 2) {
                return parts[1].trim();
            }
        }
        return locationData?.City || 'Unknown';
    }

    buildDealDescription(dealFields) {
        const parts = [];
        
        if (dealFields['Project Type']) {
            parts.push(`${dealFields['Project Type']} project`);
        }
        
        if (dealFields['Stage of Development']) {
            parts.push(`currently in ${dealFields['Stage of Development']} stage`);
        }
        
        if (dealFields['Current Brand Affiliation']) {
            parts.push(`currently branded as ${dealFields['Current Brand Affiliation']}`);
        }
        
        return parts.join(', ') || 'No additional details available';
    }

    extractBrandExperience(dealFields) {
        const hasExperience = dealFields['Have you worked with any of your preferred brands/operators before?'] === 'Yes';
        const brandNames = dealFields['Brands/Operators Names before'];
        
        if (hasExperience && brandNames) {
            return `Previous experience with: ${brandNames}`;
        }
        
        return hasExperience ? 'Has previous brand experience' : 'New to branded hotels';
    }

    extractSpecialConsiderations(dealFields) {
        const considerations = [];
        
        if (dealFields['Are you open to lesser-known or emerging brands with favorable terms?'] === 'Yes') {
            considerations.push('Open to emerging brands');
        }
        
        if (dealFields['Green Roof / Living Wall']) {
            considerations.push('Sustainability focus');
        }
        
        if (dealFields['Condo Residences?'] === 'Yes') {
            considerations.push('Mixed-use development');
        }
        
        return considerations.join(', ') || 'Standard requirements';
    }

    determineUserTitle(fields) {
        const userType = fields['User Type'] || 'Hotel Owner';
        
        if (userType === 'Hotel Brand') {
            return 'Brand Representative';
        } else if (userType === 'Hotel Owner') {
            return 'Property Owner';
        }
        
        return 'Property Owner';
    }

    extractUserExperience(fields) {
        const regions = fields['HO - PI - Regions Where You Operate / Invest'];
        const companyName = fields['Company Name'];
        
        if (regions && regions.length > 0) {
            return `Operates in: ${regions.join(', ')}`;
        }
        
        return companyName || 'Independent operator';
    }

    calculateUserEngagement(fields) {
        // Calculate engagement based on profile completion and activity
        const profileComplete = fields['Profile'] && fields['Profile'].length > 0;
        const hasCompany = fields['Company Name'];
        const hasPhone = fields['Phone Number'];
        
        let completionScore = 0;
        if (profileComplete) completionScore += 3;
        if (hasCompany) completionScore += 2;
        if (hasPhone) completionScore += 1;
        
        if (completionScore >= 5) return 'fast';
        if (completionScore >= 3) return 'medium';
        return 'slow';
    }

    countAmenities(dealFields) {
        const amenityFields = [
            'Lobby', 'Co-working or lounge space', 'Bar or Beverage Concept',
            'Fitness Center', 'Pool', 'Meeting/Event Space', 'Business Center',
            'Solar Power', 'Water Recycling System', 'Green Roof / Living Wall'
        ];
        
        let count = 0;
        amenityFields.forEach(field => {
            if (dealFields[field]) count++;
        });
        
        return count;
    }

    // Tab and filtering logic
    async applyTabFilter() {
        const tabStatuses = {
            'new': ['new', 'viewed-by-brand', 'deal-received'],
            'active': ['closed-won', 'closed-lost', 'awarded', 'in-negotiation', 'shortlisting', 'in-discovery', 'on-hold', 'deferred', 'expired', 'archived'],
            'archives': ['archived', 'expired', 'closed-lost'],
            'deal-log': ['all']
        };

        const currentStatuses = tabStatuses[this.currentTab] || [];
        
        if (this.currentTab === 'deal-log') {
            this.filteredDeals = [...this.deals];
        } else {
            this.filteredDeals = this.deals.filter(deal => 
                currentStatuses.includes(deal.status)
            );
        }
        
        await this.renderDeals();
    }

    // Optimized render deals with performance tracking
    async renderDeals() {
        return await this.timeOperation('Render Deals', async () => {
        const container = document.getElementById('dealsContainer');
        
        if (this.filteredDeals.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <h3>No deals found</h3>
                    <p>There are no deals matching your current filter criteria.</p>
                </div>
            `;
            return;
        }

            // Apply current sort if one is active
            if (this.currentSort.field && this.currentSort.direction) {
                this.filteredDeals.sort((a, b) => {
                    let aValue, bValue;
                    
                    switch (this.currentSort.field) {
                        case 'status':
                            aValue = a.status || '';
                            bValue = b.status || '';
                            break;
                        case 'brandMatch':
                            aValue = a.brandMatch || '';
                            bValue = b.brandMatch || '';
                            break;
                        case 'matchScore':
                            aValue = parseInt(a.matchScore) || 0;
                            bValue = parseInt(b.matchScore) || 0;
                            break;
                        default:
                            return 0;
                    }
                    
                    // Handle string comparison
                    if (typeof aValue === 'string' && typeof bValue === 'string') {
                        aValue = aValue.toLowerCase();
                        bValue = bValue.toLowerCase();
                    }
                    
                    if (this.currentSort.direction === 'asc') {
                        return aValue > bValue ? 1 : aValue < bValue ? -1 : 0;
                    } else {
                        return aValue < bValue ? 1 : aValue > bValue ? -1 : 0;
                    }
                });
            }

            // Use document fragment for better performance
            const fragment = document.createDocumentFragment();
            const tempDiv = document.createElement('div');
            
            // Batch DOM updates with async rendering
            const dealsHtmlPromises = this.filteredDeals.map(deal => this.renderDealRow(deal));
            const dealsHtml = await Promise.all(dealsHtmlPromises);
            tempDiv.innerHTML = dealsHtml.join('');
            
            // Move all child nodes to fragment
            while (tempDiv.firstChild) {
                fragment.appendChild(tempDiv.firstChild);
            }
            
            // Single DOM update
            container.innerHTML = '';
            container.appendChild(fragment);
            
            // Re-apply any cached contact images after rendering
            this.reapplyCachedContactImages();
            
            this.debugLog(`Rendered ${this.filteredDeals.length} deals`);
        });
    }

    // Fetch user profile image from either User Management or Users table
    async fetchUserProfileImage(userId) {
        if (!userId) return null;
        
        // Try User Management table first
        try {
            const url = `${this.getApiBaseUrl()}/api/user-management/profile-image?userId=${encodeURIComponent(userId)}`;
            const response = await this.fetchWithRateLimit(url, {
                method: 'GET',
                headers: {
                    'Accept': 'application/json',
                    'X-Frontend-Auth-Disabled': 'true',
                },
            });
            
            if (response.ok) {
                const data = await response.json();
                const profileImage = data.fields?.['Profile']?.[0]?.url || null;
                if (profileImage) {
                    this.debugLog(`✅ Found profile image in User Management table for user ${userId}:`, profileImage);
                    return profileImage;
                }
            }
        } catch (error) {
            this.debugLog(`User Management table not accessible for user ${userId}:`, error, 'warn');
        }
        
        // Try Users table if User Management didn't work
        try {
            const url = `${this.getApiBaseUrl()}/api/user-management/profile-image?userId=${encodeURIComponent(userId)}`;
            this.debugLog(`🔍 FETCHING from Users table: ${url}`);
            const response = await this.fetchWithRateLimit(url, {
                method: 'GET',
                headers: {
                    'Accept': 'application/json',
                    'X-Frontend-Auth-Disabled': 'true',
                },
            });
            
            this.debugLog(`📡 Users table response status: ${response.status} for user ${userId}`);
            
            if (response.ok) {
                const data = await response.json();
                this.debugLog(`📋 Users table data for ${userId}:`, data);
                const profileImage = data.fields?.['Profile']?.[0]?.url || null;
                this.debugLog(`🖼️ Profile field value for ${userId}:`, data.fields?.['Profile']);
                if (profileImage) {
                    this.debugLog(`✅ Found profile image in Users table for user ${userId}:`, profileImage);
                    return profileImage;
                } else {
                    this.debugLog(`⚠️ No profile image found in Users table for user ${userId} - Profile field:`, data.fields?.['Profile']);
                }
            } else {
                this.debugLog(`❌ Users table request failed for user ${userId}: ${response.status} ${response.statusText}`);
            }
        } catch (error) {
            this.debugLog(`💥 Users table not accessible for user ${userId}:`, error, 'warn');
        }
        
        this.debugLog(`❌ No profile image found for user ${userId} in either table`);
        return null;
    }

    getContactImage(deal) {
        console.log(`🔍 getContactImage called for deal ${deal.id} - Contact: ${deal.ownerName}`);
        
        // Special debugging for Maria Elena Vargas
        if (deal.ownerName && deal.ownerName.toLowerCase().includes('maria elena vargas')) {
            console.log(`🎯 ===== MARIA ELENA VARGAS DEBUGGING =====`);
            console.log(`🎯 Deal ID: ${deal.id}`);
            console.log(`🎯 Contact Name: ${deal.ownerName}`);
            console.log(`🎯 Owner Email: ${deal.ownerEmail}`);
            console.log(`🎯 Contact Data:`, deal.contactData);
            console.log(`🎯 Deal Object:`, deal);
        }
        
        // Check if we already have the image cached in the deal data
        if (deal.contactImage) {
            console.log(`📦 Using cached contact image for deal ${deal.id}: ${deal.contactImage}`);
            return deal.contactImage;
        }
        
        // Track statistics
        if (!window.contactImageStats) {
            window.contactImageStats = { total: 0, withEmail: 0, withoutEmail: 0, successful: 0, failed: 0, cached: 0 };
        }
        window.contactImageStats.total++;
        
        // Enhanced email extraction logic
        let emailAddress = this.extractEmailFromDeal(deal);
        
        console.log(`📧 Final email address: ${emailAddress}`);
        console.log(`📧 Deal ownerEmail: ${deal.ownerEmail}`);
        console.log(`📧 Contact data available: ${!!deal.contactData}`);
        if (deal.contactData) {
            console.log(`📧 Available contact fields:`, Object.keys(deal.contactData));
        }
        
        if (!emailAddress) {
            window.contactImageStats.withoutEmail++;
            console.log(`❌ No email address found for deal ${deal.id}`);
            console.log(`📋 Deal ownerEmail: ${deal.ownerEmail}`);
            console.log(`📋 Contact data:`, deal.contactData);
            console.log(`📊 Stats: ${window.contactImageStats.withEmail}/${window.contactImageStats.total} contacts have emails`);
        return null;
    }

        window.contactImageStats.withEmail++;
        console.log(`📊 Stats: ${window.contactImageStats.withEmail}/${window.contactImageStats.total} contacts have emails`);
        
        // Special logging for Maria Elena Vargas
        if (deal.ownerName && deal.ownerName.toLowerCase().includes('maria elena vargas')) {
            console.log(`🎯 Maria Elena Vargas - About to fetch profile image for email: ${emailAddress}`);
        }
        
        // Start the image fetch process asynchronously
        // Use setTimeout to ensure this doesn't block the rendering
        setTimeout(() => {
            this.fetchProfileImageByEmail(emailAddress, deal.id);
        }, Math.random() * 1000); // Add random delay to spread out requests
        
        return null; // Return null initially - the UI will show fallback and update when image loads
    }

    extractEmailFromDeal(deal) {
        // First, try to use the ownerEmail that was already processed from contactData
        let emailAddress = deal.ownerEmail;
        
        // If ownerEmail is not available, try to find email in contactData directly
        if (!emailAddress && deal.contactData) {
            const emailFields = [
                'Email Address', 'Email', 'Email_Address', 'A Email Address', 
                'A Main Contact Email', 'Main Contact Email', 'Contact Email',
                'Primary Email', 'Email Address 1', 'Contact Email Address',
                'Email Address 2', 'Secondary Email', 'Business Email',
                'Company Email', 'Work Email', 'Professional Email',
                'Contact Email Address 1', 'Contact Email Address 2',
                'Main Email', 'Primary Contact Email', 'Owner Email',
                'Deal Contact Email', 'Property Contact Email'
            ];
            
            for (const field of emailFields) {
                if (deal.contactData[field]) {
                    emailAddress = deal.contactData[field];
                    console.log(`📧 Found email in contactData field "${field}": ${emailAddress}`);
                    break;
                }
            }
        }
        
        // If still no email, try to extract from the deal's airtableData
        if (!emailAddress && deal.airtableData) {
            const dealEmailFields = [
                'Contact Email', 'Email', 'Email Address', 'Primary Email',
                'Contact Email Address', 'Business Email', 'Company Email',
                'Owner Email', 'Deal Contact Email', 'Property Contact Email',
                'Main Contact Email', 'Contact Email Address 1', 'Contact Email Address 2',
                'Work Email', 'Professional Email', 'Secondary Email'
            ];
            
            for (const field of dealEmailFields) {
                if (deal.airtableData[field]) {
                    emailAddress = deal.airtableData[field];
                    console.log(`📧 Found email in deal field "${field}": ${emailAddress}`);
                    break;
                }
            }
        }
        
        // Clean and validate the email address
        if (emailAddress) {
            emailAddress = emailAddress.toString().trim();
            
            // Basic email validation
            const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
            if (!emailRegex.test(emailAddress)) {
                console.log(`⚠️ Invalid email format: ${emailAddress}`);
                return null;
            }
        }
        
        return emailAddress;
    }

    async fetchProfileImageByEmail(emailAddress, dealId) {
        console.log(`🔍 Searching for profile image by email: ${emailAddress} for deal ${dealId}`);
        
        // Find the deal to get contact name for debugging
        const deal = this.filteredDeals.find(d => d.id === dealId) || this.deals.find(d => d.id === dealId);
        const contactName = deal ? deal.ownerName : 'Unknown';
        
        // Special debugging for Maria Elena Vargas
        if (emailAddress && (emailAddress.toLowerCase().includes('maria') || emailAddress.toLowerCase().includes('vargas')) || 
            contactName && contactName.toLowerCase().includes('maria elena vargas')) {
            console.log(`🎯 ===== MARIA ELENA VARGAS PROFILE IMAGE SEARCH =====`);
            console.log(`🎯 Email Address: ${emailAddress}`);
            console.log(`🎯 Deal ID: ${dealId}`);
            console.log(`🎯 Contact Name: ${contactName}`);
        }
        
        // Validate and clean email address
        if (!emailAddress || typeof emailAddress !== 'string') {
            console.warn(`⚠️ Invalid email address provided:`, emailAddress);
            return;
        }
        
        // Clean email address - trim whitespace and convert to lowercase
        const cleanEmail = emailAddress.trim().toLowerCase();
        console.log(`🔍 Cleaned email address: "${cleanEmail}"`);
        
        // Special logging for Maria Elena Vargas
        if (cleanEmail.includes('maria') || cleanEmail.includes('vargas') || 
            contactName && contactName.toLowerCase().includes('maria elena vargas')) {
            console.log(`🎯 Maria Elena Vargas - Cleaned email: "${cleanEmail}"`);
        }
        
        // Initialize success tracking
        if (!window.contactImageStats.successful) window.contactImageStats.successful = 0;
        if (!window.contactImageStats.failed) window.contactImageStats.failed = 0;
        if (!window.contactImageStats.cached) window.contactImageStats.cached = 0;
        
        // Check cache first to avoid duplicate API calls
        const cacheKey = `profile_${cleanEmail}`;
        const cachedImage = this.getFromCache(this.cache.userData, cacheKey);
        if (cachedImage) {
            console.log(`📦 Using cached profile image for ${cleanEmail}`);
            window.contactImageStats.cached++;
            this.updateContactImage(dealId, cachedImage);
            return;
        }
        
        // Check if we're already processing this email to avoid duplicate requests
        if (this.cache.pendingRequests && this.cache.pendingRequests.has(cacheKey)) {
            console.log(`⏳ Already processing profile image request for ${cleanEmail}, skipping duplicate`);
            return;
        }
        
        // Mark this request as pending
        if (!this.cache.pendingRequests) {
            this.cache.pendingRequests = new Set();
        }
        this.cache.pendingRequests.add(cacheKey);
        
        // Escape special characters in email for Airtable formula
        const escapedEmail = cleanEmail.replace(/'/g, "\\'");
        
        try {
            // Try Users table first - use Email field, get Profile image
            const usersUrl = `${this.getApiBaseUrl()}/api/user-management/profile-image?email=${encodeURIComponent(cleanEmail)}`;
            console.log(`🔍 Searching Users table: ${usersUrl}`);
            
            const usersResponse = await this.fetchWithRateLimit(usersUrl, {
                method: 'GET',
                headers: {
                    'Accept': 'application/json',
                    'X-Frontend-Auth-Disabled': 'true',
                },
            }, 600); // Much higher delay to reduce rate limiting
            
            if (usersResponse.ok) {
                const usersData = await usersResponse.json();
                console.log(`📋 Users table search results for ${cleanEmail}:`, usersData);
                
                if (usersData.records && usersData.records.length > 0) {
                    const userRecord = usersData.records[0];
                    const profileImage = userRecord.fields?.Profile?.[0]?.url;
                    
                    if (profileImage) {
                        console.log(`✅ Found profile image in Users table for ${cleanEmail}:`, profileImage);
                        window.contactImageStats.successful++;
                        this.setToCache(this.cache.userData, cacheKey, profileImage);
                        this.updateContactImage(dealId, profileImage);
                        this.cache.pendingRequests.delete(cacheKey);
                        return;
                    } else {
                        console.log(`⚠️ User found in Users table but no Profile image for ${cleanEmail}`);
                    }
                } else {
                    console.log(`❌ No user found in Users table for ${cleanEmail}`);
                }
            } else {
                console.log(`⚠️ Users table API call failed with status ${usersResponse.status} for ${cleanEmail}`);
            }
            
            // Try User Management table - use Company Email field, get Profile image
            const userMgmtUrl = `${this.getApiBaseUrl()}/api/user-management/profile-image?email=${encodeURIComponent(cleanEmail)}`;
            console.log(`🔍 Searching User Management table: ${userMgmtUrl}`);
            
            const userMgmtResponse = await this.fetchWithRateLimit(userMgmtUrl, {
                method: 'GET',
                headers: {
                    'Accept': 'application/json',
                    'X-Frontend-Auth-Disabled': 'true',
                },
            }, 600); // Much higher delay to reduce rate limiting
            
            if (userMgmtResponse.ok) {
                const userMgmtData = await userMgmtResponse.json();
                console.log(`📋 User Management table search results for ${cleanEmail}:`, userMgmtData);
                
                if (userMgmtData.records && userMgmtData.records.length > 0) {
                    const userMgmtRecord = userMgmtData.records[0];
                    const profileImage = userMgmtRecord.fields?.Profile?.[0]?.url;
                    
                    if (profileImage) {
                        console.log(`✅ Found profile image in User Management table for ${cleanEmail}:`, profileImage);
                        window.contactImageStats.successful++;
                        this.setToCache(this.cache.userData, cacheKey, profileImage);
                        this.updateContactImage(dealId, profileImage);
                        this.cache.pendingRequests.delete(cacheKey);
                        return;
                    } else {
                        console.log(`⚠️ User found in User Management table but no Profile image for ${cleanEmail}`);
                    }
                } else {
                    console.log(`❌ No user found in User Management table for ${cleanEmail}`);
                }
            } else {
                console.log(`⚠️ User Management table API call failed with status ${userMgmtResponse.status} for ${cleanEmail}`);
            }
            
                   console.log(`❌ No profile image found for email ${cleanEmail} in either table`);
                   
                   // Additional debugging for failed lookups
                   if (cleanEmail.includes('test.com') || cleanEmail.includes('example.com') || cleanEmail.includes('@test')) {
                       console.log(`🧪 Test email detected: ${cleanEmail} - this is expected to fail`);
                   } else {
                       console.log(`⚠️ Real email not found: ${cleanEmail} - user may not exist in Airtable tables`);
                   }
                   
                   window.contactImageStats.failed++;
                   this.setToCache(this.cache.userData, cacheKey, null);
                   this.cache.pendingRequests.delete(cacheKey);
            
        } catch (error) {
            console.log(`⚠️ Error searching for profile image for ${cleanEmail}:`, error);
            
            // Check if this is a rate limiting error and we should retry
            if (error.message && error.message.includes('Rate limited') && !this.cache.retryAttempts) {
                this.cache.retryAttempts = new Map();
            }
            
            const retryKey = `${cleanEmail}_${dealId}`;
            const retryCount = this.cache.retryAttempts ? this.cache.retryAttempts.get(retryKey) || 0 : 0;
            
            if (retryCount < 3) {
                console.log(`🔄 Retrying image fetch for ${cleanEmail} (attempt ${retryCount + 1}/3) in 5 seconds...`);
                this.cache.retryAttempts.set(retryKey, retryCount + 1);
                
                setTimeout(() => {
                    this.fetchProfileImageByEmail(emailAddress, dealId);
                }, 5000 + (retryCount * 2000)); // Increasing delay
                return;
            }
            
            window.contactImageStats.failed++;
            this.setToCache(this.cache.userData, cacheKey, null);
            this.cache.pendingRequests.delete(cacheKey);
        }
    }

    updateContactImage(dealId, profileImageUrl, skipDataStorage = false) {
        console.log(`🖼️ Updating contact image for deal ${dealId}:`, profileImageUrl);
        
        // Find the deal to get contact name for debugging
        const deal = this.filteredDeals.find(d => d.id === dealId) || this.deals.find(d => d.id === dealId);
        const contactName = deal ? deal.ownerName : 'Unknown';
        
        // Special debugging for Maria Elena Vargas
        if (contactName && contactName.toLowerCase().includes('maria elena vargas')) {
            console.log(`🎯 ===== MARIA ELENA VARGAS IMAGE UPDATE =====`);
            console.log(`🎯 Deal ID: ${dealId}`);
            console.log(`🎯 Contact Name: ${contactName}`);
            console.log(`🎯 Image URL: ${profileImageUrl}`);
            console.log(`🎯 Skip Data Storage: ${skipDataStorage}`);
        }
        
        // Validate the image URL
        if (!profileImageUrl || typeof profileImageUrl !== 'string') {
            console.log(`❌ Invalid profile image URL for deal ${dealId}:`, profileImageUrl);
            return;
        }
        
        // Store the image in the deal data for persistence across re-renders (unless we're just re-applying)
        if (!skipDataStorage) {
            if (deal) {
                deal.contactImage = profileImageUrl;
                console.log(`💾 Stored contact image in deal data for ${dealId}`);
                
                // Also update all other deals that share the same contact email
                const contactEmail = this.extractEmailFromDeal(deal);
                if (contactEmail) {
                    const relatedDeals = this.deals.filter(d => {
                        const otherEmail = this.extractEmailFromDeal(d);
                        return otherEmail === contactEmail && d.id !== dealId;
                    });
                    
                    relatedDeals.forEach(relatedDeal => {
                        relatedDeal.contactImage = profileImageUrl;
                        console.log(`💾 Updated contact image for related deal ${relatedDeal.id}`);
                    });
                    
                    if (relatedDeals.length > 0) {
                        console.log(`🔄 Updated contact image for ${relatedDeals.length} related deals sharing email: ${contactEmail}`);
                    }
                }
            } else {
                console.log(`⚠️ Could not find deal ${dealId} in deals array`);
            }
        }
        
        // Use a more robust approach to find and update the DOM elements
        const updateElement = () => {
            // Find the specific deal row first
            let dealRow = document.querySelector(`[data-deal-id="${dealId}"]`);
            if (!dealRow) {
                console.log(`❌ Deal row not found for deal ${dealId}`);
                return false;
            }
            
            // If we have contact email, also update all related deals in the DOM
            if (deal && !skipDataStorage) {
                const contactEmail = this.extractEmailFromDeal(deal);
                if (contactEmail) {
                    // Find all deals that share the same contact email
                    const allDealRows = document.querySelectorAll('[data-deal-id]');
                    allDealRows.forEach(row => {
                        const rowDealId = row.getAttribute('data-deal-id');
                        const rowDeal = this.deals.find(d => d.id === rowDealId) || this.filteredDeals.find(d => d.id === rowDealId);
                        
                        if (rowDeal) {
                            const rowEmail = this.extractEmailFromDeal(rowDeal);
                            if (rowEmail === contactEmail && rowDealId !== dealId) {
                                console.log(`🔄 Updating DOM for related deal ${rowDealId} with shared email: ${contactEmail}`);
                                this.updateSingleDealRowImage(row, profileImageUrl, rowDealId);
                            }
                        }
                    });
                }
            }
            
            const avatarContainer = dealRow.querySelector('.contact-avatar');
            const avatarElement = avatarContainer?.querySelector('img');
            const fallbackElement = avatarContainer?.querySelector('.contact-avatar-fallback');
            
            console.log(`🔍 Looking for avatar elements for deal ${dealId}:`);
            console.log(`  - Deal row found:`, !!dealRow);
            console.log(`  - Avatar container found:`, !!avatarContainer);
            console.log(`  - Avatar element found:`, !!avatarElement);
            console.log(`  - Fallback element found:`, !!fallbackElement);
            
            if (avatarContainer && avatarElement && fallbackElement) {
                console.log(`🎯 Found all elements for deal ${dealId}, updating image...`);
                
                // Create a new image element to test the URL first
                const testImg = new Image();
                testImg.onload = function() {
                    console.log(`✅ Image URL is valid for deal ${dealId}, updating DOM`);
                    
                    // Set up event handlers for the actual avatar element
                    avatarElement.onload = function() {
                        console.log(`✅ Avatar image loaded successfully for deal ${dealId}`);
                        this.style.display = 'block';
                        fallbackElement.style.display = 'none';
                    };
                    
                    avatarElement.onerror = function() {
                        console.log(`❌ Avatar image failed to load for deal ${dealId}:`, this.src);
                        this.style.display = 'none';
                        fallbackElement.style.display = 'flex';
                    };
                    
                    // Set the image source
                    avatarElement.src = profileImageUrl;
                    avatarElement.style.display = 'block';
                    fallbackElement.style.display = 'none';
                };
                
                testImg.onerror = function() {
                    console.log(`❌ Image URL is invalid for deal ${dealId}:`, profileImageUrl);
                    avatarElement.style.display = 'none';
                    fallbackElement.style.display = 'flex';
                };
                
                // Test the image URL
                testImg.src = profileImageUrl;
                console.log(`✅ Contact image update initiated for deal ${dealId}`);
                return true;
            } else {
                console.log(`⚠️ Could not find avatar elements for deal ${dealId}`);
                if (dealRow) {
                    console.log(`🔍 Deal row HTML snippet:`, dealRow.outerHTML.substring(0, 500) + '...');
                    
                    // Try to find any contact-avatar elements in the row
                    const allAvatarElements = dealRow.querySelectorAll('.contact-avatar');
                    console.log(`🔍 All .contact-avatar elements found:`, allAvatarElements.length);
                    allAvatarElements.forEach((el, index) => {
                        console.log(`🔍 Avatar element ${index}:`, el.outerHTML.substring(0, 200) + '...');
                    });
                }
                return false;
            }
        };
        
        // Try to update immediately
        if (!updateElement()) {
            // If elements not found, wait a bit and try again (for cases where DOM is still updating)
            console.log(`⏳ Elements not found, retrying in 100ms...`);
            setTimeout(() => {
                if (!updateElement()) {
                    console.log(`⏳ Still not found, retrying in 500ms...`);
                    setTimeout(() => {
                        if (!updateElement()) {
                            console.log(`⚠️ Still could not find elements after multiple retries - image stored in deal data for next render`);
                        }
                    }, 500);
                }
            }, 100);
        }
    }

    // Helper function to update a single deal row's contact image
    updateSingleDealRowImage(dealRow, profileImageUrl, dealId) {
        console.log(`🖼️ Updating single deal row image for deal ${dealId}`);
        
        const avatarContainer = dealRow.querySelector('.contact-avatar');
        const avatarElement = avatarContainer?.querySelector('img');
        const fallbackElement = avatarContainer?.querySelector('.contact-avatar-fallback');
        
        if (avatarContainer && avatarElement && fallbackElement) {
            console.log(`🎯 Found avatar elements for deal ${dealId}, updating image...`);
            
            // Create a new image element to test the URL first
            const testImg = new Image();
            testImg.onload = function() {
                console.log(`✅ Image URL is valid for deal ${dealId}, updating DOM`);
                
                // Set up event handlers for the actual avatar element
                avatarElement.onload = function() {
                    console.log(`✅ Avatar image loaded successfully for deal ${dealId}`);
                    this.style.display = 'block';
                    fallbackElement.style.display = 'none';
                };
                
                avatarElement.onerror = function() {
                    console.log(`❌ Avatar image failed to load for deal ${dealId}:`, this.src);
                    this.style.display = 'none';
                    fallbackElement.style.display = 'flex';
                };
                
                // Set the image source
                avatarElement.src = profileImageUrl;
                avatarElement.style.display = 'block';
                fallbackElement.style.display = 'none';
            };
            
            testImg.onerror = function() {
                console.log(`❌ Image URL is invalid for deal ${dealId}:`, profileImageUrl);
                avatarElement.style.display = 'none';
                fallbackElement.style.display = 'flex';
            };
            
            // Test the image URL
            testImg.src = profileImageUrl;
            console.log(`✅ Contact image update initiated for deal ${dealId}`);
            return true;
        } else {
            console.log(`⚠️ Could not find avatar elements for deal ${dealId}`);
            return false;
        }
    }

    showContactImageStats() {
        if (!window.contactImageStats) {
            console.log('📊 No contact image statistics available yet');
            return;
        }
        
        const stats = window.contactImageStats;
        const total = stats.total || 0;
        const withEmail = stats.withEmail || 0;
        const withoutEmail = stats.withoutEmail || 0;
        const successful = stats.successful || 0;
        const failed = stats.failed || 0;
        const cached = stats.cached || 0;
        
        console.log('📊 ===== CONTACT IMAGE STATISTICS =====');
        console.log(`📊 Total contacts processed: ${total}`);
        console.log(`📊 Contacts with email addresses: ${withEmail} (${((withEmail/total)*100).toFixed(1)}%)`);
        console.log(`📊 Contacts without email addresses: ${withoutEmail} (${((withoutEmail/total)*100).toFixed(1)}%)`);
        console.log(`📊 Images successfully loaded: ${successful} (${((successful/withEmail)*100).toFixed(1)}% of contacts with emails)`);
        console.log(`📊 Images failed to load: ${failed} (${((failed/withEmail)*100).toFixed(1)}% of contacts with emails)`);
        console.log(`📊 Images loaded from cache: ${cached}`);
        console.log(`📊 Success rate: ${((successful/(successful+failed))*100).toFixed(1)}%`);
        console.log('📊 ===== END CONTACT IMAGE STATISTICS =====');
        
        // Also show in an alert for easy viewing
        const message = `Contact Image Statistics:
Total contacts: ${total}
With email: ${withEmail} (${((withEmail/total)*100).toFixed(1)}%)
Without email: ${withoutEmail} (${((withoutEmail/total)*100).toFixed(1)}%)
Images loaded: ${successful} (${((successful/withEmail)*100).toFixed(1)}% of contacts with emails)
Failed to load: ${failed} (${((failed/withEmail)*100).toFixed(1)}% of contacts with emails)
From cache: ${cached}
Success rate: ${((successful/(successful+failed))*100).toFixed(1)}%`;
        
        alert(message);
    }

    // New debugging function to analyze contact image issues
    debugContactImages() {
        console.log('🔍 ===== CONTACT IMAGE DEBUG ANALYSIS =====');
        
        const dealsWithImages = this.deals.filter(deal => deal.contactImage);
        const dealsWithoutImages = this.deals.filter(deal => !deal.contactImage);
        const dealsWithEmails = this.deals.filter(deal => deal.ownerEmail);
        const dealsWithoutEmails = this.deals.filter(deal => !deal.ownerEmail);
        
        console.log(`📊 Total deals: ${this.deals.length}`);
        console.log(`📊 Deals with contact images: ${dealsWithImages.length}`);
        console.log(`📊 Deals without contact images: ${dealsWithoutImages.length}`);
        console.log(`📊 Deals with emails: ${dealsWithEmails.length}`);
        console.log(`📊 Deals without emails: ${dealsWithoutEmails.length}`);
        
        // Analyze deals without images but with emails
        const dealsNeedingImages = dealsWithoutImages.filter(deal => deal.ownerEmail);
        console.log(`📊 Deals needing images (have email but no image): ${dealsNeedingImages.length}`);
        
        if (dealsNeedingImages.length > 0) {
            console.log('🔍 Deals that should have images but don\'t:');
            dealsNeedingImages.slice(0, 5).forEach(deal => {
                console.log(`  - ${deal.ownerName} (${deal.ownerEmail}) - Deal ID: ${deal.id}`);
            });
            if (dealsNeedingImages.length > 5) {
                console.log(`  ... and ${dealsNeedingImages.length - 5} more`);
            }
        }
        
        // Check for specific contacts mentioned by user
        const targetContacts = [
            'Olive Ray', 'Preetam Skyram', 'Ana Silva', 'David Chen', 
            'Maria Elena Vargas', 'Ryan Murphy'
        ];
        
        console.log('🎯 ===== SPECIFIC CONTACT ANALYSIS =====');
        targetContacts.forEach(contactName => {
            const deal = this.deals.find(deal => 
                deal.ownerName && deal.ownerName.toLowerCase().includes(contactName.toLowerCase())
            );
            
            if (deal) {
                console.log(`🎯 ${contactName} analysis:`);
                console.log(`  - Deal ID: ${deal.id}`);
                console.log(`  - Owner Name: ${deal.ownerName}`);
                console.log(`  - Owner Email: ${deal.ownerEmail}`);
                console.log(`  - Contact Image: ${deal.contactImage || 'None'}`);
                console.log(`  - Contact Data:`, deal.contactData);
                
                const extractedEmail = this.extractEmailFromDeal(deal);
                console.log(`  - Extracted Email: ${extractedEmail}`);
                
                // Check if image was loaded
                const avatarElement = document.querySelector(`.contact-avatar[data-deal-id="${deal.id}"] img`);
                if (avatarElement) {
                    console.log(`  - Avatar Element Found:`, avatarElement);
                    console.log(`  - Avatar Src: ${avatarElement.src}`);
                    console.log(`  - Avatar Complete: ${avatarElement.complete}`);
                    console.log(`  - Avatar Natural Width: ${avatarElement.naturalWidth}`);
                    console.log(`  - Avatar Natural Height: ${avatarElement.naturalHeight}`);
                } else {
                    console.log(`  - No Avatar Element Found for deal ${deal.id}`);
                }
                console.log('---');
            } else {
                console.log(`🎯 ${contactName}: NOT FOUND in deals data`);
            }
        });
        
        console.log('🔍 ===== END CONTACT IMAGE DEBUG ANALYSIS =====');
    }

    // Function to manually analyze specific contacts
    analyzeSpecificContacts() {
        console.log('🔍 ===== MANUAL CONTACT ANALYSIS =====');
        this.debugContactImages();
        console.log('🔍 ===== END MANUAL CONTACT ANALYSIS =====');
    }

    // Function to manually retry failed image loads
    retryFailedImageLoads() {
        console.log('🔄 ===== RETRYING FAILED IMAGE LOADS =====');
        
        const targetContacts = [
            'Olive Ray', 'Preetam Skyram', 'Ana Silva', 'David Chen',
            'Maria Elena Vargas', 'Ryan Murphy', 'Pierre Dubois'
        ];
        
        let retryCount = 0;
        
        targetContacts.forEach(contactName => {
            const deal = this.deals.find(deal =>
                deal.ownerName && deal.ownerName.toLowerCase().includes(contactName.toLowerCase())
            );
            
            if (deal) {
                console.log(`🔄 Retrying image load for ${contactName} (Deal ID: ${deal.id})`);
                
                // Clear any cached failed attempts
                if (this.cache.retryAttempts) {
                    const retryKey = `${deal.ownerEmail}_${deal.id}`;
                    this.cache.retryAttempts.delete(retryKey);
                }
                
                // Clear any cached null results
                if (this.cache.userData) {
                    const cacheKey = `profile_${deal.ownerEmail}`;
                    this.cache.userData.delete(cacheKey);
                }
                
                // Force retry the image load
                setTimeout(() => {
                    this.getContactImage(deal);
                }, retryCount * 2000); // Stagger the retries
                
                retryCount++;
            }
        });
        
        console.log(`🔄 Initiated retry for ${retryCount} contacts`);
        console.log('🔄 ===== END RETRY ATTEMPTS =====');
    }

    reapplyCachedContactImages() {
        console.log(`🔄 Re-applying cached contact images after render`);
        
        this.filteredDeals.forEach(deal => {
            if (deal.contactImage) {
                console.log(`🖼️ Re-applying cached image for deal ${deal.id}`);
                this.updateContactImage(deal.id, deal.contactImage, true); // Skip data storage since it's already stored
            }
        });
        
        // Also check for any pending image loads that might have completed
        setTimeout(() => {
            this.reapplyContactImages();
        }, 1000);
    }
    
    // Helper function to reapply contact images with better timing
    reapplyContactImages() {
        console.log(`🖼️ Reapplying contact images for ${this.filteredDeals.length} deals`);
        this.filteredDeals.forEach(deal => {
            if (deal.contactImage) {
                console.log(`🖼️ Re-applying image for deal ${deal.id}: ${deal.contactImage}`);
                this.updateContactImage(deal.id, deal.contactImage, true);
            }
        });
    }

    async renderDealRow(deal) {
        const statusClass = this.getStatusClass(deal.status);
        const scoreClass = this.getScoreClass(deal.matchScore);
        const respondTimeClass = `respond-${deal.respondTimeColor}`;
        
        // Check if brand is not specified or not in database - if so, gray out the score
        const isBrandNotSpecified = deal.brandMatch === 'Not specified' || !deal.brandMatch;
        const isBrandNotInDatabase = deal.brandNotInDatabase || false; // This will be set during deal processing
        const shouldGrayOut = isBrandNotSpecified || isBrandNotInDatabase;
        const displayScore = shouldGrayOut ? '-' : deal.matchScore;
        const scoreDisplayClass = shouldGrayOut ? 'match-score-disabled' : scoreClass;
        // Get contact image (asynchronous, will update UI later)
        let userImage = null;
        try {
            userImage = await this.getContactImage(deal);
        } catch (error) {
            console.error(`Error getting contact image for deal ${deal.id}:`, error);
        }
        
        // Add row-level class based on status for bold/unbold logic
        const rowStatusClass = deal.status === 'new' ? 'status-new' : 'status-viewed-by-brand';
        
        // Check if this is a NEW deal for special formatting
        const isNewDeal = deal.status === 'new';
        const statusTextStyle = isNewDeal ? 'font-weight: bold; font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px;' : '';
        
        return `
            <div class="table-row ${rowStatusClass}" data-deal-id="${deal.id}">
                <div>
                    <span class="status-text ${statusClass}" style="${statusTextStyle}">${this.getStatusLabel(deal.status)}</span>
                </div>
                <div class="brand-match">${deal.brandMatch}</div>
                <div class="match-score-with-info">
                    <div class="match-score ${scoreDisplayClass}">${displayScore}</div>
                    <button class="match-score-info-btn" onclick="dashboard.showMatchScoreDetails('${deal.id}')" title="View score breakdown" ${shouldGrayOut ? 'disabled' : ''}>
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <circle cx="12" cy="12" r="10"></circle>
                            <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"></path>
                            <line x1="12" y1="17" x2="12.01" y2="17"></line>
                        </svg>
                    </button>
                </div>
                <div class="deal-headline">${deal.headline}</div>
                <div class="deal-contact" style="display: flex; align-items: center; gap: 8px; line-height: 1.0;">
                    <div class="contact-avatar">
                        <img src="${userImage || ''}" alt="${deal.ownerName}" style="display: ${userImage ? 'block' : 'none'};" onerror="this.style.display='none'; this.nextElementSibling.style.display='flex';" onload="this.style.display='block'; this.nextElementSibling.style.display='none';">
                        <div class="contact-avatar-fallback" style="display: ${userImage ? 'none' : 'flex'}; align-items: center; justify-content: center;">
                            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"></path>
                                <circle cx="12" cy="7" r="4"></circle>
                            </svg>
                        </div>
                    </div>
                    <div class="contact-info" style="display: flex; flex-direction: column; gap: 1px; line-height: 1.0;">
                        <div class="contact-name" style="color: white; font-size: 14px; line-height: 1.0; margin: 0;">${deal.ownerName}</div>
                        <div class="contact-title" style="color: white; font-size: 14px; line-height: 1.0; margin: 0; opacity: 0.9;">${deal.ownerTitle}</div>
                        <div class="contact-company" style="color: white; font-size: 14px; line-height: 1.0; margin: 0;">${deal.ownerCompany}</div>
                    </div>
                </div>
                <div class="respond-time ${respondTimeClass}">${deal.respondTime}</div>
                <div class="deal-details">
                        <button class="learn-more-btn" onclick="dashboard.showDealDetails('${deal.id}')">LEARN MORE</button>
                </div>
                <div class="deal-actions">
                    <button class="chat-btn" onclick="dashboard.startChat('${deal.id}')" title="Start Chat">
                        <img src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjQiIGhlaWdodD0iMjQiIHZpZXdCb3g9IjAgMCAyNCAyNCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHJlY3QgeD0iMiIgeT0iMiIgd2lkdGg9IjIwIiBoZWlnaHQ9IjE0IiByeD0iNiIgZmlsbD0iI2FlYjllMSIvPgo8cGF0aCBkPSJNOCAxNkw0IDEyVjE2SDhWMTZaIiBmaWxsPSIjYWViOWUxIi8+CjxjaXJjbGUgY3g9IjkiIGN5PSI5IiByPSIxLjUiIGZpbGw9IndoaXRlIi8+CjxjaXJjbGUgY3g9IjE1IiBjeT0iOSIgcj0iMS41IiBmaWxsPSJ3aGl0ZSIvPgo8Y2lyY2xlIGN4PSIxMiIgY3k9IjkiIHI9IjEuNSIgZmlsbD0id2hpdGUiLz4KPC9zdmc+" alt="Chat" style="width: 30px; height: 30px;" />
                    </button>
                    <button class="email-btn" onclick="dashboard.sendEmail('${deal.id}')" title="Send Email">
                        <img src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjQiIGhlaWdodD0iMjQiIHZpZXdCb3g9IjAgMCAyNCAyNCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHJlY3QgeD0iMiIgeT0iNSIgd2lkdGg9IjIwIiBoZWlnaHQ9IjE0IiByeD0iMiIgZmlsbD0iI2FlYjllMSIgc3Ryb2tlPSIjMDAwIiBzdHJva2Utd2lkdGg9IjIiLz4KPHBhdGggZD0iTTIgNUwxMiAxMkwyMiA1IiBzdHJva2U9IiMwMDAiIHN0cm9rZS13aWR0aD0iMiIgc3Ryb2tlLWxpbmVjYXA9InJvdW5kIiBzdHJva2UtbGluZWpvaW49InJvdW5kIi8+CjxwYXRoIGQ9Ik0yIDJMMTIgMTJMMjIgMiIgc3Ryb2tlPSIjYWViOWUxIiBzdHJva2Utd2lkdGg9IjIiIHN0cm9rZS1saW5lY2FwPSJyb3VuZCIgc3Ryb2tlLWxpbmVqb2luPSJyb3VuZCIvPgo8L3N2Zz4=" alt="Email" />
                    </button>
                    <button class="deal-menu-btn" onclick="dashboard.showDealMenu('${deal.id}')" title="More Options">
                        <img src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjQiIGhlaWdodD0iMjQiIHZpZXdCb3g9IjAgMCAyNCAyNCIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPGNpcmNsZSBjeD0iMTIiIGN5PSI1IiByPSIyIiBmaWxsPSIjYWViOWUxIi8+CjxjaXJjbGUgY3g9IjEyIiBjeT0iMTIiIHI9IjIiIGZpbGw9IiNhZWI5ZTEiLz4KPGNpcmNsZSBjeD0iMTIiIGN5PSIxOSIgcj0iMiIgZmlsbD0iI2FlYjllMSIvPgo8L3N2Zz4=" alt="More Options" />
                    </button>
                </div>
            </div>
        `;
    }

    calculateDealStatus(dealFields, brandId) {
        // Implement the Lifecycle Matrix rules
        const now = new Date();
        
        // A) New Deals tab (first touch) rules
        if (this.isNewDeal(dealFields, brandId)) {
            // 1. New
            if (this.isBrandNewDeal(dealFields, brandId)) {
                return 'new';
            }
            
            // 2. Viewed by Brand
            if (this.isViewedByBrand(dealFields, brandId)) {
                return 'viewed-by-brand';
            }
            
            // 3. Deal Received (stale)
            if (this.isStaleDeal(dealFields, brandId)) {
                return 'deal-received';
            }
        }
        
        // B) Active Deals tab (after brand accepts)
        if (this.isActiveDeal(dealFields, brandId)) {
            // Check in priority order (most advanced first)
            
            // 1. Closed Won
            if (this.isClosedWon(dealFields, brandId)) {
                return 'closed-won';
            }
            
            // 2. Closed Lost
            if (this.isClosedLost(dealFields, brandId)) {
                return 'closed-lost';
            }
            
            // 3. Awarded / Selected
            if (this.isAwarded(dealFields, brandId)) {
                return 'awarded';
            }
            
            // 4. In Negotiation
            if (this.isInNegotiation(dealFields)) {
                return 'in-negotiation';
            }
            
            // 5. Shortlisting
            if (this.isShortlisting(dealFields, brandId)) {
                return 'shortlisting';
            }
            
            // 6. In Discovery
            if (this.isInDiscovery(dealFields)) {
                return 'in-discovery';
            }
            
            // 7. On Hold
            if (this.isOnHold(dealFields)) {
                return 'on-hold';
            }
            
            // 8. Deferred
            if (this.isDeferred(dealFields)) {
                return 'deferred';
            }
            
            // 9. Expired
            if (this.isExpired(dealFields)) {
                return 'expired';
            }
            
            // 10. Archived
            if (this.isArchived(dealFields)) {
                return 'archived';
            }
        }
        
        // Default fallback
        return 'new';
    }

    // New Deals tab helper functions
    isNewDeal(dealFields, brandId) {
        // Deal is in New tab if it hasn't been approved yet
        // We track this by checking if the current user is NOT in the Users Visited list
        // after an approval action
        const usersVisited = dealFields['Users Visited'] || [];
        
        // A deal stays in New Deals tab until it's approved (moved to Active Deals)
        // Within the New Deals tab, the status can be: New → Viewed by Brand → Deal Received
        // Use userId instead of brandId for comparison
        return !usersVisited.includes(this.userId);
    }

    isBrandNewDeal(dealFields, brandId) {
        // According to lifecycle matrix: BrandSubmission.Status = Submitted (Complete) AND BrandView.Count = 0 AND now - FirstVisibleToBrandAt ≤ 14 days
        
        // Since we don't have BrandView.Count, we'll use a different approach:
        // A deal is "New" if it hasn't been viewed yet (based on our isViewedByBrand logic)
        return !this.isViewedByBrand(dealFields, brandId);
    }

    isViewedByBrand(dealFields, brandId) {
        // Check if this brand user has viewed the deal
        // According to lifecycle matrix: BrandView.Count ≥ 1
        
        // Handle case where dealFields might be undefined
        if (!dealFields) {
            return false;
        }
        
        // Since we don't have BrandView.Count field, we'll use a different approach:
        // A deal is "Viewed by Brand" if it has been in the system for a while
        // and the user has had a chance to see it (not brand new)
        
        const lastModified = dealFields['Last Modified'];
        if (!lastModified) {
            return false;
        }
        
        try {
            // If the deal was modified more than 1 hour ago, assume it's been viewed
            // This is a simplified approximation - in a full implementation, we'd track actual views
            const modifiedDate = new Date(lastModified);
            const now = new Date();
            const hoursSinceModified = (now - modifiedDate) / (1000 * 60 * 60);
            
            // Deal is considered "Viewed by Brand" if it's been in the system for more than 1 hour
            // and hasn't been approved yet
            const usersVisited = dealFields['Users Visited'] || [];
            // Use userId instead of brandId for comparison
            return hoursSinceModified > 1 && !usersVisited.includes(this.userId);
        } catch (error) {
            console.error('Error in isViewedByBrand:', error);
            return false;
        }
    }

    isStaleDeal(dealFields, brandId) {
        const firstVisible = dealFields['FirstVisibleToBrandAt'];
        if (!firstVisible) return false;
        
        const visibleDate = new Date(firstVisible);
        const daysSinceVisible = (now - visibleDate) / (1000 * 60 * 60 * 24);
        
        return daysSinceVisible > 14;
    }

    // Active Deals tab helper functions
    isActiveDeal(dealFields, brandId) {
        // Deal is in Active tab only if it has been approved/accepted
        // We track this by checking if the current user is in the Users Visited list
        // after an approval action
        const usersVisited = dealFields['Users Visited'] || [];
        // Use userId instead of brandId for comparison
        return usersVisited.includes(this.userId);
    }

    isClosedWon(dealFields, brandId) {
        const agreementStatus = dealFields['Agreement.Status'];
        const agreementCounterparty = dealFields['Agreement.Counterparty'];
        return agreementStatus === 'Executed' && agreementCounterparty === brandId;
    }

    isClosedLost(dealFields, brandId) {
        const selectionDecision = dealFields['Selection.Decision'];
        const outcome = dealFields['Outcome'];
        
        return selectionDecision === 'Selected for another party' ||
               selectionDecision === 'Not Proceeding' ||
               outcome === 'No Deal';
    }

    isAwarded(dealFields, brandId) {
        const selectionDecision = dealFields['Selection.Decision'];
        const selectionBrandId = dealFields['Selection.BrandId'];
        return selectionDecision === 'Selected' && selectionBrandId === brandId;
    }

    isInNegotiation(dealFields) {
        const negotiationStage = dealFields['Negotiation.Stage'];
        const validStages = ['LOI', 'Term Sheet', 'Draft Agreement'];
        return validStages.includes(negotiationStage);
    }

    isShortlisting(dealFields, brandId) {
        const ownerShortlist = dealFields['OwnerShortlist'] || [];
        const negotiationStage = dealFields['Negotiation.Stage'];
        
        return ownerShortlist.includes(brandId) && !negotiationStage;
    }

    isInDiscovery(dealFields) {
        // A deal is in discovery if it's been approved (active) but no further stages
        const usersVisited = dealFields['Users Visited'] || [];
        const negotiationStage = dealFields['Negotiation.Stage'];
        const selectionDecision = dealFields['Selection.Decision'];
        
        // For now, if a deal is approved (user in Users Visited) and no other stages,
        // it's in discovery phase
        return usersVisited.length > 0 && 
               !negotiationStage && 
               !selectionDecision;
    }

    isOnHold(dealFields) {
        return dealFields['Deal.HoldFlag'] === true;
    }

    isArchivedDeal(dealFields, brandId) {
        // Since there's no "Declined" option in Deal Status, 
        // we'll track declined deals through the interaction system
        // For now, no deals will be archived
        return false;
    }

    isDeferred(dealFields) {
        const expectedStart = dealFields['Timeline.ExpectedStart'];
        const agreementStatus = dealFields['Agreement.Status'];
        
        if (!expectedStart || agreementStatus) return false;
        
        const startDate = new Date(expectedStart);
        const sixMonthsFromNow = new Date();
        sixMonthsFromNow.setMonth(sixMonthsFromNow.getMonth() + 6);
        
        return startDate > sixMonthsFromNow;
    }

    isExpired(dealFields) {
        const expiryDate = dealFields['Visibility.ExpiryDate'];
        const agreementStatus = dealFields['Agreement.Status'];
        
        if (!expiryDate || agreementStatus) return false;
        
        const expiry = new Date(expiryDate);
        return expiry < now;
    }

    isArchived(dealFields) {
        return dealFields['Deal.ArchiveFlag'] === true;
    }

    // Dealality Response Behavior Classification System
    calculateResponseTimeCategory(elapsedMinutes) {
        if (elapsedMinutes <= 60) {
            return { category: 'lightning-fast', label: 'Lightning Fast', icon: '⚡', description: 'Immediate engagement; highly attentive' };
        } else if (elapsedMinutes <= 360) { // 6 hours
            return { category: 'very-fast', label: 'Very Fast', icon: '↗️', description: 'Quick and reliable replies' };
        } else if (elapsedMinutes <= 1440) { // 24 hours
            return { category: 'responsive', label: 'Responsive', icon: '✅', description: 'Normal expected response time' };
        } else if (elapsedMinutes <= 4320) { // 72 hours (3 days)
            return { category: 'slow', label: 'Slow', icon: '⏳', description: 'May indicate lower urgency or limited time' };
        } else if (elapsedMinutes <= 10080) { // 7 days
            return { category: 'stalled', label: 'Stalled', icon: '❌', description: 'Possible disinterest or delay; needs nudge' };
        } else {
            return { category: 'unresponsive', label: 'Unresponsive', icon: '🚫', description: 'No engagement; at risk of deal breakdown' };
        }
    }

    calculateResponseFrequencyCategory(responsePercentage) {
        if (responsePercentage >= 90) {
            return { category: 'frequently', label: 'Frequently', icon: '💬', description: 'Consistently responsive across all activity' };
        } else if (responsePercentage >= 50) {
            return { category: 'occasionally', label: 'Occasionally', icon: '💭', description: 'Mixed response history' };
        } else {
            return { category: 'rarely', label: 'Rarely', icon: '💔', description: 'Often unresponsive; low engagement level' };
        }
    }

    calculateCombinedResponsivenessBadge(userData) {
        // Calculate response time from user engagement data
        const avgResponseTime = this.calculateAverageResponseTime(userData);
        const responseTimeCategory = this.calculateResponseTimeCategory(avgResponseTime);
        
        // Calculate response frequency from user engagement data
        const responseFrequency = this.calculateResponseFrequency(userData);
        const frequencyCategory = this.calculateResponseFrequencyCategory(responseFrequency);
        
        // Create combined badge
        return {
            timeCategory: responseTimeCategory,
            frequencyCategory: frequencyCategory,
            combinedLabel: `${responseTimeCategory.label} + ${frequencyCategory.label}`,
            combinedIcon: `${responseTimeCategory.icon} ${frequencyCategory.icon}`,
            combinedDescription: this.getCombinedDescription(responseTimeCategory.category, frequencyCategory.category),
            score: this.calculateResponsivenessScore(responseTimeCategory.category, frequencyCategory.category)
        };
    }

    calculateAverageResponseTime(userData) {
        // Mock calculation - in real implementation, this would come from interaction tracking
        // For now, we'll use a combination of user engagement metrics
        
        const experience = userData?.experience || 0;
        const responseTime = userData?.responseTime || 'Unknown';
        
        // Convert experience and response time to minutes for calculation
        if (responseTime === 'Excellent') return 60; // 1 hour
        if (responseTime === 'Good') return 240; // 4 hours
        if (responseTime === 'Average') return 720; // 12 hours
        if (responseTime === 'Slow') return 1440; // 24 hours
        if (responseTime === 'Poor') return 2880; // 48 hours
        
        // Default based on experience
        if (experience >= 10) return 120; // 2 hours for experienced users
        if (experience >= 5) return 360; // 6 hours for moderate experience
        return 720; // 12 hours for new users
    }

    calculateResponseFrequency(userData) {
        // Mock calculation - in real implementation, this would come from interaction tracking
        const experience = userData?.experience || 0;
        const responseTime = userData?.responseTime || 'Unknown';
        
        // Calculate based on user engagement profile
        if (responseTime === 'Excellent') return 95;
        if (responseTime === 'Good') return 85;
        if (responseTime === 'Average') return 70;
        if (responseTime === 'Slow') return 45;
        if (responseTime === 'Poor') return 20;
        
        // Default based on experience
        if (experience >= 10) return 90;
        if (experience >= 5) return 75;
        return 60;
    }

    getCombinedDescription(timeCategory, frequencyCategory) {
        const descriptions = {
            'lightning-fast-frequently': 'Great communicator, strong partner - instant responses',
            'lightning-fast-occasionally': 'Very quick when they respond, but not always available',
            'lightning-fast-rarely': 'Quick when they do respond, but very selective',
            'very-fast-frequently': 'Highly responsive and reliable partner',
            'very-fast-occasionally': 'Quick replies when engaged, moderate availability',
            'very-fast-rarely': 'Fast responder but limited availability',
            'responsive-frequently': 'Consistent and reliable communication',
            'responsive-occasionally': 'Good communicator when available',
            'responsive-rarely': 'Decent response time but inconsistent availability',
            'slow-frequently': 'Always responds but takes time',
            'slow-occasionally': 'Slow but sometimes responsive',
            'slow-rarely': 'Both slow and infrequent responses',
            'stalled-frequently': 'Usually responds but with significant delays',
            'stalled-occasionally': 'Inconsistent and delayed responses',
            'stalled-rarely': 'Poor communication - needs reminders',
            'unresponsive-frequently': 'Rarely responds despite being active',
            'unresponsive-occasionally': 'Minimal engagement',
            'unresponsive-rarely': 'No engagement - avoid if possible'
        };
        
        return descriptions[`${timeCategory}-${frequencyCategory}`] || 'Communication pattern analysis unavailable';
    }

    calculateResponsivenessScore(timeCategory, frequencyCategory) {
        // Score from 1-100 based on responsiveness
        const timeScores = {
            'lightning-fast': 100,
            'very-fast': 85,
            'responsive': 70,
            'slow': 50,
            'stalled': 25,
            'unresponsive': 0
        };
        
        const frequencyScores = {
            'frequently': 100,
            'occasionally': 60,
            'rarely': 20
        };
        
        return Math.round((timeScores[timeCategory] + frequencyScores[frequencyCategory]) / 2);
    }

    getResponsivenessScoreRange(score) {
        if (score >= 90) return '90-100';
        if (score >= 70) return '70-89';
        if (score >= 50) return '50-69';
        if (score >= 30) return '30-49';
        return '0-29';
    }

    // Deal Headline Generator - Simple mail merge template
    generateDealHeadline(dealFields, locationData, userData) {
        // Extract key fields from correct sources
        const roomCount = locationData?.['Total Number of Rooms/Keys'] || dealFields['Number of Rooms'] || '';
        const chainScale = locationData?.['Hotel Chain Scale'] || '';
        const projectType = dealFields['Project Type'] || '';
        const city = locationData?.City || '';
        const country = locationData?.Country || '';
        const openingDate = dealFields['Expected Opening or Rebranding Date'] || '';
        const seekingFlag = dealFields['Seeking Flag'] || dealFields['Desired Brand'] || dealFields['Brand Preference'] || '';
        
        // Improved template: "[X]-key [chain scale] [project type] in [city], [country] opening [date]"
        let headline = '';
        
        // Room count
        if (roomCount && roomCount > 0) {
            headline += `${roomCount}-key `;
        }
        
        // Chain scale
        if (chainScale && chainScale !== 'Not Specified' && chainScale !== 'Unknown') {
            headline += `${chainScale.toLowerCase()} `;
        }
        
        // Project type
        if (projectType && projectType !== 'Unknown') {
            headline += `${projectType.toLowerCase()} `;
        }
        
        // Location
        if (city && country) {
            headline += `in ${city}, ${country} `;
        } else if (city) {
            headline += `in ${city} `;
        } else if (country) {
            headline += `in ${country} `;
        }
        
        // Opening date (formatted as full date)
        if (openingDate) {
            const formattedDate = this.formatOpeningDateForDisplay(openingDate);
            if (formattedDate) {
                headline += `opening ${formattedDate} `;
            }
        }
        
        // Clean up and format
        headline = headline.trim();
        
        // Capitalize first letter and add period if needed
        if (headline) {
            headline = headline.charAt(0).toUpperCase() + headline.slice(1);
            if (!headline.endsWith('.') && !headline.endsWith(')')) {
                headline += '.';
            }
        }
        
        return headline || 'Property details pending.';
    }

    formatPropertyType(propertyType) {
        const typeMap = {
            'Resort': 'resort',
            'Hotel': 'hotel',
            'Boutique Hotel': 'boutique hotel',
            'Business Hotel': 'business hotel',
            'Airport Hotel': 'airport hotel',
            'Extended Stay': 'extended stay',
            'Luxury Hotel': 'luxury hotel',
            'Upscale Hotel': 'upscale hotel',
            'Midscale Hotel': 'midscale hotel',
            'Economy Hotel': 'economy hotel',
            'All-Suite Hotel': 'all-suite hotel',
            'Conference Center': 'conference center',
            'Golf Resort': 'golf resort',
            'Beach Resort': 'beach resort',
            'Ski Resort': 'ski resort',
            'Casino Hotel': 'casino hotel',
            'Historic Hotel': 'historic hotel'
        };
        
        return typeMap[propertyType] || propertyType.toLowerCase();
    }

    formatLocation(locationData, dealFields) {
        const city = locationData?.City || 
                    dealFields['City'] || 
                    dealFields['Property City'] ||
                    dealFields['Location City'] ||
                    dealFields['Project City'];
                    
        const country = locationData?.Country || 
                       dealFields['Country'] || 
                       dealFields['Property Country'] ||
                       dealFields['Location Country'] ||
                       dealFields['Project Country'];
        
        // Try to extract from full address if available
        const fullAddress = locationData?.['Full Address'] || dealFields['Full Address'] || dealFields['Address'];
        let extractedCity = city;
        let extractedCountry = country;
        
        if (fullAddress && (!city || !country)) {
            // Try to parse city and country from full address
            const addressParts = fullAddress.split(',');
            if (addressParts.length >= 2) {
                // Filter out numeric-only parts and get meaningful city/country
                const meaningfulParts = addressParts.filter(part => 
                    part.trim() && 
                    !/^\d+$/.test(part.trim()) && // Not just numbers
                    part.trim().length > 2 // At least 3 characters
                );
                
                if (meaningfulParts.length >= 2) {
                    extractedCity = extractedCity || meaningfulParts[0].trim();
                    extractedCountry = extractedCountry || meaningfulParts[meaningfulParts.length - 1].trim();
                }
            }
        }
        
        if (extractedCity && extractedCountry) {
            return `in ${extractedCity}, ${extractedCountry}`;
        } else if (extractedCity) {
            return `in ${extractedCity}`;
        } else if (extractedCountry) {
            return `in ${extractedCountry}`;
        }
        
        return null;
    }

    getSignificantConsiderations(dealFields) {
        const considerations = [];
        
        // Check for key differentiators
        if (dealFields['Golf Course']) {
            considerations.push('golf course');
        }
        if (dealFields['Beachfront']) {
            considerations.push('beachfront');
        }
        if (dealFields['Historic Building']) {
            considerations.push('historic building');
        }
        if (dealFields['Airport Proximity']) {
            considerations.push('airport proximity');
        }
        if (dealFields['Conference Facilities']) {
            considerations.push('conference facilities');
        }
        if (dealFields['Spa']) {
            considerations.push('spa');
        }
        if (dealFields['Casino']) {
            considerations.push('casino');
        }
        if (dealFields['Waterfront']) {
            considerations.push('waterfront');
        }
        if (dealFields['Mountain View']) {
            considerations.push('mountain views');
        }
        
        // Limit to most significant ones
        if (considerations.length > 0) {
            return considerations.slice(0, 2).join(', ');
        }
        
        return null;
    }

    formatChainScale(chainScale) {
        const scaleMap = {
            'Luxury': 'luxury',
            'Upper Upscale': 'upper upscale',
            'Upscale': 'upscale',
            'Midscale': 'midscale',
            'Economy': 'economy',
            'Extended Stay': 'extended stay',
            'Select Service': 'select service',
            'Full Service': 'full service'
        };
        
        return scaleMap[chainScale] || chainScale.toLowerCase();
    }

    isRelevantOpeningDate(dateString) {
        if (!dateString) return false;
        
        try {
            const date = new Date(dateString);
            const now = new Date();
            const twoYearsFromNow = new Date();
            twoYearsFromNow.setFullYear(now.getFullYear() + 2);
            
            // Only show dates within the next 2 years
            return date > now && date <= twoYearsFromNow;
        } catch (error) {
            return false;
        }
    }

    formatOpeningDateForDisplay(dateString) {
        if (!dateString) return '';
        
        try {
            const date = new Date(dateString);
            if (isNaN(date.getTime())) return '';
            
            // Format as "Month YYYY" (e.g., "June 2026")
            const options = { year: 'numeric', month: 'long' };
            return date.toLocaleDateString('en-US', options);
        } catch (error) {
            return '';
        }
    }

    formatOpeningDateForHeadline(dateString) {
        if (!dateString) return '';
        
        try {
            const date = new Date(dateString);
            if (isNaN(date.getTime())) return '';
            
            const now = new Date();
            const diffTime = date - now;
            const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
            
            if (diffDays < 30) {
                return `${diffDays} days`;
            } else if (diffDays < 365) {
                const months = Math.round(diffDays / 30);
                return `${months}mo`;
            } else {
                const years = Math.round(diffDays / 365);
                return `${years}y`;
            }
        } catch (error) {
            return '';
        }
    }

    // Additional information extraction functions for brand developers
    extractDevelopmentBudget(dealFields) {
        const budget = dealFields['Development Budget'] || 
                      dealFields['Total Project Cost'] || 
                      dealFields['Investment Amount'] ||
                      dealFields['Project Value'];
        
        if (budget) {
            return this.formatCurrency(budget);
        }
        return 'Not Specified';
    }

    calculateCostPerKey(dealFields, locationData) {
        const budget = dealFields['Development Budget'] || 
                      dealFields['Total Project Cost'] || 
                      dealFields['Investment Amount'];
        const rooms = locationData?.['Total Number of Rooms/Keys'] || 0;
        
        if (budget && rooms > 0) {
            const costPerKey = budget / rooms;
            return this.formatCurrency(costPerKey);
        }
        return 'Not Available';
    }

    extractKeyAmenities(dealFields, locationData) {
        const amenities = [];
        
        // Check various amenity fields
        const amenityFields = [
            'Key Amenities', 'Amenities', 'Property Amenities',
            'Spa', 'Golf Course', 'Conference Facilities', 'Pool',
            'Restaurant', 'Fitness Center', 'Business Center'
        ];
        
        amenityFields.forEach(field => {
            if (dealFields[field] === true || dealFields[field] === 'Yes') {
                amenities.push(field);
            } else if (typeof dealFields[field] === 'string' && dealFields[field].length > 0) {
                amenities.push(dealFields[field]);
            }
        });
        
        // Also check location data for amenities
        if (locationData?.Amenities) {
            amenities.push(...locationData.Amenities.split(',').map(a => a.trim()));
        }
        
        return amenities.slice(0, 5); // Limit to top 5 amenities
    }

    extractDeveloperExperience(userData) {
        if (!userData) return 'Unknown';
        
        const experience = userData.experience || 0;
        const company = userData.company || '';
        
        if (experience >= 10) return 'Highly Experienced';
        if (experience >= 5) return 'Moderately Experienced';
        if (experience >= 2) return 'Some Experience';
        return 'New Developer';
    }

    formatCurrency(amount) {
        if (typeof amount === 'number') {
            return new Intl.NumberFormat('en-US', {
                style: 'currency',
                currency: 'USD',
                minimumFractionDigits: 0,
                maximumFractionDigits: 0
            }).format(amount);
        }
        return amount;
    }

    formatOpeningDate(dateString) {
        if (!dateString || dateString === 'Not Specified') return 'Not Specified';
        
        try {
            const date = new Date(dateString);
            if (isNaN(date.getTime())) return dateString; // Return original if invalid date
            
            return new Intl.DateTimeFormat('en-US', {
                year: 'numeric',
                month: 'short',
                day: 'numeric'
            }).format(date);
        } catch (error) {
            return dateString; // Return original if formatting fails
        }
    }

    getStatusClass(status) {
        const statusMap = {
            // New Deals tab
            'new': 'status-new',
            'viewed-by-brand': 'status-viewed-by-brand',
            'deal-received': 'status-deal-received',
            
            // Active Deals tab
            'closed-won': 'status-closed-won',
            'closed-lost': 'status-closed-lost',
            'awarded': 'status-awarded',
            'in-negotiation': 'status-in-negotiation',
            'shortlisting': 'status-shortlisting',
            'in-discovery': 'status-in-discovery',
            'on-hold': 'status-on-hold',
            'deferred': 'status-deferred',
            'expired': 'status-expired',
            'archived': 'status-archived',
            
            // Legacy statuses
            'Active': 'status-active',
            'Archived': 'status-archived',
            'Declined': 'status-declined'
        };
        return statusMap[status] || 'status-new';
    }

    getStatusLabel(status) {
        const statusLabels = {
            // New Deals tab
            'new': 'New',
            'viewed-by-brand': 'Viewed by Brand',
            'deal-received': 'Deal Received',
            
            // Active Deals tab
            'closed-won': 'Closed Won',
            'closed-lost': 'Closed Lost',
            'awarded': 'Awarded',
            'in-negotiation': 'In Negotiation',
            'shortlisting': 'Shortlisting',
            'in-discovery': 'In Discovery',
            'on-hold': 'On Hold',
            'deferred': 'Deferred',
            'expired': 'Expired',
            'archived': 'Archived',
            
            // Legacy statuses
            'Active': 'Active',
            'Declined': 'Declined'
        };
        return statusLabels[status] || this.toTitleCase(status);
    }

    toTitleCase(str) {
        return str.replace(/\w\S*/g, function(txt) {
            return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();
        });
    }

    getScoreClass(score) {
        if (score >= 80) return 'score-high';
        if (score >= 60) return 'score-medium';
        return 'score-low';
    }

    updateTabCounts() {
        const counts = {
            'new': this.deals.filter(d => ['new', 'viewed-by-brand', 'deal-received'].includes(d.status)).length,
            'active': this.deals.filter(d => ['closed-won', 'closed-lost', 'awarded', 'in-negotiation', 'shortlisting', 'in-discovery', 'on-hold', 'deferred', 'expired', 'archived'].includes(d.status)).length,
            'archives': this.deals.filter(d => ['archived', 'expired', 'closed-lost'].includes(d.status)).length,
            'deal-log': this.deals.length
        };

        document.getElementById('newCount').textContent = counts.new;
        document.getElementById('activeCount').textContent = counts.active;
        document.getElementById('archivesCount').textContent = counts.archives;
        document.getElementById('dealLogCount').textContent = counts['deal-log'];
    }

    setupEventListeners() {
        // Tab switching
        document.querySelectorAll('.dashboard-tab').forEach(tab => {
            tab.addEventListener('click', (e) => {
                e.preventDefault();
                this.switchTab(tab.dataset.tab).catch(console.error);
            });
        });

        // Brand selector
        const brandSelector = document.getElementById('brandSelector');
        if (brandSelector) {
            console.log('✅ Brand selector found, adding event listener');
            brandSelector.addEventListener('change', async (e) => {
                console.log('🔄 Brand selector changed to:', e.target.value);
            this.brandId = e.target.value;
                console.log('🔄 this.brandId is now:', this.brandId);
                // Clear brand data cache when brand changes to get fresh data
                console.log('🔄 Clearing brand data cache for fresh Airtable data...');
                this.cache.brandData.clear();
            this.showLoading(true, 'Loading deals for selected brand...', '1-2 seconds');
            await this.loadDeals();
        });
        } else {
            console.error('❌ Brand selector element not found!');
        }

        // Bulk actions
        document.getElementById('bulkActions').addEventListener('change', async (e) => {
            if (e.target.value) {
                await this.performBulkAction(e.target.value);
                e.target.value = '';
            }
        });

        // Filter button
        document.getElementById('filterBtn').addEventListener('click', () => {
            this.showFilterModal();
        });
    }

    async switchTab(tab) {
        this.currentTab = tab;
        
        document.querySelectorAll('.dashboard-tab').forEach(t => t.classList.remove('active'));
        document.querySelector(`[data-tab="${tab}"]`).classList.add('active');
        
        await this.applyTabFilter();
    }

    async performBulkAction(action) {
        if (this.selectedDeals.size === 0) {
            alert('Please select deals first');
            return;
        }

        const confirmMessage = `Are you sure you want to ${action} ${this.selectedDeals.size} deal(s)?`;
        if (!confirm(confirmMessage)) return;

        try {
            for (const dealId of this.selectedDeals) {
                await this.updateDealStatus(dealId, action);
            }
            
            await this.loadDeals();
            this.selectedDeals.clear();
            
            alert(`Successfully ${action}d ${this.selectedDeals.size} deal(s)`);
            
        } catch (error) {
            console.error('Error performing bulk action:', error);
            alert('Error performing bulk action. Please try again.');
        }
    }



    startChat(dealId) {
        const deal = this.deals.find(d => d.id === dealId);
        if (!deal) return;

        alert(`Starting chat with ${deal.ownerName} about ${deal.propertyName}`);
    }

    sendEmail(dealId) {
        const deal = this.deals.find(d => d.id === dealId);
        if (!deal) return;

        const subject = encodeURIComponent(`Re: ${deal.propertyName} - Deal Inquiry`);
        const body = encodeURIComponent(`Hi ${deal.ownerName},\n\nThank you for your interest in our brand...`);
        window.open(`mailto:${deal.ownerEmail}?subject=${subject}&body=${body}`);
    }

    showLoading(show, message = 'Processing...', estimatedTime = '2-3 seconds') {
        const systemStatus = document.getElementById('systemStatus');
        if (systemStatus) {
            if (show) {
                // Update the status text
                const statusText = systemStatus.querySelector('.status-text div:first-child');
                const statusTime = systemStatus.querySelector('.status-time');
                
                if (statusText) statusText.textContent = message;
                if (statusTime) statusTime.textContent = `Estimated time: ${estimatedTime}`;
                
                // Show the system status
                systemStatus.style.display = 'block';
                systemStatus.classList.add('show');
            } else {
                // Hide the system status
                systemStatus.style.display = 'none';
                systemStatus.classList.remove('show');
            }
        }
    }

    async showLoadingWave(show) {
        const dealsContainer = document.getElementById('dealsContainer');
        if (dealsContainer) {
            if (show) {
                // Show loading wave animation
                dealsContainer.innerHTML = `
                    <div class="loading-wave-container">
                        <div class="loading-wave-spinner">
                            <div class="loading-wave-container-inner">
                                <div class="loading-wave loading-wave-1"></div>
                                <div class="loading-wave loading-wave-2"></div>
                                <div class="loading-wave loading-wave-3"></div>
                                <div class="loading-wave-particles">
                                    <div class="loading-particle"></div>
                                    <div class="loading-particle"></div>
                                    <div class="loading-particle"></div>
                                    <div class="loading-particle"></div>
                                </div>
                            </div>
                        </div>
                        <div class="loading-wave-text">Loading deals...</div>
                    </div>
                `;
            } else {
                // Hide loading wave and render deals
                await this.renderDeals();
            }
        }
    }

    showError(message) {
        const container = document.getElementById('dealsContainer');
        container.innerHTML = `<div class="empty-state"><h3>Error</h3><p>${message}</p></div>`;
    }


    showFilterModal() {
        alert('Filter functionality would be implemented here');
    }

    // Fetch functions for all additional tables
    async fetchAllMarketPerformanceData(ids) {
        if (!ids || ids.length === 0) return {};
        
        const dataMap = {};
        // Process sequentially to avoid rate limiting
        for (let i = 0; i < ids.length; i++) {
            const id = ids[i];
            try {
                const url = `${this.getApiBaseUrl()}/api/brand-library/operational-support?brandId=${encodeURIComponent(id)}`;
                const response = await this.fetchWithRateLimit(url, {
                    method: 'GET',
                    headers: {
                        'Accept': 'application/json',
                        'X-Frontend-Auth-Disabled': 'true',
                    },
                });
                
                if (response.ok) {
                    const data = await response.json();
                    dataMap[id] = data.fields;
                }
            } catch (error) {
                console.error(`Error fetching market performance data for ${id}:`, error);
            }
        }
        return dataMap;
    }

    async fetchAllStrategicIntentData(ids) {
        if (!ids || ids.length === 0) {
            console.log('📊 No Strategic Intent IDs to fetch');
            return {};
        }
        
        console.log(`📊 Fetching Strategic Intent data for ${ids.length} records:`, ids);
        
        const dataMap = {};
        // Process sequentially to avoid rate limiting
        for (let i = 0; i < ids.length; i++) {
            const id = ids[i];
            try {
                const url = `${this.getApiBaseUrl()}/api/brand-library/operational-support?brandId=${encodeURIComponent(id)}`;
                console.log(`📊 Fetching Strategic Intent record: ${id}`);
                
                const response = await this.fetchWithRateLimit(url, {
                    method: 'GET',
                    headers: {
                        'Accept': 'application/json',
                        'X-Frontend-Auth-Disabled': 'true',
                    },
                });
                
                if (response.ok) {
                    const data = await response.json();
                    dataMap[id] = data.fields;
                    console.log(`✅ Strategic Intent record ${id} fetched:`, data.fields);
                } else {
                    console.error(`❌ Failed to fetch Strategic Intent record ${id}:`, response.status, response.statusText);
                }
            } catch (error) {
                console.error(`Error fetching strategic intent data for ${id}:`, error);
            }
        }
        return dataMap;
    }

    async fetchAllHotelOwnershipData(ids) {
        if (!ids || ids.length === 0) return {};
        
        const dataMap = {};
        // Process sequentially to avoid rate limiting
        for (let i = 0; i < ids.length; i++) {
            const id = ids[i];
            try {
                const url = `${this.getApiBaseUrl()}/api/brand-library/operational-support?brandId=${encodeURIComponent(id)}`;
                const response = await this.fetchWithRateLimit(url, {
                    method: 'GET',
                    headers: {
                        'Accept': 'application/json',
                        'X-Frontend-Auth-Disabled': 'true',
                    },
                });
                
                if (response.ok) {
                    const data = await response.json();
                    dataMap[id] = data.fields;
                }
            } catch (error) {
                console.error(`Error fetching hotel ownership data for ${id}:`, error);
            }
        }
        return dataMap;
    }

    async fetchAllCompanyProfileData(ids) {
        if (!ids || ids.length === 0) {
            console.log('🏢 No Company Profile IDs to fetch');
            return {};
        }
        
        console.log(`🏢 Fetching Company Profile data for ${ids.length} records:`, ids);
        
        const dataMap = {};
        // Process sequentially to avoid rate limiting
        for (let i = 0; i < ids.length; i++) {
            const id = ids[i];
            try {
                const url = `${this.getApiBaseUrl()}/api/brand-library/operational-support?brandId=${encodeURIComponent(id)}`;
                console.log(`🏢 Fetching Company Profile record: ${id}`);
                
                const response = await this.fetchWithRateLimit(url, {
                    method: 'GET',
                    headers: {
                        'Accept': 'application/json',
                        'X-Frontend-Auth-Disabled': 'true',
                    },
                });
                
                if (response.ok) {
                    const data = await response.json();
                    dataMap[id] = data.fields;
                    console.log(`✅ Company Profile data fetched for ${id}:`, data.fields);
                    console.log(`🏢 Company Profile fields available:`, Object.keys(data.fields));
                } else {
                    console.error(`❌ Failed to fetch Company Profile data for ${id}:`, response.status, response.statusText);
                }
            } catch (error) {
                console.error(`❌ Error fetching Company Profile data for ${id}:`, error);
            }
        }
        
        console.log(`🏢 Company Profile data fetch complete. Retrieved ${Object.keys(dataMap).length} records.`);
        return dataMap;
    }
}

// Initialize dashboard when DOM is loaded - Version 71 with Redesigned Modal
console.log('🚀 Dashboard loaded - Version 71 with Redesigned Modal');
console.log('✅ If you see this message, the new card-based modal design should be active!');
document.addEventListener('DOMContentLoaded', () => {
    window.dashboard = new ProductionBrandDashboard();
});

// Export for Webflow integration
window.ProductionBrandDashboard = ProductionBrandDashboard;


