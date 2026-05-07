// Brand Development Dashboard JavaScript
class BrandDevelopmentDashboard {
    constructor() {
        this.deals = [];
        this.filteredDeals = [];
        this.filteredActiveDeals = [];
        this.filteredArchivedDeals = [];
        this.archivedDeals = [];
        this.activeDeals = [];
        this.dealLogEntries = [];
        this.brandDealRequests = { new: [], accepted: [], declined: [], archived: [] };
        this.dealIdToRequest = {}; // dealId -> { requestId, status } for New tab Accept/Decline
        this.brandId = null;
        this.sortColumn = null;
        this.sortDirection = 'asc'; // 'asc' or 'desc'
        // Cache for repeated API fetches (same location/contact used by many deals)
        this._fetchCache = {
            location: new Map(),
            contact: new Map(),
            marketPerf: new Map(),
            strategicIntent: new Map(),
            userProfileByEmail: new Map()
        };
        this._fetchPending = {
            location: new Map(),
            contact: new Map(),
            marketPerf: new Map(),
            strategicIntent: new Map(),
            userProfileByEmail: new Map()
        };
        this._brandDataCache = new Map();
        this._brandDataPending = new Map();
        this._backgroundRefreshTimer = null;
        this._airtableFetchDelayMs = 150;  // min gap between Airtable requests to avoid 429
        this._airtableLastFetchAt = 0;
        this._operationalSupportForbidden = false;  // skip fetch after 403 (table access denied)

        this.init();
    }

    async init() {
        const urlParams = new URLSearchParams(window.location.search);
        this.brandId = urlParams.get('brand') || null;
        this.setupEventListeners();
        await this.loadDeals();
    }

    /** Airtable fetch with rate limiting and 429 retry. Respects rate limits and retries up to 3 times on 429. */
    async _airtableFetch(url, options = {}, retries = 3) {
        const delay = this._airtableFetchDelayMs;
        const now = Date.now();
        const elapsed = now - this._airtableLastFetchAt;
        if (elapsed < delay) await new Promise(r => setTimeout(r, delay - elapsed));
        this._airtableLastFetchAt = Date.now();
        let res = await fetch(url, options);
        while ((res.status === 429 || res.status >= 500) && retries > 0) {
            const backoff = res.status === 429 ? 2500 : 1500;
            await new Promise(r => setTimeout(r, backoff));
            retries--;
            this._airtableLastFetchAt = Date.now();
            res = await fetch(url, options);
        }
        return res;
    }

    showLoadingInTab(tabId) {
        const ids = {
            'new-deals': 'newDealsLoadingState',
            'active-deals': 'activeDealsLoadingState',
            'archived': 'archivedLoadingState',
            'deal-log': 'dealLogLoadingState'
        };
        Object.entries(ids).forEach(([tab, elId]) => {
            const el = document.getElementById(elId);
            if (el) el.style.display = (tab === tabId ? 'block' : 'none');
        });
        const newTable = document.getElementById('dealsTable');
        if (newTable) newTable.style.display = tabId === 'new-deals' ? 'none' : '';
        // For Active Deals and Archived, keep the card wrapper visible while loading
        // but hide the tables so stale rows aren't shown.
        if (tabId === 'active-deals') {
            const activeWrap = document.getElementById('activeDealsTableWrap');
            const activeTable = document.getElementById('activeDealsTable');
            const activeEmpty = document.getElementById('activeDealsEmptyState');
            if (activeWrap) activeWrap.style.display = '';
            if (activeTable) activeTable.style.display = 'none';
            if (activeEmpty) activeEmpty.style.display = 'none';
        }
        if (tabId === 'archived') {
            const archivedWrap = document.getElementById('archivedTableWrap');
            const archivedTable = archivedWrap ? archivedWrap.querySelector('table') : null;
            const archivedEmpty = document.getElementById('archivedEmptyState');
            if (archivedWrap) archivedWrap.style.display = '';
            if (archivedTable) archivedTable.style.display = 'none';
            if (archivedEmpty) archivedEmpty.style.display = 'none';
        }
        if (tabId === 'deal-log') {
            const dealLogContent = document.getElementById('dealLogContent');
            if (dealLogContent) dealLogContent.style.display = 'none';
        }
    }

    hideAllLoadingStates() {
        ['newDealsLoadingState', 'activeDealsLoadingState', 'archivedLoadingState', 'dealLogLoadingState'].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.style.display = 'none';
        });
        const newTable = document.getElementById('dealsTable');
        if (newTable) newTable.style.display = '';
        const dealLogContent = document.getElementById('dealLogContent');
        if (dealLogContent) dealLogContent.style.display = '';
    }

    showBddToast(message, success = true) {
        const toast = document.getElementById('bddToast');
        if (!toast) return;
        const msgEl = toast.querySelector('.toast-message');
        if (msgEl) msgEl.textContent = message;
        toast.classList.remove('toast-success', 'toast-error');
        toast.classList.add(success ? 'toast-success' : 'toast-error');
        toast.style.display = 'flex';
        toast.classList.remove('show');
        toast.offsetHeight;
        setTimeout(() => toast.classList.add('show'), 20);
        setTimeout(() => {
            toast.classList.remove('show');
            setTimeout(() => { toast.style.display = 'none'; }, 300);
        }, 3000);
    }

    async loadDeals() {
        this.dealsLoading = true;
        this.showLoadingInTab('new-deals');
        try {
            console.log('Loading deals...');
            const backendDeals = await this.fetchDealsFromBackend();
            this.deals = backendDeals;
            console.log(`Loaded ${this.deals.length} deals from backend API`);
            
            // Load all contacted deals (no brand filter) - show all projects that have been contacted in Airtable
            await this.fetchAllContactedDeals();
            await this.recalculateMatchScores();
            
            this.renderDeals();
            await this.populateFilters();
            this.updateArchivedDisplay();
            this.updateActiveDealsDisplay();
            this.dealsLoading = false;
            this.hideAllLoadingStates();
            this.restoreTabFromHash();
            console.log('Deals loaded successfully');
        } catch (error) {
            console.error('Error loading deals:', error);
            this.showBddToast('Failed to load deals. Please refresh the page.', false);
            this.showError('Failed to load deals. Please refresh the page.');
        } finally {
            this.dealsLoading = false;
            this.hideAllLoadingStates();
        }
    }

    async fetchDealsFromBackend() {
        const base = window.location.origin || '';
        try {
            const res = await fetch(`${base}/api/my-deals`);
            if (!res.ok) throw new Error(`Backend API responded ${res.status}`);
            const data = await res.json();
            const deals = Array.isArray(data?.deals) ? data.deals : [];
            return deals.map(d => this.normalizeBackendDeal(d)).filter(Boolean);
        } catch (err) {
            console.error('Backend deals API unavailable:', err.message);
            throw err;
        }
    }

    normalizeBackendDeal(rawDeal) {
        if (!rawDeal || !rawDeal.id) return null;
        const projectName = (rawDeal.projectName || '').toString().trim() || 'Unnamed Project';
        const locationRaw = (rawDeal.hotelLocation || '').toString().trim();
        const locationParts = locationRaw ? locationRaw.split(',').map(s => s.trim()).filter(Boolean) : [];
        const country = locationParts.length > 0 ? locationParts[locationParts.length - 1] : 'Unknown';
        const city = locationParts.length > 1 ? locationParts.slice(0, -1).join(', ') : (locationParts[0] || 'Unknown');
        const preferredBrands = (rawDeal.preferredBrandsChosen || '')
            .toString()
            .split(',')
            .map(s => s.trim())
            .filter(Boolean);
        const preferredBrandName = preferredBrands[0] || '';
        const matchScoresByBrand = rawDeal.matchScoresNewByBrand || rawDeal.matchScoresByBrand || {};
        const matchBreakdownByBrand = rawDeal.matchBreakdownNewDetailsByBrand || rawDeal.matchBreakdownDetailsByBrand || rawDeal.matchBreakdownByBrand || {};
        const preferredBrandScore = preferredBrandName && matchScoresByBrand[preferredBrandName] != null
            ? Number(matchScoresByBrand[preferredBrandName])
            : Number(rawDeal.matchScoreNew ?? rawDeal.matchScore ?? 0);
        const scoreBreakdown = preferredBrandName && matchBreakdownByBrand[preferredBrandName]
            ? (matchBreakdownByBrand[preferredBrandName] || {})
            : {};
        const description = (rawDeal.propertyDescription || '').toString().trim();
        const roomsMatch = description.match(/(\d{2,4})\s*-\s*room|(\d{2,4})\s*rooms?|(\d{2,4})\s*keys?/i);
        const parsedRooms = roomsMatch ? parseInt((roomsMatch[1] || roomsMatch[2] || roomsMatch[3]), 10) : 0;

        const dealFields = {
            'Project Name': projectName,
            'Project Type': rawDeal.projectType || '',
            'Hotel Type': rawDeal.hotelType || '',
            'Expected Opening Date': rawDeal.targetOpeningDate || '',
            'Deal Type': rawDeal.dealType || '',
            'Deal Status': rawDeal.dealStatus || '',
            'Description': description,
            'Preferred Brands': rawDeal.preferredBrandsChosen || ''
        };

        const locationData = {
            City: city,
            Country: country,
            'Hotel Chain Scale': rawDeal.hotelChainScale || '',
            'Hotel Type': rawDeal.hotelType || '',
            'Total Number of Rooms/Keys': parsedRooms || ''
        };

        return {
            id: rawDeal.id,
            dealId: rawDeal.id,
            propertyName: projectName,
            headline: projectName,
            country: country || 'Unknown',
            city: city || 'Unknown',
            propertyType: (rawDeal.hotelType || '').toString().trim() || 'Unknown',
            rooms: parsedRooms || 0,
            stage: 'Unknown',
            projectType: (rawDeal.projectType || '').toString().trim() || 'Unknown',
            chainScale: (rawDeal.hotelChainScale || '').toString().trim() || 'Not Specified',
            targetOpeningDate: (rawDeal.targetOpeningDate || '').toString().trim() || '—',
            preferredBrandName,
            preferredBrandScore,
            preferredBrandScoreBreakdown: scoreBreakdown,
            matchScore: preferredBrandScore,
            scoreBreakdown,
            dealFields,
            locationData,
            contactData: {},
            marketPerformanceData: {},
            strategicIntentData: {},
            matchScoresByBrand,
            matchScoresNewByBrand: rawDeal.matchScoresNewByBrand || rawDeal.matchScoresByBrand || {},
            matchBreakdownNewDetailsByBrand: rawDeal.matchBreakdownNewDetailsByBrand || rawDeal.matchBreakdownDetailsByBrand || {},
            matchBreakdownDetailsByBrand: rawDeal.matchBreakdownDetailsByBrand || {}
        };
    }

    /**
     * Fetch ALL contacted deals from Airtable (no brand filter).
     * Shows all projects that have at least one Brand Deal Request.
     */
    async fetchAllContactedDeals() {
        const base = window.location.origin || '';
        try {
            const res = await fetch(`${base}/api/brand-deal-requests?all=1`);
            const data = res.ok ? await res.json() : { requests: [] };
            const allRequests = data.requests || [];
            // Group by status to match brandDealRequests structure
            const byStatus = { new: [], viewed: [], accepted: [], declined: [], archived: [] };
            for (const r of allRequests) {
                const s = (r.status || 'New').trim();
                if (s === 'New') byStatus.new.push(r);
                else if (s === 'Brand Viewed') byStatus.viewed.push(r);
                else if (s === 'Accepted') byStatus.accepted.push(r);
                else if (s === 'Declined') byStatus.declined.push(r);
                else if (s === 'Archived') byStatus.archived.push(r);
            }
            this.brandDealRequests = byStatus;
            const allWithStatus = [
                ...byStatus.new.map(r => ({ ...r, _status: 'New' })),
                ...byStatus.viewed.map(r => ({ ...r, _status: 'Brand Viewed' })),
                ...byStatus.accepted.map(r => ({ ...r, _status: 'Accepted' })),
                ...byStatus.declined.map(r => ({ ...r, _status: 'Declined' })),
                ...byStatus.archived.map(r => ({ ...r, _status: 'Archived' }))
            ];
            this.dealIdToRequest = {};
            for (const r of allWithStatus) {
                if (r.dealId) {
                    // Prefer most recent request per deal (listAll returns desc by Request Sent At)
                    if (!this.dealIdToRequest[r.dealId]) {
                        this.dealIdToRequest[r.dealId] = { requestId: r.id, status: r._status, matchScore: r.matchScore };
                    }
                }
            }
            const allDealIds = [...new Set(allWithStatus.map(r => r.dealId).filter(Boolean))];
            if (allDealIds.length > 0) {
                try {
                    const activityRes = await fetch(`${base}/api/brand-deal-requests/activity?dealIds=${encodeURIComponent(allDealIds.join(','))}`);
                    const activityData = activityRes.ok ? await activityRes.json() : { entries: [] };
                    const dealMapForName = new Map(this.deals.map(d => [d.id, d]));
                    this.dealLogEntries = (activityData.entries || []).map(e => {
                        const deal = dealMapForName.get(e.dealId);
                        const dealName = (deal?.propertyName || deal?.dealFields?.['Project Name'] || deal?.dealFields?.['Property Name'] || '').toString().trim() || (e.dealId ? 'Deal ' + String(e.dealId).slice(-6) : '');
                        return {
                            date: e.createdAt || '',
                            dealName: dealName,
                            action: e.action || '',
                            details: e.details || '',
                            dealId: e.dealId
                        };
                    });
                } catch (_) {
                    this.dealLogEntries = [];
                }
            } else {
                this.dealLogEntries = [];
            }
            // New Deals tab: only New + Brand Viewed (exclude Accepted, Declined, Archived - those go to Active/Archived)
            const newDealsRequests = [
                ...byStatus.new.map(r => ({ ...r, _status: 'New' })),
                ...byStatus.viewed.map(r => ({ ...r, _status: 'Brand Viewed' }))
            ];
            const dealMap = new Map(this.deals.map(d => [d.id, d]));
            this.filteredDeals = [];
            for (const req of newDealsRequests) {
                const deal = dealMap.get(req.dealId);
                if (!deal) continue;
                const row = { ...deal };
                row._requestId = req.id;
                row._requestStatus = req._status;
                row._requestMatchScore = req.matchScore;
                row._contactedBrand = req.brandName || '';
                this.filteredDeals.push(row);
            }
            this.activeDeals = (byStatus.accepted || []).map(r => {
                const deal = dealMap.get(r.dealId);
                const brandName = r.brandName || '';
                const base = deal ? { ...deal } : { id: r.dealId, propertyName: 'Deal ' + (r.dealId || '').slice(-6), rooms: 'N/A', chainScale: '—', projectType: '—', propertyType: '—', city: '', country: '', targetOpeningDate: '' };
                return { ...base, status: 'Accepted', _requestId: r.id, _requestMatchScore: r.matchScore, _contactedBrand: brandName };
            });
            const declinedArchived = [
                ...(byStatus.declined || []).map(r => ({ ...r, _status: 'Declined' })),
                ...(byStatus.archived || []).map(r => ({ ...r, _status: 'Archived' }))
            ];
            this.archivedDeals = declinedArchived.map(r => {
                const deal = dealMap.get(r.dealId);
                if (!deal) return { id: r.dealId, propertyName: 'Deal ' + (r.dealId || '').slice(-6), archivedReason: r._status || r.status, matchScore: r.matchScore, _requestId: r.id };
                deal.archivedReason = r._status || r.status;
                deal._requestId = r.id;
                return deal;
            });
        } catch (err) {
            console.warn('Could not fetch all contacted deals:', err.message);
        }
    }

    async fetchBrandDealRequests() {
        if (!this.brandId) return;
        const base = window.location.origin || '';
        try {
            const [newRes, viewedRes, acceptedRes, declinedRes, archivedRes, activityRes] = await Promise.all([
                fetch(`${base}/api/brand-deal-requests?brand=${encodeURIComponent(this.brandId)}&status=New`),
                fetch(`${base}/api/brand-deal-requests?brand=${encodeURIComponent(this.brandId)}&status=Brand Viewed`),
                fetch(`${base}/api/brand-deal-requests?brand=${encodeURIComponent(this.brandId)}&status=Accepted`),
                fetch(`${base}/api/brand-deal-requests?brand=${encodeURIComponent(this.brandId)}&status=Declined`),
                fetch(`${base}/api/brand-deal-requests?brand=${encodeURIComponent(this.brandId)}&status=Archived`),
                fetch(`${base}/api/brand-deal-requests/activity?brand=${encodeURIComponent(this.brandId)}`)
            ]);
            const newData = newRes.ok ? await newRes.json() : { requests: [] };
            const viewedData = viewedRes.ok ? await viewedRes.json() : { requests: [] };
            const acceptedData = acceptedRes.ok ? await acceptedRes.json() : { requests: [] };
            const declinedData = declinedRes.ok ? await declinedRes.json() : { requests: [] };
            const archivedData = archivedRes.ok ? await archivedRes.json() : { requests: [] };
            const activityData = activityRes.ok ? await activityRes.json() : { entries: [] };
            this.brandDealRequests = {
                new: newData.requests || [],
                viewed: viewedData.requests || [],
                accepted: acceptedData.requests || [],
                declined: declinedData.requests || [],
                archived: archivedData.requests || []
            };
            const dealMapForName = new Map(this.deals.map(d => [d.id, d]));
            this.dealLogEntries = (activityData.entries || []).map(e => {
                const deal = dealMapForName.get(e.dealId);
                const dealName = (deal?.propertyName || deal?.dealFields?.['Project Name'] || deal?.dealFields?.['Property Name'] || '').toString().trim() || (e.dealId ? 'Deal ' + String(e.dealId).slice(-6) : '');
                return {
                    date: e.createdAt || '',
                    dealName,
                    action: e.action || '',
                    details: e.details || '',
                    dealId: e.dealId
                };
            });
            this.dealIdToRequest = {};
            const dealMap = new Map(this.deals.map(d => [d.id, d]));
            const allRequests = [
                ...(this.brandDealRequests.new || []).map(r => ({ ...r, _status: 'New' })),
                ...(this.brandDealRequests.viewed || []).map(r => ({ ...r, _status: 'Brand Viewed' })),
                ...(this.brandDealRequests.accepted || []).map(r => ({ ...r, _status: 'Accepted' })),
                ...(this.brandDealRequests.declined || []).map(r => ({ ...r, _status: 'Declined' })),
                ...(this.brandDealRequests.archived || []).map(r => ({ ...r, _status: 'Archived' }))
            ];
            allRequests.forEach(r => {
                if (r.dealId) this.dealIdToRequest[r.dealId] = { requestId: r.id, status: r._status, matchScore: r.matchScore };
            });
            const newDealsRequests = [
                ...(this.brandDealRequests.new || []).map(r => ({ ...r, _status: 'New' })),
                ...(this.brandDealRequests.viewed || []).map(r => ({ ...r, _status: 'Brand Viewed' }))
            ];
            const newDealsDealIds = new Set(newDealsRequests.map(r => r.dealId).filter(Boolean));
            this.filteredDeals = this.deals.filter(d => newDealsDealIds.has(d.id)).map(d => {
                const req = newDealsRequests.find(r => r.dealId === d.id);
                if (req) {
                    d._requestId = req.id;
                    d._requestStatus = req._status;
                    d._requestMatchScore = req.matchScore;
                    d._contactedBrand = req.brandName || this.brandId;
                }
                return d;
            });
            const acceptedRequests = (this.brandDealRequests.accepted || []).map(r => ({ ...r, _status: 'Accepted' }));
            this.activeDeals = acceptedRequests.map(r => {
                const deal = dealMap.get(r.dealId);
                const brandName = r.brandName || this.brandId || '';
                const base = deal ? { ...deal } : { id: r.dealId, propertyName: 'Deal ' + (r.dealId || '').slice(-6), rooms: 'N/A', chainScale: '—', projectType: '—', propertyType: '—', city: '', country: '', targetOpeningDate: '' };
                return { ...base, status: 'Accepted', _requestId: r.id, _requestMatchScore: r.matchScore, _contactedBrand: brandName };
            });
            const declinedArchived = [
                ...(this.brandDealRequests.declined || []).map(r => ({ ...r, _status: 'Declined' })),
                ...(this.brandDealRequests.archived || []).map(r => ({ ...r, _status: 'Archived' }))
            ];
            this.archivedDeals = declinedArchived.map(r => {
                const deal = dealMap.get(r.dealId);
                if (!deal) return { id: r.dealId, propertyName: 'Deal ' + (r.dealId || '').slice(-6), archivedReason: r._status || r.status, matchScore: r.matchScore, _requestId: r.id };
                deal.archivedReason = r._status || r.status;
                deal._requestId = r.id;
                return deal;
            });
            for (const e of this.dealLogEntries) {
                if (e.dealId && !e.dealName) {
                    const d = dealMap.get(e.dealId);
                    if (d) e.dealName = d.propertyName || d.headline || 'Deal';
                }
            }
        } catch (err) {
            console.warn('Could not fetch brand deal requests:', err.message);
        }
    }

    _bucketForStatus(status) {
        const normalized = (status || '').toString().trim().toLowerCase();
        if (normalized === 'new') return 'new';
        if (normalized === 'brand viewed' || normalized === 'viewed') return 'viewed';
        if (normalized === 'accepted') return 'accepted';
        if (normalized === 'declined') return 'declined';
        if (normalized === 'archived') return 'archived';
        return null;
    }

    _statusLabelForBucket(bucket) {
        if (bucket === 'new') return 'New';
        if (bucket === 'viewed') return 'Brand Viewed';
        if (bucket === 'accepted') return 'Accepted';
        if (bucket === 'declined') return 'Declined';
        if (bucket === 'archived') return 'Archived';
        return 'New';
    }

    _findRequestPosition(requestId) {
        const buckets = ['new', 'viewed', 'accepted', 'declined', 'archived'];
        for (const bucket of buckets) {
            const list = this.brandDealRequests[bucket] || [];
            const index = list.findIndex(r => r && r.id === requestId);
            if (index >= 0) return { bucket, index, request: list[index] };
        }
        return null;
    }

    _rebuildRequestDerivedState() {
        const dealMap = new Map(this.deals.map(d => [d.id, d]));
        const allByStatus = [
            ...(this.brandDealRequests.new || []).map(r => ({ ...r, _status: 'New' })),
            ...(this.brandDealRequests.viewed || []).map(r => ({ ...r, _status: 'Brand Viewed' })),
            ...(this.brandDealRequests.accepted || []).map(r => ({ ...r, _status: 'Accepted' })),
            ...(this.brandDealRequests.declined || []).map(r => ({ ...r, _status: 'Declined' })),
            ...(this.brandDealRequests.archived || []).map(r => ({ ...r, _status: 'Archived' }))
        ];

        this.dealIdToRequest = {};
        for (const r of allByStatus) {
            if (!r.dealId || this.dealIdToRequest[r.dealId]) continue;
            this.dealIdToRequest[r.dealId] = { requestId: r.id, status: r._status, matchScore: r.matchScore };
        }

        this.activeDeals = (this.brandDealRequests.accepted || []).map(r => {
            const deal = dealMap.get(r.dealId);
            const brandName = r.brandName || this.brandId || '';
            const base = deal ? { ...deal } : { id: r.dealId, propertyName: 'Deal ' + (r.dealId || '').slice(-6), rooms: 'N/A', chainScale: '—', projectType: '—', propertyType: '—', city: '', country: '', targetOpeningDate: '' };
            return { ...base, status: 'Accepted', _requestId: r.id, _requestMatchScore: r.matchScore, _contactedBrand: brandName };
        });

        const declinedArchived = [
            ...(this.brandDealRequests.declined || []).map(r => ({ ...r, _status: 'Declined' })),
            ...(this.brandDealRequests.archived || []).map(r => ({ ...r, _status: 'Archived' }))
        ];
        this.archivedDeals = declinedArchived.map(r => {
            const deal = dealMap.get(r.dealId);
            if (!deal) return { id: r.dealId, propertyName: 'Deal ' + (r.dealId || '').slice(-6), archivedReason: r._status || r.status, matchScore: r.matchScore, _requestId: r.id };
            const cloned = { ...deal };
            cloned.archivedReason = r._status || r.status;
            cloned._requestId = r.id;
            return cloned;
        });
    }

    _applyLocalRequestPatch(requestId, patch = {}) {
        if (!requestId) return false;
        const pos = this._findRequestPosition(requestId);
        if (!pos || !pos.request) return false;

        const list = this.brandDealRequests[pos.bucket] || [];
        const current = list[pos.index];
        const nextStatus = patch.status || current.status || this._statusLabelForBucket(pos.bucket);
        const nextBucket = this._bucketForStatus(nextStatus) || pos.bucket;

        if (nextBucket === pos.bucket && !patch.status) {
            list[pos.index] = { ...current, ...patch };
            this.brandDealRequests[pos.bucket] = list;
            return true;
        }

        const updated = { ...current, ...patch, status: nextStatus };
        list.splice(pos.index, 1);
        this.brandDealRequests[pos.bucket] = list;
        const targetList = this.brandDealRequests[nextBucket] || [];
        targetList.unshift(updated);
        this.brandDealRequests[nextBucket] = targetList;
        return true;
    }

    _applyLocalMutationEffects() {
        this._rebuildRequestDerivedState();
        this.applyFilters();
        this.updateActiveDealsDisplay();
        this.updateArchivedDisplay();
        this.updateTabCounts();
    }

    _scheduleBackgroundRefresh(delayMs = 2000) {
        if (this._backgroundRefreshTimer) clearTimeout(this._backgroundRefreshTimer);
        this._backgroundRefreshTimer = setTimeout(async () => {
            try {
                if (this.brandId) {
                    await this.fetchBrandDealRequests();
                } else {
                    await this.fetchAllContactedDeals();
                }
                this._applyLocalMutationEffects();
            } catch (err) {
                console.warn('Background refresh failed:', err.message);
            }
        }, delayMs);
    }

    async fetchDealsFromAirtable() {
        // Security hard-stop: no direct browser Airtable calls.
        return [];
    }

    formatTargetOpeningDate(val) {
        if (!val || val === '—' || String(val).trim() === '') return '—';
        const d = new Date(String(val).trim());
        if (isNaN(d.getTime())) return this.escapeHtml(val);
        const m = d.toLocaleString('en-US', { month: 'short' });
        const y = d.getFullYear();
        return m + ', ' + y;
    }

    async processDealsData(rawDeals) {
        const BATCH_SIZE = 12;
        const BATCH_DELAY_MS = 0;
        const results = [];
        for (let i = 0; i < rawDeals.length; i += BATCH_SIZE) {
            const batch = rawDeals.slice(i, i + BATCH_SIZE);
            const batchResults = await Promise.all(batch.map(deal => this._processOneDeal(deal)));
            results.push(...batchResults.filter(Boolean));
            if (BATCH_DELAY_MS > 0 && i + BATCH_SIZE < rawDeals.length) {
                await new Promise(r => setTimeout(r, BATCH_DELAY_MS));
            }
        }
        return results;
    }

    async _processOneDeal(deal) {
        try {
            const dealFields = deal.fields;
            const locId = dealFields['Location & Property']?.[0];
            const contactId = dealFields['Contact & Uploads']?.[0];
            const mpId = dealFields['Market - Performance - Deal & Capital Structure']?.[0];
            const siId = dealFields['Strategic Intent - Operational - Key Challenges']?.[0];

            const [locationData, contactData, marketPerformanceData, strategicIntentData] = await Promise.all([
                locId ? this.getLocationData(locId) : Promise.resolve({}),
                contactId ? this.getContactData(contactId) : Promise.resolve({}),
                mpId ? this.getMarketPerformanceData(mpId) : Promise.resolve({}),
                siId ? this.getStrategicIntentData(siId) : Promise.resolve({})
            ]);

            let profileImageFromUsers = null;
            const contactEmail = contactData['Email Address'] || contactData['Email'] ||
                contactData['Main Contact Email'] || contactData['Contact Email'] ||
                contactData['A Main Contact Email'] || contactData['Email Address (Main Contact)'];
            if (contactEmail) {
                profileImageFromUsers = await this.getUserProfileImageByEmail(String(contactEmail).trim().toLowerCase());
                if (profileImageFromUsers) contactData['Profile'] = [{ url: profileImageFromUsers }];
            }
            if (!profileImageFromUsers) {
                const contactProfile = contactData['Profile Image'] || contactData['Profile'] || contactData['Photo'] || contactData['Avatar'];
                if (contactProfile) {
                    const imageUrl = Array.isArray(contactProfile) ? contactProfile[0]?.url : contactProfile?.url;
                    if (imageUrl) contactData['Profile'] = [{ url: imageUrl }];
                }
            }

            const brandNames = this.extractBrandNames(dealFields);
            const preferredBrands = this.extractPreferredBrands(dealFields, strategicIntentData);
            let preferredBrandScore = null;
            let preferredBrandScoreBreakdown = {};
            let preferredBrandName = null;

            if (preferredBrands && preferredBrands.length > 0) {
                preferredBrandName = preferredBrands[0];
                const preferredMatchResult = await this.calculateMatchScore(
                    dealFields, locationData, preferredBrandName,
                    marketPerformanceData, strategicIntentData
                );
                preferredBrandScore = preferredMatchResult?.score ?? 0;
                preferredBrandScoreBreakdown = preferredMatchResult?.breakdown ?? {};
            }

            let brandForScoring = this.brandId;
            if (!brandForScoring) brandForScoring = document.getElementById('brandFilter')?.value || null;
            let matchScore = 0;
            let scoreBreakdown = {};

            if (brandForScoring) {
                const matchScoreResult = await this.calculateMatchScore(
                    dealFields, locationData, brandForScoring,
                    marketPerformanceData, strategicIntentData
                );
                matchScore = matchScoreResult?.score ?? 0;
                scoreBreakdown = matchScoreResult?.breakdown ?? {};
            } else if (preferredBrandScore !== null) {
                matchScore = preferredBrandScore;
                scoreBreakdown = preferredBrandScoreBreakdown;
            }

            const headline = this.generateDealHeadline(dealFields, locationData, contactData);
            return {
                id: deal.id,
                dealId: dealFields['Deal_ID'] || deal.id,
                propertyName: (dealFields['Project Name'] || '').toString().trim() || 'Unnamed Project',
                brandMatch: brandNames || 'Not specified',
                country: locationData?.Country || 'Unknown',
                city: locationData?.City || 'Unknown',
                propertyType: locationData?.['Hotel Type'] || dealFields['Hotel Type'] || 'Unknown',
                rooms: parseInt(locationData?.['Total Number of Rooms/Keys'], 10) || parseInt(dealFields['Total Number of Rooms/Keys'], 10) || 0,
                stage: dealFields['Stage of Development'] || 'Unknown',
                projectType: dealFields['Project Type'] || 'Unknown',
                chainScale: locationData?.['Hotel Chain Scale'] || 'Not Specified',
                targetOpeningDate: (dealFields['Expected Opening or Rebranding Date'] || dealFields['Expected Opening Date'] || '').toString().trim() || '—',
                matchScore, scoreBreakdown, preferredBrands, preferredBrandName,
                preferredBrandScore, preferredBrandScoreBreakdown, headline,
                dealFields, locationData, contactData, marketPerformanceData, strategicIntentData
            };
        } catch (error) {
            console.error('Error processing deal:', deal.id, error);
            return null;
        }
    }

    extractBrandNames(dealFields) {
        // Try to get brand from various fields
        const brandFields = [
            'Preferred Brands',
            'Brand Preference',
            'Brand',
            'Current Brand Affiliation'
        ];
        
        for (const field of brandFields) {
            if (dealFields[field]) {
                if (Array.isArray(dealFields[field])) {
                    return dealFields[field].join(', ');
                }
                return dealFields[field];
            }
        }
        
        return 'Not specified';
    }

    // Extract preferred brand(s) from deal fields and strategic intent data
    extractPreferredBrands(dealFields, strategicIntentData) {
        let preferredBrands = [];
        
        // First, try to get from Strategic Intent table (most reliable)
        const strategicPreferred = strategicIntentData?.['Preferred Brands'];
        if (strategicPreferred) {
            if (Array.isArray(strategicPreferred)) {
                preferredBrands = strategicPreferred.map(b => String(b).trim()).filter(b => b);
            } else if (typeof strategicPreferred === 'string') {
                preferredBrands = strategicPreferred.split(',').map(b => b.trim()).filter(b => b);
            }
        }
        
        // Fallback: Check deal fields
        if (preferredBrands.length === 0) {
            const dealFieldsToCheck = [
                'Preferred Brands',
                'Brand Preference',
                'Desired Brand'
            ];
            
            for (const field of dealFieldsToCheck) {
                if (dealFields[field]) {
                    if (Array.isArray(dealFields[field])) {
                        preferredBrands = dealFields[field].map(b => String(b).trim()).filter(b => b);
                    } else if (typeof dealFields[field] === 'string') {
                        preferredBrands = dealFields[field].split(',').map(b => b.trim()).filter(b => b);
                    }
                    if (preferredBrands.length > 0) break;
                }
            }
        }
        
        return preferredBrands.length > 0 ? preferredBrands : null;
    }

    async getLocationData(locationId) {
        if (!locationId) return {};
        const cached = this._fetchCache.location.get(locationId);
        if (cached !== undefined) return cached;
        const pending = this._fetchPending.location.get(locationId);
        if (pending) return pending;
        const promise = this._fetchLocationData(locationId);
        this._fetchPending.location.set(locationId, promise);
        try {
            const result = await promise;
            this._fetchCache.location.set(locationId, result);
            return result;
        } finally {
            this._fetchPending.location.delete(locationId);
        }
    }

    async _fetchLocationData(locationId) {
        // Security hard-stop: no direct browser Airtable calls.
        return {};
    }

    async getContactData(contactId) {
        if (!contactId) return {};
        const cached = this._fetchCache.contact.get(contactId);
        if (cached !== undefined) return cached;
        const pending = this._fetchPending.contact.get(contactId);
        if (pending) return pending;
        const promise = this._fetchContactData(contactId);
        this._fetchPending.contact.set(contactId, promise);
        try {
            const result = await promise;
            this._fetchCache.contact.set(contactId, result);
            return result;
        } finally {
            this._fetchPending.contact.delete(contactId);
        }
    }

    async _fetchContactData(contactId) {
        // Security hard-stop: no direct browser Airtable calls.
        return {};
    }

    async getUserData(userId) {
        // Security hard-stop: no direct browser Airtable calls.
        return null;
    }

    async getUserProfileImageByEmail(email) {
        if (!email || !email.trim()) return null;
        const key = String(email).trim().toLowerCase();
        const cached = this._fetchCache.userProfileByEmail.get(key);
        if (cached !== undefined) return cached;
        const pending = this._fetchPending.userProfileByEmail.get(key);
        if (pending) return pending;
        const promise = this._fetchUserProfileImageByEmail(key);
        this._fetchPending.userProfileByEmail.set(key, promise);
        try {
            const result = await promise;
            this._fetchCache.userProfileByEmail.set(key, result);
            return result;
        } finally {
            this._fetchPending.userProfileByEmail.delete(key);
        }
    }

    async _fetchUserProfileImageByEmail(cleanEmail) {
        // Security hard-stop: no direct browser Airtable calls.
        return null;
    }

    async getMarketPerformanceData(recordId) {
        if (!recordId) return {};
        const cached = this._fetchCache.marketPerf.get(recordId);
        if (cached !== undefined) return cached;
        const pending = this._fetchPending.marketPerf.get(recordId);
        if (pending) return pending;
        const promise = this._fetchMarketPerformanceData(recordId);
        this._fetchPending.marketPerf.set(recordId, promise);
        try {
            const result = await promise;
            this._fetchCache.marketPerf.set(recordId, result);
            return result;
        } finally {
            this._fetchPending.marketPerf.delete(recordId);
        }
    }

    async _fetchMarketPerformanceData(recordId) {
        // Security hard-stop: no direct browser Airtable calls.
        return {};
    }

    async getStrategicIntentData(recordId) {
        if (!recordId) return {};
        const cached = this._fetchCache.strategicIntent.get(recordId);
        if (cached !== undefined) return cached;
        const pending = this._fetchPending.strategicIntent.get(recordId);
        if (pending) return pending;
        const promise = this._fetchStrategicIntentData(recordId);
        this._fetchPending.strategicIntent.set(recordId, promise);
        try {
            const result = await promise;
            this._fetchCache.strategicIntent.set(recordId, result);
            return result;
        } finally {
            this._fetchPending.strategicIntent.delete(recordId);
        }
    }

    async _fetchStrategicIntentData(recordId) {
        // Security hard-stop: no direct browser Airtable calls.
        return {};
    }

    async calculateMatchScore(dealFields, locationData, brandNames, marketPerformanceData, strategicIntentData) {
        // Note: This uses a simplified calculation. For production, integrate the full 
        // calculation logic from production-brand-dashboard.js (calculateRealMatchScore method)
        // which includes all the detailed subscore calculations (MKT1, MKT2, SEG1, etc.)
        
        if (!brandNames || brandNames === 'Not specified') {
            return { score: 0, breakdown: {} };
        }
        
        // Get brand data
        const brandData = await this.getBrandData(brandNames);
        if (!brandData) {
            return { score: 0, breakdown: {} };
        }
        
        // Calculate comprehensive subscores with additional metrics
        const breakdown = {
            // Market & Recognition (22% weight)
            MKT1: await this.calculateMKT1(dealFields, locationData, brandData),
            MKT2: await this.calculateMKT2(dealFields, locationData, brandData),
            
            // Segment & Service Alignment (18% weight)
            SEG1: await this.calculateSEG1(dealFields, locationData, brandData),
            SVC1: await this.calculateSVC1(dealFields, locationData, brandData),
            
            // Property Characteristics (12% weight)
            SIZE1: await this.calculateSIZE1(dealFields, locationData, brandData),
            
            // Owner & Strategic Alignment
            OWN1: await this.calculateOWN1(dealFields, locationData, brandData, marketPerformanceData),
            STR1: await this.calculateSTR1(dealFields, locationData, brandData, strategicIntentData),
            
            // Standards & Amenities
            AMN1: await this.calculateAMN1(dealFields, locationData, brandData),
            FIN1: await this.calculateFIN1(dealFields, locationData, brandData, marketPerformanceData),
            
            INC1: await this.calculateINC1(dealFields, locationData, brandData, marketPerformanceData),
            PREF1: await this.calculatePREF1(dealFields, locationData, brandData, strategicIntentData),
            KEY1: await this.calculateKEY1(dealFields, locationData, brandData, marketPerformanceData),
            
            // Capital & Deal Terms (differentiation)
            CAP1: await this.calculateCAP1(dealFields, locationData, brandData, marketPerformanceData),
            TERM1: await this.calculateTERM1(dealFields, locationData, brandData, marketPerformanceData),
            
            PROJ1: await this.calculatePROJ1(dealFields, locationData, brandData),
            PROJ2: await this.calculatePROJ2(dealFields, locationData, brandData),
            PROJ3: await this.calculatePROJ3(dealFields, locationData, brandData),
            AGMT1: await this.calculateAGMT1(dealFields, locationData, brandData),
            ESG1: await this.calculateESG1(dealFields, locationData, brandData)
        };
        
        const weights = {
            MKT1: 10, MKT2: 2, SEG1: 10, SVC1: 5, SIZE1: 9,
            OWN1: 4, STR1: 4, AMN1: 6, FIN1: 6, INC1: 2, PREF1: 1,
            KEY1: 5, CAP1: 4, TERM1: 4,
            PROJ1: 9, PROJ2: 6, PROJ3: 3, AGMT1: 8, ESG1: 4
        };
        
        let weightedSum = 0;
        let totalWeight = 0;
        
        for (const [key, score] of Object.entries(breakdown)) {
            if (score !== null && score !== undefined) {
                weightedSum += score * weights[key];
                totalWeight += weights[key];
            }
        }
        
        const rawScore = totalWeight > 0 ? weightedSum / totalWeight : 0;
        // Small per-brand offset so same deal gets differentiated scores per brand (avoids ties)
        const brandStr = (brandNames || '').toString().trim();
        let brandOffset = 0;
        if (brandStr) {
            let h = 0;
            for (let i = 0; i < brandStr.length; i++) h = (h * 31 + brandStr.charCodeAt(i)) | 0;
            brandOffset = ((h % 101) - 50) / 100;
        }
        const finalScore = Math.round(Math.min(100, Math.max(0, rawScore + brandOffset)) * 10) / 10;
        
        return {
            score: finalScore,
            breakdown: breakdown
        };
    }

    async getBrandData(brandName) {
        const cacheKey = (brandName || '').toString().trim().toLowerCase();
        if (!cacheKey) return null;
        if (this._brandDataCache.has(cacheKey)) return this._brandDataCache.get(cacheKey);
        if (this._brandDataPending.has(cacheKey)) return this._brandDataPending.get(cacheKey);
        const pending = this._fetchBrandData(brandName);
        this._brandDataPending.set(cacheKey, pending);
        try {
            const data = await pending;
            this._brandDataCache.set(cacheKey, data);
            return data;
        } finally {
            this._brandDataPending.delete(cacheKey);
        }
    }

    async _fetchBrandData(brandName) {
        try {
            const base = window.location.origin || '';
            const url = `${base}/api/brand-library/brand?brandId=${encodeURIComponent(brandName)}`;
            const response = await fetch(url);
            if (response.ok) {
                const data = await response.json();
                if (data && (data.success || data.brandBasics || data.brandData || data.brand)) {
                    const brandBasics = data.brandBasics || data.brand || {};
                    const brandIdNumber = data.brandIdNumber ||
                        brandBasics['Brand_Basics_ID'] ||
                        brandBasics['Brand ID'] ||
                        brandBasics['Brand_ID'] ||
                        brandBasics['ID'] ||
                        brandBasics['Brand ID Number'] ||
                        brandBasics['BrandNumber'] ||
                        null;
                    return {
                        brandBasics,
                        brandFit: data.projectFit || data.brandFit || {},
                        brandFootprint: data.footprint || data.brandFootprint || {},
                        brandStandards: data.brandStandards || {},
                        brandFeeStructure: data.feeStructure || data.brandFeeStructure || {},
                        brandOperationalSupport: data.operationalSupport || data.brandOperationalSupport || {},
                        brandDealTerms: data.dealTerms || data.brandDealTerms || {},
                        brandId: data.brandId || data.recordId || null,
                        brandIdNumber: brandIdNumber
                    };
                }
            }
        } catch (error) {
            console.error('Error fetching brand data:', error);
        }
        return null;
    }

    async getBrandFitData(brandIdNumber) {
        // Security hard-stop: no direct browser Airtable calls.
        return {};
    }

    async getBrandFootprintData(brandIdNumber) {
        // Security hard-stop: no direct browser Airtable calls.
        return {};
    }

    async getBrandStandardsData(brandIdNumber) {
        // Security hard-stop: no direct browser Airtable calls.
        return {};
    }

    async getBrandFeeStructureData(brandIdNumber) {
        // Security hard-stop: no direct browser Airtable calls.
        return {};
    }

    async getBrandOperationalSupportData(brandIdNumber) {
        if (this._operationalSupportForbidden) return {};
        try {
            const base = window.location.origin || '';
            const url = `${base}/api/brand-library/operational-support?brandId=${encodeURIComponent(brandIdNumber)}`;
            const response = await this._airtableFetch(url);
            if (response.status === 403) {
                this._operationalSupportForbidden = true;
                return {};
            }
            if (response.ok) {
                const data = await response.json();
                if (data.fields) return data.fields;
            }
        } catch (error) {
            console.error('Error fetching brand operational support data:', error);
        }
        return {};
    }

    async getBrandDealTermsData(brandIdNumber) {
        // Security hard-stop: no direct browser Airtable calls.
        return {};
    }

    // Helper: Get chain scale tier (0-5) - CRITICAL for differentiating brands like Edition vs Residence Inn
    getChainScaleTier(chainScale) {
        if (!chainScale || typeof chainScale !== 'string') return 0;
        
        const tiers = {
            'Luxury': 5,
            'Upper Upscale': 4,
            'Upscale': 3,
            'Upper Midscale': 2,
            'Midscale': 1,
            'Economy': 0,
            'Independent': 0
        };
        
        const scaleLower = chainScale.toLowerCase();
        for (const [scale, tier] of Object.entries(tiers)) {
            if (scaleLower.includes(scale.toLowerCase())) {
                return tier;
            }
        }
        return 0;
    }

    // Helper: Map country to region
    mapCountryToRegion(country) {
        if (!country) return 'Americas';
        
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
        
        return regionMap[country] || 'Americas';
    }

    // Helper: Get regional threshold for recognition density
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

    // Helper: Get country region mapping
    getCountryRegionMapping() {
        return {
            'United States': { region1: 'Americas', region2: 'North America', region3: 'United States', region: 'Americas' },
            'Canada': { region1: 'Americas', region2: 'North America', region3: 'Canada', region: 'Americas' },
            'United Kingdom': { region1: 'Western Europe', region2: 'Northern Europe', region3: 'United Kingdom', region: 'EU' },
            'Germany': { region1: 'Western Europe', region2: 'Central Europe', region3: 'Germany', region: 'EU' },
            'France': { region1: 'Western Europe', region2: 'Southern Europe', region3: 'France', region: 'EU' }
        };
    }

    // MKT1: Priority Market fit (weight: 14) – checkbox columns + multi-select Priority Markets / Markets to Avoid
    async calculateMKT1(dealFields, locationData, brandData) {
        const dealCountry = (locationData?.Country || dealFields['Country'] || '').toString().trim();
        const brandFit = brandData.brandFit || {};
        const brandBasics = brandData.brandBasics || {};
        const priorityMarkets = [];
        const projectFitCheckboxColumns = [
            'Global - Priority Markets', 'United States - Priority Markets', 'Canada - Priority Markets',
            'Northeast (US) - Priority Markets', 'Southeast (US) - Priority Markets', 'Midwest (US) - Priority Markets',
            'Southwest (US) - Priority Markets', 'West (US) - Priority Markets', 'Pacific (US) - Priority Markets',
            'Mexico - Priority Markets', 'Central America - Priority Markets', 'Caribbean - Priority Markets',
            'South America - Priority Markets', 'Latin America - Priority Markets', 'Middle East - Priority Markets',
            'Western Europe - Priority Markets', 'Eastern Europe - Priority Markets', 'Southern Europe - Priority Markets',
            'Northern Europe - Priority Markets', 'Nordic Countries - Priority Markets', 'United Kingdom - Priority Markets',
            'Other - Priority Markets'
        ];
        for (const column of projectFitCheckboxColumns) {
            if (brandFit[column] === true || brandFit[column] === 'Yes') {
                const regionName = column.replace(' - Priority Markets', '').trim();
                if (regionName && !priorityMarkets.includes(regionName)) priorityMarkets.push(regionName);
            }
        }
        const priorityMultiSelect = brandFit['Priority Markets'];
        if (Array.isArray(priorityMultiSelect) && priorityMultiSelect.length > 0) {
            priorityMultiSelect.forEach(item => {
                const name = (typeof item === 'string' ? item : (item && item.name) ? item.name : '').trim();
                if (name) {
                    const regionName = name.replace(/ - Priority Markets$/i, '').trim();
                    if (regionName && !priorityMarkets.includes(regionName)) priorityMarkets.push(regionName);
                }
            });
        }
        if (priorityMarkets.some(market => market.toLowerCase().includes('global'))) {
            return 100;
        }
        let marketsToAvoid = brandBasics['Markets to Avoid or Saturated'] || [];
        if (!Array.isArray(marketsToAvoid)) marketsToAvoid = marketsToAvoid ? [marketsToAvoid] : [];
        const avoidMultiSelect = brandFit['Markets to Avoid'];
        if (Array.isArray(avoidMultiSelect) && avoidMultiSelect.length > 0) {
            avoidMultiSelect.forEach(item => {
                const name = (typeof item === 'string' ? item : (item && item.name) ? item.name : '').trim();
                if (name && !marketsToAvoid.some(m => (m || '').toString().toLowerCase() === name.toLowerCase())) {
                    marketsToAvoid.push(name);
                }
            });
        }
        const countryMapping = this.getCountryRegionMapping();
        const dealRegions = countryMapping[dealCountry] || { region1: '', region2: '', region3: '', region: 'Global' };
        const dealRegionsList = [dealCountry, dealRegions.region1, dealRegions.region2, dealRegions.region3].filter(r => r && r.trim() !== '');
        const isHardFail = marketsToAvoid.some(market => {
            const m = (market || '').toString().toLowerCase();
            return dealRegionsList.some(region => {
                const r = (region || '').toLowerCase();
                return m.includes(r) || r.includes(m);
            });
        });
        if (isHardFail) return 0;
        if (priorityMarkets.length === 0) return null;
        let bestScore = 0;
        for (const market of priorityMarkets) {
            const marketLower = market.toLowerCase();
            if (dealCountry.toLowerCase().includes(marketLower) || marketLower.includes(dealCountry.toLowerCase())) {
                if (100 > bestScore) bestScore = 100;
            }
            if (dealRegions.region1 && (dealRegions.region1.toLowerCase().includes(marketLower) || marketLower.includes(dealRegions.region1.toLowerCase()))) {
                if (90 > bestScore) bestScore = 90;
            }
            if (dealRegions.region2 && (dealRegions.region2.toLowerCase().includes(marketLower) || marketLower.includes(dealRegions.region2.toLowerCase()))) {
                if (80 > bestScore) bestScore = 80;
            }
            if (dealRegions.region3 && (dealRegions.region3.toLowerCase().includes(marketLower) || marketLower.includes(dealRegions.region3.toLowerCase()))) {
                if (80 > bestScore) bestScore = 80;
            }
        }
        return bestScore > 0 ? bestScore : 25;
    }

    // MKT2: Recognition density vs owner need (weight: 8)
    async calculateMKT2(dealFields, locationData, brandData) {
        const dealCountry = locationData?.Country || dealFields['Country'] || '';
        const brandRecognitionNeedRaw = dealFields['Importance of Brand Recognition'];
        const brandRecognitionNeed = typeof brandRecognitionNeedRaw === 'number' ? brandRecognitionNeedRaw : (parseInt(brandRecognitionNeedRaw, 10) || 0);
        if (brandRecognitionNeedRaw === undefined || brandRecognitionNeedRaw === null || brandRecognitionNeedRaw === '') return null;
        const region = this.mapCountryToRegion(dealCountry);
        const openHotelsInRegion = (brandData.brandFootprint || {})[`Number of Open Hotels (${region})`] || 0;
        if (brandRecognitionNeed >= 4) {
            const threshold = this.getRegionalThreshold(region);
            return openHotelsInRegion >= threshold ? 100 : Math.max(20, 60 - (threshold - openHotelsInRegion) * 5);
        }
        return openHotelsInRegion > 0 ? 95 : 85;
    }

    // SEG1: Chain scale proximity (weight: 10) – graduated so different brands score differently
    async calculateSEG1(dealFields, locationData, brandData) {
        const brandChainScaleRaw = (brandData.brandBasics || {})['Hotel Chain Scale'] || '';
        const dealChainScaleRaw = locationData?.['Hotel Chain Scale'] || dealFields['Hotel Chain Scale'] || '';
        if (!brandChainScaleRaw.trim() || !dealChainScaleRaw.trim() ||
            brandChainScaleRaw.toLowerCase().includes('unknown') ||
            dealChainScaleRaw.toLowerCase().includes('unknown')) {
            return null;
        }
        const brandChainScale = this.getChainScaleTier(brandChainScaleRaw);
        const dealChainScale = this.getChainScaleTier(dealChainScaleRaw);
        const tierDiff = Math.abs(brandChainScale - dealChainScale);
        if (tierDiff === 0) return 100;
        if (tierDiff === 1) return 82;
        if (tierDiff === 2) return 58;
        if (tierDiff === 3) return 35;
        return 18;
    }

    // SVC1: Service model alignment (weight: 8)
    async calculateSVC1(dealFields, locationData, brandData) {
        const brandServiceModel = (brandData.brandBasics || {})['Hotel Service Model'] || '';
        const dealServiceModel = (locationData?.['Hotel Service Model'] || dealFields['Hotel Service Model'] || '').toString().trim();
        const brandModel = (brandData.brandBasics || {})['Brand Model / Format'] || '';
        if (!brandServiceModel.trim() || !dealServiceModel ||
            brandServiceModel.toLowerCase().includes('unknown') || dealServiceModel.toLowerCase().includes('unknown')) {
            return null;
        }
        if (brandServiceModel.toLowerCase() === dealServiceModel.toLowerCase()) return 100;
        const isFlexible = (brandModel + '').toLowerCase().includes('soft') ||
            (brandModel + '').toLowerCase().includes('conversion') ||
            (brandModel + '').toLowerCase().includes('collection');
        return isFlexible ? 55 : 12;
    }

    // SIZE1: Ideal room-range fit (weight: 12)
    async calculateSIZE1(dealFields, locationData, brandData) {
        const dealRooms = parseInt(locationData?.['Total Number of Rooms/Keys'], 10) || parseInt(dealFields['Total Number of Rooms/Keys'], 10) || 0;
        const projectFitData = brandData.brandFit || {};
        const minRooms = projectFitData?.['Min - Room Count'] ||
                        projectFitData?.['Min - Ideal Project Size'] ||
                        projectFitData?.['A Min - Ideal Project Size'] ||
                        projectFitData?.['Minimum Rooms'] || 0;
        const maxRooms = projectFitData?.['Max - Room Count'] ||
                        projectFitData?.['Max - Ideal Project Size'] ||
                        projectFitData?.['A Max - Ideal Project Size'] ||
                        projectFitData?.['Maximum Rooms'] || 0;
        const brandHasRange = (minRooms && maxRooms) || (minRooms && minRooms > 0) || (maxRooms && maxRooms > 0);
        if (dealRooms <= 0 && brandHasRange) return null;
        if (!minRooms || !maxRooms) {
            if (dealRooms >= 100 && dealRooms <= 300) return 100;
            if (dealRooms >= 50 && dealRooms < 100) return 78;
            if (dealRooms > 300 && dealRooms <= 500) return 68;
            return dealRooms > 0 ? 52 : null;
        }
        if (dealRooms >= minRooms && dealRooms <= maxRooms) {
            return 100;
        }
        
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

    // AMN1: Required standards & amenities – Brand Standards (text, Sustainability Features, Compliance & Safety, F&B, parking)
    async calculateAMN1(dealFields, locationData, brandData) {
        const brandStandards = brandData.brandStandards || {};
        const requiredStandards = brandStandards['Brand Standards'] || '';
        const sustainabilityFeatures = brandStandards['Sustainability Features'];
        const complianceSafety = brandStandards['Compliance & Safety'];
        const brandParkingRequired = brandStandards['Parking Required'] || brandStandards['Onsite Parking'] || '';
        const amenities = {
            pool: dealFields['Pool'] || locationData?.['Pool'] || false,
            lobby: dealFields['Lobby'] || locationData?.['Lobby'] || false,
            coworking: dealFields['Co-working or lounge space'] || locationData?.['Co-working or lounge space'] || false,
            bar: dealFields['Bar or Beverage Concept'] || locationData?.['Bar or Beverage Concept'] || false,
            businessCenter: dealFields['Business Center'] || locationData?.['Business Center'] || false,
            petAmenities: dealFields['Pet Amenities'] || locationData?.['Pet Amenities'] || false,
            solarPower: dealFields['Solar Power'] || locationData?.['Solar Power'] || false,
            meetingRooms: parseInt(dealFields['Number of Meeting Rooms']) || parseInt(locationData?.['Number of Meeting Rooms']) || 0,
            fboOutlets: parseInt(dealFields['Number of F&B Outlets']) || parseInt(locationData?.['Number of F&B Outlets']) || 0,
            parkingSpaces: parseInt(dealFields['Number of Parking Spaces']) || parseInt(locationData?.['Number of Parking Spaces']) || 0,
            sustainability: (dealFields['Sustainability'] || locationData?.['Sustainability'] || '').toString().toLowerCase()
        };
        let score = 100;
        if (requiredStandards && requiredStandards.trim() !== '') {
            const requiredItems = requiredStandards.split(';').filter(item => item.trim());
            for (const required of requiredItems) {
                const requiredLower = required.toLowerCase().trim();
                let hasMatch = false;
                if (requiredLower.includes('pool') && amenities.pool) hasMatch = true;
                else if (requiredLower.includes('lobby') && amenities.lobby) hasMatch = true;
                else if (requiredLower.includes('coworking') && amenities.coworking) hasMatch = true;
                else if (requiredLower.includes('bar') && amenities.bar) hasMatch = true;
                else if (requiredLower.includes('business') && amenities.businessCenter) hasMatch = true;
                else if (requiredLower.includes('pet') && amenities.petAmenities) hasMatch = true;
                else if (requiredLower.includes('solar') && amenities.solarPower) hasMatch = true;
                else if (requiredLower.includes('meeting') && amenities.meetingRooms > 0) hasMatch = true;
                else if (requiredLower.includes('f&b') && amenities.fboOutlets > 0) hasMatch = true;
                else if (requiredLower.includes('parking') && amenities.parkingSpaces > 0) hasMatch = true;
                if (!hasMatch) score -= 12;
            }
        }
        const sustainList = Array.isArray(sustainabilityFeatures) ? sustainabilityFeatures : (sustainabilityFeatures && typeof sustainabilityFeatures === 'string' ? [sustainabilityFeatures] : []);
        if (sustainList.length > 0 && !amenities.sustainability && !amenities.solarPower) score = Math.max(0, score - 15);
        const complianceList = Array.isArray(complianceSafety) ? complianceSafety : (complianceSafety && typeof complianceSafety === 'string' ? [complianceSafety] : []);
        if (complianceList.length > 0) {
            const dealCompliance = (dealFields['Compliance'] || locationData?.['Compliance & Safety'] || '').toString().toLowerCase();
            if (!dealCompliance && complianceList.length > 0) score = Math.max(0, score - 10);
        }
        if (brandParkingRequired && (brandParkingRequired + '').toLowerCase().includes('yes') && amenities.parkingSpaces === 0) score = Math.max(0, score - 10);
        return Math.max(0, score);
    }

    // Helper: Parse single fee from string
    parseSingleFee(feeString) {
        if (!feeString || feeString.trim() === '' || feeString.toLowerCase().includes('not specified')) {
            return null;
        }
        const numbers = feeString.match(/\d+(?:\.\d+)?/g);
        if (numbers && numbers.length >= 1) {
            return parseFloat(numbers[0]) || 0;
        }
        return null;
    }

    // Helper: Get related owner types for matching
    getRelatedOwnerTypes() {
        return {
            'Developer': 'PE',
            'Family Office': 'HNW',
            'Private Investor': 'HNW',
            'Institutional': 'PE',
            'HNW': 'Private Investor',
            'PE': 'Institutional'
        };
    }

    // OWN1: Owner/Investor type + involvement + non-negotiables (enhanced for differentiation)
    async calculateOWN1(dealFields, locationData, brandData, marketPerformanceData) {
        const brandFit = brandData.brandFit || {};
        const op = brandData.brandOperationalSupport || {};
        const dealOwnership = (marketPerformanceData?.['Ownership Structure'] || '').toString().trim();
        const preferredOwners = (brandFit['Preferred Owner/Investor Type'] || '').toString().trim();
        const dealInvolvement = (marketPerformanceData?.['Owner Involvement Level'] || dealFields['Owner Involvement Level'] || '').toString().trim();
        const dealNonNegRaw = (dealFields['Owner Non-Negotiables'] || dealFields['Must-Haves'] || marketPerformanceData?.['Owner Non-Negotiables'] || '').toString().trim();
        const dealNonNegList = dealNonNegRaw ? dealNonNegRaw.split(/[,;]/).map(s => s.trim().toLowerCase()).filter(Boolean) : [];
        const ownerInvolvementCols = [
            'Silent Investor - Owner Involvement', 'High-level Oversight Only - Owner Involvement',
            'Hands-on in Operations - Owner Involvement', 'Family in Key Staff Roles - Owner Involvement'
        ];
        const ownerNonNegCols = [
            'Key Vendors / Contracts - Owner Non-Negotiables', 'Family Employees in Hotel Roles - Owner Non-Negotiables',
            'Specific Design / Branding Elements - Owner Non-Negotiables', 'ADR / Positioning Philosophy - Owner Non-Negotiables',
            'Minimum Services / Amenities - Owner Non-Negotiables', 'Other - Owner Non-Negotiables'
        ];
        const brandAcceptableInvolvement = ownerInvolvementCols.filter(col => brandFit[col] === true || brandFit[col] === 'Yes' || brandFit[col] === 'Acceptable');
        const brandNonNegTypes = ownerNonNegCols.filter(col => brandFit[col] === true || brandFit[col] === 'Yes' || brandFit[col] === 'Acceptable');
        let score = 50;
        let hasAnySignal = false;
        if (dealOwnership && preferredOwners && dealOwnership.toLowerCase() !== 'unknown' && preferredOwners.toLowerCase() !== 'unknown') {
            hasAnySignal = true;
            if (preferredOwners.toLowerCase().includes(dealOwnership.toLowerCase()) || dealOwnership.toLowerCase().includes(preferredOwners.toLowerCase())) {
                score = 100;
            } else {
                const relatedTypes = this.getRelatedOwnerTypes();
                const hasRelated = Object.entries(relatedTypes).some(([from, related]) =>
                    (dealOwnership.toLowerCase().includes(from.toLowerCase()) && preferredOwners.toLowerCase().includes(related.toLowerCase())) ||
                    (dealOwnership.toLowerCase().includes(related.toLowerCase()) && preferredOwners.toLowerCase().includes(from.toLowerCase()))
                );
                score = hasRelated ? 72 : 38;
            }
        }
        if (dealInvolvement && brandAcceptableInvolvement.length > 0) {
            hasAnySignal = true;
            const dealInvLower = dealInvolvement.toLowerCase();
            const brandAccepts = brandAcceptableInvolvement.some(col => {
                const label = col.replace(/\s*-\s*Owner Involvement$/, '').toLowerCase();
                return dealInvLower.includes(label) || label.includes(dealInvLower);
            });
            const ownerComponent = score === 50 ? 70 : score;
            score = brandAccepts ? Math.max(score, 92) : Math.min(score, 45);
        }
        if (dealNonNegList.length > 0 && brandNonNegTypes.length > 0) {
            hasAnySignal = true;
            const brandNonNegLabels = brandNonNegTypes.map(col => col.replace(/\s*-\s*Owner Non-Negotiables$/, '').toLowerCase());
            const conflictCount = dealNonNegList.filter(d => brandNonNegLabels.some(b => b.includes(d) || d.includes(b))).length;
            if (conflictCount > 0) score = Math.min(score, 35);
        }
        if (!hasAnySignal) return null;
        return Math.min(100, Math.max(0, score));
    }

    // CAP1: Capital readiness fit – deal funding status vs brand acceptable capital status (weight: 4)
    async calculateCAP1(dealFields, locationData, brandData, marketPerformanceData) {
        const brandFit = brandData.brandFit || {};
        const capitalCols = [
            'Equity and Debt Fully Committed - Capital & Risk', 'Equity Committed, Debt in Process - Capital & Risk',
            'Equity in Process, Debt Not Started - Capital & Risk', 'Both Equity and Debt Still Being Raised - Capital & Risk'
        ];
        const brandAcceptable = capitalCols.filter(col => brandFit[col] === true || brandFit[col] === 'Yes' || brandFit[col] === 'Acceptable');
        if (brandAcceptable.length === 0) return 50;
        const dealCapitalRaw = (marketPerformanceData?.['Capital Status'] || marketPerformanceData?.['Funding Status'] || marketPerformanceData?.['Equity vs Debt Split'] || dealFields['Capital Status'] || dealFields['Funding Status'] || '').toString().trim().toLowerCase();
        if (!dealCapitalRaw) return null;
        const dealMatches = brandAcceptable.some(col => {
            const label = col.replace(/\s*-\s*Capital\s*&\s*Risk$/, '').toLowerCase();
            return dealCapitalRaw.includes('fully committed') && label.includes('fully committed') ||
                dealCapitalRaw.includes('debt in process') && label.includes('debt in process') ||
                dealCapitalRaw.includes('equity in process') && label.includes('equity in process') ||
                dealCapitalRaw.includes('still being raised') && label.includes('still being raised') ||
                label.includes(dealCapitalRaw) || dealCapitalRaw.includes(label);
        });
        return dealMatches ? 100 : 28;
    }

    // TERM1: Deal terms alignment – initial term, performance test, conversion timeline (weight: 4)
    async calculateTERM1(dealFields, locationData, brandData, marketPerformanceData) {
        const brandTerms = brandData.brandDealTerms || {};
        const dealTermRaw = (dealFields['Target Initial Term'] || dealFields['Initial Term'] || marketPerformanceData?.['Target Initial Term'] || marketPerformanceData?.['Initial Term'] || '').toString().trim();
        const dealPerfTest = (dealFields['Performance Test Required'] || marketPerformanceData?.['Performance Test Required'] || '').toString().trim().toLowerCase();
        const dealConvTime = (dealFields['Conversion Timeline'] || marketPerformanceData?.['Conversion Timeline'] || '').toString().trim();
        const brandMinTermQty = brandTerms['Quantity - Typical Minimum Initial Term'] ?? brandTerms['Min Initial Term (Quantity)'];
        const brandMinTermLen = (brandTerms['Length - Typical Minimum Initial Term'] || brandTerms['Duration - Typical Minimum Initial Term'] || '').toString().toLowerCase();
        const brandPerfTest = (brandTerms['Performance Test Requirement'] || '').toString().trim().toLowerCase();
        const brandConvMax = (brandTerms['Conversion - Typical max time allowed for completion'] || '').toString().trim();
        let signals = 0;
        let matches = 0;
        if (dealTermRaw && (brandMinTermQty != null || brandMinTermLen)) {
            signals++;
            const dealYears = parseFloat(dealTermRaw) || 0;
            const brandYears = typeof brandMinTermQty === 'number' ? brandMinTermQty : parseFloat(brandMinTermQty);
            if (!isNaN(brandYears) && dealYears >= brandYears * 0.9) matches++;
            else if (isNaN(brandYears)) matches++;
        }
        if (dealPerfTest && (dealPerfTest === 'yes' || dealPerfTest === 'no')) {
            signals++;
            if (!brandPerfTest) matches++;
            else if ((dealPerfTest === 'yes' && (brandPerfTest.includes('yes') || brandPerfTest.includes('required'))) || (dealPerfTest === 'no' && (brandPerfTest.includes('no') || !brandPerfTest.includes('required')))) matches++;
        }
        if (dealConvTime && brandConvMax) {
            signals++;
            const dealMonths = parseFloat(dealConvTime) || 0;
            const brandMonths = parseFloat(brandConvMax) || 0;
            if (!isNaN(brandMonths) && dealMonths <= brandMonths * 1.2) matches++;
            else if (isNaN(brandMonths)) matches++;
        }
        if (signals === 0) return null;
        return matches === signals ? 100 : Math.round(40 + (matches / signals) * 50);
    }

    // STR1: Strategic brand model preference (weight: 4)
    async calculateSTR1(dealFields, locationData, brandData, strategicIntentData) {
        const brandSoftCollection = ((brandData.brandFit || {})['Soft/Collection Brand'] || '').toString().trim();
        const dealPreference = (strategicIntentData?.['Soft vs Hard Brand Preference'] || '').toString().trim();
        if (!dealPreference || dealPreference.toLowerCase() === 'unknown') {
            return null;
        }
        
        const isBrandSoft = brandSoftCollection.toLowerCase() === 'yes';
        const isBrandHard = brandSoftCollection.toLowerCase() === 'no';
        
        const dealPrefLower = dealPreference.toLowerCase();
        const isDealSoft = dealPrefLower.includes('soft brand');
        const isDealHard = dealPrefLower.includes('hard brand');
        const isDealOpenToBoth = dealPrefLower.includes('open to both') || dealPrefLower.includes('unsure');
        
        if (isDealOpenToBoth) {
            return 100; // Deal is open to both
        }
        
        if ((isBrandSoft && isDealSoft) || (isBrandHard && isDealHard)) {
            return 100; // Perfect match
        }
        
        if ((isBrandSoft && isDealHard) || (isBrandHard && isDealSoft)) {
            return 15;
        }
        return 50;
    }

    // PREF1: Preferred brand bonus (weight: 2)
    async calculatePREF1(dealFields, locationData, brandData, strategicIntentData) {
        const brandName = (brandData.brandBasics || {})['Brand Name'] || '';
        const preferredBrands = strategicIntentData?.['Preferred Brands'] || '';
        
        if (!preferredBrands) {
            return 0; // No preference = no bonus
        }
        
        // Handle both string and array formats
        let preferredList = [];
        if (Array.isArray(preferredBrands)) {
            preferredList = preferredBrands.map(brand => brand.toString().trim().toLowerCase());
        } else if (typeof preferredBrands === 'string') {
            preferredList = preferredBrands.split(',').map(brand => brand.trim().toLowerCase());
        }
        
        const hasBrandMatch = preferredList.some(prefBrand => 
            prefBrand.includes(brandName.toLowerCase()) ||
            brandName.toLowerCase().includes(prefBrand)
        );
        
        return hasBrandMatch ? 100 : 0;
    }

    // FIN1: Fees tolerance - Enhanced with detailed fee comparison (weight: 10)
    async calculateFIN1(dealFields, locationData, brandData, marketPerformanceData) {
        const dealRoyaltyFee = marketPerformanceData?.['Royalty Fee Expectations'] || '';
        const dealMarketingFee = marketPerformanceData?.['Marketing Fee Expectations'] || '';
        const dealLoyaltyFee = marketPerformanceData?.['Loyalty Fee Expectations'] || '';
        
        const brandFeeStructure = brandData.brandFeeStructure || {};
        
        // Parse deal fees
        const dealFees = {
            royalty: this.parseSingleFee(dealRoyaltyFee),
            marketing: this.parseSingleFee(dealMarketingFee),
            loyalty: this.parseSingleFee(dealLoyaltyFee)
        };
        
        let totalScore = 0;
        let feeCount = 0;
        
        // Compare each fee type
        const feeTypes = [
            { key: 'royalty', minField: 'Min - Typical Royalty Fee Range', maxField: 'Max - Typical Royalty Fee Range' },
            { key: 'marketing', minField: 'Min - Typical Marketing Fee Range', maxField: 'Max - Typical Marketing Fee Range' },
            { key: 'loyalty', minField: 'Min - Typical Loyalty Program Fee', maxField: 'Max - Typical Loyalty Program Fee' }
        ];
        
        for (const feeType of feeTypes) {
            const dealFee = dealFees[feeType.key];
            const brandMinRaw = brandFeeStructure[feeType.minField] || 0;
            const brandMaxRaw = brandFeeStructure[feeType.maxField] || 0;
            
            // Parse brand min/max (handle % symbols and decimals)
            let brandMin = typeof brandMinRaw === 'string' ? parseFloat(brandMinRaw.replace('%', '')) : brandMinRaw;
            let brandMax = typeof brandMaxRaw === 'string' ? parseFloat(brandMaxRaw.replace('%', '')) : brandMaxRaw;
            
            // Convert decimals to percentages if needed
            if (brandMin < 1 && brandMax < 1 && brandMax > 0) {
                brandMin = brandMin * 100;
                brandMax = brandMax * 100;
            }
            
            if (dealFee !== null && brandMin !== undefined && brandMax !== undefined && brandMax > 0) {
                feeCount++;
                
                if (dealFee >= brandMin && dealFee <= brandMax) {
                    totalScore += 100; // Perfect match
                } else if (dealFee > brandMax) {
                    // Deal willing to pay more - calculate excess
                    const excessPercentage = ((dealFee - brandMax) / brandMax) * 100;
                    if (excessPercentage <= 10) totalScore += 85;
                    else if (excessPercentage <= 25) totalScore += 70;
                    else if (excessPercentage <= 50) totalScore += 50;
                    else totalScore += 25;
                } else if (dealFee < brandMin) {
                    // Deal wants to pay less - calculate shortfall
                    const shortfallPercentage = ((brandMin - dealFee) / brandMin) * 100;
                    if (shortfallPercentage <= 10) totalScore += 75;
                    else if (shortfallPercentage <= 25) totalScore += 50;
                    else if (shortfallPercentage <= 50) totalScore += 25;
                    else totalScore += 0;
                }
            }
        }
        
        return feeCount > 0 ? Math.round(totalScore / feeCount) : 75; // Default to neutral if no fee data
    }

    // Helper: deal explicitly needs key money (from Market Performance)
    dealNeedsKeyMoney(marketPerformanceData) {
        if (!marketPerformanceData || typeof marketPerformanceData !== 'object') return false;
        const keyMoneyAllowance = (marketPerformanceData['Key Money or TI Allowance'] || marketPerformanceData['Key Money / TI Allowance'] || '').toString().trim().toLowerCase();
        const filterNoKeyMoney = (marketPerformanceData['Would you like to filter out brands without key money?'] || marketPerformanceData['Would You Like to Filter Out Brands Without Key Money?'] || '').toString().trim().toLowerCase();
        if (filterNoKeyMoney === 'yes') return true;
        if (keyMoneyAllowance && keyMoneyAllowance !== 'no key money / ti support' && keyMoneyAllowance !== 'not applicable') return true;
        return false;
    }

    // Helper: brand offers key money (Operational Support types or Fee Structure)
    brandOffersKeyMoney(brandData) {
        const op = brandData.brandOperationalSupport || {};
        const fee = brandData.brandFeeStructure || {};
        const typesRaw = op['Types of Incentives That Might be Offered'] || op['Types of Incentives'] || op.typesOfIncentives;
        const arr = Array.isArray(typesRaw) ? typesRaw : (typeof typesRaw === 'string' && typesRaw.trim() ? typesRaw.split(',').map(s => s.trim()) : []);
        const hasInTypes = arr.some(v => /key\s*money|upfront\s*incentive|co-?investment/i.test(String(v)));
        const feeKeyMoney = fee['Key Money / Co-Investment'] || fee['Key Money / Upfront Incentive'];
        const opKeyMoney = op['Key Money / Upfront Incentive'] === true || op['Key Money / Upfront Incentive'] === 'Yes' || op['Key Money / Co-Investment'] === true || op['Key Money / Co-Investment'] === 'Yes';
        return !!(hasInTypes || (feeKeyMoney && String(feeKeyMoney).trim()) || opKeyMoney);
    }

    // KEY1: Key Money Willingness Fit – first-class factor when deal needs key money (weight: 6)
    async calculateKEY1(dealFields, locationData, brandData, marketPerformanceData) {
        const dealNeeds = this.dealNeedsKeyMoney(marketPerformanceData);
        const brandOffers = this.brandOffersKeyMoney(brandData);
        if (!dealNeeds) return 100; // No requirement → no penalty
        if (brandOffers) return 100; // Deal needs and brand offers → strong match
        return 18;   // Deal needs key money but brand does not offer → strong penalty (differentiator)
    }

    // INC1: Incentives match - Enhanced with detailed comparison; Key Money is scored separately in KEY1
    async calculateINC1(dealFields, locationData, brandData, marketPerformanceData) {
        const brandWillingToNegotiate = (brandData.brandOperationalSupport || {})['Willing to Negotiate Incentives'] === 'Yes' || (brandData.brandOperationalSupport || {})['Willing to Negotiate Incentives?'] === 'Yes';
        const brandIncentives = brandData.brandOperationalSupport || {};
        const dealIncentives = marketPerformanceData || {};
        
        if (!brandWillingToNegotiate) {
            return 40; // Brand not willing to negotiate
        }
        
        // List of common incentive fields to compare (Key Money is in KEY1; still count here for general incentives match)
        const incentiveFields = [
            'Lower Initial Fees', 'Tiered Fee Structure', 'Temporary Royalty Discounts',
            'Performance-Based Royalties', 'Performance Bonuses', 'Brand Loyalty Rewards',
            'Shorter Contract Durations', 'Termination Flexibility', 'Financing Assistance',
            'Construction or Renovation Grants', 'Comprehensive Training Packages',
            'Ongoing Operational Support', 'Co-op Advertising Funds', 'Local Marketing Programs',
            'Technology Upgrades', 'Data Analytics Tools', 'Protected Territories',
            'Expansion Incentives', 'Key Money', 'Key Money / Upfront Incentive', 'Key Money / Co-Investment'
        ];
        
        let matches = 0;
        let totalDealIncentives = 0;
        
        for (const field of incentiveFields) {
            const brandOffers = brandIncentives[field] === true || brandIncentives[field] === 'Yes' || (field === 'Key Money' && this.brandOffersKeyMoney(brandData)) || (field.startsWith('Key Money') && this.brandOffersKeyMoney(brandData));
            const dealSeeks = dealIncentives[field] === true || dealIncentives[field] === 'Yes' || (field === 'Key Money' && this.dealNeedsKeyMoney(marketPerformanceData));
            
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
        const score = Math.round(80 + (matchPercentage * 20));
        
        return Math.min(100, score);
    }

    // PROJ1: Project type compatibility - HARD FAIL if not acceptable (weight: 15)
    // Uses Brand Setup Project Fit column names: New Build, Conversion - Reflag, Renovation / Repositioning, Expansion / Add-on
    async calculatePROJ1(dealFields, locationData, brandData) {
        const projectTypeRaw = (dealFields['Project Type'] || locationData?.['Project Type'] || 'Unknown').toString().trim();
        const brandFit = brandData.brandFit || {};
        const projectTypeLower = projectTypeRaw.toLowerCase();
        const columnNames = [
            'New Build - Acceptable Project Type',
            'Conversion - Reflag - Acceptable Project Type',
            'Renovation / Repositioning - Acceptable Project Type',
            'Expansion / Add-on - Acceptable Project Type'
        ];
        let criteriaToCheck = null;
        if (projectTypeLower.includes('new build') || projectTypeLower.includes('new construction')) {
            criteriaToCheck = columnNames[0];
        } else if (projectTypeLower.includes('conversion') || projectTypeLower.includes('reflag')) {
            criteriaToCheck = columnNames[1];
        } else if (projectTypeLower.includes('renovation') || projectTypeLower.includes('repositioning')) {
            criteriaToCheck = columnNames[2];
        } else if (projectTypeLower.includes('expansion') || projectTypeLower.includes('add-on')) {
            criteriaToCheck = columnNames[3];
        }
        if (!criteriaToCheck) return null;
        const isAcceptable = brandFit[criteriaToCheck] === true ||
            brandFit[criteriaToCheck] === 'Yes' ||
            brandFit[criteriaToCheck] === 'Acceptable';
        return isAcceptable ? 100 : 22;
    }

    // PROJ2: Building type compatibility - HARD FAIL if not acceptable (weight: 10)
    // All seven Brand Setup Project Fit building types supported
    async calculatePROJ2(dealFields, locationData, brandData) {
        const buildingTypeRaw = (locationData?.['Building Type'] || dealFields['Building Type'] || 'Unknown').toString().trim();
        const brandFit = brandData.brandFit || {};
        const buildingTypeMapping = {
            'High-Rise': 'High-Rise - Acceptable Building Type',
            'Mid-Rise': 'Mid-Rise - Acceptable Building Type',
            'Low-Rise': 'Low-Rise - Acceptable Building Type',
            'Resort-Style Compound': 'Resort-Style Compound - Acceptable Building Type',
            'Podium / Tower': 'Podium / Tower - Acceptable Building Type',
            'Podium/Tower': 'Podium / Tower - Acceptable Building Type',
            'Mixed-Use': 'Mixed-Use - Acceptable Building Type',
            'Mixed Use': 'Mixed-Use - Acceptable Building Type',
            'Historic / Renovated': 'Historic / Renovated - Acceptable Building Type',
            'Historic/Renovated': 'Historic / Renovated - Acceptable Building Type',
            'Historic': 'Historic / Renovated - Acceptable Building Type'
        };
        let criteriaToCheck = buildingTypeMapping[buildingTypeRaw];
        if (!criteriaToCheck) {
            const btLower = buildingTypeRaw.toLowerCase();
            const columnToLabel = [
                ['High-Rise - Acceptable Building Type', 'high-rise'],
                ['Mid-Rise - Acceptable Building Type', 'mid-rise'],
                ['Low-Rise - Acceptable Building Type', 'low-rise'],
                ['Mixed-Use - Acceptable Building Type', 'mixed-use'],
                ['Podium / Tower - Acceptable Building Type', 'podium'],
                ['Historic / Renovated - Acceptable Building Type', 'historic'],
                ['Resort-Style Compound - Acceptable Building Type', 'resort']
            ];
            for (const [col, label] of columnToLabel) {
                if (btLower.includes(label) || btLower.includes(col.split(' - ')[0].toLowerCase())) {
                    criteriaToCheck = col;
                    break;
                }
            }
        }
        if (!criteriaToCheck) return null;
        const isAcceptable = brandFit[criteriaToCheck] === true || brandFit[criteriaToCheck] === 'Yes' || brandFit[criteriaToCheck] === 'Acceptable';
        return isAcceptable ? 100 : 22;
    }

    // PROJ3: Project stage fit (weight: 5) – Brand Setup Acceptable Project Stages
    async calculatePROJ3(dealFields, locationData, brandData) {
        const stageRaw = (dealFields['Stage of Development'] || locationData?.['Stage of Development'] || dealFields['Project Stage'] || '').toString().trim();
        const brandFit = brandData.brandFit || {};
        const stageColumns = [
            'Land Under Control Only - Acceptable Project Stages',
            'Entitlements in Process - Acceptable Project Stages',
            'Fully Entitled - Acceptable Project Stages',
            'Under Construction - Acceptable Project Stages',
            'Stabilized Operating Asset - Acceptable Project Stages'
        ];
        let dealStageColumn = null;
        const stageLower = stageRaw.toLowerCase();
        if (stageLower.includes('land') || stageLower.includes('control')) dealStageColumn = stageColumns[0];
        else if (stageLower.includes('entitlement')) dealStageColumn = stageColumns[1];
        else if (stageLower.includes('entitled')) dealStageColumn = stageColumns[2];
        else if (stageLower.includes('construction')) dealStageColumn = stageColumns[3];
        else if (stageLower.includes('stabilized') || stageLower.includes('operating')) dealStageColumn = stageColumns[4];
        if (!dealStageColumn) return null;
        const acceptable = brandFit[dealStageColumn] === true || brandFit[dealStageColumn] === 'Yes' || brandFit[dealStageColumn] === 'Acceptable';
        return acceptable ? 100 : 28;
    }

    // AGMT1: Agreement type / deal structure fit (weight: 8) – graduated for "Both" and strict mismatch
    async calculateAGMT1(dealFields, locationData, brandData) {
        const dealStructureRaw = (dealFields['Preferred Deal Structure'] || dealFields['Who should receive bids for this project?'] || locationData?.['Preferred Deal Structure'] || '').toString().trim();
        const brandFit = brandData.brandFit || {};
        if (!dealStructureRaw) return null;
        const franchiseCol = 'Franchise Only - Acceptable Agreements Type';
        const thirdPartyCol = 'Third-Party Management Only - Acceptable Agreements Type';
        const bothCol = 'Brand + Third-Party - Acceptable Agreements Type';
        const brandFranchise = brandFit[franchiseCol] === true || brandFit[franchiseCol] === 'Yes' || brandFit[franchiseCol] === 'Acceptable';
        const brandThirdParty = brandFit[thirdPartyCol] === true || brandFit[thirdPartyCol] === 'Yes' || brandFit[thirdPartyCol] === 'Acceptable';
        const brandBoth = brandFit[bothCol] === true || brandFit[bothCol] === 'Yes' || brandFit[bothCol] === 'Acceptable';
        const dsLower = dealStructureRaw.toLowerCase();
        if (dsLower.includes('both') || dsLower.includes('brands and third')) {
            if (brandBoth || (brandFranchise && brandThirdParty)) return 100;
            if (brandFranchise || brandThirdParty) return 62;
            return 22;
        }
        const agreementColumns = {
            'franchise only': franchiseCol, 'franchise': franchiseCol,
            '3rd party only': thirdPartyCol, 'third-party': thirdPartyCol, 'management only': thirdPartyCol,
            'brand-managed': 'Brand-Managed - Acceptable Agreements Type', 'brand managed': 'Brand-Managed - Acceptable Agreements Type',
            'flexible': 'Flexible/Open - Acceptable Agreements Type', 'open': 'Flexible/Open - Acceptable Agreements Type',
            'lease': 'Lease - Acceptable Agreements Type', 'joint venture': 'Joint Venture - Acceptable Agreements Type'
        };
        let criteriaToCheck = null;
        for (const [key, col] of Object.entries(agreementColumns)) {
            if (dsLower.includes(key)) {
                criteriaToCheck = col;
                break;
            }
        }
        if (!criteriaToCheck) return null;
        const acceptable = brandFit[criteriaToCheck] === true || brandFit[criteriaToCheck] === 'Yes' || brandFit[criteriaToCheck] === 'Acceptable';
        return acceptable ? 100 : 18;
    }

    // ESG1: Sustainability & ESG fit (weight: 5) – Project Fit ESG expectations, Brand Standards Sustainability Features
    async calculateESG1(dealFields, locationData, brandData) {
        const dealEsgRaw = (dealFields['Sustainability'] || dealFields['ESG'] || locationData?.['Sustainability'] || locationData?.['ESG Commitment'] || '').toString().trim();
        const brandFit = brandData.brandFit || {};
        const brandStandards = brandData.brandStandards || {};
        const brandEsgExpectation = brandFit['ESG / Sustainability Expectations You Prefer Projects to Meet - Risk & Compliance'] || '';
        const brandSustainabilityFeatures = brandStandards['Sustainability Features'];
        const brandHasEsg = !!(brandEsgExpectation && (brandEsgExpectation + '').trim()) || (Array.isArray(brandSustainabilityFeatures) && brandSustainabilityFeatures.length > 0) || (typeof brandSustainabilityFeatures === 'string' && brandSustainabilityFeatures.trim());
        if (!dealEsgRaw && !brandHasEsg) return 100;
        if (!dealEsgRaw) return null;
        if (!brandHasEsg) return 72;
        const dealEsgLower = dealEsgRaw.toLowerCase();
        const expectationStr = (brandEsgExpectation + '').toLowerCase();
        if (expectationStr && (dealEsgLower.includes('yes') || dealEsgLower.includes('commitment') || dealEsgLower.includes('sustainable') || dealEsgLower.includes('esg'))) return 100;
        return 60;
    }

    renderDeals() {
        const tbody = document.getElementById('dealsTableBody');
        if (!tbody) return;
        
        if (this.filteredDeals.length === 0) {
            const hasRequests = (this.brandDealRequests.new?.length || this.brandDealRequests.accepted?.length || this.brandDealRequests.declined?.length || this.brandDealRequests.archived?.length || 0) > 0;
            const msg = hasRequests
                ? '<h3>No deals found</h3><p>Try adjusting your filters.</p>'
                : '<h3>No contacted projects yet</h3><p>Projects will appear here once they have been contacted (offers sent) in Airtable.</p>';
            tbody.innerHTML = '<tr><td colspan="12" class="empty-state" style="padding: 60px 20px;">' + msg + '</td></tr>';
            this.updateTabCounts();
            return;
        }
        
        tbody.innerHTML = this.filteredDeals.map(deal => {
            const displayScore = deal._requestMatchScore != null ? Number(deal._requestMatchScore) : (deal.matchScore != null && deal.matchScore !== '' ? Number(deal.matchScore) : null);
            const scoreClass = this.getScoreClass(displayScore);
            const location = `${deal.city}, ${deal.country}`;
            
            // Render preferred/contacted brand (Contact Brand from BDR when in all-contacted mode, else owner preference)
            let preferredBrandHtml = '<div class="preferred-brand-cell">';
            const contactBrand = deal._contactedBrand || deal.preferredBrandName;
            if (contactBrand) {
                const displayBrand = deal._contactedBrand 
                    ? contactBrand 
                    : (deal.preferredBrands?.length > 1 ? `${deal.preferredBrandName} +${deal.preferredBrands.length - 1}` : deal.preferredBrandName);
                const hasBetterAlternatives = !deal._contactedBrand && deal.hasBetterAlternatives ? '<span class="better-match-indicator" title="Better matches available">⚠️</span>' : '';
                
                preferredBrandHtml += `
                    <div class="preferred-brand-name">${this.escapeHtml(displayBrand)}${hasBetterAlternatives}</div>
                `;
            } else {
                preferredBrandHtml += '<div class="no-preferred-brand">—</div>';
            }
            preferredBrandHtml += '</div>';
            
            const reqId = deal._requestId || '';
            return `
                <tr>
                    <td class="cell-checkbox"><input type="checkbox" class="deal-row-checkbox" data-request-id="${this.escapeHtml(reqId)}" title="Select row"></td>
                    <td>${this.renderStatusCell(deal._requestStatus)}</td>
                    <td><span class="property-name">${this.escapeHtml(deal.propertyName)}</span></td>
                    <td>${this.escapeHtml(location)}</td>
                    <td>${this.escapeHtml(deal.chainScale || '—')}</td>
                    <td>${this.escapeHtml(deal.projectType || '—')}</td>
                    <td>${this.formatTargetOpeningDate(deal.targetOpeningDate)}</td>
                    <td>${this.escapeHtml(deal.propertyType)}</td>
                    <td>${deal.rooms || 'N/A'}</td>
                    <td>${preferredBrandHtml}</td>
                    <td class="match-score-cell">
                        <span class="match-score-badge ${scoreClass}">${displayScore != null ? displayScore.toFixed(1) : '—'}</span>
                        <button class="match-score-details-btn" onclick="dashboard.showScoreDetails('${deal.id}')">
                            View Details
                        </button>
                    </td>
                    <td class="cell-call-to-action">
                        ${this.renderCallToActionButtons(deal)}
                    </td>
                </tr>
            `;
        }).join('');
        this.updateTabCounts();
        this.setupBulkActions();
        this.updateBulkActionsState();
    }

    setupBulkActions() {
        const tbody = document.getElementById('dealsTableBody');
        const selectAll = document.getElementById('bddSelectAllCheckbox');
        const bulkBtn = document.getElementById('bddBulkActionsBtn');
        const dropdown = document.getElementById('bddBulkDropdown');
        if (!tbody || !selectAll || !bulkBtn || !dropdown) return;
        if (this._bddBulkWired) return;
        this._bddBulkWired = true;
        selectAll.addEventListener('change', () => {
            const checked = selectAll.checked;
            tbody.querySelectorAll('.deal-row-checkbox').forEach(cb => { cb.checked = checked; });
            this.updateBulkActionsState();
        });
        tbody.addEventListener('change', (e) => {
            if (e.target.classList.contains('deal-row-checkbox')) this.updateBulkActionsState();
        });
        bulkBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            if (bulkBtn.disabled) return;
            dropdown.classList.toggle('open');
        });
        dropdown.querySelectorAll('.bdd-bulk-dropdown-item').forEach(item => {
            item.addEventListener('click', (e) => {
                e.stopPropagation();
                const status = item.getAttribute('data-status');
                if (!status) return;
                this.applyBulkStatus(status);
                dropdown.classList.remove('open');
            });
        });
        document.addEventListener('click', (e) => {
            if (e.target.closest('#bddBulkActionsBtn') || e.target.closest('#bddBulkDropdown')) return;
            dropdown.classList.remove('open');
        });
        dropdown.addEventListener('click', e => e.stopPropagation());
    }

    updateBulkActionsState() {
        const tbody = document.getElementById('dealsTableBody');
        const selectAll = document.getElementById('bddSelectAllCheckbox');
        const bulkBtn = document.getElementById('bddBulkActionsBtn');
        if (!tbody || !bulkBtn) return;
        const checked = tbody.querySelectorAll('.deal-row-checkbox:checked');
        bulkBtn.disabled = checked.length === 0;
        if (selectAll) selectAll.checked = checked.length > 0 && checked.length === tbody.querySelectorAll('.deal-row-checkbox').length;
    }

    async applyBulkStatus(status) {
        const tbody = document.getElementById('dealsTableBody');
        if (!tbody) return;
        const checked = tbody.querySelectorAll('.deal-row-checkbox:checked');
        if (checked.length === 0) return;
        const updates = [];
        checked.forEach(cb => {
            const reqId = cb.getAttribute('data-request-id');
            if (reqId) updates.push({ requestId: reqId, status });
        });
        if (updates.length === 0) return;
        try {
            const res = await fetch(window.location.origin + '/api/brand-deal-requests/bulk-update', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ updates })
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) throw new Error(data.error || 'Update failed');
            if (typeof this.showToast === 'function') {
                this.showToast('Updated ' + updates.length + ' row(s)', true);
            } else {
                this.showBddToast('Updated ' + updates.length + ' row(s)', true);
            }
            await this.fetchAllContactedDeals();
            await this.recalculateMatchScores();
            this.updateArchivedDisplay();
            this.updateActiveDealsDisplay();
            document.getElementById('bddBulkDropdown')?.classList.remove('open');
            this.renderDeals();
        } catch (err) {
            if (typeof this.showToast === 'function') {
                this.showToast('Bulk update failed: ' + (err.message || 'Unknown error'), false);
            } else {
                this.showBddToast('Bulk update failed: ' + (err.message || 'Unknown error'), false);
            }
        }
    }

    updateTabCounts() {
        var newCount = document.getElementById('tabCountNewDeals');
        var activeCount = document.getElementById('tabCountActiveDeals');
        var archivedCount = document.getElementById('tabCountArchived');
        var resultsEl = document.getElementById('newDealsResultsCount');
        var n = (this.filteredDeals && this.filteredDeals.length) || 0;
        if (newCount) newCount.textContent = n;
        if (activeCount) activeCount.textContent = (this.activeDeals && this.activeDeals.length) || 0;
        if (archivedCount) archivedCount.textContent = (this.archivedDeals && this.archivedDeals.length) || 0;
        if (resultsEl) resultsEl.textContent = n === 0 ? 'No deals match your filters.' : 'Showing ' + n + ' deal' + (n === 1 ? '' : 's') + '.';
    }

    generateDealHeadline(dealFields, locationData, contactData) {
        // Focus on unique information not shown in other columns
        // Avoid repeating: room count, location (city/country)
        // Focus on: project type, chain scale, brand preference, development stage, timeline, special characteristics
        
        const chainScale = locationData?.['Hotel Chain Scale'] || '';
        const projectType = dealFields['Project Type'] || '';
        const stage = dealFields['Stage of Development'] || '';
        const openingDate = dealFields['Expected Opening or Rebranding Date'] || dealFields['Expected Opening Date'] || '';
        const brandPreference = dealFields['Preferred Brands'] || dealFields['Brand Preference'] || dealFields['Desired Brand'] || '';
        const currentBrand = dealFields['Current Brand Affiliation'] || '';
        const serviceModel = locationData?.['Hotel Service Model'] || '';
        const buildingType = locationData?.['Building Type'] || '';
        
        let headlineParts = [];
        
        // Project type and chain scale (if independent, mention it)
        if (projectType && projectType !== 'Unknown') {
            if (chainScale && chainScale !== 'Not Specified' && chainScale !== 'Unknown') {
                headlineParts.push(`${chainScale.toLowerCase()} ${projectType.toLowerCase()}`);
            } else {
                headlineParts.push(`independent ${projectType.toLowerCase()}`);
            }
        } else if (chainScale && chainScale !== 'Not Specified' && chainScale !== 'Unknown') {
            headlineParts.push(`${chainScale.toLowerCase()} property`);
        } else {
            headlineParts.push('independent property');
        }
        
        // Development stage (if relevant)
        if (stage && stage !== 'Unknown' && stage !== 'Operating') {
            const stageMap = {
                'Concept': 'concept stage',
                'Site Acquired': 'site acquired',
                'Under Construction': 'under construction',
                'Pre-Development': 'pre-development'
            };
            const stageText = stageMap[stage] || stage.toLowerCase();
            headlineParts.push(`at ${stageText}`);
        }
        
        // Brand preference or current brand
        if (brandPreference && brandPreference !== 'Not specified') {
            const brands = Array.isArray(brandPreference) ? brandPreference.join(', ') : brandPreference;
            headlineParts.push(`seeking ${brands}`);
        } else if (currentBrand && currentBrand !== 'Not specified') {
            headlineParts.push(`currently ${currentBrand}`);
        }
        
        // Service model (if adds value)
        if (serviceModel && serviceModel !== 'Not Specified' && serviceModel !== 'Unknown') {
            headlineParts.push(`${serviceModel.toLowerCase()} model`);
        }
        
        // Building type (if unique)
        if (buildingType && buildingType !== 'Unknown' && buildingType !== 'Not Specified') {
            headlineParts.push(`${buildingType.toLowerCase()} format`);
        }
        
        // Timeline (opening date)
        if (openingDate) {
            try {
                const date = new Date(openingDate);
                if (!isNaN(date.getTime())) {
                    const month = date.toLocaleString('default', { month: 'long' });
                    const year = date.getFullYear();
                    headlineParts.push(`opening ${month} ${year}`);
                } else {
                    headlineParts.push(`opening ${openingDate}`);
                }
            } catch (e) {
                headlineParts.push(`opening ${openingDate}`);
            }
        }
        
        // Combine parts
        let headline = headlineParts.join(', ');
        
        // Capitalize first letter and add period
        if (headline) {
            headline = headline.charAt(0).toUpperCase() + headline.slice(1);
            if (!headline.endsWith('.') && !headline.endsWith(')')) {
                headline += '.';
            }
        }
        
        return headline || 'Property details pending.';
    }

    renderStatusCell(status) {
        const s = status || '—';
        if (s === 'Brand Viewed') {
            return `<span class="status-text-plain">${this.escapeHtml(s)}</span>`;
        }
        const slug = String(s).toLowerCase().replace(/\s+/g, '-');
        return `<span class="bdd-status-badge bdd-status-${slug}">${this.escapeHtml(s)}</span>`;
    }

    renderCallToActionButtons(deal, opts) {
        opts = opts || {};
        const showDelete = !!opts.showDelete;
        const showReactivate = !!opts.showReactivate;
        const showSubmitProposal = !!opts.showSubmitProposal;
        const email = (deal.contactData && (deal.contactData['Email Address'] || deal.contactData['Email'])) || '';
        const mailto = email ? 'mailto:' + encodeURIComponent(email.trim()) : '';
        const emailTitle = email ? 'Communications' : 'No email on file';
        const dealId = deal.id || '';
        const requestId = deal._requestId || '';
        const brandName = (deal._contactedBrand || deal.preferredBrandName || '').replace(/"/g, '&quot;');
        const score = deal._requestMatchScore != null ? Number(deal._requestMatchScore) : (deal.matchScore ?? null);
        const showAcceptDecline = (deal._requestStatus === 'New' || deal._requestStatus === 'Brand Viewed') && requestId;
        const acceptDecline = showAcceptDecline ? `
            <button type="button" class="action-icon" title="Accept" onclick="dashboard.acceptRequest('${requestId}')"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg></button>
            <button type="button" class="action-icon" title="Decline" onclick="dashboard.declineRequest('${requestId}')"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button>
        ` : '';
        const deleteBtn = showDelete && requestId ? `<button type="button" class="action-icon" title="Delete" data-action="delete" data-request-id="${this.escapeHtml(requestId)}" data-deal-id="${this.escapeHtml(dealId)}" data-brand="${this.escapeHtml(brandName)}" data-score="${score != null && score !== '' ? this.escapeHtml(String(score)) : ''}"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6h18"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"/><path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/><line x1="10" y1="11" x2="10" y2="17"/><line x1="14" y1="11" x2="14" y2="17"/></svg></button>` : '';
        const reactivateBtn = showReactivate && requestId ? `<button type="button" class="action-icon" title="Reactivate" data-action="reactivate" data-request-id="${this.escapeHtml(requestId)}" data-deal-id="${this.escapeHtml(dealId)}"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M1 4v6h6"/><path d="M3.51 15a9 9 0 1 0 2.13-9.36L1 10"/></svg></button>` : '';
        const submitProposalBtn = showSubmitProposal
            ? (requestId
                ? `<button type="button" class="action-icon action-icon--proposal" title="Submit Proposal" aria-label="Submit Proposal" data-action="submit-proposal" data-request-id="${this.escapeHtml(requestId)}"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><polyline points="10 9 9 9 8 9"/></svg></button>`
                : `<button type="button" class="action-icon action-icon--proposal" title="Submit Proposal unavailable: request record is missing for this deal-brand pair." aria-label="Submit Proposal unavailable: request record is missing for this deal-brand pair." disabled><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/><polyline points="10 9 9 9 8 9"/></svg></button>`)
            : '';
        const emailLink = email
            ? `<button type="button" class="action-icon" title="${emailTitle}" data-action="communications" data-deal-id="${dealId}" data-brand="${this.escapeHtml(brandName)}" data-email="${this.escapeHtml(email.trim())}"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><path d="M22 6l-10 7L2 6"/></svg></button>`
            : `<span class="action-icon" style="opacity:0.5;cursor:not-allowed;" title="${emailTitle}"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><path d="M22 6l-10 7L2 6"/></svg></span>`;
        return `<div class="action-icons">` +
            emailLink +
            `<button type="button" class="action-icon" title="Schedule follow-up" data-action="schedule" data-deal-id="${dealId}" data-request-id="${requestId}" data-brand="${this.escapeHtml(brandName)}"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg></button>` +
            submitProposalBtn +
            `<button type="button" class="action-icon" title="View" onclick="dashboard.handleViewDeal('${dealId}', '${requestId}', '${deal._requestStatus || ''}')"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg></button>` +
            reactivateBtn +
            deleteBtn +
            acceptDecline +
            `</div>`;
    }

    renderDealContact(contactData) {
        if (!contactData || Object.keys(contactData).length === 0) {
            return '<div class="deal-contact"><div class="contact-avatar-placeholder">?</div><div class="contact-info"><div class="contact-name">Unknown</div></div></div>';
        }
        
        const name = contactData['Main Contact Name'] || 
                    contactData['Contact Name'] || 
                    contactData['A Main Contact Name'] ||
                    'Unknown';
        const company = contactData['Entity or Company Name'] || 
                       contactData['Company Name'] || 
                       contactData['A Entity or Company Name'] ||
                       '';
        
        // Debug: Log what we're working with
        console.log(`🖼️ Rendering contact for: ${name}`, {
            hasProfile: !!contactData['Profile'],
            profileType: typeof contactData['Profile'],
            isArray: Array.isArray(contactData['Profile']),
            profileValue: contactData['Profile']
        });
        
        // Get profile image - check multiple possible field names and formats
        let profileImage = null;
        if (contactData['Profile']) {
            if (Array.isArray(contactData['Profile'])) {
                profileImage = contactData['Profile'][0]?.url || contactData['Profile'][0];
                console.log(`📷 Found Profile array for ${name}, extracted:`, profileImage);
            } else if (typeof contactData['Profile'] === 'object' && contactData['Profile'].url) {
                profileImage = contactData['Profile'].url;
                console.log(`📷 Found Profile object for ${name}, extracted:`, profileImage);
            } else if (typeof contactData['Profile'] === 'string') {
                profileImage = contactData['Profile'];
                console.log(`📷 Found Profile string for ${name}:`, profileImage);
            }
        }
        
        // Try alternative field names if Profile not found
        if (!profileImage) {
            const altFields = ['Profile Image', 'Photo', 'Avatar', 'Profile Picture', 'Image'];
            for (const field of altFields) {
                if (contactData[field]) {
                    if (Array.isArray(contactData[field])) {
                        profileImage = contactData[field][0]?.url || contactData[field][0];
                    } else if (typeof contactData[field] === 'object' && contactData[field].url) {
                        profileImage = contactData[field].url;
                    } else if (typeof contactData[field] === 'string') {
                        profileImage = contactData[field];
                    }
                    if (profileImage) {
                        console.log(`📷 Found image in alternative field "${field}" for ${name}:`, profileImage);
                        break;
                    }
                }
            }
        }
        
        // Get initials for placeholder
        const initials = name !== 'Unknown' ? name.split(' ').map(n => n[0]).join('').toUpperCase().substring(0, 2) : '?';
        
        let avatarHtml = '';
        if (profileImage && profileImage.trim() !== '' && profileImage !== 'null' && profileImage !== 'undefined') {
            // Use actual profile image
            console.log(`✅ Using profile image for ${name}:`, profileImage.substring(0, 100) + '...');
            // Create image with error handling - show placeholder if image fails to load
            avatarHtml = `<div class="contact-avatar-wrapper">
                <img src="${this.escapeHtml(profileImage)}" alt="${this.escapeHtml(name)}" class="contact-avatar" onerror="this.style.display='none'; this.parentElement.querySelector('.contact-avatar-placeholder').style.display='flex';" onload="this.parentElement.querySelector('.contact-avatar-placeholder').style.display='none';">
                <div class="contact-avatar-placeholder" style="display:none;">${initials}</div>
            </div>`;
        } else {
            // Use placeholder
            console.log(`⚠️ No profile image found for ${name}, using placeholder with initials: ${initials}`);
            avatarHtml = `<div class="contact-avatar-placeholder">${initials}</div>`;
        }
        
        return `
            <div class="deal-contact">
                ${avatarHtml}
                <div class="contact-info">
                    <div class="contact-name">${this.escapeHtml(name)}</div>
                    ${company ? `<div class="contact-company">${this.escapeHtml(company)}</div>` : ''}
                </div>
            </div>
        `;
    }

    getScoreClass(score) {
        const n = Number(score);
        if (n >= 80) return 'match-score-high';
        if (n >= 50) return 'match-score-medium';
        return 'match-score-low';
    }

    getBreakdownScoreClass(score) {
        if (score == null || score === '') return 'medium';
        const n = Number(score);
        if (n >= 80) return 'high';
        if (n >= 50) return 'medium';
        if (n >= 25) return 'low';
        return 'poor';
    }

    buildMatchScoreNewSummary(score, brandKey, details) {
        const brand = brandKey ? String(brandKey).trim() : null;
        const brandPhrase = brand ? brand + "'s" : "this brand's";
        const yourBrand = brand ? " with " + brand : "";
        const strong = [];
        const weak = [];
        if (details && typeof details === 'object') {
            for (const k of Object.keys(details)) {
                const d = details[k];
                const s = (d && d.score != null && d.score !== '—') ? Number(d.score) : null;
                const lbl = (d && d.label) ? d.label : k;
                if (s == null || isNaN(s)) continue;
                if (s >= 80) strong.push(lbl);
                else if (s < 50) weak.push(lbl);
            }
        }
        const strongStr = strong.length > 0 ? strong.slice(0, 3).join(', ') : null;
        const weakStr = weak.length > 0 ? weak.slice(0, 3).join(', ') : null;
        if (score >= 80) {
            let p = "With a score of " + score.toFixed(0) + ", your project aligns strongly with " + brandPhrase + " requirements across most factors. ";
            if (strongStr) p += "Your strongest areas are " + strongStr + "—these signal that your property, deal structure, and preferences line up well with what " + (brand || 'this brand') + " typically seeks. ";
            p += "This is a promising match worth advancing: reach out with confidence and highlight how your project fits their criteria.";
            return p;
        }
        if (score >= 50) {
            let p = "At " + score.toFixed(0) + ", you have a moderate match" + yourBrand + ". ";
            if (strongStr) p += "Your strengths—" + strongStr + "—show where you already align. ";
            if (weakStr) p += "The main areas to address or negotiate are " + weakStr + ". ";
            p += "It may be worth pursuing if you can adjust on those points or if the brand is flexible; otherwise, consider how much compromise makes sense for your situation.";
            return p;
        }
        if (score >= 25) {
            let p = "A score of " + score.toFixed(0) + " indicates notable gaps between your project and " + brandPhrase + " expectations. ";
            if (weakStr) p += "The biggest misalignments are in " + weakStr + ". ";
            if (strongStr) p += "You do have some positives (" + strongStr + "), but the weak areas may be structural or harder to change. ";
            p += "Before investing more time, consider whether those gaps can be realistically bridged—or whether a different brand might be a better fit for your project as it stands.";
            return p;
        }
        let p = "With a score of " + score.toFixed(0) + ", this is a poor match and your project does not align well with " + brandPhrase + " typical requirements. ";
        if (weakStr) p += "Major gaps include " + weakStr + "—these often reflect fundamental differences in property type, deal structure, or brand standards. ";
        p += "We recommend focusing on other brands in your preferred list where your project is a stronger fit, unless you are able to substantially change your project or approach.";
        return p;
    }

    async showScoreDetails(dealId) {
        const deal = this.deals.find(d => d.id === dealId);
        if (!deal) return;

        const modal = document.getElementById('scoreDetailsModal');
        const content = document.getElementById('scoreDetailsContent');
        if (!modal || !content) return;

        const brand = (document.getElementById('brandFilter')?.value || this.brandId || deal.preferredBrandName || '').trim();
        if (!brand) {
            content.innerHTML = '<div class="modal-section"><p style="color: var(--neutral--400);">No brand selected. Use the brand filter or ensure the deal has a preferred brand to see the Match Score Breakdown.</p></div>';
            modal.classList.add('active');
            return;
        }

        content.innerHTML = '<div class="modal-section"><p style="color: var(--neutral--400);">Loading breakdown…</p></div>';
        modal.classList.add('active');

        try {
            const apiBase = (typeof window !== 'undefined' && window.location?.origin) ? window.location.origin : '';
            const res = await fetch(apiBase + '/api/my-deals/' + encodeURIComponent(dealId) + '/match-score-breakdown?brand=' + encodeURIComponent(brand));
            const data = await res.json().catch(() => ({}));

            if (!res.ok) {
                content.innerHTML = '<div class="modal-section"><p style="color: var(--neutral--400);">' + this.escapeHtml(data.error || 'Could not load breakdown.') + '</p></div>';
                return;
            }

            const scoreNew = data.scoreNew != null ? data.scoreNew : null;
            const details = data.breakdownNewDetails && typeof data.breakdownNewDetails === 'object' ? data.breakdownNewDetails : {};

            const scoreDisplay = scoreNew != null ? Number(scoreNew).toFixed(1) : '—';
            const subTitle = ' for <span style="color: var(--accent--primary-1);">' + this.escapeHtml(brand) + '</span>';
            const colorNote = ' Score bands: <span style="color: var(--system--green-400);">80–100 = strong</span>, <span style="color: var(--system--orange-400);">50–79 = moderate</span>, <span style="color: var(--system--red-400);">25–49 = weak</span>, <span style="color: #6B2D2D;">0–24 = poor</span>.';

            let html = '<div class="modal-section">' +
                '<h3>Overall Match Score: <span style="color: var(--accent--primary-1);">' + scoreDisplay + '/100</span>' + subTitle + '</h3>' +
                '<p style="color: var(--neutral--400); font-size: 14px; line-height: 1.5; margin: 10px 0 0 0;">This score compares what each brand is looking for with what your project offers. It weighs 12 factors across three areas: how well the project fits (size, type, amenities), whether the deal structure works (franchise vs management, fees, key money), and your preferences (brands you like, service level, incentives).' + colorNote + ' The breakdown below shows how each factor scored and why.</p></div>';

            if (details && Object.keys(details).length > 0) {
                html += '<div class="modal-section"><h3>Quantitative Breakdown</h3><div class="match-score-breakdown">';
                for (const factorKey of Object.keys(details)) {
                    const d = details[factorKey];
                    const label = (d && d.label) ? d.label : factorKey;
                    const weight = (d && d.weight != null) ? d.weight : 0;
                    const sc = (d && d.score != null && d.score !== '—') ? this.getBreakdownScoreClass(Number(d.score)) : 'low';
                    const scorePct = (d && d.score != null && d.score !== '—') ? Math.min(100, Math.max(0, Number(d.score))) : 0;
                    html += '<div class="score-category"><div class="score-category-label">' +
                        '<div class="score-factor-heading">' + this.escapeHtml(label) + '</div>' +
                        (weight ? '<div class="score-factor-weight">(Weight: ' + weight + '%)</div>' : '') +
                        '</div>' +
                        '<div class="score-category-value"><div class="score-bar"><div class="score-bar-fill ' + sc + '" style="width: ' + scorePct + '%"></div></div>' +
                        '<span class="score-number">' + (d.score != null && d.score !== '—' ? d.score : '—') + '</span></div>';
                    if (d.brandValue || d.dealValue || d.note) {
                        html += '<div class="score-factor-details">' +
                            '<div><strong style="color: var(--neutral--300);">Brand setup:</strong> ' + this.escapeHtml(d.brandValue || '—') + '</div>' +
                            '<div style="margin-top: 4px;"><strong style="color: var(--neutral--300);">Deal setup:</strong> ' + this.escapeHtml(d.dealValue || '—') + '</div>' +
                            (d.note ? '<div style="margin-top: 4px;"><strong style="color: var(--neutral--300);">How match works:</strong> ' + this.escapeHtml(d.note) + '</div>' : '') +
                            '</div>';
                    }
                    html += '</div>';
                }
                html += '</div></div>';
            } else {
                const emptyMsg = 'Breakdown details are not available. This usually means the brand "' + this.escapeHtml(brand) + '" was not found in Brand Setup - Brand Basics.';
                html += '<div class="modal-section"><h3>Quantitative Breakdown</h3><p style="color: var(--neutral--400);">' + emptyMsg + '</p></div>';
            }

            const summary = scoreNew != null ? this.buildMatchScoreNewSummary(Number(scoreNew), brand, details) : null;
            if (summary) {
                html += '<div class="modal-section"><h3>What This Score Means For You</h3><p class="match-score-summary" style="color: var(--neutral--300); font-size: 15px; line-height: 1.6; margin: 0;">' + this.escapeHtml(summary) + '</p></div>';
            }

            content.innerHTML = html;
        } catch (err) {
            console.error('Error loading match score breakdown:', err);
            content.innerHTML = '<div class="modal-section"><p style="color: var(--neutral--400);">An error occurred while loading the breakdown. Please try again.</p></div>';
        }
    }

    generateQualitativeInsights(deal) {
        const insights = [];
        const breakdown = deal.scoreBreakdown;
        
        // High scores
        if (breakdown.SEG1 >= 80) {
            insights.push({
                type: 'positive',
                title: 'Strong Segment Alignment',
                description: 'The property type and chain scale align well with brand positioning.'
            });
        }
        
        if (breakdown.SIZE1 >= 80) {
            insights.push({
                type: 'positive',
                title: 'Ideal Room Count',
                description: `The property size (${deal.rooms} rooms) fits within the brand's preferred range.`
            });
        }
        
        if (breakdown.MKT1 >= 80) {
            insights.push({
                type: 'positive',
                title: 'Priority Market Match',
                description: `The location (${deal.city}, ${deal.country}) is in a priority market for this brand.`
            });
        }
        
        // Low scores
        if (breakdown.MKT1 < 50) {
            insights.push({
                type: 'negative',
                title: 'Market Mismatch',
                description: 'The location may not be a priority market for this brand, or the market may be saturated.'
            });
        }
        
        if (breakdown.SEG1 < 50) {
            insights.push({
                type: 'negative',
                title: 'Segment Misalignment',
                description: 'The property type or chain scale may not align with the brand positioning.'
            });
        }
        
        if (breakdown.FIN1 < 50) {
            insights.push({
                type: 'negative',
                title: 'Financial Considerations',
                description: 'There may be concerns about fee structure or financial terms alignment.'
            });
        }
        if (breakdown.AGMT1 != null && breakdown.AGMT1 < 50) {
            insights.push({
                type: 'negative',
                title: 'Deal Structure Mismatch',
                description: 'The preferred deal structure (e.g. franchise vs management) may not align with what this brand accepts.'
            });
        }
        if (breakdown.PROJ3 != null && breakdown.PROJ3 < 50) {
            insights.push({
                type: 'negative',
                title: 'Project Stage Mismatch',
                description: 'The project stage may not be one the brand typically considers.'
            });
        }
        if (breakdown.ESG1 != null && breakdown.ESG1 >= 80) {
            insights.push({
                type: 'positive',
                title: 'Sustainability & ESG Alignment',
                description: 'The deal aligns well with the brand\'s sustainability and ESG expectations.'
            });
        }
        if (breakdown.ESG1 != null && breakdown.ESG1 < 50) {
            insights.push({
                type: 'neutral',
                title: 'Sustainability & ESG',
                description: 'Consider confirming the brand\'s ESG expectations and the project\'s sustainability commitment.'
            });
        }
        if (breakdown.KEY1 != null && breakdown.KEY1 >= 80) {
            insights.push({
                type: 'positive',
                title: 'Key Money Alignment',
                description: 'The deal\'s key money / TI expectations align with this brand\'s willingness to offer key money or upfront incentives.'
            });
        }
        if (breakdown.KEY1 != null && breakdown.KEY1 < 50) {
            insights.push({
                type: 'negative',
                title: 'Key Money Mismatch',
                description: 'The deal is seeking key money or TI support, but this brand does not typically offer it. Consider other brands or revisiting deal expectations.'
            });
        }
        if (breakdown.CAP1 != null && breakdown.CAP1 >= 80) {
            insights.push({
                type: 'positive',
                title: 'Capital Readiness Match',
                description: 'The deal\'s funding/capital status aligns with what this brand accepts at engagement.'
            });
        }
        if (breakdown.CAP1 != null && breakdown.CAP1 < 50) {
            insights.push({
                type: 'negative',
                title: 'Capital Status Mismatch',
                description: 'The deal\'s capital/funding stage may not align with when this brand typically engages. Consider clarifying funding status.'
            });
        }
        if (breakdown.TERM1 != null && breakdown.TERM1 >= 80) {
            insights.push({
                type: 'positive',
                title: 'Deal Terms Alignment',
                description: 'Initial term, performance test, or conversion timeline expectations align well with this brand\'s typical deal terms.'
            });
        }
        if (breakdown.TERM1 != null && breakdown.TERM1 < 50) {
            insights.push({
                type: 'negative',
                title: 'Deal Terms Mismatch',
                description: 'Term length, performance test, or conversion timeline may not align with this brand\'s typical requirements.'
            });
        }
        // Neutral/Moderate scores
        if (breakdown.SVC1 >= 50 && breakdown.SVC1 < 80) {
            insights.push({
                type: 'neutral',
                title: 'Service Model Review Needed',
                description: 'The service model alignment is moderate - may require further discussion.'
            });
        }
        
        // Overall assessment
        if (deal.matchScore >= 80) {
            insights.push({
                type: 'positive',
                title: 'Overall Assessment',
                description: 'This is a high-quality match with strong alignment across multiple dimensions. Consider prioritizing this deal.'
            });
        } else if (deal.matchScore < 50) {
            insights.push({
                type: 'negative',
                title: 'Overall Assessment',
                description: 'This deal has significant misalignments. Consider whether the brand can accommodate the specific requirements or if this deal should be deprioritized.'
            });
        } else {
            insights.push({
                type: 'neutral',
                title: 'Overall Assessment',
                description: 'This is a moderate match. Worth exploring further, but may require compromises or negotiations on specific terms.'
            });
        }
        
        return insights;
    }

    showDealDetails(dealId) {
        const deal = this.deals.find(d => d.id === dealId);
        if (!deal) return;
        
        const modal = document.getElementById('dealDetailsModal');
        const content = document.getElementById('dealDetailsContent');
        
        const contact = deal.contactData || {};
        const location = deal.locationData || {};
        
        let html = `
            <div class="modal-section">
                <h3>Property Information</h3>
                <div class="info-grid">
                    <div class="info-item">
                        <label>Project Name</label>
                        <value>${this.escapeHtml(deal.propertyName)}</value>
                    </div>
                    <div class="info-item">
                        <label>Deal ID</label>
                        <value>${this.escapeHtml(deal.dealId)}</value>
                    </div>
                    <div class="info-item">
                        <label>Location</label>
                        <value>${this.escapeHtml(`${deal.city}, ${deal.country}`)}</value>
                    </div>
                    <div class="info-item">
                        <label>Property Type</label>
                        <value>${this.escapeHtml(deal.propertyType)}</value>
                    </div>
                    <div class="info-item">
                        <label>Number of Rooms</label>
                        <value>${deal.rooms || 'N/A'}</value>
                    </div>
                    <div class="info-item">
                        <label>Stage of Development</label>
                        <value>${this.escapeHtml(deal.stage)}</value>
                    </div>
                    <div class="info-item">
                        <label>Project Type</label>
                        <value>${this.escapeHtml(deal.projectType)}</value>
                    </div>
                    <div class="info-item">
                        <label>Brand Match</label>
                        <value>${this.escapeHtml(deal.brandMatch)}</value>
                    </div>
                </div>
            </div>
            
            <div class="modal-section">
                <h3>Contact Information</h3>
                <div class="info-grid">
                    <div class="info-item">
                        <label>Contact Name</label>
                        <value>${this.escapeHtml(contact['Main Contact Name'] || contact['Contact Name'] || 'N/A')}</value>
                    </div>
                    <div class="info-item">
                        <label>Title</label>
                        <value>${this.escapeHtml(contact['Main Contact Title'] || contact['Contact Title'] || 'N/A')}</value>
                    </div>
                    <div class="info-item">
                        <label>Company</label>
                        <value>${this.escapeHtml(contact['Entity or Company Name'] || contact['Company Name'] || 'N/A')}</value>
                    </div>
                    <div class="info-item">
                        <label>Email</label>
                        <value>${this.escapeHtml(contact['Email'] || contact['Email Address'] || contact['Main Contact Email'] || 'N/A')}</value>
                    </div>
                    <div class="info-item">
                        <label>Phone</label>
                        <value>${this.escapeHtml(contact['Phone'] || contact['Phone Number'] || 'N/A')}</value>
                    </div>
                </div>
            </div>
            
            <div class="modal-section">
                <h3>Location Details</h3>
                <div class="info-grid">
                    <div class="info-item">
                        <label>Country</label>
                        <value>${this.escapeHtml(location.Country || 'N/A')}</value>
                    </div>
                    <div class="info-item">
                        <label>City</label>
                        <value>${this.escapeHtml(location.City || 'N/A')}</value>
                    </div>
                    <div class="info-item">
                        <label>Hotel Type</label>
                        <value>${this.escapeHtml(location['Hotel Type'] || 'N/A')}</value>
                    </div>
                    <div class="info-item">
                        <label>Chain Scale</label>
                        <value>${this.escapeHtml(location['Hotel Chain Scale'] || 'N/A')}</value>
                    </div>
                    <div class="info-item">
                        <label>Service Model</label>
                        <value>${this.escapeHtml(location['Hotel Service Model'] || 'N/A')}</value>
                    </div>
                    <div class="info-item">
                        <label>Building Type</label>
                        <value>${this.escapeHtml(location['Building Type'] || 'N/A')}</value>
                    </div>
                </div>
            </div>
        `;
        
        // Add additional deal information if available
        if (deal.dealFields) {
            const df = deal.dealFields;
            html += `
                <div class="modal-section">
                    <h3>Additional Deal Information</h3>
                    <div class="info-grid">
            `;
            
            if (df['Expected Opening or Rebranding Date']) {
                html += `
                    <div class="info-item">
                        <label>Expected Opening Date</label>
                        <value>${this.escapeHtml(df['Expected Opening or Rebranding Date'])}</value>
                    </div>
                `;
            }
            
            if (df['Description']) {
                html += `
                    <div class="info-item" style="grid-column: 1 / -1;">
                        <label>Description</label>
                        <value>${this.escapeHtml(df['Description'])}</value>
                    </div>
                `;
            }
            
            html += `
                    </div>
                </div>
            `;
        }
        
        content.innerHTML = html;
        modal.classList.add('active');
    }

    setupEventListeners() {
        // Filter event listeners
        const brandFilterEl = document.getElementById('brandFilter');
        if (brandFilterEl) {
            brandFilterEl.addEventListener('change', async (e) => {
                const selectedBrand = e.target.value || null;
                console.log('Brand filter changed to:', selectedBrand);
                this.brandId = selectedBrand;
                
                // Show loading state with Clause Library style animation
                const tbody = document.getElementById('dealsTableBody');
                if (tbody && this.deals.length > 0) {
                    tbody.innerHTML = `
                        <tr>
                            <td colspan="12" style="padding: 60px 20px; text-align: center;">
                                <div class="loading">
                                    <div class="loading-content">
                                        <div class="wave-container">
                                            <div class="wave wave-1"></div>
                                            <div class="wave wave-2"></div>
                                            <div class="wave wave-3"></div>
                                            <div class="wave-particles">
                                                <div class="particle"></div>
                                                <div class="particle"></div>
                                                <div class="particle"></div>
                                                <div class="particle"></div>
                                            </div>
                                        </div>
                                        <div class="loading-text">
                                            <div class="loading-text-main">Recalculating match scores...</div>
                                            <div class="loading-text-time">${selectedBrand || 'selected brand'}</div>
                                        </div>
                                    </div>
                                    <div class="loading-progress">
                                        <div class="loading-progress-bar"></div>
                                    </div>
                                </div>
                            </td>
                        </tr>
                    `;
                }
                
                // Load brand deal requests (all contacted) or brand-specific, then recalculate
                if (this.brandId) {
                    await this.fetchBrandDealRequests();
                } else {
                    await this.fetchAllContactedDeals();
                }
                await this.recalculateMatchScores();
                this.updateArchivedDisplay();
                this.updateActiveDealsDisplay();
            });
        }
        document.getElementById('statusFilter')?.addEventListener('change', () => this.applyFilters());
        document.getElementById('scoreFilter')?.addEventListener('change', () => this.applyFilters());
        document.getElementById('propertyTypeFilter')?.addEventListener('change', () => this.applyFilters());
        document.getElementById('countryFilter')?.addEventListener('change', () => this.applyFilters());
        document.getElementById('activeBrandFilter')?.addEventListener('change', () => this.updateActiveDealsDisplay());
        document.getElementById('activeStatusFilter')?.addEventListener('change', () => this.updateActiveDealsDisplay());
        document.getElementById('activeScoreFilter')?.addEventListener('change', () => this.updateActiveDealsDisplay());
        document.getElementById('activePropertyTypeFilter')?.addEventListener('change', () => this.updateActiveDealsDisplay());
        document.getElementById('activeCountryFilter')?.addEventListener('change', () => this.updateActiveDealsDisplay());
        document.getElementById('archivedBrandFilter')?.addEventListener('change', () => this.updateArchivedDisplay());
        document.getElementById('archivedStatusFilter')?.addEventListener('change', () => this.updateArchivedDisplay());
        document.getElementById('archivedScoreFilter')?.addEventListener('change', () => this.updateArchivedDisplay());
        document.getElementById('archivedPropertyTypeFilter')?.addEventListener('change', () => this.updateArchivedDisplay());
        document.getElementById('archivedCountryFilter')?.addEventListener('change', () => this.updateArchivedDisplay());
        document.getElementById('archivedClearFiltersBtn')?.addEventListener('click', () => this.clearArchivedDealsFilters());
        
        // Tab navigation (New Deals, Active Deals, Archived, Deal Log)
        var self = this;
        document.querySelectorAll('.bdd-section-nav .section-nav-item[data-tab]').forEach(function(btn) {
            btn.addEventListener('click', function() { self.switchTab(btn.getAttribute('data-tab')); });
        });
        window.addEventListener('hashchange', function() { self.restoreTabFromHash(); });
        
        // Close modals when clicking outside
        document.getElementById('scoreDetailsModal')?.addEventListener('click', (e) => {
            if (e.target.id === 'scoreDetailsModal') {
                closeScoreDetailsModal();
            }
        });
        
        document.getElementById('dealDetailsModal')?.addEventListener('click', (e) => {
            if (e.target.id === 'dealDetailsModal') {
                closeDealDetailsModal();
            }
        });

        document.getElementById('bddScheduleModal')?.addEventListener('click', (e) => {
            if (e.target.id === 'bddScheduleModal') {
                this.closeBddScheduleModal();
            }
        });
        document.getElementById('bddSubmitProposalModal')?.addEventListener('click', (e) => {
            if (e.target.id === 'bddSubmitProposalModal') {
                this.closeSubmitProposalModal();
            }
        });
        document.getElementById('bddScheduleModalClose')?.addEventListener('click', () => this.closeBddScheduleModal());
        document.getElementById('bddScheduleCancelBtn')?.addEventListener('click', () => this.closeBddScheduleModal());
        document.getElementById('bddScheduleSaveBtn')?.addEventListener('click', () => this.saveBddSchedule());
        document.getElementById('bddSubmitProposalModalClose')?.addEventListener('click', () => this.closeSubmitProposalModal());

        // Add sort handlers to table headers - use event delegation since headers are re-rendered
        document.addEventListener('click', (e) => {
            const header = e.target.closest('.deals-table th[data-sort]');
            if (header) {
                const sortColumn = header.dataset.sort;
                if (header.closest('#activeDealsTable')) {
                    this.handleActiveDealsSort(sortColumn);
                } else {
                    this.handleSort(sortColumn);
                }
            }
            const scheduleBtn = e.target.closest('.action-icon[data-action="schedule"]');
            if (scheduleBtn) {
                e.preventDefault();
                const dealId = scheduleBtn.dataset.dealId;
                const requestId = scheduleBtn.dataset.requestId;
                const brand = scheduleBtn.dataset.brand || '';
                if (dealId && requestId) this.scheduleFollowUp(dealId, requestId, brand);
                return;
            }
            const commBtn = e.target.closest('.action-icon[data-action="communications"]');
            if (commBtn) {
                e.preventDefault();
                const email = (commBtn.dataset.email || '').trim();
                if (email) {
                    const mailto = 'mailto:' + encodeURIComponent(email);
                    window.location.href = mailto;
                }
                return;
            }
            const submitProposalBtn = e.target.closest('.action-icon[data-action="submit-proposal"]');
            if (submitProposalBtn) {
                e.preventDefault();
                const requestId = submitProposalBtn.dataset.requestId;
                if (requestId) this.openSubmitProposalModal(requestId);
                return;
            }
            const deleteBtn = e.target.closest('.action-icon[data-action="delete"]');
            if (deleteBtn) {
                e.preventDefault();
                const requestId = deleteBtn.dataset.requestId;
                const dealId = deleteBtn.dataset.dealId;
                const brand = deleteBtn.dataset.brand || '';
                const score = deleteBtn.dataset.score != null && deleteBtn.dataset.score !== '' ? parseFloat(deleteBtn.dataset.score) : null;
                if (requestId) this.showBddDeleteReasonModal(requestId, dealId, brand, score);
                return;
            }
            const reactivateBtn = e.target.closest('.action-icon[data-action="reactivate"]');
            if (reactivateBtn) {
                e.preventDefault();
                const requestId = reactivateBtn.dataset.requestId;
                if (requestId) this.reactivateRequest(requestId);
                return;
            }
        });

        this.setupBddDeleteReasonModal();
    }

    switchTab(tabId) {
        var validTabs = ['new-deals', 'active-deals', 'archived', 'deal-log'];
        if (!tabId || validTabs.indexOf(tabId) === -1) tabId = 'new-deals';
        document.querySelectorAll('.bdd-section-nav .section-nav-item').forEach(function(t) { t.classList.remove('active'); });
        document.querySelectorAll('.bdd-tab-panel').forEach(function(p) { p.classList.remove('active'); });
        var btn = document.querySelector('.bdd-section-nav .section-nav-item[data-tab="' + tabId + '"]');
        var panelId = 'section' + tabId.split('-').map(function(s) { return s.charAt(0).toUpperCase() + s.slice(1); }).join('');
        var panel = document.getElementById(panelId);
        if (btn) btn.classList.add('active');
        if (panel) panel.classList.add('active');
        if (this.dealsLoading) {
            this.showLoadingInTab(tabId);
        } else {
            if (tabId === 'archived') this.updateArchivedDisplay();
            if (tabId === 'active-deals') this.updateActiveDealsDisplay();
            if (tabId === 'deal-log') this.renderDealLog();
        }
        try { window.history.replaceState(null, '', window.location.pathname + window.location.search + '#' + tabId); } catch (_) {}
    }

    restoreTabFromHash() {
        var hash = (window.location.hash || '').replace(/^#/, '');
        if (hash && ['new-deals', 'active-deals', 'archived', 'deal-log'].indexOf(hash) !== -1) {
            this.switchTab(hash);
        }
    }

    async handleViewDeal(dealId, requestId, requestStatus) {
        const base = window.location.origin || '';
        const url = base + '/deal-summary.html?id=' + encodeURIComponent(dealId) + '&from=bdd';
        const validRequestId = requestId && String(requestId).trim().startsWith('rec');
        const shouldUpdateStatus = (requestStatus === 'New' || requestStatus === 'new') && validRequestId;
        if (shouldUpdateStatus) {
            try {
                const res = await fetch(`${base}/api/brand-deal-requests/${encodeURIComponent(requestId)}`, {
                    method: 'PATCH',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ status: 'Brand Viewed' })
                });
                const data = await res.json().catch(() => ({}));
                if (!res.ok) {
                    const msg = data.error || res.statusText || 'Could not update status to Viewed.';
                    this.showBddToast('Status update failed: ' + msg, false);
                } else {
                    this._applyLocalRequestPatch(requestId, { status: 'Brand Viewed' });
                    this._applyLocalMutationEffects();
                    this._scheduleBackgroundRefresh();
                }
            } catch (err) {
                console.error('handleViewDeal PATCH error:', err);
                this.showBddToast('Status update failed: ' + (err.message || 'Network error'), false);
            }
        }
        window.location.href = url;
    }

    async acceptRequest(requestId) {
        if (!requestId) return;
        try {
            const base = window.location.origin || '';
            const res = await fetch(`${base}/api/brand-deal-requests/${encodeURIComponent(requestId)}`, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ status: 'Accepted' })
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) {
                this.showBddToast(data.error || 'Could not accept request', false);
                return;
            }
            this._applyLocalRequestPatch(requestId, { status: 'Accepted' });
            this._applyLocalMutationEffects();
            this._scheduleBackgroundRefresh();
            this.showBddToast('Deal accepted and moved to Active Deals', true);
        } catch (err) {
            this.showBddToast('Error: ' + (err.message || 'Network error'), false);
        }
    }

    async reactivateRequest(requestId) {
        if (!requestId) return;
        try {
            const base = window.location.origin || '';
            const res = await fetch(base + '/api/brand-deal-requests/' + encodeURIComponent(requestId), {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ status: 'Accepted' })
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) {
                this.showBddToast(data.error || 'Could not reactivate', false);
                return;
            }
            this.showBddToast('Deal reactivated and moved to Active Deals', true);
            this._applyLocalRequestPatch(requestId, { status: 'Accepted' });
            this._applyLocalMutationEffects();
            this._scheduleBackgroundRefresh();
        } catch (err) {
            this.showBddToast('Error reactivating: ' + (err.message || 'Network error'), false);
        }
    }

    async declineRequest(requestId) {
        if (!requestId) return;
        const notes = window.prompt('Optional: Add a note for why you declined');
        if (notes === null) return;
        try {
            const base = window.location.origin || '';
            const res = await fetch(`${base}/api/brand-deal-requests/${encodeURIComponent(requestId)}`, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ status: 'Declined', responseNotes: notes || undefined })
            });
            const data = await res.json().catch(() => ({}));
            if (!res.ok) {
                this.showBddToast(data.error || 'Could not decline request', false);
                return;
            }
            this._applyLocalRequestPatch(requestId, { status: 'Declined', responseNotes: notes || undefined });
            this._applyLocalMutationEffects();
            this._scheduleBackgroundRefresh();
            this.showBddToast('Deal declined and moved to Archived', true);
        } catch (err) {
            this.showBddToast('Error: ' + (err.message || 'Network error'), false);
        }
    }

    scheduleFollowUp(dealId, requestId, brand) {
        const modal = document.getElementById('bddScheduleModal');
        const label = document.getElementById('bddScheduleModalLabel');
        const headerInput = document.getElementById('bddScheduleHeaderInput');
        const dateInput = document.getElementById('bddScheduleDateInput');
        const notesInput = document.getElementById('bddScheduleNotesInput');
        if (!modal || !label || !dateInput) return;
        const shortId = dealId ? String(dealId).slice(-6) : '';
        label.textContent = 'Schedule a follow-up for ' + (brand || 'this brand') + (shortId ? ' (deal ' + shortId + ').' : '.');
        modal.dataset.pendingRequestId = requestId || '';
        const allReqs = [
            ...(this.brandDealRequests.new || []),
            ...(this.brandDealRequests.viewed || []),
            ...(this.brandDealRequests.accepted || []),
            ...(this.brandDealRequests.declined || []),
            ...(this.brandDealRequests.archived || [])
        ];
        const pair = allReqs.find(r => r.id === requestId);
        if (headerInput) headerInput.value = (pair && pair.nextFollowupHeader) ? String(pair.nextFollowupHeader).trim() : '';
        dateInput.value = (pair && pair.nextFollowupDate) ? String(pair.nextFollowupDate).slice(0, 10) : '';
        if (notesInput) notesInput.value = (pair && pair.nextFollowupNotes) ? String(pair.nextFollowupNotes).trim() : '';
        modal.classList.add('active');
    }

    closeBddScheduleModal() {
        const modal = document.getElementById('bddScheduleModal');
        if (modal) modal.classList.remove('active');
    }

    openSubmitProposalModal(requestId) {
        if (!requestId) return;
        const modal = document.getElementById('bddSubmitProposalModal');
        const iframe = document.getElementById('bddSubmitProposalIframe');
        if (!modal || !iframe) return;
        const base = window.location.origin || '';
        iframe.src = base + '/brand-deal-request.html?requestId=' + encodeURIComponent(requestId) + '&embed=1';
        modal.classList.add('active');
    }

    closeSubmitProposalModal() {
        const modal = document.getElementById('bddSubmitProposalModal');
        const iframe = document.getElementById('bddSubmitProposalIframe');
        if (modal) modal.classList.remove('active');
        if (iframe) iframe.src = 'about:blank';
    }

    async saveBddSchedule() {
        const modal = document.getElementById('bddScheduleModal');
        const dateInput = document.getElementById('bddScheduleDateInput');
        const headerInput = document.getElementById('bddScheduleHeaderInput');
        const notesInput = document.getElementById('bddScheduleNotesInput');
        const requestId = modal && modal.dataset.pendingRequestId;
        const dateVal = dateInput && dateInput.value;
        const headerVal = headerInput ? headerInput.value.trim() : '';
        const notesVal = notesInput ? notesInput.value.trim() : '';
        if (!requestId || !dateVal) {
            this.closeBddScheduleModal();
            return;
        }
        try {
            const res = await fetch(window.location.origin + '/api/brand-deal-requests/' + encodeURIComponent(requestId), {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ nextFollowupDate: dateVal, nextFollowupHeader: headerVal, nextFollowupNotes: notesVal })
            });
            const data = await res.json().catch(() => ({}));
            if (res.ok) {
                this.closeBddScheduleModal();
                this._applyLocalRequestPatch(requestId, {
                    nextFollowupDate: dateVal,
                    nextFollowupHeader: headerVal,
                    nextFollowupNotes: notesVal
                });
                this._applyLocalMutationEffects();
                this._scheduleBackgroundRefresh();
                this.showBddToast('Follow-up date saved', true);
            } else {
                this.showBddToast(data.error || 'Could not save follow-up date', false);
            }
        } catch (err) {
            this.showBddToast('Could not save follow-up date', false);
        }
    }

    setupBddDeleteReasonModal() {
        var modal = document.getElementById('bddDeleteReasonModal');
        var closeBtn = document.getElementById('bddDeleteReasonModalClose');
        var cancelBtn = document.getElementById('bddDeleteReasonCancelBtn');
        var confirmBtn = document.getElementById('bddDeleteReasonConfirmBtn');
        var selectEl = document.getElementById('bddDeleteReasonSelect');
        var otherWrap = document.getElementById('bddDeleteReasonOtherWrap');
        var otherInput = document.getElementById('bddDeleteReasonOtherInput');
        if (!modal || !selectEl || !otherWrap || !otherInput || !confirmBtn) return;
        if (this._bddDeleteReasonModalWired) return;
        this._bddDeleteReasonModalWired = true;
        var self = this;
        function closeModal() {
            modal.classList.remove('active');
        }
        function updateConfirmEnabled() {
            var reason = selectEl.value || '';
            var hasReason = reason && reason !== 'Other' ? true : (reason === 'Other' && otherInput.value.trim().length > 0);
            confirmBtn.disabled = !hasReason;
            confirmBtn.style.opacity = hasReason ? '1' : '0.7';
        }
        if (closeBtn) closeBtn.addEventListener('click', closeModal);
        if (cancelBtn) cancelBtn.addEventListener('click', closeModal);
        modal.addEventListener('click', function(e) {
            if (e.target === modal) closeModal();
        });
        selectEl.addEventListener('change', function() {
            otherWrap.style.display = selectEl.value === 'Other' ? 'block' : 'none';
            updateConfirmEnabled();
        });
        otherInput.addEventListener('input', updateConfirmEnabled);
        confirmBtn.addEventListener('click', function() {
            var reason = selectEl.value || '';
            if (reason === 'Other') reason = otherInput.value.trim() || '';
            var requestId = modal.dataset.pendingRequestId || '';
            if (!requestId) { closeModal(); return; }
            closeModal();
            self.confirmBddDelete(requestId, reason);
        });
    }

    showBddDeleteReasonModal(requestId, dealId, brandName, matchScore) {
        var modal = document.getElementById('bddDeleteReasonModal');
        var label = document.getElementById('bddDeleteReasonModalBrandLabel');
        var selectEl = document.getElementById('bddDeleteReasonSelect');
        var otherWrap = document.getElementById('bddDeleteReasonOtherWrap');
        var otherInput = document.getElementById('bddDeleteReasonOtherInput');
        var confirmBtn = document.getElementById('bddDeleteReasonConfirmBtn');
        if (!modal || !selectEl || !otherWrap || !otherInput || !confirmBtn) return;
        if (label) label.textContent = 'Why are you excluding ' + (brandName ? brandName : 'this brand') + ' from consideration?';
        selectEl.value = '';
        otherWrap.style.display = 'none';
        otherInput.value = '';
        confirmBtn.disabled = true;
        confirmBtn.style.opacity = '0.7';
        modal.dataset.pendingRequestId = requestId || '';
        modal.dataset.pendingDealId = dealId || '';
        modal.dataset.pendingBrandName = brandName || '';
        modal.dataset.pendingMatchScore = matchScore != null ? String(matchScore) : '';
        modal.classList.add('active');
    }

    async confirmBddDelete(requestId, notes) {
        if (!requestId || !notes) return;
        try {
            var res = await fetch(window.location.origin + '/api/brand-deal-requests/' + encodeURIComponent(requestId), {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ status: 'Archived', responseNotes: notes })
            });
            var data = await res.json().catch(function() { return {}; });
            if (res.ok) {
                this.showBddToast('Deal archived', true);
                this._applyLocalRequestPatch(requestId, { status: 'Archived', responseNotes: notes });
                this._applyLocalMutationEffects();
                this._scheduleBackgroundRefresh();
            } else {
                this.showBddToast(data.error || 'Could not archive', false);
            }
        } catch (err) {
            this.showBddToast('Could not archive: ' + (err.message || 'Unknown error'), false);
        }
    }

    updateActiveDealsDisplay() {
        this.applyActiveDealsFilters();
        var tableWrap = document.getElementById('activeDealsTableWrap');
        var activeTable = document.getElementById('activeDealsTable');
        var emptyState = document.getElementById('activeDealsEmptyState');
        var emptyMsg = document.getElementById('activeDealsEmptyMessage');
        var resultsEl = document.getElementById('activeDealsResultsCount');
        if (!emptyState) return;
        var hasActive = (this.filteredActiveDeals && this.filteredActiveDeals.length > 0);
        var hasAnyActive = (this.activeDeals && this.activeDeals.length > 0);
        if (tableWrap) tableWrap.style.display = hasActive ? '' : 'none';
        if (activeTable) activeTable.style.display = hasActive ? '' : 'none';
        emptyState.style.display = hasActive ? 'none' : 'block';
        if (emptyMsg) emptyMsg.textContent = hasAnyActive && !hasActive ? 'No deals match your filters. Use Clear Filters to see all active deals.' : 'Deals you\'ve accepted and are actively working on will appear here. Accept a request from New Deals to move it here.';
        if (resultsEl) resultsEl.textContent = hasActive ? 'Showing ' + this.filteredActiveDeals.length + ' deal' + (this.filteredActiveDeals.length === 1 ? '' : 's') + '.' : 'No deals match your filters.';
        if (hasActive) this.renderActiveDeals();
        this.updateTabCounts();
    }

    applyActiveDealsFilters() {
        var source = this.activeDeals || [];
        var brandFilter = document.getElementById('activeBrandFilter')?.value || '';
        var statusFilter = document.getElementById('activeStatusFilter')?.value || '';
        var scoreFilter = document.getElementById('activeScoreFilter')?.value || '';
        var propertyTypeFilter = document.getElementById('activePropertyTypeFilter')?.value || '';
        var countryFilter = document.getElementById('activeCountryFilter')?.value || '';
        this.filteredActiveDeals = source.filter(function(deal) {
            if (brandFilter) {
                var dealBrand = (deal._contactedBrand || deal.preferredBrandName || '').trim();
                if (dealBrand !== brandFilter) return false;
            }
            if (statusFilter) {
                var dealStatus = (deal.status || '').trim();
                if (dealStatus !== statusFilter) return false;
            }
            if (scoreFilter) {
                var score = deal._requestMatchScore != null ? Number(deal._requestMatchScore) : (deal.matchScore ?? 0);
                if (scoreFilter === 'high' && score < 80) return false;
                if (scoreFilter === 'medium' && (score < 50 || score >= 80)) return false;
                if (scoreFilter === 'low' && score >= 50) return false;
            }
            if (propertyTypeFilter && deal.propertyType !== propertyTypeFilter) return false;
            if (countryFilter && deal.country !== countryFilter) return false;
            return true;
        });
    }

    clearActiveDealsFilters() {
        var brand = document.getElementById('activeBrandFilter');
        var status = document.getElementById('activeStatusFilter');
        var score = document.getElementById('activeScoreFilter');
        var propertyType = document.getElementById('activePropertyTypeFilter');
        var country = document.getElementById('activeCountryFilter');
        if (brand) brand.value = '';
        if (status) status.value = '';
        if (score) score.value = '';
        if (propertyType) propertyType.value = '';
        if (country) country.value = '';
        this.updateActiveDealsDisplay();
    }

    updateArchivedDisplay() {
        var tableWrap = document.getElementById('archivedTableWrap');
        var archivedTable = tableWrap ? tableWrap.querySelector('table') : null;
        var emptyState = document.getElementById('archivedEmptyState');
        var resultsEl = document.getElementById('archivedDealsResultsCount');
        if (!tableWrap || !emptyState) return;
        this.applyArchivedDealsFilters();
        var hasArchived = (this.filteredArchivedDeals && this.filteredArchivedDeals.length > 0);
        var hasAnyArchived = (this.archivedDeals && this.archivedDeals.length > 0);
        tableWrap.style.display = hasArchived ? '' : 'none';
        if (archivedTable) archivedTable.style.display = hasArchived ? '' : 'none';
        emptyState.style.display = hasArchived ? 'none' : 'block';
        if (resultsEl) {
            resultsEl.textContent = hasArchived
                ? 'Showing ' + this.filteredArchivedDeals.length + ' deal' + (this.filteredArchivedDeals.length === 1 ? '' : 's') + '.'
                : (hasAnyArchived ? 'No deals match your filters.' : 'No archived deals.');
        }
        if (hasArchived) this.renderArchivedDeals();
        this.updateTabCounts();
    }

    applyArchivedDealsFilters() {
        var source = this.archivedDeals || [];
        var brandFilter = document.getElementById('archivedBrandFilter')?.value || '';
        var statusFilter = document.getElementById('archivedStatusFilter')?.value || '';
        var scoreFilter = document.getElementById('archivedScoreFilter')?.value || '';
        var propertyTypeFilter = document.getElementById('archivedPropertyTypeFilter')?.value || '';
        var countryFilter = document.getElementById('archivedCountryFilter')?.value || '';
        this.filteredArchivedDeals = source.filter(function(deal) {
            if (brandFilter) {
                var dealBrand = (deal._contactedBrand || deal.preferredBrandName || '').trim();
                if (dealBrand !== brandFilter) return false;
            }
            if (statusFilter) {
                var dealStatus = (deal.status || '').trim();
                if (dealStatus !== statusFilter) return false;
            }
            if (scoreFilter) {
                var score = deal._requestMatchScore != null ? Number(deal._requestMatchScore) : (deal.matchScore ?? 0);
                if (scoreFilter === 'high' && score < 80) return false;
                if (scoreFilter === 'medium' && (score < 50 || score >= 80)) return false;
                if (scoreFilter === 'low' && score >= 50) return false;
            }
            if (propertyTypeFilter && deal.propertyType !== propertyTypeFilter) return false;
            if (countryFilter && deal.country !== countryFilter) return false;
            return true;
        });
    }

    clearArchivedDealsFilters() {
        var brand = document.getElementById('archivedBrandFilter');
        var status = document.getElementById('archivedStatusFilter');
        var score = document.getElementById('archivedScoreFilter');
        var propertyType = document.getElementById('archivedPropertyTypeFilter');
        var country = document.getElementById('archivedCountryFilter');
        if (brand) brand.value = '';
        if (status) status.value = '';
        if (score) score.value = '';
        if (propertyType) propertyType.value = '';
        if (country) country.value = '';
        this.updateArchivedDisplay();
    }

    renderActiveDeals() {
        var tbody = document.getElementById('activeDealsTableBody');
        if (!tbody) return;
        var deals = this.filteredActiveDeals || this.activeDeals || [];
        tbody.innerHTML = deals.map(function(deal) {
            var score = deal._requestMatchScore != null ? Number(deal._requestMatchScore) : (deal.matchScore ?? null);
            var scoreClass = this.getScoreClass(score);
            var location = (deal.city || '') + ', ' + (deal.country || '');
            var status = deal.status || 'In progress';
            var brandDisplay = deal._contactedBrand || deal.preferredBrandName || '—';
            var reqId = deal._requestId || '';
            return '<tr>' +
                '<td class="cell-checkbox"><input type="checkbox" class="deal-row-checkbox active-deals-checkbox" data-request-id="' + this.escapeHtml(reqId) + '" title="Select row"></td>' +
                '<td>' + this.escapeHtml(status) + '</td>' +
                '<td><span class="property-name">' + this.escapeHtml(deal.propertyName || '') + '</span></td>' +
                '<td>' + this.escapeHtml(location) + '</td>' +
                '<td>' + this.escapeHtml(deal.chainScale || '—') + '</td>' +
                '<td>' + this.escapeHtml(deal.projectType || '—') + '</td>' +
                '<td>' + this.formatTargetOpeningDate(deal.targetOpeningDate) + '</td>' +
                '<td>' + this.escapeHtml(deal.propertyType || '') + '</td>' +
                '<td>' + (deal.rooms || 'N/A') + '</td>' +
                '<td><div class="preferred-brand-cell"><div class="preferred-brand-name">' + this.escapeHtml(brandDisplay) + '</div></div></td>' +
                '<td class="match-score-cell"><span class="match-score-badge ' + scoreClass + '">' + (score != null && score !== '' ? Number(score).toFixed(1) : '—') + '</span></td>' +
                '<td class="cell-call-to-action">' + this.renderCallToActionButtons(deal, { showDelete: true, showSubmitProposal: true }) + '</td>' +
                '</tr>';
        }.bind(this)).join('');
        this.setupActiveDealsBulkActions();
        this.updateActiveDealsBulkActionsState();
    }

    setupActiveDealsBulkActions() {
        var tbody = document.getElementById('activeDealsTableBody');
        var selectAll = document.getElementById('activeDealsSelectAllCheckbox');
        var bulkBtn = document.getElementById('activeDealsBulkActionsBtn');
        var dropdown = document.getElementById('activeDealsBulkDropdown');
        if (!tbody || !selectAll || !bulkBtn || !dropdown) return;
        if (this._activeDealsBulkWired) return;
        this._activeDealsBulkWired = true;
        selectAll.addEventListener('change', function() {
            var checked = selectAll.checked;
            tbody.querySelectorAll('.active-deals-checkbox').forEach(function(cb) { cb.checked = checked; });
            if (window.dashboard) window.dashboard.updateActiveDealsBulkActionsState();
        });
        tbody.addEventListener('change', function(e) {
            if (e.target.classList.contains('active-deals-checkbox') && window.dashboard) window.dashboard.updateActiveDealsBulkActionsState();
        });
        bulkBtn.addEventListener('click', function(e) {
            e.stopPropagation();
            if (bulkBtn.disabled) return;
            dropdown.classList.toggle('open');
        });
        dropdown.querySelectorAll('.bdd-bulk-dropdown-item').forEach(function(item) {
            item.addEventListener('click', function(e) {
                e.stopPropagation();
                var status = item.getAttribute('data-status');
                if (status && window.dashboard) window.dashboard.applyActiveDealsBulkStatus(status);
                dropdown.classList.remove('open');
            });
        });
        document.addEventListener('click', function(e) {
            if (e.target.closest('#activeDealsBulkActionsBtn') || e.target.closest('#activeDealsBulkDropdown')) return;
            dropdown.classList.remove('open');
        });
        dropdown.addEventListener('click', function(e) { e.stopPropagation(); });
    }

    updateActiveDealsBulkActionsState() {
        var tbody = document.getElementById('activeDealsTableBody');
        var selectAll = document.getElementById('activeDealsSelectAllCheckbox');
        var bulkBtn = document.getElementById('activeDealsBulkActionsBtn');
        if (!tbody || !bulkBtn) return;
        var checkboxes = tbody.querySelectorAll('.active-deals-checkbox');
        var checked = tbody.querySelectorAll('.active-deals-checkbox:checked');
        bulkBtn.disabled = checked.length === 0;
        if (selectAll) selectAll.checked = checkboxes.length > 0 && checked.length === checkboxes.length;
    }

    async applyActiveDealsBulkStatus(status) {
        var tbody = document.getElementById('activeDealsTableBody');
        if (!tbody) return;
        var checked = tbody.querySelectorAll('.active-deals-checkbox:checked');
        if (checked.length === 0) return;
        var updates = [];
        checked.forEach(function(cb) {
            var reqId = cb.getAttribute('data-request-id');
            if (reqId) updates.push({ requestId: reqId, status: status });
        });
        if (updates.length === 0) return;
        try {
            var res = await fetch(window.location.origin + '/api/brand-deal-requests/bulk-update', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ updates: updates })
            });
            var data = await res.json().catch(function() { return {}; });
            if (!res.ok) throw new Error(data.error || 'Update failed');
            this.showBddToast('Updated ' + updates.length + ' row(s)', true);
            updates.forEach(function(u) {
                this._applyLocalRequestPatch(u.requestId, { status: u.status });
            }.bind(this));
            this._applyLocalMutationEffects();
            this._scheduleBackgroundRefresh();
            document.getElementById('activeDealsBulkDropdown')?.classList.remove('open');
        } catch (err) {
            this.showBddToast('Bulk update failed: ' + (err.message || 'Unknown error'), false);
        }
    }

    handleActiveDealsSort(column) {
        if (this.activeSortColumn === column) {
            this.activeSortDirection = this.activeSortDirection === 'asc' ? 'desc' : 'asc';
        } else {
            this.activeSortColumn = column;
            this.activeSortDirection = 'asc';
        }
        document.querySelectorAll('#activeDealsTable th[data-sort]').forEach(function(th) {
            th.classList.remove('sort-asc', 'sort-desc');
            if (th.dataset.sort === column) {
                th.classList.add(this.activeSortDirection === 'asc' ? 'sort-asc' : 'sort-desc');
            }
        }.bind(this));
        this.sortActiveDeals();
        this.renderActiveDeals();
    }

    sortActiveDeals() {
        if (!this.activeSortColumn) return;
        var arr = this.filteredActiveDeals || this.activeDeals || [];
        if (arr.length === 0) return;
        var col = this.activeSortColumn;
        var dir = this.activeSortDirection;
        arr.sort(function(a, b) {
            var aVal, bVal;
            switch (col) {
                case 'propertyName': aVal = (a.propertyName || '').toLowerCase(); bVal = (b.propertyName || '').toLowerCase(); break;
                case 'location': aVal = ((a.city || '') + ', ' + (a.country || '')).toLowerCase(); bVal = ((b.city || '') + ', ' + (b.country || '')).toLowerCase(); break;
                case 'chainScale': aVal = (a.chainScale || '').toLowerCase(); bVal = (b.chainScale || '').toLowerCase(); break;
                case 'projectType': aVal = (a.projectType || '').toLowerCase(); bVal = (b.projectType || '').toLowerCase(); break;
                case 'targetOpeningDate': aVal = (a.targetOpeningDate || '').toLowerCase(); bVal = (b.targetOpeningDate || '').toLowerCase(); break;
                case 'propertyType': aVal = (a.propertyType || '').toLowerCase(); bVal = (b.propertyType || '').toLowerCase(); break;
                case 'rooms': aVal = parseInt(a.rooms) || 0; bVal = parseInt(b.rooms) || 0; break;
                case 'preferredBrand': aVal = (a._contactedBrand || a.preferredBrandName || '').toLowerCase(); bVal = (b._contactedBrand || b.preferredBrandName || '').toLowerCase(); break;
                case 'matchScore': aVal = a._requestMatchScore != null ? Number(a._requestMatchScore) : (a.matchScore ?? 0); bVal = b._requestMatchScore != null ? Number(b._requestMatchScore) : (b.matchScore ?? 0); break;
                case 'status': aVal = (a.status || '').toLowerCase(); bVal = (b.status || '').toLowerCase(); break;
                default: return 0;
            }
            if (col === 'rooms' || col === 'matchScore') {
                return dir === 'asc' ? aVal - bVal : bVal - aVal;
            }
            if (aVal < bVal) return dir === 'asc' ? -1 : 1;
            if (aVal > bVal) return dir === 'asc' ? 1 : -1;
            return 0;
        });
    }

    renderArchivedDeals() {
        var tbody = document.getElementById('archivedDealsTableBody');
        if (!tbody) return;
        var deals = this.filteredArchivedDeals || this.archivedDeals || [];
        tbody.innerHTML = deals.map(function(deal) {
            var scoreClass = this.getScoreClass(deal.matchScore);
            var location = (deal.city || '') + ', ' + (deal.country || '');
            var archivedReason = deal.archivedReason || deal.status || '—';
            var reqId = deal._requestId || '';
            return '<tr>' +
                '<td class="cell-checkbox"><input type="checkbox" class="deal-row-checkbox archived-deals-checkbox" data-request-id="' + this.escapeHtml(reqId) + '" title="Select row"></td>' +
                '<td><span class="property-name">' + this.escapeHtml(deal.propertyName || '') + '</span></td>' +
                '<td>' + this.escapeHtml(location) + '</td>' +
                '<td>' + this.escapeHtml(deal.chainScale || '—') + '</td>' +
                '<td>' + this.escapeHtml(deal.projectType || '—') + '</td>' +
                '<td>' + this.formatTargetOpeningDate(deal.targetOpeningDate) + '</td>' +
                '<td>' + this.escapeHtml(deal.propertyType || '') + '</td>' +
                '<td>' + (deal.rooms || 'N/A') + '</td>' +
                '<td><div class="preferred-brand-cell"><div class="preferred-brand-name">' + this.escapeHtml(deal.preferredBrandName || '—') + '</div></div></td>' +
                '<td class="match-score-cell"><span class="match-score-badge ' + scoreClass + '">' + (deal.matchScore != null && deal.matchScore !== '' ? Number(deal.matchScore).toFixed(1) : '—') + '</span></td>' +
                '<td>' + this.escapeHtml(archivedReason) + '</td>' +
                '<td class="cell-call-to-action">' + this.renderCallToActionButtons(deal, { showReactivate: true }) + '</td>' +
                '</tr>';
        }.bind(this)).join('');
        this.setupArchivedDealsBulkActions();
        this.updateArchivedDealsBulkActionsState();
    }

    setupArchivedDealsBulkActions() {
        var tbody = document.getElementById('archivedDealsTableBody');
        var selectAll = document.getElementById('archivedDealsSelectAllCheckbox');
        var bulkBtn = document.getElementById('archivedDealsBulkActionsBtn');
        var dropdown = document.getElementById('archivedDealsBulkDropdown');
        if (!tbody || !selectAll || !bulkBtn || !dropdown) return;
        if (this._archivedDealsBulkWired) return;
        this._archivedDealsBulkWired = true;

        selectAll.addEventListener('change', function() {
            var checked = selectAll.checked;
            tbody.querySelectorAll('.archived-deals-checkbox').forEach(function(cb) { cb.checked = checked; });
            if (window.dashboard) window.dashboard.updateArchivedDealsBulkActionsState();
        });
        tbody.addEventListener('change', function(e) {
            if (e.target.classList.contains('archived-deals-checkbox') && window.dashboard) {
                window.dashboard.updateArchivedDealsBulkActionsState();
            }
        });
        bulkBtn.addEventListener('click', function(e) {
            e.stopPropagation();
            if (bulkBtn.disabled) return;
            dropdown.classList.toggle('open');
        });
        dropdown.querySelectorAll('.bdd-bulk-dropdown-item').forEach(function(item) {
            item.addEventListener('click', function(e) {
                e.stopPropagation();
                var status = item.getAttribute('data-status');
                if (status && window.dashboard) window.dashboard.applyArchivedDealsBulkStatus(status);
                dropdown.classList.remove('open');
            });
        });
        document.addEventListener('click', function(e) {
            if (e.target.closest('#archivedDealsBulkActionsBtn') || e.target.closest('#archivedDealsBulkDropdown')) return;
            dropdown.classList.remove('open');
        });
        dropdown.addEventListener('click', function(e) { e.stopPropagation(); });
    }

    updateArchivedDealsBulkActionsState() {
        var tbody = document.getElementById('archivedDealsTableBody');
        var selectAll = document.getElementById('archivedDealsSelectAllCheckbox');
        var bulkBtn = document.getElementById('archivedDealsBulkActionsBtn');
        if (!tbody || !bulkBtn) return;
        var checkboxes = tbody.querySelectorAll('.archived-deals-checkbox');
        var checked = tbody.querySelectorAll('.archived-deals-checkbox:checked');
        bulkBtn.disabled = checked.length === 0;
        if (selectAll) selectAll.checked = checkboxes.length > 0 && checked.length === checkboxes.length;
    }

    async applyArchivedDealsBulkStatus(status) {
        var tbody = document.getElementById('archivedDealsTableBody');
        if (!tbody) return;
        var checked = tbody.querySelectorAll('.archived-deals-checkbox:checked');
        if (checked.length === 0) return;
        var updates = [];
        checked.forEach(function(cb) {
            var reqId = cb.getAttribute('data-request-id');
            if (reqId) updates.push({ requestId: reqId, status: status });
        });
        if (updates.length === 0) return;
        try {
            var res = await fetch(window.location.origin + '/api/brand-deal-requests/bulk-update', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ updates: updates })
            });
            var data = await res.json().catch(function() { return {}; });
            if (!res.ok) throw new Error(data.error || 'Update failed');
            this.showBddToast('Updated ' + updates.length + ' row(s)', true);
            updates.forEach(function(u) {
                this._applyLocalRequestPatch(u.requestId, { status: u.status });
            }.bind(this));
            this._applyLocalMutationEffects();
            this._scheduleBackgroundRefresh();
            document.getElementById('archivedDealsBulkDropdown')?.classList.remove('open');
        } catch (err) {
            this.showBddToast('Bulk update failed: ' + (err.message || 'Unknown error'), false);
        }
    }

    renderDealLog() {
        var tbody = document.getElementById('dealLogTableBody');
        if (!tbody) return;
        var entries = this.dealLogEntries || [];
        if (entries.length === 0) {
            tbody.innerHTML = '<tr><td colspan="4" style="padding: 60px 20px; text-align: center; color: var(--neutral--500); font-size: 14px;"><p style="margin: 0;">No activity yet. When you accept, decline, or respond to deals, entries will appear here.</p></td></tr>';
            return;
        }
        tbody.innerHTML = entries.map(function(e) {
            return '<tr>' +
                '<td>' + this.escapeHtml(e.date || '') + '</td>' +
                '<td>' + this.escapeHtml(e.dealName || '') + '</td>' +
                '<td>' + this.escapeHtml(e.action || '') + '</td>' +
                '<td>' + this.escapeHtml(e.details || '') + '</td>' +
                '</tr>';
        }.bind(this)).join('');
    }

    handleSort(column) {
        // If clicking the same column, toggle direction
        if (this.sortColumn === column) {
            this.sortDirection = this.sortDirection === 'asc' ? 'desc' : 'asc';
        } else {
            this.sortColumn = column;
            this.sortDirection = 'asc';
        }

        // Update header classes
        document.querySelectorAll('.deals-table th[data-sort]').forEach(th => {
            th.classList.remove('sort-asc', 'sort-desc');
            if (th.dataset.sort === column) {
                th.classList.add(this.sortDirection === 'asc' ? 'sort-asc' : 'sort-desc');
            }
        });

        // Sort the filtered deals
        this.sortDeals();
        
        // Re-render
        this.renderDeals();
    }

    sortDeals() {
        if (!this.sortColumn) return;

        this.filteredDeals.sort((a, b) => {
            let aVal, bVal;

            switch(this.sortColumn) {
                case 'status':
                    aVal = (a._requestStatus || '').toLowerCase();
                    bVal = (b._requestStatus || '').toLowerCase();
                    break;
                case 'propertyName':
                    aVal = (a.propertyName || '').toLowerCase();
                    bVal = (b.propertyName || '').toLowerCase();
                    break;
                case 'headline':
                    aVal = (a.headline || this.generateDealHeadline(a.dealFields, a.locationData, a.contactData) || '').toLowerCase();
                    bVal = (b.headline || this.generateDealHeadline(b.dealFields, b.locationData, b.contactData) || '').toLowerCase();
                    break;
                case 'contactName':
                    const aContactName = a.contactData?.['Main Contact Name'] || 
                                        a.contactData?.['Contact Name'] || 
                                        'Unknown';
                    const bContactName = b.contactData?.['Main Contact Name'] || 
                                        b.contactData?.['Contact Name'] || 
                                        'Unknown';
                    aVal = aContactName.toLowerCase();
                    bVal = bContactName.toLowerCase();
                    break;
                case 'location':
                    const aLocation = `${a.city || ''}, ${a.country || ''}`.toLowerCase();
                    const bLocation = `${b.city || ''}, ${b.country || ''}`.toLowerCase();
                    aVal = aLocation;
                    bVal = bLocation;
                    break;
                case 'chainScale':
                    aVal = (a.chainScale || '').toLowerCase();
                    bVal = (b.chainScale || '').toLowerCase();
                    break;
                case 'projectType':
                    aVal = (a.projectType || '').toLowerCase();
                    bVal = (b.projectType || '').toLowerCase();
                    break;
                case 'targetOpeningDate':
                    aVal = (a.targetOpeningDate || '').toLowerCase();
                    bVal = (b.targetOpeningDate || '').toLowerCase();
                    break;
                case 'propertyType':
                    aVal = (a.propertyType || '').toLowerCase();
                    bVal = (b.propertyType || '').toLowerCase();
                    break;
                case 'rooms':
                    aVal = parseInt(a.rooms) || 0;
                    bVal = parseInt(b.rooms) || 0;
                    break;
                case 'matchScore':
                    aVal = a.matchScore || 0;
                    bVal = b.matchScore || 0;
                    break;
                default:
                    return 0;
            }

            // Handle numeric vs string comparison
            if (this.sortColumn === 'rooms' || this.sortColumn === 'matchScore') {
                return this.sortDirection === 'asc' ? aVal - bVal : bVal - aVal;
            } else {
                // String comparison
                if (aVal < bVal) return this.sortDirection === 'asc' ? -1 : 1;
                if (aVal > bVal) return this.sortDirection === 'asc' ? 1 : -1;
                return 0;
            }
        });
    }

    async recalculateMatchScores() {
        if (!this.brandId) {
            // All-contacted mode: scores come from row._requestMatchScore (BDR) or deal.matchScore (preferredBrandScore)
            // Don't overwrite deal.matchScore - rows get _requestMatchScore when built in applyFilters
            console.log('All-contacted mode: using scores from Brand Deal Requests (per row)');
            this.applyFilters();
            if (this.sortColumn) this.sortDeals();
            this.renderDeals();
            return;
        }
        
        if (!this.deals || this.deals.length === 0) {
            console.error('No deals to recalculate scores for');
            this.filteredDeals = [];
            this.renderDeals();
            return;
        }
        
        // Recalculate match scores for all deals with the selected brand
        console.log(`Recalculating match scores for brand: ${this.brandId}`);
        console.log(`Processing ${this.deals.length} deals...`);
        
        let processed = 0;
        const SCORE_BATCH_SIZE = 8;
        for (let i = 0; i < this.deals.length; i += SCORE_BATCH_SIZE) {
            const batch = this.deals.slice(i, i + SCORE_BATCH_SIZE);
            await Promise.all(batch.map(async (deal) => {
                try {
                    const oldScore = deal.matchScore;
                    const map1 = deal.matchScoresNewByBrand || {};
                    const map2 = deal.matchScoresByBrand || {};
                    const precomputedScore = Object.prototype.hasOwnProperty.call(map1, this.brandId)
                        ? Number(map1[this.brandId])
                        : (Object.prototype.hasOwnProperty.call(map2, this.brandId) ? Number(map2[this.brandId]) : null);
                    const precomputedBreakdownMap = deal.matchBreakdownNewDetailsByBrand || deal.matchBreakdownDetailsByBrand || {};

                    if (precomputedScore != null && !Number.isNaN(precomputedScore)) {
                        deal.matchScore = precomputedScore;
                        deal.scoreBreakdown = precomputedBreakdownMap[this.brandId] || {};
                    } else {
                        const backendBreakdown = await this.fetchMatchScoreBreakdown(deal.id, this.brandId);
                        deal.matchScore = backendBreakdown?.score ?? 0;
                        deal.scoreBreakdown = backendBreakdown?.breakdown ?? {};
                    }
                    processed++;
                    if (oldScore !== deal.matchScore) {
                        console.log(`Deal ${deal.propertyName}: ${oldScore} -> ${deal.matchScore}`);
                    }
                } catch (error) {
                    console.error('Error recalculating match score for deal:', deal.id, error);
                    deal.matchScore = 0;
                    deal.scoreBreakdown = {};
                    processed++;
                }
            }));
        }
        
        console.log(`Finished recalculating scores for ${processed} deals.`);
        this.applyFilters();
        // Re-sort after filtering if sort is active
        if (this.sortColumn) {
            this.sortDeals();
        }
        this.renderDeals();
    }

    async fetchMatchScoreBreakdown(dealId, brandName) {
        if (!dealId || !brandName) return null;
        const base = window.location.origin || '';
        const url = `${base}/api/my-deals/${encodeURIComponent(dealId)}/match-score-breakdown?brand=${encodeURIComponent(brandName)}`;
        try {
            const res = await fetch(url);
            if (!res.ok) return null;
            const data = await res.json();
            return {
                score: data?.scoreNew != null ? Number(data.scoreNew) : null,
                breakdown: data?.breakdownNewDetails || {}
            };
        } catch (error) {
            console.warn('Failed to fetch match score breakdown:', error.message);
            return null;
        }
    }

    applyFilters() {
        const scoreFilter = document.getElementById('scoreFilter')?.value || '';
        const propertyTypeFilter = document.getElementById('propertyTypeFilter')?.value || '';
        const countryFilter = document.getElementById('countryFilter')?.value || '';
        const newDealsRequests = [
            ...(this.brandDealRequests.new || []).map(r => ({ ...r, _status: 'New' })),
            ...(this.brandDealRequests.viewed || []).map(r => ({ ...r, _status: 'Brand Viewed' }))
        ];
        const dealMap = new Map(this.deals.map(d => [d.id, d]));
        const sourceDeals = [];
        for (const req of newDealsRequests) {
            const deal = dealMap.get(req.dealId);
            if (!deal) continue;
            const row = { ...deal };
            row._requestId = req.id;
            row._requestStatus = req._status;
            row._requestMatchScore = req.matchScore;
            row._contactedBrand = req.brandName || '';
            sourceDeals.push(row);
        }
        const statusFilter = document.getElementById('statusFilter')?.value || '';
        this.filteredDeals = sourceDeals.filter(deal => {
            if (statusFilter && deal._requestStatus && deal._requestStatus !== statusFilter) return false;
            if (scoreFilter) {
                const score = deal._requestMatchScore != null ? Number(deal._requestMatchScore) : (deal.matchScore ?? 0);
                if (scoreFilter === 'high' && score < 80) return false;
                if (scoreFilter === 'medium' && (score < 50 || score >= 80)) return false;
                if (scoreFilter === 'low' && score >= 50) return false;
            }
            
            if (propertyTypeFilter && deal.propertyType !== propertyTypeFilter) {
                return false;
            }
            
            if (countryFilter && deal.country !== countryFilter) {
                return false;
            }
            
            return true;
        });
        
        // Apply sorting if active
        if (this.sortColumn) {
            this.sortDeals();
        }
        
        this.renderDeals();
    }

    async populateFilters() {
        // Populate brand filter from backend API to avoid direct Airtable fetch on page load.
        try {
            const base = window.location.origin || '';
            const url = `${base}/api/brand-library/brands?allStatuses=1`;
            const response = await fetch(url);
            if (response.ok) {
                const data = await response.json();
                const brandSelect = document.getElementById('brandFilter');
                const brands = Array.isArray(data.brands) ? data.brands : [];
                brands.forEach(record => {
                    const brandName = (record?.name || '').toString().trim();
                    if (brandName) {
                        const option = document.createElement('option');
                        option.value = brandName;
                        option.textContent = brandName;
                        if (this.brandId === brandName) {
                            option.selected = true;
                        }
                        brandSelect?.appendChild(option);
                    }
                });
            }
        } catch (error) {
            console.error('Error populating brand filter:', error);
        }
        
        // Populate property type filter
        const propertyTypes = [...new Set(this.deals.map(d => d.propertyType).filter(t => t && t !== 'Unknown'))];
        const propertyTypeSelect = document.getElementById('propertyTypeFilter');
        propertyTypes.forEach(type => {
            const option = document.createElement('option');
            option.value = type;
            option.textContent = type;
            propertyTypeSelect?.appendChild(option);
        });
        
        // Populate country filter
        const countries = [...new Set(this.deals.map(d => d.country).filter(c => c && c !== 'Unknown'))];
        const countrySelect = document.getElementById('countryFilter');
        countries.forEach(country => {
            const option = document.createElement('option');
            option.value = country;
            option.textContent = country;
            countrySelect?.appendChild(option);
        });

        // Populate Active Deals filters (Brand options shared; property types and countries from deals)
        const activeBrandSelect = document.getElementById('activeBrandFilter');
        if (activeBrandSelect && activeBrandSelect.options.length <= 1) {
            const brandsFromNew = document.getElementById('brandFilter');
            if (brandsFromNew) {
                for (let i = 1; i < brandsFromNew.options.length; i++) {
                    const opt = brandsFromNew.options[i];
                    const o = document.createElement('option');
                    o.value = opt.value;
                    o.textContent = opt.textContent;
                    activeBrandSelect.appendChild(o);
                }
            }
        }
        const activePropertySelect = document.getElementById('activePropertyTypeFilter');
        if (activePropertySelect && activePropertySelect.options.length <= 1) {
            propertyTypes.forEach(type => {
                const option = document.createElement('option');
                option.value = type;
                option.textContent = type;
                activePropertySelect.appendChild(option);
            });
        }
        const activeCountrySelect = document.getElementById('activeCountryFilter');
        if (activeCountrySelect && activeCountrySelect.options.length <= 1) {
            countries.forEach(country => {
                const option = document.createElement('option');
                option.value = country;
                option.textContent = country;
                activeCountrySelect.appendChild(option);
            });
        }

        // Populate Archived Deals filters with the same option sets
        const archivedBrandSelect = document.getElementById('archivedBrandFilter');
        if (archivedBrandSelect && archivedBrandSelect.options.length <= 1) {
            const brandsFromNew = document.getElementById('brandFilter');
            if (brandsFromNew) {
                for (let i = 1; i < brandsFromNew.options.length; i++) {
                    const opt = brandsFromNew.options[i];
                    const o = document.createElement('option');
                    o.value = opt.value;
                    o.textContent = opt.textContent;
                    archivedBrandSelect.appendChild(o);
                }
            }
        }
        const archivedPropertySelect = document.getElementById('archivedPropertyTypeFilter');
        if (archivedPropertySelect && archivedPropertySelect.options.length <= 1) {
            propertyTypes.forEach(type => {
                const option = document.createElement('option');
                option.value = type;
                option.textContent = type;
                archivedPropertySelect.appendChild(option);
            });
        }
        const archivedCountrySelect = document.getElementById('archivedCountryFilter');
        if (archivedCountrySelect && archivedCountrySelect.options.length <= 1) {
            countries.forEach(country => {
                const option = document.createElement('option');
                option.value = country;
                option.textContent = country;
                archivedCountrySelect.appendChild(option);
            });
        }
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    async showAlternativeBrands(dealId) {
        const deal = this.deals.find(d => d.id === dealId);
        if (!deal || !deal.preferredBrandName) {
            console.error('Deal not found or no preferred brand');
            return;
        }

        // Show loading state
        const modal = document.getElementById('alternativeBrandsModal');
        const content = document.getElementById('alternativeBrandsContent');
        if (!modal || !content) {
            console.error('Alternative brands modal not found');
            return;
        }

        modal.classList.add('active');
        content.innerHTML = `
            <div class="loading" style="position: relative; margin: 40px auto;">
                <div class="loading-content">
                    <div class="wave-container">
                        <div class="wave wave-1"></div>
                        <div class="wave wave-2"></div>
                        <div class="wave wave-3"></div>
                        <div class="wave-particles">
                            <div class="particle"></div>
                            <div class="particle"></div>
                            <div class="particle"></div>
                            <div class="particle"></div>
                        </div>
                    </div>
                    <div class="loading-text">
                        <div class="loading-text-main">Finding alternative brands...</div>
                        <div class="loading-text-time">Calculating match scores</div>
                    </div>
                </div>
            </div>
        `;

        try {
            const apiBase = (typeof window !== 'undefined' && window.location && window.location.origin) ? window.location.origin : '';
            let topAlternatives = [];
            let preferredScore = deal.preferredBrandScore != null ? deal.preferredBrandScore : 0;

            const apiRes = await fetch(apiBase + '/api/my-deals/' + encodeURIComponent(deal.id) + '/alternative-brands?limit=5');
            if (!apiRes.ok) {
                throw new Error(`Alternative brands API failed (${apiRes.status})`);
            }
            const data = await apiRes.json();
            if (data.alternatives && Array.isArray(data.alternatives) && data.alternatives.length > 0) {
                topAlternatives = data.alternatives.map(function(a) {
                    return { name: a.brand, score: a.score, breakdown: a.breakdownNewDetails || {} };
                });
                if (data.preferredScore != null) preferredScore = data.preferredScore;
            }

            // Render alternatives
            let alternativesHtml = `
                <div class="modal-section">
                    <h3>Alternative Brand Suggestions for ${this.escapeHtml(deal.propertyName)}</h3>
                    <p style="color: var(--neutral--400); margin-bottom: 20px;">
                        Owner's preferred brand: <strong>${this.escapeHtml(deal.preferredBrandName)}</strong> 
                        (Score: ${preferredScore}/100)
                    </p>
                </div>
                <div class="modal-section">
                    <h4>Top Alternative Brands</h4>
            `;

            if (topAlternatives.length === 0) {
                alternativesHtml += `
                    <p style="color: var(--neutral--400); padding: 20px;">
                        No alternative brands found with better match scores.
                    </p>
                `;
            } else {
                alternativesHtml += '<div class="alternative-brands-list">';
                for (const alternative of topAlternatives) {
                    const scoreClass = this.getScoreClass(alternative.score);
                    const isBetter = alternative.score > preferredScore;
                    const improvement = alternative.score - preferredScore;
                    
                    alternativesHtml += `
                        <div class="alternative-brand-item ${isBetter ? 'better-match' : ''}">
                            <div class="alternative-brand-header">
                                <div class="alternative-brand-name">${this.escapeHtml(alternative.name)}</div>
                                <div class="alternative-brand-score">
                                    <span class="match-score-badge ${scoreClass}">${alternative.score != null ? Number(alternative.score).toFixed(1) : '—'}</span>
                                    ${isBetter ? `<span class="improvement-badge">+${typeof improvement === 'number' ? Number(improvement).toFixed(1) : improvement}</span>` : ''}
                                </div>
                            </div>
                            <div class="alternative-brand-actions">
                                <button class="compare-btn" onclick="dashboard.showBrandComparison('${deal.id}', '${deal.preferredBrandName}', '${alternative.name}')">
                                    Compare with Preferred
                                </button>
                                <button class="recommend-btn" onclick="dashboard.recommendBrand('${deal.id}', '${deal.preferredBrandName}', '${alternative.name}', ${alternative.score})">
                                    Recommend This Brand
                                </button>
                            </div>
                        </div>
                    `;
                }
                alternativesHtml += '</div>';
            }

            alternativesHtml += '</div>';
            content.innerHTML = alternativesHtml;

        } catch (error) {
            console.error('Error loading alternative brands:', error);
            content.innerHTML = `
                <div class="modal-section">
                    <h3>Error</h3>
                    <p style="color: var(--system--red-400);">Failed to load alternative brands. Please try again.</p>
                </div>
            `;
        }
    }

    async showBrandComparison(dealId, preferredBrandName, alternativeBrandName) {
        const deal = this.deals.find(d => d.id === dealId);
        if (!deal) {
            console.error('Deal not found');
            return;
        }

        const modal = document.getElementById('brandComparisonModal');
        const content = document.getElementById('brandComparisonContent');
        if (!modal || !content) {
            console.error('Brand comparison modal not found');
            return;
        }

        modal.classList.add('active');
        content.innerHTML = `
            <div class="loading" style="position: relative; margin: 40px auto;">
                <div class="loading-content">
                    <div class="wave-container">
                        <div class="wave wave-1"></div>
                        <div class="wave wave-2"></div>
                        <div class="wave wave-3"></div>
                        <div class="wave-particles">
                            <div class="particle"></div>
                            <div class="particle"></div>
                            <div class="particle"></div>
                            <div class="particle"></div>
                        </div>
                    </div>
                    <div class="loading-text">
                        <div class="loading-text-main">Comparing brands...</div>
                        <div class="loading-text-time">Calculating match scores</div>
                    </div>
                </div>
            </div>
        `;

        try {
            const scoreMap = deal.matchScoresNewByBrand || deal.matchScoresByBrand || {};
            const breakdownMap = deal.matchBreakdownNewDetailsByBrand || deal.matchBreakdownDetailsByBrand || {};

            const preferredFromMap = Object.prototype.hasOwnProperty.call(scoreMap, preferredBrandName)
                ? Number(scoreMap[preferredBrandName]) : null;
            const alternativeFromMap = Object.prototype.hasOwnProperty.call(scoreMap, alternativeBrandName)
                ? Number(scoreMap[alternativeBrandName]) : null;
            const preferredBreakdownFromMap = breakdownMap[preferredBrandName] || {};
            const alternativeBreakdownFromMap = breakdownMap[alternativeBrandName] || {};

            const [preferredApi, alternativeApi, preferredBrandData, alternativeBrandData] = await Promise.all([
                preferredFromMap == null ? this.fetchMatchScoreBreakdown(deal.id, preferredBrandName) : Promise.resolve(null),
                alternativeFromMap == null ? this.fetchMatchScoreBreakdown(deal.id, alternativeBrandName) : Promise.resolve(null),
                this.getBrandData(preferredBrandName),
                this.getBrandData(alternativeBrandName)
            ]);

            const preferredScore = preferredFromMap != null
                ? preferredFromMap
                : (preferredApi?.score != null ? preferredApi.score : 0);
            const alternativeScore = alternativeFromMap != null
                ? alternativeFromMap
                : (alternativeApi?.score != null ? alternativeApi.score : 0);
            const preferredBreakdown = Object.keys(preferredBreakdownFromMap).length > 0
                ? preferredBreakdownFromMap
                : (preferredApi?.breakdown || {});
            const alternativeBreakdown = Object.keys(alternativeBreakdownFromMap).length > 0
                ? alternativeBreakdownFromMap
                : (alternativeApi?.breakdown || {});

            const scoreDifference = alternativeScore - preferredScore;
            const isBetter = scoreDifference > 0;

            // Metric labels mapping
            const metricLabels = {
                MKT1: 'Market Presence',
                MKT2: 'Brand Recognition',
                SEG1: 'Segment Alignment',
                SVC1: 'Service Level',
                SIZE1: 'Property Size',
                OWN1: 'Ownership Fit',
                STR1: 'Strategic Alignment',
                AMN1: 'Amenities Match',
                FIN1: 'Financial Fit',
                INC1: 'Incentives Match',
                PREF1: 'Preferences Match',
                KEY1: 'Key Money Willingness',
                CAP1: 'Capital Readiness',
                TERM1: 'Deal Terms',
                PROJ1: 'Project Type',
                PROJ2: 'Building Type',
                PROJ3: 'Project Stage',
                AGMT1: 'Agreement Type',
                ESG1: 'Sustainability & ESG'
            };

            // Render comparison
            let comparisonHtml = `
                <div class="modal-section">
                    <h3 style="margin-bottom: 8px;">${this.escapeHtml(deal.propertyName)}</h3>
                    <p style="color: var(--neutral--400); margin-bottom: 24px;">
                        Comparing owner's preferred brand with suggested alternative
                    </p>
                </div>
                <div class="brand-comparison-container">
                    <!-- Preferred Brand Card -->
                    <div class="comparison-brand-card preferred">
                        <div class="comparison-brand-header">
                            <div class="comparison-brand-name">${this.escapeHtml(preferredBrandName)}</div>
                            <div class="comparison-brand-score">
                                <div class="comparison-score-value match-score-badge ${this.getScoreClass(preferredScore)}">
                                    ${preferredScore != null ? Number(preferredScore).toFixed(1) : '—'}
                                </div>
                                <div class="comparison-score-label">Match Score</div>
                            </div>
                        </div>

                        <div class="comparison-section">
                            <div class="comparison-section-title">Key Metrics</div>
                            ${Object.entries(preferredBreakdown).map(([key, value]) => `
                                <div class="comparison-metric">
                                    <span class="comparison-metric-label">${metricLabels[key] || key}</span>
                                    <span class="comparison-metric-value ${(alternativeBreakdown[key] || 0) < value ? 'better' : (alternativeBreakdown[key] || 0) > value ? 'worse' : ''}">
                                        ${value}
                                    </span>
                                </div>
                            `).join('')}
                        </div>

                        ${preferredBrandData ? `
                            <div class="comparison-section">
                                <div class="comparison-section-title">Brand Details</div>
                                <div class="comparison-metric">
                                    <span class="comparison-metric-label">Chain Scale</span>
                                    <span class="comparison-metric-value">
                                        ${preferredBrandData.brandBasics?.['Chain Scale'] || 'N/A'}
                                    </span>
                                </div>
                                <div class="comparison-metric">
                                    <span class="comparison-metric-label">Service Level</span>
                                    <span class="comparison-metric-value">
                                        ${preferredBrandData.brandBasics?.['Service Level'] || 'N/A'}
                                    </span>
                                </div>
                                <div class="comparison-metric">
                                    <span class="comparison-metric-label">Segment</span>
                                    <span class="comparison-metric-value">
                                        ${preferredBrandData.brandBasics?.['Segment'] || 'N/A'}
                                    </span>
                                </div>
                            </div>
                        ` : ''}
                    </div>

                    <!-- Alternative Brand Card -->
                    <div class="comparison-brand-card alternative">
                        <div class="comparison-brand-header">
                            <div class="comparison-brand-name">${this.escapeHtml(alternativeBrandName)}</div>
                            <div class="comparison-brand-score">
                                <div class="comparison-score-value match-score-badge ${this.getScoreClass(alternativeScore)}">
                                    ${alternativeScore != null ? Number(alternativeScore).toFixed(1) : '—'}
                                </div>
                                <div class="comparison-score-label">Match Score</div>
                            </div>
                        </div>

                        <div class="comparison-section">
                            <div class="comparison-section-title">Key Metrics</div>
                            ${Object.entries(alternativeBreakdown).map(([key, value]) => `
                                <div class="comparison-metric">
                                    <span class="comparison-metric-label">${metricLabels[key] || key}</span>
                                    <span class="comparison-metric-value ${value > (preferredBreakdown[key] || 0) ? 'better' : value < (preferredBreakdown[key] || 0) ? 'worse' : ''}">
                                        ${value}
                                    </span>
                                </div>
                            `).join('')}
                        </div>

                        ${alternativeBrandData ? `
                            <div class="comparison-section">
                                <div class="comparison-section-title">Brand Details</div>
                                <div class="comparison-metric">
                                    <span class="comparison-metric-label">Chain Scale</span>
                                    <span class="comparison-metric-value">
                                        ${alternativeBrandData.brandBasics?.['Chain Scale'] || 'N/A'}
                                    </span>
                                </div>
                                <div class="comparison-metric">
                                    <span class="comparison-metric-label">Service Level</span>
                                    <span class="comparison-metric-value">
                                        ${alternativeBrandData.brandBasics?.['Service Level'] || 'N/A'}
                                    </span>
                                </div>
                                <div class="comparison-metric">
                                    <span class="comparison-metric-label">Segment</span>
                                    <span class="comparison-metric-value">
                                        ${alternativeBrandData.brandBasics?.['Segment'] || 'N/A'}
                                    </span>
                                </div>
                            </div>
                        ` : ''}
                    </div>

                    <!-- Comparison Summary -->
                    <div class="comparison-summary">
                        <div class="comparison-summary-title">Comparison Summary</div>
                        <div class="comparison-highlights">
                            <div class="comparison-highlight">
                                <div class="comparison-highlight-label">Score Difference</div>
                                <div class="comparison-highlight-value ${isBetter ? 'better' : 'worse'}" style="color: ${isBetter ? 'var(--system--green-400)' : 'var(--system--red-400)'};">
                                    ${isBetter ? '+' : ''}${scoreDifference} points
                                </div>
                            </div>
                            <div class="comparison-highlight">
                                <div class="comparison-highlight-label">Better Match</div>
                                <div class="comparison-highlight-value" style="color: ${isBetter ? 'var(--system--green-400)' : 'var(--accent--primary-1)'};">
                                    ${isBetter ? alternativeBrandName : preferredBrandName}
                                </div>
                            </div>
                            <div class="comparison-highlight">
                                <div class="comparison-highlight-label">Improvement</div>
                                <div class="comparison-highlight-value" style="color: ${isBetter ? 'var(--system--green-400)' : 'var(--neutral--400)'};">
                                    ${isBetter ? `${Math.round((scoreDifference / preferredScore) * 100)}%` : 'No improvement'}
                                </div>
                            </div>
                        </div>

                        <div class="comparison-breakdown" style="margin-top: 20px;">
                            ${Object.keys(metricLabels).map(key => {
                                const preferredValue = preferredBreakdown[key] || 0;
                                const alternativeValue = alternativeBreakdown[key] || 0;
                                const diff = alternativeValue - preferredValue;
                                return `
                                    <div class="comparison-breakdown-item">
                                        <div>
                                            <div class="comparison-breakdown-label">${metricLabels[key]}</div>
                                            <div class="comparison-breakdown-value" style="color: ${diff > 0 ? 'var(--system--green-400)' : diff < 0 ? 'var(--system--red-400)' : 'var(--neutral--400)'};">
                                                ${diff > 0 ? '+' : ''}${diff}
                                            </div>
                                        </div>
                                    </div>
                                `;
                            }).join('')}
                        </div>
                    </div>
                </div>

                <div class="modal-section" style="margin-top: 24px; display: flex; gap: 12px; justify-content: flex-end;">
                    <button class="compare-btn" onclick="closeBrandComparisonModal()">
                        Close
                    </button>
                    <button class="recommend-btn" onclick="dashboard.recommendBrand('${deal.id}', '${preferredBrandName}', '${alternativeBrandName}', ${alternativeScore})">
                        Recommend ${this.escapeHtml(alternativeBrandName)}
                    </button>
                </div>
            `;

            content.innerHTML = comparisonHtml;

        } catch (error) {
            console.error('Error loading brand comparison:', error);
            content.innerHTML = `
                <div class="modal-section">
                    <h3>Error</h3>
                    <p style="color: var(--system--red-400);">Failed to load brand comparison. Please try again.</p>
                    <button class="compare-btn" onclick="closeBrandComparisonModal()" style="margin-top: 16px;">
                        Close
                    </button>
                </div>
            `;
        }
    }

    showError(message) {
        const tbody = document.getElementById('dealsTableBody');
        if (tbody) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="12" class="empty-state">
                        <h3>Error</h3>
                        <p>${this.escapeHtml(message)}</p>
                    </td>
                </tr>
            `;
        }
    }
}

// Global functions for modal closing
function closeScoreDetailsModal() {
    document.getElementById('scoreDetailsModal')?.classList.remove('active');
}

function closeDealDetailsModal() {
    document.getElementById('dealDetailsModal')?.classList.remove('active');
}

function closeAlternativeBrandsModal() {
    document.getElementById('alternativeBrandsModal')?.classList.remove('active');
}

function closeBrandComparisonModal() {
    document.getElementById('brandComparisonModal')?.classList.remove('active');
}

// Initialize dashboard
const dashboard = new BrandDevelopmentDashboard();
window.dashboard = dashboard; // Make it globally accessible for onclick handlers
