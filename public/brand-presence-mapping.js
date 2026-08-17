// White Space Finder Tool JavaScript

let map;
let markersCluster;
let currentMarkers = [];
let hotelData = [];
let whiteSpaceLayer = null;
let territoryLayer = null;
let territoryRadiusCircle = null;
let territoryPinMarker = null;
let lastTerritoryRadiusAnalysis = null;
let penetrationLayer = null;
let pipelineLayer = null;
let infrastructureLayer = null;
let demandAnchorsLayer = null;
let cityAggregationLayer = null;
let allHotels = [];
let currentFilteredHotels = [];
let isCityAggregationEnabled = false;


// Global flag to prevent applyFilters during reset
let isResetting = false;

// Cleanup function to prevent memory leaks
function cleanup() {
    // Clear any running intervals
    if (statusProgressInterval) {
        clearInterval(statusProgressInterval);
        statusProgressInterval = null;
    }
    
    // Clear any running timeouts
    if (typeof timeout !== 'undefined' && timeout) {
        clearTimeout(timeout);
    }
    
    // Remove event listeners if needed
    if (map) {
        map.off();
    }
}

// Filter object
const PARENT_COMPANY_BLANK_VALUE = 'Unknown';
const PARENT_COMPANY_BLANK_LABEL = 'No Parent Company (Blank)';

function formatParentCompanyLabel(value) {
    if (!value || value === PARENT_COMPANY_BLANK_VALUE) return PARENT_COMPANY_BLANK_LABEL;
    return value;
}

let currentFilters = {
    parentCompany: '',
    brand: '',
    status: '',
    statuses: [],
    propertyType: '',
    region: '',
    country: '',
    market: '',
    submarket: '',
    locationType: '',
    hotelType: '',
    hotelServiceModel: '',
    operationType: '',
    managementCompany: '',
    roomsMin: '',
    roomsMax: '',
    search: '',
};

window.getRadarMapFilters = function getRadarMapFilters() {
    return currentFilters;
};

// Toggle states
let isHotelVisibilityEnabled = true;
let isWhiteSpaceVisible = false;
let isBrandTerritoryVisible = false;
let territoryPinMode = false;
let territoryRadiusFlyToNext = false;
let territoryGenerationToken = 0;
let territoryGenerateTimer = null;

function scheduleGenerateTerritoryMarkers() {
    if (territoryGenerateTimer) {
        clearTimeout(territoryGenerateTimer);
    }
    territoryGenerateTimer = setTimeout(function () {
        territoryGenerateTimer = null;
        generateTerritoryMarkers();
    }, 40);
}
let isPenetrationVisible = false;
let isPipelineVisible = false;
let isInfrastructureVisible = false;
let infrastructurePointTypeFilter = "all";
let infrastructureTypeCounts = null;
let infrastructureTotalCount = null;
let isDemandAnchorsVisible = false;
let demandAnchorsPointTypeFilter = "all";
let demandAnchorsTypeCounts = null;
let demandAnchorsTotalCount = null;
let isChainScaleView = false;

/** Per-layer display filters (drawer sub-controls). */
const MAP_LAYER_FILTERS = {
    hotelStatuses: { Open: true, Pipeline: true, Candidate: true },
    chainScales: {
        Luxury: true,
        'Upper Upscale': true,
        Upscale: true,
        'Upper Midscale': true,
        Midscale: true,
        Economy: true,
        Independent: true,
    },
    cityAggFilter: 'hotels:3',
    whiteSpaceLevels: { high: true, medium: true },
    territoryUnit: 'submarket',
    territoryRadiusKm: 10,
    territoryStatuses: { open: true, review_same: true, review_adjacent: true },
    territoryPin: null,
    penetrationLevels: { high: true, medium: true, low: true },
    pipelineLevels: { high: true, medium: true, low: true },
};

const MAP_LAYER_FILTER_DEFAULTS = JSON.parse(JSON.stringify(MAP_LAYER_FILTERS));

function migrateTerritoryStatusFilters() {
    const ts = MAP_LAYER_FILTERS.territoryStatuses;
    if (!ts || typeof ts !== 'object') return;
    if (Object.prototype.hasOwnProperty.call(ts, 'caution') &&
        !Object.prototype.hasOwnProperty.call(ts, 'review_same')) {
        const legacyOn = ts.caution !== false;
        ts.review_same = legacyOn;
        ts.review_adjacent = legacyOn;
        delete ts.caution;
    }
}

migrateTerritoryStatusFilters();

function resetMapLayerFilterGroup(group) {
    if (!MAP_LAYER_FILTER_DEFAULTS[group]) return;
    const defaultVal = MAP_LAYER_FILTER_DEFAULTS[group];
    if (typeof defaultVal === 'string' || typeof defaultVal === 'number') {
        MAP_LAYER_FILTERS[group] = defaultVal;
        return;
    }
    if (defaultVal && typeof defaultVal === 'object') {
        Object.keys(defaultVal).forEach(function (key) {
            MAP_LAYER_FILTERS[group][key] = defaultVal[key];
        });
    }
}

function normalizeRoomFilterValue(raw) {
    const trimmed = String(raw ?? '').trim();
    if (!trimmed) return null;
    const num = parseInt(trimmed, 10);
    if (!Number.isFinite(num) || num < 0) return null;
    return num;
}

function getActiveRoomFilterBounds() {
    let minRooms = normalizeRoomFilterValue(currentFilters.roomsMin);
    let maxRooms = normalizeRoomFilterValue(currentFilters.roomsMax);
    if (minRooms != null && maxRooms != null && minRooms > maxRooms) {
        const swap = minRooms;
        minRooms = maxRooms;
        maxRooms = swap;
    }
    return { minRooms, maxRooms };
}

function hotelMatchesRoomFilter(hotel, minRooms, maxRooms) {
    if (minRooms == null && maxRooms == null) return true;
    const rooms = parseInt(hotel && hotel.rooms, 10);
    if (!Number.isFinite(rooms) || rooms <= 0) return false;
    if (minRooms != null && rooms < minRooms) return false;
    if (maxRooms != null && rooms > maxRooms) return false;
    return true;
}

/** Territory scans need full competitive context — never narrow census by subject brand. */
function filterHotelsForTerritoryScope(hotels) {
    let filteredHotels = [...(hotels || [])];
    const roomBounds = getActiveRoomFilterBounds();

    if (currentFilters.search) {
        const searchTerm = currentFilters.search.toLowerCase();
        filteredHotels = filteredHotels.filter(function (hotel) {
            return (
                String(hotel.name || '').toLowerCase().includes(searchTerm) ||
                String(hotel.city || '').toLowerCase().includes(searchTerm) ||
                String(hotel.country || '').toLowerCase().includes(searchTerm) ||
                String(hotel.brand || '').toLowerCase().includes(searchTerm)
            );
        });
    }

    if (currentFilters.parentCompany) {
        filteredHotels = filteredHotels.filter(function (hotel) {
            return hotel.parentCompany === currentFilters.parentCompany;
        });
    }

    if (currentFilters.statuses && currentFilters.statuses.length) {
        filteredHotels = filteredHotels.filter(function (hotel) {
            return currentFilters.statuses.includes(hotel.status);
        });
    } else if (currentFilters.status) {
        filteredHotels = filteredHotels.filter(function (hotel) {
            return hotel.status === currentFilters.status;
        });
    }

    if (currentFilters.propertyType) {
        filteredHotels = filteredHotels.filter(function (hotel) {
            return hotel.propertyType === currentFilters.propertyType;
        });
    }

    if (currentFilters.region) {
        filteredHotels = filteredHotels.filter(function (hotel) {
            return matchesGeoField(hotel.region, currentFilters.region);
        });
    }

    if (currentFilters.country) {
        filteredHotels = filteredHotels.filter(function (hotel) {
            return matchesGeoField(hotel.country, currentFilters.country);
        });
    }

    if (currentFilters.market) {
        filteredHotels = filteredHotels.filter(function (hotel) {
            return matchesGeoField(hotel.market, currentFilters.market);
        });
    }

    if (currentFilters.submarket && currentFilters.market) {
        filteredHotels = filteredHotels.filter(function (hotel) {
            return matchesGeoField(hotel.submarket, currentFilters.submarket);
        });
    }

    if (currentFilters.locationType) {
        filteredHotels = filteredHotels.filter(function (hotel) {
            return hotel.locationType === currentFilters.locationType;
        });
    }

    if (currentFilters.hotelType) {
        filteredHotels = filteredHotels.filter(function (hotel) {
            return hotel.censusPropertyType === currentFilters.hotelType;
        });
    }

    if (currentFilters.hotelServiceModel) {
        filteredHotels = filteredHotels.filter(function (hotel) {
            return hotel.hotelServiceModel === currentFilters.hotelServiceModel;
        });
    }

    if (currentFilters.operationType) {
        filteredHotels = filteredHotels.filter(function (hotel) {
            return hotel.operationType === currentFilters.operationType;
        });
    }

    if (currentFilters.managementCompany) {
        filteredHotels = filteredHotels.filter(function (hotel) {
            return hotel.managementCompany === currentFilters.managementCompany;
        });
    }

    if (roomBounds.minRooms != null || roomBounds.maxRooms != null) {
        filteredHotels = filteredHotels.filter(function (hotel) {
            return hotelMatchesRoomFilter(hotel, roomBounds.minRooms, roomBounds.maxRooms);
        });
    }

    return filteredHotels;
}

function getHotelsForTerritoryAnalysis() {
    const baseHotels = hotelData && hotelData.length ? hotelData : allHotels;
    return filterHotelsForMapLayer(filterHotelsForTerritoryScope(baseHotels));
}

function getTerritorySubjectBrand() {
    const territoryBrand = document.getElementById('territoryBrandFilter');
    if (territoryBrand && String(territoryBrand.value || '').trim()) {
        return String(territoryBrand.value).trim();
    }
    return String(currentFilters.brand || '').trim();
}

/** Service / Operating Model filter display order (matches census Hotel Service Model select options). */
const HOTEL_SERVICE_MODEL_ORDER = [
    'Full-Service',
    'Select-Service',
    'Extended Stay',
    'All-Inclusive',
    'Lifestyle / Boutique',
    'All-Inclusive Resort',
    'Lifestyle / Full-Service',
    'Lifestyle / Wellness Resort',
    'Lifestyle / Select-Service',
];

function sortHotelServiceModelOptions(options) {
    return [...options].sort(function (a, b) {
        const ia = HOTEL_SERVICE_MODEL_ORDER.indexOf(a);
        const ib = HOTEL_SERVICE_MODEL_ORDER.indexOf(b);
        if (ia === -1 && ib === -1) return (a || '').localeCompare(b || '');
        if (ia === -1) return 1;
        if (ib === -1) return -1;
        return ia - ib;
    });
}

/** Keep "Other" last when it is a real census submarket value — never inject it. */
function sortSubmarketOptions(options) {
    return [...options].sort(function (a, b) {
        const aIsOther = String(a || '').trim().toLowerCase() === 'other';
        const bIsOther = String(b || '').trim().toLowerCase() === 'other';
        if (aIsOther && !bIsOther) return 1;
        if (!aIsOther && bIsOther) return -1;
        return (a || '').localeCompare(b || '', undefined, { sensitivity: 'base' });
    });
}

function normalizeGeoField(value) {
    return String(value || '').trim();
}

function matchesGeoField(hotelValue, filterValue) {
    if (!filterValue) return true;
    return normalizeGeoField(hotelValue) === normalizeGeoField(filterValue);
}

function applyGeographyFiltersToHotels(hotels) {
    let scoped = [...(hotels || [])];
    if (currentFilters.region) {
        scoped = scoped.filter(function (hotel) {
            return matchesGeoField(hotel.region, currentFilters.region);
        });
    }
    if (currentFilters.country) {
        scoped = scoped.filter(function (hotel) {
            return matchesGeoField(hotel.country, currentFilters.country);
        });
    }
    if (currentFilters.market) {
        scoped = scoped.filter(function (hotel) {
            return matchesGeoField(hotel.market, currentFilters.market);
        });
    }
    if (currentFilters.submarket && currentFilters.market) {
        scoped = scoped.filter(function (hotel) {
            return matchesGeoField(hotel.submarket, currentFilters.submarket);
        });
    }
    return scoped;
}

function getGeographyDropdownScope(dataSource) {
    let scoped = [...(dataSource || [])];
    if (currentFilters.region) {
        scoped = scoped.filter(function (hotel) {
            return matchesGeoField(hotel.region, currentFilters.region);
        });
    }
    if (currentFilters.country) {
        scoped = scoped.filter(function (hotel) {
            return matchesGeoField(hotel.country, currentFilters.country);
        });
    }
    return scoped;
}

function clearGeographyFiltersBelow(level) {
    const levels = ['region', 'country', 'market', 'submarket'];
    const start = levels.indexOf(level);
    if (start < 0) return;
    for (let i = start + 1; i < levels.length; i += 1) {
        currentFilters[levels[i]] = '';
        const node = document.getElementById(levels[i] + 'Filter');
        if (node) node.value = '';
    }
}

function cloneMapLayerFilters() {
    return JSON.parse(JSON.stringify(MAP_LAYER_FILTERS));
}

function normalizeMapChainScale(hotel) {
    const brand = String((hotel && hotel.brand) || '').trim().toLowerCase();
    if (brand === 'independent') return 'Independent';

    let scale = String((hotel && (hotel.chainScale || hotel.propertyType)) || '').trim();
    if (!scale || scale === 'Unknown') return null;

    scale = scale.replace(/\s+Chain\s*$/i, '').trim() || scale;
    const scaleKey = scale.toLowerCase();
    if (scaleKey === 'independent' || scaleKey === 'indepandant') return 'Independent';
    return scale;
}

function isMapLayerHotelStatusVisible(status) {
    const key = String(status || '').trim();
    if (key === 'Open') return MAP_LAYER_FILTERS.hotelStatuses.Open !== false;
    if (key === 'Pipeline') return MAP_LAYER_FILTERS.hotelStatuses.Pipeline !== false;
    if (key === 'Candidate') return MAP_LAYER_FILTERS.hotelStatuses.Candidate !== false;
    return true;
}

function isMapLayerChainScaleVisible(hotel) {
    if (!isChainScaleView) return true;
    const scale = normalizeMapChainScale(hotel);
    if (!scale) return false;
    if (MAP_LAYER_FILTERS.chainScales[scale] === false) return false;
    return true;
}

function filterHotelsForMapLayer(hotels) {
    return (hotels || []).filter(function (hotel) {
        return isMapLayerHotelStatusVisible(hotel.status) && isMapLayerChainScaleVisible(hotel);
    });
}

function isMapLayerLevelVisible(group, level) {
    const bucket = MAP_LAYER_FILTERS[group];
    if (!bucket) return true;
    return bucket[String(level).toLowerCase()] !== false;
}

function refreshMapLayersAfterFilterChange(options) {
    const opts = options || {};
    if (isHotelVisibilityEnabled && !opts.skipHotelMarkers) {
        displayHotels(currentFilteredHotels);
    }
    if (isWhiteSpaceVisible) generateWhiteSpaceMarkers();
    if (isBrandTerritoryVisible) scheduleGenerateTerritoryMarkers();
    if (isPenetrationVisible) generatePenetrationMarkers();
    if (isPipelineVisible) generatePipelineMarkers();
    if (isCityAggregationEnabled) renderCityAggregationMarkers();
}

window.setMapLayerFilter = function setMapLayerFilter(group, key, enabled) {
    if (group === 'cityAggFilter') {
        MAP_LAYER_FILTERS.cityAggFilter = String(key || 'hotels:1');
    } else if (group === 'cityAggMinHotels') {
        MAP_LAYER_FILTERS.cityAggFilter = 'hotels:' + Math.max(1, parseInt(key, 10) || 1);
    } else if (group === 'territoryUnit') {
        MAP_LAYER_FILTERS.territoryUnit = String(key || 'submarket');
        if (window.RadarLayerDrawer && typeof window.RadarLayerDrawer.syncTerritoryControls === 'function') {
            window.RadarLayerDrawer.syncTerritoryControls();
        }
    } else if (group === 'territoryRadiusKm') {
        MAP_LAYER_FILTERS.territoryRadiusKm = Math.max(1, parseInt(key, 10) || 10);
        if (MAP_LAYER_FILTERS.territoryUnit === 'radius' && MAP_LAYER_FILTERS.territoryPin) {
            showTerritoryRadiusOverlay(MAP_LAYER_FILTERS.territoryPin, MAP_LAYER_FILTERS.territoryRadiusKm, {
                flyTo: false,
                territory: lastTerritoryRadiusAnalysis,
            });
        }
    } else if (MAP_LAYER_FILTERS[group] && Object.prototype.hasOwnProperty.call(MAP_LAYER_FILTERS[group], key)) {
        MAP_LAYER_FILTERS[group][key] = !!enabled;
    }
    refreshMapLayersAfterFilterChange();
    if (window.RadarLayerDrawer && typeof window.RadarLayerDrawer.syncControls === 'function') {
        window.RadarLayerDrawer.syncControls();
    }
    notifyRadarFilterBadge();
};

window.resetMapLayerFilters = function resetMapLayerFilters() {
    const defaults = cloneMapLayerFilters();
    Object.keys(defaults).forEach(function (k) {
        MAP_LAYER_FILTERS[k] = defaults[k];
    });
    refreshMapLayersAfterFilterChange();
    if (window.RadarLayerDrawer && typeof window.RadarLayerDrawer.syncControls === 'function') {
        window.RadarLayerDrawer.syncControls();
    }
    notifyRadarFilterBadge();
};

window.getMapLayerFilters = function getMapLayerFilters() {
    return MAP_LAYER_FILTERS;
};

window.resetMapLayerSubfilters = function resetMapLayerSubfilters(toggleId) {
    const id = String(toggleId || '').trim();
    if (!id) return;

    switch (id) {
        case 'hotelVisibilityToggle':
            resetMapLayerFilterGroup('hotelStatuses');
            break;
        case 'chainScaleToggle':
            resetMapLayerFilterGroup('chainScales');
            break;
        case 'cityAggregationToggle':
            MAP_LAYER_FILTERS.cityAggFilter = MAP_LAYER_FILTER_DEFAULTS.cityAggFilter;
            break;
        case 'whiteSpaceToggle':
            resetMapLayerFilterGroup('whiteSpaceLevels');
            break;
        case 'brandTerritoryToggle':
            resetMapLayerFilterGroup('territoryStatuses');
            migrateTerritoryStatusFilters();
            MAP_LAYER_FILTERS.territoryUnit = MAP_LAYER_FILTER_DEFAULTS.territoryUnit;
            MAP_LAYER_FILTERS.territoryRadiusKm = MAP_LAYER_FILTER_DEFAULTS.territoryRadiusKm;
            MAP_LAYER_FILTERS.territoryPin = null;
            if (typeof stopTerritoryPinMode === 'function') {
                stopTerritoryPinMode();
            }
            if (typeof resetTerritoryRadiusState === 'function') {
                resetTerritoryRadiusState();
            } else if (typeof clearTerritoryRadiusCircle === 'function') {
                clearTerritoryRadiusCircle();
            }
            {
                const pinHint = document.getElementById('territoryPinHint');
                if (pinHint) pinHint.textContent = '';
                const unitSelect = document.getElementById('territoryUnitSelect');
                if (unitSelect) unitSelect.value = MAP_LAYER_FILTER_DEFAULTS.territoryUnit || 'submarket';
                const radiusWrap = document.getElementById('territoryRadiusControls');
                if (radiusWrap) radiusWrap.style.display = 'none';
                const hint = document.getElementById('territoryBrandRequiredHint');
                if (hint) {
                    hint.textContent = 'Select a Parent Company and Subject Brand to scan territories.';
                }
            }
            setTerritoryParentCompanyFilterValue('');
            setTerritoryBrandFilterValue('');
            if (window.RadarFilterDrawer && typeof window.RadarFilterDrawer.syncFromBar === 'function') {
                window.RadarFilterDrawer.syncFromBar();
            }
            applyFilters();
            if (window.RadarLayerDrawer && typeof window.RadarLayerDrawer.syncControls === 'function') {
                window.RadarLayerDrawer.syncControls();
            }
            if (window.RadarLayerDrawer && typeof window.RadarLayerDrawer.syncTerritoryControls === 'function') {
                window.RadarLayerDrawer.syncTerritoryControls();
            }
            notifyRadarFilterBadge();
            return;
        case 'penetrationToggle':
            resetMapLayerFilterGroup('penetrationLevels');
            break;
        case 'pipelineToggle':
            resetMapLayerFilterGroup('pipelineLevels');
            break;
        case 'infrastructureToggle':
            selectInfrastructurePointTypeFilter('all');
            if (window.RadarLayerDrawer && typeof window.RadarLayerDrawer.syncControls === 'function') {
                window.RadarLayerDrawer.syncControls();
            }
            return;
        case 'demandAnchorsToggle':
            selectDemandAnchorsPointTypeFilter('all');
            if (window.RadarLayerDrawer && typeof window.RadarLayerDrawer.syncControls === 'function') {
                window.RadarLayerDrawer.syncControls();
            }
            return;
        default:
            return;
    }

    refreshMapLayersAfterFilterChange();
    if (window.RadarLayerDrawer && typeof window.RadarLayerDrawer.syncControls === 'function') {
        window.RadarLayerDrawer.syncControls();
    }
    if (window.RadarLayerDrawer && typeof window.RadarLayerDrawer.syncTerritoryControls === 'function') {
        window.RadarLayerDrawer.syncTerritoryControls();
    }
    notifyRadarFilterBadge();
};

window.countRadarMapLayerFilters = function countRadarMapLayerFilters() {
    let count = 0;

    const hotelToggle = document.getElementById('hotelVisibilityToggle');
    if (hotelToggle && !hotelToggle.checked) count += 1;

    const overlayToggleIds = [
        'chainScaleToggle',
        'cityAggregationToggle',
        'whiteSpaceToggle',
        'brandTerritoryToggle',
        'penetrationToggle',
        'pipelineToggle',
        'infrastructureToggle',
        'demandAnchorsToggle',
    ];
    overlayToggleIds.forEach(function (id) {
        const toggle = document.getElementById(id);
        if (toggle && toggle.checked) count += 1;
    });

    function groupNarrowed(group, keys) {
        const bucket = MAP_LAYER_FILTERS[group];
        if (!bucket) return false;
        return keys.some(function (key) { return bucket[key] === false; });
    }

    if (hotelToggle && hotelToggle.checked && groupNarrowed('hotelStatuses', ['Open', 'Pipeline', 'Candidate'])) {
        count += 1;
    }
    if (isChainScaleView && groupNarrowed('chainScales', Object.keys(MAP_LAYER_FILTERS.chainScales))) {
        count += 1;
    }
    if (isCityAggregationEnabled && MAP_LAYER_FILTERS.cityAggFilter !== 'hotels:3') {
        count += 1;
    }
    if (isWhiteSpaceVisible && groupNarrowed('whiteSpaceLevels', ['high', 'medium'])) {
        count += 1;
    }
    if (isBrandTerritoryVisible) {
        if (MAP_LAYER_FILTERS.territoryUnit && MAP_LAYER_FILTERS.territoryUnit !== 'submarket') {
            count += 1;
        }
        if (MAP_LAYER_FILTERS.territoryUnit === 'radius') {
            if (MAP_LAYER_FILTERS.territoryRadiusKm !== 10) count += 1;
            if (MAP_LAYER_FILTERS.territoryPin) count += 1;
        }
        if (groupNarrowed('territoryStatuses', ['open', 'review_same', 'review_adjacent'])) {
            count += 1;
        }
    }
    if (isPenetrationVisible && groupNarrowed('penetrationLevels', ['high', 'medium', 'low'])) {
        count += 1;
    }
    if (isPipelineVisible && groupNarrowed('pipelineLevels', ['high', 'medium', 'low'])) {
        count += 1;
    }
    if (isInfrastructureVisible && infrastructurePointTypeFilter && infrastructurePointTypeFilter !== 'all') {
        count += 1;
    }
    if (isDemandAnchorsVisible && demandAnchorsPointTypeFilter && demandAnchorsPointTypeFilter !== 'all') {
        count += 1;
    }

    return count;
};

function notifyRadarFilterBadge() {
    if (window.RadarFilterDrawer && typeof window.RadarFilterDrawer.updateBadge === 'function') {
        window.RadarFilterDrawer.updateBadge();
    }
}

window.preloadRadarLayerFilterChips = async function preloadRadarLayerFilterChips() {
    try {
        if (!infrastructureTypeCounts) {
            await loadInfrastructureSummaryCounts();
        } else {
            renderInfrastructureFilterChips();
        }
    } catch (err) {
        console.warn("[radar-layer-drawer] infrastructure chip preload failed", err);
    }
    try {
        if (!demandAnchorsTypeCounts) {
            await loadDemandAnchorsSummaryCounts();
        } else {
            renderDemandAnchorsFilterChips();
        }
    } catch (err) {
        console.warn("[radar-layer-drawer] demand anchor chip preload failed", err);
    }
    setInfrastructureFilterBarVisible(isInfrastructureVisible);
    setDemandAnchorsFilterBarVisible(isDemandAnchorsVisible);
};

// Initialize the map
function initializeMap() {
    // Initialize map centered on CALA region with improved zoom settings
    map = L.map('map', {
        center: [10.0, -80.0],
        zoom: 4,
        minZoom: 2,        // Allow zooming out to see entire region
        maxZoom: 16,       // Allow detailed zoom for city level
        zoomSnap: 0.5,     // Snap to half zoom levels for smoother transitions
        zoomDelta: 0.5,    // Smaller zoom increments for better control
        wheelPxPerZoomLevel: 120,  // More scroll wheel sensitivity
        scrollWheelZoom: true,     // Enable scroll wheel zoom
        doubleClickZoom: true,     // Enable double-click zoom
        boxZoom: true,             // Enable box zoom
        keyboard: true,            // Enable keyboard navigation
        dragging: true,            // Enable map dragging
        touchZoom: true,           // Enable touch zoom for mobile
        bounceAtZoomLimits: true,  // Bounce effect at zoom limits
        zoomAnimation: true,       // Smooth zoom animations
        fadeAnimation: true,       // Fade animations
        markerZoomAnimation: true, // Animate markers during zoom
        zoomControl: true          // Explicitly enable zoom controls
    });
    
    // Add tile layer
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap contributors',
        keepBuffer: 3,
        updateWhenIdle: true,
    }).addTo(map);
    
    bindMapSizeSync();
    refreshMapSize({ pan: false });
    setTimeout(function () { refreshMapSize({ pan: false }); }, 300);
    
    // Initialize marker cluster with optimized settings for performance
    markersCluster = L.markerClusterGroup({
        maxClusterRadius: 60,        // Balanced clustering for performance
        spiderfyOnMaxZoom: true,     // Show individual markers when zoomed in
        showCoverageOnHover: false,  // Disabled for better performance
        zoomToBoundsOnClick: true,   // Zoom to bounds when clicking cluster
        disableClusteringAtZoom: 12,
        chunkedLoading: true,
        removeOutsideVisibleBounds: true,
        // Custom cluster styling to make clusters more visible
        iconCreateFunction: function(cluster) {
            const childCount = cluster.getChildCount();
            let size = 'small';
            if (childCount > 100) size = 'large';
            else if (childCount > 20) size = 'medium';
            
            return L.divIcon({
                html: '<div class="cluster-icon cluster-' + size + '">' + childCount + '</div>',
                className: 'marker-cluster',
                iconSize: size === 'large' ? [30, 30] : size === 'medium' ? [25, 25] : [20, 20]
            });
        },
        chunkProgress: function(processed, total, elapsed, layersArray) {
            // Optional: show progress if needed
        }
    });
    map.addLayer(markersCluster);
    
    
    // Zoom controls are enabled by default in Leaflet
    // Add keyboard shortcuts for zoom
    addZoomKeyboardShortcuts();

    // Add additional map controls
    
    // Load hotel data
    loadHotelData();
}

let mapSizeSyncBound = false;
let mapSizeRefreshTimer = null;

/** Re-measure #map after layout/zoom changes so tiles fill the full container. */
function refreshMapSize(opts) {
    if (!map) return;
    const options = opts || {};
    const delay = Number.isFinite(options.delay) ? options.delay : 100;
    const pan = options.pan === true;
    if (mapSizeRefreshTimer) clearTimeout(mapSizeRefreshTimer);
    mapSizeRefreshTimer = setTimeout(function () {
        mapSizeRefreshTimer = null;
        try {
            map.invalidateSize({ pan: pan, animate: false });
        } catch (err) {
            console.warn('[radar-map] invalidateSize failed:', err);
        }
    }, delay);
}

function bindMapSizeSync() {
    if (mapSizeSyncBound || !map) return;
    mapSizeSyncBound = true;

    window.addEventListener('resize', function () {
        refreshMapSize({ pan: false, delay: 150 });
    });

    window.addEventListener('load', function () {
        refreshMapSize({ pan: false, delay: 50 });
    });

    map.on('zoomend', function () {
        refreshMapSize({ pan: false, delay: 50 });
    });
}

window.refreshRadarMapSize = refreshMapSize;

// Load hotel data
async function loadHotelData() {
    try {
        showLoading(true);
        showSystemStatus('Loading Market Signals…');
        
        // Fetch all data from API with high limit
        const response = await fetch('/api/brand-presence?limit=100000', {
            headers: {
                'ngrok-skip-browser-warning': 'true'
            }
        });
        const result = await response.json();
        
        if (result.success) {
            updateSystemStatus('Processing Market Signals…');
            allHotels = result.hotels;
            window.allHotels = allHotels;
            hotelData = [...allHotels];
            currentFilteredHotels = [...allHotels];
            if (result.skippedNoCoordinates) console.warn(result.skippedNoCoordinates + " Airtable records have no coordinates and are not shown on the map.");
            
            updateSystemStatus('Displaying hotels on map…');
            await displayHotels(hotelData);
            refreshMapSize({ pan: false, delay: 50 });
            showLoading(false);
            hideSystemStatus();
            // Defer sidebar stats so the map can paint first
            requestAnimationFrame(function () {
                updateStatistics(hotelData);
                updateBrandDistribution(hotelData);
                generateInsights(hotelData);
                updateAllDropdowns(hotelData);
                if (isCityAggregationEnabled) renderCityAggregationMarkers();
            });
        } else {
            throw new Error('API returned error: ' + result.error);
        }
    } catch (error) {
        console.error('Error loading hotel data:', error);
        allHotels = [];
        hotelData = [];
        currentFilteredHotels = [];
        window.allHotels = [];
        markersCluster.clearLayers();
        currentMarkers = [];
        updateStatistics([]);
        updateBrandDistribution([]);
        generateInsights([]);
        updateAllDropdowns([]);
        showNoResultsMessage('Unable to load live hotel census data. Please verify /api/brand-presence response.');
    } finally {
        showLoading(false);
        hideSystemStatus();
    }
}

// Display hotels on map
async function displayHotels(hotels) {
    // Check if hotel visibility is enabled
    if (!isHotelVisibilityEnabled) {
        return;
    }
    
    // Debug: Log first few hotels to see their data
    if (hotels.length > 0) {
        console.log('Sample hotel data:', {
            name: hotels[0].name,
            lat: hotels[0].lat,
            lng: hotels[0].lng,
            status: hotels[0].status,
            parentCompany: hotels[0].parentCompany
        });
    }
    
    // Clear existing markers
    markersCluster.clearLayers();
    currentMarkers = [];
    
    // Add markers for each hotel
    let markersAdded = 0;
    let skippedInvalid = 0;
    const markerPositions = buildHotelMarkerPositions(filterHotelsForMapLayer(hotels));
    const batchSize = 800;
    const pendingLayers = [];

    console.log(
        'Processing ' + markerPositions.length + ' map markers from ' + hotels.length + ' hotels'
    );

    for (let i = 0; i < markerPositions.length; i += 1) {
        const entry = markerPositions[i];
        const marker = createHotelMarker(entry.hotel, entry);
        if (marker) {
            pendingLayers.push(marker);
            currentMarkers.push(marker);
            markersAdded++;
        } else {
            skippedInvalid++;
        }

        if (pendingLayers.length >= batchSize) {
            markersCluster.addLayers(pendingLayers);
            pendingLayers.length = 0;
            updateSystemStatus(
                'Displaying hotels on map… ' +
                    Math.round(((i + 1) / markerPositions.length) * 100) +
                    '%'
            );
            await new Promise(function (resolve) {
                requestAnimationFrame(resolve);
            });
        }
    }

    if (pendingLayers.length) {
        markersCluster.addLayers(pendingLayers);
    }
    
    // Ensure cluster is added to map
    if (!map.hasLayer(markersCluster)) {
        map.addLayer(markersCluster);
    }
    
    // Force marker visibility by ensuring cluster is properly added
    console.log(`Completed processing: ${markersAdded} markers added, ${skippedInvalid} skipped`);
    if (isCityAggregationEnabled) renderCityAggregationMarkers();
}

function getHotelsForAggregation() {
    const hasActiveFilters = Object.values(currentFilters || {}).some(function (value) {
        return String(value || '').trim() !== '';
    });
    if (hasActiveFilters) return currentFilteredHotels || [];
    return allHotels || [];
}

function clearCityAggregationLayer() {
    if (cityAggregationLayer) {
        map.removeLayer(cityAggregationLayer);
        cityAggregationLayer = null;
    }
}

function parseCityAggFilter(token) {
    const raw = String(token || MAP_LAYER_FILTERS.cityAggFilter || 'hotels:1').trim();
    const parts = raw.split(':');
    const mode = parts[0] || 'hotels';
    const min = Math.max(0, parseInt(parts[1], 10) || 0);
    return { mode: mode, min: min };
}

function cityRowPassesAggFilter(row, token) {
    const filter = parseCityAggFilter(token);
    if (filter.mode === 'rooms') return row.totalRooms >= filter.min;
    if (filter.mode === 'open') return row.openHotels >= filter.min;
    if (filter.mode === 'pipeline') return row.pipelineHotels >= filter.min;
    return row.totalHotels >= Math.max(1, filter.min || 1);
}

function renderCityAggregationMarkers() {
    if (!map) return;
    clearCityAggregationLayer();
    if (!isCityAggregationEnabled) return;

    const sourceHotels = getHotelsForAggregation();
    if (!sourceHotels.length) return;

    const cityGroups = {};
    sourceHotels.forEach(function (hotel) {
        const city = (hotel.city || '').trim();
        const country = (hotel.country || '').trim();
        if (!city) return;
        const cityKey = city + '|' + (country || 'Unknown Country');
        if (!cityGroups[cityKey]) {
            cityGroups[cityKey] = {
                city: city,
                country: country || 'Unknown Country',
                totalHotels: 0,
                totalRooms: 0,
                withCoordinates: 0,
                withoutCoordinates: 0,
                openHotels: 0,
                pipelineHotels: 0,
                candidateHotels: 0,
                lat: null,
                lng: null
            };
        }
        const row = cityGroups[cityKey];
        row.totalHotels += 1;
        row.totalRooms += Number(hotel.rooms) || 0;

        const status = String(hotel.status || '').toLowerCase();
        if (status === 'open') row.openHotels += 1;
        else if (status === 'pipeline') row.pipelineHotels += 1;
        else if (status === 'candidate') row.candidateHotels += 1;

        const lat = Number(hotel.lat);
        const lng = Number(hotel.lng);
        const hasCoords = Number.isFinite(lat) && Number.isFinite(lng) && (lat !== 0 || lng !== 0);
        if (hasCoords) {
            row.withCoordinates += 1;
            if (!Number.isFinite(row.lat) || !Number.isFinite(row.lng)) {
                row.lat = lat;
                row.lng = lng;
            }
        } else {
            row.withoutCoordinates += 1;
        }
    });

    cityAggregationLayer = L.layerGroup();
    Object.values(cityGroups).forEach(function (row) {
        if (!Number.isFinite(row.lat) || !Number.isFinite(row.lng)) return;
        if (!cityRowPassesAggFilter(row, MAP_LAYER_FILTERS.cityAggFilter)) return;
        const marker = L.marker([row.lat, row.lng], {
            icon: L.divIcon({
                className: 'city-aggregate-marker',
                html: '<div style="width:24px;height:24px;border-radius:50%;background:#6c72ff;border:2px solid #fff;box-shadow:0 2px 8px rgba(0,0,0,0.35);display:flex;align-items:center;justify-content:center;color:#fff;font-size:11px;font-weight:700;">' + row.totalHotels + '</div>',
                iconSize: [24, 24],
                iconAnchor: [12, 12]
            })
        });
        marker.bindPopup(
            '<div style="min-width:260px;font-family:Inter,Segoe UI,sans-serif;">' +
                '<h3 style="margin:0 0 8px;color:#1a1a1a;">' + row.city + ', ' + row.country + '</h3>' +
                '<div style="font-size:12px;color:#444;line-height:1.45;">' +
                    '<strong>Total hotels:</strong> ' + row.totalHotels.toLocaleString() + '<br>' +
                    '<strong>Total rooms:</strong> ' + row.totalRooms.toLocaleString() + '<br>' +
                    '<strong>Open / Pipeline / Candidate:</strong> ' + row.openHotels + ' / ' + row.pipelineHotels + ' / ' + row.candidateHotels + '<br>' +
                    '<strong>With coordinates:</strong> ' + row.withCoordinates + '<br>' +
                    '<strong>Without coordinates:</strong> ' + row.withoutCoordinates +
                '</div>' +
            '</div>'
        );
        cityAggregationLayer.addLayer(marker);
    });
    map.addLayer(cityAggregationLayer);
}

function toggleCityAggregation() {
    const toggle = document.getElementById('cityAggregationToggle');
    isCityAggregationEnabled = !!(toggle && toggle.checked);
    if (isCityAggregationEnabled) {
        renderCityAggregationMarkers();
    } else {
        clearCityAggregationLayer();
    }
    syncRadarLayerDrawerControls();
}

function hasValidHotelCoords(hotel) {
    if (!hotel) return false;
    const lat = Number(hotel.lat);
    const lng = Number(hotel.lng);
    return Number.isFinite(lat) && Number.isFinite(lng) && (lat !== 0 || lng !== 0);
}

function haversineKm(lat1, lng1, lat2, lng2) {
    const toRad = Math.PI / 180;
    const dLat = (lat2 - lat1) * toRad;
    const dLng = (lng2 - lng1) * toRad;
    const a =
        Math.sin(dLat / 2) * Math.sin(dLat / 2) +
        Math.cos(lat1 * toRad) * Math.cos(lat2 * toRad) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
    return 6371 * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

/** Only nudge markers that share the exact same geocode (~1m). Dense coastal strips stay on census lat/lng. */
function buildHotelMarkerPositions(hotels) {
    const COORD_PRECISION = 5;
    const RING_KM = 0.035;
    const buckets = new Map();
    const entries = [];

    (hotels || []).forEach(function (hotel) {
        if (!hasValidHotelCoords(hotel)) return;
        const lat = Number(hotel.lat);
        const lng = Number(hotel.lng);
        const key = lat.toFixed(COORD_PRECISION) + '|' + lng.toFixed(COORD_PRECISION);
        if (!buckets.has(key)) buckets.set(key, []);
        buckets.get(key).push({ hotel: hotel, lat: lat, lng: lng });
    });

    buckets.forEach(function (group) {
        if (group.length === 1) {
            entries.push({
                hotel: group[0].hotel,
                lat: group[0].lat,
                lng: group[0].lng,
            });
            return;
        }

        const placed = [];
        group.forEach(function (item) {
            let lat = item.lat;
            let lng = item.lng;
            let collisionIndex = 0;

            while (
                placed.some(function (p) {
                    return haversineKm(lat, lng, p.lat, p.lng) < 0.02;
                })
            ) {
                collisionIndex += 1;
                const angle = (collisionIndex * 137.5 * Math.PI) / 180;
                const ring = Math.ceil(collisionIndex / 8);
                const distKm = RING_KM * ring;
                const latRad = (item.lat * Math.PI) / 180;
                lat = item.lat + (distKm / 111) * Math.cos(angle);
                lng = item.lng + (distKm / (111 * Math.cos(latRad || 0.01))) * Math.sin(angle);
            }

            placed.push({ lat: lat, lng: lng });
            entries.push({
                hotel: item.hotel,
                lat: lat,
                lng: lng,
            });
        });
    });

    return entries;
}

// Create hotel marker
function createHotelMarker(hotel, markerPosition) {
    markerPosition = markerPosition || {};
    const lat = markerPosition.lat != null ? Number(markerPosition.lat) : Number(hotel && hotel.lat);
    const lng = markerPosition.lng != null ? Number(markerPosition.lng) : Number(hotel && hotel.lng);

    if (!hotel || !Number.isFinite(lat) || !Number.isFinite(lng) || (lat === 0 && lng === 0)) {
        return null;
    }
    
    // Use chain scale color if toggle is enabled, otherwise use status color
    let markerColor;
    if (isChainScaleView) {
        const scale = normalizeMapChainScale(hotel);
        markerColor = scale ? getChainScaleColor(scale) : getMarkerColor(hotel.status);
    } else {
        markerColor = getMarkerColor(hotel.status);
    }
    
    const radius = getZoomAdjustedMarkerRadius(hotel.rooms);
    
    // Debug logging for chain scale view
    if (isChainScaleView && Math.random() < 0.01) { // Log 1% of markers to avoid spam
        console.log('Chain Scale Debug:', {
            propertyType: hotel.propertyType,
            color: markerColor,
            hotelName: hotel.name
        });
    }
    
    const marker = L.circleMarker([lat, lng], {
        radius: radius,
        fillColor: markerColor,
        color: '#ffffff',
        weight: 2,
        opacity: 1.0,
        fillOpacity: 0.8
    });
    
    // Add popup + detail panel on click
    if (window.HotelDetailPanel && typeof window.HotelDetailPanel.bindHotelMarker === 'function') {
        window.HotelDetailPanel.bindHotelMarker(marker, hotel, map);
    } else {
        const popupContent = createPopupContent(hotel);
        marker.bindPopup(popupContent);
    }
    
    return marker;
}

// Get marker color based on status
function getMarkerColor(status) {
    const colors = {
        'open': '#2563eb',  // Darker blue for better visibility
        'pipeline': '#dc2626',  // Darker red instead of yellow for better contrast
        'candidate': '#7c3aed',  // Darker purple
        'Open': '#2563eb',  // Darker blue for better visibility
        'Pipeline': '#dc2626',  // Darker red instead of yellow for better contrast
        'Candidate': '#7c3aed'   // Darker purple
    };
    return colors[status] || '#8b5cf6';
}

// Get marker color based on chain scale
function getChainScaleColor(propertyType) {
    // Handle null/undefined values
    if (!propertyType) {
        return '#6b7280'; // Gray for unknown
    }
    
        const colors = {
            'Luxury': '#68B0AB',        // Teal
            'Upper Upscale': '#FF785A', // Coral/Orange
            'Upscale': '#8EF21F',       // Lime Green
            'Upper Midscale': '#8e44ad', // Purple
            'Midscale': '#daa520',      // Goldenrod/Mustard Yellow
            'Economy': '#694A38',       // Dark Brown
            'Extended Stay': '#e74c3c', // Bright Red
            'Select Service': '#1abc9c', // Turquoise
            'Independent': '#34495e',   // Dark Gray
            // Add common variations
            'LUXURY': '#68B0AB',
            'UPPER UPSCALE': '#FF785A',
            'UPSCALE': '#8EF21F',
            'UPPER MIDSCALE': '#8e44ad',
            'MIDSCALE': '#daa520',
            'ECONOMY': '#694A38',
            'EXTENDED STAY': '#e74c3c',
            'SELECT SERVICE': '#1abc9c',
            'INDEPENDENT': '#34495e'
        };
    
    // Try exact match first
    if (colors[propertyType]) {
        return colors[propertyType];
    }
    
    // Try case-insensitive match
    const upperPropertyType = propertyType.toUpperCase();
    if (colors[upperPropertyType]) {
        return colors[upperPropertyType];
    }
    
    // Try partial matches for common variations
        if (propertyType.toLowerCase().includes('luxury')) return '#68B0AB';
        if (propertyType.toLowerCase().includes('upscale')) return '#FF785A';
        if (propertyType.toLowerCase().includes('midscale')) return '#daa520';
        if (propertyType.toLowerCase().includes('economy')) return '#694A38';
        if (propertyType.toLowerCase().includes('independent')) return '#34495e';
    
    // Default to a more visible color for debugging
    return '#ef4444'; // Red for unknown types to make them visible
}

// Get marker radius based on room count
function getMarkerRadius(rooms) {
    return 6; // Slightly larger base size for better visibility
}

// Get marker radius adjusted for zoom level
function getZoomAdjustedMarkerRadius(rooms) {
    const baseRadius = getMarkerRadius(rooms);
    const currentZoom = map.getZoom();
    
    // Slightly larger, clean marker sizes at all zoom levels
    if (currentZoom <= 4) return Math.max(baseRadius * 0.7, 4);
    if (currentZoom <= 6) return Math.max(baseRadius * 0.9, 5);
    if (currentZoom <= 8) return Math.max(baseRadius * 1.0, 6);
    
    return baseRadius;
}

// Add keyboard shortcuts for zoom functionality
function addZoomKeyboardShortcuts() {
    document.addEventListener('keydown', function(e) {
        // Only handle shortcuts when not typing in input fields
        if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') {
            return;
        }
        
        switch(e.key) {
            case '+':
            case '=':
                e.preventDefault();
                map.zoomIn();
                break;
            case '-':
                e.preventDefault();
                map.zoomOut();
                break;
            case 'f':
            case 'F':
                e.preventDefault();
                fitToView();
                break;
            case '0':
                e.preventDefault();
                map.setView([10.0, -80.0], 4); // Reset to initial view
                break;
        }
    });
}

// Fit map to show all visible markers
function fitToView() {
    if (currentFilteredHotels && currentFilteredHotels.length > 0) {
        zoomToResults(currentFilteredHotels);
    } else if (allHotels && allHotels.length > 0) {
        zoomToResults(allHotels);
    } else {
        // Default view if no hotels
        map.setView([10.0, -80.0], 4);
    }
}

// Show helpful map hints to users
function showMapHint(message) {
    // Remove any existing hint
    const existingHint = document.getElementById('map-hint');
    if (existingHint) {
        existingHint.remove();
    }
    
    // Create hint element
    const hint = document.createElement('div');
    hint.id = 'map-hint';
    hint.style.cssText = `
        position: absolute;
        top: 10px;
        left: 50%;
        transform: translateX(-50%);
        background: rgba(0, 0, 0, 0.8);
        color: white;
        padding: 8px 16px;
        border-radius: 20px;
        font-size: 12px;
        z-index: 1000;
        pointer-events: none;
        animation: fadeInOut 3s ease-in-out;
    `;
    hint.textContent = message;
    
    // Add to map container
    const mapContainer = document.getElementById('map');
    mapContainer.appendChild(hint);
    
    // Remove after 3 seconds
    setTimeout(() => {
        if (hint.parentNode) {
            hint.parentNode.removeChild(hint);
        }
    }, 3000);
}

// System Status Indicator Functions
let statusStartTime = null;
let statusProgressInterval = null;

function showSystemStatus(message = 'Processing…', timeEstimate = '') {
    const statusElement = document.getElementById('systemStatus');
    const statusText = statusElement.querySelector('.status-text');
    const statusTime = statusElement.querySelector('.status-time');
    const progressBar = statusElement.querySelector('.status-progress-bar');
    
    // Update content
    statusText.querySelector('div:first-child').textContent = message;
    statusTime.textContent = '';
    statusTime.style.display = 'none';
    statusTime.hidden = true;
    
    // Reset progress
    progressBar.style.width = '0%';
    
    // Show element
    statusElement.style.display = 'block';
    statusStartTime = Date.now();
    
    // Start progress animation
    startProgressAnimation();
    
    // Trigger slide-in animation
    setTimeout(() => {
        statusElement.classList.add('show');
    }, 10);
}

function startProgressAnimation() {
    if (statusProgressInterval) {
        clearInterval(statusProgressInterval);
    }
    
    const progressBar = document.getElementById('systemStatus').querySelector('.status-progress-bar');
    let progress = 0;
    
    statusProgressInterval = setInterval(() => {
        if (statusStartTime) {
            const elapsed = Date.now() - statusStartTime;
            const estimatedTotal = 3000; // 3 seconds default
            progress = Math.min((elapsed / estimatedTotal) * 100, 95); // Cap at 95% until completion
            progressBar.style.width = progress + '%';
        }
    }, 100);
}

function hideSystemStatus() {
    const statusElement = document.getElementById('systemStatus');
    if (!statusElement) return;
    
    const progressBar = statusElement.querySelector('.status-progress-bar');
    if (progressBar) {
        // Complete progress bar
        progressBar.style.width = '100%';
    }
    
    // Clear progress animation
    if (statusProgressInterval) {
        clearInterval(statusProgressInterval);
        statusProgressInterval = null;
    }
    
    // Hide after short delay to show completion
    setTimeout(() => {
        statusElement.classList.remove('show');
        statusStartTime = null;
        
        // Hide after animation completes
        setTimeout(() => {
            statusElement.style.display = 'none';
        }, 300);
    }, 500);
}

function updateSystemStatus(message, timeEstimate = null) {
    const statusElement = document.getElementById('systemStatus');
    const statusText = statusElement.querySelector('.status-text');
    const statusTime = statusElement.querySelector('.status-time');
    
    if (statusElement.style.display !== 'none') {
        statusText.querySelector('div:first-child').textContent = message;
        statusTime.textContent = '';
        statusTime.style.display = 'none';
        statusTime.hidden = true;
    }
}

// Create popup content
function createPopupContent(hotel) {
    const statusIcon = {
        'Open': '',
        'Pipeline': '',
        'Candidate': '',
        'open': '',
        'pipeline': '',
        'candidate': ''
    };
    
    const statusText = statusIcon[hotel.status] || '';
    
    return `
        <div style="min-width: 300px; font-family: 'Inter', sans-serif; background: #1e293b; color: #ffffff; border: 2px solid #ffffff; border-radius: 8px; padding: 15px;">
            <h3 style="margin: 0 0 10px 0; color: #ffffff; font-size: 16px; border-bottom: 2px solid #475569; padding-bottom: 5px;">
                ${hotel.name}
            </h3>
            <div style="background: #334155; padding: 10px; border-radius: 6px; margin-bottom: 10px;">
                <div style="font-size: 18px; font-weight: bold; color: #60a5fa;">
                    ${statusText} ${hotel.status}
                </div>
                <div style="font-size: 12px; color: #cbd5e1; text-transform: uppercase;">Hotel Status</div>
            </div>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 10px;">
                <div style="font-size: 12px; color: #cbd5e1;">
                    <strong>BRAND:</strong><br>
                    <span style="color: #ffffff;">${hotel.brand}</span>
                </div>
                <div style="font-size: 12px; color: #cbd5e1;">
                    <strong>PARENT COMPANY:</strong><br>
                    <span style="color: #ffffff;">${formatParentCompanyLabel(hotel.parentCompany)}</span>
                </div>
                <div style="font-size: 12px; color: #cbd5e1;">
                    <strong>CHAIN SCALE:</strong><br>
                    <span style="color: #ffffff;">${hotel.propertyType || 'Unknown'}</span>
                </div>
                <div style="font-size: 12px; color: #cbd5e1;">
                    <strong>ROOMS:</strong><br>
                    <span style="color: #ffffff;">${hotel.rooms ? hotel.rooms.toLocaleString() : 'Unknown'}</span>
                </div>
                <div style="font-size: 12px; color: #cbd5e1;">
                    <strong>OPERATION TYPE:</strong><br>
                    <span style="color: #ffffff;">${hotel.operationType || '—'}</span>
                </div>
                <div style="font-size: 12px; color: #cbd5e1;">
                    <strong>MANAGEMENT COMPANY:</strong><br>
                    <span style="color: #ffffff;">${hotel.managementCompany || '—'}</span>
                </div>
            </div>
            <div style="font-size: 12px; color: #cbd5e1;">
                <strong>Location:</strong> ${hotel.city}, ${hotel.country}<br>
                <strong>Coordinates:</strong> ${hotel.lat.toFixed(4)}, ${hotel.lng.toFixed(4)}
            </div>
        </div>
    `;
}

// Update statistics
function updateStatistics(hotels) {
    const totalHotels = hotels.length;
    const openHotels = hotels.filter(h => h.status === 'Open').length;
    const pipelineHotels = hotels.filter(h => h.status === 'Pipeline').length;
    const candidateHotels = hotels.filter(h => h.status === 'Candidate').length;
    
    // Calculate room counts
    const totalRooms = hotels.reduce((sum, h) => sum + (h.rooms || 0), 0);
    const openRooms = hotels.filter(h => h.status === 'Open').reduce((sum, h) => sum + (h.rooms || 0), 0);
    const pipelineRooms = hotels.filter(h => h.status === 'Pipeline').reduce((sum, h) => sum + (h.rooms || 0), 0);
    
    // Calculate percentages
    const openRatio = totalHotels > 0 ? Math.round((openHotels / totalHotels) * 100) : 0;
    const pipelineRatio = totalHotels > 0 ? Math.round((pipelineHotels / totalHotels) * 100) : 0;
    const candidateRatio = totalHotels > 0 ? Math.round((candidateHotels / totalHotels) * 100) : 0;
    
    // Update DOM elements with null checks
    const totalHotelsEl = document.getElementById('totalHotels');
    if (totalHotelsEl) totalHotelsEl.textContent = totalHotels.toLocaleString();
    
    const openHotelsEl = document.getElementById('openHotels');
    if (openHotelsEl) openHotelsEl.textContent = openHotels.toLocaleString();
    
    const pipelineHotelsEl = document.getElementById('pipelineHotels');
    if (pipelineHotelsEl) pipelineHotelsEl.textContent = pipelineHotels.toLocaleString();
    
    const candidateHotelsEl = document.getElementById('candidateHotels');
    if (candidateHotelsEl) candidateHotelsEl.textContent = candidateHotels.toLocaleString();
    
    const totalRoomsEl = document.getElementById('totalRooms');
    if (totalRoomsEl) totalRoomsEl.textContent = totalRooms.toLocaleString();
    
    const openRoomsEl = document.getElementById('openRooms');
    if (openRoomsEl) openRoomsEl.textContent = openRooms.toLocaleString();
    
    const pipelineRoomsEl = document.getElementById('pipelineRooms');
    if (pipelineRoomsEl) pipelineRoomsEl.textContent = pipelineRooms.toLocaleString();
    
    const openRatioEl = document.getElementById('openRatio');
    if (openRatioEl) openRatioEl.textContent = openRatio + '%';
    
    const pipelineRatioEl = document.getElementById('pipelineRatio');
    if (pipelineRatioEl) pipelineRatioEl.textContent = pipelineRatio + '%';
    
    const candidateRatioEl = document.getElementById('candidateRatio');
    if (candidateRatioEl) candidateRatioEl.textContent = candidateRatio + '%';
    
    // Enhanced statistics
    const enhancedStats = calculateEnhancedStatistics(hotels, totalHotels, totalRooms);
    
    // Update enhanced statistics with null checks
    const chainAffiliatedEl = document.getElementById('chainAffiliated');
    if (chainAffiliatedEl) chainAffiliatedEl.textContent = enhancedStats.chainAffiliated.toLocaleString();
    
    const independentHotelsEl = document.getElementById('independentHotels');
    if (independentHotelsEl) independentHotelsEl.textContent = enhancedStats.independentHotels.toLocaleString();
    
    const chainAffiliatedPctEl = document.getElementById('chainAffiliatedPct');
    if (chainAffiliatedPctEl) chainAffiliatedPctEl.textContent = enhancedStats.chainAffiliatedPct + '%';
    
    const independentPctEl = document.getElementById('independentPct');
    if (independentPctEl) independentPctEl.textContent = enhancedStats.independentPct + '%';
    
    // Update average rooms and country count
    const avgRoomsEl = document.getElementById('avgRoomsPerHotel');
    if (avgRoomsEl) avgRoomsEl.textContent = enhancedStats.avgRoomsPerHotel.toLocaleString();
    
    const countriesEl = document.getElementById('totalCountries');
    if (countriesEl) countriesEl.textContent = enhancedStats.uniqueCountries.toLocaleString();
}

// Calculate enhanced statistics
function calculateEnhancedStatistics(hotels, totalHotels, totalRooms) {
    // Chain vs Independent analysis
    const chainAffiliated = hotels.filter(h => h.brand && h.brand !== 'Independent' && h.brand !== 'Unknown').length;
    const independentHotels = hotels.filter(h => h.brand === 'Independent' || h.brand === 'Unknown').length;
    
    const chainAffiliatedPct = totalHotels > 0 ? Math.round((chainAffiliated / totalHotels) * 100) : 0;
    const independentPct = totalHotels > 0 ? Math.round((independentHotels / totalHotels) * 100) : 0;
    
    // Average rooms per hotel
    const avgRoomsPerHotel = totalHotels > 0 ? Math.round(totalRooms / totalHotels) : 0;
    
    // Country count
    const uniqueCountries = [...new Set(hotels.map(h => h.country).filter(Boolean))].length;
    
    return {
        chainAffiliated,
        independentHotels,
        chainAffiliatedPct,
        independentPct,
        avgRoomsPerHotel,
        uniqueCountries
    };
}

// Update brand distribution
function updateBrandDistribution(hotels) {
    // Filter to only Open and Pipeline hotels as indicated by the header
    const openAndPipelineHotels = hotels.filter(hotel => 
        hotel.status === 'Open' || hotel.status === 'Pipeline'
    );
    
    const brandCounts = {};
    openAndPipelineHotels.forEach(hotel => {
        const brand = hotel.brand || 'Unknown';
        // Always exclude Independent brands
        if (brand.toLowerCase() !== 'independent') {
            brandCounts[brand] = (brandCounts[brand] || 0) + 1;
        }
    });
    
    // Sort brands by count
    const sortedBrands = Object.entries(brandCounts)
        .sort(([,a], [,b]) => b - a)
        .slice(0, 10); // Top 10 brands
    
    // Update brand distribution display
    const brandChips = document.getElementById('brandChips');
    if (brandChips) {
        brandChips.innerHTML = sortedBrands.map(([brand, count]) => 
            `<div class="brand-item">
                <span class="brand-name">${brand}</span>
                <span class="brand-count">${count.toLocaleString()}</span>
            </div>`
        ).join('');
    } else {
        console.error('brandChips element not found');
    }
}

// Generate insights
function generateInsights(hotels) {
    const insights = [];
    
    if (hotels.length === 0) {
        insights.push({
            priority: 'low',
            title: 'No Data Available',
            description: 'No hotels found matching the current filters.'
        });
        displayInsights(insights);
        return;
    }
    
    // Market Leader insight
    const brandCounts = {};
    hotels.forEach(hotel => {
        const brand = hotel.brand || 'Unknown';
        brandCounts[brand] = (brandCounts[brand] || 0) + 1;
    });
    
    const sortedBrands = Object.entries(brandCounts).sort(([,a], [,b]) => b - a);
    const topBrand = sortedBrands[0];
    
    if (topBrand && topBrand[0] !== 'Unknown' && topBrand[0] !== 'Independent') {
        insights.push({
            priority: 'high',
            title: 'Market Leader',
            description: `${topBrand[0]} leads with ${topBrand[1].toLocaleString()} hotels in the current view.`
        });
    }
    
    // Development Activity insight - focus on pipeline units by parent company (excluding Unknown and Independent)
    const developmentPipelineHotels = hotels.filter(h => 
        h.status === 'Pipeline' && 
        h.parentCompany && 
        h.parentCompany !== 'Unknown' && 
        h.parentCompany !== 'Independent'
    );
    
    if (developmentPipelineHotels.length > 0) {
        // Find country with most pipeline activity
        const pipelineCountries = {};
        developmentPipelineHotels.forEach(hotel => {
            const country = hotel.country;
            if (country) {
                pipelineCountries[country] = (pipelineCountries[country] || 0) + 1;
            }
        });
        
        const sortedPipelineCountries = Object.entries(pipelineCountries).sort(([,a], [,b]) => b - a);
        const topPipelineCountry = sortedPipelineCountries[0];
        
        // Find parent company leading pipeline development in the top country
        const topCountryPipelineHotels = developmentPipelineHotels.filter(h => h.country === topPipelineCountry[0]);
        const topCountryParentCompanies = {};
        topCountryPipelineHotels.forEach(hotel => {
            const parentCompany = hotel.parentCompany || 'Unknown';
            if (parentCompany !== 'Unknown' && parentCompany !== 'Independent') {
                topCountryParentCompanies[parentCompany] = (topCountryParentCompanies[parentCompany] || 0) + 1;
            }
        });
        
        const sortedTopCountryParentCompanies = Object.entries(topCountryParentCompanies).sort(([,a], [,b]) => b - a);
        const topParentCompanyInTopCountry = sortedTopCountryParentCompanies[0];
        
        // Find city with most pipeline activity
        const pipelineCities = {};
        developmentPipelineHotels.forEach(hotel => {
            const city = hotel.city;
            if (city) {
                pipelineCities[city] = (pipelineCities[city] || 0) + 1;
            }
        });
        
        const sortedPipelineCities = Object.entries(pipelineCities).sort(([,a], [,b]) => b - a);
        const topPipelineCity = sortedPipelineCities[0];
        
        insights.push({
            priority: 'high',
            title: 'Development Activity',
            description: `${topPipelineCountry[0]} leads pipeline development with ${topPipelineCountry[1].toLocaleString()} hotels in development. ${topParentCompanyInTopCountry ? topParentCompanyInTopCountry[0] + ' leads in ' + topPipelineCountry[0] + ' with ' + topParentCompanyInTopCountry[1].toLocaleString() + ' pipeline hotels' : ''}. ${topPipelineCity[0]} has the highest pipeline activity with ${topPipelineCity[1].toLocaleString()} hotels.`
        });
    }
    
    // White Space Opportunities insight - show country with most white space cities
    const cities = {};
    hotels.forEach(hotel => {
        const city = hotel.city;
        if (city) {
            cities[city] = (cities[city] || 0) + 1;
        }
    });
    
    const lowCompetitionCities = Object.entries(cities)
        .filter(([,count]) => count <= 2)
        .sort(([,a], [,b]) => a - b);
    
    if (lowCompetitionCities.length > 0) {
        // Group white space cities by country
        const whiteSpaceCountries = {};
        lowCompetitionCities.forEach(([city, count]) => {
            const hotel = hotels.find(h => h.city === city);
            if (hotel && hotel.country) {
                whiteSpaceCountries[hotel.country] = (whiteSpaceCountries[hotel.country] || 0) + 1;
            }
        });
        
        const sortedWhiteSpaceCountries = Object.entries(whiteSpaceCountries).sort(([,a], [,b]) => b - a);
        const topWhiteSpaceCountry = sortedWhiteSpaceCountries[0];
        
        insights.push({
            priority: 'medium',
            title: 'White Space Opportunities',
            description: `${topWhiteSpaceCountry[0]} has the most white space cities with ${topWhiteSpaceCountry[1]} cities having limited competition. Total: ${lowCompetitionCities.length} cities.`
        });
    }
    
    // Regional Concentration insight - show which country has the highest pipeline
    const pipelineHotels = hotels.filter(h => h.status === 'Pipeline');
    
    if (pipelineHotels.length > 0) {
        // Find top country for pipeline
        const pipelineCountries = {};
        pipelineHotels.forEach(hotel => {
            const country = hotel.country;
            if (country) {
                pipelineCountries[country] = (pipelineCountries[country] || 0) + 1;
            }
        });
        
        const sortedPipelineCountries = Object.entries(pipelineCountries).sort(([,a], [,b]) => b - a);
        const topPipelineCountry = sortedPipelineCountries[0];
        
        if (topPipelineCountry) {
            const countryPercentage = Math.round((topPipelineCountry[1] / pipelineHotels.length) * 100);
            
            insights.push({
                priority: 'medium',
                title: 'Regional Concentration',
                description: `${topPipelineCountry[0]} leads pipeline development with ${topPipelineCountry[1].toLocaleString()} hotels in development (${countryPercentage}% of total pipeline).`
            });
        }
    }
    
    // Large-Scale Development insight - show brand with most 200+ room hotels (excluding Independent)
    const largeHotels = hotels.filter(h => h.rooms && h.rooms >= 200);
    if (largeHotels.length > 0) {
        const largeHotelBrands = {};
        largeHotels.forEach(hotel => {
            const brand = hotel.brand || 'Unknown';
            // Exclude Independent brands
            if (brand.toLowerCase() !== 'independent') {
                largeHotelBrands[brand] = (largeHotelBrands[brand] || 0) + 1;
            }
        });
        
        const sortedLargeBrands = Object.entries(largeHotelBrands).sort(([,a], [,b]) => b - a);
        const topLargeBrand = sortedLargeBrands[0];
        
        // Fix percentage calculation - use total large hotels, not total hotels
        const largeHotelPercentage = Math.round((topLargeBrand[1] / largeHotels.length) * 100);
        insights.push({
            priority: 'low',
            title: 'Large-Scale Development',
            description: `${topLargeBrand[0]} leads with ${topLargeBrand[1].toLocaleString()} hotels of 200+ rooms. Total: ${largeHotels.length.toLocaleString()} hotels (${largeHotelPercentage}%).`
        });
    }
    
    // Boutique Market Focus insight - show brand with most under 100 room hotels (excluding Independent)
    const boutiqueHotels = hotels.filter(h => h.rooms && h.rooms < 100);
    if (boutiqueHotels.length > 0) {
        const boutiqueBrands = {};
        boutiqueHotels.forEach(hotel => {
            const brand = hotel.brand || 'Unknown';
            // Exclude Independent brands
            if (brand.toLowerCase() !== 'independent') {
                boutiqueBrands[brand] = (boutiqueBrands[brand] || 0) + 1;
            }
        });
        
        const sortedBoutiqueBrands = Object.entries(boutiqueBrands).sort(([,a], [,b]) => b - a);
        const topBoutiqueBrand = sortedBoutiqueBrands[0];
        
        // Fix percentage calculation - use total boutique hotels, not total hotels
        const boutiquePercentage = Math.round((topBoutiqueBrand[1] / boutiqueHotels.length) * 100);
        insights.push({
            priority: 'low',
            title: 'Boutique Market Focus',
            description: `${topBoutiqueBrand[0]} leads with ${topBoutiqueBrand[1].toLocaleString()} hotels under 100 rooms. Total: ${boutiqueHotels.length.toLocaleString()} hotels (${boutiquePercentage}%).`
        });
    }
    
    displayInsights(insights);
}

// Display insights
function displayInsights(insights) {
    const insightsList = document.getElementById('insightsList');
    if (!insightsList) return;
    
    insightsList.innerHTML = insights.map(insight => `
        <li class="insight-item ${insight.priority}">
            <div class="insight-icon">
                <svg><use href="#${getInsightIcon(insight.title)}"></use></svg>
            </div>
            <div class="insight-content">
                <div class="insight-title">${insight.title}</div>
                <div class="insight-description">${insight.description}</div>
            </div>
        </li>
    `).join('');
}

// Get icon for insight type
function getInsightIcon(title) {
    const iconMap = {
        'Market Leader': 'crown-icon',
        'Development Activity': 'trending-up-icon',
        'White Space Opportunities': 'target-icon',
        'Regional Concentration': 'map-icon',
        'Large-Scale Development': 'building-icon',
        'Boutique Market Focus': 'home-icon'
    };
    return iconMap[title] || 'lightbulb-icon';
}

// Apply filters - immediate Excel pivot table behavior
async function applyFilters() {
    // Skip if we're in the middle of resetting
    if (isResetting) {
        return;
    }
    
    
    // Fast path: if no filters are active, show all data immediately
    const roomBounds = getActiveRoomFilterBounds();
    const hasActiveFilters = currentFilters.parentCompany || currentFilters.brand || 
                           currentFilters.status || (currentFilters.statuses && currentFilters.statuses.length) ||
                           currentFilters.propertyType || 
                           currentFilters.region || currentFilters.country ||
                           currentFilters.market || currentFilters.submarket ||
                           currentFilters.locationType || currentFilters.hotelType ||
                           currentFilters.hotelServiceModel ||
                           currentFilters.operationType || currentFilters.managementCompany ||
                           roomBounds.minRooms != null || roomBounds.maxRooms != null ||
                           currentFilters.search;
    
    if (!hasActiveFilters) {
        // Show all data immediately without processing
        currentFilteredHotels = [...allHotels];
        updateAllDropdowns(allHotels);
        refreshTravelInfrastructureIfVisible();
        refreshDemandAnchorsIfVisible();
        refreshMapLayersAfterFilterChange();
        requestAnimationFrame(function () {
            updateStatistics(allHotels);
            updateBrandDistribution(allHotels);
            generateInsights(allHotels);
        });
        if (window.RadarFilterDrawer && typeof window.RadarFilterDrawer.updateBadge === 'function') {
            window.RadarFilterDrawer.updateBadge();
        }
        return;
    }
    
    // Apply filters immediately - no debouncing for Excel-like behavior
    let filteredHotels = [...allHotels];
    
    // Apply search filter
    if (currentFilters.search) {
        const searchTerm = currentFilters.search.toLowerCase();
        filteredHotels = filteredHotels.filter(hotel => 
            hotel.name.toLowerCase().includes(searchTerm) ||
            hotel.city.toLowerCase().includes(searchTerm) ||
            hotel.country.toLowerCase().includes(searchTerm) ||
            hotel.brand.toLowerCase().includes(searchTerm)
        );
    }
    
    // Apply parent company filter
    if (currentFilters.parentCompany) {
        filteredHotels = filteredHotels.filter(hotel => 
            hotel.parentCompany === currentFilters.parentCompany
        );
    }
    
    // Apply brand filter
    if (currentFilters.brand) {
        filteredHotels = filteredHotels.filter(hotel => 
            hotel.brand === currentFilters.brand
        );
    }
    
    // Apply status filter
    if (currentFilters.statuses && currentFilters.statuses.length) {
        filteredHotels = filteredHotels.filter(hotel =>
            currentFilters.statuses.includes(hotel.status)
        );
    } else if (currentFilters.status) {
        filteredHotels = filteredHotels.filter(hotel =>
            hotel.status === currentFilters.status
        );
    }
    
    // Apply chain scale filter
    if (currentFilters.propertyType) {
        filteredHotels = filteredHotels.filter(hotel => 
            hotel.propertyType === currentFilters.propertyType
        );
    }
    
    // Apply region filter
    if (currentFilters.region) {
        filteredHotels = filteredHotels.filter(hotel =>
            matchesGeoField(hotel.region, currentFilters.region)
        );
    }

    // Apply country filter
    if (currentFilters.country) {
        filteredHotels = filteredHotels.filter(hotel =>
            matchesGeoField(hotel.country, currentFilters.country)
        );
    }

    // Apply market filter
    if (currentFilters.market) {
        filteredHotels = filteredHotels.filter(hotel =>
            matchesGeoField(hotel.market, currentFilters.market)
        );
    }

    // Submarket only applies within a selected market (avoids cross-market mis-tags)
    if (currentFilters.submarket && currentFilters.market) {
        filteredHotels = filteredHotels.filter(hotel =>
            matchesGeoField(hotel.submarket, currentFilters.submarket)
        );
    }
    
    // Apply location type filter
    if (currentFilters.locationType) {
        filteredHotels = filteredHotels.filter(hotel => 
            hotel.locationType === currentFilters.locationType
        );
    }

    // Apply hotel type filter (census Property Type)
    if (currentFilters.hotelType) {
        filteredHotels = filteredHotels.filter(hotel =>
            hotel.censusPropertyType === currentFilters.hotelType
        );
    }

    // Apply hotel service model filter
    if (currentFilters.hotelServiceModel) {
        filteredHotels = filteredHotels.filter(hotel =>
            hotel.hotelServiceModel === currentFilters.hotelServiceModel
        );
    }

    // Apply operation type filter
    if (currentFilters.operationType) {
        filteredHotels = filteredHotels.filter(hotel =>
            hotel.operationType === currentFilters.operationType
        );
    }

    // Apply management company filter
    if (currentFilters.managementCompany) {
        filteredHotels = filteredHotels.filter(hotel =>
            hotel.managementCompany === currentFilters.managementCompany
        );
    }

    // Apply room count filter (min / max keys)
    if (roomBounds.minRooms != null || roomBounds.maxRooms != null) {
        filteredHotels = filteredHotels.filter(hotel =>
            hotelMatchesRoomFilter(hotel, roomBounds.minRooms, roomBounds.maxRooms)
        );
    }
    
    
    // Store current filtered hotels for overlay generation
    currentFilteredHotels = filteredHotels;
    
    // Update all dropdowns with filtered data (Excel pivot-like behavior)
    // During reset, use full dataset to show all available options
    if (isResetting) {
        updateAllDropdowns(hotelData);
    } else {
        updateAllDropdowns(filteredHotels);
    }
    
    // Update display with error handling
    try {
        await displayHotels(filteredHotels);
        requestAnimationFrame(function () {
            updateStatistics(filteredHotels);
            updateBrandDistribution(filteredHotels);
            generateInsights(filteredHotels);
        });
    } catch (error) {
        console.error('Error updating display:', error);
        // Fallback: show basic statistics
        requestAnimationFrame(function () {
            updateStatistics(filteredHotels);
        });
    }
    
    
    // Auto-zoom to show filtered results
    if (filteredHotels.length > 0) {
        // Special handling for specific city searches - ensure markers are visible
        const searchTerm = currentFilters.search ? currentFilters.search.toLowerCase() : '';
        if (searchTerm.includes('santo domingo') || searchTerm.includes('santo dom')) {
            // First try to show actual markers, then fallback to city center
            const santoDomingoHotels = filteredHotels.filter(hotel => 
                hotel.city && hotel.city.toLowerCase().includes('santo domingo')
            );
            if (santoDomingoHotels.length > 0) {
                zoomToResults(santoDomingoHotels);
            } else {
            map.setView([18.4861, -69.9312], 10);
            }
        } else {
            zoomToResults(filteredHotels);
        }
    } else if (filteredHotels.length === 0 && hasActiveFilters()) {
        // No results found - check if it's a search for a known city
        if (currentFilters.search) {
            const searchTerm = currentFilters.search.toLowerCase();
            if (searchTerm.includes('santo domingo')) {
                // Zoom to Santo Domingo, Dominican Republic
                map.setView([18.4861, -69.9312], 10);
                showNoResultsMessage('No hotels found in Santo Domingo. Showing city location.');
            } else if (searchTerm.includes('puerto plata')) {
                // Zoom to Puerto Plata, Dominican Republic  
                map.setView([19.7808, -70.6871], 10);
                showNoResultsMessage('No hotels found in Puerto Plata. Showing city location.');
            } else if (searchTerm.includes('mexico city')) {
                // Zoom to Mexico City, Mexico
                map.setView([19.4326, -99.1332], 10);
                showNoResultsMessage('No hotels found in Mexico City. Showing city location.');
            } else if (searchTerm.includes('bogota') || searchTerm.includes('bogotÃ¡')) {
                // Zoom to BogotÃ¡, Colombia
                map.setView([4.7110, -74.0721], 10);
                showNoResultsMessage('No hotels found in BogotÃ¡. Showing city location.');
            } else if (searchTerm.includes('lima')) {
                // Zoom to Lima, Peru
                map.setView([-12.0464, -77.0428], 10);
                showNoResultsMessage('No hotels found in Lima. Showing city location.');
            } else if (searchTerm.includes('santiago')) {
                // Zoom to Santiago, Chile
                map.setView([-33.4489, -70.6693], 10);
                showNoResultsMessage('No hotels found in Santiago. Showing city location.');
            } else if (searchTerm.includes('buenos aires')) {
                // Zoom to Buenos Aires, Argentina
                map.setView([-34.6118, -58.3960], 10);
                showNoResultsMessage('No hotels found in Buenos Aires. Showing city location.');
            } else if (searchTerm.includes('rio de janeiro')) {
                // Zoom to Rio de Janeiro, Brazil
                map.setView([-22.9068, -43.1729], 10);
                showNoResultsMessage('No hotels found in Rio de Janeiro. Showing city location.');
            } else if (searchTerm.includes('sao paulo')) {
                // Zoom to SÃ£o Paulo, Brazil
                map.setView([-23.5505, -46.6333], 10);
                showNoResultsMessage('No hotels found in SÃ£o Paulo. Showing city location.');
            } else {
                // Generic no results message
                showNoResultsMessage('No hotels found matching your search criteria.');
            }
        } else {
            // No results for other filters
            showNoResultsMessage('No hotels found matching your filter criteria.');
        }
    } else {
        // Show all hotels - zoom to fit all
        zoomToResults(hotelData);
    }

    refreshTravelInfrastructureIfVisible();
    refreshDemandAnchorsIfVisible();
    refreshMapLayersAfterFilterChange({ skipHotelMarkers: true });

    if (window.RadarFilterDrawer && typeof window.RadarFilterDrawer.updateBadge === 'function') {
        window.RadarFilterDrawer.updateBadge();
    }
}

// Check if any filters are active
function hasActiveFilters() {
    const roomBounds = getActiveRoomFilterBounds();
    return currentFilters.parentCompany ||
           currentFilters.brand ||
           currentFilters.status ||
           (currentFilters.statuses && currentFilters.statuses.length) ||
           currentFilters.propertyType ||
           currentFilters.region ||
           currentFilters.country ||
           currentFilters.market ||
           currentFilters.submarket ||
           currentFilters.locationType ||
           currentFilters.hotelType ||
           currentFilters.hotelServiceModel ||
           currentFilters.operationType ||
           currentFilters.managementCompany ||
           roomBounds.minRooms != null ||
           roomBounds.maxRooms != null ||
           currentFilters.search;
}

// Show no results message
function showNoResultsMessage(message) {
    // Remove existing message if any
    const existingMessage = document.querySelector('.no-results-message');
    if (existingMessage) {
        existingMessage.remove();
    }
    
    // Create new message
    const messageDiv = document.createElement('div');
    messageDiv.className = 'no-results-message';
    messageDiv.style.cssText = `
        position: fixed;
        top: 50%;
        left: 50%;
        transform: translate(-50%, -50%);
        background: rgba(0, 0, 0, 0.8);
        color: white;
        padding: 20px;
        border-radius: 8px;
        z-index: 10000;
        font-family: 'Inter', sans-serif;
        text-align: center;
        max-width: 400px;
    `;
    messageDiv.innerHTML = `
        <div style="font-size: 18px; font-weight: 600; margin-bottom: 10px;">No Results Found</div>
        <div>${message}</div>
    `;
    
    document.body.appendChild(messageDiv);
    
    // Auto-remove after 5 seconds
    setTimeout(() => {
        if (messageDiv.parentNode) {
            messageDiv.remove();
        }
    }, 5000);
}

// Zoom to show filtered results with improved visibility
function zoomToResults(hotels) {
    if (hotels.length === 0) return;
    
    const group = new L.featureGroup();
    const validHotels = hotels.filter(hotel => hotel.lat && hotel.lng);
    
    if (validHotels.length === 0) return;
    
    // Add markers to group
    validHotels.forEach(hotel => {
            group.addLayer(L.marker([hotel.lat, hotel.lng]));
    });
    
    if (group.getLayers().length > 0) {
        const bounds = group.getBounds();
        
        // Calculate appropriate zoom level based on number of markers and their spread
        const markerCount = validHotels.length;
        const boundsSize = bounds.getNorthEast().distanceTo(bounds.getSouthWest());
        
        let targetZoom;
        
        if (markerCount === 1) {
            // Single marker - zoom in close
            targetZoom = 12;
        } else if (markerCount <= 5) {
            // Few markers - zoom in moderately
            targetZoom = 10;
        } else if (markerCount <= 20) {
            // Medium number of markers - balanced zoom
            targetZoom = 8;
        } else if (boundsSize < 100000) { // Less than ~100km spread
            // Many markers in small area - zoom in
            targetZoom = 9;
        } else if (boundsSize < 500000) { // Less than ~500km spread
            // Many markers in medium area - moderate zoom
            targetZoom = 7;
        } else {
            // Many markers spread wide - zoom out
            targetZoom = 6;
        }
        
        // Ensure zoom is within map limits
        targetZoom = Math.max(2, Math.min(16, targetZoom));
        
        // Get center of bounds
        const center = bounds.getCenter();
        
        // Offset the center southward to position results higher on screen
        // This makes search results more visible without scrolling
        const offsetLat = -0.1; // Negative offset to move map south, making markers appear higher
        const adjustedCenter = [center.lat + offsetLat, center.lng];
        
        // Set view with calculated zoom and adjusted center
        map.setView(adjustedCenter, targetZoom);
        refreshMapSize({ pan: false, delay: 100 });
        
        // If markers are still not clearly visible, adjust zoom
        setTimeout(() => {
            const currentZoom = map.getZoom();
            const visibleBounds = map.getBounds();
            
            // Check if any markers are outside visible area
            const markersOutsideView = validHotels.filter(hotel => 
                !visibleBounds.contains([hotel.lat, hotel.lng])
            );
            
            // If more than 20% of markers are outside view, zoom out slightly
            if (markersOutsideView.length > validHotels.length * 0.2) {
                map.setZoom(Math.max(2, currentZoom - 1));
            }
            
            // Show a helpful message if markers are clustered or hard to see
            if (markerCount > 50 && currentZoom < 8) {
                showMapHint('Many markers found. Use zoom controls or scroll to explore the area.');
            } else if (markerCount === 1) {
                showMapHint('1 marker found. Use zoom controls to get a closer view.');
            } else if (markerCount > 1 && markerCount <= 10) {
                showMapHint(`${markerCount} markers found in this area.`);
            }
        }, 100);
    }
}

function syncTerritoryParentCompanyFilterOptions() {
    const src = document.getElementById('parentCompanyFilter');
    const tgt = document.getElementById('territoryParentCompanyFilter');
    if (!src || !tgt) return;

    const current = tgt.value || src.value || '';
    tgt.innerHTML = src.innerHTML;
    if (tgt.options.length && tgt.options[0].value === '') {
        tgt.options[0].textContent = 'All Parent Companies';
    }
    if (current && Array.prototype.some.call(tgt.options, function (opt) { return opt.value === current; })) {
        tgt.value = current;
    } else {
        tgt.value = src.value || '';
    }
}

function syncTerritoryBrandFilterOptions() {
    const src = document.getElementById('brandFilter');
    const tgt = document.getElementById('territoryBrandFilter');
    if (!src || !tgt) return;

    const current = tgt.value || src.value || '';
    tgt.innerHTML = src.innerHTML;
    if (tgt.options.length && tgt.options[0].value === '') {
        tgt.options[0].textContent = 'Select a Brand…';
    }
    if (current && Array.prototype.some.call(tgt.options, function (opt) { return opt.value === current; })) {
        tgt.value = current;
    } else {
        tgt.value = src.value || '';
    }
}

function syncTerritoryFilterOptions() {
    syncTerritoryParentCompanyFilterOptions();
    syncTerritoryBrandFilterOptions();
}

window.syncTerritoryParentCompanyFilterOptions = syncTerritoryParentCompanyFilterOptions;
window.syncTerritoryBrandFilterOptions = syncTerritoryBrandFilterOptions;
window.syncTerritoryFilterOptions = syncTerritoryFilterOptions;

function setTerritoryParentCompanyFilterValue(parentValue) {
    const territoryParent = document.getElementById('territoryParentCompanyFilter');
    const parentFilter = document.getElementById('parentCompanyFilter');
    const parentDrawer = document.getElementById('parentCompanyFilterDrawer');
    const value = String(parentValue || '');

    if (territoryParent && territoryParent.value !== value) {
        territoryParent.value = value;
    }
    if (parentFilter && parentFilter.value !== value) {
        parentFilter.value = value;
    }
    if (parentDrawer && parentDrawer.value !== value) {
        parentDrawer.value = value;
    }
    currentFilters.parentCompany = value;
}

window.setTerritoryParentCompanyFilterValue = setTerritoryParentCompanyFilterValue;

function setTerritoryBrandFilterValue(brandValue) {
    const territoryBrand = document.getElementById('territoryBrandFilter');
    const brandFilter = document.getElementById('brandFilter');
    const brandDrawer = document.getElementById('brandFilterDrawer');
    const value = String(brandValue || '');

    if (territoryBrand && territoryBrand.value !== value) {
        territoryBrand.value = value;
    }
    if (brandFilter && brandFilter.value !== value) {
        brandFilter.value = value;
    }
    if (brandDrawer && brandDrawer.value !== value) {
        brandDrawer.value = value;
    }
    currentFilters.brand = value;
}

window.setTerritoryBrandFilterValue = setTerritoryBrandFilterValue;

// Dynamic dropdown population based on filtered data - Excel pivot table behavior
function updateAllDropdowns(filteredData) {
    // Fast path: if showing all data, use allHotels for dropdown options (faster)
    const dataSource = (filteredData.length === allHotels.length) ? allHotels : filteredData;
    
    // Calculate unique values from data source
    const uniqueParentCompanies = [...new Set(dataSource.map(hotel => hotel.parentCompany).filter(Boolean))].sort();
    const uniqueBrands = [...new Set(dataSource.map(hotel => hotel.brand).filter(Boolean))].sort();
    const uniqueStatuses = [...new Set(dataSource.map(hotel => hotel.status).filter(Boolean))].sort();
    const CHAIN_SCALE_ORDER = ['Luxury', 'Upper Upscale', 'Upscale', 'Upper Midscale', 'Midscale', 'Economy', 'Independant', 'Independent'];
    const sortChainScale = (a, b) => {
      const key = (v) => (v || '').toString().trim().replace(/\\s+Chain\\s*$/i, '') || (v || '').toString().trim();
      const idx = (v) => {
        const k = key(v).toLowerCase();
        const i = CHAIN_SCALE_ORDER.findIndex(o => k === o.toLowerCase() || k.startsWith(o.toLowerCase() + ' '));
        return i >= 0 ? i : CHAIN_SCALE_ORDER.length;
      };
      const ia = idx(a), ib = idx(b);
      return ia !== ib ? ia - ib : (a || '').localeCompare(b || '');
    };
    const uniquePropertyTypes = [...new Set(dataSource.map(hotel => hotel.propertyType).filter(Boolean))].sort(sortChainScale);
    const uniqueRegions = [...new Set(dataSource.map(hotel => hotel.region).filter(Boolean))].sort();
    const countryScope = currentFilters.region
        ? dataSource.filter(function (hotel) { return matchesGeoField(hotel.region, currentFilters.region); })
        : dataSource;
    const uniqueCountries = [...new Set(countryScope.map(hotel => hotel.country).filter(Boolean))].sort();
    const marketScope = getGeographyDropdownScope(dataSource);
    const uniqueMarkets = [...new Set(marketScope.map(hotel => hotel.market).filter(Boolean))].sort();
    const submarketScope = currentFilters.market
        ? marketScope.filter(function (hotel) { return matchesGeoField(hotel.market, currentFilters.market); })
        : [];
    const uniqueSubmarkets = sortSubmarketOptions(
        [...new Set(submarketScope.map(hotel => hotel.submarket).filter(Boolean))]
    );
    const uniqueLocationTypes = [...new Set(dataSource.map(hotel => hotel.locationType).filter(Boolean))].sort();
    const uniqueHotelTypes = [...new Set(dataSource.map(hotel => hotel.censusPropertyType).filter(Boolean))].sort();
    const uniqueServiceModels = sortHotelServiceModelOptions(
        [...new Set(dataSource.map(hotel => hotel.hotelServiceModel).filter(Boolean))]
    );
    const uniqueOperationTypes = [...new Set(dataSource.map(hotel => hotel.operationType).filter(Boolean))].sort();
    const uniqueManagementCompanies = [...new Set(dataSource.map(hotel => hotel.managementCompany).filter(Boolean))].sort();
    
    // Update all dropdowns - simple and fast
    updateDropdownOptions('parentCompanyFilter', uniqueParentCompanies, 'All Parent Companies');
    updateDropdownOptions('brandFilter', uniqueBrands, 'All Brands');
    updateDropdownOptions('statusFilter', uniqueStatuses, 'All Statuses');
    updateDropdownOptions('propertyTypeFilter', uniquePropertyTypes, 'All Property Types');
    updateDropdownOptions('regionFilter', uniqueRegions, 'All Regions');
    updateDropdownOptions('countryFilter', uniqueCountries, 'All Countries');
    updateDropdownOptions('marketFilter', uniqueMarkets, 'All Markets');
    updateDropdownOptions('locationTypeFilter', uniqueLocationTypes, 'All Location Types');
    updateDropdownOptions('hotelTypeFilter', uniqueHotelTypes, 'All Property Types');
    updateDropdownOptions('hotelServiceModelFilter', uniqueServiceModels, 'All Service / Operating Models');
    updateDropdownOptions('operationTypeFilter', uniqueOperationTypes, 'All Operation Types');
    updateDropdownOptions('managementCompanyFilter', uniqueManagementCompanies, 'All Management Companies');
    updateDropdownOptions('submarketFilter', uniqueSubmarkets, 'All Submarkets');

    const submarketSelect = document.getElementById('submarketFilter');
    if (submarketSelect) {
        submarketSelect.disabled = !currentFilters.market;
        if (!currentFilters.market) {
            submarketSelect.value = '';
            currentFilters.submarket = '';
        }
    }

    if (window.RadarFilterDrawer && typeof window.RadarFilterDrawer.mirrorOptions === 'function') {
        window.RadarFilterDrawer.mirrorOptions();
        window.RadarFilterDrawer.syncFromBar();
    }
    syncTerritoryFilterOptions();
}

// Simple dropdown update - Excel pivot table behavior
function updateDropdownOptions(dropdownId, options, defaultText) {
    const dropdown = document.getElementById(dropdownId);
    if (!dropdown) return;
    
    // Store current selection
    const currentValue = dropdown.value;
    
    // Clear existing options
    dropdown.innerHTML = `<option value="">${defaultText}</option>`;
    
    // Add new options
    options.forEach(option => {
        const optionElement = document.createElement('option');
        optionElement.value = option;
        const isParentCompanyDropdown = dropdownId === 'parentCompanyFilter' || dropdownId === 'parentCompanyFilterDrawer' || dropdownId === 'territoryParentCompanyFilter';
        optionElement.textContent = isParentCompanyDropdown ? formatParentCompanyLabel(option) : option;
        dropdown.appendChild(optionElement);
    });
    
    // Excel pivot behavior: only keep selection if it's still valid in the new options
    if (currentValue && options.includes(currentValue)) {
        dropdown.value = currentValue;
    } else {
        // Clear selection if not valid anymore
        dropdown.value = '';
        // Also clear the corresponding filter
        if (dropdownId === 'parentCompanyFilter') {
            currentFilters.parentCompany = '';
        } else if (dropdownId === 'brandFilter') {
            currentFilters.brand = '';
        } else if (dropdownId === 'statusFilter') {
            currentFilters.status = '';
            currentFilters.statuses = [];
        } else if (dropdownId === 'propertyTypeFilter') {
            currentFilters.propertyType = '';
        } else if (dropdownId === 'regionFilter') {
            currentFilters.region = '';
        } else if (dropdownId === 'countryFilter') {
            currentFilters.country = '';
        } else if (dropdownId === 'marketFilter') {
            currentFilters.market = '';
        } else if (dropdownId === 'locationTypeFilter') {
            currentFilters.locationType = '';
        } else if (dropdownId === 'hotelTypeFilter') {
            currentFilters.hotelType = '';
        } else if (dropdownId === 'hotelServiceModelFilter') {
            currentFilters.hotelServiceModel = '';
        } else if (dropdownId === 'operationTypeFilter') {
            currentFilters.operationType = '';
        } else if (dropdownId === 'managementCompanyFilter') {
            currentFilters.managementCompany = '';
        } else if (dropdownId === 'submarketFilter') {
            currentFilters.submarket = '';
        }
    }
}

// Reset view
function resetView() {
    // Show loading state on reset buttons (header + filter drawer)
    const resetButtons = [
        document.getElementById('radarResetViewBtn'),
        document.getElementById('radarFilterResetViewBtn'),
    ].filter(Boolean);
    const originalLabels = resetButtons.map(function (btn) { return btn.textContent; });
    resetButtons.forEach(function (btn) {
        btn.textContent = 'Resetting...';
        btn.disabled = true;
    });

    setTimeout(function () {
        resetButtons.forEach(function (btn, i) {
            btn.textContent = originalLabels[i] || 'Reset View';
            btn.disabled = false;
        });
    }, 100);
    
    // Set flag to prevent applyFilters from running
    isResetting = true;
    
    // Reset filters object
    currentFilters = {
        parentCompany: '',
        brand: '',
        status: '',
        statuses: [],
        propertyType: '',
        region: '',
        country: '',
        market: '',
        submarket: '',
        locationType: '',
        hotelType: '',
        hotelServiceModel: '',
        operationType: '',
        managementCompany: '',
        roomsMin: '',
        roomsMax: '',
        search: '',
    };
    
    // Reset form inputs in batch
    const formElements = [
        'locationSearch', 'parentCompanyFilter', 'brandFilter', 'statusFilter', 
        'propertyTypeFilter', 'regionFilter', 'countryFilter', 'marketFilter',
        'locationTypeFilter', 'hotelTypeFilter', 'hotelServiceModelFilter', 'operationTypeFilter', 'managementCompanyFilter', 'submarketFilter',
        'roomsMinFilter', 'roomsMaxFilter',
    ];
    formElements.forEach(id => {
        const element = document.getElementById(id);
        if (element) element.value = '';
    });

    if (typeof window.resetMapLayerFilters === 'function') {
        window.resetMapLayerFilters();
    }
    
    // Reset overlay toggles in batch
    const toggles = [
        { id: 'hotelVisibilityToggle', value: true },
        { id: 'whiteSpaceToggle', value: false },
        { id: 'brandTerritoryToggle', value: false },
        { id: 'penetrationToggle', value: false },
        { id: 'pipelineToggle', value: false },
        { id: 'infrastructureToggle', value: false },
        { id: 'demandAnchorsToggle', value: false },
        { id: 'chainScaleToggle', value: false }
    ];
    toggles.forEach(toggle => {
        const element = document.getElementById(toggle.id);
        if (element) element.checked = toggle.value;
    });
    
    // Hide all overlay layers
    if (whiteSpaceLayer) {
        map.removeLayer(whiteSpaceLayer);
        whiteSpaceLayer = null;
    }
    if (territoryLayer) {
        map.removeLayer(territoryLayer);
        territoryLayer = null;
    }
    clearTerritoryRadiusCircle();
    stopTerritoryPinMode();
    MAP_LAYER_FILTERS.territoryPin = null;
    hidePenetrationHeatmap();
    hidePipelinePressure();
    hideTravelInfrastructure();
    hideDemandAnchors();
    
    // Reset overlay visibility flags
    isHotelVisibilityEnabled = true;
    isWhiteSpaceVisible = false;
    isBrandTerritoryVisible = false;
    isPenetrationVisible = false;
    isPipelineVisible = false;
    isInfrastructureVisible = false;
    isDemandAnchorsVisible = false;
    isChainScaleView = false;
    
    // Reset legend to show status legend by default
    const mapLegend = document.querySelector('.map-legend');
    if (mapLegend) {
        mapLegend.classList.remove('chain-scale-enabled');
    }
    
    // Reset filtered hotels to show all
    hotelData = [...allHotels];
    currentFilteredHotels = [...allHotels];
    
    // Reset map view
    map.setView([10.0, -80.0], 4);
    refreshMapSize({ pan: false, delay: 100 });
    
    // Immediate display of hotels (most important)
    displayHotels(allHotels);
    
    // Use requestAnimationFrame for smooth UI updates
    requestAnimationFrame(() => {
        // Fast reset: since all filters are cleared, use fast path
        currentFilteredHotels = [...allHotels];
        displayHotels(allHotels);
        updateStatistics(allHotels);
        updateBrandDistribution(allHotels);
        generateInsights(allHotels);
        updateAllDropdowns(allHotels);
        
        requestAnimationFrame(() => {
            isResetting = false;
            if (window.RadarFilterDrawer && typeof window.RadarFilterDrawer.updateBadge === 'function') {
                window.RadarFilterDrawer.updateBadge();
            }
            if (window.RadarFilterDrawer && typeof window.RadarFilterDrawer.syncFromBar === 'function') {
                window.RadarFilterDrawer.syncFromBar();
            }
        });
    });
}

window.setRadarStatusFilter = function setRadarStatusFilter(statuses) {
    const nextStatuses = Array.isArray(statuses) ? statuses.filter(Boolean) : [];
    currentFilters.statuses = nextStatuses;
    currentFilters.status = nextStatuses.length === 1 ? nextStatuses[0] : '';
    const statusFilter = document.getElementById('statusFilter');
    if (statusFilter) {
        statusFilter.value = currentFilters.status;
    }
    applyFilters();
};

window.getRadarStatusFilter = function getRadarStatusFilter() {
    if (currentFilters.statuses && currentFilters.statuses.length) {
        return currentFilters.statuses.slice();
    }
    return currentFilters.status ? [currentFilters.status] : [];
};

// Show loading state
function showLoading(show) {
    const loadingElement = document.getElementById('loading');
    if (loadingElement) {
        loadingElement.style.display = show ? 'block' : 'none';
    }
}


// Show white space loading state
function showWhiteSpaceLoading(show) {
    const whiteSpaceLoadingElement = document.getElementById('whiteSpaceLoading');
    if (whiteSpaceLoadingElement) {
        whiteSpaceLoadingElement.style.display = show ? 'block' : 'none';
    }
}

function setWhiteSpaceLayerLoading(show) {
    showWhiteSpaceLoading(show);
    if (window.RadarLayerDrawer && typeof window.RadarLayerDrawer.setLayerLoading === 'function') {
        window.RadarLayerDrawer.setLayerLoading('whiteSpaceToggle', show);
    }
}

let whiteSpaceGenerationToken = 0;

function whiteSpaceOpportunitiesSignature(opportunities) {
    return (opportunities || []).map(function (opportunity) {
        return [
            opportunity.id || opportunity.label || '',
            opportunity.level || '',
            opportunity.opportunityScore || 0,
        ].join(':');
    }).join('|');
}

function applyWhiteSpaceOpportunitiesToLayer(opportunities) {
    const RWS = window.RadarWhiteSpace;
    if (!RWS || !map) return;

    if (whiteSpaceLayer) {
        map.removeLayer(whiteSpaceLayer);
    }

    whiteSpaceLayer = L.layerGroup();

    (opportunities || []).forEach(function (opportunity) {
        if (
            window.RadarWhiteSpace &&
            typeof window.RadarWhiteSpace.isMapEligibleOpportunity === 'function' &&
            !window.RadarWhiteSpace.isMapEligibleOpportunity(opportunity)
        ) {
            return;
        }
        if (!isMapLayerLevelVisible('whiteSpaceLevels', opportunity.level)) {
            return;
        }

        const color = RWS.levelColor(opportunity.level);

        const hexagonIcon = L.divIcon({
            className: 'custom-hexagon-marker',
            html: `<div style="
                width: 24px;
                height: 24px;
                background: #ffffff;
                clip-path: polygon(25% 0%, 75% 0%, 100% 50%, 75% 100%, 25% 100%, 0% 50%);
                position: relative;
            "><div style="
                position: absolute;
                top: 2px;
                left: 2px;
                width: 20px;
                height: 20px;
                background: ${color};
                clip-path: polygon(25% 0%, 75% 0%, 100% 50%, 75% 100%, 25% 100%, 0% 50%);
            "></div></div>`,
            iconSize: [24, 24],
            iconAnchor: [12, 12]
        });

        const marker = L.marker([opportunity.lat, opportunity.lng], { icon: hexagonIcon });
        marker.bindPopup(RWS.buildPopupHtml(opportunity), {
            maxWidth: 320,
            maxHeight: 300,
            className: 'radar-white-space-popup-wrap',
        });
        whiteSpaceLayer.addLayer(marker);
    });

    map.addLayer(whiteSpaceLayer);
}

function showTerritoryLoading(show) {
    const el = document.getElementById('territoryLoading');
    if (el) el.style.display = show ? 'block' : 'none';
}

function syncTerritoryLegendFromResult(result) {
    const territories = (result && result.territories) || [];
    const territorySection = document.getElementById('territorySection');
    const territoryLegendOpen = document.getElementById('territoryLegendOpen');
    const territoryLegendReviewSame = document.getElementById('territoryLegendReviewSame');
    const territoryLegendReviewAdjacent = document.getElementById('territoryLegendReviewAdjacent');
    const hasMarkers = territories.length > 0;
    const hasOpen = territories.some(function (t) { return t.status === 'open'; });
    const hasReviewSame = territories.some(function (t) { return t.status === 'review_same'; });
    const hasReviewAdjacent = territories.some(function (t) { return t.status === 'review_adjacent' || t.status === 'caution'; });

    if (territorySection) {
        territorySection.style.setProperty('display', hasMarkers ? 'block' : 'none', 'important');
    }
    if (territoryLegendOpen) {
        territoryLegendOpen.style.setProperty('display', hasOpen ? 'flex' : 'none', 'important');
    }
    if (territoryLegendReviewSame) {
        territoryLegendReviewSame.style.setProperty('display', hasReviewSame ? 'flex' : 'none', 'important');
    }
    if (territoryLegendReviewAdjacent) {
        territoryLegendReviewAdjacent.style.setProperty('display', hasReviewAdjacent ? 'flex' : 'none', 'important');
    }
}

function setTerritoryLayerLoading(show) {
    showTerritoryLoading(show);
    if (window.RadarLayerDrawer && typeof window.RadarLayerDrawer.setLayerLoading === 'function') {
        window.RadarLayerDrawer.setLayerLoading('brandTerritoryToggle', show, 'Scanning Brand Territories…');
    }
}

function clearTerritoryRadiusOverlay() {
    if (territoryRadiusCircle && map) {
        map.removeLayer(territoryRadiusCircle);
        territoryRadiusCircle = null;
    }
    if (territoryPinMarker && map) {
        map.removeLayer(territoryPinMarker);
        territoryPinMarker = null;
    }
}

function resetTerritoryRadiusState() {
    clearTerritoryRadiusOverlay();
    lastTerritoryRadiusAnalysis = null;
}

function clearTerritoryRadiusCircle() {
    resetTerritoryRadiusState();
}

function showTerritoryRadiusOverlay(pin, radiusKm, options) {
    const RT = window.RadarTerritory;
    if (!map || !pin || pin.lat == null || pin.lng == null) return;

    options = options || {};
    const latlng = [pin.lat, pin.lng];
    const km = Math.max(1, parseInt(radiusKm, 10) || 10);
    const territory = Object.prototype.hasOwnProperty.call(options, 'territory')
        ? options.territory
        : lastTerritoryRadiusAnalysis;
    const tileOptions = RT && typeof RT.buildRadiusResultTileOptions === 'function'
        ? RT.buildRadiusResultTileOptions(territory, {
            radiusKm: km,
            awaitingBrand: !getTerritorySubjectBrand(),
        })
        : null;
    const color = tileOptions ? tileOptions.color : (options.color || '#6c72ff');

    clearTerritoryRadiusOverlay();
    if (territory) {
        lastTerritoryRadiusAnalysis = territory;
    } else if (Object.prototype.hasOwnProperty.call(options, 'territory') && options.territory == null) {
        lastTerritoryRadiusAnalysis = null;
    }

    territoryRadiusCircle = L.circle(latlng, {
        radius: km * 1000,
        color: color,
        weight: 2,
        fillColor: color,
        fillOpacity: 0.12,
        dashArray: '6 4',
    });
    map.addLayer(territoryRadiusCircle);

    const pinIcon = tileOptions
        ? L.divIcon({
            className: tileOptions.className,
            html: tileOptions.html,
            iconSize: tileOptions.iconSize,
            iconAnchor: tileOptions.iconAnchor,
        })
        : L.divIcon({
            className: 'custom-territory-pin-marker',
            html: '<div style="' +
                'width:14px;height:14px;background:' + color + ';' +
                'border:2px solid #fff;border-radius:50%;' +
                'box-shadow:0 1px 4px rgba(0,0,0,0.35);"></div>',
            iconSize: [14, 14],
            iconAnchor: [7, 7],
        });
    territoryPinMarker = L.marker(latlng, { icon: pinIcon, zIndexOffset: 1000 });
    if (options.popupHtml) {
        territoryPinMarker.bindPopup(options.popupHtml, {
            maxWidth: 300,
            className: 'radar-territory-popup-wrap',
        });
    }
    map.addLayer(territoryPinMarker);

    if (options.flyTo !== false && territoryRadiusCircle.getBounds) {
        try {
            map.fitBounds(territoryRadiusCircle.getBounds(), {
                padding: [56, 56],
                maxZoom: 14,
                animate: true,
            });
        } catch (err) {
            map.flyTo(latlng, Math.max(map.getZoom(), 12), { animate: true });
        }
    }
}

function syncTerritoryRadiusMapView(result, flyTo) {
    const unit = MAP_LAYER_FILTERS.territoryUnit || 'submarket';
    const pin = MAP_LAYER_FILTERS.territoryPin;
    if (unit !== 'radius' || !pin) {
        resetTerritoryRadiusState();
        return;
    }

    const RT = window.RadarTerritory;
    const radiusKm = MAP_LAYER_FILTERS.territoryRadiusKm || 10;
    const territory = result && result.territories && result.territories[0];
    const popupHtml = territory && RT ? RT.buildPopupHtml(territory) : null;

    showTerritoryRadiusOverlay(pin, radiusKm, {
        territory: territory,
        flyTo: !!flyTo,
        popupHtml: popupHtml,
    });
}

function applyTerritoriesToLayer(result) {
    const RT = window.RadarTerritory;
    if (!RT || !map) return;

    if (territoryLayer) {
        map.removeLayer(territoryLayer);
    }

    territoryLayer = L.layerGroup();
    const territories = (result && result.territories) || [];

    territories.forEach(function (territory) {
        if (territory.unit === 'radius') {
            return;
        }
        if (!isMapLayerLevelVisible('territoryStatuses', territory.status)) {
            return;
        }
        if (!territory.lat || !territory.lng) return;

        const color = RT.statusColor(territory.status);
        const squareIcon = L.divIcon({
            className: 'custom-territory-marker',
            html: '<div style="' +
                'width:22px;height:22px;background:#fff;border-radius:4px;position:relative;' +
                'box-shadow:0 1px 4px rgba(0,0,0,0.25);">' +
                '<div style="position:absolute;top:2px;left:2px;width:18px;height:18px;' +
                'background:' + color + ';border-radius:3px;border:1px solid rgba(0,0,0,0.15);"></div></div>',
            iconSize: [22, 22],
            iconAnchor: [11, 11],
        });

        const marker = L.marker([territory.lat, territory.lng], { icon: squareIcon });
        marker.bindPopup(RT.buildPopupHtml(territory), {
            maxWidth: 300,
            className: 'radar-territory-popup-wrap',
        });
        territoryLayer.addLayer(marker);
    });

    if (territories.some(function (t) { return t.unit !== 'radius'; })) {
        map.addLayer(territoryLayer);
    }

    syncTerritoryRadiusMapView(result, territoryRadiusFlyToNext);
    territoryRadiusFlyToNext = false;
    updateTerritoryBrandHint(result);
}

function updateTerritoryBrandHint(result) {
    const hint = document.getElementById('territoryBrandRequiredHint');
    if (!hint) return;
    const brand = getTerritorySubjectBrand();
    if (!brand) {
        hint.textContent = 'Select a Parent Company and Subject Brand to scan territories.';
        return;
    }
    if (result && result.error) {
        hint.textContent = result.error;
        return;
    }
    const unit = MAP_LAYER_FILTERS.territoryUnit || 'submarket';
    const pin = MAP_LAYER_FILTERS.territoryPin;
    if (unit === 'radius' && pin) {
        const territory = result && result.territories && result.territories[0];
        if (territory && territory.status === 'conflict') {
            hint.textContent = 'Same-brand conflict in this radius — see circle on map.';
            return;
        }
        if (territory && territory.status === 'open') {
            hint.textContent = 'Open site radius for ' + brand + '.';
            return;
        }
        if (territory && territory.status === 'review_same') {
            hint.textContent = 'Same-chain-scale sister-brand overlap to review in this radius.';
            return;
        }
        if (territory && (territory.status === 'review_adjacent' || territory.status === 'caution')) {
            hint.textContent = 'Adjacent-chain-scale sister-brand overlap to review in this radius.';
            return;
        }
        if (!territory) {
            hint.textContent = 'Pin placed — select a Subject Brand to analyze this radius.';
            return;
        }
    }
    if (unit === 'radius' && !pin) {
        hint.textContent = 'Drop a pin on the map to analyze a site radius.';
        return;
    }
    const count = (result && result.territories && result.territories.length) || 0;
    const openCount = (result && result.territories || []).filter(function (t) { return t.status === 'open'; }).length;
    const reviewSameCount = (result && result.territories || []).filter(function (t) { return t.status === 'review_same'; }).length;
    const reviewAdjacentCount = (result && result.territories || []).filter(function (t) {
        return t.status === 'review_adjacent' || t.status === 'caution';
    }).length;
    if (!count) {
        hint.textContent = unit === 'country'
            ? 'No open countries — ' + brand + ' appears in every country in this filter scope.'
            : 'No open or review territories in current filter scope.';
        return;
    }
    if (unit === 'country') {
        const parts = [];
        if (openCount) parts.push(openCount + ' without ' + brand);
        if (reviewSameCount) parts.push(reviewSameCount + ' Same-Scale Review');
        if (reviewAdjacentCount) parts.push(reviewAdjacentCount + ' Adjacent-Scale Review');
        hint.textContent = parts.join(' · ') + ' (Green / Yellow on map).';
        return;
    }
        hint.textContent = count
        ? count + ' actionable territor' + (count === 1 ? 'y' : 'ies') + ' for ' + brand + '.'
        : 'No open or review territories in current filter scope.';
}

function buildTerritoryAnalysisOptions() {
    const territoryFilters = Object.assign({}, currentFilters);
    territoryFilters.brand = getTerritorySubjectBrand();
    return {
        unit: MAP_LAYER_FILTERS.territoryUnit || 'submarket',
        radiusKm: MAP_LAYER_FILTERS.territoryRadiusKm || 10,
        pin: MAP_LAYER_FILTERS.territoryPin,
        subjectBrand: getTerritorySubjectBrand(),
        filters: territoryFilters,
    };
}

async function generateTerritoryMarkers() {
    const generationToken = ++territoryGenerationToken;
    const RT = window.RadarTerritory;
    if (!RT || !map) {
        console.error('[territory] RadarTerritory module not loaded');
        setTerritoryLayerLoading(false);
        return;
    }

    setTerritoryLayerLoading(true);
    await new Promise(function (resolve) {
        setTimeout(resolve, 0);
    });

    try {
        const scopedHotels = getHotelsForTerritoryAnalysis();
        const result = await new Promise(function (resolve) {
            setTimeout(function () {
                resolve(RT.computeTerritories(scopedHotels, buildTerritoryAnalysisOptions()));
            }, 0);
        });

        if (generationToken !== territoryGenerationToken || !isBrandTerritoryVisible) {
            return;
        }

        applyTerritoriesToLayer(result);
        syncTerritoryLegendFromResult(result);
    } catch (err) {
        console.error('[territory] analysis failed', err);
        if (generationToken === territoryGenerationToken && isBrandTerritoryVisible) {
            applyTerritoriesToLayer({ territories: [], error: 'Territory analysis failed. Try again.' });
            syncTerritoryLegendFromResult({ territories: [] });
        }
    } finally {
        if (generationToken === territoryGenerationToken) {
            setTerritoryLayerLoading(false);
        }
    }
}

function stopTerritoryPinMode() {
    territoryPinMode = false;
    if (map) {
        map.getContainer().style.cursor = '';
        map.off('click', handleTerritoryPinMapClick);
    }
    const btn = document.getElementById('territoryDropPinBtn');
    if (btn) {
        btn.classList.remove('is-active');
        btn.textContent = 'Drop Pin on Map';
    }
}

function handleTerritoryPinMapClick(e) {
    if (!territoryPinMode || !e || !e.latlng) return;
    MAP_LAYER_FILTERS.territoryPin = { lat: e.latlng.lat, lng: e.latlng.lng };
    stopTerritoryPinMode();
    territoryRadiusFlyToNext = true;

    const radiusKm = MAP_LAYER_FILTERS.territoryRadiusKm || 10;
    showTerritoryRadiusOverlay(MAP_LAYER_FILTERS.territoryPin, radiusKm, {
        flyTo: true,
        territory: null,
    });
    territoryRadiusFlyToNext = false;

    const pinHint = document.getElementById('territoryPinHint');
    if (pinHint) {
        pinHint.textContent = 'Pin at ' + e.latlng.lat.toFixed(4) + ', ' + e.latlng.lng.toFixed(4) + '.';
    }
    if (window.RadarLayerDrawer && typeof window.RadarLayerDrawer.syncTerritoryControls === 'function') {
        window.RadarLayerDrawer.syncTerritoryControls();
    }
    notifyRadarFilterBadge();
    if (isBrandTerritoryVisible) scheduleGenerateTerritoryMarkers();
}

window.startTerritoryPinDrop = function startTerritoryPinDrop() {
    if (!map) return;
    territoryPinMode = true;
    map.getContainer().style.cursor = 'crosshair';
    map.on('click', handleTerritoryPinMapClick);
    const btn = document.getElementById('territoryDropPinBtn');
    if (btn) {
        btn.classList.add('is-active');
        btn.textContent = 'Click map to place pin…';
    }
    const pinHint = document.getElementById('territoryPinHint');
    if (pinHint) pinHint.textContent = 'Click anywhere on the map to set the site pin.';
};

window.clearTerritoryPin = function clearTerritoryPin() {
    MAP_LAYER_FILTERS.territoryPin = null;
    stopTerritoryPinMode();
    clearTerritoryRadiusCircle();
    const pinHint = document.getElementById('territoryPinHint');
    if (pinHint) pinHint.textContent = '';
    if (isBrandTerritoryVisible) scheduleGenerateTerritoryMarkers();
    notifyRadarFilterBadge();
};

function toggleBrandTerritory() {
    const toggle = document.getElementById('brandTerritoryToggle');
    if (!toggle) {
        console.error('brandTerritoryToggle element not found');
        return;
    }

    isBrandTerritoryVisible = toggle.checked;
    notifyRadarFilterBadge();
    syncRadarLayerDrawerControls();

    if (isBrandTerritoryVisible) {
        generateTerritoryMarkers().then(function () {
            syncRadarLayerDrawerControls();
        });
    } else {
        stopTerritoryPinMode();
        if (territoryLayer) {
            map.removeLayer(territoryLayer);
            territoryLayer = null;
        }
        clearTerritoryRadiusCircle();

        const territorySection = document.getElementById('territorySection');
        const territoryLegendOpen = document.getElementById('territoryLegendOpen');
        const territoryLegendReviewSame = document.getElementById('territoryLegendReviewSame');
        const territoryLegendReviewAdjacent = document.getElementById('territoryLegendReviewAdjacent');
        if (territorySection) territorySection.style.setProperty('display', 'none', 'important');
        if (territoryLegendOpen) territoryLegendOpen.style.setProperty('display', 'none', 'important');
        if (territoryLegendReviewSame) territoryLegendReviewSame.style.setProperty('display', 'none', 'important');
        if (territoryLegendReviewAdjacent) territoryLegendReviewAdjacent.style.setProperty('display', 'none', 'important');
        syncRadarLayerDrawerControls();
    }
}

window.toggleBrandTerritory = toggleBrandTerritory;

// Generate mock hotel data
function generateMockHotelData() {
    const hotels = [];
    const brands = ['Marriott International', 'Hilton Worldwide', 'Hyatt Hotels', 'IHG Hotels & Resorts', 'Choice Hotels International', 'Wyndham Hotels & Resorts', 'Accor', 'Radisson Hotel Group', 'Independent'];
    const statuses = ['Open', 'Pipeline', 'Candidate'];
    const cities = ['Mexico City', 'Cancun', 'Guadalajara', 'Sao Paulo', 'Rio de Janeiro', 'Buenos Aires', 'Bogota', 'Lima', 'Santiago', 'Santo Domingo'];
    const countries = ['Mexico', 'Brazil', 'Argentina', 'Colombia', 'Peru', 'Chile', 'Dominican Republic'];
    const propertyTypes = ['Luxury', 'Upper Upscale', 'Upscale', 'Upper Midscale', 'Midscale', 'Economy', 'Extended Stay', 'Select Service'];
    
    for (let i = 0; i < 1000; i++) {
        const brand = brands[Math.floor(Math.random() * brands.length)];
        const status = statuses[Math.floor(Math.random() * statuses.length)];
        const city = cities[Math.floor(Math.random() * cities.length)];
        const country = countries[Math.floor(Math.random() * countries.length)];
        const propertyType = propertyTypes[Math.floor(Math.random() * propertyTypes.length)];
        
        hotels.push({
            id: `mock-${i}`,
            name: `${brand} ${city}`,
            brand: brand,
            parentCompany: brand === 'Independent' ? 'Independent' : `${brand} Group`,
            status: status,
            lat: 19.4326 + (Math.random() - 0.5) * 10,
            lng: -99.1332 + (Math.random() - 0.5) * 10,
            city: city,
            country: country,
            region: 'CALA',
            locationType: 'Urban',
            rooms: Math.floor(Math.random() * 300) + 50,
            propertyType: propertyType,
            projectPhase: 'Planning'
        });
    }
    
    return hotels;
}

// Find highest concentration of hotels for auto-zoom
function findHighestConcentrationArea() {
    if (currentFilteredHotels.length === 0) return null;
    
    // Group hotels by proximity (within 0.01 degrees ≈ 1km)
    const clusters = [];
    const processed = new Set();
    
    currentFilteredHotels.forEach((hotel, index) => {
        if (processed.has(index)) return;
        
        const cluster = [hotel];
        processed.add(index);
        
        // Find nearby hotels
        currentFilteredHotels.forEach((otherHotel, otherIndex) => {
            if (processed.has(otherIndex)) return;
            
            const distance = Math.sqrt(
                Math.pow(hotel.lat - otherHotel.lat, 2) + 
                Math.pow(hotel.lng - otherHotel.lng, 2)
            );
            
            if (distance < 0.01) { // Within ~1km
                cluster.push(otherHotel);
                processed.add(otherIndex);
            }
        });
        
        if (cluster.length > 1) {
            clusters.push(cluster);
        }
    });
    
    // Find the cluster with the most hotels
    if (clusters.length === 0) return null;
    
    const largestCluster = clusters.reduce((max, cluster) => 
        cluster.length > max.length ? cluster : max
    );
    
    // Calculate center of the largest cluster
    const centerLat = largestCluster.reduce((sum, hotel) => sum + hotel.lat, 0) / largestCluster.length;
    const centerLng = largestCluster.reduce((sum, hotel) => sum + hotel.lng, 0) / largestCluster.length;
    
    return {
        center: { lat: centerLat, lng: centerLng },
        hotels: largestCluster,
        count: largestCluster.length
    };
}

function syncRadarLayerDrawerControls() {
    if (window.RadarLayerDrawer && typeof window.RadarLayerDrawer.syncControls === 'function') {
        window.RadarLayerDrawer.syncControls();
    }
    notifyRadarFilterBadge();
}

// Toggle functions
function toggleHotelVisibility() {
    const toggle = document.getElementById('hotelVisibilityToggle');
    isHotelVisibilityEnabled = toggle.checked;
    
    if (isHotelVisibilityEnabled) {
        displayHotels(currentFilteredHotels);
    } else {
        markersCluster.clearLayers();
        currentMarkers = [];
        if (isChainScaleView) {
            const chainToggle = document.getElementById('chainScaleToggle');
            if (chainToggle) chainToggle.checked = false;
            toggleChainScaleView();
        }
    }

    syncRadarLayerDrawerControls();
}

// Toggle chain scale view
function toggleChainScaleView() {
    const toggle = document.getElementById('chainScaleToggle');
    isChainScaleView = toggle.checked;

    if (isChainScaleView) {
        const hotelToggle = document.getElementById('hotelVisibilityToggle');
        if (hotelToggle && !hotelToggle.checked) {
            hotelToggle.checked = true;
            isHotelVisibilityEnabled = true;
        }
    }
    // Get the map legend container
    const mapLegend = document.querySelector('.map-legend');
    
    if (isChainScaleView) {
        // Add class to enable chain scale view
        if (mapLegend) {
            mapLegend.classList.add('chain-scale-enabled');
        }
        
        // Fallback: Direct style manipulation
        const chainScaleLegendSection = document.getElementById('chainScaleLegendSection');
        const chainScaleLegendItems = [
            document.getElementById('chainScaleLegendItem1'),
            document.getElementById('chainScaleLegendItem2'),
            document.getElementById('chainScaleLegendItem3'),
            document.getElementById('chainScaleLegendItem4'),
            document.getElementById('chainScaleLegendItem5'),
            document.getElementById('chainScaleLegendItem6'),
            document.getElementById('chainScaleLegendItem7')
        ];
        
        if (chainScaleLegendSection) {
            chainScaleLegendSection.style.setProperty('display', 'block', 'important');
        }
        chainScaleLegendItems.forEach(item => {
            if (item) item.style.setProperty('display', 'flex', 'important');
        });
        
        // Hide status legend
        const statusLegendSection = document.getElementById('statusLegendSection');
        const statusLegendItems = [
            document.getElementById('statusLegendItem1'),
            document.getElementById('statusLegendItem2'),
            document.getElementById('statusLegendItem3')
        ];
        
        if (statusLegendSection) {
            statusLegendSection.style.setProperty('display', 'none', 'important');
        }
        statusLegendItems.forEach(item => {
            if (item) item.style.setProperty('display', 'none', 'important');
        });
    } else {
        // Remove class to disable chain scale view
        if (mapLegend) {
            mapLegend.classList.remove('chain-scale-enabled');
        }
        
        // Fallback: Direct style manipulation
        const chainScaleLegendSection = document.getElementById('chainScaleLegendSection');
        const chainScaleLegendItems = [
            document.getElementById('chainScaleLegendItem1'),
            document.getElementById('chainScaleLegendItem2'),
            document.getElementById('chainScaleLegendItem3'),
            document.getElementById('chainScaleLegendItem4'),
            document.getElementById('chainScaleLegendItem5'),
            document.getElementById('chainScaleLegendItem6'),
            document.getElementById('chainScaleLegendItem7')
        ];
        
        if (chainScaleLegendSection) {
            chainScaleLegendSection.style.setProperty('display', 'none', 'important');
        }
        chainScaleLegendItems.forEach(item => {
            if (item) item.style.setProperty('display', 'none', 'important');
        });
        
        // Show status legend
        const statusLegendSection = document.getElementById('statusLegendSection');
        const statusLegendItems = [
            document.getElementById('statusLegendItem1'),
            document.getElementById('statusLegendItem2'),
            document.getElementById('statusLegendItem3')
        ];
        
        if (statusLegendSection) {
            statusLegendSection.style.setProperty('display', 'block', 'important');
        }
        statusLegendItems.forEach(item => {
            if (item) item.style.setProperty('display', 'flex', 'important');
        });
    }
    
    // Refresh the display to show new colors
    if (isHotelVisibilityEnabled) {
        displayHotels(currentFilteredHotels);
    }

    syncRadarLayerDrawerControls();
}

function hidePenetrationHeatmap() {
    if (penetrationLayer) {
        map.removeLayer(penetrationLayer);
        penetrationLayer = null;
    }
}

function hidePipelinePressure() {
    if (pipelineLayer) {
        map.removeLayer(pipelineLayer);
        pipelineLayer = null;
    }
}

function hideTravelInfrastructure() {
    if (infrastructureLayer) {
        map.removeLayer(infrastructureLayer);
        infrastructureLayer = null;
    }
    setInfrastructureFilterBarVisible(false);
    setInfrastructureEmptyState(false);
    setInfrastructureLegendVisible(false);
}

function setInfrastructureFilterBarVisible(show) {
    const wrap = document.getElementById("infrastructureFilterWrap");
    const chips = document.getElementById("infrastructureFilterChips");
    const inDrawer = wrap && wrap.closest(".radar-layer-card");

    if (inDrawer) {
        if (wrap) {
            wrap.style.removeProperty("display");
            wrap.classList.add("is-visible");
        }
        if (chips) {
            chips.style.removeProperty("display");
            chips.classList.add("is-visible");
        }
        return;
    }

    if (wrap) {
        wrap.classList.toggle("is-visible", !!show);
        wrap.style.display = show ? "" : "none";
    }
    if (chips) {
        chips.classList.toggle("is-visible", !!show);
        chips.style.display = show ? "" : "none";
    }
}

function setInfrastructureEmptyState(show) {
    const el = document.getElementById("infrastructureEmptyState");
    if (el) el.style.display = show ? "block" : "none";
}

function getTravelInfraApiOpts() {
    const opts = {
        pointTypeFilter: infrastructurePointTypeFilter,
        region: currentFilters.region || "",
    };
    if (currentFilters.country) opts.country = currentFilters.country;
    return opts;
}

function getTravelInfraScopeKey() {
    return JSON.stringify(getTravelInfraApiOpts());
}

let lastTravelInfraRefreshScopeKey = "";

async function loadInfrastructureSummaryCounts() {
    const TIR = window.TravelInfrastructureRadar;
    if (!TIR) return;
    try {
        const data = await TIR.fetchInfrastructure({ region: currentFilters.region || "" });
        infrastructureTypeCounts = TIR.extractTypeCounts(data);
        infrastructureTotalCount = TIR.getTotalCount(data, infrastructureTypeCounts);
        renderInfrastructureFilterChips();
    } catch (err) {
        console.warn("[travel-infrastructure] summary counts failed", err);
        renderInfrastructureFilterChips();
    }
}

function renderInfrastructureFilterChips() {
    const TIR = window.TravelInfrastructureRadar;
    const chips = document.getElementById("infrastructureFilterChips");
    if (!TIR || !chips) return;
    TIR.renderFilterChips(chips, {
        selectedId: infrastructurePointTypeFilter,
        typeCounts: infrastructureTypeCounts || {},
        totalCount: infrastructureTotalCount,
        onSelect: function (filterId) {
            if (infrastructurePointTypeFilter === filterId) return;
            infrastructurePointTypeFilter = filterId;
            if (isInfrastructureVisible) {
                generateInfrastructureMarkers();
            } else {
                renderInfrastructureFilterChips();
            }
            renderTravelInfrastructureLegendItems();
            notifyRadarFilterBadge();
        },
    });
    renderTravelInfrastructureLegendItems();
}

function refreshTravelInfrastructureIfVisible() {
    if (!isInfrastructureVisible) return;
    const scopeKey = getTravelInfraScopeKey();
    if (scopeKey === lastTravelInfraRefreshScopeKey) return;
    lastTravelInfraRefreshScopeKey = scopeKey;
    infrastructureTypeCounts = null;
    infrastructureTotalCount = null;
    generateInfrastructureMarkers();
}

function selectInfrastructurePointTypeFilter(filterId) {
    infrastructurePointTypeFilter = filterId || "all";
    lastTravelInfraRefreshScopeKey = "";
    renderInfrastructureFilterChips();
    renderTravelInfrastructureLegendItems();
    if (isInfrastructureVisible) {
        generateInfrastructureMarkers();
    }
    notifyRadarFilterBadge();
}

function selectDemandAnchorsPointTypeFilter(filterId) {
    demandAnchorsPointTypeFilter = filterId || "all";
    lastDemandAnchorsRefreshScopeKey = "";
    renderDemandAnchorsFilterChips();
    renderDemandAnchorsLegendItems();
    if (isDemandAnchorsVisible) {
        generateDemandAnchorsMarkers();
    }
    notifyRadarFilterBadge();
}

function hideDemandAnchors() {
    if (demandAnchorsLayer) {
        map.removeLayer(demandAnchorsLayer);
        demandAnchorsLayer = null;
    }
    setDemandAnchorsFilterBarVisible(false);
    setDemandAnchorsEmptyState(false);
    setDemandAnchorsLegendVisible(false);
}

function setDemandAnchorsFilterBarVisible(show) {
    const wrap = document.getElementById("demandAnchorsFilterWrap");
    const chips = document.getElementById("demandAnchorsFilterChips");
    const inDrawer = wrap && wrap.closest(".radar-layer-card");

    if (inDrawer) {
        if (wrap) {
            wrap.style.removeProperty("display");
            wrap.classList.add("is-visible");
        }
        if (chips) {
            chips.style.removeProperty("display");
            chips.classList.add("is-visible");
        }
        return;
    }

    if (wrap) {
        wrap.classList.toggle("is-visible", !!show);
        wrap.style.display = show ? "" : "none";
    }
    if (chips) {
        chips.classList.toggle("is-visible", !!show);
        chips.style.display = show ? "" : "none";
    }
}

function setDemandAnchorsEmptyState(show, message) {
    const el = document.getElementById("demandAnchorsEmptyState");
    if (el) {
        el.style.display = show ? "block" : "none";
        el.textContent =
            message ||
            "No demand anchor points found for this filter.";
    }
}

/** Demand Anchors use market region values (e.g. Caribbean), not hotel census regions (e.g. Puerto Rico). */
const DEMAND_ANCHOR_REGION_VALUES = new Set([
    "Caribbean",
    "Mexico",
    "Central America",
    "Colombia",
    "South America",
    "CALA",
]);

function getDemandAnchorsApiOpts() {
    const opts = {
        pointTypeFilter: demandAnchorsPointTypeFilter,
    };
    const hotelRegion = (currentFilters.region || "").trim();
    if (hotelRegion && DEMAND_ANCHOR_REGION_VALUES.has(hotelRegion)) {
        opts.region = hotelRegion;
    }
    if (currentFilters.country) opts.country = currentFilters.country;
    return opts;
}

function getDemandAnchorsScopeKey() {
    return JSON.stringify(getDemandAnchorsApiOpts());
}

let lastDemandAnchorsRefreshScopeKey = "";

async function loadDemandAnchorsSummaryCounts() {
    const DAR = window.DemandAnchorsRadar;
    if (!DAR) return;
    try {
        const data = await DAR.fetchDemandAnchors(getDemandAnchorsApiOpts());
        demandAnchorsTypeCounts = DAR.extractTypeCounts(data);
        demandAnchorsTotalCount = DAR.getTotalCount(data, demandAnchorsTypeCounts);
        renderDemandAnchorsFilterChips();
    } catch (err) {
        console.warn("[demand-anchors] summary counts failed", err);
        renderDemandAnchorsFilterChips();
    }
}

function renderDemandAnchorsFilterChips() {
    const DAR = window.DemandAnchorsRadar;
    const chips = document.getElementById("demandAnchorsFilterChips");
    if (!DAR || !chips) return;
    DAR.renderFilterChips(chips, {
        selectedId: demandAnchorsPointTypeFilter,
        typeCounts: demandAnchorsTypeCounts || {},
        totalCount: demandAnchorsTotalCount,
        onSelect: function (filterId) {
            if (demandAnchorsPointTypeFilter === filterId) return;
            demandAnchorsPointTypeFilter = filterId;
            if (isDemandAnchorsVisible) {
                generateDemandAnchorsMarkers();
            } else {
                renderDemandAnchorsFilterChips();
            }
            renderDemandAnchorsLegendItems();
            notifyRadarFilterBadge();
        },
    });
    renderDemandAnchorsLegendItems();
}

function refreshDemandAnchorsIfVisible() {
    if (!isDemandAnchorsVisible) return;
    const scopeKey = getDemandAnchorsScopeKey();
    if (scopeKey === lastDemandAnchorsRefreshScopeKey) return;
    lastDemandAnchorsRefreshScopeKey = scopeKey;
    demandAnchorsTypeCounts = null;
    demandAnchorsTotalCount = null;
    generateDemandAnchorsMarkers();
}

// Toggle functions for overlays
function toggleWhiteSpace() {
    const toggle = document.getElementById('whiteSpaceToggle');
    if (!toggle) {
        console.error('whiteSpaceToggle element not found');
        return;
    }
    
    // Show loading state for white space generation
    isWhiteSpaceVisible = toggle.checked;
    notifyRadarFilterBadge();
    syncRadarLayerDrawerControls();
    
    if (isWhiteSpaceVisible) {
        const whiteSpaceSection = document.getElementById('whiteSpaceSection');
        const whiteSpaceLegend = document.getElementById('whiteSpaceLegend');
        const whiteSpaceLegend2 = document.getElementById('whiteSpaceLegend2');

        generateWhiteSpaceMarkers().then(function () {
            if (whiteSpaceSection) whiteSpaceSection.style.setProperty('display', 'block', 'important');
            if (whiteSpaceLegend) whiteSpaceLegend.style.setProperty('display', 'flex', 'important');
            if (whiteSpaceLegend2) whiteSpaceLegend2.style.setProperty('display', 'flex', 'important');
            syncRadarLayerDrawerControls();
        });
    } else {
        const whiteSpaceSection = document.getElementById('whiteSpaceSection');
        const whiteSpaceLegend = document.getElementById('whiteSpaceLegend');
        const whiteSpaceLegend2 = document.getElementById('whiteSpaceLegend2');
        const whiteSpaceLegend3 = document.getElementById('whiteSpaceLegend3');

        if (whiteSpaceLayer) {
            map.removeLayer(whiteSpaceLayer);
            whiteSpaceLayer = null;
        }
        if (whiteSpaceSection) whiteSpaceSection.style.setProperty('display', 'none', 'important');
        if (whiteSpaceLegend) whiteSpaceLegend.style.setProperty('display', 'none', 'important');
        if (whiteSpaceLegend2) whiteSpaceLegend2.style.setProperty('display', 'none', 'important');
        if (whiteSpaceLegend3) whiteSpaceLegend3.style.setProperty('display', 'none', 'important');
        syncRadarLayerDrawerControls();
    }
}

function togglePenetrationHeatmap() {
    const toggle = document.getElementById('penetrationToggle');
    isPenetrationVisible = toggle.checked;
    
    // Force clear any existing layer
    if (penetrationLayer) {
        map.removeLayer(penetrationLayer);
        penetrationLayer = null;
    }
    
    // Show/hide legend - use class instead of inline styles to avoid layout shifts
    const penetrationSection = document.getElementById('penetrationSection');
    const penetrationLegend = document.getElementById('penetrationLegend');
    const penetrationLegend2 = document.getElementById('penetrationLegend2');
    const penetrationLegend3 = document.getElementById('penetrationLegend3');
    
    if (isPenetrationVisible) {
        // Force regenerate with fresh data
        generatePenetrationMarkers();
        if (penetrationSection) penetrationSection.style.setProperty('display', 'block', 'important');
        if (penetrationLegend) penetrationLegend.style.setProperty('display', 'flex', 'important');
        if (penetrationLegend2) penetrationLegend2.style.setProperty('display', 'flex', 'important');
        if (penetrationLegend3) penetrationLegend3.style.setProperty('display', 'flex', 'important');
    } else {
        hidePenetrationHeatmap();
        if (penetrationSection) penetrationSection.style.setProperty('display', 'none', 'important');
        if (penetrationLegend) penetrationLegend.style.setProperty('display', 'none', 'important');
        if (penetrationLegend2) penetrationLegend2.style.setProperty('display', 'none', 'important');
        if (penetrationLegend3) penetrationLegend3.style.setProperty('display', 'none', 'important');
    }
    syncRadarLayerDrawerControls();
}

function togglePipelinePressure() {
    const toggle = document.getElementById('pipelineToggle');
    isPipelineVisible = toggle.checked;
    
    // Show/hide legend section
    const pipelineSection = document.getElementById('pipelineSection');
    const pipelineLegend = document.getElementById('pipelineLegend');
    const pipelineLegend2 = document.getElementById('pipelineLegend2');
    const pipelineLegend3 = document.getElementById('pipelineLegend3');
    
    if (isPipelineVisible) {
        generatePipelineMarkers();
        // Show legend
        if (pipelineSection) pipelineSection.style.setProperty('display', 'block', 'important');
        if (pipelineLegend) pipelineLegend.style.setProperty('display', 'flex', 'important');
        if (pipelineLegend2) pipelineLegend2.style.setProperty('display', 'flex', 'important');
        if (pipelineLegend3) pipelineLegend3.style.setProperty('display', 'flex', 'important');
    } else {
        hidePipelinePressure();
        // Hide legend
        if (pipelineSection) pipelineSection.style.setProperty('display', 'none', 'important');
        if (pipelineLegend) pipelineLegend.style.setProperty('display', 'none', 'important');
        if (pipelineLegend2) pipelineLegend2.style.setProperty('display', 'none', 'important');
        if (pipelineLegend3) pipelineLegend3.style.setProperty('display', 'none', 'important');
    }
    syncRadarLayerDrawerControls();
}

const RADAR_INFRA_LEGEND_ITEM_IDS = [];
const RADAR_DEMAND_ANCHOR_LEGEND_ITEM_IDS = [];

function legendEscHtml(value) {
    return String(value == null ? "" : value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/"/g, "&quot;");
}

function getInfrastructureLegendFilterDefs() {
    const TIR = window.TravelInfrastructureRadar;
    if (!TIR) return [];
    const typeCounts = infrastructureTypeCounts || {};
    return TIR.getVisibleFilterDefs(typeCounts).filter(function (def) {
        return def.id !== "all";
    });
}

function getDemandAnchorLegendFilterDefs() {
    const DAR = window.DemandAnchorsRadar;
    if (!DAR) return [];
    const typeCounts = demandAnchorsTypeCounts || {};
    const totalCount = demandAnchorsTotalCount;
    return (DAR.PRIMARY_FILTERS || []).filter(function (def) {
        if (def.id === "all") return false;
        if ((typeCounts[def.id] || 0) === 0 && totalCount > 0) return false;
        return true;
    });
}

function resolveLegendDefsForSelection(defs, selectedId) {
    if (!selectedId || selectedId === "all") return defs;
    return defs.filter(function (def) {
        return def.id === selectedId;
    });
}

function renderTravelInfrastructureLegendItems() {
    const mount = document.getElementById("infrastructureLegendMount");
    const TIR = window.TravelInfrastructureRadar;
    if (!mount || !TIR) return;

    if (!isInfrastructureVisible) {
        mount.innerHTML = "";
        return;
    }

    const defs = resolveLegendDefsForSelection(
        getInfrastructureLegendFilterDefs(),
        infrastructurePointTypeFilter
    );

    mount.innerHTML = defs
        .map(function (def) {
            const color = TIR.getColorForPointType(def.id);
            const selected = infrastructurePointTypeFilter === "all" || infrastructurePointTypeFilter === def.id;
            return (
                '<div class="legend-item radar-legend-filter-item' +
                (selected ? " is-selected" : "") +
                '" data-legend-filter-id="' +
                legendEscHtml(def.id) +
                '">' +
                '<div class="legend-marker radar-infra-pentagon" style="background:' +
                legendEscHtml(color) +
                ';"></div>' +
                "<span>" +
                legendEscHtml(def.label) +
                "</span></div>"
            );
        })
        .join("");
}

function renderDemandAnchorsLegendItems() {
    const mount = document.getElementById("demandAnchorsLegendMount");
    const DAR = window.DemandAnchorsRadar;
    if (!mount || !DAR) return;

    if (!isDemandAnchorsVisible) {
        mount.innerHTML = "";
        return;
    }

    const defs = resolveLegendDefsForSelection(
        getDemandAnchorLegendFilterDefs(),
        demandAnchorsPointTypeFilter
    );

    mount.innerHTML = defs
        .map(function (def) {
            const color = DAR.getColorForPointType(def.id);
            const selected = demandAnchorsPointTypeFilter === "all" || demandAnchorsPointTypeFilter === def.id;
            return (
                '<div class="legend-item radar-legend-filter-item' +
                (selected ? " is-selected" : "") +
                '" data-legend-filter-id="' +
                legendEscHtml(def.id) +
                '">' +
                '<div class="legend-marker radar-demand-diamond" style="background:' +
                legendEscHtml(color) +
                ';"></div>' +
                "<span>" +
                legendEscHtml(def.label) +
                "</span></div>"
            );
        })
        .join("");
}

function setLayerLegendVisible(sectionId, itemIds, show) {
    const section = document.getElementById(sectionId);
    if (section) {
        section.style.setProperty("display", show ? "block" : "none", "important");
    }
    (itemIds || []).forEach((id) => {
        const el = document.getElementById(id);
        if (el) el.style.setProperty("display", show ? "flex" : "none", "important");
    });
}

function setInfrastructureLegendVisible(show) {
    setLayerLegendVisible("infrastructureSection", [], show);
    const mount = document.getElementById("infrastructureLegendMount");
    if (mount) {
        mount.style.setProperty("display", show ? "block" : "none", "important");
    }
    if (show) renderTravelInfrastructureLegendItems();
    else if (mount) mount.innerHTML = "";
}

function setDemandAnchorsLegendVisible(show) {
    setLayerLegendVisible("demandAnchorsSection", [], show);
    const mount = document.getElementById("demandAnchorsLegendMount");
    if (mount) {
        mount.style.setProperty("display", show ? "block" : "none", "important");
    }
    if (show) renderDemandAnchorsLegendItems();
    else if (mount) mount.innerHTML = "";
}

async function toggleTravelInfrastructure() {
    const toggle = document.getElementById('infrastructureToggle');
    isInfrastructureVisible = toggle.checked;
    
    if (isInfrastructureVisible) {
        setInfrastructureFilterBarVisible(true);
        if (!infrastructureTypeCounts) {
            await loadInfrastructureSummaryCounts();
        }
        await generateInfrastructureMarkers();
        setInfrastructureLegendVisible(true);
    } else {
        hideTravelInfrastructure();
        setInfrastructureLegendVisible(false);
    }
    syncRadarLayerDrawerControls();
}

async function toggleDemandAnchors() {
    const toggle = document.getElementById('demandAnchorsToggle');
    if (!toggle) return;
    isDemandAnchorsVisible = toggle.checked;

    if (isDemandAnchorsVisible) {
        setDemandAnchorsFilterBarVisible(true);
        if (!demandAnchorsTypeCounts) {
            await loadDemandAnchorsSummaryCounts();
        }
        await generateDemandAnchorsMarkers();
        setDemandAnchorsLegendVisible(true);
    } else {
        hideDemandAnchors();
        setDemandAnchorsLegendVisible(false);
    }
    syncRadarLayerDrawerControls();
}

// Generate white space markers — submarket/market opportunities with explanations
async function generateWhiteSpaceMarkers() {
    const generationToken = ++whiteSpaceGenerationToken;
    const sourceHotels = allHotels.length > 0 ? allHotels : hotelData;
    if (!sourceHotels.length) {
        return;
    }

    const RWS = window.RadarWhiteSpace;
    if (!RWS || typeof RWS.computeOpportunities !== 'function') {
        console.error('[white-space] RadarWhiteSpace module not loaded');
        return;
    }

    setWhiteSpaceLayerLoading(true);
    try {
        let opportunities = RWS.computeOpportunities(sourceHotels, currentFilters);
        if (generationToken !== whiteSpaceGenerationToken || !isWhiteSpaceVisible) {
            return;
        }
        const syncSignature = whiteSpaceOpportunitiesSignature(opportunities);
        applyWhiteSpaceOpportunitiesToLayer(opportunities);

        if (typeof RWS.computeOpportunitiesAsync === 'function') {
            try {
                const enriched = await RWS.computeOpportunitiesAsync(sourceHotels, currentFilters);
                if (generationToken !== whiteSpaceGenerationToken || !isWhiteSpaceVisible) {
                    return;
                }
                if (whiteSpaceOpportunitiesSignature(enriched) !== syncSignature) {
                    opportunities = enriched;
                    applyWhiteSpaceOpportunitiesToLayer(opportunities);
                }
            } catch (err) {
                console.warn('[white-space] demand anchor enrichment skipped', err);
            }
        }
    } catch (err) {
        console.error('[white-space] opportunity generation failed', err);
        if (generationToken === whiteSpaceGenerationToken && isWhiteSpaceVisible) {
            applyWhiteSpaceOpportunitiesToLayer(
                RWS.computeOpportunities(sourceHotels, currentFilters)
            );
        }
    } finally {
        if (generationToken === whiteSpaceGenerationToken) {
            setWhiteSpaceLayerLoading(false);
        }
    }
}


function generatePenetrationMarkers() {
    if (penetrationLayer) {
        map.removeLayer(penetrationLayer);
    }
    penetrationLayer = L.layerGroup();

    const RBP = window.RadarBrandedPenetration;
    const allHotelData = window.allHotels || allHotels || [];
    if (!RBP || !allHotelData.length) {
        return;
    }

    const buckets = RBP.computePenetrationBuckets(allHotelData, currentFilters);
    const levelColors = {
        high: { color: '#ff6b6b', bgColor: '#fef2f2' },
        medium: { color: '#ffd93d', bgColor: '#fffbeb' },
        low: { color: '#6bcf7f', bgColor: '#f0fdf4' },
    };

    buckets.forEach(function (bucket) {
        const metrics = bucket.metrics || {};
        const penetrationKey = metrics.level;
        if (!penetrationKey || !isMapLayerLevelVisible('penetrationLevels', penetrationKey)) {
            return;
        }
        const palette = levelColors[penetrationKey] || levelColors.low;
        const marker = L.marker([bucket.lat, bucket.lng], {
            icon: L.divIcon({
                className: 'custom-square-marker',
                html: `<div style="width: 20px; height: 20px; background: ${palette.color}; border: 2px solid #ffffff; border-radius: 3px; box-shadow: 0 2px 6px rgba(0,0,0,0.3);"></div>`,
                iconSize: [20, 20],
                iconAnchor: [10, 10],
            }),
        });

        const popupContent = `
            <div style="min-width: 250px; font-family: 'Inter', sans-serif;">
                <h3 style="margin: 0 0 10px 0; color: #333; font-size: 16px;">
                    ${bucket.title}
                </h3>
                <div style="font-size: 11px; color: #888; margin-bottom: 8px;">
                    ${bucket.country || ''}${bucket.market && bucket.market !== bucket.submarket ? ' · ' + bucket.market : ''}
                </div>
                <div style="background: ${palette.bgColor}; padding: 10px; border-radius: 6px; margin-bottom: 10px;">
                    <div style="font-size: 18px; font-weight: bold; color: ${palette.color};">
                        ${metrics.percentage}% Branded
                    </div>
                    <div style="font-size: 12px; color: #666; text-transform: uppercase;">${metrics.levelLabel} Penetration</div>
                </div>
                <div style="font-size: 12px; color: #666;">
                    <strong>Open Hotels:</strong> ${metrics.totalHotels}<br>
                    <strong>Branded:</strong> ${metrics.brandedCount}<br>
                    <strong>Independent:</strong> ${metrics.independentCount}<br>
                    <strong>Scope:</strong> Open hotels in submarket (current map filters)
                </div>
            </div>
        `;

        marker.bindPopup(popupContent);
        penetrationLayer.addLayer(marker);
    });

    map.addLayer(penetrationLayer);
}

function generatePipelineMarkers() {
    if (pipelineLayer) {
        map.removeLayer(pipelineLayer);
    }
    pipelineLayer = L.layerGroup();

    const RPP = window.RadarPipelinePressure || window.RadarBrandedPenetration;
    const allHotelData = window.allHotels || allHotels || [];
    if (!RPP || !allHotelData.length) {
        return;
    }

    const buckets = RPP.computePipelinePressureBuckets(allHotelData, currentFilters);
    const levelColors = {
        high: '#ff4757',
        medium: '#ffa502',
        low: '#2ed573',
    };

    buckets.forEach(function (bucket) {
        const metrics = bucket.metrics || {};
        const pipelineKey = metrics.level;
        if (!pipelineKey || !isMapLayerLevelVisible('pipelineLevels', pipelineKey)) {
            return;
        }
        const color = levelColors[pipelineKey] || levelColors.low;
        const displayPct = metrics.classificationPercentage;
        const marker = L.marker([bucket.lat, bucket.lng], {
            icon: L.divIcon({
                className: 'custom-triangle-marker',
                html: `<div style="
                        width: 0;
                        height: 0;
                        border-left: 12px solid transparent;
                        border-right: 12px solid transparent;
                        border-bottom: 24px solid #ffffff;
                        position: relative;
                    "><div style="
                        position: absolute;
                        top: 2px;
                        left: -10px;
                        width: 0;
                        height: 0;
                        border-left: 10px solid transparent;
                        border-right: 10px solid transparent;
                        border-bottom: 20px solid ${color};
                    "></div></div>`,
                iconSize: [24, 24],
                iconAnchor: [12, 24],
            }),
        });

        const keyLine =
            metrics.keyPercentage != null
                ? `<strong>Key pressure:</strong> ${metrics.keyPercentage}% (${metrics.pipelineRooms} pipeline keys ÷ ${metrics.openRooms} open keys)<br>`
                : '';
        const popupContent = `
            <div style="min-width: 250px; font-family: 'Inter', sans-serif;">
                <h3 style="margin: 0 0 10px 0; color: #333; font-size: 16px;">
                    ${bucket.title}
                </h3>
                <div style="font-size: 11px; color: #888; margin-bottom: 8px;">
                    ${bucket.country || ''}${bucket.market && bucket.market !== bucket.submarket ? ' · ' + bucket.market : ''}
                </div>
                <div style="background: #f8f9fa; padding: 10px; border-radius: 6px; margin-bottom: 10px;">
                    <div style="font-size: 18px; font-weight: bold; color: ${color};">
                        ${displayPct}% ${metrics.usesKeyWeighting ? 'Key' : 'Unit'} Pressure
                    </div>
                    <div style="font-size: 12px; color: #666; text-transform: uppercase;">${metrics.levelLabel}</div>
                </div>
                <div style="font-size: 12px; color: #666;">
                    <strong>Open hotels:</strong> ${metrics.openHotels}<br>
                    <strong>Pipeline hotels:</strong> ${metrics.pipelineHotels}<br>
                    <strong>Unit pressure:</strong> ${metrics.unitPercentage}% (${metrics.pipelineHotels}:${metrics.openHotels})<br>
                    ${keyLine}
                    <strong>Risk:</strong> ${metrics.riskLabel}<br>
                    <strong>Scope:</strong> Open + pipeline in submarket (current map filters)
                </div>
            </div>
        `;

        marker.bindPopup(popupContent);
        pipelineLayer.addLayer(marker);
    });

    map.addLayer(pipelineLayer);
}

// Generate infrastructure markers
async function generateInfrastructureMarkers() {
    if (infrastructureLayer) {
        map.removeLayer(infrastructureLayer);
    }

    infrastructureLayer = L.layerGroup();
    setInfrastructureEmptyState(false);

    const TIR = window.TravelInfrastructureRadar;
    if (!TIR) {
        console.error("TravelInfrastructureRadar module not loaded");
        return;
    }

    try {
        const responseData = await TIR.fetchInfrastructure(getTravelInfraApiOpts());
        if (!infrastructureTypeCounts) {
            infrastructureTypeCounts = TIR.extractTypeCounts(responseData);
            infrastructureTotalCount = TIR.getTotalCount(responseData, infrastructureTypeCounts);
        }
        renderInfrastructureFilterChips();

        const infrastructureData = TIR.parseItems(responseData);

        if (!infrastructureData.length) {
            setInfrastructureEmptyState(true);
            map.addLayer(infrastructureLayer);
            renderTravelInfrastructureLegendItems();
            return;
        }

        infrastructureData.forEach(function (item) {
            const marker = createInfrastructureMarker(item);
            if (marker) {
                infrastructureLayer.addLayer(marker);
            }
        });

        map.addLayer(infrastructureLayer);
    } catch (error) {
        console.error("Error fetching travel infrastructure data:", error);
        const fallbackData = [
            { name: "Mexico City International Airport", type: "Airport", lat: 19.4361, lng: -99.0721, city: "Mexico City", country: "Mexico" },
            { name: "Cancun International Airport", type: "Airport", lat: 21.0365, lng: -86.8771, city: "Cancun", country: "Mexico" },
        ];

        fallbackData.forEach(function (item) {
            const marker = createInfrastructureMarker(item);
            if (marker) {
                infrastructureLayer.addLayer(marker);
            }
        });

        map.addLayer(infrastructureLayer);
    }
}

async function generateDemandAnchorsMarkers() {
    if (demandAnchorsLayer) {
        map.removeLayer(demandAnchorsLayer);
    }

    demandAnchorsLayer = L.layerGroup();
    setDemandAnchorsEmptyState(false);

    const DAR = window.DemandAnchorsRadar;
    if (!DAR) {
        console.error("DemandAnchorsRadar module not loaded");
        return;
    }

    try {
        const responseData = await DAR.fetchDemandAnchors(getDemandAnchorsApiOpts());
        if (!demandAnchorsTypeCounts) {
            demandAnchorsTypeCounts = DAR.extractTypeCounts(responseData);
            demandAnchorsTotalCount = DAR.getTotalCount(responseData, demandAnchorsTypeCounts);
        }
        renderDemandAnchorsFilterChips();

        const anchorData = DAR.parseItems(responseData);
        if (!anchorData.length) {
            setDemandAnchorsEmptyState(true);
            map.addLayer(demandAnchorsLayer);
            renderDemandAnchorsLegendItems();
            return;
        }

        anchorData.forEach(function (item) {
            const marker = createDemandAnchorMarker(item);
            if (marker) {
                demandAnchorsLayer.addLayer(marker);
            }
        });

        map.addLayer(demandAnchorsLayer);
    } catch (error) {
        console.error("Error fetching demand anchors:", error);
        const msg =
            error && String(error.message || "").includes("404")
                ? "Demand Anchors API not found — restart the server (npm start) so /api/radar-map-points/demand-anchors is available."
                : "Could not load demand anchors. Check the server console and Airtable configuration.";
        setDemandAnchorsEmptyState(true, msg);
        map.addLayer(demandAnchorsLayer);
    }
}

// Debounce function to limit API calls
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

// Set up event listeners
function setupEventListeners() {
    // Search functionality - debounced search to prevent excessive API calls
    const searchInput = document.getElementById('locationSearch');
    if (searchInput) {
        // Create debounced version of applyFilters with 300ms delay
        const debouncedApplyFilters = debounce(() => {
            applyFilters();
        }, 300);
        
        searchInput.addEventListener('input', function(e) {
            currentFilters.search = e.target.value;
            debouncedApplyFilters();
        });
    }
    
    // Filter changes - all work in real-time
    const parentCompanyFilter = document.getElementById('parentCompanyFilter');
    if (parentCompanyFilter) {
        parentCompanyFilter.addEventListener('change', function(e) {
            currentFilters.parentCompany = e.target.value;
            syncTerritoryParentCompanyFilterOptions();
            applyFilters();
        });
    }
    
    const brandFilter = document.getElementById('brandFilter');
    if (brandFilter) {
        brandFilter.addEventListener('change', function(e) {
            currentFilters.brand = e.target.value;
            syncTerritoryFilterOptions();
            applyFilters();
        });
    }
    
    const statusFilter = document.getElementById('statusFilter');
    if (statusFilter) {
        statusFilter.addEventListener('change', function(e) {
            currentFilters.status = e.target.value;
            currentFilters.statuses = e.target.value ? [e.target.value] : [];
            applyFilters();
        });
    }
    
    const propertyTypeFilter = document.getElementById('propertyTypeFilter');
    if (propertyTypeFilter) {
        propertyTypeFilter.addEventListener('change', function(e) {
            currentFilters.propertyType = e.target.value;
            applyFilters();
        });
    }
    
    const regionFilter = document.getElementById('regionFilter');
    if (regionFilter) {
        regionFilter.addEventListener('change', function(e) {
            currentFilters.region = e.target.value;
            clearGeographyFiltersBelow('region');
            applyFilters();
        });
    }
    
    const locationTypeFilter = document.getElementById('locationTypeFilter');
    if (locationTypeFilter) {
        locationTypeFilter.addEventListener('change', function(e) {
            currentFilters.locationType = e.target.value;
            applyFilters();
        });
    }

    const drawerFilterBindings = [
        ['hotelTypeFilter', 'hotelType'],
        ['hotelServiceModelFilter', 'hotelServiceModel'],
        ['operationTypeFilter', 'operationType'],
        ['managementCompanyFilter', 'managementCompany'],
    ];
    drawerFilterBindings.forEach(function ([selectId, filterKey]) {
        const node = document.getElementById(selectId);
        if (!node) return;
        node.addEventListener('change', function (e) {
            currentFilters[filterKey] = e.target.value;
            applyFilters();
        });
    });

    const countryFilter = document.getElementById('countryFilter');
    if (countryFilter) {
        countryFilter.addEventListener('change', function (e) {
            currentFilters.country = e.target.value;
            clearGeographyFiltersBelow('country');
            applyFilters();
        });
    }

    const marketFilter = document.getElementById('marketFilter');
    if (marketFilter) {
        marketFilter.addEventListener('change', function (e) {
            currentFilters.market = e.target.value;
            clearGeographyFiltersBelow('market');
            applyFilters();
        });
    }

    const submarketFilter = document.getElementById('submarketFilter');
    if (submarketFilter) {
        submarketFilter.addEventListener('change', function (e) {
            if (!currentFilters.market) {
                e.target.value = '';
                currentFilters.submarket = '';
                return;
            }
            currentFilters.submarket = e.target.value;
            applyFilters();
        });
    }

    const debouncedApplyFilters = debounce(function () {
        applyFilters();
    }, 300);

    const roomsMinFilter = document.getElementById('roomsMinFilter');
    if (roomsMinFilter) {
        roomsMinFilter.addEventListener('input', function (e) {
            currentFilters.roomsMin = e.target.value;
            debouncedApplyFilters();
        });
        roomsMinFilter.addEventListener('change', function (e) {
            currentFilters.roomsMin = e.target.value;
            applyFilters();
        });
    }

    const roomsMaxFilter = document.getElementById('roomsMaxFilter');
    if (roomsMaxFilter) {
        roomsMaxFilter.addEventListener('input', function (e) {
            currentFilters.roomsMax = e.target.value;
            debouncedApplyFilters();
        });
        roomsMaxFilter.addEventListener('change', function (e) {
            currentFilters.roomsMax = e.target.value;
            applyFilters();
        });
    }
}

// Create Travel Infrastructure Marker
function createInfrastructureMarker(item) {
    const TIR = window.TravelInfrastructureRadar;
    const iconKey = item.mapIconType || item.pointType || item.type || "Unknown";
    const color = TIR ? TIR.getInfraColor(item) : "#9c27b0";
    
    // Create a pentagon marker for infrastructure data - unique shape not used by other markers
    const pentagonIcon = L.divIcon({
        className: 'custom-pentagon-marker',
        html: `<div style="
            width: 16px; 
            height: 16px; 
            position: relative;
        "><div style="
            position: absolute;
            top: 0;
            left: 0;
            width: 16px;
            height: 16px;
            background: #fff;
            clip-path: polygon(50% 0%, 100% 38%, 82% 100%, 18% 100%, 0% 38%);
            box-shadow: 0 2px 6px rgba(0,0,0,0.3);
        "></div><div style="
            position: absolute;
            top: 2px;
            left: 2px;
            width: 12px;
            height: 12px;
            background: ${color};
            clip-path: polygon(50% 0%, 100% 38%, 82% 100%, 18% 100%, 0% 38%);
        "></div></div>`,
        iconSize: [16, 16],
        iconAnchor: [8, 8]
    });
    
    const lat = item.lat != null ? item.lat : item.latitude;
    const lng = item.lng != null ? item.lng : item.longitude;
    if (!Number.isFinite(Number(lat)) || !Number.isFinite(Number(lng))) {
        return null;
    }

    const marker = L.marker([lat, lng], { icon: pentagonIcon });

    const popupContent = TIR
        ? TIR.buildPopupHtml(item)
        : "<div>" + (item.name || "Unknown") + "</div>";
    
    marker.bindPopup(popupContent);
    return marker;
}

function createDemandAnchorMarker(item) {
    const DAR = window.DemandAnchorsRadar;
    const color = DAR ? DAR.getAnchorColor(item) : "#ff9800";

    const diamondIcon = L.divIcon({
        className: 'custom-diamond-marker',
        html: `<div style="width:16px;height:16px;position:relative;"><div style="position:absolute;top:0;left:0;width:16px;height:16px;background:#fff;clip-path:polygon(50% 0%,100% 50%,50% 100%,0% 50%);box-shadow:0 2px 6px rgba(0,0,0,0.3);"></div><div style="position:absolute;top:2px;left:2px;width:12px;height:12px;background:${color};clip-path:polygon(50% 0%,100% 50%,50% 100%,0% 50%);"></div></div>`,
        iconSize: [16, 16],
        iconAnchor: [8, 8]
    });

    const lat = item.lat != null ? item.lat : item.latitude;
    const lng = item.lng != null ? item.lng : item.longitude;
    if (!Number.isFinite(Number(lat)) || !Number.isFinite(Number(lng))) {
        return null;
    }

    const marker = L.marker([lat, lng], { icon: diamondIcon });
    const popupContent = DAR
        ? DAR.buildPopupHtml(item)
        : "<div>" + (item.name || "Unknown") + "</div>";
    marker.bindPopup(popupContent);
    return marker;
}

// Initialize tooltips
function initializeTooltips() {
    // Add click event listeners to all info icons
    const infoIcons = document.querySelectorAll('.info-icon');
    
    infoIcons.forEach((icon, index) => {
        icon.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation(); // Prevent event bubbling
            
            // Get the tooltip content
            const tooltipContent = this.parentElement.querySelector('.tooltip-content');
            if (!tooltipContent) {
                console.error('Tooltip content not found');
                return;
            }
            
            // Get the dedicated tooltip container
            const tooltipContainer = document.getElementById('tooltipContainer');
            if (!tooltipContainer) {
                console.error('Tooltip container not found');
                return;
            }
            
            // Clear any existing tooltip
            tooltipContainer.innerHTML = '';
            
            // Clone and show the tooltip content
            const clonedTooltip = tooltipContent.cloneNode(true);
            clonedTooltip.style.visibility = 'visible';
            clonedTooltip.style.opacity = '1';
            clonedTooltip.style.display = 'block';
            clonedTooltip.style.position = 'relative';
            clonedTooltip.style.pointerEvents = 'auto';
            
            // Add close button
            const closeButton = document.createElement('button');
            closeButton.className = 'tooltip-close-btn';
            closeButton.innerHTML = '×';
            closeButton.onclick = function(e) {
                e.preventDefault();
                e.stopPropagation();
                tooltipContainer.innerHTML = '';
            };
            
            clonedTooltip.appendChild(closeButton);
            tooltipContainer.appendChild(clonedTooltip);
        });
    });
    
    // Close tooltips when clicking outside
    document.addEventListener('click', function(e) {
        if (!e.target.closest('.info-tooltip')) {
            const tooltipContainer = document.getElementById('tooltipContainer');
            if (tooltipContainer) {
                tooltipContainer.innerHTML = '';
            }
        }
    });
}

// Initialize toggle slider click handlers
function initializeToggleSliders() {
    // White Space toggle
    const whiteSpaceSlider = document.querySelector('#whiteSpaceToggle').nextElementSibling;
    if (whiteSpaceSlider) {
        whiteSpaceSlider.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            const checkbox = document.getElementById('whiteSpaceToggle');
            checkbox.checked = !checkbox.checked;
            toggleWhiteSpace();
        });
    }

    const territorySlider = document.querySelector('#brandTerritoryToggle') && document.querySelector('#brandTerritoryToggle').nextElementSibling;
    if (territorySlider) {
        territorySlider.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            const checkbox = document.getElementById('brandTerritoryToggle');
            if (!checkbox) return;
            checkbox.checked = !checkbox.checked;
            toggleBrandTerritory();
        });
    }
    
    // Penetration toggle
    const penetrationSlider = document.querySelector('#penetrationToggle').nextElementSibling;
    if (penetrationSlider) {
        penetrationSlider.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            const checkbox = document.getElementById('penetrationToggle');
            checkbox.checked = !checkbox.checked;
            togglePenetrationHeatmap();
        });
    }
    
    // Pipeline toggle
    const pipelineSlider = document.querySelector('#pipelineToggle').nextElementSibling;
    if (pipelineSlider) {
        pipelineSlider.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            const checkbox = document.getElementById('pipelineToggle');
            checkbox.checked = !checkbox.checked;
            togglePipelinePressure();
        });
    }
    
    // Infrastructure toggle
    const infrastructureSlider = document.querySelector('#infrastructureToggle').nextElementSibling;
    if (infrastructureSlider) {
        infrastructureSlider.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            const checkbox = document.getElementById('infrastructureToggle');
            checkbox.checked = !checkbox.checked;
            toggleTravelInfrastructure();
        });
    }
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    initializeMap();
    setupEventListeners();
    initializeTooltips();
    initializeToggleSliders();
    initializeLegend();
    notifyRadarFilterBadge();
    
    // Add cleanup on page unload
    window.addEventListener('beforeunload', cleanup);
});

// Initialize legend to default state
function initializeLegend() {
    
    // Ensure Chain Scale section is hidden by default using CSS class
    const mapLegend = document.querySelector('.map-legend');
    if (mapLegend) {
        mapLegend.classList.remove('chain-scale-enabled');
    }
    
    // Ensure Chain Scale toggle is unchecked
    const chainScaleToggle = document.getElementById('chainScaleToggle');
    if (chainScaleToggle) {
        chainScaleToggle.checked = false;
    }
}