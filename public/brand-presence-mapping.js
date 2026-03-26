// White Space Finder Tool JavaScript

let map;
let markersCluster;
let currentMarkers = [];
let hotelData = [];
let whiteSpaceLayer = null;
let penetrationLayer = null;
let pipelineLayer = null;
let infrastructureLayer = null;
let allHotels = [];
let currentFilteredHotels = [];


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
let currentFilters = { parentCompany: '', brand: '', status: '', propertyType: '', region: '', locationType: '', search: '' };

// Toggle states
let isHotelVisibilityEnabled = true;
let isWhiteSpaceVisible = false;
let isPenetrationVisible = false;
let isPipelineVisible = false;
let isInfrastructureVisible = false;
let isChainScaleView = false;

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
        attribution: '© OpenStreetMap contributors'
    }).addTo(map);
    
    // Initialize marker cluster with optimized settings for performance
    markersCluster = L.markerClusterGroup({
        maxClusterRadius: 60,        // Balanced clustering for performance
        spiderfyOnMaxZoom: true,     // Show individual markers when zoomed in
        showCoverageOnHover: false,  // Disabled for better performance
        zoomToBoundsOnClick: true,   // Zoom to bounds when clicking cluster
        disableClusteringAtZoom: 12, // Disable clustering at zoom level 12 and above
        chunkedLoading: true,        // Load markers in chunks for better performance
        removeOutsideVisibleBounds: true, // Remove markers outside view for performance
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

// Load hotel data
async function loadHotelData() {
    try {
        showLoading(true);
        showSystemStatus('Loading hotel data...', '3-5 seconds');
        
        // Fetch all data from API with high limit
        const response = await fetch('/api/brand-presence?limit=100000', {
            headers: {
                'ngrok-skip-browser-warning': 'true'
            }
        });
        const result = await response.json();
        
        if (result.success) {
            updateSystemStatus('Processing hotel data...', '1-2 seconds');
            allHotels = result.hotels;
            hotelData = [...allHotels];
            currentFilteredHotels = [...allHotels];
            if (result.skippedNoCoordinates) console.warn(result.skippedNoCoordinates + " Airtable records have no coordinates and are not shown on the map.");
            
            updateSystemStatus('Displaying hotels on map...', '1 second');
            // Display all hotels initially
            await displayHotels(hotelData);
            updateStatistics(hotelData);
            updateBrandDistribution(hotelData);
            generateInsights(hotelData);
            updateAllDropdowns(hotelData);
        } else {
            throw new Error('API returned error: ' + result.error);
        }
    } catch (error) {
        console.error('Error loading hotel data:', error);
        updateSystemStatus('Loading mock data...');
        
        // Fallback to mock data if API fails
        const mockData = generateMockHotelData();
        allHotels = mockData;
        hotelData = [...mockData];
        
        updateSystemStatus('Displaying hotels on map...');
        // Display all hotels initially
        await displayHotels(hotelData);
        updateStatistics(hotelData);
        updateBrandDistribution(hotelData);
        generateInsights(hotelData);
        updateAllDropdowns(hotelData);
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
    // Process hotels in smaller batches for better responsiveness
    const batchSize = 50;
    console.log(`Processing ${hotels.length} hotels in batches of ${batchSize}`);
    for (let i = 0; i < hotels.length; i += batchSize) {
        const batch = hotels.slice(i, i + batchSize);
        batch.forEach((hotel, index) => {
        const marker = createHotelMarker(hotel);
        if (marker) {
            markersCluster.addLayer(marker);
            currentMarkers.push(marker);
            markersAdded++;
        } else {
            skippedInvalid++;
        }
        });
        
        // Update progress and allow UI to update between batches
        if (i + batchSize < hotels.length) {
            const progress = Math.round(((i + batchSize) / hotels.length) * 100);
            updateSystemStatus(`Displaying hotels on map... ${progress}%`, '1 second');
            await new Promise(resolve => setTimeout(resolve, 10));
        }
    }
    
    // Ensure cluster is added to map
    if (!map.hasLayer(markersCluster)) {
        map.addLayer(markersCluster);
    }
    
    // Force marker visibility by ensuring cluster is properly added
    console.log(`Completed processing: ${markersAdded} markers added, ${skippedInvalid} skipped`);
    
}

// Create hotel marker
function createHotelMarker(hotel) {
    // Validate hotel data
    if (!hotel || !hotel.lat || !hotel.lng || isNaN(hotel.lat) || isNaN(hotel.lng)) {
        return null;
    }
    
    // Use chain scale color if toggle is enabled, otherwise use status color
    let markerColor;
    if (isChainScaleView) {
        // Check if this is an Independent hotel first
        if (hotel.brand === 'Independent' || hotel.brand === 'independent') {
            markerColor = getChainScaleColor('Independent');
        }
        // If propertyType is missing or empty, fall back to status colors
        else if (!hotel.propertyType || hotel.propertyType === 'Unknown' || hotel.propertyType === '') {
            markerColor = getMarkerColor(hotel.status);
        } else {
            markerColor = getChainScaleColor(hotel.propertyType);
        }
    } else {
        markerColor = getMarkerColor(hotel.status);
    }
    
    // Apply density filtering at lower zoom levels for cleaner appearance
    // But only if we have many markers to avoid hiding all markers when filtered
    const currentZoom = map.getZoom();
    const totalFilteredHotels = currentFilteredHotels ? currentFilteredHotels.length : 0;
    
    // Only apply density filtering if we have more than 100 markers AND we're showing all hotels
    // Never apply density filtering when showing filtered results
    const isShowingAllHotels = totalFilteredHotels === allHotels.length;
    if (totalFilteredHotels > 100 && isShowingAllHotels) {
        if (currentZoom <= 4 && Math.random() > 0.5) {
            return null; // Hide 50% only at very low zoom (zoom 5+ shows all)
        }
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
    
    const marker = L.circleMarker([hotel.lat, hotel.lng], {
        radius: radius,
        fillColor: markerColor,
        color: '#ffffff',
        weight: 2,
        opacity: 1.0,        // Full opacity for better visibility
        fillOpacity: 0.8     // Higher fill opacity for better visibility
    });
    
    // Add popup
    const popupContent = createPopupContent(hotel);
    marker.bindPopup(popupContent);
    
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

function showSystemStatus(message = 'Processing...', timeEstimate = '2-3 seconds') {
    const statusElement = document.getElementById('systemStatus');
    const statusText = statusElement.querySelector('.status-text');
    const statusTime = statusElement.querySelector('.status-time');
    const progressBar = statusElement.querySelector('.status-progress-bar');
    
    // Update content
    statusText.querySelector('div:first-child').textContent = message;
    statusTime.textContent = `Estimated time: ${timeEstimate}`;
    
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
        if (timeEstimate) {
            statusTime.textContent = `Estimated time: ${timeEstimate}`;
        }
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
                    <span style="color: #ffffff;">${hotel.parentCompany}</span>
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
    const hasActiveFilters = currentFilters.parentCompany || currentFilters.brand || 
                           currentFilters.status || currentFilters.propertyType || 
                           currentFilters.region || currentFilters.locationType || 
                           currentFilters.search;
    
    if (!hasActiveFilters) {
        // Show all data immediately without processing
        currentFilteredHotels = [...allHotels];
        displayHotels(allHotels);
        updateStatistics(allHotels);
        updateBrandDistribution(allHotels);
        generateInsights(allHotels);
        updateAllDropdowns(allHotels);
        return;
    }
    
    // Apply filters immediately - no debouncing for Excel-like behavior
    let filteredHotels = [...hotelData];
    
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
    if (currentFilters.status) {
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
            hotel.region === currentFilters.region
        );
    }
    
    // Apply location type filter
    if (currentFilters.locationType) {
        filteredHotels = filteredHotels.filter(hotel => 
            hotel.locationType === currentFilters.locationType
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
    updateStatistics(filteredHotels);
    updateBrandDistribution(filteredHotels);
    generateInsights(filteredHotels);
    } catch (error) {
        console.error('Error updating display:', error);
        // Fallback: show basic statistics
        updateStatistics(filteredHotels);
    }
    
    
    // If we have hotels, zoom to the concentrated area initially
    if (filteredHotels.length > 0) {
        const concentrationArea = findHighestConcentrationArea();
        if (concentrationArea) {
            const offsetLat = 0.3; // Increased offset to center hotels on screen
            const adjustedCenter = [concentrationArea.center.lat + offsetLat, concentrationArea.center.lng];
            map.setView(adjustedCenter, 16); // Reduced zoom level for better performance
        }
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
}

// Check if any filters are active
function hasActiveFilters() {
    return currentFilters.parentCompany ||
           currentFilters.brand || 
           currentFilters.status ||
           currentFilters.propertyType ||
           currentFilters.region ||
           currentFilters.locationType ||
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
    const uniqueLocationTypes = [...new Set(dataSource.map(hotel => hotel.locationType).filter(Boolean))].sort();
    
    // Update all dropdowns - simple and fast
    updateDropdownOptions('parentCompanyFilter', uniqueParentCompanies, 'All Parent Companies');
    updateDropdownOptions('brandFilter', uniqueBrands, 'All Brands');
    updateDropdownOptions('statusFilter', uniqueStatuses, 'All Statuses');
    updateDropdownOptions('propertyTypeFilter', uniquePropertyTypes, 'All Property Types');
    updateDropdownOptions('regionFilter', uniqueRegions, 'All Regions');
    updateDropdownOptions('locationTypeFilter', uniqueLocationTypes, 'All Location Types');
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
        optionElement.textContent = option;
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
        } else if (dropdownId === 'propertyTypeFilter') {
            currentFilters.propertyType = '';
        } else if (dropdownId === 'regionFilter') {
            currentFilters.region = '';
        } else if (dropdownId === 'locationTypeFilter') {
            currentFilters.locationType = '';
        }
    }
}

// Reset view
function resetView() {
    // Show loading state on reset button
    const resetButton = document.querySelector('button[onclick="resetView()"]');
    if (resetButton) {
        const originalText = resetButton.textContent;
        resetButton.textContent = 'Resetting...';
        resetButton.disabled = true;
        
        // Restore button after reset
        setTimeout(() => {
            resetButton.textContent = originalText;
            resetButton.disabled = false;
        }, 100);
    }
    
    // Set flag to prevent applyFilters from running
    isResetting = true;
    
    // Reset filters object
    currentFilters = { parentCompany: '', brand: '', status: '', propertyType: '', region: '', locationType: '', search: '' };
    
    // Reset form inputs in batch
    const formElements = [
        'locationSearch', 'parentCompanyFilter', 'brandFilter', 'statusFilter', 
        'propertyTypeFilter', 'regionFilter', 'locationTypeFilter'
    ];
    formElements.forEach(id => {
        const element = document.getElementById(id);
        if (element) element.value = '';
    });
    
    // Reset overlay toggles in batch
    const toggles = [
        { id: 'hotelVisibilityToggle', value: true },
        { id: 'whiteSpaceToggle', value: false },
        { id: 'penetrationToggle', value: false },
        { id: 'pipelineToggle', value: false },
        { id: 'infrastructureToggle', value: false },
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
    hidePenetrationHeatmap();
    hidePipelinePressure();
    hideTravelInfrastructure();
    
    // Reset overlay visibility flags
    isHotelVisibilityEnabled = true;
    isWhiteSpaceVisible = false;
    isPenetrationVisible = false;
    isPipelineVisible = false;
    isInfrastructureVisible = false;
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
        });
    });
}

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

// Toggle functions
function toggleHotelVisibility() {
    const toggle = document.getElementById('hotelVisibilityToggle');
    isHotelVisibilityEnabled = toggle.checked;
    
    if (isHotelVisibilityEnabled) {
        displayHotels(currentFilteredHotels);
    } else {
        markersCluster.clearLayers();
        currentMarkers = [];
    }
}

// Toggle chain scale view
function toggleChainScaleView() {
    const toggle = document.getElementById('chainScaleToggle');
    isChainScaleView = toggle.checked;
    
    
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
}

// Toggle functions for overlays
function toggleWhiteSpace() {
    const toggle = document.getElementById('whiteSpaceToggle');
    if (!toggle) {
        console.error('whiteSpaceToggle element not found');
        return;
    }
    
    // Show loading state for white space generation
    showWhiteSpaceLoading(true);
    
    isWhiteSpaceVisible = toggle.checked;
    
    // Show/hide legend section
    const whiteSpaceSection = document.getElementById('whiteSpaceSection');
    const whiteSpaceLegend = document.getElementById('whiteSpaceLegend');
    const whiteSpaceLegend2 = document.getElementById('whiteSpaceLegend2');
    const whiteSpaceLegend3 = document.getElementById('whiteSpaceLegend3');
    
    if (isWhiteSpaceVisible) {
        generateWhiteSpaceMarkers();
        if (whiteSpaceSection) whiteSpaceSection.style.setProperty('display', 'block', 'important');
        if (whiteSpaceLegend) whiteSpaceLegend.style.setProperty('display', 'flex', 'important');
        if (whiteSpaceLegend2) whiteSpaceLegend2.style.setProperty('display', 'flex', 'important');
        if (whiteSpaceLegend3) whiteSpaceLegend3.style.setProperty('display', 'flex', 'important');
    } else {
        if (whiteSpaceLayer) {
            map.removeLayer(whiteSpaceLayer);
            whiteSpaceLayer = null;
        }
        if (whiteSpaceSection) whiteSpaceSection.style.setProperty('display', 'none', 'important');
        if (whiteSpaceLegend) whiteSpaceLegend.style.setProperty('display', 'none', 'important');
        if (whiteSpaceLegend2) whiteSpaceLegend2.style.setProperty('display', 'none', 'important');
        if (whiteSpaceLegend3) whiteSpaceLegend3.style.setProperty('display', 'none', 'important');
    }
    
    // Hide white space loading state
    showWhiteSpaceLoading(false);
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
}

async function toggleTravelInfrastructure() {
    const toggle = document.getElementById('infrastructureToggle');
    isInfrastructureVisible = toggle.checked;
    
    // Show/hide legend section
    const infrastructureSection = document.getElementById('infrastructureSection');
    const infrastructureLegend = document.getElementById('infrastructureLegend');
    const infrastructureLegend2 = document.getElementById('infrastructureLegend2');
    const infrastructureLegend3 = document.getElementById('infrastructureLegend3');
    
    if (isInfrastructureVisible) {
        await generateInfrastructureMarkers();
        // Show legend
        if (infrastructureSection) infrastructureSection.style.setProperty('display', 'block', 'important');
        if (infrastructureLegend) infrastructureLegend.style.setProperty('display', 'flex', 'important');
        if (infrastructureLegend2) infrastructureLegend2.style.setProperty('display', 'flex', 'important');
        if (infrastructureLegend3) infrastructureLegend3.style.setProperty('display', 'flex', 'important');
    } else {
        hideTravelInfrastructure();
        // Hide legend
        if (infrastructureSection) infrastructureSection.style.setProperty('display', 'none', 'important');
        if (infrastructureLegend) infrastructureLegend.style.setProperty('display', 'none', 'important');
        if (infrastructureLegend2) infrastructureLegend2.style.setProperty('display', 'none', 'important');
        if (infrastructureLegend3) infrastructureLegend3.style.setProperty('display', 'none', 'important');
    }
}

// Generate white space markers - Advanced White Space Detector
function generateWhiteSpaceMarkers() {
    if (whiteSpaceLayer) {
        map.removeLayer(whiteSpaceLayer);
    }
    
    whiteSpaceLayer = L.layerGroup();
    
    // Use currentFilteredHotels if available, otherwise use allHotels
    const hotelsToUse = currentFilteredHotels.length > 0 ? currentFilteredHotels : allHotels;
    
    // Only show white space markers if there are hotels
    if (hotelsToUse.length === 0) {
        return;
    }
    
    // Define major cities in CALA region with their coordinates and tier levels
    const majorCities = {
        // Tier 1 Cities (Major metropolitan areas)
        'Mexico City, Mexico': { lat: 19.4326, lng: -99.1332, tier: 1, population: '22M' },
        'São Paulo, Brazil': { lat: -23.5505, lng: -46.6333, tier: 1, population: '22M' },
        'Buenos Aires, Argentina': { lat: -34.6118, lng: -58.3960, tier: 1, population: '15M' },
        'Lima, Peru': { lat: -12.0464, lng: -77.0428, tier: 1, population: '11M' },
        'Bogotá, Colombia': { lat: 4.7110, lng: -74.0721, tier: 1, population: '11M' },
        'Santiago, Chile': { lat: -33.4489, lng: -70.6693, tier: 1, population: '7M' },
        
        // Tier 2 Cities (Important regional centers)
        'Guadalajara, Mexico': { lat: 20.6597, lng: -103.3496, tier: 2, population: '5M' },
        'Monterrey, Mexico': { lat: 25.6866, lng: -100.3161, tier: 2, population: '5M' },
        'Rio de Janeiro, Brazil': { lat: -22.9068, lng: -43.1729, tier: 2, population: '6M' },
        'Brasília, Brazil': { lat: -15.7801, lng: -47.9292, tier: 2, population: '3M' },
        'Medellín, Colombia': { lat: 6.2442, lng: -75.5812, tier: 2, population: '2.5M' },
        'Cali, Colombia': { lat: 3.4516, lng: -76.5320, tier: 2, population: '2.5M' },
        'Valparaíso, Chile': { lat: -33.0458, lng: -71.6197, tier: 2, population: '1M' },
        'Córdoba, Argentina': { lat: -31.4201, lng: -64.1888, tier: 2, population: '1.5M' },
        'Rosario, Argentina': { lat: -32.9442, lng: -60.6505, tier: 2, population: '1.5M' },
        'Arequipa, Peru': { lat: -16.4090, lng: -71.5375, tier: 2, population: '1M' },
        'Trujillo, Peru': { lat: -8.1116, lng: -79.0288, tier: 2, population: '1M' },
        'Santo Domingo, Dominican Republic': { lat: 18.4861, lng: -69.9312, tier: 2, population: '3M' },
        'San Juan, Puerto Rico': { lat: 18.4655, lng: -66.1057, tier: 2, population: '2M' },
        'Havana, Cuba': { lat: 23.1136, lng: -82.3666, tier: 2, population: '2M' },
        'San José, Costa Rica': { lat: 9.9281, lng: -84.0907, tier: 2, population: '1.5M' },
        'Panama City, Panama': { lat: 8.5380, lng: -80.7821, tier: 2, population: '1.5M' },
        'Guatemala City, Guatemala': { lat: 14.6349, lng: -90.5069, tier: 2, population: '3M' },
        'Tegucigalpa, Honduras': { lat: 14.0723, lng: -87.1921, tier: 2, population: '1M' },
        'Managua, Nicaragua': { lat: 12.1150, lng: -86.2362, tier: 2, population: '1.5M' },
        'San Salvador, El Salvador': { lat: 13.6929, lng: -89.2182, tier: 2, population: '1.5M' },
        'Caracas, Venezuela': { lat: 10.4806, lng: -66.9036, tier: 2, population: '3M' },
        'Maracaibo, Venezuela': { lat: 10.6427, lng: -71.6125, tier: 2, population: '2M' },
        'Valencia, Venezuela': { lat: 10.1621, lng: -68.0077, tier: 2, population: '1.5M' },
        'Quito, Ecuador': { lat: -0.1807, lng: -78.4678, tier: 2, population: '2.5M' },
        'Guayaquil, Ecuador': { lat: -2.1894, lng: -79.8890, tier: 2, population: '2.5M' },
        'La Paz, Bolivia': { lat: -16.5000, lng: -68.1500, tier: 2, population: '1M' },
        'Santa Cruz, Bolivia': { lat: -17.7833, lng: -63.1833, tier: 2, population: '1.5M' },
        'Asunción, Paraguay': { lat: -25.2637, lng: -57.5759, tier: 2, population: '1M' },
        'Montevideo, Uruguay': { lat: -34.9011, lng: -56.1645, tier: 2, population: '1.5M' }
    };
    
    // Group existing hotels by city
    const cityGroups = {};
    hotelsToUse.forEach(hotel => {
        if (hotel.city && hotel.lat && hotel.lng) {
            const cityKey = `${hotel.city}, ${hotel.country}`;
            if (!cityGroups[cityKey]) {
                cityGroups[cityKey] = {
                    city: hotel.city,
                    country: hotel.country,
                    lat: hotel.lat,
                    lng: hotel.lng,
                    hotels: []
                };
            }
            cityGroups[cityKey].hotels.push(hotel);
        }
    });
    
    // Analyze white space opportunities
    let highCount = 0, mediumCount = 0, lowCount = 0;
    
    // Check each major city for white space opportunities
    Object.entries(majorCities).forEach(([cityKey, cityInfo]) => {
        const existingHotels = cityGroups[cityKey] ? cityGroups[cityKey].hotels : [];
        const hotelCount = existingHotels.length;
        
        // Determine opportunity level based on tier and hotel count
        let opportunityLevel, color;
        if (hotelCount === 0) {
            // No hotels in this major city - high opportunity
            opportunityLevel = 'high';
            color = '#4CAF50'; // Green
            highCount++;
        } else if (hotelCount <= 2 && cityInfo.tier === 1) {
            // Tier 1 city with limited hotels - high opportunity
            opportunityLevel = 'high';
            color = '#4CAF50'; // Green
            highCount++;
        } else if (hotelCount <= 3 && cityInfo.tier === 2) {
            // Tier 2 city with limited hotels - medium opportunity
            opportunityLevel = 'medium';
            color = '#ffeb3b'; // Yellow
            mediumCount++;
        } else if (hotelCount <= 5) {
            // Some hotels but still opportunity - low opportunity
            opportunityLevel = 'low';
            color = '#f44336'; // Red
            lowCount++;
        } else {
            // Too many hotels - no opportunity
            return;
        }
        
        // Create hexagon marker using divIcon to match legend
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
        
        const marker = L.marker([cityInfo.lat, cityInfo.lng], { icon: hexagonIcon });
        
        const popupContent = `
            <div style="min-width: 250px; font-family: 'Inter', sans-serif;">
                <h3 style="margin: 0 0 10px 0; color: #333; font-size: 16px;">
                    ${cityKey}
                </h3>
                <div style="background: #f0fdf4; padding: 10px; border-radius: 6px; margin-bottom: 10px;">
                    <div style="font-size: 18px; font-weight: bold; color: ${color};">
                        ${opportunityLevel.charAt(0).toUpperCase() + opportunityLevel.slice(1)} Opportunity
                    </div>
                    <div style="font-size: 12px; color: #666; text-transform: uppercase;">White Space</div>
                </div>
                <div style="font-size: 12px; color: #666;">
                    <strong>Tier:</strong> ${cityInfo.tier} City<br>
                    <strong>Population:</strong> ${cityInfo.population}<br>
                    <strong>Hotels in area:</strong> ${hotelCount}<br>
                    <strong>Competition level:</strong> ${hotelCount === 0 ? 'None' : hotelCount <= 2 ? 'Low' : hotelCount <= 5 ? 'Medium' : 'High'}<br>
                    <strong>Development potential:</strong> ${opportunityLevel.charAt(0).toUpperCase() + opportunityLevel.slice(1)}
                </div>
            </div>
        `;
        
        marker.bindPopup(popupContent);
        whiteSpaceLayer.addLayer(marker);
    });
    
    map.addLayer(whiteSpaceLayer);
}


// STANDALONE BRAND PENETRATION CALCULATOR - NO DEPENDENCIES
function generatePenetrationMarkers() {
    // Clear any existing layer
    if (penetrationLayer) {
        map.removeLayer(penetrationLayer);
    }
    penetrationLayer = L.layerGroup();
    
    // Try multiple ways to get the data
    const allHotelData = window.allHotels || allHotels || [];
    
    if (allHotelData.length === 0) {
        return;
    }
    
    // Group hotels by city - STANDALONE LOGIC
    const cityGroups = {};
    allHotelData.forEach(hotel => {
        if (hotel.city && hotel.lat && hotel.lng) {
            const cityKey = `${hotel.city}, ${hotel.country}`;
            if (!cityGroups[cityKey]) {
                cityGroups[cityKey] = {
                    city: hotel.city,
                    country: hotel.country,
                    lat: hotel.lat,
                    lng: hotel.lng,
                    hotels: []
                };
            }
            cityGroups[cityKey].hotels.push(hotel);
        }
    });
    
    // Calculate penetration for each city - CLEAN FORMULA
    Object.values(cityGroups).forEach(cityData => {
        if (cityData.hotels.length >= 2) {
            
            // STANDALONE BRAND PENETRATION FORMULA
            const totalHotels = cityData.hotels.length;
            let brandedCount = 0;
            let independentCount = 0;
            
            // Count each hotel directly using brand field (not Affiliation)
            cityData.hotels.forEach(hotel => {
                const brand = hotel.brand;
                
                // Normalize brand values
                let normalizedBrand = brand;
                if (brand === 'unknown' || brand === 'independent' || brand === 'Independent') {
                    normalizedBrand = 'Independent';
                }
                
                // Branded: ANYTHING that's not "Independent"
                if (normalizedBrand && normalizedBrand !== 'Independent') {
                    brandedCount++;
                } else {
                    independentCount++;
                }
            });
            
            // Calculate percentage - SIMPLE MATH
            const penetrationPercentage = Math.round((brandedCount / totalHotels) * 100);
            
            
            // Determine color based on percentage
            let color, bgColor, level;
            if (penetrationPercentage >= 70) {
                color = '#ff6b6b';
                bgColor = '#fef2f2';
                level = 'High';
            } else if (penetrationPercentage >= 40) {
                color = '#ffd93d';
                bgColor = '#fffbeb';
                level = 'Medium';
            } else {
                color = '#6bcf7f';
                bgColor = '#f0fdf4';
                level = 'Low';
            }
            
            // Create marker
            const marker = L.marker([cityData.lat, cityData.lng], {
                icon: L.divIcon({
                    className: 'custom-square-marker',
                    html: `<div style="width: 20px; height: 20px; background: ${color}; border: 2px solid #ffffff; border-radius: 3px; box-shadow: 0 2px 6px rgba(0,0,0,0.3);"></div>`,
                    iconSize: [20, 20],
                    iconAnchor: [10, 10]
                })
            });
            
            // Create popup with CLEAN DATA
            const popupContent = `
                <div style="min-width: 250px; font-family: 'Inter', sans-serif;">
                    <h3 style="margin: 0 0 10px 0; color: #333; font-size: 16px;">
                        ${cityData.city}, ${cityData.country}
                    </h3>
                    <div style="background: ${bgColor}; padding: 10px; border-radius: 6px; margin-bottom: 10px;">
                        <div style="font-size: 18px; font-weight: bold; color: ${color};">
                            ${penetrationPercentage}% Branded
                        </div>
                        <div style="font-size: 12px; color: #666; text-transform: uppercase;">${level} Penetration</div>
                    </div>
                    <div style="font-size: 12px; color: #666;">
                        <strong>Total Hotels:</strong> ${totalHotels}<br>
                        <strong>Branded Hotels:</strong> ${brandedCount}<br>
                        <strong>Independent Hotels:</strong> ${independentCount}<br>
                        <strong>Penetration:</strong> ${penetrationPercentage}%<br>
                        <strong>Formula:</strong> (${brandedCount} ÷ ${totalHotels}) × 100 = ${penetrationPercentage}%
                    </div>
                </div>
            `;
            
            marker.bindPopup(popupContent);
            penetrationLayer.addLayer(marker);
        }
    });
    
    map.addLayer(penetrationLayer);
}

// Generate pipeline markers
// STANDALONE PIPELINE PRESSURE CALCULATOR
function generatePipelineMarkers() {
    // Clear any existing layer
    if (pipelineLayer) {
        map.removeLayer(pipelineLayer);
    }
    pipelineLayer = L.layerGroup();
    
    // Get fresh data directly from the global allHotels array
    const allHotelData = window.allHotels || allHotels || [];
    
    if (allHotelData.length === 0) {
        return;
    }
    
    // Group hotels by city - STANDALONE LOGIC
    const cityGroups = {};
    allHotelData.forEach(hotel => {
        if (hotel.city && hotel.lat && hotel.lng) {
            const cityKey = `${hotel.city}, ${hotel.country}`;
            if (!cityGroups[cityKey]) {
                cityGroups[cityKey] = {
                    city: hotel.city,
                    country: hotel.country,
                    lat: hotel.lat,
                    lng: hotel.lng,
                    hotels: []
                };
            }
            cityGroups[cityKey].hotels.push(hotel);
        }
    });
    
    // Calculate pipeline pressure for each city - CLEAN FORMULA
    Object.values(cityGroups).forEach(cityData => {
        if (cityData.hotels.length >= 2) {
            
            // STANDALONE PIPELINE PRESSURE FORMULA
            const openHotels = cityData.hotels.filter(hotel => hotel.status === 'Open').length;
            const pipelineHotels = cityData.hotels.filter(hotel => hotel.status === 'Pipeline').length;
            
            // Skip cities with no pipeline or no open hotels
            if (pipelineHotels === 0 || openHotels === 0) {
                return;
            }
            
            // Calculate pressure ratio - Pipeline vs Open
            const pressureRatio = Math.round((pipelineHotels / openHotels) * 100);
            
            // Determine pressure level and color
            let color, level;
            if (pressureRatio >= 100) {
                color = '#ff4757';  // Red - High pressure (oversupply risk)
                level = 'High Pressure';
            } else if (pressureRatio >= 50) {
                color = '#ffa502';  // Orange - Medium pressure
                level = 'Medium Pressure';
            } else {
                color = '#2ed573';  // Green - Low pressure (healthy growth)
                level = 'Low Pressure';
            }
            
            // Create triangle marker with white border (24px to match white space markers)
            const marker = L.marker([cityData.lat, cityData.lng], {
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
                    iconAnchor: [12, 24]
                })
            });
            
            // Create popup with pressure analysis
            const popupContent = `
                <div style="min-width: 250px; font-family: 'Inter', sans-serif;">
                    <h3 style="margin: 0 0 10px 0; color: #333; font-size: 16px;">
                        ${cityData.city}, ${cityData.country}
                    </h3>
                    <div style="background: #f8f9fa; padding: 10px; border-radius: 6px; margin-bottom: 10px;">
                        <div style="font-size: 18px; font-weight: bold; color: ${color};">
                            ${pressureRatio}% Pipeline Pressure
                        </div>
                        <div style="font-size: 12px; color: #666; text-transform: uppercase;">${level}</div>
                    </div>
                    <div style="font-size: 12px; color: #666;">
                        <strong>Open Hotels:</strong> ${openHotels}<br>
                        <strong>Pipeline Hotels:</strong> ${pipelineHotels}<br>
                        <strong>Pressure Ratio:</strong> ${pipelineHotels}:${openHotels}<br>
                        <strong>Formula:</strong> (${pipelineHotels} ÷ ${openHotels}) × 100 = ${pressureRatio}%<br>
                        <strong>Risk Level:</strong> ${pressureRatio >= 100 ? 'Oversupply Risk' : pressureRatio >= 50 ? 'Moderate Growth' : 'Healthy Growth'}
                    </div>
                </div>
            `;
            
            marker.bindPopup(popupContent);
            pipelineLayer.addLayer(marker);
        }
    });
    
    map.addLayer(pipelineLayer);
}

// Generate infrastructure markers
async function generateInfrastructureMarkers() {
    if (infrastructureLayer) {
        map.removeLayer(infrastructureLayer);
    }
    
    infrastructureLayer = L.layerGroup();
    
    try {
        // Fetch real travel infrastructure data from API
        const response = await fetch('/api/travel-infrastructure', {
            headers: {
                'ngrok-skip-browser-warning': 'true'
            }
        });
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const responseData = await response.json();
        
        // Handle different possible response structures
        let infrastructureData = [];
        if (Array.isArray(responseData)) {
            infrastructureData = responseData;
        } else if (responseData.infrastructure && Array.isArray(responseData.infrastructure)) {
            infrastructureData = responseData.infrastructure;
        } else if (responseData.data && Array.isArray(responseData.data)) {
            infrastructureData = responseData.data;
        } else {
            console.error('Unexpected API response structure:', responseData);
            throw new Error('Invalid response structure');
        }
        
        // Create markers for infrastructure
        infrastructureData.forEach((item, index) => {
            const marker = createInfrastructureMarker(item);
            if (marker) {
                infrastructureLayer.addLayer(marker);
            }
        });
        
        map.addLayer(infrastructureLayer);
        
    } catch (error) {
        console.error('Error fetching travel infrastructure data:', error);
        // Fallback to sample data if API fails
        const fallbackData = [
            { name: 'Mexico City International Airport', type: 'Airport', lat: 19.4361, lng: -99.0721, city: 'Mexico City', country: 'Mexico' },
            { name: 'Cancun International Airport', type: 'Airport', lat: 21.0365, lng: -86.8771, city: 'Cancun', country: 'Mexico' }
        ];
        
        fallbackData.forEach(item => {
            const marker = createInfrastructureMarker(item);
            if (marker) {
                infrastructureLayer.addLayer(marker);
            }
        });
        
        map.addLayer(infrastructureLayer);
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
            applyFilters();
        });
    }
    
    const brandFilter = document.getElementById('brandFilter');
    if (brandFilter) {
        brandFilter.addEventListener('change', function(e) {
            currentFilters.brand = e.target.value;
            applyFilters();
        });
    }
    
    const statusFilter = document.getElementById('statusFilter');
    if (statusFilter) {
        statusFilter.addEventListener('change', function(e) {
            currentFilters.status = e.target.value;
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
}

// Create Travel Infrastructure Marker
function createInfrastructureMarker(item) {
    const infrastructureColors = {
        'Airport': '#9c27b0',      // Purple for airports
        'Cruise Port': '#e91e63',  // Pink for cruise ports
        'Convention Center': '#00bcd4'    // Cyan for convention centers
    };
    
    const color = infrastructureColors[item.type];
    
    const infrastructureIcons = {
        'Airport': '',
        'Cruise Port': '',
        'Convention Center': ''
    };
    
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
            background: ${infrastructureColors[item.type]};
            clip-path: polygon(50% 0%, 100% 38%, 82% 100%, 18% 100%, 0% 38%);
        "></div></div>`,
        iconSize: [16, 16],
        iconAnchor: [8, 8]
    });
    
    const marker = L.marker([item.lat, item.lng], { icon: pentagonIcon });
    
    // Add popup with infrastructure details
    const popupContent = `
        <div style="min-width: 250px; font-family: 'Inter', sans-serif;">
            <h3 style="margin: 0 0 10px 0; color: #333; font-size: 16px; border-bottom: 2px solid ${infrastructureColors[item.type]}; padding-bottom: 5px;">
                ${item.name}
            </h3>
            <div style="background: #f8f9fa; padding: 10px; border-radius: 6px; margin-bottom: 10px;">
                <div style="font-size: 18px; font-weight: bold; color: ${infrastructureColors[item.type]};">
                    ${infrastructureIcons[item.type]} ${item.type}
                </div>
                <div style="font-size: 12px; color: #666; text-transform: uppercase;">Travel Infrastructure</div>
            </div>
            <div style="font-size: 12px; color: #666;">
                <strong>Location:</strong> ${item.city}, ${item.country}<br>
                <strong>Type:</strong> ${item.type === 'Airport' ? 'International Airport' : 
                                      item.type === 'Cruise Port' ? 'Cruise Port Terminal' : 
                                      'Convention Center'}
            </div>
        </div>
    `;
    
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