import Airtable from "airtable";
import { buildHiltonAmenitiesDisplay } from "../lib/hilton-amenity-display.js";

const AIRTABLE_KEY = process.env.AIRTABLE_API_KEY;
const AIRTABLE_BASE = process.env.AIRTABLE_BASE_ID_ALT;
const base = (AIRTABLE_KEY && AIRTABLE_BASE)
  ? new Airtable({ apiKey: AIRTABLE_KEY }).base(AIRTABLE_BASE)
  : null;

function ensureBrandPresenceConfig(res) {
  if (base) return true;
  res.status(500).json({ error: "Missing Airtable API key or base id for brand presence" });
  return false;
}

// In-memory cache for performance optimization
const cache = new Map();
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes cache
/** Bump when API response shape or fetched fields change (invalidates stale cache). */
const CACHE_SCHEMA_VERSION = 7;

// Cache helper functions
function getCacheKey(query) {
  return `brand-presence-v${CACHE_SCHEMA_VERSION}-${JSON.stringify(query)}`;
}

function getFromCache(key) {
  const cached = cache.get(key);
  if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
    return cached.data;
  }
  return null;
}

function setCache(key, data) {
  cache.set(key, { data, timestamp: Date.now() });
}

// Background data refresh to keep cache fresh
let isRefreshing = false;
async function refreshCacheInBackground() {
  if (isRefreshing) return;
  
  isRefreshing = true;
  console.log('🔄 Background cache refresh started...');
  
  try {
    // Clear existing cache to force fresh data on next request
    cache.clear();
    console.log('✅ Background cache refresh completed - cache cleared');
  } catch (error) {
    console.error('❌ Background cache refresh failed:', error);
  } finally {
    isRefreshing = false;
  }
}

// Refresh cache every 10 minutes
setInterval(refreshCacheInBackground, 10 * 60 * 1000);

// Field mappings for brand presence data
const F = {
  hotels: {
    table: "Hotel Census", // Your actual table name
    id: "id", // Airtable record ID
    name: "name", // Your actual field name
    brand: "Affiliation", // Your actual field name
    parentCompany: "Parent Company", // Your actual field name
    status: "status", // Your actual field name
    lat: "Latitude", // Your actual field name
    lng: "Longitude", // Your actual field name
    city: "city", // Your actual field name
    country: "country", // Your actual field name
    region: "Region", // Using Region field
    locationType: "Location", // Using Location field for location type
    rooms: "rooms", // Your actual field name
    strNumber: "STR Number",
    chainScale: "Chain Scale",
    projectPhase: "project_phase",
    propertyType: "Chain Scale", // Using Chain Scale as property type
    operationType: "Operation Type",
    managementCompany: "Management Company",
    website: "Website",
    telephone: "Telephone",
    address1: "Address 1",
    address2: "Address 2",
    market: "Market",
    submarket: "Submarket",
    openDate: "Open Date",
    projectedOpenDate: "projected_open_date",
    starRating: "Star Rating",
    amenities: "Amenities",
    hotelDescription: process.env.AIRTABLE_CENSUS_DESCRIPTION_FIELD || "Hotel Description",
    hotelHeadline: process.env.AIRTABLE_CENSUS_HOTEL_HEADLINE_FIELD || "Hotel Headline",
    occupancyRate: "Occupancy Rate",
    adr: "ADR",
    revpar: "RevPAR",
    state: "State",
    postalCode: "Postal Code",
    floors: "Floors",
    totalMeetingSpace: "Total Meeting Space",
    dataConfidence: "Data Confidence",
    censusPropertyType: "Property Type",
    hotelServiceModel: "Hotel Service Model",
    allSuites: "All Suites (Y/N)",
    boutique: "Boutique (Y/N)",
    casino: "Casino (Y/N)",
    conference: "Conference (Y/N)",
    convention: "Convention (Y/N)",
    golf: "Golf (Y/N)",
    resort: "Resort (Y/N)",
    restaurant: "Restaurant (Y/N)",
    ski: "Ski (Y/N)",
    spa: "Spa (Y/N)"
  }
};

/** Fields required for map markers and filters — keep bulk load lean. */
const MAP_HOTEL_FIELDS = [
  F.hotels.name,
  F.hotels.brand,
  F.hotels.parentCompany,
  F.hotels.status,
  F.hotels.lat,
  F.hotels.lng,
  F.hotels.city,
  F.hotels.country,
  F.hotels.region,
  F.hotels.locationType,
  F.hotels.rooms,
  F.hotels.strNumber,
  F.hotels.chainScale,
  F.hotels.projectPhase,
  F.hotels.propertyType,
  F.hotels.operationType,
  F.hotels.managementCompany,
  F.hotels.market,
  F.hotels.submarket,
  F.hotels.censusPropertyType,
  F.hotels.hotelServiceModel,
];

function readTextField(fields, key) {
  const raw = fields[key];
  if (raw == null || raw === "") return null;
  if (typeof raw === "string") return raw.trim() || null;
  if (Array.isArray(raw)) {
    const joined = raw
      .map((item) => (typeof item === "string" ? item.trim() : item?.name || ""))
      .filter(Boolean)
      .join("; ");
    return joined || null;
  }
  if (typeof raw === "object" && raw.name) return String(raw.name).trim() || null;
  return String(raw).trim() || null;
}

function readNumberField(fields, key) {
  const raw = fields[key];
  if (raw == null || raw === "") return null;
  const num = Number(raw);
  return Number.isFinite(num) ? num : null;
}

function readYnFlag(fields, key) {
  const raw = readTextField(fields, key);
  if (!raw) return false;
  const normalized = raw.toLowerCase();
  return normalized === "y" || normalized === "yes" || normalized === "true" || normalized === "1";
}

function readDateField(fields, key) {
  const raw = fields[key];
  if (!raw) return null;
  return String(raw).slice(0, 10);
}

function formatHotelRecord(hotel) {
  const fields = hotel.fields || {};
  const lat = parseFloat(fields[F.hotels.lat]);
  const lng = parseFloat(fields[F.hotels.lng]);
  const hasCoords = Number.isFinite(lat) && Number.isFinite(lng) && (lat !== 0 || lng !== 0);

  const amenityFlags = {
    allSuites: readYnFlag(fields, F.hotels.allSuites),
    boutique: readYnFlag(fields, F.hotels.boutique),
    casino: readYnFlag(fields, F.hotels.casino),
    conference: readYnFlag(fields, F.hotels.conference),
    convention: readYnFlag(fields, F.hotels.convention),
    golf: readYnFlag(fields, F.hotels.golf),
    resort: readYnFlag(fields, F.hotels.resort),
    restaurant: readYnFlag(fields, F.hotels.restaurant),
    ski: readYnFlag(fields, F.hotels.ski),
    spa: readYnFlag(fields, F.hotels.spa)
  };

  return {
    id: hotel.id,
    name: readTextField(fields, F.hotels.name) || "Unknown Hotel",
    brand: readTextField(fields, F.hotels.brand) || "Unknown Brand",
    parentCompany: readTextField(fields, F.hotels.parentCompany) || "Unknown",
    status: readTextField(fields, F.hotels.status) || "unknown",
    lat: hasCoords ? lat : 0,
    lng: hasCoords ? lng : 0,
    city: readTextField(fields, F.hotels.city) || "Unknown City",
    country: readTextField(fields, F.hotels.country) || "Unknown Country",
    region: readTextField(fields, F.hotels.region) || "Unknown Region",
    locationType: readTextField(fields, F.hotels.locationType) || null,
    rooms: parseInt(fields[F.hotels.rooms], 10) || 0,
    strNumber: fields[F.hotels.strNumber] ?? null,
    chainScale: readTextField(fields, F.hotels.chainScale),
    projectPhase: readTextField(fields, F.hotels.projectPhase),
    propertyType: readTextField(fields, F.hotels.propertyType),
    operationType: readTextField(fields, F.hotels.operationType),
    managementCompany: readTextField(fields, F.hotels.managementCompany),
    website: readTextField(fields, F.hotels.website),
    telephone: readTextField(fields, F.hotels.telephone),
    address1: readTextField(fields, F.hotels.address1),
    address2: readTextField(fields, F.hotels.address2),
    market: readTextField(fields, F.hotels.market),
    submarket: readTextField(fields, F.hotels.submarket),
    openDate: readDateField(fields, F.hotels.openDate),
    projectedOpenDate: readDateField(fields, F.hotels.projectedOpenDate),
    starRating: readNumberField(fields, F.hotels.starRating),
    amenities: readTextField(fields, F.hotels.amenities),
    hotelDescription: readTextField(fields, F.hotels.hotelDescription),
    hotelHeadline: readTextField(fields, F.hotels.hotelHeadline),
    occupancyRate: readNumberField(fields, F.hotels.occupancyRate),
    adr: readNumberField(fields, F.hotels.adr),
    revpar: readNumberField(fields, F.hotels.revpar),
    state: readTextField(fields, F.hotels.state),
    postalCode: readTextField(fields, F.hotels.postalCode),
    floors: readNumberField(fields, F.hotels.floors),
    totalMeetingSpace: readNumberField(fields, F.hotels.totalMeetingSpace),
    dataConfidence: readTextField(fields, F.hotels.dataConfidence),
    censusPropertyType: readTextField(fields, F.hotels.censusPropertyType),
    hotelServiceModel: readTextField(fields, F.hotels.hotelServiceModel),
    amenityFlags
  };
}

// Single hotel detail — fresh read for detail panel (amenities, website, phone, etc.)
export async function getBrandPresenceHotelById(req, res) {
  if (!ensureBrandPresenceConfig(res)) return;
  try {
    const recordId = String(req.params.recordId || "").trim();
    if (!recordId || !recordId.startsWith("rec")) {
      return res.status(400).json({ success: false, error: "Valid Airtable record id required" });
    }

    const hotel = await base(F.hotels.table).find(recordId);
    const formatted = formatHotelRecord(hotel);
    formatted.amenitiesDisplay = buildHiltonAmenitiesDisplay(formatted);

    res.json({ success: true, hotel: formatted });
  } catch (error) {
    const status = error?.statusCode === 404 ? 404 : 500;
    console.error("Error getting brand presence hotel detail:", error);
    res.status(status).json({
      success: false,
      error: status === 404 ? "Hotel record not found" : "Internal Server Error",
      details: error.message
    });
  }
}

// Get brand presence data
export async function getBrandPresence(req, res) {
  if (!ensureBrandPresenceConfig(res)) return;
  try {
    const requestedLimit = parseInt(req.query.limit, 10);
    const limit = Number.isFinite(requestedLimit) && requestedLimit > 0 ? requestedLimit : null;
    const { brand, status, region, search, page = 0 } = req.query;
    
    // Check cache first
    const cacheKey = getCacheKey({ brand, status, region, search, limit, page });
    const cachedData = getFromCache(cacheKey);
    
    if (cachedData) {
      console.log('📦 Serving from cache:', cacheKey);
      return res.json(cachedData);
    }
    
    console.log('🔄 Fetching from Airtable:', { brand, status, region, search, limit, page });
    
    // Build filter formula
    let filterFormula = '';
    const conditions = [];
    
    if (brand) {
      conditions.push(`{${F.hotels.brand}} = '${brand}'`);
    }
    
    if (status) {
      conditions.push(`{${F.hotels.status}} = '${status}'`);
    }
    
    if (region) {
      conditions.push(`{${F.hotels.region}} = '${region}'`);
    }
    
    if (search) {
      const searchConditions = [
        `SEARCH('${search}', {${F.hotels.name}})`,
        `SEARCH('${search}', {${F.hotels.city}})`,
        `SEARCH('${search}', {${F.hotels.country}})`
      ];
      conditions.push(`OR(${searchConditions.join(', ')})`);
    }
    
    if (conditions.length > 0) {
      filterFormula = `AND(${conditions.join(', ')})`;
    }
    
    // Bulk map load — lean field set; detail panel fetches full record per hotel on click
    const selectOptions = {
      fields: MAP_HOTEL_FIELDS,
      pageSize: 100, // Airtable's optimal page size
      sort: [{ field: F.hotels.name, direction: 'asc' }]
    };
    if (limit) {
      selectOptions.maxRecords = limit;
    }
    
    // Only add filterByFormula if we have a valid filter
    if (filterFormula && filterFormula.trim() !== '') {
      selectOptions.filterByFormula = filterFormula;
    }

    let hotels;
    try {
      hotels = await base(F.hotels.table).select(selectOptions).all();
    } catch (selectErr) {
      const msg = String(selectErr?.message || selectErr || "");
      if (
        selectErr?.error === "UNKNOWN_FIELD_NAME" ||
        /Unknown field name/i.test(msg)
      ) {
        selectOptions.fields = MAP_HOTEL_FIELDS.filter(
          (field) => field !== F.hotels.market && field !== F.hotels.submarket
        );
        hotels = await base(F.hotels.table).select(selectOptions).all();
      } else {
        throw selectErr;
      }
    }
    
    // Format response; track valid coordinates for diagnostics
    let skippedNoCoordinates = 0;
    const formattedHotels = hotels.map((hotel) => {
      const formatted = formatHotelRecord(hotel);
      const hasCoords = Number.isFinite(formatted.lat) && Number.isFinite(formatted.lng)
        && (formatted.lat !== 0 || formatted.lng !== 0);
      if (!hasCoords) skippedNoCoordinates++;
      return formatted;
    });
    
    // Calculate statistics
    const stats = calculateStatistics(formattedHotels);
    
    // Generate insights
    const insights = generateInsights(formattedHotels);
    
    const response = {
      success: true,
      hotels: formattedHotels,
      statistics: stats,
      insights: insights,
      totalCount: formattedHotels.length,
      totalWithCoordinates: formattedHotels.length - skippedNoCoordinates,
      skippedNoCoordinates,
      hasMore: !!limit && hotels.length === limit,
      page: parseInt(page, 10),
      limit,
      cached: false
    };

    // Cache the response
    setCache(cacheKey, response);
    console.log('💾 Cached response for:', cacheKey);

    res.json(response);
    
  } catch (error) {
    console.error("Error getting brand presence data:", error);
    res.status(500).json({ 
      error: "Internal Server Error", 
      details: error.message 
    });
  }
}

// Get brand statistics
export async function getBrandStatistics(req, res) {
  if (!ensureBrandPresenceConfig(res)) return;
  try {
    const { region } = req.query;
    
    // Build filter for region if specified
    let filterFormula = '';
    if (region) {
      filterFormula = `{${F.hotels.region}} = '${region}'`;
    }
    
    // Fetch all hotels for statistics
    const selectOptions = {
      fields: [F.hotels.brand, F.hotels.status, F.hotels.rooms, F.hotels.region]
    };
    
    // Only add filterByFormula if we have a valid filter
    if (filterFormula && filterFormula.trim() !== '') {
      selectOptions.filterByFormula = filterFormula;
    }
    
    const hotels = await base(F.hotels.table)
      .select(selectOptions)
      .all();
    
    // Calculate brand statistics
    const brandStats = {};
    const regionStats = {};
    const statusStats = { open: 0, pipeline: 0, candidate: 0 };
    let totalRooms = 0;
    
    hotels.forEach(hotel => {
      const brand = hotel.fields[F.hotels.brand];
      const status = hotel.fields[F.hotels.status];
      const rooms = hotel.fields[F.hotels.rooms] || 0;
      const region = hotel.fields[F.hotels.region];
      
      // Brand statistics
      if (!brandStats[brand]) {
        brandStats[brand] = { total: 0, open: 0, pipeline: 0, candidate: 0, rooms: 0 };
      }
      brandStats[brand].total++;
      brandStats[brand][status]++;
      brandStats[brand].rooms += rooms;
      
      // Region statistics
      if (!regionStats[region]) {
        regionStats[region] = { total: 0, brands: new Set() };
      }
      regionStats[region].total++;
      regionStats[region].brands.add(brand);
      
      // Status statistics
      statusStats[status]++;
      totalRooms += rooms;
    });
    
    // Convert region stats to include brand count
    Object.keys(regionStats).forEach(region => {
      regionStats[region].brandCount = regionStats[region].brands.size;
      delete regionStats[region].brands;
    });
    
    res.json({
      success: true,
      brandStatistics: brandStats,
      regionStatistics: regionStats,
      statusStatistics: statusStats,
      totalHotels: hotels.length,
      totalRooms: totalRooms,
      averageRooms: hotels.length > 0 ? Math.round(totalRooms / hotels.length) : 0
    });
    
  } catch (error) {
    console.error("Error getting brand statistics:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
}

// Get white space opportunities
export async function getWhiteSpaceOpportunities(req, res) {
  try {
    const { region, minPopulation = 100000 } = req.query;
    
    // This would typically integrate with a cities database
    // For now, we'll return mock data
    const opportunities = [
      {
        city: "Santa Marta",
        country: "Colombia",
        region: "CALA",
        population: 500000,
        currentBrands: ["Hilton"],
        opportunityScore: 85,
        recommendedBrands: ["Marriott", "Hyatt", "Choice Hotels"]
      },
      {
        city: "Managua",
        country: "Nicaragua",
        region: "CALA",
        population: 1.5e6,
        currentBrands: [],
        opportunityScore: 92,
        recommendedBrands: ["Hilton", "Marriott", "IHG"]
      },
      {
        city: "Tegucigalpa",
        country: "Honduras",
        region: "CALA",
        population: 1.2e6,
        currentBrands: ["Holiday Inn"],
        opportunityScore: 78,
        recommendedBrands: ["Marriott", "Hilton", "Choice Hotels"]
      }
    ];
    
    res.json({
      success: true,
      opportunities: opportunities,
      totalOpportunities: opportunities.length
    });
    
  } catch (error) {
    console.error("Error getting white space opportunities:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
}

// Calculate statistics
function calculateStatistics(hotels) {
  const totalHotels = hotels.length;
  const openHotels = hotels.filter(h => h.status === 'open').length;
  const pipelineHotels = hotels.filter(h => h.status === 'pipeline').length;
  const candidateHotels = hotels.filter(h => h.status === 'candidate').length;
  
  const totalRooms = hotels.reduce((sum, h) => sum + (h.rooms || 0), 0);
  const averageRooms = totalHotels > 0 ? Math.round(totalRooms / totalHotels) : 0;
  
  // Brand distribution
  const brandCounts = {};
  hotels.forEach(hotel => {
    brandCounts[hotel.brand] = (brandCounts[hotel.brand] || 0) + 1;
  });
  
  // Region distribution
  const regionCounts = {};
  hotels.forEach(hotel => {
    regionCounts[hotel.region] = (regionCounts[hotel.region] || 0) + 1;
  });
  
  return {
    totalHotels,
    openHotels,
    pipelineHotels,
    candidateHotels,
    totalRooms,
    averageRooms,
    brandDistribution: brandCounts,
    regionDistribution: regionCounts
  };
}

// Generate insights
function generateInsights(hotels) {
  const insights = [];
  
  if (hotels.length === 0) {
    return [{
      priority: 'low',
      title: 'No Data Available',
      description: 'No hotels found matching the current filters.'
    }];
  }
  
  // Calculate brand distribution
  const brandCounts = {};
  hotels.forEach(hotel => {
    brandCounts[hotel.brand] = (brandCounts[hotel.brand] || 0) + 1;
  });
  
  const topBrand = Object.entries(brandCounts)
    .sort(([,a], [,b]) => b - a)[0];
  
  if (topBrand) {
    insights.push({
      priority: 'high',
      title: 'Market Leader',
      description: `${topBrand[0]} leads with ${topBrand[1]} properties in the selected region.`
    });
  }
  
  // Calculate pipeline vs open ratio
  const openCount = hotels.filter(h => h.status === 'open').length;
  const pipelineCount = hotels.filter(h => h.status === 'pipeline').length;
  const ratio = pipelineCount / openCount;
  
  if (ratio > 0.5) {
    insights.push({
      priority: 'medium',
      title: 'High Growth Activity',
      description: `Strong pipeline activity with ${pipelineCount} projects in development.`
    });
  }
  
  // Find white space opportunities
  const cities = [...new Set(hotels.map(h => h.city))];
  const citiesWithMultipleBrands = cities.filter(city => {
    const cityHotels = hotels.filter(h => h.city === city);
    const uniqueBrands = new Set(cityHotels.map(h => h.brand));
    return uniqueBrands.size > 1;
  });
  
  if (citiesWithMultipleBrands.length < cities.length * 0.5) {
    insights.push({
      priority: 'low',
      title: 'White Space Opportunities',
      description: `${cities.length - citiesWithMultipleBrands.length} cities have limited brand competition.`
    });
  }
  
  return insights;
}

// Get unique location types from Airtable
export async function getLocationTypes(req, res) {
  if (!ensureBrandPresenceConfig(res)) return;
  try {
    const hotels = await base(F.hotels.table)
      .select({
        fields: [F.hotels.locationType]
      })
      .all();
    
    // Extract unique location types
    const locationTypes = [...new Set(
      hotels
        .map(hotel => hotel.fields[F.hotels.locationType])
        .filter(type => type && type.trim() !== '')
    )].sort();
    
    res.json({
      success: true,
      locationTypes: locationTypes
    });
    
  } catch (error) {
    console.error("Error getting location types:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
}

// Get unique parent companies from Airtable
export async function getParentCompanies(req, res) {
  if (!ensureBrandPresenceConfig(res)) return;
  try {
    const hotels = await base(F.hotels.table)
      .select({
        fields: [F.hotels.parentCompany]
      })
      .all();
    
    // Extract unique parent companies
    const parentCompanies = [...new Set(
      hotels
        .map(hotel => hotel.fields[F.hotels.parentCompany])
        .filter(company => company && company.trim() !== '')
    )].sort();
    
    res.json({
      success: true,
      parentCompanies: parentCompanies
    });
    
  } catch (error) {
    console.error("Error getting parent companies:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
}

// Get unique brands from Airtable
export async function getBrands(req, res) {
  if (!ensureBrandPresenceConfig(res)) return;
  try {
    const hotels = await base(F.hotels.table)
      .select({
        fields: [F.hotels.brand]
      })
      .all();
    
    // Extract unique brands
    const brands = [...new Set(
      hotels
        .map(hotel => hotel.fields[F.hotels.brand])
        .filter(brand => brand && brand.trim() !== '')
    )].sort();
    
    res.json({
      success: true,
      brands: brands
    });
    
  } catch (error) {
    console.error("Error getting brands:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
}

// Get chain scales (property types) from Airtable
export async function getChainScales(req, res) {
  if (!ensureBrandPresenceConfig(res)) return;
  try {
    const hotels = await base(F.hotels.table)
      .select({
        fields: [F.hotels.propertyType]
      })
      .all();
    
    // Extract unique chain scales
    const chainScales = [...new Set(
      hotels
        .map(hotel => hotel.fields[F.hotels.propertyType])
        .filter(scale => scale && scale.trim() !== '')
    )].sort();
    
    res.json({
      success: true,
      chainScales: chainScales
    });
    
  } catch (error) {
    console.error("Error getting chain scales:", error);
    res.status(500).json({
      success: false,
      error: "Internal Server Error",
      details: error.message
    });
  }
}

// Export data
export async function exportBrandPresenceData(req, res) {
  try {
    const { format = 'csv', brand, status, region } = req.query;
    
    // Get filtered data
    const response = await getBrandPresence(req, res);
    
    if (format === 'csv') {
      const csvContent = convertToCSV(response.hotels);
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename="brand-presence-data.csv"');
      res.send(csvContent);
    } else if (format === 'json') {
      res.json(response);
    } else {
      res.status(400).json({ error: 'Unsupported format. Use csv or json.' });
    }
    
  } catch (error) {
    console.error("Error exporting brand presence data:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
}

// Convert to CSV
function convertToCSV(hotels) {
  const headers = [
    'Name', 'Brand', 'Status', 'City', 'Country', 'Region', 
    'Rooms', 'STR Number', 'Chain Scale', 'Project Phase'
  ];
  
  const rows = hotels.map(hotel => [
    hotel.name,
    hotel.brand,
    hotel.status,
    hotel.city,
    hotel.country,
    hotel.region,
    hotel.rooms,
    hotel.strNumber || '',
    hotel.chainScale || '',
    hotel.projectPhase || ''
  ]);
  
  return [headers, ...rows].map(row => 
    row.map(field => `"${field}"`).join(',')
  ).join('\n');
}
