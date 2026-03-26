import Airtable from "airtable";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
const brandBase = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY_READONLY }).base(process.env.AIRTABLE_BASE_ID_ALT);
const brandBasicsBase = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

// Field mappings for Brand Fit Analyzer
const F = {
  deals: {
    table: "tblbvSxjiIhXzW6XW",          // Deals table ID
    id: "flddZVyzlh2RuEcje",             // Deal_ID
    recordId: "fld5sPHT4p1Nw7n0a",       // Record_ID
    status: "fld4cvEAz0k3x8aaU",         // Deal Status
    name: "fldkKJzBOBoFCvbnx",           // Property Name
    userLink: "fldALlSB9UsnLhgvI",       // Link to Users
    stage: "flde0PSEQUhA9Jl5a",          // Stage of Development
    location: "fldLocationProperty",      // Location & Property link
    projectType: "fldProjectType",        // Project Type
    hotelType: "fldHotelType",           // Hotel Type
    currentBrand: "fldCurrentBrand",      // Current Brand Affiliation
    parentCompany: "fldParentCompany",    // Parent Company Name
    brandExperience: "fldBrandExperience", // Brand experience
    brandFlexibility: "fldBrandFlexibility", // Open to emerging brands
    expectedOpening: "fldExpectedOpening", // Expected Opening Date
  },
  brands: {
    table: "Brand Setup - Brand Basics",  // Brand Basics table
    name: "Brand Name",
    parentCompany: "Parent Company",
    chainScale: "Hotel Chain Scale",
    brandModel: "Brand Model",
    serviceModel: "Hotel Service Model",
    yearLaunched: "Year Brand Launched",
    developmentStage: "Brand Development Stage",
    positioning: "Brand Positioning",
    tagline: "Brand Tagline",
    customerPromise: "Brand Customer Promise",
    valueProposition: "Brand Value Proposition",
    brandPillars: "Brand Pillars",
    targetSegments: "Target Guest Segments",
    guestPsychographics: "Guest Psychographics Description",
    differentiators: "Key Brand Differentiators",
    sustainability: "Sustainability Positioning",
    status: "Brand Status",
    architecture: "Brand Architecture"
  },
  hotels: {
    table: "Hotel Census",
    name: "name",
    brand: "Affiliation",
    parentCompany: "Parent Company",
    status: "status",
    city: "city",
    country: "country",
    region: "Region",
    rooms: "rooms",
    chainScale: "Chain Scale",
    propertyType: "Chain Scale"
  }
};

// Main Brand Fit Analyzer function
export async function analyzeBrandFit(req, res) {
  try {
    if (req.method !== "POST") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const ownerInputs = req.body;
    
    // Validate required inputs
    const validation = validateOwnerInputs(ownerInputs);
    if (!validation.valid) {
      return res.status(400).json({ 
        error: "Invalid inputs", 
        details: validation.errors 
      });
    }

    // Get all brands and their criteria
    const brands = await getAllBrands();
    
    // Get market context data
    const marketContext = await getMarketContext(ownerInputs);
    
    // Analyze brand fit for each brand
    const brandFits = await analyzeBrandFits(ownerInputs, brands, marketContext);
    
    // Sort by fit score
    brandFits.sort((a, b) => b.fitScore - a.fitScore);
    
    // Generate insights and recommendations
    const insights = generateInsights(ownerInputs, brandFits, marketContext);
    
    res.json({
      success: true,
      ownerInputs,
      brandFits: brandFits.slice(0, 10), // Top 10 matches
      marketContext,
      insights,
      analysisDate: new Date().toISOString()
    });

  } catch (error) {
    console.error("Error in brand fit analysis:", error);
    res.status(500).json({ 
      error: "Internal Server Error", 
      details: error.message 
    });
  }
}

// Validate owner inputs
function validateOwnerInputs(inputs) {
  const errors = [];
  const required = [
    'location', 'propertyType', 'roomCount', 'assetStage', 
    'targetSegment', 'brandPositioning', 'guestDemandFocus'
  ];
  
  required.forEach(field => {
    if (!inputs[field]) {
      errors.push(`Missing required field: ${field}`);
    }
  });
  
  return {
    valid: errors.length === 0,
    errors
  };
}

// Get all brands from Brand Basics table
async function getAllBrands() {
  try {
    const brands = await brandBasicsBase(F.brands.table)
      .select({
        fields: [
          F.brands.name,
          F.brands.parentCompany,
          F.brands.chainScale,
          F.brands.brandModel,
          F.brands.serviceModel,
          F.brands.yearLaunched,
          F.brands.developmentStage,
          F.brands.positioning,
          F.brands.tagline,
          F.brands.customerPromise,
          F.brands.valueProposition,
          F.brands.brandPillars,
          F.brands.targetSegments,
          F.brands.guestPsychographics,
          F.brands.differentiators,
          F.brands.sustainability,
          F.brands.status,
          F.brands.architecture
        ],
        maxRecords: 100
      })
      .all();

    return brands.map(brand => ({
      id: brand.id,
      name: brand.fields[F.brands.name] || 'Unknown Brand',
      parentCompany: brand.fields[F.brands.parentCompany] || 'Unknown',
      chainScale: brand.fields[F.brands.chainScale] || 'Unknown',
      brandModel: brand.fields[F.brands.brandModel] || 'Unknown',
      serviceModel: brand.fields[F.brands.serviceModel] || 'Unknown',
      yearLaunched: brand.fields[F.brands.yearLaunched] || 'Unknown',
      developmentStage: brand.fields[F.brands.developmentStage] || 'Unknown',
      positioning: brand.fields[F.brands.positioning] || '',
      tagline: brand.fields[F.brands.tagline] || '',
      customerPromise: brand.fields[F.brands.customerPromise] || '',
      valueProposition: brand.fields[F.brands.valueProposition] || '',
      brandPillars: brand.fields[F.brands.brandPillars] || '',
      targetSegments: brand.fields[F.brands.targetSegments] || [],
      guestPsychographics: brand.fields[F.brands.guestPsychographics] || '',
      differentiators: brand.fields[F.brands.differentiators] || '',
      sustainability: brand.fields[F.brands.sustainability] || '',
      status: brand.fields[F.brands.status] || 'Unknown',
      architecture: brand.fields[F.brands.architecture] || 'Unknown'
    }));
  } catch (error) {
    console.error("Error fetching brands:", error);
    return [];
  }
}

// Get market context data
async function getMarketContext(ownerInputs) {
  try {
    // Get brand presence in the target location
    let locationFilter = `{country} = '${ownerInputs.location.country}'`;
    if (ownerInputs.location.city && ownerInputs.location.city !== 'Unknown') {
      locationFilter += ` AND {city} = '${ownerInputs.location.city}'`;
    }

    let hotels = [];
    try {
      hotels = await brandBase(F.hotels.table)
        .select({
          fields: ['Affiliation', 'Chain Scale', 'rooms', 'status'],
          filterByFormula: locationFilter,
          maxRecords: 1000
        })
        .all();
    } catch (error) {
      console.error('Error with location filter, trying without city filter:', error.message);
      // Try without city filter if the combined filter fails
      try {
        hotels = await brandBase(F.hotels.table)
          .select({
            fields: ['Affiliation', 'Chain Scale', 'rooms', 'status'],
            filterByFormula: `{country} = '${ownerInputs.location.country}'`,
            maxRecords: 1000
          })
          .all();
      } catch (secondError) {
        console.error('Error with country filter, getting all hotels:', secondError.message);
        // If all filters fail, get all hotels
        hotels = await brandBase(F.hotels.table)
          .select({
            fields: ['Affiliation', 'Chain Scale', 'rooms', 'status'],
            maxRecords: 1000
          })
          .all();
      }
    }

    // Analyze market composition
    const marketAnalysis = analyzeMarketComposition(hotels, ownerInputs);
    
    return {
      totalHotels: hotels.length,
      marketComposition: marketAnalysis,
      competitiveIntensity: calculateCompetitiveIntensity(hotels, ownerInputs),
      whiteSpaceOpportunities: identifyWhiteSpaceOpportunities(hotels, ownerInputs)
    };
  } catch (error) {
    console.error("Error getting market context:", error);
    return {
      totalHotels: 0,
      marketComposition: {},
      competitiveIntensity: 'medium',
      whiteSpaceOpportunities: []
    };
  }
}

// Analyze brand fits for all brands
async function analyzeBrandFits(ownerInputs, brands, marketContext) {
  const brandFits = [];

  for (const brand of brands) {
    if (brand.status !== 'Active – Visible') continue;

    const fitAnalysis = {
      brand: brand.name,
      parentCompany: brand.parentCompany,
      fitScore: 0,
      fitBreakdown: {},
      strengths: [],
      concerns: [],
      recommendations: []
    };

    // 1. Segment & Positioning Fit (30% weight)
    fitAnalysis.fitBreakdown.segmentFit = analyzeSegmentFit(ownerInputs, brand);
    fitAnalysis.fitScore += fitAnalysis.fitBreakdown.segmentFit * 0.30;

    // 2. Geographic & Market Fit (25% weight)
    fitAnalysis.fitBreakdown.geographicFit = analyzeGeographicFit(ownerInputs, brand, marketContext);
    fitAnalysis.fitScore += fitAnalysis.fitBreakdown.geographicFit * 0.25;

    // 3. Property & Size Fit (20% weight)
    fitAnalysis.fitBreakdown.propertyFit = analyzePropertyFit(ownerInputs, brand);
    fitAnalysis.fitScore += fitAnalysis.fitBreakdown.propertyFit * 0.20;

    // 4. Financial & CapEx Fit (15% weight)
    fitAnalysis.fitBreakdown.financialFit = analyzeFinancialFit(ownerInputs, brand);
    fitAnalysis.fitScore += fitAnalysis.fitBreakdown.financialFit * 0.15;

    // 5. Operational & Strategic Fit (10% weight)
    fitAnalysis.fitBreakdown.operationalFit = analyzeOperationalFit(ownerInputs, brand);
    fitAnalysis.fitScore += fitAnalysis.fitBreakdown.operationalFit * 0.10;

    // Round final score
    fitAnalysis.fitScore = Math.round(fitAnalysis.fitScore);

    // Generate strengths, concerns, and recommendations
    fitAnalysis.strengths = generateStrengths(ownerInputs, brand, fitAnalysis);
    fitAnalysis.concerns = generateConcerns(ownerInputs, brand, fitAnalysis);
    fitAnalysis.recommendations = generateRecommendations(ownerInputs, brand, fitAnalysis);

    brandFits.push(fitAnalysis);
  }

  return brandFits;
}

// Analyze segment and positioning fit
function analyzeSegmentFit(ownerInputs, brand) {
  let score = 50; // Base score

  // Chain scale alignment
  const segmentMapping = {
    'Economy': ['Economy Chain'],
    'Midscale': ['Midscale Chain'],
    'Upscale': ['Upscale Chain'],
    'Upper-upscale': ['Upper Upscale Chain'],
    'Luxury': ['Luxury Chain']
  };

  const targetSegments = segmentMapping[ownerInputs.targetSegment] || [];
  if (targetSegments.includes(brand.chainScale)) {
    score += 30;
  } else if (brand.chainScale.includes(ownerInputs.targetSegment)) {
    score += 20;
  }

  // Brand positioning alignment
  if (ownerInputs.brandPositioning === 'Hard brand' && brand.brandModel === 'Hard brand') {
    score += 15;
  } else if (ownerInputs.brandPositioning === 'Soft brand' && brand.brandModel !== 'Hard brand') {
    score += 15;
  }

  // Guest demand focus alignment
  if (brand.targetSegments && Array.isArray(brand.targetSegments)) {
    const hasMatchingSegment = brand.targetSegments.some(segment => 
      segment.toLowerCase().includes(ownerInputs.guestDemandFocus.toLowerCase())
    );
    if (hasMatchingSegment) {
      score += 15;
    }
  }

  return Math.min(100, score);
}

// Analyze geographic and market fit
function analyzeGeographicFit(ownerInputs, brand, marketContext) {
  let score = 50; // Base score

  // Check if brand has presence in the market
  const brandInMarket = marketContext.marketComposition[brand.name];
  if (brandInMarket) {
    // Moderate presence is good (not too saturated, not absent)
    if (brandInMarket.count >= 1 && brandInMarket.count <= 5) {
      score += 25;
    } else if (brandInMarket.count > 5) {
      score += 10; // Some presence but potentially saturated
    }
  } else {
    // No presence could be opportunity or risk
    if (marketContext.competitiveIntensity === 'low') {
      score += 20; // Good opportunity
    } else {
      score += 5; // Risky but possible
    }
  }

  // Check for white space opportunities
  const isWhiteSpace = marketContext.whiteSpaceOpportunities.includes(brand.name);
  if (isWhiteSpace) {
    score += 15;
  }

  return Math.min(100, score);
}

// Analyze property and size fit
function analyzePropertyFit(ownerInputs, brand) {
  let score = 50; // Base score

  // Room count alignment
  const roomCount = parseInt(ownerInputs.roomCount);
  if (roomCount >= 100 && roomCount <= 300) {
    score += 20; // Sweet spot for most brands
  } else if (roomCount >= 50 && roomCount <= 500) {
    score += 15; // Acceptable range
  } else {
    score += 5; // Outside typical range
  }

  // Property type alignment
  const propertyTypeMapping = {
    'Urban': ['Urban', 'City', 'Downtown'],
    'Resort': ['Resort', 'Destination'],
    'Airport': ['Airport', 'Transit'],
    'Suburban': ['Suburban', 'Business Park'],
    'Lifestyle': ['Lifestyle', 'Boutique']
  };

  const targetTypes = propertyTypeMapping[ownerInputs.propertyType] || [];
  const brandPositioning = brand.positioning.toLowerCase();
  const hasMatchingType = targetTypes.some(type => 
    brandPositioning.includes(type.toLowerCase())
  );
  
  if (hasMatchingType) {
    score += 20;
  }

  // Asset stage alignment
  if (ownerInputs.assetStage === 'Existing hotel (conversion)' && brand.developmentStage === 'Mature') {
    score += 10; // Mature brands good for conversions
  } else if (ownerInputs.assetStage === 'Under development (new build)' && brand.developmentStage !== 'Legacy') {
    score += 10; // Most brands can handle new builds
  }

  return Math.min(100, score);
}

// Analyze financial and CapEx fit
function analyzeFinancialFit(ownerInputs, brand) {
  let score = 50; // Base score

  // CapEx tolerance alignment
  if (ownerInputs.capExTolerance === 'Minimal' && brand.chainScale === 'Economy Chain') {
    score += 25; // Economy brands typically have lower CapEx requirements
  } else if (ownerInputs.capExTolerance === 'High' && brand.chainScale === 'Luxury Chain') {
    score += 25; // Luxury brands typically require higher CapEx
  } else if (ownerInputs.capExTolerance === 'Moderate') {
    score += 20; // Most brands fit moderate CapEx
  }

  // Financial goals alignment
  if (ownerInputs.financialGoals.riskTolerance === 'Conservative' && brand.chainScale === 'Midscale Chain') {
    score += 15; // Midscale typically more stable
  } else if (ownerInputs.financialGoals.riskTolerance === 'Aggressive' && brand.chainScale === 'Luxury Chain') {
    score += 15; // Luxury can offer higher returns
  }

  return Math.min(100, score);
}

// Analyze operational and strategic fit
function analyzeOperationalFit(ownerInputs, brand) {
  let score = 50; // Base score

  // Operating model alignment
  if (ownerInputs.operationalPreferences.operatingModel === 'Franchise' && brand.brandModel === 'Hard brand') {
    score += 20; // Hard brands typically offer franchise
  } else if (ownerInputs.operationalPreferences.operatingModel === 'Management contract' && brand.serviceModel === 'Full-Service') {
    score += 20; // Full-service brands often use management contracts
  }

  // Control preference alignment
  if (ownerInputs.operationalPreferences.controlPreference === 'High owner control' && brand.brandModel === 'Soft brand') {
    score += 15; // Soft brands allow more owner control
  } else if (ownerInputs.operationalPreferences.controlPreference === 'Turnkey' && brand.brandModel === 'Hard brand') {
    score += 15; // Hard brands provide more turnkey solutions
  }

  // Strategic priorities alignment
  if (ownerInputs.strategicPriorities.geographicStrategy === 'Enter new market' && brand.developmentStage === 'Emerging') {
    score += 15; // Emerging brands often looking to expand
  }

  return Math.min(100, score);
}

// Generate strengths for a brand fit
function generateStrengths(ownerInputs, brand, fitAnalysis) {
  const strengths = [];

  if (fitAnalysis.fitBreakdown.segmentFit >= 80) {
    strengths.push(`Perfect segment alignment with ${brand.chainScale}`);
  }

  if (fitAnalysis.fitBreakdown.geographicFit >= 80) {
    strengths.push(`Strong market positioning in ${ownerInputs.location.city || ownerInputs.location.country}`);
  }

  if (brand.targetSegments && brand.targetSegments.includes(ownerInputs.guestDemandFocus)) {
    strengths.push(`Brand specifically targets ${ownerInputs.guestDemandFocus} travelers`);
  }

  if (brand.developmentStage === 'Mature' && ownerInputs.assetStage === 'Existing hotel (conversion)') {
    strengths.push('Mature brand with proven conversion experience');
  }

  if (brand.brandModel === ownerInputs.brandPositioning) {
    strengths.push(`Brand model matches your ${ownerInputs.brandPositioning} preference`);
  }

  return strengths;
}

// Generate concerns for a brand fit
function generateConcerns(ownerInputs, brand, fitAnalysis) {
  const concerns = [];

  if (fitAnalysis.fitBreakdown.segmentFit < 60) {
    concerns.push(`Segment mismatch: Brand is ${brand.chainScale} but you're targeting ${ownerInputs.targetSegment}`);
  }

  if (fitAnalysis.fitBreakdown.geographicFit < 60) {
    concerns.push('Limited or no presence in your target market');
  }

  if (fitAnalysis.fitBreakdown.financialFit < 60) {
    concerns.push(`CapEx requirements may not align with your ${ownerInputs.capExTolerance} tolerance`);
  }

  if (brand.developmentStage === 'Legacy' && ownerInputs.assetStage === 'Under development (new build)') {
    concerns.push('Legacy brand may not be ideal for new development');
  }

  if (brand.brandModel !== ownerInputs.brandPositioning) {
    concerns.push(`Brand model (${brand.brandModel}) differs from your preference (${ownerInputs.brandPositioning})`);
  }

  return concerns;
}

// Generate recommendations for a brand fit
function generateRecommendations(ownerInputs, brand, fitAnalysis) {
  const recommendations = [];

  if (fitAnalysis.fitScore >= 85) {
    recommendations.push('Excellent fit - consider prioritizing this brand for negotiations');
  } else if (fitAnalysis.fitScore >= 70) {
    recommendations.push('Good fit - worth exploring further with brand representatives');
  } else if (fitAnalysis.fitScore >= 55) {
    recommendations.push('Moderate fit - consider if brand can accommodate your specific requirements');
  } else {
    recommendations.push('Limited fit - may require significant compromises or may not be suitable');
  }

  if (fitAnalysis.fitBreakdown.geographicFit < 70) {
    recommendations.push('Discuss market entry strategy and support with brand team');
  }

  if (fitAnalysis.fitBreakdown.financialFit < 70) {
    recommendations.push('Negotiate CapEx requirements and fee structure to better align with your goals');
  }

  return recommendations;
}

// Analyze market composition
function analyzeMarketComposition(hotels, ownerInputs) {
  const composition = {};
  
  hotels.forEach(hotel => {
    const brand = hotel.fields['Affiliation'];
    if (!composition[brand]) {
      composition[brand] = { count: 0, chainScales: new Set(), totalRooms: 0 };
    }
    composition[brand].count++;
    composition[brand].chainScales.add(hotel.fields['Chain Scale']);
    composition[brand].totalRooms += parseInt(hotel.fields['rooms'] || 0);
  });

  // Convert Sets to Arrays
  Object.keys(composition).forEach(brand => {
    composition[brand].chainScales = Array.from(composition[brand].chainScales);
  });

  return composition;
}

// Calculate competitive intensity
function calculateCompetitiveIntensity(hotels, ownerInputs) {
  const totalHotels = hotels.length;
  const uniqueBrands = new Set(hotels.map(h => h.fields['Affiliation'])).size;
  
  if (totalHotels < 5) return 'low';
  if (totalHotels < 15) return 'medium';
  if (uniqueBrands > totalHotels * 0.7) return 'high';
  return 'medium';
}

// Identify white space opportunities
function identifyWhiteSpaceOpportunities(hotels, ownerInputs) {
  const existingBrands = new Set(hotels.map(h => h.fields['Affiliation']));
  const allBrands = ['Marriott', 'Hilton', 'IHG', 'Hyatt', 'Choice Hotels', 'Wyndham', 'Accor'];
  
  return allBrands.filter(brand => !existingBrands.has(brand));
}

// Generate insights
function generateInsights(ownerInputs, brandFits, marketContext) {
  const insights = [];

  // Top fit insight
  if (brandFits.length > 0) {
    const topFit = brandFits[0];
    insights.push({
      type: 'top_match',
      title: `Best Match: ${topFit.brand}`,
      description: `${topFit.brand} scored ${topFit.fitScore}% fit with your requirements`,
      priority: 'high'
    });
  }

  // Market opportunity insight
  if (marketContext.whiteSpaceOpportunities.length > 0) {
    insights.push({
      type: 'market_opportunity',
      title: 'White Space Opportunity',
      description: `${marketContext.whiteSpaceOpportunities.length} major brands have no presence in ${ownerInputs.location.city || ownerInputs.location.country}`,
      priority: 'medium'
    });
  }

  // Competitive intensity insight
  insights.push({
    type: 'competitive_landscape',
    title: 'Market Competition',
    description: `${marketContext.competitiveIntensity} competitive intensity with ${marketContext.totalHotels} existing properties`,
    priority: 'medium'
  });

  // Segment insights
  const segmentFits = brandFits.filter(bf => bf.fitBreakdown.segmentFit >= 80);
  if (segmentFits.length > 0) {
    insights.push({
      type: 'segment_alignment',
      title: 'Strong Segment Alignment',
      description: `${segmentFits.length} brands have excellent alignment with your ${ownerInputs.targetSegment} target`,
      priority: 'high'
    });
  }

  return insights;
}

// Get all deals for brand fit analysis (no authentication required)
export async function getAllDealsForAnalysis(req, res) {
  try {
    if (req.method !== "GET") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    // Get all deals without authentication for brand fit analysis
    const deals = await base(F.deals.table)
      .select({
        maxRecords: 100,
        sort: [{ field: F.deals.id, direction: 'desc' }]
      })
      .all();

    // Process deals to include basic information
    const processedDeals = await Promise.all(deals.map(async (deal) => {
      const dealData = deal.fields;
      
      // Get location data if linked
      let locationData = {};
      if (dealData['Location & Property'] && dealData['Location & Property'].length > 0) {
        const locationId = dealData['Location & Property'][0];
        try {
          const locations = await base('tblLw3HRrlYldDPcr')
            .select({
              filterByFormula: `{Record_ID} = '${locationId}'`
            })
            .all();
          
          if (locations.length > 0) {
            locationData = locations[0].fields;
          }
        } catch (error) {
          console.error('Error fetching location data:', error);
        }
      }

      return {
        id: dealData[F.deals.id] || deal.id,
        propertyName: dealData['Property Name'] || 'Unnamed Property',
        status: dealData['Deal Status'] || 'New',
        stage: dealData['Stage of Development'] || 'Unknown',
        projectType: dealData['Project Type'] || 'Unknown',
        city: locationData.City || 'Unknown',
        country: locationData.Country || 'Unknown',
        rooms: parseInt(locationData['Total Number of Rooms/Keys']) || 0,
        propertyType: locationData['Hotel Type'] || 'Unknown',
        chainScale: locationData['Hotel Chain Scale'] || 'Unknown',
        serviceModel: locationData['Hotel Service Model'] || 'Unknown'
      };
    }));

    res.json({
      success: true,
      deals: processedDeals,
      totalCount: processedDeals.length
    });

  } catch (error) {
    console.error("Error getting deals for analysis:", error);
    res.status(500).json({ 
      error: "Internal Server Error", 
      details: error.message 
    });
  }
}

// Get brand fit analysis for a specific deal
export async function getDealBrandFit(req, res) {
  try {
    const { dealId } = req.query;
    
    if (!dealId) {
      return res.status(400).json({ error: "Deal ID is required" });
    }

    // Clean the deal ID (remove any field references like :1)
    const cleanDealId = dealId.split(':')[0];

    // Try to get deal by Deal_ID field first
    let deals = await base(F.deals.table)
      .select({
        filterByFormula: `{${F.deals.id}} = '${cleanDealId}'`
      })
      .all();

    // If not found by Deal_ID, try by Record_ID
    if (deals.length === 0) {
      deals = await base(F.deals.table)
        .select({
          filterByFormula: `{${F.deals.recordId}} = '${cleanDealId}'`
        })
        .all();
    }

    // If still not found, try by Airtable record ID
    if (deals.length === 0) {
      try {
        const deal = await base(F.deals.table).find(cleanDealId);
        deals = [deal];
      } catch (error) {
        console.log('Deal not found by record ID:', cleanDealId);
      }
    }

    if (deals.length === 0) {
      return res.status(404).json({ error: "Deal not found" });
    }

    const deal = deals[0].fields;
    
    // Convert deal data to owner inputs format
    const ownerInputs = await convertDealToOwnerInputs(cleanDealId);
    
    // Perform brand fit analysis
    const brands = await getAllBrands();
    const marketContext = await getMarketContext(ownerInputs);
    const brandFits = await analyzeBrandFits(ownerInputs, brands, marketContext);
    
    brandFits.sort((a, b) => b.fitScore - a.fitScore);
    
    // Generate insights
    const insights = generateInsights(ownerInputs, brandFits, marketContext);

    res.json({
      success: true,
      dealId,
      ownerInputs,
      brandFits: brandFits.slice(0, 10),
      marketContext,
      insights,
      analysisDate: new Date().toISOString()
    });

  } catch (error) {
    console.error("Error getting deal brand fit:", error);
    res.status(500).json({ 
      error: "Internal Server Error", 
      details: error.message 
    });
  }
}

// Convert deal data to owner inputs format
async function convertDealToOwnerInputs(dealId) {
  try {
    // Clean the deal ID (remove any field references like :1)
    const cleanDealId = dealId.split(':')[0];

    // Try to get deal by Deal_ID field first
    let deals = await base(F.deals.table)
      .select({
        filterByFormula: `{${F.deals.id}} = '${cleanDealId}'`
      })
      .all();

    // If not found by Deal_ID, try by Record_ID
    if (deals.length === 0) {
      deals = await base(F.deals.table)
        .select({
          filterByFormula: `{${F.deals.recordId}} = '${cleanDealId}'`
        })
        .all();
    }

    // If still not found, try by Airtable record ID
    if (deals.length === 0) {
      try {
        const deal = await base(F.deals.table).find(cleanDealId);
        deals = [deal];
      } catch (error) {
        console.log('Deal not found by record ID:', cleanDealId);
      }
    }

    if (deals.length === 0) {
      throw new Error('Deal not found');
    }

    const deal = deals[0].fields;
    
    // Get location data if linked
    let locationData = {};
    if (deal['Location & Property'] && deal['Location & Property'].length > 0) {
      const locationId = deal['Location & Property'][0];
      const locations = await base('tblLw3HRrlYldDPcr')
        .select({
          filterByFormula: `{Record_ID} = '${locationId}'`
        })
        .all();
      
      if (locations.length > 0) {
        locationData = locations[0].fields;
      }
    }

    // Map deal data to owner inputs format
    return {
      location: {
        country: locationData.Country || 'Unknown',
        city: locationData.City || 'Unknown'
      },
      propertyType: locationData['Hotel Type'] || 'Urban',
      roomCount: parseInt(locationData['Total Number of Rooms/Keys']) || 100,
      assetStage: mapAssetStage(deal['Stage of Development']),
      physicalFormat: mapPhysicalFormat(locationData['Building Type']),
      facilities: mapFacilities(locationData),
      targetSegment: mapTargetSegment(locationData['Hotel Chain Scale']),
      brandPositioning: 'Hard brand', // Default assumption
      guestDemandFocus: mapGuestFocus(locationData['Hotel Service Model']),
      designFlexibility: 'Low', // Default assumption
      capitalStructure: 'Equity + debt', // Default assumption
      capExTolerance: mapCapExTolerance(deal['Project Type']),
      financialGoals: {
        riskTolerance: 'Balanced' // Default assumption
      },
      feeStructure: 'Lower royalty but higher support', // Default assumption
      operationalPreferences: {
        operatingModel: 'Franchise', // Default assumption
        controlPreference: 'High owner control' // Default assumption
      },
      strategicPriorities: {
        geographicStrategy: 'Enter new market' // Default assumption
      }
    };
  } catch (error) {
    console.error('Error converting deal to owner inputs:', error);
    throw error;
  }
}

// Helper functions to map deal data to owner input format
function mapAssetStage(stage) {
  const stageMapping = {
    'Concept': 'Concept only',
    'Site Acquired': 'Under development (new build)',
    'Under Construction': 'Under development (new build)',
    'Operating': 'Existing hotel (conversion)'
  };
  return stageMapping[stage] || 'Under development (new build)';
}

function mapPhysicalFormat(buildingType) {
  const formatMapping = {
    'High-Rise': 'Standalone',
    'Mid-Rise': 'Standalone',
    'Low-Rise': 'Standalone',
    'Mixed-Use': 'Mixed-use'
  };
  return formatMapping[buildingType] || 'Standalone';
}

function mapFacilities(locationData) {
  const facilities = [];
  if (locationData['Total F&B Outlet Size Sq. Meters']) {
    facilities.push('F&B outlets');
  }
  // Add more facility mappings based on your data structure
  return facilities;
}

function mapTargetSegment(chainScale) {
  const segmentMapping = {
    'Economy Chain': 'Economy',
    'Midscale Chain': 'Midscale',
    'Upscale Chain': 'Upscale',
    'Upper Upscale Chain': 'Upper-upscale',
    'Luxury Chain': 'Luxury'
  };
  return segmentMapping[chainScale] || 'Upscale';
}

function mapGuestFocus(serviceModel) {
  const focusMapping = {
    'Select-Service': 'Business',
    'Full-Service': 'Mixed',
    'Limited-Service': 'Business'
  };
  return focusMapping[serviceModel] || 'Business';
}

function mapCapExTolerance(projectType) {
  const toleranceMapping = {
    'New Build': 'High',
    'Conversion/Reflag': 'Moderate',
    'Renovation': 'Minimal'
  };
  return toleranceMapping[projectType] || 'Moderate';
}
