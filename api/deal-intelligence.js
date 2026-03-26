import Airtable from "airtable";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

// Field mappings for deal intelligence
const F = {
  deals: {
    table: "tblbvSxjiIhXzW6XW",
    id: "flddZVyzlh2RuEcje",
    name: "fldkKJzBOBoFCvbnx",
    stage: "flde0PSEQUhA9Jl5a",
    status: "fld4cvEAz0k3x8aaU",
    country: "fldCountry",
    budget: "fldBudget",
    rooms: "fldRooms",
    propertyType: "fldPropertyType"
  }
};

// Analyze deal and provide intelligence
export async function analyzeDeal(req, res) {
  try {
    const { dealId, dealData } = req.body;

    if (!dealId && !dealData) {
      return res.status(400).json({ error: "Deal ID or deal data is required" });
    }

    let deal;
    if (dealId) {
      // Fetch deal from Airtable
      const deals = await base(F.deals.table)
        .select({
          filterByFormula: `{${F.deals.id}} = '${dealId}'`
        })
        .all();

      if (deals.length === 0) {
        return res.status(404).json({ error: "Deal not found" });
      }

      deal = deals[0].fields;
    } else {
      deal = dealData;
    }

    // Perform analysis
    const analysis = await performDealAnalysis(deal);

    res.json({
      success: true,
      analysis,
      recommendations: generateRecommendations(analysis),
      marketContext: await getMarketContext(deal),
      lastUpdated: new Date().toISOString()
    });

  } catch (error) {
    console.error("Error analyzing deal:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
}

// Perform comprehensive deal analysis
async function performDealAnalysis(deal) {
  const analysis = {
    overallScore: 0,
    marketFit: 0,
    financialViability: 0,
    competitivePosition: 0,
    timingScore: 0,
    riskAssessment: 'medium',
    strengths: [],
    weaknesses: [],
    opportunities: [],
    threats: []
  };

  // Market Fit Analysis
  analysis.marketFit = await analyzeMarketFit(deal);
  
  // Financial Viability Analysis
  analysis.financialViability = await analyzeFinancialViability(deal);
  
  // Competitive Position Analysis
  analysis.competitivePosition = await analyzeCompetitivePosition(deal);
  
  // Timing Analysis
  analysis.timingScore = await analyzeTiming(deal);
  
  // Risk Assessment
  analysis.riskAssessment = await assessRisk(deal);
  
  // Calculate overall score
  analysis.overallScore = Math.round(
    (analysis.marketFit * 0.25) +
    (analysis.financialViability * 0.25) +
    (analysis.competitivePosition * 0.20) +
    (analysis.timingScore * 0.15) +
    (getRiskScore(analysis.riskAssessment) * 0.15)
  );

  // SWOT Analysis
  analysis.strengths = generateStrengths(deal, analysis);
  analysis.weaknesses = generateWeaknesses(deal, analysis);
  analysis.opportunities = generateOpportunities(deal, analysis);
  analysis.threats = generateThreats(deal, analysis);

  return analysis;
}

// Analyze market fit
async function analyzeMarketFit(deal) {
  // Mock analysis - in reality, this would use market data
  const marketFactors = {
    location: getLocationScore(deal.country),
    propertyType: getPropertyTypeScore(deal.propertyType),
    size: getSizeScore(deal.rooms),
    budget: getBudgetScore(deal.budget)
  };

  return Math.round(
    (marketFactors.location * 0.3) +
    (marketFactors.propertyType * 0.3) +
    (marketFactors.size * 0.2) +
    (marketFactors.budget * 0.2)
  );
}

// Analyze financial viability
async function analyzeFinancialViability(deal) {
  // Mock analysis - would use financial models
  const budget = parseFloat(deal.budget?.replace(/[^0-9.]/g, '') || '0');
  const rooms = parseInt(deal.rooms || '0');
  
  if (budget === 0 || rooms === 0) return 50;

  const costPerRoom = budget / rooms;
  
  // Score based on cost per room benchmarks
  if (costPerRoom < 100000) return 90; // Excellent
  if (costPerRoom < 200000) return 75; // Good
  if (costPerRoom < 300000) return 60; // Average
  return 40; // Below average
}

// Analyze competitive position
async function analyzeCompetitivePosition(deal) {
  // Mock analysis - would use competitive data
  const region = deal.country;
  const propertyType = deal.propertyType;
  
  // Simulate competitive analysis
  const baseScore = 70;
  const regionMultiplier = getRegionCompetitionMultiplier(region);
  const typeMultiplier = getPropertyTypeCompetitionMultiplier(propertyType);
  
  return Math.min(100, Math.round(baseScore * regionMultiplier * typeMultiplier));
}

// Analyze timing
async function analyzeTiming(deal) {
  // Mock analysis - would use market timing data
  const currentDate = new Date();
  const quarter = Math.floor(currentDate.getMonth() / 3) + 1;
  
  // Simulate seasonal and market cycle analysis
  const seasonalScore = getSeasonalScore(quarter);
  const marketCycleScore = getMarketCycleScore();
  
  return Math.round((seasonalScore + marketCycleScore) / 2);
}

// Assess risk
async function assessRisk(deal) {
  const riskFactors = [];
  
  // Budget risk
  const budget = parseFloat(deal.budget?.replace(/[^0-9.]/g, '') || '0');
  if (budget > 100) riskFactors.push('high-budget');
  
  // Location risk
  const location = deal.country;
  if (['emerging', 'volatile'].includes(getLocationRiskLevel(location))) {
    riskFactors.push('location-risk');
  }
  
  // Property type risk
  const propertyType = deal.propertyType;
  if (['luxury', 'boutique'].includes(propertyType?.toLowerCase())) {
    riskFactors.push('type-risk');
  }
  
  // Determine overall risk level
  if (riskFactors.length >= 3) return 'high';
  if (riskFactors.length >= 2) return 'medium-high';
  if (riskFactors.length >= 1) return 'medium';
  return 'low';
}

// Get market context
async function getMarketContext(deal) {
  return {
    regionTrends: await getRegionTrends(deal.country),
    propertyTypeTrends: await getPropertyTypeTrends(deal.propertyType),
    competitiveLandscape: await getCompetitiveLandscape(deal),
    marketOutlook: await getMarketOutlook(deal)
  };
}

// Generate recommendations
function generateRecommendations(analysis) {
  const recommendations = [];
  
  if (analysis.marketFit < 70) {
    recommendations.push({
      priority: 'high',
      category: 'market-fit',
      title: 'Improve Market Positioning',
      description: 'Consider adjusting property type or location to better match market demand.',
      impact: 'High',
      effort: 'Medium'
    });
  }
  
  if (analysis.financialViability < 60) {
    recommendations.push({
      priority: 'high',
      category: 'financial',
      title: 'Optimize Financial Structure',
      description: 'Review budget allocation and consider cost-saving measures.',
      impact: 'High',
      effort: 'High'
    });
  }
  
  if (analysis.competitivePosition < 65) {
    recommendations.push({
      priority: 'medium',
      category: 'competitive',
      title: 'Enhance Competitive Advantage',
      description: 'Identify unique value propositions to differentiate from competitors.',
      impact: 'Medium',
      effort: 'Medium'
    });
  }
  
  if (analysis.timingScore < 60) {
    recommendations.push({
      priority: 'medium',
      category: 'timing',
      title: 'Consider Timing Adjustment',
      description: 'Evaluate if delaying or accelerating the project would improve outcomes.',
      impact: 'Medium',
      effort: 'Low'
    });
  }
  
  if (analysis.riskAssessment === 'high') {
    recommendations.push({
      priority: 'high',
      category: 'risk',
      title: 'Mitigate Risk Factors',
      description: 'Develop contingency plans and risk mitigation strategies.',
      impact: 'High',
      effort: 'High'
    });
  }
  
  return recommendations;
}

// Helper functions for scoring
function getLocationScore(country) {
  const scores = {
    'US': 85, 'CA': 80, 'UK': 75, 'AU': 70, 'DE': 75,
    'FR': 70, 'ES': 65, 'IT': 65, 'JP': 80, 'SG': 75
  };
  return scores[country] || 60;
}

function getPropertyTypeScore(propertyType) {
  const scores = {
    'Luxury': 70, 'Upper Upscale': 80, 'Upscale': 85,
    'Midscale': 90, 'Economy': 75, 'Boutique': 65
  };
  return scores[propertyType] || 70;
}

function getSizeScore(rooms) {
  const roomCount = parseInt(rooms || '0');
  if (roomCount < 50) return 60;
  if (roomCount < 100) return 70;
  if (roomCount < 200) return 85;
  if (roomCount < 400) return 90;
  return 75;
}

function getBudgetScore(budget) {
  const budgetValue = parseFloat(budget?.replace(/[^0-9.]/g, '') || '0');
  if (budgetValue < 10) return 60;
  if (budgetValue < 25) return 70;
  if (budgetValue < 50) return 80;
  if (budgetValue < 100) return 85;
  return 75;
}

function getRiskScore(riskLevel) {
  const scores = { 'low': 90, 'medium': 70, 'medium-high': 50, 'high': 30 };
  return scores[riskLevel] || 70;
}

function getRegionCompetitionMultiplier(region) {
  const multipliers = {
    'US': 1.0, 'CA': 0.95, 'UK': 0.9, 'AU': 0.85,
    'DE': 0.9, 'FR': 0.85, 'ES': 0.8, 'IT': 0.8
  };
  return multipliers[region] || 0.8;
}

function getPropertyTypeCompetitionMultiplier(propertyType) {
  const multipliers = {
    'Luxury': 0.7, 'Upper Upscale': 0.8, 'Upscale': 0.9,
    'Midscale': 1.0, 'Economy': 0.85, 'Boutique': 0.75
  };
  return multipliers[propertyType] || 0.8;
}

function getSeasonalScore(quarter) {
  const scores = { 1: 60, 2: 85, 3: 90, 4: 70 };
  return scores[quarter] || 70;
}

function getMarketCycleScore() {
  // Mock market cycle analysis
  return 75;
}

function getLocationRiskLevel(location) {
  const riskLevels = {
    'US': 'low', 'CA': 'low', 'UK': 'low', 'AU': 'low',
    'DE': 'low', 'FR': 'low', 'ES': 'medium', 'IT': 'medium'
  };
  return riskLevels[location] || 'medium';
}

// Generate SWOT analysis components
function generateStrengths(deal, analysis) {
  const strengths = [];
  
  if (analysis.marketFit >= 80) {
    strengths.push('Strong market positioning');
  }
  
  if (analysis.financialViability >= 80) {
    strengths.push('Solid financial foundation');
  }
  
  if (deal.country === 'US' || deal.country === 'CA') {
    strengths.push('Stable market environment');
  }
  
  if (parseInt(deal.rooms || '0') >= 200) {
    strengths.push('Economies of scale');
  }
  
  return strengths;
}

function generateWeaknesses(deal, analysis) {
  const weaknesses = [];
  
  if (analysis.marketFit < 70) {
    weaknesses.push('Limited market appeal');
  }
  
  if (analysis.financialViability < 60) {
    weaknesses.push('Financial constraints');
  }
  
  if (analysis.competitivePosition < 60) {
    weaknesses.push('Strong competition');
  }
  
  if (deal.propertyType === 'Boutique') {
    weaknesses.push('Niche market segment');
  }
  
  return weaknesses;
}

function generateOpportunities(deal, analysis) {
  const opportunities = [];
  
  if (deal.country === 'US') {
    opportunities.push('Growing domestic travel market');
  }
  
  if (deal.propertyType === 'Luxury') {
    opportunities.push('High-end market recovery');
  }
  
  if (parseInt(deal.rooms || '0') < 100) {
    opportunities.push('Boutique hotel trend');
  }
  
  opportunities.push('Technology integration opportunities');
  opportunities.push('Sustainability initiatives');
  
  return opportunities;
}

function generateThreats(deal, analysis) {
  const threats = [];
  
  if (analysis.riskAssessment === 'high') {
    threats.push('High market volatility');
  }
  
  if (deal.propertyType === 'Economy') {
    threats.push('Price competition pressure');
  }
  
  threats.push('Economic uncertainty');
  threats.push('Regulatory changes');
  threats.push('Supply chain disruptions');
  
  return threats;
}

// Mock data functions (would be replaced with real API calls)
async function getRegionTrends(country) {
  return {
    growth: Math.random() * 20 + 5,
    avgDealSize: Math.random() * 50 + 25,
    competition: 'Medium'
  };
}

async function getPropertyTypeTrends(propertyType) {
  return {
    demand: Math.random() * 30 + 50,
    supply: Math.random() * 20 + 30,
    pricing: Math.random() * 15 + 5
  };
}

async function getCompetitiveLandscape(deal) {
  return {
    directCompetitors: Math.floor(Math.random() * 10 + 5),
    marketShare: Math.random() * 20 + 10,
    barriers: 'Medium'
  };
}

async function getMarketOutlook(deal) {
  return {
    shortTerm: 'Positive',
    longTerm: 'Stable',
    keyDrivers: ['Economic recovery', 'Travel demand', 'Brand expansion']
  };
}






