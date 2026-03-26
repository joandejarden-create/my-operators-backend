import Airtable from "airtable";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

// Field mappings for market intelligence data
const F = {
  deals: {
    table: "tblbvSxjiIhXzW6XW",
    id: "flddZVyzlh2RuEcje",
    name: "fldkKJzBOBoFCvbnx",
    stage: "flde0PSEQUhA9Jl5a",
    status: "fld4cvEAz0k3x8aaU",
    createdAt: "fldCreatedTime", // Assuming this field exists
    country: "fldCountry", // Assuming this field exists
    budget: "fldBudget", // Assuming this field exists
    rooms: "fldRooms" // Assuming this field exists
  },
  users: {
    table: "tbl6shiyz2wdUqE5F",
    id: "fldUX9GvjFcIbuzAR",
    country: "fld2LWEer7PgkSCe9"
  }
};

export default async function marketIntelligence(req, res) {
  try {
    if (req.method !== "POST") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const { region, timeframe } = req.body;
    
    // Calculate date range based on timeframe
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(endDate.getDate() - parseInt(timeframe));

    // Build filter formula for deals
    let filterFormula = `AND({${F.deals.status}} != 'Closed', {${F.deals.createdAt}} >= '${startDate.toISOString().split('T')[0]}')`;
    
    if (region !== 'all') {
      // Add region filter if specified
      const regionMap = {
        'north-america': ['US', 'CA', 'MX'],
        'europe': ['UK', 'DE', 'FR', 'ES', 'IT', 'NL', 'CH'],
        'asia-pacific': ['AU', 'JP', 'SG', 'HK', 'TH', 'VN', 'IN']
      };
      
      if (regionMap[region]) {
        const regionFilter = regionMap[region].map(country => `{${F.deals.country}} = '${country}'`).join(', ');
        filterFormula += `, OR(${regionFilter})`;
      }
    }

    // Fetch deals data
    const deals = await base(F.deals.table)
      .select({
        filterByFormula: filterFormula,
        fields: [F.deals.name, F.deals.stage, F.deals.status, F.deals.budget, F.deals.rooms, F.deals.country],
        sort: [{ field: F.deals.createdAt, direction: 'desc' }]
      })
      .all();

    // Process data for metrics
    const metrics = calculateMetrics(deals);
    
    // Generate chart data
    const charts = generateChartData(deals, timeframe);
    
    // Generate market alerts
    const alerts = generateMarketAlerts(deals, metrics);

    res.json({
      success: true,
      metrics,
      charts,
      alerts,
      lastUpdated: new Date().toISOString()
    });

  } catch (error) {
    console.error("Error in market intelligence:", error);
    res.status(500).json({ 
      error: "Internal Server Error", 
      details: error.message 
    });
  }
}

// Calculate key metrics
function calculateMetrics(deals) {
  const totalDeals = deals.length;
  const activeDeals = deals.filter(deal => deal.fields[F.deals.status] === 'Active').length;
  
  // Calculate average deal size (mock data for now)
  const avgDealSize = totalDeals > 0 ? Math.round(Math.random() * 50 + 25) : 0;
  
  // Calculate market activity (mock data)
  const marketActivity = Math.round(Math.random() * 20 + 70);
  
  // Calculate conversion rate (mock data)
  const conversionRate = Math.round(Math.random() * 15 + 65);

  return {
    activeDeals: activeDeals.toString(),
    avgDealSize: `$${avgDealSize}M`,
    marketActivity: `${marketActivity}%`,
    conversionRate: `${conversionRate}%`
  };
}

// Generate chart data
function generateChartData(deals, timeframe) {
  const days = parseInt(timeframe);
  const labels = [];
  const dealVolumeData = [];
  
  // Generate labels for the time period
  for (let i = days - 1; i >= 0; i--) {
    const date = new Date();
    date.setDate(date.getDate() - i);
    labels.push(date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }));
  }

  // Generate mock deal volume data
  for (let i = 0; i < days; i++) {
    dealVolumeData.push(Math.floor(Math.random() * 20 + 5));
  }

  // Generate market distribution data
  const marketDistribution = {
    labels: ['Luxury', 'Upper Upscale', 'Upscale', 'Midscale', 'Economy'],
    data: [25, 30, 20, 15, 10]
  };

  return {
    dealVolume: {
      labels,
      data: dealVolumeData
    },
    marketDistribution
  };
}

// Generate market alerts
function generateMarketAlerts(deals, metrics) {
  const alerts = [];
  
  // High priority alerts
  if (parseInt(metrics.activeDeals) > 50) {
    alerts.push({
      priority: 'high',
      title: 'High Deal Volume',
      description: `Active deals have reached ${metrics.activeDeals}, indicating strong market activity.`,
      timestamp: new Date(Date.now() - Math.random() * 3600000).toISOString()
    });
  }

  // Medium priority alerts
  if (parseInt(metrics.conversionRate) > 80) {
    alerts.push({
      priority: 'medium',
      title: 'Strong Conversion Rate',
      description: `Conversion rate of ${metrics.conversionRate} is above market average.`,
      timestamp: new Date(Date.now() - Math.random() * 7200000).toISOString()
    });
  }

  // Low priority alerts
  alerts.push({
    priority: 'low',
    title: 'Market Update Available',
    description: 'New market analysis report is available for download.',
    timestamp: new Date(Date.now() - Math.random() * 86400000).toISOString()
  });

  // Add some random alerts for demonstration
  const randomAlerts = [
    {
      priority: 'medium',
      title: 'Luxury Segment Growth',
      description: 'Luxury hotel deals increased 23% this quarter.',
      timestamp: new Date(Date.now() - Math.random() * 172800000).toISOString()
    },
    {
      priority: 'low',
      title: 'New Market Opportunity',
      description: 'Southeast Asia showing 35% growth in hotel investment.',
      timestamp: new Date(Date.now() - Math.random() * 259200000).toISOString()
    }
  ];

  alerts.push(...randomAlerts.slice(0, Math.floor(Math.random() * 3)));

  return alerts.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
}

// Additional utility functions for market intelligence

// Get market trends
export async function getMarketTrends(req, res) {
  try {
    const { region, propertyType, timeframe } = req.body;
    
    // This would typically fetch from external APIs or your own data warehouse
    const trends = {
      luxury: { growth: 23, avgDealSize: 85, marketShare: 15 },
      upscale: { growth: 12, avgDealSize: 45, marketShare: 35 },
      midscale: { growth: 8, avgDealSize: 25, marketShare: 40 },
      economy: { growth: 5, avgDealSize: 15, marketShare: 10 }
    };

    res.json({
      success: true,
      trends,
      lastUpdated: new Date().toISOString()
    });

  } catch (error) {
    console.error("Error getting market trends:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
}

// Get competitive analysis
export async function getCompetitiveAnalysis(req, res) {
  try {
    const { dealId, region } = req.body;
    
    // Mock competitive analysis data
    const analysis = {
      marketPosition: 'Above Average',
      competitiveScore: 78,
      strengths: [
        'Prime location',
        'Strong brand alignment',
        'Competitive pricing'
      ],
      opportunities: [
        'Consider extended stay amenities',
        'Explore co-working spaces',
        'Add wellness facilities'
      ],
      benchmarks: {
        avgDealSize: 45,
        avgTimeline: 180,
        successRate: 72
      }
    };

    res.json({
      success: true,
      analysis,
      lastUpdated: new Date().toISOString()
    });

  } catch (error) {
    console.error("Error getting competitive analysis:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
}

// Get market forecasts
export async function getMarketForecasts(req, res) {
  try {
    const { region, timeframe } = req.body;
    
    // Mock forecast data
    const forecasts = {
      nextQuarter: {
        dealVolume: '+15%',
        avgDealSize: '+8%',
        marketActivity: '+12%'
      },
      nextYear: {
        dealVolume: '+25%',
        avgDealSize: '+15%',
        marketActivity: '+20%'
      },
      keyDrivers: [
        'Economic recovery driving business travel',
        'Leisure travel returning to pre-pandemic levels',
        'New brand partnerships expanding opportunities'
      ]
    };

    res.json({
      success: true,
      forecasts,
      lastUpdated: new Date().toISOString()
    });

  } catch (error) {
    console.error("Error getting market forecasts:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
}






