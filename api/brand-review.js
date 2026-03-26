import Airtable from "airtable";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);

// Field mappings for brand review - extending the existing deal structure
const F = {
  deals: {
    table: "tblbvSxjiIhXzW6XW",          // Deals table ID
    id: "flddZVyzlh2RuEcje",             // Deal_ID
    recordId: "fld5sPHT4p1Nw7n0a",       // Record_ID
    status: "fld4cvEAz0k3x8aaU",         // Deal Status
    name: "fldkKJzBOBoFCvbnx",           // Property Name
    userLink: "fldALlSB9UsnLhgvI",       // Link to Users
    stage: "flde0PSEQUhA9Jl5a",          // Stage of Development
    
    // Additional fields for brand review (these would need to be added to your Airtable)
    country: "fldCountry",               // Country field
    city: "fldCity",                     // City field
    rooms: "fldRooms",                   // Number of rooms
    budget: "fldBudget",                 // Project budget
    propertyType: "fldPropertyType",     // Property type (Luxury, Upscale, etc.)
    description: "fldDescription",       // Project description
    timeline: "fldTimeline",             // Expected timeline
    brandExperience: "fldBrandExperience", // Owner's brand experience
    specialConsiderations: "fldSpecialConsiderations", // Special requirements
    submitDate: "fldCreatedTime",        // When deal was submitted
    lastUpdated: "fldLastUpdated",       // Last status update
    brandNotes: "fldBrandNotes",         // Brand's internal notes
    matchScore: "fldMatchScore",         // Algorithmic match score
    brandResponse: "fldBrandResponse",   // Brand's response (approve/decline/etc.)
    responseDate: "fldResponseDate",     // When brand responded
    responseNotes: "fldResponseNotes"    // Notes from brand response
  },
  users: {
    table: "tbl6shiyz2wdUqE5F",         // Users table ID
    id: "fldUX9GvjFcIbuzAR",            // User_ID (Auto Number)
    recordId: "fld8YSQaChTZCexeL",      // Record_ID
    email: "fldBl7IXEscwkMhnZ",         // Email
    firstName: "fldG5nbAijQkUVSzr",     // First Name
    lastName: "fldV0g50iRB8J46Hh",      // Last Name
    company: "fldCompany",              // Company/Organization
    phone: "fldPhone",                  // Phone number
    country: "fld2LWEer7PgkSCe9"        // Country
  }
};

// Get deals for brand review
export async function getBrandReviewDeals(req, res) {
  try {
    if (req.method !== "GET") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    // Authentication check - in production, you'd validate the brand user's session
    const brandUserId = req.headers["x-brand-user-id"];
    if (!brandUserId) {
      return res.status(401).json({ error: "Unauthorized - Brand user ID required" });
    }

    const { status, limit = 50, offset = 0, sort = "created_time", sortDirection = "desc" } = req.query;

    // Build filter formula
    let filterFormula = "";
    if (status) {
      filterFormula = `{${F.deals.status}} = '${status}'`;
    }

    // Get deals with user information
    const deals = await base(F.deals.table)
      .select({
        filterByFormula: filterFormula || "",
        sort: [{ field: F.deals.submitDate, direction: sortDirection }],
        maxRecords: parseInt(limit),
        offset: parseInt(offset)
      })
      .all();

    // Enrich deals with user information
    const enrichedDeals = await Promise.all(
      deals.map(async (deal) => {
        const dealData = deal.fields;
        
        // Get user information if linked
        let userData = {};
        if (dealData[F.deals.userLink] && dealData[F.deals.userLink].length > 0) {
          try {
            const user = await base(F.users.table).find(dealData[F.deals.userLink][0]);
            userData = {
              id: user.id,
              firstName: user.fields[F.users.firstName] || "",
              lastName: user.fields[F.users.lastName] || "",
              email: user.fields[F.users.email] || "",
              company: user.fields[F.users.company] || "",
              phone: user.fields[F.users.phone] || "",
              country: user.fields[F.users.country] || ""
            };
          } catch (error) {
            console.error("Error fetching user data:", error);
          }
        }

        // Calculate match score if not present
        const matchScore = dealData[F.deals.matchScore] || calculateMatchScore(dealData, userData);

        return {
          id: deal.id,
          propertyName: dealData[F.deals.name] || "",
          status: dealData[F.deals.status] || "new",
          stage: dealData[F.deals.stage] || "",
          country: dealData[F.deals.country] || userData.country || "",
          city: dealData[F.deals.city] || "",
          rooms: dealData[F.deals.rooms] || 0,
          budget: dealData[F.deals.budget] || "",
          propertyType: dealData[F.deals.propertyType] || "",
          description: dealData[F.deals.description] || "",
          timeline: dealData[F.deals.timeline] || "",
          brandExperience: dealData[F.deals.brandExperience] || "",
          specialConsiderations: dealData[F.deals.specialConsiderations] || "",
          submitDate: dealData[F.deals.submitDate] || new Date().toISOString(),
          lastUpdated: dealData[F.deals.lastUpdated] || new Date().toISOString(),
          matchScore: matchScore,
          brandNotes: dealData[F.deals.brandNotes] || "",
          brandResponse: dealData[F.deals.brandResponse] || "",
          responseDate: dealData[F.deals.responseDate] || "",
          responseNotes: dealData[F.deals.responseNotes] || "",
          owner: {
            name: `${userData.firstName} ${userData.lastName}`.trim(),
            company: userData.company,
            email: userData.email,
            phone: userData.phone,
            country: userData.country
          }
        };
      })
    );

    res.json({
      success: true,
      deals: enrichedDeals,
      total: deals.length,
      hasMore: deals.length === parseInt(limit)
    });

  } catch (error) {
    console.error("Error fetching brand review deals:", error);
    res.status(500).json({ error: "Internal Server Error", details: error.message });
  }
}

// Update deal status from brand review
export async function updateDealStatus(req, res) {
  try {
    if (req.method !== "POST") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    // Authentication check
    const brandUserId = req.headers["x-brand-user-id"];
    if (!brandUserId) {
      return res.status(401).json({ error: "Unauthorized - Brand user ID required" });
    }

    const { dealId, status, notes = "", responseNotes = "" } = req.body;

    if (!dealId || !status) {
      return res.status(400).json({ error: "Deal ID and status are required" });
    }

    // Validate status
    const validStatuses = ["new", "under-review", "approved", "declined", "request-info", "schedule-call"];
    if (!validStatuses.includes(status)) {
      return res.status(400).json({ error: "Invalid status value" });
    }

    // Update the deal record
    const updateFields = {
      [F.deals.status]: status,
      [F.deals.lastUpdated]: new Date().toISOString(),
      [F.deals.brandResponse]: status,
      [F.deals.responseDate]: new Date().toISOString()
    };

    // Add notes if provided
    if (notes) {
      updateFields[F.deals.brandNotes] = notes;
    }

    if (responseNotes) {
      updateFields[F.deals.responseNotes] = responseNotes;
    }

    const updatedDeal = await base(F.deals.table).update(dealId, updateFields, { typecast: true });

    // Log the action for audit trail
    console.log(`Brand user ${brandUserId} updated deal ${dealId} to status: ${status}`);

    res.json({
      success: true,
      dealId: dealId,
      status: status,
      updatedAt: new Date().toISOString(),
      message: "Deal status updated successfully"
    });

  } catch (error) {
    console.error("Error updating deal status:", error);
    res.status(500).json({ error: "Internal Server Error", details: error.message });
  }
}

// Get deal details for brand review
export async function getDealDetails(req, res) {
  try {
    if (req.method !== "GET") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const brandUserId = req.headers["x-brand-user-id"];
    if (!brandUserId) {
      return res.status(401).json({ error: "Unauthorized - Brand user ID required" });
    }

    const { dealId } = req.query;

    if (!dealId) {
      return res.status(400).json({ error: "Deal ID is required" });
    }

    // Get the deal record
    const deal = await base(F.deals.table).find(dealId);
    const dealData = deal.fields;

    // Get user information
    let userData = {};
    if (dealData[F.deals.userLink] && dealData[F.deals.userLink].length > 0) {
      try {
        const user = await base(F.users.table).find(dealData[F.deals.userLink][0]);
        userData = {
          id: user.id,
          firstName: user.fields[F.users.firstName] || "",
          lastName: user.fields[F.users.lastName] || "",
          email: user.fields[F.users.email] || "",
          company: user.fields[F.users.company] || "",
          phone: user.fields[F.users.phone] || "",
          country: user.fields[F.users.country] || ""
        };
      } catch (error) {
        console.error("Error fetching user data:", error);
      }
    }

    const enrichedDeal = {
      id: deal.id,
      propertyName: dealData[F.deals.name] || "",
      status: dealData[F.deals.status] || "new",
      stage: dealData[F.deals.stage] || "",
      country: dealData[F.deals.country] || userData.country || "",
      city: dealData[F.deals.city] || "",
      rooms: dealData[F.deals.rooms] || 0,
      budget: dealData[F.deals.budget] || "",
      propertyType: dealData[F.deals.propertyType] || "",
      description: dealData[F.deals.description] || "",
      timeline: dealData[F.deals.timeline] || "",
      brandExperience: dealData[F.deals.brandExperience] || "",
      specialConsiderations: dealData[F.deals.specialConsiderations] || "",
      submitDate: dealData[F.deals.submitDate] || new Date().toISOString(),
      lastUpdated: dealData[F.deals.lastUpdated] || new Date().toISOString(),
      matchScore: dealData[F.deals.matchScore] || calculateMatchScore(dealData, userData),
      brandNotes: dealData[F.deals.brandNotes] || "",
      brandResponse: dealData[F.deals.brandResponse] || "",
      responseDate: dealData[F.deals.responseDate] || "",
      responseNotes: dealData[F.deals.responseNotes] || "",
      owner: {
        name: `${userData.firstName} ${userData.lastName}`.trim(),
        company: userData.company,
        email: userData.email,
        phone: userData.phone,
        country: userData.country
      }
    };

    res.json({
      success: true,
      deal: enrichedDeal
    });

  } catch (error) {
    console.error("Error fetching deal details:", error);
    res.status(500).json({ error: "Internal Server Error", details: error.message });
  }
}

// Bulk update multiple deals
export async function bulkUpdateDeals(req, res) {
  try {
    if (req.method !== "POST") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const brandUserId = req.headers["x-brand-user-id"];
    if (!brandUserId) {
      return res.status(401).json({ error: "Unauthorized - Brand user ID required" });
    }

    const { dealIds, status, notes = "", responseNotes = "" } = req.body;

    if (!dealIds || !Array.isArray(dealIds) || dealIds.length === 0) {
      return res.status(400).json({ error: "Deal IDs array is required" });
    }

    if (!status) {
      return res.status(400).json({ error: "Status is required" });
    }

    const validStatuses = ["new", "under-review", "approved", "declined", "request-info", "schedule-call"];
    if (!validStatuses.includes(status)) {
      return res.status(400).json({ error: "Invalid status value" });
    }

    // Update all deals
    const updatePromises = dealIds.map(dealId => {
      const updateFields = {
        [F.deals.status]: status,
        [F.deals.lastUpdated]: new Date().toISOString(),
        [F.deals.brandResponse]: status,
        [F.deals.responseDate]: new Date().toISOString()
      };

      if (notes) {
        updateFields[F.deals.brandNotes] = notes;
      }

      if (responseNotes) {
        updateFields[F.deals.responseNotes] = responseNotes;
      }

      return base(F.deals.table).update(dealId, updateFields, { typecast: true });
    });

    await Promise.all(updatePromises);

    console.log(`Brand user ${brandUserId} bulk updated ${dealIds.length} deals to status: ${status}`);

    res.json({
      success: true,
      updatedCount: dealIds.length,
      status: status,
      updatedAt: new Date().toISOString(),
      message: `${dealIds.length} deals updated successfully`
    });

  } catch (error) {
    console.error("Error bulk updating deals:", error);
    res.status(500).json({ error: "Internal Server Error", details: error.message });
  }
}

// Calculate match score based on deal criteria
function calculateMatchScore(dealData, userData) {
  let score = 0;
  let factors = 0;

  // Property type scoring (example criteria)
  const propertyType = dealData[F.deals.propertyType];
  if (propertyType) {
    factors++;
    switch (propertyType.toLowerCase()) {
      case 'luxury':
        score += 85;
        break;
      case 'upper upscale':
        score += 90;
        break;
      case 'upscale':
        score += 80;
        break;
      case 'midscale':
        score += 75;
        break;
      default:
        score += 70;
    }
  }

  // Room count scoring
  const rooms = parseInt(dealData[F.deals.rooms]) || 0;
  if (rooms > 0) {
    factors++;
    if (rooms >= 200 && rooms <= 400) {
      score += 90;
    } else if (rooms >= 100 && rooms <= 500) {
      score += 80;
    } else {
      score += 70;
    }
  }

  // Budget scoring
  const budget = dealData[F.deals.budget];
  if (budget) {
    factors++;
    const budgetNum = parseFloat(budget.replace(/[$,]/g, ''));
    if (budgetNum >= 25000000 && budgetNum <= 75000000) {
      score += 85;
    } else if (budgetNum >= 10000000 && budgetNum <= 100000000) {
      score += 75;
    } else {
      score += 65;
    }
  }

  // Owner experience scoring
  const brandExperience = dealData[F.deals.brandExperience];
  if (brandExperience) {
    factors++;
    if (brandExperience.toLowerCase().includes('marriott') || 
        brandExperience.toLowerCase().includes('hilton') ||
        brandExperience.toLowerCase().includes('ihg')) {
      score += 90;
    } else if (brandExperience.toLowerCase().includes('brand') || 
               brandExperience.toLowerCase().includes('hotel')) {
      score += 75;
    } else {
      score += 60;
    }
  }

  // Location scoring (example)
  const country = dealData[F.deals.country] || userData.country;
  if (country) {
    factors++;
    const preferredCountries = ['US', 'CA', 'UK', 'AU', 'DE'];
    if (preferredCountries.includes(country)) {
      score += 85;
    } else {
      score += 70;
    }
  }

  // Return average score, or default if no factors
  return factors > 0 ? Math.round(score / factors) : 75;
}

// Get brand review statistics
export async function getBrandReviewStats(req, res) {
  try {
    if (req.method !== "GET") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const brandUserId = req.headers["x-brand-user-id"];
    if (!brandUserId) {
      return res.status(401).json({ error: "Unauthorized - Brand user ID required" });
    }

    // Get all deals for statistics
    const allDeals = await base(F.deals.table)
      .select({
        fields: [F.deals.status, F.deals.submitDate, F.deals.matchScore]
      })
      .all();

    const stats = {
      total: allDeals.length,
      new: 0,
      underReview: 0,
      approved: 0,
      declined: 0,
      avgMatchScore: 0,
      responseTime: 0
    };

    let totalScore = 0;
    let scoreCount = 0;

    allDeals.forEach(deal => {
      const status = deal.fields[F.deals.status];
      const matchScore = deal.fields[F.deals.matchScore];

      switch (status) {
        case 'new':
          stats.new++;
          break;
        case 'under-review':
          stats.underReview++;
          break;
        case 'approved':
          stats.approved++;
          break;
        case 'declined':
          stats.declined++;
          break;
      }

      if (matchScore) {
        totalScore += matchScore;
        scoreCount++;
      }
    });

    stats.avgMatchScore = scoreCount > 0 ? Math.round(totalScore / scoreCount) : 0;

    res.json({
      success: true,
      stats: stats
    });

  } catch (error) {
    console.error("Error fetching brand review stats:", error);
    res.status(500).json({ error: "Internal Server Error", details: error.message });
  }
}

// Get matched brands for brand review
export async function getMatchedBrands(req, res) {
  try {
    const brandUserId = req.headers["x-brand-user-id"];
    // For now, allow without auth for development - add auth check in production
    // if (!brandUserId) {
    //   return res.status(401).json({ error: "Unauthorized - Brand user ID required" });
    // }

    const { dealId } = req.query;

    // If dealId is provided, use Brand Fit Analyzer for real scores
    if (dealId) {
      try {
        // Call the Brand Fit Analyzer API endpoint internally
        const brandFitModule = await import("./brand-fit-analyzer.js");
        
        // Get deal and convert to owner inputs (replicate getDealBrandFit logic)
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
          return res.status(404).json({ 
            success: false,
            error: "Deal not found",
            matchedBrands: []
          });
        }
        
        // Call Brand Fit Analyzer via HTTP request to our own endpoint
        const http = await import('http');
        const port = process.env.PORT || 3000;
        const brandFitUrl = `/api/brand-fit-analyzer/deal?dealId=${encodeURIComponent(cleanDealId)}`;
        
        let brandFitData = null;
        
        try {
          // Make HTTP request to brand fit analyzer endpoint
          const response = await new Promise((resolve, reject) => {
            const request = http.get(`http://localhost:${port}${brandFitUrl}`, (res) => {
              let data = '';
              res.on('data', chunk => data += chunk);
              res.on('end', () => {
                try {
                  const jsonData = JSON.parse(data);
                  if (res.statusCode === 200) {
                    resolve(jsonData);
                  } else {
                    reject(new Error(jsonData.error || 'Brand fit analysis failed'));
                  }
                } catch (parseError) {
                  reject(parseError);
                }
              });
            });
            request.on('error', reject);
            request.setTimeout(30000, () => {
              request.destroy();
              reject(new Error('Request timeout'));
            });
          });
          
          brandFitData = response;
          
          if (!brandFitData || !brandFitData.success) {
            throw new Error(brandFitData?.error || 'Brand fit analysis failed');
          }
          
          // Debug: Log brand fits to ensure fitBreakdown is present
          if (brandFitData.brandFits && brandFitData.brandFits.length > 0) {
            console.log('Sample brand fit data:', JSON.stringify(brandFitData.brandFits[0], null, 2));
          }
        } catch (httpError) {
          console.error("Error calling Brand Fit Analyzer via HTTP:", httpError);
          throw httpError;
        }
        
        // Transform brand fit data to matched brands format
        const matchedBrands = brandFitData.brandFits.map((brandFit, index) => {
          // Get additional brand info from Airtable if needed
          const brandName = brandFit.brand || "Unknown Brand";
          const parentCompany = brandFit.parentCompany || "Unknown";
          
          // Determine response time based on fit score (higher score = better response)
          let respondTime, respondTimeColor;
          if (brandFit.fitScore >= 85) {
            respondTime = "Lightning Fast - Occasionally";
            respondTimeColor = "green";
          } else if (brandFit.fitScore >= 75) {
            respondTime = "Very Fast - Frequently";
            respondTimeColor = "green";
          } else if (brandFit.fitScore >= 65) {
            respondTime = "Responsive - Occasionally";
            respondTimeColor = "orange";
          } else {
            respondTime = "Unresponsive - Rarely";
            respondTimeColor = "red";
          }
          
          return {
            id: `brand-${index}-${brandName.replace(/\s+/g, '-').toLowerCase()}`,
            brandName: brandName,
            parentCompany: parentCompany,
            chainScale: brandFit.chainScale || "Unknown",
            fitScore: brandFit.fitScore,
            fitBreakdown: brandFit.fitBreakdown || {},
            strengths: brandFit.strengths || [],
            concerns: brandFit.concerns || [],
            recommendations: brandFit.recommendations || [],
            status: "Not Contacted",
            respondTime: respondTime,
            respondTimeColor: respondTimeColor,
            contactName: getMockContactName(index),
            contactTitle: getMockContactTitle(index),
            contactCompany: parentCompany
          };
        });
        
        res.json({
          success: true,
          matchedBrands: matchedBrands,
          total: matchedBrands.length,
          dealId: dealId,
          analysisDate: brandFitData.analysisDate || new Date().toISOString(),
          usingBrandFitAnalyzer: true
        });
        return;
        
      } catch (error) {
        console.error("Error getting brand fit analysis:", error);
        console.log("Falling back to mock data due to error:", error.message);
        // Fall through to return mock data if brand fit analyzer fails
      }
    }
    
    // If no dealId provided, try to get the most recent deal and use it
    if (!dealId) {
      try {
        const recentDeals = await base(F.deals.table)
          .select({
            maxRecords: 1,
            sort: [{ field: F.deals.submitDate || "created_time", direction: "desc" }]
          })
          .all();
        
        if (recentDeals.length > 0) {
          const recentDealId = recentDeals[0].fields[F.deals.id] || recentDeals[0].id;
          console.log(`Using most recent deal for matched brands: ${recentDealId}`);
          
          // Recursively call with the default dealId
          const newQuery = { ...req.query, dealId: recentDealId };
          const newReq = { ...req, query: newQuery };
          return await getMatchedBrands(newReq, res);
        }
      } catch (error) {
        console.error("Error getting recent deal:", error);
        // Continue to fallback mock data
      }
    }

    // Get matched brands from Brand Basics table
    let matchedBrands = [];
    
    try {
      // Get brands from Brand Basics table
      // Use table ID for more reliable access: tbl1x6S7I7JwTcRdV
      let brands = [];
      
      try {
        // Try using table ID first (more reliable)
        const brandTableId = "tbl1x6S7I7JwTcRdV";
        brands = await base(brandTableId)
          .select({
            fields: ["Brand Name", "Parent Company", "Hotel Chain Scale", "Brand Status"],
            maxRecords: 100
          })
          .all();
        
        console.log(`✅ Found ${brands.length} brands from Brand Basics table`);
        
      } catch (idError) {
        console.error(`Error accessing table by ID:`, idError.message);
        
        // Fallback: Try using table name
        try {
          brands = await base("Brand Setup - Brand Basics")
            .select({
              fields: ["Brand Name", "Parent Company", "Hotel Chain Scale", "Brand Status"],
              maxRecords: 100
            })
            .all();
          
          console.log(`✅ Found ${brands.length} brands from Brand Basics table (by name)`);
          
        } catch (nameError) {
          console.error(`Error accessing table by name:`, nameError.message);
          throw new Error(`Could not access brand table. ID error: ${idError.message}, Name error: ${nameError.message}`);
        }
      }
      
      // Filter for active brands
      brands = brands.filter(brand => {
        const status = brand.fields["Brand Status"];
        if (!status) return false;
        const statusStr = String(status).toLowerCase();
        return statusStr.includes("active") && (statusStr.includes("visible") || statusStr.includes("–") || statusStr.includes("-"));
      });
      
      console.log(`✅ Filtered to ${brands.length} active brands`);

      // Transform to matched brands format
      matchedBrands = brands.map((brand, index) => {
        const brandName = brand.fields["Brand Name"] || "Unknown Brand";
        const parentCompany = brand.fields["Parent Company"] || "Unknown";
        const chainScale = brand.fields["Hotel Chain Scale"] || "Unknown";
        
        // Calculate a mock match score based on position (for now)
        // In production, this would come from actual brand fit analysis
        const fitScore = 92 - (index * 3); // Decreasing scores
        
        // Create mock breakdown for demonstration (when not using Brand Fit Analyzer)
        const mockBreakdown = {
          segmentFit: Math.max(70, fitScore - 5),
          geographicFit: Math.max(70, fitScore - 3),
          propertyFit: Math.max(70, fitScore - 7),
          financialFit: Math.max(70, fitScore - 10),
          operationalFit: Math.max(70, fitScore - 8)
        };
        
        return {
          id: brand.id,
          brandName: brandName,
          parentCompany: parentCompany,
          chainScale: chainScale,
          fitScore: Math.max(45, fitScore), // Ensure minimum score
          fitBreakdown: mockBreakdown, // Include mock breakdown
          strengths: [],
          concerns: [],
          recommendations: [],
          status: "Not Contacted", // Default status
          respondTime: index < 2 ? "Lightning Fast - Occasionally" : 
                      index < 4 ? "Very Fast - Frequently" : 
                      index < 5 ? "Responsive - Occasionally" : "Unresponsive - Rarely",
          respondTimeColor: index < 2 ? "green" : index < 4 ? "green" : index < 5 ? "orange" : "red",
          contactName: getMockContactName(index),
          contactTitle: getMockContactTitle(index),
          contactCompany: parentCompany
        };
      });

      // Sort by fit score descending
      matchedBrands.sort((a, b) => b.fitScore - a.fitScore);

    } catch (error) {
      console.error("Error fetching brands from Brand Basics:", error);
      // Return empty array if error
      matchedBrands = [];
    }

    res.json({
      success: true,
      matchedBrands: matchedBrands,
      total: matchedBrands.length
    });

  } catch (error) {
    console.error("Error fetching matched brands:", error);
    res.status(500).json({ error: "Internal Server Error", details: error.message });
  }
}

// Helper function to get mock contact names
function getMockContactName(index) {
  const names = [
    "Katherin Giles",
    "Jane Smith",
    "Olivia Kline",
    "Jared Woods",
    "Winston Sawyer",
    "Maria Rodriguez"
  ];
  return names[index % names.length];
}

// Helper function to get mock contact titles
function getMockContactTitle(index) {
  const titles = [
    "Sr. Dir. Development - CALA",
    "Dir. Development - LATAM",
    "Sr. Dir. Development - CALA",
    "VP Development - Upscale",
    "VP Development - Carribean",
    "VP Expansion - LATAM"
  ];
  return titles[index % titles.length];
}



