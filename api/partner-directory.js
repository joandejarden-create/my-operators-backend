import Airtable from "airtable";

// Set PARTNER_DIRECTORY_DEBUG=true in .env to enable verbose logs (e.g. for debugging Airtable field mapping).
const DEBUG = process.env.PARTNER_DIRECTORY_DEBUG === 'true';

// Lazy initialization of Airtable base
function getBase() {
    if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
        return null;
    }
    return new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
}

// Helper function to generate default description
function generateDefaultDescription(companyName, userType) {
  const company = companyName || 'Company';
  let typeText = '';
  
  if (userType === 'HOTEL BRANDS (FRANCHISE)') {
    typeText = 'hotel brand franchise';
  } else if (userType === 'HOTEL MGMT. COMPANY') {
    typeText = 'hotel management company';
  } else if (userType === 'HOTEL OWNERS') {
    typeText = 'hotel owner';
  } else {
    typeText = 'hospitality company';
  }
  
  return `${company} is a ${typeText}.`;
}

// Field mappings for Airtable tables
const F = {
  // Users table for individuals
  users: {
    table: "tbl6shiyz2wdUqE5F", // Users table ID
    firstName: "fldG5nbAijQkUVSzr", // First Name
    lastName: "fldV0g50iRB8J46Hh", // Last Name
    email: "fldBl7IXEscwkMhnZ", // Email
    company: "fldCompany", // Company/Organization (field name)
    phone: "fldPhone", // Phone number (field name)
    country: "fld2LWEer7PgkSCe9", // Country
    userType: "User Type", // User Type field (field name)
    profile: "Profile" // Profile image (field name)
  },
  // Company Profile table - main source for companies
  companyProfile: {
    table: "tblItyfH6MlOnMKZ9", // Company Profile table ID
    companyId: "Company ID", // Company ID field (like Term ID in Financial Term Library)
    companyName: "Company Name",
    userType: "User Type", // Should have: HOTEL MGMT. COMPANY, HOTEL BRANDS (FRANCHISE), HOTEL OWNERS
    companyType: "Company Type", // Primary field for company type display
    location: "Location", // or "Headquarters" or "Headquarters Location"
    website: "Website",
    description: "Company Description", // or "Description" or "Company Overview"
    companyOverview: "Company Overview", // Alternative field name
    regions: "Regions", // or "Regions Supported"
    closedDeals: "Closed Deals",
    brandCount: "Brand Count", // or "# of Brand" or "Number of Brands"
    submittedBids: "Submitted Bids",
    logo: "Logo" // or "Company Logo"
  },
  // Brand Setup - Brand Basics for hotel brands (franchise) - as backup
  brands: {
    table: "Brand Setup - Brand Basics", // Table name
    name: "Brand Name",
    parentCompany: "Parent Company",
    chainScale: "Hotel Chain Scale",
    status: "Brand Status",
    tagline: "Brand Tagline",
    positioning: "Brand Positioning",
    valueProposition: "Brand Value Proposition",
    differentiators: "Key Brand Differentiators"
  },
  // Third Party Operators for hotel management companies - as backup
  operators: {
    table: "Third Party Operators", // Table name
    companyName: "Company Name",
    website: "Website",
    headquarters: "Headquarters",
    description: "Company Description",
    regions: "Regions Supported",
    brandsManaged: "Brands Managed",
    numberOfBrands: "Number of Brands Supported"
  }
};

// Get all partners (companies and individuals)
export async function getPartners(req, res) {
  let companies = [];
  let individuals = [];
  
  try {
    const base = getBase();
    if (!base) {
      // Return empty arrays if Airtable is not configured
      return res.json({
        companies: [],
        individuals: []
      });
    }

    // Fetch companies from Company Profile table (primary source)
    let companyProfileRecords = [];
    try {
      // Try table ID first, then table name as fallback
      const tableIdentifier = F.companyProfile.table;
      try {
        await new Promise((resolve, reject) => {
          const table = base(tableIdentifier);
          table
            .select({
              maxRecords: 100 // Airtable pagination limit per page
            })
            .eachPage(
              (pageRecords, fetchNextPage) => {
                try {
                  companyProfileRecords.push(...pageRecords);
                  fetchNextPage();
                } catch (err) {
                  console.error('❌ Error in eachPage callback:', err);
                  console.error('Error details:', err.message, err.stack);
                  reject(err);
                }
              },
              (err) => {
                if (err) {
                  console.error('❌ Error in eachPage completion:', err);
                  console.error('Error type:', err.constructor.name);
                  console.error('Error message:', err.message);
                  if (err.error) {
                    console.error('Airtable error object:', JSON.stringify(err.error, null, 2));
                  }
                  reject(err);
                } else {
                  resolve();
                }
              }
            );
        });
      } catch (selectError) {
        console.error('❌ Error in select/eachPage:', selectError);
        throw selectError; // Re-throw to be caught by outer catch
      }

      // Process company records
      
      const companyProfiles = companyProfileRecords
        .filter(record => {
          const fields = record.fields || {};
          // Try multiple field name variations
          const companyName = fields[F.companyProfile.companyName] 
            || fields["Company Name"] 
            || fields["companyName"]
            || fields["company_name"]
            || fields["Name"]
            || '';
          
          return companyName && companyName.trim();
        })
        .map(record => {
          try {
            const fields = record.fields || {};
            // Try all possible field name variations
            const companyName = fields[F.companyProfile.companyName] 
              || fields["Company Name"] 
              || fields["companyName"]
              || fields["Name"]
              || fields["name"]
              || '';
            // Prioritize "Company Type" field - try multiple variations
            let companyType = '';
            const fieldKeysForType = Object.keys(fields);
            
            // Try exact matches first
            for (const key of fieldKeysForType) {
              const lowerKey = key.toLowerCase().trim();
              if (lowerKey === 'company type' || lowerKey === 'companytype') {
                const value = fields[key];
                companyType = (value != null && value !== undefined) ? String(value).trim() : '';
                if (companyType) break;
              }
            }
            
            // If not found, try partial matches
            if (!companyType) {
              for (const key of fieldKeysForType) {
                const lowerKey = key.toLowerCase().trim();
                if (lowerKey.includes('company') && lowerKey.includes('type')) {
                  const value = fields[key];
                  companyType = (value != null && value !== undefined) ? String(value).trim() : '';
                  if (companyType) break;
                }
              }
            }
            
            // Get User Type separately (don't use companyType as fallback - they're different fields)
            const userType = fields[F.companyProfile.userType] || fields["User Type"] || '';
            
            // Get Company ID field (like Term ID in Financial Term Library)
            const companyId = fields[F.companyProfile.companyId] || fields["Company ID"] || '';
            
            // Get location from "Company HQ Country" column in Airtable
            const location = fields["Company HQ Country"] || '';
            // Get "Company Website" directly from Airtable - simple and direct
            const website = fields["Company Website"] ? String(fields["Company Website"]).trim() : '';
            // Prioritize "Company Overview" field from Airtable - try multiple variations
            // Check all possible field name variations
            let companyOverview = '';
            const fieldKeysForOverview = Object.keys(fields);
            
            // Try exact matches first
            for (const key of fieldKeysForOverview) {
              const lowerKey = key.toLowerCase().trim();
              if (lowerKey === 'company overview' || lowerKey === 'companyoverview') {
                const value = fields[key];
                companyOverview = (value != null && value !== undefined) ? String(value).trim() : '';
                if (companyOverview) break;
              }
            }
            if (!companyOverview) {
              for (const key of fieldKeysForOverview) {
                const lowerKey = key.toLowerCase().trim();
                if (lowerKey.includes('company') && lowerKey.includes('overview')) {
                  const value = fields[key];
                  companyOverview = (value != null && value !== undefined) ? String(value).trim() : '';
                  if (companyOverview) break;
                }
              }
            }
            // Use Company Overview only - don't fall back to other description fields
            const description = companyOverview;
            
            // SIMPLE region detection: Check ALL boolean fields - if checked (true), extract region from field name
            let regions = [];
            try {
              const allFieldNames = Object.keys(fields);
              for (const fieldName of allFieldNames) {
                const fieldValue = fields[fieldName];
                if (typeof fieldValue !== 'boolean' || fieldValue !== true) continue;
                const fieldNameLower = fieldName.trim().toLowerCase();
                if (fieldNameLower.includes('americas') && !fieldNameLower.includes('latin') && !regions.includes("AMERICAS")) regions.push("AMERICAS");
                else if ((fieldNameLower.includes('caribbean') || fieldNameLower.includes('latin')) && !fieldNameLower.includes('americas') && !regions.includes("CALA")) regions.push("CALA");
                else if (fieldNameLower.includes('europe') && !fieldNameLower.includes('middle') && !fieldNameLower.includes('africa') && !regions.includes("EUROPE")) regions.push("EUROPE");
                else if ((fieldNameLower.includes('middle') && fieldNameLower.includes('east')) || (fieldNameLower.includes('middle') && fieldNameLower.includes('africa')) || fieldNameLower.includes('mea') && !regions.includes("MEA")) regions.push("MEA");
                else if ((fieldNameLower.includes('asia') && fieldNameLower.includes('pacific')) || fieldNameLower.includes('asia pacific') || fieldNameLower === 'ap' && !regions.includes("AP")) regions.push("AP");
              }
            } catch (regionError) {
              // regions stay empty on error
            }
            
            // Get fields directly from Airtable - no fallbacks
            const closedDeals = fields[F.companyProfile.closedDeals] || fields["Closed Deals"];
            const brandCount = fields[F.companyProfile.brandCount] || fields["Brand Count"] || fields["# of Brand"] || fields["Number of Brands"];
            const submittedBids = fields[F.companyProfile.submittedBids] || fields["Submitted Bids"];
            const logo = fields[F.companyProfile.logo] || fields["Logo"] || fields["Company Logo"];

            // Determine user type - normalize Company Type to expected format
            let normalizedUserType = "HOTEL OWNERS"; // Default
            try {
              if (companyType && typeof companyType === 'string' && companyType.trim()) {
                // Normalize Company Type to expected format
                const upperType = companyType.toUpperCase().trim();
                if (upperType.includes('BRAND') || upperType.includes('FRANCHISE')) {
                  normalizedUserType = "HOTEL BRANDS (FRANCHISE)";
                } else if (upperType.includes('MGMT') || upperType.includes('MANAGEMENT') || upperType.includes('OPERATOR')) {
                  normalizedUserType = "HOTEL MGMT. COMPANY";
                } else if (upperType.includes('OWNER')) {
                  normalizedUserType = "HOTEL OWNERS";
                } else {
                  // If it doesn't match any pattern, use as-is (uppercase)
                  normalizedUserType = upperType;
                }
              } else if (userType && typeof userType === 'string' && userType.trim()) {
                // Fallback to User Type and normalize
                const upperUserType = userType.toUpperCase().trim();
                // Handle exact matches first (check these before pattern matching)
                if (upperUserType === "HOTEL OWNERS" || upperUserType === "HOTEL OWNER" || upperUserType === "OWNER" || upperUserType === "OWNERS") {
                  normalizedUserType = "HOTEL OWNERS";
                } else if (upperUserType === "HOTEL BRANDS (FRANCHISE)" || upperUserType === "HOTEL BRAND" || upperUserType === "HOTEL BRANDS" || upperUserType === "BRAND" || upperUserType === "BRANDS" || upperUserType === "FRANCHISE") {
                  normalizedUserType = "HOTEL BRANDS (FRANCHISE)";
                } else if (upperUserType === "HOTEL MGMT. COMPANY" || upperUserType === "HOTEL MGMT COMPANY" || upperUserType === "HOTEL MANAGEMENT COMPANY" || upperUserType === "MGMT" || upperUserType === "MANAGEMENT" || upperUserType === "OPERATOR") {
                  normalizedUserType = "HOTEL MGMT. COMPANY";
                } else if (upperUserType.includes('BRAND') || upperUserType.includes('FRANCHISE')) {
                  normalizedUserType = "HOTEL BRANDS (FRANCHISE)";
                } else if (upperUserType.includes('MGMT') || upperUserType.includes('MANAGEMENT') || upperUserType.includes('OPERATOR')) {
                  normalizedUserType = "HOTEL MGMT. COMPANY";
                } else if (upperUserType.includes('OWNER')) {
                  normalizedUserType = "HOTEL OWNERS";
                } else {
                  normalizedUserType = upperUserType;
                }
              } else {
                // use default normalizedUserType
              }
            } catch (userTypeError) {
              // use default normalizedUserType
            }

            // Get logo - use first letter of company name as default
            // Only use logo URL if it's a valid image URL, otherwise use initials
            let logoDisplay = companyName && companyName.length > 0 ? companyName.charAt(0).toUpperCase() : '?';
            try {
              if (logo && Array.isArray(logo) && logo.length > 0 && logo[0]) {
                // Check if it's an object with a url property (Airtable attachment)
                if (logo[0].url && typeof logo[0].url === 'string' && logo[0].url.startsWith('http')) {
                  // Store as object to indicate it's an image URL
                  logoDisplay = { type: 'image', url: logo[0].url };
                }
              } else if (logo && typeof logo === 'string' && logo.startsWith('http')) {
                // Direct URL string
                logoDisplay = { type: 'image', url: logo };
              }
            } catch (logoError) {
              // keep logoDisplay as initial
            }

            return {
              id: record.id || '',
              companyId: companyId || '', // Company ID field (like Term ID in Financial Term Library)
              name: companyName || '',
              userType: normalizedUserType,
              companyType: companyType || '', // Include original Company Type from Airtable for reference
              location: location || '', // No fallback - use exactly what's in Airtable
              website: website || '', // No fallback - use exactly what's in Airtable
              description: description || '', // Only use Company Overview - no fallback
              companyOverview: description || '', // Primary field from Airtable Company Overview column
              regions: regions, // No fallback - use exactly what's in Airtable (empty array if no checkboxes checked)
              closedDeals: closedDeals ? Number(closedDeals) : 0,
              brandCount: brandCount ? Number(brandCount) : 0,
              submittedBids: submittedBids ? Number(submittedBids) : 0,
              logo: logoDisplay
            };
          } catch (recordError) {
            console.error('Error processing record:', recordError);
            console.error('Record ID:', record.id);
            return null; // Return null for failed records, we'll filter them out
          }
        })
        .filter(record => record !== null); // Remove any failed records

      companies.push(...companyProfiles);

    } catch (companyProfileError) {
      console.error("❌ ERROR fetching Company Profile with table ID:", companyProfileError.message);
      
      // Try with table name as fallback
      try {
        companyProfileRecords = [];
        await new Promise((resolve, reject) => {
          base("Company Profile")
            .select({
              maxRecords: 100
            })
            .eachPage(
              (pageRecords, fetchNextPage) => {
                try {
                  companyProfileRecords.push(...pageRecords);
                  fetchNextPage();
                } catch (err) {
                  reject(err);
                }
              },
              (err) => {
                if (err) {
                  console.error("❌ Error with table name too:", err);
                  reject(err);
                } else {
                  resolve();
                }
              }
            );
        });
        
        // Process the records we just fetched
        if (companyProfileRecords.length > 0) {
          const companyProfiles = companyProfileRecords
            .filter(record => {
              const fields = record.fields;
              const companyName = fields["Company Name"] || fields["companyName"] || '';
              return companyName && companyName.trim();
            })
            .map(record => {
              const fields = record.fields;
              const companyName = fields["Company Name"] || fields["companyName"] || '';
              // Prioritize "Company Type" field - try multiple variations
              let companyType = '';
              const fieldKeysForTypeFallback = Object.keys(fields);
              
              // Try exact matches first
              for (const key of fieldKeysForTypeFallback) {
                const lowerKey = key.toLowerCase().trim();
                if (lowerKey === 'company type' || lowerKey === 'companytype') {
                  const value = fields[key];
                  companyType = (value != null && value !== undefined) ? String(value).trim() : '';
                  if (companyType) break;
                }
              }
              
              // If not found, try partial matches
              if (!companyType) {
                for (const key of fieldKeysForTypeFallback) {
                  const lowerKey = key.toLowerCase().trim();
                  if (lowerKey.includes('company') && lowerKey.includes('type')) {
                    const value = fields[key];
                    companyType = (value != null && value !== undefined) ? String(value).trim() : '';
                    if (companyType) break;
                  }
                }
              }
              
              // Fallback to User Type
              const userType = companyType || fields["User Type"] || fields["userType"] || '';
              // Get location from "Company HQ Country" column in Airtable
              const location = fields["Company HQ Country"] || '';
              // Get "Company Website" directly from Airtable - simple and direct
              const website = fields["Company Website"] ? String(fields["Company Website"]).trim() : '';
              // Prioritize "Company Overview" field from Airtable - try multiple variations
              let companyOverview = '';
              const fieldKeysForOverviewFallback = Object.keys(fields);
              
              // Try exact matches first
              for (const key of fieldKeysForOverviewFallback) {
                const lowerKey = key.toLowerCase().trim();
                if (lowerKey === 'company overview' || lowerKey === 'companyoverview') {
                  const value = fields[key];
                  companyOverview = (value != null && value !== undefined) ? String(value).trim() : '';
                  if (companyOverview) break;
                }
              }
              
              // If not found, try partial matches
              if (!companyOverview) {
                for (const key of fieldKeysForOverviewFallback) {
                  const lowerKey = key.toLowerCase().trim();
                  if (lowerKey.includes('company') && lowerKey.includes('overview')) {
                    const value = fields[key];
                    companyOverview = (value != null && value !== undefined) ? String(value).trim() : '';
                    if (companyOverview) break;
                  }
                }
              }
              
              const description = companyOverview; // Only use Company Overview - no fallback
              // Simple region detection: Check ALL boolean fields - if checked (true), check if field name contains region keywords
              let regions = [];
              try {
                const allFieldNames = Object.keys(fields);
                
                // Simple approach: Check every boolean field - if it's true, check if the field name contains region keywords
                for (const fieldName of allFieldNames) {
                  const fieldValue = fields[fieldName];
                  
                  // Only process boolean (checkbox) fields that are checked (true)
                  if (typeof fieldValue !== 'boolean' || fieldValue !== true) continue;
                  
                  const fieldNameLower = fieldName.trim().toLowerCase();
                  
                  // Simple keyword matching - if field name contains region keywords and checkbox is checked, add it
                  if (fieldNameLower.includes('americas') && !fieldNameLower.includes('latin') && !regions.includes("AMERICAS")) regions.push("AMERICAS");
                  else if ((fieldNameLower.includes('caribbean') || fieldNameLower.includes('latin')) && !fieldNameLower.includes('americas') && !regions.includes("CALA")) regions.push("CALA");
                  else if (fieldNameLower.includes('europe') && !fieldNameLower.includes('middle') && !fieldNameLower.includes('africa') && !regions.includes("EUROPE")) regions.push("EUROPE");
                  else if ((fieldNameLower.includes('middle') && fieldNameLower.includes('east')) || (fieldNameLower.includes('middle') && fieldNameLower.includes('africa')) || (fieldNameLower.includes('mea')) && !regions.includes("MEA")) regions.push("MEA");
                  else if ((fieldNameLower.includes('asia') && fieldNameLower.includes('pacific')) || fieldNameLower.includes('asia pacific') || fieldNameLower === 'ap' && !regions.includes("AP")) regions.push("AP");
                }
              } catch (regionError) {
                // regions stay empty
              }
              // Get fields directly from Airtable - no fallbacks
              const closedDeals = fields["Closed Deals"];
              const brandCount = fields["Brand Count"] || fields["# of Brand"];
              const submittedBids = fields["Submitted Bids"];

              // Normalize Company Type to expected format
              let normalizedUserType = "HOTEL OWNERS"; // Default
              if (companyType && typeof companyType === 'string' && companyType.trim()) {
                // Normalize Company Type to expected format
                const upperType = companyType.toUpperCase().trim();
                if (upperType.includes('BRAND') || upperType.includes('FRANCHISE')) {
                  normalizedUserType = "HOTEL BRANDS (FRANCHISE)";
                } else if (upperType.includes('MGMT') || upperType.includes('MANAGEMENT') || upperType.includes('OPERATOR')) {
                  normalizedUserType = "HOTEL MGMT. COMPANY";
                } else if (upperType.includes('OWNER')) {
                  normalizedUserType = "HOTEL OWNERS";
                } else {
                  // If it doesn't match any pattern, use as-is (uppercase)
                  normalizedUserType = upperType;
                }
              } else if (userType && typeof userType === 'string' && userType.trim()) {
                // Fallback to User Type and normalize
                const upperUserType = userType.toUpperCase().trim();
                // Handle exact matches first (check these before pattern matching)
                if (upperUserType === "HOTEL OWNERS" || upperUserType === "HOTEL OWNER" || upperUserType === "OWNER" || upperUserType === "OWNERS") {
                  normalizedUserType = "HOTEL OWNERS";
                } else if (upperUserType === "HOTEL BRANDS (FRANCHISE)" || upperUserType === "HOTEL BRAND" || upperUserType === "HOTEL BRANDS" || upperUserType === "BRAND" || upperUserType === "BRANDS" || upperUserType === "FRANCHISE") {
                  normalizedUserType = "HOTEL BRANDS (FRANCHISE)";
                } else if (upperUserType === "HOTEL MGMT. COMPANY" || upperUserType === "HOTEL MGMT COMPANY" || upperUserType === "HOTEL MANAGEMENT COMPANY" || upperUserType === "MGMT" || upperUserType === "MANAGEMENT" || upperUserType === "OPERATOR") {
                  normalizedUserType = "HOTEL MGMT. COMPANY";
                } else if (upperUserType.includes('BRAND') || upperUserType.includes('FRANCHISE')) {
                  normalizedUserType = "HOTEL BRANDS (FRANCHISE)";
                } else if (upperUserType.includes('MGMT') || upperUserType.includes('MANAGEMENT')) {
                  normalizedUserType = "HOTEL MGMT. COMPANY";
                } else if (upperUserType.includes('OWNER')) {
                  normalizedUserType = "HOTEL OWNERS";
                } else {
                  normalizedUserType = upperUserType;
                }
              }

              // Get logo - use first letter of company name as default only if no logo in Airtable
              let logoDisplay = '';
              const logo = fields["Logo"] || fields["Company Logo"];
              if (logo && Array.isArray(logo) && logo.length > 0 && logo[0] && logo[0].url) {
                logoDisplay = { type: 'image', url: logo[0].url };
              } else if (logo && typeof logo === 'string' && logo.startsWith('http')) {
                logoDisplay = { type: 'image', url: logo };
              } else if (companyName && companyName.length > 0) {
                logoDisplay = companyName.charAt(0).toUpperCase();
              }

              // Get Company ID field (like Term ID in Financial Term Library)
              const companyId = fields[F.companyProfile.companyId] || fields["Company ID"] || '';
              
              return {
                id: record.id || '',
                companyId: companyId || '', // Company ID field (like Term ID in Financial Term Library)
                name: companyName || '',
                userType: normalizedUserType,
                companyType: companyType || '', // Include original Company Type from Airtable for reference
                location: location || '', // No fallback - use exactly what's in Airtable
                website: website || '', // No fallback - use exactly what's in Airtable
                description: description || '', // Only use Company Overview - no fallback
                companyOverview: description || '', // Primary field from Airtable Company Overview column
                regions: regions, // No fallback - use exactly what's in Airtable (empty array if no checkboxes checked)
                closedDeals: closedDeals ? Number(closedDeals) : 0,
                brandCount: brandCount ? Number(brandCount) : 0,
                submittedBids: submittedBids ? Number(submittedBids) : 0,
                logo: logoDisplay
              };
            });
          
          companies.push(...companyProfiles);
        }
      } catch (fallbackError) {
        // Company Profile table not accessible; companies stay empty
      }
    }

    // SKIP Users table for companies - all company data should come from Company Profile table only
    // This ensures we only use data from the Company Profile table as requested

    // Fetch individuals from Users table
    let userRecords = [];
    try {
      await new Promise((resolve, reject) => {
        base(F.users.table)
          .select({
            maxRecords: 100 // Airtable pagination limit per page
          })
          .eachPage(
            (pageRecords, fetchNextPage) => {
              try {
                userRecords.push(...pageRecords);
                fetchNextPage();
              } catch (err) {
                reject(err);
              }
            },
            (err) => {
              if (err) {
                console.error('Error fetching users:', err);
                reject(err);
              } else {
                resolve();
              }
            }
          );
      });

      // Filter out users without at least a first name or last name
      // Also try alternative field names in case field IDs don't match
      individuals = userRecords
        .filter(record => {
          const fields = record.fields;
          const firstName = fields[F.users.firstName] || fields["First Name"] || '';
          const lastName = fields[F.users.lastName] || fields["Last Name"] || '';
          return firstName || lastName; // Include if we have at least one name
        })
        .map(record => formatUserRecord(record));
    } catch (userError) {
      individuals = [];
    }

    // Always return data, even if empty
    return res.json({
      companies: companies || [],
      individuals: individuals || []
    });
    } catch (error) {
    console.error("❌ CRITICAL ERROR in getPartners:", error);
    console.error("Error message:", error.message);
    console.error("Error name:", error.name);
    console.error("Error stack:", error.stack);
    if (error.error) {
      console.error("Airtable error:", JSON.stringify(error.error, null, 2));
    }
    if (error.statusCode) {
      console.error("Airtable status code:", error.statusCode);
    }
    
    // Try to return at least empty data so the page doesn't completely break
    try {
      return res.status(500).json({ 
        error: "Failed to fetch partners", 
        details: error.message,
        companies: [],
        individuals: [],
        stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
      });
    } catch (responseError) {
      console.error("❌ Even error response failed:", responseError);
      // Last resort - just send a basic error
      res.status(500).send("Internal Server Error");
    }
  }
}


// Format user record from Airtable
function formatUserRecord(record) {
  const fields = record.fields;
  // Try both field IDs and field names for compatibility
  const firstName = fields[F.users.firstName] || fields["First Name"] || "";
  const lastName = fields[F.users.lastName] || fields["Last Name"] || "";
  const company = fields[F.users.company] || fields["Company/Organization"] || "";
  const country = fields[F.users.country] || fields["Country"] || "";
  const userType = fields[F.users.userType] || fields["User Type"] || "";
  
  // Determine location from country
  const location = country ? `${country}` : "";
  
  // Determine regions from user type and country (simplified mapping)
  let regions = [];
  if (country) {
    if (country.includes("United States") || country.includes("Canada")) {
      regions = ["AMERICAS"];
    } else if (country.includes("Mexico") || country.includes("Brazil") || country.includes("Argentina")) {
      regions = ["CALA"];
    } else if (country.includes("United Kingdom") || country.includes("France") || country.includes("Germany") || country.includes("Spain") || country.includes("Italy")) {
      regions = ["EUROPE"];
    } else {
      regions = ["GLOBAL"];
    }
  }

  return {
    id: record.id,
    firstName: firstName,
    lastName: lastName,
    companyTitle: "", // Not in Users table - would need to add
    companyName: company,
    phoneNumber: fields[F.users.phone] || fields["Phone Number"] || fields["Phone"] || "",
    companyEmail: fields[F.users.email] || "",
    platformRole: userType || "",
    regions: regions,
    contactVisibility: "Show Contact", // Default
    location: location,
    closedDeals: 0, // Would need to calculate from linked deals
    brandCount: 0, // Would need to calculate
    submittedBids: 0 // Would need to calculate
  };
}

// Create a new user
export async function createUser(req, res) {
  try {
    if (req.method !== "POST") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const {
      firstName,
      lastName,
      companyTitle,
      phoneNumber,
      companyEmail,
      platformRole,
      regions,
      contactVisibility
    } = req.body;

    // Validate required fields
    if (!firstName || !lastName || !companyTitle || !phoneNumber || !companyEmail || !platformRole || !contactVisibility) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    // Create user record in Airtable
    const base = getBase();
    if (!base) {
      return res.status(500).json({ error: "Airtable not configured" });
    }
    
    const userRecord = await base(F.users.table).create({
      [F.users.firstName]: firstName,
      [F.users.lastName]: lastName,
      [F.users.email]: companyEmail,
      [F.users.phone]: phoneNumber,
      [F.users.company]: companyTitle, // Using company field for company title temporarily
      [F.users.userType]: platformRole
      // Note: regions, contactVisibility would need to be added as fields to Users table
    }, { typecast: true });

    return res.json({
      id: userRecord.id,
      message: "User created successfully"
    });
  } catch (error) {
    console.error("Error creating user:", error);
    return res.status(500).json({ error: "Failed to create user", details: error.message });
  }
}

// Update an existing user
export async function updateUser(req, res) {
  try {
    if (req.method !== "PUT") {
      return res.status(405).json({ error: "Method not allowed" });
    }

    const userId = req.params.userId;
    if (!userId) {
      return res.status(400).json({ error: "User ID is required" });
    }

    const {
      firstName,
      lastName,
      companyTitle,
      phoneNumber,
      companyEmail,
      platformRole,
      regions,
      contactVisibility
    } = req.body;

    // Validate required fields
    if (!firstName || !lastName || !companyTitle || !phoneNumber || !companyEmail || !platformRole || !contactVisibility) {
      return res.status(400).json({ error: "Missing required fields" });
    }

    // Update user record in Airtable
    const base = getBase();
    if (!base) {
      return res.status(500).json({ error: "Airtable not configured" });
    }
    
    const userRecord = await base(F.users.table).update(userId, {
      [F.users.firstName]: firstName,
      [F.users.lastName]: lastName,
      [F.users.email]: companyEmail,
      [F.users.phone]: phoneNumber,
      [F.users.company]: companyTitle, // Using company field for company title temporarily
      [F.users.userType]: platformRole
      // Note: regions, contactVisibility would need to be added as fields to Users table
    }, { typecast: true });

    return res.json({
      id: userRecord.id,
      message: "User updated successfully"
    });
  } catch (error) {
    console.error("Error updating user:", error);
    return res.status(500).json({ error: "Failed to update user", details: error.message });
  }
}

// Get single company by Company ID (like getTermById in Financial Term Library)
export async function getCompanyById(req, res) {
  try {
    const { id } = req.query;

    if (!id) {
      return res.status(400).json({ error: 'Company ID is required' });
    }

    const base = getBase();
    if (!base) {
      return res.status(500).json({ error: 'Airtable base not configured' });
    }

    // Find company by Company ID field
    const tableNameOrId = F.companyProfile.table; // Use table ID
    const records = await base(tableNameOrId)
      .select({
        filterByFormula: `{Company ID}='${id.replace(/'/g, "\\'")}'`,
        maxRecords: 1
      })
      .firstPage();

    if (records.length === 0) {
      return res.status(404).json({ error: 'Company not found' });
    }

    // Process the record using the same logic as getPartners
    const record = records[0];
    const fields = record.fields || {};
    const companyName = fields[F.companyProfile.companyName] || fields["Company Name"] || '';
    const companyId = fields[F.companyProfile.companyId] || fields["Company ID"] || '';
    const companyType = fields[F.companyProfile.companyType] || fields["Company Type"] || '';
    const userType = fields[F.companyProfile.userType] || fields["User Type"] || '';
    const location = fields["Company HQ Country"] || '';
    const website = fields["Company Website"] ? String(fields["Company Website"]).trim() : '';
    const companyOverview = fields["Company Overview"] || '';
    
    // Get regions from checkbox fields
    let regions = [];
    const allFieldNames = Object.keys(fields);
    for (const fieldName of allFieldNames) {
      const fieldValue = fields[fieldName];
      if (typeof fieldValue !== 'boolean' || fieldValue !== true) continue;
      
      const fieldNameLower = fieldName.trim().toLowerCase();
      if (fieldNameLower.includes('americas') && !fieldNameLower.includes('latin') && !regions.includes("AMERICAS")) {
        regions.push("AMERICAS");
      } else if ((fieldNameLower.includes('caribbean') || fieldNameLower.includes('latin')) && 
                !fieldNameLower.includes('americas') && !regions.includes("CALA")) {
        regions.push("CALA");
      } else if (fieldNameLower.includes('europe') && 
                !fieldNameLower.includes('middle') && !fieldNameLower.includes('africa') && !regions.includes("EUROPE")) {
        regions.push("EUROPE");
      } else if ((fieldNameLower.includes('middle') && fieldNameLower.includes('east')) || 
                (fieldNameLower.includes('middle') && fieldNameLower.includes('africa')) ||
                fieldNameLower.includes('mea') && !regions.includes("MEA")) {
        regions.push("MEA");
      } else if ((fieldNameLower.includes('asia') && fieldNameLower.includes('pacific')) ||
                fieldNameLower.includes('asia pacific') ||
                fieldNameLower === 'ap' && !regions.includes("AP")) {
        regions.push("AP");
      }
    }
    
    const closedDeals = fields[F.companyProfile.closedDeals] || fields["Closed Deals"] || 0;
    const brandCount = fields[F.companyProfile.brandCount] || fields["Brand Count"] || 0;
    const submittedBids = fields[F.companyProfile.submittedBids] || fields["Submitted Bids"] || 0;
    
    // Get logo
    let logoDisplay = companyName && companyName.length > 0 ? companyName.charAt(0).toUpperCase() : '?';
    const logo = fields[F.companyProfile.logo] || fields["Logo"] || fields["Company Logo"];
    if (logo && Array.isArray(logo) && logo.length > 0 && logo[0] && logo[0].url) {
      logoDisplay = { type: 'image', url: logo[0].url };
    } else if (logo && typeof logo === 'string' && logo.startsWith('http')) {
      logoDisplay = { type: 'image', url: logo };
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
      }
    }

    const company = {
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
      logo: logoDisplay
    };

    res.json(company);
  } catch (error) {
    console.error('❌ Error fetching company by ID:', error);
    res.status(500).json({ 
      error: 'Failed to fetch company',
      message: error.message 
    });
  }
}

// Default export for route handler
export default async function partnerDirectoryHandler(req, res) {
  if (req.method === "GET") {
    // Check if it's a request for a single company by ID
    if (req.query.id && !req.query.search && !req.query.userType && !req.query.region) {
      return getCompanyById(req, res);
    }
    return getPartners(req, res);
  } else {
    return res.status(405).json({ error: "Method not allowed" });
  }
}
