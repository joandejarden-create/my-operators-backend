import Airtable from "airtable";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY_READONLY }).base(process.env.AIRTABLE_BASE_ID_ALT);

// Field mappings for travel infrastructure data
const F = {
  infrastructure: {
    table: "Travel Infrastructure data",
    id: "id",
    name: "Name",
    type: "Type",
    lat: "Latitude",
    lng: "Longitude",
    city: "City",
    country: "Country",
    region: "Region"
  }
};

// Get travel infrastructure data
export async function getTravelInfrastructure(req, res) {
  try {
    const { type, country, region } = req.query;
    
    // Build filter formula
    let filterFormula = '';
    const conditions = [];
    
    if (type) {
      conditions.push(`{${F.infrastructure.type}} = '${type}'`);
    }
    
    if (country) {
      conditions.push(`{${F.infrastructure.country}} = '${country}'`);
    }
    
    if (region) {
      conditions.push(`{${F.infrastructure.region}} = '${region}'`);
    }
    
    if (conditions.length > 0) {
      filterFormula = `AND(${conditions.join(', ')})`;
    }
    
    // Fetch infrastructure data
    const selectOptions = {
      fields: [
        F.infrastructure.name,
        F.infrastructure.type,
        F.infrastructure.lat,
        F.infrastructure.lng,
        F.infrastructure.city,
        F.infrastructure.country,
        F.infrastructure.region
      ],
      maxRecords: 1000
    };
    
    // Only add filterByFormula if we have a valid filter
    if (filterFormula && filterFormula.trim() !== '') {
      selectOptions.filterByFormula = filterFormula;
    }
    
    const records = await base(F.infrastructure.table)
      .select(selectOptions)
      .all();
    
    // Format response
    const formattedInfrastructure = records.map(record => ({
      id: record.id,
      name: record.fields[F.infrastructure.name] || 'Unknown',
      type: record.fields[F.infrastructure.type] || 'Unknown',
      lat: parseFloat(record.fields[F.infrastructure.lat]) || 0,
      lng: parseFloat(record.fields[F.infrastructure.lng]) || 0,
      city: record.fields[F.infrastructure.city] || 'Unknown City',
      country: record.fields[F.infrastructure.country] || 'Unknown Country',
      region: record.fields[F.infrastructure.region] || 'Unknown Region'
    }));
    
    // Calculate statistics
    const stats = calculateInfrastructureStatistics(formattedInfrastructure);
    
    res.json({
      success: true,
      infrastructure: formattedInfrastructure,
      statistics: stats,
      totalCount: formattedInfrastructure.length
    });
    
  } catch (error) {
    console.error("Error getting travel infrastructure data:", error);
    res.status(500).json({ 
      error: "Internal Server Error", 
      details: error.message 
    });
  }
}

// Calculate infrastructure statistics
function calculateInfrastructureStatistics(infrastructure) {
  const totalInfrastructure = infrastructure.length;
  
  // Count by type
  const typeCounts = {};
  infrastructure.forEach(item => {
    const type = item.type || 'Unknown';
    typeCounts[type] = (typeCounts[type] || 0) + 1;
  });
  
  // Count by country
  const countryCounts = {};
  infrastructure.forEach(item => {
    const country = item.country || 'Unknown';
    countryCounts[country] = (countryCounts[country] || 0) + 1;
  });
  
  // Count by region
  const regionCounts = {};
  infrastructure.forEach(item => {
    const region = item.region || 'Unknown';
    regionCounts[region] = (regionCounts[region] || 0) + 1;
  });
  
  return {
    totalInfrastructure,
    typeCounts,
    countryCounts,
    regionCounts
  };
}





