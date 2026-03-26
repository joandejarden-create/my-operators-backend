# Top 100 Projects Canada 2025 - Data Extraction Tools

This repository contains tools to extract and process data from the [Top 100 Projects Canada 2025 ranking website](https://top100projects.ca/2025-ranking/).

## 🚀 Quick Start

### Local Development (Deal Capture Proxy / API Server)
For local development, use `npm run dev` to avoid EADDRINUSE when restarting:
```bash
npm install
npm run dev
```
This kills any process on port 3000 before starting, then runs the server at http://localhost:3000. Use `npm start` for production (no port pre-kill).

Copy `.env.local.example` to `.env.local` for My Deals fast-mode config overrides.

### Option 1: Extract from Provided Data (Recommended)
If you already have the website content, use this faster method:

```bash
node extract-from-provided-data.js
```

### Option 2: Live Web Scraping
For real-time data extraction from the website:

```bash
# Install dependencies
npm install

# Run the scraper
npm run scrape
```

## 📊 What Data Gets Extracted

### 1. Comprehensive Project Data
- **Rank**: Project ranking (1-100, or "NR" for not ranked)
- **Project Name**: Full project name
- **Project Value**: Project value in USD
- **Industry**: Primary industry sector (Energy, Transit, Transportation, Health Care, Water, Other)
- **Project Type**: Specific project type (Nuclear, LRT, Highway, Hydroelectric, etc.)
- **Funding Source**: Primary funding mechanism
- **Provincial Funding**: Provincial funding details
- **Province**: Canadian province or territory
- **City/Region**: Specific city or region location
- **Estimated End Date**: Expected completion date
- **Description**: Brief project description

### 2. Key Players by Tier
- **Platinum**: Companies with 20+ projects
- **Gold**: Companies with 15-19 projects  
- **Silver**: Companies with 10-14 projects
- **Bronze**: Companies with 5-9 projects

### 3. Filter Options
- **Provinces**: All Canadian provinces and territories
- **Funding Sources**: P3, Public, Private, Public/Private
- **Industry Sectors**: Buildings, Energy, Health Care, Transit, etc.
- **Project Types**: Airport, Bridge, Highway, Nuclear, etc.
- **LEED Statuses**: Certified, Silver, Gold, Platinum

## 📁 Output Files

The extraction process creates several files:

### JSON Files
- `top100-projects-YYYY-MM-DD.json` - Complete dataset
- `top100-filters-YYYY-MM-DD.json` - Filter options

### CSV Files  
- `top100-projects-YYYY-MM-DD.csv` - Project rankings
- `top100-key-players-YYYY-MM-DD.csv` - Key players by tier

## 🔧 Technical Details

### Dependencies
- **Puppeteer**: For web scraping (headless browser automation)
- **Cheerio**: For HTML parsing (jQuery-like server-side)
- **Axios**: For HTTP requests
- **Node.js**: Runtime environment

### Architecture
- **Modular Design**: Separate classes for different extraction methods
- **Error Handling**: Comprehensive error handling and logging
- **Data Validation**: Ensures data quality and completeness
- **Multiple Formats**: Outputs both JSON and CSV formats

## 📈 Sample Data

### Top 5 Projects by Value
1. **Site C Clean Energy Project** - $16,000,000,000
2. **GO Expansion – On-Corridor Works** - $15,705,000,000
3. **Bruce Power Refurbishment** - $13,000,000,000
4. **Darlington Nuclear Refurbishment** - $12,800,000,000
5. **Eglinton Crosstown LRT** - $12,571,000,000

### Key Statistics
- **Total Projects**: 10+ (from provided data)
- **Total Value**: $110+ billion
- **Key Players**: 77 companies across 4 tiers
- **Filter Options**: 50+ categories across 5 filter types

## 🛠️ Customization

### Adding New Data Fields
Edit the extraction methods in the respective classes to capture additional data points.

### Changing Output Format
Modify the `saveToFiles()` method to output in different formats (XML, Excel, etc.).

### Filtering Data
Add filtering logic to extract only specific project types, provinces, or value ranges.

## ⚠️ Important Notes

### Legal Considerations
- Respect the website's robots.txt and terms of service
- Use reasonable delays between requests
- Don't overload the server with too many concurrent requests

### Data Accuracy
- The extracted data reflects only what is actually available on the website
- **No inference or guessing** - fields are left blank when data is not available
- Some projects may have "NR" (Not Ranked) status
- Project values are in USD as displayed on the website

### Rate Limiting
- The live scraper includes delays to be respectful to the server
- Consider running during off-peak hours for large extractions

## 🔍 Troubleshooting

### Common Issues
1. **Puppeteer Installation**: Run `npm install` to ensure all dependencies are installed
2. **Network Timeouts**: Increase timeout values in the scraper configuration
3. **Data Parsing**: Check if the website structure has changed

### Debug Mode
Set `headless: false` in the Puppeteer configuration to see the browser in action.

## 📞 Support

For issues or questions about the data extraction tools, please check:
1. The console output for error messages
2. The generated log files
3. The website structure for changes

## 📄 License

This project is for educational and research purposes. Please respect the original website's terms of service and copyright.
