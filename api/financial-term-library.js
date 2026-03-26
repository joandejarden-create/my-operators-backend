import Airtable from "airtable";

// Use the same API key and base as the Clause Library
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY_READONLY }).base(process.env.AIRTABLE_BASE_ID_ALT);
const writeBase = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID_ALT);

// Helper function to format Airtable records for the frontend
function formatTermRecord(record) {
    const fields = record.fields;
    
    // Helper to format multi-select arrays as comma-separated strings
    const formatArray = (value) => {
        if (value === null || value === undefined) {
            return '';
        }
        
        if (Array.isArray(value)) {
            const filtered = value.filter(item => {
                if (!item) return false;
                if (typeof item === 'string') return item.trim().length > 0;
                return true;
            });
            return filtered.map(item => typeof item === 'string' ? item.trim() : String(item)).join(',');
        }
        
        if (typeof value === 'string') {
            const trimmed = value.trim();
            
            if (!trimmed || trimmed === '[]' || trimmed === '""' || trimmed === '') {
                return '';
            }
            
            if (trimmed.startsWith('[')) {
                try {
                    const parsed = JSON.parse(trimmed);
                    if (Array.isArray(parsed)) {
                        return parsed.filter(item => item && typeof item === 'string' && item.trim().length > 0).join(',');
                    }
                } catch (e) {
                    const matches = trimmed.match(/"([^"\\]+|\\"|\\\\)*"/g);
                    if (matches && matches.length > 0) {
                        const extracted = matches.map(m => {
                            let item = m.slice(1, -1);
                            item = item.replace(/\\"/g, '"');
                            item = item.replace(/\\\\/g, '\\');
                            return item.trim();
                        }).filter(item => item.length > 0 && item !== '[' && item !== ']' && item !== '\\n');
                        
                        if (extracted.length > 0) {
                            return extracted.join(',');
                        }
                    }
                }
            }
            
            return trimmed.replace(/;/g, ',');
        }
        
        if (typeof value === 'number' || typeof value === 'boolean') {
            return String(value);
        }
        
        return '';
    };
    
    // Process Jurisdiction Tags
    let jurisdictionTagsValue = '';
    if (fields.hasOwnProperty('Jurisdiction Tags')) {
        const rawValue = fields['Jurisdiction Tags'];
        if (rawValue !== null && rawValue !== undefined) {
            if (typeof rawValue === 'string') {
                jurisdictionTagsValue = rawValue.trim();
            } else if (Array.isArray(rawValue)) {
                jurisdictionTagsValue = rawValue.filter(item => item && typeof item === 'string' && item.trim().length > 0).join(',');
            } else {
                jurisdictionTagsValue = formatArray(rawValue);
            }
        }
    }
    
    // Process Owner Goal Tags
    let ownerGoalTagsValue = '';
    if (fields.hasOwnProperty('Owner Goal Tags')) {
        ownerGoalTagsValue = formatArray(fields['Owner Goal Tags']);
    }
    
    // Helper to safely get field value
    const getField = (fieldName, defaultValue = '') => {
        if (!fields.hasOwnProperty(fieldName)) {
            return defaultValue;
        }
        const value = fields[fieldName];
        // Handle null, undefined, or empty values
        if (value === null || value === undefined || value === '') {
            return defaultValue;
        }
        return value;
    };
    
    
    return {
        id: record.id,
        termId: getField('Term ID'),
        termName: getField('Term Name'),
        agreementType: getField('Agreement Type'),
        category: getField('Category'),
        subcategory: getField('Subcategory'),
        lifecyclePhase: getField('Lifecycle Phase'),
        ownerGoalTags: ownerGoalTagsValue,
        riskLevel: getField('Risk Level (Owner)'),
        lean: getField('Lean'),
        prevalence: getField('Prevalence'),
        jurisdictionTags: jurisdictionTagsValue,
        status: getField('Status'),
        version: getField('Version'),
        lastReviewed: getField('Last Reviewed'),
        plainEnglishSummary: getField('Plain-English Summary'),
        whyItMatters: getField('Why It Matters'),
        whatDoesThisMean: getField('What Does This Mean'),
        termDefinition: getField('Term Definition'),
        commonValues: getField('Common Values / Ranges'),
        calculationMethod: getField('Calculation Method'),
        // Negotiation Notes (Structured)
        negotiationNote: getField('Negotiation Notes'),
        negotiationTradeoff: getField('Negotiation Trade-off'),
        negotiationCompromise: getField('Negotiation Compromise'),
        negotiationMarketNote: getField('Negotiation Market Note'),
        // Legacy field (for backward compatibility)
        negotiationNotes: getField('Negotiation Notes'),
        redFlags: getField('Red Flags'),
        // Structured common positions fields
        commonPositionsOwner: getField('Common Positions - Owner'),
        commonPositionsOwnerRationale: getField('Common Positions - Owner Rational'),
        commonPositionsBrand: getField('Common Positions - Brand'),
        commonPositionsBrandRationale: getField('Common Positions - Brand Rational'),
        commonPositionsBalanced: getField('Common Positions - Balanced'),
        commonPositionsBalancedRationale: getField('Common Positions - Balanced Rational'),
        readingTime: (() => {
            const rawValue = getField('Reading Time (minutes)', null);
            if (rawValue !== null && rawValue !== undefined && rawValue !== '') {
                const numValue = Number(rawValue);
                return isNaN(numValue) ? null : numValue;
            }
            return null;
        })(),
        // Asset Management & Best Practices
        assetManagementBestPractices: getField('Asset Management Best Practices'),
        dueDiligenceChecklist: getField('Due Diligence Checklist'),
        ownerDecisionFramework: getField('Owner Decision Framework'),
        industryBenchmarks: getField('Industry Benchmarks'),
        calculationExamples: getField('Calculation Examples'),
        quickReferenceCard: getField('Quick Reference Card'),
        // Related Terms
        relatedTerms: (() => {
            const relatedTermsValue = getField('Related Terms', '');
            if (relatedTermsValue && typeof relatedTermsValue === 'string') {
                return relatedTermsValue.split(',').map(t => t.trim()).filter(t => t);
            }
            if (Array.isArray(relatedTermsValue)) {
                return relatedTermsValue.filter(t => t);
            }
            return [];
        })(),
        seeAlso: getField('See Also')
    };
}

// Get list of financial terms with optional filters
export async function getTerms(req, res) {
    try {
        const {
            search,
            agreementType,
            category,
            phase,
            risk,
            lean,
            jurisdiction,
            ownerGoal,
            pageSize = 20,
            offset
        } = req.query;

        // Build filter formula
        let filterFormula = null;
        const filters = [];

        if (agreementType) {
            filters.push(`{Agreement Type}='${agreementType.replace(/'/g, "\\'")}'`);
        }

        if (category) {
            filters.push(`{Category}='${category.replace(/'/g, "\\'")}'`);
        }

        if (phase) {
            filters.push(`{Lifecycle Phase}='${phase.replace(/'/g, "\\'")}'`);
        }

        if (risk) {
            filters.push(`{Risk Level (Owner)}='${risk.replace(/'/g, "\\'")}'`);
        }

        if (lean) {
            filters.push(`{Lean}='${lean.replace(/'/g, "\\'")}'`);
        }

        if (req.query.prevalence) {
            const prevalence = req.query.prevalence;
            filters.push(`{Prevalence}='${prevalence.replace(/'/g, "\\'")}'`);
        }

        if (jurisdiction) {
            filters.push(`FIND('${jurisdiction.replace(/'/g, "\\'")}', ARRAYJOIN({Jurisdiction Tags})) > 0`);
        }

        if (ownerGoal) {
            const escapedGoal = ownerGoal.replace(/'/g, "\\'");
            // Use case-insensitive SEARCH function (Airtable) for Owner Goal Tags
            // SEARCH is case-insensitive, FIND is case-sensitive
            filters.push(`SEARCH('${escapedGoal}', ARRAYJOIN({Owner Goal Tags})) > 0`);
        }

        // Build final filter formula
        if (filterFormula && filters.length > 0) {
            filterFormula = `AND(${filterFormula}, ${filters.join(', ')})`;
        } else if (filters.length > 0) {
            filterFormula = filters.length === 1 ? filters[0] : `AND(${filters.join(', ')})`;
        }

        // Build query options
        const requestedPageSize = parseInt(pageSize) || 20;
        const airtablePageSize = Math.min(requestedPageSize, 100);
        const queryOptions = {
            pageSize: airtablePageSize,
            sort: [{ field: 'Term Name', direction: 'asc' }]
        };
        
        if (filterFormula) {
            queryOptions.filterByFormula = filterFormula;
        }

        // Fetch all records and paginate in memory to support search filtering
        delete queryOptions.offset;

        const allRecords = [];

        // Helper function to check if record matches search
        function matchesSearch(record, searchTerm) {
            if (!searchTerm) return true;
            const searchLower = searchTerm.toLowerCase();
            const termName = (record.fields['Term Name'] || '').toLowerCase();
            const summary = (record.fields['Plain-English Summary'] || '').toLowerCase();
            const categoryField = (record.fields['Category'] || '').toLowerCase();
            const agreementTypeField = (record.fields['Agreement Type'] || '').toLowerCase();
            
            return termName.includes(searchLower) ||
                   summary.includes(searchLower) ||
                   categoryField.includes(searchLower) ||
                   agreementTypeField.includes(searchLower);
        }

        // Fetch all matching records
        const tableNameOrId = 'Financial_Term_Library'; // Table name (can also use table ID)
        await new Promise((resolve, reject) => {
            base(tableNameOrId).select(queryOptions).eachPage(
                (pageRecords, fetchNextPage) => {
                    try {
                        pageRecords.forEach(record => {
                            if (matchesSearch(record, search)) {
                                allRecords.push(formatTermRecord(record));
                            }
                        });
                        fetchNextPage();
                    } catch (err) {
                        reject(err);
                    }
                },
                (err) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                }
            );
        });

        // Apply pagination to filtered results
        const pageSizeNum = parseInt(pageSize) || 20;
        const startIndex = (offset && !isNaN(parseInt(offset))) ? parseInt(offset) : 0;
        const endIndex = startIndex + pageSizeNum;
        const paginatedRecords = allRecords.slice(startIndex, endIndex);
        const hasMore = endIndex < allRecords.length;

        const response = {
            records: paginatedRecords,
            offset: hasMore ? endIndex.toString() : null,
            totalRecords: allRecords.length
        };

        res.json(response);
    } catch (error) {
        res.status(500).json({ 
            error: 'Failed to fetch financial terms',
            message: error.message || 'Unknown error occurred',
            details: process.env.NODE_ENV === 'development' ? error.stack : undefined
        });
    }
}

// Get single financial term by Term ID
export async function getTermById(req, res) {
    try {
        const { id } = req.query;

        if (!id) {
            return res.status(400).json({ error: 'Term ID is required' });
        }

        // Find term by Term ID field
        const tableNameOrId = 'Financial_Term_Library'; // Table name (can also use table ID)
        const records = await base(tableNameOrId)
            .select({
                filterByFormula: `{Term ID}='${id.replace(/'/g, "\\'")}'`,
                maxRecords: 1
            })
            .firstPage();

        if (records.length === 0) {
            return res.status(404).json({ error: 'Financial term not found' });
        }

        const term = formatTermRecord(records[0]);
        res.json(term);
    } catch (error) {
        res.status(500).json({ 
            error: 'Failed to fetch financial term',
            message: error.message 
        });
    }
}

// Get all term IDs for navigation (prev/next)
export async function getTermIds(req, res) {
    try {
        const tableNameOrId = 'Financial_Term_Library';
        const allRecords = [];
        await base(tableNameOrId)
            .select({ fields: ['Term ID'], sort: [{ field: 'Term ID', direction: 'asc' }] })
            .eachPage((records, fetchNextPage) => {
                allRecords.push(...records);
                fetchNextPage();
            });
        const ids = allRecords
            .map(r => (r.fields && r.fields['Term ID']) ? r.fields['Term ID'] : null)
            .filter(Boolean)
            .sort();
        res.json(ids);
    } catch (error) {
        console.error('Error in getTermIds:', error);
        res.status(500).json({ 
            error: 'Failed to fetch term IDs',
            message: error.message 
        });
    }
}

// Create a new financial term record
export async function createTerm(req, res) {
    try {
        if (req.method !== 'POST') {
            return res.status(405).json({ error: 'Method not allowed. Use POST to create terms.' });
        }

        const {
            termId,
            termName,
            agreementType,
            category,
            subcategory,
            lifecyclePhase,
            ownerGoalTags,
            riskLevel,
            lean,
            jurisdictionTags,
            status = 'Draft',
            version = '1.0',
            lastReviewed,
            plainEnglishSummary,
            whyItMatters,
            termDefinition,
            commonValues,
            calculationMethod,
            negotiationNote,
            negotiationCompromise,
            negotiationMarketNote,
            negotiationNotes,
            redFlags,
            commonPositionsOwner,
            commonPositionsOwnerRationale,
            commonPositionsBrand,
            commonPositionsBrandRationale,
            commonPositionsBalanced,
            commonPositionsBalancedRationale
        } = req.body;

        // Validate required fields
        if (!termId || !termName) {
            return res.status(400).json({ error: 'Term ID and Term Name are required' });
        }

        // Check if term with this ID already exists
        const tableNameOrId = 'Financial_Term_Library'; // Table name (can also use table ID)
        const existing = await base(tableNameOrId)
            .select({
                filterByFormula: `{Term ID}='${termId.replace(/'/g, "\\'")}'`,
                maxRecords: 1
            })
            .firstPage();

        if (existing.length > 0) {
            return res.status(409).json({ 
                error: 'Term ID already exists',
                termId: termId
            });
        }

        // Prepare fields for Airtable
        const fields = {
            'Term ID': termId,
            'Term Name': termName,
            'Agreement Type': agreementType || '',
            'Category': category || '',
            'Subcategory': subcategory || '',
            'Lifecycle Phase': lifecyclePhase || '',
            'Owner Goal Tags': Array.isArray(ownerGoalTags) ? ownerGoalTags : (ownerGoalTags ? [ownerGoalTags] : []),
            'Risk Level (Owner)': riskLevel || '',
            'Lean': lean || '',
            'Jurisdiction Tags': Array.isArray(jurisdictionTags) ? jurisdictionTags : (jurisdictionTags ? [jurisdictionTags] : []),
            'Status': status,
            'Version': version,
            'Plain-English Summary': plainEnglishSummary || '',
            'Why It Matters': whyItMatters || '',
            'Term Definition': termDefinition || '',
            'Common Values / Ranges': commonValues || '',
            'Calculation Method': calculationMethod || '',
            'Negotiation Notes': negotiationNote || negotiationNotes || '',
            'Negotiation Compromise': negotiationCompromise || '',
            'Negotiation Market Note': negotiationMarketNote || '',
            'Red Flags': redFlags || '',
            'Common Positions - Owner': commonPositionsOwner || '',
            'Common Positions - Owner Rational': commonPositionsOwnerRationale || '',
            'Common Positions - Brand': commonPositionsBrand || '',
            'Common Positions - Brand Rational': commonPositionsBrandRationale || '',
            'Common Positions - Balanced': commonPositionsBalanced || '',
            'Common Positions - Balanced Rational': commonPositionsBalancedRationale || ''
        };

        // Add Last Reviewed date if provided
        if (lastReviewed) {
            fields['Last Reviewed'] = lastReviewed;
        }

        // Create the record using write-capable base
        const record = await writeBase(tableNameOrId).create(fields, { typecast: true });

        // Return formatted record
        const formattedRecord = formatTermRecord(record);
        res.status(201).json({
            success: true,
            message: 'Financial term created successfully',
            term: formattedRecord
        });

    } catch (error) {
        res.status(500).json({ 
            error: 'Failed to create financial term',
            message: error.message 
        });
    }
}
