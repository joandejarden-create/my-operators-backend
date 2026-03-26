import Airtable from "airtable";

// Use the same API key and base as the Radar page (brand-presence.js) for reading
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY_READONLY }).base(process.env.AIRTABLE_BASE_ID_ALT);

// Use write-capable API key for write operations (same base - AIRTABLE_BASE_ID_ALT)
// Note: The API key must have write permissions to this base
const writeBase = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID_ALT);

// Helper function to format Airtable records for the frontend
function formatClauseRecord(record) {
    const fields = record.fields;
    
    // Helper to format multi-select arrays as comma-separated strings
    const formatArray = (value) => {
        // Handle null, undefined, or empty
        if (value === null || value === undefined) {
            return '';
        }
        
        // If it's already an array, join it
        if (Array.isArray(value)) {
            const filtered = value.filter(item => {
                if (!item) return false;
                if (typeof item === 'string') return item.trim().length > 0;
                return true;
            });
            return filtered.map(item => typeof item === 'string' ? item.trim() : String(item)).join(',');
        }
        
        // If it's a string, try to parse it
        if (typeof value === 'string') {
            const trimmed = value.trim();
            
            // If it's empty or just brackets, return empty
            if (!trimmed || trimmed === '[]' || trimmed === '""' || trimmed === '') {
                return '';
            }
            
            // Try to parse as JSON first (handles "[\n    \"item1\",\n    \"item2\"\n]")
            if (trimmed.startsWith('[')) {
                try {
                    const parsed = JSON.parse(trimmed);
                    if (Array.isArray(parsed)) {
                        return parsed.filter(item => item && typeof item === 'string' && item.trim().length > 0).join(',');
                    }
                } catch (e) {
                    // If JSON parsing fails, try to extract quoted strings using regex
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
            
            // If it's a simple string (not JSON), normalize separators
            return trimmed.replace(/;/g, ',');
        }
        
        // Handle other types (number, boolean, etc.) - convert to string
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
    
    return {
        id: record.id,
        clauseId: fields['Clause ID'] || '',
        clauseName: fields['Clause Name'] || '',
        agreementType: fields['Agreement Type'] || '',
        category: fields['Category'] || '',
        subcategory: fields['Subcategory'] || '',
        lifecyclePhase: fields['Lifecycle Phase'] || '',
        ownerGoalTags: ownerGoalTagsValue,
        riskLevel: fields['Risk Level (Owner)'] || '',
        lean: fields['Lean'] || '',
        prevalence: fields['Prevalence'] || '',
        jurisdictionTags: jurisdictionTagsValue,
        status: fields['Status'] || '',
        version: fields['Version'] || '',
        lastReviewed: fields['Last Reviewed'] || '',
        plainEnglishSummary: fields['Plain-English Summary'] || '',
        whyItMatters: fields['Why It Matters'] || '',
        clauseText: fields['Clause Text'] || '',
        // New structured negotiation notes fields
        negotiationNote: fields['Negotiation Notes'] || '', // Main note (using existing field)
        negotiationTradeoff: fields['Negotiation Trade-off'] || '',
        negotiationCompromise: fields['Negotiation Compromise'] || '',
        negotiationMarketNote: fields['Negotiation Market Note'] || '',
        // Legacy field (for backward compatibility)
        negotiationNotes: fields['Negotiation Notes'] || '',
        redFlags: fields['Red Flags'] || '',
        // Structured common positions fields
        commonPositionsOwner: fields['Common Positions - Owner'] || '',
        commonPositionsOwnerRationale: fields['Common Positions - Owner Rational'] || '',
        commonPositionsBrand: fields['Common Positions - Brand'] || '',
        commonPositionsBrandRationale: fields['Common Positions - Brand Rational'] || '',
        commonPositionsBalanced: fields['Common Positions - Balanced'] || '',
        commonPositionsBalancedRationale: fields['Common Positions - Balanced Rational'] || '',
        variablesTokens: fields['Variables (tokens)'] || '',
        readingTime: (() => {
            const rawValue = fields['Reading Time (minutes)'];
            console.log(`🔍 [formatClauseRecord] Reading Time field:`, rawValue, typeof rawValue);
            if (rawValue !== undefined && rawValue !== null && rawValue !== '') {
                const numValue = Number(rawValue);
                return isNaN(numValue) ? null : numValue;
            }
            return null;
        })() // Reading time in minutes (from Airtable)
    };
}

// Get list of clauses with optional filters
export async function getClauses(req, res) {
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

        // Add additional filters
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
            // Tags in Airtable now match frontend filter format (sentence case)
            // Frontend sends: "Fee transparency", "Exit flexibility", etc.
            // Airtable stores: "Fee transparency", "Exit flexibility", etc. (matching format)
            const escapedGoal = ownerGoal.replace(/'/g, "\\'");
            filters.push(`FIND('${escapedGoal}', ARRAYJOIN({Owner Goal Tags})) > 0`);
        }

        // Build final filter formula
        if (filterFormula && filters.length > 0) {
            filterFormula = `AND(${filterFormula}, ${filters.join(', ')})`;
        } else if (filters.length > 0) {
            // If we only have filters (no status), just join them
            filterFormula = filters.length === 1 ? filters[0] : `AND(${filters.join(', ')})`;
        }

        // Build query options
        // Airtable has a maximum page size of 100, so cap it at 100
        // We'll fetch all records using eachPage and paginate in memory
        const requestedPageSize = parseInt(pageSize) || 20;
        const airtablePageSize = Math.min(requestedPageSize, 100);
        const queryOptions = {
            pageSize: airtablePageSize,
            sort: [{ field: 'Clause Name', direction: 'asc' }]
        };
        
        // Only add filterByFormula if we have a filter
        if (filterFormula) {
            queryOptions.filterByFormula = filterFormula;
        }

        // We fetch all records and paginate in memory to support search filtering
        delete queryOptions.offset;

        // Fetch records from Airtable
        const allRecords = [];

        // Helper function to check if record matches search
        function matchesSearch(record, searchTerm) {
            if (!searchTerm) return true;
            const searchLower = searchTerm.toLowerCase();
            const clauseName = (record.fields['Clause Name'] || '').toLowerCase();
            const summary = (record.fields['Plain-English Summary'] || '').toLowerCase();
            const categoryField = (record.fields['Category'] || '').toLowerCase();
            const agreementTypeField = (record.fields['Agreement Type'] || '').toLowerCase();
            
            return clauseName.includes(searchLower) ||
                   summary.includes(searchLower) ||
                   categoryField.includes(searchLower) ||
                   agreementTypeField.includes(searchLower);
        }

        // Fetch all matching records using Promise-based approach
        const tableNameOrId = 'tbl4wXAIpWLhiRP6W'; // Clause_Library table ID
        await new Promise((resolve, reject) => {
            base(tableNameOrId).select(queryOptions).eachPage(
                (pageRecords, fetchNextPage) => {
                    try {
                        pageRecords.forEach(record => {
                            if (matchesSearch(record, search)) {
                                allRecords.push(formatClauseRecord(record));
                            }
                        });
                        fetchNextPage();
                    } catch (err) {
                        reject(err);
                    }
                },
                (err) => {
                    if (err) {
                        console.error('Airtable API error:', err);
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
        console.error('Error in getClauses:', error);
        res.status(500).json({ 
            error: 'Failed to fetch clauses',
            message: error.message || 'Unknown error occurred'
        });
    }
}

// Get single clause by Clause ID
export async function getClauseById(req, res) {
    try {
        const { id } = req.query;

        if (!id) {
            return res.status(400).json({ error: 'Clause ID is required' });
        }

        // Find clause by Clause ID field
        const tableNameOrId = 'tbl4wXAIpWLhiRP6W';
        const records = await base(tableNameOrId)
            .select({
                filterByFormula: `{Clause ID}='${id.replace(/'/g, "\\'")}'`,
                maxRecords: 1
            })
            .firstPage();

        if (records.length === 0) {
            return res.status(404).json({ error: 'Clause not found' });
        }

        // Debug: Check what fields are available BEFORE formatClauseRecord
        const rawFields = records[0].fields;
        const fieldNames = Object.keys(rawFields);
        const readingTimeFields = fieldNames.filter(name => 
            name.toLowerCase().includes('reading') || name.toLowerCase().includes('time')
        );
        console.log(`\n📋 [API getClauseById] ${id} - Fields containing "reading" or "time":`, readingTimeFields);
        console.log(`📋 [API getClauseById] ${id} - Direct field access:`, {
            'Reading Time (minutes)': rawFields['Reading Time (minutes)'],
            type: typeof rawFields['Reading Time (minutes)'],
            exists: 'Reading Time (minutes)' in rawFields
        });

        const clause = formatClauseRecord(records[0]);
        
        // Debug: Check what formatClauseRecord returned
        console.log(`📋 [API getClauseById] ${id} - After formatClauseRecord, readingTime:`, clause.readingTime);
        // Debug: Log reading time value
        const rawField = records[0].fields['Reading Time (minutes)'];
        console.log(`📖 [API] Reading Time for ${id}:`, {
            rawField: rawField,
            rawFieldType: typeof rawField,
            formatted: clause.readingTime,
            formattedType: typeof clause.readingTime
        });
        res.json(clause);
    } catch (error) {
        console.error('Error in getClauseById:', error);
        res.status(500).json({ 
            error: 'Failed to fetch clause',
            message: error.message 
        });
    }
}

// Get clause variables (from Clause_Variables table)
export async function getClauseVariables(req, res) {
    try {
        const { clauseId } = req.query;

        if (!clauseId) {
            return res.status(400).json({ error: 'Clause ID is required' });
        }

        // First, get the clause to extract variables tokens
        const clauseTableId = 'tbl4wXAIpWLhiRP6W';
        const clauseRecords = await base(clauseTableId)
            .select({
                filterByFormula: `{Clause ID}='${clauseId.replace(/'/g, "\\'")}'`,
                maxRecords: 1
            })
            .firstPage();

        if (clauseRecords.length === 0) {
            return res.status(404).json({ error: 'Clause not found' });
        }

        const clause = clauseRecords[0];
        const variablesTokens = clause.fields['Variables (tokens)'] || '';

        if (!variablesTokens || !variablesTokens.trim()) {
            return res.json([]);
        }

        // Parse tokens from the "Variables (tokens)" field
        const tokens = variablesTokens
            .split(/[;,\n]/)
            .map(token => token.trim())
            .filter(token => token && token.length > 0)
            .map(token => {
                if (token.startsWith('{') && token.endsWith('}')) {
                    return token.replace(/[{}]/g, '');
                }
                return token;
            })
            .filter(token => token.length > 0);

        if (tokens.length === 0) {
            return res.json([]);
        }

        // Build filter formula to match any of the tokens
        const tokenFilters = tokens.map(token => `{Variable}='{${token}}'`);
        const filterFormula = tokenFilters.length === 1 
            ? tokenFilters[0]
            : `OR(${tokenFilters.join(', ')})`;

        // Fetch matching variables from Clause_Variables table
        const variablesTableId = 'tbltDsYa9J266USdB';
        const records = await base(variablesTableId)
            .select({
                filterByFormula: filterFormula
            })
            .all();

        const variables = records.map(record => ({
            id: record.id,
            variable: record.fields['Variable'] || '',
            variableName: record.fields['Variable Name'] || '',
            description: record.fields['Description'] || '',
            dataType: record.fields['Data Type'] || '',
            typicalRange: record.fields['Typical Range / Options'] || record.fields['Typical Range'] || '',
            exampleValue: record.fields['Example Value'] || '',
            units: record.fields['Units'] || '',
            agreementTypes: Array.isArray(record.fields['Agreement Type(s)']) ? record.fields['Agreement Type(s)'].join(', ') : (record.fields['Agreement Type(s)'] || ''),
            category: record.fields['Category'] || ''
        }));

        // Sort variables to match the order of tokens in the clause
        const sortedVariables = tokens
            .map(token => variables.find(v => {
                const varToken = (v.variable || '').replace(/[{}]/g, '');
                return varToken === token;
            }))
            .filter(v => v !== undefined)
            .concat(variables.filter(v => {
                const varToken = (v.variable || '').replace(/[{}]/g, '');
                return !tokens.includes(varToken);
            }));

        res.json(sortedVariables);
    } catch (error) {
        console.error('Error in getClauseVariables:', error);
        res.status(500).json({ 
            error: 'Failed to fetch clause variables',
            message: error.message 
        });
    }
}

// Get all clause IDs for navigation (prev/next)
export async function getClauseIds(req, res) {
    try {
        const tableNameOrId = 'tbl4wXAIpWLhiRP6W';
        const allRecords = [];
        await base(tableNameOrId)
            .select({ fields: ['Clause ID'], sort: [{ field: 'Clause ID', direction: 'asc' }] })
            .eachPage((records, fetchNextPage) => {
                allRecords.push(...records);
                fetchNextPage();
            });
        const ids = allRecords
            .map(r => (r.fields && r.fields['Clause ID']) ? r.fields['Clause ID'] : null)
            .filter(Boolean)
            .sort();
        res.json(ids);
    } catch (error) {
        console.error('Error in getClauseIds:', error);
        res.status(500).json({ 
            error: 'Failed to fetch clause IDs',
            message: error.message 
        });
    }
}

// Create a new clause record
export async function createClause(req, res) {
    try {
        if (req.method !== 'POST') {
            return res.status(405).json({ error: 'Method not allowed. Use POST to create clauses.' });
        }

        const {
            clauseId,
            clauseName,
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
            clauseText,
            negotiationNote,
            negotiationCompromise,
            negotiationMarketNote,
            // Legacy field for backward compatibility
            negotiationNotes,
            redFlags,
            // Structured common positions fields
            commonPositionsOwner,
            commonPositionsOwnerRationale,
            commonPositionsBrand,
            commonPositionsBrandRationale,
            commonPositionsBalanced,
            commonPositionsBalancedRationale,
            variablesTokens
        } = req.body;

        // Validate required fields
        if (!clauseId || !clauseName) {
            return res.status(400).json({ error: 'Clause ID and Clause Name are required' });
        }

        // Check if clause with this ID already exists
        const tableNameOrId = 'tbl4wXAIpWLhiRP6W'; // Clause_Library table ID
        const existing = await base(tableNameOrId)
            .select({
                filterByFormula: `{Clause ID}='${clauseId.replace(/'/g, "\\'")}'`,
                maxRecords: 1
            })
            .firstPage();

        if (existing.length > 0) {
            return res.status(409).json({ 
                error: 'Clause ID already exists',
                clauseId: clauseId
            });
        }

        // Prepare fields for Airtable
        const fields = {
            'Clause ID': clauseId,
            'Clause Name': clauseName,
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
            'Clause Text': clauseText || '',
            // New structured negotiation notes fields
            'Negotiation Note': negotiationNote || '',
            'Negotiation Compromise': negotiationCompromise || '',
            'Negotiation Market Note': negotiationMarketNote || '',
            // Legacy field for backward compatibility
            'Negotiation Notes': negotiationNotes || '',
            'Red Flags': redFlags || '',
            // Structured common positions fields
            'Common Positions - Owner': commonPositionsOwner || '',
            'Common Positions - Owner Rational': commonPositionsOwnerRationale || '',
            'Common Positions - Brand': commonPositionsBrand || '',
            'Common Positions - Brand Rational': commonPositionsBrandRationale || '',
            'Common Positions - Balanced': commonPositionsBalanced || '',
            'Common Positions - Balanced Rational': commonPositionsBalancedRationale || '',
            'Variables (tokens)': variablesTokens || ''
        };

        // Add Last Reviewed date if provided
        if (lastReviewed) {
            fields['Last Reviewed'] = lastReviewed;
        }

        // Create the record using write-capable base
        const record = await writeBase(tableNameOrId).create(fields, { typecast: true });

        // Return formatted record
        const formattedRecord = formatClauseRecord(record);
        res.status(201).json({
            success: true,
            message: 'Clause created successfully',
            clause: formattedRecord
        });

    } catch (error) {
        console.error('Error in createClause:', error);
        res.status(500).json({ 
            error: 'Failed to create clause',
            message: error.message 
        });
    }
}
