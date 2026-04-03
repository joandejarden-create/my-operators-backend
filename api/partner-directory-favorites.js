import Airtable from "airtable";

// Lazy initialization of Airtable base
function getBase() {
    if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
        return null;
    }
    return new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
}

// Field mappings for User Favorites table
const FAVORITES_FIELDS = {
    userId: "User_ID", // Linked record to Users table
    partnerType: "Partner Type", // Single select: "Company" or "Individual"
    companyProfile: "Company Profile", // Linked record to Company Profile table
    individualProfile: "Individual Profile", // Linked record to User Management table
    userProfile: "User Profile", // Linked record to Users table (for Users table individuals)
    category: "Category", // Single select: "Hot Leads", "Follow Up", "Important", "Research", "Competitors", "Partners"
    notes: "Notes", // Long text
    favoritedDate: "Favorited Date", // Date with time
    lastViewed: "Last Viewed" // Date with time
};

// Get all favorites for a user
export async function getUserFavorites(req, res) {
    try {
        let { userId } = req.query;
        
        if (!userId) {
            return res.json({ favorites: [] }); // Return empty if no userId provided
        }

        const base = getBase();
        if (!base) {
            return res.status(500).json({ error: "Airtable not configured" });
        }

        // Get the User Favorites table ID from config (will be passed via query or env)
        const USER_FAVORITES_TABLE_ID = req.query.tableId || process.env.USER_FAVORITES_TABLE_ID;
        if (!USER_FAVORITES_TABLE_ID) {
            return res.status(500).json({ error: "User Favorites table ID not configured" });
        }

        // Validate userId is a valid Airtable record ID
        if (!userId.startsWith('rec')) {
            // Try to find a valid user ID
            const USERS_TABLE_ID = process.env.USERS_TABLE_ID || 'tbl6shiyz2wdUqE5F';
            try {
                const users = await base(USERS_TABLE_ID)
                    .select({ maxRecords: 1 })
                    .firstPage();
                if (users.length > 0) {
                    userId = users[0].id;
                } else {
                    return res.json({ favorites: [] }); // Return empty if no valid user
                }
            } catch (error) {
                return res.json({ favorites: [] }); // Return empty if can't find user
            }
        }

        // Fetch favorites for this user
        // User_ID is a linked record, so we need to filter by the linked record ID
        // For linked records in Airtable, use the record ID directly in the filter
        const records = await base(USER_FAVORITES_TABLE_ID)
            .select({
                filterByFormula: `{User_ID} = '${userId}'`,
                maxRecords: 1000
            })
            .all();

        const favorites = records.map(record => {
            const fields = record.fields || {};
            const partnerType = fields[FAVORITES_FIELDS.partnerType] || '';
            
            // Get the partner record ID based on type
            let partnerId = null;
            if (partnerType === 'Company') {
                const companyProfile = fields[FAVORITES_FIELDS.companyProfile];
                if (Array.isArray(companyProfile) && companyProfile.length > 0) {
                    partnerId = companyProfile[0];
                } else if (typeof companyProfile === 'string') {
                    partnerId = companyProfile;
                }
            } else if (partnerType === 'Individual') {
                // Check both Individual Profile (User Management) and User Profile (Users) fields
                const individualProfile = fields[FAVORITES_FIELDS.individualProfile];
                const userProfile = fields[FAVORITES_FIELDS.userProfile];
                
                if (Array.isArray(individualProfile) && individualProfile.length > 0) {
                    partnerId = individualProfile[0];
                } else if (typeof individualProfile === 'string') {
                    partnerId = individualProfile;
                } else if (Array.isArray(userProfile) && userProfile.length > 0) {
                    partnerId = userProfile[0];
                } else if (typeof userProfile === 'string') {
                    partnerId = userProfile;
                }
            }

            return {
                id: record.id,
                partnerId: partnerId,
                type: partnerType.toLowerCase(), // 'company' or 'individual'
                category: fields[FAVORITES_FIELDS.category] || 'Important',
                notes: fields[FAVORITES_FIELDS.notes] || '',
                favoritedDate: fields[FAVORITES_FIELDS.favoritedDate] || new Date().toISOString(),
                lastViewed: fields[FAVORITES_FIELDS.lastViewed] || null
            };
        });

        res.json({ favorites });
    } catch (error) {
        console.error("Error fetching user favorites:", error);
        res.status(500).json({ 
            error: "Failed to fetch favorites", 
            details: error.message 
        });
    }
}

// Create a new favorite
export async function createFavorite(req, res) {
    try {
        if (req.method !== "POST") {
            return res.status(405).json({ error: "Method not allowed" });
        }

        let { userId, partnerId, partnerType, category, notes, sourceTable } = req.body;

        if (!partnerId || !partnerType) {
            return res.status(400).json({ error: "Missing required fields: partnerId, partnerType" });
        }

        // userId is optional - we'll find one if not provided

        const base = getBase();
        if (!base) {
            return res.status(500).json({ error: "Airtable not configured" });
        }

        const USER_FAVORITES_TABLE_ID = req.body.tableId || process.env.USER_FAVORITES_TABLE_ID;
        if (!USER_FAVORITES_TABLE_ID) {
            return res.status(500).json({ error: "User Favorites table ID not configured" });
        }

        // Normalize partner type
        const normalizedPartnerType = partnerType.charAt(0).toUpperCase() + partnerType.slice(1).toLowerCase(); // "Company" or "Individual"
        
        // Get source table from request body (if provided by frontend)
        // This tells us which table the individual belongs to: "Users" or "User Management"
        const individualSourceTable = sourceTable || null;

        // Validate userId is a valid Airtable record ID (starts with 'rec')
        // If userId is null/undefined or not a valid record ID, find a user from Users table
        let validUserId = userId;
        if (!userId || !userId.startsWith('rec')) {
            // Not a valid record ID - try to find or create a user
            const USERS_TABLE_ID = process.env.USERS_TABLE_ID || 'tbl6shiyz2wdUqE5F';
            
            try {
                // Try to find an existing user (for testing, use first available user)
                const users = await base(USERS_TABLE_ID)
                    .select({ maxRecords: 1 })
                    .firstPage();
                
                if (users.length > 0) {
                    validUserId = users[0].id;
                } else {
                    return res.status(400).json({ 
                        error: "Invalid user ID. User_ID must be a valid Airtable record ID (starts with 'rec').",
                        details: `Received: "${userId}". Please set a valid user ID via authentication or HTML element.`,
                        help: "In production, set userId via: <div id='airtable-user-id'>recYourUserId</div> or URL parameter ?userId=recYourUserId"
                    });
                }
            } catch (error) {
                return res.status(400).json({ 
                    error: "Invalid user ID and could not find alternative user.",
                    details: `User ID "${userId}" is not a valid Airtable record ID. User_ID field requires a linked record to the Users table.`,
                    help: "Please set a valid user ID. In production, this should come from your authentication system."
                });
            }
        }


        // Determine which field to use based on partner type and source table
        let partnerField;
        if (normalizedPartnerType === 'Company') {
            partnerField = FAVORITES_FIELDS.companyProfile;
        } else if (normalizedPartnerType === 'Individual') {
            // For individuals, use the appropriate field based on source table
            if (individualSourceTable === 'Users') {
                partnerField = FAVORITES_FIELDS.userProfile; // Link to Users table
            } else {
                partnerField = FAVORITES_FIELDS.individualProfile; // Link to User Management table (default)
            }
        } else {
            return res.status(400).json({ error: "Invalid partner type. Must be 'Company' or 'Individual'" });
        }

        // Check for existing favorite
        // For individuals, check both fields (User Profile and Individual Profile) in case the source table changed
        let existingRecords = [];
        if (normalizedPartnerType === 'Individual') {
            // Check both User Profile and Individual Profile fields
            const userProfileFilter = `AND({User_ID} = '${validUserId}', {${FAVORITES_FIELDS.userProfile}} = '${partnerId}')`;
            const individualProfileFilter = `AND({User_ID} = '${validUserId}', {${FAVORITES_FIELDS.individualProfile}} = '${partnerId}')`;
            const combinedFilter = `OR(${userProfileFilter}, ${individualProfileFilter})`;
            
            existingRecords = await base(USER_FAVORITES_TABLE_ID)
                .select({
                    filterByFormula: combinedFilter,
                    maxRecords: 1
                })
                .all();
        } else {
            // For companies, use the company profile field
            existingRecords = await base(USER_FAVORITES_TABLE_ID)
                .select({
                    filterByFormula: `AND({User_ID} = '${validUserId}', {${partnerField}} = '${partnerId}')`,
                    maxRecords: 1
                })
                .all();
        }

        if (existingRecords.length > 0) {
            // Update existing favorite
            const existingRecord = existingRecords[0];
            const updateFields = {
                [FAVORITES_FIELDS.category]: category || 'Important',
                [FAVORITES_FIELDS.lastViewed]: new Date().toISOString()
            };
            if (notes !== undefined) {
                updateFields[FAVORITES_FIELDS.notes] = notes;
            }

            const updatedRecord = await base(USER_FAVORITES_TABLE_ID).update(existingRecord.id, updateFields);
            
            return res.json({
                id: updatedRecord.id,
                message: "Favorite updated",
                favorite: {
                    id: updatedRecord.id,
                    partnerId: partnerId,
                    type: partnerType.toLowerCase(),
                    category: category || 'Important',
                    notes: notes || '',
                    favoritedDate: existingRecord.fields[FAVORITES_FIELDS.favoritedDate] || new Date().toISOString()
                }
            });
        }

        // Create new favorite
        const fields = {
            [FAVORITES_FIELDS.userId]: [validUserId], // Linked record array
            [FAVORITES_FIELDS.partnerType]: normalizedPartnerType,
            [partnerField]: [partnerId], // Linked record array
            [FAVORITES_FIELDS.category]: category || 'Important',
            [FAVORITES_FIELDS.favoritedDate]: new Date().toISOString(),
            [FAVORITES_FIELDS.lastViewed]: new Date().toISOString()
        };

        if (notes) {
            fields[FAVORITES_FIELDS.notes] = notes;
        }

        let record;
        try {
            record = await base(USER_FAVORITES_TABLE_ID).create(fields, { typecast: true });
        } catch (createError) {
            // Handle Airtable errors - if the record doesn't belong to the correct table, Airtable will return an error
            if (createError.message && createError.message.includes('belongs to table')) {
                // Check if it's a Users table individual trying to be favorited
                if (normalizedPartnerType === 'Individual' && createError.message.includes('Users table')) {
                    return res.status(400).json({
                        error: "Cannot favorite individual from Users table",
                        details: createError.message,
                        help: "The Individual Profile field in User_Favorites only links to the User Management table. To favorite Users table individuals, you may need to update your Airtable schema to add a separate field that links to the Users table, or migrate the individual to the User Management table."
                    });
                }
                return res.status(400).json({
                    error: "Cannot create favorite - table mismatch",
                    details: createError.message,
                    help: normalizedPartnerType === 'Individual' 
                        ? "The Individual Profile field only links to the User Management table. Make sure you're favoriting an individual from the User Management table."
                        : "Please check that the record belongs to the correct table."
                });
            }
            // Re-throw other errors to be handled by the outer catch
            throw createError;
        }

        res.json({
            id: record.id,
            message: "Favorite created",
            favorite: {
                id: record.id,
                partnerId: partnerId,
                type: partnerType.toLowerCase(),
                category: category || 'Important',
                notes: notes || '',
                favoritedDate: new Date().toISOString()
            }
        });
    } catch (error) {
        console.error("Error creating favorite:", error);
        res.status(500).json({ 
            error: "Failed to create favorite", 
            details: error.message 
        });
    }
}

// Delete a favorite
export async function deleteFavorite(req, res) {
    try {
        if (req.method !== "DELETE") {
            return res.status(405).json({ error: "Method not allowed" });
        }

        const { favoriteId } = req.params;
        let { userId } = req.query; // Optional: verify user owns this favorite
        
        // Normalize userId - handle string "null" or "undefined"
        if (userId === 'null' || userId === 'undefined' || !userId) {
            userId = null;
        }

        if (!favoriteId) {
            return res.status(400).json({ error: "Favorite ID is required" });
        }

        const base = getBase();
        if (!base) {
            return res.status(500).json({ error: "Airtable not configured" });
        }

        const USER_FAVORITES_TABLE_ID = req.query.tableId || process.env.USER_FAVORITES_TABLE_ID;
        if (!USER_FAVORITES_TABLE_ID) {
            return res.status(500).json({ error: "User Favorites table ID not configured" });
        }

        // Optional: Verify user owns this favorite before deleting
        // Skip verification if userId is null (for testing/fallback scenarios)
        if (userId) {
            try {
                const record = await base(USER_FAVORITES_TABLE_ID).find(favoriteId);
                const fields = record.fields || {};
                const recordUserId = fields[FAVORITES_FIELDS.userId];
                const recordUserIdValue = Array.isArray(recordUserId) ? recordUserId[0] : recordUserId;
                
                // Only check if record has a userId field set
                if (recordUserIdValue && recordUserIdValue !== userId) {
                    return res.status(403).json({ error: "Unauthorized: You can only delete your own favorites" });
                }
            } catch (error) {
                // If record not found or other error, log but continue with deletion
                console.warn('⚠️ Could not verify favorite ownership:', error.message);
            }
        }

        await base(USER_FAVORITES_TABLE_ID).destroy(favoriteId);

        res.json({ 
            message: "Favorite deleted",
            id: favoriteId
        });
    } catch (error) {
        console.error("Error deleting favorite:", error);
        res.status(500).json({ 
            error: "Failed to delete favorite", 
            details: error.message 
        });
    }
}

// Update a favorite (e.g., change category or notes)
export async function updateFavorite(req, res) {
    try {
        if (req.method !== "PUT") {
            return res.status(405).json({ error: "Method not allowed" });
        }

        const { favoriteId } = req.params;
        const { category, notes, userId } = req.body;

        if (!favoriteId) {
            return res.status(400).json({ error: "Favorite ID is required" });
        }

        const base = getBase();
        if (!base) {
            return res.status(500).json({ error: "Airtable not configured" });
        }

        const USER_FAVORITES_TABLE_ID = req.body.tableId || process.env.USER_FAVORITES_TABLE_ID;
        if (!USER_FAVORITES_TABLE_ID) {
            return res.status(500).json({ error: "User Favorites table ID not configured" });
        }

        // Optional: Verify user owns this favorite before updating
        if (userId) {
            const record = await base(USER_FAVORITES_TABLE_ID).find(favoriteId);
            const fields = record.fields || {};
            const recordUserId = fields[FAVORITES_FIELDS.userId];
            const recordUserIdValue = Array.isArray(recordUserId) ? recordUserId[0] : recordUserId;
            
            if (recordUserIdValue !== userId) {
                return res.status(403).json({ error: "Unauthorized: You can only update your own favorites" });
            }
        }

        const updateFields = {};
        if (category !== undefined) {
            updateFields[FAVORITES_FIELDS.category] = category;
        }
        if (notes !== undefined) {
            updateFields[FAVORITES_FIELDS.notes] = notes;
        }
        updateFields[FAVORITES_FIELDS.lastViewed] = new Date().toISOString();

        const record = await base(USER_FAVORITES_TABLE_ID).update(favoriteId, updateFields);

        res.json({
            id: record.id,
            message: "Favorite updated",
            favorite: {
                id: record.id,
                category: record.fields[FAVORITES_FIELDS.category] || category,
                notes: record.fields[FAVORITES_FIELDS.notes] || notes,
                lastViewed: record.fields[FAVORITES_FIELDS.lastViewed]
            }
        });
    } catch (error) {
        console.error("Error updating favorite:", error);
        res.status(500).json({ 
            error: "Failed to update favorite", 
            details: error.message 
        });
    }
}
