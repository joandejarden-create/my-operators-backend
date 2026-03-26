/**
 * Load .env and .env.local BEFORE any Airtable-dependent modules.
 * Must be imported first in server.js so process.env is populated before intake-deal.js loads.
 */
import dotenv from "dotenv";
dotenv.config({ path: ".env", override: false });
dotenv.config({ path: ".env.local", override: true });
