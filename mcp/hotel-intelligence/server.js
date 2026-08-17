#!/usr/bin/env node
/**
 * Dealality Hotel Intelligence MCP (stdio).
 *
 * Tools: hotel_search, hotel_get, hotel_resolve, hotel_enrich,
 *        hotel_nearby, hotel_sources, hotel_review_queue, hotel_census_ingest,
 *        hotel_room_count_research
 *
 * Default: no Airtable writes. Never log secrets.
 */

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import {
  createHotelIntelligenceService,
  FUTURE_TOOL_CONTRACTS,
} from "../../lib/hotel-intelligence/index.js";

const SERVER_NAME = "dealality-hotel-intelligence";
const SERVER_VERSION = "1.0.0";

function jsonResult(payload) {
  return {
    content: [
      {
        type: "text",
        text: JSON.stringify(payload, null, 2),
      },
    ],
  };
}

function errorResult(err) {
  const message = String(err?.message || err).slice(0, 400);
  if (process.env.NODE_ENV !== "production") {
    console.error(
      JSON.stringify({
        module: "hotel-intelligence-mcp",
        event: "tool_error",
        message,
      })
    );
  }
  return {
    isError: true,
    content: [{ type: "text", text: JSON.stringify({ ok: false, error: message }) }],
  };
}

export function createHotelIntelligenceMcpServer(service) {
  const mcp = new McpServer({
    name: SERVER_NAME,
    version: SERVER_VERSION,
  });

  mcp.registerTool(
    "hotel_search",
    {
      description:
        "Search Dealality census (and optionally Hotelbeds) for hotel candidates. Returns normalized candidates; does not write census.",
      inputSchema: {
        name: z.string().optional(),
        city: z.string().optional(),
        country: z.string().optional(),
        country_code: z.string().optional(),
        brand: z.string().optional(),
        latitude: z.number().optional(),
        longitude: z.number().optional(),
        radius: z.number().optional(),
        limit: z.number().optional(),
        providers: z.array(z.string()).optional(),
        hotel_codes: z.array(z.string()).optional(),
      },
    },
    async (args) => {
      try {
        return jsonResult(await service.hotelSearch(args || {}));
      } catch (err) {
        return errorResult(err);
      }
    }
  );

  mcp.registerTool(
    "hotel_get",
    {
      description:
        "Get canonical hotel by Dealality hotel_id (dhl_…) plus evidence summary.",
      inputSchema: {
        hotel_id: z.string(),
      },
    },
    async (args) => {
      try {
        return jsonResult(await service.hotelGet(args || {}));
      } catch (err) {
        return errorResult(err);
      }
    }
  );

  mcp.registerTool(
    "hotel_resolve",
    {
      description:
        "Resolve incoming property identity against Hotel Property Census. Never auto-merges ambiguous matches.",
      inputSchema: {
        name: z.string().optional(),
        address: z.string().optional(),
        city: z.string().optional(),
        country: z.string().optional(),
        latitude: z.number().optional(),
        longitude: z.number().optional(),
        brand: z.string().optional(),
        website: z.string().optional(),
        phone: z.string().optional(),
        external_ids: z.record(z.string(), z.string()).optional(),
      },
    },
    async (args) => {
      try {
        return jsonResult(await service.hotelResolve(args || {}));
      } catch (err) {
        return errorResult(err);
      }
    }
  );

  mcp.registerTool(
    "hotel_enrich",
    {
      description:
        "Enrich missing/current fields from providers. Stages evidence locally; does not blindly write canonical census.",
      inputSchema: {
        hotel_id: z.string(),
        fields: z.array(z.string()).optional(),
        providers: z.array(z.string()).optional(),
      },
    },
    async (args) => {
      try {
        return jsonResult(await service.hotelEnrich(args || {}));
      } catch (err) {
        return errorResult(err);
      }
    }
  );

  mcp.registerTool(
    "hotel_nearby",
    {
      description:
        "Find hotels within radius_km of coordinates (in-memory Haversine over census slice).",
      inputSchema: {
        latitude: z.number(),
        longitude: z.number(),
        radius_km: z.number().optional(),
        radius: z.number().optional(),
        limit: z.number().optional(),
        filters: z
          .object({
            brand: z.string().optional(),
            parent_company: z.string().optional(),
            chain_scale: z.string().optional(),
            status: z.string().optional(),
            room_count_min: z.number().optional(),
            room_count_max: z.number().optional(),
          })
          .optional(),
      },
    },
    async (args) => {
      try {
        return jsonResult(await service.hotelNearby(args || {}));
      } catch (err) {
        return errorResult(err);
      }
    }
  );

  mcp.registerTool(
    "hotel_sources",
    {
      description: "Return field-level source evidence and conflicts for a hotel_id.",
      inputSchema: {
        hotel_id: z.string(),
        field: z.string().optional(),
      },
    },
    async (args) => {
      try {
        return jsonResult(await service.hotelSources(args || {}));
      } catch (err) {
        return errorResult(err);
      }
    }
  );

  mcp.registerTool(
    "hotel_review_queue",
    {
      description:
        "List machine-readable review items (ambiguous identity, conflicts, missing material fields).",
      inputSchema: {
        status: z.string().optional(),
        issue_type: z.string().optional(),
        hotel_id: z.string().optional(),
      },
    },
    async (args) => {
      try {
        return jsonResult(await service.hotelReviewQueue(args || {}));
      } catch (err) {
        return errorResult(err);
      }
    }
  );

  mcp.registerTool(
    "hotel_census_ingest",
    {
      description:
        "Controlled ingest: normalize → resolve → enrich(optional) → score → classify → stage. Default stage-only (no Airtable writes).",
      inputSchema: {
        record: z.record(z.string(), z.any()).optional(),
        records: z.array(z.record(z.string(), z.any())).optional(),
        enrich: z.boolean().optional(),
        batch_id: z.string().optional(),
      },
    },
    async (args) => {
      try {
        return jsonResult(await service.hotelCensusIngest(args || {}));
      } catch (err) {
        return errorResult(err);
      }
    }
  );

  mcp.registerTool(
    "hotel_room_count_research",
    {
      description:
        "Research TOTAL PROPERTY ROOM COUNT / KEYS for one known hotel using official website + targeted public-source searches. Evidence-backed only; never crawls broadly; never writes census.",
      inputSchema: {
        hotel_id: z.string().optional(),
        hotel_name: z.string().optional(),
        name: z.string().optional(),
        city: z.string().optional(),
        country: z.string().optional(),
        brand: z.string().optional(),
        website: z.string().optional(),
        latitude: z.number().optional(),
        longitude: z.number().optional(),
        identity_confidence: z.number().optional(),
        max_searches: z.number().optional(),
        max_page_fetches: z.number().optional(),
        allow_serpapi: z.boolean().optional(),
      },
    },
    async (args) => {
      try {
        return jsonResult(await service.hotelRoomCountResearch(args || {}));
      } catch (err) {
        return errorResult(err);
      }
    }
  );

  mcp.registerTool(
    "hotel_intelligence_meta",
    {
      description:
        "Server metadata, provider availability, and future tool contracts (not implemented).",
      inputSchema: {},
    },
    async () => {
      try {
        const availability = await service.providers.availability();
        return jsonResult({
          ok: true,
          server: SERVER_NAME,
          version: SERVER_VERSION,
          service_version: service.version,
          airtable_writes_enabled: service.airtableWritesEnabled(),
          providers: availability,
          future_tools: FUTURE_TOOL_CONTRACTS,
          tools: [
            "hotel_search",
            "hotel_get",
            "hotel_resolve",
            "hotel_enrich",
            "hotel_nearby",
            "hotel_sources",
            "hotel_review_queue",
            "hotel_census_ingest",
            "hotel_room_count_research",
            "hotel_intelligence_meta",
          ],
          provider_notes: {
            stayingapi_rooms_capability: "NOT_SUPPORTED",
            stayingapi_env: "HOTEL_INTELLIGENCE_STAYINGAPI + STAYINGAPI_KEY",
            serpapi_rooms_capability: "NOT_SUPPORTED",
            serpapi_env: "HOTEL_INTELLIGENCE_SERPAPI + SERPAPI_KEY",
            serpapi_engine: "google_hotels (Maps place_id not wired)",
            giata_drive_role:
              "COMPLEMENTARY_IDENTITY_GEO_BRAND_ENRICHMENT — not primary CALA universe, not room_count, not MultiCodes",
            giata_drive_rooms_capability: "SUPPORTED_BUT_NOT_ENTITLED",
            giata_drive_env: "HOTEL_INTELLIGENCE_GIATA_DRIVE + GIATA_DRIVE_API_KEY",
            room_count_research:
              "hotel_room_count_research — evidence-backed; uses official URL + optional SerpApi google search; no Airtable writes",
          },
        });
      } catch (err) {
        return errorResult(err);
      }
    }
  );

  return mcp;
}

async function main() {
  const service = createHotelIntelligenceService();
  const server = createHotelIntelligenceMcpServer(service);
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

const isDirect =
  process.argv[1] &&
  (process.argv[1].endsWith("mcp/hotel-intelligence/server.js") ||
    process.argv[1].endsWith("mcp\\hotel-intelligence\\server.js"));

if (isDirect) {
  main().catch((err) => {
    console.error(
      JSON.stringify({
        module: "hotel-intelligence-mcp",
        event: "fatal",
        message: String(err?.message || err).slice(0, 400),
      })
    );
    process.exit(1);
  });
}
