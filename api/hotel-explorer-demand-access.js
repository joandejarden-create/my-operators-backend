/**
 * Hotel Explorer Demand Drivers + Access & Connectivity — GET-only reads.
 *
 * GET /api/hotel-explorer/hotels/:hotelId/demand-drivers
 * GET /api/hotel-explorer/hotels/:hotelId/access-connectivity
 *
 * Reuses Demand Anchors + Travel Infrastructure Data; HPC for hotel identity.
 * No paid providers. No writes.
 */

import {
  buildHeDemandDriversRead,
  buildHeAccessConnectivityRead,
} from "../lib/hotel-intelligence/he-demand-access-read-v1.js";

function send(res, status, body) {
  res.setHeader("X-Dealality-Write", "forbidden");
  res.setHeader("X-Dealality-Paid-Providers", "forbidden");
  res.setHeader("Cache-Control", "private, max-age=30, stale-while-revalidate=30");
  return res.status(status).json(body);
}

function hotelIdFromReq(req) {
  return String(req.params?.hotelId || "").trim();
}

export async function getHotelExplorerDemandDrivers(req, res) {
  const hotelId = hotelIdFromReq(req);
  if (!/^rec[A-Za-z0-9]{14}$/.test(hotelId)) {
    return send(res, 400, {
      success: false,
      ok: false,
      error: "invalid_hotel_id",
      writeOnGet: false,
      paidProviders: false,
    });
  }
  try {
    const payload = await buildHeDemandDriversRead(hotelId, {
      radiusKm: req.query?.radiusKm ? Number(req.query.radiusKm) : undefined,
      limit: req.query?.limit ? Number(req.query.limit) : undefined,
    });
    if (!payload.ok) {
      const status = payload.error === "hpc_hotel_not_found" ? 404 : 502;
      return send(res, status, { success: false, ...payload });
    }
    return send(res, 200, { success: true, ...payload });
  } catch (err) {
    console.error("[he-demand-drivers]", err);
    return send(res, 500, {
      success: false,
      ok: false,
      error: "demand_drivers_read_failed",
      message: String(err?.message || err).slice(0, 240),
      writeOnGet: false,
      paidProviders: false,
    });
  }
}

export async function getHotelExplorerAccessConnectivity(req, res) {
  const hotelId = hotelIdFromReq(req);
  if (!/^rec[A-Za-z0-9]{14}$/.test(hotelId)) {
    return send(res, 400, {
      success: false,
      ok: false,
      error: "invalid_hotel_id",
      writeOnGet: false,
      paidProviders: false,
    });
  }
  try {
    const payload = await buildHeAccessConnectivityRead(hotelId, {
      radiusKm: req.query?.radiusKm ? Number(req.query.radiusKm) : undefined,
      limit: req.query?.limit ? Number(req.query.limit) : undefined,
      minAirports: req.query?.minAirports ? Number(req.query.minAirports) : 2,
    });
    if (!payload.ok) {
      const status = payload.error === "hpc_hotel_not_found" ? 404 : 502;
      return send(res, status, { success: false, ...payload });
    }
    return send(res, 200, { success: true, ...payload });
  } catch (err) {
    console.error("[he-access-connectivity]", err);
    return send(res, 500, {
      success: false,
      ok: false,
      error: "access_connectivity_read_failed",
      message: String(err?.message || err).slice(0, 240),
      writeOnGet: false,
      paidProviders: false,
    });
  }
}
