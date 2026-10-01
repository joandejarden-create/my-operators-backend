/**
 * INEGI DENUE — Hotel Intelligence source adapter surface.
 */

export {
  DENUE_CLIENT_VERSION,
  DENUE_API_BASE,
  DENUE_TOKEN_SETUP_URL,
  DENUE_DOCS_URL,
  DENUE_STATE_CSV_URL,
  DENUE_ENTIDAD,
  DENUE_HOTEL_ACTIVITY_RE,
  isDenueHotelEstablishment,
  isDenueApiConfigured,
  describeDenueAccess,
  normalizeDenueRecord,
  denueBuscar,
  denueNombre,
  denueFicha,
  ensureDenueStateCsvZip,
  loadDenueHotelRowsFromCsvDir,
  prepareDenueHotelIndexForEntidad,
} from "./client.js";

export {
  DENUE_MATCH_VERSION,
  BUSINESS_RELATIONSHIP,
  haversineMeters,
  scoreDenueCandidate,
  matchHotelToDenue,
} from "./match.js";
