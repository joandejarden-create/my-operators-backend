/**
 * Validation and sanitization for POST /api/intake/deal.
 * Reuses Deal Setup pattern: validate → payload → write; soft validation; dev-only field mapping.
 */

const DEV = process.env.NODE_ENV !== "production" || process.env.DEBUG_DEAL_SETUP === "true" || process.env.DEBUG_INTAKE_DEAL === "true";

/** Redact email for dev log (first 2 chars + domain). */
function redactEmail(email) {
  if (!email || typeof email !== "string") return "(empty)";
  const trimmed = email.trim().toLowerCase();
  const at = trimmed.indexOf("@");
  if (at <= 0 || at === trimmed.length - 1) return trimmed.slice(0, 2) + "***";
  return trimmed.slice(0, 2) + "***@" + trimmed.slice(at + 1);
}

/**
 * Validate and sanitize intake-deal request body.
 * - valid: false if missing required projectName or email.
 * - errors: list of { field, message }.
 * - payload: { projectName, email, memberstackId?, country?, firstName?, lastName? } with trimmed/lowercased values.
 * - fieldMappingUsed: (dev only) which key maps to which table (Users vs Deals).
 *
 * @param {Record<string, unknown>} body - req.body
 * @returns {{ valid: boolean, errors: Array<{ field: string, message: string }>, payload: Record<string, string>, fieldMappingUsed?: Record<string, string> }}
 */
export function validateIntakeDealPayload(body) {
  const errors = [];
  const fieldMappingUsed = DEV ? { projectName: "Deals", email: "Users", memberstackId: "Users", country: "Users", firstName: "Users", lastName: "Users" } : undefined;

  if (!body || typeof body !== "object") {
    return { valid: false, errors: [{ field: "_", message: "Request body must be an object" }], payload: {}, fieldMappingUsed };
  }

  let projectName = body.projectName;
  let email = body.email;
  let memberstackId = body.memberstackId;
  let country = body.country;
  let firstName = body.firstName;
  let lastName = body.lastName;

  if (projectName != null && typeof projectName === "string") projectName = projectName.trim();
  if (email != null && typeof email === "string") email = email.trim().toLowerCase();
  if (memberstackId != null && typeof memberstackId === "string") memberstackId = memberstackId.trim();
  if (country != null && typeof country === "string") country = country.trim();
  if (firstName != null && typeof firstName === "string") firstName = firstName.trim();
  if (lastName != null && typeof lastName === "string") lastName = lastName.trim();

  if (!projectName || projectName === "") errors.push({ field: "projectName", message: "projectName is required" });
  if (!email || email === "") errors.push({ field: "email", message: "email is required" });

  if (errors.length > 0) {
    return { valid: false, errors, payload: {}, fieldMappingUsed };
  }

  const payload = {
    projectName,
    email,
    ...(memberstackId !== undefined && memberstackId !== "" ? { memberstackId } : {}),
    ...(country !== undefined && country !== "" ? { country } : {}),
    ...(firstName !== undefined && firstName !== "" ? { firstName } : {}),
    ...(lastName !== undefined && lastName !== "" ? { lastName } : {}),
  };

  return { valid: true, errors: [], payload, fieldMappingUsed };
}

/** Build a safe payload preview for dev logs (redact email). */
export function intakePayloadPreview(payload) {
  if (!payload || typeof payload !== "object") return {};
  const p = { ...payload };
  if (p.email) p.email = redactEmail(p.email);
  return p;
}
