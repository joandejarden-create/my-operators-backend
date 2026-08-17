/**
 * Upload file bytes to an Airtable attachment field via content API.
 * POST https://content.airtable.com/v0/{baseId}/{recordId}/{fieldIdOrName}/uploadAttachment
 *
 * Airtable hosts the file and returns airtableusercontent.com URLs.
 * Max file size: 5 MB (Airtable content API limit).
 */

export const MAX_AIRTABLE_ATTACHMENT_BYTES = 5 * 1024 * 1024;

const EXT_MIME = {
  ".pdf": "application/pdf",
  ".doc": "application/msword",
  ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  ".xls": "application/vnd.ms-excel",
  ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".png": "image/png",
};

export function contentTypeFromFilename(filename) {
  const ext = String(filename || "")
    .toLowerCase()
    .match(/\.[^.]+$/)?.[0];
  return (ext && EXT_MIME[ext]) || "application/octet-stream";
}

/** Airtable uploadAttachment responses key fields by field ID, not display name. */
export function extractUploadResponseAttachments(json, fieldName) {
  const fields = json?.fields ?? json?.records?.[0]?.fields;
  if (!fields || typeof fields !== "object") return null;
  if (Array.isArray(fields[fieldName])) return fields[fieldName];
  for (const value of Object.values(fields)) {
    if (
      Array.isArray(value) &&
      value.length > 0 &&
      value.every((item) => item && typeof item === "object" && typeof item.url === "string")
    ) {
      return value;
    }
  }
  return null;
}

/**
 * @returns {Promise<object[]|null>} Attachment array on the field after upload, or null if response omits it (caller should re-fetch).
 */
export async function uploadFileBytesToAirtable({
  baseId,
  recordId,
  fieldName,
  buffer,
  contentType,
  filename,
  apiKey = process.env.AIRTABLE_API_KEY,
}) {
  if (!baseId || !recordId || !fieldName) {
    throw new Error("baseId, recordId, and fieldName are required for Airtable upload");
  }
  if (!apiKey) {
    throw new Error("AIRTABLE_API_KEY not configured");
  }
  if (!Buffer.isBuffer(buffer)) {
    throw new Error("buffer must be a Buffer");
  }
  if (buffer.length > MAX_AIRTABLE_ATTACHMENT_BYTES) {
    throw new Error(
      `File exceeds Airtable attachment limit (${MAX_AIRTABLE_ATTACHMENT_BYTES} bytes)`
    );
  }

  const endpoint = `https://content.airtable.com/v0/${baseId}/${recordId}/${encodeURIComponent(fieldName)}/uploadAttachment`;
  const res = await fetch(endpoint, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      contentType: contentType || contentTypeFromFilename(filename),
      file: buffer.toString("base64"),
      filename: filename || "attachment",
    }),
  });

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Airtable uploadAttachment ${res.status}: ${text.slice(0, 400)}`);
  }

  const json = text ? JSON.parse(text) : {};
  return extractUploadResponseAttachments(json, fieldName);
}
