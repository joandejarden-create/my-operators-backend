/**
 * Upload image bytes to an Airtable attachment field (max 5 MB).
 * API: POST /v0/{baseId}/{recordId}/{fieldName}/uploadAttachment
 */
export async function uploadImageBytesToAirtable({
  baseId,
  recordId,
  fieldName = "Image",
  buffer,
  contentType,
  filename,
  apiKey = process.env.AIRTABLE_API_KEY,
}) {
  const endpoint = `https://content.airtable.com/v0/${baseId}/${recordId}/${encodeURIComponent(fieldName)}/uploadAttachment`;
  const res = await fetch(endpoint, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      contentType,
      file: buffer.toString("base64"),
      filename,
    }),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`uploadAttachment ${res.status}: ${text.slice(0, 300)}`);
  const json = text ? JSON.parse(text) : {};
  return json?.fields?.[fieldName] || json?.records?.[0]?.fields?.[fieldName];
}

export function extFromImageContentType(ct) {
  if (/png/i.test(ct)) return "png";
  if (/webp/i.test(ct)) return "webp";
  if (/gif/i.test(ct)) return "gif";
  return "jpg";
}

export const MAX_AIRTABLE_ATTACHMENT_BYTES = 5 * 1024 * 1024;
