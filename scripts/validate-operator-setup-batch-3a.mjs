import fs from "fs";
import path from "path";

const BASE_URL = process.env.BATCH3A_BASE_URL || "http://127.0.0.1:8095";

function result(name, expected, actual, pass, extra = {}) {
  return { name, expected, actual, pass: !!pass, ...extra };
}

async function safeJson(res) {
  try {
    return await res.json();
  } catch {
    return null;
  }
}

function hasNarrative(prefill, key, expected) {
  const actual = prefill?.[key];
  return typeof actual === "string" && actual.trim() === String(expected).trim();
}

async function run() {
  const stamp = Date.now();
  const payload = {
    companyName: `Batch3A Validation ${stamp}`,
    contactEmail: `batch3a+${stamp}@example.com`,
    website: "https://example.com",
    yearEstablished: 2014,
    companySize: "101-250",
    primaryServiceModel: "Third-Party Management",
    companyTagline: "Owner outcomes through disciplined execution",
    missionStatement: "Deliver transparent, accountable operations for owners.",
    companyHistory: "Founded in 2014; expanded across multi-market conversions.",
    differentiators: "Structured operating cadence and owner governance.",
    managementPhilosophy: "Data-informed decisions with local market execution.",
    brand_conversion_project_count: 12,
    submitMode: "full",
  };

  const multipart = new FormData();
  multipart.set("payload", JSON.stringify(payload));
  multipart.set(
    "companyLogo",
    new Blob([`fake-image-${stamp}`], { type: "image/png" }),
    "batch3a-logo.png"
  );

  const checks = [];

  const uploadCreateRes = await fetch(`${BASE_URL}/api/intake/third-party-operator`, {
    method: "POST",
    body: multipart,
  });
  const uploadCreateBody = await safeJson(uploadCreateRes);
  const uploadRecordId = uploadCreateBody?.recordId || null;
  checks.push(
    result(
      "save_multipart_with_logo_and_narratives",
      "201 + writeMode=canonical + recordId",
      { status: uploadCreateRes.status, writeMode: uploadCreateBody?.writeMode, recordId: uploadRecordId },
      uploadCreateRes.status === 201 && uploadCreateBody?.writeMode === "canonical" && !!uploadRecordId
    )
  );

  const canonicalLogoPayload = {
    ...payload,
    companyName: `${payload.companyName} Canonical Logo`,
    companyLogo: [{ url: `https://picsum.photos/seed/batch3a-${stamp}/640/360`, filename: "batch3a-public-logo.png" }],
  };
  const canonicalLogoCreateRes = await fetch(`${BASE_URL}/api/intake/third-party-operator`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(canonicalLogoPayload),
  });
  const canonicalLogoCreateBody = await safeJson(canonicalLogoCreateRes);
  const recordId = canonicalLogoCreateBody?.recordId || null;
  checks.push(
    result(
      "save_canonical_logo_payload",
      "201 + writeMode=canonical + recordId",
      { status: canonicalLogoCreateRes.status, writeMode: canonicalLogoCreateBody?.writeMode, recordId },
      canonicalLogoCreateRes.status === 201 && canonicalLogoCreateBody?.writeMode === "canonical" && !!recordId
    )
  );

  if (!recordId) {
    return { ok: false, checks, recordId: null };
  }

  const detailRes = await fetch(`${BASE_URL}/api/intake/third-party-operators/${recordId}`);
  const detailBody = await safeJson(detailRes);
  const prefill = detailBody?.operator?.prefill || {};
  const fields = detailBody?.operator?.fields || {};

  checks.push(
    result(
      "detail_read_path_new_base",
      "200 + meta.readPath=new_base",
      { status: detailRes.status, readPath: detailBody?.meta?.readPath || null },
      detailRes.status === 200 && detailBody?.meta?.readPath === "new_base"
    )
  );

  checks.push(
    result(
      "prefill_narrative_roundtrip",
      "prefill contains 5 narrative keys with saved values",
      {
        companyTagline: prefill.companyTagline || null,
        missionStatement: prefill.missionStatement || null,
        companyHistory: prefill.companyHistory || null,
        differentiators: prefill.differentiators || null,
        managementPhilosophy: prefill.managementPhilosophy || null,
      },
      hasNarrative(prefill, "companyTagline", payload.companyTagline) &&
        hasNarrative(prefill, "missionStatement", payload.missionStatement) &&
        hasNarrative(prefill, "companyHistory", payload.companyHistory) &&
        hasNarrative(prefill, "differentiators", payload.differentiators) &&
        hasNarrative(prefill, "managementPhilosophy", payload.managementPhilosophy)
    )
  );

  checks.push(
    result(
      "detail_fields_brand_conversion_project_count",
      "detail fields include brand_conversion_project_count=12",
      { value: fields.brand_conversion_project_count ?? null },
      Number(fields.brand_conversion_project_count) === Number(payload.brand_conversion_project_count)
    )
  );

  checks.push(
    result(
      "detail_prefill_logo_present",
      "prefill.companyLogo includes uploaded logo attachment",
      {
        companyLogo: Array.isArray(prefill.companyLogo) ? prefill.companyLogo.slice(0, 1) : prefill.companyLogo || null,
      },
      Array.isArray(prefill.companyLogo) &&
        prefill.companyLogo.length > 0 &&
        typeof prefill.companyLogo[0]?.url === "string" &&
        prefill.companyLogo[0].url.trim() !== ""
    )
  );

  const explorerRes = await fetch(`${BASE_URL}/api/operator-explorer/operator?operatorId=${recordId}`);
  const explorerBody = await safeJson(explorerRes);
  checks.push(
    result(
      "explorer_detail_live_record",
      "200 + readPath=new_base + no demoData",
      {
        status: explorerRes.status,
        readPath: explorerBody?.meta?.readPath || null,
        demoData: explorerBody?.meta?.demoData || false,
      },
      explorerRes.status === 200 &&
        explorerBody?.meta?.readPath === "new_base" &&
        !explorerBody?.meta?.demoData
    )
  );

  const listRes = await fetch(`${BASE_URL}/api/intake/third-party-operators`);
  const listBody = await safeJson(listRes);
  const listRows = Array.isArray(listBody?.operators) ? listBody.operators : Array.isArray(listBody?.data) ? listBody.data : [];
  const listed = listRows.find((row) => row && row.id === recordId) || null;
  checks.push(
    result(
      "list_logo_readback",
      "list row includes non-empty logo URL for saved operator",
      { found: !!listed, logo: listed?.logo || null },
      !!listed && typeof listed.logo === "string" && listed.logo.trim() !== ""
    )
  );

  const ok = checks.every((c) => c.pass);
  return { ok, checks, recordId };
}

const out = await run();
const outputPath = path.resolve("reports", "operator-setup-batch-3a-validation-output.json");
fs.writeFileSync(outputPath, JSON.stringify(out, null, 2));
console.log(JSON.stringify({ ok: out.ok, recordId: out.recordId, checks: out.checks.length, outputPath }, null, 2));
