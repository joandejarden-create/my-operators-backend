import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function extractBetween(source, startNeedle, endNeedle) {
  const start = source.indexOf(startNeedle);
  if (start === -1) return "";
  const from = start + startNeedle.length;
  const end = source.indexOf(endNeedle, from);
  if (end === -1) return "";
  return source.slice(from, end);
}

function extractIdentifiersFromDestructure(block) {
  return Array.from(
    new Set(
      block
        .split("\n")
        .map((line) => line.replace(/\/\/.*$/, "").trim())
        .filter(Boolean)
        .map((line) => line.replace(/,$/, "").trim())
        .filter((line) => /^[A-Za-z_][A-Za-z0-9_]*$/.test(line))
    )
  );
}

function extractFormInputNames(html) {
  const out = new Set();
  const re = /<(input|select|textarea)\b[^>]*\bname="([^"]+)"/gi;
  let match;
  while ((match = re.exec(html)) !== null) {
    out.add(match[2]);
  }
  return Array.from(out).sort((a, b) => a.localeCompare(b));
}

function buildFieldMappingCoverage(requestKeys, fieldsObjectBlock) {
  const mappedRequestKeys = [];
  const unmappedRequestKeys = [];
  const intentionallyNonFieldKeys = ["submittedAt"];
  const intentionallyDerivedRequestKeys = [
    "mgmtFeeMin",
    "mgmtFeeMax",
    "mgmtFeeBasis",
    "mgmtFeeNotes",
    "incentiveFeeMin",
    "incentiveFeeMax",
    "incentiveFeeBasis",
    "incentiveFeeNotes",
    "incentiveExcessMin",
    "incentiveExcessMax",
    "incentiveExcessBasis",
    "incentiveExcessNotes",
    "brandsPortfolioDetail",
  ];

  for (const key of requestKeys) {
    if (intentionallyNonFieldKeys.includes(key) || intentionallyDerivedRequestKeys.includes(key)) continue;
    const keyRegex = new RegExp(`\\b${key}\\b`);
    if (keyRegex.test(fieldsObjectBlock)) {
      mappedRequestKeys.push(key);
    } else {
      unmappedRequestKeys.push(key);
    }
  }

  return {
    mappedRequestKeys,
    unmappedRequestKeys,
    intentionallyNonFieldKeys,
    intentionallyDerivedRequestKeys,
  };
}

export default async function getThirdPartyOperatorMappingReport(req, res) {
  try {
    if (req.method !== "GET") {
      return res.status(405).json({ error: "Method not allowed. Use GET." });
    }

    const intakeApiPath = path.join(__dirname, "third-party-operator-intake.js");
    const intakeHtmlPath = path.join(__dirname, "..", "public", "third-party-operator-intake.html");
    const [apiSource, htmlSource] = await Promise.all([
      fs.readFile(intakeApiPath, "utf8"),
      fs.readFile(intakeHtmlPath, "utf8"),
    ]);

    const destructureBlock = extractBetween(apiSource, "const {", "} = req.body;");
    const fieldsObjectBlock = extractBetween(apiSource, "const fields = {", "\n        };");

    if (!destructureBlock || !fieldsObjectBlock) {
      return res.status(500).json({
        error: "Unable to parse mapping source blocks",
        message: "Could not locate req.body destructure or fields mapping object.",
      });
    }

    const requestKeys = extractIdentifiersFromDestructure(destructureBlock);
    const formInputNames = extractFormInputNames(htmlSource);
    const {
      mappedRequestKeys,
      unmappedRequestKeys,
      intentionallyNonFieldKeys,
      intentionallyDerivedRequestKeys,
    } =
      buildFieldMappingCoverage(requestKeys, fieldsObjectBlock);

    const requestKeySet = new Set(requestKeys);
    const formInputSet = new Set(formInputNames);
    const mappedRequestKeySet = new Set(mappedRequestKeys);

    const intentionallyHelperOnlyFormInputs = formInputNames.filter((name) => name.endsWith("Other"));
    const helperOnlySet = new Set(["companyLogo", ...intentionallyHelperOnlyFormInputs]);

    const formInputsMissingInBackend = formInputNames.filter((name) => !requestKeySet.has(name));
    const actionableFormInputsMissingInBackend = formInputsMissingInBackend.filter((name) => !helperOnlySet.has(name));
    const backendKeysNotInForm = requestKeys.filter((key) => !formInputSet.has(key));
    const formInputsNotMappedToAirtableFields = formInputNames.filter((name) => {
      if (helperOnlySet.has(name)) return false; // helper/multipart fields, handled separately
      return requestKeySet.has(name) && !mappedRequestKeySet.has(name);
    });
    const actionableBackendKeysNotInForm = backendKeysNotInForm.filter(
      (key) => !intentionallyNonFieldKeys.includes(key) && !intentionallyDerivedRequestKeys.includes(key)
    );
    const actionableUnmappedRequestKeys = unmappedRequestKeys.filter(
      (key) => !intentionallyDerivedRequestKeys.includes(key)
    );

    return res.status(200).json({
      success: true,
      source: {
        form: "public/third-party-operator-intake.html",
        intakeApi: "api/third-party-operator-intake.js",
      },
      counts: {
        formInputNames: formInputNames.length,
        backendRequestKeys: requestKeys.length,
        mappedRequestKeys: mappedRequestKeys.length,
        unmappedRequestKeys: unmappedRequestKeys.length,
        formInputsMissingInBackend: formInputsMissingInBackend.length,
        backendKeysNotInForm: backendKeysNotInForm.length,
        formInputsNotMappedToAirtableFields: formInputsNotMappedToAirtableFields.length,
        actionableFormInputsMissingInBackend: actionableFormInputsMissingInBackend.length,
        actionableBackendKeysNotInForm: actionableBackendKeysNotInForm.length,
        actionableUnmappedRequestKeys: actionableUnmappedRequestKeys.length,
      },
      intentionallyNonFieldKeys,
      intentionallyDerivedRequestKeys,
      intentionallyHelperOnlyFormInputs,
      formInputsMissingInBackend,
      actionableFormInputsMissingInBackend,
      backendKeysNotInForm,
      actionableBackendKeysNotInForm,
      unmappedRequestKeys,
      actionableUnmappedRequestKeys,
      formInputsNotMappedToAirtableFields,
    });
  } catch (error) {
    console.error("Error building third-party operator mapping report:", error);
    return res.status(500).json({
      error: "Failed to generate mapping report",
      message: error && error.message ? error.message : "Unknown error",
    });
  }
}
