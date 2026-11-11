import fs from 'node:fs';
import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import {
  SCHEMA_V1_2_PATH,
  SCHEMA_V1_1_LEGACY_PATH,
  SCHEMA_V1_LEGACY_PATH,
  SCHEMA_VERSION,
  SCHEMA_VERSION_V1_1,
  SCHEMA_VERSION_LEGACY,
} from './constants.js';

let cachedValidators = null;

function loadSchema(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function buildValidators() {
  const ajv = new Ajv2020({
    allErrors: true,
    strict: true,
    validateSchema: true,
  });
  addFormats(ajv);

  const schemaV12 = loadSchema(SCHEMA_V1_2_PATH);
  const validateV12 = ajv.compile(schemaV12);

  let validateV11 = null;
  if (fs.existsSync(SCHEMA_V1_1_LEGACY_PATH)) {
    validateV11 = ajv.compile(loadSchema(SCHEMA_V1_1_LEGACY_PATH));
  }

  let validateLegacy = null;
  if (fs.existsSync(SCHEMA_V1_LEGACY_PATH)) {
    validateLegacy = ajv.compile(loadSchema(SCHEMA_V1_LEGACY_PATH));
  }

  return { ajv, validateV12, validateV11, validateLegacy, schemaV12 };
}

export function getValidators() {
  if (!cachedValidators) cachedValidators = buildValidators();
  return cachedValidators;
}

function mapErrors(validateFn) {
  return (validateFn.errors || []).map((e) => ({
    instancePath: e.instancePath,
    message: e.message,
    keyword: e.keyword,
    params: e.params,
  }));
}

/**
 * Validate handoff object against draft 2020-12 schema (AJV-driven).
 */
export function validateHandoffSchema(handoff) {
  const { validateV12, validateV11, validateLegacy } = getValidators();
  const version = handoff?.schemaVersion;

  if (version === SCHEMA_VERSION) {
    const ok = validateV12(handoff);
    return {
      ok: Boolean(ok),
      schemaVersion: version,
      schemaDriven: true,
      draft: '2020-12',
      errors: ok ? [] : mapErrors(validateV12),
    };
  }

  if (version === SCHEMA_VERSION_V1_1 && validateV11) {
    const ok = validateV11(handoff);
    return {
      ok: Boolean(ok),
      schemaVersion: version,
      schemaDriven: true,
      draft: '2020-12',
      legacy: true,
      errors: ok ? [] : mapErrors(validateV11),
    };
  }

  if (version === SCHEMA_VERSION_LEGACY && validateLegacy) {
    const ok = validateLegacy(handoff);
    return {
      ok: Boolean(ok),
      schemaVersion: version,
      schemaDriven: true,
      draft: '2020-12',
      legacy: true,
      errors: ok ? [] : mapErrors(validateLegacy),
    };
  }

  return {
    ok: false,
    schemaVersion: version || null,
    schemaDriven: true,
    draft: '2020-12',
    errors: [
      {
        instancePath: '/schemaVersion',
        message: `unsupported schemaVersion (expected ${SCHEMA_VERSION}, ${SCHEMA_VERSION_V1_1}, or ${SCHEMA_VERSION_LEGACY})`,
        keyword: 'const',
        params: {},
      },
    ],
  };
}
