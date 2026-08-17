/**
 * Field-state helpers for Operator Fit adapters.
 */

import { FIELD_STATE } from "../config.js";

export function fieldPresent(value, meta = {}) {
  return {
    state: FIELD_STATE.PRESENT,
    value,
    ...meta,
  };
}

export function fieldAbsent(meta = {}) {
  return {
    state: FIELD_STATE.ABSENT,
    value: null,
    ...meta,
  };
}

export function fieldUnknown(meta = {}) {
  return {
    state: FIELD_STATE.UNKNOWN,
    value: null,
    ...meta,
  };
}

export function fieldNotApplicable(meta = {}) {
  return {
    state: FIELD_STATE.NOT_APPLICABLE,
    value: null,
    ...meta,
  };
}

export function fieldInvalid(raw, meta = {}) {
  return {
    state: FIELD_STATE.INVALID,
    value: null,
    raw,
    ...meta,
  };
}

export function fieldInferred(value, meta = {}) {
  return {
    state: FIELD_STATE.INFERRED,
    value,
    ...meta,
  };
}

export function isKnownPositive(field) {
  return (
    field &&
    (field.state === FIELD_STATE.PRESENT || field.state === FIELD_STATE.INFERRED) &&
    field.value != null &&
    !(Array.isArray(field.value) && field.value.length === 0) &&
    !(typeof field.value === "string" && !field.value.trim())
  );
}

export function isUnknownish(field) {
  if (!field) return true;
  return (
    field.state === FIELD_STATE.UNKNOWN ||
    field.state === FIELD_STATE.ABSENT ||
    field.state === FIELD_STATE.INVALID
  );
}

export function isNotApplicable(field) {
  return field && field.state === FIELD_STATE.NOT_APPLICABLE;
}

export function listValue(field) {
  if (!isKnownPositive(field)) return [];
  const v = field.value;
  if (Array.isArray(v)) return v.map((x) => String(x).trim()).filter(Boolean);
  if (typeof v === "string") {
    return v
      .split(/\s*,\s*/)
      .map((x) => x.trim())
      .filter(Boolean);
  }
  return [String(v)];
}

export function scalarValue(field) {
  if (!isKnownPositive(field)) return "";
  if (Array.isArray(field.value)) return field.value.map(String).join(", ");
  return String(field.value || "").trim();
}
