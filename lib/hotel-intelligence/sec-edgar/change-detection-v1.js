/**
 * Incremental change detection vs prior verified state.
 */

import { CHANGE_STATUS } from "./contract.js";

export const CHANGE_DETECTION_VERSION = "sec-edgar-change-detection-v1";

function findingKey(f) {
  return [
    f.hpcHotelId || f.attributedHotelName || f.hotelNameHints?.[0] || "",
    f.evidenceFieldName || "",
    f.primaryDate || "",
    f.source?.accessionNumber || "",
  ]
    .join("|")
    .toLowerCase();
}

/**
 * @param {object[]} previousFindings
 * @param {object[]} currentFindings
 */
export function diffContractFindings(previousFindings = [], currentFindings = []) {
  const prevMap = new Map(previousFindings.map((f) => [findingKey(f), f]));
  const currMap = new Map(currentFindings.map((f) => [findingKey(f), f]));
  const changes = [];

  for (const [key, curr] of currMap) {
    const prev = prevMap.get(key);
    if (!prev) {
      changes.push({ status: CHANGE_STATUS.NEW, finding: curr });
      continue;
    }
    const sameSnippet = String(prev.snippet || "") === String(curr.snippet || "");
    const sameClass = prev.verificationClass === curr.verificationClass;
    if (sameSnippet && sameClass) {
      changes.push({ status: CHANGE_STATUS.UNCHANGED, finding: curr });
    } else if (prev.primaryDate && curr.primaryDate && prev.primaryDate !== curr.primaryDate) {
      changes.push({
        status: CHANGE_STATUS.SUPERSEDED,
        finding: curr,
        prior: prev,
      });
    } else if (!sameClass && sameSnippet === false) {
      changes.push({ status: CHANGE_STATUS.UPDATED, finding: curr, prior: prev });
    } else if (!sameClass) {
      changes.push({ status: CHANGE_STATUS.CONFLICTED, finding: curr, prior: prev });
    } else {
      changes.push({ status: CHANGE_STATUS.UPDATED, finding: curr, prior: prev });
    }
  }

  for (const [key, prev] of prevMap) {
    if (!currMap.has(key)) {
      changes.push({ status: CHANGE_STATUS.SUPERSEDED, finding: null, prior: prev, dropped: true });
    }
  }

  const counts = Object.fromEntries(
    Object.values(CHANGE_STATUS).map((s) => [s, changes.filter((c) => c.status === s).length]),
  );

  return {
    version: CHANGE_DETECTION_VERSION,
    counts,
    changes,
  };
}

/** Skip AI / re-extraction when document fingerprint unchanged. */
export function documentUnchanged(prevHash, nextHash) {
  if (!prevHash || !nextHash) return false;
  return String(prevHash) === String(nextHash);
}
