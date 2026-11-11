import fs from 'node:fs';
import path from 'node:path';
import { MAX_ARTIFACT_BYTES } from './constants.js';

/**
 * Blocking patterns for pre-export / pre-GitHub review packages.
 * Intentionally conservative; findings block external sync.
 */
export const BLOCKING_SECRET_PATTERNS = [
  { id: 'env_assignment', re: /(?:^|\n)\s*(?:export\s+)?[A-Z][A-Z0-9_]*(?:API_KEY|SECRET|TOKEN|PASSWORD|PRIVATE_KEY)\s*=\s*['"]?[^'\s\n]+/i, label: 'credential assignment' },
  { id: 'openai_sk', re: /\bsk-[a-zA-Z0-9]{20,}\b/, label: 'OpenAI-style key' },
  { id: 'airtable_pat', re: /\bpat[a-zA-Z0-9]{14,}\b/, label: 'Airtable PAT-style token' },
  { id: 'github_pat', re: /\bghp_[a-zA-Z0-9]{20,}\b/, label: 'GitHub PAT' },
  { id: 'aws_key', re: /\bAKIA[0-9A-Z]{16}\b/, label: 'AWS access key id' },
  { id: 'private_key', re: /-----BEGIN (?:RSA |OPENSSH |EC )?PRIVATE KEY-----/, label: 'private key block' },
  { id: 'bearer', re: /\bBearer\s+[A-Za-z0-9\-._~+/]{20,}=*/, label: 'Bearer token' },
  { id: 'memberstack_secret', re: /\bMEMBERSTACK_SECRET\b\s*[:=]/i, label: 'Memberstack secret marker' },
  {
    id: 'signed_share_url',
    re: /(?:group-demand-intelligence-share|ai-demand-positioning-share|client-share)[^?\s]*\?[^?\s]*share=[A-Za-z0-9\-._~%+]{16,}/i,
    label: 'signed customer share URL',
  },
  {
    id: 'share_token_param',
    re: /[?&]share=[A-Za-z0-9\-._~%+]{24,}/i,
    label: 'share token query parameter',
  },
  {
    id: 'email_pii_dense',
    re: /(?:[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\s*){5,}/i,
    label: 'dense email PII cluster',
  },
];

/**
 * Paths that intentionally embed secret-like fixtures for negative tests / scanner source.
 * Still scanned for real .env files / binaries; pattern matches here are warnings, not blockers.
 */
export function isSecretPatternAllowlistedPath(relPosix) {
  const p = String(relPosix || '').replace(/\\/g, '/');
  if (p === 'lib/gdi-2-review-loop/secrets-scan.js') return true;
  if (p === 'lib/gdi-2-review-loop/export-scan.js') return true;
  if (/^scripts\/test-gdi-2-review-loop-.*\.mjs$/.test(p)) return true;
  return false;
}

const TEXT_EXT = new Set(['.md', '.json', '.txt', '.log', '.csv', '.yml', '.yaml', '.js', '.mjs', '.cjs', '.html']);
const BINARY_EXT = new Set(['.png', '.jpg', '.jpeg', '.gif', '.webp', '.pdf', '.zip', '.exe', '.dll', '.node', '.wasm', '.bin']);

function walkFiles(dir, out = []) {
  if (!fs.existsSync(dir)) return out;
  for (const name of fs.readdirSync(dir)) {
    const p = path.join(dir, name);
    const st = fs.statSync(p);
    if (st.isDirectory()) walkFiles(p, out);
    else out.push(p);
  }
  return out;
}

/**
 * Scan a run directory for secrets, oversized artifacts, unexpected binaries.
 * Returns blocking findings (errors) and warnings.
 */
export function scanRunPackageForSecrets(runDir, { maxBytes = MAX_ARTIFACT_BYTES } = {}) {
  const errors = [];
  const warnings = [];
  const files = walkFiles(runDir);

  for (const abs of files) {
    const rel = path.relative(runDir, abs).replace(/\\/g, '/');
    const ext = path.extname(abs).toLowerCase();
    const st = fs.statSync(abs);

    if (st.size > maxBytes) {
      errors.push(`oversized artifact (${st.size} bytes > ${maxBytes}): ${rel}`);
    }

    const base = path.basename(abs);
    if (/^\.env($|\.)/i.test(base)) {
      errors.push(`sensitive config file present in package: ${rel}`);
      continue;
    }

    if (BINARY_EXT.has(ext)) {
      errors.push(`unexpected binary artifact in review package: ${rel}`);
      continue;
    }

    if (!TEXT_EXT.has(ext) && st.size > 0) {
      // Peek for null bytes → treat as binary
      const fd = fs.openSync(abs, 'r');
      const buf = Buffer.alloc(Math.min(512, st.size));
      fs.readSync(fd, buf, 0, buf.length, 0);
      fs.closeSync(fd);
      if (buf.includes(0)) {
        errors.push(`unexpected binary content in ${rel}`);
        continue;
      }
    }

    if (!TEXT_EXT.has(ext)) continue;

    let text;
    try {
      text = fs.readFileSync(abs, 'utf8');
    } catch (e) {
      warnings.push(`could not read ${rel}: ${e.message}`);
      continue;
    }

    for (const pat of BLOCKING_SECRET_PATTERNS) {
      if (pat.re.test(text)) {
        // Run-package scans are under runs/<id>/... — no allowlist there.
        errors.push(`secret/privacy finding [${pat.id}/${pat.label}] in ${rel}`);
      }
    }
  }

  return { ok: errors.length === 0, errors, warnings, filesScanned: files.length };
}

/**
 * Scan arbitrary text (for unit tests / pre-export buffers).
 */
export function scanTextForSecrets(text, label = 'buffer') {
  const errors = [];
  for (const pat of BLOCKING_SECRET_PATTERNS) {
    if (pat.re.test(text)) {
      errors.push(`secret/privacy finding [${pat.id}/${pat.label}] in ${label}`);
    }
  }
  return { ok: errors.length === 0, errors };
}
