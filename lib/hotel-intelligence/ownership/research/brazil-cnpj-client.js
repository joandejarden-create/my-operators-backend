/**
 * Brazil CNPJ public registry client (BrasilAPI) — P1.6.
 * Read-only; no Receita writes.
 */

import { loadCnpjRegistryFixture } from "./brazil-cnpj-fixtures.js";

export const BRAZIL_CNPJ_CLIENT_VERSION = "ownership-brazil-cnpj-client-v1";

const DEFAULT_BASE = "https://brasilapi.com.br/api/cnpj/v1";

/** @type {Map<string, { at: number, data: object }>} */
const cache = new Map();
const CACHE_TTL_MS = 15 * 60 * 1000;

/**
 * @param {string} value
 * @returns {string|null}
 */
export function normalizeCnpjDigits(value) {
  const d = String(value || "").replace(/\D/g, "");
  if (d.length !== 14) return null;
  return d;
}

/**
 * Format 14-digit CNPJ as XX.XXX.XXX/XXXX-XX
 * @param {string} digits
 */
export function formatCnpj(digits) {
  const d = normalizeCnpjDigits(digits);
  if (!d) return null;
  return `${d.slice(0, 2)}.${d.slice(2, 5)}.${d.slice(5, 8)}/${d.slice(8, 12)}-${d.slice(12, 14)}`;
}

/**
 * Compute CNPJ check digits for base 12 digits (8 root + 4 branch).
 * @param {string} base12
 */
export function computeCnpjCheckDigits(base12) {
  const b = String(base12 || "").replace(/\D/g, "");
  if (b.length !== 12) return null;
  const w1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2];
  const w2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2];
  let sum = 0;
  for (let i = 0; i < 12; i++) sum += Number(b[i]) * w1[i];
  let d1 = sum % 11;
  d1 = d1 < 2 ? 0 : 11 - d1;
  const b13 = b + String(d1);
  sum = 0;
  for (let i = 0; i < 13; i++) sum += Number(b13[i]) * w2[i];
  let d2 = sum % 11;
  d2 = d2 < 2 ? 0 : 11 - d2;
  return b + String(d1) + String(d2);
}

/**
 * @param {string} cnpj14
 * @returns {string|null} matriz CNPJ digits
 */
export function matrizCnpjFromAny(cnpj14) {
  const d = normalizeCnpjDigits(cnpj14);
  if (!d) return null;
  if (d.slice(8, 12) === "0001") return d;
  return computeCnpjCheckDigits(`${d.slice(0, 8)}0001`);
}

/**
 * @param {object} raw — BrasilAPI payload
 */
export function mapBrasilApiCnpj(raw) {
  if (!raw || typeof raw !== "object") return null;
  const cnpj = normalizeCnpjDigits(raw.cnpj);
  if (!cnpj) return null;
  const isFilial =
    String(raw.descricao_identificador_matriz_filial || "")
      .toUpperCase()
      .includes("FILIAL") || cnpj.slice(8, 12) !== "0001";

  return {
    cnpj,
    cnpj_formatted: formatCnpj(cnpj),
    cnpj_raiz: String(raw.cnpj_raiz || cnpj.slice(0, 8)),
    razao_social: String(raw.razao_social || "").trim(),
    nome_fantasia: String(raw.nome_fantasia || "").trim() || null,
    municipio: String(raw.municipio || "").trim(),
    uf: String(raw.uf || "").trim(),
    logradouro: String(raw.logradouro || "").trim(),
    numero: String(raw.numero || "").trim(),
    bairro: String(raw.bairro || "").trim(),
    cep: String(raw.cep || "").replace(/\D/g, ""),
    cnae_fiscal: raw.cnae_fiscal ?? null,
    cnae_fiscal_descricao: String(raw.cnae_fiscal_descricao || "").trim(),
    situacao_cadastral: String(raw.descricao_situacao_cadastral || "").trim(),
    is_filial: isFilial,
    is_matriz: !isFilial,
    matriz_cnpj: isFilial ? matrizCnpjFromAny(cnpj) : cnpj,
    qsa: Array.isArray(raw.qsa)
      ? raw.qsa.map((p) => ({
          nome: String(p.nome_socio || p.nome || "").trim(),
          qualificacao: String(p.qualificacao_socio || p.qualificacao || "").trim(),
          documento: String(p.cnpj_cpf_do_socio || p.cpf_cnpj_socio || "").replace(
            /\D/g,
            ""
          ),
          data_entrada: p.data_entrada_sociedade || p.data_entrada || null,
          is_pessoa_juridica: String(p.identificador_de_socio || "")
            .toLowerCase()
            .includes("juridica") || String(p.cnpj_cpf_do_socio || "").replace(/\D/g, "").length === 14,
        }))
      : [],
    source: "brasilapi",
    raw_slim: {
      porte: raw.porte,
      natureza_juridica: raw.natureza_juridica,
      capital_social: raw.capital_social,
    },
  };
}

/**
 * @param {object} raw — cnpj.ws payload
 */
export function mapCnpjWs(raw) {
  if (!raw || typeof raw !== "object") return null;
  const est = raw.estabelecimento || {};
  const cnpj = normalizeCnpjDigits(est.cnpj || raw.cnpj);
  if (!cnpj) return null;
  const tipo = String(est.tipo || "").toLowerCase();
  const isFilial = tipo.includes("filial") || cnpj.slice(8, 12) !== "0001";
  const socios = Array.isArray(raw.socios) ? raw.socios : [];
  return {
    cnpj,
    cnpj_formatted: formatCnpj(cnpj),
    cnpj_raiz: String(raw.cnpj_raiz || cnpj.slice(0, 8)),
    razao_social: String(raw.razao_social || est.nome_empresa || "").trim(),
    nome_fantasia: String(est.nome_fantasia || "").trim() || null,
    municipio: String(est.cidade?.nome || est.cidade || "").trim(),
    uf: String(est.estado?.sigla || est.estado || "").trim(),
    logradouro: String(est.logradouro || "").trim(),
    numero: String(est.numero || "").trim(),
    bairro: String(est.bairro || "").trim(),
    cep: String(est.cep || "").replace(/\D/g, ""),
    cnae_fiscal: est.atividade_principal?.id || null,
    cnae_fiscal_descricao: String(est.atividade_principal?.descricao || "").trim(),
    situacao_cadastral: String(est.situacao_cadastral || "").trim(),
    is_filial: isFilial,
    is_matriz: !isFilial,
    matriz_cnpj: isFilial ? matrizCnpjFromAny(cnpj) : cnpj,
    qsa: socios.map((p) => ({
      nome: String(p.nome || "").trim(),
      qualificacao: String(p.qualificacao?.descricao || p.qualificacao || "").trim(),
      documento: String(p.cpf_cnpj || p.cnpj_cpf || "").replace(/\D/g, ""),
      data_entrada: p.data_entrada || null,
      is_pessoa_juridica:
        String(p.tipo || "").toLowerCase().includes("pj") ||
        String(p.cpf_cnpj || "").replace(/\D/g, "").length === 14,
    })),
    source: "cnpj_ws",
    raw_slim: { porte: raw.porte, capital_social: raw.capital_social },
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function fixtureFallback(digits, reason) {
  const fixture = loadCnpjRegistryFixture(digits);
  if (!fixture) return null;
  return {
    ok: true,
    record: fixture,
    cached: false,
    url: `fixture://ownership-brazil-cnpj-registry/${digits}.json`,
    fixture: true,
    fallback_reason: reason,
  };
}

async function fetchCnpjWs(digits, timeoutMs, retry429 = true) {
  const url = `https://publica.cnpj.ws/cnpj/${digits}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal: controller.signal,
      headers: { Accept: "application/json" },
    });
    if (res.status === 429 && retry429) {
      clearTimeout(timer);
      await sleep(2500);
      return fetchCnpjWs(digits, timeoutMs, false);
    }
    if (!res.ok) return { ok: false, error: `http_${res.status}`, record: null, url };
    const json = await res.json();
    const record = mapCnpjWs(json);
    if (!record) return { ok: false, error: "unmapped_response", record: null, url };
    return { ok: true, record, url };
  } catch (err) {
    return {
      ok: false,
      error: String(err?.name === "AbortError" ? "timeout" : err?.message || err).slice(0, 80),
      record: null,
      url,
    };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * @param {string} cnpj
 * @param {{ env?: object, timeoutMs?: number, force?: boolean }} [opts]
 */
export async function fetchCnpjRegistry(cnpj, opts = {}) {
  const digits = normalizeCnpjDigits(cnpj);
  if (!digits) {
    return { ok: false, error: "invalid_cnpj", record: null };
  }

  const cacheKey = digits;
  const cached = cache.get(cacheKey);
  if (cached && !opts.force && Date.now() - cached.at < CACHE_TTL_MS) {
    return { ok: true, record: cached.data, cached: true };
  }

  const base = String(
    opts.env?.BRAZIL_CNPJ_API_BASE || process.env.BRAZIL_CNPJ_API_BASE || DEFAULT_BASE
  ).replace(/\/$/, "");
  const url = `${base}/${digits}`;
  const timeoutMs = Number(opts.timeoutMs || 25000);

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal: controller.signal,
      headers: { Accept: "application/json" },
    });
    if (!res.ok) {
      clearTimeout(timer);
      const fallback = await fetchCnpjWs(digits, timeoutMs);
      if (fallback.ok && fallback.record) {
        cache.set(cacheKey, { at: Date.now(), data: fallback.record });
        return { ok: true, record: fallback.record, cached: false, url: fallback.url, fallback: true };
      }
      const fx = fixtureFallback(digits, `brasilapi_http_${res.status}`);
      if (fx) {
        cache.set(cacheKey, { at: Date.now(), data: fx.record });
        return fx;
      }
      return {
        ok: false,
        error: `http_${res.status}`,
        record: null,
        url,
      };
    }
    const json = await res.json();
    const record = mapBrasilApiCnpj(json);
    if (!record) {
      return { ok: false, error: "unmapped_response", record: null, url };
    }
    cache.set(cacheKey, { at: Date.now(), data: record });
    return { ok: true, record, cached: false, url };
  } catch (err) {
    clearTimeout(timer);
    const fallback = await fetchCnpjWs(digits, timeoutMs);
    if (fallback.ok && fallback.record) {
      cache.set(cacheKey, { at: Date.now(), data: fallback.record });
      return { ok: true, record: fallback.record, cached: false, url: fallback.url, fallback: true };
    }
    const fx = fixtureFallback(
      digits,
      String(err?.name === "AbortError" ? "timeout" : err?.message || err).slice(0, 80)
    );
    if (fx) {
      cache.set(cacheKey, { at: Date.now(), data: fx.record });
      return fx;
    }
    return {
      ok: false,
      error: String(err?.name === "AbortError" ? "timeout" : err?.message || err).slice(
        0,
        80
      ),
      record: null,
      url,
    };
  } finally {
    clearTimeout(timer);
  }
}

/** Test helper */
export function clearCnpjClientCache() {
  cache.clear();
}
