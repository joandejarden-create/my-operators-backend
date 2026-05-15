/**
 * Full FDD extraction with async job + SSE + polling fallback (Franchise Intelligence admin).
 * Same-origin only; no secrets.
 */

function formatElapsed(ms) {
  const s = Math.floor(ms / 1000);
  const m = Math.floor(s / 60);
  const r = s % 60;
  if (m <= 0) return `${r}s`;
  return `${m}m ${String(r).padStart(2, "0")}s`;
}

function absUrl(pathOrUrl) {
  if (!pathOrUrl) return "";
  try {
    return new URL(pathOrUrl, window.location.href).href;
  } catch {
    return pathOrUrl;
  }
}

function mountOverlay() {
  const root = document.createElement("div");
  root.className = "fdd-extract-loading";
  root.setAttribute("role", "status");
  root.setAttribute("aria-live", "polite");
  root.innerHTML = `
    <div class="fdd-extract-loading__panel">
      <div class="fdd-extract-loading__content">
        <div class="fdd-extract-loading__wave-box" aria-hidden="true">
          <div class="fdd-extract-loading__wave fdd-extract-loading__wave--1"></div>
          <div class="fdd-extract-loading__wave fdd-extract-loading__wave--2"></div>
          <div class="fdd-extract-loading__wave fdd-extract-loading__wave--3"></div>
          <div class="fdd-extract-loading__particles">
            <div class="fdd-extract-loading__particle"></div>
            <div class="fdd-extract-loading__particle"></div>
            <div class="fdd-extract-loading__particle"></div>
            <div class="fdd-extract-loading__particle"></div>
          </div>
        </div>
        <div class="fdd-extract-loading__text">
          <p class="fdd-extract-loading__title">Franchise Intelligence extraction</p>
          <p class="fdd-extract-loading__phase" data-fdd-phase>Starting…</p>
          <p class="fdd-extract-loading__elapsed" data-fdd-elapsed>Running… 0s</p>
          <p class="fdd-extract-loading__hint" data-fdd-hint>Live phase updates when available; this page also polls job status.</p>
        </div>
      </div>
      <div class="fdd-extract-loading__bar" aria-hidden="true"><span></span></div>
    </div>
  `;
  document.body.appendChild(root);
  const phaseEl = root.querySelector("[data-fdd-phase]");
  const elapsedEl = root.querySelector("[data-fdd-elapsed]");
  const hintEl = root.querySelector("[data-fdd-hint]");
  const t0 = Date.now();
  const tick = setInterval(() => {
    elapsedEl.textContent = `Running… ${formatElapsed(Date.now() - t0)}`;
  }, 500);
  return {
    setPhase(text) {
      if (phaseEl) phaseEl.textContent = text || "…";
    },
    setHint(text) {
      if (hintEl) hintEl.textContent = text || "";
    },
    remove() {
      clearInterval(tick);
      root.remove();
    },
  };
}

/**
 * Start full extraction with progress UI: prefers async job (202) via POST …/extract-full + `{ async: true }`
 * (same URL as the legacy sync extractor so proxies/old deploys still hit a real route). Falls back to treating a
 * synchronous 200 JSON body as a completed job when the server ignores `async`.
 * @param {{ apiBase?: string, documentId: string }} opts
 * @returns {Promise<object>} Latest job snapshot (includes `result` when status is `done`)
 */
export async function runFddFullExtractWithMonitoring(opts) {
  const apiBase = (opts && opts.apiBase) || "/api/fdd-intelligence";
  const documentId = opts && opts.documentId;
  if (!documentId) throw new Error("documentId required");

  const overlay = mountOverlay();
  overlay.setPhase("Starting extraction…");
  overlay.setHint("Requesting async job on the server (or legacy sync if unsupported)…");

  const extractFullUrl = `${apiBase}/documents/${encodeURIComponent(documentId)}/extract-full`;
  let startRes;
  let startJson;
  try {
    startRes = await fetch(extractFullUrl, {
      method: "POST",
      headers: { Accept: "application/json", "Content-Type": "application/json" },
      body: JSON.stringify({ async: true }),
    });
    startJson = await startRes.json().catch(() => ({}));
  } catch (e) {
    overlay.remove();
    throw e;
  }

  /** Old servers: same URL runs sync and returns the full extraction JSON (200). */
  if (startRes.status === 200 && startJson && (startJson.fees !== undefined || startJson.terms !== undefined) && !startJson.jobId) {
    overlay.remove();
    if (!startJson.ok && !startJson.partialSuccess) {
      throw new Error(startJson.error || startJson.warnings?.[0] || "Full extraction failed");
    }
    return {
      jobId: null,
      status: "done",
      result: startJson,
      fullExtractionRunId: startJson.fullExtractionRunId,
    };
  }

  if (!(startRes.status === 202 && startJson.jobId)) {
    overlay.remove();
    if (startRes.status === 404 && startJson && startJson.accepted === false) {
      throw new Error(startJson.error || "Document not found");
    }
    throw new Error(startJson.error || startRes.statusText || "Could not start extraction");
  }

  overlay.setPhase(startJson.message || "Job queued…");
  overlay.setHint("Live phase updates when available; this page also polls job status.");

  const pollUrl = absUrl(startJson.pollUrl || `${apiBase}/extract-jobs/${encodeURIComponent(startJson.jobId)}`);
  const eventsUrl = absUrl(startJson.eventsUrl || `${apiBase}/extract-jobs/${encodeURIComponent(startJson.jobId)}/events`);

  let settled = false;
  let es = null;
  let pollTimer = null;

  const cleanup = () => {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
    if (es) {
      try {
        es.close();
      } catch (_) {
        /* ignore */
      }
      es = null;
    }
    overlay.remove();
  };

  const applyJob = (job) => {
    if (!job) return;
    const line = job.message || job.stage || "…";
    overlay.setPhase(line);
  };

  async function fetchJob() {
    const r = await fetch(pollUrl, { headers: { Accept: "application/json" } });
    const j = await r.json().catch(() => ({}));
    return j && j.job ? j.job : null;
  }

  return new Promise((resolve, reject) => {
    const settle = (job, err) => {
      if (settled) return;
      settled = true;
      cleanup();
      if (err) reject(err);
      else resolve(job);
    };

    try {
      es = new EventSource(eventsUrl);
      es.addEventListener("progress", (e) => {
        try {
          applyJob(JSON.parse(e.data));
        } catch (_) {
          /* ignore */
        }
      });
      es.addEventListener("complete", () => {
        fetchJob()
          .then((job) => settle(job, null))
          .catch((e) => settle(null, e));
      });
      es.addEventListener("joberror", (e) => {
        let msg = "Extraction failed";
        try {
          const d = JSON.parse(e.data);
          if (d && d.message) msg = d.message;
        } catch (_) {
          /* keep default */
        }
        settle(null, new Error(msg));
      });
      es.onerror = () => {
        overlay.setHint("Live stream interrupted — polling continues every few seconds.");
      };
    } catch (e) {
      overlay.setHint("Live stream unavailable — using polling only.");
    }

    pollTimer = setInterval(() => {
      fetchJob()
        .then((job) => {
          if (!job) return;
          applyJob(job);
          if (job.status === "done") settle(job, null);
          else if (job.status === "failed") settle(null, new Error(job.error || "Extraction failed"));
        })
        .catch(() => {
          /* ignore transient network errors */
        });
    }, 4000);

    fetchJob()
      .then((job) => {
        if (job) applyJob(job);
        if (job && job.status === "done") settle(job, null);
        if (job && job.status === "failed") settle(null, new Error(job.error || "Extraction failed"));
      })
      .catch(() => {});
  });
}
