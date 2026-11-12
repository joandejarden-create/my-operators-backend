/**
 * Old Home — How We Do It compact one-screen process (v20260802e)
 * Path-gated to /, /es, and /old-home (homepage cutover).
 * Click left nav to swap the right step in place. No scroll runway.
 * Namespace: dealality-process_*
 * 8 steps: 5 platform + 3 offline (Meet, Collaborate, Hand Off).
 * CTA: See Dealality in Action → Hero yellow + request-demo iframe.
 *
 * v20260802e: platform step titles/labels + visual mock UI stay English on /es
 *             (Deal Compare table, Opportunity Brief chrome, etc.).
 *             Marketing chrome + offline steps still localize.
 * v20260802d: locale OR iframe URL on /es.
 * v20260802c: /es translates step labels (Tracked Outcome → Resultado registrado).
 * v20260801b cold-load fixes:
 * - FOUC-critical inline CSS before mount (hide inactive steps)
 * - Inactive steps get `hidden` in initial HTML
 * - Mount once (no replaceChild remount on load/+900/+2200)
 * - Do not reassign stylesheet href on every retry
 * - Skip enter animation on first paint
 *
 * v20260801c: principle callout margin-top 10px (CSS 01c)
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    var isEs = path === "/es" || path.indexOf("/es/") === 0;
    if (path !== "/" && path !== "/old-home" && !isEs) return;
    var VERSION = "02e";
    var VERSION_NUM = 202608025;
    var CSS_HREF =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e0b3bf5f1ad337d2154ae_dealality-old-home-how-we-do-it.v20260801c.css";
    if (window.__ohHowWeDoIt >= VERSION_NUM) return;
    window.__ohHowWeDoIt = VERSION_NUM;
    var IMG_BRAND =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a679518a66ce83bcb18be55_brand-explorer.png";
    var IMG_COMPARE =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a679519982910d5c314bf1f_deal-compare.png";
    var STEPS = [
      {
        n: "01",
        mode: "platform",
        title: "Define the Opportunity",
        primary:
          "Clarify what the owner wants before any relationship starts shaping the answer.",
        support:
          "Bring the hotel, market context, objectives, constraints, and decision criteria into one brief—so later conversations start from clarity.",
        bridge: "Prepares the first offline conversations with a shared opportunity story.",
        labels: [
          "Asset Context",
          "Owner Objectives",
          "Commercial Priorities",
          "Control Preferences",
          "Timing",
          "Decision Criteria",
        ],
        visual: "define",
      },
      {
        n: "02",
        mode: "platform",
        title: "Explore Credible Paths",
        primary: "See more than the first available option.",
        support:
          "Identify the brand, operator, conversion, and partner paths worth evaluating—so the owner chooses who to meet with intention.",
        bridge: "Shortlists who is worth meeting before outreach begins.",
        labels: [
          "Brand Paths",
          "Operator Paths",
          "Conversion",
          "Operating Models",
          "Capital",
          "Alternatives",
        ],
        visual: "paths",
        img: IMG_BRAND,
        alt: "Exploring credible brand and operator paths for one hotel opportunity",
      },
      {
        n: "03",
        mode: "platform",
        title: "Prepare & Engage",
        primary: "Open the right conversations—without losing control of the story.",
        support:
          "Select participants, manage confidentiality, coordinate outreach, and capture responses so introduction calls start ready—not cold.",
        bridge: "Hands a clean brief and status into the first relationship step.",
        labels: [
          "Opportunity Brief",
          "Participants",
          "Confidentiality",
          "Outreach",
          "Questions",
          "Responses",
        ],
        visual: "engage",
      },
      {
        n: "04",
        mode: "offline",
        title: "Meet & Align",
        primary: "Put people in the room—where trust actually begins.",
        support:
          "Introductions and clarifications happen offline, between people. Dealality prepares both sides and tracks outcomes so the conversation stays part of the process—not a side channel.",
        bridge: "Met offline. Facilitated by Dealality · tracked in Dealality.",
        labels: [
          "Introduction Call",
          "Clarification Call",
          "Fit Discussion",
          "Open Questions",
          "Tracked Outcome",
        ],
        visual: "meet",
      },
      {
        n: "05",
        mode: "platform",
        title: "Compare What Matters",
        primary: "Give every proposal a shared basis—so the next conversation can go deeper.",
        support:
          "Compare economics, control, requirements, support, timing, and gaps side by side. The numbers do not replace judgment; they prepare it.",
        bridge: "Creates a shared comparison for facilitated offline diligence.",
        labels: [
          "Fees",
          "Owner Control",
          "Capital Support",
          "Requirements",
          "Timing",
          "Gaps",
        ],
        visual: "compare",
        img: IMG_COMPARE,
        alt: "Side-by-side proposal comparison across fees, control, and missing terms",
      },
      {
        n: "06",
        mode: "offline",
        title: "Collaborate & Review",
        primary:
          "Take the comparison into the real world—calls, walkthroughs, and technical review.",
        support:
          "Collaboration calls and site or tech review happen offline. Dealality facilitates scheduling and context, then tracks what was covered so judgment stays connected to the deal record.",
        bridge: "Met offline. Facilitated by Dealality · tracked in Dealality.",
        labels: [
          "Collaboration Calls",
          "Site Visit",
          "Tech Review",
          "Trade-offs",
          "Tracked Notes",
        ],
        visual: "diligence",
      },
      {
        n: "07",
        mode: "platform",
        title: "Pursue the Selected Direction",
        primary:
          "Hold finalists, materials, and feasibility in one place while conversations continue.",
        support:
          "Confirm finalists, open the Deal Room, and sync feasibility—so momentum does not disappear between facilitated offline alignment moments.",
        bridge: "Keeps the record ready for final alignment—still tracked in Dealality.",
        labels: [
          "Finalists",
          "Deal Room",
          "Feasibility",
          "Conditions",
          "Decision Record",
        ],
        visual: "pursue",
      },
      {
        n: "08",
        mode: "offline",
        title: "Align & Hand Off",
        primary:
          "Close alignment between people—then hand into legal with a clean, tracked exit.",
        support:
          "Final alignment on vision, cost, and ops stays human and offline. Dealality tracks readiness through LOI intent, then supports a clean handoff into legal drafting.",
        bridge: "Aligned offline. Tracked in Dealality through handoff.",
        labels: [
          "Final Alignment Call",
          "LOI Intent",
          "Legal Start",
          "MA / FA Drafting",
          "Tracked Handoff",
        ],
        visual: "handoff",
      },
    ];

    function t(en, es) { return isEs ? es : en; }
    /* Platform mock UI + platform step chrome stay English on /es.
       Only offline relationship steps localize (not product UI). */
    if (isEs) {
      var TITLE_ES = {
        "Meet & Align": "Reúnete Y Alinea",
        "Collaborate & Review": "Colabora Y Revisa",
        "Align & Hand Off": "Alinea Y Traspasa"
      };
      var LABEL_ES = {
        "Introduction Call": "Llamada De Introducción",
        "Clarification Call": "Llamada De Aclaración",
        "Fit Discussion": "Discusión De Fit",
        "Open Questions": "Preguntas Abiertas",
        "Tracked Outcome": "Resultado Registrado",
        "Collaboration Calls": "Llamadas De Colaboración",
        "Site Visit": "Visita Al Sitio",
        "Tech Review": "Revisión Técnica",
        "Trade-offs": "Trade-offs",
        "Tracked Notes": "Notas Registradas",
        "Final Alignment Call": "Llamada De Alineación Final",
        "LOI Intent": "Intención De LOI",
        "Legal Start": "Inicio Legal",
        "MA / FA Drafting": "Redacción Ma / Fa",
        "Tracked Handoff": "Traspaso Registrado"
      };
      for (var _si = 0; _si < STEPS.length; _si++) {
        if (STEPS[_si].mode !== "offline") continue;
        if (TITLE_ES[STEPS[_si].title]) STEPS[_si].title = TITLE_ES[STEPS[_si].title];
        if (STEPS[_si].labels && STEPS[_si].labels.length) {
          STEPS[_si].labels = STEPS[_si].labels.map(function (lab) {
            return LABEL_ES[lab] || lab;
          });
        }
      }
    }

    function injectFoucCss() {
      if (document.getElementById("oh-how-fouc")) return;
      var st = document.createElement("style");
      st.id = "oh-how-fouc";
      st.textContent =
        "#oh-how-we-do-it .dealality-process_step:not(.is-active)," +
        "#oh-how-we-do-it .dealality-process_step[hidden]{" +
        "display:none!important;visibility:hidden!important;opacity:0!important;" +
        "height:0!important;overflow:hidden!important;pointer-events:none!important}" +
        "#oh-how-we-do-it[data-oh-how-first] .dealality-process_step.is-active{" +
        "animation:none!important}";
      (document.head || document.documentElement).appendChild(st);
    }
    function ensureCss() {
      if (document.querySelector('link[data-oh-how="' + VERSION + '"]')) return;
      var existing = document.querySelector(
        'link[href*="dealality-old-home-how-we-do-it"]'
      );
      if (existing) {
        /* Keep current href if already pointing at how-we-do-it CSS — avoid reload flash. */
        if (existing.getAttribute("href") !== CSS_HREF) {
          existing.setAttribute("href", CSS_HREF);
        }
        existing.setAttribute("data-oh-how", VERSION);
        return;
      }
      var link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = CSS_HREF;
      link.setAttribute("data-oh-how", VERSION);
      (document.head || document.documentElement).appendChild(link);
    }
    function esc(s) {
      return String(s || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
    }
    function labelsHtml(labels) {
      return (
        '<ul class="dealality-process_labels">' +
        labels
          .map(function (l) {
            return "<li>" + esc(l) + "</li>";
          })
          .join("") +
        "</ul>"
      );
    }
    function visualHtml(step) {
      if (step.visual === "define") {
        return (
          '<div class="dealality-process_visual is-large" aria-label="Opportunity brief panel">' +
          '<div class="dealality-process_chrome" aria-hidden="true"><span></span><span></span><span></span><em>Opportunity Review · Brief</em></div>' +
          '<div class="dealality-process_panel">' +
          '<div class="dealality-process_panel-head">' +
          '<span class="dealality-process_chip">Opportunity Brief</span>' +
          '<span class="dealality-process_meta">Clarity before the first conversation</span>' +
          "</div>" +
          '<div class="dealality-process_rows">' +
          '<div class="dealality-process_row"><strong>Opportunity Type</strong><span>Brand + operator review</span></div>' +
          '<div class="dealality-process_row"><strong>Owner Objectives</strong><span>Stabilize NOI · Preserve control</span></div>' +
          '<div class="dealality-process_row"><strong>Constraints</strong><span>Capex limited · 18-month horizon</span></div>' +
          '<div class="dealality-process_row"><strong>Decision Criteria</strong><span>Control · fees · timeline · support</span></div>' +
          "</div></div></div>"
        );
      }
      if (step.visual === "paths") {
        return (
          '<div class="dealality-process_visual is-large" aria-label="Credible strategic paths">' +
          '<div class="dealality-process_chrome" aria-hidden="true"><span></span><span></span><span></span><em>Brand Explorer · Path Map</em></div>' +
          '<div class="dealality-process_shot">' +
          (step.img
            ? '<img src="' +
              esc(step.img) +
              '" alt="' +
              esc(step.alt) +
              '" loading="lazy" width="960" height="540">'
            : "") +
          "</div>" +
          '<div class="dealality-process_panel">' +
          '<div class="dealality-process_panel-head">' +
          '<span class="dealality-process_chip">Credible Paths</span>' +
          '<span class="dealality-process_meta">Choose who is worth meeting</span>' +
          "</div>" +
          '<div class="dealality-process_paths">' +
          '<div class="dealality-process_path"><strong>Soft Brand</strong><span>Fit rationale · Capex moderate</span></div>' +
          '<div class="dealality-process_path"><strong>Operator Partner</strong><span>Operating model lift · Control trade-offs</span></div>' +
          '<div class="dealality-process_path"><strong>Conversion</strong><span>Positioning shift · Evidence pack</span></div>' +
          "</div></div></div>"
        );
      }
      if (step.visual === "engage") {
        return (
          '<div class="dealality-process_visual is-large" aria-label="Prepare and engage workspace">' +
          '<div class="dealality-process_chrome" aria-hidden="true"><span></span><span></span><span></span><em>Controlled Outreach · Workspace</em></div>' +
          '<div class="dealality-process_panel">' +
          '<div class="dealality-process_panel-head">' +
          '<span class="dealality-process_chip">Controlled Outreach</span>' +
          '<span class="dealality-process_meta">Ready for the first meeting</span>' +
          "</div>" +
          '<div class="dealality-process_progress-bar" aria-hidden="true"><i style="width:62%"></i></div>' +
          '<div class="dealality-process_rows">' +
          '<div class="dealality-process_row"><strong>Selected Participants</strong><span>3 brands · 2 operators</span></div>' +
          '<div class="dealality-process_row"><strong>Confidentiality</strong><span>NDA active · gated materials</span></div>' +
          '<div class="dealality-process_row"><strong>Outreach Status</strong><span>2 contacted · 1 ready</span></div>' +
          '<div class="dealality-process_row"><strong>Next Step</strong><span>Introduction call · offline</span></div>' +
          "</div></div></div>"
        );
      }
      if (step.visual === "meet") {
        return (
          '<div class="dealality-process_visual is-large is-offline-visual" aria-label="Offline relationship moments">' +
          '<div class="dealality-process_chrome" aria-hidden="true"><span></span><span></span><span></span><em>Offline · Relationship Moments</em></div>' +
          '<div class="dealality-process_panel">' +
          '<div class="dealality-process_panel-head">' +
          '<span class="dealality-process_chip">Meet &amp; Align</span>' +
          '<span class="dealality-process_meta">Where trust begins</span>' +
          "</div>" +
          '<div class="dealality-process_moments">' +
          '<div class="dealality-process_moment"><strong>Introduction Call</strong><p>Owner and brand/operator meet. Fit, intent, and chemistry—not a form submission.</p></div>' +
          '<div class="dealality-process_moment"><strong>Clarification Call</strong><p>Questions get answered live before commercial terms harden.</p></div>' +
          "</div>" +
          '<div class="dealality-process_rows">' +
          '<div class="dealality-process_row is-human"><strong>People meet</strong><span>Calls · video · in person</span></div>' +
          '<div class="dealality-process_row"><strong>Dealality role</strong><span>Prepared · facilitated · outcome tracked</span></div>' +
          "</div></div></div>"
        );
      }
      if (step.visual === "compare") {
        return (
          '<div class="dealality-process_visual is-compare is-large" aria-label="Proposal comparison">' +
          '<div class="dealality-process_chrome" aria-hidden="true"><span></span><span></span><span></span><em>Shared Comparison · Strongest View</em></div>' +
          '<div class="dealality-process_shot is-compare-shot">' +
          (step.img
            ? '<img src="' +
              esc(step.img) +
              '" alt="' +
              esc(step.alt) +
              '" loading="lazy" width="960" height="540">'
            : "") +
          "</div>" +
          '<div class="dealality-process_panel">' +
          '<div class="dealality-process_panel-head">' +
          '<span class="dealality-process_chip">Shared Comparison</span>' +
          '<span class="dealality-process_meta">Prepares the next offline diligence</span>' +
          "</div>" +
          '<div class="dealality-process_compare" role="table" aria-label="Proposal differences">' +
          '<div class="is-head" role="columnheader">Category</div><div class="is-head" role="columnheader">Path A</div><div class="is-head" role="columnheader">Path B</div><div class="is-head" role="columnheader">Path C</div>' +
          "<div>Fees</div><div>4.5% + 2%</div><div>5.0% + 1%</div><div>3.8% + 3%</div>" +
          "<div>Owner Control</div><div>High</div><div>Medium</div><div>Shared</div>" +
          '<div>Missing Terms</div><div class="is-gap">Capex TBD</div><div>—</div><div class="is-gap">Exit TBD</div>' +
          "</div></div></div>"
        );
      }
      if (step.visual === "diligence") {
        return (
          '<div class="dealality-process_visual is-large is-offline-visual" aria-label="Offline human diligence">' +
          '<div class="dealality-process_chrome" aria-hidden="true"><span></span><span></span><span></span><em>Offline · Human Diligence</em></div>' +
          '<div class="dealality-process_panel">' +
          '<div class="dealality-process_panel-head">' +
          '<span class="dealality-process_chip">Collaborate &amp; Review</span>' +
          '<span class="dealality-process_meta">Where judgment happens</span>' +
          "</div>" +
          '<div class="dealality-process_moments">' +
          '<div class="dealality-process_moment"><strong>Collaboration Calls</strong><p>Walk economics, support models, and strategic fit together—live.</p></div>' +
          '<div class="dealality-process_moment"><strong>Site / Tech Review</strong><p>Property walkthrough or remote technical review. Human diligence.</p></div>' +
          "</div>" +
          '<div class="dealality-process_rows">' +
          '<div class="dealality-process_row is-human"><strong>People meet</strong><span>Calls · site visit · tech review</span></div>' +
          '<div class="dealality-process_row"><strong>Dealality role</strong><span>Context ready · coverage tracked</span></div>' +
          "</div></div></div>"
        );
      }
      if (step.visual === "pursue") {
        return (
          '<div class="dealality-process_visual is-large" aria-label="Selected direction workspace">' +
          '<div class="dealality-process_chrome" aria-hidden="true"><span></span><span></span><span></span><em>Preferred Direction · Decision Record</em></div>' +
          '<div class="dealality-process_panel">' +
          '<div class="dealality-process_panel-head">' +
          '<span class="dealality-process_chip">Preferred Direction</span>' +
          '<span class="dealality-process_meta">Momentum between conversations</span>' +
          "</div>" +
          '<div class="dealality-process_rows">' +
          '<div class="dealality-process_row"><strong>Preferred Direction</strong><span>Soft brand + operator support</span></div>' +
          '<div class="dealality-process_row"><strong>Deal Room</strong><span>Open for finalists</span></div>' +
          '<div class="dealality-process_row"><strong>Feasibility Sync</strong><span>Docs + checklist tracked</span></div>' +
          '<div class="dealality-process_row"><strong>Next Step</strong><span>Final alignment call · offline</span></div>' +
          "</div></div></div>"
        );
      }
      return (
        '<div class="dealality-process_visual is-large is-offline-visual" aria-label="Offline align and hand off">' +
        '<div class="dealality-process_chrome" aria-hidden="true"><span></span><span></span><span></span><em>Offline · Align &amp; Platform Exit</em></div>' +
        '<div class="dealality-process_panel">' +
        '<div class="dealality-process_panel-head">' +
        '<span class="dealality-process_chip">Align &amp; Hand Off</span>' +
        '<span class="dealality-process_meta">Where the deal closes</span>' +
        "</div>" +
        '<div class="dealality-process_moments">' +
        '<div class="dealality-process_moment"><strong>Final Alignment Call</strong><p>Vision, cost, and ops confirmed between people—last strategic discussion.</p></div>' +
        '<div class="dealality-process_moment"><strong>Legal Start</strong><p>MA / FA drafting and signature move offline. Clean platform exit.</p></div>' +
        "</div>" +
        '<div class="dealality-process_rows">' +
        '<div class="dealality-process_row is-human"><strong>People meet</strong><span>Alignment · counsel · signing path</span></div>' +
        '<div class="dealality-process_row"><strong>Dealality role</strong><span>LOI readiness tracked · clean handoff</span></div>' +
        "</div></div></div>"
      );
    }
    function stepHtml(step, i) {
      var offline = step.mode === "offline";
      return (
        '<article class="dealality-process_step' +
        (offline ? " is-offline-step" : "") +
        (i === 0 ? " is-active" : "") +
        '" id="dealality-process-step-' +
        (i + 1) +
        '" data-dealality-process-step="' +
        i +
        '"' +
        (i === 0 ? "" : " hidden") +
        ">" +
        '<p class="dealality-process_kicker">Step ' +
        esc(step.n) +
        "</p>" +
        '<h3 class="dealality-process_step-title">' +
        esc(step.title) +
        "</h3>" +
        '<div class="dealality-process_mode-pill ' +
        (offline ? "is-offline" : "is-platform") +
        '"><i aria-hidden="true"></i>' +
        (offline
          ? t("Offline · Facilitated by Dealality", "Offline · Facilitado Por Dealality")
          : t("In Platform · Structure", "En Plataforma · Estructura")) +
        "</div>" +
        '<p class="dealality-process_primary">' +
        esc(step.primary) +
        "</p>" +
        '<p class="dealality-process_support">' +
        esc(step.support) +
        "</p>" +
        '<p class="dealality-process_bridge"><strong>' +
        (offline
          ? t("Where people meet:", "Dónde se reúnen las personas:")
          : t("Leads into:", "Conduce a:")) +
        "</strong> " +
        esc(step.bridge) +
        "</p>" +
        visualHtml(step) +
        labelsHtml(step.labels) +
        "</article>"
      );
    }
    function buildSection() {
      var nav = STEPS.map(function (step, i) {
        var offline = step.mode === "offline";
        return (
          '<button type="button" class="dealality-process_nav-item' +
          (i === 0 ? " is-active" : "") +
          (offline ? " is-offline" : "") +
          '" data-dealality-process-nav="' +
          i +
          '" aria-controls="dealality-process-step-' +
          (i + 1) +
          '" aria-current="' +
          (i === 0 ? "step" : "false") +
          '" aria-label="' +
          esc(step.n) +
          " " +
          esc(step.title) +
          ", " +
          (offline ? "Offline" : "Platform") +
          '">' +
          '<span class="dealality-process_nav-num" aria-hidden="true">' +
          esc(step.n) +
          "</span>" +
          '<span class="dealality-process_nav-label">' +
          esc(step.title) +
          "</span>" +
          "</button>"
        );
      }).join("");
      return (
        '<section id="oh-how-we-do-it" class="dealality-process_section oh-how" aria-labelledby="dealality-process-h2" style="--oh-how-steps:' +
        STEPS.length +
        '" data-oh-how="' +
        VERSION +
        '" data-oh-how-first="1">' +
        '<div class="dealality-process_glow" aria-hidden="true"></div>' +
        '<div class="dealality-process_inner">' +
        '<header class="dealality-process_intro">' +
        '<div class="dealality-process_eyebrow">' +
        '<span class="dealality-process_eyebrow-pill">' + t("How Dealality Works", "Cómo Funciona Dealality") + "</span>" +
        '<span class="dealality-process_eyebrow-right">' + t("A Connected Process From Opportunity to Agreement.", "Un Proceso Conectado De La Oportunidad Al Acuerdo.") + "</span>" +
        "</div>" +
        '<h2 class="dealality-process_h2" id="dealality-process-h2">' + t("Turn One Hotel Opportunity Into a Structured Decision Process.", "Convierte Una Oportunidad Hotelera En Un Proceso De Decisión Estructurado.") + "</h2>" +
        '<p class="dealality-process_lead">' + t("Dealality organizes the opportunity on platform—and <em>facilitates the offline conversations that actually close deals</em>. People meet in person or on a call; Dealality keeps those moments prepared, tracked, and connected to the process.", "Dealality organiza la oportunidad en la plataforma—y <em>facilita las conversaciones offline que realmente cierran deals</em>. Las personas se reúnen en persona o por llamada; Dealality mantiene esos momentos preparados, rastreados y conectados al proceso.") + "</p>" +
        '<div class="dealality-process_principle">' +
        '<span class="dealality-process_principle-mark" aria-hidden="true"></span>' +
        t("<p><strong>People meet offline. Dealality stays with them.</strong> Introduction, diligence, and final alignment happen between people—prepared, facilitated, and tracked in the platform so nothing falls out of the process.</p>", "<p><strong>Las personas se reúnen offline. Dealality se queda con ellas.</strong> La introducción, la diligencia y la alineación final ocurren entre personas—preparadas, facilitadas y rastreadas en la plataforma para que nada salga del proceso.</p>") +
        "</div>" +
        "</header>" +
        '<div class="dealality-process_stage-track" id="dealality-process-runway">' +
        '<div class="dealality-process_stage-pin">' +
        '<div class="dealality-process_layout">' +
        '<div class="dealality-process_nav-col">' +
        '<nav class="dealality-process_nav" aria-label="' +
        t("Dealality process steps", "Pasos del proceso Dealality") +
        '">' +
        '<div class="dealality-process_nav-legend" aria-label="Step type legend">' +
        '<span class="dealality-process_nav-legend-item"><span class="dealality-process_nav-legend-swatch is-platform" aria-hidden="true"></span>' + t("Platform", "Plataforma") + "</span>" +
        '<span class="dealality-process_nav-legend-item"><span class="dealality-process_nav-legend-swatch is-offline" aria-hidden="true"></span>' + t("Offline", "Offline") + "</span>" +
        "</div>" +
        '<div class="dealality-process_nav-rail">' +
        '<div class="dealality-process_progress" aria-hidden="true"><span id="dealality-process-progress"></span></div>' +
        nav +
        "</div></nav></div>" +
        '<div class="dealality-process_content" id="dealality-process-content">' +
        STEPS.map(stepHtml).join("") +
        "</div>" +
        "</div></div></div>" +
        '<div class="dealality-process_cta">' +
        "<div>" +
        t("<h3>One Opportunity. One Connected Process.</h3>", "<h3>Una Oportunidad. Un Proceso Conectado.</h3>") +
        t("<p>From the first question to the selected direction, Dealality keeps the opportunity, participants, proposals, and decision criteria connected.</p>", "<p>Desde la primera pregunta hasta la dirección seleccionada, Dealality mantiene conectados la oportunidad, los participantes, las propuestas y los criterios de decisión.</p>") +
        "</div>" +
        '<div class="dealality-process_cta-row">' +
        '<button type="button" class="dealality-process_btn dealality-process_btn-primary" data-dealality-process-cta="explore">' + t("Explore Your Opportunity", "Explora Tu Oportunidad") + "</button>" +
        '<button type="button" class="dealality-process_btn dealality-process_btn-secondary" data-dealality-process-cta="demo">' + t("See Dealality in Action", "Ve Dealality En Acción") + "</button>" +
        "</div></div>" +
        '<p class="dealality-process_sr" id="dealality-process-live" aria-live="polite"></p>' +
        "</div></section>"
      );
    }
    function track(eventName, payload) {
      try {
        if (window.dataLayer && Array.isArray(window.dataLayer)) {
          window.dataLayer.push(
            Object.assign({ event: eventName }, payload || {})
          );
        } else if (typeof window.gtag === "function") {
          window.gtag("event", eventName, payload || {});
        }
      } catch (_e) {}
    }
    function mount() {
      var existing = document.getElementById("oh-how-we-do-it");
      /* Already on current version — never remount (kills refresh flash). */
      if (existing && existing.getAttribute("data-oh-how") === VERSION) {
        return true;
      }
      var html = buildSection();
      if (existing) {
        var wrap = document.createElement("div");
        wrap.innerHTML = html;
        existing.parentNode.replaceChild(wrap.firstChild, existing);
        return true;
      }
      var anchor =
        document.getElementById("platform-features") ||
        document.getElementById("features") ||
        document.getElementById("modules");
      if (!anchor || !anchor.parentNode) return false;
      var holder = document.createElement("div");
      holder.innerHTML = html;
      if (anchor.id === "features") {
        anchor.parentNode.insertBefore(holder.firstChild, anchor.nextSibling);
      } else {
        anchor.parentNode.insertBefore(holder.firstChild, anchor);
      }
      return true;
    }
    function bind(section) {
      if (!section || section.getAttribute("data-dealality-process-bound") === "1")
        return;
      section.setAttribute("data-dealality-process-bound", "1");
      var navItems = Array.prototype.slice.call(
        section.querySelectorAll("[data-dealality-process-nav]")
      );
      var steps = Array.prototype.slice.call(
        section.querySelectorAll("[data-dealality-process-step]")
      );
      var progress = section.querySelector("#dealality-process-progress");
      var live = section.querySelector("#dealality-process-live");
      var active = 0;
      var viewed = {};
      function setActive(index, fromUser) {
        if (index < 0 || index >= STEPS.length) return;
        active = index;
        navItems.forEach(function (btn, i) {
          var on = i === active;
          var offline = STEPS[i].mode === "offline";
          btn.classList.toggle("is-active", on);
          btn.classList.toggle("is-done", i < active);
          btn.classList.toggle("is-offline", offline);
          btn.setAttribute("aria-current", on ? "step" : "false");
        });
        steps.forEach(function (el, i) {
          var on = i === active;
          el.classList.toggle("is-active", on);
          if (on) el.removeAttribute("hidden");
          else el.setAttribute("hidden", "");
        });
        if (progress) {
          var pct =
            STEPS.length <= 1
              ? 100
              : Math.round(((active + 1) / STEPS.length) * 100);
          progress.style.height = pct + "%";
        }
        if (live) {
          live.textContent =
            "Step " +
            STEPS[active].n +
            ": " +
            STEPS[active].title +
            " (" +
            (STEPS[active].mode === "offline" ? "Offline" : "Platform") +
            ")";
        }
        if (!viewed[active]) {
          viewed[active] = 1;
          track("process_step_view", {
            step: STEPS[active].n,
            title: STEPS[active].title,
            mode: STEPS[active].mode,
          });
        }
        if (fromUser) {
          track("process_step_click", { step: STEPS[active].n, title: STEPS[active].title, mode: STEPS[active].mode, });
        }
      }
      navItems.forEach(function (btn) {
        btn.addEventListener("click", function () {
          var idx = parseInt(btn.getAttribute("data-dealality-process-nav"), 10);
          if (!isNaN(idx)) setActive(idx, true);
        });
      });
      var explore = section.querySelector('[data-dealality-process-cta="explore"]');
      if (explore) {
        explore.addEventListener("click", function () {
          track("process_cta_click", { cta: "explore" });
          var url = isEs ? "/es/opportunity-review" : "/opportunity-review";
          var label = t("Explore Your Opportunity", "Explora Tu Oportunidad");
          if (typeof window.ohOpenOpportunityReview === "function") {
            window.ohOpenOpportunityReview(url, label);
            return;
          }
          var orBtn =
            document.querySelector("#pricing-owners-cta") ||
            document.querySelector("#fsw-btn") ||
            document.querySelector("[data-oh-or-open]");
          if (orBtn) {
            orBtn.click();
            return;
          }
          window.location.href = url;
        });
      }
      var demoCta = section.querySelector('[data-dealality-process-cta="demo"]');
      if (demoCta) {
        demoCta.addEventListener("click", function () {
          track("process_cta_click", { cta: "demo" });
          if (typeof window.ohOpenRequestDemo === "function") {
            window.ohOpenRequestDemo();
            return;
          }
          var demoBtn =
            document.querySelector("[data-dealality-demo-open]") ||
            document.querySelector('a[href="#request-demo"]') ||
            document.querySelector("#fsw-demo-link");
          if (demoBtn) demoBtn.click();
        });
      }
      setActive(0, false);
      /* Drop first-paint gate after layout so later nav clicks still animate. */
      window.requestAnimationFrame(function () {
        window.requestAnimationFrame(function () {
          section.removeAttribute("data-oh-how-first");
        });
      });
      track("process_section_view", { section: "how-we-do-it" });
    }
    function run() {
      injectFoucCss();
      ensureCss();
      if (!mount()) return false;
      bind(document.getElementById("oh-how-we-do-it"));
      return true;
    }
    /** Retry only until first successful mount; never remount a live section. */
    function scheduleMountRetries() {
      var delays = [0, 120, 400, 900, 2200];
      delays.forEach(function (ms) {
        window.setTimeout(function () {
          var section = document.getElementById("oh-how-we-do-it");
          if (section && section.getAttribute("data-oh-how") === VERSION) {
            bind(section);
            return;
          }
          run();
        }, ms);
      });
    }
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", function () {
        run();
        scheduleMountRetries();
      });
    } else {
      run();
      scheduleMountRetries();
    }
    window.addEventListener("load", function () {
      var section = document.getElementById("oh-how-we-do-it");
      if (section && section.getAttribute("data-oh-how") === VERSION) {
        bind(section);
        return;
      }
      run();
    });
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-how-we-do-it]", err);
    }
  }
})();