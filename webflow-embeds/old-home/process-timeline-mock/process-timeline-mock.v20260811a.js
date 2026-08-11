/**
 * Staging review — How Dealality Works timeline mock (v20260811a)
 * Dual-track infographic: Platform vs Offline steps.
 * Does not replace the live #oh-how-we-do-it section.
 */
(function () {
  "use strict";
  var ROOT_ID = "oh-process-timeline-mock";
  if (document.getElementById(ROOT_ID) && document.getElementById(ROOT_ID).getAttribute("data-ptm-ready") === "1") {
    return;
  }

  var STEPS = [
    {
      n: "01", mode: "platform", title: "Define the Opportunity",
      short: "Capture scope, goals, constraints, and decision criteria in one brief.",
      trigger: "Shared opportunity brief ready for outreach."
    },
    {
      n: "02", mode: "platform", title: "See the Real Options",
      short: "Score brand, operator, conversion, and partner paths worth evaluating.",
      trigger: "Credible paths shortlisted before anyone is contacted."
    },
    {
      n: "03", mode: "platform", title: "Prepare and Engage",
      short: "Select participants, manage confidentiality, and coordinate outreach.",
      trigger: "Brands and operators notified; intro calls scheduled."
    },
    {
      n: "04", mode: "offline", title: "Meet and Align",
      short: "Introduction and clarification calls happen between people—not in email threads.",
      trigger: "Outcomes tracked back into Dealality."
    },
    {
      n: "05", mode: "platform", title: "Compare What Matters",
      short: "Side-by-side view of fees, control, support, timing, and gaps.",
      trigger: "Shared comparison ready for diligence conversations."
    },
    {
      n: "06", mode: "offline", title: "Collaborate and Review",
      short: "Collaboration calls, site visits, and technical review offline.",
      trigger: "Review notes and trade-offs captured in the deal record."
    },
    {
      n: "07", mode: "platform", title: "Choose a Path and Move Forward",
      short: "Confirm finalists, open the Deal Room, and sync feasibility.",
      trigger: "Deal Room active; feasibility checklist underway."
    },
    {
      n: "08", mode: "offline", title: "Align and Hand Off",
      short: "Final alignment on vision, cost, and ops—then into legal drafting.",
      trigger: "LOI intent tracked; legal handoff begins."
    }
  ];

  var CSS = [
    "#oh-process-timeline-mock{--ptm-font:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;--ptm-display:Georgia,\"Times New Roman\",serif;--ptm-ease:cubic-bezier(.22,1,.36,1);box-sizing:border-box;font-family:var(--ptm-font);color:#12131a;background:linear-gradient(180deg,#f3f1ec 0%,#ebe7df 100%);padding:72px 0 80px;border-top:1px solid rgba(0,0,0,.06)}",
    "#oh-process-timeline-mock *,#oh-process-timeline-mock *::before,#oh-process-timeline-mock *::after{box-sizing:border-box}",
    "#oh-process-timeline-mock .ptm-wrap{width:min(1180px,calc(100% - 48px));margin:0 auto}",
    "#oh-process-timeline-mock .ptm-badge{display:inline-flex;align-items:center;gap:8px;margin:0 0 18px;padding:6px 12px;border-radius:999px;background:rgba(184,74,50,.1);border:1px solid rgba(184,74,50,.22);color:#8a3a28;font-size:11px;font-weight:800;letter-spacing:.08em;text-transform:uppercase}",
    "#oh-process-timeline-mock .ptm-eyebrow{display:flex;flex-wrap:wrap;align-items:baseline;gap:10px 16px;margin:0 0 10px}",
    "#oh-process-timeline-mock .ptm-eyebrow-pill{display:inline-flex;padding:6px 12px;border-radius:999px;background:#12131a;color:#fff;font-size:12px;font-weight:750;letter-spacing:.02em}",
    "#oh-process-timeline-mock .ptm-eyebrow-right{font-size:14px;font-weight:650;color:#5c6372}",
    "#oh-process-timeline-mock .ptm-title{font-family:var(--ptm-display);font-size:clamp(1.75rem,3vw,2.35rem);font-weight:400;letter-spacing:-.02em;line-height:1.1;margin:0 0 10px}",
    "#oh-process-timeline-mock .ptm-title em{font-style:italic;color:#b84a32}",
    "#oh-process-timeline-mock .ptm-lead{max-width:54ch;margin:0 0 22px;font-size:15px;line-height:1.55;color:#5c6372}",
    "#oh-process-timeline-mock .ptm-legend{display:flex;flex-wrap:wrap;gap:12px 28px;margin:0 0 22px;font-size:12.5px;font-weight:600;color:#5c6372}",
    "#oh-process-timeline-mock .ptm-legend span{display:inline-flex;align-items:center;gap:8px}",
    "#oh-process-timeline-mock .ptm-legend span::before{content:\"\";width:11px;height:11px;border-radius:50%;flex-shrink:0}",
    "#oh-process-timeline-mock .ptm-legend .lg-offline::before{background:linear-gradient(135deg,#e8c06a,#c8942e);box-shadow:0 0 0 2px rgba(201,148,46,.2)}",
    "#oh-process-timeline-mock .ptm-legend .lg-platform::before{background:linear-gradient(135deg,#8b90ff,#5c62e8);box-shadow:0 0 0 2px rgba(108,114,255,.2)}",
    "#oh-process-timeline-mock .ptm-scroll{overflow-x:auto;margin:0 -24px;padding:4px 24px 12px;-webkit-overflow-scrolling:touch}",
    "#oh-process-timeline-mock .ptm-chart{position:relative;min-width:1180px;border-radius:16px;overflow:hidden;background:#faf8f4;box-shadow:0 1px 0 rgba(255,255,255,.8) inset,0 24px 64px rgba(18,19,26,.12),0 4px 16px rgba(18,19,26,.06);border:1px solid rgba(0,0,0,.07)}",
    "#oh-process-timeline-mock .ptm-grid{display:grid;grid-template-columns:repeat(8,minmax(132px,1fr));grid-template-rows:auto 148px 228px auto}",
    "#oh-process-timeline-mock .ptm-grid-head{grid-column:1/-1;grid-row:1;padding:14px 16px 6px;background:#faf8f4}",
    "#oh-process-timeline-mock .ptm-grid-head span{font-size:9.5px;font-weight:800;letter-spacing:.11em;text-transform:uppercase;color:#9a7b38}",
    "#oh-process-timeline-mock .ptm-col-offline{grid-row:2;padding:0 10px;min-height:132px;display:flex;align-items:flex-end;justify-content:center;position:relative}",
    "#oh-process-timeline-mock .ptm-band-label--platform{position:absolute;top:10px;left:0;right:0;z-index:3;text-align:center;font-size:9.5px;font-weight:800;letter-spacing:.11em;text-transform:uppercase;color:rgba(255,255,255,.48);pointer-events:none}",
    "#oh-process-timeline-mock .ptm-col-platform-wrap{grid-column:1/-1;grid-row:3;display:grid;grid-template-columns:subgrid;position:relative;min-height:220px;background:linear-gradient(180deg,rgba(6,10,24,.55) 0%,rgba(8,15,37,.82) 45%,rgba(8,15,37,.92) 100%),url(https://images.unsplash.com/photo-1506905925346-21bda4d32df4?auto=format&fit=crop&w=1800&q=70) center 40%/cover;border-top:1px solid rgba(255,255,255,.1);border-bottom:1px solid rgba(255,255,255,.08)}",
    "#oh-process-timeline-mock .ptm-col-platform-wrap::before{content:\"\";position:absolute;inset:0;background:radial-gradient(ellipse 80% 60% at 50% 100%,rgba(108,114,255,.12),transparent 60%);pointer-events:none}",
    "#oh-process-timeline-mock .ptm-col-platform{padding:0 10px;display:flex;align-items:center;justify-content:center;position:relative;z-index:2;min-height:220px}",
    "#oh-process-timeline-mock .ptm-path-layer{position:absolute;inset:0;z-index:1;pointer-events:none}",
    "#oh-process-timeline-mock .ptm-path-layer path.ptm-rail{fill:none;stroke:url(#ptmGoldGrad);stroke-width:5;stroke-linecap:round;stroke-linejoin:round;filter:drop-shadow(0 1px 2px rgba(0,0,0,.35))}",
    "#oh-process-timeline-mock .ptm-path-layer circle.ptm-node{fill:#fff;stroke:#d4a853;stroke-width:4;filter:drop-shadow(0 2px 6px rgba(0,0,0,.35))}",
    "#oh-process-timeline-mock .ptm-path-layer circle.ptm-node-offline{fill:#faf8f4;stroke:#d4a853;stroke-width:3}",
    "#oh-process-timeline-mock .ptm-path-layer text.ptm-node-label{fill:#5a5fe6;font-size:11px;font-weight:800;font-family:var(--ptm-font);text-anchor:middle;dominant-baseline:central}",
    "#oh-process-timeline-mock .ptm-offline-card{position:relative;width:100%;max-width:148px;padding:14px 12px 13px;border-radius:12px;background:linear-gradient(165deg,#fffdf8 0%,#f5ead6 100%);border:1px solid rgba(201,148,46,.28);box-shadow:0 10px 24px rgba(138,90,24,.1),0 1px 0 rgba(255,255,255,.9) inset;transition:transform .25s var(--ptm-ease),box-shadow .25s var(--ptm-ease);animation:ptmCardIn .55s var(--ptm-ease) both;animation-delay:calc(var(--ptm-i,0)*.06s)}",
    "#oh-process-timeline-mock .ptm-offline-card:hover{transform:translateY(-3px);box-shadow:0 16px 32px rgba(138,90,24,.14),0 1px 0 rgba(255,255,255,.9) inset}",
    "#oh-process-timeline-mock .ptm-offline-card::after{content:\"\";position:absolute;left:50%;bottom:-28px;width:2px;height:28px;transform:translateX(-50%);background:linear-gradient(180deg,#d4a853,rgba(212,168,83,.15));border-radius:2px}",
    "#oh-process-timeline-mock .ptm-offline-card .ptm-num{color:#9a7b38;font-size:10px;letter-spacing:.08em;text-transform:uppercase;margin-bottom:6px}",
    "#oh-process-timeline-mock .ptm-offline-card h4{color:#2e2618;font-size:13px;font-weight:800;line-height:1.22;letter-spacing:-.02em;margin:0 0 7px}",
    "#oh-process-timeline-mock .ptm-offline-card p{color:#6b5d45;font-size:11px;line-height:1.5;margin:0}",
    "#oh-process-timeline-mock .ptm-platform-card{position:relative;width:100%;max-width:148px;margin-top:-36px;padding:14px 12px 13px;border-radius:12px;background:rgba(12,18,42,.72);border:1px solid rgba(255,255,255,.16);backdrop-filter:blur(8px);box-shadow:0 12px 32px rgba(0,0,0,.35),0 1px 0 rgba(255,255,255,.08) inset;transition:transform .25s var(--ptm-ease),border-color .25s var(--ptm-ease),box-shadow .25s var(--ptm-ease);animation:ptmCardIn .55s var(--ptm-ease) both;animation-delay:calc(var(--ptm-i,0)*.06s)}",
    "#oh-process-timeline-mock .ptm-platform-card:hover{transform:translateY(-4px);border-color:rgba(168,171,255,.45);box-shadow:0 18px 40px rgba(0,0,0,.42),0 0 0 1px rgba(108,114,255,.2),0 1px 0 rgba(255,255,255,.1) inset}",
    "#oh-process-timeline-mock .ptm-platform-card .ptm-num{color:#a8abff;font-size:10px;letter-spacing:.08em;text-transform:uppercase;margin-bottom:6px}",
    "#oh-process-timeline-mock .ptm-platform-card h4{color:#fff;font-size:13px;font-weight:800;line-height:1.22;letter-spacing:-.02em;margin:0 0 7px}",
    "#oh-process-timeline-mock .ptm-platform-card p{color:rgba(255,255,255,.74);font-size:11px;line-height:1.5;margin:0}",
    "#oh-process-timeline-mock .ptm-node-pass{width:40px;height:40px;border-radius:50%;background:#fff;border:4px solid #d4a853;box-shadow:0 4px 14px rgba(0,0,0,.28);flex-shrink:0}",
    "#oh-process-timeline-mock .ptm-col-trigger-wrap{grid-column:1/-1;grid-row:4;display:grid;grid-template-columns:subgrid;background:#fff;border-top:1px solid rgba(0,0,0,.06)}",
    "#oh-process-timeline-mock .ptm-col-trigger-wrap > .ptm-band-label{grid-column:1/-1;font-size:9.5px;font-weight:800;letter-spacing:.11em;text-transform:uppercase;color:#9aa0ad;padding:12px 16px 0;margin:0}",
    "#oh-process-timeline-mock .ptm-col-trigger{padding:8px 12px 16px;border-right:1px solid rgba(0,0,0,.05);min-height:78px}",
    "#oh-process-timeline-mock .ptm-col-trigger:last-child{border-right:0}",
    "#oh-process-timeline-mock .ptm-col-trigger strong{display:block;font-size:9px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:#b0b5c0;margin-bottom:5px}",
    "#oh-process-timeline-mock .ptm-col-trigger p{font-size:11px;line-height:1.48;color:#4a5060;margin:0}",
    "#oh-process-timeline-mock .ptm-loi-badge{position:absolute;right:16px;bottom:20px;z-index:4;display:flex;align-items:center;gap:7px;padding:7px 12px;border-radius:999px;background:rgba(255,255,255,.1);border:1px solid rgba(255,255,255,.22);color:#faf3e1;font-size:10px;font-weight:800;letter-spacing:.07em;text-transform:uppercase;backdrop-filter:blur(6px)}",
    "#oh-process-timeline-mock .ptm-loi-badge svg{width:14px;height:14px;stroke:#6ee7a8;fill:none;stroke-width:2.5}",
    "#oh-process-timeline-mock .ptm-scroll-hint{display:none;margin:10px 0 0;font-size:11px;font-weight:600;color:#8a909c;text-align:center}",
    "#oh-process-timeline-mock .ptm-note{margin:18px 0 0;font-size:12px;color:#8a909c}",
    "@keyframes ptmCardIn{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}",
    "@media (max-width:900px){#oh-process-timeline-mock{padding:56px 0 64px}#oh-process-timeline-mock .ptm-wrap{width:min(1180px,calc(100% - 32px))}#oh-process-timeline-mock .ptm-scroll{margin:0 -16px;padding:0 16px 8px}#oh-process-timeline-mock .ptm-scroll-hint{display:block}}"
  ].join("");

  function buildChart(chart) {
    var COLS = STEPS.length;
    var VB_W = 1180;
    var VB_H = 200;
    var PAD = 74;
    var colW = (VB_W - PAD * 2) / (COLS - 1);
    var platformY = 118;
    var offlineY = 36;

    function colX(i) {
      return PAD + i * colW;
    }

    function buildPath() {
      var d = "M " + colX(0) + " " + platformY;
      for (var i = 1; i < COLS; i++) {
        var x = colX(i);
        var prev = colX(i - 1);
        if (STEPS[i].mode === "offline") {
          var lead = prev + (x - prev) * 0.52;
          d += " L " + lead + " " + platformY;
          d += " C " + (lead + (x - lead) * 0.55) + " " + platformY;
          d += " " + (x - colW * 0.04) + " " + offlineY;
          d += " " + x + " " + offlineY;
          d += " C " + (x + colW * 0.04) + " " + offlineY;
          d += " " + (x + colW * 0.08) + " " + platformY;
          d += " " + x + " " + platformY;
        } else {
          d += " L " + x + " " + platformY;
        }
      }
      return d;
    }

    var grid = document.createElement("div");
    grid.className = "ptm-grid";

    var head = document.createElement("div");
    head.className = "ptm-grid-head";
    head.innerHTML = "<span>Outside Platform · Facilitated by Dealality, Executed Offline</span>";
    grid.appendChild(head);

    STEPS.forEach(function (s, i) {
      var offlineCol = document.createElement("div");
      offlineCol.className = "ptm-col-offline";
      offlineCol.style.gridColumn = String(i + 1);
      if (s.mode === "offline") {
        offlineCol.innerHTML =
          '<article class="ptm-offline-card" style="--ptm-i:' + i + '">' +
          '<span class="ptm-num">Step ' + s.n + "</span>" +
          "<h4>" + s.title + "</h4>" +
          "<p>" + s.short + "</p></article>";
      }
      grid.appendChild(offlineCol);
    });

    var platformWrap = document.createElement("div");
    platformWrap.className = "ptm-col-platform-wrap";

    var platformLabel = document.createElement("p");
    platformLabel.className = "ptm-band-label ptm-band-label--platform";
    platformLabel.textContent = "In Platform · Managed Within Dealality";
    platformWrap.appendChild(platformLabel);

    var svgNS = "http://www.w3.org/2000/svg";
    var svg = document.createElementNS(svgNS, "svg");
    svg.setAttribute("class", "ptm-path-layer");
    svg.setAttribute("viewBox", "0 0 " + VB_W + " " + VB_H);
    svg.setAttribute("preserveAspectRatio", "none");
    svg.setAttribute("aria-hidden", "true");

    var defs = document.createElementNS(svgNS, "defs");
    defs.innerHTML =
      '<linearGradient id="ptmGoldGrad" x1="0%" y1="0%" x2="100%" y2="0%">' +
      '<stop offset="0%" stop-color="#e8c06a"/>' +
      '<stop offset="50%" stop-color="#d4a853"/>' +
      '<stop offset="100%" stop-color="#c8942e"/>' +
      "</linearGradient>";
    svg.appendChild(defs);

    var path = document.createElementNS(svgNS, "path");
    path.setAttribute("class", "ptm-rail");
    path.setAttribute("d", buildPath());
    svg.appendChild(path);

    STEPS.forEach(function (s, i) {
      var cx = colX(i);
      var cy = s.mode === "offline" ? offlineY : platformY;
      var circle = document.createElementNS(svgNS, "circle");
      circle.setAttribute("class", "ptm-node" + (s.mode === "offline" ? " ptm-node-offline" : ""));
      circle.setAttribute("cx", cx);
      circle.setAttribute("cy", cy);
      circle.setAttribute("r", s.mode === "offline" ? 14 : 16);
      svg.appendChild(circle);
      if (s.mode === "platform") {
        var label = document.createElementNS(svgNS, "text");
        label.setAttribute("class", "ptm-node-label");
        label.setAttribute("x", cx);
        label.setAttribute("y", cy);
        label.textContent = s.n;
        svg.appendChild(label);
      }
    });
    platformWrap.appendChild(svg);

    STEPS.forEach(function (s, i) {
      var pCol = document.createElement("div");
      pCol.className = "ptm-col-platform";
      if (s.mode === "platform") {
        pCol.innerHTML =
          '<article class="ptm-platform-card" style="--ptm-i:' + i + '">' +
          '<span class="ptm-num">Step ' + s.n + "</span>" +
          "<h4>" + s.title + "</h4>" +
          "<p>" + s.short + "</p></article>";
      } else {
        pCol.innerHTML = '<div class="ptm-node-pass" aria-hidden="true"></div>';
      }
      platformWrap.appendChild(pCol);
    });

    var loiBadge = document.createElement("div");
    loiBadge.className = "ptm-loi-badge";
    loiBadge.innerHTML =
      '<svg viewBox="0 0 24 24"><path d="M5 13l4 4L19 7"/></svg> LOI intent tracked';
    platformWrap.appendChild(loiBadge);
    grid.appendChild(platformWrap);

    var triggerWrap = document.createElement("div");
    triggerWrap.className = "ptm-col-trigger-wrap";
    var triggerLabel = document.createElement("p");
    triggerLabel.className = "ptm-band-label";
    triggerLabel.textContent = "Next Trigger";
    triggerWrap.appendChild(triggerLabel);

    STEPS.forEach(function (s) {
      var tCol = document.createElement("div");
      tCol.className = "ptm-col-trigger";
      tCol.innerHTML = "<strong>Next trigger</strong><p>" + s.trigger + "</p>";
      triggerWrap.appendChild(tCol);
    });
    grid.appendChild(triggerWrap);

    chart.innerHTML = "";
    chart.appendChild(grid);
  }

  function mount(host) {
    if (!document.getElementById("ptm-timeline-css")) {
      var style = document.createElement("style");
      style.id = "ptm-timeline-css";
      style.textContent = CSS;
      document.head.appendChild(style);
    }

    host.id = ROOT_ID;
    host.setAttribute("data-ptm-ready", "1");
    host.setAttribute("aria-label", "How Dealality Works — Timeline Mock");
    host.innerHTML =
      '<div class="ptm-wrap">' +
      '<p class="ptm-badge">Staging review · Not on dealality.com</p>' +
      '<div class="ptm-eyebrow">' +
      '<span class="ptm-eyebrow-pill">How Dealality Works</span>' +
      '<span class="ptm-eyebrow-right">One Clear Process from Opportunity to Agreement.</span>' +
      "</div>" +
      '<h2 class="ptm-title" id="ptm-h2">Built for Deals—<em>On &amp; Off</em> the Screen</h2>' +
      '<p class="ptm-lead">Structured. Confidential. Owner-led. Platform steps keep the record clear; offline steps keep the relationships human.</p>' +
      '<div class="ptm-legend">' +
      '<span class="lg-offline">Outside Platform · Facilitated by Dealality, Executed Offline</span>' +
      '<span class="lg-platform">In Platform · Managed Within Dealality</span>' +
      "</div>" +
      '<div class="ptm-scroll"><div class="ptm-chart" id="ptm-chart"></div></div>' +
      '<p class="ptm-scroll-hint" aria-hidden="true">Scroll timeline to see all eight steps</p>' +
      '<p class="ptm-note">Copy of the How Dealality Works section using a dual-track timeline. Live homepage section is unchanged.</p>' +
      "</div>";

    buildChart(document.getElementById("ptm-chart"));
  }

  function boot() {
    var host =
      document.getElementById("ptm-embed-host") ||
      document.getElementById(ROOT_ID) ||
      document.querySelector("[data-ptm-host]");
    if (!host) {
      host = document.createElement("section");
      var anchor = document.getElementById("oh-how-we-do-it");
      if (anchor && anchor.parentNode) {
        anchor.parentNode.insertBefore(host, anchor.nextSibling);
      } else {
        document.body.appendChild(host);
      }
    }
    mount(host.tagName === "SECTION" || host.id === ROOT_ID ? host : (function () {
      var section = document.createElement("section");
      host.replaceWith(section);
      return section;
    })());
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
