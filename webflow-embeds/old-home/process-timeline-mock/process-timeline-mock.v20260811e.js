/**
 * How Dealality Works timeline mock (v20260811e)
 * Matches live process section chrome: #080F25, 1120px inner, shared eyebrow.
 * Smooth dual-track gold path with numbered step circles; left-aligned band labels.
 * Dual-track: Platform vs Offline. Does not replace #oh-how-we-do-it.
 */
(function () {
  "use strict";
  var ROOT_ID = "oh-process-timeline-mock";
  var VERSION = "v20260811e";
  if (document.getElementById(ROOT_ID) && document.getElementById(ROOT_ID).getAttribute("data-ptm") === VERSION) {
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
    "#" + ROOT_ID + "{--ptm-font:\"Inter Tight\",\"Plus Jakarta Sans\",system-ui,sans-serif;--ptm-display:\"Plus Jakarta Sans\",\"Inter Tight\",system-ui,sans-serif;--ptm-ease:cubic-bezier(.22,1,.36,1);--ptm-card-h:140px;--ptm-trigger-h:72px;position:relative;overflow:hidden;box-sizing:border-box;font-family:var(--ptm-font);color:#e8ecf5;background:#080F25;padding:52px 1.5rem 44px;border-top:1px solid rgba(255,255,255,.07);border-bottom:1px solid rgba(255,255,255,.07)}",
    "#" + ROOT_ID + " *,#" + ROOT_ID + " *::before,#" + ROOT_ID + " *::after{box-sizing:border-box}",
    "#" + ROOT_ID + " .ptm-glow{position:absolute;left:50%;top:18%;width:min(92vw,900px);height:280px;transform:translate(-50%,-50%);border-radius:50%;background:radial-gradient(ellipse at center,rgba(108,114,255,.14) 0%,rgba(108,114,255,.04) 46%,transparent 72%);filter:blur(36px);pointer-events:none;z-index:0}",
    "#" + ROOT_ID + " .ptm-wrap{position:relative;z-index:1;max-width:1120px;width:100%;margin:0 auto}",
    "#" + ROOT_ID + " .ptm-eyebrow{display:inline-flex;align-items:center;overflow:hidden;border-radius:999px;border:1px solid rgba(255,255,255,.14);background:rgba(8,15,37,.92);padding:4px 12px 4px 4px;box-shadow:0 0 0 1px rgba(109,92,216,.1),0 0 28px rgba(109,92,216,.18);margin:0 0 12px;max-width:100%}",
    "#" + ROOT_ID + " .ptm-eyebrow-pill{display:inline-flex;align-items:center;padding:0 10px;height:32px;border-radius:10px;background:#343259;font-size:1rem;font-weight:500;line-height:1;color:#fff;white-space:nowrap;flex:0 0 auto}",
    "#" + ROOT_ID + " .ptm-eyebrow-right{display:inline-flex;align-items:center;margin-left:15px;font-size:1rem;font-weight:500;line-height:1;color:#fff;white-space:nowrap}",
    "#" + ROOT_ID + " .ptm-title{margin:0 0 8px;font-family:var(--ptm-display);font-size:clamp(26px,3.5vw,44px);font-weight:800;line-height:1.15;letter-spacing:-.03em;color:#fff}",
    "#" + ROOT_ID + " .ptm-title em{font-style:normal;color:#8B90FF}",
    "#" + ROOT_ID + " .ptm-lead{margin:0 0 18px;max-width:42rem;font-size:1.05rem;font-weight:500;line-height:1.65;color:rgba(255,255,255,.62)}",
    "#" + ROOT_ID + " .ptm-legend{display:flex;flex-wrap:wrap;gap:10px 22px;margin:0 0 18px;font-size:12.5px;font-weight:600;color:rgba(255,255,255,.58)}",
    "#" + ROOT_ID + " .ptm-legend span{display:inline-flex;align-items:center;gap:8px}",
    "#" + ROOT_ID + " .ptm-legend span::before{content:\"\";width:10px;height:10px;border-radius:50%;flex-shrink:0}",
    "#" + ROOT_ID + " .ptm-legend .lg-offline::before{background:#E8A84A;box-shadow:0 0 0 3px rgba(215,142,44,.22)}",
    "#" + ROOT_ID + " .ptm-legend .lg-platform::before{background:#8b90ff;box-shadow:0 0 0 3px rgba(108,114,255,.2)}",
    "#" + ROOT_ID + " .ptm-scroll{overflow-x:auto;-webkit-overflow-scrolling:touch;margin:0;padding:0 0 4px;scrollbar-width:thin;scrollbar-color:rgba(255,255,255,.18) transparent}",
    "#" + ROOT_ID + " .ptm-chart{position:relative;width:100%;border-radius:14px;overflow:hidden;background:rgba(10,16,36,.35);border:1px solid rgba(255,255,255,.08)}",
    "#" + ROOT_ID + " .ptm-grid{display:grid;grid-template-columns:repeat(8,minmax(0,1fr));grid-template-rows:auto var(--ptm-card-h) 220px auto;width:100%;min-width:0}",
    "#" + ROOT_ID + " .ptm-grid-head{grid-column:1/-1;grid-row:1;padding:12px 14px 6px}",
    "#" + ROOT_ID + " .ptm-grid-head span{font-size:9.5px;font-weight:800;letter-spacing:.11em;text-transform:uppercase;color:#F0B86A}",
    "#" + ROOT_ID + " .ptm-col-offline{grid-row:2;padding:0 6px;height:var(--ptm-card-h);display:flex;align-items:stretch;justify-content:center;min-width:0}",
    "#" + ROOT_ID + " .ptm-band-label--platform{position:absolute;top:10px;left:14px;right:auto;z-index:3;text-align:left;font-size:9.5px;font-weight:800;letter-spacing:.11em;text-transform:uppercase;color:rgba(255,255,255,.42);pointer-events:none;margin:0}",
    "#" + ROOT_ID + " .ptm-col-platform-wrap{grid-column:1/-1;grid-row:3;display:grid;grid-template-columns:subgrid;position:relative;height:220px;background:linear-gradient(180deg,rgba(17,27,58,.55),rgba(8,15,37,.72));border-top:1px solid rgba(255,255,255,.08);border-bottom:1px solid rgba(255,255,255,.06)}",
    "#" + ROOT_ID + " .ptm-col-platform-wrap::before{content:\"\";position:absolute;inset:0;background:radial-gradient(ellipse 70% 55% at 50% 100%,rgba(108,114,255,.14),transparent 62%);pointer-events:none}",
    "#" + ROOT_ID + " .ptm-col-platform{padding:0 6px;display:flex;align-items:center;justify-content:center;position:relative;z-index:2;height:220px;min-width:0}",
    "#" + ROOT_ID + " .ptm-path-layer{position:absolute;inset:0;z-index:1;pointer-events:none}",
    "#" + ROOT_ID + " .ptm-path-layer path.ptm-rail{fill:none;stroke:url(#ptmGoldGrad);stroke-width:5;stroke-linecap:round;stroke-linejoin:round;filter:drop-shadow(0 1px 2px rgba(0,0,0,.35))}",
    "#" + ROOT_ID + " .ptm-path-layer circle.ptm-node{fill:#0d1530;stroke:#E8A84A;stroke-width:3.5}",
    "#" + ROOT_ID + " .ptm-path-layer circle.ptm-node-offline{fill:#0d1530;stroke:#E8A84A;stroke-width:3.5}",
    "#" + ROOT_ID + " .ptm-path-layer text.ptm-node-label{fill:#F0B86A;font-size:11px;font-weight:800;font-family:var(--ptm-font);text-anchor:middle;dominant-baseline:central}",
    "#" + ROOT_ID + " .ptm-path-layer text.ptm-node-label-platform{fill:#a8abff}",
    "#" + ROOT_ID + " .ptm-card{display:flex;flex-direction:column;width:100%;max-width:118px;height:var(--ptm-card-h);min-height:var(--ptm-card-h);max-height:var(--ptm-card-h);padding:11px 9px;border-radius:12px;animation:ptmCardIn .55s var(--ptm-ease) both;animation-delay:calc(var(--ptm-i,0)*.05s)}",
    "#" + ROOT_ID + " .ptm-card .ptm-num{font-size:10px;letter-spacing:.08em;text-transform:uppercase;margin-bottom:5px;flex:0 0 auto;line-height:1.2}",
    "#" + ROOT_ID + " .ptm-card h4{font-size:12px;font-weight:800;line-height:1.22;letter-spacing:-.02em;margin:0 0 5px;height:2.44em;display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:2;overflow:hidden;flex:0 0 auto}",
    "#" + ROOT_ID + " .ptm-card p{font-size:10.5px;line-height:1.4;margin:0;display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:4;overflow:hidden;flex:1 1 auto;min-height:0}",
    "#" + ROOT_ID + " .ptm-offline-card{position:relative;background:linear-gradient(165deg,rgba(215,142,44,.16),rgba(32,26,18,.55));border:1px solid rgba(215,142,44,.32);box-shadow:0 8px 20px rgba(0,0,0,.18)}",
    "#" + ROOT_ID + " .ptm-offline-card::after{content:\"\";position:absolute;left:50%;bottom:-22px;width:2px;height:22px;transform:translateX(-50%);background:linear-gradient(180deg,#E8A84A,rgba(232,168,74,.12));border-radius:2px}",
    "#" + ROOT_ID + " .ptm-offline-card .ptm-num{color:#F0B86A}",
    "#" + ROOT_ID + " .ptm-offline-card h4{color:#fff}",
    "#" + ROOT_ID + " .ptm-offline-card p{color:rgba(255,255,255,.68)}",
    "#" + ROOT_ID + " .ptm-platform-card{margin-top:-22px;background:rgba(12,18,42,.88);border:1px solid rgba(108,114,255,.28);backdrop-filter:blur(8px);box-shadow:0 10px 24px rgba(0,0,0,.28),0 1px 0 rgba(255,255,255,.06) inset}",
    "#" + ROOT_ID + " .ptm-platform-card .ptm-num{color:#a8abff}",
    "#" + ROOT_ID + " .ptm-platform-card h4{color:#fff}",
    "#" + ROOT_ID + " .ptm-platform-card p{color:rgba(255,255,255,.68)}",
    "#" + ROOT_ID + " .ptm-node-pass{width:34px;height:34px;border-radius:50%;background:#0d1530;border:3.5px solid #E8A84A;box-shadow:0 4px 14px rgba(0,0,0,.28);flex-shrink:0}",
    "#" + ROOT_ID + " .ptm-col-trigger-wrap{grid-column:1/-1;grid-row:4;display:grid;grid-template-columns:subgrid;background:rgba(8,15,37,.45);border-top:1px solid rgba(255,255,255,.06)}",
    "#" + ROOT_ID + " .ptm-col-trigger-wrap > .ptm-band-label{grid-column:1/-1;font-size:9.5px;font-weight:800;letter-spacing:.11em;text-transform:uppercase;color:rgba(255,255,255,.38);padding:12px 14px 0;margin:0}",
    "#" + ROOT_ID + " .ptm-col-trigger{padding:8px 8px 14px;border-right:1px solid rgba(255,255,255,.06);height:calc(var(--ptm-trigger-h) + 34px);min-width:0}",
    "#" + ROOT_ID + " .ptm-col-trigger:last-child{border-right:0}",
    "#" + ROOT_ID + " .ptm-col-trigger strong{display:block;font-size:9px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:rgba(255,255,255,.35);margin-bottom:5px}",
    "#" + ROOT_ID + " .ptm-col-trigger p{font-size:11px;line-height:1.4;color:rgba(255,255,255,.58);margin:0;height:var(--ptm-trigger-h);display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:4;overflow:hidden}",
    "#" + ROOT_ID + " .ptm-loi-badge{position:absolute;right:12px;bottom:14px;z-index:4;display:flex;align-items:center;gap:7px;padding:7px 11px;border-radius:999px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.14);color:rgba(255,255,255,.82);font-size:10px;font-weight:800;letter-spacing:.06em;text-transform:uppercase;backdrop-filter:blur(6px)}",
    "#" + ROOT_ID + " .ptm-loi-badge svg{width:14px;height:14px;stroke:#6ee7a8;fill:none;stroke-width:2.5}",
    "#" + ROOT_ID + " .ptm-scroll-hint{display:none;margin:10px 0 0;font-size:11px;font-weight:600;color:rgba(255,255,255,.4);text-align:center}",
    "@keyframes ptmCardIn{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}",
    "@media (max-width:1100px){#" + ROOT_ID + " .ptm-grid{min-width:980px}#" + ROOT_ID + " .ptm-scroll-hint{display:block}}",
    "@media (max-width:900px){#" + ROOT_ID + "{padding:44px 1rem 40px}#" + ROOT_ID + " .ptm-eyebrow{flex-wrap:wrap;padding:4px 10px 4px 4px}#" + ROOT_ID + " .ptm-eyebrow-right{white-space:normal;margin:6px 2px 2px 8px}#" + ROOT_ID + "{--ptm-card-h:148px}}"
  ].join("");

  function buildChart(chart) {
    var COLS = STEPS.length;
    var VB_W = 1120;
    var VB_H = 200;
    var PAD = 70;
    var colW = (VB_W - PAD * 2) / (COLS - 1);
    var platformY = 118;
    var offlineY = 36;

    function colX(i) {
      return PAD + i * colW;
    }

    function buildPath() {
      var d = "M " + colX(0).toFixed(1) + " " + platformY;
      var currX = colX(0);
      for (var i = 1; i < COLS; i++) {
        var x = colX(i);
        var dx = x - currX;
        if (STEPS[i].mode === "offline") {
          /* Smooth rise up to the numbered offline circle */
          d += " C " + (currX + dx * 0.42).toFixed(1) + " " + platformY;
          d += " " + (x - dx * 0.18).toFixed(1) + " " + offlineY;
          d += " " + x.toFixed(1) + " " + offlineY;
          /* Smooth descent forward onto the platform rail */
          var landX = i < COLS - 1 ? (x + colX(i + 1)) * 0.5 : x + Math.max(48, colW * 0.45);
          var fall = landX - x;
          d += " C " + (x + fall * 0.2).toFixed(1) + " " + offlineY;
          d += " " + (landX - fall * 0.25).toFixed(1) + " " + platformY;
          d += " " + landX.toFixed(1) + " " + platformY;
          currX = landX;
        } else {
          d += " C " + (currX + (x - currX) * 0.45).toFixed(1) + " " + platformY;
          d += " " + (currX + (x - currX) * 0.7).toFixed(1) + " " + platformY;
          d += " " + x.toFixed(1) + " " + platformY;
          currX = x;
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
          '<article class="ptm-card ptm-offline-card" style="--ptm-i:' + i + '">' +
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
      '<stop offset="0%" stop-color="#F0B86A"/>' +
      '<stop offset="50%" stop-color="#D78E2C"/>' +
      '<stop offset="100%" stop-color="#E8A84A"/>' +
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
      circle.setAttribute("r", 14);
      svg.appendChild(circle);
      var label = document.createElementNS(svgNS, "text");
      label.setAttribute(
        "class",
        "ptm-node-label" + (s.mode === "platform" ? " ptm-node-label-platform" : "")
      );
      label.setAttribute("x", cx);
      label.setAttribute("y", cy);
      label.textContent = s.n;
      svg.appendChild(label);
    });
    platformWrap.appendChild(svg);

    STEPS.forEach(function (s, i) {
      var pCol = document.createElement("div");
      pCol.className = "ptm-col-platform";
      if (s.mode === "platform") {
        pCol.innerHTML =
          '<article class="ptm-card ptm-platform-card" style="--ptm-i:' + i + '">' +
          '<span class="ptm-num">Step ' + s.n + "</span>" +
          "<h4>" + s.title + "</h4>" +
          "<p>" + s.short + "</p></article>";
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
    var style = document.getElementById("ptm-timeline-css");
    if (!style) {
      style = document.createElement("style");
      style.id = "ptm-timeline-css";
      document.head.appendChild(style);
    }
    style.textContent = CSS;

    host.id = ROOT_ID;
    host.setAttribute("data-ptm", VERSION);
    host.setAttribute("aria-label", "How Dealality Works");
    host.innerHTML =
      '<div class="ptm-glow" aria-hidden="true"></div>' +
      '<div class="ptm-wrap">' +
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
    if (host.tagName === "SECTION" || host.id === ROOT_ID) {
      mount(host);
    } else {
      var section = document.createElement("section");
      host.replaceWith(section);
      mount(section);
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
