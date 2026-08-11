/**
 * How Dealality Works timeline mock (v20260812g)
 * Place LOI Intent Tracked badge on step 08 card top-right.
 * Does not replace #oh-how-we-do-it.
 */
(function () {
  "use strict";
  var ROOT_ID = "oh-process-timeline-mock";
  var VERSION = "v20260812g";
  var NODE_R = 14;
  var NODE_R_ACTIVE = 17;
  if (document.getElementById(ROOT_ID) && document.getElementById(ROOT_ID).getAttribute("data-ptm") === VERSION) {
    return;
  }

  var IMG_BRAND =
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a679518a66ce83bcb18be55_brand-explorer.png";
  var IMG_COMPARE =
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a679519982910d5c314bf1f_deal-compare.png";

  var STEPS = [
    {
      n: "01", mode: "platform", title: "Define the Opportunity",
      titleHtml: "Define the<br>Opportunity",
      short: "Capture scope, goals, constraints, and decision criteria in one brief.",
      trigger: "Shared opportunity brief ready for outreach.",
      primary: "Clarify what the owner wants before any relationship starts shaping the answer.",
      support: "Bring the hotel, market context, objectives, constraints, and decision criteria into one brief—so later conversations start from clarity.",
      bridge: "Prepares the first offline conversations with a shared opportunity story.",
      visual: "define"
    },
    {
      n: "02", mode: "platform", title: "See the Real Options",
      titleHtml: "See the<br>Real Options",
      short: "Score brand, operator, conversion, and partner paths worth evaluating.",
      trigger: "Credible paths shortlisted before anyone is contacted.",
      primary: "See more than the first available option.",
      support: "Identify the brand, operator, conversion, and partner paths worth evaluating—so the owner chooses who to meet with intention.",
      bridge: "Shortlists who is worth meeting before outreach begins.",
      visual: "paths", img: IMG_BRAND,
      alt: "Exploring credible brand and operator paths for one hotel opportunity"
    },
    {
      n: "03", mode: "platform", title: "Prepare & Engage",
      titleHtml: "Prepare &amp;<br>Engage",
      short: "Select participants, manage confidentiality, and coordinate outreach.",
      trigger: "Brands and operators notified; intro calls scheduled.",
      primary: "Open the right conversations—without losing control of the story.",
      support: "Select participants, manage confidentiality, coordinate outreach, and capture responses so introduction calls start ready—not cold.",
      bridge: "Hands a clean brief and status into the first relationship step.",
      visual: "engage"
    },
    {
      n: "04", mode: "offline", title: "Meet & Align",
      titleHtml: "Meet &amp;<br>Align",
      short: "Introduction and clarification calls happen between people—not in email threads.",
      trigger: "Outcomes tracked back into Dealality.",
      primary: "Put people in the room—where trust actually begins.",
      support: "Introductions and clarifications happen offline, between people. Dealality prepares both sides and tracks outcomes so the conversation stays part of the process—not a side channel.",
      bridge: "Met offline. Facilitated by Dealality · tracked in Dealality.",
      visual: "meet"
    },
    {
      n: "05", mode: "platform", title: "Compare What Matters",
      titleHtml: "Compare<br>What Matters",
      short: "Side-by-side view of fees, control, support, timing, and gaps.",
      trigger: "Shared comparison ready for diligence conversations.",
      primary: "Give every proposal a shared basis—so the next conversation can go deeper.",
      support: "Compare economics, control, requirements, support, timing, and gaps side by side. The numbers do not replace judgment; they prepare it.",
      bridge: "Creates a shared comparison for facilitated offline diligence.",
      visual: "compare", img: IMG_COMPARE,
      alt: "Side-by-side proposal comparison across fees, control, and missing terms"
    },
    {
      n: "06", mode: "offline", title: "Collaborate & Review",
      titleHtml: "Collaborate &amp;<br>Review",
      short: "Collaboration calls, site visits, and technical review offline.",
      trigger: "Review notes and trade-offs captured in the deal record.",
      primary: "Take the comparison into the real world—calls, walkthroughs, and technical review.",
      support: "Collaboration calls and site or tech review happen offline. Dealality facilitates scheduling and context, then tracks what was covered so judgment stays connected to the deal record.",
      bridge: "Met offline. Facilitated by Dealality · tracked in Dealality.",
      visual: "diligence"
    },
    {
      n: "07", mode: "platform", title: "Choose Path & Move Forward",
      titleHtml: "Choose Path &amp;<br>Move Forward",
      short: "Confirm finalists, open the Deal Room, and sync feasibility.",
      trigger: "Deal Room active; feasibility checklist underway.",
      primary: "Hold finalists, materials, and feasibility in one place while conversations continue.",
      support: "Confirm finalists, open the Deal Room, and sync feasibility—so momentum does not disappear between facilitated offline alignment moments.",
      bridge: "Keeps the record ready for final alignment—still tracked in Dealality.",
      visual: "pursue"
    },
    {
      n: "08", mode: "offline", title: "Align & Hand Off",
      titleHtml: "Align &amp;<br>Hand Off",
      short: "Final alignment on vision, cost, and ops—then into legal drafting.",
      trigger: "LOI intent tracked; legal handoff begins.",
      primary: "Close alignment between people—then hand into legal with a clean, tracked exit.",
      support: "Final alignment on vision, cost, and ops stays human and offline. Dealality tracks readiness through LOI intent, then supports a clean handoff into legal drafting.",
      bridge: "Aligned offline. Tracked in Dealality through handoff.",
      visual: "handoff"
    }
  ];

  var CSS = [
    "#" + ROOT_ID + "{--ptm-font:\"Inter Tight\",\"Plus Jakarta Sans\",system-ui,sans-serif;--ptm-display:\"Plus Jakarta Sans\",\"Inter Tight\",system-ui,sans-serif;--ptm-ease:cubic-bezier(.22,1,.36,1);--ptm-card-h:205px;--ptm-path-h:72px;position:relative;overflow:visible;box-sizing:border-box;font-family:var(--ptm-font);color:#e8ecf5;background:#080F25;padding:52px 1.5rem 44px;border-top:1px solid rgba(255,255,255,.07);border-bottom:1px solid rgba(255,255,255,.07)}",
    "#" + ROOT_ID + " *,#" + ROOT_ID + " *::before,#" + ROOT_ID + " *::after{box-sizing:border-box}",
    "#" + ROOT_ID + " .ptm-glow{position:absolute;left:50%;top:18%;width:min(92vw,900px);height:280px;transform:translate(-50%,-50%);border-radius:50%;background:radial-gradient(ellipse at center,rgba(108,114,255,.14) 0%,rgba(108,114,255,.04) 46%,transparent 72%);filter:blur(36px);pointer-events:none;z-index:0}",
    "#" + ROOT_ID + " .ptm-wrap{position:relative;z-index:1;max-width:1120px;width:100%;margin:0 auto}",
    "#" + ROOT_ID + " .ptm-eyebrow{display:inline-flex;align-items:center;overflow:hidden;border-radius:999px;border:1px solid rgba(255,255,255,.14);background:rgba(8,15,37,.92);padding:4px 12px 4px 4px;box-shadow:0 0 0 1px rgba(109,92,216,.1),0 0 28px rgba(109,92,216,.18);margin:0 0 12px;max-width:100%}",
    "#" + ROOT_ID + " .ptm-eyebrow-pill{display:inline-flex;align-items:center;padding:0 10px;height:32px;border-radius:10px;background:#343259;font-size:1rem;font-weight:500;line-height:1;color:#fff;white-space:nowrap;flex:0 0 auto}",
    "#" + ROOT_ID + " .ptm-eyebrow-right{display:inline-flex;align-items:center;margin-left:15px;font-size:1rem;font-weight:500;line-height:1;color:#fff;white-space:nowrap}",
    "#" + ROOT_ID + " .ptm-title{margin:0 0 8px;font-family:var(--ptm-display);font-size:clamp(26px,3.5vw,44px);font-weight:800;line-height:1.15;letter-spacing:-.03em;color:#fff}",
    "#" + ROOT_ID + " .ptm-lead{margin:0 0 18px;max-width:42rem;font-size:1.05rem;font-weight:500;line-height:1.65;color:rgba(255,255,255,.62)}",
    "#" + ROOT_ID + " .ptm-lead em{font-style:normal;color:#E8A84A;font-weight:650}",
    "#" + ROOT_ID + " .ptm-legend{display:flex;flex-wrap:wrap;gap:14px 36px;margin:0 0 28px;font-size:12.5px;font-weight:600;color:rgba(255,255,255,.58)}",
    "#" + ROOT_ID + " .ptm-legend span{display:inline-flex;align-items:center;gap:8px}",
    "#" + ROOT_ID + " .ptm-legend span::before{content:\"\";width:10px;height:10px;border-radius:50%;flex-shrink:0}",
    "#" + ROOT_ID + " .ptm-legend .lg-offline::before{background:#E8A84A;box-shadow:0 0 0 3px rgba(215,142,44,.22)}",
    "#" + ROOT_ID + " .ptm-legend .lg-platform::before{background:#8b90ff;box-shadow:0 0 0 3px rgba(108,114,255,.2)}",
    "#" + ROOT_ID + " .ptm-stage{position:relative;min-width:0}",
    "#" + ROOT_ID + " .ptm-hint{margin:0 0 16px;font-size:12px;font-weight:600;color:rgba(255,255,255,.4)}",
    "#" + ROOT_ID + " .ptm-canvas{position:relative;min-width:0}",
    "#" + ROOT_ID + " .ptm-scroll{overflow-x:auto;-webkit-overflow-scrolling:touch;margin:0;padding:0 0 4px;scrollbar-width:thin;scrollbar-color:rgba(255,255,255,.18) transparent;position:relative;z-index:2}",
    "#" + ROOT_ID + " .ptm-chart{position:relative;width:100%;border-radius:14px;overflow:visible;background:linear-gradient(180deg,rgba(17,27,58,.45),rgba(8,15,37,.72));border:1px solid rgba(255,255,255,.08);padding:24px 10px 24px}",
    "#" + ROOT_ID + " .ptm-grid{display:grid;grid-template-columns:repeat(8,minmax(0,1fr));grid-template-rows:var(--ptm-path-h) var(--ptm-card-h);width:100%;min-width:980px;row-gap:18px;position:relative}",
    "#" + ROOT_ID + " .ptm-path-layer{grid-column:1/-1;grid-row:1;width:100%;height:var(--ptm-path-h);z-index:1;pointer-events:none;overflow:visible}",
    "#" + ROOT_ID + " .ptm-path-layer path.ptm-rail-track{fill:none;stroke:rgba(232,168,74,.22);stroke-width:4.5;stroke-linecap:round;stroke-linejoin:round}",
    "#" + ROOT_ID + " .ptm-path-layer path.ptm-rail-progress{fill:none;stroke:url(#ptmGoldGrad);stroke-width:4.5;stroke-linecap:round;stroke-linejoin:round;filter:drop-shadow(0 1px 2px rgba(0,0,0,.35));transition:stroke-dasharray .28s var(--ptm-ease)}",
    "#" + ROOT_ID + " .ptm-path-layer circle.ptm-node{fill:#0d1530;stroke:rgba(232,168,74,.35);stroke-width:3;pointer-events:auto;cursor:pointer;transition:r .2s var(--ptm-ease),filter .2s,stroke .2s,fill .2s,stroke-width .2s}",
    "#" + ROOT_ID + " .ptm-path-layer circle.ptm-node-platform{stroke:rgba(139,144,255,.4)}",
    "#" + ROOT_ID + " .ptm-path-layer circle.ptm-node.is-done{stroke:#E8A84A;fill:#1a1420}",
    "#" + ROOT_ID + " .ptm-path-layer circle.ptm-node-platform.is-done{stroke:#8B90FF;fill:#12182f}",
    "#" + ROOT_ID + " .ptm-path-layer circle.ptm-node.is-active{filter:drop-shadow(0 0 8px rgba(232,168,74,.55));stroke:#F0B86A;stroke-width:3.5}",
    "#" + ROOT_ID + " .ptm-path-layer circle.ptm-node-platform.is-active{filter:drop-shadow(0 0 8px rgba(139,144,255,.5));stroke:#a8abff}",
    "#" + ROOT_ID + " .ptm-path-layer text.ptm-node-label{fill:rgba(240,184,106,.45);font-size:12px;font-weight:800;font-family:var(--ptm-font);text-anchor:middle;dominant-baseline:central;pointer-events:none;transition:fill .2s}",
    "#" + ROOT_ID + " .ptm-path-layer text.ptm-node-label-platform{fill:rgba(168,171,255,.45)}",
    "#" + ROOT_ID + " .ptm-path-layer text.ptm-node-label.is-done,#" + ROOT_ID + " .ptm-path-layer text.ptm-node-label.is-active{fill:#F0B86A}",
    "#" + ROOT_ID + " .ptm-path-layer text.ptm-node-label-platform.is-done,#" + ROOT_ID + " .ptm-path-layer text.ptm-node-label-platform.is-active{fill:#a8abff}",
    "#" + ROOT_ID + " .ptm-col{grid-row:2;padding:0 5px;height:var(--ptm-card-h);display:flex;align-items:stretch;justify-content:center;min-width:0;position:relative;z-index:2;transition:opacity .22s var(--ptm-ease)}",
    "#" + ROOT_ID + " .ptm-col::before{content:\"\";position:absolute;left:50%;top:-14px;width:2px;height:14px;transform:translateX(-50%);background:linear-gradient(180deg,rgba(232,168,74,.4),rgba(232,168,74,.08));border-radius:2px;pointer-events:none;transition:opacity .22s,background .22s}",
    "#" + ROOT_ID + " .ptm-col.is-platform::before{background:linear-gradient(180deg,rgba(139,144,255,.4),rgba(139,144,255,.08))}",
    "#" + ROOT_ID + " .ptm-card{display:flex;flex-direction:column;position:relative;width:100%;max-width:156px;height:var(--ptm-card-h);min-height:var(--ptm-card-h);max-height:var(--ptm-card-h);padding:14px 12px 12px;border-radius:12px;overflow:hidden;cursor:pointer;transition:border-color .2s,box-shadow .2s,transform .2s var(--ptm-ease),opacity .22s var(--ptm-ease),filter .22s;animation:ptmCardIn .55s var(--ptm-ease) both;animation-delay:calc(var(--ptm-i,0)*.05s)}",
    "#" + ROOT_ID + " .ptm-card .ptm-num{font-size:11px;letter-spacing:.08em;text-transform:uppercase;margin-bottom:6px;flex:0 0 auto;line-height:1.2;font-weight:700}",
    "#" + ROOT_ID + " .ptm-card h4{font-size:14px;font-weight:800;line-height:1.25;letter-spacing:-.02em;margin:0 0 8px;flex:0 0 auto;min-height:2.5em}",
    "#" + ROOT_ID + " .ptm-card p{font-size:12px;line-height:1.4;margin:0;flex:1 1 auto;min-height:0;overflow:hidden;display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:5;line-clamp:5}",
    "#" + ROOT_ID + " .ptm-offline-card{position:relative;background:rgba(12,18,42,.88);border:1px solid rgba(215,142,44,.28);box-shadow:0 8px 20px rgba(0,0,0,.18)}",
    "#" + ROOT_ID + " .ptm-offline-card .ptm-num{color:#F0B86A}",
    "#" + ROOT_ID + " .ptm-offline-card h4,#" + ROOT_ID + " .ptm-platform-card h4{color:#fff}",
    "#" + ROOT_ID + " .ptm-offline-card p,#" + ROOT_ID + " .ptm-platform-card p{color:rgba(255,255,255,.72)}",
    "#" + ROOT_ID + " .ptm-platform-card{margin-top:0;background:rgba(12,18,42,.88);border:1px solid rgba(108,114,255,.28);backdrop-filter:blur(8px);box-shadow:0 10px 24px rgba(0,0,0,.28),0 1px 0 rgba(255,255,255,.06) inset}",
    "#" + ROOT_ID + " .ptm-platform-card .ptm-num{color:#a8abff}",
    "#" + ROOT_ID + " .ptm-chart.is-focusing .ptm-col{opacity:.34}",
    "#" + ROOT_ID + " .ptm-chart.is-focusing .ptm-col.is-active-col{opacity:1}",
    "#" + ROOT_ID + " .ptm-chart.is-focusing .ptm-col:not(.is-active-col)::before{opacity:.2}",
    "#" + ROOT_ID + " .ptm-card.is-active{transform:translateY(-3px)}",
    "#" + ROOT_ID + " .ptm-offline-card.is-active{border-color:rgba(232,168,74,.85);background:linear-gradient(165deg,rgba(215,142,44,.22),rgba(32,26,18,.6));box-shadow:0 12px 28px rgba(0,0,0,.32),0 0 0 1px rgba(232,168,74,.3)}",
    "#" + ROOT_ID + " .ptm-platform-card.is-active{border-color:rgba(139,144,255,.85);box-shadow:0 12px 28px rgba(0,0,0,.38),0 0 0 1px rgba(108,114,255,.35)}",
    "#" + ROOT_ID + " .ptm-path-layer circle.ptm-node.is-ahead{stroke:rgba(232,168,74,.28)}",
    "#" + ROOT_ID + " .ptm-path-layer circle.ptm-node-platform.is-ahead{stroke:rgba(139,144,255,.28)}",
    "#" + ROOT_ID + " .ptm-loi-badge{position:absolute;top:8px;right:8px;z-index:3;display:inline-flex;align-items:center;gap:4px;max-width:calc(100% - 16px);padding:4px 7px;border-radius:999px;background:rgba(8,15,37,.88);border:1px solid rgba(110,231,168,.35);color:rgba(255,255,255,.88);font-size:8px;font-weight:800;letter-spacing:.04em;text-transform:uppercase;line-height:1.2;pointer-events:none}",
    "#" + ROOT_ID + " .ptm-loi-badge svg{width:10px;height:10px;flex:0 0 auto;stroke:#6ee7a8;fill:none;stroke-width:2.5}",
    "#" + ROOT_ID + " .ptm-detail{position:absolute;left:auto;right:10px;bottom:4px;top:auto;z-index:8;width:min(520px,54%);max-height:0;overflow:hidden;border-radius:16px;border:1px solid transparent;background:rgba(8,15,37,.97);padding:0 20px;box-shadow:none;opacity:0;visibility:hidden;pointer-events:none;transition:opacity .22s var(--ptm-ease),visibility .22s,max-height .28s var(--ptm-ease),padding .22s,border-color .22s,box-shadow .22s,left .28s var(--ptm-ease),right .28s var(--ptm-ease),transform .22s var(--ptm-ease)}",
    "#" + ROOT_ID + " .ptm-detail.is-dock-right{right:10px;left:auto;transform:translateY(6px)}",
    "#" + ROOT_ID + " .ptm-detail.is-dock-left{left:10px;right:auto;transform:translateY(6px)}",
    "#" + ROOT_ID + " .ptm-detail.is-dock-center{left:50%;right:auto;transform:translate(-50%,6px)}",
    "#" + ROOT_ID + " .ptm-detail.is-open{opacity:1;visibility:visible;pointer-events:auto;max-height:min(78vh,640px);overflow:hidden;padding:18px 20px 16px;border-color:rgba(255,255,255,.14);box-shadow:0 22px 56px rgba(0,0,0,.5),0 0 0 1px rgba(108,114,255,.12)}",
    "#" + ROOT_ID + " .ptm-detail.is-dock-right.is-open,#" + ROOT_ID + " .ptm-detail.is-dock-left.is-open{transform:translateY(0)}",
    "#" + ROOT_ID + " .ptm-detail.is-dock-center.is-open{transform:translate(-50%,0)}",
    "#" + ROOT_ID + " .ptm-detail.is-dock-right.is-swapping,#" + ROOT_ID + " .ptm-detail.is-dock-left.is-swapping{opacity:0;transform:translateY(4px)}",
    "#" + ROOT_ID + " .ptm-detail.is-dock-center.is-swapping{opacity:0;transform:translate(-50%,4px)}",
    "#" + ROOT_ID + " .ptm-detail.is-offline{border-color:rgba(215,142,44,.35);box-shadow:0 22px 56px rgba(0,0,0,.5),0 0 0 1px rgba(215,142,44,.18)}",
    "#" + ROOT_ID + " .ptm-detail-backdrop{display:none}",
    "#" + ROOT_ID + " .ptm-detail-close{position:absolute;top:12px;right:12px;width:30px;height:30px;border-radius:8px;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.04);color:rgba(255,255,255,.7);cursor:pointer;font-size:18px;line-height:1}",
    "#" + ROOT_ID + " .ptm-detail-close:hover{background:rgba(255,255,255,.08);color:#fff}",
    "#" + ROOT_ID + " .ptm-d-kicker{margin:0 0 6px;font-size:.74rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:#8B90FF}",
    "#" + ROOT_ID + " .ptm-detail.is-offline .ptm-d-kicker{color:#E8A84A}",
    "#" + ROOT_ID + " .ptm-d-title{margin:0 0 10px;font-size:clamp(1.15rem,2vw,1.45rem);font-weight:800;letter-spacing:-.02em;color:#fff;line-height:1.2;text-transform:capitalize}",
    "#" + ROOT_ID + " .ptm-d-pill{display:inline-flex;align-items:center;gap:7px;margin:0 0 12px;padding:5px 10px;border-radius:999px;border:1px solid rgba(255,255,255,.12);font-size:11px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;color:rgba(255,255,255,.7)}",
    "#" + ROOT_ID + " .ptm-d-pill i{width:7px;height:7px;border-radius:50%;display:inline-block}",
    "#" + ROOT_ID + " .ptm-d-pill.is-platform{border-color:rgba(108,114,255,.35);color:#b8bbff;background:rgba(108,114,255,.1)}",
    "#" + ROOT_ID + " .ptm-d-pill.is-platform i{background:#8b90ff}",
    "#" + ROOT_ID + " .ptm-d-pill.is-offline{border-color:rgba(215,142,44,.4);color:#f0c789;background:rgba(215,142,44,.12)}",
    "#" + ROOT_ID + " .ptm-d-pill.is-offline i{background:#E8A84A}",
    "#" + ROOT_ID + " .ptm-d-primary{margin:0 0 8px;font-size:.98rem;font-weight:650;line-height:1.45;color:#fff}",
    "#" + ROOT_ID + " .ptm-d-support{margin:0 0 12px;font-size:.9rem;line-height:1.55;color:rgba(255,255,255,.62)}",
    "#" + ROOT_ID + " .ptm-d-bridge{margin:0 0 14px;padding:10px 12px;border-radius:10px;border:1px dashed rgba(255,255,255,.16);font-size:.84rem;line-height:1.45;color:rgba(255,255,255,.78)}",
    "#" + ROOT_ID + " .ptm-detail.is-offline .ptm-d-bridge{border-color:rgba(215,142,44,.35);color:#F0B86A}",
    "#" + ROOT_ID + " .ptm-d-bridge strong{color:rgba(255,255,255,.9)}",
    "#" + ROOT_ID + " .ptm-detail.is-offline .ptm-d-bridge strong{color:#F0B86A}",
    "#" + ROOT_ID + " .ptm-d-visual{border-radius:12px;border:1px solid rgba(255,255,255,.1);overflow:hidden;background:rgba(8,15,37,.75)}",
    "#" + ROOT_ID + " .ptm-detail.is-offline .ptm-d-visual{border-color:rgba(215,142,44,.28)}",
    "#" + ROOT_ID + " .ptm-d-chrome{display:flex;align-items:center;gap:6px;padding:8px 10px;background:rgba(255,255,255,.03);border-bottom:1px solid rgba(255,255,255,.06)}",
    "#" + ROOT_ID + " .ptm-d-chrome span{width:8px;height:8px;border-radius:50%;display:inline-block}",
    "#" + ROOT_ID + " .ptm-d-chrome span:nth-child(1){background:#ff5f57}",
    "#" + ROOT_ID + " .ptm-d-chrome span:nth-child(2){background:#febc2e}",
    "#" + ROOT_ID + " .ptm-d-chrome span:nth-child(3){background:#28c840}",
    "#" + ROOT_ID + " .ptm-d-chrome em{margin-left:6px;font-style:normal;font-size:.72rem;color:rgba(255,255,255,.45)}",
    "#" + ROOT_ID + " .ptm-d-panel{padding:10px 12px;display:grid;gap:8px}",
    "#" + ROOT_ID + " .ptm-d-panel-head{display:flex;justify-content:space-between;gap:10px;align-items:center;flex-wrap:wrap}",
    "#" + ROOT_ID + " .ptm-d-chip{display:inline-flex;padding:4px 9px;border-radius:999px;background:rgba(108,114,255,.16);border:1px solid rgba(108,114,255,.3);color:#c5c8ff;font-size:.72rem;font-weight:700}",
    "#" + ROOT_ID + " .ptm-detail.is-offline .ptm-d-chip{background:rgba(215,142,44,.14);border-color:rgba(215,142,44,.35);color:#F0B86A}",
    "#" + ROOT_ID + " .ptm-d-meta{font-size:.72rem;color:rgba(255,255,255,.5)}",
    "#" + ROOT_ID + " .ptm-d-bar{height:6px;border-radius:999px;background:rgba(255,255,255,.08);overflow:hidden}",
    "#" + ROOT_ID + " .ptm-d-bar i{display:block;height:100%;background:linear-gradient(90deg,#8B90FF,#6C72FF);border-radius:999px}",
    "#" + ROOT_ID + " .ptm-d-rows{display:grid;gap:5px}",
    "#" + ROOT_ID + " .ptm-d-row{display:grid;grid-template-columns:1fr auto;gap:10px;align-items:baseline;padding:6px 0;border-bottom:1px solid rgba(255,255,255,.06)}",
    "#" + ROOT_ID + " .ptm-d-row:last-child{border-bottom:0}",
    "#" + ROOT_ID + " .ptm-d-row strong{font-size:.78rem;font-weight:650;color:#fff}",
    "#" + ROOT_ID + " .ptm-d-row span{font-size:.72rem;color:rgba(255,255,255,.55);text-align:right}",
    "#" + ROOT_ID + " .ptm-d-row.is-human strong{color:#E8A84A}",
    "#" + ROOT_ID + " .ptm-d-paths{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:7px}",
    "#" + ROOT_ID + " .ptm-d-path{padding:8px;border-radius:8px;border:1px solid rgba(255,255,255,.08);background:rgba(255,255,255,.03)}",
    "#" + ROOT_ID + " .ptm-d-path strong{display:block;color:#fff;font-size:.78rem;margin:0 0 4px}",
    "#" + ROOT_ID + " .ptm-d-path span{display:block;color:rgba(255,255,255,.5);font-size:.7rem;line-height:1.35}",
    "#" + ROOT_ID + " .ptm-d-moments{display:grid;grid-template-columns:1fr 1fr;gap:8px}",
    "#" + ROOT_ID + " .ptm-d-moment{padding:9px;border-radius:8px;border:1px solid rgba(215,142,44,.28);background:rgba(215,142,44,.06)}",
    "#" + ROOT_ID + " .ptm-d-moment strong{display:block;color:#F0B86A;font-size:.78rem;margin:0 0 4px}",
    "#" + ROOT_ID + " .ptm-d-moment p{margin:0;color:rgba(255,255,255,.58);font-size:.72rem;line-height:1.4}",
    "#" + ROOT_ID + " .ptm-d-shot{max-height:110px;overflow:hidden}",
    "#" + ROOT_ID + " .ptm-d-shot img{width:100%;height:100%;object-fit:cover;display:block}",
    "#" + ROOT_ID + " .ptm-d-compare{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:0;font-size:.7rem}",
    "#" + ROOT_ID + " .ptm-d-compare > *{padding:6px 7px;border-bottom:1px solid rgba(255,255,255,.06);color:rgba(255,255,255,.65)}",
    "#" + ROOT_ID + " .ptm-d-compare .is-head{color:#fff;font-weight:700;background:#111b3a}",
    "#" + ROOT_ID + " .ptm-d-compare .is-gap{color:#D78E2C}",
    "@keyframes ptmCardIn{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}",
    "@media (max-width:980px){#" + ROOT_ID + " .ptm-detail{width:min(480px,58%)}#" + ROOT_ID + " .ptm-detail.is-open{max-height:min(76vh,600px)}#" + ROOT_ID + " .ptm-d-paths,#" + ROOT_ID + " .ptm-d-moments{grid-template-columns:1fr}}",
    "@media (max-width:900px){#" + ROOT_ID + "{padding:44px 1rem 40px;--ptm-card-h:205px;--ptm-path-h:64px}#" + ROOT_ID + " .ptm-eyebrow{flex-wrap:wrap;padding:4px 10px 4px 4px}#" + ROOT_ID + " .ptm-eyebrow-right{white-space:normal;margin:6px 2px 2px 8px}#" + ROOT_ID + " .ptm-detail{width:min(420px,70%);bottom:4px;top:auto}}"
  ].join("");

  function esc(s) {
    return String(s || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function visualHtml(step) {
    if (step.visual === "define") {
      return (
        '<div class="ptm-d-visual">' +
        '<div class="ptm-d-chrome" aria-hidden="true"><span></span><span></span><span></span><em>Opportunity Review · Brief</em></div>' +
        '<div class="ptm-d-panel"><div class="ptm-d-panel-head"><span class="ptm-d-chip">Opportunity Brief</span><span class="ptm-d-meta">Clarity before the first conversation</span></div>' +
        '<div class="ptm-d-rows">' +
        '<div class="ptm-d-row"><strong>Opportunity Type</strong><span>Brand + operator review</span></div>' +
        '<div class="ptm-d-row"><strong>Owner Objectives</strong><span>Stabilize NOI · Preserve control</span></div>' +
        '<div class="ptm-d-row"><strong>Constraints</strong><span>Capex limited · 18-month horizon</span></div>' +
        '<div class="ptm-d-row"><strong>Decision Criteria</strong><span>Control · fees · timeline · support</span></div>' +
        "</div></div></div>"
      );
    }
    if (step.visual === "paths") {
      return (
        '<div class="ptm-d-visual">' +
        '<div class="ptm-d-chrome" aria-hidden="true"><span></span><span></span><span></span><em>Brand Explorer · Path Map</em></div>' +
        (step.img ? '<div class="ptm-d-shot"><img src="' + esc(step.img) + '" alt="' + esc(step.alt) + '" loading="lazy"></div>' : "") +
        '<div class="ptm-d-panel"><div class="ptm-d-panel-head"><span class="ptm-d-chip">Credible Paths</span><span class="ptm-d-meta">Choose who is worth meeting</span></div>' +
        '<div class="ptm-d-paths">' +
        '<div class="ptm-d-path"><strong>Soft Brand</strong><span>Fit rationale · Capex moderate</span></div>' +
        '<div class="ptm-d-path"><strong>Operator Partner</strong><span>Operating model lift · Control trade-offs</span></div>' +
        '<div class="ptm-d-path"><strong>Conversion</strong><span>Positioning shift · Evidence pack</span></div>' +
        "</div></div></div>"
      );
    }
    if (step.visual === "engage") {
      return (
        '<div class="ptm-d-visual">' +
        '<div class="ptm-d-chrome" aria-hidden="true"><span></span><span></span><span></span><em>Controlled Outreach · Workspace</em></div>' +
        '<div class="ptm-d-panel"><div class="ptm-d-panel-head"><span class="ptm-d-chip">Controlled Outreach</span><span class="ptm-d-meta">Ready for the first meeting</span></div>' +
        '<div class="ptm-d-bar" aria-hidden="true"><i style="width:62%"></i></div>' +
        '<div class="ptm-d-rows">' +
        '<div class="ptm-d-row"><strong>Selected Participants</strong><span>3 brands · 2 operators</span></div>' +
        '<div class="ptm-d-row"><strong>Confidentiality</strong><span>NDA active · gated materials</span></div>' +
        '<div class="ptm-d-row"><strong>Outreach Status</strong><span>2 contacted · 1 ready</span></div>' +
        '<div class="ptm-d-row"><strong>Next Step</strong><span>Introduction call · offline</span></div>' +
        "</div></div></div>"
      );
    }
    if (step.visual === "meet") {
      return (
        '<div class="ptm-d-visual">' +
        '<div class="ptm-d-chrome" aria-hidden="true"><span></span><span></span><span></span><em>Offline · Relationship Moments</em></div>' +
        '<div class="ptm-d-panel"><div class="ptm-d-panel-head"><span class="ptm-d-chip">Meet &amp; align</span><span class="ptm-d-meta">Where trust begins</span></div>' +
        '<div class="ptm-d-moments">' +
        '<div class="ptm-d-moment"><strong>Introduction Call</strong><p>Owner and brand/operator meet. Fit, intent, and chemistry—not a form submission.</p></div>' +
        '<div class="ptm-d-moment"><strong>Clarification Call</strong><p>Questions get answered live before commercial terms harden.</p></div>' +
        "</div>" +
        '<div class="ptm-d-rows">' +
        '<div class="ptm-d-row is-human"><strong>People meet</strong><span>Calls · video · in person</span></div>' +
        '<div class="ptm-d-row"><strong>Dealality role</strong><span>Prepared · facilitated · outcome tracked</span></div>' +
        "</div></div></div>"
      );
    }
    if (step.visual === "compare") {
      return (
        '<div class="ptm-d-visual">' +
        '<div class="ptm-d-chrome" aria-hidden="true"><span></span><span></span><span></span><em>Shared Comparison · Strongest View</em></div>' +
        (step.img ? '<div class="ptm-d-shot"><img src="' + esc(step.img) + '" alt="' + esc(step.alt) + '" loading="lazy"></div>' : "") +
        '<div class="ptm-d-panel"><div class="ptm-d-panel-head"><span class="ptm-d-chip">Shared Comparison</span><span class="ptm-d-meta">Prepares the next offline diligence</span></div>' +
        '<div class="ptm-d-compare" role="table">' +
        '<div class="is-head">Category</div><div class="is-head">Path A</div><div class="is-head">Path B</div><div class="is-head">Path C</div>' +
        "<div>Fees</div><div>4.5% + 2%</div><div>5.0% + 1%</div><div>3.8% + 3%</div>" +
        "<div>Owner Control</div><div>High</div><div>Medium</div><div>Shared</div>" +
        '<div>Missing Terms</div><div class="is-gap">Capex TBD</div><div>—</div><div class="is-gap">Exit TBD</div>' +
        "</div></div></div>"
      );
    }
    if (step.visual === "diligence") {
      return (
        '<div class="ptm-d-visual">' +
        '<div class="ptm-d-chrome" aria-hidden="true"><span></span><span></span><span></span><em>Offline · Human Diligence</em></div>' +
        '<div class="ptm-d-panel"><div class="ptm-d-panel-head"><span class="ptm-d-chip">Collaborate &amp; review</span><span class="ptm-d-meta">Where judgment happens</span></div>' +
        '<div class="ptm-d-moments">' +
        '<div class="ptm-d-moment"><strong>Collaboration Calls</strong><p>Walk economics, support models, and strategic fit together—live.</p></div>' +
        '<div class="ptm-d-moment"><strong>Site / Tech Review</strong><p>Property walkthrough or remote technical review. Human diligence.</p></div>' +
        "</div>" +
        '<div class="ptm-d-rows">' +
        '<div class="ptm-d-row is-human"><strong>People meet</strong><span>Calls · site visit · tech review</span></div>' +
        '<div class="ptm-d-row"><strong>Dealality role</strong><span>Context ready · coverage tracked</span></div>' +
        "</div></div></div>"
      );
    }
    if (step.visual === "pursue") {
      return (
        '<div class="ptm-d-visual">' +
        '<div class="ptm-d-chrome" aria-hidden="true"><span></span><span></span><span></span><em>Preferred Direction · Decision Record</em></div>' +
        '<div class="ptm-d-panel"><div class="ptm-d-panel-head"><span class="ptm-d-chip">Preferred Direction</span><span class="ptm-d-meta">Momentum between conversations</span></div>' +
        '<div class="ptm-d-rows">' +
        '<div class="ptm-d-row"><strong>Preferred Direction</strong><span>Soft brand + operator support</span></div>' +
        '<div class="ptm-d-row"><strong>Deal Room</strong><span>Open for finalists</span></div>' +
        '<div class="ptm-d-row"><strong>Feasibility Sync</strong><span>Docs + checklist tracked</span></div>' +
        '<div class="ptm-d-row"><strong>Next Step</strong><span>Final alignment call · offline</span></div>' +
        "</div></div></div>"
      );
    }
    return (
      '<div class="ptm-d-visual">' +
      '<div class="ptm-d-chrome" aria-hidden="true"><span></span><span></span><span></span><em>Offline · Align &amp; Platform Exit</em></div>' +
      '<div class="ptm-d-panel"><div class="ptm-d-panel-head"><span class="ptm-d-chip">Align &amp; hand off</span><span class="ptm-d-meta">Where the deal closes</span></div>' +
      '<div class="ptm-d-moments">' +
      '<div class="ptm-d-moment"><strong>Final Alignment Call</strong><p>Vision, cost, and ops confirmed between people—last strategic discussion.</p></div>' +
      '<div class="ptm-d-moment"><strong>Legal Start</strong><p>MA / FA drafting and signature move offline. Clean platform exit.</p></div>' +
      "</div>" +
      '<div class="ptm-d-rows">' +
      '<div class="ptm-d-row is-human"><strong>People meet</strong><span>Alignment · counsel · signing path</span></div>' +
      '<div class="ptm-d-row"><strong>Dealality role</strong><span>LOI readiness tracked · clean handoff</span></div>' +
      "</div></div></div>"
    );
  }

  function detailHtml(step) {
    var offline = step.mode === "offline";
    return (
      '<p class="ptm-d-kicker">Step ' + esc(step.n) + "</p>" +
      '<h3 class="ptm-d-title">' + esc(step.title) + "</h3>" +
      '<div class="ptm-d-pill ' + (offline ? "is-offline" : "is-platform") + '"><i aria-hidden="true"></i>' +
      (offline ? "Offline · Facilitated by Dealality" : "In Platform · Structure") +
      "</div>" +
      '<p class="ptm-d-primary">' + esc(step.primary) + "</p>" +
      '<p class="ptm-d-support">' + esc(step.support) + "</p>" +
      '<p class="ptm-d-bridge"><strong>' + (offline ? "Where people meet:" : "Leads into:") + "</strong> " +
      esc(step.bridge) + "</p>" +
      visualHtml(step)
    );
  }

  function buildChart(chart, onSelect) {
    var COLS = STEPS.length;
    var VB_W = 1120;
    var rootEl = document.getElementById(ROOT_ID);
    var cs = rootEl ? getComputedStyle(rootEl) : null;
    function cssPx(name, fallback) {
      if (!cs) return fallback;
      var v = parseFloat(cs.getPropertyValue(name));
      return isFinite(v) ? v : fallback;
    }
    var PATH_H = cssPx("--ptm-path-h", 64);
    var VB_H = PATH_H;
    var PAD = 70;
    var colW = (VB_W - PAD * 2) / (COLS - 1);
    var railY = PATH_H * 0.5;

    function colX(i) {
      return PAD + i * colW;
    }

    function buildFlatPath() {
      var d = "M " + colX(0).toFixed(1) + " " + railY.toFixed(1);
      for (var i = 1; i < COLS; i++) {
        d += " L " + colX(i).toFixed(1) + " " + railY.toFixed(1);
      }
      return d;
    }

    function setProgress(i) {
      var prog = chart.querySelector(".ptm-rail-progress");
      if (!prog) return;
      var pct = i < 0 ? 0 : (i / Math.max(1, COLS - 1)) * 100;
      prog.setAttribute("stroke-dasharray", pct.toFixed(2) + " " + (100 - pct).toFixed(2));
    }
    chart._ptmSetProgress = setProgress;

    var grid = document.createElement("div");
    grid.className = "ptm-grid";

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

    var flatD = buildFlatPath();
    var track = document.createElementNS(svgNS, "path");
    track.setAttribute("class", "ptm-rail-track");
    track.setAttribute("d", flatD);
    svg.appendChild(track);

    var progress = document.createElementNS(svgNS, "path");
    progress.setAttribute("class", "ptm-rail-progress");
    progress.setAttribute("d", flatD);
    progress.setAttribute("pathLength", "100");
    progress.setAttribute("stroke-dasharray", "0 100");
    svg.appendChild(progress);

    STEPS.forEach(function (s, i) {
      var cx = colX(i);
      var cy = railY;
      var circle = document.createElementNS(svgNS, "circle");
      circle.setAttribute(
        "class",
        "ptm-node" + (s.mode === "offline" ? " ptm-node-offline" : " ptm-node-platform")
      );
      circle.setAttribute("cx", cx);
      circle.setAttribute("cy", cy);
      circle.setAttribute("r", String(NODE_R));
      circle.setAttribute("data-ptm-step", String(i));
      svg.appendChild(circle);
      var label = document.createElementNS(svgNS, "text");
      label.setAttribute(
        "class",
        "ptm-node-label" + (s.mode === "platform" ? " ptm-node-label-platform" : "")
      );
      label.setAttribute("x", cx);
      label.setAttribute("y", cy);
      label.setAttribute("data-ptm-label", String(i));
      label.textContent = s.n;
      svg.appendChild(label);
    });
    grid.appendChild(svg);

    STEPS.forEach(function (s, i) {
      var col = document.createElement("div");
      col.className = "ptm-col" + (s.mode === "platform" ? " is-platform" : " is-offline");
      col.style.gridColumn = String(i + 1);
      col.setAttribute("data-ptm-col", String(i));
      var cardClass = s.mode === "offline" ? "ptm-offline-card" : "ptm-platform-card";
      var loiBadgeHtml = "";
      if (s.n === "08") {
        loiBadgeHtml =
          '<span class="ptm-loi-badge" aria-label="LOI intent tracked">' +
          '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 13l4 4L19 7"/></svg>' +
          "LOI Intent Tracked</span>";
      }
      col.innerHTML =
        '<article class="ptm-card ' + cardClass + '" data-ptm-step="' + i + '" tabindex="0" role="button" aria-label="Step ' + s.n + ": " + s.title + '" style="--ptm-i:' + i + '">' +
        loiBadgeHtml +
        '<span class="ptm-num">Step ' + s.n + "</span>" +
        "<h4>" + (s.titleHtml || esc(s.title)) + "</h4>" +
        "<p>" + s.short + "</p></article>";
      grid.appendChild(col);
    });

    chart.innerHTML = "";
    chart.appendChild(grid);
    setProgress(-1);

    chart.addEventListener("mouseover", function (e) {
      var el = e.target.closest("[data-ptm-step]");
      if (!el) return;
      onSelect(Number(el.getAttribute("data-ptm-step")), false);
    });
    chart.addEventListener("focusin", function (e) {
      var el = e.target.closest("[data-ptm-step]");
      if (!el) return;
      onSelect(Number(el.getAttribute("data-ptm-step")), true);
    });
    chart.addEventListener("click", function (e) {
      var el = e.target.closest("[data-ptm-step]");
      if (!el) return;
      onSelect(Number(el.getAttribute("data-ptm-step")), true);
    });
    chart.addEventListener("keydown", function (e) {
      if (e.key !== "Enter" && e.key !== " ") return;
      var el = e.target.closest("[data-ptm-step]");
      if (!el) return;
      e.preventDefault();
      onSelect(Number(el.getAttribute("data-ptm-step")), true);
    });
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
      '<span class="ptm-eyebrow-right">A Clear Path to Agreement</span>' +
      "</div>" +
      '<h2 class="ptm-title" id="ptm-h2">From Opportunity to Signed Path In Clear Steps.</h2>' +
      '<p class="ptm-lead">Dealality organizes the opportunity on platform—and <em>facilitates the offline conversations that actually close deals</em>. People meet in person or on a call; Dealality keeps those moments prepared, tracked, and connected to the process.</p>' +
      '<div class="ptm-legend">' +
      '<span class="lg-offline">Outside Platform</span>' +
      '<span class="lg-platform">In Platform</span>' +
      "</div>" +
      '<div class="ptm-stage" id="ptm-stage">' +
      '<p class="ptm-hint">Hover any step to learn more</p>' +
      '<div class="ptm-canvas">' +
      '<div class="ptm-scroll"><div class="ptm-chart" id="ptm-chart"></div></div>' +
      '<aside class="ptm-detail" id="ptm-detail" aria-live="polite" hidden></aside>' +
      "</div></div>" +
      "</div>";

    var stage = document.getElementById("ptm-stage");
    var detail = document.getElementById("ptm-detail");
    var active = -1;
    var swapTimer = null;
    var hideTimer = null;
    var locked = false;

    function dockForStep(i) {
      if (i < 0) return "center";
      // Early steps → panel on the right; late steps → left; keep nearby steps free.
      if (i <= 2) return "right";
      if (i >= 5) return "left";
      return i <= 3 ? "right" : "left";
    }

    function setDetailDock(i) {
      if (!detail) return;
      detail.classList.remove("is-dock-left", "is-dock-right", "is-dock-center");
      detail.classList.add("is-dock-" + dockForStep(i));
    }

    function setActiveUI(i) {
      var chart = document.getElementById("ptm-chart");
      if (chart) {
        chart.classList.toggle("is-focusing", i >= 0);
        if (typeof chart._ptmSetProgress === "function") chart._ptmSetProgress(i);
      }
      setDetailDock(i);
      host.querySelectorAll("circle[data-ptm-step]").forEach(function (el) {
        var idx = Number(el.getAttribute("data-ptm-step"));
        var on = idx === i;
        var done = i >= 0 && idx <= i;
        el.classList.toggle("is-active", on);
        el.classList.toggle("is-done", done);
        el.classList.toggle("is-ahead", i >= 0 && idx > i);
        el.setAttribute("r", String(on ? NODE_R_ACTIVE : NODE_R));
      });
      host.querySelectorAll("article[data-ptm-step]").forEach(function (el) {
        var idx = Number(el.getAttribute("data-ptm-step"));
        var on = idx === i;
        var done = i >= 0 && idx <= i;
        el.classList.toggle("is-active", on);
        el.classList.toggle("is-done", done);
        el.classList.toggle("is-ahead", i >= 0 && idx > i);
      });
      host.querySelectorAll("[data-ptm-label]").forEach(function (el) {
        var idx = Number(el.getAttribute("data-ptm-label"));
        el.classList.toggle("is-active", idx === i);
        el.classList.toggle("is-done", i >= 0 && idx <= i);
      });
      host.querySelectorAll("[data-ptm-col]").forEach(function (el) {
        var idx = Number(el.getAttribute("data-ptm-col"));
        el.classList.toggle("is-active-col", idx === i);
      });
    }

    function openOverlay() {
      stage.classList.add("is-previewing");
      detail.hidden = false;
      detail.classList.add("is-open");
      detail.removeAttribute("hidden");
    }

    function closeOverlay() {
      locked = false;
      stage.classList.remove("is-previewing");
      detail.classList.remove("is-open");
      detail.classList.remove("is-swapping");
      setActiveUI(-1);
      setTimeout(function () {
        if (!detail.classList.contains("is-open")) detail.hidden = true;
      }, 220);
    }

    function showStep(i, lock) {
      if (i < 0 || i >= STEPS.length) return;
      if (hideTimer) {
        clearTimeout(hideTimer);
        hideTimer = null;
      }
      openOverlay();
      if (lock) locked = true;
      if (i === active && detail.querySelector(".ptm-d-title")) {
        setActiveUI(i);
        detail.classList.add("is-open");
        detail.classList.remove("is-swapping");
        return;
      }
      active = i;
      setActiveUI(i);
      var step = STEPS[i];
      detail.classList.add("is-swapping");
      if (swapTimer) clearTimeout(swapTimer);
      swapTimer = setTimeout(function () {
        detail.classList.toggle("is-offline", step.mode === "offline");
        detail.innerHTML =
          '<button type="button" class="ptm-detail-close" aria-label="Close step detail">&times;</button>' +
          detailHtml(step);
        detail.classList.remove("is-swapping");
        detail.classList.add("is-open");
        var closeBtn = detail.querySelector(".ptm-detail-close");
        if (closeBtn) closeBtn.addEventListener("click", closeOverlay);
      }, 120);
    }

    function scheduleHide() {
      if (locked) return;
      if (hideTimer) clearTimeout(hideTimer);
      hideTimer = setTimeout(closeOverlay, 180);
    }

    buildChart(document.getElementById("ptm-chart"), function (i, lock) {
      showStep(i, lock);
    });

    stage.addEventListener("mouseleave", scheduleHide);
    detail.addEventListener("mouseenter", function () {
      if (hideTimer) {
        clearTimeout(hideTimer);
        hideTimer = null;
      }
    });
    detail.addEventListener("mouseleave", scheduleHide);
    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape") closeOverlay();
    });
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
