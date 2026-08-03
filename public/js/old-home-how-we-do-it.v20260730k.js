/**
 * Old Home — How We Do It compact one-screen process (v20260730k)
 * Path-gated to /old-home.
 * Click left nav to swap the right step in place. No scroll runway.
 * Right panel sized so no step needs a scrollbar.
 * Namespace: dealality-process_*
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    var CSS_TEXT = "/* How We Do It — compact one-screen, no right-panel scroll (v20260730k)\r\n   Namespace: dealality-process_*\r\n   Click left nav to swap right panel in place. No scroll runway / no step scrollbars. */\r\n#oh-how-we-do-it.dealality-process_section,\r\n#oh-how-we-do-it{\r\n  position:relative!important;\r\n  overflow:hidden!important;\r\n  padding:40px 1.5rem 32px!important;\r\n  background:#080F25!important;\r\n  color:#e8ecf5!important;\r\n  border-top:1px solid rgba(255,255,255,.07)!important;\r\n  border-bottom:1px solid rgba(255,255,255,.07)!important;\r\n  min-height:0!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_glow{\r\n  position:absolute!important;left:50%!important;top:18%!important;\r\n  width:min(92vw,900px)!important;height:240px!important;\r\n  transform:translate(-50%,-50%)!important;border-radius:50%!important;\r\n  background:radial-gradient(ellipse at center,rgba(108,114,255,.14) 0%,rgba(108,114,255,.04) 46%,transparent 72%)!important;\r\n  filter:blur(36px)!important;pointer-events:none!important;z-index:0!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_inner{\r\n  position:relative!important;z-index:1!important;max-width:1120px!important;margin:0 auto!important;\r\n  display:flex!important;flex-direction:column!important;gap:0!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_intro{margin:0 0 14px!important;max-width:760px!important}\r\n#oh-how-we-do-it .dealality-process_eyebrow{\r\n  display:inline-flex!important;\r\n  align-items:center!important;\r\n  overflow:hidden!important;\r\n  border-radius:999px!important;\r\n  border:1px solid rgba(255,255,255,.14)!important;\r\n  background:rgba(8,15,37,.92)!important;\r\n  padding:4px 12px 4px 4px!important;\r\n  box-shadow:0 0 0 1px rgba(109,92,216,.1),0 0 28px rgba(109,92,216,.18)!important;\r\n  margin:0 0 10px!important;\r\n  gap:0!important;\r\n  flex-wrap:nowrap!important;\r\n  max-width:100%!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_eyebrow-pill{\r\n  display:inline-flex!important;\r\n  align-items:center!important;\r\n  padding:0 10px!important;\r\n  height:26px!important;\r\n  min-height:26px!important;\r\n  border:0!important;\r\n  border-radius:10px!important;\r\n  background:#343259!important;\r\n  font-family:\"Inter Tight\",\"Plus Jakarta Sans\",system-ui,sans-serif!important;\r\n  font-size:.84rem!important;\r\n  font-weight:500!important;\r\n  letter-spacing:normal!important;\r\n  line-height:1!important;\r\n  text-transform:none!important;\r\n  white-space:nowrap!important;\r\n  color:#fff!important;\r\n  flex:0 0 auto!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_eyebrow-right{\r\n  display:inline-flex!important;\r\n  align-items:center!important;\r\n  margin-left:12px!important;\r\n  font-family:\"Inter Tight\",\"Plus Jakarta Sans\",system-ui,sans-serif!important;\r\n  font-size:.82rem!important;\r\n  font-weight:500!important;\r\n  line-height:1.25!important;\r\n  text-transform:none!important;\r\n  white-space:nowrap!important;\r\n  color:#fff!important;\r\n}\r\n@media(max-width:900px){\r\n  #oh-how-we-do-it .dealality-process_eyebrow{flex-wrap:wrap!important;padding:4px 10px 4px 4px!important}\r\n  #oh-how-we-do-it .dealality-process_eyebrow-right{\r\n    white-space:normal!important;margin:6px 2px 2px 8px!important;\r\n  }\r\n}\r\n#oh-how-we-do-it .dealality-process_h2{\r\n  margin:0 0 6px!important;\r\n  font-family:\"Plus Jakarta Sans\",\"Inter Tight\",system-ui,sans-serif!important;\r\n  font-size:clamp(1.25rem,2.2vw,1.7rem)!important;font-weight:800!important;\r\n  line-height:1.18!important;letter-spacing:-.03em!important;color:#fff!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_lead{\r\n  margin:0!important;max-width:42rem!important;\r\n  font-family:\"Plus Jakarta Sans\",\"Inter Tight\",system-ui,sans-serif!important;\r\n  font-size:.88rem!important;line-height:1.45!important;color:rgba(255,255,255,.58)!important;\r\n}\r\n\r\n/* Compact stage — no tall scroll track */\r\n#oh-how-we-do-it .dealality-process_stage-track{\r\n  position:relative!important;\r\n  height:auto!important;\r\n  margin:0 0 14px!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_stage-pin{\r\n  position:relative!important;\r\n  top:auto!important;\r\n  height:auto!important;\r\n  max-height:none!important;\r\n  display:block!important;\r\n  overflow:visible!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_layout{\r\n  display:grid!important;\r\n  grid-template-columns:minmax(180px,.26fr) minmax(0,.74fr)!important;\r\n  gap:12px 22px!important;\r\n  align-items:start!important;\r\n  width:100%!important;\r\n  min-height:0!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_nav-col{\r\n  position:relative!important;min-width:0!important;\r\n  display:block!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_nav{\r\n  position:relative!important;\r\n  display:flex!important;flex-direction:column!important;gap:2px!important;padding:2px 0 2px 14px!important;\r\n  width:100%!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_progress{\r\n  position:absolute!important;left:4px!important;top:8px!important;bottom:8px!important;width:2px!important;\r\n  background:rgba(255,255,255,.1)!important;border-radius:2px!important;overflow:hidden!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_progress > span{\r\n  display:block!important;width:100%!important;height:0%;background:linear-gradient(180deg,#8B90FF,#6C72FF)!important;\r\n  transition:height .22s ease!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_nav-item{\r\n  appearance:none!important;-webkit-appearance:none!important;border:0!important;background:transparent!important;\r\n  color:rgba(255,255,255,.48)!important;text-align:left!important;cursor:pointer!important;\r\n  display:grid!important;grid-template-columns:auto 1fr!important;gap:8px!important;align-items:start!important;\r\n  padding:6px 8px!important;border-radius:10px!important;\r\n  font-family:\"Plus Jakarta Sans\",\"Inter Tight\",system-ui,sans-serif!important;\r\n  transition:color .2s ease,background .2s ease!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_nav-item:hover{color:rgba(255,255,255,.78)!important;background:rgba(255,255,255,.03)!important}\r\n#oh-how-we-do-it .dealality-process_nav-item:focus-visible{outline:2px solid #6C72FF!important;outline-offset:2px!important}\r\n#oh-how-we-do-it .dealality-process_nav-item.is-active{color:#fff!important;background:rgba(108,114,255,.12)!important}\r\n#oh-how-we-do-it .dealality-process_nav-item.is-done{color:rgba(255,255,255,.7)!important}\r\n#oh-how-we-do-it .dealality-process_nav-num{\r\n  font-size:.7rem!important;font-weight:700!important;letter-spacing:.04em!important;color:#8B90FF!important;min-width:1.5em!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_nav-item.is-active .dealality-process_nav-num{color:#B8BCFF!important}\r\n#oh-how-we-do-it .dealality-process_nav-label{\r\n  font-size:.8rem!important;font-weight:650!important;line-height:1.25!important;\r\n}\r\n\r\n/* Right panel — never scroll; content is sized to fit */\r\n#oh-how-we-do-it .dealality-process_content{\r\n  position:relative!important;min-width:0!important;min-height:0!important;\r\n  height:auto!important;\r\n  max-height:none!important;\r\n  overflow:hidden!important;\r\n  scrollbar-width:none!important;\r\n  border-radius:12px!important;\r\n  border:1px solid rgba(255,255,255,.08)!important;\r\n  background:rgba(10,16,36,.35)!important;\r\n  padding:10px 12px 10px!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_content::-webkit-scrollbar{display:none!important;width:0!important;height:0!important}\r\n#oh-how-we-do-it .dealality-process_step{\r\n  display:none!important;\r\n  opacity:0!important;\r\n  position:relative!important;\r\n  min-height:0!important;\r\n  padding:0!important;margin:0!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_step.is-active{\r\n  display:block!important;\r\n  opacity:1!important;\r\n  animation:ohProcessStepIn .28s ease both!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_step[hidden]{display:none!important}\r\n@keyframes ohProcessStepIn{\r\n  from{opacity:0;transform:translateY(6px)}\r\n  to{opacity:1;transform:none}\r\n}\r\n#oh-how-we-do-it .dealality-process_kicker{\r\n  margin:0 0 2px!important;font-size:.65rem!important;font-weight:700!important;letter-spacing:.08em!important;\r\n  text-transform:uppercase!important;color:#8B90FF!important;\r\n  font-family:\"Inter Tight\",\"Plus Jakarta Sans\",system-ui,sans-serif!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_step-title{\r\n  margin:0 0 4px!important;\r\n  font-family:\"Plus Jakarta Sans\",\"Inter Tight\",system-ui,sans-serif!important;\r\n  font-size:clamp(1rem,1.5vw,1.25rem)!important;font-weight:800!important;line-height:1.2!important;color:#fff!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_primary{\r\n  margin:0 0 8px!important;font-size:.84rem!important;font-weight:600!important;line-height:1.35!important;color:rgba(255,255,255,.88)!important;\r\n}\r\n/* Support copy + label chips take vertical space — hide so mockups fit */\r\n#oh-how-we-do-it .dealality-process_support,\r\n#oh-how-we-do-it .dealality-process_labels{\r\n  display:none!important;\r\n}\r\n\r\n#oh-how-we-do-it .dealality-process_visual{\r\n  position:relative!important;border-radius:10px!important;border:1px solid rgba(255,255,255,.12)!important;\r\n  background:linear-gradient(180deg,rgba(17,27,58,.98),rgba(10,16,36,.99))!important;\r\n  box-shadow:inset 0 1px 0 rgba(255,255,255,.06),0 10px 22px rgba(0,0,0,.24)!important;\r\n  overflow:hidden!important;min-height:0!important;margin:0!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_visual.is-large,\r\n#oh-how-we-do-it .dealality-process_visual.is-compare{min-height:0!important}\r\n#oh-how-we-do-it .dealality-process_chrome{\r\n  display:flex!important;align-items:center!important;gap:5px!important;\r\n  padding:5px 8px!important;border-bottom:1px solid rgba(255,255,255,.08)!important;\r\n  background:rgba(8,15,37,.65)!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_chrome span{\r\n  width:6px!important;height:6px!important;border-radius:50%!important;background:rgba(255,255,255,.22)!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_chrome span:nth-child(1){background:#ff5f57!important}\r\n#oh-how-we-do-it .dealality-process_chrome span:nth-child(2){background:#febc2e!important}\r\n#oh-how-we-do-it .dealality-process_chrome span:nth-child(3){background:#28c840!important}\r\n#oh-how-we-do-it .dealality-process_chrome em{\r\n  margin-left:5px!important;font-style:normal!important;font-size:.62rem!important;\r\n  letter-spacing:.04em!important;color:rgba(255,255,255,.45)!important;\r\n  font-family:\"Inter Tight\",\"Plus Jakarta Sans\",system-ui,sans-serif!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_shot{\r\n  max-height:64px!important;overflow:hidden!important;border-bottom:1px solid rgba(255,255,255,.06)!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_shot.is-compare-shot{max-height:72px!important}\r\n#oh-how-we-do-it .dealality-process_visual img,\r\n#oh-how-we-do-it .dealality-process_shot img{\r\n  display:block!important;width:100%!important;height:100%!important;max-height:72px!important;\r\n  object-fit:cover!important;object-position:top left!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_panel{padding:7px 9px 8px!important;display:grid!important;gap:5px!important}\r\n#oh-how-we-do-it .dealality-process_panel-head{\r\n  display:flex!important;justify-content:space-between!important;gap:8px!important;align-items:center!important;flex-wrap:wrap!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_chip{\r\n  display:inline-flex!important;align-items:center!important;padding:2px 7px!important;border-radius:999px!important;\r\n  border:1px solid rgba(108,114,255,.28)!important;background:rgba(108,114,255,.12)!important;\r\n  color:#B8BCFF!important;font-size:.62rem!important;font-weight:700!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_meta{font-size:.66rem!important;color:rgba(255,255,255,.5)!important}\r\n#oh-how-we-do-it .dealality-process_hero-stat{\r\n  display:flex!important;flex-direction:column!important;gap:1px!important;\r\n  padding:6px 8px!important;border-radius:8px!important;\r\n  border:1px solid rgba(108,114,255,.22)!important;\r\n  background:linear-gradient(135deg,rgba(108,114,255,.16),rgba(255,255,255,.03))!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_hero-stat strong{font-size:.95rem!important;color:#fff!important;letter-spacing:-.02em!important}\r\n#oh-how-we-do-it .dealality-process_hero-stat span{font-size:.68rem!important;color:rgba(255,255,255,.55)!important}\r\n#oh-how-we-do-it .dealality-process_progress-bar{\r\n  height:4px!important;border-radius:999px!important;background:rgba(255,255,255,.08)!important;overflow:hidden!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_progress-bar i{\r\n  display:block!important;height:100%!important;border-radius:inherit!important;\r\n  background:linear-gradient(90deg,#8B90FF,#6C72FF)!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_rows{display:grid!important;gap:4px!important}\r\n#oh-how-we-do-it .dealality-process_row{\r\n  display:grid!important;grid-template-columns:1.1fr 1fr!important;gap:8px!important;align-items:center!important;\r\n  padding:5px 7px!important;border-radius:7px!important;border:1px solid rgba(255,255,255,.07)!important;\r\n  background:rgba(255,255,255,.025)!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_row strong{font-size:.72rem!important;font-weight:650!important;color:#fff!important}\r\n#oh-how-we-do-it .dealality-process_row span{font-size:.68rem!important;color:rgba(255,255,255,.55)!important;text-align:right!important}\r\n#oh-how-we-do-it .dealality-process_paths{display:grid!important;grid-template-columns:repeat(3,minmax(0,1fr))!important;gap:5px!important}\r\n#oh-how-we-do-it .dealality-process_path{\r\n  padding:7px!important;border-radius:8px!important;border:1px solid rgba(255,255,255,.08)!important;\r\n  background:rgba(8,15,37,.55)!important;min-height:0!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_path strong{display:block!important;color:#fff!important;font-size:.72rem!important;margin:0 0 2px!important}\r\n#oh-how-we-do-it .dealality-process_path span{display:block!important;color:rgba(255,255,255,.5)!important;font-size:.64rem!important;line-height:1.3!important}\r\n#oh-how-we-do-it .dealality-process_compare{\r\n  display:grid!important;grid-template-columns:1.1fr repeat(3,minmax(0,1fr))!important;gap:1px!important;\r\n  background:rgba(255,255,255,.08)!important;border-radius:8px!important;overflow:hidden!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_compare > *{\r\n  background:#0d1530!important;padding:5px 6px!important;font-size:.64rem!important;color:rgba(255,255,255,.7)!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_compare .is-head{color:#fff!important;font-weight:700!important;background:#111b3a!important}\r\n#oh-how-we-do-it .dealality-process_compare .is-gap{color:#F0C674!important}\r\n\r\n/* Slim CTA bar — stays in the same screen */\r\n#oh-how-we-do-it .dealality-process_cta{\r\n  margin-top:0!important;padding:12px 14px!important;border-radius:12px!important;\r\n  border:1px solid rgba(255,255,255,.1)!important;\r\n  background:linear-gradient(135deg,rgba(108,114,255,.12),rgba(17,27,58,.4) 55%,rgba(8,15,37,.2))!important;\r\n  text-align:left!important;\r\n  display:grid!important;\r\n  grid-template-columns:minmax(0,1.2fr) auto!important;\r\n  gap:10px 16px!important;\r\n  align-items:center!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_cta h3{\r\n  margin:0 0 2px!important;font-size:clamp(.98rem,1.5vw,1.15rem)!important;font-weight:800!important;color:#fff!important;\r\n  font-family:\"Plus Jakarta Sans\",\"Inter Tight\",system-ui,sans-serif!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_cta p{\r\n  margin:0!important;max-width:36rem!important;font-size:.78rem!important;line-height:1.4!important;color:rgba(255,255,255,.58)!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_cta-row{\r\n  display:flex!important;flex-wrap:wrap!important;gap:8px!important;justify-content:flex-end!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_btn{\r\n  display:inline-flex!important;align-items:center!important;justify-content:center!important;\r\n  min-height:36px!important;padding:0 12px!important;border-radius:10px!important;border:0!important;cursor:pointer!important;\r\n  font-family:\"Plus Jakarta Sans\",\"Inter Tight\",system-ui,sans-serif!important;font-size:.82rem!important;font-weight:700!important;\r\n  text-decoration:none!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_btn-primary{background:#6C72FF!important;color:#fff!important}\r\n#oh-how-we-do-it .dealality-process_btn-primary:hover{background:#7B80FF!important}\r\n#oh-how-we-do-it .dealality-process_btn-secondary{\r\n  background:transparent!important;color:#fff!important;border:1px solid rgba(255,255,255,.18)!important;\r\n}\r\n#oh-how-we-do-it .dealality-process_sr{\r\n  position:absolute!important;width:1px!important;height:1px!important;padding:0!important;margin:-1px!important;\r\n  overflow:hidden!important;clip:rect(0,0,0,0)!important;border:0!important;\r\n}\r\n\r\n@media(max-width:960px){\r\n  #oh-how-we-do-it{padding:36px 1.25rem 28px!important}\r\n  #oh-how-we-do-it .dealality-process_layout{\r\n    display:flex!important;flex-direction:column!important;gap:12px!important;\r\n  }\r\n  #oh-how-we-do-it .dealality-process_nav{\r\n    flex-direction:row!important;flex-wrap:wrap!important;padding:0!important;gap:6px!important;\r\n  }\r\n  #oh-how-we-do-it .dealality-process_progress{display:none!important}\r\n  #oh-how-we-do-it .dealality-process_nav-item{padding:6px 8px!important;border:1px solid rgba(255,255,255,.08)!important}\r\n  #oh-how-we-do-it .dealality-process_content{max-height:none!important;overflow:hidden!important}\r\n  #oh-how-we-do-it .dealality-process_cta{\r\n    grid-template-columns:1fr!important;\r\n  }\r\n  #oh-how-we-do-it .dealality-process_cta-row{justify-content:flex-start!important}\r\n  #oh-how-we-do-it .dealality-process_paths{grid-template-columns:1fr 1fr 1fr!important}\r\n  #oh-how-we-do-it .dealality-process_row{grid-template-columns:1fr!important}\r\n  #oh-how-we-do-it .dealality-process_row span{text-align:left!important}\r\n}\r\n@media(max-width:640px){\r\n  #oh-how-we-do-it .dealality-process_compare{display:block!important}\r\n  #oh-how-we-do-it .dealality-process_compare > *{border-bottom:1px solid rgba(255,255,255,.06)!important}\r\n  #oh-how-we-do-it .dealality-process_paths{grid-template-columns:1fr!important}\r\n}\r\n@media(prefers-reduced-motion:reduce){\r\n  #oh-how-we-do-it .dealality-process_step.is-active{animation:none!important}\r\n  #oh-how-we-do-it .dealality-process_progress > span,\r\n  #oh-how-we-do-it .dealality-process_nav-item{transition:none!important}\r\n}\r\n@media(max-height:740px){\r\n  #oh-how-we-do-it{padding:28px 1.5rem 24px!important}\r\n  #oh-how-we-do-it .dealality-process_lead,\r\n  #oh-how-we-do-it .dealality-process_cta p{display:none!important}\r\n  #oh-how-we-do-it .dealality-process_shot,\r\n  #oh-how-we-do-it .dealality-process_shot.is-compare-shot{max-height:48px!important}\r\n  #oh-how-we-do-it .dealality-process_shot img{max-height:48px!important}\r\n}\r\n";
    if (window.__ohHowWeDoIt >= 202607314) return;
    window.__ohHowWeDoIt = 202607314;
    var IMG_BRAND =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a679518a66ce83bcb18be55_brand-explorer.png";
    var IMG_COMPARE =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a679519982910d5c314bf1f_deal-compare.png";

    var STEPS = [
      {
        n: "01",
        title: "Define the Opportunity",
        primary:
          "Clarify what the owner wants the asset to achieve before the first available relationship begins shaping the direction.",
        support:
          "Bring the hotel, market context, objectives, constraints, commercial priorities, and decision criteria into one structured opportunity brief.",
        labels: [
          "Asset and Market Context",
          "Owner Objectives",
          "Commercial Priorities",
          "Control Preferences",
          "Timing and Constraints",
          "Decision Criteria",
        ],
        visual: "define",
      },
      {
        n: "02",
        title: "Explore Credible Paths",
        primary: "See more than the first available option.",
        support:
          "Identify the brand, operator, conversion, positioning, operating-model, capital, and strategic-partner paths that may be worth evaluating.",
        labels: [
          "Brand Paths",
          "Operator Paths",
          "Conversion Options",
          "Operating Models",
          "Capital Partners",
          "Strategic Alternatives",
        ],
        visual: "paths",
        img: IMG_BRAND,
        alt: "Exploring credible brand and operator paths for one hotel opportunity",
      },
      {
        n: "03",
        title: "Prepare and Engage",
        primary: "Present the opportunity consistently to the right participants.",
        support:
          "Prepare the opportunity, select relevant participants, manage confidentiality, coordinate outreach, and capture questions and responses in one place.",
        labels: [
          "Opportunity Brief",
          "Selected Participants",
          "Confidentiality",
          "Outreach Status",
          "Information Requests",
          "Response Tracking",
        ],
        visual: "engage",
      },
      {
        n: "04",
        title: "Compare What Matters",
        primary: "Turn different proposals into a meaningful comparison.",
        support:
          "Compare economics, control, requirements, support, timing, missing terms, and material trade-offs on a shared basis.",
        labels: [
          "Fees and Economics",
          "Owner Control",
          "Capital Support",
          "Brand Requirements",
          "Operating Terms",
          "Missing Information",
        ],
        visual: "compare",
        img: IMG_COMPARE,
        alt: "Side-by-side proposal comparison across fees, control, and missing terms",
      },
      {
        n: "05",
        title: "Pursue the Selected Direction",
        primary: "Move the preferred path forward with clarity and leverage.",
        support:
          "Identify negotiation priorities, resolve remaining gaps, preserve the decision record, and support the owner through the agreed milestone.",
        labels: [
          "Preferred Direction",
          "Negotiation Priorities",
          "Remaining Conditions",
          "Decision Rationale",
          "Next Actions",
          "Decision Record",
        ],
        visual: "pursue",
      },
    ];

    function ensureCss() {
      var links = document.querySelectorAll('link[href*="dealality-old-home-how-we-do-it"]');
      for (var i = 0; i < links.length; i++) links[i].disabled = true;
      if (document.querySelector('style[data-oh-how="30k"]')) return;
      var style = document.createElement("style");
      style.setAttribute("data-oh-how", "30k");
      style.textContent = CSS_TEXT;
      (document.head || document.documentElement).appendChild(style);
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
          '<span class="dealality-process_meta">Demo · Coastal Boutique Hotel</span>' +
          "</div>" +
          '<div class="dealality-process_hero-stat"><strong>118 keys</strong><span>Upscale boutique · Greater Santo Domingo</span></div>' +
          '<div class="dealality-process_rows">' +
          '<div class="dealality-process_row"><strong>Opportunity Type</strong><span>Brand + operator review</span></div>' +
          '<div class="dealality-process_row"><strong>Owner Objectives</strong><span>Stabilize NOI · Preserve control</span></div>' +
          '<div class="dealality-process_row"><strong>Constraints</strong><span>Capex limited · 18-month horizon</span></div>' +
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
          '<span class="dealality-process_meta">One hotel · several futures</span>' +
          "</div>" +
          '<div class="dealality-process_paths">' +
          '<div class="dealality-process_path"><strong>Soft Brand</strong><span>Fit · Capex moderate</span></div>' +
          '<div class="dealality-process_path"><strong>Operator Partner</strong><span>Model lift · Control</span></div>' +
          '<div class="dealality-process_path"><strong>Conversion</strong><span>Positioning · Evidence</span></div>' +
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
          '<span class="dealality-process_meta">One opportunity · one consistent story</span>' +
          "</div>" +
          '<div class="dealality-process_progress-bar" aria-hidden="true"><i style="width:62%"></i></div>' +
          '<div class="dealality-process_rows">' +
          '<div class="dealality-process_row"><strong>Selected Participants</strong><span>3 brands · 2 operators</span></div>' +
          '<div class="dealality-process_row"><strong>Confidentiality</strong><span>NDA active · gated materials</span></div>' +
          '<div class="dealality-process_row"><strong>Outreach Status</strong><span>2 contacted · 1 ready</span></div>' +
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
          '<span class="dealality-process_meta">Economics · control · gaps</span>' +
          "</div>" +
          '<div class="dealality-process_compare" role="table" aria-label="Proposal differences">' +
          '<div class="is-head" role="columnheader">Category</div><div class="is-head" role="columnheader">Path A</div><div class="is-head" role="columnheader">Path B</div><div class="is-head" role="columnheader">Path C</div>' +
          "<div>Fees</div><div>4.5% + 2%</div><div>5.0% + 1%</div><div>3.8% + 3%</div>" +
          "<div>Owner Control</div><div>High</div><div>Medium</div><div>Shared</div>" +
          '<div>Missing Terms</div><div class="is-gap">Capex TBD</div><div>—</div><div class="is-gap">Exit TBD</div>' +
          "</div></div></div>"
        );
      }
      return (
        '<div class="dealality-process_visual is-large" aria-label="Selected direction workspace">' +
        '<div class="dealality-process_chrome" aria-hidden="true"><span></span><span></span><span></span><em>Preferred Direction · Decision Record</em></div>' +
        '<div class="dealality-process_panel">' +
        '<div class="dealality-process_panel-head">' +
        '<span class="dealality-process_chip">Preferred Direction</span>' +
        '<span class="dealality-process_meta">Owner remains decision-maker</span>' +
        "</div>" +
        '<div class="dealality-process_rows">' +
        '<div class="dealality-process_row"><strong>Preferred Direction</strong><span>Soft brand + operator support</span></div>' +
        '<div class="dealality-process_row"><strong>Key Reasons</strong><span>Control · brand lift · timeline</span></div>' +
        '<div class="dealality-process_row"><strong>Next Action</strong><span>Owner review · counsel align</span></div>' +
        "</div></div></div>"
      );
    }

    function stepHtml(step, i) {
      return (
        '<article class="dealality-process_step' +
        (i === 0 ? " is-active" : "") +
        '" id="dealality-process-step-' +
        (i + 1) +
        '" data-dealality-process-step="' +
        i +
        '">' +
        '<p class="dealality-process_kicker">Step ' +
        esc(step.n) +
        "</p>" +
        '<h3 class="dealality-process_step-title">' +
        esc(step.title) +
        "</h3>" +
        '<p class="dealality-process_primary">' +
        esc(step.primary) +
        "</p>" +
        '<p class="dealality-process_support">' +
        esc(step.support) +
        "</p>" +
        visualHtml(step) +
        labelsHtml(step.labels) +
        "</article>"
      );
    }

    function buildSection() {
      var nav = STEPS.map(function (step, i) {
        return (
          '<button type="button" class="dealality-process_nav-item' +
          (i === 0 ? " is-active" : "") +
          '" data-dealality-process-nav="' +
          i +
          '" aria-controls="dealality-process-step-' +
          (i + 1) +
          '" aria-current="' +
          (i === 0 ? "step" : "false") +
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
        '" data-oh-how="30k">' +
        '<div class="dealality-process_glow" aria-hidden="true"></div>' +
        '<div class="dealality-process_inner">' +
        '<header class="dealality-process_intro">' +
        '<div class="dealality-process_eyebrow">' +
        '<span class="dealality-process_eyebrow-pill">How Dealality Works</span>' +
        '<span class="dealality-process_eyebrow-right">A Connected Process From Opportunity to Agreement.</span>' +
        "</div>" +
        '<h2 class="dealality-process_h2" id="dealality-process-h2">Turn One Hotel Opportunity Into a Structured Decision Process.</h2>' +
        '<p class="dealality-process_lead">Dealality brings the opportunity, credible strategic paths, market engagement, responses, and decision criteria into one confidential process—helping owners explore more possibilities without losing control or momentum.</p>' +
        "</header>" +
        '<div class="dealality-process_stage-track" id="dealality-process-runway">' +
        '<div class="dealality-process_stage-pin">' +
        '<div class="dealality-process_layout">' +
        '<div class="dealality-process_nav-col">' +
        '<nav class="dealality-process_nav" aria-label="Dealality process steps">' +
        '<div class="dealality-process_progress" aria-hidden="true"><span id="dealality-process-progress"></span></div>' +
        nav +
        "</nav></div>" +
        '<div class="dealality-process_content" id="dealality-process-content">' +
        STEPS.map(stepHtml).join("") +
        "</div>" +
        "</div></div></div>" +
        '<div class="dealality-process_cta">' +
        "<div>" +
        "<h3>One Opportunity. One Connected Process.</h3>" +
        "<p>From the first question to the selected direction, Dealality keeps the opportunity, participants, proposals, and decision criteria connected.</p>" +
        "</div>" +
        '<div class="dealality-process_cta-row">' +
        '<button type="button" class="dealality-process_btn dealality-process_btn-primary" data-dealality-process-cta="explore">Explore Your Opportunity</button>' +
        '<button type="button" class="dealality-process_btn dealality-process_btn-secondary" data-dealality-process-cta="video">See Dealality in Action</button>' +
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
      var html = buildSection();
      if (existing) {
        var wrap = document.createElement("div");
        wrap.innerHTML = html;
        existing.parentNode.replaceChild(wrap.firstChild, existing);
        return true;
      }
      // Prefer insert before Features (#platform-features). Legacy #features
      // Process leftover was removed; keep a soft fallback if it still exists.
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
          btn.classList.toggle("is-active", on);
          btn.classList.toggle("is-done", i < active);
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
            "Step " + STEPS[active].n + ": " + STEPS[active].title;
        }
        if (!viewed[active]) {
          viewed[active] = 1;
          track("process_step_view", {
            step: STEPS[active].n,
            title: STEPS[active].title,
          });
        }
        if (fromUser) {
          track("process_step_click", {
            step: STEPS[active].n,
            title: STEPS[active].title,
          });
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
          if (typeof window.ohOpenOpportunityReview === "function") {
            window.ohOpenOpportunityReview();
            return;
          }
          var orBtn =
            document.querySelector("[data-oh-or-open]") ||
            document.querySelector('a[href*="opportunity"]');
          if (orBtn) orBtn.click();
        });
      }
      var videoCta = section.querySelector('[data-dealality-process-cta="video"]');
      if (videoCta) {
        videoCta.addEventListener("click", function () {
          track("process_cta_click", { cta: "video" });
          var launcher =
            document.querySelector("[data-oh-video-open]") ||
            document.querySelector("#oh-platform-video-launcher") ||
            document.querySelector(".oh-video-launcher");
          if (launcher) launcher.click();
        });
      }

      setActive(0, false);
      track("process_section_view", { section: "how-we-do-it" });
    }

    function run() {
      ensureCss();
      if (!mount()) return;
      bind(document.getElementById("oh-how-we-do-it"));
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", run);
    } else {
      run();
    }
    window.addEventListener("load", run);
    setTimeout(run, 900);
    setTimeout(run, 2200);
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-how-we-do-it]", err);
    }
  }
})();
