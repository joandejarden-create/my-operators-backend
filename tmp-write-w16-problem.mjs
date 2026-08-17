import fs from "fs";

const src = "public/marketing/dealality-old-home-freeform-head.v20260729w15.css";
const dest = "public/marketing/dealality-old-home-freeform-head.v20260729w16.css";
let css = fs.readFileSync(src, "utf8");

const start = css.indexOf("/* Problem (#about)");
const end = css.indexOf("/* Testimonials — Varko Clients Feedback carousel */");
if (start < 0 || end < 0) {
  console.error("markers not found", start, end);
  process.exit(1);
}

const replacement = `/* Problem (#about) — header + Owners Gain-style cards + fragmented evaluation visual */
#about,.oh-about{
  padding:100px 1.5rem!important;
  background:#0D1530!important;
  position:relative!important;
  overflow:hidden!important;
  border-top:1px solid rgba(255,255,255,.07)!important;
  border-bottom:1px solid rgba(255,255,255,.07)!important;
}
#about-inner,.oh-about-inner{
  max-width:1120px!important;
  margin:0 auto!important;
  display:grid!important;
  grid-template-columns:1.05fr .95fr!important;
  gap:1.25rem 2.75rem!important;
  align-items:start!important;
  text-align:left!important;
  position:relative!important;
  z-index:1!important;
}
#about-copy{
  display:contents!important;
}
#about-badge,.oh-section-badge#about-badge{
  grid-column:1 / -1!important;
  justify-self:center!important;
  display:inline-flex!important;
  align-items:center!important;
  overflow:hidden!important;
  border-radius:999px!important;
  border:1px solid rgba(255,255,255,.14)!important;
  background:rgba(8,15,37,.92)!important;
  padding:5px 15px 5px 5px!important;
  box-shadow:0 0 0 1px rgba(109,92,216,.1),0 0 28px rgba(109,92,216,.18)!important;
  margin:0 0 14px!important;
  font-size:0!important;
  line-height:1!important;
  letter-spacing:normal!important;
  text-transform:none!important;
  color:transparent!important;
  font-weight:500!important;
}
#about-badge::before{
  content:"The Problem";
  display:inline-flex!important;
  align-items:center!important;
  padding:0 12px!important;
  height:32px!important;
  border-radius:10px!important;
  background:#343259!important;
  color:#fff!important;
  font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;
  font-size:1rem!important;
  font-weight:500!important;
  line-height:1!important;
  text-transform:none!important;
  white-space:nowrap!important;
}
#about-badge::after{
  content:"Manual. Fragmented. Hard to Compare.";
  display:inline-flex!important;
  align-items:center!important;
  margin-left:14px!important;
  color:#fff!important;
  font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;
  font-size:.95rem!important;
  font-weight:500!important;
  line-height:1.25!important;
  text-transform:none!important;
  white-space:nowrap!important;
}
#about-h2,.oh-section-h2#about-h2{
  grid-column:1 / -1!important;
  margin:0 auto 18px!important;
  max-width:22em!important;
  font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;
  font-size:clamp(1.85rem,1.45rem + 1.6vw,2.75rem)!important;
  line-height:1.22!important;
  font-weight:500!important;
  letter-spacing:-.015em!important;
  color:#fff!important;
  text-align:center!important;
  text-transform:none!important;
}
#about-lead,.oh-section-lead#about-lead{
  grid-column:1 / -1!important;
  display:block!important;
  margin:0 auto 44px!important;
  max-width:46rem!important;
  font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;
  font-size:1.05rem!important;
  line-height:1.65!important;
  font-weight:400!important;
  color:rgba(255,255,255,.68)!important;
  text-align:center!important;
}
#about-lead-2,#about-close{
  display:none!important;
}
#about-points,.oh-about-points{
  grid-column:1!important;
  margin:0!important;
  padding:0!important;
  list-style:none!important;
  display:flex!important;
  flex-direction:column!important;
  gap:1rem!important;
}
#about-points li,.oh-about-point{
  display:block!important;
  padding:1.45rem 1.35rem 1.35rem!important;
  border:1px solid rgba(255,255,255,.08)!important;
  border-left:1px solid rgba(255,255,255,.08)!important;
  background:linear-gradient(180deg,#0E1630 0%,#0A1228 100%)!important;
  border-radius:18px!important;
  box-shadow:0 0 0 1px rgba(0,0,0,.2)!important;
  color:rgba(255,255,255,.72)!important;
  font-size:.95rem!important;
  line-height:1.55!important;
  box-sizing:border-box!important;
}
#about-points .about-point-icon,#about-point-1-icon,#about-point-2-icon,#about-point-3-icon{
  width:56px!important;
  height:56px!important;
  min-width:56px!important;
  min-height:56px!important;
  display:flex!important;
  align-items:center!important;
  justify-content:center!important;
  border-radius:999px!important;
  margin:0 0 1rem!important;
  background:radial-gradient(circle at 50% 45%,rgba(140,146,255,.55) 0%,rgba(108,114,255,.22) 42%,rgba(108,114,255,.06) 68%,transparent 78%)!important;
  box-shadow:0 0 28px rgba(108,114,255,.28)!important;
  border:none!important;
}
#about-points .about-point-icon svg,#about-point-1-icon svg,#about-point-2-icon svg,#about-point-3-icon svg{
  width:40px!important;
  height:40px!important;
  flex-shrink:0!important;
}
#about-points li strong{
  display:block!important;
  margin:0 0 .45rem!important;
  color:#fff!important;
  font-size:1.08rem!important;
  font-weight:600!important;
  line-height:1.3!important;
}
#about-points li strong::before{
  content:none!important;
}
#about-points li span{
  display:block!important;
  color:rgba(255,255,255,.55)!important;
  font-weight:400!important;
  font-size:.95rem!important;
  line-height:1.55!important;
}
#about-visual,.oh-about-visual{
  grid-column:2!important;
  position:relative!important;
  min-height:420px!important;
  padding:1.35rem 1.2rem 1.45rem!important;
  border-radius:20px!important;
  background:linear-gradient(165deg,#0E1630 0%,#0A1228 55%,#0C1430 100%)!important;
  border:1px solid rgba(255,255,255,.08)!important;
  color:#e8ecf5!important;
  display:flex!important;
  flex-direction:column!important;
  align-items:stretch!important;
  gap:0!important;
  box-sizing:border-box!important;
  overflow:hidden!important;
}
#about-visual::before{
  content:""!important;
  position:absolute!important;
  inset:-20% auto auto 35%!important;
  width:280px!important;
  height:280px!important;
  border-radius:50%!important;
  background:rgba(108,114,255,.14)!important;
  filter:blur(60px)!important;
  pointer-events:none!important;
  z-index:0!important;
}
#about-visual-label,.oh-about-visual-label,
#hv-s1,#hv-s2,#hv-s3,#hv-s4,.oh-hv-s1,.oh-hv-s2,.oh-hv-s3,.oh-hv-s4{
  display:none!important;
}
#about-frag{
  position:relative!important;
  z-index:1!important;
  display:flex!important;
  flex-direction:column!important;
  gap:1rem!important;
  height:100%!important;
  min-height:390px!important;
}
#about-frag-eyebrow{
  margin:0!important;
  font-size:.72rem!important;
  letter-spacing:.1em!important;
  text-transform:uppercase!important;
  color:rgba(232,236,245,.48)!important;
  font-weight:600!important;
}
#about-frag-anchor{
  align-self:center!important;
  margin:0 0 .15rem!important;
  padding:.7rem 1.15rem!important;
  border-radius:12px!important;
  border:1px solid rgba(108,114,255,.35)!important;
  background:rgba(108,114,255,.14)!important;
  color:#fff!important;
  font-weight:650!important;
  font-size:.92rem!important;
  text-align:center!important;
  box-shadow:0 0 24px rgba(108,114,255,.18)!important;
  animation:aboutFragPulse 4.8s ease-in-out infinite!important;
}
#about-frag-scatter{
  position:relative!important;
  flex:1 1 auto!important;
  min-height:210px!important;
  margin:0 -.15rem!important;
}
#about-frag-scatter .about-frag-chip{
  position:absolute!important;
  max-width:46%!important;
  padding:.48rem .7rem!important;
  border-radius:10px!important;
  border:1px solid rgba(255,255,255,.1)!important;
  background:rgba(17,27,58,.88)!important;
  color:rgba(255,255,255,.86)!important;
  font-size:.78rem!important;
  font-weight:550!important;
  line-height:1.3!important;
  white-space:nowrap!important;
  box-shadow:0 8px 22px rgba(0,0,0,.22)!important;
  opacity:0!important;
  transform:translate3d(0,10px,0) rotate(var(--r,0deg))!important;
  animation:aboutFragIn .9s cubic-bezier(.22,.7,.25,1) forwards, aboutFragDrift 7.5s ease-in-out infinite!important;
  animation-delay:var(--d,0s), calc(var(--d,0s) + .9s)!important;
}
#about-frag-scatter .about-frag-chip.is-channel{
  border-color:rgba(108,114,255,.28)!important;
  background:rgba(108,114,255,.11)!important;
}
#about-frag-scatter .about-frag-chip.is-format{
  border-color:rgba(255,255,255,.12)!important;
  background:rgba(8,15,37,.9)!important;
  color:rgba(255,255,255,.72)!important;
  font-weight:500!important;
}
#about-frag-scatter .c1{top:4%;left:2%;--r:-4deg;--d:.15s}
#about-frag-scatter .c2{top:2%;right:3%;left:auto;--r:5deg;--d:.35s}
#about-frag-scatter .c3{top:28%;left:6%;--r:3deg;--d:.55s}
#about-frag-scatter .c4{top:24%;right:4%;left:auto;--r:-6deg;--d:.75s}
#about-frag-scatter .c5{top:52%;left:0;--r:-2deg;--d:.95s}
#about-frag-scatter .c6{top:48%;right:1%;left:auto;--r:4deg;--d:1.15s}
#about-frag-scatter .c7{top:74%;left:8%;--r:6deg;--d:1.35s}
#about-frag-scatter .c8{top:70%;right:6%;left:auto;--r:-5deg;--d:1.55s}
#about-frag-verdict{
  display:flex!important;
  flex-direction:column!important;
  gap:.55rem!important;
  margin-top:auto!important;
}
#about-frag-hard{
  margin:0!important;
  text-align:center!important;
  font-size:.92rem!important;
  font-weight:650!important;
  color:#fff!important;
  opacity:0!important;
  animation:aboutFragIn .8s ease forwards!important;
  animation-delay:1.7s!important;
}
#about-frag-diffs{
  display:flex!important;
  flex-wrap:wrap!important;
  justify-content:center!important;
  gap:.4rem!important;
}
#about-frag-diffs span{
  display:inline-flex!important;
  align-items:center!important;
  padding:.35rem .65rem!important;
  border-radius:999px!important;
  border:1px solid rgba(108,114,255,.25)!important;
  background:rgba(108,114,255,.08)!important;
  color:rgba(255,255,255,.72)!important;
  font-size:.72rem!important;
  font-weight:500!important;
  opacity:0!important;
  animation:aboutFragIn .7s ease forwards!important;
}
#about-frag-diffs span:nth-child(1){animation-delay:1.9s!important}
#about-frag-diffs span:nth-child(2){animation-delay:2.05s!important}
#about-frag-diffs span:nth-child(3){animation-delay:2.2s!important}
#about-frag-hidden{
  margin:.15rem 0 0!important;
  text-align:center!important;
  font-size:.8rem!important;
  font-weight:550!important;
  letter-spacing:.01em!important;
  color:rgba(155,138,251,.92)!important;
  opacity:0!important;
  animation:aboutFragIn .8s ease forwards!important;
  animation-delay:2.4s!important;
}
@keyframes aboutFragIn{
  from{opacity:0;transform:translate3d(0,12px,0) rotate(var(--r,0deg))}
  to{opacity:1;transform:translate3d(0,0,0) rotate(var(--r,0deg))}
}
@keyframes aboutFragDrift{
  0%,100%{transform:translate3d(0,0,0) rotate(var(--r,0deg))}
  50%{transform:translate3d(0,-5px,0) rotate(calc(var(--r,0deg) + 1.5deg))}
}
@keyframes aboutFragPulse{
  0%,100%{box-shadow:0 0 18px rgba(108,114,255,.16)}
  50%{box-shadow:0 0 28px rgba(108,114,255,.28)}
}
@media(prefers-reduced-motion:reduce){
  #about-frag-anchor,#about-frag-scatter .about-frag-chip,#about-frag-hard,#about-frag-diffs span,#about-frag-hidden{
    animation:none!important;
    opacity:1!important;
    transform:none!important;
  }
}
@media(max-width:960px){
  #about,.oh-about{padding:72px 1.25rem!important}
  #about-inner,.oh-about-inner{grid-template-columns:1fr!important;gap:1.5rem!important}
  #about-h2,.oh-section-h2#about-h2{font-size:clamp(1.55rem,5.2vw,2.25rem)!important;margin-bottom:14px!important}
  #about-lead,.oh-section-lead#about-lead{font-size:.98rem!important;margin:0 auto 36px!important}
  #about-points,.oh-about-points,#about-visual,.oh-about-visual{grid-column:1!important}
  #about-visual,.oh-about-visual{min-height:380px!important}
  #about-badge::before,#about-badge::after{font-size:.9rem!important}
  #about-badge::after{margin-left:10px!important;white-space:normal!important;max-width:14rem!important}
}
@media(max-width:480px){
  #about-badge{padding:5px 12px 5px 5px!important;flex-wrap:wrap!important;max-width:100%!important}
  #about-badge::after{content:"Manual. Fragmented. Hard to Compare.";max-width:11.5rem!important}
  #about-frag-scatter .about-frag-chip{max-width:52%!important;font-size:.72rem!important;padding:.42rem .58rem!important}
  #about-frag-scatter{min-height:230px!important}
}

`;

css = css.slice(0, start) + replacement + css.slice(end);
fs.writeFileSync(dest, css);
console.log("wrote", dest, "bytes", css.length);
console.log("has Hard to Compare", css.includes("Hard to Compare."));
console.log("has about-frag", css.includes("#about-frag"));
console.log("no Difficult", !css.includes("Difficult to Compare"));
