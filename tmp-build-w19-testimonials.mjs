import fs from "fs";
import crypto from "crypto";
import https from "https";

const src = "public/marketing/dealality-old-home-freeform-head.v20260729w18.css";
const dest = "public/marketing/dealality-old-home-freeform-head.v20260729w19.css";

let css = fs.readFileSync(src, "utf8");

const start = css.indexOf("/* Testimonials — Varko Clients Feedback carousel */");
const end = css.indexOf("#mod-1-icon,#mod-2-icon");
if (start < 0 || end < 0 || end <= start) {
  console.error("markers missing", { start, end });
  process.exit(1);
}

const testimonialsBlock = `/* Testimonials — two rectangular quote tiles (Varko carousel) */
#trust,#testimonials,.oh-testimonials{
  position:relative!important;
  overflow:hidden!important;
  padding:96px 1.5rem 88px!important;
  background:#080F25!important;
  color:#e8ecf5!important;
  border-top:1px solid rgba(255,255,255,.06)!important;
  border-bottom:1px solid rgba(255,255,255,.06)!important;
}
#testimonials-particles{
  position:absolute!important;
  left:0!important;
  top:12%!important;
  width:min(42vw,420px)!important;
  height:min(52vh,520px)!important;
  pointer-events:none!important;
  z-index:0!important;
  opacity:.85!important;
  background:
    radial-gradient(circle at 18% 22%,rgba(236,72,153,.55) 0 2px,transparent 3px),
    radial-gradient(circle at 32% 38%,rgba(168,85,247,.45) 0 1.5px,transparent 2.5px),
    radial-gradient(circle at 12% 58%,rgba(109,92,216,.5) 0 2px,transparent 3px),
    radial-gradient(circle at 28% 72%,rgba(236,72,153,.35) 0 1.5px,transparent 2.5px),
    radial-gradient(circle at 8% 84%,rgba(139,144,255,.4) 0 2px,transparent 3px);
  animation:ohTestimonialParticles 14s ease-in-out infinite alternate!important;
}
@keyframes ohTestimonialParticles{
  0%{transform:translateY(0) scale(1);opacity:.75}
  100%{transform:translateY(-12px) scale(1.03);opacity:.95}
}
#testimonials-glow{
  position:absolute!important;
  left:50%!important;
  top:58%!important;
  width:min(92vw,980px)!important;
  height:340px!important;
  transform:translate(-50%,-50%)!important;
  border-radius:50%!important;
  background:radial-gradient(ellipse at center,rgba(109,92,216,.22) 0%,rgba(109,92,216,.08) 42%,transparent 72%)!important;
  filter:blur(40px)!important;
  pointer-events:none!important;
  z-index:0!important;
}
#testimonials-inner{
  position:relative!important;
  z-index:1!important;
  max-width:1120px!important;
  margin:0 auto!important;
  text-align:center!important;
}
#testimonials-badges{
  display:flex!important;
  justify-content:center!important;
  margin:0 0 18px!important;
}
#testimonials-badge{
  display:inline-flex!important;
  align-items:center!important;
  overflow:hidden!important;
  border-radius:999px!important;
  border:1px solid rgba(255,255,255,.14)!important;
  background:rgba(8,15,37,.92)!important;
  padding:5px 15px 5px 5px!important;
  box-shadow:0 0 0 1px rgba(109,92,216,.1),0 0 28px rgba(109,92,216,.18)!important;
  font-size:0!important;
}
#testimonials-badge::before{
  content:"About";
  display:inline-flex!important;
  align-items:center!important;
  padding:0 10px!important;
  height:32px!important;
  border-radius:10px!important;
  background:#343259!important;
  color:#fff!important;
  font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;
  font-size:1rem!important;
  font-weight:500!important;
  line-height:1!important;
  text-transform:capitalize!important;
  white-space:nowrap!important;
}
#testimonials-badge::after{
  content:"Built from Real Hotel Decisions";
  display:inline-flex!important;
  align-items:center!important;
  margin-left:15px!important;
  color:#fff!important;
  font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;
  font-size:1rem!important;
  font-weight:500!important;
  line-height:1!important;
  text-transform:capitalize!important;
  white-space:nowrap!important;
}
#testimonials-h2{
  margin:0 auto 16px!important;
  max-width:18em!important;
  font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;
  font-size:clamp(2rem,1.567rem + 1.846vw,3rem)!important;
  line-height:1.22!important;
  font-weight:500!important;
  letter-spacing:-.015em!important;
  color:#fff!important;
  text-transform:none!important;
  text-align:center!important;
}
#testimonials-lead{
  margin:0 auto 48px!important;
  max-width:40rem!important;
  font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;
  font-size:1.05rem!important;
  line-height:1.55!important;
  font-weight:400!important;
  color:rgba(255,255,255,.68)!important;
  text-align:center!important;
}
#testimonials-carousel{
  position:relative!important;
  max-width:1040px!important;
  margin:0 auto!important;
}
#testimonials-viewport{
  position:relative!important;
  height:340px!important;
  min-height:340px!important;
  max-height:340px!important;
}
#testimonials-viewport > div[data-slide]{
  position:absolute!important;
  inset:0!important;
  opacity:0!important;
  visibility:hidden!important;
  transform:translateY(14px)!important;
  transition:opacity .65s ease,transform .65s ease,visibility .65s!important;
  pointer-events:none!important;
}
#testimonials-viewport > div[data-slide].is-active{
  position:relative!important;
  opacity:1!important;
  visibility:visible!important;
  transform:translateY(0)!important;
  pointer-events:auto!important;
}
#testimonials-viewport > div[data-slide] > div,
#testimonials-viewport .oh-testimonial-grid{
  display:grid!important;
  grid-template-columns:repeat(2,minmax(0,1fr))!important;
  gap:22px!important;
  align-items:stretch!important;
  height:100%!important;
  max-width:none!important;
  margin:0!important;
}
#testimonials-viewport > div[data-slide] > div > article:nth-child(n+2),
#testimonials-viewport .oh-testimonial-grid > article:nth-child(n+2){
  display:flex!important;
}
#testimonials-viewport article,
#testimonials-viewport .oh-testimonial-card{
  position:relative!important;
  display:flex!important;
  flex-direction:column!important;
  align-items:center!important;
  justify-content:flex-start!important;
  gap:0!important;
  box-sizing:border-box!important;
  height:320px!important;
  min-height:320px!important;
  max-height:320px!important;
  padding:26px 28px 22px!important;
  border-radius:18px!important;
  background:#0b1228!important;
  background-image:none!important;
  border:1px solid rgba(255,255,255,.08)!important;
  box-shadow:0 14px 36px rgba(0,0,0,.28)!important;
  text-align:center!important;
  overflow:hidden!important;
}
#testimonials-viewport article::before,
#testimonials-viewport .oh-testimonial-card::before{
  content:""!important;
  display:block!important;
  position:absolute!important;
  left:10%!important;
  right:10%!important;
  top:0!important;
  height:1px!important;
  background:linear-gradient(90deg,transparent,rgba(183,162,252,.9),rgba(139,144,255,.85),transparent)!important;
  box-shadow:0 0 14px rgba(139,144,255,.45)!important;
  opacity:1!important;
  z-index:1!important;
}
#testimonials-viewport article img,
#testimonials-viewport .oh-testimonial-card img{
  display:block!important;
  width:56px!important;
  height:56px!important;
  min-width:56px!important;
  min-height:56px!important;
  margin:0 auto 16px!important;
  border-radius:50%!important;
  object-fit:cover!important;
  object-position:center 18%!important;
  border:2px solid rgba(255,255,255,.12)!important;
  box-shadow:0 4px 14px rgba(0,0,0,.3)!important;
  flex-shrink:0!important;
  background:transparent!important;
}
#testimonials-viewport blockquote,
#testimonials-viewport .oh-testimonial-quote{
  margin:0!important;
  padding:0!important;
  width:100%!important;
  flex:1 1 auto!important;
  min-height:0!important;
  max-height:7.6em!important;
  overflow:hidden!important;
  display:-webkit-box!important;
  -webkit-box-orient:vertical!important;
  -webkit-line-clamp:5!important;
  line-clamp:5!important;
  font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;
  font-size:.95rem!important;
  line-height:1.55!important;
  font-weight:400!important;
  font-style:normal!important;
  color:rgba(255,255,255,.88)!important;
  text-align:center!important;
  background:transparent!important;
  background-color:transparent!important;
  background-image:none!important;
  border:0!important;
  border-radius:0!important;
  box-shadow:none!important;
  quotes:"\\201C" "\\201D"!important;
}
#testimonials-viewport blockquote::before,
#testimonials-viewport .oh-testimonial-quote::before{
  content:open-quote!important;
  font-style:normal!important;
  color:rgba(255,255,255,.88)!important;
}
#testimonials-viewport blockquote::after,
#testimonials-viewport .oh-testimonial-quote::after{
  content:close-quote!important;
  font-style:normal!important;
  color:rgba(255,255,255,.88)!important;
}
#testimonials-viewport article > *:not(img):not(blockquote):not(p),
#testimonials-viewport .oh-testimonial-card > *:not(img):not(blockquote):not(p){
  background:transparent!important;
  background-image:none!important;
  box-shadow:none!important;
}
#testimonials-viewport article p,
#testimonials-viewport .oh-testimonial-card p{
  position:relative!important;
  display:block!important;
  width:100%!important;
  max-width:none!important;
  margin:18px auto 0!important;
  padding:18px 8px 0!important;
  border:0!important;
  border-top:0!important;
  font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;
  font-size:.92rem!important;
  line-height:1.4!important;
  color:rgba(255,255,255,.72)!important;
  text-align:center!important;
  flex-shrink:0!important;
  background:transparent!important;
}
#testimonials-viewport article p::before,
#testimonials-viewport .oh-testimonial-card p::before{
  content:""!important;
  position:absolute!important;
  left:6%!important;
  right:6%!important;
  top:0!important;
  height:1px!important;
  background-image:linear-gradient(90deg,transparent,rgba(255,255,255,.28) 50%,transparent)!important;
  background-color:transparent!important;
}
#testimonials-viewport article p::after,
#testimonials-viewport .oh-testimonial-card p::after{
  content:""!important;
  position:absolute!important;
  left:50%!important;
  top:-1.5px!important;
  transform:translateX(-50%)!important;
  width:90px!important;
  height:4px!important;
  border-radius:70px!important;
  background-image:linear-gradient(90deg,transparent,#6C72FF 50%,transparent)!important;
  filter:blur(1.5px)!important;
  pointer-events:none!important;
}
#testimonials-viewport article p strong,
#testimonials-viewport .oh-testimonial-card p strong{
  color:#fff!important;
  font-weight:650!important;
}
#testimonials-dots{
  display:flex!important;
  justify-content:center!important;
  align-items:center!important;
  gap:10px!important;
  margin-top:28px!important;
}
.oh-testimonial-dot{
  width:10px!important;
  height:10px!important;
  padding:0!important;
  border:0!important;
  border-radius:50%!important;
  background:rgba(255,255,255,.28)!important;
  cursor:pointer!important;
  transition:transform .25s ease,background .25s ease,box-shadow .25s ease!important;
}
.oh-testimonial-dot.is-active{
  background:#fff!important;
  box-shadow:0 0 0 4px rgba(255,255,255,.12)!important;
  transform:scale(1.05)!important;
}
.oh-testimonial-dot:focus-visible{
  outline:2px solid #8B90FF!important;
  outline-offset:3px!important;
}
@media(max-width:900px){
  #testimonials,.oh-testimonials{padding:72px 1.25rem 64px!important}
  #testimonials-viewport{
    height:auto!important;
    min-height:0!important;
    max-height:none!important;
  }
  #testimonials-viewport > div[data-slide] > div,
  #testimonials-viewport .oh-testimonial-grid{
    grid-template-columns:1fr!important;
    gap:16px!important;
    height:auto!important;
  }
  #testimonials-viewport article,
  #testimonials-viewport .oh-testimonial-card{
    height:300px!important;
    min-height:300px!important;
    max-height:300px!important;
    padding:24px 22px 20px!important;
  }
  #testimonials-lead{margin-bottom:36px!important;font-size:.98rem!important}
  #testimonials-h2{font-size:clamp(1.55rem,5.2vw,2.25rem)!important}
  #testimonials-badge::before,#testimonials-badge::after{font-size:.95rem!important}
  #testimonials-badge::after{margin-left:10px!important}
}
@media(max-width:480px){
  #testimonials-badge{padding:5px 12px 5px 5px!important}
  #testimonials-badge::after{content:"Real Decisions"}
}
@media(prefers-reduced-motion:reduce){
  #testimonials-particles{animation:none!important}
  #testimonials-viewport > div[data-slide]{transition:none!important}
}

`;

css = css.slice(0, start) + testimonialsBlock + css.slice(end);
fs.writeFileSync(dest, css);
const bytes = fs.readFileSync(dest);
const md5 = crypto.createHash("md5").update(bytes).digest("hex");
console.log(
  JSON.stringify(
    {
      dest,
      bytes: bytes.length,
      md5,
      twoCol: css.includes("grid-template-columns:repeat(2,minmax(0,1fr))"),
      noHide: !css.includes("article:nth-child(n+2){display:none"),
      quotes: css.includes('quotes:"\\\\201C"'),
      faqGlow: css.includes("background-image:linear-gradient(90deg,transparent,#6C72FF 50%,transparent)"),
      fixedH: css.includes("height:320px!important"),
      transparentQuote: css.includes("background:transparent!important"),
      heroGutter: css.includes("Left-aligned hero copy") || css.includes("hero-copy"),
      hardCompare: css.includes("Hard to Compare"),
    },
    null,
    2
  )
);
