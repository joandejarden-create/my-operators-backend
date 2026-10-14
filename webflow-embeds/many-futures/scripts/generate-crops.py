#!/usr/bin/env python3
"""Regenerate curated desktop/mobile product crops from full Dealality screenshots."""
from PIL import Image, ImageDraw
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "assets"
OUT = ROOT / "crops"
OUT.mkdir(exist_ok=True)

def load(path):
    return Image.open(path).convert("RGB")

def dim_outside(im, box, dim=0.42):
    out = im.copy()
    overlay = Image.new("RGBA", out.size, (8, 15, 37, 0))
    d = ImageDraw.Draw(overlay)
    d.rectangle([0, 0, out.width, out.height], fill=(8, 15, 37, int(255 * (1 - dim))))
    x0, y0, x1, y1 = box
    d.rectangle([x0, y0, x1, y1], fill=(0, 0, 0, 0))
    return Image.alpha_composite(out.convert("RGBA"), overlay).convert("RGB")

def draw_highlight(im, box, color=(108, 114, 255), width=3, radius=10):
    out = im.convert("RGBA")
    overlay = Image.new("RGBA", out.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(overlay)
    x0, y0, x1, y1 = box
    for i, a in [(8, 40), (5, 70), (3, 110)]:
        d.rounded_rectangle([x0 - i, y0 - i, x1 + i, y1 + i], radius=radius + i, outline=(*color, a), width=2)
    d.rounded_rectangle([x0, y0, x1, y1], radius=radius, outline=(*color, 220), width=width)
    return Image.alpha_composite(out, overlay).convert("RGB")

def make_crop(src, out_desk, out_mob, region, highlight, desk_size=(1280, 760), mob_size=(780, 980), dim=0.42, mob_focus=None):
    im = load(ROOT / src)
    W, H = im.size
    l, t, r, b = region
    box = (int(W * l), int(H * t), int(W * r), int(H * b))
    cropped = im.crop(box)
    hl, ht, hr, hb = highlight
    cw, ch = cropped.size
    hbox = (int(cw * hl), int(ch * ht), int(cw * hr), int(ch * hb))
    desk = cropped.resize(desk_size, Image.Resampling.LANCZOS)
    sx, sy = desk_size[0] / cw, desk_size[1] / ch
    dhbox = (int(hbox[0] * sx), int(hbox[1] * sy), int(hbox[2] * sx), int(hbox[3] * sy))
    desk = draw_highlight(dim_outside(desk, dhbox, dim=dim), dhbox)
    desk.save(OUT / out_desk, "PNG", optimize=True)
    if mob_focus:
        ml, mt, mr, mb = mob_focus
    else:
        ml = max(0, hl - 0.12); mr = min(1, hr + 0.12)
        mt = max(0, ht - 0.11); mb = min(1, hb + 0.31)
    mbox = (int(cw * ml), int(ch * mt), int(cw * mr), int(ch * mb))
    mob = cropped.crop(mbox).resize(mob_size, Image.Resampling.LANCZOS)
    mw, mh = mbox[2] - mbox[0], mbox[3] - mbox[1]
    mhbox = (
        max(8, int((hbox[0] - mbox[0]) / mw * mob_size[0])),
        max(8, int((hbox[1] - mbox[1]) / mh * mob_size[1])),
        min(mob_size[0] - 8, int((hbox[2] - mbox[0]) / mw * mob_size[0])),
        min(mob_size[1] - 8, int((hbox[3] - mbox[1]) / mh * mob_size[1])),
    )
    mob = draw_highlight(dim_outside(mob, mhbox, dim=dim), mhbox)
    mob.save(OUT / out_mob, "PNG", optimize=True)
    print("wrote", out_desk, out_mob)

make_crop("operator-explorer.png", "new-operator-desktop.png", "new-operator-mobile.png",
          (0.02, 0.0, 0.985, 0.55), (0.01, 0.32, 0.99, 0.62), (1280, 700), (780, 900), 0.5, (0.0, 0.18, 1.0, 0.72))
make_crop("brand-explorer.png", "rebrand-desktop.png", "rebrand-mobile.png",
          (0.02, 0.08, 0.98, 0.92), (0.02, 0.08, 0.98, 0.42), (1280, 760), (780, 1000), 0.45)
make_crop("fee-estimator.png", "soft-brand-desktop.png", "soft-brand-mobile.png",
          (0.0, 0.0, 1.0, 0.70), (0.34, 0.16, 0.985, 0.46), (1280, 740), (780, 960), 0.4, (0.30, 0.10, 1.0, 0.72))
make_crop("radar.png", "independent-desktop.png", "independent-mobile.png",
          (0.0, 0.12, 1.0, 0.92), (0.58, 0.28, 0.985, 0.72), (1280, 760), (780, 1000), 0.4, (0.52, 0.18, 1.0, 0.88))
make_crop("opportunity-review.png", "branded-residences-desktop.png", "branded-residences-mobile.png",
          (0.0, 0.0, 1.0, 0.62), (0.02, 0.18, 0.98, 0.72), (1280, 760), (780, 1000), 0.45)
print("done")
