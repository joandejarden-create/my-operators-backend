#!/usr/bin/env python3
"""Generate desktop crops + stylized mobile presentations for Many Futures.

Desktop: tighter product-truth crops filling most of the preview frame.
Mobile: marketing-quality reconstructions using only existing Dealality fields.
"""
from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageEnhance, ImageFilter, ImageFont

ROOT = Path(__file__).resolve().parents[1] / "assets"
OUT = ROOT / "crops"
OUT.mkdir(exist_ok=True)

BG = (8, 15, 37)
PANEL = (17, 27, 58)
PANEL2 = (13, 21, 48)
BORDER = (40, 52, 90)
ACCENT = (108, 114, 255)
ACCENT_SOFT = (139, 144, 255)
TEXT = (255, 255, 255)
MUTED = (170, 180, 210)
MUTED2 = (120, 132, 168)
GOLD = (232, 196, 92)


def font(size: int, bold: bool = False):
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except OSError:
            continue
    return ImageFont.load_default()


def soft_focus(img: Image.Image, box, dim=0.42, glow_pad=10, glow_blur=14) -> Image.Image:
    base = img.convert("RGBA")
    overlay = Image.new("RGBA", base.size, (8, 15, 37, int(255 * dim)))
    dimmed = Image.alpha_composite(base, overlay)
    x0, y0, x1, y1 = [int(v) for v in box]
    x0, y0 = max(0, x0), max(0, y0)
    x1, y1 = min(base.width, x1), min(base.height, y1)
    clear = base.crop((x0, y0, x1, y1))
    dimmed.paste(clear, (x0, y0))
    glow = Image.new("RGBA", base.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(glow)
    d.rounded_rectangle(
        [max(0, x0 - glow_pad), max(0, y0 - glow_pad), min(base.width, x1 + glow_pad), min(base.height, y1 + glow_pad)],
        radius=12,
        fill=(108, 114, 255, 42),
    )
    glow = glow.filter(ImageFilter.GaussianBlur(glow_blur))
    out = Image.alpha_composite(dimmed, glow)
    out.paste(clear, (x0, y0))
    edge = Image.new("RGBA", base.size, (0, 0, 0, 0))
    de = ImageDraw.Draw(edge)
    de.rounded_rectangle([x0, y0, x1 - 1, y1 - 1], radius=8, outline=(139, 144, 255, 65), width=1)
    return Image.alpha_composite(out, edge).convert("RGB")


def export(img: Image.Image, path: Path, max_w: int) -> Path:
    w, h = img.size
    if w > max_w:
        nh = int(round(h * (max_w / w)))
        img = img.resize((max_w, nh), Image.Resampling.LANCZOS)
    img = ImageEnhance.Sharpness(img).enhance(1.08)
    path = path.with_suffix(".png")
    img.save(path, "PNG", optimize=True)
    print(f"  {path.name}: {img.size[0]}x{img.size[1]} ({path.stat().st_size // 1024}KB)")
    return path


def rounded_rect(draw, xy, radius, fill=None, outline=None, width=1):
    draw.rounded_rectangle(xy, radius=radius, fill=fill, outline=outline, width=width)


def wrap_text(draw, text, fnt, max_w):
    words = text.split()
    lines, cur = [], ""
    for w in words:
        trial = (cur + " " + w).strip()
        if draw.textlength(trial, font=fnt) <= max_w:
            cur = trial
        else:
            if cur:
                lines.append(cur)
            cur = w
    if cur:
        lines.append(cur)
    return lines


def card_base(w, h):
    img = Image.new("RGB", (w, h), BG)
    draw = ImageDraw.Draw(img)
    # subtle top sheen
    for i in range(80):
        a = int(18 * (1 - i / 80))
        draw.line([(0, i), (w, i)], fill=(12 + a // 3, 20 + a // 3, 48 + a // 2))
    return img, draw


def draw_label(draw, x, y, text="INTERFACE SIMPLIFIED FOR PRESENTATION"):
    f = font(15, True)
    pad_x, pad_y = 12, 7
    tw = draw.textlength(text, font=f)
    rounded_rect(
        draw,
        [x, y, x + tw + pad_x * 2, y + 15 + pad_y * 2],
        999,
        fill=(12, 18, 40),
        outline=(70, 78, 120),
        width=1,
    )
    draw.text((x + pad_x, y + pad_y), text, font=f, fill=MUTED2)
    return y + 15 + pad_y * 2 + 14


def metric_grid(draw, x, y, w, metrics, cols=2, row_h=78):
    gap = 12
    col_w = (w - gap * (cols - 1)) / cols
    f_lab = font(14, True)
    f_val = font(24, True)
    for i, (lab, val) in enumerate(metrics):
        c = i % cols
        r = i // cols
        mx = x + c * (col_w + gap)
        my = y + r * (row_h + gap)
        rounded_rect(
            draw,
            [mx, my, mx + col_w, my + row_h],
            12,
            fill=PANEL,
            outline=BORDER,
            width=1,
        )
        draw.text((mx + 14, my + 12), lab.upper(), font=f_lab, fill=MUTED2)
        # wrap value if needed
        lines = wrap_text(draw, val, f_val, col_w - 28)
        ty = my + 34
        for line in lines[:2]:
            draw.text((mx + 14, ty), line, font=f_val, fill=TEXT)
            ty += 26
    rows = (len(metrics) + cols - 1) // cols
    return y + rows * (row_h + gap)


def content_bottom(img: Image.Image, thresh: int = 42) -> int:
    """Last row with non-background pixels."""
    px = img.load()
    w, h = img.size
    for y in range(h - 1, -1, -1):
        for x in range(0, w, 6):
            r, g, b = px[x, y][:3]
            if abs(r - BG[0]) + abs(g - BG[1]) + abs(b - BG[2]) > thresh:
                return y
    return h - 1


def finalize_mobile(img: Image.Image, bottom_pad: int = 28) -> Image.Image:
    """Crop empty canvas so the presentation fills the frame."""
    bottom = min(img.height - 1, content_bottom(img) + bottom_pad)
    return img.crop((0, 0, img.width, max(bottom + 1, 480)))


def make_operator_mobile():
    w, h = 780, 900
    img, draw = card_base(w, h)
    y = 22
    y = draw_label(draw, 22, y)

    # identity panel — large type for 390px readability
    rounded_rect(draw, [22, y, w - 22, y + 188], 16, fill=PANEL, outline=BORDER, width=1)
    draw.text((42, y + 20), "Cenote Azul Operadores", font=font(32, True), fill=TEXT)
    draw.text((42, y + 62), "Operator Explorer", font=font(16, True), fill=ACCENT_SOFT)
    pos = "Riviera logic with Yucatán practicality."
    draw.text((42, y + 94), pos, font=font(22, True), fill=ACCENT_SOFT)
    blurb = "Branded select-service along the Cancún–Tulum corridor. CALA-heavy with a disciplined US border pipeline."
    by = y + 128
    for line in wrap_text(draw, blurb, font(17), w - 92):
        draw.text((42, by), line, font=font(17), fill=MUTED)
        by += 22
    y += 208

    metrics = [
        ("Headquarters", "Mérida, Mexico"),
        ("Years in Business", "13"),
        ("Hotels Managed", "19"),
        ("Rooms Managed", "3,120"),
        ("Asset Focus", "Resort · Select Service · Boutique"),
        ("Brand Mix", "62% branded / 38% independent"),
    ]
    metric_grid(draw, 22, y, w - 44, metrics, cols=2, row_h=96)
    return finalize_mobile(img)


def make_rebrand_mobile():
    w, h = 780, 900
    img, draw = card_base(w, h)
    y = 22
    y = draw_label(draw, 22, y)
    rounded_rect(draw, [22, y, w - 22, y + 118], 16, fill=PANEL, outline=BORDER, width=1)
    draw.text((42, y + 20), "Brand Explorer", font=font(30, True), fill=TEXT)
    draw.text((42, y + 64), "Brand Positioning", font=font(19, True), fill=ACCENT_SOFT)
    draw.text((42, y + 92), "Upper-upscale hospitality experience", font=font(17), fill=MUTED)
    y += 138

    blocks = [
        (
            "Positioning",
            "We exist to redefine the upper-upscale hospitality experience—style with substance, innovation with comfort, and belonging in an elevated environment.",
        ),
        (
            "Audience",
            "Experience-oriented inspired professionals seeking Scandinavian-inspired design, seamless technology, and stimulating spaces.",
        ),
    ]
    for title, body in blocks:
        lines = wrap_text(draw, body, font(18), w - 92)
        box_h = 58 + 24 * len(lines)
        rounded_rect(draw, [22, y, w - 22, y + box_h], 14, fill=PANEL, outline=BORDER, width=1)
        draw.text((42, y + 16), title.upper(), font=font(14, True), fill=MUTED2)
        by = y + 44
        for line in lines:
            draw.text((42, by), line, font=font(18), fill=TEXT)
            by += 24
        y += box_h + 14

    rounded_rect(draw, [22, y, w - 22, y + 88], 14, fill=PANEL2, outline=BORDER, width=1)
    draw.text((42, y + 16), "WHERE THIS BRAND CREATES THE MOST VALUE", font=font(13, True), fill=MUTED2)
    draw.text((42, y + 48), "Visual brand environments from Brand Explorer", font=font(17), fill=MUTED)
    return finalize_mobile(img)


def make_soft_brand_mobile():
    w, h = 780, 900
    img, draw = card_base(w, h)
    y = 22
    y = draw_label(draw, 22, y)
    rounded_rect(draw, [22, y, w - 22, y + 128], 16, fill=PANEL, outline=BORDER, width=1)
    draw.text((42, y + 20), "Fee Estimator", font=font(30, True), fill=TEXT)
    draw.text((42, y + 62), "Your Results · Upper-Midscale tier", font=font(18, True), fill=ACCENT_SOFT)
    draw.text((42, y + 94), "Royalty 5% · Marketing 3%", font=font(17), fill=MUTED)
    y += 148

    metrics = [
        ("Total Franchise Fees", "$3.78M"),
        ("Effective Fee Rate", "12.1%"),
        ("Yr1 Recurring Fees", "$267k"),
        ("Amort. Ann. Cost", "$378k"),
        ("Fee Assessment", "Higher"),
        ("Yr1 Gross Room Rev.", "$2.63M"),
    ]
    metric_grid(draw, 22, y, w - 44, metrics, cols=2, row_h=100)
    return finalize_mobile(img)


def make_independent_mobile():
    w, h = 780, 900
    img, draw = card_base(w, h)
    y = 22
    y = draw_label(draw, 22, y)
    rounded_rect(draw, [22, y, w - 22, y + 128], 16, fill=PANEL, outline=BORDER, width=1)
    draw.text((42, y + 20), "Dealality Radar", font=font(30, True), fill=TEXT)
    draw.text((42, y + 62), "Summary Statistics", font=font(18, True), fill=ACCENT_SOFT)
    draw.text((42, y + 94), "Hotel status · Open · Pipeline · Candidates", font=font(17), fill=MUTED)
    y += 148

    metrics = [
        ("Total Hotels", "15,692"),
        ("Total Rooms", "1,519,066"),
        ("Open Hotels", "14,938"),
        ("Open Rooms", "1,402,174"),
        ("Pipeline", "752"),
        ("Candidates", "0"),
    ]
    y = metric_grid(draw, 22, y, w - 44, metrics, cols=2, row_h=96)
    rounded_rect(draw, [22, y + 4, w - 22, y + 108], 14, fill=PANEL2, outline=BORDER, width=1)
    draw.text((42, y + 22), "HOTEL STATUS", font=font(14, True), fill=MUTED2)
    legend = [("Open Hotels", (80, 140, 255)), ("Pipeline", (220, 80, 90)), ("Candidate/LOI", (160, 110, 230))]
    lx = 42
    for label, color in legend:
        draw.ellipse([lx, y + 56, lx + 14, y + 70], fill=color)
        draw.text((lx + 22, y + 52), label, font=font(16), fill=MUTED)
        lx += 230
    return finalize_mobile(img)


def make_branded_residences_mobile():
    w, h = 780, 900
    img, draw = card_base(w, h)
    y = 22
    y = draw_label(draw, 22, y)
    rounded_rect(draw, [22, y, w - 22, y + 132], 16, fill=PANEL, outline=BORDER, width=1)
    draw.text((42, y + 18), "Opportunity Review", font=font(30, True), fill=TEXT)
    draw.text((42, y + 60), "Deal Brief · New Build", font=font(18, True), fill=ACCENT_SOFT)
    draw.text((42, y + 92), "162 Rooms · Both Brands And Third-Party Operators", font=font(16), fill=MUTED)
    y += 152

    opp = "The current inputs describe a new-build hospitality opportunity that may be relevant for brand and operator screening, with potential fit across Upper Midscale and adjacent brand pathways."
    lines = wrap_text(draw, opp, font(17), w - 92)
    box_h = 52 + 22 * len(lines)
    rounded_rect(draw, [22, y, w - 22, y + box_h], 14, fill=PANEL, outline=BORDER, width=1)
    draw.text((42, y + 16), "THE OPPORTUNITY", font=font(14, True), fill=GOLD)
    by = y + 42
    for line in lines:
        draw.text((42, by), line, font=font(17), fill=TEXT)
        by += 22
    y += box_h + 14

    metrics = [
        ("Amenities", "Mid-Rise · 3 F&B · Parking"),
        ("Rooms", "148 Standard, 14 Suites"),
        ("Service Model", "Select-Service"),
        ("Stories", "6"),
        ("Meeting", "4,500 Sq. Ft."),
        ("Deal Structure", "Franchise Only"),
    ]
    metric_grid(draw, 22, y, w - 44, metrics, cols=2, row_h=92)
    return finalize_mobile(img)


def framed_desktop(piece: Image.Image, canvas_w: int = 1280, canvas_h: int = 520, height_fill: float = 0.82) -> Image.Image:
    """Place a product crop on a navy canvas so UI fills ~70–80% of the preview frame."""
    canvas = Image.new("RGB", (canvas_w, canvas_h), BG)
    pw, ph = piece.size
    if pw < 8 or ph < 8:
        return canvas
    scale = (canvas_h * height_fill) / ph
    # Prefer filling height; if that under-fills width badly, bump toward width fill.
    width_fill = (pw * scale) / canvas_w
    if width_fill < 0.72:
        scale = (canvas_w * 0.88) / pw
    nw = max(1, int(round(pw * scale)))
    nh = max(1, int(round(ph * scale)))
    scaled = piece.resize((nw, nh), Image.Resampling.LANCZOS)
    if nw > canvas_w or nh > canvas_h:
        left = max(0, (nw - canvas_w) // 2)
        top = max(0, (nh - canvas_h) // 2)
        scaled = scaled.crop((left, top, left + min(nw, canvas_w), top + min(nh, canvas_h)))
        nw, nh = scaled.size
    x = (canvas_w - nw) // 2
    y = (canvas_h - nh) // 2
    canvas.paste(scaled, (x, y))
    return canvas


def make_desktop_crops():
    """Tighter desktop frames so meaningful UI fills ~70–80% of the preview area."""
    specs = {
        # Operator: identity + positioning + metric strip (exclude tabs / lower sections)
        "new-operator": {
            "src": "operator-explorer.png",
            # Identity + positioning + metric strip only (exclude application tabs).
            "crop": (0, 0, 1024, 240),
            "focus": (14, 118, 1010, 232),
            "height_fill": 0.88,
        },
        # Brand: positioning + audience panels, less nav chrome
        "rebrand": {
            "src": "brand-explorer.png",
            "crop": (40, 70, 984, 420),
            "focus": (20, 20, 900, 300),
            "height_fill": 0.84,
        },
        # Fee: results cards dominant
        "soft-brand": {
            "src": "fee-estimator.png",
            "crop": (300, 40, 1010, 430),
            "focus": (16, 40, 690, 360),
            "height_fill": 0.84,
        },
        # Radar: summary stats + legend
        "independent": {
            "src": "radar.png",
            "crop": (420, 90, 1010, 500),
            "focus": (20, 40, 560, 380),
            "height_fill": 0.84,
        },
        # Opportunity: metrics + summary cards
        "branded-residences": {
            "src": "opportunity-review.png",
            "crop": (0, 70, 1024, 420),
            "focus": (20, 20, 980, 330),
            "height_fill": 0.84,
        },
    }
    print("=== DESKTOP ===")
    for name, s in specs.items():
        im = Image.open(ROOT / s["src"]).convert("RGB")
        piece = soft_focus(im.crop(s["crop"]), s["focus"])
        framed = framed_desktop(piece, 1280, 520, s.get("height_fill", 0.82))
        export(framed, OUT / f"{name}-desktop", 1280)


def main():
    make_desktop_crops()
    print("=== MOBILE PRESENTATIONS ===")
    makers = {
        "new-operator": make_operator_mobile,
        "rebrand": make_rebrand_mobile,
        "soft-brand": make_soft_brand_mobile,
        "independent": make_independent_mobile,
        "branded-residences": make_branded_residences_mobile,
    }
    for name, fn in makers.items():
        img = fn()
        export(img, OUT / f"{name}-mobile", 780)
    print("done")


if __name__ == "__main__":
    main()
