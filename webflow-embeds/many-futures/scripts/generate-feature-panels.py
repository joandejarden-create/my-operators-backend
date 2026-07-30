#!/usr/bin/env python3
"""Generate Phase B feature panel images for Many Futures.

Screenshot panels (1–7): crop + soft navy frame, object-fit friendly.
Stylized panels (8–11): dark navy Dealality UI reconstructions using only real fields,
with discreet "INTERFACE SIMPLIFIED FOR PRESENTATION" label.

Outputs to assets/features/. Does not touch Webflow or upload.
"""
from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageEnhance, ImageFilter, ImageFont

ROOT = Path(__file__).resolve().parents[1] / "assets"
SOURCES = ROOT / "sources"
OUT = ROOT / "features"
OUT.mkdir(parents=True, exist_ok=True)

BG = (8, 15, 37)  # #080f25
PANEL = (17, 27, 58)  # #111b3a
PANEL2 = (13, 21, 48)
BORDER = (40, 52, 90)
ACCENT = (108, 114, 255)  # #6c72ff
ACCENT_SOFT = (139, 144, 255)
TEXT = (255, 255, 255)
MUTED = (170, 180, 210)
MUTED2 = (120, 132, 168)
GOLD = (232, 196, 92)
GREEN = (72, 168, 120)
ORANGE = (220, 140, 70)
RED = (200, 90, 90)

DESKTOP = (960, 540)
MOBILE_W = 780
MOBILE_H = 520

CREATED: list[tuple[str, int, int, int]] = []


def font(size: int, bold: bool = False):
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        if bold
        else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"
        if bold
        else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except OSError:
            continue
    return ImageFont.load_default()


def resolve_src(*names: str) -> Path:
    for name in names:
        for base in (SOURCES, ROOT):
            p = base / name
            if p.exists():
                return p
    raise FileNotFoundError(f"Missing source among: {names}")


def soft_focus(img: Image.Image, box, dim=0.38, glow_pad=10, glow_blur=14) -> Image.Image:
    base = img.convert("RGBA")
    overlay = Image.new("RGBA", base.size, (*BG, int(255 * dim)))
    dimmed = Image.alpha_composite(base, overlay)
    x0, y0, x1, y1 = [int(v) for v in box]
    x0, y0 = max(0, x0), max(0, y0)
    x1, y1 = min(base.width, x1), min(base.height, y1)
    clear = base.crop((x0, y0, x1, y1))
    dimmed.paste(clear, (x0, y0))
    glow = Image.new("RGBA", base.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(glow)
    d.rounded_rectangle(
        [
            max(0, x0 - glow_pad),
            max(0, y0 - glow_pad),
            min(base.width, x1 + glow_pad),
            min(base.height, y1 + glow_pad),
        ],
        radius=12,
        fill=(*ACCENT, 40),
    )
    glow = glow.filter(ImageFilter.GaussianBlur(glow_blur))
    out = Image.alpha_composite(dimmed, glow)
    out.paste(clear, (x0, y0))
    edge = Image.new("RGBA", base.size, (0, 0, 0, 0))
    de = ImageDraw.Draw(edge)
    de.rounded_rectangle([x0, y0, x1 - 1, y1 - 1], radius=8, outline=(*ACCENT_SOFT, 70), width=1)
    return Image.alpha_composite(out, edge).convert("RGB")


def navy_frame(
    piece: Image.Image,
    size: tuple[int, int],
    pad: int = 18,
    radius: int = 14,
    height_fill: float = 0.88,
) -> Image.Image:
    """Place a product crop on navy canvas with soft frame — object-fit friendly."""
    cw, ch = size
    canvas = Image.new("RGB", (cw, ch), BG)
    draw = ImageDraw.Draw(canvas)
    # subtle top sheen
    for i in range(min(90, ch // 4)):
        a = int(16 * (1 - i / max(1, min(90, ch // 4))))
        draw.line([(0, i), (cw, i)], fill=(12 + a // 3, 20 + a // 3, 48 + a // 2))

    inner_w = cw - pad * 2
    inner_h = ch - pad * 2
    pw, ph = piece.size
    if pw < 4 or ph < 4:
        return canvas

    scale = min(inner_w / pw, (inner_h * height_fill) / ph)
    # Prefer filling most of the frame
    if (pw * scale) / inner_w < 0.78:
        scale = (inner_w * 0.92) / pw
    nw = max(1, int(round(pw * scale)))
    nh = max(1, int(round(ph * scale)))
    scaled = piece.resize((nw, nh), Image.Resampling.LANCZOS)

    if nw > inner_w or nh > inner_h:
        left = max(0, (nw - inner_w) // 2)
        top = max(0, (nh - inner_h) // 2)
        scaled = scaled.crop((left, top, left + min(nw, inner_w), top + min(nh, inner_h)))
        nw, nh = scaled.size

    x = (cw - nw) // 2
    y = (ch - nh) // 2

    # Soft outer frame ring
    frame = Image.new("RGBA", (cw, ch), (0, 0, 0, 0))
    fd = ImageDraw.Draw(frame)
    fd.rounded_rectangle(
        [x - 6, y - 6, x + nw + 5, y + nh + 5],
        radius=radius + 4,
        outline=(*BORDER, 180),
        width=2,
    )
    fd.rounded_rectangle(
        [x - 2, y - 2, x + nw + 1, y + nh + 1],
        radius=radius,
        outline=(*ACCENT, 55),
        width=1,
    )
    canvas = Image.alpha_composite(canvas.convert("RGBA"), frame).convert("RGB")
    canvas.paste(scaled, (x, y))
    return canvas


def content_bottom(img: Image.Image, thresh: int = 42) -> int:
    px = img.load()
    w, h = img.size
    for y in range(h - 1, -1, -1):
        for x in range(0, w, 6):
            r, g, b = px[x, y][:3]
            if abs(r - BG[0]) + abs(g - BG[1]) + abs(b - BG[2]) > thresh:
                return y
    return h - 1


def finalize_mobile(img: Image.Image, bottom_pad: int = 28, min_h: int = 480) -> Image.Image:
    bottom = min(img.height - 1, content_bottom(img) + bottom_pad)
    return img.crop((0, 0, img.width, max(bottom + 1, min_h)))


def export(img: Image.Image, name: str) -> Path:
    path = OUT / f"{name}.png"
    img = ImageEnhance.Sharpness(img.convert("RGB")).enhance(1.06)
    img.save(path, "PNG", optimize=True)
    CREATED.append((path.name, img.size[0], img.size[1], path.stat().st_size))
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
    for i in range(80):
        a = int(18 * (1 - i / 80))
        draw.line([(0, i), (w, i)], fill=(12 + a // 3, 20 + a // 3, 48 + a // 2))
    return img, draw


def draw_label(draw, x, y, text="INTERFACE SIMPLIFIED FOR PRESENTATION"):
    f = font(13, True)
    pad_x, pad_y = 10, 6
    tw = draw.textlength(text, font=f)
    rounded_rect(
        draw,
        [x, y, x + tw + pad_x * 2, y + 13 + pad_y * 2],
        999,
        fill=(12, 18, 40),
        outline=(70, 78, 120),
        width=1,
    )
    draw.text((x + pad_x, y + pad_y), text, font=f, fill=MUTED2)
    return y + 13 + pad_y * 2 + 12


def metric_grid(draw, x, y, w, metrics, cols=2, row_h=72):
    gap = 10
    col_w = (w - gap * (cols - 1)) / cols
    f_lab = font(12, True)
    f_val = font(20, True)
    for i, (lab, val) in enumerate(metrics):
        c = i % cols
        r = i // cols
        mx = x + c * (col_w + gap)
        my = y + r * (row_h + gap)
        rounded_rect(draw, [mx, my, mx + col_w, my + row_h], 10, fill=PANEL, outline=BORDER, width=1)
        draw.text((mx + 12, my + 10), lab.upper(), font=f_lab, fill=MUTED2)
        lines = wrap_text(draw, val, f_val, col_w - 24)
        ty = my + 30
        for line in lines[:2]:
            draw.text((mx + 12, ty), line, font=f_val, fill=TEXT)
            ty += 22
    rows = (len(metrics) + cols - 1) // cols
    return y + rows * (row_h + gap)


# ---------------------------------------------------------------------------
# Screenshot crop panels (1–7)
# ---------------------------------------------------------------------------

CROP_SPECS = {
    # Brand Explorer — positioning / identity
    "brand-explorer": {
        "src": ("brand-explorer.png",),
        "desktop": {
            "crop": (24, 70, 1000, 360),
            "focus": (10, 8, 960, 280),
            "size": DESKTOP,
            "height_fill": 0.90,
        },
        "mobile": {
            "crop": (40, 78, 984, 300),
            "focus": (8, 6, 930, 210),
            "size": (MOBILE_W, MOBILE_H),
            "height_fill": 0.92,
        },
    },
    # Operator Explorer — identity + metric strip
    "operator-explorer": {
        "src": ("operator-explorer.png", "operator-profile.png"),
        "desktop": {
            "crop": (0, 0, 1024, 248),
            "focus": (12, 100, 1010, 236),
            "size": DESKTOP,
            "height_fill": 0.88,
        },
        "mobile": {
            "crop": (0, 0, 1024, 230),
            "focus": (10, 90, 1010, 220),
            "size": (MOBILE_W, MOBILE_H),
            "height_fill": 0.90,
        },
    },
    # Fee Estimator — results cards
    "fee-estimator": {
        "src": ("fee-estimator.png",),
        "desktop": {
            "crop": (300, 48, 1010, 360),
            "focus": (10, 30, 690, 300),
            "size": DESKTOP,
            "height_fill": 0.88,
        },
        "mobile": {
            "crop": (310, 50, 1000, 300),
            "focus": (8, 20, 670, 240),
            "size": (MOBILE_W, MOBILE_H),
            "height_fill": 0.90,
        },
    },
    # Dealality Radar — summary stats + map edge
    "radar": {
        "src": ("radar.png", "market-map.png"),
        "desktop": {
            "crop": (380, 100, 1010, 520),
            "focus": (16, 30, 600, 390),
            "size": DESKTOP,
            "height_fill": 0.90,
        },
        "mobile": {
            "crop": (420, 110, 1008, 500),
            "focus": (10, 24, 560, 360),
            "size": (MOBILE_W, MOBILE_H),
            "height_fill": 0.92,
        },
    },
    # Opportunity Review / Deal Brief
    "opportunity-review": {
        "src": ("deal-brief.png", "opportunity-review.png"),
        "desktop": {
            "crop": (20, 40, 1000, 400),
            "focus": (16, 50, 960, 340),
            "size": DESKTOP,
            "height_fill": 0.88,
        },
        "mobile": {
            "crop": (40, 50, 980, 360),
            "focus": (12, 40, 920, 290),
            "size": (MOBILE_W, MOBILE_H),
            "height_fill": 0.90,
        },
    },
    # Deal Compare
    "deal-compare": {
        "src": ("deal-compare.png",),
        "desktop": {
            "crop": (0, 0, 1024, 463),
            "focus": (8, 8, 1010, 450),
            "size": DESKTOP,
            "height_fill": 0.86,
        },
        "mobile": {
            "crop": (0, 0, 1024, 463),
            "focus": (6, 6, 1010, 450),
            "size": (MOBILE_W, MOBILE_H),
            "height_fill": 0.88,
        },
    },
    # Smart Matching — Match Score & Fit signals only
    "smart-matching": {
        "src": ("matched-brands.png",),
        "desktop": {
            # Preferred Brand + Match Score columns
            "crop": (420, 70, 1020, 500),
            "focus": (20, 40, 580, 410),
            "size": DESKTOP,
            "height_fill": 0.90,
        },
        "mobile": {
            "crop": (480, 70, 1020, 500),
            "focus": (10, 30, 520, 400),
            "size": (MOBILE_W, MOBILE_H),
            "height_fill": 0.92,
        },
    },
}


def make_crop_panel(name: str, variant: str) -> Image.Image:
    spec = CROP_SPECS[name]
    src = resolve_src(*spec["src"])
    v = spec[variant]
    im = Image.open(src).convert("RGB")
    piece = soft_focus(im.crop(v["crop"]), v["focus"])
    return navy_frame(piece, v["size"], height_fill=v.get("height_fill", 0.88))


# ---------------------------------------------------------------------------
# Stylized reconstructions (8–11) — real fields only
# ---------------------------------------------------------------------------


def make_deal_readiness(desktop: bool) -> Image.Image:
    """Deal Readiness Snapshot: score area, priority gaps, commercial inputs."""
    if desktop:
        w, h = DESKTOP
        pad = 22
    else:
        w, h = MOBILE_W, 640
        pad = 18
    img, draw = card_base(w, h)
    y = pad
    y = draw_label(draw, pad, y)

    # Header
    rounded_rect(draw, [pad, y, w - pad, y + (68 if desktop else 78)], 12, fill=PANEL, outline=BORDER)
    draw.text((pad + 16, y + 12), "Deal Readiness Snapshot", font=font(22 if desktop else 24, True), fill=TEXT)
    draw.text(
        (pad + 16, y + 42),
        "Aeropuerto Cancún Select-Service · New Build · Shaping",
        font=font(13 if desktop else 14),
        fill=MUTED,
    )
    y += 84 if desktop else 94

    # Score + commercial inputs row
    score_w = 168 if desktop else w - pad * 2
    if desktop:
        # Score ring card
        rounded_rect(draw, [pad, y, pad + score_w, y + 168], 12, fill=PANEL, outline=BORDER)
        cx, cy, r = pad + score_w // 2, y + 78, 52
        draw.ellipse([cx - r, cy - r, cx + r, cy + r], outline=BORDER, width=8)
        # arc approx via thick accent wedge
        draw.arc([cx - r, cy - r, cx + r, cy + r], start=-90, end=170, fill=ACCENT, width=8)
        draw.text((cx - 28, cy - 18), "72", font=font(28, True), fill=TEXT)
        draw.text((cx - 22, cy + 12), "/ 100", font=font(12), fill=MUTED2)
        draw.text((pad + 28, y + 140), "Final Readiness Score", font=font(12, True), fill=MUTED2)

        # Commercial inputs / domain bars
        bx = pad + score_w + 14
        bw = w - pad - bx
        rounded_rect(draw, [bx, y, w - pad, y + 168], 12, fill=PANEL, outline=BORDER)
        draw.text((bx + 16, y + 12), "COMMERCIAL INPUTS", font=font(12, True), fill=MUTED2)
        bars = [
            ("Project Completeness", 78),
            ("Commercial Inputs", 64),
            ("Market Context", 82),
            ("Partner Fit Signals", 71),
        ]
        by = y + 36
        for lab, pct in bars:
            draw.text((bx + 16, by), lab, font=font(13), fill=MUTED)
            track_x = bx + 200
            track_w = bw - 260
            rounded_rect(draw, [track_x, by + 4, track_x + track_w, by + 14], 4, fill=PANEL2, outline=BORDER)
            fill_w = int(track_w * pct / 100)
            rounded_rect(draw, [track_x, by + 4, track_x + fill_w, by + 14], 4, fill=ACCENT)
            draw.text((track_x + track_w + 10, by), f"{pct}%", font=font(13, True), fill=TEXT)
            by += 30
        y += 184
    else:
        rounded_rect(draw, [pad, y, w - pad, y + 120], 12, fill=PANEL, outline=BORDER)
        draw.text((pad + 16, y + 14), "Final Readiness Score", font=font(12, True), fill=MUTED2)
        draw.text((pad + 16, y + 36), "72 / 100", font=font(32, True), fill=TEXT)
        draw.text((pad + 16, y + 78), "Current stage · Shaping", font=font(14), fill=ACCENT_SOFT)
        y += 134
        bars = [
            ("Project Completeness", 78),
            ("Commercial Inputs", 64),
            ("Market Context", 82),
            ("Partner Fit Signals", 71),
        ]
        for lab, pct in bars:
            rounded_rect(draw, [pad, y, w - pad, y + 44], 10, fill=PANEL, outline=BORDER)
            draw.text((pad + 14, y + 8), lab, font=font(13), fill=MUTED)
            track_x = pad + 14
            track_w = w - pad * 2 - 70
            rounded_rect(draw, [track_x, y + 28, track_x + track_w, y + 36], 4, fill=PANEL2)
            rounded_rect(draw, [track_x, y + 28, track_x + int(track_w * pct / 100), y + 36], 4, fill=ACCENT)
            draw.text((track_x + track_w + 8, y + 24), f"{pct}%", font=font(13, True), fill=TEXT)
            y += 52

    # Priority gaps — missing information only (no invented recommendations)
    rounded_rect(draw, [pad, y, w - pad, y + (150 if desktop else 170)], 12, fill=PANEL, outline=BORDER)
    draw.text((pad + 16, y + 12), "PRIORITY GAPS", font=font(12, True), fill=GOLD)
    gaps = [
        ("High", "PIP / Capex clarity"),
        ("High", "Preferred deal structure detail"),
        ("Medium", "Ownership / control documentation"),
        ("Medium", "Market performance benchmarks"),
    ]
    gy = y + 38
    for pri, label in gaps:
        color = RED if pri == "High" else ORANGE
        rounded_rect(draw, [pad + 16, gy, pad + 16 + 62, gy + 20], 6, fill=PANEL2, outline=color)
        draw.text((pad + 24, gy + 3), pri, font=font(11, True), fill=color)
        draw.text((pad + 90, gy + 2), label, font=font(14), fill=TEXT)
        gy += 26

    if not desktop:
        return finalize_mobile(img, min_h=500)
    return img


def make_clause_library(desktop: bool) -> Image.Image:
    """Clause Library: search/filter + clause cards with category."""
    if desktop:
        w, h = DESKTOP
        pad = 20
    else:
        w, h = MOBILE_W, 700
        pad = 16
    img, draw = card_base(w, h)
    y = pad
    y = draw_label(draw, pad, y)

    draw.text((pad, y), "Clause Library", font=font(24 if desktop else 26, True), fill=TEXT)
    y += 36

    # Search + filters
    if desktop:
        # search
        rounded_rect(draw, [pad, y, pad + 280, y + 36], 8, fill=PANEL, outline=BORDER)
        draw.text((pad + 12, y + 10), "Search clauses…", font=font(13), fill=MUTED2)
        filters = ["Agreement Type", "Category", "Phase", "Risk"]
        fx = pad + 294
        for flab in filters:
            rounded_rect(draw, [fx, y, fx + 140, y + 36], 8, fill=PANEL, outline=BORDER)
            draw.text((fx + 10, y + 10), flab, font=font(12), fill=MUTED)
            fx += 150
        y += 50
    else:
        rounded_rect(draw, [pad, y, w - pad, y + 40], 8, fill=PANEL, outline=BORDER)
        draw.text((pad + 14, y + 12), "Search by name, category…", font=font(14), fill=MUTED2)
        y += 52
        filters = ["Franchise", "Term & Renewal", "Pre-Sign"]
        fx = pad
        for flab in filters:
            tw = draw.textlength(flab, font=font(12, True)) + 24
            rounded_rect(draw, [fx, y, fx + tw, y + 28], 999, fill=PANEL, outline=ACCENT)
            draw.text((fx + 12, y + 7), flab, font=font(12, True), fill=ACCENT_SOFT)
            fx += tw + 8
        y += 42

    draw.text((pad, y), "Showing 4 of 186 clauses", font=font(12), fill=MUTED2)
    y += 24

    clauses = [
        (
            "Territorial Protection / Radius Restriction",
            "Franchise",
            "Territorial Rights",
            "Pre-Sign",
            "Defines the protected geographic area where the brand will not authorize competing properties.",
            "Medium",
            "Owner-friendly",
            "Common",
        ),
        (
            "Initial Term & Renewal Options",
            "Franchise",
            "Term & Renewal",
            "Pre-Sign",
            "Sets the initial franchise term length and available renewal option quantities and conditions.",
            "Low",
            "Neutral",
            "Common",
        ),
        (
            "Royalty Fee Calculation Basis",
            "Franchise",
            "Fees",
            "Operations",
            "Specifies how ongoing royalty is calculated — typically a percent of rooms or gross revenue.",
            "High",
            "Brand-friendly",
            "Common",
        ),
        (
            "Property Improvement Plan (PIP) Scope",
            "Franchise",
            "Property Improvement Plan",
            "Pre-Opening",
            "Outlines required capital improvements, timing, and brand standards before opening or reflag.",
            "High",
            "Neutral",
            "Common",
        ),
    ]

    risk_colors = {"Low": GREEN, "Medium": ORANGE, "High": RED}
    for title, agree, cat, phase, summary, risk, lean, prev in clauses:
        card_h = 96 if desktop else 118
        if y + card_h > h - 16:
            break
        rounded_rect(draw, [pad, y, w - pad, y + card_h], 10, fill=PANEL, outline=BORDER)
        draw.text((pad + 14, y + 10), title, font=font(15 if desktop else 16, True), fill=TEXT)
        meta = f"{agree}  ·  {cat}  ·  {phase}"
        draw.text((pad + 14, y + 32), meta, font=font(11), fill=MUTED2)
        for i, line in enumerate(wrap_text(draw, summary, font(12), w - pad * 2 - 28)[: 1 if desktop else 2]):
            draw.text((pad + 14, y + 50 + i * 16), line, font=font(12), fill=MUTED)
        # badges
        bx = pad + 14
        by = y + card_h - 26
        for badge, color in ((f"{risk} Risk", risk_colors[risk]), (lean, ACCENT_SOFT), (prev, MUTED2)):
            tw = draw.textlength(badge, font=font(10, True)) + 16
            rounded_rect(draw, [bx, by, bx + tw, by + 18], 6, fill=PANEL2, outline=color)
            draw.text((bx + 8, by + 3), badge, font=font(10, True), fill=color)
            bx += tw + 8
        y += card_h + 10

    if not desktop:
        return finalize_mobile(img, min_h=520)
    return img


def make_financial_term_library(desktop: bool) -> Image.Image:
    """Financial Term Library cards — real library UI pattern."""
    if desktop:
        w, h = DESKTOP
        pad = 20
    else:
        w, h = MOBILE_W, 680
        pad = 16
    img, draw = card_base(w, h)
    y = pad
    y = draw_label(draw, pad, y)

    draw.text((pad, y), "Financial Term Library", font=font(24 if desktop else 26, True), fill=TEXT)
    y += 34

    if desktop:
        rounded_rect(draw, [pad, y, pad + 300, y + 34], 8, fill=PANEL, outline=BORDER)
        draw.text((pad + 12, y + 9), "Search by name, category…", font=font(12), fill=MUTED2)
        fx = pad + 314
        for flab in ("Agreement Type", "Category", "Risk Level"):
            rounded_rect(draw, [fx, y, fx + 150, y + 34], 8, fill=PANEL, outline=BORDER)
            draw.text((fx + 10, y + 9), flab, font=font(12), fill=MUTED)
            fx += 160
        y += 48
    else:
        rounded_rect(draw, [pad, y, w - pad, y + 38], 8, fill=PANEL, outline=BORDER)
        draw.text((pad + 14, y + 11), "Search financial terms…", font=font(14), fill=MUTED2)
        y += 50
        fx = pad
        for flab in ("Fees", "Franchise", "Common"):
            tw = draw.textlength(flab, font=font(12, True)) + 24
            rounded_rect(draw, [fx, y, fx + tw, y + 28], 999, fill=PANEL, outline=ACCENT)
            draw.text((fx + 12, y + 7), flab, font=font(12, True), fill=ACCENT_SOFT)
            fx += tw + 8
        y += 42

    draw.text((pad, y), "Showing 4 of 142 terms", font=font(12), fill=MUTED2)
    y += 22

    terms = [
        (
            "Royalty Fee",
            "Franchise",
            "Fees",
            "Operations",
            "Ongoing fee paid to the brand, typically as a percentage of rooms or gross revenue.",
            "High",
            "Brand-friendly",
            "Common",
        ),
        (
            "Marketing / Program Fee",
            "Franchise",
            "Fees",
            "Operations",
            "Contribution to brand marketing, loyalty, and system programs — often a percent of revenue.",
            "Medium",
            "Neutral",
            "Common",
        ),
        (
            "Initial Franchise Fee",
            "Franchise",
            "Fees",
            "Pre-Sign",
            "One-time upfront fee payable upon signing or opening, sometimes structured per property.",
            "Medium",
            "Neutral",
            "Common",
        ),
        (
            "Key Money / Upfront Incentive",
            "Franchise",
            "Incentives",
            "Pre-Sign",
            "Cash or other upfront contribution from brand to owner to support conversion or signing.",
            "Low",
            "Owner-friendly",
            "Sometimes",
        ),
    ]

    risk_colors = {"Low": GREEN, "Medium": ORANGE, "High": RED}
    cols = 2 if desktop else 1
    gap = 10
    card_w = (w - pad * 2 - gap) / cols if cols == 2 else w - pad * 2
    card_h = 118 if desktop else 112

    for i, (title, agree, cat, phase, summary, risk, lean, prev) in enumerate(terms):
        c = i % cols
        r = i // cols
        x = pad + c * (card_w + gap)
        cy = y + r * (card_h + gap)
        rounded_rect(draw, [x, cy, x + card_w, cy + card_h], 10, fill=PANEL, outline=BORDER)
        draw.text((x + 14, cy + 12), title, font=font(15, True), fill=TEXT)
        draw.text((x + 14, cy + 34), f"{agree}  ·  {cat}  ·  {phase}", font=font(11), fill=MUTED2)
        for li, line in enumerate(wrap_text(draw, summary, font(12), card_w - 28)[:2]):
            draw.text((x + 14, cy + 52 + li * 16), line, font=font(12), fill=MUTED)
        bx = x + 14
        by = cy + card_h - 26
        for badge, color in ((f"{risk} Risk", risk_colors[risk]), (lean, ACCENT_SOFT)):
            tw = draw.textlength(badge, font=font(10, True)) + 14
            rounded_rect(draw, [bx, by, bx + tw, by + 18], 6, fill=PANEL2, outline=color)
            draw.text((bx + 7, by + 3), badge, font=font(10, True), fill=color)
            bx += tw + 6

    if not desktop:
        return finalize_mobile(img, min_h=520)
    return img


def make_submit_proposal(desktop: bool) -> Image.Image:
    """Submit Proposal form — Agreement Type, Term & Renewal, Royalty %, Marketing %,
    Initial Franchise Fee, Key Money (demo fictional numbers OK)."""
    if desktop:
        w, h = DESKTOP
        pad = 22
    else:
        w, h = MOBILE_W, 700
        pad = 18
    img, draw = card_base(w, h)
    y = pad
    y = draw_label(draw, pad, y)

    draw.text((pad, y), "Submit Proposal", font=font(24 if desktop else 26, True), fill=TEXT)
    y += 28
    draw.text(
        (pad, y),
        "Aeropuerto Cancún Select-Service Hotel · Franchise Only",
        font=font(13),
        fill=MUTED,
    )
    y += 28

    def field_row(label, value, x, fy, fw, fh=44):
        draw.text((x, fy), label.upper(), font=font(11, True), fill=MUTED2)
        rounded_rect(draw, [x, fy + 16, x + fw, fy + 16 + fh], 8, fill=PANEL, outline=BORDER)
        draw.text((x + 12, fy + 16 + (fh - 16) // 2), value, font=font(15), fill=TEXT)
        return fy + 16 + fh + 14

    if desktop:
        # Two-column form
        col_w = (w - pad * 2 - 16) // 2
        left_x, right_x = pad, pad + col_w + 16
        ly = ry = y

        ly = field_row("Agreement Type", "Franchise", left_x, ly, col_w)
        draw.text((left_x, ly), "TERM & RENEWAL", font=font(12, True), fill=ACCENT_SOFT)
        ly += 22
        ly = field_row("Initial Term", "1 × 20 Years", left_x, ly, col_w)
        ly = field_row("Renewal Options", "2 × 5 Years", left_x, ly, col_w)

        ry = field_row("Royalty %", "5.0% of Rooms Revenue", right_x, ry, col_w)
        ry = field_row("Marketing %", "2.5% of Gross Revenue", right_x, ry, col_w)
        ry = field_row("Initial Franchise Fee", "$75,000 — Per Property", right_x, ry, col_w)
        ry = field_row("Key Money", "$250,000 — Staggered", right_x, ry, col_w)
    else:
        y = field_row("Agreement Type", "Franchise", pad, y, w - pad * 2)
        draw.text((pad, y), "TERM & RENEWAL", font=font(13, True), fill=ACCENT_SOFT)
        y += 24
        y = field_row("Initial Term", "1 × 20 Years", pad, y, w - pad * 2)
        y = field_row("Renewal Options", "2 × 5 Years", pad, y, w - pad * 2)
        y = field_row("Royalty %", "5.0% of Rooms Revenue", pad, y, w - pad * 2)
        y = field_row("Marketing %", "2.5% of Gross Revenue", pad, y, w - pad * 2)
        y = field_row("Initial Franchise Fee", "$75,000 — Per Property", pad, y, w - pad * 2)
        y = field_row("Key Money", "$250,000 — Staggered", pad, y, w - pad * 2)

    if not desktop:
        return finalize_mobile(img, min_h=520)
    return img


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    print("=== FEATURE PANELS (screenshot crops) ===")
    for name in CROP_SPECS:
        for variant in ("desktop", "mobile"):
            img = make_crop_panel(name, variant)
            if variant == "mobile":
                # Content-tight around ~780×520; keep framed crop at least MOBILE_H
                img = finalize_mobile(img, bottom_pad=16, min_h=MOBILE_H)
            export(img, f"{name}-{variant}")
            print(f"  {name}-{variant}")

    print("=== FEATURE PANELS (stylized) ===")
    stylized = [
        ("deal-readiness", make_deal_readiness),
        ("clause-library", make_clause_library),
        ("financial-term-library", make_financial_term_library),
        ("submit-proposal", make_submit_proposal),
    ]
    for name, fn in stylized:
        for desktop in (True, False):
            img = fn(desktop)
            variant = "desktop" if desktop else "mobile"
            export(img, f"{name}-{variant}")
            print(f"  {name}-{variant}")

    # hotel-temp.jpg must remain untouched
    hotel = ROOT / "hotel-temp.jpg"
    assert hotel.exists(), "assets/hotel-temp.jpg missing — do not regenerate"

    print()
    print("=== SUMMARY ===")
    print(f"{'file':<42} {'w':>5} {'h':>5} {'KB':>6}")
    print("-" * 62)
    for name, w, h, size in CREATED:
        print(f"{name:<42} {w:>5} {h:>5} {size // 1024:>5}KB")
    print("-" * 62)
    print(f"{len(CREATED)} files → {OUT}")
    print(f"hotel-temp.jpg preserved: {hotel} ({hotel.stat().st_size // 1024}KB)")


if __name__ == "__main__":
    main()
