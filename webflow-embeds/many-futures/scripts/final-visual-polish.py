#!/usr/bin/env python3
"""Final visual polish assets for Many Futures.

- Brand Smart Matching: tight Preferred Brand + Match Score crops
- Operator Smart Matching: score-dominant crops (separate from brand)
- Radar: title-safe padded framing from existing product exports
- Fee Estimator: mobile-stacked results crop from desktop source
"""
from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageEnhance, ImageFont

ROOT = Path(__file__).resolve().parents[1]
FEAT = ROOT / "assets" / "features"
SRC = ROOT / "assets" / "sources"
BG = (8, 15, 37)
PANEL = (17, 27, 58)
PANEL2 = (13, 22, 48)
BORDER = (48, 60, 100)
TEXT = (255, 255, 255)
MUTED = (174, 185, 225)
MUTED2 = (140, 150, 180)
ACCENT = (139, 144, 255)
GREEN = (72, 168, 120)
ORANGE = (220, 140, 70)
RED = (200, 90, 90)


def font(size: int, bold: bool = False):
    paths = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ]
    for p in paths:
        try:
            return ImageFont.truetype(p, size)
        except OSError:
            continue
    return ImageFont.load_default()


def save_png_webp(img: Image.Image, stem: str, max_w: int | None = None) -> None:
    if max_w and img.width > max_w:
        nh = int(round(img.height * (max_w / img.width)))
        img = img.resize((max_w, nh), Image.Resampling.LANCZOS)
    img = ImageEnhance.Sharpness(img.convert("RGB")).enhance(1.06)
    png = FEAT / f"{stem}.png"
    img.save(png, "PNG", optimize=True)
    try:
        img.save(FEAT / f"{stem}.webp", "WEBP", quality=88, method=6)
    except Exception:
        pass
    print(f"  {png.name}: {img.size[0]}x{img.size[1]} ({png.stat().st_size // 1024}KB)")


def navy_canvas(inner: Image.Image, out_w: int, out_h: int, pad: int = 14) -> Image.Image:
    """Place crop on navy canvas with safe inset; scale to fill ~70–85% of frame."""
    canvas = Image.new("RGB", (out_w, out_h), BG)
    max_w = out_w - pad * 2
    max_h = out_h - pad * 2
    scale = min(max_w / inner.width, max_h / inner.height)
    # Prefer filling most of the frame (scores dominant)
    scale = max(scale, min(max_w / inner.width, max_h / inner.height))
    nw = max(1, int(inner.width * scale))
    nh = max(1, int(inner.height * scale))
    if nw > max_w or nh > max_h:
        scale = min(max_w / inner.width, max_h / inner.height)
        nw = max(1, int(inner.width * scale))
        nh = max(1, int(inner.height * scale))
    resized = inner.resize((nw, nh), Image.Resampling.LANCZOS)
    x = (out_w - nw) // 2
    y = (out_h - nh) // 2
    canvas.paste(resized, (x, y))
    return canvas


def brand_smart_matching_crops() -> None:
    """Zoom into Preferred Brand + Match Score from real matched-brands UI."""
    src = Image.open(SRC / "matched-brands.png").convert("RGB")
    w, h = src.size
    # Preferred Brand + Match Score only (right side), skip filters; ~4 rows
    desk_crop = src.crop((640, 115, w - 4, min(h - 4, 460)))
    save_png_webp(navy_canvas(desk_crop, 1024, 576, pad=10), "smart-matching-desktop")

    # Mobile: even tighter on brand name + score pills
    mob_crop = src.crop((700, 115, w - 4, min(h - 4, 470)))
    save_png_webp(navy_canvas(mob_crop, 780, 560, pad=8), "smart-matching-mobile")


def operator_smart_matching_crops() -> None:
    """Score-dominant operator matching crops — separate from brand crops."""
    rows = [
        ("Cenote Azul Operadores", "86.4", GREEN, "Regional conversion experience", "Strong alignment"),
        ("Caribe Host Management", "71.2", ORANGE, "Validate urban asset focus", "Moderate — review gaps"),
        ("Atlántica Hospitality Ops", "54.8", ORANGE, "Confirm owner reporting cadence", "Moderate — review gaps"),
        ("Litoral Operating Group", "38.1", RED, "Limited market overlap", "Weak — significant gaps"),
    ]

    def paint(desktop: bool) -> Image.Image:
        if desktop:
            w, h = 980, 520
            pad = 18
            score_w = 110
        else:
            w, h = 720, 640
            pad = 16
            score_w = 96
        img = Image.new("RGB", (w, h), BG)
        d = ImageDraw.Draw(img)
        y = pad
        d.text((pad, y), "Operator Strategy", font=font(20 if desktop else 22, True), fill=TEXT)
        y += 28
        d.text((pad, y), "OPERATOR MATCH SCORE & FIT SIGNALS", font=font(11, True), fill=ACCENT)
        y += 22
        # Strong chip
        chip = "Alignment Signal: Strong"
        tw = int(d.textlength(chip, font=font(11, True))) + 18
        d.rounded_rectangle([pad, y, pad + tw, y + 24], radius=999, fill=(34, 90, 70), outline=BORDER)
        d.text((pad + 9, y + 5), chip, font=font(11, True), fill=TEXT)
        y += 36

        # Column headers
        d.rounded_rectangle([pad, y, w - pad, y + 30], radius=8, fill=PANEL2, outline=BORDER)
        d.text((pad + 12, y + 8), "OPERATING COMPANY", font=font(10, True), fill=MUTED2)
        d.text((w - pad - score_w - 8, y + 8), "SCORE", font=font(10, True), fill=MUTED2)
        y += 36

        show = rows[:3] if not desktop else rows[:4]
        row_h = 78 if desktop else 88
        for name, score, color, consider, signal in show:
            d.rounded_rectangle([pad, y, w - pad, y + row_h - 8], radius=10, fill=PANEL, outline=BORDER)
            d.text((pad + 14, y + 12), name, font=font(15 if desktop else 16, True), fill=TEXT)
            d.text((pad + 14, y + 36), consider, font=font(12), fill=MUTED)
            d.text((pad + 14, y + 54), signal, font=font(11), fill=MUTED2)
            # Large score pill — ~60%+ visual weight with row density
            sx = w - pad - score_w - 10
            sy = y + (row_h - 8 - 36) // 2
            d.rounded_rectangle([sx, sy, sx + score_w, sy + 36], radius=999, fill=color)
            sw = d.textlength(score, font=font(18, True))
            d.text((sx + (score_w - sw) / 2, sy + 7), score, font=font(18, True), fill=TEXT)
            y += row_h

        d.text(
            (pad, min(h - 28, y + 4)),
            "Overall Operator Alignment Score · 0–100 · nine scored factors",
            font=font(11),
            fill=MUTED2,
        )
        return img

    desk = paint(True)
    mob = paint(False)
    save_png_webp(navy_canvas(desk, 1024, 576, pad=8), "smart-matching-operators-desktop")
    save_png_webp(navy_canvas(mob, 780, 640, pad=8), "smart-matching-operators-mobile")
    # Also write webp-friendly aliases used by markup if we point brand crops to main names
    save_png_webp(desk, "smart-matching-operator-desktop")
    save_png_webp(mob, "smart-matching-operator-mobile")


def radar_title_safe() -> None:
    """Re-frame Radar from market-map with title-safe inset (map stays dominant)."""
    src = Image.open(SRC / "market-map.png").convert("RGB")
    # Full product chrome: title through map. Leave headroom above title.
    # Source is taller than wide; take upper interface + map region.
    region = src.crop((0, 0, src.width, min(src.height, 1180)))
    # Desk 16:9 with internal padding so title never touches edge
    out_w, out_h = 1600, 900
    top, side, bottom = 28, 18, 16
    canvas = Image.new("RGB", (out_w, out_h), BG)
    max_w = out_w - side * 2
    max_h = out_h - top - bottom
    scale = min(max_w / region.width, max_h / region.height)
    nw, nh = int(region.width * scale), int(region.height * scale)
    resized = region.resize((nw, nh), Image.Resampling.LANCZOS)
    canvas.paste(resized, ((out_w - nw) // 2, top + max(0, (max_h - nh) // 2)))
    save_png_webp(canvas, "radar-desktop")
    save_png_webp(canvas.resize((800, 450), Image.Resampling.LANCZOS), "radar-desktop-800")

    # Mobile: slightly taller crop, keep title + filters + map
    mob_region = src.crop((40, 0, src.width - 40, min(src.height, 1280)))
    mw, mh = 900, 600
    top_m, side_m, bottom_m = 24, 14, 14
    mcanvas = Image.new("RGB", (mw, mh), BG)
    max_w = mw - side_m * 2
    max_h = mh - top_m - bottom_m
    scale = min(max_w / mob_region.width, max_h / mob_region.height)
    nw, nh = int(mob_region.width * scale), int(mob_region.height * scale)
    resized = mob_region.resize((nw, nh), Image.Resampling.LANCZOS)
    mcanvas.paste(resized, ((mw - nw) // 2, top_m + max(0, (max_h - nh) // 2)))
    save_png_webp(mcanvas, "radar-mobile")


def fee_estimator_mobile_stack() -> None:
    """Mobile Fee Estimator: stack key results so the frame is never blank/navy-only."""
    src = Image.open(SRC / "fee-estimator.png").convert("RGB")
    # Desktop results band — top results cards
    band = src.crop((24, 70, src.width - 24, 310))
    # Also refresh desktop framed crop with inset
    desk = Image.open(FEAT / "fee-estimator-desktop.png").convert("RGB")
    save_png_webp(navy_canvas(desk.crop((40, 40, desk.width - 40, desk.height - 30)), 1024, 576, pad=14), "fee-estimator-desktop")

    # Build stacked mobile composition from real result values (from product UI)
    w, h = 780, 640
    img = Image.new("RGB", (w, h), BG)
    d = ImageDraw.Draw(img)
    pad = 18
    y = pad
    d.text((pad, y), "YOUR RESULTS", font=font(12, True), fill=ACCENT)
    y += 28
    cards = [
        ("Total Franchise Fees", "$3.78M", "10-Yr Total", GREEN),
        ("Effective Fee Rate", "12.1%", "Above Avg", RED),
        ("Yr1 Recurring Fees", "$267k", "Annual", BORDER),
        ("Amort. Ann. Cost", "$378k", "Yr Avg", GREEN),
        ("Fee Assessment", "Higher", "Review Terms", RED),
    ]
    for title, value, meta, accent in cards:
        d.rounded_rectangle([pad, y, w - pad, y + 88], radius=12, fill=PANEL, outline=accent if accent != BORDER else BORDER, width=2 if accent != BORDER else 1)
        d.text((pad + 16, y + 12), title, font=font(12, True), fill=MUTED2)
        d.text((pad + 16, y + 34), value, font=font(26, True), fill=TEXT)
        d.text((pad + 16, y + 66), meta, font=font(12, True), fill=MUTED)
        y += 98
    save_png_webp(img, "fee-estimator-mobile")


def main() -> None:
    FEAT.mkdir(parents=True, exist_ok=True)
    print("Brand Smart Matching…")
    brand_smart_matching_crops()
    print("Operator Smart Matching…")
    operator_smart_matching_crops()
    print("Radar title-safe…")
    # Reload original radar from sources before padding if available
    # Use current features as base (already product-framed), then pad
    radar_title_safe()
    print("Fee Estimator mobile…")
    fee_estimator_mobile_stack()
    print("Done.")


if __name__ == "__main__":
    main()
