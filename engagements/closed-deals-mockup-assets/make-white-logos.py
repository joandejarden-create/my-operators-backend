"""Bake white-on-transparent logo marks for the closed-deals mockup."""
from __future__ import annotations

import re
from pathlib import Path

from PIL import Image

ROOT = Path(__file__).resolve().parent
LOGO = ROOT / "logos"
OUT = LOGO / "white"
OUT.mkdir(exist_ok=True)


def svg_force_white(src: Path, dst: Path) -> None:
    text = src.read_text(encoding="utf-8")
    # Ensure root fill is white; rewrite common fill attrs.
    text = re.sub(r'fill="[^"]*"', 'fill="#ffffff"', text)
    text = re.sub(r"fill:'[^']*'", "fill:'#ffffff'", text)
    text = re.sub(r"fill:#[0-9A-Fa-f]{3,8}", "fill:#ffffff", text)
    if 'fill="' not in text[:200] and "<svg" in text:
        text = text.replace("<svg", '<svg fill="#ffffff"', 1)
    # Paths without fill inherit; inject fill on path/polygon/polyline.
    for tag in ("path", "polygon", "polyline"):
        text = re.sub(
            rf"<{tag}(?![^>]*\bfill=)",
            f'<{tag} fill="#ffffff"',
            text,
            flags=re.IGNORECASE,
        )
    dst.write_text(text, encoding="utf-8")
    print(f"SVG {dst.name}")


def corner_bg(px, w: int, h: int):
    samples = [
        px[0, 0],
        px[w - 1, 0],
        px[0, h - 1],
        px[w - 1, h - 1],
        px[w // 2, 0],
        px[w // 2, h - 1],
        px[0, h // 2],
        px[w - 1, h // 2],
    ]
    # Prefer opaque samples
    opaque = [c for c in samples if c[3] > 200]
    use = opaque or samples
    r = sum(c[0] for c in use) // len(use)
    g = sum(c[1] for c in use) // len(use)
    b = sum(c[2] for c in use) // len(use)
    return r, g, b


def make_white_mark(src: Path, dst: Path, max_side: int = 640) -> None:
    im = Image.open(src).convert("RGBA")
    if max(im.size) > max_side:
        im.thumbnail((max_side, max_side), Image.Resampling.LANCZOS)
    w, h = im.size
    px = im.load()
    out = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    opx = out.load()
    br, bg, bb = corner_bg(px, w, h)
    bg_lum = (br + bg + bb) / 3.0

    # Count how many pixels already have meaningful alpha (true transparent source)
    transparentish = 0
    for y in range(0, h, max(1, h // 20)):
        for x in range(0, w, max(1, w // 20)):
            if px[x, y][3] < 40:
                transparentish += 1
    has_alpha_matte = transparentish >= 8

    for y in range(h):
        for x in range(w):
            r, g, b, a = px[x, y]
            if a < 8:
                continue
            chroma = max(r, g, b) - min(r, g, b)
            lum = (r + g + b) / 3.0
            dist = abs(r - br) + abs(g - bg) + abs(b - bb)

            if has_alpha_matte:
                # Trust alpha; force ink white
                opx[x, y] = (255, 255, 255, a)
                continue

            if bg_lum >= 200:
                # Light plate / light gray favicon bg
                if lum >= 235 and chroma < 18:
                    continue
                strength = int(min(255, (255 - lum) * 1.35 + chroma * 1.2))
            elif bg_lum <= 45:
                # Dark plate — ink may be colored or light gray
                if dist < 18 and chroma < 18 and lum < 30:
                    continue
                strength = int(min(255, max(dist * 1.1, chroma * 2.2, lum * 1.15)))
            else:
                if dist < 28 and chroma < 22:
                    continue
                strength = int(min(255, dist + chroma))

            if strength < 22:
                continue
            opx[x, y] = (255, 255, 255, min(255, max(strength, 90)))

    bbox = out.getbbox()
    if not bbox:
        raise RuntimeError(f"empty mark: {src.name}")
    out = out.crop(bbox)
    # Pad slightly so marks don't touch crop edge
    pad = 4
    padded = Image.new("RGBA", (out.width + pad * 2, out.height + pad * 2), (0, 0, 0, 0))
    padded.paste(out, (pad, pad))
    padded.save(dst)
    print(f"PNG {dst.name} {padded.size} {dst.stat().st_size}B (bg_lum={bg_lum:.0f})")


def main() -> None:
    # White SVG wordmarks (browser-native, crisp)
    svg_jobs = [
        (LOGO / "brand-marriott-word.svg", OUT / "brand-marriott.svg"),
        (LOGO / "tribute-logo.svg", OUT / "tribute-logo.svg"),
        (LOGO / "brand-hilton.svg", OUT / "brand-hilton.svg"),
        (LOGO / "hilton.svg", OUT / "hilton.svg"),
    ]
    for src, dst in svg_jobs:
        if src.exists():
            svg_force_white(src, dst)

    png_jobs = [
        ("brand-marriott.png", "brand-marriott.png"),
        ("brand-hyatt-place.png", "brand-hyatt-place.png"),
        ("brand-nh.png", "brand-nh.png"),
        ("brand-sheraton.png", "brand-sheraton.png"),
        ("brand-st-regis.png", "brand-st-regis.png"),
        ("brand-ihg.png", "brand-ihg.png"),
        ("brand-hilton.png", "brand-hilton.png"),
        ("brand-curio.png", "brand-curio.png"),
        ("brand-doubletree.png", "brand-doubletree.png"),
        ("operator-playa.png", "operator-playa.png"),
        ("operator-driftwood.png", "operator-driftwood.png"),
        ("operator-minor.png", "operator-minor.png"),
        ("operator-tafer.png", "operator-tafer.png"),
        ("operator-arriva.png", "operator-arriva.png"),
        ("operator-atlantica.png", "operator-atlantica.png"),
        ("operator-gsf.png", "operator-gsf.png"),
        ("operator-brittain.png", "operator-brittain.png"),
        ("garza-blanca-icon.png", "garza-blanca-icon.png"),
        ("transamerica-icon.png", "transamerica-icon.png"),
        ("tribute-icon.png", "tribute-icon.png"),
        ("jw-marriott-icon.png", "jw-marriott-icon.png"),
        ("hyatt-icon.png", "hyatt-icon.png"),
        ("nh-icon.png", "nh-icon.png"),
        ("sheraton-icon.png", "sheraton-icon.png"),
        ("st-regis-icon.png", "st-regis-icon.png"),
        ("curio-icon.png", "curio-icon.png"),
        ("hilton-icon.png", "hilton-icon.png"),
        ("ihg-icon.png", "ihg-icon.png"),
        ("intercontinental-icon.png", "intercontinental-icon.png"),
        ("doubletree-icon.png", "doubletree-icon.png"),
        ("minor-icon.png", "minor-icon.png"),
        ("marriott-icon.png", "marriott-icon.png"),
    ]
    for src_name, dst_name in png_jobs:
        src = LOGO / src_name
        if not src.exists():
            print(f"MISSING {src_name}")
            continue
        try:
            make_white_mark(src, OUT / dst_name)
        except Exception as exc:
            print(f"FAIL {src_name}: {exc}")

    # Operator Marriott reuse brand mark
    marriott_white = OUT / "brand-marriott.png"
    if marriott_white.exists():
        (OUT / "operator-marriott.png").write_bytes(marriott_white.read_bytes())
        print("PNG operator-marriott.png (copy)")


if __name__ == "__main__":
    main()
