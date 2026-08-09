#!/usr/bin/env python3
"""Regenerate curated desktop/mobile product crops from full Dealality screenshots.

Focus treatment (Phase 2.5 → 3):
  Soft dim outside the metrics area + faint outer glow + thin low-opacity edge.
  Avoids thick double borders / selection-box appearance.

Mobile crops are dedicated regions (not zoomed desktop crops), sized for ~390px
viewports with mild ≤1.3× upscale when source pixels allow.
"""
from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageEnhance, ImageFilter

ROOT = Path(__file__).resolve().parents[1] / "assets"
OUT = ROOT / "crops"
OUT.mkdir(exist_ok=True)


def soft_focus(img: Image.Image, box, dim=0.45, glow_pad=12, glow_blur=16) -> Image.Image:
    """Dim secondary areas; keep focus clear; subtle accent glow (no selection box)."""
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
    gx0, gy0 = max(0, x0 - glow_pad), max(0, y0 - glow_pad)
    gx1, gy1 = min(base.width, x1 + glow_pad), min(base.height, y1 + glow_pad)
    d.rounded_rectangle([gx0, gy0, gx1, gy1], radius=12, fill=(108, 114, 255, 48))
    glow = glow.filter(ImageFilter.GaussianBlur(glow_blur))
    out = Image.alpha_composite(dimmed, glow)
    out.paste(clear, (x0, y0))

    edge = Image.new("RGBA", base.size, (0, 0, 0, 0))
    de = ImageDraw.Draw(edge)
    de.rounded_rectangle(
        [x0, y0, x1 - 1, y1 - 1],
        radius=8,
        outline=(139, 144, 255, 70),
        width=1,
    )
    return Image.alpha_composite(out, edge).convert("RGB")


def export(img: Image.Image, path: Path, max_w: int, max_upscale: float = 1.25) -> Path:
    """Preserve aspect. Downscale if wider than max_w; mild upscale only when useful."""
    w, h = img.size
    if w > max_w:
        nw = max_w
        nh = int(round(h * (nw / w)))
    elif w * max_upscale >= max_w * 0.9:
        nw = min(int(round(w * max_upscale)), max_w)
        nh = int(round(h * (nw / w)))
    else:
        nw, nh = w, h
        print(f"  keep native {w}x{h} for {path.name}")

    if (nw, nh) != (w, h):
        img = img.resize((nw, nh), Image.Resampling.LANCZOS)
    img = ImageEnhance.Sharpness(img).enhance(1.1)
    path = path.with_suffix(".png")
    img.save(path, "PNG", optimize=True)
    print(f"  {path.name}: {img.size[0]}x{img.size[1]} ({path.stat().st_size // 1024}KB)")
    return path


# Absolute pixel regions on ~1024-wide source screenshots.
# Mobile frames are dedicated crops focused on ~3–5 readable metrics.
SPECS = {
    "new-operator": {
        "src": "operator-explorer.png",
        # Header + bio + KPI strip; exclude tab row / quick facts
        "desk_crop": (0, 0, 1024, 235),
        "desk_focus": (20, 140, 1004, 228),
        "desk_max_w": 1280,
        # Left ~4–5 KPI cards only — no tiny navigation
        "mob_crop": (16, 138, 640, 232),
        "mob_focus": (8, 6, 616, 88),
        "mob_max_w": 780,
        "mob_upscale": 1.3,
    },
    "rebrand": {
        "src": "brand-explorer.png",
        "desk_crop": (0, 40, 1024, 520),
        "desk_focus": (40, 40, 984, 280),
        "desk_max_w": 1280,
        "mob_crop": (40, 70, 700, 300),
        "mob_focus": (16, 16, 640, 200),
        "mob_max_w": 780,
        "mob_upscale": 1.3,
    },
    "soft-brand": {
        "src": "fee-estimator.png",
        "desk_crop": (0, 0, 1024, 480),
        "desk_focus": (300, 70, 1000, 300),
        "desk_max_w": 1280,
        # Fee result cards (~4–5 metrics)
        "mob_crop": (280, 95, 920, 285),
        "mob_focus": (12, 20, 620, 175),
        "mob_max_w": 780,
        "mob_upscale": 1.3,
    },
    "independent": {
        "src": "radar.png",
        "desk_crop": (0, 80, 1024, 560),
        "desk_focus": (560, 40, 1000, 360),
        "desk_max_w": 1280,
        # Summary statistics cards
        "mob_crop": (500, 110, 1010, 400),
        "mob_focus": (20, 40, 480, 260),
        "mob_max_w": 780,
        "mob_upscale": 1.35,
    },
    "branded-residences": {
        "src": "opportunity-review.png",
        "desk_crop": (0, 20, 1024, 480),
        "desk_focus": (24, 80, 1000, 360),
        "desk_max_w": 1280,
        "mob_crop": (20, 70, 700, 330),
        "mob_focus": (12, 20, 660, 230),
        "mob_max_w": 780,
        "mob_upscale": 1.3,
    },
}


def main() -> None:
    for name, s in SPECS.items():
        im = Image.open(ROOT / s["src"]).convert("RGB")
        print(f"=== {name} (src {im.size}) ===")
        desk = soft_focus(im.crop(s["desk_crop"]), s["desk_focus"])
        export(desk, OUT / f"{name}-desktop", s["desk_max_w"])
        mob = soft_focus(im.crop(s["mob_crop"]), s["mob_focus"])
        export(
            mob,
            OUT / f"{name}-mobile",
            s["mob_max_w"],
            max_upscale=s.get("mob_upscale", 1.25),
        )
    print("done")


if __name__ == "__main__":
    main()
