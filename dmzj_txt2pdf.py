#!/usr/bin/env python3
"""
dmzj_txt2pdf_mt.py – Multithreaded DMZJ TXT → PDF converter
                     with optional per-volume merging.

Public API
----------
convert_dmzj_txts_to_pdf(
    root_dir: str | Path,
    output_dir: str | Path,
    group_level: int = 2,
    workers: int | None = None,
) -> list[Path]
"""

from __future__ import annotations

import argparse, os, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import groupby
from pathlib import Path
from typing import List, Dict

from bs4 import BeautifulSoup  # HTML → plain text
from reportlab.lib.pagesizes import A4  # page geometry
from reportlab.pdfgen.canvas import Canvas  # PDF canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.pdfbase.pdfmetrics import stringWidth
from pypdf import PdfWriter  # streaming merge
from tqdm import tqdm  # progress bars

# ─────────────────────────────  constants  ─────────────────────────────
pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))
DEFAULT_FONT = "STSong-Light"
DEFAULT_FONTSIZE = 12
DEFAULT_LINEHEIGHT = 13

# sensible thread default for I/O-bound workload (CPython GIL OK)
DEFAULT_WORKERS = min(32, (os.cpu_count() or 1) * 4)


# ───────────────────────────  core helpers  ───────────────────────────
def _detect_encoding(raw: bytes) -> str:
    """Best-effort guess of byte sequence → str codec."""
    for enc in ("utf-8", "gbk", "big5", "utf-16"):
        try:
            raw.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    try:
        import chardet  # external guesser
        return chardet.detect(raw)["encoding"] or "utf-8"  # may be None
    except ModuleNotFoundError:
        return "utf-8"


def _clean_fragment(raw: str) -> str:
    """Drop pseudo-HTML (<br/>, &nbsp;…) and keep explicit newlines."""
    return BeautifulSoup(raw, "html.parser").get_text("\n")


def _wrap_and_draw(
        c: Canvas, text: str, font: str, size: int, line_h: int,
        x0: int, y0: int, wmax: int, hmax: int
) -> None:
    """Soft-wrap *text* into pages; assumes current page is blank."""
    y = y0
    c.setFont(font, size)

    def new_page() -> None:
        nonlocal y
        c.showPage()
        c.setFont(font, size)
        y = y0

    for para in text.splitlines() or [" "]:
        if not para.strip():
            y -= line_h
            if y < hmax:
                new_page()
            continue

        buf = ""
        for ch in para:
            if stringWidth(buf + ch, font, size) > wmax:
                c.drawString(x0, y, buf)
                y -= line_h
                if y < hmax:
                    new_page()
                buf = ch
            else:
                buf += ch
        c.drawString(x0, y, buf)
        y -= line_h
        if y < hmax:
            new_page()


def _txt_to_pdf(
        txt_path: Path,
        pdf_path: Path,
        font: str = DEFAULT_FONT,
        size: int = DEFAULT_FONTSIZE,
        line_h: int = DEFAULT_LINEHEIGHT,
) -> Path:
    """Convert ONE DMZJ fragment; returns the written PDF path."""
    raw = txt_path.read_bytes()
    text = raw.decode(_detect_encoding(raw), errors="replace")
    cleaned = _clean_fragment(text)

    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    c = Canvas(str(pdf_path), pagesize=A4)
    width, height = A4
    margin = 42
    _wrap_and_draw(
        c, cleaned, font, size, line_h,
        margin, height - margin, width - 2 * margin, margin
    )
    c.save()
    return pdf_path


def _folder_key(p: Path, depth: int) -> str:
    return "_".join(p.parent.name.split("_")[:depth])


# ────────────────────────────  public API  ────────────────────────────
def convert_dmzj_txts_to_pdf(
        root_dir: str | Path,
        output_dir: str | Path,
        group_level: int = 2,
        workers: int | None = None,
) -> List[Path]:
    """
    Convert every *.txt in *root_dir* to PDFs (in *output_dir*),
    optionally merging by folder key.  Returns list of created PDFs.
    """
    root, out = Path(root_dir), Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    txt_files = [p for p in root.rglob("*.txt")]
    created: List[Path] = []
    pdf_of_txt: Dict[Path, Path] = {}

    # ── parallel TXT → PDF —─────────────────────────────────────────
    workers = workers or DEFAULT_WORKERS
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {
            ex.submit(
                _txt_to_pdf,
                txt,
                out / txt.relative_to(root).with_suffix(".pdf"),
            ): txt for txt in txt_files
        }
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Converting"):
            txt = futures[fut]
            try:
                pdf_path = fut.result()
                pdf_of_txt[txt] = pdf_path
                created.append(pdf_path)
            except Exception as err:
                print(f"[WARN] {txt}: {err}", file=sys.stderr)

    # ── serial merge (streaming) —────────────────────────────────────
    if 1 <= group_level <= 2:
        for key, group in groupby(
                sorted(pdf_of_txt, key=lambda p: _folder_key(p, group_level)),
                key=lambda p: _folder_key(p, group_level),
        ):
            writer = PdfWriter()
            for txt in group:
                writer.append(str(pdf_of_txt[txt]))
            merged = out / f"{key}.pdf"
            with merged.open("wb") as fh:
                writer.write(fh)
            created.append(merged)

    return created


# ────────────────────────────  CLI wrapper  ────────────────────────────
def _cli() -> None:
    ap = argparse.ArgumentParser(
        description="Convert DMZJ .txt downloads to PDF and merge by folder key."
    )
    ap.add_argument("--root", required=True, help="Novel download root directory")
    ap.add_argument("--out", required=True, help="Destination directory for PDFs")
    ap.add_argument("--group-level", type=int, default=2,
                    help="1=whole novel, 2=per volume, 3=no merge")
    ap.add_argument("--workers", type=int, default=None,
                    help=f"Max concurrent conversions (default: {DEFAULT_WORKERS})")
    ns = ap.parse_args()
    pdfs = convert_dmzj_txts_to_pdf(
        ns.root, ns.out, group_level=ns.group_level, workers=ns.workers
    )
    print(f"✅ Finished. {len(pdfs)} PDFs written.")


if __name__ == "__main__":
    _cli()
