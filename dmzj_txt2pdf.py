#!/usr/bin/env python3
"""
dmzj_txt2pdf.py  ─  Convert DMZJ Flutter-downloaded `.txt` novel fragments
                   to PDF and (optionally) merge them.

Public API
----------
convert_dmzj_txts_to_pdf(
    root_dir: str | Path,
    output_dir: str | Path,
    group_level: int = 2,
    encoding: str = "utf-8",
) -> list[Path]

When imported, call the above to perform exactly the same work the CLI does.
When executed directly (`python dmzj_txt2pdf.py …`) it falls back to
argparse-based command-line handling.
"""

from __future__ import annotations

import argparse
import os
from itertools import groupby
from pathlib import Path
from typing import List

from bs4 import BeautifulSoup  # ≥4.9 — HTML → text
from reportlab.lib.pagesizes import A4  # PDF page size
from reportlab.pdfgen.canvas import Canvas  # low-level canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont

from pypdf import PdfWriter  # fast, mmap-safe merge

pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))
DEFAULT_FONT = "STSong-Light"


# ──────────────────────────  helpers  ──────────────────────────
def _clean_fragment(raw: str) -> str:
    """Strip <br>, &nbsp;, etc. and return plain text with \n separators."""
    soup = BeautifulSoup(raw, "html.parser")
    return soup.get_text("\n")  # `separator="\n"` for explicit breaks


def _txt_to_pdf(
        txt_path: Path,
        pdf_path: Path,
        font_name: str = DEFAULT_FONT,
        font_size: int = 12,
        line_height: int = 12,
        enc_preforder: tuple[str, ...] = ("utf-8", "gbk", "big5", "utf-16"),
) -> None:
    """
    Render ONE DMZJ fragment into `pdf_path`.

    • Auto-detects encoding.
    • Strips pseudo-HTML.
    • Soft-wraps every line so its visual width ≤ printable area.
    • Re-applies font after every showPage().
    """

    # ---------- 1. read & decode ------------------------------------------------
    raw = txt_path.read_bytes()
    text: str | None = None
    for enc in enc_preforder:  # quick manual probe
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None:  # last-resort detection
        try:
            import chardet  # optional dep
            text = raw.decode(chardet.detect(raw)["encoding"] or "utf-8", errors="replace")
        except Exception:  # pragma: no cover
            text = raw.decode("utf-8", errors="replace")

    # ---------- 2. strip pseudo-HTML -------------------------------------------
    cleaned = BeautifulSoup(text, "html.parser").get_text("\n")  # <br/> → \n

    # ---------- 3. prepare canvas ----------------------------------------------
    pdf_path.parent.mkdir(parents=True, exist_ok=True)
    c = Canvas(str(pdf_path), pagesize=A4)
    width, height = A4
    x_margin, top_margin = 42, 42
    y = height - top_margin
    line_max_width = width - 2 * x_margin  # printable width

    def draw_line(s: str) -> None:
        """Draw one already-wrapped line, handling page breaks."""
        nonlocal y
        if y < top_margin:  # need new page first
            c.showPage()
            c.setFont(font_name, font_size)
            y = height - top_margin
        c.drawString(x_margin, y, s)
        y -= line_height

    # ---------- 4. wrap + draw --------------------------------------------------
    c.setFont(font_name, font_size)
    for para in cleaned.splitlines() or [" "]:
        if not para.strip():  # blank → vertical space
            y -= line_height
            continue

        buf = ""
        for ch in para:
            # Would adding this char overflow?
            if pdfmetrics.stringWidth(buf + ch, font_name, font_size) > line_max_width:
                draw_line(buf)
                buf = ch  # start new visual line
            else:
                buf += ch
        draw_line(buf)  # last segment of paragraph
        y -= line_height  # extra space after paragraph

    c.save()


def _folder_key_of(path: Path, depth: int) -> str:
    """
    Return the first *depth* underscore-separated chunks of the fragment's parent
    directory, e.g. 3084 or 3084_11641.
    """
    return "_".join(path.parent.name.split("_")[:depth])


# ────────────────────────  public function  ───────────────────────
def convert_dmzj_txts_to_pdf(
        root_dir: str | Path,
        output_dir: str | Path,
        group_level: int = 2,
        encoding: str = "utf-8",
) -> List[Path]:
    """
    Convert every *.txt under *root_dir* to individual PDFs and, if
    1 ≤ group_level ≤ 2, merge fragments that share the same leading
    underscore-parts of their folder names.

    Returns
    -------
    list[Path]
        The absolute paths of all PDFs created (individual + merged).
    """
    root, out = Path(root_dir), Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    created: list[Path] = []
    pdf_of_txt: dict[Path, Path] = {}

    # 1. individual conversions
    for dirpath, _, files in os.walk(root):
        for fname in files:
            if fname.lower().endswith(".txt"):
                txt = Path(dirpath) / fname
                rel_pdf = txt.relative_to(root).with_suffix(".pdf")
                pdf_path = out / rel_pdf
                _txt_to_pdf(txt, pdf_path)
                pdf_of_txt[txt] = pdf_path
                created.append(pdf_path)

    # 2. merging by key
    if 1 <= group_level <= 2:
        sorted_txts = sorted(pdf_of_txt, key=lambda p: _folder_key_of(p, group_level))
        for key, group in groupby(sorted_txts, key=lambda p: _folder_key_of(p, group_level)):
            writer = PdfWriter()  # supports fast append
            for txt in group:
                writer.append(str(pdf_of_txt[txt]))  # streams pages, low-RAM
            merged_path = out / f"{key}.pdf"
            with merged_path.open("wb") as fh:
                writer.write(fh)
            created.append(merged_path)

    return created


# ─────────────────────────  CLI wrapper  ─────────────────────────
def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Convert DMZJ .txt downloads to PDF and merge by folder key."
    )
    parser.add_argument("--root", required=True, help="Novel download root directory")
    parser.add_argument("--out", required=True, help="Destination directory for PDFs")
    parser.add_argument(
        "--group-level",
        type=int,
        default=2,
        help="How many underscore segments form the merge key "
             "(1=book, 2=book+volume, 3=no merge)",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        help="Character encoding of DMZJ .txt files (default: utf-8)",
    )
    args = parser.parse_args()
    done = convert_dmzj_txts_to_pdf(
        args.root, args.out, group_level=args.group_level, encoding=args.encoding
    )
    print(f"Created {len(done)} PDF files:")
    for p in done:
        print(" •", p)


# Run as a script
if __name__ == "__main__":
    _cli()
