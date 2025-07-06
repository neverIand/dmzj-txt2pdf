#!/usr/bin/env python3
"""
dmzj_txt2pdf_mt.py — Multithreaded DMZJ TXT → PDF converter
                     (deletes intermediate PDFs & empty dirs unless told not to).

Public API
----------
convert_dmzj_txts_to_pdf(
    root_dir: str | Path,
    output_dir: str | Path,
    group_level: int = 2,
    workers: int | None = None,
    keep_fragments: bool = False,
) -> list[Path]
"""

from __future__ import annotations

import argparse, os, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import groupby
from pathlib import Path
from typing import List, Dict

from bs4 import BeautifulSoup  # HTML→text
from reportlab.lib.pagesizes import A4  # PDF geometry
from reportlab.pdfgen.canvas import Canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.pdfbase.pdfmetrics import stringWidth
from pypdf import PdfWriter  # cheap streaming merge
from tqdm import tqdm  # progress bar

# ─────────────────────────────  constants  ─────────────────────────────
pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))  # CJK-safe
DEFAULT_FONT = "STSong-Light"
DEFAULT_FONTSIZE = 12
DEFAULT_LINEHEIGHT = 13
DEFAULT_WORKERS = min(32, (os.cpu_count() or 1) * 4)  # thread cap


# ───────────────────────────  helper functions  ───────────────────────────
def _detect_encoding(raw: bytes) -> str:
    """Guess encoding: quick probes then (optional) chardet."""
    for enc in ("utf-8", "gbk", "big5", "utf-16"):
        try:
            raw.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    try:  # chardet fallback
        import chardet
        return chardet.detect(raw)["encoding"] or "utf-8"
    except ModuleNotFoundError:
        return "utf-8"  # default if chardet missing


def _clean_fragment(raw: str) -> str:
    """Drop pseudo-HTML and keep explicit <br/> as newlines."""
    return BeautifulSoup(raw, "html.parser").get_text("\n")


def _wrap_draw(c: Canvas, txt: str,
               font: str, size: int, lh: int,
               x0: int, y0: int, wmax: int, bottom: int) -> None:
    """Soft-wrap text into multipage canvas."""
    y = y0
    c.setFont(font, size)

    def newpage() -> None:
        nonlocal y
        c.showPage();
        c.setFont(font, size);
        y = y0

    for para in txt.splitlines() or [" "]:
        if not para.strip():
            y -= lh
            if y < bottom: newpage()
            continue
        buf = ""
        for ch in para:
            if stringWidth(buf + ch, font, size) > wmax:
                c.drawString(x0, y, buf);
                y -= lh
                if y < bottom: newpage()
                buf = ch
            else:
                buf += ch
        c.drawString(x0, y, buf);
        y -= lh
        if y < bottom: newpage()


def _txt2pdf_one(txt: Path, pdf: Path) -> Path:
    """Convert one fragment to PDF and return its path."""
    raw = txt.read_bytes()
    cleaned = _clean_fragment(raw.decode(_detect_encoding(raw), errors="replace"))
    pdf.parent.mkdir(parents=True, exist_ok=True)
    c = Canvas(str(pdf), pagesize=A4)
    w, h = A4;
    margin = 42
    _wrap_draw(c, cleaned, DEFAULT_FONT, DEFAULT_FONTSIZE, DEFAULT_LINEHEIGHT,
               margin, h - margin, w - 2 * margin, margin)
    c.save()
    return pdf


def _folder_key(p: Path, depth: int) -> str:
    return "_".join(p.parent.name.split("_")[:depth])


def _prune(paths: List[Path], root: Path) -> None:
    """Delete files & remove any now-empty directories (leaf-to-root)."""
    parents = set()
    for p in paths:
        try:
            p.unlink()  # delete file
            parents.add(p.parent)
        except FileNotFoundError:
            pass
    # deepest-first to avoid 'directory not empty'
    for folder in sorted(parents, key=lambda d: len(d.parts), reverse=True):
        try:
            folder.rmdir()  # remove empty dir
        except OSError:  # not empty → leave
            continue
    # optional: attempt to collapse half-empty chain up to root
    for parent in sorted({f for p in parents for f in p.parents},
                         key=lambda d: len(d.parts), reverse=True):
        if parent == root: break
        try:
            parent.rmdir()
        except OSError:
            continue


# ────────────────────────────  public API  ────────────────────────────
def convert_dmzj_txts_to_pdf(
        root_dir: str | Path,
        output_dir: str | Path,
        group_level: int = 2,
        workers: int | None = None,
        keep_fragments: bool = False,
) -> List[Path]:
    """Batch convert DMZJ .txt files to PDFs; auto-merge & clean up."""
    root, out = Path(root_dir), Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    txt_files = [p for p in root.rglob("*.txt")]
    created: List[Path] = []
    frag_pdf_of_txt: Dict[Path, Path] = {}

    # ── 1. multithreaded TXT → PDF ──────────────────────────────────────
    with ThreadPoolExecutor(max_workers=workers or DEFAULT_WORKERS) as pool:
        fut_to_txt = {pool.submit(
            _txt2pdf_one,
            txt,
            out / txt.relative_to(root).with_suffix(".pdf")
        ): txt for txt in txt_files}

        for fut in tqdm(as_completed(fut_to_txt),
                        total=len(fut_to_txt), desc="Converting"):
            txt = fut_to_txt[fut]
            try:
                frag_pdf = fut.result()
                frag_pdf_of_txt[txt] = frag_pdf
                created.append(frag_pdf)
            except Exception as e:
                print(f"[WARN] {txt}: {e}", file=sys.stderr)

    # ── 2. merge by key (if requested) ──────────────────────────────────
    merged_paths: List[Path] = []
    if 1 <= group_level <= 2:
        for key, grp in groupby(
                sorted(frag_pdf_of_txt, key=lambda p: _folder_key(p, group_level)),
                key=lambda p: _folder_key(p, group_level)):
            writer = PdfWriter()
            for txt in grp:
                writer.append(str(frag_pdf_of_txt[txt]))
            vol_pdf = out / f"{key}.pdf"
            with vol_pdf.open("wb") as fh:
                writer.write(fh)
            merged_paths.append(vol_pdf)
            created.append(vol_pdf)

    # ── 3. clean-up intermediate PDFs & empty dirs ─────────────────────
    if not keep_fragments and merged_paths:
        _prune(list(frag_pdf_of_txt.values()), out)  # delete and prune
        created = merged_paths  # only keep finals

    return created


# ────────────────────────────  CLI wrapper  ────────────────────────────
def _cli() -> None:
    ap = argparse.ArgumentParser(
        description="Convert DMZJ txt downloads to PDFs and merge by folder key."
    )
    ap.add_argument("--root", required=True, help="Novel download root")
    ap.add_argument("--out", required=True, help="Destination directory")
    ap.add_argument("--group-level", type=int, default=2,
                    help="1=whole novel • 2=per-volume (default) • 3=no merge")
    ap.add_argument("--workers", type=int, default=None,
                    help=f"Concurrent conversions (default {DEFAULT_WORKERS})")
    ap.add_argument("--keep-fragments", action="store_true",
                    help="Keep per-chapter PDFs (skip clean-up)")
    ns = ap.parse_args()

    finals = convert_dmzj_txts_to_pdf(
        root_dir=ns.root,
        output_dir=ns.out,
        group_level=ns.group_level,
        workers=ns.workers,
        keep_fragments=ns.keep_fragments,
    )
    print(f"\n✅ Done. {len(finals)} final PDFs:")
    for p in finals:
        print(" •", p)


if __name__ == "__main__":
    _cli()
