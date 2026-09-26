"""Write tiny PDFs for parser tests: text lines and ruled tables, Helvetica, ASCII only. Enough for
pdfplumber's text layer and its line-based table finder; no PDF library needed."""

from __future__ import annotations


def _escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def minimal_pdf(pages: list[dict]) -> bytes:
    """``pages``: ``[{"lines": [(x, y, text, size), ...], "rules": [(x0, y0, x1, y1), ...]}]`` in points,
    origin at the bottom left of a 612 x 792 page."""
    objects: list[bytes] = [b"", b"", b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"]
    kids = []
    for page in pages:
        ops = [f"BT /F1 {size} Tf {x} {y} Td ({_escape(text)}) Tj ET" for x, y, text, size in page.get("lines", [])]
        ops += [f"{x0} {y0} m {x1} {y1} l S" for x0, y0, x1, y1 in page.get("rules", [])]
        stream = "\n".join(ops).encode("latin-1")
        objects.append(b"<< /Length %d >>\nstream\n" % len(stream) + stream + b"\nendstream")
        content = len(objects)
        objects.append(
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 3 0 R >> >> "
            f"/Contents {content} 0 R >>".encode()
        )
        kids.append(len(objects))
    objects[0] = b"<< /Type /Catalog /Pages 2 0 R >>"
    objects[1] = f"<< /Type /Pages /Kids [{' '.join(f'{k} 0 R' for k in kids)}] /Count {len(kids)} >>".encode()
    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for number, body in enumerate(objects, 1):
        offsets.append(len(out))
        out += f"{number} 0 obj\n".encode() + body + b"\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(objects) + 1}\n0000000000 65535 f \n".encode()
    for offset in offsets:
        out += f"{offset:010d} 00000 n \n".encode()
    out += f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    return bytes(out)


def text_page(lines: list[str], *, top: int = 740, size: int = 11, leading: int = 16) -> dict:
    return {"lines": [(50, top - i * leading, line, size) for i, line in enumerate(lines)]}


def table_page(title: str, grid: list[list[str]], *, col_widths: list[int] | None = None, top: int = 700,
               row_height: int = 20) -> dict:
    """A title line and a fully ruled table (every cell boxed), as exported slides draw them."""
    widths = col_widths or [150] + [70] * (len(grid[0]) - 1)
    x_edges = [50]
    for w in widths:
        x_edges.append(x_edges[-1] + w)
    y_edges = [top - i * row_height for i in range(len(grid) + 1)]
    lines = [(50, top + 20, title, 14)]
    for r, row in enumerate(grid):
        for c, cell in enumerate(row):
            lines.append((x_edges[c] + 4, y_edges[r] - row_height + 6, cell, 10))
    rules = [(x_edges[0], y, x_edges[-1], y) for y in y_edges] + [(x, y_edges[-1], x, y_edges[0]) for x in x_edges]
    return {"lines": lines, "rules": rules}
