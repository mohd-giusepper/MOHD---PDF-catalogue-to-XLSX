# PDF to XLSX Tool

Local tool for extracting a product table from the Stelton pricelist PDF and exporting to XLSX.

## Setup

Install dependencies:

```bash
python -m pip install -r requirements.txt
```

Optional OCR dependencies (only if you use `--ocr`):

```bash
python -m pip install pdf2image pytesseract
```

## CLI usage

Use `main.py` with arguments (it routes to CLI). If `--input` is omitted, it looks in `./input` for a single PDF:

```bash
python main.py --input "1 EURO Stelton pricelist  2025-1.pdf" --output output.xlsx
```

Limit pages and print raw blocks:

```bash
python main.py --input "1 EURO Stelton pricelist  2025-1.pdf" --output output.xlsx --pages 2-5 --debug-blocks 5
```

Save debug JSON:

```bash
python main.py --input "1 EURO Stelton pricelist  2025-1.pdf" --output output.xlsx --debug-json debug_blocks.json
```

Export only one currency:

```bash
python main.py --input "1 EURO Stelton pricelist  2025-1.pdf" --output output.xlsx --currency-only EUR
```

## GUI

```bash
python main.py
```

The GUI defaults to `./input` and `./output` and supports an OCR fallback checkbox.

## Smoke test

```bash
$env:PYTHONPATH = "src"
python -m unittest tests.test_smoke
```

The smoke test checks:
- rows > 0
- all rows have `art_no`
- at least one row has `price_eur`

## Parser assumptions and heuristics

- PDF is digitally generated text (selectable); OCR is optional and only used if `--ocr` is passed.
- Product blocks are detected by a combination of `Art. no.` and `RRP`.
- Product title lines appear before `Colli:`; the first non-empty title line becomes `product_name_en`.
- Section/collection is taken from a short header line near the top of each page when detected.
- Variant extraction is conservative: only obvious color/material/size endings are moved into `variant`.
- Size parsing extracts `W`, `H`, `L` (and `D` as fallback for length) when in `cm`.

If a field is uncertain, it is left empty and the row is flagged with `needs_review`.
