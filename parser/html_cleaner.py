import argparse
import json
import re
from pathlib import Path
from typing import Dict, List

from bs4 import BeautifulSoup

from pdf_extractor import (
    clean_line_noise,
    convert_value,
    extract_candidates,
    map_property,
    parse_shell_special,
    validate_value,
    PREFERRED_UNITS,
)


SKIP_TITLE_PREFIXES = (
    "MatWeb - The Online Materials Information Resource",
)


def extract_lines_from_html(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    lines: List[str] = []

    tables = soup.find_all("table")
    for table in tables:
        for row in table.find_all("tr"):
            cells = [cell.get_text(" ", strip=True) for cell in row.find_all("td")]
            if len(cells) < 2:
                continue
            if len(cells) >= 3:
                prop = cells[0]
                metric = cells[1]
                english = cells[2] if len(cells) > 2 else ""
                comments = cells[3] if len(cells) > 3 else ""
                metric_value = normalize_metric_cell(metric, comments)
                if metric_value:
                    lines.append(" ".join([prop, metric_value]).strip())
                elif english:
                    lines.append(" ".join([prop, english]).strip())
            else:
                prop = cells[0]
                value = cells[1]
                lines.append(" ".join([prop, value]).strip())

    if lines:
        return lines

    text = soup.get_text("\n", strip=True)
    return [line.strip() for line in text.split("\n") if line.strip()]


def extract_material_name(html: str, fallback: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for selector in ["h1", "#ctl00_ContentBody_lblMatName", "#ctl00_ContentBody_lnkMaterial", "#ctl00_SubHeader"]:
        node = soup.select_one(selector)
        if node:
            text = node.get_text(strip=True)
            if text:
                return text
    if soup.title and soup.title.get_text(strip=True):
        return soup.title.get_text(strip=True)
    return fallback


def normalize_metric_cell(metric: str, comments: str) -> str:
    metric = metric.strip()
    if not metric:
        return ""
    if comments:
        avg_match = re.search(r"Average value:\\s*([\\d\\.]+)\\s*([A-Za-z°/%μµ³²·/\\-]+)", comments)
        if avg_match:
            return f"{avg_match.group(1)} {avg_match.group(2)}"
    range_match = re.search(r"([\\d\\.]+)\\s*[-–~to]+\\s*([\\d\\.]+)\\s*([A-Za-z°/%μµ³²·/\\-]+)", metric)
    if range_match:
        lo = float(range_match.group(1))
        hi = float(range_match.group(2))
        unit = range_match.group(3)
        avg = round((lo + hi) / 2, 4)
        return f"{avg} {unit}"
    return metric


def should_skip_page(html: str) -> bool:
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(strip=True) if soup.title else ""
    if any(title.startswith(prefix) for prefix in SKIP_TITLE_PREFIXES):
        return True
    if "errorUser.aspx" in html or "msgid=" in html:
        return True
    return False


def process_html(path: Path) -> Dict:
    html = path.read_text(encoding="utf-8", errors="ignore")
    if should_skip_page(html):
        return {
            "material_name": path.stem,
            "source_type": "html",
            "source_file": path.name,
            "skipped": True,
            "skipped_reason": "overview_or_blocked",
        }
    material_name = extract_material_name(html, path.stem)
    lines = extract_lines_from_html(html)

    data = {
        "material_name": material_name,
        "source_type": "html",
        "source_file": path.name,
        "properties": {},
    }

    for line in lines:
        clean_text = clean_line_noise(line)
        candidates = extract_candidates(clean_text)
        if not candidates:
            res = parse_shell_special(clean_text)
            if res:
                candidates.append(res)
        if not candidates:
            continue

        first_val = candidates[0][0]
        split_val = str(int(first_val)) if first_val.is_integer() else str(first_val)
        name_part = clean_text.split(split_val)[0]
        mapped_key = map_property(name_part)
        if not mapped_key:
            continue

        best_val, best_unit = candidates[0]
        for v, u in candidates:
            if u in PREFERRED_UNITS:
                best_val, best_unit = v, u
                break

        final_val, final_unit = convert_value(best_val, best_unit)
        if not validate_value(mapped_key, final_val):
            continue

        if mapped_key == "tensile_strength":
            curr = data["properties"].get(mapped_key, {}).get("value", 0)
            if final_val > curr:
                data["properties"][mapped_key] = {"value": final_val, "unit": final_unit}
        else:
            data["properties"][mapped_key] = {"value": final_val, "unit": final_unit}

    if len(data["properties"]) < 2:
        return {
            "material_name": material_name,
            "source_type": "html",
            "source_file": path.name,
            "skipped": True,
            "skipped_reason": "insufficient_properties",
        }

    flat = {
        "material_name": data["material_name"],
        "source_type": "html",
        "source_file": data["source_file"],
    }
    for k, v in data["properties"].items():
        flat[k] = v["value"]
        flat[f"{k}_unit"] = v["unit"]
    return flat


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean HTML pages into structured JSON")
    parser.add_argument("--input-dir", default="data/html_pages")
    parser.add_argument("--out", default="data/html_data.json")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    files = sorted(input_dir.glob("*.html"))
    if not files:
        print(f"No HTML files found in {input_dir.absolute()}")
        return
    print(f"Processing {len(files)} files from {input_dir}...")
    results = []
    skipped = 0
    for path in files:
        try:
            record = process_html(path)
            if record.get("skipped"):
                skipped += 1
                continue
            results.append(record)
        except Exception as exc:
            print(f"Error in {path.name}: {exc}")
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Done! Saved to {out_path} (skipped {skipped} pages)")


if __name__ == "__main__":
    main()
