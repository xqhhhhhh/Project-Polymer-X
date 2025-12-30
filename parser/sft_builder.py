import argparse
import json
import random
import re
from pathlib import Path
from typing import Dict, List


def load_records(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", name.lower())


def merge_records(pdf_records: List[Dict], html_records: List[Dict]) -> List[Dict]:
    merged: Dict[str, Dict] = {}

    def merge_into(target: Dict, src: Dict) -> None:
        for key, value in src.items():
            if key in {"source_type", "source_file", "vendor"}:
                continue
            if key not in target:
                target[key] = value
        target.setdefault("sources", []).append(
            {"type": src.get("source_type"), "file": src.get("source_file")}
        )

    for record in pdf_records + html_records:
        name = record.get("material_name")
        if not name:
            continue
        key = normalize_name(name)
        if key not in merged:
            merged[key] = {"material_name": name}
        merge_into(merged[key], record)

    return list(merged.values())


def templates() -> List[str]:
    return [
        "作为一名化工专家，请分析 {material_name} 的加工特性。",
        "请总结 {material_name} 的关键物性并给出适用场景。",
        "基于物性数据，评估 {material_name} 的薄膜应用表现。",
        "从材料工程角度解释 {material_name} 的性能优势。",
        "给出 {material_name} 的主要参数并简述加工建议。",
    ]


def build_output(record: Dict) -> str:
    parts = []
    if record.get("density") is not None:
        parts.append(f"密度为 {record['density']} {record.get('density_unit', '')}。")
    if record.get("melt_index") is not None:
        parts.append(
            f"熔融指数为 {record['melt_index']} {record.get('melt_index_unit', '')}。"
        )
    if record.get("tensile_strength") is not None:
        parts.append(
            f"拉伸强度为 {record['tensile_strength']} {record.get('tensile_strength_unit', '')}。"
        )
    if record.get("elongation") is not None:
        parts.append(
            f"断裂伸长率为 {record['elongation']} {record.get('elongation_unit', '')}。"
        )
    if record.get("melt_peak_temperature") is not None:
        parts.append(
            f"熔融峰值温度为 {record['melt_peak_temperature']} {record.get('melt_peak_temperature_unit', '')}。"
        )
    if record.get("vicat_softening_temperature") is not None:
        parts.append(
            f"维卡软化温度为 {record['vicat_softening_temperature']} {record.get('vicat_softening_temperature_unit', '')}。"
        )
    if not parts:
        parts.append("暂无完整物性数据，需要补充测试。")
    reasoning = []
    if record.get("density") is not None:
        if record["density"] < 0.92:
            reasoning.append("较低密度通常意味着更好的柔韧性与韧性")
        elif record["density"] > 0.94:
            reasoning.append("较高密度常对应更高刚性与耐热性")
    if record.get("melt_index") is not None:
        if record["melt_index"] <= 1.0:
            reasoning.append("低熔指通常代表更高分子量和更好的力学性能")
        elif record["melt_index"] >= 10:
            reasoning.append("较高熔指通常意味着更好的流动性与加工性")
    if record.get("tensile_strength") is not None and record["tensile_strength"] >= 20:
        reasoning.append("拉伸强度较高，适合承载或耐撕裂应用")
    if record.get("elongation") is not None and record["elongation"] >= 400:
        reasoning.append("断裂伸长率高，说明材料延展性好")
    reasoning_text = ""
    if reasoning:
        reasoning_text = "专家推理：综合物性来看，" + "；".join(reasoning) + "。"

    cite = ""
    if record.get("sources"):
        files = [s.get("file") for s in record["sources"] if s.get("file")]
        if files:
            cite = f" [cite: {', '.join(files[:2])}]"
    return "".join(parts) + reasoning_text + cite


def build_sft(records: List[Dict], count: int) -> List[Dict]:
    usable = [r for r in records if r.get("material_name")]
    if not usable:
        return []
    dataset: List[Dict] = []
    tmpl = templates()
    random.seed(13)
    idx = 0
    while len(dataset) < count:
        record = usable[idx % len(usable)]
        instruction = tmpl[idx % len(tmpl)].format(material_name=record["material_name"])
        output = build_output(record)
        dataset.append({"instruction": instruction, "input": "", "output": output})
        idx += 1
    return dataset


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Alpaca JSONL SFT dataset")
    parser.add_argument("--pdf", default="data/pdf_data.json")
    parser.add_argument("--html", default="data/html_data.json")
    parser.add_argument("--merged-out", default="data/merged_data.json")
    parser.add_argument("--out", default="data/sft_dataset.jsonl")
    parser.add_argument("--count", type=int, default=100)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    pdf_records = load_records(Path(args.pdf))
    html_records = load_records(Path(args.html))
    merged = merge_records(pdf_records, html_records)
    Path(args.merged_out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.merged_out).write_text(
        json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    sft = build_sft(merged, args.count)
    with Path(args.out).open("w", encoding="utf-8") as f:
        for row in sft:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"merged {len(merged)} records -> {args.merged_out}")
    print(f"sft {len(sft)} rows -> {args.out}")


if __name__ == "__main__":
    main()
