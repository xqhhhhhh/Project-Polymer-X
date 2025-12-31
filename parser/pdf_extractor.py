import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set

import pdfplumber

# --- 1. 配置区域: 字段映射、单位归一与过滤规则 ---

# 属性映射表 (中文/英文 -> 标准字段)
PROPERTY_MAP = {
    "密度": "density",
    "density": "density",
    "比重": "density",
    "specificgravity": "density",
    
    "熔融指数": "melt_index",
    "meltindex": "melt_index",
    "meltflowrate": "melt_index",
    "meltflowindex": "melt_index",
    
    "熔融峰值温度": "melt_peak_temperature",
    "melttemperature": "melt_peak_temperature",
    "peakmeltingtemperature": "melt_peak_temperature",
    "熔点": "melt_peak_temperature",
    "meltingpoint": "melt_peak_temperature",
    
    "维卡软化温度": "vicat_softening_temperature",
    "vicat": "vicat_softening_temperature",
    
    "拉伸屈服强度": "tensile_strength_yield",
    "yieldstrength": "tensile_strength_yield",
    
    "拉伸断裂强度": "tensile_strength",
    "拉伸强度": "tensile_strength", # 默认优先取断裂
    "tensilestrength": "tensile_strength",
    "tensilebreak": "tensile_strength",
    
    "断裂伸长率": "elongation",
    "elongation": "elongation",
    "elongationatbreak": "elongation",
    
    "弯曲模量": "flexural_modulus",
    "flexuralmodulus": "flexural_modulus",
    "secantmodulus": "flexural_modulus"
}

# 单位归一化字典
UNIT_NORMALIZE = {
    "g/cm3": "g/cm³", "g/cc": "g/cm³", "g/cm^3": "g/cm³",
    "g/10min": "g/10min", "g/10 min": "g/10min", "dg/min": "g/10min",
    "mpa": "MPa",
    "psi": "psi",
    "°c": "°C", "℃": "°C", "c": "°C", "°f": "°F", "f": "°F",
    "%": "%",
    "g": "g",
    "n": "N",
    "j": "J"
}

# 只有这些单位是合法的 (白名单机制)
VALID_UNITS: Set[str] = {"g/cm³", "g/10min", "MPa", "psi", "°C", "°F", "%", "g", "N", "J"}

# 优先选择的单位 (公制)
PREFERRED_UNITS: Set[str] = {"g/cm³", "g/10min", "MPa", "°C", "%"}

# 必须忽略的行关键词 (过滤加工参数、注脚)
IGNORE_KEYWORDS = {
    "blow-up", "die gap", "screw", "extruder", "ratio", "temp profile", 
    "加工参数", "模头", "薄膜厚度", "film thickness", "typical value"
}

# 需要清除的噪音词 (防止 "MPa ExxonMobil" 这种连体词)
NOISE_TERMS = [
    r"ExxonMobil", r"Method", r"ASTM", r"ISO", r"GB/T", r"IEC",
    r"\bMD\b", r"\bTD\b", r"Test", r"Values", r"English", r"\bSI\b",
    r"Typical", r"Properties", r"Note", r"Data"
]

# 常见测试标准编号黑名单 (防止误判为数值)
STANDARD_NUMBERS = {1183, 1133, 527, 178, 306, 868, 790, 792, 1238, 1505, 1003, 2457}

# --- 2. 核心工具函数 ---

def clean_line_noise(line: str) -> str:
    """清除行内的噪音词，避免影响属性名识别"""
    cleaned = line
    for term in NOISE_TERMS:
        cleaned = re.sub(term, " ", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()

def normalize_property_name(name: str) -> str:
    """归一化属性名"""
    name = name.lower()
    name = re.sub(r"[\s\(\)（）/\-]", "", name)
    return name

def map_property(name_part: str) -> Optional[str]:
    """将文本头映射为标准字段名"""
    norm_name = normalize_property_name(name_part)
    # 按长度倒序匹配，优先匹配长词
    for key in sorted(PROPERTY_MAP.keys(), key=len, reverse=True):
        if key in norm_name:
            return PROPERTY_MAP[key]
    return None

def normalize_unit(unit: str) -> str:
    """标准化单位"""
    if not unit: return ""
    # 去除首尾非字母字符
    unit = re.sub(r"^[^a-zA-Z0-9%°]+|[^a-zA-Z0-9%°]+$", "", unit)
    key = unit.lower().replace(" ", "")
    return UNIT_NORMALIZE.get(key, unit)

def convert_value(value: float, unit: str) -> Tuple[float, str]:
    """数值换算 (psi -> MPa, F -> C)"""
    if unit.lower() == "psi":
        # 1 psi = 0.006895 MPa
        return round(value * 0.006895, 2), "MPa"
    
    if unit in ["°F", "F", "°f"]:
        # F to C
        return round((value - 32) * 5 / 9, 1), "°C"
        
    return value, unit

def validate_value_with_reason(key: str, value: float) -> Tuple[bool, str]:
    """返回 (是否通过, 失败原因)，用于脏数据日志"""
    if int(value) in STANDARD_NUMBERS and value.is_integer():
        return False, "standard_number"

    if key == "density":
        if value > 2.0 or value < 0.8:
            return False, "density_out_of_range"
    elif key == "melt_index":
        if value > 300:
            return False, "melt_index_out_of_range"
    elif key == "elongation":
        if value > 2000:
            return False, "elongation_out_of_range"
    elif "temperature" in key:
        if value > 500 or value < 0:
            return False, "temperature_out_of_range"

    return True, ""


def validate_value(key: str, value: float) -> bool:
    return validate_value_with_reason(key, value)[0]

def extract_candidates(text: str) -> List[Tuple[float, str]]:
    """提取 (数值, 单位) 对，兼容 Value-Unit / Unit-Value 两种排列"""
    candidates = []
    text = text.replace("%", " % ") # 预处理
    tokens = text.split()
    
    for i, token in enumerate(tokens):
        # 尝试解析数字
        try:
            val_str = re.sub(r"[^\d\.\-]", "", token)
            if not val_str: continue
            val = float(val_str)
        except ValueError:
            continue
            
        # 向右找单位 (Value Unit)
        if i + 1 < len(tokens):
            raw_unit = tokens[i+1]
            norm_unit = normalize_unit(raw_unit)
            if norm_unit in VALID_UNITS:
                candidates.append((val, norm_unit))
                continue
                
        # 向左找单位 (Unit Value) - Shell 格式
        if i - 1 >= 0:
            raw_unit = tokens[i-1]
            norm_unit = normalize_unit(raw_unit)
            if norm_unit in VALID_UNITS:
                candidates.append((val, norm_unit))
    
    return candidates

def parse_shell_special(line: str) -> Optional[Tuple[float, str]]:
    """Shell 专用兜底策略：行尾数值提取"""
    tokens = line.split()
    if not tokens: return None
    try:
        # 取最后一个数字
        last_val_str = re.sub(r"[^\d\.\-]", "", tokens[-1])
        if not last_val_str: return None
        last_val = float(last_val_str)
        
        # 尝试取倒数第二个词做单位
        unit_candidate = normalize_unit(tokens[-2]) if len(tokens) > 1 else ""
        
        if unit_candidate in VALID_UNITS:
            return last_val, unit_candidate
        
        # 如果没有单位，但数值像密度/熔指，且不是标准号，则返回无单位数值(后续逻辑可能会丢弃，但至少给了个机会)
        if last_val not in STANDARD_NUMBERS:
             return last_val, "unknown" # 标记为未知单位
             
        return None
    except:
        return None


def normalize_cell(cell: str) -> str:
    if cell is None:
        return ""
    return re.sub(r"\s+", " ", str(cell)).strip()


def normalize_metric_cell(metric: str, comments: str) -> str:
    metric = metric.strip()
    if not metric:
        return ""
    if comments:
        avg_match = re.search(r"Average value:\s*([\d\.]+)\s*([A-Za-z°/%μµ³²·/\-]+)", comments)
        if avg_match:
            return f"{avg_match.group(1)} {avg_match.group(2)}"
    range_match = re.search(r"([\d\.]+)\s*[-–~to]+\s*([\d\.]+)\s*([A-Za-z°/%μµ³²·/\-]+)", metric)
    if range_match:
        lo = float(range_match.group(1))
        hi = float(range_match.group(2))
        unit = range_match.group(3)
        avg = round((lo + hi) / 2, 4)
        return f"{avg} {unit}"
    return metric


def extract_property_lines_from_table(rows: List[List[str]]) -> List[str]:
    """从表格结构还原为“属性 + 值 + 单位”的行文本"""
    lines: List[str] = []
    metric_idx = None
    unit_idx = None
    value_idx = None

    for row in rows:
        if not any(row):
            continue
        lower = [c.lower() for c in row]
        if any("metric" in c for c in lower) and any("english" in c for c in lower):
            metric_idx = next(i for i, c in enumerate(lower) if "metric" in c)
            continue
        if any("unit" in c or "单位" in c for c in row) and any("value" in c or "数值" in c for c in row):
            unit_idx = next(i for i, c in enumerate(row) if "unit" in c.lower() or "单位" in c)
            value_idx = next(i for i, c in enumerate(row) if "value" in c.lower() or "数值" in c)
            continue
        if any("properties" in c for c in lower) or any("性能" in c for c in row):
            continue

        prop = row[0]
        if not prop or len(prop) > 120:
            continue

        if metric_idx is not None and len(row) > metric_idx:
            metric = row[metric_idx]
            comments = row[metric_idx + 2] if len(row) > metric_idx + 2 else ""
            metric_value = normalize_metric_cell(metric, comments)
            if metric_value:
                lines.append(f"{prop} {metric_value}")
            continue

        if unit_idx is not None and value_idx is not None:
            if len(row) > max(unit_idx, value_idx):
                unit = row[unit_idx]
                value = row[value_idx]
                lines.append(f"{prop} {value} {unit}")
            continue

        if len(row) >= 3:
            lines.append(f"{prop} {row[1]} {row[2]}")
        elif len(row) >= 2:
            lines.append(f"{prop} {row[1]}")

    return lines

# --- 3. 主流程 ---

def process_pdf(pdf_path: Path, dirty_log: List[Dict]) -> Dict:
    data = {
        "material_name": pdf_path.stem,
        "source_file": pdf_path.name,
        "properties": {}
    }
    
    lines = []
    full_text_cache = []
    
    table_lines: List[str] = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                full_text_cache.append(text)
                lines.extend([line.strip() for line in text.split('\n') if line.strip()])
            for table in page.extract_tables() or []:
                rows = [[normalize_cell(cell) for cell in row] for row in table if row]
                if rows:
                    table_lines.extend(extract_property_lines_from_table(rows))

    full_text = " ".join(full_text_cache)
    
    # 厂商判断：用于双列或中英混排的兜底策略
    is_shell = "中海壳牌" in full_text or "CNOOC" in full_text or "Shell" in full_text
    is_exxon = "ExxonMobil" in full_text
    
    # 提取材料名 (尝试在前10行找)
    for line in lines[:10]:
        if "Enable" in line or "Exceed" in line:
            data["material_name"] = line.strip()
            break
        if is_shell and re.match(r"^\d{4}[A-Z]+", line): # 匹配 2420D 这种格式
            data["material_name"] = line.strip()
            break

    def handle_line(line: str) -> None:
        if any(bad in line.lower() for bad in IGNORE_KEYWORDS):
            return
            
        clean_text = clean_line_noise(line)
        candidates = extract_candidates(clean_text)
        
        # Shell 兜底逻辑
        if not candidates and is_shell:
            res = parse_shell_special(clean_text)
            if res: candidates.append(res)
        
        if not candidates:
            return
        
        # 映射属性名
        # 截取第一个数值前的文本作为 Key 候选
        first_val = candidates[0][0]
        # 简单转换成str防止正则报错
        split_val = str(int(first_val)) if first_val.is_integer() else str(first_val)
        
        # 防止分割出错，只取行首的一段
        name_part = clean_text.split(split_val)[0]
        mapped_key = map_property(name_part)
        
        if mapped_key:
            # 优选公制单位
            best_val, best_unit = candidates[0]
            for v, u in candidates:
                if u in PREFERRED_UNITS:
                    best_val, best_unit = v, u
                    break
            
            # 转换与校验
            final_val, final_unit = convert_value(best_val, best_unit)
            
            # 【关键步骤】数据校验：如果不合法，记录脏数据
            ok, reason = validate_value_with_reason(mapped_key, final_val)
            if not ok:
                dirty_log.append(
                    {
                        "source_file": pdf_path.name,
                        "field": mapped_key,
                        "value": final_val,
                        "unit": final_unit,
                        "reason": reason,
                    }
                )
                return
            
            # 存储 (Tensile 取最大值逻辑)
            if mapped_key == "tensile_strength":
                curr = data["properties"].get(mapped_key, {}).get("value", 0)
                if final_val > curr:
                    data["properties"][mapped_key] = {"value": final_val, "unit": final_unit}
            else:
                data["properties"][mapped_key] = {"value": final_val, "unit": final_unit}
        return

    # 先用表格结构化结果，再回退到行文本（避免跨行错位）
    for line in table_lines:
        handle_line(line)
    for line in lines:
        handle_line(line)

    # 拍平输出
    flat_data = {
        "material_name": data["material_name"],
        "source_type": "pdf",
        "source_file": data["source_file"],
        "vendor": "Shell" if is_shell else ("ExxonMobil" if is_exxon else "Unknown")
    }
    for k, v in data["properties"].items():
        flat_data[k] = v["value"]
        flat_data[f"{k}_unit"] = v["unit"]
        
    return flat_data

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract material properties from PDFs")
    parser.add_argument("--input-dir", default="data_src")
    parser.add_argument("--out", default="data/pdf_data.json")
    parser.add_argument("--dirty-log", default="data/dirty_data.log")
    return parser.parse_args()


def main():
    args = parse_args()
    input_dir = Path(args.input_dir)
    output_file = Path(args.out)
    dirty_log_path = Path(args.dirty_log)
    pdf_files = list(input_dir.glob("*.pdf"))
    if not pdf_files:
        print(f"No PDF files found in {input_dir.absolute()}")
        return

    print(f"Processing {len(pdf_files)} files from {input_dir}...")
    results = []
    dirty_log: List[Dict] = []
    for f in pdf_files:
        try:
            res = process_pdf(f, dirty_log)
            results.append(res)
        except Exception as e:
            print(f"Error in {f.name}: {e}")

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    dirty_log_path.parent.mkdir(parents=True, exist_ok=True)
    if dirty_log:
        with dirty_log_path.open("w", encoding="utf-8") as f:
            for item in dirty_log:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
    print(f"Done! Saved to {output_file}")

if __name__ == "__main__":
    main()
