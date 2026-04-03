#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IDR CleanseMatch 前后端分离应用后端
"""

import os
import json
import time
import tempfile
from datetime import datetime
from typing import Any, Dict, List, Tuple

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import pandas as pd
import requests
import base64

# 获取当前脚本所在目录
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# =========================
# CONFIG (只改这里就能跑)
# =========================
CONFIG = {
    # ---- D&B Direct+ credentials ----
    "BEARER_TOKEN": "",  # 直接设置 bearer token（线下提供，粘贴到这里）
    # ---- Token获取参数 ----
    "TOKEN_URL": "https://plus.dnb.com/v3/token",  # Token获取接口
    "CLIENT_ID": "",  # 客户端ID
    "CLIENT_SECRET": "",  # 客户端密钥
    "GRANT_TYPE": "client_credentials",  # 授权类型

    # ---- endpoints ----
    "CLEANSE_URL": "https://plus.dnb.com/v1/match/cleanseMatch",

    # ---- input/output ----
    "INPUT_EXCEL": "",
    "OUTPUT_FOLDER": "",
    "TITLE_FILE": "",  # one JSONPath per line

    # ---- matchDataProfileDesc mapping file (your provided path) ----
    "MDP_MAP_XLSX": "",

    # ---- behavior ----
    "CANDIDATE_MAX_QTY": 3,     # candidateMaximumQuantity=1
    "QPS_LIMIT": 5.0,           # 0=不控速；5=约每秒最多5次
    "SAVE_JSON_ALWAYS": True,   # 每条响应都落盘
    "REPLAY_JSON_ONLY": False,  # True=不打API，直接用已保存JSON生成Excel（见 JSON_REPLAY_FOLDER）
    "JSON_REPLAY_FOLDER": r"",  # 当 REPLAY_JSON_ONLY=True 时填写（之前保存json的目录）

    # ---- request ----
    "REQUEST_TIMEOUT": 120,     # 秒

    # ---- progress logging ----
    "PRINT_EVERY": 20,          # 每处理多少行打印一次汇总（0=只打印每行）

    # ---- optional filtering ----
    "FILTER_ERROR_CODES": {"20502", "00048", "05005", "10002"},
    "CSV_SEPARATOR": "§",
}

# 设置路径为当前脚本所在目录
CONFIG["INPUT_EXCEL"] = os.path.join(SCRIPT_DIR, "IDR_input_sample.xlsx")
CONFIG["OUTPUT_FOLDER"] = os.path.join(SCRIPT_DIR, "output")
CONFIG["TITLE_FILE"] = os.path.join(SCRIPT_DIR, "IDRTitleV2.txt")
CONFIG["MDP_MAP_XLSX"] = os.path.join(SCRIPT_DIR, "matchDataProfileCode.xlsx")

# 创建FastAPI应用
app = FastAPI(
    title="IDR CleanseMatch API",
    description="D&B Direct+ IDR CleanseMatch 前后端分离应用后端API",
    version="1.0.0"
)

# 允许CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 在生产环境中应该设置具体的域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 提供静态文件服务
from fastapi.staticfiles import StaticFiles
app.mount("/static", StaticFiles(directory=SCRIPT_DIR), name="static")

# =========================
# JSONPath-like extraction (保持原脚本逻辑)
# =========================
def _split_tokens(path: str) -> List[str]:
    return [t.strip() for t in str(path).strip().split(".") if t.strip()]

def _get_by_token(cur: Any, token: str) -> Any:
    """
    token supports:
      - key
      - key[0]
      - key[*]
    """
    if isinstance(cur, list) and token.startswith("[") and token.endswith("]"):
        inner = token[1:-1].strip()
        if inner == "*":
            return cur
        try:
            idx = int(inner)
            return cur[idx] if 0 <= idx < len(cur) else None
        except Exception:
            return None

    if not isinstance(cur, dict):
        return None

    if "[" in token and token.endswith("]"):
        key, bracket = token.split("[", 1)
        key = key.strip()
        inner = bracket[:-1].strip()
        v = cur.get(key)
        if inner == "*":
            return v if isinstance(v, list) else []
        try:
            idx = int(inner)
            if isinstance(v, list) and 0 <= idx < len(v):
                return v[idx]
            return None
        except Exception:
            return None

    return cur.get(token)

def extract_jsonpath(obj: Any, path: str) -> str:
    """
    Supports:
      - a.b.c
      - a[0].b
      - a[*].b -> join multiple values with '\n'
    """
    if obj is None or not path or not str(path).strip():
        return ""
    tokens = _split_tokens(path)
    cur = obj
    for i, tok in enumerate(tokens):
        cur = _get_by_token(cur, tok)
        if cur is None:
            return ""
        if isinstance(cur, list) and "[*]" in tok and i < len(tokens) - 1:
            rest = ".".join(tokens[i + 1:])
            vals = []
            for item in cur:
                v = extract_jsonpath(item, rest)
                if v != "":
                    vals.append(v)
            return "\n".join(vals)

    if isinstance(cur, (dict, list)):
        return json.dumps(cur, ensure_ascii=False)
    return str(cur)

def normalize_title_path(p: str) -> Tuple[str, str]:
    """
    Decide whether title path should be evaluated on:
      - root json (scope='root')
      - candidate (scope='cand')
    Handle common prefixes:
      - matchCandidates[*].xxx
      - matchCandidates[0].xxx
    """
    p = (p or "").strip()
    if not p:
        return "root", ""
    if p.startswith("matchCandidates[*]."):
        return "cand", p[len("matchCandidates[*]."):]
    if p.startswith("matchCandidates[0]."):
        return "cand", p[len("matchCandidates[0]."):]
    if p.startswith("matchCandidates."):
        return "cand", p[len("matchCandidates."):]
    if p.startswith("organization.") or p.startswith("matchQualityInformation.") or p.startswith("displaySequence"):
        return "cand", p
    return "root", p


# ======================
# matchDataProfile mapping
# ======================
def load_mdp_map(xlsx_path: str) -> Dict[str, str]:
    """
    matchDataProfileCode.xlsx: col0=code, col1=description
    Return: {"01": "Primary Name.", ...}
    """
    if not xlsx_path or not os.path.exists(xlsx_path):
        print(f"[WARN] MDP_MAP_XLSX not found: {xlsx_path}")
        return {}
    df = pd.read_excel(xlsx_path, engine="openpyxl").fillna("")
    if df.shape[1] < 2:
        print(f"[WARN] MDP_MAP_XLSX has <2 columns: {xlsx_path}")
        return {}
    mp: Dict[str, str] = {}
    for _, row in df.iterrows():
        code_raw = str(row.iloc[0]).strip()
        desc = str(row.iloc[1]).strip()
        if not code_raw:
            continue
        try:
            code = f"{int(float(code_raw)):02d}"
        except Exception:
            code = code_raw.zfill(2)[:2]
        mp[code] = desc
    print(f"[INIT] Loaded matchDataProfile map: {len(mp)} rows from {xlsx_path}")
    return mp


# ======================
# IO helpers
# ======================
def build_json_name(cr1: str, cr2: str) -> str:
    try:
        return f"{int(float(cr1)):06d}_{int(float(cr2)):02d}.json"
    except Exception:
        return f"{str(cr1).zfill(6)}_{str(cr2).zfill(2)}.json"

def read_title_file(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        lines = [ln.strip() for ln in f.read().splitlines()]
    return [ln for ln in lines if ln and not ln.startswith("#")]

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)


# ======================
# API call (Token获取)
# ======================
def get_bearer_token(client_id: str, client_secret: str) -> str:
    """
    从Token接口获取Bearer token
    """
    token_url = CONFIG.get("TOKEN_URL", "")
    
    if not token_url or not client_id or not client_secret:
        raise ValueError('请提供 TOKEN_URL, CLIENT_ID 和 CLIENT_SECRET')
    
    # 生成Base64编码的client_id:client_secret
    auth_string = f"{client_id}:{client_secret}"
    base64_secret = base64.b64encode(auth_string.encode()).decode()
    
    payload = "grant_type=client_credentials&scope=read,write"
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "Authorization": f"Basic {base64_secret}"
    }
    
    try:
        response = requests.post(token_url, data=payload, headers=headers, timeout=30)
        response.raise_for_status()
        token_data = response.json()
        access_token = token_data.get("access_token")
        
        if not access_token:
            raise ValueError('获取Token失败：响应中没有access_token')
        
        print(f"[INIT] 成功获取Bearer token")
        return access_token
    except Exception as e:
        raise ValueError(f'获取Token失败：{str(e)}')


# ======================
# API call (CleanseMatch) - 仅从 CONFIG 取 token
# ======================
def call_cleanse_match(token: str, url: str, params: Dict[str, str], timeout: int) -> Tuple[int, int, Dict[str, Any]]:
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    t0 = time.time()
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    elapsed_ms = int((time.time() - t0) * 1000)
    try:
        j = r.json()
        if not isinstance(j, dict):
            j = {}
    except Exception:
        j = {}
    return r.status_code, elapsed_ms, j


# ======================
# 主要处理函数
# ======================
def process_cleanse_match(
    input_excel: str,
    title_file: str,
    mdp_map_xlsx: str,
    client_id: str,
    client_secret: str,
    candidate_max_qty: int = 3,
    qps_limit: float = 5.0,
    save_json_always: bool = True
) -> str:
    """
    处理CleanseMatch请求并返回结果文件路径
    """
    # 获取token
    token = get_bearer_token(client_id, client_secret)
    
    # 确保输出目录存在
    output_folder = os.path.join(SCRIPT_DIR, "output")
    ensure_dir(output_folder)
    
    # 验证输入文件
    if not os.path.exists(input_excel):
        raise FileNotFoundError(f"找不到 INPUT_EXCEL: {input_excel}")
    if not os.path.exists(title_file):
        raise FileNotFoundError(f"找不到 TITLE_FILE: {title_file}")
    
    # 准备输出文件
    base = "IDR_result"
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    out_base = os.path.join(output_folder, f"{base}_{stamp}")
    out_excel = out_base + ".xlsx"
    json_out_dir = os.path.join(output_folder, f"{base}_IDR_JSON_{stamp}")
    
    if save_json_always:
        ensure_dir(json_out_dir)
    
    # 读取标题文件
    title_paths = read_title_file(title_file)
    print(f"[INIT] Loaded {len(title_paths)} JSONPaths from: {title_file}")
    
    # 加载matchDataProfile映射
    mdp_map = load_mdp_map(mdp_map_xlsx)
    
    # 读取输入Excel
    df = pd.read_excel(input_excel, engine="openpyxl").fillna("")
    total = len(df)
    print(f"[INIT] Input rows: {total} from: {input_excel}")
    
    # 配置参数
    qps = float(qps_limit or 0.0)
    min_interval = (1.0 / qps) if qps > 0 else 0.0
    last_call_ts = 0.0
    timeout = int(CONFIG["REQUEST_TIMEOUT"])
    
    # 准备列名
    base_cols = [
        "rowNo",
        "displaySequence",
        "name",
        "countryCode",
        "registrationNumber",
        "streetAddressLine1",
        "addressLocality",
        "addressRegion",
        "postalCode",
        "telephoneNumber",
        "inLanguage",
        "domain",
        "customerReference1",
        "customerReference2",
        "customerReference3",
        "jsonName",
        "httpCode",
        "timeMs",
        "candidatesMatchedQuantity",
        "matchDataCriteria",
        "errorCode",
        "errorMessage",
    ]
    all_cols = base_cols + title_paths
    
    # 处理数据
    rows: List[Dict[str, str]] = []
    ok_cnt = 0
    err_cnt = 0
    print_every = int(CONFIG["PRINT_EVERY"] or 0)
    
    for idx, r in df.iterrows():
        row_no = idx + 1
        
        # mimic column mapping (0..13, skip 4) ——保持与原脚本一致
        name = str(r.iloc[0]).replace("\n", "")
        country = str(r.iloc[1]).replace("\n", "")
        reg_no = str(r.iloc[2]).replace("\n", "")
        street1 = str(r.iloc[3]).replace("\n", "")
        locality = str(r.iloc[5]).replace("\n", "") if df.shape[1] > 5 else ""
        region = str(r.iloc[6]).replace("\n", "") if df.shape[1] > 6 else ""
        postal = str(r.iloc[7]).replace("\n", "").strip() if df.shape[1] > 7 else ""
        tel = str(r.iloc[8]).replace("\n", "") if df.shape[1] > 8 else ""
        lang = str(r.iloc[9]).replace("\n", "").strip() if df.shape[1] > 9 else ""
        domain = str(r.iloc[10]).replace("\n", "").strip() if df.shape[1] > 10 else ""
        cr1 = str(r.iloc[11]).strip() if df.shape[1] > 11 else ""
        cr2 = str(r.iloc[12]).strip() if df.shape[1] > 12 else ""
        cr3 = str(r.iloc[13]).strip() if df.shape[1] > 13 else ""
        
        json_name = build_json_name(cr1, cr2)
        
        http_code = ""
        time_ms = ""
        resp_json: Dict[str, Any] = {}
        
        # 构建请求参数
        params = {
            "candidateMaximumQuantity": str(candidate_max_qty),
            "name": name,
            "countryISOAlpha2Code": country,
            "registrationNumber": reg_no,
            "streetAddressLine1": street1,
            "addressLocality": locality,
            "addressRegion": region,
            "postalCode": postal,
            "telephoneNumber": tel,
            "inLanguage": lang,
            "url": domain,
            "customerReference1": cr1,
            "customerReference2": cr2,
            "customerReference3": cr3,
        }
        
        # 控制QPS
        now = time.time()
        if min_interval > 0 and last_call_ts > 0:
            gap = now - last_call_ts
            if gap < min_interval:
                time.sleep(min_interval - gap)
        
        # 调用API
        status, elapsed, j = call_cleanse_match(token, CONFIG["CLEANSE_URL"], params, timeout)
        last_call_ts = time.time()
        
        http_code = str(status)
        time_ms = str(elapsed)
        resp_json = j
        
        # 保存JSON
        if save_json_always:
            with open(os.path.join(json_out_dir, json_name), "w", encoding="utf-8") as f:
                json.dump(resp_json, f, ensure_ascii=False, indent=2)
        
        # 处理响应
        candidates_qty = resp_json.get("candidatesMatchedQuantity", "")
        match_data_criteria = resp_json.get("matchDataCriteria", "")
        err = resp_json.get("error") if isinstance(resp_json.get("error"), dict) else {}
        error_code = str(err.get("errorCode", "") or "") if isinstance(err, dict) else ""
        error_msg = str(err.get("errorMessage", "") or "") if isinstance(err, dict) else ""
        
        candidates = resp_json.get("matchCandidates", [])
        if not isinstance(candidates, list):
            candidates = []
        
        if candidates:
            ok_cnt += 1
            print(f"[{row_no}/{total}] HTTP={http_code} {time_ms}ms candidates={len(candidates)} json={json_name}")
        else:
            err_cnt += 1
            print(f"[{row_no}/{total}] HTTP={http_code} {time_ms}ms candidates=0 err={error_code} json={json_name}")
        
        max_cand = int(candidate_max_qty)
        
        if candidates:
            for seq, cand in enumerate(candidates[:max_cand], start=1):
                out = {c: "" for c in all_cols}
                out.update({
                    "rowNo": str(row_no),
                    "displaySequence": str(seq),
                    "name": name,
                    "countryCode": country,
                    "registrationNumber": reg_no,
                    "streetAddressLine1": street1,
                    "addressLocality": locality,
                    "addressRegion": region,
                    "postalCode": postal,
                    "telephoneNumber": tel,
                    "inLanguage": lang,
                    "domain": domain,
                    "customerReference1": cr1,
                    "customerReference2": cr2,
                    "customerReference3": cr3,
                    "jsonName": json_name,
                    "httpCode": http_code,
                    "timeMs": time_ms,
                    "candidatesMatchedQuantity": str(candidates_qty),
                    "matchDataCriteria": str(match_data_criteria),
                    "errorCode": error_code,
                    "errorMessage": error_msg,
                })
                
                for p in title_paths:
                    p_clean = (p or "").strip()
                    
                    # Special handling: matchDataProfileDesc
                    if p_clean.endswith("matchDataProfileDesc"):
                        mdp_full = extract_jsonpath(cand, "matchQualityInformation.matchDataProfile")
                        mdp_code2 = (mdp_full[:2] if mdp_full else "").strip()
                        out[p] = mdp_map.get(mdp_code2, "")
                        continue
                    
                    scope, real_path = normalize_title_path(p_clean)
                    if not real_path:
                        out[p] = ""
                        continue
                    if scope == "root":
                        out[p] = extract_jsonpath(resp_json, real_path)
                    else:
                        out[p] = extract_jsonpath(cand, real_path)
                
                rows.append(out)
        else:
            out = {c: "" for c in all_cols}
            out.update({
                "rowNo": str(row_no),
                "displaySequence": "1",
                "name": name,
                "countryCode": country,
                "registrationNumber": reg_no,
                "streetAddressLine1": street1,
                "addressLocality": locality,
                "addressRegion": region,
                "postalCode": postal,
                "telephoneNumber": tel,
                "inLanguage": lang,
                "domain": domain,
                "customerReference1": cr1,
                "customerReference2": cr2,
                "customerReference3": cr3,
                "jsonName": json_name,
                "httpCode": http_code,
                "timeMs": time_ms,
                "candidatesMatchedQuantity": str(candidates_qty),
                "matchDataCriteria": str(match_data_criteria),
                "errorCode": error_code,
                "errorMessage": error_msg,
            })
            rows.append(out)
        
        if print_every > 0 and row_no % print_every == 0:
            print(f"[PROGRESS] processed={row_no}/{total} ok={ok_cnt} err={err_cnt}")
    
    # 生成Excel结果
    out_df = pd.DataFrame(rows, columns=all_cols)
    out_df.to_excel(out_excel, index=False, engine="openpyxl")
    
    print("\n[DONE] Excel:", out_excel)
    if save_json_always:
        print("[DONE] JSON folder:", json_out_dir)
    print(f"[DONE] total={total} ok={ok_cnt} err={err_cnt}")
    
    return out_excel


# ======================
# API路由
# ======================

@app.get("/")
async def root():
    return {"message": "IDR CleanseMatch API"}


@app.post("/process")
async def process(
    client_id: str = Form(...),
    client_secret: str = Form(...),
    input_excel: UploadFile = File(...),
    title_file: UploadFile = File(...),
    candidate_max_qty: int = Form(3),
    qps_limit: float = Form(5.0)
):
    """
    处理CleanseMatch请求
    """
    try:
        # 保存上传的文件
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp_excel:
            temp_excel.write(await input_excel.read())
            temp_excel_path = temp_excel.name
        
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as temp_title:
            temp_title.write(await title_file.read())
            temp_title_path = temp_title.name
        
        # 获取mdp_map文件路径
        mdp_map_path = os.path.join(SCRIPT_DIR, "matchDataProfileCode.xlsx")
        if not os.path.exists(mdp_map_path):
            raise FileNotFoundError(f"找不到 matchDataProfileCode.xlsx: {mdp_map_path}")
        
        # 处理请求
        result_file = process_cleanse_match(
            input_excel=temp_excel_path,
            title_file=temp_title_path,
            mdp_map_xlsx=mdp_map_path,
            client_id=client_id,
            client_secret=client_secret,
            candidate_max_qty=candidate_max_qty,
            qps_limit=qps_limit
        )
        
        # 返回结果文件
        # 确保文件名正确，没有前面的下划线
        filename = os.path.basename(result_file)
        # 移除文件名前面可能的下划线
        if filename.startswith('_'):
            filename = filename[1:]
        
        return FileResponse(
            path=result_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=filename
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        # 清理临时文件
        if 'temp_excel_path' in locals() and os.path.exists(temp_excel_path):
            os.unlink(temp_excel_path)
        if 'temp_title_path' in locals() and os.path.exists(temp_title_path):
            os.unlink(temp_title_path)


@app.get("/config")
async def get_config():
    """
    获取默认配置
    """
    return {
        "candidate_max_qty": CONFIG["CANDIDATE_MAX_QTY"],
        "qps_limit": CONFIG["QPS_LIMIT"]
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
