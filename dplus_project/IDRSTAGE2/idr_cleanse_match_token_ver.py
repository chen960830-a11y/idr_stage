# -*- coding: utf-8 -*-
"""
One-click IDR CleanseMatch - Bearer Token Inline
- Read input Excel
- Use CONFIG["BEARER_TOKEN"] directly (no /v3/token)
- Call /v1/match/cleanseMatch?candidateMaximumQuantity=1
- Save every response JSON to disk
- Extract fields based on IDRTitleV2.txt JSONPaths
- Special handling: matchDataProfileDesc mapping via matchDataProfileCode.xlsx
- Write Excel + filtered CSV

How to run:
 pip install pandas openpyxl requests
 python idr_cleanse_match_token_ver.py
"""

import os
import json
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple

import pandas as pd
import requests

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
    "CLIENT_ID": "c054de7487a14d8e8e22f54559855b28e11af6af8c9a496f9a09f7152661aee4",  # 客户端ID
    "CLIENT_SECRET": "2bd48c227f2e41f495cc07d6d8f087b1f681c5e728614ea78d19fbe63aa7a6d4",  # 客户端密钥
    "GRANT_TYPE": "client_credentials",  # 授权类型

    # ---- endpoints ----
    "CLEANSE_URL": "https://plus.dnb.com/v1/match/cleanseMatch",

    # ---- input/output ----
    "INPUT_EXCEL": os.path.join(SCRIPT_DIR, "IDR_input_sample.xlsx"),
    "OUTPUT_FOLDER": os.path.join(SCRIPT_DIR, "output"),
    "TITLE_FILE": os.path.join(SCRIPT_DIR, "IDRTitleV2.txt"),  # one JSONPath per line

    # ---- matchDataProfileDesc mapping file (your provided path) ----
    "MDP_MAP_XLSX": os.path.join(SCRIPT_DIR, "matchDataProfileCode.xlsx"),

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
def get_bearer_token() -> str:
    """
    从Token接口获取Bearer token
    """
    import base64
    
    token_url = CONFIG.get("TOKEN_URL", "")
    client_id = CONFIG.get("CLIENT_ID", "")
    client_secret = CONFIG.get("CLIENT_SECRET", "")
    
    if not token_url or not client_id or not client_secret:
        raise ValueError('请在 CONFIG 中配置 TOKEN_URL, CLIENT_ID 和 CLIENT_SECRET')
    
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
        print("[INIT] 成功获取Bearer token")
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


def main():
    # --- token validation ---
    token = (CONFIG.get("BEARER_TOKEN") or "").strip()
    if not token:
        # 如果没有直接提供token，通过接口获取
        token = get_bearer_token()
    else:
        print("[INIT] 使用CONFIG中配置的Bearer token")

    input_excel = CONFIG["INPUT_EXCEL"]
    output_folder = CONFIG["OUTPUT_FOLDER"]
    title_file = CONFIG["TITLE_FILE"]
    ensure_dir(output_folder)

    if not os.path.exists(input_excel):
        raise FileNotFoundError(f"找不到 INPUT_EXCEL: {input_excel}")
    if not os.path.exists(title_file):
        raise FileNotFoundError(f"找不到 TITLE_FILE: {title_file}")

    base = os.path.splitext(os.path.basename(input_excel))[0]
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    out_base = os.path.join(output_folder, f"{base}_IDR_{stamp}")
    out_excel = out_base + ".xlsx"
    out_csv = out_base + ".csv"
    json_out_dir = os.path.join(output_folder, f"{base}_IDR_JSON_{stamp}")

    if CONFIG["SAVE_JSON_ALWAYS"] and not CONFIG["REPLAY_JSON_ONLY"]:
        ensure_dir(json_out_dir)

    title_paths = read_title_file(title_file)
    print(f"[INIT] Loaded {len(title_paths)} JSONPaths from: {title_file}")

    mdp_map = load_mdp_map(CONFIG.get("MDP_MAP_XLSX", ""))

    df = pd.read_excel(input_excel, engine="openpyxl").fillna("")
    total = len(df)
    print(f"[INIT] Input rows: {total} from: {input_excel}")

    replay_only = bool(CONFIG["REPLAY_JSON_ONLY"])
    if replay_only:
        replay_folder = CONFIG["JSON_REPLAY_FOLDER"]
        if not replay_folder or not os.path.exists(replay_folder):
            raise ValueError("REPLAY_JSON_ONLY=True 时，必须配置 JSON_REPLAY_FOLDER 且路径存在。")
        print(f"[INIT] REPLAY_JSON_ONLY enabled. Using JSON folder: {replay_folder}")
    else:
        print("[INIT] Bearer token found in CONFIG (hidden). Start calling CleanseMatch ...")

    qps = float(CONFIG["QPS_LIMIT"] or 0.0)
    min_interval = (1.0 / qps) if qps > 0 else 0.0
    last_call_ts = 0.0
    timeout = int(CONFIG["REQUEST_TIMEOUT"])

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

        if replay_only:
            p = os.path.join(CONFIG["JSON_REPLAY_FOLDER"], json_name)
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as f:
                    try:
                        resp_json = json.load(f)
                    except Exception:
                        resp_json = {}
                http_code = "REPLAY"
                time_ms = "0"
            else:
                resp_json = {}
                http_code = "LOCAL_MISSING"
                time_ms = "0"
        else:
            params = {
                "candidateMaximumQuantity": str(CONFIG["CANDIDATE_MAX_QTY"]),
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

            now = time.time()
            if min_interval > 0 and last_call_ts > 0:
                gap = now - last_call_ts
                if gap < min_interval:
                    time.sleep(min_interval - gap)

            status, elapsed, j = call_cleanse_match(token, CONFIG["CLEANSE_URL"], params, timeout)
            last_call_ts = time.time()

            http_code = str(status)
            time_ms = str(elapsed)
            resp_json = j

            if CONFIG["SAVE_JSON_ALWAYS"]:
                with open(os.path.join(json_out_dir, json_name), "w", encoding="utf-8") as f:
                    json.dump(resp_json, f, ensure_ascii=False, indent=2)

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

        max_cand = int(CONFIG["CANDIDATE_MAX_QTY"])

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
                    "mber": tel,
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

    out_df = pd.DataFrame(rows, columns=all_cols)
    out_df.to_excel(out_excel, index=False, engine="openpyxl")

    filter_codes = set(CONFIG["FILTER_ERROR_CODES"] or set())
    filtered_df = out_df[(out_df["errorCode"] == "") & (~out_df["errorCode"].isin(filter_codes))].copy()
    filtered_df.to_csv(out_csv, index=False, sep=CONFIG["CSV_SEPARATOR"], encoding="utf-8")

    print("\n[DONE] Excel:", out_excel)
    print("[DONE] CSV: ", out_csv)
    if CONFIG["SAVE_JSON_ALWAYS"] and not replay_only:
        print("[DONE] JSON folder:", json_out_dir)
    print(f"[DONE] total={total} ok={ok_cnt} err={err_cnt}")


if __name__ == "__main__":
    main()