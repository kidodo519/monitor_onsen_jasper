#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, json, yaml, boto3, psycopg2, urllib.request, urllib.error, logging, argparse, traceback, socket
from typing import Optional, Tuple, List, Dict, Any
from psycopg2 import sql
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

os.environ.setdefault("PGCLIENTENCODING", "utf8")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", force=True)

def post_to_slack(webhook_url: str, text: str, blocks=None, timeout=10):
    payload = {"text": text}
    if blocks:
        payload["blocks"] = blocks
    req = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        resp.read()

def build_slack_blocks(results_subset, jst_today_str: str):
    def chunk_text(s, limit=2800):
        out, cur, cur_len = [], [], 0
        for line in s.splitlines():
            n = len(line) + 1
            if cur_len + n > limit and cur:
                out.append("\n".join(cur)); cur, cur_len = [line], n
            else:
                cur.append(line); cur_len += n
        if cur: out.append("\n".join(cur))
        return out
    header = f"データ監視アラート（{jst_today_str}）"
    sections = []
    for r in results_subset:
        if not r["errors"]:
            continue
        section = "\n".join([r["property"], *r["errors"], ""])
        for part in chunk_text(section):
            sections.append({"type": "section", "text": {"type": "mrkdwn", "text": part}})
    if not sections:
        return []
    return [{"type": "section", "text": {"type": "mrkdwn", "text": header}}] + sections

def send_slack_batches(webhook_url: str, header_text: str, blocks: list, max_blocks=40):
    if not blocks:
        return
    batch, count = [], 0
    for b in blocks:
        batch.append(b); count += 1
        if count >= max_blocks:
            post_to_slack(webhook_url, header_text, blocks=batch)
            batch, count = [], 0
    if batch:
        post_to_slack(webhook_url, header_text, blocks=batch)

def connect_db(dsn: str):
    conn = psycopg2.connect(dsn)
    conn.set_client_encoding('UTF8')
    return conn

FACILITY_FILTER_DISABLED = object()


def normalize_facility_filter_settings(
    base: Any,
    override: Any,
    fallback_column: Optional[str],
    fallback_operator: Optional[str],
    fallback_template: Optional[str],
) -> Optional[Dict[str, Any]]:
    def coerce(settings: Any) -> Any:
        if settings is None:
            return None
        if settings is False:
            return FACILITY_FILTER_DISABLED
        if isinstance(settings, str):
            return {"column": settings}
        if isinstance(settings, dict):
            if settings.get("enabled") is False:
                return FACILITY_FILTER_DISABLED
            if "column" in settings and not settings.get("column"):
                return FACILITY_FILTER_DISABLED
            return settings
        return None

    merged: Dict[str, Any] = {}

    coerced_base = coerce(base)
    if coerced_base is FACILITY_FILTER_DISABLED:
        return None
    if coerced_base:
        merged.update({k: v for k, v in coerced_base.items() if v is not None})

    coerced_override = coerce(override)
    if coerced_override is FACILITY_FILTER_DISABLED:
        return None
    if coerced_override:
        merged.update({k: v for k, v in coerced_override.items() if v is not None})

    if not merged and fallback_column:
        merged["column"] = fallback_column

    if "column" not in merged or not merged.get("column"):
        return None

    if fallback_operator and "operator" not in merged:
        merged["operator"] = fallback_operator
    if fallback_template and "value_template" not in merged and "value" not in merged:
        merged["value_template"] = fallback_template

    if "operator" not in merged or not merged.get("operator"):
        merged["operator"] = "="

    value_tpl = merged.pop("value", None)
    if value_tpl is not None and "value_template" not in merged:
        merged["value_template"] = value_tpl
    if "value_template" not in merged or not merged.get("value_template"):
        merged["value_template"] = "{code}"

    return merged


def render_facility_clause(
    filter_settings: Optional[Dict[str, Any]],
    facility: Optional[Dict[str, Any]],
) -> Tuple[Optional[sql.SQL], List[Any]]:
    if not filter_settings or not facility:
        return None, []

    column = filter_settings.get("column")
    if not column:
        return None, []

    operator_raw = (filter_settings.get("operator") or "=").strip().lower()
    value_template = filter_settings.get("value_template") or "{code}"

    ctx = dict(facility)
    ctx.setdefault("code", facility.get("code"))
    ctx.setdefault("facility_code", facility.get("code"))
    ctx.setdefault("name", facility.get("name"))
    ctx.setdefault("facility_name", facility.get("name"))

    try:
        value = value_template.format(**ctx)
    except Exception as e:
        raise ValueError(f"value_template format error: {e}")

    op_map = {
        "=": "=",
        "==": "=",
        "eq": "=",
        "!=": "!=",
        "ne": "!=",
        "<>": "!=",
        "like": "LIKE",
        "ilike": "ILIKE",
    }

    if operator_raw in ("startswith", "prefix"):
        op_sql = "LIKE"
        value = f"{value}%"
    elif operator_raw in ("endswith", "suffix"):
        op_sql = "LIKE"
        value = f"%{value}"
    elif operator_raw in ("contains", "substring"):
        op_sql = "LIKE"
        value = f"%{value}%"
    else:
        op_sql = op_map.get(operator_raw, operator_raw.upper())

    clause = sql.SQL("{col} {op} %s").format(
        col=sql.Identifier(column),
        op=sql.SQL(op_sql),
    )
    return clause, [value]


def has_rows_for_today(
    conn,
    schema: str,
    table: str,
    date_col: str,
    today_jst: datetime,
    facility: Optional[Dict[str, Any]] = None,
    facility_filter: Optional[Dict[str, Any]] = None,
) -> bool:
    with conn.cursor() as cur:
        q = sql.SQL("SELECT 1 FROM {s}.{t} WHERE {c}::date = %s").format(
            s=sql.Identifier(schema), t=sql.Identifier(table), c=sql.Identifier(date_col)
        )
        params = [today_jst.date()]
        clause, clause_params = render_facility_clause(facility_filter, facility)
        if clause is not None:
            q = q + sql.SQL(" AND ") + clause
            params.extend(clause_params)
        q = q + sql.SQL(" LIMIT 1")
        cur.execute(q, params)
        return cur.fetchone() is not None


def latest_created_at(
    conn,
    schema: str,
    table: str,
    created_col: str,
    facility: Optional[Dict[str, Any]] = None,
    facility_filter: Optional[Dict[str, Any]] = None,
):
    with conn.cursor() as cur:
        q = sql.SQL("SELECT MAX({c}) FROM {s}.{t}").format(
            c=sql.Identifier(created_col), s=sql.Identifier(schema), t=sql.Identifier(table)
        )
        params = []
        clause, clause_params = render_facility_clause(facility_filter, facility)
        if clause is not None:
            q = q + sql.SQL(" WHERE ") + clause
            params.extend(clause_params)
        cur.execute(q, params)
        row = cur.fetchone()
        return row[0] if row else None

def count_s3_files_today(s3_cli, bucket: str, prefix: str, today_jst: datetime) -> int:
    jst_start = datetime(today_jst.year, today_jst.month, today_jst.day, tzinfo=today_jst.tzinfo)
    jst_end = jst_start + timedelta(days=1)
    utc_start, utc_end = jst_start.astimezone(timezone.utc), jst_end.astimezone(timezone.utc)
    paginator = s3_cli.get_paginator("list_objects_v2")
    total = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            lm = obj["LastModified"]
            if utc_start <= lm <= utc_end:
                total += 1
    return total

def run_checks_for_property(prop: dict, defaults: dict, tz: ZoneInfo, today: datetime) -> dict:
    name = prop.get("name", "UNKNOWN")
    enabled = prop.get("enabled", True)
    results = {"property": name, "errors": [], "ok": [], "_slack_webhook": None}
    if not enabled:
        results["ok"].append("宿設定が無効です。")
        return results
    slack_webhook = (prop.get("slack") or {}).get("webhook_url") or (defaults.get("slack") or {}).get("webhook_url")
    results["_slack_webhook"] = slack_webhook
    s3_defaults = defaults.get("s3") or {}
    bucket = s3_defaults.get("bucket")
    prefix_tpl = s3_defaults.get("prefix_template", "{hotel_key}/pms-reservations/")
    require_min_default = int(s3_defaults.get("require_min_files", 1))
    db_defaults = defaults.get("db") or {}
    hotel_key = prop.get("hotel_key")
    db = prop.get("db", {}) or {}
    dsn = db.get("dsn")
    if not dsn:
        dsn_template = db_defaults.get("dsn_template")
        if dsn_template and hotel_key:
            dsn = dsn_template.format(hotel_key=hotel_key)
    schema = db.get("schema", "public")
    checks_default = defaults.get("checks") or {}
    checks_prop = prop.get("checks") or {}
    checks = {**checks_default, **checks_prop}
    raw_facilities = prop.get("facilities") or []
    facilities = []
    for idx, raw_fac in enumerate(raw_facilities, 1):
        if isinstance(raw_fac, str):
            facilities.append({"code": raw_fac, "name": raw_fac, "enabled": True})
        elif isinstance(raw_fac, dict):
            code = raw_fac.get("code") or raw_fac.get("facility_code")
            if not code:
                results["errors"].append(f"施設定義{idx}: codeが未設定のためスキップ。")
                continue
            facilities.append(
                {
                    "code": code,
                    "name": raw_fac.get("name") or code,
                    "enabled": raw_fac.get("enabled", True),
                }
            )
        else:
            results["errors"].append(
                f"施設定義{idx}: 不正な形式（{type(raw_fac).__name__}）。str もしくは dict を指定してください。"
            )

    def facility_label(fac: Optional[dict]) -> str:
        if not fac:
            return ""
        name = fac.get("name")
        code = fac.get("code")
        if name and code and name != code:
            return f"{name}({code})"
        return name or code or "UNKNOWN"

    if (checks.get("import_tables") or {}).get("enabled", False):
        if not dsn:
            results["errors"].append("① DB接続情報(dsn)未設定のためチェック不可。")
        else:
            try:
                with connect_db(dsn) as conn:
                    cfg_import = checks.get("import_tables") or {}
                    tables = cfg_import.get("tables", [])
                    default_col = cfg_import.get("date_column", "import_date")
                    default_facility_filter = normalize_facility_filter_settings(
                        base=cfg_import.get("facility_filter"),
                        override=None,
                        fallback_column=cfg_import.get("facility_column"),
                        fallback_operator=cfg_import.get("facility_operator"),
                        fallback_template=cfg_import.get("facility_value_template"),
                    )
                    default_facility_column = (
                        (default_facility_filter or {}).get("column")
                        or cfg_import.get("facility_column")
                    )
                    default_facility_operator = (
                        (default_facility_filter or {}).get("operator")
                        or cfg_import.get("facility_operator")
                    )
                    default_facility_template = (
                        (default_facility_filter or {}).get("value_template")
                        or cfg_import.get("facility_value_template")
                    )

                    def run_import_check(
                        table_name: str,
                        date_col: str,
                        fac: Optional[dict],
                        fac_filter: Optional[Dict[str, Any]],
                    ):
                        fac_label = facility_label(fac)
                        prefix = f"① {table_name}"
                        if fac_label:
                            prefix = f"{prefix} [{fac_label}]"
                        try:
                            ok = has_rows_for_today(
                                conn,
                                schema,
                                table_name,
                                date_col,
                                today,
                                facility=fac,
                                facility_filter=fac_filter,
                            )
                            if ok:
                                results["ok"].append(f"{prefix}: OK")
                            else:
                                results["errors"].append(
                                    f"{prefix}: {date_col} が『今日』のレコード無し"
                                )
                        except Exception as e:
                            results["errors"].append(
                                f"{prefix}: チェック失敗（{e.__class__.__name__}: {e}）"
                            )

                    for t in tables:
                        if isinstance(t, dict):
                            tname = t.get("name")
                            tcol = t.get("date_column", default_col)
                            inline_override: Dict[str, Any] = {}
                            has_inline_override = False
                            if "facility_column" in t:
                                inline_override["column"] = t.get("facility_column")
                                has_inline_override = True
                            if "facility_operator" in t:
                                inline_override["operator"] = t.get("facility_operator")
                                has_inline_override = True
                            if "facility_value_template" in t:
                                inline_override["value_template"] = t.get(
                                    "facility_value_template"
                                )
                                has_inline_override = True

                            table_facility_filter = normalize_facility_filter_settings(
                                base=default_facility_filter,
                                override=inline_override if has_inline_override else None,
                                fallback_column=(
                                    inline_override.get("column")
                                    if has_inline_override and inline_override.get("column") is not None
                                    else default_facility_column
                                ),
                                fallback_operator=(
                                    inline_override.get("operator")
                                    if has_inline_override and inline_override.get("operator") is not None
                                    else default_facility_operator
                                ),
                                fallback_template=(
                                    inline_override.get("value_template")
                                    if has_inline_override
                                    and inline_override.get("value_template") is not None
                                    else default_facility_template
                                ),
                            )

                            if "facility_filter" in t:
                                table_facility_filter = normalize_facility_filter_settings(
                                    base=table_facility_filter,
                                    override=t.get("facility_filter"),
                                    fallback_column=(
                                        (table_facility_filter or {}).get("column")
                                        or inline_override.get("column")
                                        if has_inline_override
                                        else default_facility_column
                                    ),
                                    fallback_operator=(
                                        (table_facility_filter or {}).get("operator")
                                        or inline_override.get("operator")
                                        if has_inline_override
                                        else default_facility_operator
                                    ),
                                    fallback_template=(
                                        (table_facility_filter or {}).get("value_template")
                                        or inline_override.get("value_template")
                                        if has_inline_override
                                        else default_facility_template
                                    ),
                                )
                        else:
                            tname = str(t)
                            tcol = default_col
                            table_facility_filter = default_facility_filter
                        if not tname:
                            results["errors"].append("① テーブル名未設定のエントリが存在します。")
                            continue
                        if facilities and table_facility_filter:
                            for fac in facilities:
                                if not fac.get("enabled", True):
                                    results["ok"].append(
                                        f"① {tname} [{facility_label(fac)}]: 施設設定が無効のためスキップ。"
                                    )
                                    continue
                                run_import_check(tname, tcol, fac, table_facility_filter)
                        else:
                            run_import_check(tname, tcol, None, table_facility_filter if not facilities else None)
            except Exception as e:
                results["errors"].append(f"① DB接続失敗（{e.__class__.__name__}: {e}）")
    if (checks.get("s3_uploads") or {}).get("enabled", False):
        if not bucket: results["errors"].append("② S3: bucket が未設定です。")
        if not hotel_key: results["errors"].append("② S3: hotel_key が未設定です。")
        if bucket and hotel_key:
            try:
                require_min = int((checks.get("s3_uploads") or {}).get("require_min_files", require_min_default))
                prefix = prefix_tpl.format(hotel_key=hotel_key)
                s3_cli = boto3.client("s3")
                cnt = count_s3_files_today(s3_cli, bucket, prefix, today)
                if cnt < require_min:
                    results["errors"].append(f"② S3: 今日({today.strftime('%Y-%m-%d')})のLastModified件数 {cnt} < 必要 {require_min} (bucket={bucket}, prefix={prefix})")
                else:
                    results["ok"].append(f"② S3: OK（{cnt}件）")
            except Exception as e:
                results["errors"].append(f"② S3チェック失敗（{e.__class__.__name__}: {e}）")
    if (checks.get("repeat_track_tags_stall") or {}).get("enabled", False):
        if not dsn:
            results["errors"].append("③ DB接続情報(dsn)未設定のためチェック不可。")
        else:
            try:
                with connect_db(dsn) as conn:
                    tcfg = checks.get("repeat_track_tags_stall") or {}
                    table = tcfg.get("table", "repeat_track_tags")
                    col = tcfg.get("created_at_column", "created_at")
                    max_days = int(tcfg.get("max_stall_days", 3))
                    default_repeat_filter = normalize_facility_filter_settings(
                        base=tcfg.get("facility_filter"),
                        override=None,
                        fallback_column=tcfg.get("facility_column"),
                        fallback_operator=tcfg.get("facility_operator"),
                        fallback_template=tcfg.get("facility_value_template"),
                    )

                    def run_repeat_check(
                        fac: Optional[dict], fac_filter: Optional[Dict[str, Any]]
                    ):
                        fac_label = facility_label(fac)
                        prefix = f"③ {table}"
                        if fac_label:
                            prefix = f"{prefix} [{fac_label}]"
                        latest = latest_created_at(
                            conn,
                            schema,
                            table,
                            col,
                            facility=fac,
                            facility_filter=fac_filter,
                        )
                        if latest is None:
                            results["errors"].append(f"{prefix}: データ無し（MAX({col})がNULL）")
                            return
                        if latest.tzinfo is None:
                            latest_local = latest.replace(tzinfo=tz)
                        else:
                            latest_local = latest.astimezone(tz)
                        now_jst = datetime.now(tz)
                        diff_days = (now_jst - latest_local).days
                        if diff_days >= max_days:
                            results["errors"].append(
                                f"{prefix}: 最終作成 {latest_local.strftime('%Y-%m-%d %H:%M:%S %Z')} / {max_days}日以上更新無し"
                            )
                        else:
                            results["ok"].append(
                                f"{prefix}: OK（最終 {latest_local.strftime('%Y-%m-%d %H:%M')}）"
                            )

                    if facilities and default_repeat_filter:
                        for fac in facilities:
                            if not fac.get("enabled", True):
                                results["ok"].append(
                                    f"③ {table} [{facility_label(fac)}]: 施設設定が無効のためスキップ。"
                                )
                                continue
                            run_repeat_check(fac, default_repeat_filter)
                    else:
                        run_repeat_check(None, default_repeat_filter if not facilities else None)
            except Exception as e:
                results["errors"].append(f"③ DB照会失敗（{e.__class__.__name__}: {e}）")
    return results

def probe_slack(url: str):
    if not url or not isinstance(url, str) or not url.startswith("https://hooks.slack.com/services/"):
        return {"ok": False, "error": "invalid_url"}
    try:
        post_to_slack(url, "monitor self-test", [{"type": "section", "text": {"type": "mrkdwn", "text": "self-test"}}], timeout=5)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def probe_dns(host: str, port: int = 443):
    try:
        socket.gethostbyname(host)
        with socket.create_connection((host, port), timeout=5) as s:
            return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def probe_s3(bucket: str, prefix: str):
    try:
        s3 = boto3.client("s3")
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        found = resp.get("KeyCount", 0)
        return {"ok": True, "key_count": found}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def probe_db(dsn: str):
    if not dsn:
        return {"ok": False, "error": "dsn_empty"}
    try:
        with connect_db(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def main():
    print("START")
    parser = argparse.ArgumentParser(description="宿ごとのデータ監視")
    parser.add_argument("-c", "--config", default="config.yaml")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--log-file", default="")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--probe-all", action="store_true")
    args = parser.parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    if args.log_file:
        fh = logging.FileHandler(args.log_file, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logging.getLogger().addHandler(fh)
    try:
        try:
            with open(args.config, "r", encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
        except UnicodeDecodeError:
            with open(args.config, "r", encoding="cp932") as f:
                cfg = yaml.safe_load(f)
            logging.warning("config.yaml を cp932 で読み込みました。可能なら UTF-8 で保存してください。")
    except Exception as e:
        print(f"CONFIG_LOAD_ERROR: {e}")
        print("RESULT: ERROR")
        return 1
    defaults = cfg.get("defaults") or {}
    props = cfg.get("properties") or []
    tz = ZoneInfo(defaults.get("timezone", "Asia/Tokyo"))
    today = datetime.now(tz)
    jst_today_str = today.strftime("%Y-%m-%d")
    global_slack = (defaults.get("slack") or {}).get("webhook_url")
    print(f"PROPERTIES: {len(props)}")
    if args.self_test:
        checks = []
        host = "hooks.slack.com"
        checks.append({"slack_dns": probe_dns(host)})
        checks.append({"slack_probe": probe_slack(global_slack)})
        s3d = defaults.get("s3") or {}
        if s3d:
            bucket = s3d.get("bucket"); tpl = s3d.get("prefix_template", "{hotel_key}/pms-reservations/")
            if props:
                hk = props[0].get("hotel_key")
                prefix = tpl.format(hotel_key=hk) if hk else tpl
                checks.append({"s3_probe": probe_s3(bucket, prefix)})
        dbd = defaults.get("db") or {}
        for p in props[:1]:
            hk = p.get("hotel_key"); dsn = (p.get("db") or {}).get("dsn")
            if not dsn and dbd.get("dsn_template") and hk:
                dsn = dbd["dsn_template"].format(hotel_key=hk)
            checks.append({"db_probe": probe_db(dsn)})
        print(json.dumps({"self_test": checks}, ensure_ascii=False, indent=2))
        if global_slack and isinstance(global_slack, str) and global_slack.startswith("https://hooks.slack.com/services/"):
            try:
                post_to_slack(global_slack, "monitor self-test", [{"type": "section", "text": {"type": "mrkdwn", "text": "self-test ok"}}])
                print("SELF_TEST_SLACK: SENT")
            except Exception as e:
                print(f"SELF_TEST_SLACK: FAIL {e}")
        print("RESULT: SELF_TEST_DONE")
        return 0
    if args.probe_all:
        report = []
        s3d = defaults.get("s3") or {}
        dbd = defaults.get("db") or {}
        for p in props:
            name = p.get("name", "UNKNOWN")
            hk = p.get("hotel_key")
            dsn = (p.get("db") or {}).get("dsn")
            if not dsn and dbd.get("dsn_template") and hk:
                dsn = dbd["dsn_template"].format(hotel_key=hk)
            bucket = s3d.get("bucket"); tpl = s3d.get("prefix_template", "{hotel_key}/pms-reservations/")
            prefix = tpl.format(hotel_key=hk) if hk else tpl
            url = (p.get("slack") or {}).get("webhook_url") or global_slack
            item = {
                "property": name,
                "slack_url_valid": bool(url and isinstance(url, str) and url.startswith("https://hooks.slack.com/services/")),
                "slack_probe": probe_slack(url) if url else {"ok": False, "error": "no_url"},
                "db_probe": probe_db(dsn),
                "s3_probe": probe_s3(bucket, prefix) if bucket else {"ok": False, "error": "no_bucket"},
            }
            report.append(item)
        print(json.dumps({"probe_all": report}, ensure_ascii=False, indent=2))
        print("RESULT: PROBE_DONE")
        return 0
    all_results, any_error = [], False
    for p in props:
        res = run_checks_for_property(p, defaults, tz, today)
        all_results.append(res)
        if res["errors"]:
            any_error = True
    if args.dry_run:
        print(json.dumps(all_results, ensure_ascii=False, indent=2))
        print("RESULT: ERROR" if any_error else "RESULT: OK")
        return 1 if any_error else 0
    if any_error:
        url_to_subset = {}
        for r in all_results:
            if not r["errors"]:
                continue
            url = r.get("_slack_webhook") or global_slack
            if not url or not isinstance(url, str) or not url.startswith("https://hooks.slack.com/services/"):
                logging.error(f"Slack Webhook不正のため送信不可: {r['property']}")
                continue
            url_to_subset.setdefault(url, []).append(r)
        sent_any = False
        header_text = f"データ監視アラート（{jst_today_str}）"
        for url, subset in url_to_subset.items():
            blocks = build_slack_blocks(subset, jst_today_str)
            if not blocks:
                continue
            try:
                send_slack_batches(url, header_text, blocks, max_blocks=40)
                sent_any = True
            except Exception as e:
                logging.error(f"Slack送信失敗: {e}")
        print(json.dumps(all_results, ensure_ascii=False, indent=2))
        print("RESULT: ERROR_SENT" if sent_any else "RESULT: ERROR_NO_SLACK")
        return 1
    else:
        print(json.dumps(all_results, ensure_ascii=False, indent=2))
        print("RESULT: OK")
        return 0

if __name__ == "__main__":
    try:
        code = main()
        sys.stdout.flush(); sys.stderr.flush()
        raise SystemExit(code)
    except SystemExit as e:
        raise
    except Exception as e:
        print(f"UNCAUGHT: {e}")
        traceback.print_exc()
        print("RESULT: ERROR")
        raise SystemExit(1)
