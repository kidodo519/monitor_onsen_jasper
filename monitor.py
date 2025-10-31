#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, json, yaml, boto3, psycopg2, urllib.request, urllib.error, logging, argparse, traceback, socket
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

def has_rows_for_today(conn, schema: str, table: str, date_col: str, today_jst: datetime) -> bool:
    with conn.cursor() as cur:
        q = sql.SQL("SELECT 1 FROM {s}.{t} WHERE {c}::date = %s LIMIT 1").format(
            s=sql.Identifier(schema), t=sql.Identifier(table), c=sql.Identifier(date_col)
        )
        cur.execute(q, (today_jst.date(),))
        return cur.fetchone() is not None

def latest_created_at(conn, schema: str, table: str, created_col: str):
    with conn.cursor() as cur:
        q = sql.SQL("SELECT MAX({c}) FROM {s}.{t}").format(
            c=sql.Identifier(created_col), s=sql.Identifier(schema), t=sql.Identifier(table)
        )
        cur.execute(q)
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
    if (checks.get("import_tables") or {}).get("enabled", False):
        if not dsn:
            results["errors"].append("① DB接続情報(dsn)未設定のためチェック不可。")
        else:
            try:
                with connect_db(dsn) as conn:
                    tables = (checks.get("import_tables") or {}).get("tables", [])
                    default_col = (checks.get("import_tables") or {}).get("date_column", "import_date")
                    for t in tables:
                        if isinstance(t, dict):
                            tname = t.get("name"); tcol = t.get("date_column", default_col)
                        else:
                            tname = str(t); tcol = default_col
                        try:
                            ok = has_rows_for_today(conn, schema, tname, tcol, today)
                            if ok: results["ok"].append(f"① {tname}: OK")
                            else:  results["errors"].append(f"① {tname}: {tcol} が『今日』のレコード無し")
                        except Exception as e:
                            results["errors"].append(f"① {tname}: チェック失敗（{e.__class__.__name__}: {e}）")
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
                    latest = latest_created_at(conn, schema, table, col)
                    if latest is None:
                        results["errors"].append(f"③ {table}: データ無し（MAX({col})がNULL）")
                    else:
                        if latest.tzinfo is None: latest = latest.replace(tzinfo=tz)
                        now_jst = datetime.now(tz)
                        diff_days = (now_jst - latest.astimezone(tz)).days
                        if diff_days >= max_days:
                            results["errors"].append(f"③ {table}: 最終作成 {latest.astimezone(tz).strftime('%Y-%m-%d %H:%M:%S %Z')} / {max_days}日以上更新無し")
                        else:
                            results["ok"].append(f"③ {table}: OK（最終 {latest.astimezone(tz).strftime('%Y-%m-%d %H:%M')}）")
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
