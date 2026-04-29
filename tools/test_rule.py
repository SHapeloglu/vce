#!/usr/bin/env python3
"""
tools/test_rule.py
==================
VCE Kural SQL Doğrulama CLI Aracı
===================================

Yeni bir vce_dq_rules kuralı yazmadan önce SQL'in doğru çalışıp
çalışmadığını ve beklenen sonucu verip vermediğini test eder.

KULLANIM:
  # Tek SQL doğrula
  python tools/test_rule.py \\
    --sql "SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_log WHERE status='failed'" \\
    --type threshold

  # Dosyadan SQL oku
  python tools/test_rule.py \\
    --file my_rule.sql \\
    --type anomaly \\
    --domain send_log \\
    --subdomain my_new_check

  # Mevcut kuralı test et (vce_dq_rules'dan ID ile)
  python tools/test_rule.py --rule-id 15

  # Tüm aktif kuralları test et (smoke test)
  python tools/test_rule.py --all

  # pre_sql ile birlikte test et
  python tools/test_rule.py \\
    --pre-sql "CREATE TEMPORARY TABLE tmp AS SELECT 1" \\
    --sql "SELECT COUNT(*) FROM tmp" \\
    --type threshold

ÇIKTI ÖRNEĞİ:
  ┌─────────────────────────────────────────────────────┐
  │  VCE Kural SQL Test Aracı                           │
  └─────────────────────────────────────────────────────┘
  Bağlantı  : mailsender → aws_mailsender_pro_v3  ✅
  Schema    : aws_mailsender_pro_v3

  ── SQL Çalıştırılıyor ──────────────────────────────
  SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_log
  WHERE status='failed'

  ── Sonuç ───────────────────────────────────────────
  Dönen değer : 142
  Çalışma süresi: 0.23s
  Tip         : threshold
  İhlal       : EVET ❌ (142 > 0)
  Tahmini aksiyon: fail → DAG task'ı başarısız olur

BAĞLANTI AYARLARI:
  Araç Airflow olmadan doğrudan MySQL'e bağlanır.
  Bağlantı bilgilerini aşağıdaki yöntemlerden biriyle sağlayın:

  1. Environment variables (önerilen):
       export VCE_MS_HOST=localhost
       export VCE_MS_PORT=3306
       export VCE_MS_USER=airflow_ms_dml
       export VCE_MS_PASSWORD=sifre
       export VCE_MS_DB=aws_mailsender_pro_v3

       export VCE_HOST=localhost
       export VCE_PORT=3306
       export VCE_USER=airflow_vce
       export VCE_PASSWORD=sifre
       export VCE_DB=vce

  2. .env dosyası (proje kökünde):
       VCE_MS_HOST=localhost
       ...

  3. CLI parametreleri:
       --ms-host localhost --ms-user airflow_ms_dml ...
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import statistics
from datetime import datetime
from typing import Any

# .env dosyasını yükle (varsa)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv kurulu değilse env var'lardan oku

try:
    import pymysql
    import pymysql.cursors
except ImportError:
    print("❌ PyMySQL kurulu değil: pip install pymysql")
    sys.exit(1)


# ── Renk sabitleri (terminal çıktısı) ────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"
DIM    = "\033[2m"

def c(color, text):
    """Renkli terminal çıktısı."""
    return f"{color}{text}{RESET}"

def ok(text):    return c(GREEN, f"✅ {text}")
def err(text):   return c(RED,   f"❌ {text}")
def warn(text):  return c(YELLOW,f"⚠️  {text}")
def info(text):  return c(BLUE,  f"ℹ️  {text}")
def header(text): return c(BOLD, text)


# ── Bağlantı ──────────────────────────────────────────────────────────────────

def get_mailsender_conn(args) -> pymysql.Connection:
    """
    aws_mailsender_pro_v3 bağlantısı.
    Kural SQL'lerini çalıştırmak için kullanılır.
    """
    return pymysql.connect(
        host    = args.ms_host,
        port    = args.ms_port,
        user    = args.ms_user,
        password= args.ms_password,
        database= args.ms_db,
        charset = "utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
    )


def get_vce_conn(args) -> pymysql.Connection:
    """
    vce schema bağlantısı.
    Mevcut kural ve baseline verilerini okumak için kullanılır.
    """
    return pymysql.connect(
        host    = args.vce_host,
        port    = args.vce_port,
        user    = args.vce_user,
        password= args.vce_password,
        database= args.vce_db,
        charset = "utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
    )


def test_connections(args) -> bool:
    """Her iki bağlantıyı test eder, başarı durumunu döner."""
    print(f"\n{header('── Bağlantı Kontrolü ──────────────────────────────────')}")
    all_ok = True

    # MailSender bağlantısı
    try:
        conn = get_mailsender_conn(args)
        ver = conn.server_version
        conn.close()
        print(f"  mailsender  → {args.ms_db} {ok(f'v{ver}')}")
    except Exception as e:
        print(f"  mailsender  → {args.ms_db} {err(str(e))}")
        all_ok = False

    # VCE bağlantısı
    try:
        conn = get_vce_conn(args)
        ver = conn.server_version
        conn.close()
        print(f"  vce         → {args.vce_db} {ok(f'v{ver}')}")
    except Exception as e:
        print(f"  vce         → {args.vce_db} {err(str(e))}")
        all_ok = False

    return all_ok


# ── SQL Çalıştırma ────────────────────────────────────────────────────────────

def run_sql(conn: pymysql.Connection, sql: str) -> tuple[list[dict], float]:
    """SQL çalıştırır, (sonuçlar, süre_saniye) döner."""
    start = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall() or []
    elapsed = time.perf_counter() - start
    return rows, elapsed


def extract_scalar(rows: list[dict]) -> float | None:
    """Sorgu sonucunun ilk satırının ilk sütununu döner."""
    if rows:
        val = list(rows[0].values())[0]
        try:
            return float(val)
        except (TypeError, ValueError):
            return val
    return None


# ── Sonuç Analizi ─────────────────────────────────────────────────────────────

def analyze_threshold(value: float) -> dict:
    """
    Threshold tipi kural analizi.
    Sonuç > 0 ise ihlal.
    """
    violation = value > 0
    return {
        "violation": violation,
        "message"  : f"Değer: {value:.2f} {'> 0 → İHLAL' if violation else '= 0 → PASS'}",
        "status"   : err("İHLAL") if violation else ok("PASS"),
    }


def analyze_anomaly(value: float, rule_id: int | None, args) -> dict:
    """
    Anomaly tipi kural analizi.
    vce schema'dan geçmiş değerleri çeker, Z-skoru hesaplar.
    """
    if rule_id is None:
        return {
            "violation": False,
            "message"  : "rule_id gerekli — vce'den geçmiş verisi çekilemedi",
            "status"   : warn("Baseline yok"),
        }

    try:
        conn = get_vce_conn(args)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT result_value FROM vce_dq_executions
                   WHERE rule_id = %s
                     AND result_status = 'Passed'
                     AND run_date >= NOW() - INTERVAL 30 DAY
                   ORDER BY run_date DESC
                   LIMIT 100""",
                (rule_id,)
            )
            rows = cur.fetchall()
        conn.close()

        values = [float(r["result_value"]) for r in rows if r["result_value"] is not None]

        if len(values) < 7:
            return {
                "violation": False,
                "message"  : f"Yetersiz geçmiş: {len(values)} değer (min 7 gerekli). Baseline biriktirilecek.",
                "status"   : warn("Yetersiz baseline"),
            }

        mean = statistics.mean(values)
        std  = statistics.stdev(values)

        if std == 0:
            return {
                "violation": False,
                "message"  : f"Std=0 — tüm geçmiş değerler {mean:.2f}. Anomali hesaplanamaz.",
                "status"   : warn("Std=0"),
            }

        z = abs(value - mean) / std
        threshold = 3.0
        violation = z > threshold

        return {
            "violation": violation,
            "message"  : (
                f"Değer={value:.2f} | Ort={mean:.2f} | Std={std:.2f} | "
                f"Z={z:.2f} | Eşik={threshold} → "
                f"{'ANOMALİ' if violation else 'Normal'}"
            ),
            "status"   : err(f"ANOMALİ (Z={z:.2f})") if violation else ok(f"Normal (Z={z:.2f})"),
            "extra"    : {
                "sample_count": len(values),
                "mean"        : round(mean, 4),
                "std"         : round(std, 4),
                "z_score"     : round(z, 4),
            }
        }

    except Exception as e:
        return {
            "violation": False,
            "message"  : f"Baseline okunamadı: {e}",
            "status"   : warn("Baseline hatası"),
        }


def analyze_freshness(value: float) -> dict:
    """Freshness tipi: 0=taze, 1=bayat."""
    violation = value > 0
    return {
        "violation": violation,
        "message"  : "Tablo bayat (taze veri yok)" if violation else "Tablo taze",
        "status"   : err("BAYAT") if violation else ok("TAZE"),
    }


# ── Tek SQL Testi ─────────────────────────────────────────────────────────────

def test_single_sql(args):
    """Tek bir SQL'i test eder, detaylı rapor yazdırır."""
    sql      = args.sql
    pre_sql  = args.pre_sql
    sql_type = args.type
    rule_id  = args.rule_id
    action   = args.action

    print(f"\n{header('── SQL Test Raporu ─────────────────────────────────────')}")
    print(f"  Tip    : {c(CYAN, sql_type)}")
    print(f"  Aksiyon: {c(RED if action == 'fail' else YELLOW, action)}")

    conn = get_mailsender_conn(args)

    try:
        # pre_sql
        if pre_sql:
            print(f"\n{header('── pre_sql ─────────────────────────────────────────')}")
            print(f"  {c(DIM, pre_sql[:200])}")
            _, pre_elapsed = run_sql(conn, pre_sql)
            print(f"  {ok(f'Tamamlandı ({pre_elapsed:.3f}s)')}")

        # Asıl SQL
        print(f"\n{header('── SQL ─────────────────────────────────────────────')}")
        # SQL'i satırlara böl, her satırı girintili yaz
        for line in sql.strip().split("\n"):
            print(f"  {c(DIM, line)}")

        rows, elapsed = run_sql(conn, sql)
        value = extract_scalar(rows)

        print(f"\n{header('── Sonuç ───────────────────────────────────────────')}")
        print(f"  Ham sonuç   : {c(BOLD, str(rows[:3]))}")
        print(f"  Scalar değer: {c(BOLD, str(value))}")
        print(f"  Çalışma süresi: {c(CYAN, f'{elapsed:.3f}s')}")

        # Tip bazlı analiz
        print(f"\n{header('── Analiz ──────────────────────────────────────────')}")
        if sql_type in ("threshold", "volume", "schema", "duplicate", "custom"):
            result = analyze_threshold(float(value) if value is not None else 0)
        elif sql_type == "anomaly":
            result = analyze_anomaly(float(value) if value is not None else 0, rule_id, args)
        elif sql_type == "freshness":
            result = analyze_freshness(float(value) if value is not None else 0)
        else:
            result = analyze_threshold(float(value) if value is not None else 0)

        print(f"  {result['status']}")
        print(f"  {result['message']}")

        if "extra" in result:
            for k, v in result["extra"].items():
                print(f"  {c(DIM, k)}: {v}")

        # Aksiyon tahmini
        print(f"\n{header('── Tahmini Airflow Etkisi ──────────────────────────')}")
        if result["violation"]:
            if action == "fail":
                print(f"  {err('DAG task başarısız olur + Teams/Slack bildirimi')}")
            else:
                print(f"  {warn('Teams/Slack uyarısı gider, DAG devam eder')}")
        else:
            print(f"  {ok('Herhangi bir aksiyon alınmaz')}")

        return result["violation"]

    finally:
        conn.close()


# ── Mevcut Kuralı Test Et ─────────────────────────────────────────────────────

def test_existing_rule(rule_id: int, args):
    """vce_dq_rules'dan kuralı yükler ve test eder."""
    print(f"\n{header(f'── Kural #{rule_id} Test Ediliyor ──────────────────────────')}")

    try:
        conn = get_vce_conn(args)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM vce_dq_rules WHERE id = %s", (rule_id,)
            )
            rule = cur.fetchone()
        conn.close()
    except Exception as e:
        print(err(f"vce_dq_rules okunamadı: {e}"))
        return

    if not rule:
        print(err(f"Kural #{rule_id} bulunamadı"))
        return

    print(f"  Domain    : {rule['rule_domain']}/{rule['rule_subdomain']}")
    print(f"  Tip       : {rule['check_type']}")
    print(f"  Aksiyon   : {rule['action']}")
    print(f"  Aktif     : {'Evet' if rule['active_flag'] else 'Hayır'}")
    print(f"  Yazar     : {rule.get('author', '-')}")
    print(f"  Açıklama  : {rule.get('description', '')[:100]}")

    # SQL'i args'a ata
    args.sql      = rule["sql_statement"]
    args.pre_sql  = rule.get("pre_sql_statement")
    args.type     = rule["check_type"]
    args.action   = rule["action"]
    args.rule_id  = rule_id

    test_single_sql(args)


# ── Tüm Kuralları Test Et (Smoke Test) ───────────────────────────────────────

def test_all_rules(args):
    """
    vce_dq_rules'daki tüm aktif kuralları sırayla çalıştırır.
    Smoke test olarak kullanılır: "şu an kaç kural başarısız?"
    """
    print(f"\n{header('── Tüm Aktif Kurallar — Smoke Test ─────────────────────')}")

    try:
        conn = get_vce_conn(args)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM vce_dq_rules WHERE active_flag=1 ORDER BY rule_domain, rule_subdomain"
            )
            rules = cur.fetchall()
        conn.close()
    except Exception as e:
        print(err(f"Kurallar yüklenemedi: {e}"))
        return

    print(f"  {len(rules)} aktif kural bulundu\n")

    results = {"pass": 0, "fail": 0, "error": 0}
    failed_rules = []

    ms_conn = get_mailsender_conn(args)

    for rule in rules:
        label = f"{rule['rule_domain']}/{rule['rule_subdomain']}"
        try:
            if rule.get("pre_sql_statement"):
                run_sql(ms_conn, rule["pre_sql_statement"])

            rows, elapsed = run_sql(ms_conn, rule["sql_statement"])
            value = extract_scalar(rows)
            scalar = float(value) if value is not None else 0.0

            violation = scalar > 0  # Basit threshold kontrolü (anomaly atlanır)

            if violation and rule["check_type"] not in ("anomaly",):
                results["fail"] += 1
                failed_rules.append({
                    "label" : label,
                    "value" : scalar,
                    "action": rule["action"],
                })
                status = err(f"FAIL ({scalar:.0f})")
            else:
                results["pass"] += 1
                status = ok(f"PASS ({scalar:.0f}, {elapsed:.2f}s)")

            print(f"  {label:<50} {status}")

        except Exception as e:
            results["error"] += 1
            print(f"  {label:<50} {err(f'HATA: {str(e)[:50]}')}")

    ms_conn.close()

    # Özet
    print(f"\n{header('── Smoke Test Özeti ────────────────────────────────────')}")
    print(f"  {ok(f'PASS  : {results[\"pass\"]}')} ")
    print(f"  {err(f'FAIL  : {results[\"fail\"]}')} ")
    print(f"  {warn(f'HATA  : {results[\"error\"]}')} ")

    if failed_rules:
        print(f"\n{header('  Başarısız Kurallar:')}")
        for r in failed_rules:
            action_color = RED if r["action"] == "fail" else YELLOW
            print(f"    {c(action_color, r['label'])} → değer={r['value']:.0f}, aksiyon={r['action']}")


# ── CLI Argümanları ───────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="VCE Kural SQL Doğrulama CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # ── Mod seçimi
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--rule-id", type=int, metavar="ID",
                      help="vce_dq_rules'dan kural ID ile test et")
    mode.add_argument("--all", action="store_true",
                      help="Tüm aktif kuralları smoke test et")

    # ── SQL
    parser.add_argument("--sql", help="Test edilecek SQL")
    parser.add_argument("--pre-sql", help="Asıl SQL'den önce çalışacak hazırlık SQL")
    parser.add_argument("--file", help="SQL'i dosyadan oku (.sql)")
    parser.add_argument("--type",
                        choices=["threshold","anomaly","freshness","volume",
                                 "schema","duplicate","custom"],
                        default="threshold",
                        help="Kural tipi (varsayılan: threshold)")
    parser.add_argument("--action",
                        choices=["fail","warn"],
                        default="fail",
                        help="Aksiyon tipi (varsayılan: fail)")
    parser.add_argument("--domain",    help="rule_domain (bilgi amaçlı)")
    parser.add_argument("--subdomain", help="rule_subdomain (bilgi amaçlı)")

    # ── MailSender bağlantısı
    ms = parser.add_argument_group("MailSender Bağlantısı (aws_mailsender_pro_v3)")
    ms.add_argument("--ms-host",     default=os.getenv("VCE_MS_HOST", "localhost"))
    ms.add_argument("--ms-port",     type=int, default=int(os.getenv("VCE_MS_PORT", "3306")))
    ms.add_argument("--ms-user",     default=os.getenv("VCE_MS_USER", "airflow_ms_dml"))
    ms.add_argument("--ms-password", default=os.getenv("VCE_MS_PASSWORD", ""))
    ms.add_argument("--ms-db",       default=os.getenv("VCE_MS_DB", "aws_mailsender_pro_v3"))

    # ── VCE bağlantısı
    vce = parser.add_argument_group("VCE Bağlantısı (vce schema)")
    vce.add_argument("--vce-host",     default=os.getenv("VCE_HOST", "localhost"))
    vce.add_argument("--vce-port",     type=int, default=int(os.getenv("VCE_PORT", "3306")))
    vce.add_argument("--vce-user",     default=os.getenv("VCE_USER", "airflow_vce"))
    vce.add_argument("--vce-password", default=os.getenv("VCE_PASSWORD", ""))
    vce.add_argument("--vce-db",       default=os.getenv("VCE_DB", "vce"))

    # ── Diğer
    parser.add_argument("--no-color", action="store_true",
                        help="Renksiz çıktı (CI/CD ortamları için)")
    parser.add_argument("--skip-conn-test", action="store_true",
                        help="Bağlantı testini atla")

    return parser


# ── Ana Giriş Noktası ─────────────────────────────────────────────────────────

def main():
    parser = build_parser()
    args   = parser.parse_args()

    # Renksiz mod
    if args.no_color:
        global GREEN, RED, YELLOW, BLUE, CYAN, BOLD, RESET, DIM
        GREEN = RED = YELLOW = BLUE = CYAN = BOLD = RESET = DIM = ""

    # Banner
    print(f"\n{c(BOLD+CYAN, '┌─────────────────────────────────────────────────────┐')}")
    print(f"{c(BOLD+CYAN, '│')}  {c(BOLD, 'VCE Kural SQL Test Aracı')}                           {c(BOLD+CYAN, '│')}")
    print(f"{c(BOLD+CYAN, '│')}  {c(DIM, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}                              {c(BOLD+CYAN, '│')}")
    print(f"{c(BOLD+CYAN, '└─────────────────────────────────────────────────────┘')}")

    # Bağlantı testi
    if not args.skip_conn_test:
        if not test_connections(args):
            print(f"\n{err('Bağlantı başarısız. Çıkılıyor.')}")
            sys.exit(1)

    # Dosyadan SQL oku
    if args.file:
        try:
            with open(args.file, "r", encoding="utf-8") as f:
                args.sql = f.read()
        except FileNotFoundError:
            print(err(f"Dosya bulunamadı: {args.file}"))
            sys.exit(1)

    # Mod seçimi
    if args.all:
        test_all_rules(args)

    elif args.rule_id:
        test_existing_rule(args.rule_id, args)

    elif args.sql:
        # Schema prefix kontrolü
        if "aws_mailsender_pro_v3." not in args.sql:
            print(f"\n{warn('SQL içinde schema prefix bulunamadı.')}")
            print(f"  Kural SQL\\'leri aws_mailsender_pro_v3.tablo_adi formatında olmalıdır.")
            print(f"  Devam etmek istiyor musunuz? [e/H] ", end="")
            if input().strip().lower() not in ("e", "evet", "y", "yes"):
                print("İptal edildi.")
                sys.exit(0)

        test_single_sql(args)

    else:
        parser.print_help()
        print(f"\n{warn('SQL, --rule-id veya --all parametresinden biri gerekli.')}")
        sys.exit(1)

    print()


if __name__ == "__main__":
    main()
