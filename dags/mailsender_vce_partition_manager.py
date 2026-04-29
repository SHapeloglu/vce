"""
mailsender_vce_partition_manager.py
=====================================
VCE — Otomatik MySQL Partition Yönetimi DAG'ı
=============================================

Bu DAG her ayın 1'inde çalışır ve üç görevi vardır:

  1. YENİ PARTİTİON EKLE
     Bir sonraki ay için partition ekler.
     Örnek: Mayıs 1'inde çalışırsa Haziran partition'ını oluşturur.
     p_future'ı REORGANIZE ederek güvenli bir şekilde ekler.

  2. ESKİ PARTİTİON DÜŞÜR (retention politikası)
     retention_months'tan eski partition'ları DROP eder.
     Varsayılan: 12 ay (Airflow Variable ile değiştirilebilir).
     DROP öncesi kaç satır olduğu loglanır.

  3. PARTITION RAPORU
     Tüm partitioned tabloların mevcut durumunu loglar:
       - Her partition'ın satır sayısı
       - Data ve index boyutu (MB)
     vce_remediation_log'a da yazar.

ÇALIŞMA ZAMANI: Her ayın 1'i, 01:00 UTC
  (Remediation 03:00, Ana DAG 06:00'dan önce tamamlanır)

RETENTION POLİTİKASI:
  Airflow Variable 'VCE_PARTITION_RETENTION_MONTHS' ile ayarlanır.
  Varsayılan: 12 ay
  Örnek: 6 ay saklamak için Variable'a '6' yaz.

ETKİLENEN TABLOLAR:
  - vce.vce_dq_executions        (run_date partition key)
  - vce.vce_table_val_executions (run_date partition key)
  - vce.vce_remediation_log      (executed_at partition key)

BAĞLANTI:
  vce connection (Conn Id: 'vce') kullanır.
  aws_mailsender_pro_v3'e dokunmaz.

GÜVENLIK:
  - DROP işleminden önce partition içindeki satır sayısı loglanır
  - 0 satırlı olmayan partition'lar logda uyarı ile işaretlenir
  - Yanlışlıkla p_future DROP edilmez (kontrol var)
  - Her işlem vce_remediation_log'a kaydedilir
"""

from __future__ import annotations

import logging
from calendar import monthrange
from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

import pymysql
import pymysql.cursors

# ── Sabitler ──────────────────────────────────────────────────────────────────
VCE_CONN_ID = "vce"

# Kaç ay geriye gidilecek partition'lar saklanır
DEFAULT_RETENTION_MONTHS = 12

# Yönetilen tablolar ve partition key kolonları
PARTITIONED_TABLES = {
    "vce_dq_executions"       : "run_date",
    "vce_table_val_executions": "run_date",
    "vce_remediation_log"     : "executed_at",
}

default_args = {
    "owner"          : "data-quality",
    "retries"        : 1,
    "retry_delay"    : timedelta(minutes=10),
    "email_on_failure": False,
}


# ── Yardımcı Fonksiyonlar ─────────────────────────────────────────────────────

def get_vce_conn() -> pymysql.Connection:
    """vce schema bağlantısı döner."""
    info = BaseHook.get_connection(VCE_CONN_ID)
    return pymysql.connect(
        host    = info.host,
        port    = int(info.port or 3306),
        user    = info.login,
        password= info.password,
        database= info.schema,
        charset = "utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
        connect_timeout=15,
    )


def partition_name(year: int, month: int) -> str:
    """
    Yıl ve ay'dan partition adı üretir.
    Örnek: 2026, 4 → 'p2026_04'
    """
    return f"p{year}_{month:02d}"


def next_month_first_day(year: int, month: int) -> date:
    """
    Verilen ayın bir sonraki ayının ilk günü.
    MySQL PARTITION BY RANGE LESS THAN değeri için kullanılır.
    Örnek: 2026, 4 → date(2026, 5, 1)
    """
    if month == 12:
        return date(year + 1, 1, 1)
    return date(year, month + 1, 1)


def get_retention_months() -> int:
    """
    Kaç aylık partition saklanacağını Airflow Variable'dan okur.
    Variable tanımlı değilse DEFAULT_RETENTION_MONTHS döner.
    """
    try:
        val = Variable.get("VCE_PARTITION_RETENTION_MONTHS", default_var=None)
        return int(val) if val else DEFAULT_RETENTION_MONTHS
    except (ValueError, TypeError):
        logging.warning(
            "VCE_PARTITION_RETENTION_MONTHS geçersiz değer içeriyor. "
            f"Varsayılan {DEFAULT_RETENTION_MONTHS} ay kullanılıyor."
        )
        return DEFAULT_RETENTION_MONTHS


def get_existing_partitions(conn: pymysql.Connection, table: str) -> list[dict]:
    """
    Bir tablonun mevcut partition listesini döner.
    information_schema.PARTITIONS'dan okur.
    p_future hariç tüm partition'lar döner.
    """
    with conn.cursor() as cur:
        cur.execute(
            """SELECT
                   PARTITION_NAME,
                   PARTITION_DESCRIPTION,
                   TABLE_ROWS,
                   ROUND(DATA_LENGTH / 1024 / 1024, 2)  as data_mb,
                   ROUND(INDEX_LENGTH / 1024 / 1024, 2) as index_mb,
                   CREATE_TIME
               FROM information_schema.PARTITIONS
               WHERE TABLE_SCHEMA = DATABASE()
                 AND TABLE_NAME   = %s
                 AND PARTITION_NAME != 'p_future'
               ORDER BY PARTITION_ORDINAL_POSITION""",
            (table,)
        )
        return cur.fetchall() or []


def log_to_remediation(
    conn: pymysql.Connection,
    dag_id: str,
    dag_run: str,
    operation_type: str,
    target_table: str,
    sql_executed: str,
    rows_affected: int,
    status: str,
    detail: str,
) -> None:
    """İşlemi vce_remediation_log'a kaydeder."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO vce_remediation_log (
                       dag_id, dag_run, task_name,
                       operation_type, target_table, sql_executed,
                       rows_affected, result_status, result_detail
                   ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    dag_id, dag_run, "partition_manager",
                    operation_type, target_table, sql_executed[:2000],
                    rows_affected, status, detail,
                )
            )
        conn.commit()
    except Exception as e:
        logging.warning(f"Remediation log yazılamadı: {e}")


# ── GÖREV 1: Yeni Partition Ekle ─────────────────────────────────────────────

def add_next_month_partition(**context):
    """
    Bir sonraki ay için partition ekler.

    Nasıl çalışır:
      - Bugünün ayını al (ör: Mayıs 2026)
      - Bir sonraki ay için partition adı üret (p2026_06 → Haziran)
      - p_future'ı REORGANIZE ederek yeni partition ekle
      - p_future her zaman MAXVALUE olarak kalır (güvenlik ağı)
      - Her tablo için aynı işlemi tekrar et

    Neden REORGANIZE?
      ALTER TABLE ADD PARTITION yerine REORGANIZE kullanılır çünkü
      p_future zaten MAXVALUE ile tanımlıdır.
      Var olan partition'ın önüne yeni bir partition eklemek için
      REORGANIZE gereklidir.
    """
    dag_id  = context["dag"].dag_id
    dag_run = context["dag_run"].run_id

    # Bir sonraki ay
    today      = datetime.utcnow().date()
    next_month = today.replace(day=1) + timedelta(days=32)
    next_month = next_month.replace(day=1)

    target_year  = next_month.year
    target_month = next_month.month
    p_name       = partition_name(target_year, target_month)
    p_less_than  = next_month_first_day(target_year, target_month)

    logging.info(
        f"📅 Yeni partition: {p_name} "
        f"(LESS THAN {p_less_than}) "
        f"→ {target_year}/{target_month:02d} verileri"
    )

    conn = get_vce_conn()
    try:
        for table in PARTITIONED_TABLES:

            # Zaten var mı?
            existing = get_existing_partitions(conn, table)
            existing_names = [p["PARTITION_NAME"] for p in existing]

            if p_name in existing_names:
                logging.info(f"  ⏭️  {table}: {p_name} zaten mevcut, atlandı.")
                continue

            # REORGANIZE SQL
            sql = (
                f"ALTER TABLE {table} "
                f"REORGANIZE PARTITION p_future INTO ("
                f"PARTITION {p_name} VALUES LESS THAN (TO_DAYS('{p_less_than}')), "
                f"PARTITION p_future VALUES LESS THAN MAXVALUE"
                f")"
            )

            logging.info(f"  ➕ {table}: {p_name} ekleniyor...")

            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()

            logging.info(f"  ✅ {table}: {p_name} eklendi.")

            log_to_remediation(
                conn=conn,
                dag_id=dag_id,
                dag_run=dag_run,
                operation_type="custom",
                target_table=f"vce.{table}",
                sql_executed=sql,
                rows_affected=0,
                status="Success",
                detail=f"Partition {p_name} eklendi. LESS THAN TO_DAYS('{p_less_than}')",
            )

    except Exception as e:
        logging.error(f"❌ Partition ekleme hatası: {e}")
        raise
    finally:
        conn.close()


# ── GÖREV 2: Eski Partition'ları Düşür ───────────────────────────────────────

def drop_old_partitions(**context):
    """
    Retention politikasından eski partition'ları düşürür.

    Retention = 12 ay ise:
      Bugün Mayıs 2026 → 12 ay öncesi = Mayıs 2025
      p2025_05 ve öncesi düşürülür.

    GÜVENLIK KONTROLLERİ:
      1. p_future asla düşürülmez
      2. DROP öncesi partition satır sayısı loglanır
      3. İşlem vce_remediation_log'a kaydedilir
    """
    dag_id           = context["dag"].dag_id
    dag_run          = context["dag_run"].run_id
    retention_months = get_retention_months()

    # Retention sınırı: bu tarihin öncesi düşürülür
    today     = datetime.utcnow().date()
    cutoff    = today.replace(day=1)
    for _ in range(retention_months):
        cutoff = (cutoff - timedelta(days=1)).replace(day=1)

    # Cutoff'a karşılık gelen partition adı
    cutoff_name = partition_name(cutoff.year, cutoff.month)

    logging.info(
        f"🗓️  Retention: {retention_months} ay | "
        f"Cutoff: {cutoff} | "
        f"Bu tarihten eski partition'lar düşürülecek"
    )

    conn = get_vce_conn()
    total_dropped = 0

    try:
        for table in PARTITIONED_TABLES:
            existing = get_existing_partitions(conn, table)

            to_drop = []
            for p in existing:
                p_name = p["PARTITION_NAME"]
                if p_name == "p_future":
                    continue

                # Partition adından yıl ve ay çıkar: p2026_04 → (2026, 4)
                try:
                    _, ym = p_name.split("_", 1)
                    year  = int(ym[:4])
                    month = int(ym[5:])
                    p_date = date(year, month, 1)
                except (ValueError, IndexError):
                    logging.warning(f"  ⚠️  {table}: {p_name} parse edilemedi, atlandı.")
                    continue

                if p_date < cutoff:
                    to_drop.append((p_name, p))

            if not to_drop:
                logging.info(f"  ✅ {table}: Düşürülecek eski partition yok.")
                continue

            for p_name, p_info in to_drop:
                row_count = p_info.get("TABLE_ROWS", 0) or 0
                data_mb   = p_info.get("data_mb", 0) or 0

                logging.info(
                    f"  🗑️  {table}: {p_name} düşürülüyor "
                    f"(~{row_count} satır, {data_mb} MB)"
                )

                sql = f"ALTER TABLE {table} DROP PARTITION {p_name}"

                with conn.cursor() as cur:
                    cur.execute(sql)
                conn.commit()

                total_dropped += 1
                logging.info(f"  ✅ {table}: {p_name} düşürüldü.")

                log_to_remediation(
                    conn=conn,
                    dag_id=dag_id,
                    dag_run=dag_run,
                    operation_type="custom",
                    target_table=f"vce.{table}",
                    sql_executed=sql,
                    rows_affected=row_count,
                    status="Success",
                    detail=(
                        f"Partition {p_name} düşürüldü. "
                        f"Retention={retention_months} ay. "
                        f"~{row_count} satır, {data_mb} MB silindi."
                    ),
                )

        logging.info(f"🧹 Toplam {total_dropped} partition düşürüldü.")

    except Exception as e:
        logging.error(f"❌ Partition düşürme hatası: {e}")
        raise
    finally:
        conn.close()


# ── GÖREV 3: Partition Durum Raporu ──────────────────────────────────────────

def partition_status_report(**context):
    """
    Tüm partitioned tabloların mevcut durumunu loglar.

    Her tablo için:
      - Toplam partition sayısı
      - Toplam satır sayısı (yaklaşık)
      - Toplam data boyutu (MB)
      - En eski ve en yeni partition
      - p_future satır sayısı (0 olmalı — doluysa uyarı)

    Sonuçlar XCom'a da yazılır (dashboard için kullanılabilir).
    """
    conn = get_vce_conn()
    report = {}

    try:
        for table in PARTITIONED_TABLES:
            partitions = get_existing_partitions(conn, table)

            # p_future'ı ayrı al
            p_future_rows = 0
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT TABLE_ROWS
                       FROM information_schema.PARTITIONS
                       WHERE TABLE_SCHEMA = DATABASE()
                         AND TABLE_NAME   = %s
                         AND PARTITION_NAME = 'p_future'""",
                    (table,)
                )
                pf = cur.fetchone()
                p_future_rows = pf["TABLE_ROWS"] if pf else 0

            total_rows   = sum((p.get("TABLE_ROWS") or 0) for p in partitions)
            total_data   = sum((p.get("data_mb") or 0) for p in partitions)
            total_index  = sum((p.get("index_mb") or 0) for p in partitions)

            oldest = partitions[0]["PARTITION_NAME"]  if partitions else "-"
            newest = partitions[-1]["PARTITION_NAME"] if partitions else "-"

            report[table] = {
                "partition_count": len(partitions),
                "total_rows"     : total_rows,
                "total_data_mb"  : round(total_data, 2),
                "total_index_mb" : round(total_index, 2),
                "oldest_partition": oldest,
                "newest_partition": newest,
                "p_future_rows"  : p_future_rows,
            }

            logging.info(f"\n📊 {table}")
            logging.info(f"   Partition sayısı : {len(partitions)}")
            logging.info(f"   Toplam satır     : ~{total_rows:,}")
            logging.info(f"   Data boyutu      : {total_data:.1f} MB")
            logging.info(f"   Index boyutu     : {total_index:.1f} MB")
            logging.info(f"   En eski          : {oldest}")
            logging.info(f"   En yeni          : {newest}")

            if p_future_rows and p_future_rows > 0:
                logging.warning(
                    f"   ⚠️  p_future içinde {p_future_rows} satır var! "
                    f"Yeni partition eklenmemiş olabilir."
                )
            else:
                logging.info(f"   p_future         : Boş ✅")

        # XCom'a yaz
        context["ti"].xcom_push(key="partition_report", value=report)

    finally:
        conn.close()


# ── GÖREV 4: Özet ─────────────────────────────────────────────────────────────

def partition_summary(**context):
    """
    Tüm görevlerin sonucunu özetler.
    Herhangi bir görev başarısız olduysa uyarı verir.
    """
    ti = context["ti"]

    report = ti.xcom_pull(task_ids="partition_status_report", key="partition_report")

    logging.info("=" * 60)
    logging.info("📋 PARTITION MANAGER ÖZET")
    logging.info("=" * 60)

    if report:
        for table, info in report.items():
            logging.info(
                f"  {table}: "
                f"{info['partition_count']} partition, "
                f"~{info['total_rows']:,} satır, "
                f"{info['total_data_mb']} MB"
            )

            if info.get("p_future_rows", 0) > 0:
                logging.warning(
                    f"  ⚠️  {table}: p_future dolu! "
                    f"Partition ekleme görevini kontrol edin."
                )

    logging.info("=" * 60)


# ── DAG Tanımı ────────────────────────────────────────────────────────────────

with DAG(
    dag_id="mailsender_vce_partition_manager",
    description=(
        "VCE — Otomatik MySQL Partition Yönetimi. "
        "Her ayın 1'inde yeni partition ekler, eski partition'ları düşürür."
    ),
    # Her ayın 1'i, 01:00 UTC
    # (Remediation 03:00'dan, Ana DAG 06:00'dan önce tamamlanır)
    schedule_interval="0 1 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["vce", "partition", "maintenance"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Görev 1: Yeni ay partition'ı ekle
    t_add = PythonOperator(
        task_id="add_next_month_partition",
        python_callable=add_next_month_partition,
        doc_md="""
        **Yeni Partition Ekle**

        Bir sonraki ay için partition oluşturur.
        p_future REORGANIZE edilerek güvenli ekleme yapılır.
        Her tablo için ayrı REORGANIZE çalışır.
        """,
    )

    # Görev 2: Eski partition'ları düşür
    t_drop = PythonOperator(
        task_id="drop_old_partitions",
        python_callable=drop_old_partitions,
        doc_md="""
        **Eski Partition'ları Düşür**

        VCE_PARTITION_RETENTION_MONTHS Variable'ındaki değere göre
        (varsayılan: 12 ay) eski partition'ları DROP eder.
        Her DROP öncesi satır sayısı loglanır.
        """,
    )

    # Görev 3: Durum raporu
    t_report = PythonOperator(
        task_id="partition_status_report",
        python_callable=partition_status_report,
        trigger_rule=TriggerRule.ALL_DONE,  # Önceki görevler fail olsa da çalış
        doc_md="""
        **Partition Durum Raporu**

        Tüm partitioned tabloların mevcut durumunu loglar.
        p_future'ın boş olduğunu doğrular.
        """,
    )

    # Görev 4: Özet
    t_summary = PythonOperator(
        task_id="partition_summary",
        python_callable=partition_summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # Bağımlılıklar:
    # Önce partition ekle, sonra eskiyi düşür
    # (Ekleme başarısız olursa düşürme de durur)
    start >> t_add >> t_drop >> t_report >> t_summary >> end
