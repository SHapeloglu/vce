"""
mailsender_vce_main.py
======================
MailSender Pro — VCE Tabanlı Ana Veri Kalitesi DAG'ı
=====================================================

Bu DAG, vce_operators.py'deki custom operatörleri kullanarak
MailSender Pro veritabanını günlük olarak denetler.

Tüm kurallar vce_dq_rules tablosundaki SQL'lerden yüklenir.
Kod değiştirmeden yeni kural eklemek için tabloya INSERT yeterlidir.

ÇALIŞMA ZAMANI : Her gün 06:00 UTC
PARALEL GÖREVLER: Tüm domain kontrolleri aynı anda çalışır
BAĞIMLILIK       : Önce şema kontrolü, ardından paralel domainler, son olarak özet

EKLENECEK AIRFLOW VARIABLE'LAR (Admin → Variables):
  VCE_TEAMS_WEBHOOK_URL : Teams bildirim webhook URL'si
  VCE_SLACK_WEBHOOK_URL : Slack bildirim webhook URL'si (opsiyonel)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# VCE custom operatörlerini içe aktar
# Bu dosya airflow/dags/ klasöründeyse:
#   operators/vce_operators.py → dags/operators/vce_operators.py
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from operators.vce_operators import DataQualityOperator, TableValidationOperator

VCE_CONN_ID = "vce"
MAILSENDER_CONN_ID = "mailsender"

default_args = {
    "owner"          : "data-quality",
    "retries"        : 1,
    "retry_delay"    : timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry" : False,
}


def generate_summary(**context):
    """
    Tüm task'ların XCom sonuçlarını birleştirir ve özet rapor oluşturur.

    - PASS/WARN/FAIL sayılarını hesaplar
    - Başarısız kontrolleri listeler
    - Kritik hata varsa AirflowException fırlatır (DAG FAIL)
    - Sonucu XCom'a yazar (dashboard DAG'ı buradan okur)
    """
    ti = context["ti"]

    domain_tasks = [
        "dq_schema", "dq_security", "dq_send_log",
        "dq_suppression", "dq_queue", "dq_verify",
        "dq_senders", "dq_integrity", "dq_freshness_volume",
        "tv_send_consistency",
    ]

    summary = {
        "run_date"      : datetime.utcnow().isoformat(),
        "dag_run_id"    : context["dag_run"].run_id,
        "total_tasks"   : len(domain_tasks),
        "passed_tasks"  : 0,
        "failed_tasks"  : 0,
        "task_states"   : {},
    }

    for task_id in domain_tasks:
        state = ti.xcom_pull(task_ids=task_id, key="return_value")
        # Airflow task state'i doğrudan XCom'dan gelmiyor;
        # task instance'tan çek
        task_inst = context["dag_run"].get_task_instance(task_id)
        if task_inst:
            ts = task_inst.state
            summary["task_states"][task_id] = ts
            if ts == "success":
                summary["passed_tasks"] += 1
            elif ts in ("failed", "upstream_failed"):
                summary["failed_tasks"] += 1

    summary["overall_status"] = (
        "FAIL" if summary["failed_tasks"] > 0 else "PASS"
    )

    logging.info("=" * 70)
    logging.info("📋 VCE VERİ KALİTESİ ÖZET RAPORU")
    logging.info("=" * 70)
    logging.info(f"  Tarih  : {summary['run_date']}")
    logging.info(f"  Durum  : {summary['overall_status']}")
    logging.info(f"  ✅ PASS: {summary['passed_tasks']}")
    logging.info(f"  ❌ FAIL: {summary['failed_tasks']}")
    logging.info("=" * 70)

    ti.xcom_push(key="vce_summary", value=summary)

    if summary["failed_tasks"] > 0:
        failed = [k for k, v in summary["task_states"].items() if v in ("failed","upstream_failed")]
        raise Exception(f"❌ {summary['failed_tasks']} domain kontrolü başarısız: {failed}")


with DAG(
    dag_id="mailsender_vce_main",
    description="MailSender Pro — VCE Tabanlı MySQL Veri Kalitesi (kurallar DB'de)",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["vce", "data-quality", "mailsender", "mysql"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ── Grup 1: Şema & Bağlantı ──────────────────────────────────────────────
    # Diğer tüm kontrollerin önkoşulu — bu geçmeden paralel başlamaz
    dq_schema = DataQualityOperator(
        task_id="dq_schema",
        rule_domain="schema",
        execute_time="06:00",
    )

    # ── Paralel Gruplar ───────────────────────────────────────────────────────

    dq_security = DataQualityOperator(
        task_id="dq_security",
        rule_domain="security",
        execute_time="06:00",
    )

    dq_send_log = DataQualityOperator(
        task_id="dq_send_log",
        rule_domain="send_log",
        execute_time="06:00",
    )

    dq_suppression = DataQualityOperator(
        task_id="dq_suppression",
        rule_domain="suppression",
        execute_time="06:00",
    )

    dq_queue = DataQualityOperator(
        task_id="dq_queue",
        rule_domain="queue",
        execute_time="06:00",
    )

    dq_verify = DataQualityOperator(
        task_id="dq_verify",
        rule_domain="verify",
        execute_time="06:00",
    )

    dq_senders = DataQualityOperator(
        task_id="dq_senders",
        rule_domain="senders",
        execute_time="06:00",
    )

    dq_integrity = DataQualityOperator(
        task_id="dq_integrity",
        rule_domain="integrity",
        execute_time="06:00",
    )

    dq_freshness_volume = DataQualityOperator(
        task_id="dq_freshness_volume",
        rule_domain="freshness",
        execute_time="06:00",
    )

    # ── Tablo Karşılaştırması ─────────────────────────────────────────────────
    tv_send_consistency = TableValidationOperator(
        task_id="tv_send_consistency",
        validation_domain="send_consistency",
    )

    # ── Özet ─────────────────────────────────────────────────────────────────
    t_summary = PythonOperator(
        task_id="generate_summary",
        python_callable=generate_summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # ── Bağımlılıklar ─────────────────────────────────────────────────────────
    parallel_tasks = [
        dq_security, dq_send_log, dq_suppression, dq_queue,
        dq_verify, dq_senders, dq_integrity, dq_freshness_volume,
        tv_send_consistency,
    ]

    start >> dq_schema >> parallel_tasks >> t_summary >> end
