"""
mailsender_vce_remediation.py
==============================
MailSender Pro — VCE Otomatik Temizlik DAG'ı
Her gün 03:00'da çalışır. Ana denetim DAG'ından (06:00) önce tamamlanır.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from operators.vce_operators import RemediationOperator

default_args = {
    "owner"          : "data-quality",
    "retries"        : 0,
    "email_on_failure": False,
}

with DAG(
    dag_id="mailsender_vce_remediation",
    description="MailSender Pro — Otomatik DB Temizliği (vce_remediation_log'a kaydedilir)",
    schedule_interval="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["vce", "remediation", "mailsender"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Tüm tanımlı temizlik işlemlerini çalıştır
    # Her işlem vce_remediation_log tablosuna kaydedilir
    remediation = RemediationOperator(
        task_id="run_all_remediations",
        operations=["all"],  # Tüm OPERATIONS dict anahtarları
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    start >> remediation >> end
