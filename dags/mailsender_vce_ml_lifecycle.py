"""
mailsender_vce_ml_lifecycle.py
================================
VCE — ML Lifecycle + Data Product Mindset DAG'ı
=================================================

Bu DAG üç kavramı günlük ve haftalık olarak otomatik yönetir:

  ML MODEL LİFECYCLE:
    Concept drift tespiti → Baseline sıfırlama → Model performans raporu

  DATA PRODUCT MİNDSET:
    Kalite skoru hesaplama → Freshness SLA kontrolü → Haftalık sağlık raporu

ÇALIŞMA TAKVİMİ:
  Her gün 07:00 UTC:
    1. concept_drift_check     → Anomaly baseline'larında drift var mı?
    2. model_performance       → Dünün model performansını özetle
    3. quality_scores          → Her data product için kalite skoru hesapla
    4. sla_monitor             → Freshness SLA'larını denetle
    5. daily_summary           → Günlük özet XCom'a yaz

  Her Pazartesi 07:00 UTC:
    6. weekly_report           → Haftalık data product sağlık raporu

BAĞLANTI:
  vce connection (Conn Id: 'vce') — tüm yazma işlemleri
  mailsender connection (Conn Id: 'mailsender') — kaynak veri okuma
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from operators.vce_operators_ml_lifecycle import (
    ConceptDriftOperator,
    ModelPerformanceOperator,
    QualityScoreOperator,
    SLAMonitorOperator,
    DataProductReportOperator,
)

default_args = {
    "owner"           : "data-quality",
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=5),
    "email_on_failure": False,
}


def daily_summary(**context):
    """
    Günlük tüm sonuçları birleştirir ve XCom'a yazar.
    Airflow UI'da generate_summary task'ının XCom sekmesinden görüntülenebilir.
    """
    ti = context["ti"]
    import logging

    drift_count      = ti.xcom_pull(task_ids="concept_drift_check",
                                    key="drift_count") or 0
    below_threshold  = ti.xcom_pull(task_ids="quality_scores",
                                    key="below_threshold_count") or 0
    freshness_viols  = ti.xcom_pull(task_ids="sla_monitor",
                                    key="freshness_violations") or 0

    summary = {
        "date"                    : datetime.utcnow().date().isoformat(),
        "concept_drifts_detected" : drift_count,
        "products_below_threshold": below_threshold,
        "freshness_sla_violations": freshness_viols,
        "overall_status"          : (
            "CRITICAL" if (drift_count > 3 or below_threshold > 2)
            else "WARN" if (drift_count > 0 or below_threshold > 0 or freshness_viols > 0)
            else "HEALTHY"
        ),
    }

    logging.info("=" * 60)
    logging.info("📋 ML LİFECYCLE + DATA PRODUCT GÜNLÜK ÖZET")
    logging.info("=" * 60)
    logging.info(f"  Tarih              : {summary['date']}")
    logging.info(f"  Genel Durum        : {summary['overall_status']}")
    logging.info(f"  Concept Drift      : {drift_count}")
    logging.info(f"  Eşik Altı Ürün     : {below_threshold}")
    logging.info(f"  Freshness İhlali   : {freshness_viols}")
    logging.info("=" * 60)

    ti.xcom_push(key="daily_summary", value=summary)


with DAG(
    dag_id="mailsender_vce_ml_lifecycle",
    description=(
        "VCE — ML Lifecycle (concept drift, model performance) + "
        "Data Product (quality score, SLA, weekly report)"
    ),
    schedule_interval="0 7 * * *",   # Her gün 07:00 UTC (TR 10:00)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["vce", "ml-lifecycle", "data-product", "monitoring"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ── ML Lifecycle ──────────────────────────────────────────────────────────

    t_drift = ConceptDriftOperator(
        task_id="concept_drift_check",
        drift_threshold=2.0,
        auto_reset=True,
        doc_md="""
        **Concept Drift Kontrolü**

        Anomaly kurallarının baseline'larında istatistiksel kayma var mı denetler.
        Son 7 günlük kısa dönem ortalama, 90 günlük baseline'dan
        2 standart sapma uzaklaşırsa drift tespit edilir ve baseline sıfırlanır.
        Olay `vce_concept_drift_log` tablosuna kaydedilir.
        """,
    )

    t_model_perf = ModelPerformanceOperator(
        task_id="model_performance",
        fp_rate_threshold=0.30,
        doc_md="""
        **Model Performans Özeti**

        Dünün anomali tespitlerini özetler:
        toplam çalışma, tespit edilen anomali, precision skoru (feedback varsa).
        Yüksek false positive oranı varsa uyarı gönderir.
        Sonuçlar `vce_model_performance` tablosuna kaydedilir.
        """,
    )

    # ── Data Product ──────────────────────────────────────────────────────────

    t_quality = QualityScoreOperator(
        task_id="quality_scores",
        doc_md="""
        **Data Product Kalite Skoru**

        Her data product için dünün kalite skorunu hesaplar:
        geçen kural / toplam kural × 100.
        Eşiğin altına düşen ürünler için SLA ihlal kaydı oluşturur.
        Sonuçlar `vce_quality_scores` tablosuna kaydedilir.
        """,
    )

    t_sla = SLAMonitorOperator(
        task_id="sla_monitor",
        doc_md="""
        **Freshness SLA Kontrolü**

        Her data product'ın son güncelleme zamanını kontrol eder.
        `freshness_sla_hours` süresi geçmişse ihlal kaydı oluşturur.
        Sonuçlar `vce_sla_violations` tablosuna kaydedilir.
        """,
    )

    # ── Günlük Özet ───────────────────────────────────────────────────────────

    t_summary = PythonOperator(
        task_id="daily_summary",
        python_callable=daily_summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # ── Bağımlılıklar ─────────────────────────────────────────────────────────
    # Drift kontrolü ve model performansı paralel çalışır (ML lifecycle)
    # Kalite skoru ve SLA kontrolü paralel çalışır (data product)
    # Hepsi bitince günlük özet

    start >> [t_drift, t_model_perf] >> t_summary
    start >> [t_quality, t_sla]      >> t_summary
    t_summary >> end


# ── Haftalık Rapor DAG'ı ─────────────────────────────────────────────────────

with DAG(
    dag_id="mailsender_vce_weekly_report",
    description="VCE — Haftalık Data Product Sağlık Raporu (Her Pazartesi 07:00)",
    schedule_interval="0 7 * * 1",   # Her Pazartesi 07:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["vce", "data-product", "report", "weekly"],
) as weekly_dag:

    weekly_start = EmptyOperator(task_id="start")

    t_weekly_report = DataProductReportOperator(
        task_id="weekly_data_product_report",
        doc_md="""
        **Haftalık Data Product Sağlık Raporu**

        Son 7 günlük özet:
        - Her ürünün ortalama kalite skoru ve trendi
        - SLA ihlal sayıları
        - Concept drift olayları
        - False positive oranı yüksek anomaly kuralları

        Rapor Teams/Slack'e gönderilir ve XCom'a yazılır.
        """,
    )

    weekly_end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    weekly_start >> t_weekly_report >> weekly_end
