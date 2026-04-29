"""
vce_operators_ml_lifecycle.py
==============================
VCE — ML Lifecycle + Data Product Mindset Operatörleri
========================================================

Bu dosya üç kavramı Airflow operatörlerine dönüştürür:

ML MODEL LİFECYCLE:
  1. ConceptDriftOperator     — Baseline'ın geçerliliğini denetler,
                                drift tespit edince sıfırlar
  2. ModelPerformanceOperator — Günlük anomali model performansını özetler
                                (precision, false positive oranı)

DATA PRODUCT MİNDSET:
  3. QualityScoreOperator     — Her data product için günlük kalite skoru
                                hesaplar ve saklar
  4. SLAMonitorOperator       — Data product SLA'larını denetler,
                                ihlal varsa bildirir ve kayıt altına alır
  5. DataProductReportOperator— Haftalık data product sağlık raporu üretir

BAĞLANTI MİMARİSİ (diğer operatörlerle aynı):
  mailsender conn → aws_mailsender_pro_v3 okunur
  vce conn        → tüm VCE tablolarına yazılır

KULLANIM (DAG içinde):
  from operators.vce_operators_ml_lifecycle import (
      ConceptDriftOperator,
      ModelPerformanceOperator,
      QualityScoreOperator,
      SLAMonitorOperator,
  )
"""

from __future__ import annotations

import json
import logging
import statistics
from datetime import datetime, timedelta, date
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults

import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from vce_operators import VCEBaseOperator


# ── Sabitler ─────────────────────────────────────────────────────────────────

# Concept drift: son 7 günlük ortalama, son 90 günlük baseline'dan
# bu kadar standart sapma uzaklaşırsa drift var sayılır
DRIFT_THRESHOLD_STD = 2.0

# Precision hesabı için minimum geri bildirim sayısı
MIN_FEEDBACK_FOR_PRECISION = 10


# ═════════════════════════════════════════════════════════════════════════════
# OPERATÖR 1: ConceptDriftOperator — ML Model Lifecycle
# ═════════════════════════════════════════════════════════════════════════════

class ConceptDriftOperator(VCEBaseOperator):
    """
    Anomali tespiti baseline'larında concept drift olup olmadığını denetler.

    CONCEPT DRIFT NEDİR?
      Zamanla verinin istatistiksel özellikleri değişir.
      Örneğin MailSender büyüdükçe günlük gönderim hacmi artar.
      Eski baseline (mean=500) ile yeni verileri (mean=5000) karşılaştırmak
      her gün "ANOMALİ" uyarısı verir — yanlış alarm.

    NASIL TESPİT EDİLİR?
      Son 7 günlük kısa dönem ortalama ile
      son 90 günlük uzun dönem baseline karşılaştırılır.
      |kısa_mean - uzun_mean| > DRIFT_THRESHOLD_STD × uzun_std
      koşulu sağlanırsa drift var — baseline sıfırlanır.

    SIFIRLAMA SONRASI:
      vce_anomaly_baselines tablosundaki istatistikler temizlenir.
      Olay vce_concept_drift_log'a yazılır.
      Model yeniden 7+ gün veri biriktirinceye kadar anomali tetiklemez.

    BAĞLANTI AKIŞI:
      vce conn → vce_anomaly_baselines'dan mevcut baseline'ı oku
      vce conn → vce_dq_executions'dan son 7 ve 90 günlük değerleri oku
      vce conn → drift varsa vce_anomaly_baselines'ı sıfırla
      vce conn → vce_concept_drift_log'a olay yaz
    """

    @apply_defaults
    def __init__(
        self,
        rule_domain: str | None = None,
        drift_threshold: float = DRIFT_THRESHOLD_STD,
        auto_reset: bool = True,
        *args,
        **kwargs,
    ):
        """
        Args:
            rule_domain    : Sadece bu domain'deki kuralları kontrol et
                             (None = tüm anomaly kuralları)
            drift_threshold: Kaç standart sapma uzaklaşırsa drift?
                             (varsayılan: 2.0 — %95 güven aralığı)
            auto_reset     : True ise drift tespit edilince otomatik sıfırla
                             False ise sadece tespit et ve logla
        """
        super().__init__(*args, **kwargs)
        self.rule_domain    = rule_domain
        self.drift_threshold = drift_threshold
        self.auto_reset     = auto_reset

    def execute(self, context: dict) -> None:
        dag_run = context["dag_run"].run_id

        # Anomaly tipi kuralları yükle — sadece bunlarda drift mantıklı
        rules = self._load_anomaly_rules()
        self.log.info(f"🔍 {len(rules)} anomaly kuralı drift kontrolüne alındı.")

        drift_count = 0
        for rule in rules:
            drifted, drift_info = self._check_drift(rule)
            if drifted:
                drift_count += 1
                self.log.warning(
                    f"⚠️ Concept drift tespit edildi: "
                    f"[{rule['rule_domain']}/{rule['rule_subdomain']}] "
                    f"drift_score={drift_info.get('drift_score', '?'):.2f}"
                )
                if self.auto_reset:
                    self._reset_baseline(rule, drift_info, dag_run)
                self._log_drift(rule, drift_info, dag_run)

        self.log.info(
            f"✅ Drift kontrolü tamamlandı. "
            f"{drift_count}/{len(rules)} kuralda drift tespit edildi."
        )

        if drift_count > 0:
            self.send_notifications(
                "Concept Drift Tespit Edildi",
                [
                    f"{drift_count} anomaly kuralında concept drift tespit edildi. "
                    f"Baseline'lar sıfırlandı — yeniden öğrenme başladı."
                ]
            )

        context["ti"].xcom_push(key="drift_count", value=drift_count)

    def _load_anomaly_rules(self) -> list[dict]:
        """vce_dq_rules'dan anomaly tipi aktif kuralları yükler."""
        conditions = ["r.active_flag = 1", "r.check_type = 'anomaly'"]
        params = []

        if self.rule_domain:
            conditions.append("r.rule_domain = %s")
            params.append(self.rule_domain)

        return self.run_vce_query(
            f"""SELECT r.id, r.rule_domain, r.rule_subdomain,
                       b.mean_value as baseline_mean,
                       b.std_value  as baseline_std,
                       b.sample_count
                FROM vce_dq_rules r
                LEFT JOIN vce_anomaly_baselines b ON b.rule_id = r.id
                WHERE {' AND '.join(conditions)}""",
            tuple(params)
        )

    def _check_drift(self, rule: dict) -> tuple[bool, dict]:
        """
        Tek bir kural için drift kontrolü yapar.

        Yöntem:
          - Son 7 günlük değerlerin ortalamasını hesapla (kısa dönem)
          - Mevcut baseline (90 gün) ile karşılaştır
          - Fark > threshold × std ise drift var
        """
        rule_id = rule['id']

        # Son 7 günlük değerler
        recent = self.run_vce_query(
            """SELECT result_value
               FROM vce_dq_executions
               WHERE rule_id = %s
                 AND result_status = 'Passed'
                 AND run_date >= NOW() - INTERVAL 7 DAY
               ORDER BY run_date DESC""",
            (rule_id,)
        )

        recent_values = [
            float(r['result_value'])
            for r in recent
            if r['result_value'] is not None
        ]

        # Yeterli veri yoksa drift kontrolü yapma
        if len(recent_values) < 3:
            return False, {}

        recent_mean = statistics.mean(recent_values)

        # Mevcut baseline
        baseline_mean = rule.get('baseline_mean')
        baseline_std  = rule.get('baseline_std')

        if baseline_mean is None or baseline_std is None or float(baseline_std) == 0:
            return False, {}

        baseline_mean = float(baseline_mean)
        baseline_std  = float(baseline_std)

        # Drift skoru: kaç standart sapma uzaklaşıldı
        drift_score = abs(recent_mean - baseline_mean) / baseline_std

        is_drift = drift_score > self.drift_threshold

        return is_drift, {
            "drift_score"   : drift_score,
            "recent_mean"   : recent_mean,
            "recent_count"  : len(recent_values),
            "baseline_mean" : baseline_mean,
            "baseline_std"  : baseline_std,
        }

    def _reset_baseline(self, rule: dict, drift_info: dict, dag_run: str) -> None:
        """
        Baseline'ı sıfırlar — istatistikleri temizler.
        Sıfırlamadan sonra sistem yeniden öğrenme moduna girer.
        """
        self.execute_vce_dml(
            """UPDATE vce_anomaly_baselines
               SET mean_value   = NULL,
                   std_value    = NULL,
                   sample_count = 0,
                   last_updated = NOW()
               WHERE rule_id = %s""",
            (rule['id'],)
        )
        self.log.info(
            f"  🔄 [{rule['rule_domain']}/{rule['rule_subdomain']}] "
            f"Baseline sıfırlandı. Yeni öğrenme başlıyor."
        )

    def _log_drift(self, rule: dict, drift_info: dict, dag_run: str) -> None:
        """Drift olayını vce_concept_drift_log tablosuna kaydeder."""
        self.execute_vce_dml(
            """INSERT INTO vce_concept_drift_log (
                   rule_id, rule_domain, rule_subdomain,
                   drift_type, detected_by,
                   old_mean, old_std, old_sample_count, old_window_days,
                   drift_score, drift_reason,
                   reset_performed, new_learning_start
               ) VALUES (
                   %s, %s, %s, 'automatic', 'ConceptDriftOperator',
                   %s, %s, %s, 90,
                   %s, %s,
                   %s, NOW()
               )""",
            (
                rule['id'], rule['rule_domain'], rule['rule_subdomain'],
                drift_info.get('baseline_mean'), drift_info.get('baseline_std'),
                rule.get('sample_count'),
                drift_info.get('drift_score'),
                (f"Kısa dönem ortalama ({drift_info.get('recent_mean', '?'):.2f}) "
                 f"baseline'dan {drift_info.get('drift_score', '?'):.2f} std uzaklaştı "
                 f"(eşik: {self.drift_threshold} std)."),
                1 if self.auto_reset else 0,
            )
        )


# ═════════════════════════════════════════════════════════════════════════════
# OPERATÖR 2: ModelPerformanceOperator — ML Model Lifecycle
# ═════════════════════════════════════════════════════════════════════════════

class ModelPerformanceOperator(VCEBaseOperator):
    """
    Anomali tespit modelinin günlük performansını özetler ve raporlar.

    NEDEN GEREKLİ?
      Sistem "anomali tespit ettim" dediğinde bu:
        a) Gerçek bir anomaliydi (true positive  — iyi!)
        b) Yanlış alarmdı      (false positive  — gürültü)
      Zamanla false positive oranı artarsa sistem güvenilirliğini kaybeder.
      Bu operatör precision ve false alarm oranını günlük olarak hesaplar.

    NASIL ÇALIŞIR?
      1. vce_dq_executions'dan dün tespit edilen anomalileri say
      2. vce_anomaly_feedback'ten geri bildirimleri oku
      3. Precision hesapla (yeterli feedback varsa)
      4. vce_model_performance tablosuna yaz
      5. False positive oranı eşiği aşarsa bildirim gönder

    BAĞLANTI AKIŞI:
      vce conn → vce_dq_executions'dan anomali sayısı oku
      vce conn → vce_anomaly_feedback'ten geri bildirim oku
      vce conn → vce_model_performance'a yaz
    """

    @apply_defaults
    def __init__(
        self,
        target_date: str | None = None,
        fp_rate_threshold: float = 0.30,
        *args,
        **kwargs,
    ):
        """
        Args:
            target_date      : Hangi gün için hesaplansın (YYYY-MM-DD)
                               None = dün
            fp_rate_threshold: False positive oranı bu eşiği aşarsa uyarı
                               (varsayılan: %30)
        """
        super().__init__(*args, **kwargs)
        self.target_date      = target_date
        self.fp_rate_threshold = fp_rate_threshold

    def execute(self, context: dict) -> None:
        if self.target_date:
            target = datetime.strptime(self.target_date, "%Y-%m-%d").date()
        else:
            target = (datetime.utcnow() - timedelta(days=1)).date()

        self.log.info(f"📊 Model performansı hesaplanıyor: {target}")

        # Anomaly tipi kuralları al
        rules = self.run_vce_query(
            """SELECT DISTINCT rule_id, rule_domain, rule_subdomain
               FROM vce_dq_executions
               WHERE check_type = 'anomaly'
                 AND DATE(run_date) = %s""",
            (target,)
        )

        for rule in rules:
            self._calculate_performance(rule, target)

        self.log.info(
            f"✅ {len(rules)} kural için model performansı hesaplandı."
        )

    def _calculate_performance(self, rule: dict, target: date) -> None:
        """Tek bir kural için performans metriklerini hesaplar ve kaydeder."""
        rule_id    = rule['rule_id']
        domain     = rule['rule_domain']
        subdomain  = rule['rule_subdomain']

        # O gün çalışma sayısı ve anomali sayısı
        runs = self.run_vce_query(
            """SELECT
                   COUNT(*) as total_runs,
                   SUM(result_status = 'Failed') as anomaly_detected,
                   AVG(z_score) as avg_z_score,
                   AVG(baseline_mean) as baseline_mean,
                   AVG(baseline_std)  as baseline_std
               FROM vce_dq_executions
               WHERE rule_id = %s AND DATE(run_date) = %s""",
            (rule_id, target)
        )

        if not runs or not runs[0]['total_runs']:
            return

        r = runs[0]
        total_runs      = r['total_runs'] or 0
        anomaly_detected = int(r['anomaly_detected'] or 0)
        anomaly_rate    = anomaly_detected / total_runs if total_runs > 0 else 0.0

        # Geri bildirimler
        feedback = self.run_vce_query(
            """SELECT
                   SUM(was_true_anomaly = 1) as true_positives,
                   SUM(was_true_anomaly = 0) as false_positives,
                   SUM(was_true_anomaly IS NULL) as unreviewed
               FROM vce_anomaly_feedback
               WHERE rule_id = %s
                 AND DATE(detected_at) = %s""",
            (rule_id, target)
        )

        true_positives  = 0
        false_positives = 0
        unreviewed      = anomaly_detected
        precision_score = None

        if feedback and feedback[0]['true_positives'] is not None:
            fb = feedback[0]
            true_positives  = int(fb['true_positives'] or 0)
            false_positives = int(fb['false_positives'] or 0)
            unreviewed      = int(fb['unreviewed'] or 0)

            total_reviewed = true_positives + false_positives
            if total_reviewed >= MIN_FEEDBACK_FOR_PRECISION:
                precision_score = (
                    true_positives / total_reviewed
                    if total_reviewed > 0 else None
                )

        # Concept drift bu gün oldu mu?
        drift_today = self.run_vce_query(
            """SELECT COUNT(*) as cnt FROM vce_concept_drift_log
               WHERE rule_id = %s AND DATE(drift_detected_at) = %s""",
            (rule_id, target)
        )
        drift_occurred = bool(drift_today and drift_today[0]['cnt'] > 0)

        # vce_model_performance'a yaz
        self.execute_vce_dml(
            """INSERT INTO vce_model_performance (
                   performance_date, rule_id, rule_domain, rule_subdomain,
                   total_runs, anomaly_detected, anomaly_rate,
                   true_positives, false_positives, unreviewed, precision_score,
                   baseline_mean, baseline_std, avg_z_score, drift_occurred
               ) VALUES (
                   %s, %s, %s, %s,
                   %s, %s, %s,
                   %s, %s, %s, %s,
                   %s, %s, %s, %s
               )
               ON DUPLICATE KEY UPDATE
                   total_runs       = VALUES(total_runs),
                   anomaly_detected = VALUES(anomaly_detected),
                   anomaly_rate     = VALUES(anomaly_rate),
                   true_positives   = VALUES(true_positives),
                   false_positives  = VALUES(false_positives),
                   unreviewed       = VALUES(unreviewed),
                   precision_score  = VALUES(precision_score),
                   drift_occurred   = VALUES(drift_occurred),
                   calculated_at    = NOW()""",
            (
                target, rule_id, domain, subdomain,
                total_runs, anomaly_detected, anomaly_rate,
                true_positives, false_positives, unreviewed, precision_score,
                r['baseline_mean'], r['baseline_std'], r['avg_z_score'],
                drift_occurred,
            )
        )

        # False positive oranı yüksekse uyar
        if false_positives > 0:
            total_with_fb = true_positives + false_positives
            fp_rate = false_positives / total_with_fb if total_with_fb > 0 else 0
            if fp_rate > self.fp_rate_threshold:
                self.warn_checks.append(
                    f"[{domain}/{subdomain}] Yüksek false positive oranı: "
                    f"%{fp_rate*100:.1f} (eşik: %{self.fp_rate_threshold*100:.0f}). "
                    f"Threshold veya baseline güncellenmeli olabilir."
                )

        self.log.info(
            f"  [{domain}/{subdomain}] "
            f"runs={total_runs}, anomali={anomaly_detected}, "
            f"precision={f'{precision_score:.2f}' if precision_score else 'N/A'}, "
            f"drift={'EVET' if drift_occurred else 'hayır'}"
        )


# ═════════════════════════════════════════════════════════════════════════════
# OPERATÖR 3: QualityScoreOperator — Data Product Mindset
# ═════════════════════════════════════════════════════════════════════════════

class QualityScoreOperator(VCEBaseOperator):
    """
    Her data product için günlük kalite skoru hesaplar.

    KALITE SKORU FORMÜLÜ:
      Basit  : passed / total × 100
      Ağırlıklı: fail kuralları tam puan keser, warn kuralları yarım keser
        weighted = (passed × 1.0 + warned × 0.5) / total × 100

    TREND HESABI:
      Bir önceki günün skoru ile karşılaştırılır.
      +5'ten büyük iyileşme  → 'improving'
      -5'ten küçük düşüş     → 'degrading'
      Arada                  → 'stable'

    EŞIK KONTROLÜ:
      vce_data_products.quality_threshold değerinin altına düşülürse
      vce_sla_violations tablosuna kayıt eklenir ve bildirim gönderilir.

    BAĞLANTI AKIŞI:
      vce conn → vce_data_products'tan ürün listesini oku
      vce conn → vce_dq_executions'dan o güne ait kural sonuçlarını oku
      vce conn → vce_quality_scores tablosuna yaz
      vce conn → eşik ihlali varsa vce_sla_violations'a yaz
    """

    @apply_defaults
    def __init__(
        self,
        target_date: str | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.target_date = target_date

    def execute(self, context: dict) -> None:
        if self.target_date:
            target = datetime.strptime(self.target_date, "%Y-%m-%d").date()
        else:
            target = (datetime.utcnow() - timedelta(days=1)).date()

        dag = {
            "dag_id"   : context["dag"].dag_id,
            "dag_run"  : context["dag_run"].run_id,
            "task_name": context["task"].task_id,
        }

        self.log.info(f"📊 Kalite skorları hesaplanıyor: {target}")

        products = self.run_vce_query(
            "SELECT id, product_code, schema_name, table_name, "
            "quality_threshold, quality_action FROM vce_data_products "
            "WHERE active_flag = 1"
        )

        below_threshold = []

        for product in products:
            score_info = self._calculate_score(product, target)
            if score_info:
                self._save_score(product, score_info, target)
                if score_info['is_below_threshold']:
                    below_threshold.append(
                        f"{product['product_code']}: "
                        f"%{score_info['quality_score']:.1f} "
                        f"(eşik: %{product['quality_threshold']:.0f})"
                    )
                    self._record_sla_violation(product, score_info, target, dag)

        self.log.info(
            f"✅ {len(products)} data product için kalite skoru hesaplandı. "
            f"{len(below_threshold)} ürün eşiğin altında."
        )

        if below_threshold:
            self.send_notifications(
                "Data Product Kalite Eşiği İhlali",
                below_threshold
            )

        context["ti"].xcom_push(key="below_threshold_count", value=len(below_threshold))

    def _calculate_score(self, product: dict, target: date) -> dict | None:
        """Bir data product için o günün kalite skorunu hesaplar."""
        table = product['table_name']

        # O gün bu tabloyu kapsayan kural sonuçları
        results = self.run_vce_query(
            """SELECT result_status, action, COUNT(*) as cnt
               FROM vce_dq_executions
               WHERE table_name = %s
                 AND DATE(run_date) = %s
               GROUP BY result_status, action""",
            (table, target)
        )

        if not results:
            return None

        counts = {'Passed': 0, 'Failed_fail': 0, 'Failed_warn': 0, 'Error': 0}
        for r in results:
            status = r['result_status']
            action = r['action'] or 'fail'
            if status == 'Passed':
                counts['Passed'] += r['cnt']
            elif status == 'Failed':
                key = f"Failed_{action}"
                counts[key] = counts.get(key, 0) + r['cnt']
            elif status == 'Error':
                counts['Error'] += r['cnt']

        total = sum(counts.values())
        if total == 0:
            return None

        passed  = counts['Passed']
        failed_fail = counts.get('Failed_fail', 0)
        failed_warn = counts.get('Failed_warn', 0)
        errors  = counts['Error']

        quality_score  = passed / total * 100
        weighted_score = (passed + failed_warn * 0.5) / total * 100

        # Önceki günün skoru
        prev = self.run_vce_query(
            """SELECT quality_score FROM vce_quality_scores
               WHERE product_id = %s AND score_date = %s""",
            (product['id'], target - timedelta(days=1))
        )
        previous_score = float(prev[0]['quality_score']) if prev else None
        score_delta    = (quality_score - previous_score) if previous_score else None

        if score_delta is None:
            trend = 'stable'
        elif score_delta >= 5:
            trend = 'improving'
        elif score_delta <= -5:
            trend = 'degrading'
        else:
            trend = 'stable'

        threshold         = float(product['quality_threshold'])
        is_below_threshold = quality_score < threshold

        return {
            'total_rules'       : total,
            'passed_rules'      : passed,
            'failed_rules'      : failed_fail,
            'warned_rules'      : failed_warn,
            'error_rules'       : errors,
            'quality_score'     : quality_score,
            'weighted_score'    : weighted_score,
            'threshold'         : threshold,
            'is_below_threshold': is_below_threshold,
            'previous_score'    : previous_score,
            'score_delta'       : score_delta,
            'trend'             : trend,
        }

    def _save_score(self, product: dict, info: dict, target: date) -> None:
        """Kalite skorunu vce_quality_scores tablosuna kaydeder."""
        self.execute_vce_dml(
            """INSERT INTO vce_quality_scores (
                   score_date, product_id, product_code,
                   total_rules, passed_rules, failed_rules, warned_rules, error_rules,
                   quality_score, weighted_score,
                   threshold, is_below_threshold,
                   previous_score, score_delta, trend
               ) VALUES (
                   %s, %s, %s,
                   %s, %s, %s, %s, %s,
                   %s, %s,
                   %s, %s,
                   %s, %s, %s
               )
               ON DUPLICATE KEY UPDATE
                   quality_score  = VALUES(quality_score),
                   weighted_score = VALUES(weighted_score),
                   trend          = VALUES(trend),
                   calculated_at  = NOW()""",
            (
                target, product['id'], product['product_code'],
                info['total_rules'], info['passed_rules'],
                info['failed_rules'], info['warned_rules'], info['error_rules'],
                info['quality_score'], info['weighted_score'],
                info['threshold'], info['is_below_threshold'],
                info['previous_score'], info['score_delta'], info['trend'],
            )
        )

        trend_icon = {'improving': '📈', 'stable': '➡️', 'degrading': '📉'}
        self.log.info(
            f"  {trend_icon.get(info['trend'], '?')} "
            f"{product['product_code']}: %{info['quality_score']:.1f} "
            f"(ağırlıklı: %{info['weighted_score']:.1f}) "
            f"{'⚠️ EŞIK ALTINDA' if info['is_below_threshold'] else '✅'}"
        )

    def _record_sla_violation(
        self, product: dict, info: dict, target: date, dag: dict
    ) -> None:
        """Kalite eşiği ihlalini vce_sla_violations tablosuna kaydeder."""
        severity = 'low'
        gap = info['threshold'] - info['quality_score']
        if gap > 20:
            severity = 'critical'
        elif gap > 10:
            severity = 'high'
        elif gap > 5:
            severity = 'medium'

        self.execute_vce_dml(
            """INSERT INTO vce_sla_violations (
                   product_id, product_code, violation_type,
                   expected_score, actual_score, score_gap, severity
               ) VALUES (%s, %s, 'quality', %s, %s, %s, %s)""",
            (
                product['id'], product['product_code'],
                info['threshold'], info['quality_score'], gap, severity,
            )
        )


# ═════════════════════════════════════════════════════════════════════════════
# OPERATÖR 4: SLAMonitorOperator — Data Product Mindset
# ═════════════════════════════════════════════════════════════════════════════

class SLAMonitorOperator(VCEBaseOperator):
    """
    Data product'ların freshness SLA'larını denetler.

    FRESHNESS SLA NEDİR?
      vce_data_products.freshness_sla_hours: bu tablonun kaç saatte bir
      güncellenmesi gerektiğini belirtir.

      Örn: send_log için freshness_sla_hours = 24
      → Son 24 saatte en az bir yeni kayıt gelmeli.
      Gelmemişse: tablo bayat → SLA ihlali.

    NASIL ÇALIŞIR?
      Her data product için mailsender'daki tablonun son güncelleme zamanına bakılır.
      SLA süresi geçmişse vce_sla_violations'a kayıt eklenir.

    BAĞLANTI AKIŞI:
      vce conn        → vce_data_products'tan ürün listesini oku
      mailsender conn → her tablonun son güncelleme zamanını sorgula
      vce conn        → ihlal varsa vce_sla_violations'a yaz
    """

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context: dict) -> None:
        products = self.run_vce_query(
            """SELECT id, product_code, schema_name, table_name,
                      freshness_sla_hours, sla_description
               FROM vce_data_products
               WHERE active_flag = 1
                 AND freshness_sla_hours IS NOT NULL"""
        )

        self.log.info(f"⏱️ {len(products)} data product freshness SLA kontrolü.")

        violations = []
        for product in products:
            violated, info = self._check_freshness(product)
            if violated:
                violations.append(
                    f"{product['product_code']}: "
                    f"{info.get('delay_hours', '?'):.1f} saat gecikmeli"
                )
                self._record_freshness_violation(product, info)
                self.log.warning(
                    f"  ⚠️ {product['product_code']}: "
                    f"SLA ihlali! {info.get('delay_hours', '?'):.1f} saat gecikme"
                )
            else:
                self.log.info(
                    f"  ✅ {product['product_code']}: "
                    f"Taze (son güncelleme: {info.get('last_update', '?')})"
                )

        if violations:
            self.send_notifications("Data Product Freshness SLA İhlali", violations)

        context["ti"].xcom_push(key="freshness_violations", value=len(violations))

    def _check_freshness(self, product: dict) -> tuple[bool, dict]:
        """Bir data product'ın freshness durumunu kontrol eder."""
        table = f"{product['schema_name']}.{product['table_name']}"
        sla_hours = product['freshness_sla_hours']

        # Tablonun son güncelleme zamanını bul
        # Her tablo için updated_at veya created_at sütunlarından birini dene
        for time_col in ['sent_at', 'updated_at', 'created_at',
                         'received_at', 'executed_at', 'finished_at']:
            try:
                rows = self.run_mailsender_query(
                    f"SELECT MAX(`{time_col}`) as last_update FROM {table}"
                )
                if rows and rows[0]['last_update']:
                    last_update = rows[0]['last_update']
                    if isinstance(last_update, str):
                        last_update = datetime.strptime(
                            last_update, '%Y-%m-%d %H:%M:%S'
                        )

                    delay = datetime.utcnow() - last_update
                    delay_hours = delay.total_seconds() / 3600

                    if delay_hours > sla_hours:
                        return True, {
                            'last_update'  : str(last_update),
                            'delay_hours'  : delay_hours,
                            'sla_hours'    : sla_hours,
                            'time_column'  : time_col,
                        }
                    return False, {
                        'last_update': str(last_update),
                        'delay_hours': delay_hours,
                    }
            except Exception:
                continue

        return False, {'last_update': 'belirlenemedi'}

    def _record_freshness_violation(self, product: dict, info: dict) -> None:
        """Freshness SLA ihlalini vce_sla_violations tablosuna kaydeder."""
        delay_hours = info.get('delay_hours', 0)
        severity    = 'low'
        if delay_hours > product['freshness_sla_hours'] * 4:
            severity = 'critical'
        elif delay_hours > product['freshness_sla_hours'] * 2:
            severity = 'high'
        elif delay_hours > product['freshness_sla_hours'] * 1.5:
            severity = 'medium'

        self.execute_vce_dml(
            """INSERT INTO vce_sla_violations (
                   product_id, product_code, violation_type,
                   expected_update_by, actual_last_update, delay_hours, severity
               ) VALUES (%s, %s, 'freshness', %s, %s, %s, %s)""",
            (
                product['id'], product['product_code'],
                datetime.utcnow() - timedelta(hours=product['freshness_sla_hours']),
                info.get('last_update'),
                delay_hours,
                severity,
            )
        )


# ═════════════════════════════════════════════════════════════════════════════
# OPERATÖR 5: DataProductReportOperator — Haftalık Sağlık Raporu
# ═════════════════════════════════════════════════════════════════════════════

class DataProductReportOperator(VCEBaseOperator):
    """
    Haftalık data product sağlık raporunu üretir ve XCom'a yazar.
    Teams/Slack'e özet bildirim gönderir.

    RAPOR İÇERİĞİ:
      - Her data product'ın haftalık ortalama kalite skoru
      - En çok düşüş yaşayan ürünler
      - Haftalık SLA ihlal sayıları
      - Concept drift olayları
      - False positive oranı yüksek anomaly kuralları

    KULLANIM: Her Pazartesi 07:00'da çalışır (haftalık özet)
    """

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context: dict) -> None:
        self.log.info("📋 Haftalık Data Product Sağlık Raporu üretiliyor...")

        report = {
            "report_date"        : datetime.utcnow().isoformat(),
            "period"             : "son 7 gün",
            "quality_summary"    : self._quality_summary(),
            "sla_violations"     : self._sla_violation_summary(),
            "concept_drifts"     : self._drift_summary(),
            "high_fp_rules"      : self._high_fp_rules(),
        }

        # XCom'a yaz (dashboard ve diğer sistemler okuyabilir)
        context["ti"].xcom_push(key="weekly_report", value=report)

        # Teams bildirimi
        self._send_weekly_report(report)

        self.log.info("✅ Haftalık rapor tamamlandı.")
        return report

    def _quality_summary(self) -> list[dict]:
        """Son 7 günlük ortalama kalite skorları."""
        return self.run_vce_query(
            """SELECT
                   p.product_code,
                   p.product_name,
                   ROUND(AVG(q.quality_score), 2) as avg_score,
                   ROUND(MIN(q.quality_score), 2) as min_score,
                   MAX(q.trend) as latest_trend,
                   SUM(q.is_below_threshold) as days_below_threshold
               FROM vce_quality_scores q
               JOIN vce_data_products p ON p.id = q.product_id
               WHERE q.score_date >= CURDATE() - INTERVAL 7 DAY
               GROUP BY p.id, p.product_code, p.product_name
               ORDER BY avg_score ASC"""
        )

    def _sla_violation_summary(self) -> list[dict]:
        """Son 7 günlük SLA ihlal özeti."""
        return self.run_vce_query(
            """SELECT product_code, violation_type,
                      COUNT(*) as violation_count,
                      MAX(severity) as max_severity,
                      SUM(CASE WHEN resolved_at IS NOT NULL THEN 1 ELSE 0 END) as resolved
               FROM vce_sla_violations
               WHERE violation_at >= NOW() - INTERVAL 7 DAY
               GROUP BY product_code, violation_type
               ORDER BY violation_count DESC"""
        )

    def _drift_summary(self) -> list[dict]:
        """Son 7 günlük concept drift olayları."""
        return self.run_vce_query(
            """SELECT rule_domain, rule_subdomain,
                      COUNT(*) as drift_count,
                      MAX(drift_score) as max_drift_score
               FROM vce_concept_drift_log
               WHERE drift_detected_at >= NOW() - INTERVAL 7 DAY
               GROUP BY rule_domain, rule_subdomain
               ORDER BY drift_count DESC"""
        )

    def _high_fp_rules(self) -> list[dict]:
        """False positive oranı yüksek kurallar."""
        return self.run_vce_query(
            """SELECT rule_domain, rule_subdomain,
                      SUM(false_positives) as total_fp,
                      SUM(true_positives)  as total_tp,
                      ROUND(
                          SUM(false_positives) /
                          NULLIF(SUM(true_positives) + SUM(false_positives), 0) * 100,
                      2) as fp_rate_pct
               FROM vce_model_performance
               WHERE performance_date >= CURDATE() - INTERVAL 7 DAY
                 AND (false_positives + true_positives) >= %s
               GROUP BY rule_domain, rule_subdomain
               HAVING fp_rate_pct > 30
               ORDER BY fp_rate_pct DESC""",
            (MIN_FEEDBACK_FOR_PRECISION,)
        )

    def _send_weekly_report(self, report: dict) -> None:
        """Haftalık raporu Teams/Slack'e gönderir."""
        quality = report.get('quality_summary', [])
        violations = report.get('sla_violations', [])
        drifts = report.get('concept_drifts', [])
        high_fp = report.get('high_fp_rules', [])

        below_threshold = [
            q for q in quality if q.get('days_below_threshold', 0) > 0
        ]
        degrading = [
            q for q in quality if q.get('latest_trend') == 'degrading'
        ]

        lines = [
            f"📊 Haftalık Data Product Sağlık Raporu — {report['report_date'][:10]}",
            "",
            f"Data Products: {len(quality)} ürün izleniyor",
            f"  ⚠️ Eşik altında olan: {len(below_threshold)} ürün",
            f"  📉 Düşüş trendi gösteren: {len(degrading)} ürün",
            f"  🚨 SLA ihlali: {sum(v.get('violation_count',0) for v in violations)}",
            f"  🔄 Concept drift: {sum(d.get('drift_count',0) for d in drifts)}",
            f"  ❌ Yüksek false positive: {len(high_fp)} kural",
        ]

        if below_threshold:
            lines.append("")
            lines.append("Eşik altındaki ürünler:")
            for q in below_threshold[:5]:
                lines.append(
                    f"  • {q['product_code']}: "
                    f"ort. %{q.get('avg_score','?')}"
                )

        self.send_notifications("Haftalık Data Product Raporu", lines)
