"""
vce_operators.py
================
MailSender Pro — VCE Custom Airflow Operatörleri
=================================================

BAĞLANTI MİMARİSİ:
  Bu projede iki ayrı MySQL schema, aynı sunucuda bulunmaktadır.
  Her biri için ayrı Airflow Connection tanımlanmıştır:

  ┌─────────────────────────────────────────────────────────────┐
  │  Conn Id : vce                                              │
  │  Schema  : vce                                              │
  │  Kullanım: VCE tabloları (vce_dq_rules, vce_dq_executions,  │
  │            vce_rule_audit_log, vce_remediation_log vb.)     │
  │  Yetki   : SELECT + INSERT + UPDATE + DELETE (vce.*)        │
  └─────────────────────────────────────────────────────────────┘

  ┌─────────────────────────────────────────────────────────────┐
  │  Conn Id : mailsender                                       │
  │  Schema  : aws_mailsender_pro_v3                            │
  │  Kullanım: Kural SQL'lerinin çalıştırıldığı kaynak DB       │
  │            (send_log, suppression_list, senders vb.)        │
  │  Yetki   : Yalnızca SELECT (aws_mailsender_pro_v3.*)        │
  └─────────────────────────────────────────────────────────────┘

  Neden iki ayrı connection?
    - Yetki ayrımı: mailsender connection'ı hiçbir zaman VCE
      tablolarına yazamaz, vce connection'ı MailSender verilerini
      değiştiremez.
    - Sorumluluk ayrımı: kural SQL'leri mailsender üzerinde çalışır,
      sonuçlar vce üzerine yazılır — karışıklık olmaz.
    - Güvenlik: MailSender production DB'sine yazma yetkisi yok.

SINIFLAR:
  1. VCEBaseOperator        — Ortak metodlar (iki connection, bildirim, kayıt)
  2. DataQualityOperator    — Kural SQL'lerini çalıştırır
  3. TableValidationOperator— Kaynak-hedef karşılaştırması yapar
  4. RemediationOperator    — Otomatik temizlik işlemleri yapar

AIRFLOW CONNECTIONS (Admin → Connections):
  vce:
    Conn Type: Generic / MySQL
    Host     : host.docker.internal
    Schema   : vce
    Login    : airflow_vce
    Port     : 3306

  mailsender:
    Conn Type: Generic / MySQL
    Host     : host.docker.internal
    Schema   : aws_mailsender_pro_v3
    Login    : airflow_ms_readonly
    Port     : 3306

AIRFLOW VARIABLES (Admin → Variables):
  VCE_TEAMS_WEBHOOK_URL : Teams Incoming Webhook URL
  VCE_SLACK_WEBHOOK_URL : Slack Incoming Webhook URL (opsiyonel)
"""

from __future__ import annotations

import logging
import statistics
from datetime import datetime
from typing import Any

import pymysql
import pymysql.cursors
import requests

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults


# ── Connection ID Sabitleri ───────────────────────────────────────────────────
# Airflow UI'da bu isimlerle tanımlı olmalıdır.

VCE_CONN_ID = "vce"
# vce schema'sına bağlanır.
# VCE tablolarına yazar (vce_dq_executions, vce_remediation_log vb.)
# Yetki: SELECT + INSERT + UPDATE + DELETE → vce.*

MAILSENDER_CONN_ID = "mailsender"
# aws_mailsender_pro_v3 schema'sına bağlanır.
# Kural SQL'leri bu connection üzerinden çalışır.
# Yetki: Yalnızca SELECT → aws_mailsender_pro_v3.*

# ── Diğer Sabitler ────────────────────────────────────────────────────────────
DEFAULT_ANOMALY_THRESHOLD = 3.0   # |Z| > 3 ise anomali
TEAMS_WEBHOOK_VAR = "VCE_TEAMS_WEBHOOK_URL"
SLACK_WEBHOOK_VAR  = "VCE_SLACK_WEBHOOK_URL"


# ── VCEBaseOperator ───────────────────────────────────────────────────────────

class VCEBaseOperator(BaseOperator):
    """
    Tüm VCE operatörlerinin türetildiği temel sınıf.

    İKİ AYRI BAĞLANTI YÖNETİMİ:
      get_vce_conn()         → vce schema (VCE tabloları için)
      get_mailsender_conn()  → aws_mailsender_pro_v3 (kural SQL'leri için)

      run_vce_query()        → vce üzerinde SELECT
      run_mailsender_query() → aws_mailsender_pro_v3 üzerinde SELECT
      execute_vce_dml()      → vce üzerinde INSERT/UPDATE/DELETE

    NEDEN AYRI METODLAR?
      Tek bir run_query() metodu olsaydı hangi connection'ın kullanıldığı
      belirsiz olurdu. Ayrı metodlar sayesinde:
        - Kural SQL'i yanlışlıkla vce'ye çalıştırılamaz
        - VCE kayıtları yanlışlıkla mailsender'a yazılamaz
        - Kod okunurken bağlantı akışı açıkça görülür
    """

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Instance variable — paralel task'larda class variable paylaşımı yok
        self.fail_checks: list[str] = []
        self.warn_checks: list[str] = []

    # ── Bağlantı Fabrikaları ─────────────────────────────────────────────────

    def _make_conn(self, conn_id: str) -> pymysql.Connection:
        """
        Verilen Airflow conn_id için PyMySQL bağlantısı oluşturur.
        Her çağrıda yeni bağlantı açılır — finally bloğunda kapatılmalıdır.
        autocommit=False: DML işlemler manuel commit gerektirir.
        """
        info = BaseHook.get_connection(conn_id)
        return pymysql.connect(
            host=info.host,
            port=int(info.port or 3306),
            user=info.login,
            password=info.password,
            database=info.schema,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
            connect_timeout=15,
        )

    def get_vce_conn(self) -> pymysql.Connection:
        """
        vce schema bağlantısı.
        VCE tablolarını okumak ve yazmak için kullanılır.
        Conn Id: 'vce' → Schema: vce
        """
        return self._make_conn(VCE_CONN_ID)

    def get_mailsender_conn(self) -> pymysql.Connection:
        """
        aws_mailsender_pro_v3 schema bağlantısı.
        Kural SQL'lerini çalıştırmak için kullanılır — sadece SELECT.
        Conn Id: 'mailsender' → Schema: aws_mailsender_pro_v3
        """
        return self._make_conn(MAILSENDER_CONN_ID)

    # ── VCE Schema Sorgu Metodları ────────────────────────────────────────────

    def run_vce_query(self, sql: str, params: tuple = ()) -> list[dict]:
        """
        vce schema üzerinde SELECT çalıştırır.
        Kullanım: VCE tablolarından kural, baseline, execution okuma.
        """
        conn = self.get_vce_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall() or []
        finally:
            conn.close()

    def vce_scalar(self, sql: str, params: tuple = (), default: Any = 0) -> Any:
        """vce schema'da tek değer dönen sorgu için kısayol."""
        rows = self.run_vce_query(sql, params)
        return list(rows[0].values())[0] if rows else default

    def execute_vce_dml(self, sql: str, params: tuple = ()) -> int:
        """
        vce schema üzerinde INSERT/UPDATE/DELETE çalıştırır.
        Başarıyla commit eder, hata durumunda rollback yapar.
        Döner: etkilenen satır sayısı.
        """
        conn = self.get_vce_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                affected = cur.rowcount
            conn.commit()
            return affected
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ── MailSender Schema Sorgu Metodları ─────────────────────────────────────

    def run_mailsender_query(self, sql: str, params: tuple = ()) -> list[dict]:
        """
        aws_mailsender_pro_v3 schema üzerinde SELECT çalıştırır.
        Kullanım: Kural SQL'lerini çalıştırmak (send_log, suppression_list vb.)

        ÖNEMLI: Bu metod SADECE SELECT için kullanılmalıdır.
        airflow_ms_readonly kullanıcısının yazma yetkisi yoktur.
        """
        conn = self.get_mailsender_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall() or []
        finally:
            conn.close()

    def mailsender_scalar(self, sql: str, params: tuple = (), default: Any = 0) -> Any:
        """aws_mailsender_pro_v3 schema'da tek değer dönen sorgu için kısayol."""
        rows = self.run_mailsender_query(sql, params)
        return list(rows[0].values())[0] if rows else default

    # ── Execution Kaydı (vce schema'ya yazar) ────────────────────────────────

    def log_execution(
        self,
        dag: dict,
        rule_id: int | None,
        rule_domain: str,
        rule_subdomain: str,
        dataset_name: str | None,
        table_name: str | None,
        check_type: str,
        sql_statement: str,
        action: str,
        result_value: float,
        result_status: str,
        error_detail: str | None = None,
        baseline_mean: float | None = None,
        baseline_std: float | None = None,
        z_score: float | None = None,
    ) -> None:
        """
        Kural çalışma sonucunu vce.vce_dq_executions tablosuna kaydeder.
        execute_vce_dml() kullanır — vce connection üzerinden yazar.
        """
        sql = """
            INSERT INTO vce_dq_executions (
                rule_id, rule_domain, rule_subdomain,
                dataset_name, table_name, check_type,
                dag_id, dag_task_name, dag_run,
                sql_statement, action,
                result_value, result_status, error_detail,
                baseline_mean, baseline_std, z_score,
                run_date
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                NOW()
            )
        """
        params = (
            rule_id, rule_domain, rule_subdomain,
            dataset_name, table_name, check_type,
            dag["dag_id"], dag.get("task_name"), dag.get("dag_run"),
            sql_statement[:3000] if sql_statement else None,
            action,
            result_value, result_status, error_detail,
            baseline_mean, baseline_std, z_score,
        )
        try:
            self.execute_vce_dml(sql, params)
        except Exception as e:
            self.log.warning(f"Execution kaydı yazılamadı (vce schema): {e}")

    # ── Bildirimler ───────────────────────────────────────────────────────────

    def notify_teams(self, title: str, checks: list[str]) -> None:
        """Teams webhook bildirimi. URL Airflow Variable'dan okunur."""
        try:
            url = Variable.get(TEAMS_WEBHOOK_VAR, default_var=None)
            if not url:
                self.log.warning(
                    f"Teams webhook URL bulunamadı. "
                    f"Airflow Variable '{TEAMS_WEBHOOK_VAR}' tanımlı mı?"
                )
                return
            message = {
                "summary": f"VCE: {title}",
                "themeColor": "FF0000" if "fail" in title.lower() else "FFA500",
                "sections": [{
                    "activityTitle": f"VCE Veri Kalitesi: {title}",
                    "activitySubtitle": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
                    "facts": [
                        {"name": f"Kontrol {i+1}:", "value": c}
                        for i, c in enumerate(checks[:20])
                    ],
                }],
            }
            r = requests.post(url, json=message,
                              headers={"Content-Type": "application/json"}, timeout=10)
            if r.status_code == 200:
                self.log.info("Teams bildirimi gönderildi.")
            else:
                self.log.warning(f"Teams bildirimi başarısız: {r.status_code}")
        except Exception as e:
            self.log.warning(f"Teams bildirimi gönderilemedi: {e}")

    def notify_slack(self, title: str, checks: list[str]) -> None:
        """Slack webhook bildirimi. URL Airflow Variable'dan okunur."""
        try:
            url = Variable.get(SLACK_WEBHOOK_VAR, default_var=None)
            if not url:
                return
            emoji = "🔴" if any("fail" in c.lower() for c in checks) else "🟡"
            text  = "\n".join(f"• {c}" for c in checks[:20])
            requests.post(url, json={"text": f"{emoji} *VCE: {title}*\n{text}"}, timeout=10)
        except Exception as e:
            self.log.warning(f"Slack bildirimi gönderilemedi: {e}")

    def send_notifications(self, title: str, checks: list[str]) -> None:
        """Teams ve Slack bildirimini birlikte gönderir."""
        if checks:
            self.notify_teams(title, checks)
            self.notify_slack(title, checks)


# ── DataQualityOperator ───────────────────────────────────────────────────────

class DataQualityOperator(VCEBaseOperator):
    """
    SQL tabanlı veri kalitesi kurallarını çalıştıran operatör.

    BAĞLANTI AKIŞI:
      1. vce connection → vce_dq_rules tablosundan kuralları yükler
      2. mailsender connection → her kuralın sql_statement'ını çalıştırır
      3. vce connection → sonucu vce_dq_executions'a kaydeder
      4. vce connection → anomali için vce_anomaly_baselines günceller

    Bu ayrım sayesinde:
      - Kural SQL'leri aws_mailsender_pro_v3 üzerinde çalışır (doğru veri)
      - Sonuçlar vce üzerinde saklanır (doğru yer)
      - mailsender connection'ı hiçbir zaman VCE tablolarına dokunmaz
    """

    template_fields = ("rule_domain", "rule_subdomain", "execute_time")

    @apply_defaults
    def __init__(
        self,
        rule_domain: str,
        rule_subdomain: str | None = None,
        execute_time: str | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.rule_domain    = rule_domain
        self.rule_subdomain = rule_subdomain
        self.execute_time   = execute_time

    def execute(self, context: dict) -> None:
        dag = {
            "dag_id"   : context["dag"].dag_id,
            "dag_run"  : context["dag_run"].run_id,
            "task_name": context["task"].task_id,
        }

        # vce connection → kural listesi
        rules = self._load_rules()
        self.log.info(
            f"📋 {len(rules)} kural yüklendi "
            f"[vce.vce_dq_rules → {self.rule_domain}/{self.rule_subdomain or '*'}]"
        )

        # mailsender connection → her kuralı çalıştır
        for rule in rules:
            self._execute_rule(rule, dag)

        # Bildirim ve aksiyon
        if self.warn_checks:
            self.log.warning(f"⚠️ {len(self.warn_checks)} uyarı.")
            self.send_notifications(f"Data Quality WARN — {self.rule_domain}", self.warn_checks)

        if self.fail_checks:
            self.send_notifications(f"Data Quality FAIL — {self.rule_domain}", self.fail_checks)
            raise AirflowException(
                f"❌ {len(self.fail_checks)} kural başarısız:\n"
                + "\n".join(self.fail_checks)
            )

        self.log.info(f"✅ Tüm kontroller tamamlandı [{self.rule_domain}]")

    def _load_rules(self) -> list[dict]:
        """
        vce.vce_dq_rules tablosundan aktif kuralları yükler.
        run_vce_query() → vce connection kullanır.
        """
        conditions = ["active_flag = 1", "rule_domain = %s"]
        params: list = [self.rule_domain]

        if self.rule_subdomain:
            conditions.append("rule_subdomain = %s")
            params.append(self.rule_subdomain)

        if self.execute_time:
            conditions.append("(execute_time = %s OR execute_time IS NULL)")
            params.append(self.execute_time)

        sql = f"""
            SELECT id, rule_domain, rule_subdomain, dataset_name, table_name,
                   check_type, sql_statement, pre_sql_statement,
                   action, description, anomaly_threshold, test_flag
            FROM vce_dq_rules
            WHERE {' AND '.join(conditions)}
            ORDER BY rule_domain, rule_subdomain
        """
        return self.run_vce_query(sql, tuple(params))

    def _execute_rule(self, rule: dict, dag: dict) -> None:
        """
        Tek bir kuralı çalıştırır.

        BAĞLANTI AKIŞI:
          pre_sql_statement → run_mailsender_query() (aws_mailsender_pro_v3)
          sql_statement     → run_mailsender_query() (aws_mailsender_pro_v3)
          anomali baseline  → run_vce_query()        (vce)
          sonuç kaydı       → execute_vce_dml()      (vce)
        """
        rule_id     = rule["id"]
        domain      = rule["rule_domain"]
        subdomain   = rule["rule_subdomain"]
        check_type  = rule["check_type"]
        sql         = rule["sql_statement"]
        pre_sql     = rule["pre_sql_statement"]
        action      = rule["action"]
        description = rule["description"]
        test_flag   = rule["test_flag"]
        anomaly_thr = rule.get("anomaly_threshold") or DEFAULT_ANOMALY_THRESHOLD

        baseline_mean = baseline_std = z_score_val = None
        result_value  = 0.0
        result_status = "Passed"

        try:
            # 1. Hazırlık SQL → mailsender connection
            if pre_sql and pre_sql.strip():
                self.log.info(f"  🔧 pre_sql [{domain}/{subdomain}] → mailsender conn")
                self.run_mailsender_query(pre_sql)

            # 2. Kural SQL → mailsender connection
            self.log.info(
                f"  🔍 [{domain}/{subdomain}] tip={check_type} "
                f"→ aws_mailsender_pro_v3"
            )
            rows = self.run_mailsender_query(sql)
            result_value = float(list(rows[0].values())[0]) if rows else 0.0

            # 3. Anomali kontrolü → vce connection (baseline okuma)
            violation = False
            if check_type == "anomaly":
                baseline_mean, baseline_std, z_score_val, violation = \
                    self._check_anomaly(rule_id, domain, subdomain,
                                        result_value, anomaly_thr)
            else:
                violation = result_value > 0

            # 4. Aksiyon
            if violation:
                result_status = "Failed"
                msg = (
                    f"[{domain}/{subdomain}] İhlal! "
                    f"Değer={result_value:.2f} | Tip={check_type} | {description}"
                )
                if z_score_val is not None:
                    msg += (
                        f" | Z={z_score_val:.2f} "
                        f"(Ort={baseline_mean:.2f}, Std={baseline_std:.2f})"
                    )
                if test_flag:
                    self.log.warning(f"🧪 TEST MOD: {msg}")
                elif action == "fail":
                    self.fail_checks.append(msg)
                else:
                    self.warn_checks.append(msg)
            else:
                self.log.info(f"  ✅ PASS [{domain}/{subdomain}] değer={result_value:.2f}")

        except Exception as e:
            result_status = "Error"
            result_value  = -1.0
            err = f"[{domain}/{subdomain}] Hata: {e}"
            self.log.error(err)
            self.warn_checks.append(err)

        # 5. Sonucu vce'ye kaydet → execute_vce_dml()
        self.log_execution(
            dag=dag, rule_id=rule_id,
            rule_domain=domain, rule_subdomain=subdomain,
            dataset_name=rule.get("dataset_name"),
            table_name=rule.get("table_name"),
            check_type=check_type, sql_statement=sql,
            action=action, result_value=result_value,
            result_status=result_status,
            baseline_mean=baseline_mean, baseline_std=baseline_std,
            z_score=z_score_val,
        )

    def _check_anomaly(
        self,
        rule_id: int,
        domain: str,
        subdomain: str,
        current_value: float,
        threshold: float,
    ) -> tuple:
        """
        Z-skoru tabanlı anomali tespiti.
        run_vce_query() → vce.vce_dq_executions geçmiş değerleri okur.
        execute_vce_dml() → vce.vce_anomaly_baselines günceller.
        """
        # Geçmiş 30 günlük değerleri vce'den oku
        historical = self.run_vce_query(
            """SELECT result_value
               FROM vce_dq_executions
               WHERE rule_id = %s
                 AND result_status = 'Passed'
                 AND run_date >= NOW() - INTERVAL 30 DAY
               ORDER BY run_date DESC
               LIMIT 100""",
            (rule_id,)
        )

        values = [
            float(r["result_value"])
            for r in historical
            if r["result_value"] is not None
        ]

        if len(values) < 7:
            self.log.info(
                f"  ℹ️ Anomali [{domain}/{subdomain}]: "
                f"yetersiz geçmiş ({len(values)} < 7). Baseline biriktirilecek."
            )
            self._update_baseline(rule_id, domain, subdomain, values + [current_value])
            return None, None, None, False

        mean = statistics.mean(values)
        std  = statistics.stdev(values) if len(values) > 1 else 0.0

        if std == 0:
            return mean, std, 0.0, False

        z = abs(current_value - mean) / std
        is_anomaly = z > threshold

        self.log.info(
            f"  📊 [{domain}/{subdomain}]: "
            f"değer={current_value:.2f}, ort={mean:.2f}, std={std:.2f}, "
            f"z={z:.2f} → {'ANOMALİ' if is_anomaly else 'normal'}"
        )

        self._update_baseline(rule_id, domain, subdomain, values + [current_value])
        return mean, std, z, is_anomaly

    def _update_baseline(
        self, rule_id: int, domain: str, subdomain: str, values: list[float]
    ) -> None:
        """
        Anomali istatistiklerini vce.vce_anomaly_baselines tablosuna yazar.
        execute_vce_dml() → vce connection kullanır.
        """
        if not values:
            return
        mean     = statistics.mean(values)
        std      = statistics.stdev(values) if len(values) > 1 else 0.0
        sorted_v = sorted(values)
        p25      = sorted_v[len(sorted_v) // 4]
        p75      = sorted_v[3 * len(sorted_v) // 4]

        self.execute_vce_dml(
            """INSERT INTO vce_anomaly_baselines
                   (rule_id, rule_domain, rule_subdomain, window_days, sample_count,
                    mean_value, std_value, min_value, max_value, p25_value, p75_value)
               VALUES (%s, %s, %s, 30, %s, %s, %s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE
                   sample_count = VALUES(sample_count),
                   mean_value   = VALUES(mean_value),
                   std_value    = VALUES(std_value),
                   min_value    = VALUES(min_value),
                   max_value    = VALUES(max_value),
                   p25_value    = VALUES(p25_value),
                   p75_value    = VALUES(p75_value),
                   last_updated = NOW()""",
            (rule_id, domain, subdomain, len(values),
             mean, std, min(values), max(values), p25, p75)
        )


# ── TableValidationOperator ───────────────────────────────────────────────────

class TableValidationOperator(VCEBaseOperator):
    """
    Kaynak ve hedef tablo sorgularını karşılaştıran operatör.

    BAĞLANTI AKIŞI:
      1. vce connection      → vce_table_validations'dan tanımları yükler
      2. mailsender connection → source_sql ve target_sql çalıştırır
                                 (her ikisi de aws_mailsender_pro_v3 üzerinde)
      3. vce connection      → sonucu vce_table_val_executions'a kaydeder

    NOT: source_conn_id ve target_conn_id alanları tabloda tanımlıdır.
    Gelecekte farklı connection'lardan karşılaştırma yapılmak istenirse
    bu alanlar kullanılabilir. Şu an her ikisi de 'mailsender' olarak
    varsayılmaktadır çünkü her iki tablo da aws_mailsender_pro_v3'te.
    """

    template_fields = ("validation_domain", "validation_subdomain")

    @apply_defaults
    def __init__(
        self,
        validation_domain: str,
        validation_subdomain: str | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.validation_domain    = validation_domain
        self.validation_subdomain = validation_subdomain

    def execute(self, context: dict) -> None:
        dag = {
            "dag_id"   : context["dag"].dag_id,
            "dag_run"  : context["dag_run"].run_id,
            "task_name": context["task"].task_id,
            "retry"    : context["ti"].try_number,
        }

        # vce connection → tanımları yükle
        validations = self._load_validations()
        self.log.info(
            f"📋 {len(validations)} karşılaştırma yüklendi "
            f"[vce.vce_table_validations → {self.validation_domain}]"
        )

        # mailsender connection → sorguları çalıştır
        for val in validations:
            self._execute_validation(val, dag)

        if self.warn_checks:
            self.send_notifications(
                f"Table Validation WARN — {self.validation_domain}", self.warn_checks)

        if self.fail_checks:
            self.send_notifications(
                f"Table Validation FAIL — {self.validation_domain}", self.fail_checks)
            raise AirflowException(
                f"❌ {len(self.fail_checks)} karşılaştırma başarısız:\n"
                + "\n".join(self.fail_checks)
            )

        self.log.info("✅ Tüm tablo karşılaştırmaları tamamlandı.")

    def _load_validations(self) -> list[dict]:
        """vce.vce_table_validations'dan tanımları yükler — vce connection."""
        conditions = ["active_flag = 1", "validation_domain = %s"]
        params: list = [self.validation_domain]

        if self.validation_subdomain:
            conditions.append("validation_subdomain = %s")
            params.append(self.validation_subdomain)

        sql = f"""
            SELECT id, validation_domain, validation_subdomain,
                   source_conn_id, source_dataset, source_table,
                   source_sql, pre_source_sql,
                   target_conn_id, target_dataset, target_table,
                   target_sql, pre_target_sql,
                   comparison_type, tolerance_pct,
                   action, description, test_flag
            FROM vce_table_validations
            WHERE {' AND '.join(conditions)}
        """
        return self.run_vce_query(sql, tuple(params))

    def _execute_validation(self, val: dict, dag: dict) -> None:
        """
        Tek bir karşılaştırmayı çalıştırır.
        Kaynak ve hedef SQL'ler mailsender connection üzerinde çalışır.
        """
        domain    = val["validation_domain"]
        subdomain = val["validation_subdomain"]
        action    = val["action"]

        result_status = "Passed"
        diff_detail   = None
        source_rows   = []
        target_rows   = []

        try:
            # Hazırlık SQL'leri → mailsender connection
            if val.get("pre_source_sql"):
                self.log.info(f"  🔧 pre_source_sql [{domain}/{subdomain}] → mailsender")
                self.run_mailsender_query(val["pre_source_sql"])
            if val.get("pre_target_sql"):
                self.log.info(f"  🔧 pre_target_sql [{domain}/{subdomain}] → mailsender")
                self.run_mailsender_query(val["pre_target_sql"])

            # Kaynak ve hedef sorgular → mailsender connection
            self.log.info(
                f"  🔍 [{domain}/{subdomain}] "
                f"source → aws_mailsender_pro_v3.{val.get('source_table','?')}"
            )
            source_rows = self.run_mailsender_query(val["source_sql"])

            self.log.info(
                f"  🔍 [{domain}/{subdomain}] "
                f"target → aws_mailsender_pro_v3.{val.get('target_table','?')}"
            )
            target_rows = self.run_mailsender_query(val["target_sql"])

            # Karşılaştır
            is_valid, diff_detail = self._compare(
                source_rows, target_rows,
                val["comparison_type"],
                val.get("tolerance_pct") or 0.0,
            )

            if not is_valid:
                result_status = "Failed"
                msg = (
                    f"[{domain}/{subdomain}] Karşılaştırma başarısız! "
                    f"Tip={val['comparison_type']} | {diff_detail}"
                )
                if val["test_flag"]:
                    self.log.warning(f"🧪 TEST: {msg}")
                elif action == "fail":
                    self.fail_checks.append(msg)
                else:
                    self.warn_checks.append(msg)
            else:
                self.log.info(f"  ✅ PASS [{domain}/{subdomain}]")

        except Exception as e:
            result_status = "Error"
            diff_detail   = str(e)
            self.log.error(f"[{domain}/{subdomain}] Hata: {e}")
            self.warn_checks.append(f"[{domain}/{subdomain}] Çalıştırma hatası: {e}")

        # Sonucu vce'ye kaydet → execute_vce_dml()
        self._log_val_execution(val, dag, source_rows, target_rows,
                                result_status, diff_detail)

    def _compare(
        self,
        source: list[dict],
        target: list[dict],
        comparison_type: str,
        tolerance_pct: float,
    ) -> tuple[bool, str | None]:
        """Kaynak ve hedef sonuçlarını karşılaştırır."""
        if not source and not target:
            return True, None
        if not source:
            return False, "Kaynak sorgu boş sonuç döndürdü."
        if not target:
            return False, "Hedef sorgu boş sonuç döndürdü."

        if comparison_type == "count":
            if len(source) != len(target):
                return False, (
                    f"Satır sayısı farklı: "
                    f"kaynak={len(source)}, hedef={len(target)}"
                )
            return True, None

        if comparison_type == "sum":
            s = sum(float(list(r.values())[0]) for r in source if list(r.values())[0])
            t = sum(float(list(r.values())[0]) for r in target if list(r.values())[0])
            if abs(s - t) > 0.01:
                return False, f"Toplam farklı: kaynak={s:.2f}, hedef={t:.2f}"
            return True, None

        if comparison_type == "tolerance":
            s = float(list(source[0].values())[0]) if source else 0.0
            t = float(list(target[0].values())[0]) if target else 0.0
            if t == 0:
                return s == 0, None if s == 0 else "Hedef=0, kaynak sıfır değil."
            diff = abs(s - t) / t * 100
            if diff > tolerance_pct:
                return False, (
                    f"Yüzde fark %{diff:.2f} > eşik %{tolerance_pct:.2f} "
                    f"(kaynak={s}, hedef={t})"
                )
            return True, None

        # exact
        def norm(rows):
            return sorted([tuple(str(v) for v in r.values()) for r in rows])

        s_n, t_n = norm(source), norm(target)
        if s_n != t_n:
            for i, (s, t) in enumerate(zip(s_n, t_n)):
                if s != t:
                    return False, f"Satır {i+1} farklı: kaynak={s}, hedef={t}"
            if len(s_n) != len(t_n):
                return False, (
                    f"Satır sayısı farklı: "
                    f"kaynak={len(s_n)}, hedef={len(t_n)}"
                )
        return True, None

    def _log_val_execution(
        self, val, dag, source_rows, target_rows, status, diff
    ) -> None:
        """
        Karşılaştırma sonucunu vce.vce_table_val_executions'a kaydeder.
        execute_vce_dml() → vce connection kullanır.
        """
        def serialize(rows):
            txt = str(rows)
            return txt[:5000] if len(txt) > 5000 else txt

        try:
            self.execute_vce_dml(
                """INSERT INTO vce_table_val_executions (
                       validation_id, validation_domain, validation_subdomain,
                       dag_id, dag_task_name, dag_run, task_retry_count,
                       source_conn_id, source_dataset, source_table,
                       source_sql, source_result,
                       target_conn_id, target_dataset, target_table,
                       target_sql, target_result,
                       result_status, diff_detail, run_date
                   ) VALUES (
                       %s, %s, %s, %s, %s, %s, %s,
                       %s, %s, %s, %s, %s,
                       %s, %s, %s, %s, %s,
                       %s, %s, NOW()
                   )""",
                (
                    val["id"], val["validation_domain"], val["validation_subdomain"],
                    dag["dag_id"], dag["task_name"], dag["dag_run"], dag["retry"],
                    val["source_conn_id"], val.get("source_dataset"),
                    val.get("source_table"), val["source_sql"][:2000],
                    serialize(source_rows),
                    val["target_conn_id"], val.get("target_dataset"),
                    val.get("target_table"), val["target_sql"][:2000],
                    serialize(target_rows),
                    status, diff,
                )
            )
        except Exception as e:
            self.log.warning(f"TableValidation kaydı yazılamadı (vce schema): {e}")


# ── RemediationOperator ───────────────────────────────────────────────────────

class RemediationOperator(VCEBaseOperator):
    """
    Otomatik temizlik operatörü.

    BAĞLANTI AKIŞI:
      Temizlik SQL'leri → mailsender connection (aws_mailsender_pro_v3'teki
                          MailSender tablolarından eski kayıtları siler)
      Remediation log  → vce connection (vce.vce_remediation_log'a yazar)

    NEDEN mailsender connection temizlik yapıyor?
      Silinecek tablolar aws_mailsender_pro_v3 schema'sında:
        - aws_mailsender_pro_v3.password_reset_tokens
        - aws_mailsender_pro_v3.unsubscribe_tokens
        - aws_mailsender_pro_v3.rate_limit_log
        - aws_mailsender_pro_v3.ses_notifications

      Bu nedenle mailsender connection'ın DELETE yetkisi olmalıdır.
      airflow_ms_readonly kullanıcısı burada uygun değildir —
      airflow_ms_dml gibi ayrı bir kullanıcı önerilir.

    ÖNEMLI: Bu operatör iş verisi silmez.
      Sadece açıkça tanımlı geçici/süresi dolmuş kayıtları siler.
    """

    OPERATIONS = {
        "delete_expired_tokens": {
            "sql": (
                "DELETE FROM aws_mailsender_pro_v3.password_reset_tokens "
                "WHERE (used = 1 OR expires_at < NOW()) "
                "AND created_at < NOW() - INTERVAL 7 DAY"
            ),
            "table": "aws_mailsender_pro_v3.password_reset_tokens",
            "description": "Süresi dolmuş/kullanılmış şifre sıfırlama tokenları temizlendi.",
        },
        "delete_expired_unsub": {
            "sql": (
                "DELETE FROM aws_mailsender_pro_v3.unsubscribe_tokens "
                "WHERE used = 0 AND expires_at < NOW() - INTERVAL 30 DAY"
            ),
            "table": "aws_mailsender_pro_v3.unsubscribe_tokens",
            "description": "30+ gün önce süresi dolmuş kullanılmamış unsubscribe tokenları temizlendi.",
        },
        "delete_old_rate_logs": {
            "sql": (
                "DELETE FROM aws_mailsender_pro_v3.rate_limit_log "
                "WHERE hit_at < NOW() - INTERVAL 7 DAY"
            ),
            "table": "aws_mailsender_pro_v3.rate_limit_log",
            "description": "7+ günlük eski rate limit logları temizlendi.",
        },
        "delete_old_ses_notif": {
            "sql": (
                "DELETE FROM aws_mailsender_pro_v3.ses_notifications "
                "WHERE received_at < NOW() - INTERVAL 90 DAY"
            ),
            "table": "aws_mailsender_pro_v3.ses_notifications",
            "description": "90+ günlük eski SES bildirimleri temizlendi.",
        },
    }

    @apply_defaults
    def __init__(self, operations: list[str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.operations = (
            list(self.OPERATIONS.keys())
            if operations == ["all"]
            else operations
        )

    def execute(self, context: dict) -> None:
        dag = {
            "dag_id"   : context["dag"].dag_id,
            "dag_run"  : context["dag_run"].run_id,
            "task_name": context["task"].task_id,
        }

        results = []
        for op_type in self.operations:
            if op_type not in self.OPERATIONS:
                self.log.warning(f"Bilinmeyen işlem: {op_type}")
                continue
            result = self._run_operation(op_type, dag)
            results.append(result)

        total = sum(r.get("rows_affected", 0) for r in results)
        self.log.info(f"🧹 Remediation tamamlandı. Toplam: {total} satır etkilendi.")

    def _run_operation(self, op_type: str, dag: dict) -> dict:
        """
        Tek bir temizlik işlemini çalıştırır.
        DELETE → mailsender connection (aws_mailsender_pro_v3)
        LOG    → vce connection (vce.vce_remediation_log)
        """
        op            = self.OPERATIONS[op_type]
        rows_affected = 0
        status        = "Success"
        detail        = op["description"]

        try:
            # DELETE → mailsender connection (aws_mailsender_pro_v3)
            conn = self.get_mailsender_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(op["sql"])
                    rows_affected = cur.rowcount
                conn.commit()
            finally:
                conn.close()

            self.log.info(
                f"  ✅ {op_type}: {rows_affected} satır silindi "
                f"[{op['table']}]"
            )

        except Exception as e:
            status = "Failed"
            detail = str(e)
            self.log.error(f"  ❌ {op_type} başarısız: {e}")

        # LOG → vce connection (vce.vce_remediation_log)
        try:
            self.execute_vce_dml(
                """INSERT INTO vce_remediation_log (
                       dag_id, dag_run, task_name,
                       operation_type, target_table, sql_executed,
                       rows_affected, result_status, result_detail
                   ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    dag["dag_id"], dag["dag_run"], dag["task_name"],
                    op_type, op["table"], op["sql"][:2000],
                    rows_affected, status, detail,
                )
            )
        except Exception as e:
            self.log.warning(f"Remediation log yazılamadı (vce schema): {e}")

        return {"operation_type": op_type, "rows_affected": rows_affected, "status": status}
