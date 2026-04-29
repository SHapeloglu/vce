"""
vce_operators_extended.py
==========================
VCE Genişletilmiş Operatörler
===============================

Bu dosya Great Expectations ve Soda Core'dan ilham alınan üç yeni operatörü içerir.
vce_operators.py dosyasındaki VCEBaseOperator'dan türetilmiştir — aynı iki connection
mimarisini (vce / mailsender) kullanır.

OPERATÖRLER:
  1. ColumnStatsOperator      — Kolon profili istatistiklerini toplar
                                (GE column-level stats ilhamıyla)

  2. FailedRowsSamplingMixin  — DataQualityOperator'a mixin olarak eklenir
                                Fail eden kuralların örnek ihlal satırlarını çeker
                                (GE row-level validation + Soda failed rows sampling)

  3. DistributionCheckOperator— Değer dağılımının beklenen aralıkta olup
                                olmadığını kontrol eder
                                (Soda distribution check ilhamıyla)

BAĞLANTI MİMARİSİ (vce_operators.py ile aynı):
  mailsender conn → aws_mailsender_pro_v3 üzerinde istatistik/dağılım SQL'leri çalışır
  vce conn        → vce_column_stats, vce_failed_rows_samples, vce_dq_executions'a yazar

KULLANIM (DAG içinde):
  from operators.vce_operators_extended import (
      ColumnStatsOperator,
      DistributionCheckOperator,
  )
"""

from __future__ import annotations

import json
import logging
import statistics
from datetime import datetime
from typing import Any

from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults

# VCEBaseOperator'dan türetiyoruz — iki connection yönetimi ortak
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from vce_operators import VCEBaseOperator


# ═════════════════════════════════════════════════════════════════════════════
# OPERATÖR 1: ColumnStatsOperator
# Great Expectations "column-level stats" ilhamıyla
# ═════════════════════════════════════════════════════════════════════════════

class ColumnStatsOperator(VCEBaseOperator):
    """
    Kolon profili istatistiklerini toplar ve vce.vce_column_stats'a yazar.

    DEĞER:
      - "send_log.recipient NULL oranı bu ay arttı mı?" → trend analizi
      - "status kolonu hangi değerleri ne sıklıkla alıyor?" → dağılım izleme
      - "sent_count kolonunun ortalaması değişti mi?" → anomali tespiti temeli

    ÇALIŞMA MANTIĞI:
      1. vce.vce_column_stats_config tablosundan izlenecek kolon listesi yüklenir
      2. Her kolon için mailsender connection üzerinde istatistik SQL'leri çalışır
      3. Sonuçlar vce.vce_column_stats tablosuna yazılır

    BAĞLANTI AKIŞI:
      vce conn        → vce_column_stats_config'den kolon listesi oku
      mailsender conn → her kolon için istatistik hesapla
      vce conn        → vce_column_stats'a yaz

    ÖRNEK DAG KULLANIMI:
      stats_task = ColumnStatsOperator(
          task_id="collect_column_stats",
          schema_filter="aws_mailsender_pro_v3",  # Tüm izlenen tablolar
          table_filter="send_log",                 # Sadece bu tablo (opsiyonel)
      )
    """

    @apply_defaults
    def __init__(
        self,
        schema_filter: str = "aws_mailsender_pro_v3",
        table_filter: str | None = None,
        *args,
        **kwargs,
    ):
        """
        Args:
            schema_filter: Hangi schema'nın kolonları işlenecek
            table_filter : Sadece bu tablo (None ise tüm aktif tablolar)
        """
        super().__init__(*args, **kwargs)
        self.schema_filter = schema_filter
        self.table_filter  = table_filter

    def execute(self, context: dict) -> None:
        dag_run = context["dag_run"].run_id

        # vce_column_stats_config'den izlenecek kolonları yükle
        configs = self._load_configs()
        self.log.info(f"📊 {len(configs)} kolon izlenecek.")

        success_count = 0
        error_count   = 0

        for cfg in configs:
            try:
                stats = self._collect_stats(cfg)
                self._save_stats(cfg, stats, dag_run)
                self._check_thresholds(cfg, stats)
                success_count += 1
                self.log.info(
                    f"  ✅ {cfg['table_name']}.{cfg['column_name']}: "
                    f"null_rate={stats.get('null_rate', 'N/A'):.4f}, "
                    f"distinct={stats.get('distinct_count', 'N/A')}"
                )
            except Exception as e:
                error_count += 1
                self.log.error(f"  ❌ {cfg['table_name']}.{cfg['column_name']}: {e}")

        self.log.info(
            f"📊 Kolon istatistikleri tamamlandı. "
            f"Başarılı: {success_count}, Hata: {error_count}"
        )

        # Eşik ihlalleri varsa bildirim gönder
        if self.warn_checks:
            self.send_notifications("Column Stats — Eşik İhlali", self.warn_checks)

    def _load_configs(self) -> list[dict]:
        """vce.vce_column_stats_config'den aktif kolon tanımlarını yükler."""
        conditions = ["active_flag = 1", "schema_name = %s"]
        params = [self.schema_filter]

        if self.table_filter:
            conditions.append("table_name = %s")
            params.append(self.table_filter)

        return self.run_vce_query(
            f"""SELECT id, schema_name, table_name, column_name, column_type,
                       collect_nulls, collect_distinct, collect_stats,
                       collect_top_values, top_values_limit,
                       max_null_rate, min_distinct_count
                FROM vce_column_stats_config
                WHERE {' AND '.join(conditions)}
                ORDER BY table_name, column_name""",
            tuple(params)
        )

    def _collect_stats(self, cfg: dict) -> dict:
        """
        Bir kolon için istatistikleri hesaplar.
        mailsender connection kullanır — aws_mailsender_pro_v3 üzerinde çalışır.
        """
        table  = f"{cfg['schema_name']}.{cfg['table_name']}"
        col    = cfg['column_name']
        ctype  = cfg['column_type']
        stats  = {}

        # NULL ve distinct (tüm tipler)
        if cfg['collect_nulls'] or cfg['collect_distinct']:
            row = self.run_mailsender_query(
                f"""SELECT
                        COUNT(*) as row_count,
                        SUM(CASE WHEN `{col}` IS NULL THEN 1 ELSE 0 END) as null_count,
                        COUNT(DISTINCT `{col}`) as distinct_count
                    FROM {table}"""
            )
            if row:
                r = row[0]
                stats['row_count']      = r['row_count'] or 0
                stats['null_count']     = r['null_count'] or 0
                stats['distinct_count'] = r['distinct_count'] or 0
                stats['null_rate']      = (
                    stats['null_count'] / stats['row_count']
                    if stats['row_count'] > 0 else 0.0
                )
                stats['distinct_rate']  = (
                    stats['distinct_count'] / stats['row_count']
                    if stats['row_count'] > 0 else 0.0
                )

        # Sayısal istatistikler
        if cfg['collect_stats'] and ctype == 'numeric':
            row = self.run_mailsender_query(
                f"""SELECT
                        MIN(`{col}`) as min_value,
                        MAX(`{col}`) as max_value,
                        AVG(`{col}`) as mean_value,
                        STDDEV(`{col}`) as std_value
                    FROM {table}
                    WHERE `{col}` IS NOT NULL"""
            )
            if row and row[0]['min_value'] is not None:
                r = row[0]
                stats['min_value']  = float(r['min_value'])
                stats['max_value']  = float(r['max_value'])
                stats['mean_value'] = float(r['mean_value']) if r['mean_value'] else None
                stats['std_value']  = float(r['std_value'])  if r['std_value']  else None

            # Persentiller
            try:
                pcts = self.run_mailsender_query(
                    f"""SELECT
                            MAX(CASE WHEN pct_rank <= 25 THEN `{col}` END) as p25,
                            MAX(CASE WHEN pct_rank <= 50 THEN `{col}` END) as p50,
                            MAX(CASE WHEN pct_rank <= 75 THEN `{col}` END) as p75
                        FROM (
                            SELECT `{col}`,
                                   PERCENT_RANK() OVER (ORDER BY `{col}`) * 100 as pct_rank
                            FROM {table}
                            WHERE `{col}` IS NOT NULL
                        ) ranked"""
                )
                if pcts and pcts[0]['p50'] is not None:
                    stats['p25_value'] = float(pcts[0]['p25']) if pcts[0]['p25'] else None
                    stats['p50_value'] = float(pcts[0]['p50']) if pcts[0]['p50'] else None
                    stats['p75_value'] = float(pcts[0]['p75']) if pcts[0]['p75'] else None
            except Exception:
                pass  # Persentil bazı MySQL versiyonlarında desteklenmez

        # Metin istatistikleri
        if cfg['collect_stats'] and ctype == 'text':
            row = self.run_mailsender_query(
                f"""SELECT
                        MIN(LENGTH(`{col}`)) as min_length,
                        MAX(LENGTH(`{col}`)) as max_length,
                        AVG(LENGTH(`{col}`)) as avg_length
                    FROM {table}
                    WHERE `{col}` IS NOT NULL"""
            )
            if row and row[0]['min_length'] is not None:
                r = row[0]
                stats['min_length'] = r['min_length']
                stats['max_length'] = r['max_length']
                stats['avg_length'] = float(r['avg_length']) if r['avg_length'] else None

        # Tarih istatistikleri
        if cfg['collect_stats'] and ctype == 'datetime':
            row = self.run_mailsender_query(
                f"""SELECT
                        MIN(`{col}`) as min_date,
                        MAX(`{col}`) as max_date,
                        DATEDIFF(MAX(`{col}`), MIN(`{col}`)) as date_range_days
                    FROM {table}
                    WHERE `{col}` IS NOT NULL"""
            )
            if row and row[0]['min_date'] is not None:
                r = row[0]
                stats['min_date']        = str(r['min_date'])
                stats['max_date']        = str(r['max_date'])
                stats['date_range_days'] = r['date_range_days']

        # Top değerler (kategorik)
        if cfg['collect_top_values'] and ctype in ('categorical', 'text'):
            limit = cfg.get('top_values_limit', 5)
            rows = self.run_mailsender_query(
                f"""SELECT `{col}` as value, COUNT(*) as cnt
                    FROM {table}
                    WHERE `{col}` IS NOT NULL
                    GROUP BY `{col}`
                    ORDER BY cnt DESC
                    LIMIT {limit}"""
            )
            if rows:
                stats['top_values'] = json.dumps(
                    [{"value": str(r['value']), "count": r['cnt']} for r in rows],
                    ensure_ascii=False
                )

        return stats

    def _save_stats(self, cfg: dict, stats: dict, dag_run: str) -> None:
        """İstatistikleri vce.vce_column_stats tablosuna yazar."""
        self.execute_vce_dml(
            """INSERT INTO vce_column_stats (
                   schema_name, table_name, column_name, dag_run,
                   row_count, null_count, null_rate, distinct_count, distinct_rate,
                   min_value, max_value, mean_value, std_value,
                   p25_value, p50_value, p75_value,
                   min_length, max_length, avg_length,
                   min_date, max_date, date_range_days,
                   top_values, collected_at
               ) VALUES (
                   %s, %s, %s, %s,
                   %s, %s, %s, %s, %s,
                   %s, %s, %s, %s,
                   %s, %s, %s,
                   %s, %s, %s,
                   %s, %s, %s,
                   %s, NOW()
               )""",
            (
                cfg['schema_name'], cfg['table_name'], cfg['column_name'], dag_run,
                stats.get('row_count'), stats.get('null_count'),
                stats.get('null_rate'), stats.get('distinct_count'),
                stats.get('distinct_rate'),
                stats.get('min_value'), stats.get('max_value'),
                stats.get('mean_value'), stats.get('std_value'),
                stats.get('p25_value'), stats.get('p50_value'), stats.get('p75_value'),
                stats.get('min_length'), stats.get('max_length'), stats.get('avg_length'),
                stats.get('min_date'), stats.get('max_date'), stats.get('date_range_days'),
                stats.get('top_values'),
            )
        )

    def _check_thresholds(self, cfg: dict, stats: dict) -> None:
        """
        Eşik değerlerini kontrol eder — ihlal varsa warn_checks'e ekler.
        NULL oranı max_null_rate'i geçiyor mu?
        Distinct sayısı min_distinct_count'un altına düştü mü?
        """
        table = f"{cfg['table_name']}.{cfg['column_name']}"

        if cfg.get('max_null_rate') is not None and stats.get('null_rate') is not None:
            null_rate = stats['null_rate']
            max_rate  = float(cfg['max_null_rate'])
            if null_rate > max_rate:
                self.warn_checks.append(
                    f"[column_stats/{table}] NULL oranı eşiği aşıldı: "
                    f"{null_rate:.4f} > {max_rate:.4f} "
                    f"({null_rate*100:.2f}% > {max_rate*100:.2f}%)"
                )

        if cfg.get('min_distinct_count') is not None and stats.get('distinct_count') is not None:
            distinct = stats['distinct_count']
            minimum  = cfg['min_distinct_count']
            if distinct < minimum:
                self.warn_checks.append(
                    f"[column_stats/{table}] Distinct sayısı minimumun altında: "
                    f"{distinct} < {minimum}"
                )


# ═════════════════════════════════════════════════════════════════════════════
# OPERATÖR 2: DistributionCheckOperator
# Soda Core "distribution check" ilhamıyla
# ═════════════════════════════════════════════════════════════════════════════

class DistributionCheckOperator(VCEBaseOperator):
    """
    Değer dağılımının beklenen aralıkta olup olmadığını kontrol eder.

    DEĞER:
      Klasik threshold: "failed satır sayısı > 100 ise fail" → COUNT bazlı
      Distribution check: "status kolonu dağılımı beklenen gibi mi?" → ORAN bazlı

      Fark neden önemli?
        Toplam 1000 gönderim: 950 sent (%95), 50 failed (%5)  → Normal
        Toplam 100 gönderim:  45 sent  (%45), 55 failed (%55) → Anormal!

        COUNT bazlı kontrol: 55 failed > 50 eşik → fail (ama bu yanlış alarm olabilir)
        Distribution check: failed %55 > max %30 → fail (her zaman doğru karar)

    ÇALIŞMA MANTIĞI:
      1. vce.vce_distribution_checks tablosundan tanımları yükler
      2. Her tanım için mailsender'da değer dağılımını hesaplar
      3. Her değerin oranı expected_distribution JSON'undaki aralıkla karşılaştırılır
      4. İhlal varsa action'a göre fail_checks veya warn_checks'e ekler
      5. Sonuç vce.vce_dq_executions'a kaydedilir

    BAĞLANTI AKIŞI:
      vce conn        → vce_distribution_checks'ten tanımları yükle
      mailsender conn → dağılım SQL'ini çalıştır
      vce conn        → sonucu vce_dq_executions'a yaz

    ÖRNEK DAG KULLANIMI:
      dist_task = DistributionCheckOperator(
          task_id="check_distributions",
          check_domain="send_log",
      )
    """

    @apply_defaults
    def __init__(
        self,
        check_domain: str,
        check_subdomain: str | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.check_domain    = check_domain
        self.check_subdomain = check_subdomain

    def execute(self, context: dict) -> None:
        dag = {
            "dag_id"   : context["dag"].dag_id,
            "dag_run"  : context["dag_run"].run_id,
            "task_name": context["task"].task_id,
        }

        checks = self._load_checks()
        self.log.info(
            f"📊 {len(checks)} dağılım kontrolü yüklendi [{self.check_domain}]"
        )

        for check in checks:
            self._run_check(check, dag)

        if self.warn_checks:
            self.send_notifications(
                f"Distribution Check WARN — {self.check_domain}", self.warn_checks
            )
        if self.fail_checks:
            self.send_notifications(
                f"Distribution Check FAIL — {self.check_domain}", self.fail_checks
            )
            raise AirflowException(
                f"❌ {len(self.fail_checks)} dağılım kontrolü başarısız:\n"
                + "\n".join(self.fail_checks)
            )

        self.log.info("✅ Tüm dağılım kontrolleri tamamlandı.")

    def _load_checks(self) -> list[dict]:
        """vce.vce_distribution_checks'ten tanımları yükler."""
        conditions = ["active_flag = 1", "check_domain = %s"]
        params = [self.check_domain]

        if self.check_subdomain:
            conditions.append("check_subdomain = %s")
            params.append(self.check_subdomain)

        return self.run_vce_query(
            f"""SELECT id, check_domain, check_subdomain,
                       schema_name, table_name, column_name,
                       where_clause, expected_distribution,
                       action, description
                FROM vce_distribution_checks
                WHERE {' AND '.join(conditions)}""",
            tuple(params)
        )

    def _run_check(self, check: dict, dag: dict) -> None:
        """
        Tek bir dağılım kontrolü çalıştırır.

        1. mailsender'da değer dağılımını hesapla
        2. Her değerin oranını expected_distribution ile karşılaştır
        3. Sonucu vce_dq_executions'a yaz
        """
        domain    = check['check_domain']
        subdomain = check['check_subdomain']
        table     = f"{check['schema_name']}.{check['table_name']}"
        col       = check['column_name']
        action    = check['action']

        # WHERE clause
        where = f"WHERE {check['where_clause']}" if check.get('where_clause') else ""

        # Dağılımı hesapla — mailsender connection
        dist_sql = f"""
            SELECT `{col}` as val,
                   COUNT(*) as cnt,
                   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct
            FROM {table}
            {where}
            GROUP BY `{col}`
            ORDER BY cnt DESC
        """

        try:
            rows = self.run_mailsender_query(dist_sql)
            if not rows:
                self.log.warning(f"  ⚠️ [{domain}/{subdomain}]: Veri yok, atlandı.")
                return

            # Gerçek dağılım dict'i: {değer: yüzde}
            actual = {str(r['val']): float(r['pct']) for r in rows}

            # Beklenen dağılımı parse et
            expected = check['expected_distribution']
            if isinstance(expected, str):
                expected = json.loads(expected)

            # Her beklenen değeri kontrol et
            violations = []
            for exp in expected:
                value    = str(exp.get('value', exp.get('metric', '?')))
                min_pct  = float(exp.get('min_pct', 0))
                max_pct  = float(exp.get('max_pct', 100))
                actual_pct = actual.get(value, 0.0)

                if not (min_pct <= actual_pct <= max_pct):
                    violations.append(
                        f"'{value}': gerçek={actual_pct:.1f}% "
                        f"(beklenen: {min_pct:.0f}%-{max_pct:.0f}%)"
                    )

            # Sonucu değerlendir
            result_status = "Passed"
            if violations:
                result_status = "Failed"
                msg = (
                    f"[{domain}/{subdomain}] Dağılım ihlali! "
                    f"{table}.{col} → " + " | ".join(violations)
                )
                if action == "fail":
                    self.fail_checks.append(msg)
                else:
                    self.warn_checks.append(msg)
                self.log.warning(f"  ⚠️ {msg}")
            else:
                self.log.info(
                    f"  ✅ [{domain}/{subdomain}] Dağılım normal. "
                    f"Gerçek: {actual}"
                )

            # vce_dq_executions'a kaydet
            self.log_execution(
                dag=dag,
                rule_id=check['id'],
                rule_domain=domain,
                rule_subdomain=subdomain,
                dataset_name=check['schema_name'],
                table_name=check['table_name'],
                check_type="distribution",
                sql_statement=dist_sql,
                action=action,
                result_value=float(len(violations)),
                result_status=result_status,
                error_detail="; ".join(violations) if violations else None,
            )

        except Exception as e:
            self.log.error(f"  ❌ [{domain}/{subdomain}] Hata: {e}")
            self.warn_checks.append(f"[{domain}/{subdomain}] Çalıştırma hatası: {e}")


# ═════════════════════════════════════════════════════════════════════════════
# OPERATÖR 3: FailedRowsSamplingMixin
# GE row-level validation + Soda failed rows sampling ilhamıyla
# ═════════════════════════════════════════════════════════════════════════════

class FailedRowsSamplingMixin:
    """
    DataQualityOperator'a eklenebilen mixin.
    Kural fail ettiğinde 'sample_sql' alanındaki SQL'i çalıştırır
    ve örnek ihlal satırlarını vce.vce_failed_rows_samples'a yazar.

    DEĞER:
      Klasik VCE: "142 satır NULL recipient var" → ne kadar büyük sorun?
      Bu mixin ile: Hangi 10 satır? ID'leri neler? Ne zaman gönderildi?

    KULLANIM:
      DataQualityOperator zaten bu mixin'i destekler.
      vce_dq_rules.sample_sql kolonuna örnek satırları çeken SQL yazılır:

      Örnek kural:
        sql_statement : SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_log
                        WHERE recipient IS NULL
        sample_sql    : SELECT id, sender_id, sent_at, error_msg
                        FROM aws_mailsender_pro_v3.send_log
                        WHERE recipient IS NULL
                        ORDER BY sent_at DESC
                        LIMIT 10

      sample_sql NULL ise örnek alınmaz (varsayılan davranış korunur).

    NOT: Bu mixin DataQualityOperator._execute_rule() içinde çağrılır.
         vce_operators.py'deki DataQualityOperator sınıfına şu satır eklenir:
         if violation and rule.get('sample_sql'):
             self._collect_failed_samples(rule, dag, result_value)
    """

    def _collect_failed_samples(
        self,
        rule: dict,
        dag: dict,
        total_violation_count: float,
        execution_id: int | None = None,
    ) -> None:
        """
        Fail eden kural için örnek ihlal satırlarını toplar.

        Args:
            rule                  : vce_dq_rules satırı
            dag                   : Airflow dag bağlamı
            total_violation_count : Ana SQL'in döndürdüğü toplam ihlal sayısı
            execution_id          : vce_dq_executions.id (bağlantı için)
        """
        sample_sql = rule.get('sample_sql')
        if not sample_sql or not sample_sql.strip():
            return  # sample_sql tanımlanmamışsa sessizce geç

        domain    = rule['rule_domain']
        subdomain = rule['rule_subdomain']

        try:
            # Örnek satırları çek — mailsender connection
            sample_rows = self.run_mailsender_query(sample_sql)

            if not sample_rows:
                self.log.info(
                    f"  ℹ️ [{domain}/{subdomain}] sample_sql boş sonuç döndürdü."
                )
                return

            # JSON formatına çevir
            sample_json = json.dumps(
                [
                    {k: str(v) if v is not None else None for k, v in row.items()}
                    for row in sample_rows
                ],
                ensure_ascii=False,
                default=str,
            )

            # vce_failed_rows_samples'a yaz — vce connection
            self.execute_vce_dml(
                """INSERT INTO vce_failed_rows_samples (
                       execution_id, rule_id, rule_domain, rule_subdomain,
                       dag_id, dag_run,
                       sample_sql, sample_data, sample_count,
                       total_violation_count, sampled_at
                   ) VALUES (
                       %s, %s, %s, %s, %s, %s,
                       %s, %s, %s, %s, NOW()
                   )""",
                (
                    execution_id, rule['id'], domain, subdomain,
                    dag['dag_id'], dag['dag_run'],
                    sample_sql[:2000], sample_json, len(sample_rows),
                    int(total_violation_count),
                )
            )

            self.log.info(
                f"  🔍 [{domain}/{subdomain}] {len(sample_rows)} örnek ihlal satırı "
                f"kaydedildi (toplam ihlal: {int(total_violation_count)})"
            )

        except Exception as e:
            # Sampling başarısız olursa ana akışı durdurma — sadece logla
            self.log.warning(
                f"  ⚠️ [{domain}/{subdomain}] Örnek alınamadı: {e}"
            )

    def get_failed_samples(self, rule_id: int, last_n: int = 5) -> list[dict]:
        """
        Bir kuralın son N çalışmasındaki örnek ihlal satırlarını döner.
        Dashboard ve raporlama için kullanılabilir.

        Args:
            rule_id : vce_dq_rules.id
            last_n  : Son kaç çalışmanın örnekleri

        Returns:
            Örnek satırlar listesi
        """
        return self.run_vce_query(
            """SELECT rule_subdomain, sample_data, sample_count,
                      total_violation_count, sampled_at
               FROM vce_failed_rows_samples
               WHERE rule_id = %s
               ORDER BY sampled_at DESC
               LIMIT %s""",
            (rule_id, last_n)
        )


# ═════════════════════════════════════════════════════════════════════════════
# YARDIMCI: İstatistik Sorgulama Fonksiyonları
# ═════════════════════════════════════════════════════════════════════════════

def get_column_trend(vce_conn_func, table: str, column: str, days: int = 30) -> list[dict]:
    """
    Bir kolonun istatistik trendini döner.
    Dashboard ve rapor üretimi için kullanılabilir.

    Örnek kullanım:
        trend = get_column_trend(op.run_vce_query, 'send_log', 'recipient', 30)
        # Her gün için null_rate, distinct_count döner
        # "Bu ay NULL oranı arttı mı?" sorusunu yanıtlar

    Args:
        vce_conn_func : run_vce_query metodu
        table         : Tablo adı
        column        : Kolon adı
        days          : Kaç günlük trend

    Returns:
        Her gün için istatistik içeren dict listesi
    """
    return vce_conn_func(
        """SELECT
               DATE(collected_at) as gun,
               null_rate,
               distinct_count,
               mean_value,
               std_value,
               row_count
           FROM vce_column_stats
           WHERE table_name   = %s
             AND column_name  = %s
             AND collected_at >= NOW() - INTERVAL %s DAY
           ORDER BY collected_at""",
        (table, column, days)
    )


def get_distribution_history(vce_conn_func, domain: str, subdomain: str, days: int = 7) -> list[dict]:
    """
    Bir dağılım kontrolünün geçmiş sonuçlarını döner.

    Args:
        vce_conn_func : run_vce_query metodu
        domain        : check_domain
        subdomain     : check_subdomain
        days          : Kaç günlük geçmiş

    Returns:
        Her çalışma için result_status ve error_detail içeren liste
    """
    return vce_conn_func(
        """SELECT run_date, result_status, result_value, error_detail
           FROM vce_dq_executions
           WHERE rule_domain    = %s
             AND rule_subdomain = %s
             AND check_type     = 'distribution'
             AND run_date       >= NOW() - INTERVAL %s DAY
           ORDER BY run_date DESC""",
        (domain, subdomain, days)
    )
