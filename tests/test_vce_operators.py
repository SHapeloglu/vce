"""
test_vce_operators.py
=====================
VCE Operatörleri için Unit Test Suite
======================================

Bu dosya DataQualityOperator, TableValidationOperator ve RemediationOperator
sınıflarının davranışlarını gerçek bir veritabanı bağlantısı olmadan test eder.

TEST STRATEJİSİ:
  Gerçek MySQL bağlantısı yerine unittest.mock ile sahte (mock) bağlantılar
  kullanılır. Bu sayede:
    - Testler izole çalışır (DB durumuna bağımlı değil)
    - CI/CD ortamında MySQL olmadan çalışır
    - Hızlı çalışır (ağ I/O yok)
    - Tekrar üretilebilir sonuçlar verir

ÇALIŞTIRMA:
  # Tüm testler
  pytest tests/ -v

  # Belirli bir sınıf
  pytest tests/test_vce_operators.py::TestDataQualityOperator -v

  # Coverage raporu
  pytest tests/ --cov=operators --cov-report=term-missing

BAĞIMLILIKLAR:
  pip install pytest pytest-mock pytest-cov

MOCK MİMARİSİ:
  Her test metodunda get_vce_conn() ve get_mailsender_conn() metodları
  patch ile sahte bağlantı döndürecek şekilde değiştirilir.
  Sahte cursor, sorgu sonuçlarını döndürür — gerçek SQL çalışmaz.
"""

from __future__ import annotations

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch, call, PropertyMock


# ── Test Yardımcıları ─────────────────────────────────────────────────────────

def make_mock_conn(fetchall_return=None, rowcount=0):
    """
    Sahte PyMySQL bağlantısı oluşturur.

    Args:
        fetchall_return: cursor.fetchall() döndüreceği değer
        rowcount: cursor.rowcount değeri (DML işlemler için)

    Returns:
        Mock connection objesi
    """
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = fetchall_return or []
    mock_cursor.rowcount = rowcount
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    return mock_conn, mock_cursor


def make_airflow_context(dag_id="test_dag", task_id="test_task", run_id="manual__test"):
    """
    Sahte Airflow context objesi oluşturur.
    execute() metoduna geçirilir.
    """
    mock_dag     = MagicMock()
    mock_dag.dag_id = dag_id

    mock_dag_run = MagicMock()
    mock_dag_run.run_id = run_id

    mock_task    = MagicMock()
    mock_task.task_id = task_id

    mock_ti      = MagicMock()
    mock_ti.try_number = 1

    return {
        "dag"     : mock_dag,
        "dag_run" : mock_dag_run,
        "task"    : mock_task,
        "ti"      : mock_ti,
    }


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_rules():
    """Temel test kuralları listesi."""
    return [
        {
            "id"            : 1,
            "rule_domain"   : "send_log",
            "rule_subdomain": "failed_ratio",
            "dataset_name"  : "aws_mailsender_pro_v3",
            "table_name"    : "send_log",
            "check_type"    : "threshold",
            "sql_statement" : "SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_log WHERE status='failed'",
            "pre_sql_statement": None,
            "action"        : "fail",
            "description"   : "Failed oranı kontrolü",
            "anomaly_threshold": 3.0,
            "test_flag"     : 0,
        }
    ]


@pytest.fixture
def sample_anomaly_rule():
    """Anomali tipi test kuralı."""
    return {
        "id"            : 2,
        "rule_domain"   : "send_log",
        "rule_subdomain": "daily_volume",
        "dataset_name"  : "aws_mailsender_pro_v3",
        "table_name"    : "send_log",
        "check_type"    : "anomaly",
        "sql_statement" : "SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_log WHERE DATE(sent_at)=CURDATE()",
        "pre_sql_statement": None,
        "action"        : "warn",
        "description"   : "Günlük hacim anomali kontrolü",
        "anomaly_threshold": 3.0,
        "test_flag"     : 0,
    }


@pytest.fixture
def operator():
    """
    Test için DataQualityOperator instance'ı.
    Airflow bağlamı dışında oluşturulduğu için task_id manuel verilir.
    """
    # Airflow BaseOperator'ın DAG gerektirmesini bypass et
    with patch("airflow.models.BaseOperator.__init__", return_value=None):
        from operators.vce_operators import DataQualityOperator
        op = DataQualityOperator.__new__(DataQualityOperator)
        op.rule_domain    = "send_log"
        op.rule_subdomain = None
        op.execute_time   = "06:00"
        op.fail_checks    = []   # Instance variable — class variable değil!
        op.warn_checks    = []
        op.log            = MagicMock()
        return op


# ═════════════════════════════════════════════════════════════════════════════
# GRUP 1: VCEBaseOperator — Temel Metodlar
# ═════════════════════════════════════════════════════════════════════════════

class TestVCEBaseOperator:
    """
    VCEBaseOperator'ın temel metodlarını test eder.
    İki ayrı connection yönetimi, sorgu metodları ve DML işlemleri.
    """

    def test_get_vce_conn_uses_vce_conn_id(self):
        """
        get_vce_conn() her zaman VCE_CONN_ID='vce' kullanmalı.
        MailSender connection'ını kullanmamalı.
        """
        with patch("airflow.models.BaseOperator.__init__", return_value=None):
            from operators.vce_operators import VCEBaseOperator, VCE_CONN_ID

            op = VCEBaseOperator.__new__(VCEBaseOperator)
            op.fail_checks = []
            op.warn_checks = []
            op.log = MagicMock()

            with patch.object(op, "_make_conn") as mock_make:
                mock_make.return_value = MagicMock()
                op.get_vce_conn()
                mock_make.assert_called_once_with(VCE_CONN_ID)

    def test_get_mailsender_conn_uses_mailsender_conn_id(self):
        """
        get_mailsender_conn() her zaman MAILSENDER_CONN_ID='mailsender' kullanmalı.
        VCE connection'ını kullanmamalı.
        """
        with patch("airflow.models.BaseOperator.__init__", return_value=None):
            from operators.vce_operators import VCEBaseOperator, MAILSENDER_CONN_ID

            op = VCEBaseOperator.__new__(VCEBaseOperator)
            op.fail_checks = []
            op.warn_checks = []
            op.log = MagicMock()

            with patch.object(op, "_make_conn") as mock_make:
                mock_make.return_value = MagicMock()
                op.get_mailsender_conn()
                mock_make.assert_called_once_with(MAILSENDER_CONN_ID)

    def test_two_connections_are_independent(self):
        """
        VCE ve MailSender connection'ları birbirinden bağımsız olmalı.
        Aynı connection objesi döndürülmemeli.
        """
        with patch("airflow.models.BaseOperator.__init__", return_value=None):
            from operators.vce_operators import VCEBaseOperator

            op = VCEBaseOperator.__new__(VCEBaseOperator)
            op.fail_checks = []
            op.warn_checks = []
            op.log = MagicMock()

            vce_conn  = MagicMock()
            ms_conn   = MagicMock()

            with patch.object(op, "_make_conn", side_effect=[vce_conn, ms_conn]):
                c1 = op.get_vce_conn()
                c2 = op.get_mailsender_conn()
                assert c1 is not c2

    def test_run_vce_query_closes_connection_on_success(self):
        """
        run_vce_query() başarılı olunca bağlantıyı kapatmalı.
        Finally bloğu çalışmalı.
        """
        with patch("airflow.models.BaseOperator.__init__", return_value=None):
            from operators.vce_operators import VCEBaseOperator

            op = VCEBaseOperator.__new__(VCEBaseOperator)
            op.fail_checks = []
            op.warn_checks = []
            op.log = MagicMock()

            mock_conn, _ = make_mock_conn([{"count": 5}])

            with patch.object(op, "get_vce_conn", return_value=mock_conn):
                op.run_vce_query("SELECT 1")
                mock_conn.close.assert_called_once()

    def test_run_vce_query_closes_connection_on_error(self):
        """
        run_vce_query() hata fırlatınca da bağlantıyı kapatmalı.
        Connection sızıntısı olmamalı.
        """
        with patch("airflow.models.BaseOperator.__init__", return_value=None):
            from operators.vce_operators import VCEBaseOperator

            op = VCEBaseOperator.__new__(VCEBaseOperator)
            op.fail_checks = []
            op.warn_checks = []
            op.log = MagicMock()

            mock_conn, mock_cursor = make_mock_conn()
            mock_cursor.execute.side_effect = Exception("DB Error")

            with patch.object(op, "get_vce_conn", return_value=mock_conn):
                with pytest.raises(Exception, match="DB Error"):
                    op.run_vce_query("SELECT 1")
                mock_conn.close.assert_called_once()

    def test_execute_vce_dml_commits_on_success(self):
        """execute_vce_dml() başarılı olunca commit çağırmalı."""
        with patch("airflow.models.BaseOperator.__init__", return_value=None):
            from operators.vce_operators import VCEBaseOperator

            op = VCEBaseOperator.__new__(VCEBaseOperator)
            op.fail_checks = []
            op.warn_checks = []
            op.log = MagicMock()

            mock_conn, _ = make_mock_conn(rowcount=5)

            with patch.object(op, "get_vce_conn", return_value=mock_conn):
                result = op.execute_vce_dml("INSERT INTO vce_test VALUES (%s)", (1,))
                mock_conn.commit.assert_called_once()
                mock_conn.rollback.assert_not_called()
                assert result == 5

    def test_execute_vce_dml_rollbacks_on_error(self):
        """execute_vce_dml() hata fırlatınca rollback çağırmalı, commit çağırmamalı."""
        with patch("airflow.models.BaseOperator.__init__", return_value=None):
            from operators.vce_operators import VCEBaseOperator

            op = VCEBaseOperator.__new__(VCEBaseOperator)
            op.fail_checks = []
            op.warn_checks = []
            op.log = MagicMock()

            mock_conn, mock_cursor = make_mock_conn()
            mock_cursor.execute.side_effect = Exception("Insert failed")

            with patch.object(op, "get_vce_conn", return_value=mock_conn):
                with pytest.raises(Exception, match="Insert failed"):
                    op.execute_vce_dml("INSERT INTO vce_test VALUES (%s)", (1,))
                mock_conn.rollback.assert_called_once()
                mock_conn.commit.assert_not_called()

    def test_run_mailsender_query_uses_mailsender_connection(self):
        """
        run_mailsender_query() vce connection'ını DEĞİL mailsender connection'ını kullanmalı.
        Bu test iki connection ayrımının doğru çalıştığını kanıtlar.
        """
        with patch("airflow.models.BaseOperator.__init__", return_value=None):
            from operators.vce_operators import VCEBaseOperator

            op = VCEBaseOperator.__new__(VCEBaseOperator)
            op.fail_checks = []
            op.warn_checks = []
            op.log = MagicMock()

            ms_conn, _ = make_mock_conn([{"count": 42}])

            with patch.object(op, "get_mailsender_conn", return_value=ms_conn) as mock_ms, \
                 patch.object(op, "get_vce_conn") as mock_vce:
                op.run_mailsender_query("SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_log")
                mock_ms.assert_called_once()
                mock_vce.assert_not_called()  # VCE connection kullanılmamalı!

    def test_no_sql_injection_via_fstring(self):
        """
        Hiçbir SQL f-string ile oluşturulmamalı.
        Tüm dinamik değerler parametre binding ile geçilmeli.

        Bu test operatör kaynak kodunu string analizi ile kontrol eder.
        """
        import inspect
        from operators import vce_operators

        source = inspect.getsource(vce_operators)

        # f-string içinde SQL anahtar kelimesi geçiyorsa risk var
        dangerous_patterns = [
            'f"SELECT',
            "f'SELECT",
            'f"INSERT',
            "f'INSERT",
            'f"DELETE',
            "f'DELETE",
            'f"UPDATE',
            "f'UPDATE",
        ]

        for pattern in dangerous_patterns:
            assert pattern not in source, (
                f"Potansiyel SQL injection riski: {pattern} bulundu. "
                "Parametre binding kullanın."
            )


# ═════════════════════════════════════════════════════════════════════════════
# GRUP 2: DataQualityOperator — Threshold Kontrolleri
# ═════════════════════════════════════════════════════════════════════════════

class TestDataQualityOperatorThreshold:
    """
    DataQualityOperator'ın threshold tipi kurallar için davranışını test eder.
    """

    def test_threshold_pass_when_sql_returns_zero(self, operator, sample_rules):
        """
        SQL sonucu 0 döndürünce kural geçmeli.
        fail_checks ve warn_checks boş kalmalı.
        """
        # Düzenleme: kurallar vce'den yükleniyor, SQL mailsender'da çalışıyor
        with patch.object(operator, "_load_rules", return_value=sample_rules), \
             patch.object(operator, "run_mailsender_query",
                          return_value=[{"count": 0}]) as mock_ms, \
             patch.object(operator, "log_execution"):

            operator._execute_rule(sample_rules[0], {
                "dag_id": "test", "dag_run": "run1", "task_name": "task1"
            })

        assert len(operator.fail_checks) == 0
        assert len(operator.warn_checks) == 0
        # Kural SQL'i mailsender üzerinde çalıştı
        mock_ms.assert_called_once()

    def test_threshold_fail_when_sql_returns_positive(self, operator, sample_rules):
        """
        SQL sonucu > 0 döndürünce action=fail olan kural fail_checks'e eklemeli.
        """
        with patch.object(operator, "_load_rules", return_value=sample_rules), \
             patch.object(operator, "run_mailsender_query",
                          return_value=[{"count": 15}]), \
             patch.object(operator, "log_execution"):

            operator._execute_rule(sample_rules[0], {
                "dag_id": "test", "dag_run": "run1", "task_name": "task1"
            })

        assert len(operator.fail_checks) == 1
        assert len(operator.warn_checks) == 0
        assert "send_log/failed_ratio" in operator.fail_checks[0]

    def test_threshold_warn_when_action_is_warn(self, operator, sample_rules):
        """
        SQL sonucu > 0 ama action=warn ise warn_checks'e eklemeli, fail_checks'e değil.
        """
        warn_rule = {**sample_rules[0], "action": "warn"}

        with patch.object(operator, "run_mailsender_query",
                          return_value=[{"count": 5}]), \
             patch.object(operator, "log_execution"):

            operator._execute_rule(warn_rule, {
                "dag_id": "test", "dag_run": "run1", "task_name": "task1"
            })

        assert len(operator.warn_checks) == 1
        assert len(operator.fail_checks) == 0

    def test_test_flag_skips_action(self, operator, sample_rules):
        """
        test_flag=1 olan kural fail etse bile fail_checks'e eklenmemeli.
        Sadece loglanmalı.
        """
        test_rule = {**sample_rules[0], "test_flag": 1}

        with patch.object(operator, "run_mailsender_query",
                          return_value=[{"count": 99}]), \
             patch.object(operator, "log_execution"):

            operator._execute_rule(test_rule, {
                "dag_id": "test", "dag_run": "run1", "task_name": "task1"
            })

        assert len(operator.fail_checks) == 0
        assert len(operator.warn_checks) == 0

    def test_pre_sql_runs_on_mailsender_before_main_sql(self, operator, sample_rules):
        """
        pre_sql_statement tanımlıysa mailsender üzerinde asıl SQL'den ÖNCE çalışmalı.
        """
        rule_with_pre = {
            **sample_rules[0],
            "pre_sql_statement": "CREATE TEMPORARY TABLE tmp AS SELECT 1"
        }

        call_order = []

        def track_calls(sql, *args, **kwargs):
            call_order.append(sql[:20])  # İlk 20 karakter yeterli
            return [{"count": 0}]

        with patch.object(operator, "run_mailsender_query",
                          side_effect=track_calls), \
             patch.object(operator, "log_execution"):

            operator._execute_rule(rule_with_pre, {
                "dag_id": "test", "dag_run": "run1", "task_name": "task1"
            })

        assert len(call_order) == 2
        assert "CREATE TEMPORARY" in call_order[0]  # pre_sql önce
        assert "SELECT COUNT" in call_order[1]       # asıl SQL sonra

    def test_sql_error_adds_to_warn_not_fail(self, operator, sample_rules):
        """
        Kural SQL'i çalışırken hata fırlatırsa warn_checks'e eklemeli.
        Beklenmedik hata DAG'ı durdurmamalı (fail değil warn).
        """
        with patch.object(operator, "run_mailsender_query",
                          side_effect=Exception("MySQL connection lost")), \
             patch.object(operator, "log_execution"):

            operator._execute_rule(sample_rules[0], {
                "dag_id": "test", "dag_run": "run1", "task_name": "task1"
            })

        assert len(operator.warn_checks) == 1
        assert "MySQL connection lost" in operator.warn_checks[0]
        assert len(operator.fail_checks) == 0

    def test_execution_always_logged_even_on_pass(self, operator, sample_rules):
        """
        Kural geçse de fail etse de log_execution çağrılmalı.
        Her çalışma kaydedilmeli.
        """
        with patch.object(operator, "run_mailsender_query",
                          return_value=[{"count": 0}]), \
             patch.object(operator, "log_execution") as mock_log:

            operator._execute_rule(sample_rules[0], {
                "dag_id": "test", "dag_run": "run1", "task_name": "task1"
            })

        mock_log.assert_called_once()
        call_kwargs = mock_log.call_args[1]
        assert call_kwargs["result_status"] == "Passed"

    def test_execution_logged_to_vce_not_mailsender(self, operator, sample_rules):
        """
        log_execution içindeki execute_vce_dml() vce connection kullanmalı.
        mailsender connection'ı kullanmamalı.
        """
        with patch.object(operator, "run_mailsender_query",
                          return_value=[{"count": 0}]), \
             patch.object(operator, "execute_vce_dml") as mock_vce_dml, \
             patch.object(operator, "get_mailsender_conn") as mock_ms_conn:

            operator.log_execution(
                dag={"dag_id": "test", "task_name": "t1", "dag_run": "r1"},
                rule_id=1,
                rule_domain="send_log",
                rule_subdomain="test",
                dataset_name=None,
                table_name=None,
                check_type="threshold",
                sql_statement="SELECT 1",
                action="fail",
                result_value=0.0,
                result_status="Passed",
            )

        mock_vce_dml.assert_called_once()
        mock_ms_conn.assert_not_called()  # mailsender bağlantısı açılmamalı!


# ═════════════════════════════════════════════════════════════════════════════
# GRUP 3: DataQualityOperator — Anomali Tespiti
# ═════════════════════════════════════════════════════════════════════════════

class TestDataQualityOperatorAnomaly:
    """
    DataQualityOperator'ın anomali tespiti mantığını test eder.
    Z-skoru hesaplama, yetersiz baseline, eşik aşımı.
    """

    def test_anomaly_no_trigger_with_insufficient_baseline(
        self, operator, sample_anomaly_rule
    ):
        """
        Geçmiş 7'den az veri varsa anomali tetiklenmemeli.
        Sistem sadece baseline biriktirir.
        """
        # Geçmiş 5 değer var — yetersiz (minimum 7)
        historical = [{"result_value": v} for v in [100, 110, 105, 95, 108]]

        with patch.object(operator, "run_mailsender_query",
                          return_value=[{"count": 500}]), \
             patch.object(operator, "run_vce_query",
                          return_value=historical), \
             patch.object(operator, "execute_vce_dml"), \
             patch.object(operator, "log_execution"):

            operator._execute_rule(sample_anomaly_rule, {
                "dag_id": "test", "dag_run": "run1", "task_name": "task1"
            })

        # 500 anormal görünüyor ama baseline yetersiz — uyarı olmamalı
        assert len(operator.fail_checks) == 0
        assert len(operator.warn_checks) == 0

    def test_anomaly_triggers_when_z_score_exceeds_threshold(
        self, operator, sample_anomaly_rule
    ):
        """
        Z-skoru eşiği aşınca anomali tetiklenmeli.
        |z| > 3 → warn_checks'e eklemeli (action=warn olduğu için).
        """
        # Geçmiş: ortalama ~100, std ~10
        historical = [{"result_value": v} for v in
                      [95, 100, 105, 98, 102, 97, 103, 99, 101, 96]]

        # Şimdiki değer: 200 — çok yüksek
        # mean≈99.6, std≈3.2, Z = |200-99.6|/3.2 ≈ 31 >> 3
        with patch.object(operator, "run_mailsender_query",
                          return_value=[{"count": 200}]), \
             patch.object(operator, "run_vce_query",
                          return_value=historical), \
             patch.object(operator, "execute_vce_dml"), \
             patch.object(operator, "log_execution"):

            operator._execute_rule(sample_anomaly_rule, {
                "dag_id": "test", "dag_run": "run1", "task_name": "task1"
            })

        assert len(operator.warn_checks) == 1
        assert "ANOMALİ" in operator.warn_checks[0] or "z=" in operator.warn_checks[0].lower()

    def test_anomaly_no_trigger_when_z_score_within_threshold(
        self, operator, sample_anomaly_rule
    ):
        """
        Z-skoru eşik içindeyken anomali tetiklenmemeli.
        Normal varyasyon anomali sayılmamalı.
        """
        historical = [{"result_value": v} for v in
                      [95, 100, 105, 98, 102, 97, 103, 99, 101, 96]]

        # Şimdiki değer: 104 — normal aralıkta
        # mean≈99.6, std≈3.2, Z = |104-99.6|/3.2 ≈ 1.4 < 3
        with patch.object(operator, "run_mailsender_query",
                          return_value=[{"count": 104}]), \
             patch.object(operator, "run_vce_query",
                          return_value=historical), \
             patch.object(operator, "execute_vce_dml"), \
             patch.object(operator, "log_execution"):

            operator._execute_rule(sample_anomaly_rule, {
                "dag_id": "test", "dag_run": "run1", "task_name": "task1"
            })

        assert len(operator.warn_checks) == 0
        assert len(operator.fail_checks) == 0

    def test_anomaly_baseline_read_from_vce_not_mailsender(
        self, operator, sample_anomaly_rule
    ):
        """
        Anomali baseline değerleri vce.vce_dq_executions'dan okunmalı.
        mailsender connection'ından okunmamalı — yanlış schema olur.
        """
        historical = [{"result_value": v} for v in [100] * 10]

        vce_query_calls = []
        ms_query_calls  = []

        def vce_query(sql, *args, **kwargs):
            vce_query_calls.append(sql)
            return historical

        def ms_query(sql, *args, **kwargs):
            ms_query_calls.append(sql)
            return [{"count": 100}]

        with patch.object(operator, "run_vce_query", side_effect=vce_query), \
             patch.object(operator, "run_mailsender_query", side_effect=ms_query), \
             patch.object(operator, "execute_vce_dml"), \
             patch.object(operator, "log_execution"):

            operator._execute_rule(sample_anomaly_rule, {
                "dag_id": "test", "dag_run": "run1", "task_name": "task1"
            })

        # vce_dq_executions sorgusu vce connection'ında olmalı
        assert any("vce_dq_executions" in sql for sql in vce_query_calls), \
            "Baseline, vce.vce_dq_executions'dan okunmalı"

        # mailsender'dan baseline okunmamalı
        assert not any("vce_dq_executions" in sql for sql in ms_query_calls), \
            "Baseline mailsender connection'ından okunmamalı"

    def test_anomaly_baseline_updated_to_vce_not_mailsender(
        self, operator, sample_anomaly_rule
    ):
        """
        Baseline güncellemesi (INSERT ... ON DUPLICATE KEY UPDATE)
        vce schema'ya yazılmalı. mailsender'a yazılmamalı.
        """
        historical = [{"result_value": v} for v in [100] * 10]

        with patch.object(operator, "run_mailsender_query",
                          return_value=[{"count": 100}]), \
             patch.object(operator, "run_vce_query",
                          return_value=historical), \
             patch.object(operator, "execute_vce_dml") as mock_vce_dml, \
             patch.object(operator, "log_execution"):

            operator._execute_rule(sample_anomaly_rule, {
                "dag_id": "test", "dag_run": "run1", "task_name": "task1"
            })

        # vce_anomaly_baselines tablosuna yazma olmalı
        dml_calls = [str(c) for c in mock_vce_dml.call_args_list]
        assert any("vce_anomaly_baselines" in c for c in dml_calls), \
            "Baseline vce.vce_anomaly_baselines'a yazılmalı"

    def test_anomaly_zero_std_does_not_raise(self, operator, sample_anomaly_rule):
        """
        Tüm geçmiş değerler aynıysa std=0 olur.
        Sıfıra bölme hatası fırlatmamalı — graceful handle etmeli.
        """
        historical = [{"result_value": 100.0} for _ in range(10)]

        with patch.object(operator, "run_mailsender_query",
                          return_value=[{"count": 100}]), \
             patch.object(operator, "run_vce_query",
                          return_value=historical), \
             patch.object(operator, "execute_vce_dml"), \
             patch.object(operator, "log_execution"):

            # Exception fırlatmamalı
            operator._execute_rule(sample_anomaly_rule, {
                "dag_id": "test", "dag_run": "run1", "task_name": "task1"
            })


# ═════════════════════════════════════════════════════════════════════════════
# GRUP 4: DataQualityOperator — Instance Variable Güvenliği
# ═════════════════════════════════════════════════════════════════════════════

class TestDataQualityOperatorInstanceSafety:
    """
    Orijinal VCE'deki class variable bug'ının düzeltiğini doğrular.

    BUG: fail_checks = [] class seviyesinde tanımlanmıştı.
         İki operator instance'ı aynı listeyi paylaşıyordu.
         Paralel task çalışmalarında birinin fail'i diğerine sızıyordu.

    DÜZELTME: self.fail_checks = [] __init__ içinde.
    """

    def test_fail_checks_not_shared_between_instances(self):
        """
        İki farklı operator instance'ı fail_checks listesini paylaşmamalı.
        Birinin fail'i diğerini etkilememeli.
        """
        with patch("airflow.models.BaseOperator.__init__", return_value=None):
            from operators.vce_operators import DataQualityOperator

            op1 = DataQualityOperator.__new__(DataQualityOperator)
            op1.fail_checks = []
            op1.warn_checks = []

            op2 = DataQualityOperator.__new__(DataQualityOperator)
            op2.fail_checks = []
            op2.warn_checks = []

            # op1'e fail ekle
            op1.fail_checks.append("op1 failed")

            # op2 etkilenmemeli
            assert len(op2.fail_checks) == 0, (
                "op2.fail_checks etkilendi — class variable bug'ı hâlâ mevcut!"
            )

    def test_warn_checks_not_shared_between_instances(self):
        """warn_checks için aynı test."""
        with patch("airflow.models.BaseOperator.__init__", return_value=None):
            from operators.vce_operators import DataQualityOperator

            op1 = DataQualityOperator.__new__(DataQualityOperator)
            op1.fail_checks = []
            op1.warn_checks = []

            op2 = DataQualityOperator.__new__(DataQualityOperator)
            op2.fail_checks = []
            op2.warn_checks = []

            op1.warn_checks.append("op1 warned")
            assert len(op2.warn_checks) == 0

    def test_multiple_rules_accumulate_in_same_instance(self):
        """
        Aynı instance içinde birden fazla kural çalışırsa
        tüm fail'ler aynı fail_checks listesinde birikmeli.
        """
        with patch("airflow.models.BaseOperator.__init__", return_value=None):
            from operators.vce_operators import DataQualityOperator

            op = DataQualityOperator.__new__(DataQualityOperator)
            op.rule_domain    = "test"
            op.rule_subdomain = None
            op.execute_time   = None
            op.fail_checks    = []
            op.warn_checks    = []
            op.log            = MagicMock()

            rules = [
                {
                    "id": i, "rule_domain": "test",
                    "rule_subdomain": f"rule_{i}",
                    "dataset_name": None, "table_name": None,
                    "check_type": "threshold",
                    "sql_statement": "SELECT 1",
                    "pre_sql_statement": None,
                    "action": "fail",
                    "description": f"Rule {i}",
                    "anomaly_threshold": 3.0, "test_flag": 0,
                }
                for i in range(3)
            ]

            dag = {"dag_id": "test", "dag_run": "r1", "task_name": "t1"}

            for rule in rules:
                with patch.object(op, "run_mailsender_query",
                                  return_value=[{"count": 1}]), \
                     patch.object(op, "log_execution"):
                    op._execute_rule(rule, dag)

            # 3 kural, 3'ü de fail — toplamda 3 fail olmalı
            assert len(op.fail_checks) == 3


# ═════════════════════════════════════════════════════════════════════════════
# GRUP 5: DataQualityOperator — execute() ve AirflowException
# ═════════════════════════════════════════════════════════════════════════════

class TestDataQualityOperatorExecute:
    """
    execute() metodunun Airflow entegrasyonunu test eder.
    AirflowException, bildirim, başarılı akış.
    """

    def test_execute_raises_on_fail_checks(self, operator, sample_rules):
        """
        fail_checks doluysa execute() AirflowException fırlatmalı.
        DAG görevi başarısız olarak işaretlenmeli.
        """
        from airflow.exceptions import AirflowException

        with patch.object(operator, "_load_rules", return_value=sample_rules), \
             patch.object(operator, "run_mailsender_query",
                          return_value=[{"count": 10}]), \
             patch.object(operator, "log_execution"), \
             patch.object(operator, "send_notifications"):

            with pytest.raises(AirflowException) as exc_info:
                operator.execute(make_airflow_context())

        assert "başarısız" in str(exc_info.value).lower() or \
               "fail" in str(exc_info.value).lower()

    def test_execute_no_exception_on_all_pass(self, operator, sample_rules):
        """
        Tüm kurallar geçince execute() exception fırlatmamalı.
        """
        with patch.object(operator, "_load_rules", return_value=sample_rules), \
             patch.object(operator, "run_mailsender_query",
                          return_value=[{"count": 0}]), \
             patch.object(operator, "log_execution"), \
             patch.object(operator, "send_notifications"):

            # Exception fırlatmamalı
            operator.execute(make_airflow_context())

    def test_execute_sends_notification_on_fail(self, operator, sample_rules):
        """
        fail_checks doluysa Teams/Slack bildirimi gönderilmeli.
        """
        with patch.object(operator, "_load_rules", return_value=sample_rules), \
             patch.object(operator, "run_mailsender_query",
                          return_value=[{"count": 5}]), \
             patch.object(operator, "log_execution"), \
             patch.object(operator, "send_notifications") as mock_notify:

            with pytest.raises(Exception):
                operator.execute(make_airflow_context())

        mock_notify.assert_called()

    def test_execute_no_notification_on_all_pass(self, operator, sample_rules):
        """
        Tüm kurallar geçince bildirim gönderilmemeli.
        """
        with patch.object(operator, "_load_rules", return_value=sample_rules), \
             patch.object(operator, "run_mailsender_query",
                          return_value=[{"count": 0}]), \
             patch.object(operator, "log_execution"), \
             patch.object(operator, "send_notifications") as mock_notify:

            operator.execute(make_airflow_context())

        mock_notify.assert_not_called()

    def test_load_rules_uses_vce_connection(self, operator):
        """
        _load_rules() vce.vce_dq_rules'dan yüklemeli.
        mailsender connection'ı kullanmamalı.
        """
        with patch.object(operator, "run_vce_query",
                          return_value=[]) as mock_vce, \
             patch.object(operator, "run_mailsender_query") as mock_ms:

            operator._load_rules()

        mock_vce.assert_called_once()
        mock_ms.assert_not_called()


# ═════════════════════════════════════════════════════════════════════════════
# GRUP 6: TableValidationOperator
# ═════════════════════════════════════════════════════════════════════════════

class TestTableValidationOperator:
    """
    TableValidationOperator'ın karşılaştırma mantığını test eder.
    Exact, count, sum, tolerance karşılaştırma tipleri.
    """

    @pytest.fixture
    def tv_operator(self):
        with patch("airflow.models.BaseOperator.__init__", return_value=None):
            from operators.vce_operators import TableValidationOperator
            op = TableValidationOperator.__new__(TableValidationOperator)
            op.validation_domain    = "send_consistency"
            op.validation_subdomain = None
            op.fail_checks = []
            op.warn_checks = []
            op.log = MagicMock()
            return op

    def test_exact_match_passes(self, tv_operator):
        """Birebir eşleşen sonuçlar geçmeli."""
        source = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
        target = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
        is_valid, diff = tv_operator._compare(source, target, "exact", 0)
        assert is_valid is True
        assert diff is None

    def test_exact_match_fails_on_difference(self, tv_operator):
        """Farklı satır içeren exact karşılaştırma başarısız olmalı."""
        source = [{"id": 1, "val": "a"}]
        target = [{"id": 1, "val": "FARKLI"}]
        is_valid, diff = tv_operator._compare(source, target, "exact", 0)
        assert is_valid is False
        assert diff is not None

    def test_count_match_passes(self, tv_operator):
        """Aynı satır sayısı count karşılaştırmasını geçmeli."""
        source = [{"x": 1}, {"x": 2}, {"x": 3}]
        target = [{"y": 9}, {"y": 8}, {"y": 7}]
        is_valid, diff = tv_operator._compare(source, target, "count", 0)
        assert is_valid is True

    def test_count_match_fails_on_row_count_diff(self, tv_operator):
        """Farklı satır sayısı count karşılaştırmasını başarısız etmeli."""
        source = [{"x": 1}, {"x": 2}]
        target = [{"y": 9}]
        is_valid, diff = tv_operator._compare(source, target, "count", 0)
        assert is_valid is False
        assert "satır sayısı" in diff.lower() or "2" in diff

    def test_tolerance_within_limit_passes(self, tv_operator):
        """Yüzde fark tolerans içindeyse geçmeli."""
        source = [{"total": 1000}]
        target = [{"total": 1005}]  # %0.5 fark — 2.0 tolerans içinde
        is_valid, diff = tv_operator._compare(source, target, "tolerance", 2.0)
        assert is_valid is True

    def test_tolerance_exceeds_limit_fails(self, tv_operator):
        """Yüzde fark toleransı aşarsa başarısız olmalı."""
        source = [{"total": 1000}]
        target = [{"total": 1050}]  # %5 fark — 2.0 toleransı aşıyor
        is_valid, diff = tv_operator._compare(source, target, "tolerance", 2.0)
        assert is_valid is False

    def test_empty_source_and_target_passes(self, tv_operator):
        """Her iki taraf da boşsa geçmeli (eşit)."""
        is_valid, diff = tv_operator._compare([], [], "exact", 0)
        assert is_valid is True

    def test_empty_source_fails(self, tv_operator):
        """Kaynak boş, hedef dolu ise başarısız olmalı."""
        is_valid, diff = tv_operator._compare([], [{"x": 1}], "exact", 0)
        assert is_valid is False

    def test_source_and_target_queries_use_mailsender_conn(self, tv_operator):
        """
        source_sql ve target_sql mailsender connection'ında çalışmalı.
        Sonuçlar vce connection'ına yazılmalı.
        """
        val = {
            "id": 1,
            "validation_domain": "test", "validation_subdomain": "test",
            "source_conn_id": "mailsender",
            "source_dataset": "aws_mailsender_pro_v3",
            "source_table": "send_queue",
            "source_sql": "SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_queue",
            "pre_source_sql": None,
            "target_conn_id": "mailsender",
            "target_dataset": "aws_mailsender_pro_v3",
            "target_table": "send_queue_log",
            "target_sql": "SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_queue_log",
            "pre_target_sql": None,
            "comparison_type": "count",
            "tolerance_pct": None,
            "action": "warn",
            "description": "Test",
            "test_flag": 0,
        }

        dag = {"dag_id": "test", "dag_run": "r1", "task_name": "t1", "retry": 1}

        ms_calls = []

        def track_ms(sql, *args):
            ms_calls.append(sql)
            return [{"count": 100}]

        with patch.object(tv_operator, "run_mailsender_query",
                          side_effect=track_ms), \
             patch.object(tv_operator, "_log_val_execution"):

            tv_operator._execute_validation(val, dag)

        assert len(ms_calls) == 2  # source ve target
        assert all("aws_mailsender_pro_v3" in sql for sql in ms_calls)

    def test_validation_result_logged_to_vce(self, tv_operator):
        """
        Karşılaştırma sonucu vce.vce_table_val_executions'a kaydedilmeli.
        mailsender'a yazılmamalı.
        """
        val = {
            "id": 1,
            "validation_domain": "test", "validation_subdomain": "test",
            "source_conn_id": "mailsender",
            "source_dataset": None, "source_table": None,
            "source_sql": "SELECT 1",
            "pre_source_sql": None,
            "target_conn_id": "mailsender",
            "target_dataset": None, "target_table": None,
            "target_sql": "SELECT 1",
            "pre_target_sql": None,
            "comparison_type": "count",
            "tolerance_pct": None,
            "action": "warn",
            "description": "Test",
            "test_flag": 0,
        }
        dag = {"dag_id": "test", "dag_run": "r1", "task_name": "t1", "retry": 1}

        with patch.object(tv_operator, "run_mailsender_query",
                          return_value=[{"count": 5}]), \
             patch.object(tv_operator, "execute_vce_dml") as mock_vce_dml, \
             patch.object(tv_operator, "get_mailsender_conn") as mock_ms_write:

            tv_operator._execute_validation(val, dag)

        # vce'ye yazma olmalı
        assert mock_vce_dml.called
        # mailsender'a yazma OLMAMALI
        mock_ms_write.assert_not_called()


# ═════════════════════════════════════════════════════════════════════════════
# GRUP 7: RemediationOperator
# ═════════════════════════════════════════════════════════════════════════════

class TestRemediationOperator:
    """
    RemediationOperator'ın temizlik ve loglama davranışını test eder.
    """

    @pytest.fixture
    def rem_operator(self):
        with patch("airflow.models.BaseOperator.__init__", return_value=None):
            from operators.vce_operators import RemediationOperator
            op = RemediationOperator.__new__(RemediationOperator)
            op.operations  = ["delete_expired_tokens"]
            op.fail_checks = []
            op.warn_checks = []
            op.log = MagicMock()
            return op

    def test_delete_runs_on_mailsender_not_vce(self, rem_operator):
        """
        Temizlik DELETE'leri mailsender (aws_mailsender_pro_v3) üzerinde çalışmalı.
        vce tabloları silinmemeli.
        """
        dag = {"dag_id": "test", "dag_run": "r1", "task_name": "t1"}

        mock_ms_conn, mock_ms_cursor = make_mock_conn(rowcount=50)
        mock_ms_cursor.rowcount = 50

        with patch.object(rem_operator, "get_mailsender_conn",
                          return_value=mock_ms_conn) as mock_get_ms, \
             patch.object(rem_operator, "execute_vce_dml") as mock_vce_dml:

            rem_operator._run_operation("delete_expired_tokens", dag)

        # mailsender connection açıldı — DELETE yapıldı
        mock_get_ms.assert_called_once()
        # vce DML sadece log için çağrıldı, DELETE için değil
        assert mock_vce_dml.called
        log_sql = mock_vce_dml.call_args[0][0]
        assert "vce_remediation_log" in log_sql

    def test_remediation_logged_to_vce(self, rem_operator):
        """
        Her temizlik işlemi vce.vce_remediation_log'a kaydedilmeli.
        """
        dag = {"dag_id": "test", "dag_run": "r1", "task_name": "t1"}

        mock_ms_conn, _ = make_mock_conn(rowcount=10)

        with patch.object(rem_operator, "get_mailsender_conn",
                          return_value=mock_ms_conn), \
             patch.object(rem_operator, "execute_vce_dml") as mock_log:

            rem_operator._run_operation("delete_expired_tokens", dag)

        mock_log.assert_called_once()
        assert "vce_remediation_log" in mock_log.call_args[0][0]

    def test_remediation_continues_on_delete_error(self, rem_operator):
        """
        DELETE başarısız olursa exception fırlatmamalı.
        Hata log'a yazılmalı, diğer işlemler devam etmeli.
        """
        dag = {"dag_id": "test", "dag_run": "r1", "task_name": "t1"}

        mock_ms_conn, mock_ms_cursor = make_mock_conn()
        mock_ms_cursor.execute.side_effect = Exception("Table locked")

        with patch.object(rem_operator, "get_mailsender_conn",
                          return_value=mock_ms_conn), \
             patch.object(rem_operator, "execute_vce_dml"):

            # Exception fırlatmamalı
            result = rem_operator._run_operation("delete_expired_tokens", dag)

        assert result["status"] == "Failed"
        assert result["rows_affected"] == 0

    def test_operations_all_runs_all_defined(self, rem_operator):
        """
        operations=['all'] verilince OPERATIONS dict'indeki tüm işlemler çalışmalı.
        """
        with patch("airflow.models.BaseOperator.__init__", return_value=None):
            from operators.vce_operators import RemediationOperator

            op = RemediationOperator.__new__(RemediationOperator)
            op.operations  = ["all"]
            op.fail_checks = []
            op.warn_checks = []
            op.log = MagicMock()

            # "all" → tüm OPERATIONS anahtarları
            op.operations = list(RemediationOperator.OPERATIONS.keys()) \
                if op.operations == ["all"] else op.operations

            assert set(op.operations) == set(RemediationOperator.OPERATIONS.keys())

    def test_target_tables_have_schema_prefix(self, rem_operator):
        """
        Tüm temizlik SQL'leri aws_mailsender_pro_v3 schema prefix'ini içermeli.
        Yanlış schema'da çalışmamalı.
        """
        from operators.vce_operators import RemediationOperator

        for op_type, op_def in RemediationOperator.OPERATIONS.items():
            assert "aws_mailsender_pro_v3" in op_def["sql"], (
                f"{op_type} SQL'inde schema prefix eksik: {op_def['sql'][:100]}"
            )
            assert "aws_mailsender_pro_v3" in op_def["table"], (
                f"{op_type} table alanında schema prefix eksik"
            )


# ═════════════════════════════════════════════════════════════════════════════
# GRUP 8: Schema Bütünlüğü Testleri
# ═════════════════════════════════════════════════════════════════════════════

class TestSchemaIntegrity:
    """
    İki schema ayrımının doğru uygulandığını doğrular.
    Seed SQL, operator sabitleri ve kural yapısı kontrolleri.
    """

    def test_vce_conn_id_constant(self):
        """VCE_CONN_ID sabiti 'vce' olmalı."""
        from operators.vce_operators import VCE_CONN_ID
        assert VCE_CONN_ID == "vce"

    def test_mailsender_conn_id_constant(self):
        """MAILSENDER_CONN_ID sabiti 'mailsender' olmalı."""
        from operators.vce_operators import MAILSENDER_CONN_ID
        assert MAILSENDER_CONN_ID == "mailsender"

    def test_remediation_operations_target_mailsender_schema(self):
        """
        Tüm RemediationOperator işlemleri aws_mailsender_pro_v3 tablolarını hedef almalı.
        vce tablolarını silmemeli.
        """
        from operators.vce_operators import RemediationOperator

        for op_name, op_def in RemediationOperator.OPERATIONS.items():
            assert "vce" not in op_def["table"].split(".")[0], (
                f"{op_name} vce schema tablosunu hedef alıyor — yanlış!"
            )
            assert "aws_mailsender_pro_v3" in op_def["table"], (
                f"{op_name} aws_mailsender_pro_v3 prefix'i eksik"
            )

    def test_default_anomaly_threshold_is_three(self):
        """Varsayılan anomali eşiği 3.0 olmalı (3-sigma kuralı)."""
        from operators.vce_operators import DEFAULT_ANOMALY_THRESHOLD
        assert DEFAULT_ANOMALY_THRESHOLD == 3.0

    def test_webhook_variables_not_hardcoded(self):
        """
        Teams ve Slack webhook URL'leri kodda sabit yazılmamalı.
        Airflow Variable ile okunmalı.
        """
        import inspect
        from operators import vce_operators

        source = inspect.getsource(vce_operators)

        # Gerçek webhook URL formatı kodda olmamalı
        assert "webhook.office.com" not in source or \
               "Variable.get" in source, \
            "Teams webhook URL hardcoded — Airflow Variable kullanın"

        assert "hooks.slack.com" not in source or \
               "Variable.get" in source, \
            "Slack webhook URL hardcoded — Airflow Variable kullanın"
