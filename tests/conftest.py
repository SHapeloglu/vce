# conftest.py
# ===========
# pytest konfigürasyonu ve paylaşılan fixture'lar
#
# Bu dosya pytest tarafından otomatik yüklenir.
# tests/ klasöründeki tüm test dosyaları bu fixture'lara erişebilir.

import sys
import os

# operators/ klasörünü Python path'ine ekle
# Böylece testlerde 'from operators.vce_operators import ...' çalışır
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Airflow'u import etmeden önce gerekli env değişkenlerini ayarla
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow_test")
os.environ.setdefault(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "sqlite:////tmp/airflow_test/airflow.db"
)
