# VCE — Validity Control Engine for MailSender Pro

> **Apache Airflow + MySQL tabanlı, kural yönetimi veritabanında olan, üretim seviyesi veri kalitesi sistemi.**

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python)](https://python.org)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.6%2B-017CEE?logo=apacheairflow)](https://airflow.apache.org)
[![MySQL](https://img.shields.io/badge/MySQL-8.0%2B-4479A1?logo=mysql)](https://mysql.com)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

---

## İçindekiler

- [Proje Hakkında](#proje-hakkında)
- [Mimari](#mimari)
- [Özellikler](#özellikler)
- [Klasör Yapısı](#klasör-yapısı)
- [Kurulum](#kurulum)
  - [Ön Gereksinimler](#ön-gereksinimler)
  - [MySQL Kurulumu](#mysql-kurulumu)
  - [Airflow Kurulumu](#airflow-kurulumu)
  - [Airflow Variable Tanımları](#airflow-variable-tanımları)
- [Kullanım](#kullanım)
  - [Yeni Kural Ekleme](#yeni-kural-ekleme)
  - [Kural Tipleri](#kural-tipleri)
  - [Anomali Tespiti](#anomali-tespiti)
  - [DAG'ları Çalıştırma](#dagları-çalıştırma)
- [Dashboard](#dashboard)
- [Operatörler](#operatörler)
  - [DataQualityOperator](#dataqualityoperator)
  - [TableValidationOperator](#tablevalidationoperator)
  - [RemediationOperator](#remediationoperator)
- [Veritabanı Şeması](#veritabanı-şeması)
- [Kural Kataloğu](#kural-kataloğu)
- [Bildirimler](#bildirimler)
- [Orijinal VCE ile Farklar](#orijinal-vce-ile-farklar)
- [Sorun Giderme](#sorun-giderme)
- [Katkıda Bulunma](#katkıda-bulunma)

---

## Proje Hakkında

Bu proje, Cocacolaya yaptığım SAP ERP Source ve Google Big DWH arası verikalitesi projesinde yaptığım çalışmaların birebir aynını örnek olark kedi  **MailSender Pro** e-posta gönderim platformumun MySQL veritabanı için geliştirilmiş tam kapsamlı bir veri kalitesi (Data Quality) sistemidir.

### Temel Felsefe

> **"Kurallar kodda değil, veritabanında yaşar."**

Geleneksel veri kalitesi projelerinde her yeni kontrol için Python kodu yazılır, DAG değiştirilir ve yeniden deploy edilir. Bu yaklaşımda:

- Yeni kural eklemek için tek bir `INSERT` yeterlidir
- Kuralı devre dışı bırakmak için `active_flag = 0` yapılır
- Kim ne zaman ne ekledi — tam audit trail vardır
- Sonuçlar veritabanında kalıcı olarak saklanır ve sorgulanabilir

### Hangi İhtiyaca Cevap Veriyor?

| Problem | Çözüm |
|---------|-------|
| Suppressed adrese gönderim yapılması | Kritik FAIL kuralı, DAG durdurur |
| Gönderim başarısızlık oranının artması | Hem sabit eşik hem anomali tespiti |
| Takılı queue görevleri | Otomatik tespit ve loglama |
| Eski tokenların birikmesi | Nightly remediation DAG'ı |
| Güvenlik sorunu (brute force) | Audit log'dan otomatik tespit |
| "Dün gece ne silindi?" sorusu | `vce_remediation_log` tablosu |

---

## Mimari

```
┌─────────────────────────────────────────────────────────────────┐
│                   MySQL — Kural & Sonuç Tabloları               │
│  vce_dq_rules │ vce_dq_executions │ vce_rule_audit_log         │
│  vce_table_validations │ vce_remediation_log │ vce_anomaly_...  │
└──────────────────────────┬──────────────────────────────────────┘
                           │ kuralları yükle / sonuçları yaz
┌──────────────────────────▼──────────────────────────────────────┐
│              Custom Airflow Operatörleri (BaseOperator)          │
│  DataQualityOperator │ TableValidationOperator │ RemediationOp.  │
└──────────────────────────┬──────────────────────────────────────┘
                           │ task olarak çalışır
┌──────────────────────────▼──────────────────────────────────────┐
│                    Apache Airflow DAG'ları                        │
│  mailsender_vce_main (06:00) │ mailsender_vce_remediation (03:00)│
└──────────────────────────┬──────────────────────────────────────┘
                           │ bildirim / rapor
┌──────────────────────────▼──────────────────────────────────────┐
│            Çıktılar: Teams · Slack · HTML Dashboard · MySQL Log  │
└─────────────────────────────────────────────────────────────────┘
```

### Çalışma Akışı

```
03:00  mailsender_vce_remediation → süresi dolmuş tokenları sil
       her işlem vce_remediation_log'a kaydedilir

06:00  mailsender_vce_main başlar
  │
  ├─ dq_schema (şema kontrolü — zorunlu 16 tablo)
  │
  ├─ [Paralel]
  │   ├─ dq_security          (kullanıcı, brute force, zombie hesap)
  │   ├─ dq_send_log          (failed oranı, spam riski, anomali)
  │   ├─ dq_suppression       (ihlal tespiti, büyüme, domain blacklist)
  │   ├─ dq_queue             (takılı görev, sayı tutarsızlığı)
  │   ├─ dq_verify            (verify job durumu)
  │   ├─ dq_senders           (konfigürasyon kalitesi)
  │   ├─ dq_integrity         (FK bütünlüğü, duplicate)
  │   ├─ dq_freshness_volume  (taze veri, hacim kontrolleri)
  │   └─ tv_send_consistency  (kaynak-hedef karşılaştırma)
  │
  └─ generate_summary → PASS/WARN/FAIL özeti → Teams/Slack
```

---

## Özellikler

### Kural Yönetimi
- ✅ Tüm kurallar `vce_dq_rules` tablosunda — kod değişmeden yeni kural eklenir
- ✅ `active_flag = 0` ile kural devre dışı bırakılır, `non_active_description` ile sebebi yazılır
- ✅ `test_flag = 1` ile kural test modunda çalışır (aksiyon alınmaz, sadece loglanır)
- ✅ `execute_time` ile hangi saatteki DAG çalışmasında aktif olduğu belirlenir
- ✅ `author` ile kimin yazdığı kayıt altında

### Kontrol Tipleri

| Tip | Açıklama |
|-----|----------|
| `threshold` | SQL sonucu > 0 ise ihlal (COUNT döndürmeli) |
| `anomaly` | Z-skoru tabanlı dinamik eşik (30 günlük kayan pencere) |
| `freshness` | Tablonun belirli süre içinde güncellenip güncellenmediği |
| `volume` | Satır sayısının beklenen aralıkta olup olmadığı |
| `schema` | Kolon varlığı / veri tipi kontrolü |
| `duplicate` | Unique ihlali tespiti |
| `custom` | Herhangi bir SQL mantığı |

### Anomali Tespiti
- Geçmiş 30 günlük değerler üzerinden ortalama ve standart sapma hesaplanır
- Z-skoru = `|değer - ortalama| / std`
- `|Z| > 3` → anomali (güven aralığı dışı)
- Yetersiz geçmiş verisi (< 7 gün) varsa baseline biriktirilir, anomali sayılmaz
- Taban değerleri `vce_anomaly_baselines` tablosunda saklanır

### Güvenlik
- SQL injection: tüm sorgularda parametre binding (`%s` placeholder)
- Webhook URL'leri Airflow Variable'da (hardcoded değil)
- `airflow_dq` kullanıcısı sadece `SELECT` ve belirli `DELETE` yetkilerine sahip

### Remediation (Otomatik Temizlik)

Her gece 03:00'da çalışır ve şunları temizler:

| İşlem | Hedef | Kriter |
|-------|-------|--------|
| `delete_expired_tokens` | `password_reset_tokens` | Süresi dolmuş veya kullanılmış, 7+ gün eski |
| `delete_expired_unsub` | `unsubscribe_tokens` | Kullanılmamış, 30+ gün önce süresi dolmuş |
| `delete_old_rate_logs` | `rate_limit_log` | 7+ günlük kayıtlar |
| `delete_old_ses_notif` | `ses_notifications` | 90+ günlük bildirimler |

Her temizlik işlemi `vce_remediation_log` tablosuna kaydedilir.

### Audit Trail
- Kural değişiklikleri `vce_rule_audit_log` tablosuna yazılır
- Kim, ne zaman, ne değiştirdi, neden — tam iz
- `old_sql` / `new_sql` karşılaştırması mevcut

### Bildirimler
- Microsoft Teams webhook (fail + warn için)
- Slack webhook (opsiyonel)
- Airflow Variable üzerinden yapılandırılır

---

## Klasör Yapısı

```
vce_mailsender/
├── README.md                          # Bu dosya
│
├── sql/
│   ├── 01_vce_schema.sql              # VCE tablolarını oluşturur (7 tablo)
│   └── 02_vce_seed_rules.sql          # Tüm MailSender kurallarını ekler (30+ kural)
│
├── operators/
│   └── vce_operators.py               # Custom Airflow operatörleri
│       ├── VCEBaseOperator            # Ortak metodlar (bağlantı, bildirim, kayıt)
│       ├── DataQualityOperator        # SQL kural çalıştırıcı
│       ├── TableValidationOperator    # Kaynak-hedef karşılaştırma
│       └── RemediationOperator        # Otomatik temizlik
│
├── dags/
│   ├── mailsender_vce_main.py         # Ana denetim DAG'ı (her gün 06:00)
│   └── mailsender_vce_remediation.py  # Temizlik DAG'ı (her gün 03:00)
│
└── dashboard/
    └── vce_dashboard.html             # Tek sayfalık HTML dashboard
```

---

## Kurulum

### Ön Gereksinimler

| Bileşen | Sürüm | Not |
|---------|-------|-----|
| Python | 3.9+ | |
| Apache Airflow | 2.6+ | Docker önerilir |
| MySQL | 8.0+ | Yerel kurulum |
| PyMySQL | 1.0+ | `pip install pymysql` |

### MySQL Kurulumu

#### 1. Airflow kullanıcısı oluştur

```sql
-- Yalnızca okuma + belirli DELETE yetkileri
CREATE USER 'airflow_dq'@'%' IDENTIFIED BY 'GUCLU_BIR_SIFRE';

-- MailSender tablolarını okuma yetkisi
GRANT SELECT ON mailsender.* TO 'airflow_dq'@'%';

-- VCE tablolarına yazma yetkisi (execution kayıtları için)
GRANT INSERT ON mailsender.vce_dq_executions        TO 'airflow_dq'@'%';
GRANT INSERT ON mailsender.vce_table_val_executions  TO 'airflow_dq'@'%';
GRANT INSERT, UPDATE ON mailsender.vce_anomaly_baselines TO 'airflow_dq'@'%';
GRANT INSERT ON mailsender.vce_remediation_log       TO 'airflow_dq'@'%';
GRANT INSERT ON mailsender.vce_rule_audit_log        TO 'airflow_dq'@'%';

-- Remediation için DELETE yetkileri (sadece belirli tablolar)
GRANT DELETE ON mailsender.password_reset_tokens TO 'airflow_dq'@'%';
GRANT DELETE ON mailsender.unsubscribe_tokens    TO 'airflow_dq'@'%';
GRANT DELETE ON mailsender.rate_limit_log        TO 'airflow_dq'@'%';
GRANT DELETE ON mailsender.ses_notifications     TO 'airflow_dq'@'%';

FLUSH PRIVILEGES;
```

#### 2. VCE şemasını oluştur

```bash
mysql -u root -p mailsender < sql/01_vce_schema.sql
```

Bu komut şu 7 tabloyu oluşturur:

| Tablo | Açıklama |
|-------|----------|
| `vce_dq_rules` | Kural tanımları (tek gerçek kaynağı) |
| `vce_dq_executions` | Her kural çalışmasının sonucu |
| `vce_table_validations` | Kaynak-hedef karşılaştırma tanımları |
| `vce_table_val_executions` | Karşılaştırma sonuçları |
| `vce_rule_audit_log` | Kural değişiklik geçmişi |
| `vce_remediation_log` | Otomatik temizlik kayıtları |
| `vce_anomaly_baselines` | Anomali tespiti istatistikleri |

#### 3. Kuralları yükle

```bash
mysql -u root -p mailsender < sql/02_vce_seed_rules.sql
```

30'dan fazla hazır kural yüklenir. Yüklenen kuralları doğrulamak için:

```sql
SELECT rule_domain, rule_subdomain, check_type, action, active_flag
FROM vce_dq_rules
ORDER BY rule_domain, rule_subdomain;
```

#### 4. MySQL bind-address kontrolü (Docker için)

Airflow Docker container'ının host MySQL'e erişebilmesi için:

```ini
# /etc/mysql/mysql.conf.d/mysqld.cnf
[mysqld]
bind-address = 0.0.0.0
```

```bash
sudo systemctl restart mysql
```

### Airflow Kurulumu

#### 1. docker-compose.yml güncelle

Docker içindeki Airflow'un host MySQL'e erişmesi için `extra_hosts` ekle:

```yaml
services:
  airflow-webserver:
    extra_hosts:
      - "host.docker.internal:host-gateway"
    environment:
      _PIP_ADDITIONAL_REQUIREMENTS: "pymysql"

  airflow-scheduler:
    extra_hosts:
      - "host.docker.internal:host-gateway"
    environment:
      _PIP_ADDITIONAL_REQUIREMENTS: "pymysql"

  airflow-worker:          # varsa
    extra_hosts:
      - "host.docker.internal:host-gateway"
    environment:
      _PIP_ADDITIONAL_REQUIREMENTS: "pymysql"
```

> **Not:** Linux'ta `host-gateway` Docker'ın host IP'sini otomatik çözer. Mac/Windows'ta `host.docker.internal` zaten varsayılan tanımlıdır.

#### 2. DAG dosyalarını kopyala

```bash
# operators klasörünü dags altına taşı
cp -r operators/ /path/to/airflow/dags/operators/

# DAG dosyalarını kopyala
cp dags/mailsender_vce_main.py         /path/to/airflow/dags/
cp dags/mailsender_vce_remediation.py  /path/to/airflow/dags/
```

#### 3. Airflow Connection ekle

**Airflow UI → Admin → Connections → +**

| Alan | Değer |
|------|-------|
| Conn Id | `mailsender_mysql` |
| Conn Type | `Generic` |
| Host | `host.docker.internal` |
| Schema | `mailsender` |
| Login | `airflow_dq` |
| Password | (oluşturduğunuz şifre) |
| Port | `3306` |

### Airflow Variable Tanımları

**Airflow UI → Admin → Variables → +**

| Key | Value | Zorunlu |
|-----|-------|---------|
| `VCE_TEAMS_WEBHOOK_URL` | Teams Incoming Webhook URL | Opsiyonel |
| `VCE_SLACK_WEBHOOK_URL` | Slack Incoming Webhook URL | Opsiyonel |

Teams webhook URL almak için:
**Teams → Kanal → Bağlayıcılar → Incoming Webhook → Yapılandır**

---

## Kullanım

### Yeni Kural Ekleme

Kod değişikliği gerekmez. Doğrudan tabloya INSERT yeterlidir:

```sql
INSERT INTO vce_dq_rules (
    rule_domain,
    rule_subdomain,
    dataset_name,
    table_name,
    check_type,
    sql_statement,
    action,
    description,
    execute_time,
    active_flag,
    author
) VALUES (
    'send_log',                           -- domain grubu
    'my_custom_check',                    -- alt kategori
    'mailsender',                         -- veritabanı
    'send_log',                           -- tablo
    'threshold',                          -- kontrol tipi
    -- SQL: sonucu > 0 ise ihlal
    'SELECT COUNT(*) FROM send_log
     WHERE status = ''failed''
       AND error_msg LIKE ''%timeout%''
       AND sent_at >= NOW() - INTERVAL 1 HOUR',
    'warn',                               -- fail veya warn
    'Son 1 saatte timeout nedeniyle başarısız olan gönderim sayısını kontrol eder.
     Yüksek timeout sayısı SMTP sunucusu sorununu işaret edebilir.',
    '06:00',                              -- hangi DAG çalışmasında aktif
    1,                                    -- aktif
    'senin_adin'
);
```

### Kural Tipleri

#### threshold (en yaygın)

SQL sonucu `> 0` ise ihlal. SQL her zaman sayısal bir değer döndürmelidir.

```sql
-- ✅ Doğru: COUNT döndürüyor
SELECT COUNT(*) FROM send_log WHERE recipient IS NULL

-- ✅ Doğru: CASE ile kontrol
SELECT CASE WHEN COUNT(*) > 100 THEN COUNT(*) - 100 ELSE 0 END
FROM suppression_list WHERE DATE(created_at) = CURDATE()

-- ❌ Yanlış: Çok satır döndürüyor
SELECT * FROM send_log WHERE status = 'failed'
```

#### anomaly (dinamik eşik)

Geçmiş 30 günlük değerlere göre istatistiksel sapma tespiti.
SQL **tek bir sayısal değer** döndürmeli:

```sql
-- Bugünkü failed sayısı — geçmiş 30 güne göre anormal mı?
SELECT COUNT(*)
FROM send_log
WHERE status = 'failed'
  AND DATE(sent_at) = CURDATE()
```

`anomaly_threshold` alanı ile hassasiyet ayarlanabilir (varsayılan: 3.0).
`2.0` → daha hassas, `4.0` → daha az hassas.

#### freshness

Tablonun belirli sürede güncellenip güncellenmediğini kontrol eder:

```sql
-- send_log'a son 24 saatte kayıt eklendiyse 0, eklenmediyse 1 döner
SELECT CASE
    WHEN MAX(sent_at) < NOW() - INTERVAL 24 HOUR OR MAX(sent_at) IS NULL
    THEN 1 ELSE 0
END FROM send_log
```

#### pre_sql_statement kullanımı

Asıl kontrolden önce hazırlık işlemi gerekiyorsa:

```sql
-- pre_sql: geçici tablo oluştur
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_today_stats AS
SELECT sender_id, COUNT(*) as total, SUM(status='failed') as fails
FROM send_log
WHERE DATE(sent_at) = CURDATE()
GROUP BY sender_id;

-- sql: geçici tablodan kontrol
SELECT COUNT(*) FROM tmp_today_stats
WHERE fails / NULLIF(total, 0) > 0.5 AND total >= 10
```

### Anomali Tespiti

Anomali sistemi şu şekilde çalışır:

```
1. DAG çalışır → SQL çalıştırılır → sonuç alınır (ör: 145 failed)

2. vce_dq_executions'dan son 30 günün değerleri çekilir
   [120, 98, 134, 87, 156, 112, 103, ...]

3. İstatistik hesaplanır:
   mean = 113.2, std = 21.4

4. Z-skoru: |145 - 113.2| / 21.4 = 1.49

5. |1.49| < 3.0 (eşik) → Normal ✅

6. Eğer bugün 280 failed olsaydı:
   |280 - 113.2| / 21.4 = 7.79 → ANOMALİ ⚠️
```

**Yeterli geçmiş yoksa (< 7 gün):** Sistem bu durumu fark eder,
sadece veriyi biriktirir ve anomali tetiklemez.

### DAG'ları Çalıştırma

**Otomatik:** DAG'lar schedule'a göre çalışır (03:00 ve 06:00 UTC).

**Manuel tetikleme:**
```bash
# Airflow CLI
airflow dags trigger mailsender_vce_main
airflow dags trigger mailsender_vce_remediation

# Belirli bir domain için
airflow dags trigger mailsender_vce_main \
  --conf '{"domain": "suppression"}'
```

**Airflow UI'dan:** DAG sayfasında ▶ butonu.

**Sonuçları sorgulama:**

```sql
-- Bugünkü kontrol özeti
SELECT rule_domain, rule_subdomain, result_status, result_value
FROM vce_dq_executions
WHERE DATE(run_date) = CURDATE()
ORDER BY result_status DESC, rule_domain;

-- Son 7 günlük trend
SELECT DATE(run_date) as gun,
       SUM(result_status='Passed') as gecti,
       SUM(result_status='Failed') as basarisiz
FROM vce_dq_executions
WHERE run_date >= NOW() - INTERVAL 7 DAY
GROUP BY DATE(run_date)
ORDER BY gun;

-- Anomali geçmişi
SELECT rule_domain, rule_subdomain, result_value,
       baseline_mean, z_score, run_date
FROM vce_dq_executions
WHERE check_type = 'anomaly' AND z_score IS NOT NULL
ORDER BY ABS(z_score) DESC
LIMIT 20;
```

---

## Dashboard

`dashboard/vce_dashboard.html` dosyası tarayıcıda doğrudan açılabilir.

### Özellikler

- **Genel Bakış:** Son 24h PASS/WARN/FAIL sayıları, domain tile'ları
- **Trend Grafiği:** Son 7 günlük durum değişimi (Chart.js)
- **Detay Loglar:** Domain ve durum filtresi ile tüm kontrol sonuçları
- **Anomali Tablosu:** Z-skoru geçmişi
- **Temizlik Logları:** Remediation geçmişi
- **JSON Export:** Raporu JSON olarak indir

### Üretim Entegrasyonu

Dashboard şu an demo veri veya manuel JSON ile çalışır.
Gerçek verilerle beslemek için küçük bir API servisi gerekir:

```python
# Örnek: Flask ile basit API endpoint
from flask import Flask, jsonify
import pymysql

app = Flask(__name__)

@app.route('/api/vce/executions')
def get_executions():
    conn = pymysql.connect(...)
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("""
            SELECT rule_domain, rule_subdomain, check_type, action,
                   result_value, result_status, z_score, baseline_mean, run_date
            FROM vce_dq_executions
            WHERE run_date >= NOW() - INTERVAL 7 DAY
            ORDER BY run_date DESC
        """)
        return jsonify({"executions": cur.fetchall()})
```

---

## Operatörler

### DataQualityOperator

SQL tabanlı kuralları `vce_dq_rules` tablosundan yükler ve çalıştırır.

```python
from operators.vce_operators import DataQualityOperator

# Belirli bir domain
task = DataQualityOperator(
    task_id="check_send_log",
    rule_domain="send_log",        # Zorunlu: hangi domain
    rule_subdomain=None,           # Opsiyonel: belirli bir subdomain
    execute_time="06:00",          # Opsiyonel: hangi saatteki kurallar
)

# Belirli bir kural
task = DataQualityOperator(
    task_id="check_failed_ratio",
    rule_domain="send_log",
    rule_subdomain="failed_ratio_threshold",
)
```

**Parametre Referansı:**

| Parametre | Tip | Varsayılan | Açıklama |
|-----------|-----|-----------|----------|
| `rule_domain` | str | Zorunlu | Kural grubu (ör: `send_log`) |
| `rule_subdomain` | str | None | Alt grup filtresi |
| `execute_time` | str | None | Saat filtresi (ör: `06:00`) |

**Çalışma mantığı:**

1. `vce_dq_rules`'dan aktif kuralları yükler
2. Her kural için `pre_sql` varsa çalıştırır
3. `sql_statement` çalıştırır
4. `check_type='anomaly'` ise Z-skoru hesaplar
5. İhlal varsa `action` değerine göre `fail_checks` veya `warn_checks`'e ekler
6. `test_flag=1` ise aksiyon almaz, sadece loglar
7. Sonucu `vce_dq_executions`'a kaydeder
8. `fail_checks` doluysa `AirflowException` fırlatır

### TableValidationOperator

Kaynak ve hedef sorguların sonuçlarını karşılaştırır.

```python
from operators.vce_operators import TableValidationOperator

task = TableValidationOperator(
    task_id="validate_send_consistency",
    validation_domain="send_consistency",
    validation_subdomain=None,   # Opsiyonel
)
```

**Karşılaştırma Tipleri:**

| Tip | Açıklama | Kullanım |
|-----|----------|----------|
| `exact` | Satırlar birebir eşleşmeli | Küçük referans tablolar |
| `count` | Satır sayıları eşit olmalı | Büyük tablolar, kaba kontrol |
| `sum` | Sayısal toplamlar eşit olmalı | Finansal/metrik kontroller |
| `tolerance` | Yüzde fark izin verilen aralıkta olmalı | Yaklaşık eşleşme |

**Örnek kural tanımı (`vce_table_validations`):**

```sql
INSERT INTO vce_table_validations (
    validation_domain, validation_subdomain,
    source_conn_id, source_dataset, source_table,
    source_sql,
    target_conn_id, target_dataset, target_table,
    target_sql,
    comparison_type, tolerance_pct,
    action, description, active_flag, author
) VALUES (
    'send_consistency', 'queue_vs_log',
    'mailsender_mysql', 'mailsender', 'send_queue',
    'SELECT sent_count FROM send_queue WHERE status = ''done'' ORDER BY id',
    'mailsender_mysql', 'mailsender', 'send_queue_log',
    'SELECT queue_id, COUNT(*) FROM send_queue_log
     WHERE status=''sent'' GROUP BY queue_id ORDER BY queue_id',
    'tolerance', 2.0,
    'warn',
    'Tamamlanmış queue görevlerindeki sent_count ile send_queue_log kayıt sayısının
     %2 toleransla eşit olup olmadığını kontrol eder.',
    1, 'data-team'
);
```

### RemediationOperator

Güvenli DELETE işlemleri yapar ve her işlemi loglar.

```python
from operators.vce_operators import RemediationOperator

# Tüm tanımlı işlemleri çalıştır
task = RemediationOperator(
    task_id="run_all",
    operations=["all"],
)

# Belirli işlemleri çalıştır
task = RemediationOperator(
    task_id="cleanup_tokens",
    operations=[
        "delete_expired_tokens",
        "delete_expired_unsub",
    ],
)
```

**Yeni temizlik işlemi eklemek:**

`vce_operators.py` içindeki `OPERATIONS` dict'ine ekle:

```python
OPERATIONS = {
    # ... mevcut işlemler ...
    "delete_old_audit_logs": {
        "sql": "DELETE FROM audit_log WHERE created_at < NOW() - INTERVAL 180 DAY",
        "table": "audit_log",
        "description": "180+ günlük audit logları temizlendi.",
    },
}
```

---

## Veritabanı Şeması

### vce_dq_rules (Ana kural tablosu)

```
id                  INT PK AUTO_INCREMENT
rule_domain         VARCHAR(100) NOT NULL      -- Gruplama: send_log, security...
rule_subdomain      VARCHAR(100) NOT NULL      -- Alt grup: failed_ratio, null_check...
dataset_name        VARCHAR(100)               -- Bilgi amaçlı
table_name          VARCHAR(200)               -- Bilgi amaçlı
check_type          ENUM(threshold|anomaly|freshness|volume|schema|duplicate|custom)
sql_statement       TEXT NOT NULL              -- Çalışacak SQL (COUNT döndürmeli)
pre_sql_statement   TEXT                       -- Hazırlık SQL (opsiyonel)
action              ENUM(fail|warn)
description         TEXT NOT NULL              -- Kapsamlı açıklama
anomaly_threshold   DECIMAL(10,4)              -- Z-skoru eşiği (varsayılan: 3.0)
execute_time        VARCHAR(10)                -- Saat filtresi: '06:00'
active_flag         TINYINT(1) DEFAULT 1       -- 0: devre dışı
author              VARCHAR(100)
test_flag           TINYINT(1) DEFAULT 0       -- 1: aksiyon alma, sadece logla
non_active_description TEXT                   -- Neden kapatıldı?
insert_timestamp    DATETIME
update_timestamp    DATETIME
```

### vce_dq_executions (Sonuç tablosu)

```
id                  BIGINT PK
rule_id             INT                        -- vce_dq_rules.id
rule_domain         VARCHAR(100)
rule_subdomain      VARCHAR(100)
dag_id              VARCHAR(200)
dag_run             VARCHAR(500)
sql_statement       TEXT                       -- Çalışan SQL snapshot
action              VARCHAR(10)
result_value        DECIMAL(20,4)              -- SQL sonucu
result_status       ENUM(Passed|Failed|Skipped|Error)
error_detail        TEXT
baseline_mean       DECIMAL(20,4)              -- Anomali: geçmiş ortalama
baseline_std        DECIMAL(20,4)              -- Anomali: standart sapma
z_score             DECIMAL(10,4)              -- Anomali: hesaplanan Z
run_date            DATETIME
```

### vce_rule_audit_log (Değişiklik geçmişi)

```
id              BIGINT PK
rule_id         INT
change_type     ENUM(INSERT|UPDATE|DELETE|ACTIVATE|DEACTIVATE)
old_sql         TEXT                           -- Önceki SQL
new_sql         TEXT                           -- Sonraki SQL
old_action      VARCHAR(10)
new_action      VARCHAR(10)
changed_by      VARCHAR(100)
change_reason   TEXT
changed_at      DATETIME
```

### vce_remediation_log (Temizlik kayıtları)

```
id              BIGINT PK
dag_id          VARCHAR(200)
operation_type  ENUM(delete_expired_tokens|delete_old_rate_logs|...)
target_table    VARCHAR(200)
sql_executed    TEXT
rows_affected   INT
result_status   ENUM(Success|Failed|Warning)
result_detail   TEXT
executed_at     DATETIME
```

---

## Kural Kataloğu

Seed dosyasıyla yüklenen 30+ hazır kural:

### Güvenlik (4 kural)

| Kural | Tip | Aksiyon | Açıklama |
|-------|-----|---------|----------|
| `no_active_admin` | threshold | fail | Aktif admin kullanıcısı yok |
| `brute_force_detection` | threshold | warn | 1 saatte 50+ başarısız login |
| `orphan_reset_tokens` | threshold | warn | 1000+ süresi dolmuş token |
| `inactive_users_with_access` | threshold | warn | 90+ gün login olmamış aktif hesaplar |

### Gönderim Logu (6 kural)

| Kural | Tip | Aksiyon | Açıklama |
|-------|-----|---------|----------|
| `failed_ratio_threshold` | threshold | fail | Failed oranı > %30 |
| `failed_ratio_anomaly` | anomaly | warn | Failed sayısı istatistiksel anomali |
| `null_critical_fields` | threshold | fail | recipient/status/sender_id NULL |
| `spam_risk_same_recipient` | threshold | warn | 1 saatte aynı adrese 10+ gönderim |
| `daily_volume` | anomaly | warn | Günlük hacim anormal |
| `freshness` | freshness | warn | 24 saattir kayıt yok |

### Suppression (4 kural)

| Kural | Tip | Aksiyon | Açıklama |
|-------|-----|---------|----------|
| `violation_critical` | threshold | **fail** | Suppressed adrese gönderim yapılmış |
| `daily_growth_anomaly` | anomaly | warn | Günlük suppression artışı anormal |
| `unsubscribe_integrity` | threshold | warn | Unsubscribe token var ama suppression yok |
| `domain_blacklist_check` | threshold | **fail** | Blacklist domain'e gönderim |

### Queue (5 kural)

| Kural | Tip | Aksiyon | Açıklama |
|-------|-----|---------|----------|
| `stuck_running` | threshold | warn | 6+ saattir takılı görev |
| `count_inconsistency` | threshold | fail | total ≠ sent + failed + skipped |
| `overdue_pending` | threshold | warn | 30+ dk geçmiş pending görev |
| `ab_ratio_invalid` | threshold | fail | A/B ratio 0-100 dışı |
| `log_count_mismatch` | threshold | warn | sent_count ile log sayısı uyuşmuyor |

### Sender (5 kural)

| Kural | Tip | Aksiyon | Açıklama |
|-------|-----|---------|----------|
| `no_active_sender` | threshold | fail | Aktif gönderici yok |
| `smtp_incomplete_config` | threshold | warn | SMTP konfigürasyonu eksik |
| `ses_incomplete_config` | threshold | warn | SES AWS key eksik |
| `warmup_without_daily_limit` | threshold | warn | Warmup açık ama limit=0 |
| `high_failure_rate` | threshold | warn | Sender failed oranı > %50 |

---

## Bildirimler

### Teams Bildirimi Yapılandırma

1. Teams kanalında: **Kanallar → ⋯ → Bağlayıcılar**
2. **Incoming Webhook → Yapılandır**
3. İsim ver, **Oluştur** tıkla
4. URL'yi kopyala

```
Airflow UI → Admin → Variables → +
Key   : VCE_TEAMS_WEBHOOK_URL
Value : https://xxxx.webhook.office.com/webhookb2/...
```

### Slack Bildirimi Yapılandırma

1. [Slack App oluştur](https://api.slack.com/apps) → **Incoming Webhooks**
2. Kanalı seç, URL'yi al

```
Airflow UI → Admin → Variables → +
Key   : VCE_SLACK_WEBHOOK_URL
Value : https://hooks.slack.com/services/...
```

### Bildirim Formatı

Teams bildirimi şu bilgileri içerir:
- DAG adı ve çalışma zamanı
- Başarısız/uyarı veren kontrollerin listesi (max 20)
- İhlal açıklaması (kural description alanından)

---

## Orijinal VCE ile Farklar

Bu proje, Coca-Cola AMATIL ortamı için geliştirilen orijinal VCE projesinden türetilmiştir. Temel mimari (kural tabanlı DB yaklaşımı, custom BaseOperator, execution kayıtları) korunmuş; aşağıdaki iyileştirmeler yapılmıştır:

### Düzeltilen Bug'lar

| Bug | Orijinal | Düzeltilmiş |
|-----|----------|-------------|
| **Class variable** | `fail_checks = []` sınıf seviyesinde → paralel task'larda paylaşılıyor | `self.fail_checks = []` instance seviyesinde |
| **SQL Injection** | `f"...WHERE domain = '{self.domain}'"` | `cur.execute(sql, (self.domain,))` parametre binding |
| **Hardcoded URL** | Webhook URL direkt kod içinde | Airflow Variable'dan okunur |
| **Eksik raise** | `vce_monitoring.py`'de fail_checks dolunca raise yok | `AirflowException` eklendi |
| **Bağlantı sızıntısı** | Bazı metodlarda finally bloğu yok | Tüm bağlantılar `finally` ile kapatılır |

### Yeni Eklenen Özellikler

- ✅ **Anomali tespiti** (Z-skoru, 30 günlük kayan pencere)
- ✅ **Audit log** (kural değişiklik geçmişi)
- ✅ **Remediation log** (temizlik kayıtları)
- ✅ **Slack bildirimi** (Teams'e ek olarak)
- ✅ **HTML Dashboard** (grafik, filtre, export)
- ✅ **Freshness/Volume/Duplicate/Schema** kural tipleri
- ✅ **tolerance** karşılaştırma tipi (TableValidation)
- ✅ **test_flag** desteği (kural test modu)
- ✅ **pre_sql_statement** (hem DataQuality hem TableValidation)

### Değişmeyen Yapılar

- ✅ `BaseOperator`'dan türetme yaklaşımı
- ✅ `vce_dq_rules` tablosu konsepti
- ✅ `rule_domain` / `rule_subdomain` gruplama
- ✅ `fail` / `warn` aksiyon ayrımı
- ✅ Execution sonuçlarını tabloya yazma
- ✅ Teams bildirimi

---

## Sorun Giderme

### Bağlantı Hatası

```python
# Docker içinden test:
docker exec -it <airflow-scheduler> python3 -c "
import pymysql
c = pymysql.connect(
    host='host.docker.internal',
    port=3306,
    user='airflow_dq',
    password='SIFRE',
    database='mailsender'
)
print('OK:', c.server_version)
c.close()
"
```

Hata alıyorsanız kontrol edin:
- MySQL `bind-address = 0.0.0.0` mi?
- `airflow_dq` kullanıcısı `%` host ile mi oluşturuldu?
- Docker `extra_hosts` tanımlı mı?

### Kural Çalışmıyor

```sql
-- Aktif mi?
SELECT active_flag, execute_time FROM vce_dq_rules
WHERE rule_domain = 'send_log' AND rule_subdomain = 'failed_ratio_threshold';

-- Doğru execute_time mi?
-- DAG'da execute_time='06:00' verildiyse, kural da '06:00' olmalı
```

### Anomali Hiç Tetiklenmiyor

```sql
-- Geçmiş veri yeterli mi? (7+ gün gerekli)
SELECT COUNT(*), MIN(run_date), MAX(run_date)
FROM vce_dq_executions
WHERE rule_id = (
    SELECT id FROM vce_dq_rules
    WHERE rule_domain = 'send_log' AND rule_subdomain = 'failed_ratio_anomaly'
)
AND result_status = 'Passed';
```

7'den az kayıt varsa sistem baseline biriktiriyor, anomali tetiklemiyor.

### Teams Bildirimi Gitmiyor

```sql
-- Airflow Variable mevcut mu?
-- Airflow UI → Admin → Variables → VCE_TEAMS_WEBHOOK_URL
```

Airflow CLI ile kontrol:
```bash
airflow variables get VCE_TEAMS_WEBHOOK_URL
```

---

## Katkıda Bulunma

### Yeni Kural Önerisi

En iyi yol doğrudan `02_vce_seed_rules.sql` dosyasına INSERT eklemek:

1. Kural domain ve subdomain belirle
2. SQL'i yaz ve manuel test et: sonucu > 0 mu?
3. Açıklama yaz (en az 2 cümle, neden var, ne kontrol ediyor)
4. Pull request aç

### Yeni Operatör

`VCEBaseOperator`'dan türet ve şunları implement et:
- `execute(self, context)` metodu
- `fail_checks` ve `warn_checks` instance variable olarak başlat
- Tüm DB bağlantılarını `finally` ile kapat
- Bildirimler için `self.send_notifications()` kullan

### Kod Standardı

- Tüm SQL'lerde parametre binding (`%s`)
- Her method için docstring
- Türkçe log mesajları (bu proje Türkçe ekip için)
- Kritik SQL'ler için inline yorum

---

## Lisans

MIT License — ayrıntılar için [LICENSE](LICENSE) dosyasına bakın.

---

<div align="center">
  <sub>MailSender Pro VCE · Apache Airflow · MySQL · Python</sub>
</div>
