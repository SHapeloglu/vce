# VCE — Validity Control Engine for MailSender Pro
#Orjinal halini cocacolada yaptım fakat eksikleri vardı bu halinde tüm eksikler giderildi, sayfanın altında yenilikleri görebilirisiniz.
> **Apache Airflow + MySQL tabanlı, kural yönetimi veritabanında olan, üretim seviyesi veri kalitesi sistemi.**

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python)](https://python.org)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.6%2B-017CEE?logo=apacheairflow)](https://airflow.apache.org)
[![MySQL](https://img.shields.io/badge/MySQL-8.0%2B-4479A1?logo=mysql)](https://mysql.com)
[![Tests](https://img.shields.io/badge/Tests-51%20unit-success)](tests/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

---

## İçindekiler

- [Proje Hakkında](#proje-hakkında)
- [Hızlı Başlangıç — Çalıştırma Sırası](#hızlı-başlangıç--çalıştırma-sırası)
- [Schema Mimarisi](#schema-mimarisi)
- [Sistem Mimarisi](#sistem-mimarisi)
- [Özellikler](#özellikler)
- [Klasör Yapısı](#klasör-yapısı)
- [Kurulum](#kurulum)
  - [Ön Gereksinimler](#ön-gereksinimler)
  - [MySQL Kurulumu](#mysql-kurulumu)
  - [Airflow Kurulumu](#airflow-kurulumu)
  - [Airflow Connection Tanımları](#airflow-connection-tanımları)
  - [Airflow Variable Tanımları](#airflow-variable-tanımları)
- [Kullanım](#kullanım)
  - [Yeni Kural Ekleme](#yeni-kural-ekleme)
  - [Kural Tipleri](#kural-tipleri)
  - [Anomali Tespiti](#anomali-tespiti)
  - [Kural SQL Test Aracı](#kural-sql-test-aracı)
  - [DAG'ları Çalıştırma](#dagları-çalıştırma)
- [Dashboard](#dashboard)
- [Operatörler](#operatörler)
- [Veritabanı Şeması](#veritabanı-şeması)
- [Partition Yönetimi](#partition-yönetimi)
- [Kural Kataloğu](#kural-kataloğu)
- [Bildirimler](#bildirimler)
- [Test Altyapısı](#test-altyapısı)
- [Orijinal VCE ile Farklar](#orijinal-vce-ile-farklar)
- [Sorun Giderme](#sorun-giderme)
- [Katkıda Bulunma](#katkıda-bulunma)

---

## Proje Hakkında

Bu proje, **MailSender Pro** e-posta gönderim platformunun MySQL veritabanı için geliştirilmiş tam kapsamlı bir veri kalitesi sistemidir. Coca-Cola AMATIL bünyesinde geliştirilen **Validity Control Engine (VCE)** mimarisi esas alınarak MySQL ortamı için yeniden tasarlanmıştır.

### Temel Felsefe

> **"Kurallar kodda değil, veritabanında yaşar."**

Geleneksel veri kalitesi projelerinde her yeni kontrol için Python kodu yazılır, DAG değiştirilir ve yeniden deploy edilir. VCE yaklaşımında:

- Yeni kural eklemek için tek bir `INSERT` yeterlidir
- Kuralı devre dışı bırakmak için `active_flag = 0` yapılır
- Her kural değişikliği MySQL trigger ile otomatik audit log'a yazılır
- Sonuçlar aylık partitioned tablolarda kalıcı olarak saklanır

### Hangi İhtiyaca Cevap Veriyor?

| Problem | Çözüm |
|---------|-------|
| Suppressed adrese gönderim | Kritik FAIL kuralı — DAG durdurur, bildirim gider |
| Başarısızlık oranı artışı | Sabit eşik + Z-skoru tabanlı anomali tespiti |
| Takılı queue görevleri | 6 saatlik inaktiflik tespiti ve loglama |
| Eski tokenların birikmesi | Nightly remediation DAG'ı — `vce_remediation_log`'a kaydedilir |
| "Dün gece ne silindi?" | Her temizlik işlemi kalıcı olarak kayıt altında |
| "Bu kural 3 ay önce neydi?" | MySQL trigger ile tam audit trail |
| Tablo büyümesi ve yavaşlama | Aylık MySQL RANGE partition |
| Hatalı kural SQL'i | `tools/test_rule.py` CLI ile deploy öncesi doğrulama |

---

## Hızlı Başlangıç — Çalıştırma Sırası

Bu bölüm sıfırdan kurulum yapan biri için adım adım çalıştırma sırasını gösterir.
Her adım bir öncekinin tamamlanmasına bağlıdır.

---

### Adım 1 — Host MySQL: bind-address

Docker container'ının host makinedeki MySQL'e erişebilmesi için:

```ini
# Windows: C:\ProgramData\MySQL\MySQL Server 8.0\my.ini
[mysqld]
bind-address = 0.0.0.0
```

Sonra MySQL servisini yeniden başlat:

```
Windows Hizmetleri (services.msc) → MySQL80 → Yeniden Başlat
```

---

### Adım 2 — Host MySQL: VCE Schema ve Tabloları

MySQL Workbench, DBeaver veya terminal ile host MySQL'e bağlan:

```sql
-- Önce vce schema'sını oluştur
CREATE DATABASE IF NOT EXISTS vce
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;
```

Sonra SQL dosyalarını sırayla çalıştır:

```bash
# 1. Tabloları, partition'ları ve trigger'ları oluşturur (7 tablo, 3 trigger)
mysql -u root -p vce < sql/01_vce_schema.sql

# 2. 35 hazır kuralı yükler
mysql -u root -p vce < sql/02_vce_seed_rules.sql
```

Doğrula:

```sql
USE vce;
SHOW TABLES;                        -- 7 tablo görünmeli
SHOW TRIGGERS;                      -- 3 trigger görünmeli
SELECT COUNT(*) FROM vce_dq_rules;  -- 35 görünmeli
```

> **Not:** `02_vce_seed_rules.sql`, `01_vce_schema.sql`'den önce çalıştırılırsa tablolar olmadığı için hata verir. Sıra önemli.

---

### Adım 3 — Host MySQL: Kullanıcılar

```sql
-- VCE kullanıcısı: vce schema'ya tam yetki
CREATE USER 'airflow_vce'@'%' IDENTIFIED BY 'GUCLU_SIFRE_1';
GRANT SELECT, INSERT, UPDATE, DELETE ON vce.* TO 'airflow_vce'@'%';

-- MailSender kullanıcısı: okuma + belirli DELETE yetkileri
CREATE USER 'airflow_ms_dml'@'%' IDENTIFIED BY 'GUCLU_SIFRE_2';
GRANT SELECT ON aws_mailsender_pro_v3.* TO 'airflow_ms_dml'@'%';
GRANT DELETE ON aws_mailsender_pro_v3.password_reset_tokens TO 'airflow_ms_dml'@'%';
GRANT DELETE ON aws_mailsender_pro_v3.unsubscribe_tokens    TO 'airflow_ms_dml'@'%';
GRANT DELETE ON aws_mailsender_pro_v3.rate_limit_log        TO 'airflow_ms_dml'@'%';
GRANT DELETE ON aws_mailsender_pro_v3.ses_notifications     TO 'airflow_ms_dml'@'%';

FLUSH PRIVILEGES;
```

---

### Adım 4 — DAG Dosyalarını Kopyala

ZIP'ten çıkan `dags/` klasörünün içeriğini Airflow'un dags klasörüne kopyala:

```
C:\Users\yeliz\Desktop\apache-airflow-docker\dags\
    operators\
        vce_operators.py                     ← VCE custom operatörleri
    mailsender_vce_main.py                   ← Ana denetim DAG'ı
    mailsender_vce_remediation.py            ← Temizlik DAG'ı
    mailsender_vce_partition_manager.py      ← Partition yönetimi DAG'ı
```

> **Önemli:** `vce_operators.py` mutlaka `dags/operators/` altında olmalı.
> DAG dosyaları şu şekilde import eder:
> ```python
> from operators.vce_operators import DataQualityOperator
> ```
> `operators/` klasörü `dags/` altında değilse import hatası alırsın.

---

### Adım 5 — docker-compose.yml Güncelle ve Docker'ı Başlat

`docker-compose.yml` dosyasına şu iki blok eklenmiş olmalı (her iki servise):

```yaml
extra_hosts:
  - "host.docker.internal:host-gateway"
environment:
  _PIP_ADDITIONAL_REQUIREMENTS: "pymysql"
```

Sonra Docker'ı yeniden başlat:

```bash
docker-compose down
docker-compose up -d
```

Container'ların ayağa kalktığını doğrula:

```bash
docker-compose ps
# airflow-webserver   → Up
# airflow-scheduler   → Up
# airflow-db          → Up
```

---

### Adım 6 — Airflow UI: Connection Tanımları

`http://localhost:8080` → Admin → Connections → **+**

**Connection 1 — VCE Schema:**

| Alan | Değer |
|------|-------|
| Conn Id | `vce` |
| Conn Type | `Generic` |
| Host | `host.docker.internal` |
| Schema | `vce` |
| Login | `airflow_vce` |
| Password | GUCLU_SIFRE_1 |
| Port | `3306` |

**Connection 2 — MailSender Schema:**

| Alan | Değer |
|------|-------|
| Conn Id | `mailsender` |
| Conn Type | `Generic` |
| Host | `host.docker.internal` |
| Schema | `aws_mailsender_pro_v3` |
| Login | `airflow_ms_dml` |
| Password | GUCLU_SIFRE_2 |
| Port | `3306` |

Bağlantıları kaydet, ardından container içinden test et:

```bash
docker exec -it airflow-scheduler python3 -c "
import pymysql
c = pymysql.connect(host='host.docker.internal', port=3306,
    user='airflow_vce', password='GUCLU_SIFRE_1', database='vce')
print('VCE OK:', c.server_version)
c.close()

c = pymysql.connect(host='host.docker.internal', port=3306,
    user='airflow_ms_dml', password='GUCLU_SIFRE_2', database='aws_mailsender_pro_v3')
print('MailSender OK:', c.server_version)
c.close()
"
```

Her ikisi de `OK` dönüyorsa devam et.

---

### Adım 7 — Airflow UI: Variable Tanımları

Admin → Variables → **+**

| Key | Value | Açıklama |
|-----|-------|----------|
| `VCE_PARTITION_RETENTION_MONTHS` | `12` | Kaç aylık partition saklanacak |
| `VCE_TEAMS_WEBHOOK_URL` | *(webhook url)* | Teams bildirimi (opsiyonel) |
| `VCE_SLACK_WEBHOOK_URL` | *(webhook url)* | Slack bildirimi (opsiyonel) |

---

### Adım 8 — DAG'ları İlk Kez Çalıştır

Airflow UI'da DAG listesinde üç DAG görünmeli:

```
mailsender_vce_partition_manager
mailsender_vce_remediation
mailsender_vce_main
```

**Çalıştırma sırası:**

**1. mailsender_vce_partition_manager** — Toggle ON → ▶ Trigger

```
Ne yapar: Partition yapısının doğru kurulduğunu doğrular,
          eksik partition varsa ekler.
Beklenen: Tüm task'lar yeşil (success)
Süre    : ~30 saniye
```

Başarılıysa MySQL'de doğrula:

```sql
SELECT PARTITION_NAME, TABLE_ROWS
FROM information_schema.PARTITIONS
WHERE TABLE_SCHEMA = 'vce' AND TABLE_NAME = 'vce_dq_executions'
ORDER BY PARTITION_ORDINAL_POSITION;
-- p2026_01 ... p2026_12, p_future görünmeli
```

**2. mailsender_vce_remediation** — Toggle ON → ▶ Trigger

```
Ne yapar: aws_mailsender_pro_v3'teki eski/süresi dolmuş kayıtları temizler,
          her işlemi vce.vce_remediation_log'a yazar.
Beklenen: Tüm task'lar yeşil
Süre    : ~1 dakika
```

Başarılıysa MySQL'de doğrula:

```sql
SELECT operation_type, rows_affected, result_status, executed_at
FROM vce.vce_remediation_log
ORDER BY executed_at DESC LIMIT 10;
```

**3. mailsender_vce_main** — Toggle ON → ▶ Trigger

```
Ne yapar: Tüm veri kalitesi kontrollerini çalıştırır (35 kural, 9 domain).
          Sonuçları vce.vce_dq_executions'a yazar.
Beklenen: Tüm task'lar yeşil veya bazıları kırmızı
          (kırmızı = kural ihlali tespit etti, bu normal olabilir)
Süre    : ~2-5 dakika
```

Başarılıysa MySQL'de doğrula:

```sql
SELECT rule_domain, rule_subdomain, result_status, result_value, run_date
FROM vce.vce_dq_executions
WHERE DATE(run_date) = CURDATE()
ORDER BY result_status DESC, rule_domain;
```

---

### Adım 9 — Otomatik Çalışma Takvimi

İlk manuel testler başarılıysa DAG'lar schedule'a göre otomatik çalışır:

```
Her ayın 1'i  01:00 UTC  → mailsender_vce_partition_manager
Her gün       03:00 UTC  → mailsender_vce_remediation
Her gün       06:00 UTC  → mailsender_vce_main
```

> **UTC/Türkiye saati farkı:** UTC+3 olduğu için Türkiye saatiyle:
> - Partition manager : her ayın 1'i **04:00**
> - Remediation       : her gün **06:00**
> - Ana denetim       : her gün **09:00**

Schedule'ı yerel saate uyarlamak istersen DAG dosyalarındaki
`schedule_interval` değerini değiştir:

```python
# mailsender_vce_main.py
schedule_interval="0 6 * * *",   # UTC 06:00 = TR 09:00
# TR 06:00'da çalışması için:
schedule_interval="0 3 * * *",   # UTC 03:00 = TR 06:00
```

---

### Sorun Çıkarsa

| Belirti | Olası Neden | Çözüm |
|---------|------------|-------|
| DAG listesinde görünmüyor | Import hatası | `docker logs airflow-scheduler` incele |
| Connection hatası | bind-address kapalı | MySQL my.ini → `bind-address = 0.0.0.0` |
| `pymysql` bulunamıyor | Paket kurulmamış | `docker-compose down && docker-compose up -d` |
| "Table not found" | Schema prefix eksik | Kural SQL'ini `test_rule.py` ile test et |
| Task kırmızı ama hata yok | Kural ihlali — normal | `vce_dq_executions` tablosunu sorgula |
| p_future dolu uyarısı | Partition eklemedi | `partition_manager` DAG'ını manuel tetikle |

---

## Schema Mimarisi

Bu projenin en kritik tasarım kararı: **iki ayrı MySQL schema, aynı sunucuda, iki ayrı Airflow connection ile yönetilir.**

```
Aynı MySQL Sunucusu
│
├── vce                          ← VCE altyapı tabloları
│   ├── vce_dq_rules             (kural tanımları)
│   ├── vce_dq_executions        (sonuçlar — aylık partition)
│   ├── vce_table_validations    (karşılaştırma tanımları)
│   ├── vce_table_val_executions (karşılaştırma sonuçları — aylık partition)
│   ├── vce_rule_audit_log       (kural değişiklik geçmişi)
│   ├── vce_remediation_log      (temizlik kayıtları — aylık partition)
│   └── vce_anomaly_baselines    (anomali istatistikleri)
│
└── aws_mailsender_pro_v3        ← MailSender Pro uygulama tabloları
    ├── send_log
    ├── suppression_list
    ├── senders
    ├── send_queue / send_queue_log
    ├── users / audit_log
    └── ... (diğer uygulama tabloları)
```

### Neden İki Ayrı Connection?

```
┌─────────────────────────────────────────────────────────┐
│  Conn Id : vce                                          │
│  Schema  : vce                                          │
│  Yetki   : SELECT + INSERT + UPDATE + DELETE → vce.*    │
│  Kullanım: VCE tablolarını okur ve yazar                │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Conn Id : mailsender                                   │
│  Schema  : aws_mailsender_pro_v3                        │
│  Yetki   : SELECT (+ belirli DELETE — remediation için) │
│  Kullanım: Kural SQL'lerini çalıştırır                  │
└─────────────────────────────────────────────────────────┘
```

Tek connection kullanılsaydı hangi schema'da çalışıldığı belirsiz kalır, yetki sınırları karışır ve yanlışlıkla MailSender production verisi değiştirilebilirdi.

### Operatör İçindeki Bağlantı Akışı

```
DataQualityOperator çalışırken:

  1. vce conn      → vce.vce_dq_rules'dan kuralları yükle
  2. mailsender conn → aws_mailsender_pro_v3 üzerinde SQL çalıştır
  3. vce conn      → sonucu vce.vce_dq_executions'a kaydet
  4. vce conn      → anomali için vce.vce_anomaly_baselines güncelle
```

---

## Sistem Mimarisi

```
┌──────────────────────────────────────────────────────────────────┐
│              MySQL — İki Ayrı Schema, Aynı Sunucu                │
│  ┌──────────────────────────┐  ┌──────────────────────────────┐  │
│  │  vce (VCE Tabloları)     │  │  aws_mailsender_pro_v3       │  │
│  │  7 tablo                 │  │  send_log, senders           │  │
│  │  3 tablo aylık partition │  │  suppression_list            │  │
│  │  3 MySQL trigger         │  │  send_queue, users...        │  │
│  └────────────┬─────────────┘  └──────────────┬───────────────┘  │
└───────────────┼──────────────────────────────┼───────────────────┘
                │ yazar / okur                  │ sadece okur
   ┌────────────▼──────────────────────────────▼───────────────┐
   │           Custom Airflow Operatörleri (BaseOperator)       │
   │   get_vce_conn()            get_mailsender_conn()          │
   │   run_vce_query()           run_mailsender_query()         │
   │   execute_vce_dml()                                        │
   └──────────────────────────────┬─────────────────────────────┘
                                  │
   ┌──────────────────────────────▼─────────────────────────────┐
   │                   Apache Airflow DAG'ları                   │
   │  mailsender_vce_main          — her gün 06:00              │
   │  mailsender_vce_remediation   — her gün 03:00              │
   │  mailsender_vce_partition_manager — her ayın 1'i 01:00     │
   └──────────────────────────────┬─────────────────────────────┘
                                  │
   ┌──────────────────────────────▼─────────────────────────────┐
   │        Teams · Slack · HTML Dashboard · MySQL Log           │
   └─────────────────────────────────────────────────────────────┘
```

### DAG Çalışma Takvimi

```
Her ayın 1'i
  01:00  partition_manager  → yeni ay partition'ı ekle, eskiyi düşür

Her gün
  03:00  remediation        → mailsender tabloları temizle, vce'ye logla
  06:00  vce_main başlar
           │
           ├─ dq_schema         → 16 zorunlu tablo kontrolü
           │
           ├─ [Paralel — 9 task]
           │   ├─ dq_security
           │   ├─ dq_send_log
           │   ├─ dq_suppression
           │   ├─ dq_queue
           │   ├─ dq_verify
           │   ├─ dq_senders
           │   ├─ dq_integrity
           │   ├─ dq_freshness_volume
           │   └─ tv_send_consistency
           │
           └─ generate_summary  → Teams/Slack özet bildirimi
```

---

## Özellikler

### Kural Yönetimi
- Tüm kurallar `vce.vce_dq_rules` tablosunda — kod değişmeden yeni kural eklenir
- `active_flag = 0` ile kural devre dışı, `non_active_description` ile sebebi kayıtlı
- `test_flag = 1` ile kural test modunda (aksiyon yok, sadece log)
- `execute_time` ile hangi saatteki DAG çalışmasında aktif
- MySQL trigger ile her INSERT/UPDATE/DELETE otomatik `vce_rule_audit_log`'a yazılır

### Kontrol Tipleri

| Tip | Açıklama | Kullanım |
|-----|----------|---------|
| `threshold` | SQL sonucu > 0 ise ihlal | En yaygın — COUNT döndürmeli |
| `anomaly` | Z-skoru tabanlı dinamik eşik | 30 günlük kayan pencere |
| `freshness` | Tablo güncellik kontrolü | Son N saatte kayıt geldi mi? |
| `volume` | Satır sayısı kontrolü | Tablo boş mu? |
| `schema` | Kolon varlığı / tipi | Şema değişikliği tespiti |
| `duplicate` | Unique ihlali | Tekrarlı kayıt tespiti |
| `custom` | Herhangi bir SQL mantığı | Özel iş kuralları |

### Anomali Tespiti
- Geçmiş 30 günlük değerler üzerinden ortalama ve standart sapma hesaplanır
- Z-skoru = `|değer - ortalama| / std`
- `|Z| > 3` → anomali (%99.7 güven aralığı dışı)
- Yetersiz geçmiş (< 7 gün) varsa baseline biriktirilir, anomali sayılmaz
- Taban değerleri `vce.vce_anomaly_baselines` tablosunda saklanır

### Güvenlik ve Yetki Ayrımı
- SQL injection: tüm sorgularda parametre binding (`%s` placeholder)
- Webhook URL'leri Airflow Variable'da — kodda hardcoded değil
- `vce` connection → VCE tablolarına yazar, MailSender'a dokunamaz
- `mailsender` connection → sadece okur (remediation hariç)
- Remediation için ayrı MySQL kullanıcısı (`airflow_ms_dml`)

### Data Retention — MySQL Partitioning
- 3 büyüyen tablo aylık RANGE partition ile yönetilir
- Eski partition'ı düşürmek microsaniye sürer
- Tek tablo görünümü — JOIN/UNION/VIEW gerekmez
- Partition yönetimi `mailsender_vce_partition_manager` DAG'ı ile otomatik

### Audit Trail
- `vce_rule_audit_log` MySQL trigger ile otomatik dolar
- INSERT / UPDATE / DELETE / ACTIVATE / DEACTIVATE olayları kaydedilir
- `old_sql` / `new_sql` karşılaştırması — tam değişiklik geçmişi
- `CURRENT_USER()` ile kim değiştirdi bilgisi otomatik

### Test Altyapısı
- 51 unit test, 8 test sınıfı — gerçek MySQL olmadan çalışır
- `tools/test_rule.py` CLI ile kural SQL'i deploy öncesi doğrulanır
- Orijinal VCE'deki class variable bug'ı unit test ile yakalanır

---

## Klasör Yapısı

```
vce_mailsender/
├── README.md
│
├── sql/
│   ├── 01_vce_schema.sql              # vce schema: 7 tablo + 3 trigger + partition tanımları
│   └── 02_vce_seed_rules.sql          # 35 hazır kural (9 domain, aws_mailsender_pro_v3 prefix'li)
│
├── operators/
│   └── vce_operators.py               # Custom Airflow operatörleri (~998 satır)
│       ├── VCEBaseOperator            # İki connection yönetimi, bildirim, kayıt
│       ├── DataQualityOperator        # Kural SQL çalıştırıcı + anomali motoru
│       ├── TableValidationOperator    # Kaynak-hedef karşılaştırma
│       └── RemediationOperator        # Otomatik temizlik
│
├── dags/
│   ├── mailsender_vce_main.py         # Ana denetim DAG'ı — her gün 06:00
│   ├── mailsender_vce_remediation.py  # Temizlik DAG'ı — her gün 03:00
│   └── mailsender_vce_partition_manager.py  # Partition yönetimi — her ayın 1'i 01:00
│
├── tests/
│   ├── conftest.py                    # pytest konfigürasyonu
│   ├── requirements-test.txt          # Test bağımlılıkları
│   └── test_vce_operators.py          # 51 unit test, 8 sınıf (~1205 satır)
│
├── tools/
│   └── test_rule.py                   # Kural SQL doğrulama CLI aracı (~612 satır)
│
└── dashboard/
    └── vce_dashboard.html             # HTML dashboard (Chart.js)
```

---

## Kurulum

### Ön Gereksinimler

| Bileşen | Sürüm | Not |
|---------|-------|-----|
| Python | 3.9+ | |
| Apache Airflow | 2.6+ | Docker önerilir |
| MySQL | 8.0+ | Partition desteği için 8.0 zorunlu |
| PyMySQL | 1.0+ | `pip install pymysql` |

### MySQL Kurulumu

#### 1. VCE Schema'sını Oluştur

```sql
CREATE DATABASE IF NOT EXISTS vce
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;
```

#### 2. MySQL Kullanıcılarını Oluştur

```sql
-- ── Kullanıcı 1: airflow_vce ──────────────────────────────────────
-- vce schema'sına tam yetkili — VCE tablolarını okur ve yazar
-- Airflow Connection: vce
CREATE USER 'airflow_vce'@'%' IDENTIFIED BY 'GUCLU_SIFRE_1';
GRANT SELECT, INSERT, UPDATE, DELETE ON vce.* TO 'airflow_vce'@'%';

-- ── Kullanıcı 2: airflow_ms_dml ───────────────────────────────────
-- aws_mailsender_pro_v3'ü okur + belirli tablolara DELETE yapar
-- Airflow Connection: mailsender
CREATE USER 'airflow_ms_dml'@'%' IDENTIFIED BY 'GUCLU_SIFRE_2';
GRANT SELECT ON aws_mailsender_pro_v3.* TO 'airflow_ms_dml'@'%';
GRANT DELETE ON aws_mailsender_pro_v3.password_reset_tokens TO 'airflow_ms_dml'@'%';
GRANT DELETE ON aws_mailsender_pro_v3.unsubscribe_tokens    TO 'airflow_ms_dml'@'%';
GRANT DELETE ON aws_mailsender_pro_v3.rate_limit_log        TO 'airflow_ms_dml'@'%';
GRANT DELETE ON aws_mailsender_pro_v3.ses_notifications     TO 'airflow_ms_dml'@'%';

FLUSH PRIVILEGES;
```

#### 3. VCE Şemasını ve Tablolarını Oluştur

```bash
mysql -u root -p vce < sql/01_vce_schema.sql
```

Bu komut şunları oluşturur:

**7 Tablo:**

| Tablo | Partition | Açıklama |
|-------|-----------|----------|
| `vce_dq_rules` | — | Kural tanımları |
| `vce_dq_executions` | ✅ Aylık | Kural sonuçları |
| `vce_table_validations` | — | Karşılaştırma tanımları |
| `vce_table_val_executions` | ✅ Aylık | Karşılaştırma sonuçları |
| `vce_rule_audit_log` | — | Kural değişiklik geçmişi |
| `vce_remediation_log` | ✅ Aylık | Temizlik kayıtları |
| `vce_anomaly_baselines` | — | Anomali istatistikleri |

**3 MySQL Trigger** (`vce_dq_rules` tablosu için):
- `trg_vce_rules_after_insert`
- `trg_vce_rules_after_update`
- `trg_vce_rules_after_delete`

#### 4. Kuralları Yükle

```bash
mysql -u root -p vce < sql/02_vce_seed_rules.sql
```

Yüklemeyi doğrula:

```sql
USE vce;

-- 35 kural yüklendi mi?
SELECT rule_domain, COUNT(*) as kural_sayisi
FROM vce_dq_rules
GROUP BY rule_domain ORDER BY rule_domain;

-- Trigger'lar aktif mi?
SHOW TRIGGERS FROM vce;

-- Partition'lar oluştu mu?
SELECT PARTITION_NAME, TABLE_ROWS
FROM information_schema.PARTITIONS
WHERE TABLE_SCHEMA = 'vce' AND TABLE_NAME = 'vce_dq_executions'
ORDER BY PARTITION_ORDINAL_POSITION;
```

#### 5. MySQL bind-address (Docker için)

```ini
# /etc/mysql/mysql.conf.d/mysqld.cnf
[mysqld]
bind-address = 0.0.0.0
```

```bash
sudo systemctl restart mysql
```

### Airflow Kurulumu

#### docker-compose.yml

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
  airflow-worker:
    extra_hosts:
      - "host.docker.internal:host-gateway"
    environment:
      _PIP_ADDITIONAL_REQUIREMENTS: "pymysql"
```

#### DAG Dosyalarını Kopyala

```bash
cp -r operators/     /path/to/airflow/dags/operators/
cp dags/mailsender_vce_main.py                  /path/to/airflow/dags/
cp dags/mailsender_vce_remediation.py           /path/to/airflow/dags/
cp dags/mailsender_vce_partition_manager.py     /path/to/airflow/dags/
```

### Airflow Connection Tanımları

**Airflow UI → Admin → Connections → +**

#### Connection 1: `vce`

| Alan | Değer |
|------|-------|
| Conn Id | `vce` |
| Conn Type | `Generic` |
| Host | `host.docker.internal` |
| Schema | `vce` |
| Login | `airflow_vce` |
| Password | GUCLU_SIFRE_1 |
| Port | `3306` |

#### Connection 2: `mailsender`

| Alan | Değer |
|------|-------|
| Conn Id | `mailsender` |
| Conn Type | `Generic` |
| Host | `host.docker.internal` |
| Schema | `aws_mailsender_pro_v3` |
| Login | `airflow_ms_dml` |
| Password | GUCLU_SIFRE_2 |
| Port | `3306` |

### Airflow Variable Tanımları

**Airflow UI → Admin → Variables → +**

| Key | Açıklama | Varsayılan |
|-----|----------|-----------|
| `VCE_TEAMS_WEBHOOK_URL` | Teams Incoming Webhook URL | — (opsiyonel) |
| `VCE_SLACK_WEBHOOK_URL` | Slack Incoming Webhook URL | — (opsiyonel) |
| `VCE_PARTITION_RETENTION_MONTHS` | Partition saklama süresi (ay) | `12` |

---

## Kullanım

### Yeni Kural Ekleme

Kod değişikliği gerekmez. `vce` schema'sına INSERT yeterlidir:

```sql
USE vce;

INSERT INTO vce_dq_rules (
    rule_domain,       -- Kural grubu: send_log, security, queue...
    rule_subdomain,    -- Alt grup: my_custom_check
    dataset_name,      -- Bilgi amaçlı: aws_mailsender_pro_v3
    table_name,        -- Bilgi amaçlı: send_log
    check_type,        -- threshold | anomaly | freshness | volume | duplicate | schema | custom
    sql_statement,     -- ⚠️ Schema prefix zorunlu: aws_mailsender_pro_v3.tablo_adi
    action,            -- fail: DAG durur | warn: bildirim gider, DAG devam eder
    description,       -- Kapsamlı açıklama — ne kontrol ediyor, neden var?
    execute_time,      -- '06:00' — hangi DAG çalışmasında aktif
    active_flag,       -- 1: aktif | 0: devre dışı
    author
) VALUES (
    'send_log',
    'my_timeout_check',
    'aws_mailsender_pro_v3',
    'send_log',
    'threshold',
    'SELECT COUNT(*)
     FROM aws_mailsender_pro_v3.send_log
     WHERE status = ''failed''
       AND error_msg LIKE ''%timeout%''
       AND sent_at >= NOW() - INTERVAL 1 HOUR',
    'warn',
    'Son 1 saatte timeout kaynaklı başarısız gönderim sayısını kontrol eder.
     Yüksek timeout SMTP sunucu sorununa işaret edebilir.',
    '06:00',
    1,
    'senin_adin'
);
-- INSERT sonrası trigger otomatik vce_rule_audit_log'a yazar.
```

> **Kritik:** Kural SQL'lerinde tüm tablo referansları `aws_mailsender_pro_v3.tablo_adi` formatında olmalıdır. Aksi halde sorgu `vce` schema'sında çalışır ve hata alırsınız.

### Kural Tipleri

#### threshold

En yaygın tip. SQL COUNT döndürmeli, sonuç `> 0` ise ihlal:

```sql
SELECT COUNT(*)
FROM aws_mailsender_pro_v3.send_log
WHERE recipient IS NULL
```

#### anomaly

Tek sayısal değer döndürmeli. Geçmiş 30 güne göre Z-skoru hesaplanır:

```sql
SELECT COUNT(*)
FROM aws_mailsender_pro_v3.send_log
WHERE status = 'failed' AND DATE(sent_at) = CURDATE()
```

`anomaly_threshold` alanı ile hassasiyet ayarlanabilir (varsayılan: `3.0`).

#### freshness

`0` = taze, `1` = bayat (ihlal):

```sql
SELECT CASE
    WHEN MAX(sent_at) < NOW() - INTERVAL 24 HOUR OR MAX(sent_at) IS NULL
    THEN 1 ELSE 0
END FROM aws_mailsender_pro_v3.send_log
```

#### pre_sql_statement

Asıl SQL'den önce çalışır (geçici tablo vb.):

```sql
-- pre_sql:
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_stats AS
SELECT sender_id, COUNT(*) as total, SUM(status='failed') as fails
FROM aws_mailsender_pro_v3.send_log
WHERE DATE(sent_at) = CURDATE()
GROUP BY sender_id;

-- sql:
SELECT COUNT(*) FROM tmp_stats
WHERE fails / NULLIF(total, 0) > 0.5 AND total >= 10
```

### Anomali Tespiti

```
Örnek: send_log/failed_ratio_anomaly

1. mailsender conn → SQL: bugün 280 failed
2. vce conn → son 30 gün: [120, 98, 134, 87, 156, 112, 103...]
3. mean = 113.2, std = 21.4
4. Z = |280 - 113.2| / 21.4 = 7.79
5. 7.79 > 3.0 → ANOMALİ → warn_checks'e eklenir
6. vce conn → vce_dq_executions'a yazar: z_score = 7.79
```

### Kural SQL Test Aracı

Yeni kural deploy etmeden önce `tools/test_rule.py` ile test edin:

```bash
# Ortam değişkenleri
export VCE_MS_HOST=localhost
export VCE_MS_USER=airflow_ms_dml
export VCE_MS_PASSWORD=sifre
export VCE_MS_DB=aws_mailsender_pro_v3
export VCE_HOST=localhost
export VCE_USER=airflow_vce
export VCE_PASSWORD=sifre
export VCE_DB=vce

# Yeni SQL test et
python tools/test_rule.py \
  --sql "SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_log WHERE status='failed'" \
  --type threshold \
  --action fail

# Mevcut kuralı ID ile test et
python tools/test_rule.py --rule-id 15

# Tüm aktif kuralları smoke test et
python tools/test_rule.py --all
```

Örnek çıktı:

```
┌─────────────────────────────────────────────────────┐
│  VCE Kural SQL Test Aracı                           │
└─────────────────────────────────────────────────────┘
── Bağlantı Kontrolü ──────────────────────────────────
  mailsender  → aws_mailsender_pro_v3  ✅ v8.0.36
  vce         → vce                   ✅ v8.0.36

── SQL ─────────────────────────────────────────────────
  SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_log
  WHERE status='failed'

── Sonuç ────────────────────────────────────────────────
  Scalar değer   : 142
  Çalışma süresi : 0.231s
  Tip            : threshold

── Analiz ───────────────────────────────────────────────
  ❌ İHLAL (142 > 0)

── Tahmini Airflow Etkisi ───────────────────────────────
  ❌ DAG task başarısız olur + Teams/Slack bildirimi
```

### DAG'ları Çalıştırma

```bash
# Manuel tetikleme
airflow dags trigger mailsender_vce_main
airflow dags trigger mailsender_vce_remediation
airflow dags trigger mailsender_vce_partition_manager
```

**Sonuçları sorgulama:**

```sql
USE vce;

-- Bugünkü kontrol özeti
SELECT rule_domain, rule_subdomain, result_status, result_value
FROM vce_dq_executions
WHERE DATE(run_date) = CURDATE()
ORDER BY result_status DESC, rule_domain;

-- Son 30 günlük trend
SELECT DATE(run_date) as gun,
       SUM(result_status='Passed') as gecti,
       SUM(result_status='Failed') as basarisiz
FROM vce_dq_executions
WHERE run_date >= NOW() - INTERVAL 30 DAY
GROUP BY DATE(run_date) ORDER BY gun;

-- Kural değişiklik geçmişi (trigger otomatik doldurdu)
SELECT change_type, changed_by, changed_at,
       LEFT(old_sql, 80) as eski,
       LEFT(new_sql, 80) as yeni
FROM vce_rule_audit_log
ORDER BY changed_at DESC LIMIT 20;

-- Dün gece ne silindi?
SELECT operation_type, target_table, rows_affected, executed_at
FROM vce_remediation_log
WHERE DATE(executed_at) = CURDATE() - INTERVAL 1 DAY;
```

---

## Dashboard

`dashboard/vce_dashboard.html` tarayıcıda doğrudan açılır.

**Sekmeler:**
- **Genel Bakış:** PASS/WARN/FAIL sayıları, domain tile'ları, 7 günlük trend grafikleri
- **Detay Loglar:** Domain ve durum filtresi, Z-skoru gösterimi
- **Trend Analizi:** send_log failed oranı, suppression büyümesi, anomali geçmişi
- **Temizlik Logları:** Remediation geçmişi — ne silindiği, kaç satır

Üretim entegrasyonu için `vce` schema'sından JSON döndüren bir Flask/FastAPI servisi önerilir.

---

## Operatörler

### DataQualityOperator

```python
from operators.vce_operators import DataQualityOperator

task = DataQualityOperator(
    task_id="check_send_log",
    rule_domain="send_log",       # vce.vce_dq_rules'dan bu domain yüklenir
    rule_subdomain=None,          # None → tüm subdomain'ler
    execute_time="06:00",         # Bu saatteki kurallar filtrelenir
)
```

**Bağlantı akışı:**

```
1. vce conn        → vce.vce_dq_rules'dan kuralları yükle
2. mailsender conn → kural SQL'ini aws_mailsender_pro_v3'te çalıştır
3. vce conn        → vce.vce_dq_executions'a sonucu kaydet
4. vce conn        → vce.vce_anomaly_baselines güncelle (anomaly için)
```

### TableValidationOperator

```python
from operators.vce_operators import TableValidationOperator

task = TableValidationOperator(
    task_id="validate_consistency",
    validation_domain="send_consistency",
)
```

Karşılaştırma tipleri: `exact` · `count` · `sum` · `tolerance`

### RemediationOperator

```python
from operators.vce_operators import RemediationOperator

task = RemediationOperator(
    task_id="cleanup",
    operations=["all"],
    # veya: ['delete_expired_tokens', 'delete_old_rate_logs']
)
```

**Bağlantı akışı:**

```
DELETE → mailsender conn (aws_mailsender_pro_v3)
LOG    → vce conn (vce.vce_remediation_log)
```

---

## Veritabanı Şeması

### vce.vce_dq_rules (Ana Kural Tablosu)

```
id, rule_domain, rule_subdomain
dataset_name, table_name
check_type: threshold|anomaly|freshness|volume|schema|duplicate|custom
sql_statement TEXT        ← aws_mailsender_pro_v3.tablo formatında
pre_sql_statement TEXT
action: fail|warn
description TEXT
anomaly_threshold DECIMAL (varsayılan: 3.0)
execute_time VARCHAR(10)  (ör: '06:00')
active_flag TINYINT       (1: aktif, 0: devre dışı)
author, test_flag, non_active_description
insert_timestamp, update_timestamp
```

### vce.vce_dq_executions (Aylık Partition)

```
PRIMARY KEY (id, run_date)    ← MySQL partition zorunluluğu
run_date DATETIME             ← Partition key
rule_id, rule_domain, rule_subdomain
dag_id, dag_task_name, dag_run
sql_statement, action
result_value DECIMAL, result_status: Passed|Failed|Skipped|Error
baseline_mean, baseline_std, z_score  ← anomaly tipi için
```

### vce.vce_rule_audit_log (Trigger Tarafından Doldurulur)

```
id, rule_id, rule_domain, rule_subdomain
change_type: INSERT|UPDATE|DELETE|ACTIVATE|DEACTIVATE
old_sql, new_sql
old_action, new_action
old_active_flag, new_active_flag
changed_by VARCHAR(100)   ← CURRENT_USER() otomatik
change_reason, changed_at
```

### vce.vce_remediation_log (Aylık Partition)

```
PRIMARY KEY (id, executed_at)
operation_type ENUM
target_table   ← aws_mailsender_pro_v3.tablo formatında
sql_executed, rows_affected
result_status: Success|Failed|Warning
result_detail
```

---

## Partition Yönetimi

`mailsender_vce_partition_manager` DAG'ı her ayın 1'inde otomatik çalışır:

1. **Yeni partition ekle** — gelecek ay için REORGANIZE
2. **Eski partition düşür** — `VCE_PARTITION_RETENTION_MONTHS` (varsayılan: 12 ay)
3. **Durum raporu** — boyutlar, satır sayıları, p_future kontrolü

**Manuel müdahale gerekirse:**

```sql
USE vce;

-- Partition boyutlarını görüntüle
SELECT PARTITION_NAME,
       TABLE_ROWS,
       ROUND(DATA_LENGTH/1024/1024, 2) as data_mb
FROM information_schema.PARTITIONS
WHERE TABLE_SCHEMA = 'vce' AND TABLE_NAME = 'vce_dq_executions'
ORDER BY PARTITION_ORDINAL_POSITION;

-- Yeni ay partition'ı ekle
ALTER TABLE vce_dq_executions
REORGANIZE PARTITION p_future INTO (
    PARTITION p2027_01 VALUES LESS THAN (TO_DAYS('2027-02-01')),
    PARTITION p_future  VALUES LESS THAN MAXVALUE
);

-- Eski partition düşür (VERİ SİLİNİR, GERİ ALINAMAZ)
ALTER TABLE vce_dq_executions        DROP PARTITION p2026_01;
ALTER TABLE vce_table_val_executions DROP PARTITION p2026_01;
ALTER TABLE vce_remediation_log      DROP PARTITION p2026_01;
```

**Partition olmayan tablolar ve nedenleri:**

| Tablo | Neden Partition Yok |
|-------|---------------------|
| `vce_dq_rules` | Sabit boyut — kural sayısı kadar satır |
| `vce_table_validations` | Sabit boyut |
| `vce_rule_audit_log` | Haftada 2-3 satır; tüm geçmiş erişilebilir kalmalı |
| `vce_anomaly_baselines` | Kural başına 1 satır, üzerine yazılır, büyümez |

---

## Kural Kataloğu

Seed dosyasıyla yüklenen **35 hazır kural**, **9 domain**:

| Domain | Kural Sayısı | Kurallar |
|--------|-------------|---------|
| `schema` | 1 | `missing_tables` |
| `security` | 4 | `no_active_admin`, `brute_force_detection`, `orphan_reset_tokens`, `inactive_users_with_access` |
| `send_log` | 6 | `failed_ratio_threshold`, `failed_ratio_anomaly`, `null_critical_fields`, `spam_risk_same_recipient`, `daily_volume`, `freshness` |
| `suppression` | 4 | `violation_critical`★, `daily_growth_anomaly`, `unsubscribe_integrity`, `domain_blacklist_check`★ |
| `queue` | 5 | `stuck_running`, `count_inconsistency`, `overdue_pending`, `ab_ratio_invalid`, `log_count_mismatch` |
| `verify` | 3 | `stuck_jobs`, `processed_exceeds_total`, `low_valid_rate` |
| `senders` | 5 | `no_active_sender`, `smtp_incomplete_config`, `ses_incomplete_config`, `warmup_without_daily_limit`, `high_failure_rate` |
| `integrity` | 4 | `send_log_orphan_sender`, `queue_log_orphan_queue`, `duplicate_suppression_email`, `ses_orphan_sender` |
| `freshness / volume` | 3 | `audit_log_stale`, `suppression_list_empty`, `ses_notification_volume` |

★ Kritik FAIL kuralları — DAG'ı durdurur.

Tam listeyi sorgulamak için:

```sql
SELECT rule_domain, rule_subdomain, check_type, action, description
FROM vce.vce_dq_rules ORDER BY rule_domain, rule_subdomain;
```

---

## Bildirimler

**Teams:** `Airflow UI → Admin → Variables → VCE_TEAMS_WEBHOOK_URL`

**Slack:** `Airflow UI → Admin → Variables → VCE_SLACK_WEBHOOK_URL`

Her iki kanal da opsiyoneldir. Tanımlanmamışsa sessizce geçilir, DAG durdurmaz.

---

## Test Altyapısı

```bash
# Bağımlılıkları kur
pip install -r tests/requirements-test.txt

# Tüm testleri çalıştır
pytest tests/ -v

# Coverage raporu
pytest tests/ --cov=operators --cov-report=term-missing
```

**8 Test Sınıfı, 51 Test:**

| Sınıf | Test Sayısı | Kapsam |
|-------|------------|--------|
| `TestVCEBaseOperator` | 8 | İki connection ayrımı, finally kapanma, rollback |
| `TestDataQualityOperatorThreshold` | 8 | Pass/fail/warn, test_flag, pre_sql sırası |
| `TestDataQualityOperatorAnomaly` | 6 | Z-skoru, yetersiz baseline, sıfır std |
| `TestDataQualityOperatorInstanceSafety` | 3 | **Class variable bug tespiti** |
| `TestDataQualityOperatorExecute` | 5 | AirflowException, bildirim, connection ayrımı |
| `TestTableValidationOperator` | 9 | exact/count/sum/tolerance, boş sonuç |
| `TestRemediationOperator` | 5 | DELETE→mailsender, log→vce, hata toleransı |
| `TestSchemaIntegrity` | 7 | Sabitler, prefix kontrolü, hardcoded URL |

Testler gerçek MySQL gerektirmez — `unittest.mock` ile sahte bağlantılar kullanılır.

---

## Orijinal VCE ile Farklar

| Konu | Orijinal VCE | Bu Proje |
|------|-------------|----------|
| Platform | BigQuery | MySQL |
| Schema | Tek schema | `vce` + `aws_mailsender_pro_v3` |
| Connection | Tek connection | İki ayrı connection |
| `fail_checks = []` | Class variable — paralelde paylaşılıyor | Instance variable — düzeltildi |
| SQL Injection | f-string INSERT | Parametre binding |
| Webhook URL | Hardcoded | Airflow Variable |
| `vce_monitoring`'da raise | Eksik | AirflowException eklendi |
| Audit log | Manuel doldurulması gerekiyor | MySQL trigger otomatik dolduruyor |
| Data retention | Yok | Aylık MySQL partition |
| Anomali tespiti | Yok | Z-skoru, 30 günlük pencere |
| Remediation log | Yok | `vce.vce_remediation_log` |
| Test altyapısı | Yok | 51 unit test |
| Kural test aracı | Yok | `tools/test_rule.py` CLI |
| Dashboard | Yok | HTML + Chart.js |
| Partition yönetimi | Yok | Otomatik DAG |

---

## Sorun Giderme

### Bağlantı Testi

```bash
# vce connection
docker exec -it <airflow-scheduler> python3 -c "
import pymysql
c = pymysql.connect(host='host.docker.internal', port=3306,
    user='airflow_vce', password='SIFRE', database='vce')
print('VCE OK:', c.server_version); c.close()
"

# mailsender connection
docker exec -it <airflow-scheduler> python3 -c "
import pymysql
c = pymysql.connect(host='host.docker.internal', port=3306,
    user='airflow_ms_dml', password='SIFRE', database='aws_mailsender_pro_v3')
print('MailSender OK:', c.server_version); c.close()
"
```

### "Table not found" Hatası

Kural SQL'inde schema prefix eksik:

```sql
-- ❌ Yanlış — vce schema'sında aranır
SELECT COUNT(*) FROM send_log WHERE ...

-- ✅ Doğru
SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_log WHERE ...
```

Prefix eksik kuralları bul:

```sql
SELECT rule_domain, rule_subdomain, LEFT(sql_statement, 100) as sql_preview
FROM vce.vce_dq_rules
WHERE sql_statement NOT LIKE '%aws_mailsender_pro_v3%'
  AND active_flag = 1;
```

### Trigger Çalışmıyor

```sql
SHOW TRIGGERS FROM vce;
-- 3 trigger görünmeli: trg_vce_rules_after_insert/update/delete
```

Görünmüyorsa şemayı yeniden yükle:
```bash
mysql -u root -p vce < sql/01_vce_schema.sql
```

### p_future Dolu Uyarısı

```sql
SELECT TABLE_ROWS FROM information_schema.PARTITIONS
WHERE TABLE_SCHEMA = 'vce' AND TABLE_NAME = 'vce_dq_executions'
  AND PARTITION_NAME = 'p_future';
```

Sıfırdan büyükse `partition_manager` DAG'ını manuel tetikle:
```bash
airflow dags trigger mailsender_vce_partition_manager
```

---

## Katkıda Bulunma

### Yeni Kural Ekle

```sql
-- Schema prefix zorunlu!
INSERT INTO vce.vce_dq_rules (..., sql_statement, ...) VALUES (
    ...,
    'SELECT COUNT(*) FROM aws_mailsender_pro_v3.TABLO WHERE ...',
    ...
);
-- Trigger otomatik audit log'a yazar — ekstra işlem gerekmez
```

### Yeni Operatör Yaz

`VCEBaseOperator`'dan türet, metodları doğru kullan:

```python
self.run_vce_query(sql)         # vce schema'dan oku
self.run_mailsender_query(sql)  # aws_mailsender_pro_v3'ten oku
self.execute_vce_dml(sql)       # vce schema'ya yaz
# mailsender'a YAZMA — bu yetki yok ve olmamalı
```

### Test Yaz

Her yeni operatör davranışı için test:

```python
def test_new_behavior(self, operator):
    with patch.object(operator, "run_mailsender_query",
                      return_value=[{"count": 0}]), \
         patch.object(operator, "log_execution"):
        operator._execute_rule(rule, dag)
    assert ...
```

---

<div align="center">
  <sub>
    VCE · vce schema · aws_mailsender_pro_v3 · Apache Airflow · MySQL 8.0 Partitioning · 35 kural · 51 unit test
  </sub>
</div>
