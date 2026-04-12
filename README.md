# VCE — Validity Control Engine for MailSender Pro

> **Apache Airflow + MySQL tabanlı, kural yönetimi veritabanında olan, üretim seviyesi veri kalitesi sistemi.**

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python)](https://python.org)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.6%2B-017CEE?logo=apacheairflow)](https://airflow.apache.org)
[![MySQL](https://img.shields.io/badge/MySQL-8.0%2B-4479A1?logo=mysql)](https://mysql.com)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

---

## İçindekiler

- [Proje Hakkında](#proje-hakkında)
- [Schema Mimarisi](#schema-mimarisi)
- [Mimari](#mimari)
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
  - [DAG'ları Çalıştırma](#dagları-çalıştırma)
- [Dashboard](#dashboard)
- [Operatörler](#operatörler)
  - [DataQualityOperator](#dataqualityoperator)
  - [TableValidationOperator](#tablevalidationoperator)
  - [RemediationOperator](#remediationoperator)
- [Veritabanı Şeması](#veritabanı-şeması)
- [Partition Yönetimi](#partition-yönetimi)
- [Kural Kataloğu](#kural-kataloğu)
- [Bildirimler](#bildirimler)
- [Orijinal VCE ile Farklar](#orijinal-vce-ile-farklar)
- [Sorun Giderme](#sorun-giderme)
- [Katkıda Bulunma](#katkıda-bulunma)

---

## Proje Hakkında

Bu proje, **MailSender Pro** e-posta gönderim platformunun MySQL veritabanı için geliştirilmiş tam kapsamlı bir veri kalitesi (Data Quality) sistemidir.

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
| Kural geçmişi ("2 ay önce ne vardı?") | MySQL trigger ile `vce_rule_audit_log` |
| Performans (tablo büyümesi) | MySQL aylık partition ile yönetim |

---

## Schema Mimarisi

Bu projenin en önemli tasarım kararı: **iki ayrı MySQL schema, aynı sunucuda, iki ayrı Airflow connection ile yönetilir.**

```
Aynı MySQL Sunucusu
├── vce                      ← VCE tabloları (kurallar, sonuçlar, loglar)
│   ├── vce_dq_rules
│   ├── vce_dq_executions        (aylık partition)
│   ├── vce_table_validations
│   ├── vce_table_val_executions (aylık partition)
│   ├── vce_rule_audit_log
│   ├── vce_remediation_log      (aylık partition)
│   └── vce_anomaly_baselines
│
└── aws_mailsender_pro_v3    ← MailSender Pro uygulama tabloları
    ├── send_log
    ├── suppression_list
    ├── senders
    ├── send_queue
    ├── users
    └── ... (diğer uygulama tabloları)
```

### Neden İki Ayrı Connection?

```
Tek connection olsaydı:
  - Hangi schema'da sorgu çalıştığı belirsiz olurdu
  - Kural SQL'i yanlışlıkla VCE tablosuna, sonuç yanlış schema'ya yazılabilirdi
  - Yetki yönetimi karmaşıklaşırdı

İki ayrı connection ile:
  ┌──────────────────────────────────────────────────────────┐
  │ Conn Id: vce                                             │
  │ Schema : vce                                             │
  │ Yetki  : SELECT + INSERT + UPDATE + DELETE → vce.*       │
  │ Kullanım: VCE tablolarını okur ve yazar                  │
  └──────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────┐
  │ Conn Id: mailsender                                      │
  │ Schema : aws_mailsender_pro_v3                           │
  │ Yetki  : Yalnızca SELECT (readonly)                      │
  │ Kullanım: Kural SQL'lerini çalıştırır, veri okur         │
  └──────────────────────────────────────────────────────────┘
```

### Operatör İçindeki Bağlantı Akışı

Her operatörde hangi adımın hangi connection'ı kullandığı açıkça ayrılmıştır:

```
DataQualityOperator çalışınca:

  Adım 1: vce connection
          → vce.vce_dq_rules'dan kuralları yükle

  Adım 2: mailsender connection
          → aws_mailsender_pro_v3.send_log üzerinde kural SQL'ini çalıştır

  Adım 3: vce connection
          → Sonucu vce.vce_dq_executions'a kaydet

  Adım 4: vce connection
          → Anomali için vce.vce_anomaly_baselines güncelle
```

---

## Mimari

```
┌──────────────────────────────────────────────────────────────────┐
│              MySQL — İKİ AYRI SCHEMA, AYNI SUNUCU                │
│                                                                   │
│  ┌─────────────────────────────┐  ┌──────────────────────────┐   │
│  │  vce (VCE Tabloları)        │  │  aws_mailsender_pro_v3   │   │
│  │  vce_dq_rules               │  │  send_log                │   │
│  │  vce_dq_executions [part.]  │  │  suppression_list        │   │
│  │  vce_rule_audit_log         │  │  senders                 │   │
│  │  vce_remediation_log [part.]│  │  send_queue              │   │
│  │  vce_anomaly_baselines      │  │  users                   │   │
│  └──────────┬──────────────────┘  └──────────┬───────────────┘   │
└─────────────┼────────────────────────────────┼───────────────────┘
              │ yazar/okur                      │ sadece okur
   ┌──────────▼─────────────────────────────────▼──────────────┐
   │            Custom Airflow Operatörleri (BaseOperator)       │
   │  get_vce_conn()          get_mailsender_conn()              │
   │  run_vce_query()         run_mailsender_query()             │
   │  execute_vce_dml()                                          │
   └──────────────────────────┬─────────────────────────────────┘
                              │
   ┌──────────────────────────▼─────────────────────────────────┐
   │                   Apache Airflow DAG'ları                    │
   │  mailsender_vce_main (06:00)                                │
   │  mailsender_vce_remediation (03:00)                         │
   └──────────────────────────┬─────────────────────────────────┘
                              │
   ┌──────────────────────────▼─────────────────────────────────┐
   │         Teams · Slack · HTML Dashboard · MySQL Log           │
   └─────────────────────────────────────────────────────────────┘
```

### Çalışma Akışı

```
03:00  mailsender_vce_remediation
       mailsender conn → aws_mailsender_pro_v3 tablolarını temizle
       vce conn        → vce.vce_remediation_log'a yaz

06:00  mailsender_vce_main
  │
  ├─ dq_schema       vce conn → vce.vce_dq_rules'dan kuralları yükle
  │                  mailsender conn → information_schema kontrol
  │
  ├─ [Paralel — her task için aynı akış]
  │   vce conn        → kuralları yükle
  │   mailsender conn → kural SQL'ini çalıştır
  │   vce conn        → sonucu kaydet
  │
  └─ generate_summary → Teams/Slack bildirimi
```

---

## Özellikler

### Kural Yönetimi
- ✅ Tüm kurallar `vce.vce_dq_rules` tablosunda — kod değişmeden kural eklenir
- ✅ `active_flag = 0` ile kural devre dışı, `non_active_description` ile sebebi kayıtlı
- ✅ `test_flag = 1` ile kural test modunda (aksiyon yok, sadece log)
- ✅ `execute_time` ile hangi saatteki DAG çalışmasında aktif
- ✅ MySQL trigger ile her kural değişikliği `vce_rule_audit_log`'a otomatik kaydedilir

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
- `|Z| > 3` → anomali (%99.7 güven aralığı dışı)
- Yetersiz geçmiş (< 7 gün) varsa baseline biriktirilir, anomali sayılmaz
- Taban değerleri `vce.vce_anomaly_baselines` tablosunda saklanır

### Güvenlik & Yetki Ayrımı
- SQL injection: tüm sorgularda parametre binding (`%s` placeholder)
- Webhook URL'leri Airflow Variable'da (hardcoded değil)
- `vce` connection → VCE tablolarına yazar, MailSender'a dokunamaz
- `mailsender` connection → sadece SELECT, VCE tablolarına dokunamaz
- Remediation için ayrı DML kullanıcısı (`airflow_ms_dml`)

### Data Retention (MySQL Partitioning)
- 3 büyüyen tablo aylık RANGE partition ile yönetilir
- Eski partition'ı düşürmek microsaniye sürer (DELETE'den çok hızlı)
- Tek tablo görünümü — JOIN/UNION/VIEW gerekmez
- Sorgu performansı: sadece ilgili ay partition'ı taranır

### Audit Trail
- `vce_rule_audit_log` MySQL trigger ile otomatik dolar
- Kural INSERT/UPDATE/DELETE/ACTIVATE/DEACTIVATE olayları kaydedilir
- `old_sql` / `new_sql` karşılaştırması — tam geçmiş

### Remediation (Otomatik Temizlik)

Her gece 03:00'da `aws_mailsender_pro_v3`'ten temizler:

| İşlem | Hedef Tablo | Kriter |
|-------|------------|--------|
| `delete_expired_tokens` | `password_reset_tokens` | Süresi dolmuş/kullanılmış, 7+ gün eski |
| `delete_expired_unsub` | `unsubscribe_tokens` | Kullanılmamış, 30+ gün süresi dolmuş |
| `delete_old_rate_logs` | `rate_limit_log` | 7+ günlük kayıtlar |
| `delete_old_ses_notif` | `ses_notifications` | 90+ günlük bildirimler |

Her işlem `vce.vce_remediation_log`'a kaydedilir.

---

## Klasör Yapısı

```
vce_mailsender/
├── README.md
│
├── sql/
│   ├── 01_vce_schema.sql          # vce schema'daki 7 tabloyu oluşturur
│   │                              # + MySQL trigger'lar (audit log için)
│   │                              # + Aylık partition tanımları
│   └── 02_vce_seed_rules.sql      # 30+ hazır kural (aws_mailsender_pro_v3 prefix'li)
│
├── operators/
│   └── vce_operators.py           # Custom Airflow operatörleri
│       ├── VCEBaseOperator        # İki connection yönetimi + bildirim + kayıt
│       ├── DataQualityOperator    # Kural SQL çalıştırıcı
│       ├── TableValidationOperator# Kaynak-hedef karşılaştırma
│       └── RemediationOperator    # Otomatik temizlik
│
├── dags/
│   ├── mailsender_vce_main.py         # Ana denetim DAG'ı (06:00)
│   └── mailsender_vce_remediation.py  # Temizlik DAG'ı (03:00)
│
└── dashboard/
    └── vce_dashboard.html         # HTML dashboard (Chart.js)
```

---

## Kurulum

### Ön Gereksinimler

| Bileşen | Sürüm | Not |
|---------|-------|-----|
| Python | 3.9+ | |
| Apache Airflow | 2.6+ | Docker önerilir |
| MySQL | 8.0+ | Partition desteği için 8.0 şart |
| PyMySQL | 1.0+ | `pip install pymysql` |

### MySQL Kurulumu

#### 1. VCE schema'sını oluştur

```sql
CREATE DATABASE IF NOT EXISTS vce
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;
```

#### 2. MySQL kullanıcılarını oluştur

Bu projede üç ayrı MySQL kullanıcısı kullanılır. Her birinin sorumluluğu nettir:

```sql
-- ── Kullanıcı 1: airflow_vce ──────────────────────────────────────────
-- vce schema'sına tam yetkili — VCE tablolarını okur ve yazar.
-- Airflow Connection: vce
CREATE USER 'airflow_vce'@'%' IDENTIFIED BY 'GUCLU_SIFRE_1';
GRANT SELECT, INSERT, UPDATE, DELETE ON vce.* TO 'airflow_vce'@'%';

-- ── Kullanıcı 2: airflow_ms_readonly ──────────────────────────────────
-- aws_mailsender_pro_v3 schema'sını sadece okur.
-- Kural SQL'leri bu kullanıcıyla çalışır — yazma yetkisi kesinlikle yok.
-- Airflow Connection: mailsender (DataQualityOperator ve TableValidationOperator için)
CREATE USER 'airflow_ms_readonly'@'%' IDENTIFIED BY 'GUCLU_SIFRE_2';
GRANT SELECT ON aws_mailsender_pro_v3.* TO 'airflow_ms_readonly'@'%';

-- ── Kullanıcı 3: airflow_ms_dml ───────────────────────────────────────
-- aws_mailsender_pro_v3'te sadece belirli tablolara DELETE yetkisi.
-- RemediationOperator bu kullanıcıyla çalışır.
-- Airflow Connection: mailsender (RemediationOperator için — ayrı bağlantı önerilir)
CREATE USER 'airflow_ms_dml'@'%' IDENTIFIED BY 'GUCLU_SIFRE_3';
GRANT SELECT ON aws_mailsender_pro_v3.* TO 'airflow_ms_dml'@'%';
GRANT DELETE ON aws_mailsender_pro_v3.password_reset_tokens TO 'airflow_ms_dml'@'%';
GRANT DELETE ON aws_mailsender_pro_v3.unsubscribe_tokens    TO 'airflow_ms_dml'@'%';
GRANT DELETE ON aws_mailsender_pro_v3.rate_limit_log        TO 'airflow_ms_dml'@'%';
GRANT DELETE ON aws_mailsender_pro_v3.ses_notifications     TO 'airflow_ms_dml'@'%';

FLUSH PRIVILEGES;
```

> **Not:** `airflow_ms_readonly` ve `airflow_ms_dml` için aynı Airflow connection (`mailsender`) kullanılıyorsa DML kullanıcısını tercih edin — SELECT yetkisi zaten içinde. Güvenlik öncelikli ortamlarda iki ayrı connection tanımlanabilir.

#### 3. VCE şemasını ve tablolarını oluştur

```bash
# vce schema'sına bağlanarak SQL'i çalıştır
mysql -u root -p vce < sql/01_vce_schema.sql
```

Bu komut şunları oluşturur:

**7 tablo:**

| Tablo | Partition | Açıklama |
|-------|-----------|----------|
| `vce_dq_rules` | — | Kural tanımları |
| `vce_dq_executions` | ✅ Aylık | Her çalışmanın sonucu |
| `vce_table_validations` | — | Karşılaştırma tanımları |
| `vce_table_val_executions` | ✅ Aylık | Karşılaştırma sonuçları |
| `vce_rule_audit_log` | — | Kural değişiklik geçmişi |
| `vce_remediation_log` | ✅ Aylık | Temizlik kayıtları |
| `vce_anomaly_baselines` | — | Anomali istatistikleri |

**3 MySQL trigger** (`vce_dq_rules` için):
- `trg_vce_rules_after_insert` — yeni kural eklendiğinde
- `trg_vce_rules_after_update` — kural değiştirildiğinde
- `trg_vce_rules_after_delete` — kural silindiğinde

#### 4. Kuralları yükle

```bash
# Kurallar aws_mailsender_pro_v3 prefix'li — vce schema'sına yüklenir
mysql -u root -p vce < sql/02_vce_seed_rules.sql
```

Yüklemeyi doğrula:

```sql
USE vce;

-- Kural sayısı domain bazlı
SELECT rule_domain, COUNT(*) as kural_sayisi,
       SUM(active_flag) as aktif
FROM vce_dq_rules
GROUP BY rule_domain
ORDER BY rule_domain;

-- Trigger'lar oluştu mu?
SHOW TRIGGERS FROM vce;

-- Partition'lar oluştu mu?
SELECT PARTITION_NAME, TABLE_ROWS
FROM information_schema.PARTITIONS
WHERE TABLE_SCHEMA = 'vce'
  AND TABLE_NAME = 'vce_dq_executions'
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

#### 1. docker-compose.yml

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

#### 2. DAG dosyalarını kopyala

```bash
cp -r operators/ /path/to/airflow/dags/operators/
cp dags/mailsender_vce_main.py         /path/to/airflow/dags/
cp dags/mailsender_vce_remediation.py  /path/to/airflow/dags/
```

### Airflow Connection Tanımları

**Airflow UI → Admin → Connections → +**

Bu projede **iki ayrı connection** tanımlanmalıdır:

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

Bu connection VCE tablolarını okumak ve yazmak için kullanılır.

#### Connection 2: `mailsender`

| Alan | Değer |
|------|-------|
| Conn Id | `mailsender` |
| Conn Type | `Generic` |
| Host | `host.docker.internal` |
| Schema | `aws_mailsender_pro_v3` |
| Login | `airflow_ms_dml` |
| Password | GUCLU_SIFRE_3 |
| Port | `3306` |

Bu connection kural SQL'lerini çalıştırmak ve remediation için kullanılır.

> **Neden `airflow_ms_dml`?** Readonly kullanıcı kural SQL'lerini çalıştırabilir ama RemediationOperator DELETE yapamaz. DML kullanıcısı her ikisini de karşılar. Daha katı güvenlik için iki ayrı connection (`mailsender_ro` / `mailsender_dml`) oluşturabilirsiniz.

### Airflow Variable Tanımları

**Airflow UI → Admin → Variables → +**

| Key | Value | Zorunlu |
|-----|-------|---------|
| `VCE_TEAMS_WEBHOOK_URL` | Teams Incoming Webhook URL | Opsiyonel |
| `VCE_SLACK_WEBHOOK_URL` | Slack Incoming Webhook URL | Opsiyonel |

---

## Kullanım

### Yeni Kural Ekleme

Kod değişikliği gerekmez. `vce` schema'sına INSERT yeterlidir:

```sql
USE vce;

INSERT INTO vce_dq_rules (
    rule_domain, rule_subdomain,
    dataset_name, table_name,
    check_type, sql_statement,
    action, description,
    execute_time, active_flag, author
) VALUES (
    'send_log', 'my_custom_check',
    'aws_mailsender_pro_v3', 'send_log',
    'threshold',
    -- ÖNEMLI: Schema prefix zorunlu — aws_mailsender_pro_v3.tablo_adi
    'SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_log
     WHERE status = ''failed''
       AND error_msg LIKE ''%timeout%''
       AND sent_at >= NOW() - INTERVAL 1 HOUR',
    'warn',
    'Son 1 saatte timeout nedeniyle başarısız gönderim sayısını kontrol eder.',
    '06:00', 1, 'senin_adin'
);
```

> **Kritik:** Kural SQL'lerinde tüm tablo referansları `aws_mailsender_pro_v3.tablo_adi` formatında olmalıdır. Aksi halde sorgu `vce` schema'sında çalışır ve tablo bulunamaz.

INSERT sonrasında trigger otomatik `vce_rule_audit_log`'a yazar — audit kaydı için ekstra işlem gerekmez.

### Kural Tipleri

#### threshold

```sql
-- SQL COUNT döndürmeli, sonuç > 0 ise ihlal
SELECT COUNT(*)
FROM aws_mailsender_pro_v3.send_log
WHERE recipient IS NULL
```

#### anomaly

```sql
-- Tek sayısal değer döndürmeli — geçmişe göre Z-skoru hesaplanır
SELECT COUNT(*)
FROM aws_mailsender_pro_v3.send_log
WHERE status = 'failed'
  AND DATE(sent_at) = CURDATE()
```

#### freshness

```sql
-- 0 = taze, 1 = bayat (ihlal)
SELECT CASE
    WHEN MAX(sent_at) < NOW() - INTERVAL 24 HOUR OR MAX(sent_at) IS NULL
    THEN 1 ELSE 0
END
FROM aws_mailsender_pro_v3.send_log
```

#### pre_sql_statement kullanımı

```sql
-- pre_sql (mailsender connection üzerinde çalışır):
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_stats AS
SELECT sender_id, COUNT(*) as total, SUM(status='failed') as fails
FROM aws_mailsender_pro_v3.send_log
WHERE DATE(sent_at) = CURDATE()
GROUP BY sender_id;

-- sql (aynı mailsender connection, aynı session):
SELECT COUNT(*) FROM tmp_stats
WHERE fails / NULLIF(total, 0) > 0.5 AND total >= 10
```

### Anomali Tespiti

```
Örnek: send_log/failed_ratio_anomaly kuralı

1. mailsender conn → SQL çalışır: bugün 280 failed
2. vce conn → son 30 günlük geçmiş: [120, 98, 134, 87, 156, 112, 103...]
3. mean=113.2, std=21.4
4. Z = |280 - 113.2| / 21.4 = 7.79
5. 7.79 > 3.0 (eşik) → ANOMALİ → warn_checks'e eklenir
6. vce conn → vce_dq_executions'a yazar: z_score=7.79
```

### DAG'ları Çalıştırma

```bash
# Manuel tetikleme
airflow dags trigger mailsender_vce_main
airflow dags trigger mailsender_vce_remediation
```

**Sonuçları sorgulama (`vce` schema'sında):**

```sql
USE vce;

-- Bugünkü kontrol özeti
SELECT rule_domain, rule_subdomain, result_status, result_value, run_date
FROM vce_dq_executions
WHERE DATE(run_date) = CURDATE()
ORDER BY result_status DESC, rule_domain;

-- Bir kuralın geçmiş trendi
SELECT DATE(run_date) as gun, result_value, result_status, z_score
FROM vce_dq_executions
WHERE rule_domain = 'send_log'
  AND rule_subdomain = 'failed_ratio_threshold'
  AND run_date >= NOW() - INTERVAL 30 DAY
ORDER BY run_date;

-- Kural değişiklik geçmişi (trigger otomatik doldurdu)
SELECT change_type, changed_by, changed_at,
       LEFT(old_sql, 100) as eski_sql,
       LEFT(new_sql, 100) as yeni_sql
FROM vce_rule_audit_log
WHERE rule_domain = 'send_log'
ORDER BY changed_at DESC;

-- Dün gece ne silindi?
SELECT operation_type, target_table, rows_affected, result_status, executed_at
FROM vce_remediation_log
WHERE DATE(executed_at) = CURDATE() - INTERVAL 1 DAY
ORDER BY executed_at;
```

---

## Dashboard

`dashboard/vce_dashboard.html` tarayıcıda doğrudan açılır.

Özellikler: son 24h PASS/WARN/FAIL sayıları, domain tile'ları, 7 günlük trend grafikleri, anomali Z-skoru tablosu, temizlik logları, JSON export.

Üretim entegrasyonu için `vce` schema'sından JSON servisi yapan bir Flask/FastAPI endpoint'i önerilir.

---

## Operatörler

### DataQualityOperator

```python
from operators.vce_operators import DataQualityOperator

task = DataQualityOperator(
    task_id="check_send_log",
    rule_domain="send_log",       # vce.vce_dq_rules'dan bu domain yüklenir
    rule_subdomain=None,          # None ise tüm subdomain'ler
    execute_time="06:00",         # Bu saatteki kurallar filtrelenir
)
```

**Bağlantı akışı:**

```
vce conn        → vce.vce_dq_rules'dan kuralları yükle
mailsender conn → aws_mailsender_pro_v3 üzerinde kural SQL'ini çalıştır
vce conn        → vce.vce_dq_executions'a sonucu yaz
vce conn        → vce.vce_anomaly_baselines'ı güncelle (anomaly tipi için)
```

### TableValidationOperator

```python
from operators.vce_operators import TableValidationOperator

task = TableValidationOperator(
    task_id="validate_consistency",
    validation_domain="send_consistency",
)
```

**Örnek kural tanımı (`vce.vce_table_validations`):**

```sql
INSERT INTO vce.vce_table_validations (
    validation_domain, validation_subdomain,
    source_conn_id, source_dataset, source_table,
    source_sql,
    target_conn_id, target_dataset, target_table,
    target_sql,
    comparison_type, tolerance_pct, action, description, active_flag, author
) VALUES (
    'send_consistency', 'queue_vs_log',
    'mailsender', 'aws_mailsender_pro_v3', 'send_queue',
    'SELECT id, sent_count FROM aws_mailsender_pro_v3.send_queue
     WHERE status = ''done'' ORDER BY id',
    'mailsender', 'aws_mailsender_pro_v3', 'send_queue_log',
    'SELECT queue_id, COUNT(*) FROM aws_mailsender_pro_v3.send_queue_log
     WHERE status=''sent'' GROUP BY queue_id ORDER BY queue_id',
    'tolerance', 2.0, 'warn',
    'Queue sent_count ile send_queue_log satır sayısını %2 toleransla karşılaştırır.',
    1, 'data-team'
);
```

**Karşılaştırma tipleri:**

| Tip | Açıklama |
|-----|----------|
| `exact` | Satırlar birebir eşleşmeli |
| `count` | Satır sayıları eşit olmalı |
| `sum` | Sayısal toplamlar eşit olmalı |
| `tolerance` | Yüzde fark izin verilen aralıkta |

### RemediationOperator

```python
from operators.vce_operators import RemediationOperator

task = RemediationOperator(
    task_id="cleanup",
    operations=["all"],   # veya ['delete_expired_tokens', 'delete_old_rate_logs']
)
```

**Bağlantı akışı:**

```
mailsender conn → aws_mailsender_pro_v3'te DELETE çalıştır
vce conn        → vce.vce_remediation_log'a sonucu yaz
```

---

## Veritabanı Şeması

Tüm VCE tabloları `vce` schema'sındadır. Tablo SQL'lerindeki referanslar `aws_mailsender_pro_v3.tablo` formatındadır.

### vce.vce_dq_rules

```
id, rule_domain, rule_subdomain
dataset_name, table_name
check_type: threshold|anomaly|freshness|volume|schema|duplicate|custom
sql_statement TEXT        ← aws_mailsender_pro_v3.tablo formatında
pre_sql_statement TEXT
action: fail|warn
description TEXT
anomaly_threshold DECIMAL
execute_time VARCHAR(10)
active_flag, author, test_flag, non_active_description
insert_timestamp, update_timestamp
```

### vce.vce_dq_executions (Aylık Partition)

```
id BIGINT, run_date DATETIME    ← PRIMARY KEY (id, run_date) — partition zorunluluğu
rule_id, rule_domain, rule_subdomain
dag_id, dag_task_name, dag_run
sql_statement, action
result_value DECIMAL, result_status: Passed|Failed|Skipped|Error
baseline_mean, baseline_std, z_score    ← anomaly için
```

### vce.vce_rule_audit_log

```
id, rule_id, rule_domain, rule_subdomain
change_type: INSERT|UPDATE|DELETE|ACTIVATE|DEACTIVATE
old_sql, new_sql
old_action, new_action
old_active_flag, new_active_flag
changed_by VARCHAR(100)    ← CURRENT_USER() — trigger doldurur
change_reason, changed_at
```

### vce.vce_remediation_log (Aylık Partition)

```
id BIGINT, executed_at DATETIME    ← PRIMARY KEY (id, executed_at)
dag_id, dag_run, task_name
operation_type ENUM
target_table VARCHAR(200)    ← aws_mailsender_pro_v3.tablo formatında
sql_executed, rows_affected
result_status: Success|Failed|Warning
result_detail
```

---

## Partition Yönetimi

Üç tablo aylık RANGE partition kullanır. Airflow DAG'ı (gelecekte eklenecek `partition_manager`) bunları otomatik yönetir. Manuel müdahale için:

```sql
USE vce;

-- Mevcut partition'ları listele
SELECT PARTITION_NAME,
       TABLE_ROWS,
       ROUND(DATA_LENGTH/1024/1024, 2) as data_mb,
       ROUND(INDEX_LENGTH/1024/1024, 2) as index_mb
FROM information_schema.PARTITIONS
WHERE TABLE_SCHEMA = 'vce'
  AND TABLE_NAME = 'vce_dq_executions'
ORDER BY PARTITION_ORDINAL_POSITION;

-- Yeni ay partition'ı ekle (p_future'ı reorganize et)
ALTER TABLE vce_dq_executions
REORGANIZE PARTITION p_future INTO (
    PARTITION p2027_01 VALUES LESS THAN (TO_DAYS('2027-02-01')),
    PARTITION p_future  VALUES LESS THAN MAXVALUE
);

-- Aynı işlemi diğer tablolar için tekrarla
ALTER TABLE vce_table_val_executions
REORGANIZE PARTITION p_future INTO (
    PARTITION p2027_01 VALUES LESS THAN (TO_DAYS('2027-02-01')),
    PARTITION p_future  VALUES LESS THAN MAXVALUE
);

ALTER TABLE vce_remediation_log
REORGANIZE PARTITION p_future INTO (
    PARTITION p2027_01 VALUES LESS THAN (TO_DAYS('2027-02-01')),
    PARTITION p_future  VALUES LESS THAN MAXVALUE
);

-- Partition içindeki veriyi kontrol et (silmeden önce!)
SELECT COUNT(*) FROM vce_dq_executions PARTITION (p2026_01);

-- Eski partition'ı düşür (VERİ SİLİNİR, GERİ ALINAMAZ)
ALTER TABLE vce_dq_executions        DROP PARTITION p2026_01;
ALTER TABLE vce_table_val_executions DROP PARTITION p2026_01;
ALTER TABLE vce_remediation_log      DROP PARTITION p2026_01;
```

**Partition olmayan tablolar ve neden:**

| Tablo | Neden Partition Yok |
|-------|---------------------|
| `vce_dq_rules` | Sabit boyut, 200 kural = 200 satır |
| `vce_table_validations` | Sabit boyut |
| `vce_rule_audit_log` | Haftada 2-3 satır, tüm geçmiş erişilebilir kalmalı |
| `vce_anomaly_baselines` | Kural başına 1 satır, üzerine yazılır, büyümez |

---

## Kural Kataloğu

Seed dosyasıyla yüklenen 30+ hazır kural. Tüm SQL'ler `aws_mailsender_pro_v3.` prefix'lidir.

### Güvenlik (4 kural)

| Kural | Tip | Aksiyon |
|-------|-----|---------|
| `no_active_admin` | threshold | fail |
| `brute_force_detection` | threshold | warn |
| `orphan_reset_tokens` | threshold | warn |
| `inactive_users_with_access` | threshold | warn |

### Gönderim Logu (6 kural)

| Kural | Tip | Aksiyon |
|-------|-----|---------|
| `failed_ratio_threshold` | threshold | fail |
| `failed_ratio_anomaly` | anomaly | warn |
| `null_critical_fields` | threshold | fail |
| `spam_risk_same_recipient` | threshold | warn |
| `daily_volume` | anomaly | warn |
| `freshness` | freshness | warn |

### Suppression (4 kural)

| Kural | Tip | Aksiyon |
|-------|-----|---------|
| `violation_critical` | threshold | **fail** |
| `domain_blacklist_check` | threshold | **fail** |
| `daily_growth_anomaly` | anomaly | warn |
| `unsubscribe_integrity` | threshold | warn |

### Queue, Verify, Sender, Integrity, Freshness/Volume

Toplam 16+ ek kural — `vce.vce_dq_rules` tablosunu sorgulayarak tam listeye ulaşabilirsiniz:

```sql
SELECT rule_domain, rule_subdomain, check_type, action, description
FROM vce.vce_dq_rules
ORDER BY rule_domain, rule_subdomain;
```

---

## Bildirimler

**Teams:** Airflow UI → Admin → Variables → `VCE_TEAMS_WEBHOOK_URL`

**Slack:** Airflow UI → Admin → Variables → `VCE_SLACK_WEBHOOK_URL`

---

## Orijinal VCE ile Farklar

| Konu | Orijinal VCE | Bu Proje |
|------|-------------|----------|
| **Platform** | BigQuery | MySQL |
| **Schema** | Tek schema | İki schema (vce + aws_mailsender_pro_v3) |
| **Connection** | Tek connection | İki ayrı connection (vce / mailsender) |
| **Class variable bug** | `fail_checks = []` class level | `self.fail_checks = []` instance level |
| **SQL Injection** | f-string INSERT | Parametre binding |
| **Webhook URL** | Hardcoded | Airflow Variable |
| **Eksik raise** | vce_monitoring'de yok | AirflowException eklendi |
| **Audit log** | Manuel dolduruluyor | MySQL trigger otomatik dolduruyor |
| **Data retention** | Yok | Aylık MySQL partition |
| **Anomali tespiti** | Yok | Z-skoru, 30 günlük kayan pencere |
| **Remediation log** | Yok | vce.vce_remediation_log |
| **Dashboard** | Yok | HTML + Chart.js |

---

## Sorun Giderme

### Bağlantı Testi

```bash
# vce connection testi
docker exec -it <airflow-scheduler> python3 -c "
import pymysql
c = pymysql.connect(host='host.docker.internal', port=3306,
    user='airflow_vce', password='SIFRE', database='vce')
print('VCE OK:', c.server_version)
c.close()
"

# mailsender connection testi
docker exec -it <airflow-scheduler> python3 -c "
import pymysql
c = pymysql.connect(host='host.docker.internal', port=3306,
    user='airflow_ms_dml', password='SIFRE', database='aws_mailsender_pro_v3')
print('MailSender OK:', c.server_version)
c.close()
"
```

### "Table not found" Hatası

Kural SQL'inde schema prefix eksik olabilir:

```sql
-- ❌ Yanlış — vce schema'sında send_log aranır, bulunamaz
SELECT COUNT(*) FROM send_log WHERE...

-- ✅ Doğru
SELECT COUNT(*) FROM aws_mailsender_pro_v3.send_log WHERE...
```

Mevcut kuralları kontrol et:

```sql
SELECT rule_domain, rule_subdomain, LEFT(sql_statement, 200) as sql_preview
FROM vce.vce_dq_rules
WHERE sql_statement NOT LIKE '%aws_mailsender_pro_v3%'
  AND active_flag = 1;
```

### Trigger Çalışmıyor

```sql
SHOW TRIGGERS FROM vce;
-- trg_vce_rules_after_insert, _after_update, _after_delete görünmeli
```

Görünmüyorsa şemayı yeniden yükle:

```bash
mysql -u root -p vce < sql/01_vce_schema.sql
```

### Partition Sorunu

```sql
-- p_future doldu mu? (yeni partition eklenmesi gerekebilir)
SELECT PARTITION_NAME, TABLE_ROWS
FROM information_schema.PARTITIONS
WHERE TABLE_SCHEMA = 'vce'
  AND TABLE_NAME = 'vce_dq_executions'
  AND PARTITION_NAME = 'p_future';
```

Doluysa yeni ay partition'ı ekle (Partition Yönetimi bölümüne bak).

---

## Katkıda Bulunma

### Yeni Kural

```sql
-- Schema prefix zorunlu!
INSERT INTO vce.vce_dq_rules (..., sql_statement, ...) VALUES (
    ...,
    'SELECT COUNT(*) FROM aws_mailsender_pro_v3.TABLO WHERE ...',
    ...
);
-- Trigger otomatik audit log'a yazar
```

### Yeni Operatör

`VCEBaseOperator`'dan türet. Metodları doğru kullan:

```python
self.run_vce_query(sql)        # vce schema'dan oku
self.run_mailsender_query(sql) # aws_mailsender_pro_v3'ten oku
self.execute_vce_dml(sql)      # vce schema'ya yaz
```

---

<div align="center">
  <sub>VCE · vce schema · aws_mailsender_pro_v3 schema · Apache Airflow · MySQL Partitioning</sub>
</div>
