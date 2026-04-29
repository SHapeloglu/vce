-- =============================================================================
-- VCE — Genişletilmiş Özellikler Şeması
-- Great Expectations ve Soda Core'dan İlham Alınan Eklemeler
-- =============================================================================
--
-- Bu dosya 3 yeni tablo ve 1 yeni kural tipi ekler:
--
-- 1. vce_failed_rows_samples  — Kural fail ettiğinde örnek ihlal satırları
--    İlham: Great Expectations "row-level validation" + Soda "failed rows sampling"
--    Değer : "Hangi 5 satır bu kuralı ihlal etti?" sorusunu yanıtlar
--
-- 2. vce_column_stats          — Tablo/kolon profili istatistikleri
--    İlham: Great Expectations "column-level stats" + Soda "metric store"
--    Değer : NULL oranı, distinct sayısı, min/max/mean/std günlük olarak saklanır
--            Trend analizi: "Bu kolonun NULL oranı artıyor mu?"
--
-- 3. vce_distribution_checks   — Değer dağılımı kontrol tanımları
--    İlham: Soda Core "distribution check"
--    Değer : Bir kolonun değer dağılımının beklenen aralıkta olup olmadığını
--            kontrol eder. Örn: send_log'da 'sent' oranı %70'in üzerinde olmalı.
--
-- MEVCUT TABLOLARLA ENTEGRASYON:
--   vce_dq_rules.check_type ENUM'una 'distribution' eklenir.
--   vce_dq_rules.sample_sql kolonu eklenir — fail eden satırları çeken SQL.
--   vce_dq_executions'a sample_count kolonu eklenir.
-- =============================================================================


-- =============================================================================
-- 1. VCE_FAILED_ROWS_SAMPLES — İhlal Eden Satır Örnekleri
-- =============================================================================
-- Great Expectations ve Soda'nın en güçlü özelliklerinden biri:
-- Bir kural fail ettiğinde sadece "142 satır ihlal var" demek yerine
-- hangi satırların ihlal ettiğini göstermek.
--
-- Nasıl çalışır:
--   vce_dq_rules tablosuna 'sample_sql' kolonu eklenir.
--   Bu SQL, ihlal eden satırları döndürür (LIMIT 10 ile sınırlandırılır).
--   Kural fail ettiğinde DataQualityOperator bu SQL'i de çalıştırır
--   ve sonucu bu tabloya yazar.
--
-- Örnek kullanım:
--   Kural: "recipient NULL olan satır sayısı"
--   sample_sql: SELECT id, recipient, sender_id, sent_at
--               FROM aws_mailsender_pro_v3.send_log
--               WHERE recipient IS NULL
--               LIMIT 10
--
-- Partition: Aylık — vce_dq_executions gibi büyür
-- =============================================================================
CREATE TABLE IF NOT EXISTS vce_failed_rows_samples (
    id                  BIGINT NOT NULL AUTO_INCREMENT,
    sampled_at          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                            COMMENT 'Partition key.',

    -- Hangi kural çalışmasına ait?
    execution_id        BIGINT          COMMENT 'vce_dq_executions.id — hangi çalışmada alındı',
    rule_id             INT             COMMENT 'vce_dq_rules.id',
    rule_domain         VARCHAR(100) NOT NULL,
    rule_subdomain      VARCHAR(100) NOT NULL,

    -- Airflow bağlamı
    dag_id              VARCHAR(200),
    dag_run             VARCHAR(500),

    -- Örnek veri
    sample_sql          TEXT            COMMENT 'Örnek satırları çeken SQL',
    sample_data         MEDIUMTEXT      COMMENT 'JSON formatında örnek satırlar (max 10 satır)',
    sample_count        INT DEFAULT 0   COMMENT 'Kaç ihlal satırı döndü (LIMIT öncesi toplam)',
    total_violation_count BIGINT        COMMENT 'Toplam ihlal sayısı (ana SQL sonucu)',

    PRIMARY KEY         (id, sampled_at),
    INDEX idx_rule      (rule_id, sampled_at),
    INDEX idx_domain    (rule_domain, rule_subdomain)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Fail eden kuralların örnek ihlal satırları. GE row-level validation + Soda failed rows sampling ilhamıyla.'

PARTITION BY RANGE (TO_DAYS(sampled_at)) (
    PARTITION p2026_01 VALUES LESS THAN (TO_DAYS('2026-02-01')),
    PARTITION p2026_02 VALUES LESS THAN (TO_DAYS('2026-03-01')),
    PARTITION p2026_03 VALUES LESS THAN (TO_DAYS('2026-04-01')),
    PARTITION p2026_04 VALUES LESS THAN (TO_DAYS('2026-05-01')),
    PARTITION p2026_05 VALUES LESS THAN (TO_DAYS('2026-06-01')),
    PARTITION p2026_06 VALUES LESS THAN (TO_DAYS('2026-07-01')),
    PARTITION p2026_07 VALUES LESS THAN (TO_DAYS('2026-08-01')),
    PARTITION p2026_08 VALUES LESS THAN (TO_DAYS('2026-09-01')),
    PARTITION p2026_09 VALUES LESS THAN (TO_DAYS('2026-10-01')),
    PARTITION p2026_10 VALUES LESS THAN (TO_DAYS('2026-11-01')),
    PARTITION p2026_11 VALUES LESS THAN (TO_DAYS('2026-12-01')),
    PARTITION p2026_12 VALUES LESS THAN (TO_DAYS('2027-01-01')),
    PARTITION p_future  VALUES LESS THAN MAXVALUE
);


-- =============================================================================
-- 2. VCE_COLUMN_STATS — Kolon Profili İstatistikleri
-- =============================================================================
-- Great Expectations'ın "column-level stats" özelliğinin MySQL karşılığı.
-- Her tablo ve kolon için günlük istatistikler hesaplanır ve saklanır.
--
-- Neden değerli?
--   - "send_log.recipient kolonunun NULL oranı bu ay arttı mı?" → Evet: %0.1 → %2.3
--   - "suppression_list kaç benzersiz email içeriyor?" → Trend: 10k → 15k → 18k
--   - "send_log'daki status değer dağılımı değişti mi?" → sent:%95 → sent:%72 (alarm!)
--
-- Nasıl çalışır:
--   vce_column_stats_config tablosundaki tanımlardan hangi tablo/kolonun
--   izleneceği belirlenir. DataQualityOperator günlük bu istatistikleri hesaplar.
--
-- Partition: Aylık
-- =============================================================================
CREATE TABLE IF NOT EXISTS vce_column_stats (
    id                  BIGINT NOT NULL AUTO_INCREMENT,
    collected_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                            COMMENT 'Partition key — istatistiğin toplandığı zaman',

    -- Hangi tablo/kolon?
    schema_name         VARCHAR(100) NOT NULL   COMMENT 'aws_mailsender_pro_v3',
    table_name          VARCHAR(200) NOT NULL,
    column_name         VARCHAR(200) NOT NULL,
    dag_run             VARCHAR(500),

    -- Temel istatistikler (tüm tipler için)
    row_count           BIGINT          COMMENT 'Toplam satır sayısı',
    null_count          BIGINT          COMMENT 'NULL değer sayısı',
    null_rate           DECIMAL(8,4)    COMMENT 'NULL oranı (0.00-1.00). 0.05 = %5',
    distinct_count      BIGINT          COMMENT 'Benzersiz değer sayısı',
    distinct_rate       DECIMAL(8,4)    COMMENT 'Benzersizlik oranı. 1.0 = tamamen unique',

    -- Sayısal kolonlar için (INT, DECIMAL, FLOAT)
    min_value           DECIMAL(20,4)   COMMENT 'Minimum değer',
    max_value           DECIMAL(20,4)   COMMENT 'Maksimum değer',
    mean_value          DECIMAL(20,4)   COMMENT 'Ortalama',
    std_value           DECIMAL(20,4)   COMMENT 'Standart sapma',
    p25_value           DECIMAL(20,4)   COMMENT '25. persentil',
    p50_value           DECIMAL(20,4)   COMMENT 'Medyan (50. persentil)',
    p75_value           DECIMAL(20,4)   COMMENT '75. persentil',

    -- Metin kolonlar için (VARCHAR, TEXT)
    min_length          INT             COMMENT 'En kısa değerin karakter sayısı',
    max_length          INT             COMMENT 'En uzun değerin karakter sayısı',
    avg_length          DECIMAL(10,2)   COMMENT 'Ortalama karakter uzunluğu',

    -- Zaman kolonlar için (DATETIME, DATE)
    min_date            DATETIME        COMMENT 'En eski tarih',
    max_date            DATETIME        COMMENT 'En yeni tarih',
    date_range_days     INT             COMMENT 'max_date - min_date (gün cinsinden)',

    -- Değer frekansı (en çok tekrar eden 5 değer — JSON)
    top_values          TEXT            COMMENT 'JSON: [{"value":"sent","count":9821}, ...]',

    PRIMARY KEY         (id, collected_at),
    INDEX idx_table     (schema_name, table_name, column_name),
    INDEX idx_date      (collected_at)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Kolon profili istatistikleri. GE column-level stats ilhamıyla. Trend analizi için temel.'

PARTITION BY RANGE (TO_DAYS(collected_at)) (
    PARTITION p2026_01 VALUES LESS THAN (TO_DAYS('2026-02-01')),
    PARTITION p2026_02 VALUES LESS THAN (TO_DAYS('2026-03-01')),
    PARTITION p2026_03 VALUES LESS THAN (TO_DAYS('2026-04-01')),
    PARTITION p2026_04 VALUES LESS THAN (TO_DAYS('2026-05-01')),
    PARTITION p2026_05 VALUES LESS THAN (TO_DAYS('2026-06-01')),
    PARTITION p2026_06 VALUES LESS THAN (TO_DAYS('2026-07-01')),
    PARTITION p2026_07 VALUES LESS THAN (TO_DAYS('2026-08-01')),
    PARTITION p2026_08 VALUES LESS THAN (TO_DAYS('2026-09-01')),
    PARTITION p2026_09 VALUES LESS THAN (TO_DAYS('2026-10-01')),
    PARTITION p2026_10 VALUES LESS THAN (TO_DAYS('2026-11-01')),
    PARTITION p2026_11 VALUES LESS THAN (TO_DAYS('2026-12-01')),
    PARTITION p2026_12 VALUES LESS THAN (TO_DAYS('2027-01-01')),
    PARTITION p_future  VALUES LESS THAN MAXVALUE
);


-- =============================================================================
-- 2b. VCE_COLUMN_STATS_CONFIG — Hangi Kolon İzlenecek?
-- =============================================================================
-- Bu tablo hangi tablo/kolonun istatistiklerinin toplanacağını tanımlar.
-- Partitionsuz — konfigürasyon tablosu, küçük boyutlu.
-- =============================================================================
CREATE TABLE IF NOT EXISTS vce_column_stats_config (
    id                  INT AUTO_INCREMENT PRIMARY KEY,

    schema_name         VARCHAR(100) NOT NULL   COMMENT 'aws_mailsender_pro_v3',
    table_name          VARCHAR(200) NOT NULL,
    column_name         VARCHAR(200) NOT NULL,
    column_type         ENUM('numeric','text','datetime','categorical')
                            NOT NULL DEFAULT 'numeric'
                            COMMENT 'Kolon tipi — hangi istatistiklerin hesaplanacağını belirler',

    -- Hangi istatistikler hesaplansın?
    collect_nulls       TINYINT(1) NOT NULL DEFAULT 1  COMMENT 'NULL oranı hesaplansın mı?',
    collect_distinct    TINYINT(1) NOT NULL DEFAULT 1  COMMENT 'Distinct sayısı hesaplansın mı?',
    collect_stats       TINYINT(1) NOT NULL DEFAULT 1  COMMENT 'min/max/mean/std hesaplansın mı?',
    collect_top_values  TINYINT(1) NOT NULL DEFAULT 0  COMMENT 'En çok tekrar eden değerler hesaplansın mı?',
    top_values_limit    INT NOT NULL DEFAULT 5         COMMENT 'Kaç top değer saklanacak',

    -- Uyarı eşikleri (istatistik beklenen aralık dışına çıkarsa)
    max_null_rate       DECIMAL(8,4)    COMMENT 'Bu oranı geçerse warn. Ör: 0.05 = %5',
    min_distinct_count  BIGINT          COMMENT 'Bu sayının altına düşerse warn',

    active_flag         TINYINT(1) NOT NULL DEFAULT 1,
    author              VARCHAR(100),
    insert_timestamp    DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uk_col   (schema_name, table_name, column_name),
    INDEX idx_active    (active_flag)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Hangi tablo/kolonun istatistiklerinin toplanacağını tanımlar.';


-- =============================================================================
-- 3. VCE_DISTRIBUTION_CHECKS — Değer Dağılımı Kontrol Tanımları
-- =============================================================================
-- Soda Core'un "distribution check" özelliğinin MySQL karşılığı.
--
-- Klasik threshold kontrolü:
--   "send_log'da failed satır sayısı > 100 ise fail"  → COUNT bazlı
--
-- Distribution check:
--   "send_log.status kolonunun dağılımı beklenen gibi mi?"
--   Beklenen: sent: %80-95, failed: %1-15, skipped: %1-10
--   Gerçek  : sent: %45, failed: %50, skipped: %5 → ANOMALİ!
--
-- Neden önemli?
--   COUNT bazlı kontroller toplam sayıyı kontrol eder.
--   Distribution check oranları kontrol eder.
--   Toplam gönderim sayısı 10x artsa bile oranlar stabil kalmalı.
--
-- Nasıl çalışır:
--   Bu tabloda hangi kolonun hangi değerinin hangi oranda olması gerektiği
--   tanımlanır. DistributionCheckOperator günlük bu oranları hesaplar,
--   beklenen aralıkla karşılaştırır, sonucu vce_dq_executions'a yazar.
-- =============================================================================
CREATE TABLE IF NOT EXISTS vce_distribution_checks (
    id                  INT AUTO_INCREMENT PRIMARY KEY,

    -- Gruplama
    check_domain        VARCHAR(100) NOT NULL   COMMENT 'Ana kategori (ör: send_log)',
    check_subdomain     VARCHAR(100) NOT NULL   COMMENT 'Alt kategori (ör: status_distribution)',

    -- Hedef
    schema_name         VARCHAR(100) NOT NULL   COMMENT 'aws_mailsender_pro_v3',
    table_name          VARCHAR(200) NOT NULL,
    column_name         VARCHAR(200) NOT NULL   COMMENT 'Dağılımı kontrol edilecek kolon',

    -- Filtre (opsiyonel — sadece belirli kayıtlar)
    where_clause        TEXT                    COMMENT 'Ör: sent_at >= NOW() - INTERVAL 24 HOUR',

    -- Beklenen dağılım (JSON formatında)
    -- Format: [{"value": "sent", "min_pct": 70, "max_pct": 98}, ...]
    -- min_pct ve max_pct: 0-100 arasında yüzde değer
    expected_distribution JSON NOT NULL         COMMENT 'Her değer için beklenen yüzde aralığı',

    -- Aksiyon
    action              ENUM('fail','warn') NOT NULL DEFAULT 'warn',
    description         TEXT NOT NULL,

    active_flag         TINYINT(1) NOT NULL DEFAULT 1,
    author              VARCHAR(100),
    insert_timestamp    DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_timestamp    DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_domain    (check_domain, check_subdomain),
    INDEX idx_active    (active_flag)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Değer dağılımı kontrol tanımları. Soda distribution check ilhamıyla.';


-- =============================================================================
-- MEVCUT TABLOLARA EKLENECEKLERİ
-- =============================================================================

-- vce_dq_rules'a sample_sql kolonu ekle
-- Kural fail ettiğinde hangi satırlar sorunlu? — bu SQL'i çalıştır
ALTER TABLE vce_dq_rules
    ADD COLUMN IF NOT EXISTS sample_sql TEXT
        COMMENT 'Fail eden satırları döndüren SQL (LIMIT 10 ile). NULL ise sample alınmaz.'
    AFTER pre_sql_statement;

-- vce_dq_executions'a sample_count kolonu ekle
-- Kaç satır örnek alındığını kaydet
ALTER TABLE vce_dq_executions
    ADD COLUMN IF NOT EXISTS sample_count INT DEFAULT 0
        COMMENT 'Kaç örnek ihlal satırı toplandı (vce_failed_rows_samples tablosuna bakınız)'
    AFTER z_score;


-- =============================================================================
-- SEED DATA — Distribution Check Örnekleri (MailSender Pro)
-- =============================================================================

INSERT INTO vce_distribution_checks
    (check_domain, check_subdomain, schema_name, table_name, column_name,
     where_clause, expected_distribution, action, description, active_flag, author)
VALUES
(
    'send_log', 'status_distribution',
    'aws_mailsender_pro_v3', 'send_log', 'status',
    -- Son 24 saatteki kayıtlara bak
    "sent_at >= NOW() - INTERVAL 24 HOUR",
    -- Beklenen dağılım:
    --   sent   : %60-%98 → normal başarı aralığı
    --   failed : %1-%30  → %30'u geçerse ciddi sorun
    --   skipped: %1-%40  → suppression/kural nedeniyle atlananlar
    '[
        {"value": "sent",    "min_pct": 60, "max_pct": 98},
        {"value": "failed",  "min_pct": 1,  "max_pct": 30},
        {"value": "skipped", "min_pct": 1,  "max_pct": 40}
    ]',
    'fail',
    'send_log.status kolonunun günlük dağılımının beklenen aralıkta olup olmadığını '
    'kontrol eder. sent oranı %60 altına düşerse veya failed oranı %30 üzerine çıkarsa '
    'ciddi gönderim sorunu var demektir. Toplam sayı değil oranlar kontrol edilir.',
    1, 'system-seed'
),
(
    'send_log', 'provider_distribution',
    'aws_mailsender_pro_v3', 'send_log', 'provider',
    "sent_at >= NOW() - INTERVAL 24 HOUR AND provider IS NOT NULL",
    -- Beklenen provider dağılımı (kuruluma göre özelleştir)
    '[
        {"value": "smtp", "min_pct": 0,  "max_pct": 100},
        {"value": "ses",  "min_pct": 0,  "max_pct": 100},
        {"value": "api",  "min_pct": 0,  "max_pct": 100}
    ]',
    'warn',
    'send_log.provider kolonunun dağılımını izler. Ani değişimler '
    '(ör: SES\'in sıfıra düşmesi) gönderici konfigürasyonu sorununu gösterebilir.',
    1, 'system-seed'
),
(
    'ses_notifications', 'notification_type_distribution',
    'aws_mailsender_pro_v3', 'ses_notifications', 'notif_type',
    "received_at >= NOW() - INTERVAL 24 HOUR",
    -- SES Bounce oranı çok yüksekse hesap askıya alınabilir
    '[
        {"value": "Delivery",   "min_pct": 70, "max_pct": 100},
        {"value": "Bounce",     "min_pct": 0,  "max_pct": 10},
        {"value": "Complaint",  "min_pct": 0,  "max_pct": 2}
    ]',
    'fail',
    'AWS SES bildirimlerinin tipini izler. Bounce oranı %10\'u veya Complaint oranı '
    '%2\'yi geçerse AWS SES hesabı askıya alınabilir. Bu kontrol SES hesap sağlığı '
    'için kritiktir.',
    1, 'system-seed'
),
(
    'email_verify_jobs', 'validity_distribution',
    'aws_mailsender_pro_v3', 'email_verify_jobs', 'valid_count',
    -- Son 7 günde tamamlanan işlerde valid/invalid oranı
    "status = 'done' AND finished_at >= NOW() - INTERVAL 7 DAY AND total_count >= 100",
    -- Bu tip bir dağılım kontrolü için özel yorum:
    -- valid_count / total_count oranı beklenen aralıkta mı?
    -- Burada value yerine oran aralığı kullanılır (özel mantık)
    '[
        {"metric": "valid_rate", "min_pct": 20, "max_pct": 100,
         "description": "Valid email oranı en az %20 olmalı. Altı liste kalitesizdir."}
    ]',
    'warn',
    'Doğrulama işlemlerinde valid email oranının kabul edilebilir aralıkta olup '
    'olmadığını kontrol eder. %20 altı liste kalitesi gönderim itibarını ciddi '
    'şekilde olumsuz etkiler.',
    1, 'system-seed'
);


-- =============================================================================
-- SEED DATA — Column Stats Config (İzlenecek Kolonlar)
-- =============================================================================

INSERT INTO vce_column_stats_config
    (schema_name, table_name, column_name, column_type,
     collect_nulls, collect_distinct, collect_stats, collect_top_values,
     max_null_rate, active_flag, author)
VALUES
-- send_log: kritik kolonlar
('aws_mailsender_pro_v3', 'send_log', 'recipient',  'text',        1, 1, 0, 0, 0.001, 1, 'system-seed'),
('aws_mailsender_pro_v3', 'send_log', 'status',     'categorical', 1, 1, 0, 1, 0.000, 1, 'system-seed'),
('aws_mailsender_pro_v3', 'send_log', 'sender_id',  'numeric',     1, 1, 1, 0, 0.000, 1, 'system-seed'),
('aws_mailsender_pro_v3', 'send_log', 'provider',   'categorical', 1, 1, 0, 1, 0.050, 1, 'system-seed'),

-- suppression_list: büyüme izleme
('aws_mailsender_pro_v3', 'suppression_list', 'email',  'text',        1, 1, 0, 0, 0.000, 1, 'system-seed'),
('aws_mailsender_pro_v3', 'suppression_list', 'reason', 'categorical', 1, 1, 0, 1, 0.000, 1, 'system-seed'),

-- senders: konfigürasyon kalitesi
('aws_mailsender_pro_v3', 'senders', 'sender_mode', 'categorical', 1, 1, 0, 1, 0.000, 1, 'system-seed'),
('aws_mailsender_pro_v3', 'senders', 'daily_limit', 'numeric',     1, 0, 1, 0, 0.000, 1, 'system-seed'),

-- send_queue: kuyruk sağlığı
('aws_mailsender_pro_v3', 'send_queue', 'status',     'categorical', 1, 1, 0, 1, 0.000, 1, 'system-seed'),
('aws_mailsender_pro_v3', 'send_queue', 'sent_count', 'numeric',     1, 0, 1, 0, 0.000, 1, 'system-seed');
