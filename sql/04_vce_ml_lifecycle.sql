-- =============================================================================
-- VCE — ML Lifecycle + Data Product Mindset Şeması
-- =============================================================================
--
-- Bu dosya üç kavramı MySQL tablolarına dönüştürür:
--
-- ── ML MODEL LİFECYCLE ────────────────────────────────────────────────────
-- 1. vce_anomaly_feedback      — Anomali tespitlerinin doğruluğu işaretlenir
--                                (true positive / false positive ayrımı)
-- 2. vce_concept_drift_log     — Baseline'ın ne zaman sıfırlandığı
--                                (concept drift olayları)
-- 3. vce_model_performance     — Kural bazlı günlük model performans özeti
--                                (precision, recall, false alarm oranı)
--
-- ── DATA PRODUCT MİNDSET ─────────────────────────────────────────────────
-- 4. vce_data_products         — Her tablo bir "data product" olarak kayıtlı
--                                (owner, SLA, consumers, quality threshold)
-- 5. vce_quality_scores        — Her data product için günlük kalite skoru
--                                (geçen kurallar / toplam kural × 100)
-- 6. vce_sla_violations        — SLA ihlalleri kayıtları
--                                (hangi tablo, ne zaman, ne kadar gecikti)
-- 7. vce_data_product_changelog— Şema ve konfigürasyon değişiklik geçmişi
--
-- MEVCUT TABLOLARLA ENTEGRASYON:
--   vce_anomaly_feedback.execution_id → vce_dq_executions.id
--   vce_quality_scores.product_id     → vce_data_products.id
--   vce_sla_violations.product_id     → vce_data_products.id
-- =============================================================================


-- =============================================================================
-- 1. VCE_ANOMALY_FEEDBACK — Anomali Tespiti Geri Bildirimi
-- =============================================================================
-- ML modelinin (Z-skoru anomali tespiti) doğruluğunu takip eder.
--
-- Problem:
--   Şu an sistem "ANOMALİ TESPİT ETTİM" diyor ama gerçekten anomali miydi?
--   False positive (yanlış alarm) oranı nedir?
--   Bu bilgi olmadan modeli iyileştirmek imkânsız.
--
-- Nasıl çalışır:
--   Airflow bildirim gönderince Teams/Slack'te "Gerçek anomali mi?" butonu
--   (webhook ile) veya doğrudan SQL ile geri bildirim verilir.
--   Bu tablo zamanla modelin precision/recall metriklerini hesaplamayı sağlar.
--
-- Precision = Gerçek anomali / Toplam "anomali" dediklerimiz
-- Recall    = Gerçek anomali / Gerçek tüm anomaliler (bilinmesi zor)
-- =============================================================================
CREATE TABLE IF NOT EXISTS vce_anomaly_feedback (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,

    -- Hangi tespit?
    execution_id        BIGINT          COMMENT 'vce_dq_executions.id',
    rule_id             INT NOT NULL,
    rule_domain         VARCHAR(100),
    rule_subdomain      VARCHAR(100),

    -- Tespit detayı (snapshot — execution silinse bile korunsun)
    detected_at         DATETIME        COMMENT 'Anomalinin tespit edildiği zaman',
    z_score             DECIMAL(10,4)   COMMENT 'Tespit anındaki Z-skoru',
    result_value        DECIMAL(20,4)   COMMENT 'Tespit anındaki değer',
    baseline_mean       DECIMAL(20,4)   COMMENT 'Tespit anındaki baseline ortalaması',
    baseline_std        DECIMAL(20,4)   COMMENT 'Tespit anındaki baseline std',

    -- Geri bildirim
    was_true_anomaly    TINYINT(1)      COMMENT '1: gerçek anomali, 0: yanlış alarm, NULL: henüz değerlendirilmedi',
    feedback_by         VARCHAR(100)    COMMENT 'Geri bildirimi veren kullanıcı',
    feedback_at         DATETIME        COMMENT 'Geri bildirimin verildiği zaman',
    root_cause          TEXT            COMMENT 'Anomalinin kök nedeni (ör: kampanya, sistem hatası)',
    action_taken        TEXT            COMMENT 'Alınan aksiyon (ör: threshold güncellendi, batch iptal edildi)',

    created_at          DATETIME DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_rule      (rule_id, detected_at),
    INDEX idx_execution (execution_id),
    INDEX idx_feedback  (was_true_anomaly, feedback_at),
    INDEX idx_domain    (rule_domain, rule_subdomain)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Anomali tespiti geri bildirimi. ML model lifecycle için precision/recall hesabı temeli.';


-- =============================================================================
-- 2. VCE_CONCEPT_DRIFT_LOG — Concept Drift Olayları
-- =============================================================================
-- Modelin baseline'ının ne zaman ve neden sıfırlandığını kayıt altına alır.
--
-- Concept drift nedir?
--   Zaman içinde verinin istatistiksel özellikleri değişir.
--   Örn: MailSender büyüdü, günlük gönderim 500'den 5000'e çıktı.
--   Eski baseline (mean=500) ile yeni veriyi karşılaştırmak anlamsız.
--   Bu durumda baseline sıfırlanmalı ve model yeniden "öğrenmeli".
--
-- İki tür sıfırlama:
--   otomatik: Sistem istatistiksel sapma tespit edince kendisi sıfırlar
--   manuel  : Kullanıcı "bu baseline artık geçersiz" diyerek sıfırlar
-- =============================================================================
CREATE TABLE IF NOT EXISTS vce_concept_drift_log (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,

    rule_id             INT NOT NULL,
    rule_domain         VARCHAR(100),
    rule_subdomain      VARCHAR(100),

    -- Drift tespiti
    drift_type          ENUM('automatic','manual') NOT NULL DEFAULT 'automatic',
    drift_detected_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
    detected_by         VARCHAR(100)    COMMENT 'automatic: sistem, manual: kullanıcı adı',

    -- Eski baseline (sıfırlanmadan önceki değerler)
    old_mean            DECIMAL(20,4),
    old_std             DECIMAL(20,4),
    old_sample_count    INT,
    old_window_days     INT,

    -- Drift metriği
    drift_score         DECIMAL(10,4)   COMMENT 'Eski ve yeni ortalama arasındaki sapma büyüklüğü',
    drift_reason        TEXT            COMMENT 'Neden drift tespit edildi veya manuel sıfırlama sebebi',

    -- Sonuç
    reset_performed     TINYINT(1) NOT NULL DEFAULT 1  COMMENT '1: baseline sıfırlandı',
    new_learning_start  DATETIME        COMMENT 'Yeni baseline biriktirilmeye ne zaman başlandı',

    INDEX idx_rule      (rule_id),
    INDEX idx_date      (drift_detected_at),
    INDEX idx_domain    (rule_domain, rule_subdomain)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Concept drift olayları. Baseline ne zaman neden sıfırlandı.';


-- =============================================================================
-- 3. VCE_MODEL_PERFORMANCE — Günlük Model Performans Özeti
-- =============================================================================
-- Her kural için günlük anomali tespit performansını özetler.
-- Geri bildirimler birikmesiyle precision hesaplanabilir hale gelir.
--
-- Bu tablo şu soruları yanıtlar:
--   "Bu kural son 30 günde kaç anomali tespit etti?"
--   "Bunların kaçı gerçek anomaliydi (geri bildirim verilmişse)?"
--   "False positive oranı artıyor mu?"
--   "Threshold değiştirmeliyiz mi?"
-- =============================================================================
CREATE TABLE IF NOT EXISTS vce_model_performance (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    performance_date    DATE NOT NULL               COMMENT 'Hangi gün için hesaplandı',

    rule_id             INT NOT NULL,
    rule_domain         VARCHAR(100),
    rule_subdomain      VARCHAR(100),

    -- Tespit sayıları
    total_runs          INT DEFAULT 0   COMMENT 'Toplam çalışma sayısı',
    anomaly_detected    INT DEFAULT 0   COMMENT 'Anomali tespit edilen çalışma sayısı',
    anomaly_rate        DECIMAL(8,4)    COMMENT 'Anomali oranı (anomaly_detected / total_runs)',

    -- Geri bildirime dayalı metrikler (feedback varsa dolar)
    true_positives      INT DEFAULT 0   COMMENT 'Gerçek anomali olduğu doğrulanan tespitler',
    false_positives     INT DEFAULT 0   COMMENT 'Yanlış alarm olduğu doğrulanan tespitler',
    unreviewed          INT DEFAULT 0   COMMENT 'Henüz değerlendirilmemiş tespitler',
    precision_score     DECIMAL(8,4)    COMMENT 'TP / (TP + FP) — geri bildirim yeterliyse hesaplanır',

    -- Baseline durumu
    baseline_mean       DECIMAL(20,4)   COMMENT 'Günün başındaki baseline ortalaması',
    baseline_std        DECIMAL(20,4)   COMMENT 'Günün başındaki baseline std',
    avg_z_score         DECIMAL(10,4)   COMMENT 'Günün ortalama Z-skoru',
    drift_occurred      TINYINT(1) DEFAULT 0  COMMENT 'Bu gün concept drift oldu mu?',

    calculated_at       DATETIME DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uk_date_rule (performance_date, rule_id),
    INDEX idx_date      (performance_date),
    INDEX idx_rule      (rule_id),
    INDEX idx_domain    (rule_domain, rule_subdomain)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Günlük anomali model performans özeti. Precision, false positive oranı, drift takibi.';


-- =============================================================================
-- 4. VCE_DATA_PRODUCTS — Data Product Registry
-- =============================================================================
-- Her izlenen tablo bir "data product" olarak kayıt altına alınır.
-- Data product mindset: veri, bir ürün gibi yönetilir.
-- Her ürünün sahibi, SLA'i, kalite eşiği, tüketicileri vardır.
--
-- Bu tablo şu soruları yanıtlar:
--   "send_log tablosunun sahibi kim?"
--   "Bu tablo güncel olmalı — SLA kaç saat?"
--   "Kalite skoru bu hafta %95'in altına düştü mü?"
--   "Bu tabloyu kim/ne kullanıyor?"
-- =============================================================================
CREATE TABLE IF NOT EXISTS vce_data_products (
    id                  INT AUTO_INCREMENT PRIMARY KEY,

    -- Kimlik
    product_name        VARCHAR(200) NOT NULL   COMMENT 'İnsan okunabilir isim (ör: Günlük Gönderim Kaydı)',
    product_code        VARCHAR(100) NOT NULL   COMMENT 'Kısa kod (ör: SEND_LOG_DAILY) — raporlarda kullanılır',
    schema_name         VARCHAR(100) NOT NULL,
    table_name          VARCHAR(200) NOT NULL,

    -- Sahiplik
    owner_name          VARCHAR(100)            COMMENT 'Sorumlu kişi adı',
    owner_email         VARCHAR(200)            COMMENT 'Sorumlu kişi e-posta',
    team                VARCHAR(100)            COMMENT 'Sorumlu ekip',

    -- Açıklama
    description         TEXT                    COMMENT 'Bu data product ne işe yarar?',
    data_classification ENUM('public','internal','confidential','restricted')
                            DEFAULT 'internal'  COMMENT 'Veri sınıflandırması',

    -- SLA tanımları
    freshness_sla_hours INT                     COMMENT 'Kaç saatte bir güncellenmeli? NULL = SLA yok',
    availability_sla_pct DECIMAL(5,2)           COMMENT 'Hedef erişilebilirlik yüzdesi (ör: 99.5)',
    sla_description     TEXT                    COMMENT 'SLA'in insan okunabilir açıklaması',

    -- Kalite eşiği
    quality_threshold   DECIMAL(5,2) DEFAULT 95.00
                                                COMMENT 'Minimum kabul edilebilir kalite skoru (%)',
    quality_action      ENUM('alert','escalate','block') DEFAULT 'alert'
                                                COMMENT 'Eşik altına düşünce ne yapılsın?',

    -- Tüketiciler (JSON array)
    consumers           JSON                    COMMENT '[{"name":"MailSender UI","type":"application"}, ...]',

    -- Bağımlılıklar (JSON array)
    upstream_sources    JSON                    COMMENT 'Bu tabloyu besleyen kaynaklar',
    downstream_targets  JSON                    COMMENT 'Bu tablodan beslenen hedefler',

    -- Durum
    active_flag         TINYINT(1) NOT NULL DEFAULT 1,
    launch_date         DATE                    COMMENT 'Data product ne zaman yayına alındı',
    deprecation_date    DATE                    COMMENT 'Kullanımdan kalkacak tarih (planlanmışsa)',

    insert_timestamp    DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_timestamp    DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    UNIQUE KEY uk_table (schema_name, table_name),
    INDEX idx_owner     (owner_name),
    INDEX idx_active    (active_flag)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Data Product Registry. Her tablo bir ürün olarak kayıtlı — owner, SLA, kalite eşiği.';


-- =============================================================================
-- 5. VCE_QUALITY_SCORES — Günlük Data Product Kalite Skoru
-- =============================================================================
-- Her data product için günlük kalite skoru hesaplanır ve saklanır.
--
-- Kalite Skoru Formülü:
--   score = (geçen_kural_sayısı / toplam_kural_sayısı) × 100
--
-- Ağırlıklı versiyon:
--   fail kuralı warn kuralından daha ağır sayılır.
--   score = (pass + warn×0.5) / (pass + warn + fail) × 100
--
-- Bu tablo şu soruları yanıtlar:
--   "send_log'un bu haftaki ortalama kalite skoru nedir?"
--   "Hangi data product bu ay en çok düşüş yaşadı?"
--   "Kalite skoru %95 eşiğinin altına ne zaman düştü?"
-- =============================================================================
CREATE TABLE IF NOT EXISTS vce_quality_scores (
    id                  BIGINT NOT NULL AUTO_INCREMENT,
    score_date          DATE NOT NULL               COMMENT 'Partition key — hangi güne ait',

    product_id          INT NOT NULL                COMMENT 'vce_data_products.id',
    product_code        VARCHAR(100)                COMMENT 'Hızlı erişim için snapshot',

    -- Kural sayıları (o gün bu ürün için çalışan kurallar)
    total_rules         INT DEFAULT 0,
    passed_rules        INT DEFAULT 0,
    failed_rules        INT DEFAULT 0,
    warned_rules        INT DEFAULT 0,
    error_rules         INT DEFAULT 0               COMMENT 'SQL hatası veren kurallar',

    -- Kalite skoru (0-100)
    quality_score       DECIMAL(6,2)                COMMENT 'Basit: passed/total × 100',
    weighted_score      DECIMAL(6,2)                COMMENT 'Ağırlıklı: fail=1.0, warn=0.5 penalty',

    -- Eşik kontrolü
    threshold           DECIMAL(5,2)                COMMENT 'vce_data_products.quality_threshold snapshot',
    is_below_threshold  TINYINT(1) DEFAULT 0        COMMENT '1: eşiğin altında',

    -- Trend
    previous_score      DECIMAL(6,2)                COMMENT 'Bir önceki günün skoru',
    score_delta         DECIMAL(6,2)                COMMENT 'Değişim (pozitif = iyileşme)',
    trend               ENUM('improving','stable','degrading') DEFAULT 'stable',

    calculated_at       DATETIME DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY         (id, score_date),
    UNIQUE KEY uk_date_product (score_date, product_id),
    INDEX idx_product   (product_id),
    INDEX idx_below     (is_below_threshold, score_date)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Günlük data product kalite skoru. Trend analizi ve SLA takibi için.'
PARTITION BY RANGE (TO_DAYS(score_date)) (
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
-- 6. VCE_SLA_VIOLATIONS — SLA İhlal Kayıtları
-- =============================================================================
-- Data product SLA'larının ihlal edildiği durumları kayıt altına alır.
--
-- İki tür SLA ihlali:
--   freshness : Tablo beklenen sürede güncellenmedi
--   quality   : Kalite skoru eşiğin altına düştü
-- =============================================================================
CREATE TABLE IF NOT EXISTS vce_sla_violations (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,

    product_id          INT NOT NULL,
    product_code        VARCHAR(100),

    violation_type      ENUM('freshness','quality','availability') NOT NULL,
    violation_at        DATETIME DEFAULT CURRENT_TIMESTAMP,

    -- Freshness ihlali için
    expected_update_by  DATETIME                COMMENT 'Ne zamana kadar güncellenmesi gerekiyordu',
    actual_last_update  DATETIME                COMMENT 'Gerçekte en son ne zaman güncellendi',
    delay_hours         DECIMAL(8,2)            COMMENT 'Kaç saat gecikme',

    -- Quality ihlali için
    expected_score      DECIMAL(5,2)            COMMENT 'Minimum kalite skoru eşiği',
    actual_score        DECIMAL(5,2)            COMMENT 'Gerçekleşen kalite skoru',
    score_gap           DECIMAL(5,2)            COMMENT 'Eksik puan (expected - actual)',

    -- Yönetim
    severity            ENUM('low','medium','high','critical') DEFAULT 'medium',
    notified            TINYINT(1) DEFAULT 0    COMMENT 'Bildirim gönderildi mi?',
    resolved_at         DATETIME                COMMENT 'Ne zaman çözüldü?',
    resolution_note     TEXT,

    INDEX idx_product   (product_id),
    INDEX idx_type      (violation_type, violation_at),
    INDEX idx_resolved  (resolved_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Data Product SLA ihlal kayıtları. Freshness ve quality SLA takibi.';


-- =============================================================================
-- 7. VCE_DATA_PRODUCT_CHANGELOG — Data Product Değişiklik Geçmişi
-- =============================================================================
-- Data product'ların şema ve konfigürasyon değişikliklerini kayıt altına alır.
-- MySQL trigger yerine Airflow operatörü veya uygulama katmanı doldurur.
-- =============================================================================
CREATE TABLE IF NOT EXISTS vce_data_product_changelog (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,

    product_id          INT NOT NULL,
    product_code        VARCHAR(100),

    change_type         ENUM(
                            'schema_add_column',
                            'schema_drop_column',
                            'schema_modify_column',
                            'owner_change',
                            'sla_change',
                            'threshold_change',
                            'deprecation',
                            'activation',
                            'consumer_add',
                            'consumer_remove',
                            'custom'
                        ) NOT NULL,

    change_description  TEXT NOT NULL           COMMENT 'Ne değişti, neden?',
    old_value           TEXT                    COMMENT 'Önceki değer',
    new_value           TEXT                    COMMENT 'Yeni değer',

    changed_by          VARCHAR(100),
    changed_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    breaking_change     TINYINT(1) DEFAULT 0    COMMENT '1: Geriye dönük uyumsuz değişiklik',

    INDEX idx_product   (product_id),
    INDEX idx_date      (changed_at),
    INDEX idx_breaking  (breaking_change)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Data product değişiklik geçmişi. Şema, SLA, owner değişiklikleri.';


-- =============================================================================
-- SEED DATA — Data Products (MailSender Pro tabloları)
-- =============================================================================

INSERT INTO vce_data_products (
    product_name, product_code,
    schema_name, table_name,
    owner_name, team,
    description, data_classification,
    freshness_sla_hours, quality_threshold, quality_action,
    sla_description, active_flag, launch_date
) VALUES
(
    'Günlük Gönderim Kaydı', 'SEND_LOG_DAILY',
    'aws_mailsender_pro_v3', 'send_log',
    'Data Team', 'Platform',
    'MailSender Pro üzerinden yapılan tüm e-posta gönderimlerinin kaydı. '
    'Gönderim durumu, hata mesajları, gönderici bilgisi ve zaman damgası içerir. '
    'Raporlama, suppression kontrolü ve performans analizi için temel kaynak.',
    'internal',
    24, 95.00, 'alert',
    'Her gün en az bir gönderim kaydı içermeli. '
    'Kritik kontroller: NULL recipient, failed oranı < %30, suppression ihlali yok.',
    1, CURDATE()
),
(
    'Suppression Listesi', 'SUPPRESSION_LIST',
    'aws_mailsender_pro_v3', 'suppression_list',
    'Data Team', 'Compliance',
    'Bounce, complaint, unsubscribe ve invalid nedeniyle gönderim yapılmayacak '
    'e-posta adresleri. Yasal uyumluluk (GDPR, CAN-SPAM) açısından kritik. '
    'Bu listenin bütünlüğü gönderim altyapısının güvenilirliği için şarttır.',
    'confidential',
    1, 99.00, 'escalate',
    'Her gönderim öncesi kontrol edilmeli. Suppression ihlali = kritik SLA ihlali. '
    'Kalite eşiği %99 — bu listede bütünlük problemi kabul edilemez.',
    1, CURDATE()
),
(
    'Gönderici Konfigürasyonu', 'SENDERS_CONFIG',
    'aws_mailsender_pro_v3', 'senders',
    'Platform Team', 'Platform',
    'SMTP, SES ve API gönderici konfigürasyonları. Her aktif gönderimcinin '
    'bağlantı bilgileri, günlük limit ve warmup ayarları bu tabloda saklanır.',
    'confidential',
    NULL, 90.00, 'alert',
    'SLA yok — konfigürasyon değişikliklerinde anlık kontrol yeterli.',
    1, CURDATE()
),
(
    'Gönderim Kuyruğu', 'SEND_QUEUE',
    'aws_mailsender_pro_v3', 'send_queue',
    'Platform Team', 'Platform',
    'Toplu gönderim görevlerinin yönetim tablosu. Görev durumu, ilerleme, '
    'A/B test konfigürasyonu ve zamanlama bilgileri içerir.',
    'internal',
    6, 90.00, 'alert',
    'Aktif görevler 6 saatten fazla hareket etmeden durmamalı.',
    1, CURDATE()
),
(
    'E-posta Doğrulama İşleri', 'EMAIL_VERIFY_JOBS',
    'aws_mailsender_pro_v3', 'email_verify_jobs',
    'Data Team', 'Platform',
    'E-posta adresi doğrulama işlerinin takip tablosu. Format, MX ve SMTP '
    'doğrulama modlarını destekler. Liste kalitesi yönetimi için kullanılır.',
    'internal',
    NULL, 85.00, 'alert',
    'Tamamlanan işlerde valid oranı %20 üzerinde olmalı.',
    1, CURDATE()
),
(
    'SES Bildirimleri', 'SES_NOTIFICATIONS',
    'aws_mailsender_pro_v3', 'ses_notifications',
    'Platform Team', 'Infrastructure',
    'AWS SES Bounce, Complaint ve Delivery bildirimlerinin kaydı. '
    'SES hesap sağlığının takibi için kritik. Bounce > %10 veya '
    'Complaint > %2 hesap askıya alınma riski oluşturur.',
    'internal',
    1, 95.00, 'escalate',
    'Günlük SES bildirimleri toplanmalı. Bounce oranı < %10, Complaint < %2.',
    1, CURDATE()
);
