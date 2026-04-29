-- =============================================================================
-- VCE (Validity Control Engine) — MailSender Pro Şeması v2
-- MySQL Partitioning ile Data Retention Yönetimi
-- =============================================================================
--
-- PARTİTİONING STRATEJİSİ:
--   Büyüyen 3 tablo aylık RANGE partition ile yönetilir:
--     - vce_dq_executions        (her gün +30 satır)
--     - vce_table_val_executions (her gün +N satır, satır başı büyük)
--     - vce_remediation_log      (her gece +5 satır)
--
--   Partitioning YAPILMAYAN 4 tablo:
--     - vce_dq_rules             (kural tanımları, sabit boyut)
--     - vce_table_validations    (karşılaştırma tanımları, sabit boyut)
--     - vce_rule_audit_log       (haftada 2-3 satır, hiç büyümez)
--     - vce_anomaly_baselines    (kural sayısı kadar satır, üzerine yazılır)
--
-- NEDEN RANGE(TO_DAYS)?
--   MySQL partitioning, partition key'i PRIMARY KEY içermek zorundadır.
--   DATETIME doğrudan partition key olamaz — TO_DAYS() ile INT'e çevrilir.
--   Bu yüzden PRIMARY KEY (id, run_date) şeklinde tanımlanır.
--
-- NEDEN PRIMARY KEY (id, run_date)?
--   MySQL partitioned tablolarda PRIMARY KEY, partition key kolonunu
--   içermek ZORUNDADIR. Sadece id yeterli değildir.
--   Bu bir MySQL kısıtlamasıdır, tasarım tercihimiz değil.
--
-- YENİ PARTITION EKLEMEK:
--   Her ay sonunda Airflow DAG'ı otomatik yeni partition ekler.
--   Manuel eklemek için dosyanın sonundaki referans komutlara bakın.
--
-- ESKİ PARTITION DÜŞÜRMEK:
--   ALTER TABLE vce_dq_executions DROP PARTITION p2026_01;
--   Bu işlem microsaniye sürer — DELETE'den çok daha hızlı.
--   Silinen veri GERİ ALINAMAZ, dikkatli kullanın.
-- =============================================================================


-- =============================================================================
-- 1. VCE_DQ_RULES — Kural Tanımları (Partitionsuz)
-- =============================================================================
-- Neden partition yok?
--   Bu tablo büyümez. Kural sayısı genellikle 50-200 arasında kalır.
--   Her satır ~1 KB. 200 kural = 200 KB. Hiçbir zaman sorun olmaz.
-- =============================================================================
CREATE TABLE IF NOT EXISTS vce_dq_rules (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    rule_domain         VARCHAR(100) NOT NULL   COMMENT 'Ana kategori (ör: send_log, security)',
    rule_subdomain      VARCHAR(100) NOT NULL   COMMENT 'Alt kategori (ör: failed_ratio, null_check)',
    dataset_name        VARCHAR(100)            COMMENT 'Hangi mantıksal veritabanı/dataset',
    table_name          VARCHAR(200)            COMMENT 'Hangi tablo denetleniyor',
    check_type          ENUM('threshold','anomaly','freshness','volume','schema','duplicate','custom')
                            NOT NULL DEFAULT 'threshold',
    sql_statement       TEXT NOT NULL           COMMENT 'Çalıştırılacak SQL. Sonucu > 0 ise ihlal.',
    pre_sql_statement   TEXT                    COMMENT 'Asıl SQL öncesi hazırlık sorgusu',
    action              ENUM('fail', 'warn') NOT NULL DEFAULT 'fail',
    description         TEXT NOT NULL,
    anomaly_threshold   DECIMAL(10,4)           COMMENT 'Z-skoru eşiği (varsayılan: 3.0)',
    execute_time        VARCHAR(10)             COMMENT 'Hangi saatteki çalışmada aktif. Ör: 06:00',
    active_flag         TINYINT(1) NOT NULL DEFAULT 1,
    author              VARCHAR(100),
    test_flag           TINYINT(1) NOT NULL DEFAULT 0,
    non_active_description TEXT,
    insert_timestamp    DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_timestamp    DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_domain    (rule_domain, rule_subdomain),
    INDEX idx_active    (active_flag),
    INDEX idx_time      (execute_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Veri kalitesi kural tanımları. Yeni kural için INSERT yeterli, kod değişmez.';


-- =============================================================================
-- 2. VCE_DQ_EXECUTIONS — Kural Sonuçları (Aylık Partition)
-- =============================================================================
-- Neden partition var?
--   Her DAG çalışmasında 30+ satır eklenir.
--   30 satır/gün x 365 gün = 10.950 satır/yıl (küçük proje)
--   200 satır/gün x 365 gün = 73.000 satır/yıl (büyük proje)
--   Sorgu performansı zamanla düşer — partition ile sadece ilgili
--   ay taranır, diğerleri tamamen atlanır (partition pruning).
--
-- ÖNEMLI: PRIMARY KEY (id, run_date)
--   MySQL partitioned tablolarda partition key (run_date),
--   PRIMARY KEY içinde olmak ZORUNDADIR. MySQL gereksinimidir.
-- =============================================================================
CREATE TABLE IF NOT EXISTS vce_dq_executions (
    id                  BIGINT NOT NULL AUTO_INCREMENT,
    run_date            DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                            COMMENT 'Partition key. Bu kolona göre veriler aylara bölünür.',
    rule_id             INT                     COMMENT 'vce_dq_rules.id',
    rule_domain         VARCHAR(100) NOT NULL,
    rule_subdomain      VARCHAR(100) NOT NULL,
    dataset_name        VARCHAR(100),
    table_name          VARCHAR(200),
    check_type          VARCHAR(50),
    dag_id              VARCHAR(200) NOT NULL,
    dag_task_name       VARCHAR(200),
    dag_run             VARCHAR(500),
    sql_statement       TEXT,
    action              VARCHAR(10),
    result_value        DECIMAL(20,4),
    result_status       ENUM('Passed','Failed','Skipped','Error') NOT NULL DEFAULT 'Passed',
    error_detail        TEXT,
    baseline_mean       DECIMAL(20,4),
    baseline_std        DECIMAL(20,4),
    z_score             DECIMAL(10,4),
    PRIMARY KEY         (id, run_date),
    INDEX idx_domain    (rule_domain, rule_subdomain),
    INDEX idx_status    (result_status),
    INDEX idx_rule_id   (rule_id),
    INDEX idx_dag       (dag_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Her kural çalışmasının sonucu. Aylık partition ile yönetilir.'
PARTITION BY RANGE (TO_DAYS(run_date)) (
    PARTITION p2026_01 VALUES LESS THAN (TO_DAYS('2026-02-01')) COMMENT 'Ocak 2026',
    PARTITION p2026_02 VALUES LESS THAN (TO_DAYS('2026-03-01')) COMMENT 'Subat 2026',
    PARTITION p2026_03 VALUES LESS THAN (TO_DAYS('2026-04-01')) COMMENT 'Mart 2026',
    PARTITION p2026_04 VALUES LESS THAN (TO_DAYS('2026-05-01')) COMMENT 'Nisan 2026',
    PARTITION p2026_05 VALUES LESS THAN (TO_DAYS('2026-06-01')) COMMENT 'Mayis 2026',
    PARTITION p2026_06 VALUES LESS THAN (TO_DAYS('2026-07-01')) COMMENT 'Haziran 2026',
    PARTITION p2026_07 VALUES LESS THAN (TO_DAYS('2026-08-01')) COMMENT 'Temmuz 2026',
    PARTITION p2026_08 VALUES LESS THAN (TO_DAYS('2026-09-01')) COMMENT 'Agustos 2026',
    PARTITION p2026_09 VALUES LESS THAN (TO_DAYS('2026-10-01')) COMMENT 'Eylul 2026',
    PARTITION p2026_10 VALUES LESS THAN (TO_DAYS('2026-11-01')) COMMENT 'Ekim 2026',
    PARTITION p2026_11 VALUES LESS THAN (TO_DAYS('2026-12-01')) COMMENT 'Kasim 2026',
    PARTITION p2026_12 VALUES LESS THAN (TO_DAYS('2027-01-01')) COMMENT 'Aralik 2026',
    PARTITION p_future  VALUES LESS THAN MAXVALUE
        COMMENT 'Guvenlik agi: henuz partition acilmamis gelecek tarihler buraya duser'
);


-- =============================================================================
-- 3. VCE_TABLE_VALIDATIONS — Karşılaştırma Tanımları (Partitionsuz)
-- =============================================================================
CREATE TABLE IF NOT EXISTS vce_table_validations (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    validation_domain   VARCHAR(100) NOT NULL,
    validation_subdomain VARCHAR(100) NOT NULL,
    source_conn_id      VARCHAR(100) NOT NULL,
    source_dataset      VARCHAR(100),
    source_table        VARCHAR(200),
    source_sql          TEXT NOT NULL,
    pre_source_sql      TEXT,
    target_conn_id      VARCHAR(100) NOT NULL,
    target_dataset      VARCHAR(100),
    target_table        VARCHAR(200),
    target_sql          TEXT NOT NULL,
    pre_target_sql      TEXT,
    comparison_type     ENUM('exact','count','sum','tolerance') NOT NULL DEFAULT 'exact',
    tolerance_pct       DECIMAL(5,2),
    action              ENUM('fail','warn') NOT NULL DEFAULT 'fail',
    description         TEXT NOT NULL,
    active_flag         TINYINT(1) NOT NULL DEFAULT 1,
    author              VARCHAR(100),
    execute_time        VARCHAR(10),
    test_flag           TINYINT(1) NOT NULL DEFAULT 0,
    insert_timestamp    DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_timestamp    DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_domain    (validation_domain, validation_subdomain),
    INDEX idx_active    (active_flag)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Kaynak-hedef tablo karsılastırma tanımları.';


-- =============================================================================
-- 4. VCE_TABLE_VAL_EXECUTIONS — Karşılaştırma Sonuçları (Aylık Partition)
-- =============================================================================
-- Neden partition var?
--   source_result ve target_result MEDIUMTEXT kolonları nedeniyle
--   bir satır 5-50 KB olabilir. Eski ayların hızlıca düşürülmesi gerekir.
-- =============================================================================
CREATE TABLE IF NOT EXISTS vce_table_val_executions (
    id                  BIGINT NOT NULL AUTO_INCREMENT,
    run_date            DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                            COMMENT 'Partition key.',
    validation_id       INT,
    validation_domain   VARCHAR(100) NOT NULL,
    validation_subdomain VARCHAR(100) NOT NULL,
    dag_id              VARCHAR(200) NOT NULL,
    dag_task_name       VARCHAR(200),
    dag_run             VARCHAR(500),
    task_retry_count    INT DEFAULT 0,
    source_conn_id      VARCHAR(100),
    source_dataset      VARCHAR(100),
    source_table        VARCHAR(200),
    source_sql          TEXT,
    source_result       MEDIUMTEXT,
    target_conn_id      VARCHAR(100),
    target_dataset      VARCHAR(100),
    target_table        VARCHAR(200),
    target_sql          TEXT,
    target_result       MEDIUMTEXT,
    result_status       ENUM('Passed','Failed','Error') NOT NULL,
    diff_detail         TEXT,
    PRIMARY KEY         (id, run_date),
    INDEX idx_domain    (validation_domain, validation_subdomain),
    INDEX idx_status    (result_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Karsılastırma sonucları. Aylık partition ile yönetilir.'
PARTITION BY RANGE (TO_DAYS(run_date)) (
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
-- 5. VCE_RULE_AUDIT_LOG — Kural Değişiklik Geçmişi (Partitionsuz)
-- =============================================================================
-- Neden partition yok?
--   Haftada 2-3 satır eklenir → yılda ~150 satır → 10 yılda ~1.500 satır.
--   Bu kadar küçük bir tablo için partition gereksiz overhead'dir.
--   Üstelik TÜM geçmiş her zaman erişilebilir kalmalıdır.
--   MySQL trigger'ları tarafından otomatik doldurulur.
-- =============================================================================
CREATE TABLE IF NOT EXISTS vce_rule_audit_log (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    rule_id             INT NOT NULL,
    rule_domain         VARCHAR(100),
    rule_subdomain      VARCHAR(100),
    change_type         ENUM('INSERT','UPDATE','DELETE','ACTIVATE','DEACTIVATE') NOT NULL,
    old_sql             TEXT,
    new_sql             TEXT,
    old_action          VARCHAR(10),
    new_action          VARCHAR(10),
    old_active_flag     TINYINT(1),
    new_active_flag     TINYINT(1),
    changed_by          VARCHAR(100)            COMMENT 'CURRENT_USER() — MySQL trigger doldurur',
    change_reason       TEXT,
    changed_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_rule      (rule_id),
    INDEX idx_changed   (changed_at),
    INDEX idx_type      (change_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Kural degisiklik gecmisi. Partitionsuz — tum gecmis eriselebilir kalır.';


-- =============================================================================
-- 6. VCE_REMEDIATION_LOG — Temizlik Kayıtları (Aylık Partition)
-- =============================================================================
-- Neden partition var?
--   Her gece 4-5 satır eklenir. sql_executed TEXT kolonu satırları büyütür.
--   Eski temizlik kayıtlarının pratik değeri azalır — düşürülebilir.
-- =============================================================================
CREATE TABLE IF NOT EXISTS vce_remediation_log (
    id                  BIGINT NOT NULL AUTO_INCREMENT,
    executed_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                            COMMENT 'Partition key.',
    dag_id              VARCHAR(200) NOT NULL,
    dag_run             VARCHAR(500),
    task_name           VARCHAR(200),
    operation_type      ENUM(
                            'delete_expired_tokens',
                            'delete_old_rate_logs',
                            'delete_old_ses_notif',
                            'delete_expired_unsub',
                            'flag_stuck_queue',
                            'report_suppression_violation',
                            'custom'
                        ) NOT NULL,
    target_table        VARCHAR(200) NOT NULL,
    sql_executed        TEXT,
    rows_affected       INT DEFAULT 0,
    result_status       ENUM('Success','Failed','Warning') NOT NULL DEFAULT 'Success',
    result_detail       TEXT,
    PRIMARY KEY         (id, executed_at),
    INDEX idx_type      (operation_type),
    INDEX idx_table     (target_table)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Otomatik temizlik kayıtları. Aylık partition ile yönetilir.'
PARTITION BY RANGE (TO_DAYS(executed_at)) (
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
-- 7. VCE_ANOMALY_BASELINES — Anomali İstatistikleri (Partitionsuz)
-- =============================================================================
-- Neden partition yok?
--   UNIQUE KEY (rule_id) nedeniyle her kural için sadece 1 satır vardır.
--   DAG her çalışmada INSERT ... ON DUPLICATE KEY UPDATE ile üzerine yazar.
--   Kural sayısı = satır sayısı. 200 kural → 200 satır. Sonsuza kadar sabit.
-- =============================================================================
CREATE TABLE IF NOT EXISTS vce_anomaly_baselines (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    rule_id             INT NOT NULL,
    rule_domain         VARCHAR(100) NOT NULL,
    rule_subdomain      VARCHAR(100) NOT NULL,
    window_days         INT NOT NULL DEFAULT 30,
    sample_count        INT NOT NULL DEFAULT 0,
    mean_value          DECIMAL(20,4),
    std_value           DECIMAL(20,4),
    min_value           DECIMAL(20,4),
    max_value           DECIMAL(20,4),
    p25_value           DECIMAL(20,4),
    p75_value           DECIMAL(20,4),
    last_updated        DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_rule  (rule_id),
    INDEX idx_domain    (rule_domain, rule_subdomain)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Anomali istatistikleri. Kural bası 1 satır, üzerine yazılır, büyümez. Partitionsuz.';


-- =============================================================================
-- MySQL TRIGGER'LAR — vce_rule_audit_log Otomatik Doldurma
-- =============================================================================
-- vce_dq_rules tablosundaki her INSERT/UPDATE/DELETE otomatik audit log'a yazılır.
-- CURRENT_USER() ile değişikliği yapan MySQL kullanıcısı kaydedilir.
-- =============================================================================

DELIMITER //

CREATE TRIGGER trg_vce_rules_after_insert
AFTER INSERT ON vce_dq_rules
FOR EACH ROW
BEGIN
    INSERT INTO vce_rule_audit_log (
        rule_id, rule_domain, rule_subdomain,
        change_type, new_sql, new_action, new_active_flag,
        changed_by, changed_at
    ) VALUES (
        NEW.id, NEW.rule_domain, NEW.rule_subdomain,
        'INSERT', NEW.sql_statement, NEW.action, NEW.active_flag,
        CURRENT_USER(), NOW()
    );
END//

CREATE TRIGGER trg_vce_rules_after_update
AFTER UPDATE ON vce_dq_rules
FOR EACH ROW
BEGIN
    INSERT INTO vce_rule_audit_log (
        rule_id, rule_domain, rule_subdomain,
        change_type,
        old_sql, new_sql,
        old_action, new_action,
        old_active_flag, new_active_flag,
        changed_by, changed_at
    ) VALUES (
        OLD.id, OLD.rule_domain, OLD.rule_subdomain,
        CASE
            WHEN OLD.active_flag = 1 AND NEW.active_flag = 0 THEN 'DEACTIVATE'
            WHEN OLD.active_flag = 0 AND NEW.active_flag = 1 THEN 'ACTIVATE'
            ELSE 'UPDATE'
        END,
        OLD.sql_statement, NEW.sql_statement,
        OLD.action, NEW.action,
        OLD.active_flag, NEW.active_flag,
        CURRENT_USER(), NOW()
    );
END//

CREATE TRIGGER trg_vce_rules_after_delete
AFTER DELETE ON vce_dq_rules
FOR EACH ROW
BEGIN
    INSERT INTO vce_rule_audit_log (
        rule_id, rule_domain, rule_subdomain,
        change_type, old_sql, old_action, old_active_flag,
        changed_by, changed_at
    ) VALUES (
        OLD.id, OLD.rule_domain, OLD.rule_subdomain,
        'DELETE', OLD.sql_statement, OLD.action, OLD.active_flag,
        CURRENT_USER(), NOW()
    );
END//

DELIMITER ;


-- =============================================================================
-- Partition Yönetimi — Manuel Referans Komutları
-- =============================================================================
-- Bu komutları doğrudan çalıştırmayın.
-- Airflow DAG'ı (partition_manager) bunları otomatik yönetir.
-- Acil durumda manuel kullanım için referans olarak tutulmuştur.
-- =============================================================================

/*

-- Mevcut partition'ları ve boyutlarını listele:
SELECT
    PARTITION_NAME,
    TABLE_ROWS,
    ROUND(DATA_LENGTH / 1024 / 1024, 2) AS data_mb,
    ROUND(INDEX_LENGTH / 1024 / 1024, 2) AS index_mb,
    PARTITION_DESCRIPTION
FROM information_schema.PARTITIONS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'vce_dq_executions'
ORDER BY PARTITION_ORDINAL_POSITION;

-- Yeni ay partition'ı ekle (p_future REORGANIZE edilir):
ALTER TABLE vce_dq_executions
REORGANIZE PARTITION p_future INTO (
    PARTITION p2027_01 VALUES LESS THAN (TO_DAYS('2027-02-01')),
    PARTITION p_future  VALUES LESS THAN MAXVALUE
);

-- Diğer partitioned tablolar için aynı işlem:
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

-- Düşürmeden önce kaç satır var kontrol et:
SELECT COUNT(*) FROM vce_dq_executions
PARTITION (p2026_01);

-- Eski partition'ı düşür (VERİ SİLİNİR, GERİ ALINAMAZ):
ALTER TABLE vce_dq_executions        DROP PARTITION p2026_01;
ALTER TABLE vce_table_val_executions DROP PARTITION p2026_01;
ALTER TABLE vce_remediation_log      DROP PARTITION p2026_01;

*/
